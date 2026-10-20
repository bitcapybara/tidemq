use super::*;

use tidemq::metadata::{PartitionKey, Route};
use tidemq::transport::{
    Client as NativeClient, ClientCredentials, ClientTls as NativeTls, NodeGrant, TlsIdentity,
    TransportConfig,
    rpc::{RpcClient, RpcConfig},
};
use tidemq_protocol::{
    ProtocolLimits,
    common::{ErrorCode, ErrorReply, Frame, FrameKind, TrafficClass},
    framing::{decode_payload, encode_payload, new_frame},
    internal::{InternalCommand, InternalResult},
};

#[derive(Clone, PartialEq, prost::Message)]
struct Request {
    #[prost(uint32, tag = "1")]
    kind: u32,
    #[prost(bytes = "bytes", tag = "2")]
    body: Bytes,
    #[prost(string, tag = "3")]
    principal: String,
    #[prost(string, tag = "4")]
    scope: String,
    #[prost(uint32, tag = "5")]
    hops: u32,
}

struct Observer<'a> {
    fixture: &'a Fixture,
    node: u64,
    client: NativeClient,
    rpcs: BTreeMap<u64, RpcClient>,
}

impl<'a> Observer<'a> {
    async fn new(fixture: &'a Fixture, node: u64) -> Self {
        let config = fixture.configs.get(&node).unwrap();
        let cluster = config.cluster_id.parse().unwrap();
        let identity = TlsIdentity::new(
            tokio::fs::read(&config.tls.certificate)
                .await
                .unwrap()
                .into(),
            tokio::fs::read(&config.tls.private_key)
                .await
                .unwrap()
                .into(),
        )
        .unwrap();
        let mut grants = Vec::new();
        for peer in &config.runtime.peers {
            grants.push(NodeGrant {
                certificate_pin: NodeGrant::certificate_pin(
                    &tokio::fs::read(&peer.certificate_der).await.unwrap(),
                ),
                certificate_name: peer.server_name.clone(),
                cluster_id: cluster,
                node_id: peer.node_id,
                role: peer.role.name().into(),
                allowed_groups: vec![1, 2],
                allowed_classes: vec![TrafficClass::Control, TrafficClass::Produce],
            });
        }
        let client = NativeClient::bind(
            "127.0.0.1:0".parse().unwrap(),
            NativeTls {
                trust_roots: fixture.roots.clone(),
                identity: Some(identity),
                expected_nodes: grants,
            },
            TransportConfig::default(),
            tidemq::budget::MemoryBudget::new(&tidemq::config::MemoryConfig::default()).unwrap(),
        )
        .unwrap();
        Self {
            fixture,
            node,
            client,
            rpcs: BTreeMap::new(),
        }
    }

    async fn call(
        &mut self,
        target: u64,
        group: u64,
        request: Request,
        until: Instant,
    ) -> Option<Frame> {
        let config = self.fixture.configs.get(&self.node).unwrap();
        let peer = config.runtime.peer(target).unwrap();
        let cluster = config.cluster_id.parse().unwrap();
        if self.rpcs.get(&target).is_none_or(|rpc| rpc.is_closed()) {
            let session = self
                .client
                .connect(
                    PeerTarget {
                        address: peer.internal_address,
                        server_name: peer.server_name.clone(),
                        local_role: config.role.name().into(),
                        remote_role: peer.role.name().into(),
                        cluster_id: Some(cluster),
                        node_id: Some(target),
                    },
                    &ClientCredentials::Internal {
                        session_id: Id::random().unwrap(),
                        cluster_id: cluster,
                        node_id: self.node,
                        role: config.role.name().into(),
                    },
                    TrafficClass::Control,
                    until,
                )
                .await
                .ok()?;
            let rpc = RpcClient::from_session_with_lanes(session, RpcConfig::default(), 2, until)
                .await
                .ok()?;
            self.rpcs.insert(target, rpc);
        }
        let limits = ProtocolLimits::default();
        let operation = Id::random().unwrap();
        let frame = new_frame(
            FrameKind::InternalCommand,
            TrafficClass::Control,
            Id::random().unwrap(),
            operation,
            &InternalCommand {
                group_id: group,
                operation_id: Bytes::copy_from_slice(operation.as_bytes()),
                encoded_command: encode_payload(&request, limits.max_control_frame_bytes).unwrap(),
            },
            u64::try_from(until.saturating_duration_since(Instant::now()).as_millis())
                .unwrap()
                .clamp(1, 300_000),
            &limits,
        )
        .unwrap();
        let response = self
            .rpcs
            .get(&target)
            .unwrap()
            .call(frame, until)
            .await
            .ok()?;
        Some(response.frame().clone())
    }

    async fn route(&mut self, partition: PartitionKey, until: Instant) -> Option<Route> {
        for node in [11, 12, 13] {
            let frame = self
                .call(
                    node,
                    1,
                    Request {
                        kind: 1,
                        body: serde_json::to_vec(&serde_json::json!({"Partition": partition}))
                            .unwrap()
                            .into(),
                        ..Request::default()
                    },
                    until.min(Instant::now() + Duration::from_millis(500)),
                )
                .await;
            let Some(frame) =
                frame.filter(|frame| frame.kind == i32::from(FrameKind::InternalResult))
            else {
                continue;
            };
            let result: InternalResult =
                decode_payload(frame.payload, &ProtocolLimits::default()).unwrap();
            let proof: serde_json::Value = serde_json::from_slice(&result.encoded_result).unwrap();
            return proof
                .get("value")?
                .get("Partition")?
                .get("route")
                .and_then(|route| serde_json::from_value(route.clone()).ok());
        }
        None
    }

    async fn wait_owner(&mut self, partition: PartitionKey, expected: u64) -> Route {
        let until = deadline();
        loop {
            if let Some(route) = self.route(partition, until).await
                && route.owner_node == expected
            {
                return route;
            }
            assert!(
                Instant::now() < until,
                "Metadata did not automatically activate the expected Broker"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn close(self) {
        for rpc in self.rpcs.values() {
            rpc.shutdown(deadline()).await;
        }
        self.client.close(deadline()).await.unwrap();
    }
}

async fn data_leader(fixture: &Fixture) -> u64 {
    let response = http(
        fixture.configs.get(&21).unwrap().management_listen,
        "/diagnostics",
    )
    .await;
    let value: serde_json::Value =
        serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap();
    value
        .get("data_groups")
        .unwrap()
        .as_array()
        .unwrap()
        .first()
        .unwrap()
        .get("leader")
        .unwrap()
        .as_u64()
        .unwrap()
}

async fn assert_old_broker_fenced(fixture: &Fixture, partition: PartitionKey, epoch: u64) {
    let mut old = Observer::new(fixture, 31).await;
    let limits = ProtocolLimits::default();
    let inner = new_frame(
        FrameKind::OpenProducer,
        TrafficClass::Control,
        Id::random().unwrap(),
        Id::random().unwrap(),
        &public::OpenProducer {
            topic_id: Bytes::copy_from_slice(partition.topic_id.as_bytes()),
            partition_id: partition.partition,
            producer_id: Bytes::copy_from_slice(Id::random().unwrap().as_bytes()),
            expected_registration_epoch: 0,
            owner_epoch: epoch,
        },
        1000,
        &limits,
    )
    .unwrap();
    let response = old
        .call(
            data_leader(fixture).await,
            2,
            Request {
                kind: 3,
                body: encode_payload(&inner, limits.max_control_frame_bytes).unwrap(),
                principal: "runtime-test".into(),
                scope: "*".into(),
                hops: 2,
            },
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.kind,
        i32::from(FrameKind::Error),
        "An old Broker must never regain write authority"
    );
    let error: ErrorReply = decode_payload(response.payload, &limits).unwrap();
    assert_eq!(error.code, i32::from(ErrorCode::Fenced));
    old.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_rf3_broker_crash_automatically_fences_and_resumes_original_sdk() {
    let nodes = [
        (11, Role::Metadata),
        (12, Role::Metadata),
        (13, Role::Metadata),
        (21, Role::Storage),
        (22, Role::Storage),
        (23, Role::Storage),
        (31, Role::Broker),
        (32, Role::Broker),
        (41, Role::Gateway),
    ];
    let mut fixture = Fixture::new(&nodes, &[11, 12, 13], &[21, 22, 23]).await;
    for config in fixture.configs.values_mut() {
        config.runtime.capacity.process_memory = tidemq::quota::Limits::default().process_memory;
    }
    let mut processes = BTreeMap::new();
    for (node, _) in nodes {
        processes.insert(node, spawn_process(&fixture, node).await);
    }
    for (node, process) in &mut processes {
        wait_process(&fixture, *node, process).await;
    }
    let client = fixture.client(41).await;
    let (topic, subscription) = create(&client).await;
    let partition = PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
        .await
        .unwrap();
    assert_eq!(durable_produce(&producer, b"before-broker-crash").await, 0);
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    durable_consume(&consumer, 0).await;
    let mut observer = Observer::new(&fixture, 41).await;
    let before = observer.wait_owner(partition, 31).await;
    processes.get_mut(&31).unwrap().kill().await.unwrap();
    let fault = Instant::now();
    assert!(
        tidemq_sdk::RequestContext::with_deadline(Instant::now() + Duration::from_millis(50))
            .scope(producer.produce(vec![public::Message {
                key: Bytes::new(),
                body: Bytes::from_static(b"after-broker-crash")
            }]))
            .await
            .is_err()
    );
    let pending = producer.pending_batches().await.unwrap();
    let batch = pending.first().unwrap();
    assert_eq!(durable_retry(&producer, batch, deadline()).await, 1);
    durable_consume(&consumer, 1).await;
    eprintln!(
        "Broker SIGKILL to original SDK durable producer and consumer ACK: {:?}",
        fault.elapsed()
    );
    let after = observer.wait_owner(partition, 32).await;
    assert_eq!(after.epoch, before.epoch + 1);
    assert_eq!(after.generation, before.generation + 1);
    processes.insert(31, spawn_process(&fixture, 31).await);
    wait_process(&fixture, 31, processes.get_mut(&31).unwrap()).await;
    assert_old_broker_fenced(&fixture, partition, before.epoch).await;
    assert_eq!(
        durable_produce(&producer, b"after-old-broker-restart").await,
        2
    );
    durable_consume(&consumer, 2).await;
    let metadata_lost = Instant::now();
    for node in [12, 13, 32] {
        processes.get_mut(&node).unwrap().kill().await.unwrap();
    }
    assert!(
        tidemq_sdk::RequestContext::with_deadline(Instant::now() + Duration::from_secs(1))
            .scope(producer.produce(vec![public::Message {
                key: Bytes::new(),
                body: Bytes::from_static(b"metadata-minority")
            }]))
            .await
            .is_err()
    );
    assert!(
        observer
            .route(partition, Instant::now() + Duration::from_secs(2))
            .await
            .is_none(),
        "A metadata minority must not issue fresh ownership authority"
    );
    let pending = producer.pending_batches().await.unwrap();
    assert_eq!(pending.len(), 1);
    let original = pending.first().unwrap();
    // Expire the original control TTL before requiring refusal of all new work.
    tokio::time::sleep_until(metadata_lost + tidemq::ownership::ControlPolicy::default().ttl).await;
    assert!(
        tidemq_sdk::RequestContext::with_deadline(Instant::now() + Duration::from_secs(1))
            .scope(producer.retry(original))
            .await
            .is_err(),
        "A metadata minority cannot refresh an expired producer authority"
    );
    for node in [12, 13] {
        processes.insert(node, spawn_process(&fixture, node).await);
    }
    for node in [12, 13] {
        wait_process(&fixture, node, processes.get_mut(&node).unwrap()).await;
    }
    assert_eq!(durable_retry(&producer, original, deadline()).await, 3);
    durable_consume(&consumer, 3).await;
    let restored = observer.wait_owner(partition, 31).await;
    assert_eq!(restored.epoch, after.epoch + 1);
    assert_eq!(restored.generation, after.generation + 1);
    let status = client
        .admin()
        .subscription_status(topic, 0, subscription)
        .await
        .unwrap();
    assert_eq!(status.assignment.unwrap().acknowledged_before, 4);
    observer.close().await;
    client.close().await;
    for process in processes.values_mut() {
        if process.try_wait().unwrap().is_none() {
            process.kill().await.unwrap();
        }
        let _ = process.wait().await.unwrap();
    }
    fixture.close().await;
}
