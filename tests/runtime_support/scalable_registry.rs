use super::*;

use std::collections::VecDeque;
use std::io::{self, BufWriter, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const TRACE_CORRELATED_EVENTS: usize = 72;
const TRACE_SUPPORTING_EVENTS: usize = 160;
const TRACE_EVENT_BYTES: usize = 1024;

#[derive(Default)]
struct RegistryTraceEvents {
    correlated: VecDeque<Vec<u8>>,
    supporting: VecDeque<Vec<u8>>,
    internal_ids: BTreeSet<String>,
}

impl RegistryTraceEvents {
    fn is_correlated(&mut self, event: &[u8]) -> bool {
        let Ok(event) = std::str::from_utf8(event) else {
            return false;
        };
        let primary = event.contains("public_request_id=") || event.contains("registry_seed_");
        let internal_id = event
            .split_once(" request_id=Some(Id(")
            .or_else(|| event.split_once("{request_id=Some(Id("))
            .and_then(|(_, rest)| rest.split_once("))"))
            .map(|(id, _)| id);
        if let Some(id) = internal_id {
            if primary && self.internal_ids.len() < TRACE_CORRELATED_EVENTS {
                self.internal_ids.insert(id.to_owned());
            }
            return primary || self.internal_ids.contains(id);
        }
        primary
    }
}

#[test]
fn registry_trace_correlates_authorization_barrier_span_with_submission() {
    let mut events = RegistryTraceEvents::default();
    assert!(events.is_correlated(
        b"registry_query{public_request_id=Some(Id(public))}: event_code=connect request_id=Some(Id(internal))"
    ));
    assert!(events.is_correlated(
        b"internal_authorization{request_id=Some(Id(internal))}: event_code=barrier_finished"
    ));
    assert!(!events.is_correlated(
        b"internal_authorization{request_id=Some(Id(background))}: event_code=barrier_finished"
    ));
}

#[derive(Default)]
struct RegistryTraceBuffer {
    active: AtomicBool,
    events: Mutex<RegistryTraceEvents>,
    dropped: AtomicUsize,
}

struct RegistryTraceWriter(Arc<RegistryTraceBuffer>);

impl Write for RegistryTraceWriter {
    fn write(&mut self, event: &[u8]) -> io::Result<usize> {
        if !self.0.active.load(Ordering::Relaxed) {
            return Ok(event.len());
        }
        if event.len() <= TRACE_EVENT_BYTES
            && let Ok(mut buffer) = self.0.events.try_lock()
        {
            // Keep the registry request context even if background authorization fills its ring.
            let correlated = buffer.is_correlated(event);
            let (events, limit) = if correlated {
                (&mut buffer.correlated, TRACE_CORRELATED_EVENTS)
            } else {
                (&mut buffer.supporting, TRACE_SUPPORTING_EVENTS)
            };
            if events.len() == limit {
                events.pop_front();
                self.0.dropped.fetch_add(1, Ordering::Relaxed);
            }
            events.push_back(event.to_vec());
        } else {
            self.0.dropped.fetch_add(1, Ordering::Relaxed);
        }
        Ok(event.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct RegistryTrace {
    path: PathBuf,
    buffer: Arc<RegistryTraceBuffer>,
}

impl RegistryTrace {
    fn start(directory: &std::path::Path) -> Option<Self> {
        std::env::var_os("TIDEMQ_TEST_REGISTRY_RPC_LOG")?;
        let buffer = Arc::new(RegistryTraceBuffer::default());
        buffer.active.store(true, Ordering::Relaxed);
        let writer_buffer = buffer.clone();
        tracing_subscriber::fmt()
            .with_env_filter("tidemq::internal_rpc=debug")
            .with_timer(tracing_subscriber::fmt::time::Uptime::default())
            .with_ansi(false)
            .with_writer(move || RegistryTraceWriter(writer_buffer.clone()))
            .try_init()
            .expect("Install the registry RPC evidence subscriber in the isolated test");
        Some(Self {
            path: directory.join(format!("process-{}.log", std::process::id())),
            buffer,
        })
    }

    fn save(&self) -> io::Result<()> {
        self.buffer.active.store(false, Ordering::Relaxed);
        let events = {
            let mut events = self
                .buffer
                .events
                .lock()
                .map_err(|_| io::Error::other("Registry trace buffer was poisoned"))?;
            std::mem::take(&mut *events)
        };
        let mut file = BufWriter::new(std::fs::File::create(&self.path)?);
        writeln!(
            file,
            "Registry RPC trace uses monotonic time. Events discarded: {}",
            self.buffer.dropped.load(Ordering::Relaxed)
        )?;
        writeln!(file, "Registry request context and restart markers:")?;
        for event in events.correlated {
            file.write_all(&event)?;
        }
        writeln!(file, "Supporting internal RPC events:")?;
        for event in events.supporting {
            file.write_all(&event)?;
        }
        file.flush()
    }
}

impl Drop for RegistryTrace {
    fn drop(&mut self) {
        if let Err(error) = self.save() {
            eprintln!("Could not save registry RPC evidence: {error}");
        }
    }
}

#[cfg(feature = "test-hooks")]
async fn proxied_client(fixture: &Fixture) -> (udp_proxy::Proxy, Client) {
    let address = fixture.configs.get(&1).unwrap().listen;
    let proxy = udp_proxy::Proxy::start([address, address]).await;
    let client = Client::builder(
        proxy.address.to_string(),
        tidemq_sdk::TlsTrust::ca_pem(fixture.roots.clone()),
        tidemq_sdk::Credentials::token(TOKEN),
    )
    .request_timeout(Duration::from_secs(10))
    .build()
    .await
    .unwrap();
    (proxy, client)
}

#[cfg(feature = "test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_topic_creation_recovers_its_original_identity_after_placement_load_changes() {
    let fixture = dynamic_fixture().await;
    let direct = fixture.client(1).await;
    let tenant = direct.admin().create_tenant("acme").await.unwrap();
    direct
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let (proxy, client) = proxied_client(&fixture).await;
    let hook =
        tidemq::runtime::test_hooks::InstalledPause::topic_creation("acme/app/original-placement")
            .unwrap();
    let request_client = client.clone();
    let request = tokio::spawn(async move {
        tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(1))
            .unwrap()
            .scope(
                request_client
                    .admin()
                    .create_topic("acme/app/original-placement"),
            )
            .await
    });
    hook.wait(deadline()).await.unwrap();
    proxy.set_response_mtu(0);
    hook.resume();
    let result = request.await.unwrap();
    drop(hook);
    assert!(result.as_ref().is_err_and(|error| error.outcome_unknown()));
    let original = direct
        .admin()
        .topic_info("acme/app/original-placement")
        .await
        .unwrap();
    assert!(proxy.snapshot().over_mtu > 0);
    proxy.set_response_mtu(1200);
    let pending = client.admin().pending_operations().unwrap();
    assert_eq!(pending.len(), 1);
    let tidemq_sdk::AdminReceipt::Topic(recovered) = pending[0].retry().await.unwrap() else {
        panic!("Expected the original topic receipt");
    };
    assert_eq!(recovered.id(), original.id());
    assert!(client.admin().pending_operations().unwrap().is_empty());
    client.close().await.unwrap();
    direct.close().await.unwrap();
    proxy.close().await;
    fixture.close().await;
}

#[cfg(feature = "test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_split_reply_resumes_its_original_targets_after_new_capacity_becomes_preferable() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    configure_dynamic_fixture(&mut fixture).await;
    fixture
        .configs
        .get_mut(&1)
        .unwrap()
        .runtime
        .groups
        .push(GroupConfig {
            group_id: 4,
            voters: Vec::new(),
            standby: Vec::new(),
        });
    fixture.start_all().await;
    let direct = fixture.client(1).await;
    let tenant = direct.admin().create_tenant("acme").await.unwrap();
    direct
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = direct
        .admin()
        .create_topic("acme/app/split-placement")
        .await
        .unwrap();
    let partition = tidemq::metadata::PartitionKey {
        topic_id: topic.id().to_string().parse().unwrap(),
        partition: 0,
    };
    let hook = tidemq::runtime::test_hooks::InstalledPause::layout(partition).unwrap();
    let (proxy, client) = proxied_client(&fixture).await;
    let request_client = client.clone();
    let request = tokio::spawn(async move {
        tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(1))
            .unwrap()
            .scope(
                request_client
                    .admin()
                    .split_topic_at("acme/app/split-placement", 1_u64 << 63),
            )
            .await
    });
    hook.wait(deadline()).await.unwrap();
    proxy.set_response_mtu(0);
    hook.resume();
    let result = request.await.unwrap();
    assert!(result.as_ref().is_err_and(|error| error.outcome_unknown()));
    drop(hook);
    assert!(proxy.snapshot().over_mtu > 0);
    assert_eq!(
        direct
            .admin()
            .topic_info("acme/app/split-placement")
            .await
            .unwrap()
            .shard_count(),
        2
    );
    proxy.set_response_mtu(1200);
    let pending = client.admin().pending_operations().unwrap();
    assert_eq!(pending.len(), 1);
    let tidemq_sdk::AdminReceipt::Topic(recovered) = pending[0].retry().await.unwrap() else {
        panic!("Expected the original split receipt");
    };
    assert_eq!(recovered.id(), topic.id());
    assert_eq!(recovered.shard_count(), 2);
    assert!(client.admin().pending_operations().unwrap().is_empty());
    client.close().await.unwrap();
    direct.close().await.unwrap();
    proxy.close().await;
    fixture.close().await;
}

pub(super) async fn registration(fixture: &Fixture, node: u64) -> tidemq_sdk::NodeRegistration {
    let peer = fixture
        .configs
        .get(&node)
        .unwrap()
        .runtime
        .peer(node)
        .unwrap();
    let certificate = tokio::fs::read(&peer.certificate_der).await.unwrap();
    tidemq_sdk::NodeRegistration {
        node_id: node,
        role: match peer.role {
            Role::Combined => tidemq_sdk::ClusterRole::Combined,
            Role::Gateway => tidemq_sdk::ClusterRole::Gateway,
            Role::Broker => tidemq_sdk::ClusterRole::Broker,
            Role::Storage => tidemq_sdk::ClusterRole::Storage,
            Role::Metadata => tidemq_sdk::ClusterRole::Metadata,
        },
        zone: peer.zone.clone(),
        internal_endpoint: peer.internal_address,
        certificate_name: peer.server_name.clone(),
        certificate_pin: tidemq_sdk::NodeRegistration::certificate_pin(&certificate),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registered_nodes_and_an_independent_group_join_without_restarting_existing_services() {
    let mut fixture = Fixture::new(
        &[
            (1, Role::Metadata),
            (2, Role::Storage),
            (3, Role::Gateway),
            (21, Role::Storage),
            (31, Role::Broker),
            (41, Role::Gateway),
        ],
        &[1],
        &[21],
    )
    .await;
    for node in [1, 21, 31, 41] {
        fixture
            .configs
            .get_mut(&node)
            .unwrap()
            .runtime
            .peers
            .retain(|peer| [1, 21, 31, 41].contains(&peer.node_id));
    }
    let seed = fixture.configs.get_mut(&1).unwrap();
    seed.runtime
        .internal_listen
        .set_ip(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    let mut invalid = seed.clone();
    invalid.runtime.peers[0]
        .internal_address
        .set_ip(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    assert!(invalid.runtime.validate(&invalid).is_err());
    fixture
        .configs
        .get_mut(&2)
        .unwrap()
        .runtime
        .groups
        .push(GroupConfig {
            group_id: 3,
            voters: vec![2],
            standby: Vec::new(),
        });
    for node in [1, 21, 31, 41] {
        fixture.running.insert(
            node,
            RunningNode::start(fixture.configs.get(&node).unwrap().clone())
                .await
                .unwrap(),
        );
    }
    fixture.wait_ready().await;
    let client = fixture.client(41).await;
    assert_eq!(client.admin().cluster_nodes().await.unwrap().len(), 4);
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/discovery";
    client.admin().create_topic(topic).await.unwrap();
    let consumer = client
        .new_queue_consumer(topic, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    let first = producer
        .send("before-expansion")
        .await
        .unwrap()
        .message_id();
    let received = consumer.receive().await.unwrap();
    assert_eq!(received.message_id(), first);
    consumer.ack(&received).await.unwrap();
    fixture.running.insert(
        2,
        RunningNode::start(fixture.configs.get(&2).unwrap().clone())
            .await
            .unwrap(),
    );
    assert!(
        http(
            fixture.running.get(&2).unwrap().management_address,
            "/readyz"
        )
        .await
        .starts_with("HTTP/1.1 503")
    );
    let node = registration(&fixture, 2).await;
    assert_eq!(
        client
            .admin()
            .register_node(node.clone())
            .await
            .unwrap()
            .registration,
        node
    );
    fixture.wait_ready().await;
    let group = client.admin().create_data_group(3, [2]).await.unwrap();
    assert_eq!(group.voters, [2]);
    assert_eq!(client.admin().data_groups().await.unwrap().len(), 2);
    let until = Instant::now() + Duration::from_secs(30);
    let split_context = tidemq_sdk::RequestContext::with_deadline(until);
    loop {
        match split_context
            .scope(client.admin().split_topic_at(topic, 1_u64 << 63))
            .await
        {
            Ok(_) => break,
            Err(error)
                if matches!(
                    &error,
                    tidemq_sdk::Error::Query { source }
                        if matches!(
                            source.as_ref(),
                            tidemq_sdk::Error::Remote {
                                code: tidemq_protocol::common::ErrorCode::Unavailable,
                                outcome: tidemq_protocol::common::Outcome::Unknown,
                                retryable: true,
                                original_outcome_known: false,
                                ..
                            }
                        )
                ) =>
            {
                tokio::time::sleep_until(until.min(Instant::now() + Duration::from_millis(10)))
                    .await;
            }
            Err(error) => panic!("Topic split failed: {error:?}"),
        }
    }
    let mut expected = BTreeSet::new();
    for index in 0..32 {
        expected.insert(
            producer
                .send(
                    tidemq_sdk::Message::new(format!("after-{index}")).key(format!("key-{index}")),
                )
                .await
                .unwrap()
                .message_id()
                .to_string(),
        );
    }
    let mut seen = BTreeSet::new();
    for _ in 0..32 {
        let message = tokio::time::timeout(Duration::from_secs(15), consumer.receive())
            .await
            .unwrap()
            .unwrap();
        assert!(seen.insert(message.message_id().to_string()));
        consumer.ack(&message).await.unwrap();
    }
    assert_eq!(expected, seen);
    let gateway = registration(&fixture, 3).await;
    client.admin().register_node(gateway).await.unwrap();
    fixture.running.insert(
        3,
        RunningNode::start(fixture.configs.get(&3).unwrap().clone())
            .await
            .unwrap(),
    );
    fixture.wait_ready().await;
    let added_gateway = fixture.client(3).await;
    let second = added_gateway
        .new_queue_consumer(topic, "second")
        .build()
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(15), second.receive())
        .await
        .unwrap()
        .unwrap();
    second.ack(&message).await.unwrap();
    assert_eq!(client.admin().cluster_nodes().await.unwrap().len(), 6);
    let rpc_trace = RegistryTrace::start(&fixture.directory);
    tracing::debug!(
        target: "tidemq::internal_rpc",
        event_code = "registry_seed_restart_started",
        node_id = 1,
        "Restarting the metadata seed"
    );
    fixture.stop(1).await;
    // Observe the outage before restart so the fixture cannot reuse the gateway's old ready sample.
    let gateway_management = fixture.running.get(&41).unwrap().management_address;
    let readiness_deadline = Instant::now() + Duration::from_secs(5);
    while !http_until(gateway_management, "/readyz", readiness_deadline)
        .await
        .starts_with("HTTP/1.1 503")
    {
        assert!(
            Instant::now() < readiness_deadline,
            "The original gateway did not report unready while the metadata seed was stopped"
        );
        tokio::time::sleep_until(
            readiness_deadline.min(Instant::now() + Duration::from_millis(25)),
        )
        .await;
    }
    tracing::debug!(
        target: "tidemq::internal_rpc",
        event_code = "registry_seed_outage_observed",
        node_id = 41,
        "The original gateway reported unready while the metadata seed was stopped"
    );
    fixture.restart(1).await;
    tracing::debug!(
        target: "tidemq::internal_rpc",
        event_code = "registry_seed_ready",
        node_id = 1,
        "The metadata seed and the original gateway are ready for the registry query"
    );
    assert_eq!(client.admin().cluster_nodes().await.unwrap().len(), 6);
    assert_eq!(client.admin().data_groups().await.unwrap().len(), 2);
    drop(rpc_trace);
    let continued = producer
        .send("after-seed-restart")
        .await
        .unwrap()
        .message_id();
    let message = tokio::time::timeout(Duration::from_secs(15), consumer.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.message_id(), continued);
    consumer.ack(&message).await.unwrap();
    client.admin().set_node_schedulable(3, false).await.unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        let diagnostics = http(
            fixture.running.get(&3).unwrap().management_address,
            "/diagnostics",
        )
        .await;
        let body = diagnostics.split("\r\n\r\n").nth(1).unwrap();
        let diagnostics: serde_json::Value = serde_json::from_str(body).unwrap();
        if diagnostics["schedulable"] == false {
            assert_eq!(diagnostics["ready"], false);
            break;
        }
        assert!(
            Instant::now() < until,
            "Gateway did not observe the committed scheduling fence"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        Client::builder(
            fixture.configs.get(&3).unwrap().listen.to_string(),
            tidemq_sdk::TlsTrust::ca_pem(fixture.roots.clone()),
            tidemq_sdk::Credentials::token(TOKEN)
        )
        .connect_timeout(Duration::from_secs(1))
        .request_timeout(Duration::from_secs(1))
        .build()
        .await
        .is_err()
    );
    second.close().await.unwrap();
    added_gateway.close().await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.stop(1).await;
    let config = fixture.configs.get(&1).unwrap();
    let path = config.data_dir.join("node-directory.json");
    let original = tokio::fs::read(&path).await.unwrap();
    let mut invalid: serde_json::Value = serde_json::from_slice(&original).unwrap();
    invalid["version"] = serde_json::json!(999);
    tokio::fs::write(&path, serde_json::to_vec(&invalid).unwrap())
        .await
        .unwrap();
    assert!(
        RunningNode::start(config.clone()).await.is_err(),
        "An unknown discovery format was silently accepted"
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&tokio::fs::read(&path).await.unwrap())
            .unwrap()["version"],
        999
    );
    tokio::fs::write(&path, original).await.unwrap();
    fixture.close().await;
}
