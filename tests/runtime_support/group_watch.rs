use super::*;
use prost::Message;
use tidemq::transport::{
    Client as NativeClient, ClientCredentials, ClientTls, PeerTarget, TransportConfig,
    rpc::{RpcClient, RpcConfig, RpcResponse},
};
use tidemq_protocol::{
    ProtocolLimits,
    common::{ErrorCode, ErrorReply, Frame, FrameKind, TrafficClass},
    framing::{decode_payload, new_frame},
    public,
};

pub(super) const TOPIC: &str = "test/app/watch";
pub(super) const RPC_MS: u64 = 2_000;

pub(super) fn id() -> Id {
    Id::new_v7().unwrap()
}

pub(super) fn bytes(id: Id) -> Bytes {
    Bytes::copy_from_slice(id.as_bytes())
}

pub(super) fn frame<M: Message>(kind: FrameKind, operation: Id, body: &M) -> Frame {
    new_frame(
        kind,
        if kind == FrameKind::GroupRead {
            TrafficClass::Delivery
        } else {
            TrafficClass::Control
        },
        id(),
        operation,
        body,
        RPC_MS,
        &ProtocolLimits::default(),
    )
    .unwrap()
}

pub(super) fn decoded<M: Message + Default>(reply: &Frame, expected: FrameKind) -> M {
    if reply.kind == i32::from(FrameKind::Error) && expected != FrameKind::Error {
        let error: ErrorReply =
            decode_payload(reply.payload.clone(), &ProtocolLimits::default()).unwrap();
        panic!("Expected {expected:?}, received explicit protocol error {error:?}");
    }
    assert_eq!(reply.kind, i32::from(expected));
    decode_payload(reply.payload.clone(), &ProtocolLimits::default()).unwrap()
}

pub(super) struct WireClient {
    native: NativeClient,
    pub control: RpcClient,
    delivery: std::sync::Mutex<RpcClient>,
    target: PeerTarget,
    credentials: ClientCredentials,
    timeout: Duration,
}

impl WireClient {
    pub async fn connect(fixture: &Fixture, gateway: u64) -> Self {
        let native = NativeClient::bind(
            "127.0.0.1:0".parse().unwrap(),
            ClientTls {
                trust_roots: fixture.roots.clone(),
                identity: None,
                expected_nodes: vec![],
            },
            TransportConfig::default(),
            tidemq::budget::MemoryBudget::new(&tidemq::config::MemoryConfig::default()).unwrap(),
        )
        .unwrap();
        let target = PeerTarget {
            address: fixture.configs.get(&gateway).unwrap().listen,
            server_name: "queue.local".into(),
            local_role: "sdk".into(),
            remote_role: "gateway".into(),
            cluster_id: None,
            node_id: None,
        };
        let credentials = ClientCredentials::External {
            session_id: id(),
            access_token: TOKEN.into(),
        };
        let mut rpcs = Vec::new();
        for class in [TrafficClass::Control, TrafficClass::Delivery] {
            let session = native
                .connect(target.clone(), &credentials, class, deadline())
                .await
                .unwrap();
            rpcs.push(
                // Reach the server budget before exhausting the client's ordinary half.
                RpcClient::from_session(
                    session,
                    RpcConfig {
                        max_pending_requests: 512,
                        ..Default::default()
                    },
                )
                .await
                .unwrap(),
            );
        }
        let delivery = rpcs.pop().unwrap();
        let control = rpcs.pop().unwrap();
        Self {
            native,
            control,
            delivery: std::sync::Mutex::new(delivery),
            target,
            credentials,
            timeout: Duration::from_secs(10),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub async fn call(&self, request: Frame) -> Frame {
        let rpc = if request.traffic_class == i32::from(TrafficClass::Delivery) {
            self.delivery.lock().unwrap().clone()
        } else {
            self.control.clone()
        };
        let request_id = request.request_id.clone();
        let operation = request.operation_id.clone();
        let kind = request.kind;
        let response = rpc
            .call(request, Instant::now() + self.timeout)
            .await
            .unwrap();
        let reply = response.frame();
        assert_eq!(
            reply.request_id, request_id,
            "Reply changed the request identity"
        );
        assert_eq!(
            reply.operation_id, operation,
            "Reply changed the operation identity"
        );
        eprintln!(
            "Watch protocol ledger: request_kind={kind} reply_kind={} operation={}",
            reply.kind,
            Id::from_slice(&operation).unwrap()
        );
        reply.clone()
    }

    pub(super) async fn reopen_delivery(&self) {
        let old = self.delivery.lock().unwrap().clone();
        old.shutdown(deadline()).await;
        let session = self
            .native
            .connect(
                self.target.clone(),
                &self.credentials,
                TrafficClass::Delivery,
                deadline(),
            )
            .await
            .unwrap();
        let replacement = RpcClient::from_session(
            session,
            RpcConfig {
                max_pending_requests: 512,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        *self.delivery.lock().unwrap() = replacement;
        eprintln!("Watch setup reopened Delivery with the original authenticated client identity");
    }

    pub async fn submit(&self, request: &public::GroupWatch) -> RpcResponse {
        self.control
            .submit(
                frame(
                    FrameKind::GroupWatch,
                    Id::from_slice(&request.root_operation_id).unwrap(),
                    request,
                ),
                deadline(),
            )
            .await
            .unwrap()
    }

    pub(super) async fn submit_for(
        &self,
        request: &public::GroupWatch,
        rpc_timeout: Duration,
    ) -> RpcResponse {
        let mut request_frame = frame(
            FrameKind::GroupWatch,
            Id::from_slice(&request.root_operation_id).unwrap(),
            request,
        );
        request_frame.remaining_timeout_ms = rpc_timeout.as_millis().try_into().unwrap();
        self.control
            .submit(request_frame, Instant::now() + rpc_timeout)
            .await
            .unwrap()
    }

    pub(super) async fn submit_until(
        &self,
        request: &public::GroupWatch,
        until: Instant,
    ) -> (RpcResponse, Bytes) {
        let remaining = until.saturating_duration_since(Instant::now());
        assert!(
            remaining.as_millis() > 0,
            "The original observation deadline expired before submission"
        );
        let mut request_frame = frame(
            FrameKind::GroupWatch,
            Id::from_slice(&request.root_operation_id).unwrap(),
            request,
        );
        request_frame.remaining_timeout_ms = remaining.as_millis().try_into().unwrap();
        let request_id = request_frame.request_id.clone();
        (
            self.control.submit(request_frame, until).await.unwrap(),
            request_id,
        )
    }

    pub async fn watch(&self, request: &public::GroupWatch) -> public::GroupWatchResult {
        let reply = self
            .call(frame(
                FrameKind::GroupWatch,
                Id::from_slice(&request.root_operation_id).unwrap(),
                request,
            ))
            .await;
        decoded(&reply, FrameKind::GroupWatchResult)
    }

    pub(super) async fn original<M: Message>(&self, kind: FrameKind, request: &M) -> Frame {
        let operation = id();
        let until = Instant::now() + self.timeout;
        loop {
            let mut request_frame = frame(kind, operation, request);
            request_frame.remaining_timeout_ms =
                u64::try_from(until.saturating_duration_since(Instant::now()).as_millis())
                    .unwrap()
                    .max(1);
            let reply = self.call(request_frame).await;
            if reply.kind == i32::from(FrameKind::Error) {
                let error: ErrorReply = decoded(&reply, FrameKind::Error);
                if error.retryable && Instant::now() < until {
                    eprintln!("Retrying original {kind:?} operation {operation}: {error:?}");
                    tokio::task::yield_now().await;
                    continue;
                }
            }
            return reply;
        }
    }

    pub(super) async fn member_control(
        &self,
        request: &public::ConsumerControl,
    ) -> public::ConsumerControlResult {
        decoded(
            &self.original(FrameKind::ConsumerControl, request).await,
            FrameKind::ConsumerControlResult,
        )
    }

    pub async fn cancel(&self, request: &public::GroupWatch) {
        let reply = self
            .call(frame(
                FrameKind::GroupWatchCancel,
                id(),
                &public::GroupWatchCancel {
                    root_operation_id: request.root_operation_id.clone(),
                    operation_id: request.root_operation_id.clone(),
                    attempt: request.attempt,
                },
            ))
            .await;
        let _: public::GroupWatchCancelled = decoded(&reply, FrameKind::GroupWatchCancelled);
    }

    pub async fn close(self) {
        self.control.shutdown(deadline()).await;
        self.delivery
            .into_inner()
            .unwrap()
            .shutdown(deadline())
            .await;
        self.native.close(deadline()).await.unwrap();
    }
}

#[derive(Clone)]
pub(super) struct Member {
    pub request: public::ConsumerControl,
    pub status: public::ConsumerControlResult,
    pub partition: PartitionKey,
    pub shard: Bytes,
}

impl Member {
    pub async fn join(wire: &WireClient, topic: Id, name: &str) -> Self {
        Self::join_model(wire, topic, name, public::ConsumerModel::QueueConsumer).await
    }

    pub async fn join_model(
        wire: &WireClient,
        topic: Id,
        name: &str,
        model: public::ConsumerModel,
    ) -> Self {
        let mut request = public::ConsumerControl {
            topic_id: bytes(topic),
            name: name.into(),
            model: model.into(),
            action: public::ConsumerAction::EnsureConsumerSubscription.into(),
            max_messages: 256,
            max_bytes: 8 * 1024 * 1024,
            ..Default::default()
        };
        let mut ensured = wire.member_control(&request).await;
        request.subscription_id = ensured.subscription_id.clone();
        let until = Instant::now() + wire.timeout;
        while !ensured.plan_active {
            assert!(
                Instant::now() < until,
                "Initial subscription assignment did not activate: {ensured:?}"
            );
            let query = public::ConsumerControl {
                action: public::ConsumerAction::ConsumerStatus.into(),
                ..request.clone()
            };
            ensured = wire.member_control(&query).await;
        }
        request.member_id = bytes(id());
        request.member_epoch = ensured.next_member_epoch;
        request.action = public::ConsumerAction::JoinConsumer.into();
        let mut status = wire.member_control(&request).await;
        let until = Instant::now() + wire.timeout;
        let mut rounds = 0;
        while !status.plan_active || status.work_shards.is_empty() {
            assert!(
                Instant::now() < until,
                "Consumer assignment did not activate: {status:?}"
            );
            if rounds % 16 == 0 {
                eprintln!(
                    "Watch member activation ledger: round={rounds} subscription={} status={} active={} version={} fenced={} lease_ms={} work={}",
                    Id::from_slice(&request.subscription_id).unwrap(),
                    status.member_status,
                    status.plan_active,
                    status.assignment_version,
                    status.assignment_fenced,
                    status.remaining_lease_ms,
                    status.work_shards.len(),
                );
            }
            let query = public::ConsumerControl {
                action: if status.remaining_lease_ms < 10_000 {
                    public::ConsumerAction::HeartbeatConsumer
                } else {
                    public::ConsumerAction::ConsumerStatus
                }
                .into(),
                heartbeat_sequence: status.next_heartbeat_sequence,
                ..request.clone()
            };
            status = wire.member_control(&query).await;
            rounds += 1;
        }
        assert_eq!(status.member_epoch, request.member_epoch);
        let shard = status.work_shards.first().unwrap().clone();
        Self {
            request,
            status,
            partition: PartitionKey {
                topic_id: topic,
                partition: 0,
            },
            shard,
        }
    }

    pub fn watch(&self, token: Bytes, wait_ms: u32) -> public::GroupWatch {
        public::GroupWatch {
            topic_id: self.request.topic_id.clone(),
            subscription_id: self.request.subscription_id.clone(),
            member_id: self.request.member_id.clone(),
            member_epoch: self.request.member_epoch,
            assignment_version: self.status.assignment_version,
            attempt: 1,
            wait_ms,
            targets: vec![public::WatchTarget {
                shard_id: self.shard.clone(),
                birth_partition: 0,
                observed_token: token,
            }],
            root_operation_id: bytes(id()),
            parent_operation_id: Bytes::new(),
        }
    }

    pub async fn read(&self, wire: &WireClient, sequence: u64) -> public::GroupDeliveries {
        self.read_batch(wire, sequence, 1).await
    }

    pub async fn read_batch(
        &self,
        wire: &WireClient,
        sequence: u64,
        count: u32,
    ) -> public::GroupDeliveries {
        self.read_partition(wire, 0, sequence, count).await
    }

    pub async fn read_partition(
        &self,
        wire: &WireClient,
        partition: u32,
        sequence: u64,
        count: u32,
    ) -> public::GroupDeliveries {
        let reply = wire
            .original(
                FrameKind::GroupRead,
                &public::GroupRead {
                    topic_id: self.request.topic_id.clone(),
                    partition_id: partition,
                    subscription_id: self.request.subscription_id.clone(),
                    member_id: self.request.member_id.clone(),
                    member_epoch: self.request.member_epoch,
                    sequence,
                    max_messages: count,
                    max_bytes: 512 * 1024,
                },
            )
            .await;
        let result: public::GroupDeliveries = decoded(&reply, FrameKind::GroupDeliveries);
        assert_eq!(result.sequence, sequence);
        result
    }

    pub async fn retire(&self, wire: &WireClient, sequence: u64) {
        self.retire_partition(wire, 0, sequence).await;
    }

    pub async fn retire_partition(&self, wire: &WireClient, partition: u32, sequence: u64) {
        let reply = wire
            .original(
                FrameKind::GroupRetireRead,
                &public::GroupRetire {
                    topic_id: self.request.topic_id.clone(),
                    partition_id: partition,
                    subscription_id: self.request.subscription_id.clone(),
                    member_id: self.request.member_id.clone(),
                    member_epoch: self.request.member_epoch,
                    sequence,
                },
            )
            .await;
        let _: public::GroupRetired = decoded(&reply, FrameKind::GroupRetired);
    }

    pub async fn ack(&self, wire: &WireClient, delivery: &public::GroupDelivery, sequence: u64) {
        self.ack_batch(wire, std::slice::from_ref(delivery), sequence)
            .await;
    }

    pub(super) async fn retire_ack(&self, wire: &WireClient, sequence: u64) {
        let reply = wire
            .original(
                FrameKind::GroupRetireAck,
                &public::GroupRetire {
                    topic_id: self.request.topic_id.clone(),
                    partition_id: 0,
                    subscription_id: self.request.subscription_id.clone(),
                    member_id: self.request.member_id.clone(),
                    member_epoch: self.request.member_epoch,
                    sequence,
                },
            )
            .await;
        let _: public::GroupRetired = decoded(&reply, FrameKind::GroupRetired);
    }

    pub async fn ack_batch(
        &self,
        wire: &WireClient,
        deliveries: &[public::GroupDelivery],
        sequence: u64,
    ) {
        let reply = wire
            .original(
                FrameKind::GroupAck,
                &public::GroupAck {
                    topic_id: self.request.topic_id.clone(),
                    partition_id: 0,
                    subscription_id: self.request.subscription_id.clone(),
                    member_id: self.request.member_id.clone(),
                    member_epoch: self.request.member_epoch,
                    sequence,
                    proofs: deliveries
                        .iter()
                        .map(|delivery| delivery.proof.clone().unwrap())
                        .collect(),
                    nack_delay_ms: None,
                },
            )
            .await;
        let result: public::GroupAckResult = decoded(&reply, FrameKind::GroupAckResult);
        assert!(!result.redelivery_scheduled);
    }

    pub async fn leave(&self, wire: &WireClient) {
        let request = public::ConsumerControl {
            action: public::ConsumerAction::LeaveConsumer.into(),
            ..self.request.clone()
        };
        wire.member_control(&request).await;
    }
}

pub(super) struct WatchFixture {
    pub runtime: Fixture,
    pub admin: Client,
    pub wire: WireClient,
    pub member: Member,
}

impl WatchFixture {
    pub async fn new() -> Self {
        Self::topology(&[(1, Role::Combined)], &[1], &[1], 1).await
    }

    pub async fn topology(
        nodes: &[(u64, Role)],
        metadata: &[u64],
        storage: &[u64],
        gateway: u64,
    ) -> Self {
        let runtime = Fixture::new(nodes, metadata, storage).await;
        Self::start(runtime, gateway).await
    }

    pub async fn start(mut runtime: Fixture, gateway: u64) -> Self {
        runtime.start_all().await;
        let admin = runtime.client(gateway).await;
        let tenant = admin.admin().create_tenant("test").await.unwrap();
        admin.admin().create_namespace(tenant, "app").await.unwrap();
        let topic = admin.admin().create_topic(TOPIC).await.unwrap();
        let wire = WireClient::connect(&runtime, gateway).await;
        let member = Member::join(&wire, topic.id().to_string().parse().unwrap(), "watch").await;
        Self {
            runtime,
            admin,
            wire,
            member,
        }
    }

    pub async fn sync(&self) -> Bytes {
        let request = self.member.watch(Bytes::new(), 0);
        let result = self.wire.watch(&request).await;
        target(&request, &result, public::WatchStatus::WatchSync)
            .observed_token
            .clone()
    }

    pub async fn metrics(&self, node: u64) -> String {
        tokio::time::timeout_at(
            deadline(),
            http(
                self.runtime.running.get(&node).unwrap().management_address,
                "/metrics",
            ),
        )
        .await
        .unwrap()
    }

    pub async fn close(self) {
        let observers: Vec<_> = self
            .runtime
            .running
            .values()
            .map(RunningNode::watch_diagnostics)
            .collect();
        self.member.leave(&self.wire).await;
        self.wire.close().await;
        self.admin.close().await.unwrap();
        self.runtime.close().await;
        for observer in observers {
            assert_released(
                &tokio::time::timeout_at(deadline(), observer.snapshot())
                    .await
                    .unwrap(),
            );
        }
    }
}

pub(super) fn assert_released(snapshot: &tidemq::runtime::watch_test_hooks::Snapshot) {
    eprintln!(
        "Watch shutdown resource ledger: {}",
        serde_json::to_string(snapshot).unwrap()
    );
    assert_eq!(snapshot.charged_bytes, 0);
    assert_eq!(snapshot.target_contexts, 0);
    assert_eq!(snapshot.forwards, 0);
    assert_eq!(snapshot.cleanup_tasks, 0);
    assert_eq!(snapshot.active_requests, 0);
    assert_eq!(snapshot.cancellation_watermarks, 0);
    assert_eq!(snapshot.timers, 0);
    assert_eq!(snapshot.authority_inbound, 0);
    assert_eq!(snapshot.authority_outbound, 0);
    assert_eq!(snapshot.admission_queued, 0);
    assert_eq!(snapshot.rechecks_queued, 0);
    for group in &snapshot.groups {
        assert_eq!(group.cached_targets, 0);
        assert_eq!(group.dependency_edges, 0);
        assert_eq!(group.queued_rechecks, 0);
    }
}

pub(super) fn target<'a>(
    request: &public::GroupWatch,
    result: &'a public::GroupWatchResult,
    status: public::WatchStatus,
) -> &'a public::WatchResult {
    assert_eq!(
        result.targets.len(),
        request.targets.len(),
        "Watch omitted an input target"
    );
    assert_eq!(result.targets.len(), 1);
    let target = result.targets.first().unwrap();
    assert_eq!(target.shard_id, request.targets.first().unwrap().shard_id);
    assert_eq!(
        target.status,
        i32::from(status),
        "Unexpected Watch status: {target:?}"
    );
    assert_eq!(target.error_code, i32::from(ErrorCode::Unspecified));
    assert!(!target.retryable);
    assert!(!target.observed_token.is_empty());
    assert!(target.observed_token.len() <= tidemq_protocol::watch::MAX_TOKEN_BYTES);
    target
}

pub(super) async fn receive(response: RpcResponse) -> public::GroupWatchResult {
    let received = tokio::time::timeout_at(deadline(), response.receive())
        .await
        .unwrap()
        .unwrap();
    decoded(received.frame(), FrameKind::GroupWatchResult)
}

pub(super) async fn receive_replaced(response: RpcResponse, request: &public::GroupWatch) {
    let received = tokio::time::timeout_at(deadline(), response.receive())
        .await
        .unwrap()
        .unwrap();
    let reply = received.frame();
    assert_eq!(reply.operation_id, request.root_operation_id);
    if reply.kind == i32::from(FrameKind::Error) {
        let error: ErrorReply = decoded(reply, FrameKind::Error);
        eprintln!(
            "Replaced Watch termination ledger: root={} attempt={} code={} outcome={} retryable={}",
            Id::from_slice(&request.root_operation_id).unwrap(),
            request.attempt,
            error.code,
            error.outcome,
            error.retryable
        );
        assert_eq!(error.code, i32::from(ErrorCode::Unavailable));
        assert_eq!(
            error.outcome,
            i32::from(tidemq_protocol::common::Outcome::Rejected)
        );
        assert!(error.retryable);
        return;
    }
    let result: public::GroupWatchResult = decoded(reply, FrameKind::GroupWatchResult);
    let expected: std::collections::BTreeSet<_> = request
        .targets
        .iter()
        .map(|target| &target.shard_id)
        .collect();
    assert_eq!(result.targets.len(), expected.len());
    assert_eq!(
        result
            .targets
            .iter()
            .map(|target| &target.shard_id)
            .collect::<std::collections::BTreeSet<_>>(),
        expected
    );
    for target in result.targets {
        assert_eq!(target.status, i32::from(public::WatchStatus::WatchRetry));
        assert_eq!(target.error_code, i32::from(ErrorCode::Unavailable));
        assert!(target.retryable);
    }
}

pub(super) fn assert_stale_cancel_unmatched(
    observer: &tidemq::runtime::watch_test_hooks::Diagnostics,
    request: &public::GroupWatch,
) {
    use tidemq::runtime::watch_test_hooks::WatchPhase;
    let root = Id::from_slice(&request.root_operation_id).unwrap();
    let events: Vec<_> = observer
        .events()
        .into_iter()
        .filter(|event| {
            event.root == root
                && event.operation == root
                && event.attempt == request.attempt
                && matches!(
                    event.phase,
                    WatchPhase::Cancelled | WatchPhase::CancelUnknown
                )
        })
        .collect();
    assert_eq!(
        events.len(),
        1,
        "The real cancel handler did not record exactly one root attempt decision"
    );
    assert_eq!(
        events.first().unwrap().phase,
        WatchPhase::CancelUnknown,
        "A stale cancel unexpectedly matched the replacement registration"
    );
    eprintln!(
        "Stale Watch cancellation ledger: root={root} attempt={} actual_cancel_matched=false",
        request.attempt
    );
}

pub(super) fn metric(text: &str, name: &str) -> u64 {
    let prefix = format!("{name} ");
    text.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("Required Watch metric {name} is missing"))
        .parse()
        .unwrap()
}

pub(super) fn activity(text: &str) -> [u64; 3] {
    [
        "tidemq_watch_group_reads_total",
        "tidemq_watch_group_retires_total",
        "tidemq_watch_empty_reads_total",
    ]
    .map(|name| metric(text, name))
}
