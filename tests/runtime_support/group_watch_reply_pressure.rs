//! B03: exercise the real admission components and dispatcher without a reply pause hook.

use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use bytes::Bytes;
use tidemq_protocol::{
    Id, ProtocolLimits,
    common::{ErrorCode, ErrorReply, Frame, FrameKind, Outcome, TrafficClass},
    framing::{encode_payload, frame_limit, new_frame},
    internal::InternalCommand,
};
use tidemq_sdk::Client;
use tokio::{sync::watch, time::Instant};
use tokio_rustls::rustls::pki_types::{CertificateDer, pem::PemObject};

use super::{auth, data::consumer_tests::fixture::*, service::Service, wire};
use crate::{
    config::Role,
    metadata::PartitionKey,
    runtime::RunningNode,
    server::{DeferredReply, DispatchConfig, DispatchMetrics, SubmissionOrder, WireService},
    transport::{
        Authorization, Client as NativeClient, ClientCredentials, ClientTls, FrameReceiver,
        FrameSender, NodeGrant, PeerTarget, Server, ServerAuthorization, ServerTls, Session,
        StreamPurpose, TlsIdentity, TransportConfig,
    },
};

// Reuse fixture code only; no other runtime test suite is imported.
#[allow(dead_code)]
#[path = "group_watch.rs"]
mod watch_fixture;
use watch_fixture::{WatchFixture, bytes, decoded, id, metric};

const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";
const REQUESTS: usize = 512;
const HANDLERS: usize = 2;
const REPLY_QUEUE: usize = 1;
const WINDOW: u32 = 1_024;
const WIRE_TIMEOUT_MS: u64 = 60_000;

#[derive(Clone, Copy, Debug)]
enum Refusal {
    Budget,
    Queue,
    Authority,
}

#[derive(Default)]
struct Held {
    _bytes: Vec<crate::server::group_watch::Charge>,
    _turns: Vec<crate::server::group_watch::Admission>,
    _authority: Vec<crate::server::group_watch::AuthorityCharge>,
}

async fn hold(service: &Service, member: Id, refusal: Refusal) -> Held {
    let mut held = Held::default();
    match refusal {
        Refusal::Budget => {
            while let Some(charge) = service.watch_budget.reserve(None, member, 0, 16_384, false) {
                held._bytes.push(charge);
            }
            assert!(
                !held._bytes.is_empty(),
                "The Watch byte budget was not filled"
            );
        }
        Refusal::Queue => {
            for _ in 0..8 {
                held._turns.push(
                    service
                        .watch_budget
                        .admit(None, member, deadline())
                        .await
                        .unwrap(),
                );
            }
            assert!(
                service
                    .watch_budget
                    .admit(None, member, deadline())
                    .await
                    .is_none()
            );
        }
        Refusal::Authority => {
            for _ in 0..8 {
                held._authority.push(
                    service
                        .watch_budget
                        .reserve_authority(None, member, true)
                        .unwrap(),
                );
            }
            assert!(
                service
                    .watch_budget
                    .reserve_authority(None, member, true)
                    .is_none()
            );
        }
    }
    held
}

#[derive(Default)]
struct Count {
    live: AtomicUsize,
    peak: AtomicUsize,
    created: AtomicUsize,
}

struct Owner(Arc<Count>);
impl Owner {
    fn new(count: &Arc<Count>) -> Self {
        let live = count.live.fetch_add(1, Ordering::SeqCst) + 1;
        count.peak.fetch_max(live, Ordering::SeqCst);
        count.created.fetch_add(1, Ordering::SeqCst);
        Self(count.clone())
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}

struct OwnedPayload {
    bytes: Bytes,
    _owner: Owner,
}
impl AsRef<[u8]> for OwnedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn observe(response: Option<Frame>, owner: Owner) -> Option<Frame> {
    response.map(|mut frame| {
        frame.payload = Bytes::from_owner(OwnedPayload {
            bytes: frame.payload,
            _owner: owner,
        });
        frame
    })
}

struct ObservedService {
    inner: Arc<Service>,
    deferred: Arc<Count>,
    regular: Arc<Count>,
}

impl WireService for ObservedService {
    type Error = super::Error;

    fn defer(
        self: &Arc<Self>,
        peer: Authorization,
        frame: &Frame,
        until: Instant,
    ) -> Option<DeferredReply<Self::Error>> {
        let deferred = self.inner.defer(peer, frame, until)?;
        let owner = Owner::new(&self.deferred);
        Some(Box::pin(async move {
            deferred.await.map(|response| observe(response, owner))
        }))
    }

    async fn handle(
        &self,
        peer: Authorization,
        frame: &Frame,
        until: Instant,
        order: Option<SubmissionOrder>,
    ) -> Result<Option<Frame>, Self::Error> {
        let owner = Owner::new(&self.regular);
        self.inner
            .handle(peer, frame, until, order)
            .await
            .map(|response| observe(response, owner))
    }

    fn scheduling_key(&self, peer: &Authorization, frame: &Frame) -> Option<[u8; 16]> {
        self.inner.scheduling_key(peer, frame)
    }

    fn execution_purpose(
        &self,
        peer: &Authorization,
        frame: &Frame,
        purpose: StreamPurpose,
    ) -> StreamPurpose {
        self.inner.execution_purpose(peer, frame, purpose)
    }
}

struct Endpoint {
    service: Arc<ObservedService>,
    native: NativeClient,
    session: Arc<Session>,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<crate::Result<()>>,
    registry: prometheus_client::registry::Registry,
    transport: Arc<crate::transport::TransportMetrics>,
    native_bytes: usize,
}

async fn identity(fixture: &Fixture, node: u64) -> (TlsIdentity, [u8; 32]) {
    let tls = &fixture.configs.get(&node).unwrap().tls;
    let certificate = tokio::fs::read(&tls.certificate).await.unwrap();
    let leaf = CertificateDer::from_pem_slice(&certificate).unwrap();
    let pin = NodeGrant::certificate_pin(leaf.as_ref());
    let identity = TlsIdentity::new(
        certificate.into(),
        tokio::fs::read(&tls.private_key).await.unwrap().into(),
    )
    .unwrap();
    (identity, pin)
}

async fn authorization(
    fixture: &WatchFixture,
    internal: bool,
    server_pin: [u8; 32],
) -> (ServerAuthorization, ClientTls, ClientCredentials) {
    let service = &fixture.runtime.running.get(&1).unwrap().service;
    let mut tls = ClientTls {
        trust_roots: fixture.runtime.roots.clone(),
        identity: None,
        expected_nodes: vec![],
    };
    if internal {
        let (client_identity, client_pin) = identity(&fixture.runtime, 3).await;
        let grant = |certificate_pin, node_id, role: &str| NodeGrant {
            certificate_pin,
            certificate_name: format!("node-{node_id}.local"),
            cluster_id: service.cluster,
            node_id,
            role: role.into(),
            allowed_groups: vec![service.config.runtime.metadata_group],
            allowed_classes: vec![TrafficClass::Control],
        };
        tls.identity = Some(client_identity);
        tls.expected_nodes.push(grant(server_pin, 1, "metadata"));
        (
            ServerAuthorization::Internal(vec![grant(client_pin, 3, "broker")]),
            tls,
            ClientCredentials::Internal {
                session_id: id(),
                cluster_id: service.cluster,
                node_id: 3,
                role: "broker".into(),
            },
        )
    } else {
        let installed = Arc::new(OnceLock::new());
        installed.set(Arc::downgrade(service)).unwrap();
        (
            ServerAuthorization::Dynamic(Arc::new(auth::Authenticator(installed))),
            tls,
            ClientCredentials::External {
                session_id: id(),
                access_token: TOKEN.into(),
            },
        )
    }
}

impl Endpoint {
    async fn start(fixture: &WatchFixture, internal: bool) -> (Self, FrameSender, FrameReceiver) {
        let inner = fixture.runtime.running.get(&1).unwrap().service.clone();
        let service = Arc::new(ObservedService {
            inner: inner.clone(),
            deferred: Arc::default(),
            regular: Arc::default(),
        });
        let config = TransportConfig {
            max_connections: 4,
            max_handshakes: 2,
            max_streams: 4,
            core_streams: 1,
            max_ack_streams: 1,
            connection_window_bytes: 8 * WINDOW,
            stream_window_bytes: WINDOW,
            send_buffer_bytes: WINDOW,
            ..Default::default()
        };
        let native_bytes = (config.connection_window_bytes
            + config.max_streams as u32 * config.send_buffer_bytes)
            as usize;
        let (server_identity, server_pin) = identity(&fixture.runtime, 1).await;
        let (authorization, client_tls, credentials) =
            authorization(fixture, internal, server_pin).await;
        let server = Server::bind(
            udp_address(),
            ServerTls {
                identity: server_identity,
                client_trust_roots: internal.then(|| fixture.runtime.roots.clone()),
            },
            authorization,
            config.clone(),
            inner.budget.clone(),
        )
        .unwrap();
        let address = server.local_addr();
        let transport = server.metrics.clone();
        let health = Arc::new(DispatchMetrics::default());
        let mut registry = prometheus_client::registry::Registry::default();
        health.register(&mut registry);
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(crate::server::run(
            server,
            service.clone(),
            DispatchConfig {
                core_handlers: 1,
                data_handlers: HANDLERS,
                replies_per_stream: REPLY_QUEUE,
                drain_timeout: Duration::from_millis(200),
                ..Default::default()
            },
            inner.budget.clone(),
            health,
            stopped,
        ));
        let native = NativeClient::bind(
            udp_address(),
            client_tls,
            config,
            crate::budget::MemoryBudget::new(&crate::config::MemoryConfig::default()).unwrap(),
        )
        .unwrap();
        let session = native
            .connect(
                PeerTarget {
                    address,
                    server_name: "node-1.local".into(),
                    local_role: if internal { "broker" } else { "sdk" }.into(),
                    remote_role: if internal { "metadata" } else { "gateway" }.into(),
                    cluster_id: internal.then_some(inner.cluster),
                    node_id: internal.then_some(1),
                },
                &credentials,
                TrafficClass::Control,
                deadline(),
            )
            .await
            .unwrap();
        let stream = session
            .connection()
            .open(StreamPurpose::Request, deadline())
            .await
            .unwrap();
        let (sender, receiver) = stream.split();
        (
            Self {
                service,
                native,
                session,
                stop,
                task,
                registry,
                transport,
                native_bytes,
            },
            sender,
            receiver,
        )
    }

    fn metric(&self, name: &str) -> u64 {
        let mut text = String::new();
        prometheus_client::encoding::text::encode(&mut text, &self.registry).unwrap();
        metric(&text, name)
    }

    fn assert_bounds(&self, normal_before: usize, refusal: Refusal) {
        let service = &self.service.inner;
        let tenant_bytes = (service.budget.data_capacity() / 16).min(8 * 1024 * 1024) / 4;
        let deferred_limit = if matches!(refusal, Refusal::Queue) {
            tenant_bytes / 16_384
        } else {
            0
        };
        assert!(
            self.service.deferred.peak.load(Ordering::SeqCst) <= deferred_limit,
            "Deferred tasks or reply owners escaped independent admission"
        );
        assert!(
            self.service.regular.peak.load(Ordering::SeqCst) <= HANDLERS + REPLY_QUEUE + 1,
            "Regular reply ownership escaped handler and stream queue capacity"
        );
        assert!(self.metric("tidemq_dispatch_active_handlers") <= HANDLERS as u64);
        assert!(
            service.watch_budget.diagnostic().0 <= tenant_bytes,
            "Watch retained more than its tenant byte limit"
        );
        let ordinary_bytes = (HANDLERS + REPLY_QUEUE + 2)
            * 2
            * frame_limit(TrafficClass::Control, &service.config.protocol) as usize;
        let used = normal_before.saturating_sub(service.budget.available_normal_bytes());
        assert!(
            used <= tenant_bytes + ordinary_bytes + 2 * self.native_bytes + 512 * 1024,
            "Pending replies exceeded the Watch, handler, queue and native memory envelope: {used}"
        );
        assert_eq!(
            self.transport.snapshot().deadlines_total,
            0,
            "Deadline recycling cannot establish the pressure bound"
        );
    }

    async fn close(self) {
        self.native.close(deadline()).await.unwrap();
        self.stop.send(true).unwrap();
        tokio::time::timeout_at(deadline(), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

#[derive(serde::Serialize)]
struct Membership {
    topic: Id,
    subscription: Id,
    member: Id,
    epoch: u64,
    plan: u64,
    client: Id,
    credential: Id,
    targets: Vec<(Id, u32)>,
}

fn request(fixture: &WatchFixture, internal: bool) -> Frame {
    let watch = fixture.member.watch(Bytes::new(), 0);
    let operation = Id::from_slice(&watch.root_operation_id).unwrap();
    if internal {
        let authority = wire::Request::typed(
            super::consumer_watch_authority::AUTHORIZE,
            &Membership {
                topic: Id::from_slice(&watch.topic_id).unwrap(),
                subscription: Id::from_slice(&watch.subscription_id).unwrap(),
                member: Id::from_slice(&watch.member_id).unwrap(),
                epoch: watch.member_epoch,
                plan: watch.assignment_version,
                client: id(),
                credential: id(),
                targets: watch
                    .targets
                    .iter()
                    .map(|target| {
                        (
                            Id::from_slice(&target.shard_id).unwrap(),
                            target.birth_partition,
                        )
                    })
                    .collect(),
            },
        )
        .unwrap();
        new_frame(
            FrameKind::InternalCommand,
            TrafficClass::Control,
            id(),
            operation,
            &InternalCommand {
                group_id: 1,
                operation_id: bytes(operation),
                encoded_command: encode_payload(&authority, 16 * 1024).unwrap(),
            },
            WIRE_TIMEOUT_MS,
            &ProtocolLimits::default(),
        )
        .unwrap()
    } else {
        new_frame(
            FrameKind::GroupWatch,
            TrafficClass::Control,
            id(),
            operation,
            &watch,
            WIRE_TIMEOUT_MS,
            &ProtocolLimits::default(),
        )
        .unwrap()
    }
}

fn quota(frame: &Frame) {
    let error: ErrorReply = decoded(frame, FrameKind::Error);
    assert_eq!(
        error.code,
        i32::from(ErrorCode::QuotaExceeded),
        "Unexpected refusal: {error:?}"
    );
    assert_eq!(
        error.outcome,
        i32::from(Outcome::Rejected),
        "Unknown is not a pressure rejection"
    );
    assert!(error.retryable);
}

async fn immediate<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Ready(result) => Poll::Ready(result),
        Poll::Pending => {
            panic!("A refusal waited for work or a deadline instead of failing immediately")
        }
    })
    .await
}

async fn read_reply(receiver: &mut FrameReceiver, expected: &mut BTreeMap<Bytes, Bytes>) {
    let response = receiver
        .receive(deadline())
        .await
        .expect("The pressured reply lane closed or failed")
        .expect("The pressured reply lane ended before its replies");
    let frame = response.frame();
    let operation = expected
        .remove(&frame.request_id)
        .expect("A reply was duplicated or had an unknown request ID");
    assert_eq!(frame.operation_id, operation);
    quota(frame);
}

async fn pressure(refusal: Refusal) {
    let fixture = if matches!(refusal, Refusal::Authority) {
        WatchFixture::topology(
            &[
                (1, Role::Metadata),
                (2, Role::Storage),
                (3, Role::Broker),
                (4, Role::Gateway),
            ],
            &[1],
            &[2],
            4,
        )
        .await
    } else {
        WatchFixture::new().await
    };
    let inner = fixture.runtime.running.get(&1).unwrap().service.clone();
    let member = Id::from_slice(&fixture.member.request.member_id).unwrap();
    let held = hold(&inner, member, refusal).await;
    let (endpoint, mut sender, mut receiver) =
        Endpoint::start(&fixture, matches!(refusal, Refusal::Authority)).await;
    let requests: Vec<_> = (0..REQUESTS)
        .map(|_| request(&fixture, matches!(refusal, Refusal::Authority)))
        .collect();
    let mut expected = requests
        .iter()
        .map(|frame| (frame.request_id.clone(), frame.operation_id.clone()))
        .collect::<BTreeMap<_, _>>();
    let normal_before = inner.budget.available_normal_bytes();
    let wire_deadline = Instant::now() + Duration::from_millis(WIRE_TIMEOUT_MS);
    let sent = Arc::new(AtomicUsize::new(0));
    let progress = sent.clone();
    let writer = tokio::spawn(async move {
        for frame in requests {
            sender
                .send(&frame, wire_deadline)
                .await
                .expect("Continuous pressure input failed");
            progress.fetch_add(1, Ordering::SeqCst);
        }
        sender.finish().unwrap();
    });
    tokio::time::timeout_at(deadline(), async {
        loop {
            endpoint.assert_bounds(normal_before, refusal);
            let owners = if matches!(refusal, Refusal::Queue) {
                &endpoint.service.deferred
            } else {
                &endpoint.service.regular
            };
            if endpoint
                .transport
                .snapshot()
                .stream_data_blocked_frames_total
                > 0
                && endpoint.metric("tidemq_dispatch_rejected_total") > 0
                && owners.live.load(Ordering::SeqCst) > 0
            {
                break;
            }
            assert!(
                !writer.is_finished(),
                "Input completed without establishing slow-reply pressure"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("The withheld QUIC reader did not establish reply pressure");
    assert!(
        sent.load(Ordering::SeqCst) >= 8,
        "Too little input reached the pressured connection"
    );
    for round in 0..3 {
        let before = endpoint.transport.snapshot().frames_received_total;
        for _ in 0..16 {
            read_reply(&mut receiver, &mut expected).await;
        }
        tokio::time::timeout_at(deadline(), async {
            while endpoint.transport.snapshot().frames_received_total <= before {
                endpoint.assert_bounds(normal_before, refusal);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Reading replies did not permit fresh pressure input");
        endpoint.assert_bounds(normal_before, refusal);
        assert!(
            Instant::now() + Duration::from_secs(30) < wire_deadline,
            "The bound was only observed near request expiry"
        );
        eprintln!(
            "B03 pressure checkpoint: case={refusal:?} round={round} sent={} deferred_live={} deferred_peak={} regular_live={} regular_peak={} watch_bytes={} active_handlers={} rejected={} normal_bytes={} stream_blocked={} remaining_ms={}",
            sent.load(Ordering::SeqCst),
            endpoint.service.deferred.live.load(Ordering::SeqCst),
            endpoint.service.deferred.peak.load(Ordering::SeqCst),
            endpoint.service.regular.live.load(Ordering::SeqCst),
            endpoint.service.regular.peak.load(Ordering::SeqCst),
            inner.watch_budget.diagnostic().0,
            endpoint.metric("tidemq_dispatch_active_handlers"),
            endpoint.metric("tidemq_dispatch_rejected_total"),
            normal_before.saturating_sub(inner.budget.available_normal_bytes()),
            endpoint
                .transport
                .snapshot()
                .stream_data_blocked_frames_total,
            wire_deadline
                .saturating_duration_since(Instant::now())
                .as_millis()
        );
    }
    while !expected.is_empty() {
        read_reply(&mut receiver, &mut expected).await;
    }
    tokio::time::timeout_at(deadline(), writer)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout_at(deadline(), async {
        while endpoint.service.regular.live.load(Ordering::SeqCst)
            + endpoint.service.deferred.live.load(Ordering::SeqCst)
            != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Replies retained their owners after complete delivery");
    endpoint.assert_bounds(normal_before, refusal);
    assert_eq!(sent.load(Ordering::SeqCst), REQUESTS);
    assert_eq!(
        endpoint.metric("tidemq_dispatch_admitted_total")
            + endpoint.metric("tidemq_dispatch_rejected_total"),
        REQUESTS as u64
    );
    assert!(!endpoint.session.connection().is_closed());
    drop(receiver);
    endpoint.close().await;
    drop(held);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn budget_refusal_bounds_slow_replies_before_deadline() {
    pressure(Refusal::Budget).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_refusal_bounds_slow_replies_before_deadline() {
    pressure(Refusal::Queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authority_slot_refusal_bounds_slow_replies_before_deadline() {
    pressure(Refusal::Authority).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_refusal_charge_survives_future_frame_and_payload_clones() {
    let fixture = WatchFixture::new().await;
    let service = &fixture.runtime.running.get(&1).unwrap().service;
    let member = Id::from_slice(&fixture.member.request.member_id).unwrap();
    let held = hold(service, member, Refusal::Queue).await;
    let frame = request(&fixture, false);
    let peer = Authorization::Credential {
        credential: id(),
        tenant: None,
        session_id: id(),
        allowed_kinds: vec![FrameKind::GroupWatch],
        lease: None,
    };
    let before = service.watch_budget.diagnostic();
    let compact = 16_384 + 2 * frame.payload.len();
    let deferred = service
        .defer(peer.clone(), &frame, deadline())
        .expect("The compact request must fit before queue refusal");
    assert_eq!(service.watch_budget.diagnostic().0, before.0 + compact);
    let response = immediate(deferred).await.unwrap().unwrap();
    quota(&response);
    let payload = response.payload.clone();
    let duplicate = response.clone();
    drop(response);
    assert_eq!(service.watch_budget.diagnostic().0, before.0 + compact);
    drop(duplicate);
    assert_eq!(service.watch_budget.diagnostic().0, before.0 + compact);
    drop(payload);
    assert_eq!(service.watch_budget.diagnostic(), before);
    let mut replies = Vec::new();
    while let Some(deferred) = service.defer(peer.clone(), &frame, deadline()) {
        let response = immediate(deferred).await.unwrap().unwrap();
        quota(&response);
        replies.push(response);
        assert_eq!(
            service.watch_budget.diagnostic().0,
            before.0 + replies.len() * compact
        );
    }
    assert!(!replies.is_empty());
    assert!(replies.len() < 128);
    for _ in 0..REQUESTS {
        assert!(
            service.defer(peer.clone(), &frame, deadline()).is_none(),
            "Budget failure must retain ordinary dispatcher ownership"
        );
        let response = immediate(service.handle(peer.clone(), &frame, deadline(), None))
            .await
            .unwrap()
            .unwrap();
        quota(&response);
        assert_eq!(
            service.watch_budget.diagnostic().0,
            before.0 + replies.len() * compact
        );
    }
    eprintln!(
        "B03 retained reply ledger: replies={} compact_bytes={compact} retained_bytes={} early_regular_refusals={REQUESTS}",
        replies.len(),
        service.watch_budget.diagnostic().0 - before.0
    );
    drop(replies);
    assert_eq!(service.watch_budget.diagnostic(), before);
    drop(held);
    fixture.close().await;
}
