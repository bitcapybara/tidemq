//! A real UDP/TLS peer for SDK transport boundaries; durable delivery is tested against the runtime.
use std::{
    net::{Ipv6Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use prost::Message;
use rcgen::{CertificateParams, KeyPair};
use tidemq_protocol::{
    Id, ProtocolLimits,
    common::{Frame, FrameKind, TrafficClass},
    framing::{decode_payload, new_frame},
    public,
};
use tidemq_sdk::{Client, Credentials, TlsTrust};
use tidemq_transport::{
    FramedStream, Server, ServerAuthorization, ServerTls, Session, TlsIdentity, TokenGrant,
    TransportConfig,
    budget::{MemoryBudget, MemoryLimits},
};
use tokio::{
    task::{JoinHandle, JoinSet},
    time::Instant,
};

const TOKEN: &str = "sdk-test-token-with-at-least-32-characters";
const TOPIC: &str = "test/app/events";

fn id(byte: u8) -> Id {
    Id::from_slice(&[byte; 16]).unwrap()
}
fn bytes(id: Id) -> Bytes {
    Bytes::copy_from_slice(id.as_bytes())
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

#[derive(Default)]
struct PeerState {
    classes: Vec<TrafficClass>,
    sessions: Vec<Id>,
    traceparents: Vec<String>,
    drop_first_admin: bool,
    drop_mutation_replies: bool,
    mutations: std::collections::BTreeSet<Id>,
    topic_delay: Duration,
    topic_requests: usize,
    control_timeouts: Vec<(FrameKind, u64)>,
}

struct Peer {
    client: Client,
    state: Arc<Mutex<PeerState>>,
    task: JoinHandle<()>,
    ca: Bytes,
    endpoint: String,
    identity: TlsIdentity,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn peer() -> Peer {
    peer_at("localhost").await
}

async fn peer_at(hostname: &str) -> Peer {
    let key = KeyPair::generate().unwrap();
    let certificate = CertificateParams::new(vec![hostname.into()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let ca = Bytes::from(certificate.pem());
    let identity = TlsIdentity::new(ca.clone(), Bytes::from(key.serialize_pem())).unwrap();
    let server = server("127.0.0.1:0".parse().unwrap(), identity.clone());
    let endpoint = format!("{hostname}:{}", server.local_addr().port());
    let state = Arc::new(Mutex::new(PeerState::default()));
    let task = accept(server, state.clone());
    let client = Client::builder(
        &endpoint,
        TlsTrust::ca_pem(ca.clone()),
        Credentials::token(TOKEN),
    )
    .build()
    .await
    .unwrap();
    Peer {
        client,
        state,
        task,
        ca,
        endpoint,
        identity,
    }
}

fn server(address: SocketAddr, identity: TlsIdentity) -> Server {
    Server::bind(
        address,
        ServerTls {
            identity,
            client_trust_roots: None,
        },
        ServerAuthorization::External(vec![
            TokenGrant::new(
                TOKEN,
                "sdk-test".into(),
                "sdk-test".into(),
                vec![
                    FrameKind::GetTopic,
                    FrameKind::GetTopicLayout,
                    FrameKind::CreateLogicalTopic,
                    FrameKind::ManageIdentity,
                    FrameKind::ChangeTopicLayout,
                    FrameKind::Cancel,
                ],
            )
            .unwrap(),
        ]),
        TransportConfig::default(),
        MemoryBudget::new(MemoryLimits {
            total_bytes: 64 * 1024 * 1024,
            control_reserved_bytes: 8 * 1024 * 1024,
        })
        .unwrap(),
    )
    .unwrap()
}

fn accept(mut server: Server, observed: Arc<Mutex<PeerState>>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut sessions = JoinSet::new();
        loop {
            tokio::select! {
                session = server.accept() => {
                    let Some(session) = session else { break };
                    {
                        let mut state = observed.lock().unwrap();
                        state.classes.push(session.connection().class());
                        state.sessions.push(session.connection().session_id());
                    }
                    sessions.spawn(serve_session(session, observed.clone()));
                }
                _ = sessions.join_next(), if !sessions.is_empty() => {},
            }
        }
    })
}

async fn serve_session(session: Arc<Session>, state: Arc<Mutex<PeerState>>) {
    let mut streams = JoinSet::new();
    for stream in session.take_initial_streams().await {
        streams.spawn(serve_stream(stream, session.clone(), state.clone()));
    }
    loop {
        tokio::select! {
            stream = session.accept() => {
                let Some(stream) = stream else { break };
                streams.spawn(serve_stream(stream, session.clone(), state.clone()));
            }
            _ = streams.join_next(), if !streams.is_empty() => {},
        }
    }
}

async fn serve_stream(stream: FramedStream, session: Arc<Session>, state: Arc<Mutex<PeerState>>) {
    let (mut sender, mut receiver) = stream.split();
    while let Ok(Some(received)) = receiver.receive(deadline()).await {
        if received.frame().kind == i32::from(FrameKind::GetTopic) {
            let delay = {
                let mut state = state.lock().unwrap();
                state.topic_requests += 1;
                state.topic_delay
            };
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }
        let response = handle(received.frame(), &session, &mut state.lock().unwrap());
        if let Some(response) = response
            && sender.send(&response, deadline()).await.is_err()
        {
            break;
        }
    }
}

fn response<M: Message>(request: &Frame, kind: FrameKind, payload: &M) -> Frame {
    new_frame(
        kind,
        TrafficClass::Control,
        Id::from_slice(&request.request_id).unwrap(),
        Id::from_slice(&request.operation_id).unwrap(),
        payload,
        5000,
        &ProtocolLimits::default(),
    )
    .unwrap()
}

fn layout() -> public::TopicLayoutResult {
    public::TopicLayoutResult {
        topic_id: bytes(id(1)),
        revision: 1,
        active: vec![public::ShardRoute {
            shard_id: bytes(id(2)),
            hash_start: 0,
            hash_end: None,
            group_id: 1,
            authority_epoch: 1,
            birth_partition: 0,
            predecessors: Vec::new(),
            sealed_end: None,
        }],
        total_shards: 1,
        ..Default::default()
    }
}

fn handle(request: &Frame, session: &Session, state: &mut PeerState) -> Option<Frame> {
    state.traceparents.push(request.traceparent.clone());
    state.control_timeouts.push((
        FrameKind::try_from(request.kind).unwrap(),
        request.remaining_timeout_ms,
    ));
    if state.drop_first_admin {
        state.drop_first_admin = false;
        session.close();
        return None;
    }
    let limits = ProtocolLimits::default();
    Some(match FrameKind::try_from(request.kind).unwrap() {
        FrameKind::GetTopic => {
            let query: public::GetTopic = decode_payload(request.payload.clone(), &limits).unwrap();
            assert_eq!(query.name, TOPIC);
            response(
                request,
                FrameKind::TopicResult,
                &public::TopicResult {
                    topic_id: bytes(id(1)),
                    name: query.name,
                },
            )
        }
        FrameKind::GetTopicLayout => response(request, FrameKind::TopicLayoutResult, &layout()),
        FrameKind::CreateLogicalTopic => {
            let operation = Id::from_slice(&request.operation_id).unwrap();
            state.mutations.insert(operation);
            if state.drop_mutation_replies {
                return None;
            }
            let mut created = layout();
            created.topic_id = bytes(operation);
            response(request, FrameKind::TopicLayoutResult, &created)
        }
        FrameKind::ChangeTopicLayout => {
            state
                .mutations
                .insert(Id::from_slice(&request.operation_id).unwrap());
            if state.drop_mutation_replies {
                return None;
            }
            response(request, FrameKind::TopicLayoutResult, &layout())
        }
        FrameKind::ManageIdentity => {
            let action: public::ManageIdentity =
                decode_payload(request.payload.clone(), &limits).unwrap();
            let operation = Id::from_slice(&request.operation_id).unwrap();
            let first = state.mutations.insert(operation);
            if state.drop_mutation_replies {
                return None;
            }
            let issued = action.action == i32::from(public::IdentityAction::IssueCredential);
            assert!(issued || action.action == i32::from(public::IdentityAction::RevokeCredential));
            response(
                request,
                FrameKind::IdentityResult,
                &public::IdentityResult {
                    id: if issued {
                        bytes(operation)
                    } else {
                        action.target_id
                    },
                    parent_id: action.parent_id,
                    access_token: (issued && first).then(|| TOKEN.into()),
                    expires_at_ms: u64::MAX,
                    secret_unavailable: issued && !first,
                    ..Default::default()
                },
            )
        }
        FrameKind::Cancel => return None,
        kind => panic!("Unexpected SDK peer request: {kind:?}"),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_admin_creation_recovers_one_original_operation() {
    let peer = peer().await;
    peer.state.lock().unwrap().drop_mutation_replies = true;
    let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(200)).unwrap();
    let error = context
        .scope(peer.client.admin().create_topic(TOPIC))
        .await
        .unwrap_err();
    let tidemq_sdk::Error::Administration { pending, .. } = error else {
        panic!("An admitted administration call must retain its recovery handle");
    };
    assert_eq!(peer.state.lock().unwrap().mutations.len(), 1);
    assert_eq!(peer.client.admin().pending_operations().unwrap().len(), 1);
    peer.state.lock().unwrap().drop_mutation_replies = false;
    let until = Instant::now() + Duration::from_secs(5);
    let receipt = loop {
        match pending.retry().await {
            Ok(receipt) => break receipt,
            Err(error) if error.outcome_unknown() => assert!(Instant::now() < until),
            Err(error) => panic!("Original administration recovery failed: {error}"),
        }
    };
    let tidemq_sdk::AdminReceipt::Topic(topic) = receipt else {
        panic!("Expected the created topic");
    };
    assert_eq!(topic.name(), TOPIC);
    let operation = *peer.state.lock().unwrap().mutations.first().unwrap();
    assert_eq!(topic.id().to_string(), operation.to_string());
    assert_eq!(peer.state.lock().unwrap().mutations.len(), 1);
    assert!(peer.client.admin().pending_operations().unwrap().is_empty());
    assert_eq!(
        peer.client
            .close()
            .await
            .unwrap()
            .unresolved_administrations(),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_admin_waiters_keeps_bounded_work_and_close_reports_the_original_operations() {
    let peer = peer().await;
    let client = Client::builder(
        &peer.endpoint,
        TlsTrust::ca_pem(peer.ca.clone()),
        Credentials::token(TOKEN),
    )
    .close_timeout(Duration::from_millis(50))
    .build()
    .await
    .unwrap();
    peer.state.lock().unwrap().drop_mutation_replies = true;
    let admin = client.admin();
    let task = tokio::spawn(async move { admin.create_topic(TOPIC).await });
    let until = Instant::now() + Duration::from_secs(5);
    while peer.state.lock().unwrap().mutations.is_empty() {
        assert!(
            Instant::now() < until,
            "The original request did not reach the peer"
        );
        tokio::task::yield_now().await;
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(client.admin().pending_operations().unwrap().len(), 1);
    let closed = client.close().await.unwrap_err();
    assert_eq!(closed.report().unresolved_administrations(), 1);
    assert!(!closed.report().local_cleanup_complete());
    assert!(matches!(
        std::error::Error::source(&closed)
            .unwrap()
            .downcast_ref::<tidemq_sdk::Error>(),
        Some(tidemq_sdk::Error::Deadline)
    ));
    assert_eq!(peer.state.lock().unwrap().mutations.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_credential_secret_recovers_its_id_without_issuing_another_credential() {
    let peer = peer().await;
    peer.state.lock().unwrap().drop_mutation_replies = true;
    let principal = id(5).to_string().parse().unwrap();
    let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(200)).unwrap();
    let error = context
        .scope(
            peer.client
                .admin()
                .issue_token(principal, Duration::from_secs(3600)),
        )
        .await
        .unwrap_err();
    let tidemq_sdk::Error::Administration { pending, .. } = error else {
        panic!("Expected credential recovery");
    };
    peer.state.lock().unwrap().drop_mutation_replies = false;
    let until = Instant::now() + Duration::from_secs(5);
    let receipt = loop {
        match pending.retry().await {
            Ok(receipt) => break receipt,
            Err(error) if error.outcome_unknown() => assert!(Instant::now() < until),
            Err(error) => panic!("Credential reconciliation failed: {error}"),
        }
    };
    let tidemq_sdk::AdminReceipt::CredentialSecretUnavailable { id: credential, .. } = receipt
    else {
        panic!("A lost secret must not be reconstructed");
    };
    assert_eq!(peer.state.lock().unwrap().mutations.len(), 1);
    assert_eq!(
        credential.to_string(),
        peer.state
            .lock()
            .unwrap()
            .mutations
            .first()
            .unwrap()
            .to_string()
    );
    peer.client.admin().revoke_token(credential).await.unwrap();
    assert_eq!(peer.state.lock().unwrap().mutations.len(), 2);
    assert!(peer.client.admin().pending_operations().unwrap().is_empty());
    peer.client.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_admin_operations_stop_admission_at_the_shared_byte_and_count_limit() {
    let peer = peer().await;
    peer.state.lock().unwrap().drop_mutation_replies = true;
    for _ in 0..16 {
        let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(100)).unwrap();
        let error = context
            .scope(peer.client.admin().create_topic(TOPIC))
            .await
            .unwrap_err();
        assert!(error.outcome_unknown());
    }
    assert_eq!(peer.client.admin().pending_operations().unwrap().len(), 16);
    let error = peer.client.admin().create_topic(TOPIC).await.unwrap_err();
    assert_eq!(error.kind(), tidemq_sdk::ErrorKind::ResourceExhausted);
    assert_eq!(peer.state.lock().unwrap().mutations.len(), 16);
    let report = peer.client.close().await.unwrap();
    assert!(report.local_cleanup_complete());
    assert_eq!(report.unresolved_administrations(), 16);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_lookups_and_mutation_share_one_original_request_deadline() {
    let peer = peer().await;
    let client = Client::builder(
        &peer.endpoint,
        TlsTrust::ca_pem(peer.ca.clone()),
        Credentials::token(TOKEN),
    )
    .request_timeout(Duration::from_millis(400))
    .build()
    .await
    .unwrap();
    {
        let mut state = peer.state.lock().unwrap();
        state.topic_delay = Duration::from_millis(200);
        state.drop_mutation_replies = true;
    }
    assert!(
        client
            .admin()
            .split_topic_at(TOPIC, 1 << 63)
            .await
            .unwrap_err()
            .outcome_unknown()
    );
    {
        let state = peer.state.lock().unwrap();
        let (_, remaining) = state
            .control_timeouts
            .iter()
            .find(|(kind, _)| *kind == FrameKind::ChangeTopicLayout)
            .expect("The split mutation must be reached after its read-only lookup");
        assert!(
            *remaining > 0 && *remaining < 250,
            "The mutation must retain the original remaining deadline"
        );
        assert_eq!(state.mutations.len(), 1);
    }
    assert_eq!(
        client.close().await.unwrap().unresolved_administrations(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_queries_and_expired_preflight_lookups_do_not_report_unknown_mutations() {
    let peer = peer().await;
    peer.state.lock().unwrap().topic_delay = Duration::from_secs(1);
    let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(5)).unwrap();
    let cancellation = context.cancellation_token();
    let admin = peer.client.admin();
    let query = async {
        let until = deadline();
        while peer.state.lock().unwrap().topic_requests == 0 {
            assert!(Instant::now() < until, "The query did not reach the peer");
            tokio::task::yield_now().await;
        }
        cancellation.cancel();
    };
    let (result, ()) = tokio::join!(context.scope(admin.topic_info(TOPIC)), query);
    let error = result.unwrap_err();
    assert_eq!(error.kind(), tidemq_sdk::ErrorKind::Cancelled);
    assert!(!error.outcome_unknown());

    let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(50)).unwrap();
    let error = context
        .scope(peer.client.admin().split_topic_at(TOPIC, 1 << 63))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), tidemq_sdk::ErrorKind::DeadlineExceeded);
    assert!(!error.outcome_unknown());
    assert!(peer.state.lock().unwrap().mutations.is_empty());
    assert!(peer.client.admin().pending_operations().unwrap().is_empty());
    peer.client.close().await.unwrap();
}

#[tokio::test]
async fn trace_context_is_request_scoped_validated_and_does_not_leak_across_lanes() {
    use tidemq_sdk::scope_traceparent;
    const FIRST: &str = "00-11111111111111111111111111111111-2222222222222222-01";
    const SECOND: &str = "00-33333333333333333333333333333333-4444444444444444-01";
    let peer = peer().await;
    let admin = peer.client.admin();
    let (a, b) = tokio::join!(
        scope_traceparent(FIRST, admin.topic_info(TOPIC)),
        scope_traceparent(SECOND, admin.topic_info(TOPIC))
    );
    a.unwrap();
    b.unwrap();
    admin.topic_info(TOPIC).await.unwrap();
    let polled = std::sync::atomic::AtomicBool::new(false);
    let invalid = scope_traceparent("invalid", async {
        polled.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    })
    .await;
    assert!(invalid.is_err());
    assert!(!polled.load(std::sync::atomic::Ordering::Relaxed));
    let state = peer.state.lock().unwrap();
    assert!(state.traceparents.contains(&FIRST.into()));
    assert!(state.traceparents.contains(&SECOND.into()));
    assert_eq!(state.traceparents.last().map(String::as_str), Some(""));
}

#[tokio::test]
async fn dns_endpoint_connects_to_the_ipv4_local_development_listener() {
    let peer = peer().await;
    let client = Client::builder(
        &peer.endpoint,
        TlsTrust::ca_pem(peer.ca.clone()),
        Credentials::token(TOKEN),
    )
    .build()
    .await
    .unwrap();
    assert_eq!(
        client
            .admin()
            .topic_info(TOPIC)
            .await
            .unwrap()
            .shard_count(),
        1
    );
    client.close().await.unwrap();
}

#[tokio::test]
async fn reconnect_uses_the_same_logical_session_after_a_physical_connection_closes() {
    let peer = peer().await;
    peer.state.lock().unwrap().drop_first_admin = true;
    assert_eq!(
        peer.client
            .admin()
            .create_topic(TOPIC)
            .await
            .unwrap()
            .shard_count(),
        1
    );
    let state = peer.state.lock().unwrap();
    assert_eq!(
        state.classes,
        vec![TrafficClass::Control, TrafficClass::Control]
    );
    assert_eq!(state.sessions.len(), 2);
    assert!(
        state
            .sessions
            .iter()
            .all(|session| Some(session) == state.sessions.first())
    );
    assert!(peer.client.metrics().snapshot().retries > 0);
}

#[derive(Clone)]
struct HostWriter(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for HostWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn sdk_uses_the_existing_host_subscriber_and_omits_credentials() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let sink = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NEW)
        .with_writer(move || HostWriter(sink.clone()))
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let peer = peer().await;
    peer.client.admin().topic_info(TOPIC).await.unwrap();
    tracing::info!("host-subscriber-survived");
    let text = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(text.contains("host-subscriber-survived"));
    assert!(text.contains("tidemq.sdk.request"));
    assert!(!text.contains(TOKEN));
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn optional_metrics_register_into_the_hosts_registry() {
    let peer = peer().await;
    peer.client.admin().topic_info(TOPIC).await.unwrap();
    let mut registry = prometheus_client::registry::Registry::default();
    peer.client
        .metrics()
        .register(registry.sub_registry_with_prefix("host_queue_client"));
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    assert!(text.contains("host_queue_client_requests_total 2"));
    assert!(text.contains("host_queue_client_operation_duration_seconds_bucket"));
    assert!(!text.contains(TOKEN));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_reconnect_falls_back_to_ipv6_without_replacing_the_logical_session() {
    let hostname =
        std::env::var("TIDEMQ_TEST_DUAL_STACK_HOST").unwrap_or_else(|_| "localhost".into());
    let addresses: Vec<_> = tokio::net::lookup_host((hostname.as_str(), 1))
        .await
        .unwrap()
        .collect();
    assert!(
        addresses.iter().any(SocketAddr::is_ipv4) && addresses.iter().any(SocketAddr::is_ipv6),
        "The dual-stack fixture must resolve to IPv4 and IPv6 loopback addresses; configure TIDEMQ_TEST_DUAL_STACK_HOST and the test container hosts file"
    );
    assert!(addresses.iter().all(|address| address.ip().is_loopback()));
    let mut peer = peer_at(&hostname).await;
    let port = peer
        .endpoint
        .rsplit_once(':')
        .unwrap()
        .1
        .parse::<u16>()
        .unwrap();
    let replacement = server(
        SocketAddr::from((Ipv6Addr::LOCALHOST, port)),
        peer.identity.clone(),
    );
    let replacement_task = accept(replacement, peer.state.clone());
    peer.task.abort();
    let _ = (&mut peer.task).await;
    let info = tokio::time::timeout(
        Duration::from_secs(12),
        peer.client.admin().topic_info(TOPIC),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(info.name(), TOPIC);
    {
        let state = peer.state.lock().unwrap();
        assert!(state.sessions.len() >= 2);
        assert!(
            state
                .sessions
                .iter()
                .all(|session| Some(session) == state.sessions.first())
        );
    }
    peer.client.close().await.unwrap();
    replacement_task.abort();
    let _ = replacement_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_uses_the_configured_connect_budget_inside_the_request_deadline() {
    let mut peer = peer().await;
    let client = Client::builder(
        &peer.endpoint,
        TlsTrust::ca_pem(peer.ca.clone()),
        Credentials::token(TOKEN),
    )
    .connect_timeout(Duration::from_millis(200))
    .request_timeout(Duration::from_secs(10))
    .build()
    .await
    .unwrap();
    peer.client.close().await.unwrap();
    let port = peer
        .endpoint
        .rsplit_once(':')
        .unwrap()
        .1
        .parse::<u16>()
        .unwrap();
    peer.task.abort();
    let _ = (&mut peer.task).await;
    let until = deadline();
    let ipv4 = loop {
        match tokio::net::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], port))).await {
            Ok(socket) => break socket,
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                assert!(
                    Instant::now() < until,
                    "Stopped peer did not release its UDP listener"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(error) => panic!("Cannot bind the blackhole listener: {error}"),
        }
    };
    let ipv6 = tokio::net::UdpSocket::bind(SocketAddr::from((Ipv6Addr::LOCALHOST, port)))
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(3), client.admin().topic_info(TOPIC))
        .await
        .expect(
            "Reconnect must use the configured 200 ms budget instead of the 10 s request deadline",
        )
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        tidemq_sdk::ErrorKind::DeadlineExceeded | tidemq_sdk::ErrorKind::Unavailable
    ));
    client.close().await.unwrap();
    drop((ipv4, ipv6));
}
