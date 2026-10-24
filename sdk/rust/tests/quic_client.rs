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
    let key = KeyPair::generate().unwrap();
    let certificate = CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let ca = Bytes::from(certificate.pem());
    let identity = TlsIdentity::new(ca.clone(), Bytes::from(key.serialize_pem())).unwrap();
    let server = server("127.0.0.1:0".parse().unwrap(), identity.clone());
    let endpoint = format!("localhost:{}", server.local_addr().port());
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
                    partitions: 1,
                },
            )
        }
        FrameKind::GetTopicLayout => response(request, FrameKind::TopicLayoutResult, &layout()),
        FrameKind::CreateLogicalTopic => response(request, FrameKind::TopicLayoutResult, &layout()),
        FrameKind::Cancel => return None,
        kind => panic!("Unexpected SDK peer request: {kind:?}"),
    })
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
    let mut peer = peer().await;
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
