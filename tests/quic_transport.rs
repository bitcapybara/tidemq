use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use tidemq::{
    budget::MemoryBudget,
    config::MemoryConfig,
    transport::{
        Client, ClientCredentials, ClientTls, Error, NodeGrant, PeerTarget, Server,
        ServerAuthorization, ServerTls, StreamPurpose, TlsIdentity, TokenGrant, TransportConfig,
    },
};
use tidemq_protocol::{
    Id, ProtocolLimits,
    common::{Frame, FrameKind, TrafficClass},
    framing::{ALPN, encode_frame, new_frame},
    internal::RaftRequest,
    public::{CreateTopic, Hello, Message, ProduceBatch, ProduceResult},
};
use tokio::{
    io::AsyncReadExt,
    time::{Instant, timeout},
};

const TOKEN: &str = "test-token-with-at-least-32-characters";

struct Leaf {
    certificate: Certificate,
    key: KeyPair,
}
struct Pki {
    ca: Certificate,
    server: Leaf,
    rotated_server: Leaf,
    client: Leaf,
    rogue_client: Leaf,
}

fn pki() -> Pki {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca = params.self_signed(&key).unwrap();
    let issuer = Issuer::new(params, key);
    let leaf = |name: &str, usage| {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![name.to_string()]).unwrap();
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![usage];
        Leaf {
            certificate: params.signed_by(&key, &issuer).unwrap(),
            key,
        }
    };
    Pki {
        ca,
        server: leaf("localhost", ExtendedKeyUsagePurpose::ServerAuth),
        rotated_server: leaf("localhost", ExtendedKeyUsagePurpose::ServerAuth),
        client: leaf("node-2.local", ExtendedKeyUsagePurpose::ClientAuth),
        rogue_client: leaf("node-2.local", ExtendedKeyUsagePurpose::ClientAuth),
    }
}

fn identity(leaf: &Leaf) -> TlsIdentity {
    TlsIdentity::new(
        Bytes::from(leaf.certificate.pem()),
        Bytes::from(leaf.key.serialize_pem()),
    )
    .unwrap()
}
fn address() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(&MemoryConfig::default()).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn config() -> TransportConfig {
    TransportConfig {
        handshake_timeout: Duration::from_secs(1),
        stream_prelude_timeout: Duration::from_millis(200),
        ..TransportConfig::default()
    }
}
fn credentials() -> ClientCredentials {
    ClientCredentials::External {
        session_id: Id::random().unwrap(),
        access_token: TOKEN.into(),
    }
}
fn target(server: &Server) -> PeerTarget {
    PeerTarget {
        address: server.local_addr(),
        server_name: "localhost".into(),
        local_role: "sdk".into(),
        remote_role: "gateway".into(),
        cluster_id: None,
        node_id: None,
    }
}

fn external_server(pki: &Pki, config: TransportConfig) -> Server {
    let grant = TokenGrant::new(
        TOKEN,
        "test-principal".into(),
        "test-scope".into(),
        vec![
            FrameKind::CreateTopic,
            FrameKind::ProduceBatch,
            FrameKind::Read,
            FrameKind::AckBatch,
            FrameKind::Cancel,
        ],
    )
    .unwrap();
    Server::bind(
        address(),
        ServerTls {
            identity: identity(&pki.server),
            client_trust_roots: None,
        },
        ServerAuthorization::External(vec![grant]),
        config,
        budget(),
    )
    .unwrap()
}
fn external_client(pki: &Pki, config: TransportConfig) -> Client {
    Client::bind(
        address(),
        ClientTls {
            trust_roots: Bytes::from(pki.ca.pem()),
            identity: None,
            expected_nodes: vec![],
        },
        config,
        budget(),
    )
    .unwrap()
}

fn frame(
    kind: FrameKind,
    class: TrafficClass,
    operation: Id,
    payload: &impl prost::Message,
) -> Frame {
    new_frame(
        kind,
        class,
        Id::random().unwrap(),
        operation,
        payload,
        2_000,
        &ProtocolLimits::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn idle_endpoints_close_without_waiting_for_future_connection_activity() {
    let pki = pki();
    let client = external_client(&pki, config());
    let server = external_server(&pki, config());
    let address = server.local_addr();
    tokio::task::yield_now().await;
    client
        .close(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    server.shutdown().await.unwrap();
    let rebound = std::net::UdpSocket::bind(address).unwrap();
    drop(rebound);
}

#[tokio::test]
async fn rpc_shutdown_drains_owned_tasks_and_releases_the_listener() {
    let pki = pki();
    let mut server = external_server(&pki, config());
    let address = server.local_addr();
    let client = external_client(&pki, config());
    let session = client
        .pooled(
            target(&server),
            &credentials(),
            TrafficClass::Control,
            deadline(),
        )
        .await
        .unwrap();
    let accepted = timeout(Duration::from_secs(1), server.accept())
        .await
        .unwrap()
        .unwrap();
    let direct = client
        .connect(
            target(&server),
            &credentials(),
            TrafficClass::Bulk,
            deadline(),
        )
        .await
        .unwrap();
    let direct_peer = server.accept().await.unwrap();
    server.stop_accepting(deadline()).await.unwrap();
    let rpc = tidemq::transport::rpc::RpcClient::from_session(session.clone(), Default::default())
        .await
        .unwrap();
    rpc.close(deadline()).await;
    direct_peer.shutdown(deadline()).await.unwrap();
    accepted.shutdown(deadline()).await.unwrap();
    client.close(deadline()).await.unwrap();
    assert!(direct.connection().is_closed());
    assert!(matches!(
        client
            .connect(
                target(&server),
                &credentials(),
                TrafficClass::Control,
                deadline()
            )
            .await,
        Err(tidemq::transport::Error::Closed)
    ));
    assert!(matches!(
        client
            .pooled(
                target(&server),
                &credentials(),
                TrafficClass::Control,
                deadline()
            )
            .await,
        Err(tidemq::transport::Error::Closed)
    ));
    assert!(session.connection().is_closed());
    assert!(
        session
            .connection()
            .open(StreamPurpose::Core, deadline())
            .await
            .is_err()
    );
    drop(rpc);
    assert_eq!(Arc::strong_count(&accepted), 1);
    assert_eq!(Arc::strong_count(&direct_peer), 1);
    assert_eq!(Arc::strong_count(&direct), 1);
    assert_eq!(Arc::strong_count(&session), 1);
    drop(accepted);
    drop(direct_peer);
    drop(direct);
    drop(session);
    drop(client);
    server.shutdown().await.unwrap();
    let rebound = std::net::UdpSocket::bind(address).unwrap();
    drop(rebound);
}
fn produce() -> Frame {
    let id = Id::random().unwrap();
    frame(
        FrameKind::ProduceBatch,
        TrafficClass::Produce,
        id,
        &ProduceBatch {
            topic_id: Bytes::copy_from_slice(id.as_bytes()),
            partition_id: 0,
            producer_id: Bytes::copy_from_slice(id.as_bytes()),
            producer_epoch: 1,
            first_sequence: 0,
            messages: vec![Message {
                key: Bytes::new(),
                body: Bytes::from(vec![42; 96_000]),
            }],
            content_digest: Bytes::from_static(&[1; 32]),
            owner_epoch: 1,
        },
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trusted_tls_pipelines_large_frames_across_small_windows() {
    let pki = pki();
    let mut server = external_server(&pki, config());
    let client = external_client(&pki, config());
    let session = client
        .connect(
            target(&server),
            &credentials(),
            TrafficClass::Produce,
            deadline(),
        )
        .await
        .unwrap();
    let accepted = server.accept().await.unwrap();
    let mut client_lane = session.take_initial_streams().await.pop().unwrap();
    let mut server_lane = accepted.take_initial_streams().await.pop().unwrap();
    let first = produce();
    let second = produce();
    let request_ids = [first.request_id.clone(), second.request_id.clone()];
    let producer = tokio::spawn(async move {
        client_lane.sender.send(&first, deadline()).await.unwrap();
        client_lane.sender.send(&second, deadline()).await.unwrap();
        for request_id in request_ids {
            let response = client_lane
                .receiver
                .receive(deadline())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response.frame().request_id, request_id);
        }
    });
    let first = server_lane
        .receiver
        .receive(deadline())
        .await
        .unwrap()
        .unwrap();
    let second = server_lane
        .receiver
        .receive(deadline())
        .await
        .unwrap()
        .unwrap();
    for request in [first, second] {
        let mut response = frame(
            FrameKind::ProduceResult,
            TrafficClass::Produce,
            Id::from_slice(&request.frame().operation_id).unwrap(),
            &ProduceResult {
                first_offset: 7,
                message_count: 1,
                duplicate: false,
            },
        );
        response.request_id = request.frame().request_id.clone();
        server_lane
            .sender
            .send(&response, deadline())
            .await
            .unwrap();
    }
    producer.await.unwrap();
    assert_eq!(client.metrics.snapshot().frames_sent_total, 2);
    assert!(server.metrics.snapshot().bytes_received_total > 190_000);
    assert!(session.connection().recovery_snapshot().unwrap().is_some());
    let mut registry = prometheus_client::registry::Registry::with_prefix("tidemq_quic_test");
    registry.register_collector(Box::new(client.metrics.clone()));
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    assert!(text.contains("tidemq_quic_test_connections_total 1"));
    assert!(text.contains("tidemq_quic_test_send_wait_seconds_total"));
    assert!(text.lines().count() < 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_connections_follow_retained_resources_without_counting_clones() {
    let pki = pki();
    let mut server = external_server(&pki, config());
    let client = external_client(&pki, config());
    let session = client
        .connect(
            target(&server),
            &credentials(),
            TrafficClass::Produce,
            deadline(),
        )
        .await
        .unwrap();
    let accepted = server.accept().await.unwrap();
    let session_clone = session.clone();
    let connection = session.connection();
    let connection_clone = connection.clone();
    assert_eq!(client.metrics.snapshot().active_connections, 1);
    assert_eq!(server.metrics.snapshot().active_connections, 1);
    session.shutdown(deadline()).await.unwrap();
    accepted.shutdown(deadline()).await.unwrap();
    assert_eq!(client.metrics.snapshot().active_connections, 1);
    drop(session_clone);
    drop(session);
    drop(connection);
    assert_eq!(client.metrics.snapshot().active_connections, 1);
    drop(connection_clone);
    drop(accepted);
    tokio::time::timeout_at(deadline(), async {
        while client.metrics.snapshot().active_connections != 0
            || server.metrics.snapshot().active_connections != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("closed connection resources must release their admission envelope");
    assert_eq!(client.metrics.snapshot().connections_total, 1);
    let mut registry = prometheus_client::registry::Registry::with_prefix("tidemq_quic_test");
    registry.register_collector(Box::new(client.metrics.clone()));
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    assert!(text.contains("# TYPE tidemq_quic_test_active_connections gauge"));
    assert!(text.contains("tidemq_quic_test_active_connections 0"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn untrusted_certificate_and_wrong_hostname_are_rejected() {
    let pki = pki();
    let server = external_server(&pki, config());
    let other_pki = self::pki();
    let untrusted = external_client(&other_pki, config());
    assert!(
        untrusted
            .connect(
                target(&server),
                &credentials(),
                TrafficClass::Produce,
                deadline()
            )
            .await
            .is_err()
    );
    let client = external_client(&pki, config());
    let mut wrong_name = target(&server);
    wrong_name.server_name = "wrong.local".into();
    assert!(
        client
            .connect(
                wrong_name,
                &credentials(),
                TrafficClass::Produce,
                deadline()
            )
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_tokens_are_checked_on_each_connection_and_frame() {
    let pki = pki();
    let mut server = external_server(&pki, config());
    let client = external_client(&pki, config());
    let wrong = ClientCredentials::External {
        session_id: Id::random().unwrap(),
        access_token: "wrong-token-with-at-least-32-characters".into(),
    };
    assert!(
        client
            .connect(target(&server), &wrong, TrafficClass::Produce, deadline())
            .await
            .is_err()
    );
    let session = client
        .connect(
            target(&server),
            &credentials(),
            TrafficClass::Control,
            deadline(),
        )
        .await
        .unwrap();
    let accepted = server.accept().await.unwrap();
    let mut lane = session.take_initial_streams().await.remove(0);
    let mut incoming = accepted.take_initial_streams().await.pop().unwrap();
    let request = frame(
        FrameKind::Diagnose,
        TrafficClass::Control,
        Id::random().unwrap(),
        &CreateTopic {
            name: "forbidden".into(),
            partitions: 1,
        },
    );
    lane.sender.send(&request, deadline()).await.unwrap();
    assert!(matches!(
        incoming.receiver.receive(deadline()).await,
        Err(Error::Authorization { .. })
    ));
}

fn node_grant(leaf: &Leaf, cluster_id: Id, node_id: u64, role: &str) -> NodeGrant {
    NodeGrant {
        certificate_pin: NodeGrant::certificate_pin(leaf.certificate.der()),
        certificate_name: if node_id == 1 {
            "localhost".into()
        } else {
            "node-2.local".into()
        },
        cluster_id,
        node_id,
        role: role.into(),
        allowed_groups: vec![7],
        allowed_classes: vec![
            TrafficClass::Control,
            TrafficClass::Replication,
            TrafficClass::Bulk,
        ],
    }
}
fn internal_credentials(cluster_id: Id, node_id: u64) -> ClientCredentials {
    ClientCredentials::Internal {
        session_id: Id::random().unwrap(),
        cluster_id,
        node_id,
        role: "storage".into(),
    }
}
fn internal_server(pki: &Pki, cluster: Id) -> Server {
    Server::bind(
        address(),
        ServerTls {
            identity: identity(&pki.server),
            client_trust_roots: Some(Bytes::from(pki.ca.pem())),
        },
        ServerAuthorization::Internal(vec![node_grant(&pki.client, cluster, 2, "storage")]),
        config(),
        budget(),
    )
    .unwrap()
}
fn internal_client(pki: &Pki, expected: NodeGrant) -> Client {
    Client::bind(
        address(),
        ClientTls {
            trust_roots: Bytes::from(pki.ca.pem()),
            identity: Some(identity(&pki.client)),
            expected_nodes: vec![expected],
        },
        config(),
        budget(),
    )
    .unwrap()
}
fn internal_target(server: &Server, cluster: Id) -> PeerTarget {
    PeerTarget {
        local_role: "storage".into(),
        remote_role: "storage".into(),
        cluster_id: Some(cluster),
        node_id: Some(1),
        ..target(server)
    }
}

struct BlockedSemanticControl {
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

impl tidemq::server::WireService for BlockedSemanticControl {
    type Error = tidemq_protocol::Error;

    async fn handle(
        &self,
        _peer: tidemq::transport::Authorization,
        request: &Frame,
        _deadline: Instant,
        order: Option<tidemq::server::SubmissionOrder>,
    ) -> Result<Option<Frame>, Self::Error> {
        drop(order);
        if request.kind == i32::from(FrameKind::InternalCommand) {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        let mut response = request.clone();
        response.kind = FrameKind::RaftResult.into();
        response.payload = prost::Message::encode_to_vec(&tidemq_protocol::internal::RaftResult {
            rpc_id: request.request_id.clone(),
            encoded_result: Bytes::from_static(b"completed"),
        })
        .into();
        Ok(Some(response))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_raft_control_progresses_when_semantic_core_handlers_are_full() {
    let pki = pki();
    let cluster = Id::random().unwrap();
    let server = internal_server(&pki, cluster);
    let target = internal_target(&server, cluster);
    let service = Arc::new(BlockedSemanticControl {
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let (stop, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(tidemq::server::run(
        server,
        service.clone(),
        tidemq::server::DispatchConfig {
            core_handlers: 2,
            raft_handlers: 1,
            ..Default::default()
        },
        budget(),
        Arc::new(tidemq::server::DispatchMetrics::default()),
        receiver,
    ));
    let client = internal_client(&pki, node_grant(&pki.server, cluster, 1, "storage"));
    let session = client
        .connect(
            target,
            &internal_credentials(cluster, 2),
            TrafficClass::Control,
            deadline(),
        )
        .await
        .unwrap();
    let rpc = tidemq::transport::rpc::RpcClient::from_session(
        session,
        tidemq::transport::rpc::RpcConfig {
            liveness_timeout: Duration::from_millis(100),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let semantic = || {
        let operation = Id::random().unwrap();
        frame(
            FrameKind::InternalCommand,
            TrafficClass::Control,
            operation,
            &tidemq_protocol::internal::InternalCommand {
                group_id: 7,
                operation_id: Bytes::copy_from_slice(operation.as_bytes()),
                encoded_command: Bytes::from_static(b"blocked semantic read"),
            },
        )
    };
    let first = rpc.submit(semantic(), deadline()).await.unwrap();
    let second = rpc.submit(semantic(), deadline()).await.unwrap();
    timeout(Duration::from_secs(1), service.started.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    let wire_id = Id::random().unwrap();
    let heartbeat = new_frame(
        FrameKind::RaftVote,
        TrafficClass::Control,
        wire_id,
        Id::random().unwrap(),
        &RaftRequest {
            group_id: 7,
            rpc_id: Bytes::copy_from_slice(wire_id.as_bytes()),
            encoded_rpc: Bytes::from_static(b"control"),
        },
        2_000,
        &ProtocolLimits::default(),
    )
    .unwrap();
    let response = timeout(Duration::from_secs(1), rpc.call(heartbeat, deadline()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.frame().kind, i32::from(FrameKind::RaftResult));
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        !rpc.is_closed(),
        "Native progress must preserve slow application handlers"
    );
    assert!(client.metrics.snapshot().liveness_probes_total >= 1);
    assert_eq!(client.metrics.snapshot().liveness_failures_total, 0);
    service.release.add_permits(2);
    first.receive().await.unwrap();
    second.receive().await.unwrap();
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    rpc.close(deadline()).await;
    client.close(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_requires_certificate_pin_node_identity_and_group_authority() {
    let pki = pki();
    let cluster = Id::random().unwrap();
    let mut server = internal_server(&pki, cluster);
    let no_identity = external_client(&pki, config());
    assert!(
        no_identity
            .connect(
                target(&server),
                &credentials(),
                TrafficClass::Control,
                deadline()
            )
            .await
            .is_err()
    );
    let mut wrong_pin = node_grant(&pki.server, cluster, 1, "storage");
    wrong_pin.certificate_pin = [9; 32];
    let wrong_server = internal_client(&pki, wrong_pin);
    assert!(
        wrong_server
            .connect(
                internal_target(&server, cluster),
                &internal_credentials(cluster, 2),
                TrafficClass::Replication,
                deadline()
            )
            .await
            .is_err()
    );
    let client = internal_client(&pki, node_grant(&pki.server, cluster, 1, "storage"));
    let rogue = Client::bind(
        address(),
        ClientTls {
            trust_roots: Bytes::from(pki.ca.pem()),
            identity: Some(identity(&pki.rogue_client)),
            expected_nodes: vec![node_grant(&pki.server, cluster, 1, "storage")],
        },
        config(),
        budget(),
    )
    .unwrap();
    assert!(
        rogue
            .connect(
                internal_target(&server, cluster),
                &internal_credentials(cluster, 2),
                TrafficClass::Replication,
                deadline()
            )
            .await
            .is_err()
    );
    assert!(
        client
            .connect(
                internal_target(&server, cluster),
                &internal_credentials(cluster, 3),
                TrafficClass::Replication,
                deadline()
            )
            .await
            .is_err()
    );
    let session = client
        .connect(
            internal_target(&server, cluster),
            &internal_credentials(cluster, 2),
            TrafficClass::Replication,
            deadline(),
        )
        .await
        .unwrap();
    let accepted = server.accept().await.unwrap();
    let mut outgoing = session.take_initial_streams().await.pop().unwrap();
    let mut incoming = accepted.take_initial_streams().await.pop().unwrap();
    let id = Id::random().unwrap();
    let request = frame(
        FrameKind::RaftAppend,
        TrafficClass::Replication,
        id,
        &RaftRequest {
            group_id: 99,
            rpc_id: Bytes::copy_from_slice(id.as_bytes()),
            encoded_rpc: Bytes::from_static(b"not-authorized"),
        },
    );
    outgoing.sender.send(&request, deadline()).await.unwrap();
    assert!(matches!(
        incoming.receiver.receive(deadline()).await,
        Err(Error::Authorization { .. })
    ));
}

async fn raw_client(
    pki: &Pki,
    server: &Server,
) -> (
    s2n_quic::Client,
    s2n_quic::Connection,
    s2n_quic::stream::BidirectionalStream,
) {
    let roots = pki.ca.pem();
    let tls = s2n_quic::provider::tls::s2n_tls::Client::builder()
        .with_application_protocols([ALPN])
        .unwrap()
        .with_empty_trust_store()
        .unwrap()
        .with_certificate(roots.as_str())
        .unwrap()
        .build()
        .unwrap();
    let endpoint = s2n_quic::Client::builder()
        .with_tls(tls)
        .unwrap()
        .with_io(address())
        .unwrap()
        .start()
        .unwrap();
    let mut connection = endpoint
        .connect(s2n_quic::client::Connect::new(server.local_addr()).with_server_name("localhost"))
        .await
        .unwrap();
    let stream = connection.open_bidirectional_stream().await.unwrap();
    (endpoint, connection, stream)
}

async fn raw_hello(stream: &mut s2n_quic::stream::BidirectionalStream, class: TrafficClass) {
    let session = Id::random().unwrap();
    let hello = frame(
        FrameKind::Hello,
        class,
        session,
        &Hello {
            session_id: Bytes::copy_from_slice(session.as_bytes()),
            traffic_class: class.into(),
            access_token: TOKEN.into(),
        },
    );
    stream
        .send(encode_frame(&hello, &ProtocolLimits::default()).unwrap())
        .await
        .unwrap();
    let mut prefix = [0; 4];
    stream.read_exact(&mut prefix).await.unwrap();
    let mut response = vec![0; u32::from_be_bytes(prefix) as usize];
    stream.read_exact(&mut response).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_unclassified_hello_is_rejected_without_body_allocation() {
    let pki = pki();
    let server = external_server(&pki, config());
    let (_endpoint, _connection, mut stream) = raw_client(&pki, &server).await;
    stream
        .send(Bytes::copy_from_slice(&16_385_u32.to_be_bytes()))
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_secs(2), stream.receive())
            .await
            .unwrap()
            .is_err()
    );
    assert!(server.metrics.snapshot().rejected_total > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_and_oversized_bound_frames_are_rejected() {
    let pki = pki();
    let mut server = external_server(&pki, config());
    for bytes in [
        Bytes::copy_from_slice(&u32::MAX.to_be_bytes()),
        Bytes::from_static(&[0, 0, 0, 1, 0xff]),
    ] {
        let (_endpoint, _connection, mut stream) = raw_client(&pki, &server).await;
        raw_hello(&mut stream, TrafficClass::Produce).await;
        let accepted = server.accept().await.unwrap();
        let mut lane = accepted.take_initial_streams().await.pop().unwrap();
        stream.send(bytes).await.unwrap();
        assert!(lane.receiver.receive(deadline()).await.is_err());
        assert!(matches!(
            lane.receiver.receive(deadline()).await,
            Err(Error::Poisoned)
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_frame_deadline_poisons_only_its_lane() {
    let pki = pki();
    let mut server = external_server(&pki, config());
    let (_endpoint, _connection, mut stream) = raw_client(&pki, &server).await;
    raw_hello(&mut stream, TrafficClass::Produce).await;
    let accepted = server.accept().await.unwrap();
    let mut lane = accepted.take_initial_streams().await.pop().unwrap();
    stream
        .send(Bytes::copy_from_slice(&100_u32.to_be_bytes()))
        .await
        .unwrap();
    assert!(matches!(
        lane.receiver
            .receive(Instant::now() + Duration::from_millis(30))
            .await,
        Err(Error::Deadline { .. })
    ));
    assert!(matches!(
        lane.receiver.receive(deadline()).await,
        Err(Error::Poisoned)
    ));
    assert!(server.metrics.snapshot().deadlines_total > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ack_admission_exhaustion_preserves_preestablished_control_streams() {
    let pki = pki();
    let config = TransportConfig {
        max_streams: 6,
        max_ack_streams: 2,
        ..config()
    };
    let mut server = external_server(&pki, config.clone());
    let client = external_client(&pki, config);
    let session = client
        .connect(
            target(&server),
            &credentials(),
            TrafficClass::Control,
            deadline(),
        )
        .await
        .unwrap();
    let accepted = server.accept().await.unwrap();
    assert!(session.connection().core_streams_ready());
    let ack_one = session
        .connection()
        .open(StreamPurpose::Ack, deadline())
        .await
        .unwrap();
    let ack_two = session
        .connection()
        .open(StreamPurpose::Ack, deadline())
        .await
        .unwrap();
    assert!(matches!(
        session
            .connection()
            .open(StreamPurpose::Ack, deadline())
            .await,
        Err(Error::Admission { .. })
    ));
    let mut core = session.take_initial_streams().await.remove(0);
    let mut incoming = accepted.take_initial_streams().await.pop().unwrap();
    let request = frame(
        FrameKind::CreateTopic,
        TrafficClass::Control,
        Id::random().unwrap(),
        &CreateTopic {
            name: "control-progress".into(),
            partitions: 1,
        },
    );
    core.sender.send(&request, deadline()).await.unwrap();
    assert_eq!(
        incoming
            .receiver
            .receive(deadline())
            .await
            .unwrap()
            .unwrap()
            .frame()
            .request_id,
        request.request_id
    );
    drop((ack_one, ack_two));
    core.sender.reset().unwrap();
    drop(core);
    assert!(!session.connection().core_streams_ready());
    assert!(matches!(
        session
            .connection()
            .open(StreamPurpose::Ack, deadline())
            .await,
        Err(Error::Admission { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_bulk_connection_does_not_block_control_progress() {
    let pki = pki();
    let mut server = external_server(&pki, config());
    let client = external_client(&pki, config());
    let creds = credentials();
    let bulk = client
        .connect(target(&server), &creds, TrafficClass::Bulk, deadline())
        .await
        .unwrap();
    let _slow_peer = server.accept().await.unwrap();
    let mut bulk_lane = bulk.take_initial_streams().await.pop().unwrap();
    let body = tidemq_protocol::public::DeliveryBatch {
        deliveries: vec![tidemq_protocol::public::Delivery {
            offset: 0,
            key: Bytes::new(),
            body: Bytes::from(vec![7; 200_000]),
            delivery_token: Bytes::new(),
        }],
        next_offset: 1,
        ..Default::default()
    };
    let bulk_frame = frame(
        FrameKind::DeliveryBatch,
        TrafficClass::Bulk,
        Id::random().unwrap(),
        &body,
    );
    let mut blocked = tokio::spawn(async move {
        for _ in 0..16 {
            bulk_lane.sender.send(&bulk_frame, deadline()).await?;
        }
        Ok::<_, Error>(())
    });
    assert!(
        timeout(Duration::from_millis(30), &mut blocked)
            .await
            .is_err()
    );
    let control = client
        .connect(target(&server), &creds, TrafficClass::Control, deadline())
        .await
        .unwrap();
    let control_peer = server.accept().await.unwrap();
    let mut outgoing = control.take_initial_streams().await.remove(0);
    let mut incoming = control_peer.take_initial_streams().await.pop().unwrap();
    let request = frame(
        FrameKind::CreateTopic,
        TrafficClass::Control,
        Id::random().unwrap(),
        &CreateTopic {
            name: "bulk-isolation".into(),
            partitions: 1,
        },
    );
    outgoing.sender.send(&request, deadline()).await.unwrap();
    assert!(
        incoming
            .receiver
            .receive(deadline())
            .await
            .unwrap()
            .is_some()
    );
    blocked.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_reuses_credential_session_class_and_separates_classes() {
    let pki = pki();
    let server = external_server(&pki, config());
    let client = external_client(&pki, config());
    let creds = credentials();
    let (one, two) = tokio::join!(
        client.pooled(target(&server), &creds, TrafficClass::Control, deadline()),
        client.pooled(target(&server), &creds, TrafficClass::Control, deadline())
    );
    let one = one.unwrap();
    let two = two.unwrap();
    assert!(Arc::ptr_eq(&one, &two));
    let produce = client
        .pooled(target(&server), &creds, TrafficClass::Produce, deadline())
        .await
        .unwrap();
    assert_ne!(
        one.connection().physical_id(),
        produce.connection().physical_id()
    );
    assert_eq!(
        one.connection().session_id(),
        produce.connection().session_id()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_lane_does_not_consume_a_future_requests_budget() {
    let pki = pki();
    let mut server = external_server(&pki, config());
    let client = external_client(&pki, config());
    let session = client
        .connect(
            target(&server),
            &credentials(),
            TrafficClass::Produce,
            deadline(),
        )
        .await
        .unwrap();
    let accepted = server.accept().await.unwrap();
    let mut outgoing = session.take_initial_streams().await.pop().unwrap();
    let mut incoming = accepted.take_initial_streams().await.pop().unwrap();
    let reader = tokio::spawn(async move { incoming.receiver.receive(deadline()).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut request = produce();
    request.remaining_timeout_ms = 50;
    outgoing.sender.send(&request, deadline()).await.unwrap();
    assert!(reader.await.unwrap().unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_clones_retain_memory_admission_until_last_drop() {
    let pki = pki();
    let grant = TokenGrant::new(
        TOKEN,
        "test".into(),
        "produce".into(),
        vec![FrameKind::ProduceBatch],
    )
    .unwrap();
    let memory = budget();
    let mut server = Server::bind(
        address(),
        ServerTls {
            identity: identity(&pki.server),
            client_trust_roots: None,
        },
        ServerAuthorization::External(vec![grant]),
        config(),
        memory.clone(),
    )
    .unwrap();
    let client = external_client(&pki, config());
    let session = client
        .connect(
            target(&server),
            &credentials(),
            TrafficClass::Produce,
            deadline(),
        )
        .await
        .unwrap();
    let accepted = server.accept().await.unwrap();
    let mut outgoing = session.take_initial_streams().await.pop().unwrap();
    let mut incoming = accepted.take_initial_streams().await.pop().unwrap();
    let before = memory.available_data_bytes();
    let sending = tokio::spawn(async move {
        outgoing.sender.send(&produce(), deadline()).await.unwrap();
        outgoing
    });
    let received = incoming
        .receiver
        .receive(deadline())
        .await
        .unwrap()
        .unwrap();
    let retained = received.frame().payload.clone();
    let charged = memory.available_data_bytes();
    assert!(charged < before);
    drop(received);
    assert_eq!(memory.available_data_bytes(), charged);
    drop(retained);
    assert_eq!(memory.available_data_bytes(), before);
    let _outgoing = sending.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_core_lane_does_not_block_its_sibling() {
    let pki = pki();
    let mut server = external_server(&pki, config());
    let client = external_client(&pki, config());
    let session = client
        .connect(
            target(&server),
            &credentials(),
            TrafficClass::Control,
            deadline(),
        )
        .await
        .unwrap();
    let accepted = server.accept().await.unwrap();
    let mut outgoing = session.take_initial_streams().await;
    let mut reset = outgoing.remove(0);
    let mut live = outgoing.pop().unwrap();
    let mut reset_peer = accepted.take_initial_streams().await.pop().unwrap();
    let mut live_peer = accepted.accept().await.unwrap();
    reset.sender.reset().unwrap();
    assert!(!session.connection().core_streams_ready());
    assert!(reset_peer.receiver.receive(deadline()).await.is_err());
    let request = frame(
        FrameKind::CreateTopic,
        TrafficClass::Control,
        Id::random().unwrap(),
        &CreateTopic {
            name: "sibling-progress".into(),
            partitions: 1,
        },
    );
    live.sender.send(&request, deadline()).await.unwrap();
    assert_eq!(
        live_peer
            .receiver
            .receive(deadline())
            .await
            .unwrap()
            .unwrap()
            .frame()
            .request_id,
        request.request_id
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn application_cancel_preserves_the_reusable_lane() {
    let pki = pki();
    let mut server = external_server(&pki, config());
    let client = external_client(&pki, config());
    let session = client
        .connect(
            target(&server),
            &credentials(),
            TrafficClass::Produce,
            deadline(),
        )
        .await
        .unwrap();
    let accepted = server.accept().await.unwrap();
    let mut outgoing = session.take_initial_streams().await.pop().unwrap();
    let mut incoming = accepted.take_initial_streams().await.pop().unwrap();
    let first = produce();
    let cancel = frame(
        FrameKind::Cancel,
        TrafficClass::Produce,
        Id::from_slice(&first.operation_id).unwrap(),
        &tidemq_protocol::common::CancelRequest {
            request_id: first.request_id.clone(),
        },
    );
    let next = produce();
    let sending = tokio::spawn(async move {
        for frame in [first, cancel, next] {
            outgoing.sender.send(&frame, deadline()).await.unwrap();
        }
        outgoing
    });
    for kind in [
        FrameKind::ProduceBatch,
        FrameKind::Cancel,
        FrameKind::ProduceBatch,
    ] {
        let received = incoming
            .receiver
            .receive(deadline())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.frame().kind, i32::from(kind));
    }
    let _outgoing = sending.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_saturation_allows_control_reauthentication_and_ack_progress() {
    let pki = pki();
    let memory = MemoryBudget::new(&MemoryConfig {
        total_bytes: 8_388_608,
        control_reserved_bytes: 4_194_304,
        ..MemoryConfig::default()
    })
    .unwrap();
    let grant = TokenGrant::new(
        TOKEN,
        "consumer".into(),
        "consume".into(),
        vec![FrameKind::Read, FrameKind::AckBatch],
    )
    .unwrap();
    let mut server = Server::bind(
        address(),
        ServerTls {
            identity: identity(&pki.server),
            client_trust_roots: None,
        },
        ServerAuthorization::External(vec![grant]),
        config(),
        memory.clone(),
    )
    .unwrap();
    let client = Client::bind(
        address(),
        ClientTls {
            trust_roots: Bytes::from(pki.ca.pem()),
            identity: None,
            expected_nodes: vec![],
        },
        config(),
        memory.clone(),
    )
    .unwrap();
    let creds = credentials();
    let delivery = client
        .connect(target(&server), &creds, TrafficClass::Delivery, deadline())
        .await
        .unwrap();
    let _delivery_peer = server.accept().await.unwrap();
    let normal_before = memory.available_normal_bytes();
    let old_control = client
        .connect(target(&server), &creds, TrafficClass::Control, deadline())
        .await
        .unwrap();
    let old_peer = server.accept().await.unwrap();
    old_control.close();
    old_peer.close();
    drop((old_control, old_peer));
    timeout(Duration::from_secs(1), async {
        while memory.available_normal_bytes() < normal_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let held_delivery_bytes = memory
        .try_reserve(
            TrafficClass::Delivery,
            u32::try_from(memory.available_data_bytes()).unwrap(),
        )
        .unwrap();
    assert_eq!(memory.available_data_bytes(), 0);
    assert!(memory.try_reserve(TrafficClass::Produce, 1).is_err());
    let control = client
        .connect(target(&server), &creds, TrafficClass::Control, deadline())
        .await
        .unwrap();
    let peer = server.accept().await.unwrap();
    let mut outgoing = control
        .connection()
        .open(StreamPurpose::Ack, deadline())
        .await
        .unwrap();
    let mut extra_core = Vec::new();
    let mut incoming = loop {
        let stream = peer.accept().await.unwrap();
        if stream.purpose() == StreamPurpose::Ack {
            break stream;
        }
        extra_core.push(stream);
    };
    let id = Id::random().unwrap();
    let ack = frame(
        FrameKind::AckBatch,
        TrafficClass::Control,
        id,
        &tidemq_protocol::public::AckBatch {
            subscription_id: Bytes::copy_from_slice(id.as_bytes()),
            partition_id: 0,
            cursor_generation: 1,
            assignment_generation: 1,
            delivery_tokens: vec![Bytes::from_static(b"bounded-delivery-token")],
            ..Default::default()
        },
    );
    outgoing.sender.send(&ack, deadline()).await.unwrap();
    assert_eq!(
        incoming
            .receiver
            .receive(deadline())
            .await
            .unwrap()
            .unwrap()
            .frame()
            .request_id,
        ack.request_id
    );
    // Application-owned backlog is released only after its ACK reaches the dispatcher.
    drop(held_delivery_bytes);
    assert!(memory.try_reserve(TrafficClass::Delivery, 1).is_ok());
    assert!(!delivery.connection().is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_normal_control_and_data_preserve_core_frame_memory() {
    let pki = pki();
    let memory = budget();
    let grant = TokenGrant::new(
        TOKEN,
        "core".into(),
        "control".into(),
        vec![FrameKind::CreateTopic],
    )
    .unwrap();
    let mut server = Server::bind(
        address(),
        ServerTls {
            identity: identity(&pki.server),
            client_trust_roots: None,
        },
        ServerAuthorization::External(vec![grant]),
        config(),
        memory.clone(),
    )
    .unwrap();
    let client = Client::bind(
        address(),
        ClientTls {
            trust_roots: Bytes::from(pki.ca.pem()),
            identity: None,
            expected_nodes: vec![],
        },
        config(),
        memory.clone(),
    )
    .unwrap();
    let session = client
        .connect(
            target(&server),
            &credentials(),
            TrafficClass::Control,
            deadline(),
        )
        .await
        .unwrap();
    let peer = server.accept().await.unwrap();
    let mut initial = session.take_initial_streams().await;
    let mut lane = initial.remove(0);
    let mut incoming = peer.take_initial_streams().await.pop().unwrap();
    let normal = memory
        .try_reserve(
            TrafficClass::Control,
            u32::try_from(memory.available_normal_bytes()).unwrap(),
        )
        .unwrap();
    assert!(memory.try_reserve(TrafficClass::Produce, 1).is_err());
    assert!(memory.try_reserve(TrafficClass::Control, 1).is_err());
    let request = frame(
        FrameKind::CreateTopic,
        TrafficClass::Control,
        Id::random().unwrap(),
        &CreateTopic {
            name: "reserved-core-memory".into(),
            partitions: 1,
        },
    );
    lane.sender.send(&request, deadline()).await.unwrap();
    assert_eq!(
        incoming
            .receiver
            .receive(deadline())
            .await
            .unwrap()
            .unwrap()
            .frame()
            .request_id,
        request.request_id
    );
    assert!(memory.available_core_bytes() > 0);
    drop(normal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_pin_rotation_preserves_exact_target_node_identity() {
    let pki = pki();
    let cluster = Id::random().unwrap();
    let old_server = internal_server(&pki, cluster);
    let new_server = Server::bind(
        address(),
        ServerTls {
            identity: identity(&pki.rotated_server),
            client_trust_roots: Some(Bytes::from(pki.ca.pem())),
        },
        ServerAuthorization::Internal(vec![node_grant(&pki.client, cluster, 2, "storage")]),
        config(),
        budget(),
    )
    .unwrap();
    let old_pin = node_grant(&pki.server, cluster, 1, "storage");
    let new_pin = node_grant(&pki.rotated_server, cluster, 1, "storage");
    let client = Client::bind(
        address(),
        ClientTls {
            trust_roots: Bytes::from(pki.ca.pem()),
            identity: Some(identity(&pki.client)),
            expected_nodes: vec![old_pin.clone(), new_pin.clone()],
        },
        config(),
        budget(),
    )
    .unwrap();
    for server in [&old_server, &new_server] {
        assert!(
            client
                .connect(
                    internal_target(server, cluster),
                    &internal_credentials(cluster, 2),
                    TrafficClass::Replication,
                    deadline()
                )
                .await
                .is_ok()
        );
    }
    let mut wrong_target = internal_target(&old_server, cluster);
    wrong_target.node_id = Some(3);
    assert!(
        client
            .connect(
                wrong_target,
                &internal_credentials(cluster, 2),
                TrafficClass::Replication,
                deadline()
            )
            .await
            .is_err()
    );
    let new_only = internal_client(&pki, new_pin);
    assert!(
        new_only
            .connect(
                internal_target(&old_server, cluster),
                &internal_credentials(cluster, 2),
                TrafficClass::Replication,
                deadline()
            )
            .await
            .is_err()
    );
    assert!(
        new_only
            .connect(
                internal_target(&new_server, cluster),
                &internal_credentials(cluster, 2),
                TrafficClass::Replication,
                deadline()
            )
            .await
            .is_ok()
    );
}
