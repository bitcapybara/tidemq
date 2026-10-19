use std::collections::BTreeMap;
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::time::Duration;

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use tidemq::config::{Config, Role, TlsConfig};
use tidemq::runtime::{GroupConfig, PeerConfig, RunningNode, RuntimeConfig};
use tidemq_sdk::{
    Bytes, Client, ClientConfig, ClientTls, Consumer, ConsumerConfig, Id, PeerTarget, Producer,
    public,
};
use tokio::time::Instant;

// Shared S3 fixtures include additional helpers exercised by their own integration suites.
#[allow(dead_code)]
#[path = "lifecycle_support/rustfs.rs"]
mod rustfs;
#[path = "s3_support/mod.rs"]
mod s3;

const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";

struct Fixture {
    directory: PathBuf,
    roots: Bytes,
    configs: BTreeMap<u64, Config>,
    running: BTreeMap<u64, RunningNode>,
}

impl Fixture {
    async fn new(nodes: &[(u64, Role)], metadata: &[u64], storage: &[u64]) -> Self {
        let directory =
            std::env::temp_dir().join(format!("tidemq-runtime-{}", Id::random().unwrap()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let ca_key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
        ];
        let ca = params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(params, ca_key);
        tokio::fs::write(directory.join("ca.pem"), ca.pem())
            .await
            .unwrap();
        tokio::fs::write(directory.join("access.toml"), format!("[[tokens]]\nvalue = \"{TOKEN}\"\nprincipal = \"runtime-test\"\nscope = \"*\"\noperations = [\"CREATE_TOPIC\", \"GET_TOPIC\", \"CREATE_SUBSCRIPTION\", \"GET_SUBSCRIPTION\", \"OPEN_PRODUCER\", \"PRODUCER_STATUS\", \"RETIRE_PRODUCER\", \"PRODUCE_BATCH\", \"READ\", \"ACK_BATCH\", \"RECEIVE_BARRIER\", \"CLAIM_ASSIGNMENT\", \"SEEK_SUBSCRIPTION\", \"DELETE_SUBSCRIPTION\", \"SUBSCRIPTION_STATUS\", \"CANCEL\"]\n")).await.unwrap();
        tokio::fs::write(directory.join("delivery.key"), [42_u8; 32])
            .await
            .unwrap();
        private_file(directory.join("access.toml")).await;
        private_file(directory.join("delivery.key")).await;
        let mut peers = Vec::new();
        for (node, role) in nodes {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(vec![format!("node-{node}.local")]).unwrap();
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ClientAuth,
                ExtendedKeyUsagePurpose::ServerAuth,
            ];
            let cert: Certificate = params.signed_by(&key, &issuer).unwrap();
            tokio::fs::write(directory.join(format!("node-{node}.pem")), cert.pem())
                .await
                .unwrap();
            tokio::fs::write(
                directory.join(format!("node-{node}.key")),
                key.serialize_pem(),
            )
            .await
            .unwrap();
            private_file(directory.join(format!("node-{node}.key"))).await;
            tokio::fs::write(directory.join(format!("node-{node}.der")), cert.der())
                .await
                .unwrap();
            peers.push(PeerConfig {
                node_id: *node,
                role: *role,
                zone: format!("zone-{}", node % 3),
                internal_address: udp_address(),
                server_name: format!("node-{node}.local"),
                certificate_der: directory.join(format!("node-{node}.der")),
            });
        }
        let cluster = Id::random().unwrap();
        let configs = nodes
            .iter()
            .map(|(node, role)| {
                let peer = peers.iter().find(|peer| peer.node_id == *node).unwrap();
                let mut config = Config {
                    node_id: *node,
                    cluster_id: cluster.to_string(),
                    role: *role,
                    listen: udp_address(),
                    management_listen: tcp_address(),
                    data_dir: directory.join(format!("data-{node}")),
                    replication_factor: metadata.len() as u8,
                    tls: TlsConfig {
                        certificate: directory.join(format!("node-{node}.pem")),
                        private_key: directory.join(format!("node-{node}.key")),
                        trust_roots: directory.join("ca.pem"),
                    },
                    runtime: RuntimeConfig {
                        internal_listen: peer.internal_address,
                        metadata_group: 1,
                        metadata_voters: metadata.to_vec(),
                        groups: vec![GroupConfig {
                            group_id: 2,
                            voters: storage.to_vec(),
                        }],
                        peers: peers.clone(),
                        access_file: directory.join("access.toml"),
                        delivery_key_file: directory.join("delivery.key"),
                        bootstrap: true,
                        objects: None,
                        ..RuntimeConfig::default()
                    },
                    ..Config::default()
                };
                config.memory.total_bytes = 128 * 1024 * 1024;
                config.memory.control_reserved_bytes = 32 * 1024 * 1024;
                (*node, config)
            })
            .collect();
        Self {
            directory,
            roots: ca.pem().into(),
            configs,
            running: BTreeMap::new(),
        }
    }

    async fn start_all(&mut self) {
        for (node, config) in &self.configs {
            self.running
                .insert(*node, RunningNode::start(config.clone()).await.unwrap());
        }
        self.wait_ready().await;
    }

    async fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let gateway = self
                .running
                .values()
                .find(|node| node.external_address.is_some())
                .unwrap();
            let reply = http(gateway.management_address, "/readyz").await;
            if reply.starts_with("HTTP/1.1 200") {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "Runtime became ready before the test deadline: {reply}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn client(&self, gateway: u64) -> Client {
        let config = self.configs.get(&gateway).unwrap();
        let address = self
            .running
            .get(&gateway)
            .unwrap()
            .external_address
            .unwrap();
        let mut sdk = ClientConfig::new(format!("node-{gateway}.local:{}", address.port()));
        sdk.request_timeout_ms = 10_000;
        Client::with_target(
            sdk,
            ClientTls {
                trust_roots: self.roots.clone(),
                identity: None,
                expected_nodes: Vec::new(),
            },
            PeerTarget {
                address,
                server_name: format!("node-{gateway}.local"),
                local_role: "sdk".into(),
                remote_role: config.role.name().into(),
                cluster_id: None,
                node_id: None,
            },
            TOKEN.into(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap()
    }

    async fn stop(&mut self, node: u64) {
        let running = self.running.remove(&node).unwrap();
        let address = running.internal_address;
        running.shutdown().await.unwrap();
        let started = Instant::now();
        loop {
            if UdpSocket::bind(address).is_ok() {
                eprintln!("Native UDP endpoint released after {:?}", started.elapsed());
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(6),
                "Native UDP endpoint retained its bound socket after shutdown"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    async fn restart(&mut self, node: u64) {
        self.running.insert(
            node,
            RunningNode::start(self.configs.get(&node).unwrap().clone())
                .await
                .unwrap(),
        );
        self.wait_ready().await;
    }
    async fn close(mut self) {
        for (_, node) in std::mem::take(&mut self.running) {
            node.shutdown().await.unwrap();
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn udp_address() -> SocketAddr {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap()
}
fn tcp_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

async fn http(address: SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut connection = tokio::net::TcpStream::connect(address).await.unwrap();
    connection
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    connection.read_to_string(&mut response).await.unwrap();
    response
}

async fn create(client: &Client) -> (Id, Id) {
    let topic = client
        .admin()
        .create_topic("events", 1, deadline())
        .await
        .unwrap();
    let topic = Id::from_slice(&topic.topic_id).unwrap();
    let subscription = client
        .admin()
        .create_subscription(
            topic,
            "durable",
            0,
            public::DeliveryMode::Parallel,
            deadline(),
        )
        .await
        .unwrap();
    (
        topic,
        Id::from_slice(&subscription.subscription_id).unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf1_sdk_produce_consume_durable_ack_survives_restart() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let (topic, subscription) = create(&client).await;
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap(), deadline())
        .await
        .unwrap();
    let receipt = producer
        .produce(
            vec![public::Message {
                key: Bytes::from_static(b"key"),
                body: Bytes::from_static(b"durable-body"),
            }],
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(receipt.first_offset, 0);
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig::default(),
        deadline(),
    )
    .await
    .unwrap();
    assert_eq!(consumer.fill(deadline()).await.unwrap(), 1);
    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(delivery.body(), b"durable-body");
    assert_eq!(
        consumer
            .ack(&delivery, deadline())
            .await
            .unwrap()
            .acknowledged_before,
        1
    );
    let metrics = http(
        fixture.running.get(&1).unwrap().management_address,
        "/metrics",
    )
    .await;
    assert!(metrics.contains("tidemq_producer_new_messages_total 1"));
    assert_eq!(metrics.matches("# EOF").count(), 1);
    let diagnostics = http(
        fixture.running.get(&1).unwrap().management_address,
        "/diagnostics",
    )
    .await;
    assert!(!diagnostics.contains(TOKEN));
    assert!(!diagnostics.contains("durable-body"));
    client.close(deadline()).await;
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.stop(1).await;
    fixture.restart(1).await;
    let client = fixture.client(1).await;
    let status = client
        .admin()
        .subscription_status(topic, 0, subscription, deadline())
        .await
        .unwrap();
    assert_eq!(status.assignment.unwrap().acknowledged_before, 1);
    client.close(deadline()).await;
    drop(client);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rf3_separate_metadata_storage_broker_and_gateways_use_quic_semantics() {
    let nodes = [
        (11, Role::Metadata),
        (12, Role::Metadata),
        (13, Role::Metadata),
        (21, Role::Storage),
        (22, Role::Storage),
        (23, Role::Storage),
        (31, Role::Broker),
        (41, Role::Gateway),
        (42, Role::Gateway),
    ];
    let mut fixture = Fixture::new(&nodes, &[11, 12, 13], &[21, 22, 23]).await;
    fixture.start_all().await;
    let client = fixture.client(41).await;
    let (topic, subscription) = create(&client).await;
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap(), deadline())
        .await
        .unwrap();
    let mut work = tokio::task::JoinSet::new();
    for value in 0..8_u8 {
        let producer = producer.clone();
        work.spawn(async move {
            producer
                .produce(
                    vec![public::Message {
                        key: Bytes::from_static(b"key"),
                        body: Bytes::from(vec![value]),
                    }],
                    deadline(),
                )
                .await
                .unwrap()
        });
    }
    let mut offsets = Vec::new();
    while let Some(result) = work.join_next().await {
        offsets.push(result.unwrap().first_offset);
    }
    offsets.sort_unstable();
    assert_eq!(offsets, (0..8).collect::<Vec<_>>());
    let second_client = fixture.client(42).await;
    let consumer = Consumer::claim(
        second_client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig::default(),
        deadline(),
    )
    .await
    .unwrap();
    assert_eq!(consumer.fill(deadline()).await.unwrap(), 8);
    for offset in 0..8 {
        let delivery = consumer.next().await.unwrap().unwrap();
        assert_eq!(delivery.offset(), offset);
        consumer.ack(&delivery, deadline()).await.unwrap();
    }
    let status = client
        .admin()
        .subscription_status(topic, 0, subscription, deadline())
        .await
        .unwrap();
    assert_eq!(status.assignment.unwrap().acknowledged_before, 8);
    second_client.close(deadline()).await;
    client.close(deadline()).await;
    drop(consumer);
    drop(producer);
    drop(second_client);
    drop(client);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the verified RustFS 1.0.3 executable in TIDEMQ_TEST_RUSTFS_BIN"]
async fn actual_runtime_sdk_replays_s3_after_published_local_reclamation() {
    let rustfs = rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    let credentials_file = fixture.directory.join("object-credentials.toml");
    let (access, secret) = rustfs.credentials();
    tokio::fs::write(
        &credentials_file,
        format!("access_key = \"{access}\"\nsecret_key = \"{secret}\"\n"),
    )
    .await
    .unwrap();
    private_file(credentials_file.clone()).await;
    fixture.configs.get_mut(&1).unwrap().runtime.objects = Some(tidemq::runtime::ObjectConfig {
        endpoint: rustfs.endpoint().into(),
        region: "us-east-1".into(),
        bucket: "tidemq-sdk-test".into(),
        prefix: "runtime-test".into(),
        credentials_file,
        allow_http_loopback: true,
    });
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let (topic, subscription) = create(&client).await;
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap(), deadline())
        .await
        .unwrap();
    producer
        .produce(
            vec![public::Message {
                key: Bytes::from_static(b"cold-key"),
                body: Bytes::from_static(b"cold-body"),
            }],
            deadline(),
        )
        .await
        .unwrap();
    let partition = tidemq::metadata::PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    fixture
        .running
        .get(&1)
        .unwrap()
        .archive(
            2,
            vec![tidemq::lifecycle::RangeRequest {
                partition,
                offsets: tidemq_protocol::state::OffsetRange::new(0, 1).unwrap(),
            }],
            deadline(),
        )
        .await
        .unwrap();
    fixture
        .running
        .get(&1)
        .unwrap()
        .reclaim_local(partition, 1, deadline())
        .await
        .unwrap();
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig::default(),
        deadline(),
    )
    .await
    .unwrap();
    assert_eq!(consumer.fill(deadline()).await.unwrap(), 1);
    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(delivery.body(), b"cold-body");
    assert_eq!(
        consumer
            .ack(&delivery, deadline())
            .await
            .unwrap()
            .acknowledged_before,
        1
    );
    let metrics = http(
        fixture.running.get(&1).unwrap().management_address,
        "/metrics",
    )
    .await;
    assert_eq!(metrics.matches("# EOF").count(), 1);
    assert!(metrics.contains("tidemq_lifecycle_published_total 1"));
    client.close(deadline()).await;
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.close().await;
}

async fn private_file(path: PathBuf) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .await
            .unwrap();
    }
    #[cfg(not(unix))]
    let _ = path;
}
