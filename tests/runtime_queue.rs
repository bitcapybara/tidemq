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
                            standby: Vec::new(),
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
                config.memory.total_bytes = 192 * 1024 * 1024;
                config.memory.control_reserved_bytes = 32 * 1024 * 1024;
                // Several independently budgeted native runtimes share this test process's RSS.
                config.runtime.capacity.process_memory = tidemq::quota::Watermark {
                    low: 1_610_612_736,
                    high: 2_147_483_648,
                };
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
            let mut ready = true;
            for node in self.running.values() {
                ready &= http(node.management_address, "/readyz")
                    .await
                    .starts_with("HTTP/1.1 200");
            }
            if ready {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "Not all runtime roles obtained fresh readiness before the deadline"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn client(&self, gateway: u64) -> Client {
        let config = self.configs.get(&gateway).unwrap();
        let address = config.listen;
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
        running.shutdown().await.unwrap();
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
        if std::thread::panicking() {
            eprintln!(
                "Preserved runtime evidence directory {}",
                self.directory.display()
            );
            return;
        }
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

async fn spawn_process(fixture: &Fixture, node: u64) -> tokio::process::Child {
    let config = fixture.configs.get(&node).unwrap();
    let path = fixture.directory.join(format!("process-{node}.toml"));
    tokio::fs::write(&path, toml::to_string(config).unwrap())
        .await
        .unwrap();
    let output = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(fixture.directory.join(format!("process-{node}.log")))
        .unwrap();
    tokio::process::Command::new(env!("CARGO_BIN_EXE_tidemq"))
        .arg("serve")
        .arg("--config")
        .arg(&path)
        .stdout(output)
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}

async fn start_process(fixture: &Fixture, node: u64) -> tokio::process::Child {
    let mut child = spawn_process(fixture, node).await;
    wait_process(fixture, node, &mut child).await;
    child
}

async fn wait_process(fixture: &Fixture, node: u64, child: &mut tokio::process::Child) {
    let config = fixture.configs.get(&node).unwrap();
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            child.try_wait().unwrap().is_none(),
            "Runtime process exited during startup"
        );
        let attempt = tokio::time::timeout(Duration::from_millis(200), async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut stream = tokio::net::TcpStream::connect(config.management_listen).await?;
            stream
                .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await?;
            let mut output = String::new();
            stream.take(8192).read_to_string(&mut output).await?;
            Ok::<_, std::io::Error>(output)
        })
        .await;
        if matches!(attempt, Ok(Ok(ref response)) if response.starts_with("HTTP/1.1 200")) {
            return;
        }
        assert!(
            Instant::now() < until,
            "Runtime process did not become ready within its bound"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_crash_restart_preserves_original_sdk_producer_batch_and_consumer() {
    let fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    let mut process = start_process(&fixture, 1).await;
    let client = fixture.client(1).await;
    let (topic, subscription) = create(&client).await;
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap(), deadline())
        .await
        .unwrap();
    let message = || public::Message {
        key: Bytes::from_static(b"process-key"),
        body: Bytes::from_static(b"process-body"),
    };
    producer.produce(vec![message()], deadline()).await.unwrap();
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
    let first = consumer.next().await.unwrap().unwrap();
    assert_eq!(
        consumer
            .ack(&first, deadline())
            .await
            .unwrap()
            .acknowledged_before,
        1
    );
    process.kill().await.unwrap();
    assert!(!process.wait().await.unwrap().success());
    let failure = producer
        .produce(vec![message()], Instant::now() + Duration::from_millis(200))
        .await
        .unwrap_err();
    assert!(failure.outcome_unknown());
    let batches = producer.pending_batches().await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = batches.first().unwrap();
    let identity = (
        batch.operation_id(),
        batch.producer_epoch(),
        batch.first_sequence(),
        batch.digest().to_vec(),
    );
    let started = Instant::now();
    let mut process = start_process(&fixture, 1).await;
    let receipt = producer.retry(batch, deadline()).await.unwrap();
    assert_eq!(receipt.first_offset, 1);
    assert_eq!(
        identity,
        (
            batch.operation_id(),
            batch.producer_epoch(),
            batch.first_sequence(),
            batch.digest().to_vec()
        )
    );
    assert!(producer.pending_batches().await.unwrap().is_empty());
    assert_eq!(consumer.fill(deadline()).await.unwrap(), 1);
    let second = consumer.next().await.unwrap().unwrap();
    assert_eq!(second.offset(), 1);
    assert_eq!(
        consumer
            .ack(&second, deadline())
            .await
            .unwrap()
            .acknowledged_before,
        2
    );
    eprintln!(
        "Original SDK producer and consumer recovered after process restart in {:?}",
        started.elapsed()
    );
    client.close(deadline()).await;
    process.kill().await.unwrap();
    let _ = process.wait().await.unwrap();
    fixture.close().await;
}

async fn durable_produce(producer: &Producer, body: &'static [u8]) -> u64 {
    let until = Instant::now() + Duration::from_secs(20);
    if let Ok(receipt) = producer
        .produce(
            vec![public::Message {
                key: Bytes::new(),
                body: Bytes::from_static(body),
            }],
            Instant::now() + Duration::from_secs(2),
        )
        .await
    {
        return receipt.first_offset;
    }
    let pending = producer.pending_batches().await.unwrap();
    assert_eq!(pending.len(), 1);
    let batch = pending.first().unwrap();
    let identity = (
        batch.operation_id(),
        batch.producer_epoch(),
        batch.first_sequence(),
        batch.digest().to_vec(),
    );
    loop {
        let result = producer
            .retry(batch, until.min(Instant::now() + Duration::from_secs(2)))
            .await;
        if let Ok(receipt) = result {
            assert_eq!(
                identity,
                (
                    batch.operation_id(),
                    batch.producer_epoch(),
                    batch.first_sequence(),
                    batch.digest().to_vec()
                )
            );
            return receipt.first_offset;
        }
        assert!(
            Instant::now() < until,
            "Original producer identity could not recover: {result:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn durable_consume(consumer: &Consumer, offset: u64) {
    let until = Instant::now() + Duration::from_secs(20);
    let delivery = loop {
        if let Some(delivery) = consumer.next().await.unwrap() {
            break delivery;
        }
        let _ = consumer
            .fill(until.min(Instant::now() + Duration::from_secs(2)))
            .await;
        assert!(
            Instant::now() < until,
            "Original consumer could not resume after data leader recovery"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(delivery.offset(), offset);
    assert_eq!(
        consumer
            .ack(&delivery, until)
            .await
            .unwrap()
            .acknowledged_before,
        offset + 1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_rf3_processes_preserve_acks_and_stop_receipts_without_quorum() {
    let nodes = [
        (11, Role::Metadata),
        (12, Role::Metadata),
        (13, Role::Metadata),
        (21, Role::Storage),
        (22, Role::Storage),
        (23, Role::Storage),
        (31, Role::Broker),
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
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap(), deadline())
        .await
        .unwrap();
    for offset in 0..4 {
        assert_eq!(
            durable_produce(&producer, b"quorum-persisted").await,
            offset
        );
    }
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
    for offset in 0..4 {
        durable_consume(&consumer, offset).await;
    }
    let diagnostics = http(
        fixture.configs.get(&21).unwrap().management_listen,
        "/diagnostics",
    )
    .await;
    let diagnostics: serde_json::Value =
        serde_json::from_str(diagnostics.split_once("\r\n\r\n").unwrap().1).unwrap();
    let leader = diagnostics
        .get("data_groups")
        .unwrap()
        .as_array()
        .unwrap()
        .first()
        .unwrap()
        .get("leader")
        .unwrap()
        .as_u64()
        .unwrap();
    processes.get_mut(&leader).unwrap().kill().await.unwrap();
    let single_failure = Instant::now();
    assert_eq!(
        durable_produce(&producer, b"after-one-storage-crash").await,
        4
    );
    durable_consume(&consumer, 4).await;
    eprintln!(
        "Independent RF3 single-storage recovery completed in {:?}",
        single_failure.elapsed()
    );
    let second = [21, 22, 23]
        .into_iter()
        .find(|node| *node != leader)
        .unwrap();
    processes.get_mut(&second).unwrap().kill().await.unwrap();
    let result = producer
        .produce(
            vec![public::Message {
                key: Bytes::new(),
                body: Bytes::from_static(b"minority-unknown"),
            }],
            Instant::now() + Duration::from_secs(1),
        )
        .await;
    assert!(
        result.is_err(),
        "A data minority must never issue a successful producer receipt"
    );
    let pending = producer.pending_batches().await.unwrap();
    assert_eq!(pending.len(), 1);
    let original = pending.first().unwrap();
    let identity = (
        original.operation_id(),
        original.producer_epoch(),
        original.first_sequence(),
        original.digest().to_vec(),
    );
    for node in [leader, second] {
        processes.insert(node, spawn_process(&fixture, node).await);
    }
    for node in [leader, second] {
        wait_process(&fixture, node, processes.get_mut(&node).unwrap()).await;
    }
    let status = client
        .admin()
        .subscription_status(topic, 0, subscription, deadline())
        .await
        .unwrap();
    assert_eq!(status.assignment.unwrap().acknowledged_before, 5);
    let receipt = producer.retry(original, deadline()).await.unwrap();
    assert_eq!(receipt.first_offset, 5);
    assert_eq!(
        identity,
        (
            original.operation_id(),
            original.producer_epoch(),
            original.first_sequence(),
            original.digest().to_vec()
        )
    );
    durable_consume(&consumer, 5).await;
    client.close(deadline()).await;
    for process in processes.values_mut() {
        process.kill().await.unwrap();
        let _ = process.wait().await.unwrap();
    }
    fixture.close().await;
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
    for operation in ["Read", "Ack", "Control"] {
        let prefix =
            format!("tidemq_operations_total{{operation=\"{operation}\",outcome=\"Committed\"}} ");
        assert!(
            metrics
                .lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > 0
        );
        assert!(
            metrics.contains(&format!("stage=\"Total\",operation=\"{operation}\""))
                || metrics.contains(&format!("operation=\"{operation}\",stage=\"Total\""))
        );
    }
    for stage in ["Grant", "Fence", "Recover", "Activate"] {
        let prefix = format!(
            "tidemq_ownership_transitions_total{{stage=\"{stage}\",outcome=\"Committed\"}} "
        );
        assert!(
            metrics
                .lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > 0
        );
    }
    assert!(
        metrics
            .lines()
            .filter(
                |line| line.starts_with("tidemq_stage_duration_seconds_count")
                    && line.contains("stage=\"Sync\"")
            )
            .any(|line| line
                .split_whitespace()
                .last()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > 0)
    );

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
        (32, Role::Broker),
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
    let control = fixture.running.get(&11).unwrap();
    let partition = tidemq::metadata::PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let previous = control.route(partition, deadline()).await.unwrap();
    let moved = control
        .takeover(
            partition,
            32,
            previous.epoch,
            previous.generation,
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(moved.owner_node, 32);
    assert_eq!(moved.epoch, previous.epoch + 1);
    assert_eq!(
        producer
            .produce(
                vec![public::Message {
                    key: Bytes::new(),
                    body: Bytes::from_static(b"after-handoff")
                }],
                deadline()
            )
            .await
            .unwrap()
            .first_offset,
        8
    );
    assert_eq!(consumer.fill(deadline()).await.unwrap(), 1);
    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(delivery.offset(), 8);
    assert_eq!(
        consumer
            .ack(&delivery, deadline())
            .await
            .unwrap()
            .acknowledged_before,
        9
    );
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
    run_runtime_s3(&[(1, Role::Combined)], &[1], &[1]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires the verified RustFS 1.0.3 executable in TIDEMQ_TEST_RUSTFS_BIN"]
async fn actual_runtime_sdk_rf3_separate_leaders_archive_and_replay_s3() {
    run_runtime_s3(
        &[
            (11, Role::Metadata),
            (12, Role::Metadata),
            (13, Role::Metadata),
            (21, Role::Storage),
            (22, Role::Storage),
            (23, Role::Storage),
            (31, Role::Broker),
            (41, Role::Gateway),
        ],
        &[11, 12, 13],
        &[21, 22, 23],
    )
    .await;
}

async fn run_runtime_s3(nodes: &[(u64, Role)], metadata: &[u64], storage: &[u64]) {
    let rustfs = rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = Fixture::new(nodes, metadata, storage).await;
    let credentials_file = fixture.directory.join("object-credentials.toml");
    let (access, secret) = rustfs.credentials();
    tokio::fs::write(
        &credentials_file,
        format!("access_key = \"{access}\"\nsecret_key = \"{secret}\"\n"),
    )
    .await
    .unwrap();
    private_file(credentials_file.clone()).await;
    let objects = tidemq::runtime::ObjectConfig {
        endpoint: rustfs.endpoint().into(),
        region: "us-east-1".into(),
        bucket: "tidemq-sdk-test".into(),
        prefix: "runtime-test".into(),
        credentials_file,
        allow_http_loopback: true,
    };
    for config in fixture
        .configs
        .values_mut()
        .filter(|config| config.role.stores_data())
    {
        config.runtime.objects = Some(objects.clone());
    }
    let gateway = nodes
        .iter()
        .find(|(_, role)| role.serves_gateway())
        .unwrap()
        .0;
    fixture.start_all().await;
    let client = fixture.client(gateway).await;
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
    let until = deadline();
    loop {
        let metrics = http(
            fixture.running.get(&gateway).unwrap().management_address,
            "/metrics",
        )
        .await;
        let reclaimed = metrics
            .lines()
            .find(|line| line.starts_with("tidemq_lifecycle_reclaimed_bytes_total "))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        // Gateways export process-local counters; inspect the actual data Leader's coordinator.
        let mut reclaimed = reclaimed;
        for node in storage {
            let metrics = http(
                fixture.running.get(node).unwrap().management_address,
                "/metrics",
            )
            .await;
            reclaimed = reclaimed.max(
                metrics
                    .lines()
                    .find(|line| line.starts_with("tidemq_lifecycle_reclaimed_bytes_total "))
                    .and_then(|line| line.split_whitespace().nth(1))
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0),
            );
        }
        if reclaimed > 0 {
            break;
        }
        assert!(
            Instant::now() < until,
            "Default background archive did not publish, checkpoint and reclaim in time"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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
        fixture.running.get(&gateway).unwrap().management_address,
        "/metrics",
    )
    .await;
    assert_eq!(metrics.matches("# EOF").count(), 1);
    let mut published = false;
    for node in storage {
        published |= http(
            fixture.running.get(node).unwrap().management_address,
            "/metrics",
        )
        .await
        .contains("tidemq_lifecycle_published_total 1");
    }
    assert!(published);
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
