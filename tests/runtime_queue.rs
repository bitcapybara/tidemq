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
#[path = "runtime_support/udp_proxy.rs"]
mod udp_proxy;

use tidemq_sdk::{
    Bytes, Client, ClientConfig, ClientTls, Consumer, ConsumerConfig, Id, PeerTarget, Producer,
    public,
};
use tokio::time::Instant;

#[path = "runtime_support/broker_failover.rs"]
mod broker_failover;
#[path = "runtime_support/read_batches.rs"]
mod read_batches;
#[path = "runtime_support/tracing.rs"]
mod tracing_tests;

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
            let mut names = vec![format!("node-{node}.local")];
            if role.serves_gateway() {
                names.push("queue.local".into());
            }
            let mut params = CertificateParams::new(names).unwrap();
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
    use std::{
        collections::BTreeSet,
        os::fd::AsRawFd,
        sync::{Mutex, OnceLock},
    };
    struct Ports {
        file: std::fs::File,
        ephemeral: (u16, u16),
        next: u32,
        used: BTreeSet<u16>,
    }
    static PORTS: OnceLock<Mutex<Ports>> = OnceLock::new();
    let ports = PORTS.get_or_init(|| {
        #[cfg(target_os = "macos")]
        let ephemeral = {
            let read = |name| {
                let output = std::process::Command::new("sysctl")
                    .args(["-n", name])
                    .output()
                    .unwrap();
                assert!(output.status.success());
                String::from_utf8(output.stdout)
                    .unwrap()
                    .trim()
                    .parse::<u16>()
                    .unwrap()
            };
            (
                read("net.inet.ip.portrange.first"),
                read("net.inet.ip.portrange.last"),
            )
        };
        #[cfg(target_os = "linux")]
        let ephemeral = {
            let text = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").unwrap();
            let mut fields = text
                .split_ascii_whitespace()
                .map(|value| value.parse::<u16>().unwrap());
            (fields.next().unwrap(), fields.next().unwrap())
        };
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        compile_error!("Native runtime tests require a supported ephemeral-port observation");
        assert!(ephemeral.0 <= ephemeral.1);
        let seed = Id::random().unwrap();
        Mutex::new(Ports {
            file: std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(std::env::temp_dir().join("tidemq-runtime-ports.lock"))
                .unwrap(),
            ephemeral,
            next: u32::from(u16::from_le_bytes([seed.as_bytes()[0], seed.as_bytes()[1]])),
            used: BTreeSet::new(),
        })
    });
    let mut ports = ports.lock().unwrap();
    for _ in 0..64_512 {
        let port = 1024 + (ports.next % 64_512) as u16;
        ports.next = ports.next.wrapping_add(1);
        if (ports.ephemeral.0..=ports.ephemeral.1).contains(&port) || ports.used.contains(&port) {
            continue;
        }
        let Ok(socket) = UdpSocket::bind(("127.0.0.1", port)) else {
            continue;
        };
        // One open file retains cross-process port reservations without one descriptor per port.
        let mut reservation = std::mem::MaybeUninit::<libc::flock>::zeroed();
        // All fields are integer ABI values and zero is valid for the remaining platform fields.
        let reservation = unsafe {
            let value = reservation.assume_init_mut();
            value.l_type = libc::F_WRLCK as _;
            value.l_whence = libc::SEEK_SET as _;
            value.l_start = i64::from(port);
            value.l_len = 1;
            value
        };
        // The owned descriptor and initialized structure follow the pinned libc fcntl ABI.
        if unsafe { libc::fcntl(ports.file.as_raw_fd(), libc::F_SETLK, reservation) } != 0 {
            let error = std::io::Error::last_os_error();
            assert!(
                matches!(error.raw_os_error(), Some(libc::EACCES | libc::EAGAIN)),
                "Port reservation failed: {error}"
            );
            continue;
        }
        ports.used.insert(port);
        return socket.local_addr().unwrap();
    }
    panic!("No test UDP port is available outside the observed ephemeral range");
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
        .env("TOKIO_WORKER_THREADS", "4")
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
    durable_retry(producer, batch, until).await
}

async fn durable_retry(
    producer: &Producer,
    batch: &tidemq_sdk::PendingBatch,
    until: Instant,
) -> u64 {
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
    let mut previous = String::new();
    let delivery = loop {
        let result = consumer
            .recv(until.min(Instant::now() + Duration::from_secs(2)))
            .await;
        if let Ok(Some(delivery)) = result {
            break delivery;
        }
        let detail = format!("{result:?}");
        if previous != detail {
            eprintln!("Consumer recovery for offset {offset}: {detail}");
            previous = detail;
        }
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
    let until = deadline();
    let status = loop {
        let status = client
            .admin()
            .subscription_status(
                topic,
                0,
                subscription,
                until.min(Instant::now() + Duration::from_secs(2)),
            )
            .await;
        if let Ok(status) = status {
            break status;
        }
        assert!(
            Instant::now() < until,
            "Recovered data quorum did not return a fresh subscription status: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(status.assignment.unwrap().acknowledged_before, 5);
    let retry_deadline = deadline();
    let receipt = loop {
        assert!(
            Instant::now() < retry_deadline,
            "Original pending batch did not recover within its original deadline"
        );
        match producer
            .retry(
                original,
                retry_deadline.min(Instant::now() + Duration::from_secs(2)),
            )
            .await
        {
            Ok(receipt) => break receipt,
            Err(error) => {
                let source = match &error {
                    tidemq_sdk::Error::ProduceUnknown { source } => source.as_ref(),
                    error => error,
                };
                assert!(
                    source.outcome_unknown()
                        && !matches!(
                            source,
                            tidemq_sdk::Error::Remote {
                                retryable: false,
                                ..
                            }
                        ),
                    "Original pending batch received a terminal retry error: {error:?}"
                );
            }
        }
        assert_eq!(
            identity,
            (
                original.operation_id(),
                original.producer_epoch(),
                original.first_sequence(),
                original.digest().to_vec()
            )
        );
        tokio::time::sleep_until(retry_deadline.min(Instant::now() + Duration::from_millis(25)))
            .await;
    };
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
    assert!(
        metrics.starts_with("HTTP/1.1 200"),
        "Management response was empty or failed: {} bytes",
        metrics.len()
    );
    assert!(metrics.len() <= 262_144 + 1024);
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
    for node in [21, 22, 23] {
        let cached = fixture
            .running
            .get(&node)
            .unwrap()
            .route(partition, deadline())
            .await
            .unwrap();
        assert_eq!(cached.owner_node, previous.owner_node);
    }
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
    let blockers = control.drain_node(12, &[], deadline()).await.unwrap();
    assert!(blockers.metadata_role);
    assert!(!blockers.durable_state_drained());
    let drained = control.drain_node(31, &[], deadline()).await.unwrap();
    assert!(drained.durable_state_drained());
    fixture
        .running
        .get(&31)
        .unwrap()
        .wait_for_shutdown_request()
        .await;
    second_client.close(deadline()).await;
    client.close(deadline()).await;
    drop(consumer);
    drop(producer);
    drop(second_client);
    drop(client);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stable_udp_endpoint_recovers_original_sdk_after_gateway_crash_with_network_faults() {
    static BODY: [u8; 32 * 1024] = [b'p'; 32 * 1024];
    let mut fixture = Fixture::new(
        &[
            (1, Role::Combined),
            (41, Role::Gateway),
            (42, Role::Gateway),
        ],
        &[1],
        &[1],
    )
    .await;
    fixture
        .configs
        .get_mut(&41)
        .unwrap()
        .runtime
        .capacity
        .process_memory = tidemq::quota::Limits::default().process_memory;
    for (node, config) in &fixture.configs {
        if *node != 41 {
            fixture
                .running
                .insert(*node, RunningNode::start(config.clone()).await.unwrap());
        }
    }
    fixture.wait_ready().await;
    let mut first_gateway = start_process(&fixture, 41).await;
    let proxy = udp_proxy::Proxy::start([
        fixture.configs.get(&41).unwrap().listen,
        fixture.configs.get(&42).unwrap().listen,
    ])
    .await;
    let mut sdk = ClientConfig::new(format!("queue.local:{}", proxy.address.port()));
    sdk.request_timeout_ms = 20_000;
    let client = Client::with_target(
        sdk,
        ClientTls {
            trust_roots: fixture.roots.clone(),
            identity: None,
            expected_nodes: Vec::new(),
        },
        PeerTarget {
            address: proxy.address,
            server_name: "queue.local".into(),
            local_role: "sdk".into(),
            remote_role: "gateway".into(),
            cluster_id: None,
            node_id: None,
        },
        TOKEN.into(),
        deadline(),
    )
    .await
    .unwrap();
    let (topic, subscription) = create(&client).await;
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap(), deadline())
        .await
        .unwrap();
    for offset in 0..3 {
        assert_eq!(durable_produce(&producer, &BODY).await, offset);
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
    for offset in 0..3 {
        durable_consume(&consumer, offset).await;
    }
    let connections = client.transport_metrics().snapshot().connections_total;
    first_gateway.kill().await.unwrap();
    let _ = first_gateway.wait().await.unwrap();
    proxy.switch();
    proxy.set_mtu(1199);
    let started = Instant::now();
    let blocked = producer
        .produce(
            vec![public::Message {
                key: Bytes::new(),
                body: Bytes::from_static(b"after-gateway-crash"),
            }],
            Instant::now() + Duration::from_secs(4),
        )
        .await;
    assert!(
        blocked.is_err(),
        "The MTU blackhole must not issue a successful receipt"
    );
    let pending = producer.pending_batches().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert!(
        proxy.snapshot().over_mtu > 0,
        "The blackhole must reject actual QUIC datagrams"
    );
    proxy.set_mtu(1200);
    assert_eq!(
        durable_retry(&producer, pending.first().unwrap(), deadline()).await,
        3
    );
    durable_consume(&consumer, 3).await;
    let observed = proxy.snapshot();
    assert!(
        observed.packets > 0
            && observed.lost > 0
            && observed.reordered > 0
            && observed.over_mtu > 0,
        "Every configured fault must have affected real traffic: {observed:?}"
    );
    assert!(
        client.transport_metrics().snapshot().connections_total > connections,
        "The original SDK must establish a new physical QUIC connection through the stable endpoint"
    );
    eprintln!(
        "Stable UDP endpoint recovered original SDK after Gateway SIGKILL in {:?}; actual datagram faults: {observed:?}",
        started.elapsed()
    );
    client.close(deadline()).await;
    drop(consumer);
    drop(producer);
    drop(client);
    proxy.close().await;
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

async fn enable_objects(fixture: &mut Fixture, rustfs: &rustfs::RustFs) {
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
}

async fn wait_runtime_counter(fixture: &Fixture, node: u64, name: &str) -> String {
    let until = deadline();
    loop {
        let metrics = http(
            fixture.running.get(&node).unwrap().management_address,
            "/metrics",
        )
        .await;
        let value = metrics
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(0);
        if value > 0 {
            return metrics;
        }
        assert!(
            Instant::now() < until,
            "Automatic runtime did not advance {name}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the verified RustFS 1.0.3 executable in TIDEMQ_TEST_RUSTFS_BIN"]
async fn actual_runtime_sdk_automatically_compacts_partial_cold_objects_and_reclaims_sources() {
    let rustfs = rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    enable_objects(&mut fixture, &rustfs).await;
    let config = fixture.configs.get_mut(&1).unwrap();
    config.runtime.archive_interval_ms = 4000;
    config.runtime.retention.minimum_age_ms = 0;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let (topic, subscription) = create(&client).await;
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap(), deadline())
        .await
        .unwrap();
    assert_eq!(durable_produce(&producer, b"expired-prefix").await, 0);
    let bucket = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    while std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        == bucket
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(durable_produce(&producer, b"retained-suffix").await, 1);
    let archived =
        wait_runtime_counter(&fixture, 1, "tidemq_lifecycle_reclaimed_bytes_total ").await;
    assert!(
        archived.contains("tidemq_lifecycle_published_total 1"),
        "Both time buckets must share the first automatic object"
    );
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig {
            max_buffered_messages: 1,
            ..ConsumerConfig::default()
        },
        deadline(),
    )
    .await
    .unwrap();
    durable_consume(&consumer, 0).await;
    wait_runtime_counter(&fixture, 1, "tidemq_lifecycle_compacted_total ").await;
    wait_runtime_counter(&fixture, 1, "tidemq_lifecycle_deleted_total ").await;
    assert_eq!(consumer.fill(deadline()).await.unwrap(), 1);
    let retained = consumer.next().await.unwrap().unwrap();
    assert_eq!(retained.offset(), 1);
    assert_eq!(retained.body(), b"retained-suffix");
    assert_eq!(
        consumer
            .ack(&retained, deadline())
            .await
            .unwrap()
            .acknowledged_before,
        2
    );
    client.close(deadline()).await;
    drop(retained);
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.close().await;
}

async fn run_runtime_s3(nodes: &[(u64, Role)], metadata: &[u64], storage: &[u64]) {
    let rustfs = rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = Fixture::new(nodes, metadata, storage).await;
    enable_objects(&mut fixture, &rustfs).await;
    if nodes.len() == 1 {
        for config in fixture.configs.values_mut() {
            config.runtime.groups.push(GroupConfig {
                group_id: 3,
                voters: storage.to_vec(),
                standby: Vec::new(),
            });
        }
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
            vec![
                public::Message {
                    key: Bytes::from_static(b"cold-key"),
                    body: Bytes::from_static(b"cold-body"),
                };
                33
            ],
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
        if reclaimed >= 33 * 17 {
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
    for offset in 0..33 {
        let delivery = consumer.recv(deadline()).await.unwrap().unwrap();
        assert_eq!(delivery.offset(), offset);
        assert_eq!(delivery.body(), b"cold-body");
        assert_eq!(
            consumer
                .ack(&delivery, deadline())
                .await
                .unwrap()
                .acknowledged_before,
            offset + 1
        );
    }
    let metrics = http(
        fixture.running.get(&gateway).unwrap().management_address,
        "/metrics",
    )
    .await;
    assert_eq!(metrics.matches("# EOF").count(), 1);
    let mut published = 0_u64;
    for node in storage {
        let metrics = http(
            fixture.running.get(node).unwrap().management_address,
            "/metrics",
        )
        .await;
        let names = metrics
            .lines()
            .filter_map(|line| line.strip_prefix("# HELP "))
            .filter_map(|line| line.split_whitespace().next())
            .collect::<Vec<_>>();
        assert_eq!(
            names
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            names.len()
        );
        assert_eq!(metrics.matches("# EOF").count(), 1);
        assert_eq!(
            metrics
                .lines()
                .filter(|line| line.starts_with("# HELP tidemq_lifecycle_published "))
                .count(),
            1
        );

        published += metrics
            .lines()
            .find_map(|line| line.strip_prefix("tidemq_lifecycle_published_total "))
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
    }
    assert_eq!(published, 1);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_runtime_scaling_explicit_replacement_preserves_original_sdk_traffic_and_ack() {
    exercise_runtime_scaling(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_runtime_scaling_automatically_repairs_failed_storage_with_original_sdk_handles() {
    exercise_runtime_scaling(true).await;
}

async fn exercise_runtime_scaling(automatic: bool) {
    let mut fixture = Fixture::new(
        &[
            (11, Role::Metadata),
            (12, Role::Metadata),
            (13, Role::Metadata),
            (21, Role::Storage),
            (22, Role::Storage),
            (23, Role::Storage),
            (24, Role::Storage),
            (31, Role::Broker),
            (41, Role::Gateway),
        ],
        &[11, 12, 13],
        &[21, 22, 23],
    )
    .await;
    for config in fixture.configs.values_mut() {
        config.runtime.groups.first_mut().unwrap().standby = vec![24];
        // The replacement keeps the removed replica's availability zone.
        config
            .runtime
            .peers
            .iter_mut()
            .find(|peer| peer.node_id == 24)
            .unwrap()
            .zone = "zone-2".into();
    }
    let mut storage_process = if automatic {
        for (node, config) in &fixture.configs {
            if *node != 23 {
                fixture
                    .running
                    .insert(*node, RunningNode::start(config.clone()).await.unwrap());
            }
        }
        let child = start_process(&fixture, 23).await;
        fixture.wait_ready().await;
        Some(child)
    } else {
        fixture.start_all().await;
        None
    };
    wait_scaling_diagnostic(&fixture, 24, |value| {
        value["placement_capacity_available"] == true
    })
    .await;
    let client = fixture.client(41).await;
    let (topic, subscription) = create(&client).await;
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap(), deadline())
        .await
        .unwrap();
    let message = || public::Message {
        key: Bytes::from_static(b"scaling-key"),
        body: Bytes::from_static(b"scaling-body"),
    };
    producer
        .produce(vec![message(), message()], deadline())
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
    assert_eq!(consumer.fill(deadline()).await.unwrap(), 2);
    let first = consumer.next().await.unwrap().unwrap();
    assert_eq!(
        consumer
            .ack(&first, deadline())
            .await
            .unwrap()
            .acknowledged_before,
        1
    );
    let held = consumer.next().await.unwrap().unwrap();
    assert_eq!(held.offset(), 1);
    if let Some(child) = storage_process.as_mut() {
        child.kill().await.unwrap();
        assert!(!child.wait().await.unwrap().success());
    }
    let migration = async {
        if automatic {
            wait_scaling_diagnostic(&fixture, 21, scaling_group_is_replaced).await;
            None
        } else {
            Some(
                fixture
                    .running
                    .get(&11)
                    .unwrap()
                    .replace_replica(2, 23, 24, Instant::now() + Duration::from_secs(30))
                    .await
                    .unwrap(),
            )
        }
    };
    let traffic = async {
        let ack = consumer.ack(&held, deadline()).await;
        let produced = producer.produce(vec![message()], deadline()).await;
        let read = consumer.fill(deadline()).await;
        (ack, produced, read)
    };
    let (plan, (ack, produced, read)) = tokio::join!(migration, traffic);
    if let Some(plan) = plan {
        assert_eq!(plan.previous(), &[21, 22, 23].into());
        assert_eq!(plan.target(), &[21, 22, 24].into());
        let repeated = fixture
            .running
            .get(&11)
            .unwrap()
            .replace_replica(2, 23, 24, deadline())
            .await
            .unwrap();
        assert_eq!(
            repeated, plan,
            "A completion retry must retain its original plan identity"
        );
    }
    wait_scaling_diagnostic(&fixture, 21, scaling_group_is_replaced).await;
    let ack = match ack {
        Ok(ack) => ack,
        Err(_) => consumer.ack(&held, deadline()).await.unwrap(),
    };
    assert_eq!(ack.acknowledged_before, 2);
    let receipt = match produced {
        Ok(receipt) => receipt,
        Err(error) if error.outcome_unknown() => {
            let pending = producer.pending_batches().await.unwrap();
            assert_eq!(pending.len(), 1);
            let batch = pending.first().unwrap();
            let identity = (
                batch.operation_id(),
                batch.producer_epoch(),
                batch.first_sequence(),
                batch.digest().to_vec(),
            );
            let receipt = producer.retry(batch, deadline()).await.unwrap();
            assert_eq!(
                identity,
                (
                    batch.operation_id(),
                    batch.producer_epoch(),
                    batch.first_sequence(),
                    batch.digest().to_vec()
                )
            );
            receipt
        }
        Err(_) => producer.produce(vec![message()], deadline()).await.unwrap(),
    };
    assert_eq!(receipt.first_offset, 2);
    assert!(producer.pending_batches().await.unwrap().is_empty());
    let during = read.unwrap_or(0);
    assert_eq!(during + consumer.fill(deadline()).await.unwrap(), 1);
    let delivered = consumer.next().await.unwrap().unwrap();
    assert_eq!(delivered.offset(), 2);
    assert_eq!(delivered.body(), b"scaling-body");
    assert_eq!(
        consumer
            .ack(&delivered, deadline())
            .await
            .unwrap()
            .acknowledged_before,
        3
    );
    assert!(consumer.next().await.unwrap().is_none());
    client.close(deadline()).await;
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.close().await;
}

fn scaling_group_is_replaced(value: &serde_json::Value) -> bool {
    value["data_groups"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|group| {
            group["group_id"] == 2
                && group["joint_configs"]
                    .as_array()
                    .is_some_and(|configs| configs.len() == 1)
                && group["voters"].as_array().map(|voters| {
                    voters
                        .iter()
                        .filter_map(serde_json::Value::as_u64)
                        .collect::<std::collections::BTreeSet<_>>()
                }) == Some([21, 22, 24].into())
        })
}

async fn wait_scaling_diagnostic(
    fixture: &Fixture,
    node: u64,
    predicate: impl Fn(&serde_json::Value) -> bool,
) {
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let address = fixture.running.get(&node).unwrap().management_address;
        let response = http(address, "/diagnostics").await;
        let (_, body) = response.split_once("\r\n\r\n").unwrap();
        let value: serde_json::Value = serde_json::from_str(body).unwrap();
        if predicate(&value) {
            return;
        }
        assert!(
            Instant::now() < until,
            "Scaling diagnostic did not satisfy its predicate: {value}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
