use super::*;

pub(crate) struct Fixture {
    pub(crate) directory: PathBuf,
    pub(crate) roots: Bytes,
    pub(crate) configs: BTreeMap<u64, Config>,
    pub(crate) running: BTreeMap<u64, RunningNode>,
}

impl Fixture {
    pub(crate) async fn new(nodes: &[(u64, Role)], metadata: &[u64], storage: &[u64]) -> Self {
        let directory =
            std::env::temp_dir().join(format!("tidemq-runtime-{}", Id::new_v7().unwrap()));
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
        let bootstrap = directory.join("bootstrap.token");
        tokio::fs::write(&bootstrap, TOKEN).await.unwrap();
        private_file(bootstrap.clone()).await;
        let mut peers = Vec::new();
        for (node, role) in nodes {
            let key = KeyPair::generate().unwrap();
            let mut names = vec![
                format!("node-{node}.local"),
                "127.0.0.1".into(),
                "localhost".into(),
            ];
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
        let cluster = Id::new_v7().unwrap();
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
                        bootstrap_admin_token_file: Some(bootstrap.clone()),
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

    pub(crate) async fn start_all(&mut self) {
        for (node, config) in &self.configs {
            self.running
                .insert(*node, RunningNode::start(config.clone()).await.unwrap());
        }
        self.wait_ready().await;
    }

    pub(crate) async fn wait_ready(&self) {
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
            if Instant::now() >= deadline {
                let mut diagnostics = BTreeMap::new();
                for (id, node) in &self.running {
                    diagnostics.insert(*id, http(node.management_address, "/diagnostics").await);
                }
                tokio::fs::write(
                    self.directory.join("readiness-failure.json"),
                    serde_json::to_vec_pretty(&diagnostics).unwrap(),
                )
                .await
                .unwrap();
                panic!(
                    "Runtime readiness deadline elapsed; inspect {}",
                    self.directory.join("readiness-failure.json").display()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub(crate) async fn client(&self, gateway: u64) -> Client {
        self.client_with_timeouts(gateway, Duration::from_secs(10), Duration::from_secs(5))
            .await
    }

    pub(crate) async fn client_with_timeouts(
        &self,
        gateway: u64,
        request: Duration,
        close: Duration,
    ) -> Client {
        Client::builder(
            self.configs.get(&gateway).unwrap().listen.to_string(),
            tidemq_sdk::TlsTrust::ca_pem(self.roots.clone()),
            tidemq_sdk::Credentials::token(TOKEN),
        )
        .request_timeout(request)
        .close_timeout(close)
        .build()
        .await
        .unwrap()
    }

    pub(crate) async fn stop(&mut self, node: u64) {
        let running = self.running.remove(&node).unwrap();
        running.shutdown().await.unwrap();
    }
    pub(crate) async fn restart(&mut self, node: u64) {
        self.running.insert(
            node,
            RunningNode::start(self.configs.get(&node).unwrap().clone())
                .await
                .unwrap(),
        );
        self.wait_ready().await;
    }
    pub(crate) async fn close(mut self) {
        for (_, node) in std::mem::take(&mut self.running) {
            node.shutdown().await.unwrap();
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() || std::env::var_os("TIDEMQ_KEEP_TEST_EVIDENCE").is_some() {
            eprintln!(
                "Preserved runtime evidence directory {}",
                self.directory.display()
            );
            return;
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

pub(crate) fn udp_address() -> SocketAddr {
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
pub(crate) fn tcp_address() -> SocketAddr {
    for _ in 0..64_512 {
        // Share the cross-process reservations and stay outside client ephemeral ports.
        let address = udp_address();
        if std::net::TcpListener::bind(address).is_ok() {
            return address;
        }
    }
    panic!("No test TCP port is available outside the observed ephemeral range");
}
pub(crate) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

pub(crate) async fn http(address: SocketAddr, path: &str) -> String {
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

pub(crate) async fn enable_objects(fixture: &mut Fixture, rustfs: &rustfs::RustFs) {
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
pub(crate) async fn private_file(path: PathBuf) {
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
