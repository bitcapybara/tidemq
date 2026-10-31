use super::*;

pub(crate) struct Fixture {
    pub(crate) directory: PathBuf,
    pub(crate) roots: Bytes,
    pub(crate) configs: BTreeMap<u64, Config>,
    pub(crate) running: BTreeMap<u64, RunningNode>,
    created_at: Instant,
    timeline_enabled: bool,
}

async fn embedded_console(directory: &std::path::Path) -> tidemq::console::EmbeddedConsoleConfig {
    let static_dir = directory.join("console");
    tokio::fs::create_dir_all(static_dir.join(".vite"))
        .await
        .unwrap();
    tokio::fs::create_dir(static_dir.join("assets"))
        .await
        .unwrap();
    for (name, content) in [
        (
            "index.html",
            "<!doctype html><html><head><link rel=\"stylesheet\" href=\"/assets/fixture.css\"></head><body><script type=\"module\" src=\"/assets/fixture.js\"></script></body></html>",
        ),
        ("assets/fixture.js", "export const ready = true;"),
        ("assets/fixture.css", "body { color: #222; }"),
        (
            ".vite/manifest.json",
            r#"{"index.html":{"file":"assets/fixture.js","isEntry":true,"css":["assets/fixture.css"]}}"#,
        ),
    ] {
        tokio::fs::write(static_dir.join(name), content)
            .await
            .unwrap();
    }
    let key_file = directory.join("console-session.key");
    let mut key = [0_u8; 32];
    getrandom::fill(&mut key).unwrap();
    tokio::fs::write(&key_file, key).await.unwrap();
    private_file(key_file.clone()).await;
    let listen = tcp_address();
    tidemq::console::EmbeddedConsoleConfig {
        listen,
        static_dir,
        session: tidemq::console::SessionConfig {
            key_file,
            previous_key_files: Vec::new(),
            public_origin: format!("https://localhost:{}", listen.port()),
            max_age_seconds: 1800,
        },
        observability: Default::default(),
    }
}

impl Fixture {
    pub(crate) async fn new(nodes: &[(u64, Role)], metadata: &[u64], storage: &[u64]) -> Self {
        assert!(
            !nodes.is_empty(),
            "Runtime fixtures require at least one node"
        );
        let local = nodes.iter().any(|(_, role)| *role == Role::Combined);
        if local {
            let [(node, Role::Combined)] = nodes else {
                panic!("Combined fixtures require one local process and cannot mix roles");
            };
            // These inputs name the actual local host, not Raft membership.
            assert_eq!(
                metadata,
                &[*node],
                "Combined metadata must use its local host"
            );
            assert_eq!(storage, &[*node], "Combined data must use its local host");
        }
        let directory =
            std::env::temp_dir().join(format!("tidemq-runtime-{}", Id::new_v7().unwrap()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
                .await
                .unwrap();
        }
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
            if local {
                continue;
            }
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
        let console = if local {
            Some(embedded_console(&directory).await)
        } else {
            None
        };
        let cluster = Id::new_v7().unwrap();
        let configs = nodes
            .iter()
            .map(|(node, role)| {
                let internal_listen = if local {
                    SocketAddr::from(([127, 0, 0, 1], 0))
                } else {
                    peers
                        .iter()
                        .find(|peer| peer.node_id == *node)
                        .unwrap()
                        .internal_address
                };
                let mut config = Config {
                    node_id: *node,
                    cluster_id: cluster.to_string(),
                    role: *role,
                    listen: udp_address(),
                    management_listen: tcp_address(),
                    data_dir: directory.join(format!("data-{node}")),
                    replication_factor: if *role == Role::Combined {
                        1
                    } else {
                        metadata.len() as u8
                    },
                    console: console.clone(),
                    tls: TlsConfig {
                        certificate: directory.join(format!("node-{node}.pem")),
                        private_key: directory.join(format!("node-{node}.key")),
                        trust_roots: if local {
                            PathBuf::new()
                        } else {
                            directory.join("ca.pem")
                        },
                    },
                    runtime: RuntimeConfig {
                        internal_listen,
                        metadata_group: 1,
                        metadata_voters: if local { Vec::new() } else { metadata.to_vec() },
                        groups: vec![GroupConfig {
                            group_id: 2,
                            voters: if local { Vec::new() } else { storage.to_vec() },
                            standby: Vec::new(),
                        }],
                        peers: peers.clone(),
                        bootstrap_admin_token_file: role
                            .stores_metadata()
                            .then(|| bootstrap.clone()),
                        bootstrap: !local,
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
            created_at: Instant::now(),
            timeline_enabled: false,
        }
    }

    pub(crate) async fn start_all(&mut self) {
        self.start_all_with_ready_timeout(Duration::from_secs(15))
            .await;
    }

    pub(crate) async fn start_all_with_ready_timeout(&mut self, timeout: Duration) {
        let detail = format!("timeout_ms={}", timeout.as_millis());
        self.record_startup_event("fixture_start", None, &detail)
            .await;
        let nodes = self.configs.keys().copied().collect::<Vec<_>>();
        for node in nodes {
            self.start_node(node).await;
        }
        self.wait_ready_with_timeout(timeout).await;
    }

    pub(crate) async fn start_node(&mut self, node: u64) {
        let config = self.configs.get(&node).unwrap().clone();
        let started = Instant::now();
        self.running
            .insert(node, RunningNode::start(config).await.unwrap());
        let detail = format!("startup_ms={}", started.elapsed().as_millis());
        self.record_startup_event("running_node_started", Some(node), &detail)
            .await;
    }

    // The auth semantic test is compiled into the library test target; other targets do not use this opt-in.
    #[allow(dead_code)]
    #[cfg(feature = "test-hooks")]
    pub(crate) fn enable_startup_timeline(&mut self) {
        self.timeline_enabled = true;
    }

    pub(crate) async fn record_startup_event(&self, event: &str, node: Option<u64>, detail: &str) {
        if !self.timeline_enabled {
            return;
        }
        use tokio::io::AsyncWriteExt;

        let entry = serde_json::json!({
            "event": event,
            "node_id": node,
            "elapsed_ms": self.created_at.elapsed().as_millis(),
            "detail": detail,
        });
        let line = match serde_json::to_string(&entry) {
            Ok(line) => line,
            Err(error) => {
                eprintln!("Failed to encode runtime startup event: {error}");
                return;
            }
        };
        let path = self.directory.join("readiness-timeline.jsonl");
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await;
        let Ok(mut file) = file else {
            eprintln!("Failed to open runtime startup timeline {}", path.display());
            return;
        };
        if let Err(error) = file.write_all(format!("{line}\n").as_bytes()).await {
            eprintln!("Failed to write runtime startup timeline: {error}");
        }
    }

    pub(crate) async fn wait_ready(&self) {
        self.wait_ready_with_timeout(Duration::from_secs(15)).await;
    }

    async fn wait_ready_with_timeout(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        self.record_startup_event("readiness_wait_start", None, "waiting_for_all_nodes")
            .await;
        let mut states = BTreeMap::new();
        loop {
            let mut ready = true;
            for (id, node) in &self.running {
                let response = http_until(node.management_address, "/readyz", deadline).await;
                let node_ready = response.starts_with("HTTP/1.1 200");
                ready &= node_ready;
                if states.get(id).copied() != Some(node_ready) {
                    states.insert(*id, node_ready);
                    let detail = if node_ready {
                        "readyz_200"
                    } else {
                        "readyz_not_ready"
                    };
                    self.record_startup_event("readyz_state_changed", Some(*id), detail)
                        .await;
                }
            }
            if ready {
                self.record_startup_event("fixture_ready", None, "all_nodes_ready")
                    .await;
                return;
            }
            if Instant::now() >= deadline {
                self.record_startup_event(
                    "readiness_deadline_elapsed",
                    None,
                    "collecting_diagnostics",
                )
                .await;
                let mut diagnostics = BTreeMap::new();
                for (id, node) in &self.running {
                    let diagnostics_deadline = Instant::now() + Duration::from_secs(2);
                    diagnostics.insert(
                        *id,
                        http_until(
                            node.management_address,
                            "/diagnostics",
                            diagnostics_deadline,
                        )
                        .await,
                    );
                }
                tokio::fs::write(
                    self.directory.join("readiness-failure.json"),
                    serde_json::to_vec_pretty(&diagnostics).unwrap(),
                )
                .await
                .unwrap();
                self.record_startup_event(
                    "readiness_diagnostics_written",
                    None,
                    "readiness-failure.json",
                )
                .await;
                panic!(
                    "Runtime readiness deadline elapsed; inspect {}",
                    self.directory.join("readiness-failure.json").display()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub(crate) async fn wait_for_quorum_leaders(
        &self,
        client: &Client,
        storage_group: u64,
    ) -> (u64, u64) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match client
                .console_read(tidemq_protocol::console::Query::Cluster {
                    after: None,
                    limit: 32,
                })
                .await
            {
                Ok(tidemq_protocol::console::Reply::Cluster(page)) => {
                    let metadata = page
                        .items
                        .iter()
                        .flat_map(|node| &node.groups)
                        .find(|group| {
                            group.id == tidemq_protocol::console::Decimal(1)
                                && group.role == "metadata"
                                && group.quorum_confirmed == Some(true)
                        })
                        .filter(|group| group.voters.len() == 3 && group.leader.is_some());
                    let storage = page
                        .items
                        .iter()
                        .flat_map(|node| &node.groups)
                        .find(|group| {
                            group.id == tidemq_protocol::console::Decimal(storage_group)
                                && group.role == "storage"
                                && group.quorum_confirmed == Some(true)
                        })
                        .filter(|group| group.voters.len() == 3 && group.leader.is_some());
                    if let (Some(metadata), Some(storage)) = (metadata, storage) {
                        break (metadata.leader.unwrap().0, storage.leader.unwrap().0);
                    }
                }
                Err(error) => assert!(
                    matches!(
                        error.kind(),
                        tidemq_sdk::ErrorKind::Unavailable
                            | tidemq_sdk::ErrorKind::DeadlineExceeded
                    ),
                    "Cluster observation failed permanently: {error:?}"
                ),
                reply => panic!("Expected the native cluster observation: {reply:?}"),
            }
            assert!(
                Instant::now() < deadline,
                "RF3 quorums did not elect leaders in time"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub(crate) async fn wait_for_group_leader(
        &self,
        client: &Client,
        group_id: u64,
        role: &str,
        voter_count: usize,
    ) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let query = tidemq_sdk::RequestContext::with_deadline(deadline)
                .scope(
                    client.console_read(tidemq_protocol::console::Query::Cluster {
                        after: None,
                        limit: 32,
                    }),
                )
                .await;
            if let Ok(tidemq_protocol::console::Reply::Cluster(page)) = query {
                let stable = page
                    .items
                    .iter()
                    .flat_map(|node| &node.groups)
                    .any(|group| {
                        group.id == tidemq_protocol::console::Decimal(group_id)
                            && group.role == role
                            && group.voters.len() == voter_count
                            && group.leader.is_some()
                            && group.quorum_confirmed == Some(true)
                    });
                if stable {
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "Group {group_id} ({role}) did not regain a stable leader"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
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
        .server_name(format!("node-{gateway}.local"))
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
    http_until(address, path, Instant::now() + Duration::from_secs(2)).await
}

pub(crate) async fn http_until(address: SocketAddr, path: &str, deadline: Instant) -> String {
    http_checked_until(address, path, deadline)
        .await
        .unwrap_or_default()
}

pub(crate) async fn http_checked_until(
    address: SocketAddr,
    path: &str,
    deadline: Instant,
) -> std::io::Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::time::timeout_at(deadline, async {
        let mut connection = tokio::net::TcpStream::connect(address).await?;
        connection
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await?;
        let mut response = String::new();
        connection.read_to_string(&mut response).await?;
        Ok::<_, std::io::Error>(response)
    })
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "management HTTP request exceeded its deadline",
        )
    })?
}

pub(crate) fn validate_http_success(
    response: &str,
    address: SocketAddr,
    path: &str,
) -> std::io::Result<()> {
    let status = response
        .split_once("\r\n")
        .map(|(status, _)| status)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("management HTTP response from {address} for {path} has no status line"),
            )
        })?;
    let code = status.split_whitespace().nth(1).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("management HTTP response from {address} for {path} has no status code"),
        )
    })?;
    if code != "200" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("management HTTP response from {address} for {path} returned status {code}"),
        ));
    }
    Ok(())
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
