use super::read_batches::{finish_original_ack, finish_original_send};
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn separated_rf3_keeps_metadata_and_data_read_barriers_live_during_durable_acks() {
    let fixture = Fixture::new(
        &[
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
        ],
        &[11, 12, 13],
        &[21, 22, 23],
    )
    .await;
    let mut processes = Vec::new();
    for node in [11, 12, 13, 21, 22, 23, 31, 32, 41, 42] {
        processes.push((node, spawn_process(&fixture, node).await));
    }
    for (node, child) in &mut processes {
        wait_process(&fixture, *node, child).await;
    }
    let client = fixture.client(41).await;
    let tenant = client.admin().create_tenant("separated").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let mut work = Vec::new();
    for index in 0..4 {
        let name = format!("separated/app/read-barrier-{index}");
        let topic = client.admin().create_topic(&name).await.unwrap();
        let consumer = client
            .new_stream_consumer(&name, "durable")
            .build()
            .await
            .unwrap();
        let producer = client.new_producer(&name).build().await.unwrap();
        work.push((name, topic.id(), producer, consumer));
    }
    let phase_budget = Duration::from_secs(40);
    let mut running = tokio::task::JoinSet::new();
    for (name, topic, producer, consumer) in work {
        let client = client.clone();
        running.spawn(async move {
            let started = Instant::now();
            let until = started + phase_budget;
            let context = tidemq_sdk::RequestContext::with_timeout(phase_budget).unwrap();
            let produced = AtomicU64::new(0);
            let acknowledged = AtomicU64::new(0);
            let metadata_reads = AtomicU64::new(0);
            let metadata_admission_rejections = AtomicU64::new(0);
            let produce = async {
                for sequence in 0..64_u64 {
                    let result = context.scope(producer.send(tidemq_sdk::Message::new(sequence.to_le_bytes().to_vec()).key("combined"))).await;
                    finish_original_send(result, until).await.unwrap();
                    produced.store(sequence + 1, Ordering::Relaxed);
                }
            };
            let consume = async {
                let mut seen = std::collections::HashSet::new();
                for sequence in 0..64_u64 {
                    let delivery = context.scope(consumer.receive()).await.unwrap();
                    assert_eq!(delivery.key(), Some(b"combined".as_slice()));
                    assert_eq!(delivery.payload(), &sequence.to_le_bytes());
                    assert!(seen.insert(delivery.message_id()), "A healthy Stream repeated an identity");
                    let result = context.scope(consumer.ack_cumulative(&delivery)).await;
                    finish_original_ack(result, until).await.unwrap();
                    acknowledged.store(sequence + 1, Ordering::Relaxed);
                }
            };
            let read_metadata = async {
                for _ in 0..64 {
                    // Read-only queries honor explicit admission backpressure within the phase deadline.
                    let info = loop {
                        match context.scope(client.admin().topic_info(&name)).await {
                            Err(tidemq_sdk::Error::Remote {
                                code: tidemq_protocol::common::ErrorCode::QuotaExceeded,
                                outcome: tidemq_protocol::common::Outcome::Rejected,
                                retryable: true,
                                ..
                            }) => {
                                metadata_admission_rejections.fetch_add(1, Ordering::Relaxed);
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            result => break result.unwrap(),
                        }
                    };
                    assert_eq!(info.id(), topic);
                    metadata_reads.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
            };
            tokio::join!(produce, consume, read_metadata);
            assert_eq!(produced.load(Ordering::Relaxed), 64);
            assert_eq!(acknowledged.load(Ordering::Relaxed), 64);
            assert_eq!(metadata_reads.load(Ordering::Relaxed), 64);
            eprintln!("Separated RF3 phase completed: topic={name}, elapsed_ms={}, produced=64, acknowledged=64, metadata_reads=64, metadata_admission_rejections={}", started.elapsed().as_millis(), metadata_admission_rejections.load(Ordering::Relaxed));
            consumer.close().await.unwrap();
            producer.close().await.unwrap();
        });
    }
    tokio::time::timeout(phase_budget + Duration::from_secs(5), async {
        while let Some(result) = running.join_next().await {
            result.unwrap();
        }
    })
    .await
    .unwrap();
    client.close().await.unwrap();
    for (_, mut process) in processes {
        process.kill().await.unwrap();
        let _ = process.wait().await.unwrap();
    }
    fixture.close().await;
}

#[tokio::test]
async fn combined_fixture_provisions_real_assets_and_identity_without_raft_material() {
    let fixture = Fixture::new(&[(7, Role::Combined)], &[7], &[7]).await;
    let config = fixture.configs.get(&7).unwrap();
    config.runtime.validate(config).unwrap();
    assert!(config.tls.trust_roots.as_os_str().is_empty());
    assert_eq!(config.runtime.internal_listen.port(), 0);
    assert!(config.runtime.metadata_voters.is_empty());
    assert!(config.runtime.peers.is_empty());
    assert!(!config.runtime.bootstrap);
    for group in &config.runtime.groups {
        assert!(group.voters.is_empty());
        assert!(group.standby.is_empty());
    }
    assert!(!fixture.directory.join("node-7.der").exists());
    let console = config.console.as_ref().unwrap();
    assert_eq!(
        tokio::fs::read(&console.session.key_file)
            .await
            .unwrap()
            .len(),
        32
    );
    tidemq::console::validate_embedded(console, &config.tls)
        .await
        .unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn separated_fixture_has_real_role_topology_and_no_embedded_console() {
    let fixture = Fixture::new(
        &[
            (11, Role::Metadata),
            (21, Role::Storage),
            (31, Role::Broker),
            (41, Role::Gateway),
        ],
        &[11],
        &[21],
    )
    .await;
    for config in fixture.configs.values() {
        config.runtime.validate(config).unwrap();
        assert!(config.console.is_none());
        assert_eq!(config.replication_factor, 1);
        assert!(!config.tls.trust_roots.as_os_str().is_empty());
        assert_ne!(config.runtime.internal_listen.port(), 0);
        assert_eq!(config.runtime.metadata_voters, [11]);
        assert_eq!(config.runtime.groups.first().unwrap().voters, [21]);
        assert_eq!(config.runtime.peers.len(), 4);
    }
    fixture.close().await;
}

#[tokio::test]
#[should_panic(expected = "Combined fixtures require one local process and cannot mix roles")]
async fn fixture_rejects_multiple_combined_nodes() {
    Fixture::new(
        &[
            (1, Role::Combined),
            (2, Role::Combined),
            (3, Role::Combined),
        ],
        &[1, 2, 3],
        &[1, 2, 3],
    )
    .await;
}

#[tokio::test]
#[should_panic(expected = "Combined fixtures require one local process and cannot mix roles")]
async fn fixture_rejects_combined_mixed_with_separated_roles() {
    Fixture::new(&[(1, Role::Combined), (2, Role::Gateway)], &[1], &[1]).await;
}

fn https_client(fixture: &Fixture) -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .tls_certs_only(reqwest::Certificate::from_pem_bundle(&fixture.roots).unwrap())
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
}

fn console_url(config: &Config, path: &str) -> String {
    format!(
        "{}{path}",
        config.console.as_ref().unwrap().session.public_origin
    )
}

async fn assert_ports_released(config: &Config) {
    let until = Instant::now() + Duration::from_secs(3);
    loop {
        let public = UdpSocket::bind(config.listen);
        let management = std::net::TcpListener::bind(config.management_listen);
        let console = std::net::TcpListener::bind(config.console.as_ref().unwrap().listen);
        if public.is_ok() && management.is_ok() && console.is_ok() {
            return;
        }
        assert!(
            Instant::now() < until,
            "Combined listeners were not released: QUIC={public:?}, management={management:?}, HTTPS={console:?}"
        );
        drop((public, management, console));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn management_response(address: SocketAddr, path: &str) -> Option<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::time::timeout(Duration::from_millis(500), async {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await?;
        let mut response = String::new();
        stream.take(65_536).read_to_string(&mut response).await?;
        Ok::<_, std::io::Error>(response)
    })
    .await
    .ok()
    .and_then(Result::ok)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_successful_ack_survives_kill_and_unknown_send_keeps_original_identity() {
    let fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    let config = fixture.configs.get(&1).unwrap();
    let mut process = start_process(&fixture, 1).await;
    let client = fixture.client(1).await;
    create_logical_resources(&client).await;
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "combined-recovery")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let first = producer
        .send("committed-before-kill")
        .await
        .unwrap()
        .message_id();
    let delivery = consumer.receive().await.unwrap();
    assert_eq!(delivery.message_id(), first);
    assert_eq!(
        consumer.ack(&delivery).await.unwrap(),
        tidemq_sdk::AckReceipt::Acknowledged
    );
    process.kill().await.unwrap();
    assert!(!process.wait().await.unwrap().success());
    assert_ports_released(config).await;
    let error = tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(200))
        .unwrap()
        .scope(producer.send("original-unknown-send"))
        .await
        .unwrap_err();
    assert!(error.outcome_unknown());
    let pending = producer.pending_sends().await.unwrap();
    assert_eq!(pending.len(), 1);
    let original = pending.first().unwrap();
    let mut process = start_process(&fixture, 1).await;
    let second = original.retry().await.unwrap().message_id();
    assert_ne!(first, second);
    assert_eq!(original.retry().await.unwrap().message_id(), second);
    assert!(producer.pending_sends().await.unwrap().is_empty());
    let delivery = consumer.receive().await.unwrap();
    assert_eq!(
        delivery.message_id(),
        second,
        "A successful durable ACK was lost after restart"
    );
    assert_eq!(delivery.payload(), b"original-unknown-send");
    consumer.ack(&delivery).await.unwrap();
    tokio::fs::write(fixture.directory.join("combined-recovery.json"),
        serde_json::to_vec_pretty(&serde_json::json!({"first":first.to_string(),"recovered":second.to_string(),"original_retry_stable":true,"durable_ack_preserved":true})).unwrap()).await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    process.kill().await.unwrap();
    let _ = process.wait().await.unwrap();
    assert_ports_released(config).await;
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_startup_material_failures_release_ports_and_allow_same_directory_retry() {
    for material in [
        "index.html",
        "assets/fixture.js",
        "assets/fixture.css",
        "session",
        "certificate",
        "private-key",
    ] {
        let fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        let config = fixture.configs.get(&1).unwrap();
        let console = config.console.as_ref().unwrap();
        let path = match material {
            "session" => console.session.key_file.clone(),
            "certificate" => config.tls.certificate.clone(),
            "private-key" => config.tls.private_key.clone(),
            asset => console.static_dir.join(asset),
        };
        let original = tokio::fs::read(&path).await.unwrap();
        tokio::fs::remove_file(&path).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(10), RunningNode::start(config.clone()))
                .await
                .unwrap()
                .is_err(),
            "Missing {material} was accepted"
        );
        assert_ports_released(config).await;
        tokio::fs::write(&path, original).await.unwrap();
        if matches!(material, "session" | "private-key") {
            private_file(path).await;
        }
        let node = RunningNode::start(config.clone()).await.unwrap();
        assert!(node.internal_address.is_none());
        node.shutdown().await.unwrap();
        assert_ports_released(config).await;
        fixture.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_partial_bind_failures_release_groups_and_all_listeners() {
    for occupied in ["quic", "https", "management"] {
        let fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        let config = fixture.configs.get(&1).unwrap();
        let public = (occupied == "quic").then(|| UdpSocket::bind(config.listen).unwrap());
        let tcp = match occupied {
            "https" => {
                Some(std::net::TcpListener::bind(config.console.as_ref().unwrap().listen).unwrap())
            }
            "management" => Some(std::net::TcpListener::bind(config.management_listen).unwrap()),
            _ => None,
        };
        assert!(
            tokio::time::timeout(Duration::from_secs(10), RunningNode::start(config.clone()))
                .await
                .unwrap()
                .is_err(),
            "Occupied {occupied} port was accepted"
        );
        drop((public, tcp));
        assert_ports_released(config).await;
        let node = RunningNode::start(config.clone()).await.unwrap();
        assert!(node.internal_address.is_none());
        node.shutdown().await.unwrap();
        assert_ports_released(config).await;
        fixture.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_draining_completes_ack_and_releases_the_original_directory_before_deadline() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let config = fixture.configs.get(&1).unwrap().clone();
    let client = fixture.client(1).await;
    create_logical_resources(&client).await;
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "combined-drain")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let first = producer
        .send("ack-during-drain")
        .await
        .unwrap()
        .message_id();
    let delivery = consumer.receive().await.unwrap();
    let node = fixture.running.remove(&1).unwrap();
    assert!(node.internal_address.is_none());
    assert_eq!(
        node.console_address,
        Some(config.console.as_ref().unwrap().listen)
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let shutdown = tokio::spawn(async move {
        tokio::time::timeout_at(deadline, node.shutdown_until(deadline))
            .await
            .expect("Combined extended its absolute shutdown deadline")
    });
    loop {
        if management_response(config.management_listen, "/readyz")
            .await
            .is_some_and(|response| response.starts_with("HTTP/1.1 503"))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Shutdown did not close readiness before completion admission"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let receipt = tidemq_sdk::RequestContext::with_deadline(deadline)
        .scope(consumer.ack(&delivery))
        .await
        .unwrap();
    assert_eq!(receipt, tidemq_sdk::AckReceipt::Acknowledged);
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    shutdown.await.unwrap().unwrap();
    tokio::time::timeout_at(deadline, assert_ports_released(&config))
        .await
        .expect("Combined retained listeners past its shutdown deadline");
    tokio::time::timeout_at(deadline, fixture.restart(1))
        .await
        .expect("Combined retained its original directory past the shutdown deadline");
    let client = fixture.client(1).await;
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "combined-drain")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let second = producer
        .send("after-drain-restart")
        .await
        .unwrap()
        .message_id();
    assert_ne!(second, first);
    let delivery = consumer.receive().await.unwrap();
    assert_eq!(delivery.message_id(), second);
    consumer.ack(&delivery).await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
    assert_ports_released(&config).await;
}

#[cfg(target_os = "linux")]
async fn process_ports(pid: u32, protocol: &str) -> BTreeSet<u16> {
    let mut inodes = BTreeSet::new();
    let mut descriptors = tokio::fs::read_dir(format!("/proc/{pid}/fd"))
        .await
        .unwrap();
    while let Some(descriptor) = descriptors.next_entry().await.unwrap() {
        let target = match tokio::fs::read_link(descriptor.path()).await {
            Ok(target) => target,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => panic!("Failed to observe a Combined descriptor: {error}"),
        };
        let target = target.to_string_lossy();
        if let Some(inode) = target
            .strip_prefix("socket:[")
            .and_then(|value| value.strip_suffix(']'))
        {
            inodes.insert(inode.to_owned());
        }
    }
    let mut ports = BTreeSet::new();
    for family in [protocol.to_owned(), format!("{protocol}6")] {
        let entries = match tokio::fs::read_to_string(format!("/proc/{pid}/net/{family}")).await {
            Ok(entries) => entries,
            Err(error) if family.ends_with('6') && error.kind() == std::io::ErrorKind::NotFound => {
                continue;
            }
            Err(error) => panic!("Process socket observation failed: {error}"),
        };
        for line in entries.lines().skip(1) {
            let fields: Vec<_> = line.split_ascii_whitespace().collect();
            if !fields.get(9).is_some_and(|inode| inodes.contains(*inode)) {
                continue;
            }
            if protocol == "tcp" && fields.get(3) != Some(&"0A") {
                continue;
            }
            let (_, port) = fields.get(1).unwrap().rsplit_once(':').unwrap();
            ports.insert(u16::from_str_radix(port, 16).unwrap());
        }
    }
    ports
}

#[cfg(target_os = "macos")]
async fn process_ports(pid: u32, protocol: &str) -> BTreeSet<u16> {
    let mut command = tokio::process::Command::new("lsof");
    command.args([
        "-nP",
        "-a",
        "-p",
        &pid.to_string(),
        &format!("-i{}", protocol.to_ascii_uppercase()),
        "-Fpn",
    ]);
    if protocol == "tcp" {
        command.arg("-sTCP:LISTEN");
    }
    let output = command.output().await.unwrap();
    assert!(
        output.status.success(),
        "Process socket observation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = String::from_utf8(output.stdout).unwrap();
    assert!(
        output.lines().any(|line| line == format!("p{pid}")),
        "Socket observations belong to another process"
    );
    output
        .lines()
        .filter_map(|line| line.strip_prefix('n'))
        .map(|address| {
            assert!(
                !address.contains("->"),
                "Combined opened an internal client socket: {address}"
            );
            address.rsplit_once(':').unwrap().1.parse().unwrap()
        })
        .collect()
}

fn assert_local_diagnostics(document: &serde_json::Value) {
    let metadata = document
        .get("metadata")
        .expect("Local metadata diagnostics are missing");
    let groups = document
        .get("data_groups")
        .and_then(serde_json::Value::as_array)
        .expect("Local data diagnostics are missing");
    assert!(!groups.is_empty(), "Local data group diagnostics are empty");
    let metadata_storage = document
        .get("metadata_storage")
        .expect("Local metadata storage diagnostics are missing");
    for group in [metadata, metadata_storage].into_iter().chain(groups) {
        assert!(group.is_object(), "Local group diagnostics are missing");
        for field in [
            "leader",
            "term",
            "voters",
            "joint_configs",
            "bootstrap_voters",
            "membership",
        ] {
            assert!(
                group.get(field).is_none(),
                "Local diagnostics fabricated Raft field {field}"
            );
        }
    }
}

async fn assert_no_child_processes(pid: u32) {
    #[cfg(target_os = "linux")]
    {
        let mut threads = tokio::fs::read_dir(format!("/proc/{pid}/task"))
            .await
            .unwrap();
        while let Some(thread) = threads.next_entry().await.unwrap() {
            match tokio::fs::read_to_string(thread.path().join("children")).await {
                Ok(children) => assert!(
                    children.trim().is_empty(),
                    "Combined spawned child processes: {children}"
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    assert!(
                        !tokio::fs::try_exists(thread.path()).await.unwrap(),
                        "A live Combined thread has no child process observation"
                    );
                }
                Err(error) => panic!("Failed to observe Combined child processes: {error}"),
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        let output = tokio::process::Command::new("pgrep")
            .args(["-P", &pid.to_string()])
            .output()
            .await
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(1),
            "Combined spawned child processes: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_quic_https_and_management_belong_to_one_pid_without_internal_sockets() {
    let fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    let config = fixture.configs.get(&1).unwrap();
    let mut process = start_process(&fixture, 1).await;
    let pid = process.id().unwrap();
    let browser = https_client(&fixture);
    let response = browser.get(console_url(config, "/")).send().await.unwrap();
    assert!(response.status().is_success());
    assert!(response.text().await.unwrap().contains("fixture.js"));
    let response = browser
        .post(console_url(config, "/api/v1/session"))
        .header(
            "origin",
            config
                .console
                .as_ref()
                .unwrap()
                .session
                .public_origin
                .as_str(),
        )
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&serde_json::json!({"token":TOKEN})).unwrap())
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Native Console login failed"
    );
    let identity: serde_json::Value =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(
        identity.pointer("/capabilities/execution"),
        Some(&serde_json::json!("local"))
    );
    let client = fixture.client(1).await;
    create_logical_resources(&client).await;
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    producer.send("same-process").await.unwrap();
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "same-process")
        .build()
        .await
        .unwrap();
    let delivery = consumer.receive().await.unwrap();
    consumer.ack(&delivery).await.unwrap();
    let udp = process_ports(pid, "udp").await;
    let tcp = process_ports(pid, "tcp").await;
    assert_no_child_processes(pid).await;
    assert!(process.try_wait().unwrap().is_none());
    assert_eq!(
        udp,
        BTreeSet::from([config.listen.port()]),
        "Combined opened an internal QUIC endpoint"
    );
    assert_eq!(
        tcp,
        BTreeSet::from([
            config.management_listen.port(),
            config.console.as_ref().unwrap().listen.port()
        ])
    );
    let response = management_response(config.management_listen, "/diagnostics")
        .await
        .unwrap();
    let (_, body) = response.split_once("\r\n\r\n").unwrap();
    let diagnostics: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_local_diagnostics(&diagnostics);
    tokio::fs::write(fixture.directory.join("combined-process.json"), serde_json::to_vec_pretty(
        &serde_json::json!({"pid":pid,"udp_ports":udp,"tcp_listeners":tcp,"diagnostics":diagnostics})
    ).unwrap()).await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    process.kill().await.unwrap();
    let _ = process.wait().await.unwrap();
    assert_ports_released(config).await;
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_required_asset_loss_fails_readiness_and_cannot_reopen_after_restore() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let config = fixture.configs.get(&1).unwrap().clone();
    let browser = https_client(&fixture);
    let client = fixture.client(1).await;
    create_logical_resources(&client).await;
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let response = browser
        .get(console_url(&config, "/missing.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    response.bytes().await.unwrap();
    let response = browser
        .get(console_url(&config, "/assets/fixture.js"))
        .header("range", "bytes=invalid")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE
    );
    response.bytes().await.unwrap();
    assert!(
        management_response(config.management_listen, "/readyz")
            .await
            .unwrap()
            .starts_with("HTTP/1.1 200")
    );
    let asset = config
        .console
        .as_ref()
        .unwrap()
        .static_dir
        .join("assets/fixture.js");
    let original = tokio::fs::read(&asset).await.unwrap();
    tokio::fs::remove_file(&asset).await.unwrap();
    let _ = browser
        .get(console_url(&config, "/assets/fixture.js"))
        .send()
        .await;
    let node = fixture.running.remove(&1).unwrap();
    tokio::time::timeout(Duration::from_secs(5), node.wait_for_shutdown_request())
        .await
        .unwrap();
    assert!(
        management_response(config.management_listen, "/readyz")
            .await
            .unwrap()
            .starts_with("HTTP/1.1 503"),
        "Required asset failure did not close process readiness"
    );
    let refused =
        tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(300))
            .unwrap()
            .scope(producer.send(
                tidemq_sdk::Message::new("must-not-commit-after-failure").key("failure-probe"),
            ))
            .await
            .expect_err("A failed Combined process accepted a new publish");
    tokio::fs::write(&asset, original).await.unwrap();
    let _ = browser
        .get(console_url(&config, "/assets/fixture.js"))
        .send()
        .await;
    assert!(
        management_response(config.management_listen, "/readyz")
            .await
            .unwrap()
            .starts_with("HTTP/1.1 503"),
        "A late asset restore reopened failed admission"
    );
    let _ = producer.close().await;
    client.close().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(6), node.shutdown())
            .await
            .unwrap()
            .is_err()
    );
    assert_ports_released(&config).await;
    drop(browser);
    fixture.restart(1).await;
    let client = fixture.client(1).await;
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    // A committed marker on the same shard proves recovery is live before checking absence.
    let marker = producer
        .send(tidemq_sdk::Message::new("committed-after-recovery").key("failure-probe"))
        .await
        .unwrap()
        .message_id();
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "after-asset-failure")
        .build()
        .await
        .unwrap();
    let delivery = consumer.receive().await.unwrap();
    assert_eq!(
        delivery.message_id(),
        marker,
        "A post-failure publish survived restart"
    );
    assert_eq!(delivery.payload(), b"committed-after-recovery");
    consumer.ack(&delivery).await.unwrap();
    let delivery = tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(300))
        .unwrap()
        .scope(consumer.receive())
        .await;
    assert!(delivery.is_err(), "A post-failure publish survived restart");
    tokio::fs::write(
        fixture.directory.join("combined-asset-failure.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "failed_send_outcome_unknown": refused.outcome_unknown(),
            "recovery_marker": marker.to_string(),
            "failed_publish_absent_after_recovery": true,
            "readiness_failed_after_restore": true
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
    assert_ports_released(&config).await;
}
