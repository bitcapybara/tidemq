use std::collections::{BTreeMap, BTreeSet};
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

use bytes::Bytes;
use tidemq_protocol::{Id, public};
use tidemq_sdk::Client;
use tokio::time::Instant;

#[path = "runtime_support/broker_failover.rs"]
mod broker_failover;
#[path = "runtime_support/combined.rs"]
mod combined;
#[path = "runtime_support/read_batches.rs"]
mod read_batches;
#[path = "runtime_support/scalable.rs"]
mod scalable;
#[path = "runtime_support/scaling.rs"]
mod scaling;
#[path = "runtime_support/topic_deletion.rs"]
mod topic_deletion;
#[path = "runtime_support/tracing.rs"]
mod tracing_tests;

// Shared S3 fixtures include additional helpers exercised by their own integration suites.
#[allow(dead_code)]
#[path = "lifecycle_support/rustfs.rs"]
mod rustfs;
#[path = "s3_support/mod.rs"]
mod s3;

const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";

#[path = "runtime_support/fixture.rs"]
mod fixture;
use fixture::*;

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
        .stdout(output.try_clone().unwrap())
        .stderr(output)
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
    create_logical_resources(&client).await;
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let message = || tidemq_sdk::Message::new("process-body").key("process-key");
    let first = producer.send(message()).await.unwrap().message_id();
    let received = consumer.receive().await.unwrap();
    assert_eq!(received.message_id(), first);
    consumer.ack(&received).await.unwrap();
    process.kill().await.unwrap();
    assert!(!process.wait().await.unwrap().success());
    let failure = tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(200))
        .unwrap()
        .scope(producer.send(message()))
        .await
        .unwrap_err();
    assert!(failure.outcome_unknown());
    let pending = producer.pending_sends().await.unwrap();
    assert_eq!(pending.len(), 1);
    let pending = pending.first().unwrap();
    let started = Instant::now();
    let mut process = start_process(&fixture, 1).await;
    let receipt = pending.retry().await.unwrap();
    assert_ne!(receipt.message_id(), first);
    assert_eq!(
        pending.retry().await.unwrap().message_id(),
        receipt.message_id()
    );
    assert!(producer.pending_sends().await.unwrap().is_empty());
    let second = consumer.receive().await.unwrap();
    assert_eq!(second.message_id(), receipt.message_id());
    assert_eq!(second.payload(), b"process-body");
    consumer.ack(&second).await.unwrap();
    eprintln!(
        "Original SDK producer and consumer recovered after process restart in {:?}",
        started.elapsed()
    );
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    process.kill().await.unwrap();
    let _ = process.wait().await.unwrap();
    fixture.close().await;
}

const RUNTIME_TOPIC: &str = "test/app/events";

async fn create_logical_resources(client: &Client) -> tidemq_sdk::TopicInfo {
    let tenant = client.admin().create_tenant("test").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client.admin().create_topic(RUNTIME_TOPIC).await.unwrap()
}

async fn logical_send(
    producer: &tidemq_sdk::Producer,
    body: &'static [u8],
) -> tidemq_sdk::MessageId {
    let result = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(2))
        .unwrap()
        .scope(producer.send(tidemq_sdk::Message::new(Bytes::from_static(body))))
        .await;
    if let Ok(receipt) = result {
        return receipt.message_id();
    }
    let pending = producer.pending_sends().await.unwrap();
    assert_eq!(
        pending.len(),
        1,
        "The original logical send lost its recovery handle: {result:?}"
    );
    logical_retry(pending.first().unwrap(), deadline()).await
}

async fn logical_retry(pending: &tidemq_sdk::PendingSend, until: Instant) -> tidemq_sdk::MessageId {
    loop {
        let result = tidemq_sdk::RequestContext::with_deadline(
            until.min(Instant::now() + Duration::from_secs(2)),
        )
        .scope(pending.retry())
        .await;
        if let Ok(receipt) = result {
            assert_eq!(
                pending.retry().await.unwrap().message_id(),
                receipt.message_id()
            );
            return receipt.message_id();
        }
        assert!(
            Instant::now() < until,
            "The original send could not recover: {result:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn logical_consume(
    consumer: &tidemq_sdk::QueueConsumer,
    identities: &[tidemq_sdk::MessageId],
    body: &[u8],
) {
    let expected: std::collections::HashSet<_> = identities.iter().copied().collect();
    let mut confirmed = std::collections::HashSet::new();
    let until = Instant::now() + Duration::from_secs(45);
    while confirmed.len() < expected.len() {
        let result = tidemq_sdk::RequestContext::with_deadline(
            until.min(Instant::now() + Duration::from_secs(2)),
        )
        .scope(consumer.receive())
        .await;
        assert!(
            Instant::now() < until,
            "The original consumer did not recover every identity: {result:?}"
        );
        let message = match result {
            Ok(message) => message,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(25)).await;
                continue;
            }
        };
        assert!(
            expected.contains(&message.message_id()),
            "The consumer returned an unexpected identity"
        );
        assert_eq!(message.payload(), body);
        let result = tidemq_sdk::RequestContext::with_deadline(until)
            .scope(consumer.ack(&message))
            .await;
        if let Err(error) = result {
            if error.kind() == tidemq_sdk::ErrorKind::StaleDelivery {
                continue;
            }
            let pending = consumer.pending_acks().await.unwrap();
            assert_eq!(
                pending.len(),
                1,
                "The original ACK lost its recovery handle: {error:?}"
            );
            let pending = pending.first().unwrap();
            let acknowledged = loop {
                let result = tidemq_sdk::RequestContext::with_deadline(
                    until.min(Instant::now() + Duration::from_secs(2)),
                )
                .scope(pending.retry())
                .await;
                match result {
                    Ok(tidemq_sdk::AckReceipt::Acknowledged) => break true,
                    Err(error) if error.kind() == tidemq_sdk::ErrorKind::StaleDelivery => {
                        break false;
                    }
                    result => assert!(
                        Instant::now() < until,
                        "The original ACK did not recover: {result:?}"
                    ),
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            };
            if !acknowledged {
                continue;
            }
        }
        confirmed.insert(message.message_id());
    }
    assert_eq!(confirmed, expected);
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
    create_logical_resources(&client).await;
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let mut initial = Vec::new();
    for _ in 0..4 {
        initial.push(logical_send(&producer, b"quorum-persisted").await);
    }
    logical_consume(&consumer, &initial, b"quorum-persisted").await;
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
    let next = logical_send(&producer, b"after-one-storage-crash").await;
    assert!(!initial.contains(&next));
    logical_consume(&consumer, &[next], b"after-one-storage-crash").await;
    eprintln!(
        "Independent RF3 single-storage recovery completed in {:?}",
        single_failure.elapsed()
    );
    let second = [21, 22, 23]
        .into_iter()
        .find(|node| *node != leader)
        .unwrap();
    processes.get_mut(&second).unwrap().kill().await.unwrap();
    let result = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(1))
        .unwrap()
        .scope(producer.send("minority-unknown"))
        .await;
    assert!(
        result.is_err(),
        "A data minority must never issue a successful producer receipt"
    );
    let pending = producer.pending_sends().await.unwrap();
    assert_eq!(pending.len(), 1);
    let original = pending.first().unwrap();
    for node in [leader, second] {
        processes.insert(node, spawn_process(&fixture, node).await);
    }
    for node in [leader, second] {
        wait_process(&fixture, node, processes.get_mut(&node).unwrap()).await;
    }
    let last = logical_retry(original, deadline()).await;
    assert!(!initial.contains(&last));
    assert_ne!(last, next);
    logical_consume(&consumer, &[last], b"minority-unknown").await;
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
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
    let topic = create_logical_resources(&client).await.id().to_string();
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let receipt = producer
        .send(tidemq_sdk::Message::new("durable-body").key("key"))
        .await
        .unwrap();
    let delivery = consumer.receive().await.unwrap();
    assert_eq!(delivery.message_id(), receipt.message_id());
    assert_eq!(delivery.payload(), b"durable-body");
    consumer.ack(&delivery).await.unwrap();
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
    let ownership = http(
        fixture.running.get(&1).unwrap().management_address,
        &format!("/ownership/{topic}/0"),
    )
    .await;
    assert!(ownership.starts_with("HTTP/1.1 200"));
    let ownership: serde_json::Value =
        serde_json::from_str(ownership.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(ownership.get("source").unwrap(), "local_applied");
    assert_eq!(ownership.get("phase").unwrap(), "active");
    assert_eq!(ownership.get("owner_node").unwrap(), 1);
    assert!(ownership.get("epoch").unwrap().as_u64().unwrap() > 0);
    assert_eq!(ownership.get("partition").unwrap(), &format!("{topic}/0"));
    let invalid = http(
        fixture.running.get(&1).unwrap().management_address,
        "/ownership/invalid/0",
    )
    .await;
    assert!(invalid.starts_with("HTTP/1.1 400"));
    client.close().await.unwrap();
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.stop(1).await;
    fixture.restart(1).await;
    let client = fixture.client(1).await;
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let after = producer.send("after-restart").await.unwrap();
    let message = consumer.receive().await.unwrap();
    assert_eq!(message.message_id(), after.message_id());
    assert_eq!(message.payload(), b"after-restart");
    consumer.ack(&message).await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
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
    let topic = create_logical_resources(&client).await;
    let second_client = fixture.client(42).await;
    let consumer = second_client
        .new_queue_consumer(RUNTIME_TOPIC, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let mut work = tokio::task::JoinSet::new();
    for value in 0..8_u8 {
        let producer = producer.clone();
        work.spawn(async move {
            let receipt = producer
                .send(tidemq_sdk::Message::new(vec![value]).key("key"))
                .await
                .unwrap();
            (value, receipt.message_id())
        });
    }
    let mut expected = std::collections::HashMap::new();
    while let Some(result) = work.join_next().await {
        let (value, identity) = result.unwrap();
        assert!(expected.insert(identity, value).is_none());
    }
    assert_eq!(expected.len(), 8);
    let receiving_until = Instant::now() + Duration::from_secs(30);
    let mut seen = std::collections::HashSet::new();
    let mut redeliveries = 0;
    while seen.len() < expected.len() {
        let delivery = tidemq_sdk::RequestContext::with_deadline(receiving_until)
            .scope(consumer.receive())
            .await
            .unwrap();
        let expected_body = expected.get(&delivery.message_id()).unwrap();
        assert_eq!(delivery.key(), Some(b"key".as_slice()));
        assert_eq!(delivery.payload(), &[*expected_body]);
        consumer.ack(&delivery).await.unwrap();
        redeliveries += usize::from(!seen.insert(delivery.message_id()));
    }
    assert_eq!(seen, expected.keys().copied().collect());
    eprintln!("Parallel split-role receive verified all 8 identities; redeliveries={redeliveries}");
    let control = fixture.running.get(&11).unwrap();
    let partition = tidemq::metadata::PartitionKey {
        topic_id: topic.id().to_string().parse().unwrap(),
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
    let identity = logical_send(&producer, b"after-handoff").await;
    assert!(!seen.contains(&identity));
    logical_consume(&consumer, &[identity], b"after-handoff").await;
    client.admin().begin_node_drain(12).await.unwrap();
    let blockers = client.admin().node_drain_status(12).await.unwrap();
    assert!(blockers.metadata_voter);
    assert!(!blockers.durable_state_drained());
    client.admin().begin_node_drain(31).await.unwrap();
    let drained = client.admin().node_drain_status(31).await.unwrap();
    assert!(drained.durable_state_drained());
    client.admin().retire_node(31).await.unwrap();
    assert!(client.admin().node_drain_status(31).await.unwrap().retired);
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    second_client.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stable_udp_endpoint_recovers_original_sdk_after_gateway_crash_with_network_faults() {
    static BODY: [u8; 32 * 1024] = [b'p'; 32 * 1024];
    let mut fixture = Fixture::new(
        &[
            (1, Role::Metadata),
            (21, Role::Storage),
            (31, Role::Broker),
            (41, Role::Gateway),
            (42, Role::Gateway),
        ],
        &[1],
        &[21],
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
    let client = Client::builder(
        format!("localhost:{}", proxy.address.port()),
        tidemq_sdk::TlsTrust::ca_pem(fixture.roots.clone()),
        tidemq_sdk::Credentials::token(TOKEN),
    )
    .request_timeout(Duration::from_secs(20))
    .build()
    .await
    .unwrap();
    create_logical_resources(&client).await;
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let mut initial = Vec::new();
    for _ in 0..3 {
        initial.push(logical_send(&producer, &BODY).await);
    }
    logical_consume(&consumer, &initial, &BODY).await;
    let connections = client.transport_metrics().snapshot().connections_total;
    first_gateway.kill().await.unwrap();
    let _ = first_gateway.wait().await.unwrap();
    proxy.switch();
    proxy.set_mtu(1199);
    let started = Instant::now();
    let blocked =
        tidemq_sdk::RequestContext::with_deadline(Instant::now() + Duration::from_secs(4))
            .scope(producer.send("after-gateway-crash"))
            .await;
    assert!(
        blocked.is_err(),
        "The MTU blackhole must not issue a successful receipt"
    );
    let pending = producer.pending_sends().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert!(
        proxy.snapshot().over_mtu > 0,
        "The blackhole must reject actual QUIC datagrams"
    );
    proxy.set_mtu(1200);
    let recovered = logical_retry(pending.first().unwrap(), deadline()).await;
    assert!(!initial.contains(&recovered));
    logical_consume(&consumer, &[recovered], b"after-gateway-crash").await;
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
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
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
    maintenance_test_logging();
    let rustfs = rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    enable_objects(&mut fixture, &rustfs).await;
    let config = fixture.configs.get_mut(&1).unwrap();
    config.runtime.archive_interval_ms = 60_000;
    config.runtime.retention.minimum_age_ms = 0;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let topic = create_logical_resources(&client).await;
    client
        .new_stream_consumer(RUNTIME_TOPIC, "history")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let first = producer
        .send(tidemq_sdk::Message::new("expired-prefix").key("retention"))
        .await
        .unwrap()
        .message_id();
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
    let second = producer
        .send(tidemq_sdk::Message::new("retained-suffix").key("retention"))
        .await
        .unwrap()
        .message_id();
    let partition = tidemq::metadata::PartitionKey {
        topic_id: topic.id().to_string().parse().unwrap(),
        partition: 0,
    };
    let node = fixture.running.get(&1).unwrap();
    node.archive(
        2,
        vec![tidemq::lifecycle::RangeRequest {
            partition,
            offsets: tidemq_protocol::state::OffsetRange { start: 0, end: 2 },
        }],
        deadline(),
    )
    .await
    .unwrap();
    node.reclaim_local(partition, 2, deadline()).await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.stop(1).await;
    fixture
        .configs
        .get_mut(&1)
        .unwrap()
        .runtime
        .archive_interval_ms = 1000;
    fixture.restart(1).await;
    let client = fixture.client(1).await;
    let consumer = client
        .new_stream_consumer(RUNTIME_TOPIC, "history")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let expired = consumer.receive().await.unwrap();
    assert_eq!(expired.message_id(), first);
    assert_eq!(expired.payload(), b"expired-prefix");
    consumer.ack_cumulative(&expired).await.unwrap();
    // Discard prefetch so the next member must read the compacted object's retained suffix.
    consumer.close().await.unwrap();
    drop(expired);
    wait_runtime_counter(&fixture, 1, "tidemq_lifecycle_compacted_total ").await;
    wait_runtime_counter(&fixture, 1, "tidemq_lifecycle_deleted_total ").await;
    let consumer = client
        .new_stream_consumer(RUNTIME_TOPIC, "history")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let retained = consumer.receive().await.unwrap();
    assert_eq!(retained.message_id(), second);
    assert_eq!(retained.payload(), b"retained-suffix");
    consumer.ack_cumulative(&retained).await.unwrap();
    consumer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

async fn run_runtime_s3(nodes: &[(u64, Role)], metadata: &[u64], storage: &[u64]) {
    maintenance_test_logging();
    let rustfs = rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = Fixture::new(nodes, metadata, storage).await;
    enable_objects(&mut fixture, &rustfs).await;
    if nodes.len() == 1 {
        for config in fixture.configs.values_mut() {
            config.runtime.groups.push(GroupConfig {
                group_id: 3,
                voters: Vec::new(),
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
    create_logical_resources(&client).await;
    client
        .new_queue_consumer(RUNTIME_TOPIC, "jobs")
        .build()
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let receipts = producer
        .send_batch(vec![
            tidemq_sdk::Message::new("cold-body").key("cold-key");
            33
        ])
        .await
        .unwrap();
    let identities = receipts
        .outcomes
        .into_iter()
        .map(|result| result.unwrap().message_id())
        .collect::<Vec<_>>();
    let context = tidemq_protocol::topic::MessageMetadata {
        key_present: true,
        routing_hash: tidemq_protocol::topic::routing_hash(
            Some(b"cold-key"),
            Id::new_v7().unwrap(),
            0,
        )
        .unwrap(),
        properties: Default::default(),
    };
    let stored = tidemq::producer::MessageData {
        key: Bytes::from_static(b"cold-key"),
        body: Bytes::from_static(b"cold-body"),
        context: postcard::to_stdvec(&context).unwrap().into(),
    };
    let expected_reclaimed = 33 * stored.logical_bytes().unwrap();
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
        if reclaimed >= expected_reclaimed {
            break;
        }
        if Instant::now() >= until {
            let mut state = BTreeMap::new();
            for (id, node) in &fixture.running {
                let metrics = tokio::time::timeout(
                    Duration::from_millis(500),
                    http(node.management_address, "/metrics"),
                )
                .await
                .ok();
                let diagnostics = tokio::time::timeout(
                    Duration::from_millis(500),
                    http(node.management_address, "/diagnostics"),
                )
                .await
                .ok();
                state.insert(
                    *id,
                    serde_json::json!({"metrics": metrics, "diagnostics": diagnostics}),
                );
            }
            let evidence = fixture.directory.join("s3-reclamation-timeout.json");
            tokio::fs::write(&evidence, serde_json::to_vec_pretty(&serde_json::json!({
                "expected_reclaimed_bytes": expected_reclaimed, "observed_reclaimed_bytes": reclaimed, "nodes": state,
            })).unwrap()).await.unwrap();
            panic!(
                "Default background archive did not publish, checkpoint and reclaim; inspect {}",
                evidence.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "jobs")
        .build()
        .await
        .unwrap();
    logical_consume(&consumer, &identities, b"cold-body").await;
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
    assert!(published >= 1);
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.close().await;
}

fn maintenance_test_logging() {
    if std::env::var_os("TIDEMQ_TEST_MAINTENANCE_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter("tidemq::runtime::maintenance=debug,tidemq::runtime::tenant_quota=debug,tidemq::server::dispatch=debug,tidemq_sdk=debug")
            .try_init();
    }
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
    let target_capacity = wait_scaling_diagnostic(&fixture, 24, |value| {
        value["placement_capacity_available"] == true
    })
    .await;
    let available = target_capacity["capacity"]["data_filesystem"]["available_bytes"]
        .as_u64()
        .unwrap();
    let reserved = target_capacity["capacity"]["reserved_storage_bytes"]
        .as_u64()
        .unwrap();
    assert!(
        available.saturating_sub(reserved) >= 1024 * 1024 * 1024,
        "The scaling fixture lacks the required disk headroom before copying: available={available}, reserved={reserved}, minimum_extra=1073741824"
    );
    let client = fixture.client(41).await;
    create_logical_resources(&client).await;
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let message = || tidemq_sdk::Message::new("scaling-body").key("scaling-key");
    let receipts = producer
        .send_batch(vec![message(), message()])
        .await
        .unwrap();
    let expected = receipts
        .outcomes
        .into_iter()
        .map(|result| result.unwrap().message_id())
        .collect::<Vec<_>>();
    let first = consumer.receive().await.unwrap();
    assert!(expected.contains(&first.message_id()));
    consumer.ack(&first).await.unwrap();
    let held = consumer.receive().await.unwrap();
    assert!(expected.contains(&held.message_id()));
    assert_ne!(held.message_id(), first.message_id());
    if let Some(child) = storage_process.as_mut() {
        child.kill().await.unwrap();
        assert!(!child.wait().await.unwrap().success());
    }
    let migration = async {
        if automatic {
            wait_scaling_diagnostic(&fixture, 21, scaling_group_is_replaced).await;
            None
        } else {
            // This fresh fixture bootstraps generation one; freeze the canonical identity once.
            let original = tidemq::scaling::MembershipPlan::new(
                fixture
                    .configs
                    .get(&11)
                    .unwrap()
                    .cluster_id
                    .parse()
                    .unwrap(),
                &tidemq::metadata::DataGroup {
                    group_id: 2,
                    generation: 1,
                    placement: tidemq::metadata::GroupPlacement::Raft {
                        voters: [21, 22, 23].into(),
                    },
                },
                [21, 22, 24].into(),
            )
            .unwrap();
            let node = fixture.running.get(&11).unwrap();
            let deadline = Instant::now() + Duration::from_secs(30);
            Some(
                scaling::replace_original(&original, deadline, |deadline| {
                    node.replace_replica(2, 23, 24, deadline)
                })
                .await
                .unwrap(),
            )
        }
    };
    let traffic = async {
        let ack = consumer.ack(&held).await;
        let produced = producer.send(message()).await;
        (ack, produced)
    };
    let (plan, (ack, produced)) = tokio::join!(migration, traffic);
    if let Some(plan) = plan {
        assert_eq!(plan.previous(), &[21, 22, 23].into());
        assert_eq!(plan.target(), &[21, 22, 24].into());
        let repeated = match fixture
            .running
            .get(&11)
            .unwrap()
            .replace_replica(2, 23, 24, deadline())
            .await
        {
            Ok(repeated) => repeated,
            Err(error) => {
                let evidence_deadline = Instant::now() + Duration::from_secs(2);
                let source = scaling_diagnostic(&fixture, 21, evidence_deadline)
                    .await
                    .unwrap_or_default();
                let context =
                    scaling_diagnostic_context(&fixture, 21, &source, evidence_deadline).await;
                let error = anyhow::Error::new(error);
                panic!(
                    "Completed replica replacement replay failed: plan={plan:?}, error={error:#}, context={context:?}"
                );
            }
        };
        assert_eq!(
            repeated, plan,
            "A completion retry must retain its original plan identity"
        );
    }
    wait_scaling_diagnostic(&fixture, 21, scaling_group_is_replaced).await;
    let ack = match ack {
        Ok(ack) => ack,
        Err(_) => consumer.ack(&held).await.unwrap(),
    };
    assert_eq!(ack, tidemq_sdk::AckReceipt::Acknowledged);
    let identity = match produced {
        Ok(receipt) => receipt.message_id(),
        Err(error) if error.outcome_unknown() => {
            let pending = producer.pending_sends().await.unwrap();
            assert_eq!(pending.len(), 1);
            logical_retry(pending.first().unwrap(), deadline()).await
        }
        Err(error) => panic!("Replica replacement rejected admitted traffic: {error:?}"),
    };
    assert!(!expected.contains(&identity));
    assert!(producer.pending_sends().await.unwrap().is_empty());
    logical_consume(&consumer, &[identity], b"scaling-body").await;
    if let Err(error) = consumer.close().await {
        let mut diagnostics = BTreeMap::new();
        for (node, running) in &fixture.running {
            diagnostics.insert(
                *node,
                http(running.management_address, "/diagnostics").await,
            );
        }
        let path = fixture.directory.join("scaling-close-failure.json");
        tokio::fs::write(
            &path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "error": format!("{error:?}"), "diagnostics": diagnostics,
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        panic!(
            "Replica replacement member cleanup failed; inspect {}",
            path.display()
        );
    }
    producer.close().await.unwrap();
    client.close().await.unwrap();
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
) -> serde_json::Value {
    let until = Instant::now() + Duration::from_secs(30);
    let mut last = serde_json::Value::Null;
    loop {
        if let Some(value) = scaling_diagnostic(fixture, node, until).await {
            if predicate(&value) {
                return value;
            }
            last = value;
        }
        if Instant::now() >= until {
            let context = scaling_diagnostic_context(
                fixture,
                node,
                &last,
                Instant::now() + Duration::from_secs(2),
            )
            .await;
            panic!("Scaling diagnostic did not satisfy its predicate: {context:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn scaling_diagnostic_context(
    fixture: &Fixture,
    node: u64,
    value: &serde_json::Value,
    deadline: Instant,
) -> BTreeMap<u64, serde_json::Value> {
    let mut context = BTreeMap::from([(node, scaling_diagnostic_summary(value))]);
    for observed in [11, 12, 13, 21, 24] {
        if observed != node {
            let value = scaling_diagnostic(fixture, observed, deadline)
                .await
                .unwrap_or_default();
            context.insert(observed, scaling_diagnostic_summary(&value));
        }
    }
    context
}

async fn scaling_diagnostic(
    fixture: &Fixture,
    node: u64,
    deadline: Instant,
) -> Option<serde_json::Value> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let address = fixture.running.get(&node)?.management_address;
    tokio::time::timeout_at(
        deadline.min(Instant::now() + Duration::from_secs(1)),
        async {
            let mut connection = tokio::net::TcpStream::connect(address).await.ok()?;
            connection
                .write_all(b"GET /diagnostics HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .ok()?;
            let mut response = String::new();
            connection
                .take(128 * 1024)
                .read_to_string(&mut response)
                .await
                .ok()?;
            let (_, body) = response.split_once("\r\n\r\n")?;
            serde_json::from_str(body).ok()
        },
    )
    .await
    .ok()?
}

fn scaling_diagnostic_summary(value: &serde_json::Value) -> serde_json::Value {
    let groups: Vec<_> = value["data_groups"]
        .as_array()
        .into_iter()
        .flatten()
        .take(16)
        .map(|group| {
            serde_json::json!({
                "group": group["group_id"],
                "leader": group["leader"],
                "voters": group["voters"],
                "joint_configs": group["joint_configs"],
            })
        })
        .collect();
    serde_json::json!({
        "ready": value["ready"],
        "placement": value["placement"],
        "capacity_available": value["placement_capacity_available"],
        "disk_available_bytes": value["capacity"]["data_filesystem"]["available_bytes"],
        "reserved_storage_bytes": value["capacity"]["reserved_storage_bytes"],
        "quota_blocked": value["quota"]["blocked"],
        "quota_sample_age_ms": value["quota"]["sample_age_ms"],
        "metadata_revision": value["metadata"]["revision"],
        "metadata_applied": value["metadata_storage"]["applied_log"],
        "metadata_vote": value["metadata_storage"]["vote"],
        "data_groups": groups,
    })
}
