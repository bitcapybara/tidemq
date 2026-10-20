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

use tidemq_sdk::{
    Bytes, Client, ClientConfig, ClientTls, Consumer, ConsumerConfig, Id, PeerTarget, Producer,
    public,
};
use tokio::time::Instant;

#[path = "runtime_support/broker_failover.rs"]
mod broker_failover;
#[path = "runtime_support/combined.rs"]
mod combined;
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
    let (topic, subscription) = create(&client).await;
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
        .await
        .unwrap();
    let message = || public::Message {
        key: Bytes::from_static(b"process-key"),
        body: Bytes::from_static(b"process-body"),
    };
    producer.produce(vec![message()]).await.unwrap();
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    assert_eq!(consumer.fill().await.unwrap(), 1);
    let first = consumer.next().await.unwrap().unwrap();
    assert_eq!(consumer.ack(&first).await.unwrap().acknowledged_before, 1);
    process.kill().await.unwrap();
    assert!(!process.wait().await.unwrap().success());
    let failure =
        tidemq_sdk::RequestContext::with_deadline(Instant::now() + Duration::from_millis(200))
            .scope(producer.produce(vec![message()]))
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
    let receipt = producer.retry(batch).await.unwrap();
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
    assert_eq!(consumer.fill().await.unwrap(), 1);
    let second = consumer.next().await.unwrap().unwrap();
    assert_eq!(second.offset(), 1);
    assert_eq!(consumer.ack(&second).await.unwrap().acknowledged_before, 2);
    eprintln!(
        "Original SDK producer and consumer recovered after process restart in {:?}",
        started.elapsed()
    );
    client.close().await;
    process.kill().await.unwrap();
    let _ = process.wait().await.unwrap();
    fixture.close().await;
}

async fn durable_produce(producer: &Producer, body: &'static [u8]) -> u64 {
    let until = Instant::now() + Duration::from_secs(20);
    if let Ok(receipt) =
        tidemq_sdk::RequestContext::with_deadline(Instant::now() + Duration::from_secs(2))
            .scope(producer.produce(vec![public::Message {
                key: Bytes::new(),
                body: Bytes::from_static(body),
            }]))
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
        let result = tidemq_sdk::RequestContext::with_deadline(
            until.min(Instant::now() + Duration::from_secs(2)),
        )
        .scope(producer.retry(batch))
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

async fn durable_consume(consumer: &Consumer, offsets: std::ops::Range<u64>, body: &[u8]) {
    // A lost durable Read reply can retain deliveries for the full default lease.
    let budget = Duration::from_millis(tidemq::subscription::Limits::default().ack_timeout_ms)
        + Duration::from_secs(20);
    let until = Instant::now() + budget;
    let mut previous = String::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut acknowledged = offsets.start;
    while acknowledged < offsets.end {
        let result = tidemq_sdk::RequestContext::with_deadline(
            until.min(Instant::now() + Duration::from_secs(2)),
        )
        .scope(consumer.recv())
        .await;
        if let Ok(Some(delivery)) = result {
            assert!(offsets.contains(&delivery.offset()));
            assert_eq!(delivery.body(), body);
            assert!(
                seen.insert(delivery.offset()),
                "Duplicate recovered delivery"
            );
            while seen.contains(&acknowledged) {
                acknowledged += 1;
            }
            assert_eq!(
                tidemq_sdk::RequestContext::with_deadline(until)
                    .scope(consumer.ack(&delivery))
                    .await
                    .unwrap()
                    .acknowledged_before,
                acknowledged
            );
            continue;
        }
        let detail = format!("{result:?}");
        if previous != detail {
            eprintln!("Consumer recovery for offsets {offsets:?}: {detail}");
            previous = detail;
        }
        assert!(
            Instant::now() < until,
            "Original consumer did not recover every offset: expected={offsets:?}, seen={seen:?}, last={result:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(seen, offsets.collect());
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
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
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
    )
    .await
    .unwrap();
    durable_consume(&consumer, 0..4, b"quorum-persisted").await;
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
    durable_consume(&consumer, 4..5, b"after-one-storage-crash").await;
    eprintln!(
        "Independent RF3 single-storage recovery completed in {:?}",
        single_failure.elapsed()
    );
    let second = [21, 22, 23]
        .into_iter()
        .find(|node| *node != leader)
        .unwrap();
    processes.get_mut(&second).unwrap().kill().await.unwrap();
    let result = tidemq_sdk::RequestContext::with_deadline(Instant::now() + Duration::from_secs(1))
        .scope(producer.produce(vec![public::Message {
            key: Bytes::new(),
            body: Bytes::from_static(b"minority-unknown"),
        }]))
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
        let status = tidemq_sdk::RequestContext::with_deadline(
            until.min(Instant::now() + Duration::from_secs(2)),
        )
        .scope(client.admin().subscription_status(topic, 0, subscription))
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
        match tidemq_sdk::RequestContext::with_deadline(
            retry_deadline.min(Instant::now() + Duration::from_secs(2)),
        )
        .scope(producer.retry(original))
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
    durable_consume(&consumer, 5..6, b"minority-unknown").await;
    client.close().await;
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
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
        .await
        .unwrap();
    let receipt = producer
        .produce(vec![public::Message {
            key: Bytes::from_static(b"key"),
            body: Bytes::from_static(b"durable-body"),
        }])
        .await
        .unwrap();
    assert_eq!(receipt.first_offset, 0);
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    assert_eq!(consumer.fill().await.unwrap(), 1);
    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(delivery.body(), b"durable-body");
    assert_eq!(
        consumer.ack(&delivery).await.unwrap().acknowledged_before,
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
    client.close().await;
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.stop(1).await;
    fixture.restart(1).await;
    let client = fixture.client(1).await;
    let status = client
        .admin()
        .subscription_status(topic, 0, subscription)
        .await
        .unwrap();
    assert_eq!(status.assignment.unwrap().acknowledged_before, 1);
    client.close().await;
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
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
        .await
        .unwrap();
    let mut work = tokio::task::JoinSet::new();
    for value in 0..8_u8 {
        let producer = producer.clone();
        work.spawn(async move {
            let receipt = producer
                .produce(vec![public::Message {
                    key: Bytes::from_static(b"key"),
                    body: Bytes::from(vec![value]),
                }])
                .await
                .unwrap();
            (value, receipt)
        });
    }
    let mut offsets = Vec::new();
    let mut expected_bodies = BTreeMap::new();
    while let Some(result) = work.join_next().await {
        let (value, receipt) = result.unwrap();
        offsets.push(receipt.first_offset);
        assert!(
            expected_bodies
                .insert(receipt.first_offset, value)
                .is_none()
        );
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
    )
    .await
    .unwrap();
    // At-least-once recovery can wait one default visibility lease after a lost Read reply.
    // Keep room for the original Read and final ACK, each with the fixture's 10-second RPC budget.
    let receiving_until = Instant::now()
        + Duration::from_millis(tidemq::subscription::Limits::default().ack_timeout_ms)
        + Duration::from_secs(20);
    let mut seen = BTreeSet::new();
    let mut redeliveries = 0;
    let mut acknowledged_before = 0;
    while seen.len() < expected_bodies.len() {
        let Some(delivery) = tidemq_sdk::RequestContext::with_deadline(receiving_until)
            .scope(consumer.recv())
            .await
            .expect("Parallel delivery must recover every committed identity within its original phase budget")
        else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let offset = delivery.offset();
        let expected_body = expected_bodies
            .get(&offset)
            .expect("Every delivered identity must match an original committed receipt");
        assert_eq!(delivery.key(), b"key");
        assert_eq!(delivery.body(), &[*expected_body]);
        redeliveries += usize::from(!seen.insert(offset));
        while seen.contains(&acknowledged_before) {
            acknowledged_before += 1;
        }
        let ack = tidemq_sdk::RequestContext::with_deadline(receiving_until)
            .scope(consumer.ack(&delivery))
            .await
            .unwrap();
        assert_eq!(ack.acknowledged_before, acknowledged_before);
    }
    assert_eq!(seen, (0..8).collect());
    eprintln!("Parallel split-role receive verified all 8 identities; redeliveries={redeliveries}");
    let status = client
        .admin()
        .subscription_status(topic, 0, subscription)
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
            .produce(vec![public::Message {
                key: Bytes::new(),
                body: Bytes::from_static(b"after-handoff")
            }])
            .await
            .unwrap()
            .first_offset,
        8
    );
    assert_eq!(consumer.fill().await.unwrap(), 1);
    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(delivery.offset(), 8);
    assert_eq!(
        consumer.ack(&delivery).await.unwrap().acknowledged_before,
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
    second_client.close().await;
    client.close().await;
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
    )
    .await
    .unwrap();
    let (topic, subscription) = create(&client).await;
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
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
    )
    .await
    .unwrap();
    durable_consume(&consumer, 0..3, &BODY).await;
    let connections = client.transport_metrics().snapshot().connections_total;
    first_gateway.kill().await.unwrap();
    let _ = first_gateway.wait().await.unwrap();
    proxy.switch();
    proxy.set_mtu(1199);
    let started = Instant::now();
    let blocked =
        tidemq_sdk::RequestContext::with_deadline(Instant::now() + Duration::from_secs(4))
            .scope(producer.produce(vec![public::Message {
                key: Bytes::new(),
                body: Bytes::from_static(b"after-gateway-crash"),
            }]))
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
    durable_consume(&consumer, 3..4, b"after-gateway-crash").await;
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
    client.close().await;
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
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
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
    )
    .await
    .unwrap();
    durable_consume(&consumer, 0..1, b"expired-prefix").await;
    wait_runtime_counter(&fixture, 1, "tidemq_lifecycle_compacted_total ").await;
    wait_runtime_counter(&fixture, 1, "tidemq_lifecycle_deleted_total ").await;
    assert_eq!(consumer.fill().await.unwrap(), 1);
    let retained = consumer.next().await.unwrap().unwrap();
    assert_eq!(retained.offset(), 1);
    assert_eq!(retained.body(), b"retained-suffix");
    assert_eq!(
        consumer.ack(&retained).await.unwrap().acknowledged_before,
        2
    );
    client.close().await;
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
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
        .await
        .unwrap();
    producer
        .produce(vec![
            public::Message {
                key: Bytes::from_static(b"cold-key"),
                body: Bytes::from_static(b"cold-body"),
            };
            33
        ])
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
    )
    .await
    .unwrap();
    for offset in 0..33 {
        let delivery = consumer.recv().await.unwrap().unwrap();
        assert_eq!(delivery.offset(), offset);
        assert_eq!(delivery.body(), b"cold-body");
        assert_eq!(
            consumer.ack(&delivery).await.unwrap().acknowledged_before,
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
    client.close().await;
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.close().await;
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
    let (topic, subscription) = create(&client).await;
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
        .await
        .unwrap();
    let message = || public::Message {
        key: Bytes::from_static(b"scaling-key"),
        body: Bytes::from_static(b"scaling-body"),
    };
    producer.produce(vec![message(), message()]).await.unwrap();
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    assert_eq!(consumer.fill().await.unwrap(), 2);
    let first = consumer.next().await.unwrap().unwrap();
    assert_eq!(consumer.ack(&first).await.unwrap().acknowledged_before, 1);
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
        let ack = consumer.ack(&held).await;
        let produced = producer.produce(vec![message()]).await;
        let read = consumer.fill().await;
        (ack, produced, read)
    };
    let (plan, (ack, produced, read)) = tokio::join!(migration, traffic);
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
            let receipt = producer.retry(batch).await.unwrap();
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
        Err(_) => producer.produce(vec![message()]).await.unwrap(),
    };
    assert_eq!(receipt.first_offset, 2);
    assert!(producer.pending_batches().await.unwrap().is_empty());
    let during = read.unwrap_or(0);
    assert_eq!(during + consumer.fill().await.unwrap(), 1);
    let delivered = consumer.next().await.unwrap().unwrap();
    assert_eq!(delivered.offset(), 2);
    assert_eq!(delivered.body(), b"scaling-body");
    assert_eq!(
        consumer.ack(&delivered).await.unwrap().acknowledged_before,
        3
    );
    assert!(consumer.next().await.unwrap().is_none());
    client.close().await;
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
