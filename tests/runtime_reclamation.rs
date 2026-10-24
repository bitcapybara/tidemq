use std::{
    collections::BTreeMap,
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use tidemq::{
    config::{Config, Role, TlsConfig},
    metadata::PartitionKey,
    runtime::{GroupConfig, PeerConfig, RunningNode, RuntimeConfig, test_hooks::InstalledPause},
};
use tidemq_sdk::{
    Bytes, Client, ClientConfig, ClientTls, Consumer, ConsumerConfig, Id, PeerTarget, Producer,
    public,
};
use tokio::time::Instant;

// The shared fixtures provide additional helpers used by other runtime suites.
#[allow(dead_code)]
#[path = "runtime_support/fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "lifecycle_support/rustfs.rs"]
mod rustfs;
#[path = "s3_support/mod.rs"]
mod s3;
use fixture::*;

const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";
const MESSAGES: u64 = 8;
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn create(client: &Client) -> (Id, Id) {
    let topic = client.admin().create_topic("events", 1).await.unwrap();
    let topic = Id::from_slice(&topic.topic_id).unwrap();
    let subscription = client
        .admin()
        .create_subscription(topic, "durable", 0, public::DeliveryMode::Parallel)
        .await
        .unwrap();
    (
        topic,
        Id::from_slice(&subscription.subscription_id).unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the verified RustFS 1.0.3 executable in TIDEMQ_TEST_RUSTFS_BIN"]
async fn first_hot_delivery_reloads_cold_after_concurrent_local_reclamation() {
    run_reclaimed_read(0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the verified RustFS 1.0.3 executable in TIDEMQ_TEST_RUSTFS_BIN"]
async fn hot_reclamation_returns_only_the_durable_prefix_before_resuming_cold() {
    run_reclaimed_read(1).await;
}

async fn run_reclaimed_read(paused_offset: u64) {
    let _serial = SERIAL.lock().await;
    let rustfs = rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    enable_objects(&mut fixture, &rustfs).await;
    fixture
        .configs
        .get_mut(&1)
        .unwrap()
        .runtime
        .archive_interval_ms = 60_000;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let (topic, subscription) = create(&client).await;
    let partition = PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let producer = Producer::open(client.clone(), topic, 0, Id::new_v7().unwrap())
        .await
        .unwrap();
    // Claim before writing: the same live assignment must keep H=0 across local reclamation.
    let consumer = Arc::new(
        Consumer::claim(
            client.clone(),
            topic,
            0,
            subscription,
            ConsumerConfig::default(),
        )
        .await
        .unwrap(),
    );
    let messages = (0..MESSAGES)
        .map(|offset| public::Message {
            key: Bytes::copy_from_slice(&offset.to_be_bytes()),
            body: Bytes::from(vec![0x5a; 1024]),
        })
        .collect();
    assert_eq!(
        producer.produce(messages).await.unwrap().message_count,
        MESSAGES as u32
    );
    let node = fixture.running.get(&1).unwrap();
    node.archive(
        2,
        vec![tidemq::lifecycle::RangeRequest {
            partition,
            offsets: tidemq_protocol::state::OffsetRange::new(0, MESSAGES).unwrap(),
        }],
        deadline(),
    )
    .await
    .unwrap();

    let hook = InstalledPause::hot_delivery(partition, paused_offset).unwrap();
    let until = deadline();
    let reading = {
        let consumer = consumer.clone();
        tokio::spawn(async move {
            tidemq_sdk::RequestContext::with_deadline(until)
                .scope(consumer.fill())
                .await
        })
    };
    hook.wait(until).await.unwrap();
    // The runtime now owns a valid hot body, but has not submitted its RecordDelivery.
    node.reclaim_local(partition, MESSAGES, until)
        .await
        .unwrap();
    hook.resume();
    let first = tokio::time::timeout_at(until, reading)
        .await
        .unwrap()
        .unwrap();
    let result = consume_all(&consumer, first, paused_offset, until).await;
    let assignment = client
        .admin()
        .subscription_status(topic, 0, subscription)
        .await
        .unwrap()
        .assignment
        .unwrap();
    let metrics = http(
        fixture.running.get(&1).unwrap().management_address,
        "/metrics",
    )
    .await;
    let conflicts = metrics
        .lines()
        .find_map(|line| line.strip_prefix("tidemq_runtime_read_hot_reclaim_conflicts_total "))
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0);
    client.close().await.unwrap();
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.close().await;
    eprintln!(
        "Reclaimed hot delivery offset {paused_offset}: conflicts={conflicts}, result={result:?}"
    );
    assert_eq!(
        conflicts, 1,
        "The regression must hit the actual hot delivery rejection once"
    );
    result.expect("Local body reclamation must not masquerade as logical OffsetExpired");
    assert_eq!(assignment.cold_before, 0);
    assert_eq!(assignment.acknowledged_before, MESSAGES);
}

async fn consume_all(
    consumer: &Consumer,
    first: tidemq_sdk::Result<usize>,
    paused_offset: u64,
    until: Instant,
) -> std::result::Result<(), String> {
    let mut count =
        first.map_err(|error| format!("The first active Read was rejected: {error:?}"))?;
    if paused_offset > 0 {
        assert_eq!(count as u64, paused_offset);
    } else {
        assert!(count > 0);
    }
    let mut next = 0;
    while next < MESSAGES {
        let mut page = Vec::new();
        while let Some(delivery) = consumer.next().await.unwrap() {
            assert_eq!(delivery.offset(), next);
            assert_eq!(delivery.key(), &next.to_be_bytes());
            assert_eq!(delivery.body(), vec![0x5a; 1024].as_slice());
            next += 1;
            page.push(delivery);
        }
        assert_eq!(page.len(), count);
        if !page.is_empty() {
            let ack = tidemq_sdk::RequestContext::with_deadline(until)
                .scope(consumer.ack_many(&page))
                .await
                .map_err(|error| format!("The original delivery ACK was rejected: {error:?}"))?;
            assert_eq!(ack.acknowledged_before, next);
        }
        drop(page);
        if next < MESSAGES {
            count = tidemq_sdk::RequestContext::with_deadline(until)
                .scope(consumer.fill())
                .await
                .map_err(|error| format!("The original consumer could not resume: {error:?}"))?;
        }
    }
    assert_eq!(next, MESSAGES);
    Ok(())
}
