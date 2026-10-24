use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
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
use tidemq_sdk::{Bytes, Client, Id};
use tokio::time::Instant;

// Shared fixtures also provide helpers exercised by the other runtime suites.
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
const TOPIC: &str = "test/app/reclaimed";
const MESSAGES: u64 = 8;
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the verified RustFS 1.0.3 executable in TIDEMQ_TEST_RUSTFS_BIN"]
async fn first_hot_delivery_reloads_cold_after_concurrent_local_reclamation() {
    run_reclaimed_read(0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the verified RustFS 1.0.3 executable in TIDEMQ_TEST_RUSTFS_BIN"]
async fn hot_reclamation_keeps_the_durable_read_and_reloads_its_suffix_from_cold() {
    run_reclaimed_read(1).await;
}

async fn run_reclaimed_read(paused_offset: u64) {
    let _serial = SERIAL.lock().await;
    let rustfs = rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.enable_dynamic_auth().await;
    enable_objects(&mut fixture, &rustfs).await;
    fixture
        .configs
        .get_mut(&1)
        .unwrap()
        .runtime
        .archive_interval_ms = 60_000;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("test").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = client.admin().create_logical_topic(TOPIC).await.unwrap();
    let partition = PartitionKey {
        topic_id: topic.id().to_string().parse().unwrap(),
        partition: 0,
    };
    let consumer = client
        .new_queue_consumer(TOPIC, "durable")
        .build()
        .await
        .unwrap();
    consumer.close().await.unwrap();
    let producer = client.new_producer(TOPIC).build().await.unwrap();
    let mut expected = HashMap::new();
    for sequence in 0..MESSAGES {
        let receipt = producer
            .send(tidemq_sdk::Message::new(vec![0x5a; 1024]).key(sequence.to_be_bytes().to_vec()))
            .await
            .unwrap();
        assert!(expected.insert(receipt.message_id(), sequence).is_none());
    }
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
    let consumer = client
        .new_queue_consumer(TOPIC, "durable")
        .prefetch_messages(MESSAGES as u32)
        .build()
        .await
        .unwrap();
    let until = Instant::now() + Duration::from_secs(30);
    hook.wait(until).await.unwrap();
    // The complete Read decision is durable; reclamation can race with body materialization.
    node.reclaim_local(partition, MESSAGES, until)
        .await
        .unwrap();
    hook.resume();
    let mut seen = HashSet::new();
    while seen.len() < MESSAGES as usize {
        let delivery = tidemq_sdk::RequestContext::with_deadline(until)
            .scope(consumer.receive())
            .await
            .unwrap();
        let sequence = *expected.get(&delivery.message_id()).unwrap();
        assert_eq!(delivery.key(), Some(sequence.to_be_bytes().as_slice()));
        assert_eq!(delivery.payload(), vec![0x5a; 1024]);
        assert!(
            seen.insert(delivery.message_id()),
            "A recovered Read repeated an identity"
        );
        tidemq_sdk::RequestContext::with_deadline(until)
            .scope(consumer.ack(&delivery))
            .await
            .unwrap();
    }
    assert_eq!(seen, expected.keys().copied().collect());
    assert_eq!(client.metrics().snapshot().acknowledged, MESSAGES);
    let metrics = http(node.management_address, "/metrics").await;
    let conflicts = metrics
        .lines()
        .find_map(|line| line.strip_prefix("tidemq_runtime_read_hot_reclaim_conflicts_total "))
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0);
    assert_eq!(
        conflicts, 1,
        "The Read must reload and reuse one bounded cold page"
    );
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
