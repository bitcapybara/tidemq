use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    time::Duration,
};

use bytes::Bytes;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use tidemq::{
    config::{Config, Role, TlsConfig},
    metadata::PartitionKey,
    runtime::{
        GroupConfig, PeerConfig, RunningNode, RuntimeConfig,
        test_hooks::{InstalledPause, InstalledReadFault},
    },
};
use tidemq_protocol::Id;
use tidemq_sdk::Client;
use tokio::time::Instant;

// Shared fixtures provide the same private credentials and runtime configuration as the fault suites.
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
const TOPIC: &str = "test/app/reads";
const MESSAGES: u64 = 8;
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct ReadFixture {
    runtime: Fixture,
    client: Client,
    producer: tidemq_sdk::Producer,
    partition: PartitionKey,
    expected: HashMap<tidemq_sdk::MessageId, u64>,
}

impl ReadFixture {
    async fn new() -> Self {
        let mut runtime = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        runtime.enable_dynamic_auth().await;
        runtime.start_all().await;
        let client = runtime.client(1).await;
        let tenant = client.admin().create_tenant("test").await.unwrap();
        client
            .admin()
            .create_namespace(tenant, "app")
            .await
            .unwrap();
        let topic = client.admin().create_topic(TOPIC).await.unwrap();
        let partition = PartitionKey {
            topic_id: topic.id().to_string().parse().unwrap(),
            partition: 0,
        };
        let queue = client
            .new_queue_consumer(TOPIC, "durable")
            .build()
            .await
            .unwrap();
        queue.close().await.unwrap();
        let producer = client.new_producer(TOPIC).build().await.unwrap();
        let mut expected = HashMap::new();
        for sequence in 0..MESSAGES {
            let receipt = producer
                .send(tidemq_sdk::Message::new("owned-read").key(sequence.to_be_bytes().to_vec()))
                .await
                .unwrap();
            assert!(expected.insert(receipt.message_id(), sequence).is_none());
        }
        Self {
            runtime,
            client,
            producer,
            partition,
            expected,
        }
    }

    async fn consumer(&self) -> tidemq_sdk::QueueConsumer {
        self.client
            .new_queue_consumer(TOPIC, "durable")
            .prefetch_messages(MESSAGES as u32)
            .prefetch_bytes(64 * 1024)
            .build()
            .await
            .unwrap()
    }

    async fn receive_all(&self, queue: &tidemq_sdk::QueueConsumer) {
        let until = Instant::now() + Duration::from_secs(15);
        let mut seen = HashSet::new();
        while seen.len() < self.expected.len() {
            let message = tidemq_sdk::RequestContext::with_deadline(until)
                .scope(queue.receive())
                .await
                .unwrap();
            let sequence = *self.expected.get(&message.message_id()).unwrap();
            assert_eq!(message.key(), Some(sequence.to_be_bytes().as_slice()));
            assert_eq!(message.payload(), b"owned-read");
            assert!(
                seen.insert(message.message_id()),
                "The original Read repeated a delivery"
            );
            tidemq_sdk::RequestContext::with_deadline(until)
                .scope(queue.ack(&message))
                .await
                .unwrap();
        }
        assert_eq!(seen, self.expected.keys().copied().collect());
        assert!(queue.try_receive().unwrap().is_none());
        assert_eq!(self.client.metrics().snapshot().deliveries, MESSAGES);
        assert_eq!(self.client.metrics().snapshot().acknowledged, MESSAGES);
        assert_eq!(self.client.metrics().snapshot().active_deliveries, 0);
    }

    async fn close(self, queue: tidemq_sdk::QueueConsumer) {
        queue.close().await.unwrap();
        self.producer.close().await.unwrap();
        self.client.close().await.unwrap();
        self.runtime.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_read_reply_recovers_the_original_deliveries_without_duplicates() {
    let _serial = SERIAL.lock().await;
    let fixture = ReadFixture::new().await;
    let fault = InstalledReadFault::lose_reply(fixture.partition).unwrap();
    let queue = fixture.consumer().await;
    fixture.receive_all(&queue).await;
    assert!(
        fault.was_triggered(),
        "The real durable Read did not reach its injected fault"
    );
    fixture.close(queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_body_failure_replays_the_complete_original_read() {
    let _serial = SERIAL.lock().await;
    let fixture = ReadFixture::new().await;
    let fault = InstalledReadFault::body_failure(fixture.partition, 1).unwrap();
    let queue = fixture.consumer().await;
    fixture.receive_all(&queue).await;
    assert!(
        fault.was_triggered(),
        "The second real body read did not fail as requested"
    );
    fixture.close(queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_a_receive_waiter_preserves_the_owned_read_and_its_credit() {
    let _serial = SERIAL.lock().await;
    let fixture = ReadFixture::new().await;
    let pause = InstalledPause::hot_delivery(fixture.partition, 0).unwrap();
    let queue = fixture.consumer().await;
    pause.wait(deadline()).await.unwrap();
    let cancellation = tidemq_sdk::CancellationToken::new();
    let context = tidemq_sdk::RequestContext::new(deadline(), cancellation.clone());
    let receiver = queue.clone();
    let waiting = tokio::spawn(async move { context.scope(receiver.receive()).await });
    tokio::task::yield_now().await;
    cancellation.cancel();
    assert_eq!(
        waiting.await.unwrap().unwrap_err().kind(),
        tidemq_sdk::ErrorKind::Cancelled
    );
    assert!(queue.try_receive().unwrap().is_none());
    let metrics = http(
        fixture.runtime.running.get(&1).unwrap().management_address,
        "/metrics",
    )
    .await;
    assert!(
        metrics
            .lines()
            .find_map(|line| line.strip_prefix("tidemq_dispatch_active_handlers "))
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|active| active > 0)
    );
    pause.resume();
    fixture.receive_all(&queue).await;
    fixture.close(queue).await;
}
