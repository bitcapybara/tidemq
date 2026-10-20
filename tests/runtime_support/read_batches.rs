use super::*;

use std::collections::BTreeSet;

use tidemq_sdk::Delivery;

const MESSAGE_COUNT: u64 = 512;
const BODY_BYTES: usize = 1024;
const PRODUCE_BATCH: usize = 32;
const DEFAULT_CREDIT: usize = 256;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_credit_live_pages_preserve_every_identity_and_durable_ack() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let until = Instant::now() + Duration::from_secs(30);
    tokio::time::timeout_at(until, async {
        exercise_live_pages(&client, until).await;
        client.close().await;
    })
    .await
    .expect("The complete live-page workload must finish within its original 30-second deadline");
    fixture.close().await;
}

async fn exercise_live_pages(client: &Client, until: Instant) {
    let (topic, subscription) = create(client).await;
    let producer = tidemq_sdk::RequestContext::with_deadline(deadline().min(until))
        .scope(Producer::open(
            client.clone(),
            topic,
            0,
            Id::random().unwrap(),
        ))
        .await
        .unwrap();
    let config = ConsumerConfig::default();
    assert_eq!(config.max_buffered_messages as usize, DEFAULT_CREDIT);
    // A generous metadata allowance leaves a full-credit page below both default byte limits.
    let full_page_bytes = DEFAULT_CREDIT * (BODY_BYTES + 8 + 512);
    assert!(full_page_bytes < config.max_buffered_bytes as usize);
    assert!(
        full_page_bytes
            < ClientConfig::new("queue.local:443")
                .protocol
                .max_frame_bytes as usize
    );
    // Claim before producing so H remains zero and all deliveries use the live path.
    let consumer = tidemq_sdk::RequestContext::with_deadline(deadline().min(until))
        .scope(Consumer::claim(
            client.clone(),
            topic,
            0,
            subscription,
            config,
        ))
        .await
        .unwrap();
    produce_committed_messages(&producer, until).await;
    consume_live_pages(&consumer, until).await;
    let assignment = tidemq_sdk::RequestContext::with_deadline(deadline().min(until))
        .scope(client.admin().subscription_status(topic, 0, subscription))
        .await
        .unwrap()
        .assignment
        .unwrap();
    assert_eq!(assignment.cold_before, 0);
    assert_eq!(assignment.acknowledged_before, MESSAGE_COUNT);
}

async fn produce_committed_messages(producer: &Producer, until: Instant) {
    for first in (0..MESSAGE_COUNT).step_by(PRODUCE_BATCH) {
        let messages = (first..first + PRODUCE_BATCH as u64).map(message).collect();
        let receipt = tidemq_sdk::RequestContext::with_deadline(deadline().min(until))
            .scope(producer.produce(messages))
            .await
            .expect("Every original producer batch must obtain a durable receipt before reading");
        assert_eq!(receipt.first_offset, first);
        assert_eq!(receipt.message_count as usize, PRODUCE_BATCH);
    }
    eprintln!("Committed {MESSAGE_COUNT} messages before the first live Read");
}

async fn consume_live_pages(consumer: &Consumer, until: Instant) {
    let mut seen = BTreeSet::new();
    let mut frontier = 0;
    let mut page = 0;
    let mut partial_pages = 0;
    while seen.len() < MESSAGE_COUNT as usize {
        let pending = MESSAGE_COUNT as usize - seen.len();
        let started = Instant::now();
        let result = tidemq_sdk::RequestContext::with_deadline(deadline().min(until))
            .scope(consumer.fill())
            .await;
        eprintln!(
            "Live Read page {page}: pending={pending}, result={result:?}, elapsed_us={}",
            started.elapsed().as_micros()
        );
        let count = result.expect("Live Read errors must fail without replacing the consumer");
        assert!(count <= DEFAULT_CREDIT);
        partial_pages +=
            usize::from(pending >= DEFAULT_CREDIT && count > 0 && count < DEFAULT_CREDIT);
        let mut deliveries = Vec::new();
        while let Some(delivery) = consumer.next().await.unwrap() {
            validate_delivery(&delivery, &mut seen);
            deliveries.push(delivery);
        }
        assert_eq!(deliveries.len(), count);
        page += 1;
        if deliveries.is_empty() {
            continue;
        }
        let mut contiguous = frontier;
        while seen.contains(&contiguous) {
            contiguous += 1;
        }
        let ack = tidemq_sdk::RequestContext::with_deadline(deadline().min(until))
            .scope(consumer.ack_many(&deliveries))
            .await
            .expect("Each original delivery token must obtain a durable ACK without retries");
        assert!(ack.acknowledged_before >= frontier);
        assert_eq!(ack.acknowledged_before, contiguous);
        frontier = ack.acknowledged_before;
        eprintln!("Durable ACK after page {}: frontier={frontier}", page - 1);
        // Drop delivery byte/slot permits before the next default-credit Read.
        drop(deliveries);
    }
    assert_eq!(seen, (0..MESSAGE_COUNT).collect());
    assert_eq!(frontier, MESSAGE_COUNT);
    // Faster hosts may legally fill all 256 slots within a service slice.
    eprintln!(
        "Completed {page} live Read pages; nonempty partial pages with full credit={partial_pages}"
    );
}

fn validate_delivery(delivery: &Delivery, seen: &mut BTreeSet<u64>) {
    let identity = u64::from_be_bytes(delivery.key().try_into().unwrap());
    assert!(identity < MESSAGE_COUNT);
    assert_eq!(delivery.offset(), identity);
    assert_eq!(delivery.body(), message(identity).body.as_ref());
    assert!(
        seen.insert(identity),
        "A message identity must appear exactly once"
    );
}

fn message(identity: u64) -> public::Message {
    let key = identity.to_be_bytes();
    let mut body = Vec::with_capacity(BODY_BYTES);
    body.extend_from_slice(&key);
    body.resize(BODY_BYTES, 0x5a);
    public::Message {
        key: Bytes::copy_from_slice(&key),
        body: body.into(),
    }
}
