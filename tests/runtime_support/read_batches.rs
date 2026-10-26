use super::*;
use std::collections::{BTreeSet, HashMap};

const MESSAGE_COUNT: u64 = 512;
const BODY_BYTES: usize = 1024;
const PRODUCE_BATCH: usize = 32;
const RECEIVE_CREDIT: u32 = 256;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_credit_live_pages_preserve_every_identity_and_durable_ack() {
    maintenance_test_logging();
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let until = Instant::now() + Duration::from_secs(90);
    tokio::time::timeout_at(until, async {
        let topic = create_logical_resources(&client).await;
        let consumer = client
            .new_queue_consumer(RUNTIME_TOPIC, "pages")
            .prefetch_messages(RECEIVE_CREDIT)
            .build()
            .await
            .unwrap();
        let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
        let mut expected = HashMap::new();
        for first in (0..MESSAGE_COUNT).step_by(PRODUCE_BATCH) {
            let messages = (first..first + PRODUCE_BATCH as u64).map(message).collect();
            let receipts = tidemq_sdk::RequestContext::with_deadline(until)
                .scope(producer.send_batch(messages))
                .await
                .unwrap();
            assert_eq!(receipts.outcomes.len(), PRODUCE_BATCH);
            for (index, receipt) in receipts.outcomes.into_iter().enumerate() {
                let receipt = finish_original_send(receipt, until).await;
                if receipt.is_err() {
                    let address = fixture.running.get(&1).unwrap().management_address;
                    let diagnostics = http(address, "/diagnostics").await;
                    let metrics = http(address, "/metrics").await;
                    let ownership = http(address, &format!("/ownership/{}/0", topic.id())).await;
                    let evidence = fixture.directory.join("live-pages-send-failure.json");
                    tokio::fs::write(
                        &evidence,
                        serde_json::to_vec_pretty(&serde_json::json!({
                        "diagnostics": diagnostics, "metrics": metrics, "ownership": ownership,
                            "sdk": format!("{:?}", client.metrics().snapshot()),
                            "result": format!("{receipt:?}"),
                        }))
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                    panic!("A live-page send failed; inspect {}", evidence.display());
                }
                assert!(
                    expected
                        .insert(receipt.unwrap().message_id(), first + index as u64)
                        .is_none()
                );
            }
        }
        let mut seen = BTreeSet::new();
        while seen.len() < MESSAGE_COUNT as usize {
            let delivery = tidemq_sdk::RequestContext::with_deadline(until)
                .scope(consumer.receive())
                .await
                .unwrap();
            let sequence = *expected.get(&delivery.message_id()).unwrap();
            assert_eq!(delivery.key(), Some(sequence.to_be_bytes().as_slice()));
            let payload = message_body(sequence);
            assert_eq!(delivery.payload(), payload);
            assert!(seen.insert(sequence), "A healthy Queue repeated a message");
            let acknowledgement = tidemq_sdk::RequestContext::with_deadline(until)
                .scope(consumer.ack(&delivery))
                .await;
            let acknowledgement = finish_original_ack(acknowledgement, until)
                .await
                .unwrap_or_else(|error| {
                    panic!("ACK failed after {} messages: {error:?}", seen.len())
                });
            assert_eq!(acknowledgement, tidemq_sdk::AckReceipt::Acknowledged);
        }
        assert_eq!(seen, (0..MESSAGE_COUNT).collect());
        assert_eq!(client.metrics().snapshot().deliveries, MESSAGE_COUNT);
        assert_eq!(client.metrics().snapshot().acknowledged, MESSAGE_COUNT);
        consumer.close().await.unwrap();
        producer.close().await.unwrap();
        client.close().await.unwrap();
    })
    .await
    .expect("The complete live-page workload exceeded its original deadline");
    let metrics = http(
        fixture.running.get(&1).unwrap().management_address,
        "/metrics",
    )
    .await;
    let prefix = "tidemq_operations_total{operation=\"Read\",outcome=\"Committed\"} ";
    assert!(
        metrics
            .lines()
            .find_map(|line| line.strip_prefix(prefix))
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 1
    );
    fixture.close().await;
}

pub(super) async fn finish_original_send(
    mut result: tidemq_sdk::Result<tidemq_sdk::SendReceipt>,
    until: Instant,
) -> tidemq_sdk::Result<tidemq_sdk::SendReceipt> {
    loop {
        match result {
            Err(tidemq_sdk::Error::SendUnresolved { pending, source })
                if source.outcome_unknown() && Instant::now() < until =>
            {
                // A live RPC can lose its result; preserve the original send identity.
                result = tidemq_sdk::RequestContext::with_deadline(until)
                    .scope(pending.retry())
                    .await;
            }
            other => return other,
        }
    }
}

pub(super) async fn finish_original_ack(
    mut result: tidemq_sdk::Result<tidemq_sdk::AckReceipt>,
    until: Instant,
) -> tidemq_sdk::Result<tidemq_sdk::AckReceipt> {
    loop {
        match result {
            Err(tidemq_sdk::Error::Acknowledgement { pending, source })
                if source.outcome_unknown() && Instant::now() < until =>
            {
                result = tidemq_sdk::RequestContext::with_deadline(until)
                    .scope(pending.retry())
                    .await;
            }
            other => return other,
        }
    }
}

fn message_body(identity: u64) -> Vec<u8> {
    let mut body = Vec::with_capacity(BODY_BYTES);
    body.extend_from_slice(&identity.to_be_bytes());
    body.resize(BODY_BYTES, 0x5a);
    body
}

fn message(identity: u64) -> tidemq_sdk::Message {
    tidemq_sdk::Message::new(message_body(identity))
        .key(Bytes::copy_from_slice(&identity.to_be_bytes()))
}
