use super::read_batches::{finish_original_ack, finish_original_send};
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_rf3_keeps_metadata_and_data_read_barriers_live_during_durable_acks() {
    let fixture = Fixture::new(
        &[
            (1, Role::Combined),
            (2, Role::Combined),
            (3, Role::Combined),
        ],
        &[1, 2, 3],
        &[1, 2, 3],
    )
    .await;
    let mut processes = Vec::new();
    for node in [1, 2, 3] {
        processes.push((node, spawn_process(&fixture, node).await));
    }
    for (node, child) in &mut processes {
        wait_process(&fixture, *node, child).await;
    }
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("combined").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let mut work = Vec::new();
    for index in 0..4 {
        let name = format!("combined/app/read-barrier-{index}");
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
            eprintln!("Combined phase completed: topic={name}, elapsed_ms={}, produced=64, acknowledged=64, metadata_reads=64, metadata_admission_rejections={}", started.elapsed().as_millis(), metadata_admission_rejections.load(Ordering::Relaxed));
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
