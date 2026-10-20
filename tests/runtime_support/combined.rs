use super::*;

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
    let mut work = Vec::new();
    for partition in 0..4 {
        let name = format!("combined-read-barrier-{partition}");
        let topic = client.admin().create_topic(&name, 1).await.unwrap();
        let topic = Id::from_slice(&topic.topic_id).unwrap();
        let subscription = client
            .admin()
            .create_subscription(topic, "durable", 0, public::DeliveryMode::Parallel)
            .await
            .unwrap();
        let subscription = Id::from_slice(&subscription.subscription_id).unwrap();
        let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
            .await
            .unwrap();
        let consumer = Consumer::claim(
            client.clone(),
            topic,
            0,
            subscription,
            ConsumerConfig::default(),
        )
        .await
        .unwrap();
        work.push((name, topic, subscription, producer, consumer));
    }
    let mut running = tokio::task::JoinSet::new();
    for (name, topic, subscription, producer, consumer) in work {
        let client = client.clone();
        running.spawn(async move {
            let context =
                tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(20)).unwrap();
            let produce = async {
                for sequence in 0..64_u64 {
                    let receipt = context
                        .scope(producer.produce(vec![public::Message {
                            key: Bytes::from_static(b"combined"),
                            body: Bytes::copy_from_slice(&sequence.to_le_bytes()),
                        }]))
                        .await
                        .unwrap();
                    assert_eq!(receipt.first_offset, sequence);
                }
            };
            let consume = async {
                let mut seen = std::collections::BTreeSet::new();
                while seen.len() < 64 {
                    let Some(delivery) = context.scope(consumer.recv()).await.unwrap() else {
                        tokio::task::yield_now().await;
                        continue;
                    };
                    let mut deliveries = vec![delivery];
                    while deliveries.len() < 32 {
                        let Some(delivery) = consumer.next().await.unwrap() else {
                            break;
                        };
                        deliveries.push(delivery);
                    }
                    for delivery in &deliveries {
                        let body: [u8; 8] = delivery.body().try_into().unwrap();
                        assert!(
                            seen.insert(u64::from_le_bytes(body)),
                            "Duplicate combined-node delivery"
                        );
                    }
                    context.scope(consumer.ack_many(&deliveries)).await.unwrap();
                }
                assert_eq!(seen, (0..64).collect());
                let status = context
                    .scope(client.admin().subscription_status(topic, 0, subscription))
                    .await
                    .unwrap();
                assert_eq!(status.assignment.unwrap().acknowledged_before, 64);
            };
            let read_metadata = async {
                for _ in 0..64 {
                    context.scope(client.admin().topic(&name)).await.unwrap();
                    tokio::task::yield_now().await;
                }
            };
            tokio::join!(produce, consume, read_metadata);
        });
    }
    tokio::time::timeout(Duration::from_secs(25), async {
        while let Some(result) = running.join_next().await {
            result.unwrap();
        }
    })
    .await
    .unwrap();
    client.close().await;
    for (_, mut process) in processes {
        process.kill().await.unwrap();
        let _ = process.wait().await.unwrap();
    }
    fixture.close().await;
}
