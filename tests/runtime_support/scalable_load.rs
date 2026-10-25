use super::*;
use tidemq::metadata::PartitionKey;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_load_samples_keep_work_across_storage_restart_and_fence_process_identity() {
    let mut fixture = Fixture::new(
        &[
            (1, Role::Metadata),
            (2, Role::Storage),
            (3, Role::Broker),
            (4, Role::Gateway),
        ],
        &[1],
        &[2],
    )
    .await;
    fixture.start_all().await;
    let client = fixture.client(4).await;
    create_logical_resources(&client).await;
    let topic = client.admin().topic_info(RUNTIME_TOPIC).await.unwrap();
    let partition = PartitionKey {
        topic_id: topic.id().to_string().parse().unwrap(),
        partition: 0,
    };
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "load-observer")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    for key in ["left", "right"] {
        let receipt = producer
            .send(tidemq_sdk::Message::new("committed-work").key(key))
            .await
            .unwrap();
        let message = tokio::time::timeout_at(deadline(), consumer.receive())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.message_id(), receipt.message_id());
        consumer.ack(&message).await.unwrap();
    }
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    let metadata = fixture.running.get(&1).unwrap();
    let before = metadata
        .shard_loads(2, &[partition], deadline())
        .await
        .unwrap();
    assert_eq!(before.node, 2);
    assert_eq!(before.group, 2);
    let report = before.reports.first().unwrap().as_ref().unwrap();
    assert_eq!(report.partition, partition);
    assert!(report.counters.valid());
    assert_eq!(report.counters.publish_messages, 2);
    assert_eq!(report.counters.deliveries, 2);
    assert_eq!(report.counters.acknowledgements, 2);
    assert_eq!(report.counters.ack_requests, 2);
    assert!(report.counters.read_requests >= 2);
    assert!(report.counters.publish_bytes > 0);
    let missing = PartitionKey {
        topic_id: Id::new_v7().unwrap(),
        partition: 0,
    };
    let absent = metadata
        .shard_loads(2, &[missing], deadline())
        .await
        .unwrap();
    assert!(absent.reports.iter().all(Option::is_none));
    assert!(
        metadata
            .shard_loads(2, &[partition; 17], deadline())
            .await
            .is_err()
    );
    assert!(
        fixture
            .running
            .get(&3)
            .unwrap()
            .shard_loads(2, &[partition], deadline())
            .await
            .is_err()
    );
    fixture.stop(2).await;
    fixture.restart(2).await;
    let after = fixture
        .running
        .get(&1)
        .unwrap()
        .shard_loads(2, &[partition], deadline())
        .await
        .unwrap();
    assert_ne!(after.instance, before.instance);
    assert_eq!(after.reports, before.reports);
    fixture.close().await;
}
