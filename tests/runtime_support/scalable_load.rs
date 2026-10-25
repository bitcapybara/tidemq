use super::*;
use tidemq::metadata::PartitionKey;

fn scaling_policy() -> tidemq::runtime::TopicScalingPolicy {
    tidemq::runtime::TopicScalingPolicy {
        capacity: tidemq::runtime::WorkCapacity {
            publish_bytes_per_second: 1_000_000,
            publish_messages_per_second: 20,
            delivery_bytes_per_second: 1_000_000,
            deliveries_per_second: 10_000,
            read_requests_per_second: 10_000,
            ack_bytes_per_second: 1_000_000,
            acknowledgements_per_second: 10_000,
            ack_requests_per_second: 10_000,
        },
        sample_interval_ms: 200,
        split_window_ms: 1000,
        merge_window_ms: 4000,
        cooldown_ms: 1000,
        max_shards: 2,
        ..Default::default()
    }
}

fn distinct_keys() -> [String; 2] {
    let producer = Id::new_v7().unwrap();
    let mut keys: [Option<String>; 2] = [None, None];
    for index in 0..1000 {
        let key = format!("functional-key-{index}");
        let hash = tidemq_protocol::topic::routing_hash(Some(key.as_bytes()), producer, 1).unwrap();
        let slot = usize::from(hash >= (1_u64 << 63));
        keys[slot].get_or_insert(key);
        if keys.iter().all(Option::is_some) {
            break;
        }
    }
    keys.map(Option::unwrap)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_split_and_merge_preserve_original_sdk_and_require_an_independent_live_leader() {
    let mut fixture = Fixture::new(
        &[
            (1, Role::Metadata),
            (2, Role::Storage),
            (3, Role::Storage),
            (4, Role::Broker),
            (5, Role::Gateway),
        ],
        &[1],
        &[2],
    )
    .await;
    for config in fixture.configs.values_mut() {
        config.runtime.groups.push(GroupConfig {
            group_id: 3,
            voters: vec![3],
            standby: Vec::new(),
        });
    }
    fixture.configs.get_mut(&1).unwrap().runtime.topic_scaling = Some(scaling_policy());
    fixture.start_all().await;
    let client = fixture.client(5).await;
    create_logical_resources(&client).await;
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let stream = client
        .new_stream_consumer(RUNTIME_TOPIC, "automatic-events")
        .build()
        .await
        .unwrap();
    let queue = client
        .new_queue_consumer(RUNTIME_TOPIC, "automatic-jobs")
        .build()
        .await
        .unwrap();
    let keys = distinct_keys();
    let until = Instant::now() + Duration::from_secs(20);
    let mut sequence = 0;
    let mut expected = std::collections::HashMap::new();
    loop {
        let started = Instant::now();
        let payload = format!("functional-{sequence}");
        for key in &keys {
            let receipt = producer
                .send(tidemq_sdk::Message::new(payload.clone()).key(key.clone()))
                .await
                .unwrap();
            assert!(
                expected
                    .insert(receipt.message_id(), payload.clone())
                    .is_none()
            );
        }
        if client
            .admin()
            .topic_info(RUNTIME_TOPIC)
            .await
            .unwrap()
            .shard_count()
            == 2
        {
            break;
        }
        assert!(
            Instant::now() < until,
            "Automatic split did not complete: {}",
            http(
                fixture.running.get(&1).unwrap().management_address,
                "/diagnostics"
            )
            .await
        );
        sequence += 1;
        tokio::time::sleep_until(started + Duration::from_millis(100)).await;
    }
    let mut stream_remaining: std::collections::HashSet<_> = expected.keys().copied().collect();
    let mut queue_remaining = stream_remaining.clone();
    for _ in 0..expected.len() {
        let message = tokio::time::timeout_at(until, stream.receive())
            .await
            .unwrap()
            .unwrap();
        assert!(stream_remaining.remove(&message.message_id()));
        assert_eq!(
            message.payload(),
            expected.get(&message.message_id()).unwrap().as_bytes()
        );
        stream.ack_cumulative(&message).await.unwrap();
        let message = tokio::time::timeout_at(until, queue.receive())
            .await
            .unwrap()
            .unwrap();
        assert!(queue_remaining.remove(&message.message_id()));
        assert_eq!(
            message.payload(),
            expected.get(&message.message_id()).unwrap().as_bytes()
        );
        queue.ack(&message).await.unwrap();
    }
    assert!(stream_remaining.is_empty() && queue_remaining.is_empty());
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        client
            .admin()
            .topic_info(RUNTIME_TOPIC)
            .await
            .unwrap()
            .shard_count(),
        2
    );
    let until = Instant::now() + Duration::from_secs(20);
    while client
        .admin()
        .topic_info(RUNTIME_TOPIC)
        .await
        .unwrap()
        .shard_count()
        != 1
    {
        assert!(
            Instant::now() < until,
            "Automatic merge did not complete: {}",
            http(
                fixture.running.get(&1).unwrap().management_address,
                "/diagnostics"
            )
            .await
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let id = producer
        .send(tidemq_sdk::Message::new("after-automatic-merge").key(keys.first().unwrap().clone()))
        .await
        .unwrap()
        .message_id();
    let message = tokio::time::timeout_at(until, stream.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.message_id(), id);
    stream.ack_cumulative(&message).await.unwrap();
    let message = tokio::time::timeout_at(until, queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.message_id(), id);
    queue.ack(&message).await.unwrap();
    stream.close().await.unwrap();
    queue.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_split_reports_resource_demand_without_sealing_on_a_shared_leader() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    configure_dynamic_fixture(&mut fixture).await;
    fixture.configs.get_mut(&1).unwrap().runtime.topic_scaling = Some(scaling_policy());
    fixture.start_all().await;
    let client = fixture.client(1).await;
    create_logical_resources(&client).await;
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let keys = distinct_keys();
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        let started = Instant::now();
        for key in &keys {
            producer
                .send(tidemq_sdk::Message::new("functional-demand").key(key.clone()))
                .await
                .unwrap();
        }
        let diagnostic = http(
            fixture.running.get(&1).unwrap().management_address,
            "/diagnostics",
        )
        .await;
        let json: serde_json::Value =
            serde_json::from_str(diagnostic.split_once("\r\n\r\n").unwrap().1).unwrap();
        if json["topic_scaling"]["status"] == "StorageCapacityRequired" {
            assert_eq!(json["topic_scaling"]["required_independent_groups"], 1);
            break;
        }
        assert!(
            Instant::now() < until,
            "Expected independent capacity demand: {diagnostic}"
        );
        tokio::time::sleep_until(started + Duration::from_millis(100)).await;
    }
    assert_eq!(
        client
            .admin()
            .topic_info(RUNTIME_TOPIC)
            .await
            .unwrap()
            .shard_count(),
        1
    );
    producer.send("source-is-still-writable").await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

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
