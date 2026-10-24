use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creating_subscription_protects_available_history_across_metadata_restart() {
    creation_retention_case(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the verified RustFS 1.0.3 executable in TIDEMQ_TEST_RUSTFS_BIN"]
async fn creating_subscription_preserves_original_cold_objects_across_metadata_restart() {
    let rustfs = rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    creation_retention_case(Some(&rustfs)).await;
}

async fn creation_retention_case(objects: Option<&rustfs::RustFs>) {
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
    if let Some(objects) = objects {
        enable_objects(&mut fixture, objects).await;
    }
    for config in fixture.configs.values_mut() {
        config.runtime.retention.minimum_age_ms = 2_000;
        config.runtime.archive_interval_ms = 100;
    }
    fixture.start_all().await;
    let client = fixture.client(4).await;
    let topic = create_logical_resources(&client).await;
    let partition = tidemq::metadata::PartitionKey {
        topic_id: topic.id().to_string().parse().unwrap(),
        partition: 0,
    };
    let block =
        tidemq::runtime::test_hooks::InstalledConsumerCreationBlock::install(partition).unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let mut expected = Vec::new();
    for index in 0..3 {
        expected.push(
            producer
                .send(tidemq_sdk::Message::new(format!("history-{index}")).key("one"))
                .await
                .unwrap()
                .message_id(),
        );
    }
    let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(400)).unwrap();
    assert!(
        context
            .scope(
                client
                    .new_stream_consumer(RUNTIME_TOPIC, "creating")
                    .build()
            )
            .await
            .is_err()
    );
    assert!(block.was_triggered());
    let original = client
        .admin()
        .stream_subscription(RUNTIME_TOPIC, "creating")
        .await
        .unwrap()
        .id();
    if objects.is_some() {
        let node = fixture.running.get(&2).unwrap();
        node.archive(
            2,
            vec![tidemq::lifecycle::RangeRequest {
                partition,
                offsets: tidemq_protocol::state::OffsetRange { start: 0, end: 3 },
            }],
            deadline(),
        )
        .await
        .unwrap();
        node.reclaim_local(partition, 3, deadline()).await.unwrap();
    }
    fixture.stop(1).await;
    fixture.restart(1).await;
    let until = deadline();
    loop {
        let diagnostics = http(
            fixture.running.get(&1).unwrap().management_address,
            "/diagnostics",
        )
        .await;
        let body = diagnostics.split("\r\n\r\n").nth(1).unwrap();
        let value: serde_json::Value = serde_json::from_str(body).unwrap();
        if value["retention"]["deferrals"].as_u64().unwrap_or(0) > 0 {
            assert_eq!(value["metadata"]["creating_subscriptions"], 1);
            break;
        }
        assert!(
            Instant::now() < until,
            "No fresh retention attempt observed the creating subscription"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        client
            .admin()
            .stream_subscription(RUNTIME_TOPIC, "creating")
            .await
            .unwrap()
            .id(),
        original
    );
    drop(block);
    let consumer = client
        .new_stream_consumer(RUNTIME_TOPIC, "creating")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    for expected in expected {
        let message = tokio::time::timeout(Duration::from_secs(10), consumer.receive())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.message_id(), expected);
        consumer.ack_cumulative(&message).await.unwrap();
    }
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
