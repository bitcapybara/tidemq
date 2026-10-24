use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_size_public_message_survives_routing_context_and_restart() {
    full_size_roundtrip(256 * 1024).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_configured_public_message_limit_rejects_oversize_without_leaving_an_unknown_send() {
    full_size_roundtrip(64 * 1024).await;
}

async fn full_size_roundtrip(maximum: usize) {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    configure_dynamic_fixture(&mut fixture).await;
    fixture
        .configs
        .get_mut(&1)
        .unwrap()
        .protocol
        .max_message_bytes = maximum.try_into().unwrap();
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/full-message";
    client.admin().create_topic(topic).await.unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    let payload = Bytes::from(vec![7; maximum]);
    let identity = producer
        .send(tidemq_sdk::Message::new(payload.clone()))
        .await
        .unwrap()
        .message_id();
    let properties_payload = Bytes::from(vec![9; maximum - 104]);
    let properties_id = producer
        .send(
            tidemq_sdk::Message::new(properties_payload.clone())
                .key("key")
                .property("kind", "v"),
        )
        .await
        .unwrap()
        .message_id();
    let error = producer
        .send(tidemq_sdk::Message::new(vec![8; maximum + 1]))
        .await
        .unwrap_err();
    assert!(
        !error.outcome_unknown(),
        "Oversize must be rejected deterministically: {error}"
    );
    assert!(producer.pending_sends().await.unwrap().is_empty());
    let following_id = producer
        .send(tidemq_sdk::Message::new("following"))
        .await
        .unwrap()
        .message_id();
    let expected = std::collections::HashMap::from([
        (identity, payload),
        (properties_id, properties_payload),
        (following_id, Bytes::from_static(b"following")),
    ]);
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.stop(1).await;
    fixture.restart(1).await;
    let client = fixture.client(1).await;
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .build()
        .await
        .unwrap();
    let mut seen = std::collections::HashSet::new();
    for _ in 0..expected.len() {
        let message = tokio::time::timeout(Duration::from_secs(15), queue.receive())
            .await
            .unwrap()
            .unwrap();
        assert!(seen.insert(message.message_id()));
        assert_eq!(
            message.payload(),
            expected.get(&message.message_id()).unwrap().as_ref()
        );
        if message.message_id() == properties_id {
            assert_eq!(message.key(), Some(b"key".as_slice()));
            assert_eq!(message.properties().get("kind").unwrap().as_ref(), b"v");
        }
        queue.ack(&message).await.unwrap();
    }
    queue.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
