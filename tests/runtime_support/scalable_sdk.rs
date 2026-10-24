use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_builders_connect_to_a_combined_node_and_share_logical_handles() {
    let fixture = dynamic_fixture().await;
    let client = Client::builder(
        fixture.configs.get(&1).unwrap().listen.to_string(),
        tidemq_sdk::TlsTrust::ca_pem(fixture.roots.clone()),
        tidemq_sdk::Credentials::token(TOKEN),
    )
    .namespace("acme/app")
    .build()
    .await
    .unwrap();
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client.admin().create_logical_topic("events").await.unwrap();
    let queue = client
        .new_queue_consumer("events", "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer("events").build().await.unwrap();
    let sent = producer.clone().send("ordinary-message").await.unwrap();
    let received = queue.clone().receive().await.unwrap();
    assert_eq!(received.message_id(), sent.message_id());
    assert_eq!(received.payload(), b"ordinary-message");
    queue.ack(&received).await.unwrap();
    queue.ack(&received).await.unwrap();
    assert_eq!(client.metrics().snapshot().deliveries, 1);
    assert_eq!(client.metrics().snapshot().acknowledged, 1);
    assert_eq!(client.metrics().snapshot().active_deliveries, 1);
    assert!(client.metrics().snapshot().admitted_bytes > 0);
    let second = producer.send("retry-once").await.unwrap();
    let retry = queue.receive().await.unwrap();
    assert_eq!(retry.message_id(), second.message_id());
    queue.nack(&retry, Duration::ZERO).await.unwrap();
    assert_eq!(client.metrics().snapshot().acknowledged, 1);
    let redelivered = queue.receive().await.unwrap();
    assert_eq!(redelivered.message_id(), retry.message_id());
    queue.ack(&redelivered).await.unwrap();
    assert_eq!(client.metrics().snapshot().deliveries, 3);
    assert_eq!(client.metrics().snapshot().acknowledged, 2);
    drop((received, retry, redelivered));
    queue.clone().close().await.unwrap();
    assert!(queue.try_receive().is_err());
    producer.clone().close().await.unwrap();
    assert!(producer.send("closed").await.is_err());
    assert_eq!(client.metrics().snapshot().active_deliveries, 0);
    assert_eq!(client.metrics().snapshot().pending_batches, 0);
    assert_eq!(client.metrics().snapshot().admitted_bytes, 0);
    let metrics = http(
        fixture.running.get(&1).unwrap().management_address,
        "/metrics",
    )
    .await;
    assert!(metrics.contains("tidemq_producer_new_messages_total 2"));
    for operation in ["Produce", "Read", "Ack"] {
        let prefix =
            format!("tidemq_operations_total{{operation=\"{operation}\",outcome=\"Committed\"}} ");
        assert!(
            metrics
                .lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > 0
        );
    }
    client.close().await.unwrap();
    fixture.close().await;
}
