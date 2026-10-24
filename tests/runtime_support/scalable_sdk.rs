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
    queue.clone().close().await.unwrap();
    assert!(queue.try_receive().is_err());
    producer.clone().close().await.unwrap();
    assert!(producer.send("closed").await.is_err());
    client.close().await.unwrap();
    fixture.close().await;
}
