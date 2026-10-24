use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_queue_member_keeps_unacknowledged_work_past_cached_data_lease_expiry() {
    let fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/held-work";
    client.admin().create_topic(topic).await.unwrap();
    let first = client
        .new_queue_consumer(topic, "jobs")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    producer
        .send(tidemq_sdk::Message::new("still-processing"))
        .await
        .unwrap();
    let held = tokio::time::timeout(Duration::from_secs(15), first.receive())
        .await
        .unwrap()
        .unwrap();
    let second = client
        .new_queue_consumer(topic, "jobs")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(17), second.receive())
            .await
            .is_err(),
        "An online member's unacknowledged work was reassigned without NACK or departure"
    );
    first.ack(&held).await.unwrap();
    assert!(second.try_receive().unwrap().is_none());
    producer.close().await.unwrap();
    first.close().await.unwrap();
    second.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_handoff_keeps_cumulative_ack_usable_for_retained_ranges() {
    let fixture = dynamic_fixture().await;
    let client = fixture
        .client_with_timeouts(1, Duration::from_secs(30), Duration::from_secs(5))
        .await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/ack-handoff";
    client.admin().create_topic(topic).await.unwrap();
    let first = client
        .new_stream_consumer(topic, "history")
        .prefetch_messages(64)
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    let mut old = Vec::new();
    for index in 0..40 {
        producer
            .send(tidemq_sdk::Message::new(format!("old-{index}")).key(format!("key-{index}")))
            .await
            .unwrap();
    }
    for _ in 0..40 {
        old.push(
            tokio::time::timeout(Duration::from_secs(15), first.receive())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let second = client
        .new_stream_consumer(topic, "history")
        .build()
        .await
        .unwrap();
    let handed_off = tokio::time::timeout(Duration::from_secs(30), second.receive())
        .await
        .unwrap()
        .unwrap();
    let revoked = old
        .iter()
        .find(|message| message.message_id() == handed_off.message_id())
        .unwrap();
    second.ack_cumulative(&handed_off).await.unwrap();
    for index in 0..40 {
        producer
            .send(tidemq_sdk::Message::new(format!("new-{index}")).key(format!("key-{index}")))
            .await
            .unwrap();
    }
    let retained = tokio::time::timeout(Duration::from_secs(30), first.receive())
        .await
        .unwrap()
        .unwrap();
    assert!(retained.payload().starts_with(b"new-"));
    first.ack_cumulative(&retained).await.unwrap();
    assert!(matches!(
        first.ack_cumulative(revoked).await,
        Err(tidemq_sdk::Error::StaleDelivery)
    ));
    producer.close().await.unwrap();
    second.close().await.unwrap();
    first.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_handoff_stops_disconnected_members_from_delivering_old_prefetch() {
    let fixture = dynamic_fixture().await;
    let admin = fixture
        .client_with_timeouts(1, Duration::from_secs(30), Duration::from_secs(5))
        .await;
    let tenant = admin.admin().create_tenant("acme").await.unwrap();
    admin.admin().create_namespace(tenant, "app").await.unwrap();
    let topic = "acme/app/handoff";
    admin.admin().create_topic(topic).await.unwrap();
    let backend = fixture.configs.get(&1).unwrap().listen;
    let proxy = udp_proxy::Proxy::start([backend, backend]).await;
    let client = Client::builder(
        proxy.address.to_string(),
        tidemq_sdk::TlsTrust::ca_pem(fixture.roots.clone()),
        tidemq_sdk::Credentials::token(TOKEN),
    )
    .build()
    .await
    .unwrap();
    let first = client
        .new_stream_consumer(topic, "history")
        .prefetch_messages(128)
        .build()
        .await
        .unwrap();
    let producer = admin.new_producer(topic).build().await.unwrap();
    for index in 0..96 {
        producer
            .send(tidemq_sdk::Message::new(format!("event-{index}")).key(format!("key-{index}")))
            .await
            .unwrap();
    }
    let delivered = tokio::time::timeout(Duration::from_secs(15), first.receive())
        .await
        .unwrap()
        .unwrap();
    first.ack_cumulative(&delivered).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    proxy.set_mtu(0);
    let second = admin
        .new_stream_consumer(topic, "history")
        .build()
        .await
        .unwrap();
    let handed_off = tokio::time::timeout(Duration::from_secs(30), second.receive())
        .await
        .unwrap()
        .unwrap();
    let mut old_prefetch = Vec::new();
    loop {
        match first.try_receive() {
            Ok(Some(message)) => old_prefetch.push(message.message_id()),
            Ok(None) => break,
            Err(error) if recoverable_member_error(&error) => break,
            Err(error) => panic!("Unexpected old-member receive error: {error:#}"),
        }
    }
    assert!(
        !old_prefetch.contains(&handed_off.message_id()),
        "A disconnected member delivered a message after another member acquired its range"
    );
    proxy.set_mtu(1200);
    second.ack_cumulative(&handed_off).await.unwrap();
    producer.close().await.unwrap();
    let _ = first.close().await;
    second.close().await.unwrap();
    let _ = client.close().await;
    admin.close().await.unwrap();
    proxy.close().await;
    fixture.close().await;
}
