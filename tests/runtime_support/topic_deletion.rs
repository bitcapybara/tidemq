use super::*;

#[cfg(feature = "test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_publish_reply_recovers_its_original_success_after_topic_removal() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let admin = fixture.client(1).await;
    let topic = create_logical_resources(&admin).await;
    let tenants = admin.admin().tenants().next_page().await.unwrap().unwrap();
    let tenant = tenants.first().unwrap().id;
    let namespaces = admin
        .admin()
        .namespaces(tenant)
        .next_page()
        .await
        .unwrap()
        .unwrap();
    let namespace = namespaces.first().unwrap().id;
    let backend = fixture.configs.get(&1).unwrap().listen;
    let proxy = udp_proxy::Proxy::start([backend, backend]).await;
    let client = Client::builder(
        proxy.address.to_string(),
        tidemq_sdk::TlsTrust::ca_pem(fixture.roots.clone()),
        tidemq_sdk::Credentials::token(TOKEN),
    )
    .request_lanes(1)
    .request_timeout(Duration::from_secs(15))
    .build()
    .await
    .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    producer.send("warm-up").await.unwrap();
    let partition = tidemq::metadata::PartitionKey {
        topic_id: topic.id().to_string().parse().unwrap(),
        partition: 0,
    };
    let hook = tidemq::runtime::test_hooks::InstalledPause::publish(partition, 2).unwrap();
    let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(10)).unwrap();
    let send_context = context.clone();
    let sender = producer.clone();
    let sending = tokio::spawn(async move {
        send_context
            .scope(sender.send("committed-without-reply"))
            .await
    });
    hook.wait(deadline()).await.unwrap();
    proxy.set_response_mtu(0);
    hook.resume();
    let queue = admin
        .new_queue_consumer(RUNTIME_TOPIC, "proof")
        .build()
        .await
        .unwrap();
    let first = queue.receive().await.unwrap();
    assert_eq!(first.payload(), b"warm-up");
    queue.ack(&first).await.unwrap();
    let committed = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(5))
        .unwrap()
        .scope(queue.receive())
        .await
        .unwrap();
    assert_eq!(committed.payload(), b"committed-without-reply");
    assert!(proxy.snapshot().over_mtu > 0);
    context.cancellation_token().cancel();
    let result = sending.await.unwrap();
    assert!(result.as_ref().is_err_and(|error| error.outcome_unknown()));
    let pending = producer.pending_sends().await.unwrap();
    assert_eq!(pending.len(), 1);
    drop(hook);
    queue.ack(&committed).await.unwrap();
    queue.close().await.unwrap();
    admin
        .admin()
        .delete_queue_subscription(RUNTIME_TOPIC, "proof")
        .await
        .unwrap();
    admin.admin().delete_topic(RUNTIME_TOPIC).await.unwrap();
    admin.admin().delete_namespace(namespace).await.unwrap();
    admin.admin().delete_tenant(tenant).await.unwrap();
    let replacement = admin.admin().create_tenant("test").await.unwrap();
    assert_ne!(replacement, tenant);
    admin
        .admin()
        .create_namespace(replacement, "app")
        .await
        .unwrap();
    admin.admin().create_topic(RUNTIME_TOPIC).await.unwrap();
    proxy.set_response_mtu(1200);
    let receipt = pending.first().unwrap().retry().await.unwrap();
    assert_eq!(receipt.message_id(), committed.message_id());
    assert!(producer.pending_sends().await.unwrap().is_empty());
    producer.close().await.unwrap();
    client.close().await.unwrap();
    admin.close().await.unwrap();
    proxy.close().await;
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_old_producer_cannot_publish_into_a_recreated_topic() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let original = create_logical_resources(&client).await;
    let old = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    old.send("old-resource").await.unwrap();
    client.admin().delete_topic(RUNTIME_TOPIC).await.unwrap();
    let replacement = client.admin().create_topic(RUNTIME_TOPIC).await.unwrap();
    assert_ne!(original.id(), replacement.id());
    let rejected = old
        .send("must-not-cross-resource-identity")
        .await
        .unwrap_err();
    assert!(
        !rejected.outcome_unknown(),
        "A proven uncommitted send to a deleted topic must be definite: {rejected:?}"
    );
    let queue = client
        .new_queue_consumer(RUNTIME_TOPIC, "jobs")
        .build()
        .await
        .unwrap();
    let current = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let receipt = current.send("replacement-only").await.unwrap();
    let message = queue.receive().await.unwrap();
    assert_eq!(message.message_id(), receipt.message_id());
    assert_eq!(message.payload(), b"replacement-only");
    queue.ack(&message).await.unwrap();
    queue.close().await.unwrap();
    current.close().await.unwrap();
    assert!(old.pending_sends().await.unwrap().is_empty());
    old.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topic_removal_resumes_partial_seals_and_never_deletes_a_recreated_name() {
    let nodes = [
        (1, Role::Metadata),
        (2, Role::Storage),
        (3, Role::Storage),
        (4, Role::Broker),
        (5, Role::Gateway),
    ];
    let mut fixture = Fixture::new(&nodes, &[1], &[2]).await;
    for config in fixture.configs.values_mut() {
        config.runtime.groups.push(GroupConfig {
            group_id: 3,
            voters: vec![3],
            standby: vec![],
        });
    }
    fixture.start_all().await;
    let client = fixture.client(5).await;
    let original = create_logical_resources(&client).await;
    let queue = client
        .new_queue_consumer(RUNTIME_TOPIC, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let receipt = producer.send("protected").await.unwrap();
    let message = queue.receive().await.unwrap();
    assert_eq!(receipt.message_id(), message.message_id());
    queue.ack(&message).await.unwrap();
    queue.close().await.unwrap();
    let error = client
        .admin()
        .delete_topic(RUNTIME_TOPIC)
        .await
        .unwrap_err();
    assert!(
        !error.outcome_unknown(),
        "A durable subscription must block topic removal: {error:?}"
    );
    assert_eq!(error.kind(), tidemq_sdk::ErrorKind::Conflict);
    client
        .admin()
        .delete_queue_subscription(RUNTIME_TOPIC, "jobs")
        .await
        .unwrap();
    client
        .admin()
        .split_topic_at(RUNTIME_TOPIC, 1 << 63)
        .await
        .unwrap();
    producer.close().await.unwrap();
    fixture.stop(3).await;
    let result = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(2))
        .unwrap()
        .scope(client.admin().delete_topic(RUNTIME_TOPIC))
        .await;
    assert!(
        result.as_ref().is_err_and(|error| error.outcome_unknown()),
        "Missing data authority cannot produce removal success: {result:?}"
    );
    let pending = client.admin().pending_operations().unwrap();
    assert_eq!(pending.len(), 1);
    assert!(client.new_producer(RUNTIME_TOPIC).build().await.is_err());
    assert!(
        client
            .new_queue_consumer(RUNTIME_TOPIC, "too-late")
            .build()
            .await
            .is_err()
    );
    assert!(client.admin().create_topic(RUNTIME_TOPIC).await.is_err());
    fixture.stop(1).await;
    fixture.restart(1).await;
    fixture.restart(3).await;
    let receipt = pending.first().unwrap().retry().await.unwrap();
    assert!(matches!(receipt, tidemq_sdk::AdminReceipt::TopicDeleted(id) if id == original.id()));
    assert!(client.admin().topic_info(RUNTIME_TOPIC).await.is_err());
    let replacement = client.admin().create_topic(RUNTIME_TOPIC).await.unwrap();
    assert_ne!(replacement.id(), original.id());
    assert!(matches!(pending.first().unwrap().retry().await.unwrap(),
        tidemq_sdk::AdminReceipt::TopicDeleted(id) if id == original.id()));
    assert_eq!(
        client.admin().topic_info(RUNTIME_TOPIC).await.unwrap().id(),
        replacement.id()
    );
    let next = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    next.send("replacement").await.unwrap();
    next.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
