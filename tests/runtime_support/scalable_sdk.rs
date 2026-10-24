use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_close_wakes_unadmitted_senders_and_keeps_a_completed_receipt_retrievable() {
    let fixture = dynamic_fixture().await;
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
    let tenant = client.admin().create_tenant("closing").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "closing/app/events";
    client.admin().create_topic(topic).await.unwrap();
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client
        .new_producer(topic)
        .max_pending_sends(1)
        .build()
        .await
        .unwrap();
    proxy.set_mtu(1199);
    {
        let mut original = std::pin::pin!(producer.send("owned-before-close"));
        std::future::poll_fn(|context| {
            assert!(std::future::Future::poll(original.as_mut(), context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }
    proxy.set_mtu(1200);
    wait_runtime_counter(&fixture, 1, "tidemq_producer_new_messages_total").await;
    let pending = producer.pending_sends().await.unwrap();
    assert_eq!(pending.len(), 1);
    let queued = producer.clone();
    let (ready, started) = tokio::sync::oneshot::channel();
    let waiting = tokio::spawn(async move {
        let mut sending = std::pin::pin!(queued.send("never-admitted"));
        std::future::poll_fn(|context| {
            assert!(std::future::Future::poll(sending.as_mut(), context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        ready.send(()).unwrap();
        sending.await
    });
    started.await.unwrap();
    assert_eq!(producer.pending_sends().await.unwrap().len(), 1);
    let report = producer.close().await.unwrap();
    assert!(report.local_cleanup_complete());
    assert_eq!(report.unresolved_sends(), 0);
    assert_eq!(report.unclaimed_results(), 1);
    let failure = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(failure.kind(), tidemq_sdk::ErrorKind::Closed);
    let receipt = pending.first().unwrap().retry().await.unwrap();
    let delivery = queue.receive().await.unwrap();
    assert_eq!(delivery.message_id(), receipt.message_id());
    assert_eq!(delivery.payload(), b"owned-before-close");
    queue.ack(&delivery).await.unwrap();
    assert!(queue.try_receive().unwrap().is_none());
    queue.close().await.unwrap();
    client.close().await.unwrap();
    drop((queue, producer, client));
    proxy.close().await;
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_sends_leave_request_and_byte_capacity_for_receiving_and_acknowledging() {
    send_pressure(4, 8 * 1024).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_send_bytes_preserve_receiving_and_acknowledgement_capacity() {
    send_pressure(8, 96 * 1024).await;
}

async fn send_pressure(requests: u32, payload_bytes: usize) {
    let fixture = dynamic_fixture().await;
    let backend = fixture.configs.get(&1).unwrap().listen;
    let proxy = udp_proxy::Proxy::start([backend, backend]).await;
    let client = Client::builder(
        proxy.address.to_string(),
        tidemq_sdk::TlsTrust::ca_pem(fixture.roots.clone()),
        tidemq_sdk::Credentials::token(TOKEN),
    )
    .namespace("budget/app")
    .pending_requests(requests)
    .pending_bytes(2 * 1024 * 1024)
    .request_timeout(Duration::from_secs(5))
    .build()
    .await
    .unwrap();
    let tenant = client.admin().create_tenant("budget").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client.admin().create_topic("events").await.unwrap();
    let queue = client
        .new_queue_consumer("events", "jobs")
        .prefetch_messages(1)
        .prefetch_bytes(256 * 1024)
        .build()
        .await
        .unwrap();
    let producer = client.new_producer("events").build().await.unwrap();
    let mut initial = Vec::new();
    for _ in 0..3 {
        initial.push(producer.send("small").await.unwrap().message_id());
    }
    let held = queue.receive().await.unwrap();
    assert_eq!(held.message_id(), *initial.first().unwrap());
    proxy.set_mtu(1199);
    let payload = vec![7; payload_bytes];
    let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(500)).unwrap();
    let (first, second) = tokio::join!(
        context.scope(producer.send(tidemq_sdk::Message::new(payload.clone()).key("large-a"))),
        context.scope(producer.send(tidemq_sdk::Message::new(payload.clone()).key("large-b"))),
    );
    assert!(first.unwrap_err().outcome_unknown());
    assert!(second.unwrap_err().outcome_unknown());
    let pending = producer.pending_sends().await.unwrap();
    assert_eq!(pending.len(), 2);
    assert!(client.metrics().snapshot().admitted_bytes >= (2 * payload.len()) as u64);
    {
        let mut third = std::pin::pin!(producer.send(tidemq_sdk::Message::new(payload.clone())));
        std::future::poll_fn(|context| {
            assert!(std::future::Future::poll(third.as_mut(), context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }
    assert_eq!(
        producer.pending_sends().await.unwrap().len(),
        2,
        "Pending publication must preserve shared request and byte capacity for Read"
    );
    assert!(proxy.snapshot().over_mtu > 0);
    queue.ack(&held).await.unwrap();
    drop(held);
    logical_consume(&queue, initial.get(1..).unwrap(), b"small").await;
    proxy.set_mtu(1200);
    let mut restored = Vec::new();
    for send in &pending {
        let identity = logical_retry(send, deadline()).await;
        assert!(!initial.contains(&identity));
        assert!(!restored.contains(&identity));
        restored.push(identity);
    }
    logical_consume(&queue, &restored, &payload).await;
    let subsequent = producer.send("after-recovery").await.unwrap().message_id();
    logical_consume(&queue, &[subsequent], b"after-recovery").await;
    drop(pending);
    assert_eq!(client.metrics().snapshot().acknowledged, 6);
    queue.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    drop((queue, producer, client));
    proxy.close().await;
    fixture.close().await;
}

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
    client.admin().create_topic("events").await.unwrap();
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
    assert_eq!(client.metrics().snapshot().pending_sends, 0);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_partial_batch_keeps_the_known_receipt_and_original_pending_handles() {
    let fixture = dynamic_fixture().await;
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
    let tenant = client.admin().create_tenant("batch").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "batch/app/events";
    client.admin().create_topic(topic).await.unwrap();
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    producer.send("warmup").await.unwrap();
    let warmup = queue.receive().await.unwrap();
    queue.ack(&warmup).await.unwrap();
    drop(warmup);
    queue.close().await.unwrap();
    drop(queue);
    proxy.set_mtu(1199);
    let payload = vec![7; 96 * 1024];
    let retained =
        tidemq_protocol::topic::delivery_bytes(b"large", &payload, &BTreeMap::new()).unwrap() * 2
            + 8192;
    let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(10)).unwrap();
    let waiting_context = context.clone();
    let sending = producer.clone();
    let waiting = tokio::spawn(async move {
        waiting_context
            .scope(sending.send_batch(vec![
                tidemq_sdk::Message::new("known-small").key("small"),
                tidemq_sdk::Message::new(payload).key("large"),
            ]))
            .await
    });
    let until = Instant::now() + Duration::from_secs(3);
    loop {
        if client.metrics().snapshot().admitted_bytes == retained {
            break;
        }
        assert!(
            Instant::now() < until,
            "The first send must settle before cancelling its batch"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let metrics = http(
        fixture.running.get(&1).unwrap().management_address,
        "/metrics",
    )
    .await;
    assert!(metrics.contains("tidemq_producer_new_messages_total 2"));
    context.cancellation_token().cancel();
    let error = waiting.await.unwrap().unwrap_err();
    assert!(error.outcome_unknown());
    let pending = producer.pending_sends().await.unwrap();
    assert_eq!(
        pending.len(),
        2,
        "Cancellation must retain completed results until the batch returns"
    );
    let tidemq_sdk::Error::BatchSendInterrupted {
        pending: original, ..
    } = error
    else {
        panic!("Cancellation must return the original handles for this batch");
    };
    assert_eq!(original.len(), 2);
    proxy.set_mtu(1200);
    let mut expected = BTreeSet::new();
    for send in original {
        assert!(expected.insert(send.retry().await.unwrap().message_id().to_string()));
    }
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .build()
        .await
        .unwrap();
    let mut bodies = BTreeSet::new();
    for _ in 0..2 {
        let delivery = queue.receive().await.unwrap();
        assert!(expected.remove(&delivery.message_id().to_string()));
        bodies.insert(delivery.payload().len());
        queue.ack(&delivery).await.unwrap();
    }
    assert!(expected.is_empty());
    assert_eq!(bodies, BTreeSet::from([11, 96 * 1024]));
    assert!(producer.pending_sends().await.unwrap().is_empty());
    queue.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    drop((pending, queue, producer, client));
    proxy.close().await;
    fixture.close().await;
}
