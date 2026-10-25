use super::*;
use std::collections::HashSet;
use tidemq_sdk::{Credentials, Permission, PolicyUpdate, ResourceLimits, TlsTrust};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_small_domains_return_unused_rate_slices_and_preserve_committed_messages() {
    let fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("fair").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client
        .admin()
        .set_tenant_limits(
            tenant,
            ResourceLimits::builder()
                .publish_bytes_per_second(2 * 1024 * 1024)
                .consume_bytes_per_second(2 * 1024 * 1024)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let mut producers = Vec::new();
    let mut queues = Vec::new();
    for index in 0..3 {
        let topic = format!("fair/app/small-{index}");
        client.admin().create_topic(&topic).await.unwrap();
        queues.push(
            client
                .new_queue_consumer(&topic, "jobs")
                .build()
                .await
                .unwrap(),
        );
        producers.push(client.new_producer(&topic).build().await.unwrap());
    }
    let (stop, _) = tokio::sync::watch::channel(false);
    let mut workers = Vec::new();
    for producer in producers.iter().take(2) {
        let first = producer.send("active").await.unwrap().message_id();
        let producer = producer.clone();
        let mut stop = stop.subscribe();
        workers.push(tokio::spawn(async move {
            let mut ids = vec![first];
            loop {
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {},
                }
                ids.push(logical_send(&producer, b"active").await);
            }
            ids
        }));
    }
    let third = producers.get(2).unwrap().clone();
    let sender = third.clone();
    let sending = tokio::spawn(async move {
        let until = Instant::now() + Duration::from_secs(25);
        let mut pending: Option<tidemq_sdk::PendingSend> = None;
        loop {
            let result = tidemq_sdk::RequestContext::with_deadline(
                until.min(Instant::now() + Duration::from_secs(2)),
            )
            .scope(async {
                match &pending {
                    Some(pending) => pending.retry().await,
                    None => sender.send("waiting").await,
                }
            })
            .await;
            match result {
                Ok(receipt) => break receipt.message_id(),
                Err(error) => {
                    assert!(
                        Instant::now() < until,
                        "Rate credit was not returned: {error:?}"
                    );
                    if error.outcome_unknown() {
                        let retained = sender.pending_sends().await.unwrap();
                        assert_eq!(retained.len(), 1);
                        pending = retained.into_iter().next();
                    } else {
                        assert!(
                            matches!(
                                error.kind(),
                                tidemq_sdk::ErrorKind::Unavailable
                                    | tidemq_sdk::ErrorKind::ResourceExhausted
                                    | tidemq_sdk::ErrorKind::DeadlineExceeded
                            ),
                            "Unexpected send rejection: {error:?}"
                        );
                        pending = None;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !sending.is_finished(),
        "The other domains still hold the rate slices"
    );
    let identity = sending.await.unwrap();
    stop.send(true).unwrap();
    for (worker, queue) in workers.into_iter().zip(&queues) {
        let ids = worker.await.unwrap();
        assert!(
            ids.len() > 3,
            "The original domains must stay active while rate credit is reclaimed"
        );
        logical_consume(queue, &ids, b"active").await;
    }
    logical_consume(queues.get(2).unwrap(), &[identity], b"waiting").await;
    for queue in queues {
        queue.close().await.unwrap();
    }
    for producer in producers {
        producer.close().await.unwrap();
    }
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lowering_backlog_below_usage_fences_new_writes_and_preserves_ack_and_original_result_recovery()
 {
    let mut fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/limited";
    client.admin().create_topic(topic).await.unwrap();
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    let mut expected = HashSet::new();
    for _ in 0..3 {
        expected.insert(producer.send(vec![7; 512]).await.unwrap().message_id());
    }
    let held = queue.receive().await.unwrap();
    assert!(expected.contains(&held.message_id()));
    let limits = ResourceLimits::builder().backlog_bytes(1).build().unwrap();
    assert_eq!(
        client
            .admin()
            .set_tenant_limits(tenant, limits)
            .await
            .unwrap(),
        PolicyUpdate::Applied
    );
    let blocked = tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(300))
        .unwrap()
        .scope(producer.send("held-until-quota-recovers"))
        .await
        .unwrap_err();
    let tidemq_sdk::Error::SendUnresolved { pending, .. } = blocked else {
        panic!("Expected the original pending send");
    };
    queue.ack(&held).await.unwrap();
    assert_eq!(
        client
            .admin()
            .set_tenant_limits(tenant, ResourceLimits::default())
            .await
            .unwrap(),
        PolicyUpdate::Applied
    );
    let resumed = match pending.retry().await {
        Ok(receipt) => receipt,
        Err(error) => {
            assert!(
                !error.outcome_unknown(),
                "The original send is still unresolved: {error:?}"
            );
            producer.send("after-quota-recovers").await.unwrap()
        }
    };
    expected.insert(resumed.message_id());
    let mut seen = HashSet::from([held.message_id()]);
    while seen.len() < expected.len() {
        let message = tokio::time::timeout(Duration::from_secs(15), queue.receive())
            .await
            .unwrap()
            .unwrap();
        assert!(expected.contains(&message.message_id()));
        assert!(seen.insert(message.message_id()));
        queue.ack(&message).await.unwrap();
    }
    assert_eq!(seen, expected);
    queue.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.stop(1).await;
    fixture.restart(1).await;
    let client = fixture.client(1).await;
    assert_eq!(
        client
            .admin()
            .topic_info(topic)
            .await
            .unwrap()
            .shard_count(),
        1
    );
    client.close().await.unwrap();
    fixture.close().await;
}

async fn application_client(
    fixture: &Fixture,
    node: u64,
    token: &str,
) -> tidemq_sdk::Result<Client> {
    Client::builder(
        fixture.configs.get(&node).unwrap().listen.to_string(),
        TlsTrust::ca_pem(fixture.roots.clone()),
        Credentials::token(token),
    )
    .connect_timeout(Duration::from_millis(700))
    .request_timeout(Duration::from_secs(3))
    .build()
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_limits_are_shared_by_gateways_and_return_credit_after_close() {
    let mut fixture = Fixture::new(&[(1, Role::Combined), (2, Role::Gateway)], &[1], &[1]).await;
    fixture.start_all().await;
    let admin = fixture.client(1).await;
    let tenant = admin.admin().create_tenant("acme").await.unwrap();
    let namespace = admin.admin().create_namespace(tenant, "app").await.unwrap();
    let topic = "acme/app/connection-limit";
    admin.admin().create_topic(topic).await.unwrap();
    admin
        .admin()
        .set_tenant_limits(tenant, ResourceLimits::builder().topics(1).build().unwrap())
        .await
        .unwrap();
    assert_eq!(
        admin
            .admin()
            .set_namespace_limits(
                namespace,
                ResourceLimits::builder()
                    .backlog_bytes(256)
                    .build()
                    .unwrap()
            )
            .await
            .unwrap(),
        PolicyUpdate::Applied
    );
    assert!(
        admin
            .admin()
            .set_namespace_limits(
                namespace,
                ResourceLimits::builder().topics(2).build().unwrap()
            )
            .await
            .is_err()
    );
    let principal = admin
        .admin()
        .new_principal(tenant, "reader", [Permission::Inspect])
        .namespace(namespace)
        .build()
        .await
        .unwrap();
    let token = admin
        .admin()
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    admin
        .admin()
        .set_tenant_limits(
            tenant,
            ResourceLimits::builder().connections(1).build().unwrap(),
        )
        .await
        .unwrap();
    let first = application_client(&fixture, 1, token.expose_secret())
        .await
        .unwrap();
    first.admin().topic_info(topic).await.unwrap();
    assert!(
        application_client(&fixture, 2, token.expose_secret())
            .await
            .is_err()
    );
    first.close().await.unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    let second = loop {
        match application_client(&fixture, 2, token.expose_secret()).await {
            Ok(client) => break client,
            Err(error) => {
                assert!(
                    Instant::now() < until,
                    "Connection quota was not returned: {error:?}"
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    second.admin().topic_info(topic).await.unwrap();
    admin
        .admin()
        .set_principal_enabled(principal, false)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(second.admin().topic_info(topic).await.is_err());
    second.close().await.unwrap();
    admin.close().await.unwrap();
    fixture.close().await;
}

#[cfg(feature = "test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the verified RustFS 1.0.3 executable in TIDEMQ_TEST_RUSTFS_BIN"]
async fn cold_read_leases_prevent_another_subscription_from_multiplying_the_tenant_io_limit() {
    let rustfs = rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    enable_objects(&mut fixture, &rustfs).await;
    fixture
        .configs
        .get_mut(&1)
        .unwrap()
        .runtime
        .archive_interval_ms = 60_000;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/cold-quota";
    let info = client.admin().create_topic(topic).await.unwrap();
    for name in ["first", "second"] {
        client
            .new_queue_consumer(topic, name)
            .build()
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
    }
    let producer = client.new_producer(topic).build().await.unwrap();
    let receipt = producer.send("cold-lease-body").await.unwrap();
    let partition = tidemq::metadata::PartitionKey {
        topic_id: info.id().to_string().parse().unwrap(),
        partition: 0,
    };
    let node = fixture.running.get(&1).unwrap();
    node.archive(
        2,
        vec![tidemq::lifecycle::RangeRequest {
            partition,
            offsets: tidemq_protocol::state::OffsetRange { start: 0, end: 1 },
        }],
        deadline(),
    )
    .await
    .unwrap();
    node.reclaim_local(partition, 1, deadline()).await.unwrap();
    client
        .admin()
        .set_tenant_limits(
            tenant,
            ResourceLimits::builder().cold_reads(1).build().unwrap(),
        )
        .await
        .unwrap();
    let pause = tidemq::runtime::test_hooks::InstalledPause::cold_read(partition).unwrap();
    let first = client
        .new_queue_consumer(topic, "first")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    pause.wait(deadline()).await.unwrap();
    let second = client
        .new_queue_consumer(topic, "second")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(250), second.receive())
            .await
            .is_err()
    );
    pause.resume();
    for consumer in [&first, &second] {
        let message = tokio::time::timeout(Duration::from_secs(15), consumer.receive())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.message_id(), receipt.message_id());
        assert_eq!(message.payload(), b"cold-lease-body");
        consumer.ack(&message).await.unwrap();
        consumer.close().await.unwrap();
    }
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
