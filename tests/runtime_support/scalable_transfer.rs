use super::*;

#[cfg(feature = "test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_ack_delegation_refreshes_the_original_operation_when_the_authority_group_changes() {
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
    fixture.start_all().await;
    let client = fixture.client(5).await;
    create_logical_resources(&client).await;
    let topic = client.admin().topic_info(RUNTIME_TOPIC).await.unwrap();
    let partition = tidemq::metadata::PartitionKey {
        topic_id: topic.id().to_string().parse().unwrap(),
        partition: 0,
    };
    let queue = client
        .new_queue_consumer(RUNTIME_TOPIC, "delegation-jobs")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let original = producer.send("held").await.unwrap().message_id();
    let held = tokio::time::timeout_at(deadline(), queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.message_id(), original);
    let pause =
        tidemq::runtime::test_hooks::InstalledPause::consumer_delegation(partition).unwrap();
    let sender = queue.clone();
    let acknowledged = tokio::spawn(async move { sender.ack(&held).await });
    pause.wait(deadline()).await.unwrap();
    let shard = client.admin().topic_shards(RUNTIME_TOPIC).await.unwrap()[0].id;
    client.admin().transfer_shard(shard, 3).await.unwrap();
    let until = deadline();
    while fixture
        .running
        .get(&2)
        .unwrap()
        .route(partition, until)
        .await
        .unwrap()
        .group_id
        != 3
    {
        assert!(
            Instant::now() < until,
            "The source did not observe the new authority group"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    pause.resume();
    assert_eq!(
        acknowledged.await.unwrap().unwrap(),
        tidemq_sdk::AckReceipt::Acknowledged
    );
    assert!(queue.pending_acks().await.unwrap().is_empty());
    queue.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_operator_waiter_recovers_the_original_public_transfer_after_process_kill() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    configure_dynamic_fixture(&mut fixture).await;
    let mut process = start_process(&fixture, 1).await;
    let client = fixture
        .client_with_timeouts(1, Duration::from_secs(30), Duration::from_secs(5))
        .await;
    create_logical_resources(&client).await;
    let queue = client
        .new_queue_consumer(RUNTIME_TOPIC, "public-transfer-recovery")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let mut expected = Vec::new();
    for index in 0..16 {
        expected.push(
            producer
                .send(tidemq_sdk::Message::new(vec![index; 128 * 1024]).key("original-key"))
                .await
                .unwrap()
                .message_id(),
        );
    }
    let held = tokio::time::timeout_at(deadline(), queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.message_id(), expected[0]);
    let admin = client.admin();
    let shard = admin
        .topic_shards(RUNTIME_TOPIC)
        .await
        .unwrap()
        .first()
        .unwrap()
        .id;
    let (pending, before) = {
        let mut moving = std::pin::pin!(admin.transfer_shard(shard, 3));
        let until = Instant::now() + Duration::from_secs(30);
        loop {
            tokio::select! {
                result = &mut moving => panic!("Transfer completed before its interruption boundary: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(2)) => {
                    if let Some(pending) = admin.pending_operations().unwrap().first().cloned()
                        && let Some(status) = admin.authority_transfer_status(pending.operation_id()).await.unwrap()
                        && status.stage == tidemq_sdk::AuthorityTransferStage::Exported {
                        break (pending, status);
                    }
                    assert!(Instant::now() < until, "Public transfer did not reach its durable export stage");
                }
            }
        }
    };
    process.kill().await.unwrap();
    process.wait().await.unwrap();
    process = start_process(&fixture, 1).await;
    let receipt = pending.retry().await.unwrap();
    let tidemq_sdk::AdminReceipt::ShardTransferred(after) = receipt else {
        panic!("Original transfer returned a different administration receipt");
    };
    assert_eq!(after.operation, before.operation);
    assert_eq!(after.shard, before.shard);
    assert_eq!(after.source_group, before.source_group);
    assert_eq!(after.target_group, before.target_group);
    assert_eq!(after.stage, tidemq_sdk::AuthorityTransferStage::Retired);
    assert_eq!(
        admin
            .authority_transfer_status(after.operation)
            .await
            .unwrap(),
        Some(after)
    );
    assert!(admin.pending_operations().unwrap().is_empty());
    queue.ack(&held).await.unwrap();
    for (index, identity) in expected.iter().enumerate().skip(1) {
        let message = tokio::time::timeout_at(deadline(), queue.receive())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.message_id(), *identity);
        assert_eq!(message.payload(), vec![index as u8; 128 * 1024]);
        queue.ack(&message).await.unwrap();
    }
    let id = producer
        .send("after-public-transfer-recovery")
        .await
        .unwrap()
        .message_id();
    let message = tokio::time::timeout_at(deadline(), queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.message_id(), id);
    queue.ack(&message).await.unwrap();
    producer.close().await.unwrap();
    queue.close().await.unwrap();
    client.close().await.unwrap();
    process.kill().await.unwrap();
    process.wait().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_sdk_transfers_live_authority_and_tenant_credentials_cannot_move_or_inspect_it() {
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
    fixture.start_all().await;
    let client = fixture.client(5).await;
    let tenant = client.admin().create_tenant("test").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client.admin().create_topic(RUNTIME_TOPIC).await.unwrap();
    let principal = client
        .admin()
        .new_principal(
            tenant,
            "ordinary-application",
            [
                tidemq_sdk::Permission::Produce,
                tidemq_sdk::Permission::Consume,
                tidemq_sdk::Permission::Inspect,
            ],
        )
        .build()
        .await
        .unwrap();
    let token = client
        .admin()
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    let app = Client::builder(
        fixture.configs.get(&5).unwrap().listen.to_string(),
        tidemq_sdk::TlsTrust::ca_pem(fixture.roots.clone()),
        tidemq_sdk::Credentials::token(token.expose_secret()),
    )
    .build()
    .await
    .unwrap();
    let queue = client
        .new_queue_consumer(RUNTIME_TOPIC, "operator-jobs")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let stream = client
        .new_stream_consumer(RUNTIME_TOPIC, "operator-events")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let producer = app.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let original = producer
        .send(tidemq_sdk::Message::new("before-operator-transfer").key("same-key"))
        .await
        .unwrap()
        .message_id();
    let held_queue = tokio::time::timeout_at(deadline(), queue.receive())
        .await
        .unwrap()
        .unwrap();
    let held_stream = tokio::time::timeout_at(deadline(), stream.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held_queue.message_id(), original);
    assert_eq!(held_stream.message_id(), original);
    let shards = client.admin().topic_shards(RUNTIME_TOPIC).await.unwrap();
    let shard = shards.first().unwrap().id;
    let error = app.admin().transfer_shard(shard, 3).await.unwrap_err();
    assert_eq!(error.kind(), tidemq_sdk::ErrorKind::PermissionDenied);
    assert!(!error.outcome_unknown());
    #[cfg(feature = "test-hooks")]
    let delayed_ack = {
        let topic = client.admin().topic_info(RUNTIME_TOPIC).await.unwrap();
        let partition = tidemq::metadata::PartitionKey {
            topic_id: topic.id().to_string().parse().unwrap(),
            partition: 0,
        };
        let pause =
            tidemq::runtime::test_hooks::InstalledPause::consumer_authorization(partition).unwrap();
        let queue = queue.clone();
        let held = held_queue.clone();
        let task = tokio::spawn(async move { queue.ack(&held).await });
        pause.wait(deadline()).await.unwrap();
        (pause, task)
    };
    let receipt = client.admin().transfer_shard(shard, 3).await.unwrap();
    assert_eq!(receipt.stage, tidemq_sdk::AuthorityTransferStage::Retired);
    assert_eq!(receipt.shard, shard);
    assert_eq!((receipt.source_group, receipt.target_group), (2, 3));
    assert_eq!(
        client
            .admin()
            .authority_transfer_status(receipt.operation)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        app.admin()
            .authority_transfer_status(receipt.operation)
            .await
            .unwrap_err()
            .kind(),
        tidemq_sdk::ErrorKind::PermissionDenied
    );
    #[cfg(feature = "test-hooks")]
    {
        delayed_ack.0.resume();
        assert_eq!(
            delayed_ack.1.await.unwrap().unwrap(),
            tidemq_sdk::AckReceipt::Acknowledged
        );
    }
    #[cfg(not(feature = "test-hooks"))]
    queue.ack(&held_queue).await.unwrap();
    stream.ack_cumulative(&held_stream).await.unwrap();
    let next = producer
        .send(tidemq_sdk::Message::new("after-operator-transfer").key("same-key"))
        .await
        .unwrap()
        .message_id();
    let message = tokio::time::timeout_at(deadline(), queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.message_id(), next);
    queue.ack(&message).await.unwrap();
    let message = tokio::time::timeout_at(deadline(), stream.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.message_id(), next);
    stream.ack_cumulative(&message).await.unwrap();
    producer.close().await.unwrap();
    stream.close().await.unwrap();
    queue.close().await.unwrap();
    app.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_transfer_waiter_resumes_the_original_manifest_automatically_after_restart() {
    let mut fixture = dynamic_fixture().await;
    let client = fixture
        .client_with_timeouts(1, Duration::from_secs(30), Duration::from_secs(5))
        .await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/resumable";
    client.admin().create_topic(topic).await.unwrap();
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    let mut expected = Vec::new();
    for index in 0..16 {
        expected.push(
            producer
                .send(tidemq_sdk::Message::new(vec![index; 128 * 1024]))
                .await
                .unwrap()
                .message_id(),
        );
    }
    let held = tokio::time::timeout(Duration::from_secs(15), queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.message_id(), expected[0]);
    let shard = expected[0]
        .to_string()
        .split(':')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let operation = Id::new_v7().unwrap();
    let before = {
        let node = fixture.running.get(&1).unwrap();
        let until = Instant::now() + Duration::from_secs(30);
        let mut moving = Box::pin(node.transfer_shard(operation, shard, 3, until));
        loop {
            tokio::select! {
                result = &mut moving => panic!("Transfer finished before the interruption point: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(2)) => {
                    let status = node.authority_transfer_status(operation, until).await.unwrap();
                    if let Some(status) = status.filter(|status| status.stage == tidemq::metadata::TransferStage::Exported) { break status; }
                    assert!(Instant::now() < until, "Transfer did not reach its durable export stage");
                }
            }
        }
    };
    fixture.stop(1).await;
    fixture.restart(1).await;
    let node = fixture.running.get(&1).unwrap();
    let until = Instant::now() + Duration::from_secs(45);
    let report = loop {
        let status = node
            .authority_transfer_status(operation, until)
            .await
            .unwrap()
            .unwrap();
        if status.stage == tidemq::metadata::TransferStage::Retired {
            break status;
        }
        assert!(
            Instant::now() < until,
            "Background reconciliation did not finish the original transfer"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(report.manifest, before.manifest);
    assert_eq!(report.frozen_cut, before.frozen_cut);
    assert_eq!(report.id, operation);
    queue.ack(&held).await.unwrap();
    for identity in expected.iter().skip(1) {
        let message = tokio::time::timeout(Duration::from_secs(20), queue.receive())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.message_id(), *identity);
        queue.ack(&message).await.unwrap();
    }
    let identity = producer
        .send(tidemq_sdk::Message::new("after-resume"))
        .await
        .unwrap()
        .message_id();
    let message = tokio::time::timeout(Duration::from_secs(15), queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.message_id(), identity);
    queue.ack(&message).await.unwrap();
    tokio::fs::write(
        fixture.directory.join("resumed-authority-transfer.json"),
        serde_json::to_vec_pretty(
            &serde_json::json!({"before_restart": before, "after_restart": report}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    producer.close().await.unwrap();
    queue.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the verified RustFS 1.0.3 executable in TIDEMQ_TEST_RUSTFS_BIN"]
async fn transferred_cold_history_keeps_birth_object_keys_and_online_ack_state_after_restart() {
    let rustfs = rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    configure_dynamic_fixture(&mut fixture).await;
    enable_objects(&mut fixture, &rustfs).await;
    fixture
        .configs
        .get_mut(&1)
        .unwrap()
        .runtime
        .archive_interval_ms = 60_000;
    fixture.start_all().await;
    let client = fixture
        .client_with_timeouts(1, Duration::from_secs(30), Duration::from_secs(5))
        .await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/transferred-cold";
    client.admin().create_topic(topic).await.unwrap();
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let stream = client
        .new_stream_consumer(topic, "events")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    queue.close().await.unwrap();
    stream.close().await.unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    let mut expected = Vec::new();
    for index in 0..3 {
        let message = if index == 0 {
            tidemq_sdk::Message::new(vec![7; 256 * 1024])
        } else {
            tidemq_sdk::Message::new(format!("cold-{index}"))
                .key("same-key")
                .property("source", "birth-group")
        };
        expected.push(producer.send(message).await.unwrap().message_id());
    }
    let partition = tidemq::metadata::PartitionKey {
        topic_id: client
            .admin()
            .topic_info(topic)
            .await
            .unwrap()
            .id()
            .to_string()
            .parse()
            .unwrap(),
        partition: 0,
    };
    let node = fixture.running.get(&1).unwrap();
    let object = node
        .archive(
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
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let stream = client
        .new_stream_consumer(topic, "events")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let held_queue = tokio::time::timeout(Duration::from_secs(15), queue.receive())
        .await
        .unwrap()
        .unwrap();
    let held_stream = tokio::time::timeout(Duration::from_secs(15), stream.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held_queue.message_id(), expected[0]);
    assert_eq!(held_stream.message_id(), expected[0]);
    assert_eq!(held_queue.payload().len(), 256 * 1024);
    assert!(held_queue.payload().iter().all(|byte| *byte == 7));
    assert_eq!(held_stream.payload().len(), 256 * 1024);
    assert!(held_stream.payload().iter().all(|byte| *byte == 7));
    expected.push(
        producer
            .send(tidemq_sdk::Message::new("hot-3").key("same-key"))
            .await
            .unwrap()
            .message_id(),
    );
    let shard = expected[0]
        .to_string()
        .split(':')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let report = transfer_retry(node, Id::new_v7().unwrap(), shard, 3).await;
    assert_eq!(report.stage, tidemq::metadata::TransferStage::Retired);
    queue.ack(&held_queue).await.unwrap();
    stream.ack_cumulative(&held_stream).await.unwrap();
    node.archive(
        3,
        vec![tidemq::lifecycle::RangeRequest {
            partition,
            offsets: tidemq_protocol::state::OffsetRange { start: 3, end: 4 },
        }],
        deadline(),
    )
    .await
    .unwrap();
    assert!(
        node.reclaim_local(partition, 4, deadline()).await.is_err(),
        "Historical reclaim requires the target's own object checkpoint"
    );
    producer.close().await.unwrap();
    queue.close().await.unwrap();
    stream.close().await.unwrap();
    client.close().await.unwrap();
    fixture.stop(1).await;
    fixture
        .configs
        .get_mut(&1)
        .unwrap()
        .runtime
        .archive_interval_ms = 1000;
    fixture.restart(1).await;
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        if fixture
            .running
            .get(&1)
            .unwrap()
            .reclaim_local(partition, 4, until)
            .await
            .is_ok()
        {
            break;
        }
        assert!(
            Instant::now() < until,
            "Background recovery did not checkpoint transferred history"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let client = fixture.client(1).await;
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let stream = client
        .new_stream_consumer(topic, "events")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let mut seen = std::collections::HashSet::new();
    for _ in expected.iter().skip(1) {
        let message = tokio::time::timeout(Duration::from_secs(20), queue.receive())
            .await
            .unwrap()
            .unwrap();
        let index = expected
            .iter()
            .position(|identity| *identity == message.message_id())
            .unwrap();
        assert!(index > 0 && seen.insert(message.message_id()));
        let payload = if index < 3 {
            format!("cold-{index}")
        } else {
            "hot-3".into()
        };
        assert_eq!(message.payload(), payload.as_bytes());
        if index < 3 {
            assert_eq!(
                message.properties().get("source").unwrap().as_ref(),
                b"birth-group"
            );
        }
        queue.ack(&message).await.unwrap();
    }
    assert_eq!(seen.len(), expected.len() - 1);
    for (index, identity) in expected.iter().enumerate().skip(1) {
        let message = tokio::time::timeout(Duration::from_secs(20), stream.receive())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.message_id(), *identity);
        let payload = if index < 3 {
            format!("cold-{index}")
        } else {
            "hot-3".into()
        };
        assert_eq!(message.payload(), payload.as_bytes());
        stream.ack_cumulative(&message).await.unwrap();
    }
    tokio::fs::write(fixture.directory.join("cold-authority-transfer.json"), serde_json::to_vec_pretty(&serde_json::json!({"birth_object": object, "report": report, "message_ids": expected.iter().map(ToString::to_string).collect::<Vec<_>>() })).unwrap()).await.unwrap();
    queue.close().await.unwrap();
    stream.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

async fn transfer_retry(
    node: &RunningNode,
    operation: Id,
    shard: Id,
    target: u64,
) -> tidemq::metadata::AuthorityTransfer {
    let until = Instant::now() + Duration::from_secs(45);
    loop {
        match node.transfer_shard(operation, shard, target, until).await {
            Ok(report) => return report,
            Err(error) => {
                let retryable = error.failure().retryable;
                let detail = format!("{:#}", anyhow::Error::new(error));
                assert!(
                    retryable && Instant::now() < until,
                    "Authority transfer did not recover: {detail}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_independent_three_replica_target_snapshots_every_voter_before_source_retirement() {
    let mut fixture = Fixture::new(
        &[
            (1, Role::Combined),
            (2, Role::Combined),
            (3, Role::Combined),
            (4, Role::Storage),
            (5, Role::Storage),
            (6, Role::Storage),
        ],
        &[1, 2, 3],
        &[1, 2, 3],
    )
    .await;
    let bootstrap = fixture.directory.join("bootstrap.token");
    tokio::fs::write(&bootstrap, TOKEN).await.unwrap();
    private_file(bootstrap.clone()).await;
    for config in fixture.configs.values_mut() {
        config.runtime.bootstrap_admin_token_file = Some(bootstrap.clone());
        config.runtime.groups.push(GroupConfig {
            group_id: 3,
            voters: vec![4, 5, 6],
            standby: Vec::new(),
        });
    }
    fixture.start_all().await;
    let client = fixture
        .client_with_timeouts(1, Duration::from_secs(30), Duration::from_secs(10))
        .await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/independent";
    client.admin().create_topic(topic).await.unwrap();
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    let identity = producer
        .send(tidemq_sdk::Message::new(vec![7; 160 * 1024]))
        .await
        .unwrap()
        .message_id();
    let held = tokio::time::timeout(Duration::from_secs(15), queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.message_id(), identity);
    let shard = identity
        .to_string()
        .split(':')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let operation = Id::new_v7().unwrap();
    let until = Instant::now() + Duration::from_secs(45);
    let report = loop {
        let mut report = None;
        for node in 1..=3 {
            match fixture
                .running
                .get(&node)
                .unwrap()
                .transfer_shard(operation, shard, 3, until)
                .await
            {
                Ok(done) => {
                    report = Some(done);
                    break;
                }
                Err(error) => assert!(error.failure().retryable, "{:#}", anyhow::Error::new(error)),
            }
        }
        if let Some(report) = report {
            break report;
        }
        assert!(
            Instant::now() < until,
            "No metadata leader completed the original transfer"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(report.stage, tidemq::metadata::TransferStage::Retired);
    assert_eq!(report.source_voters, BTreeSet::from([1, 2, 3]));
    assert_eq!(report.checkpoint_voters, BTreeSet::from([4, 5, 6]));
    assert_eq!(
        report.checkpoints.keys().copied().collect::<BTreeSet<_>>(),
        report.checkpoint_voters
    );
    assert!(
        report
            .checkpoints
            .values()
            .all(|proof| proof.manifest_digest == report.manifest.as_ref().unwrap().digest)
    );
    queue.ack(&held).await.unwrap();
    for node in 1..=3 {
        fixture.stop(node).await;
    }
    for node in 1..=3 {
        let config = fixture.configs.get(&node).unwrap().clone();
        fixture
            .running
            .insert(node, RunningNode::start(config).await.unwrap());
    }
    fixture.wait_ready().await;
    let identity = producer
        .send(tidemq_sdk::Message::new("after-source-restart"))
        .await
        .unwrap()
        .message_id();
    let message = tokio::time::timeout(Duration::from_secs(20), queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.message_id(), identity);
    queue.ack(&message).await.unwrap();
    tokio::fs::write(
        fixture.directory.join("authority-transfer.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .await
    .unwrap();
    producer.close().await.unwrap();
    queue.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn online_stream_and_queue_keep_original_deliveries_across_authority_transfer_and_move_back()
{
    let mut fixture = dynamic_fixture().await;
    let client = fixture
        .client_with_timeouts(1, Duration::from_secs(30), Duration::from_secs(5))
        .await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/transferred";
    client.admin().create_topic(topic).await.unwrap();
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let stream = client
        .new_stream_consumer(topic, "events")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    let mut expected = Vec::new();
    for index in 0..3 {
        expected.push(
            producer
                .send(tidemq_sdk::Message::new(vec![index; 70 * 1024]).key("same-key"))
                .await
                .unwrap()
                .message_id(),
        );
    }
    let held_queue = tokio::time::timeout(Duration::from_secs(15), queue.receive())
        .await
        .unwrap()
        .unwrap();
    let held_stream = tokio::time::timeout(Duration::from_secs(15), stream.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held_queue.message_id(), expected[0]);
    assert_eq!(held_stream.message_id(), expected[0]);
    // Trusted node control uses the stable birth identity printed in a diagnostic message ID.
    let diagnostic = expected[0].to_string();
    let shard = diagnostic.split(':').nth(1).unwrap().parse().unwrap();
    let operation = Id::new_v7().unwrap();
    let report = transfer_retry(fixture.running.get(&1).unwrap(), operation, shard, 3).await;
    assert_eq!(report.stage, tidemq::metadata::TransferStage::Retired);
    assert_eq!(report.source.group_id, 2);
    assert_eq!(report.target.group_id, 3);
    assert!(report.manifest.as_ref().unwrap().bytes > 3 * 65_536);
    assert_eq!(report.checkpoints.len(), 1);
    assert_eq!(
        fixture
            .running
            .get(&1)
            .unwrap()
            .transfer_shard(operation, shard, 3, deadline())
            .await
            .unwrap(),
        report
    );
    queue.ack(&held_queue).await.unwrap();
    stream.ack_cumulative(&held_stream).await.unwrap();
    expected.push(
        producer
            .send(tidemq_sdk::Message::new(vec![3; 70 * 1024]).key("same-key"))
            .await
            .unwrap()
            .message_id(),
    );
    for identity in expected.iter().skip(1) {
        let message = tokio::time::timeout(Duration::from_secs(20), queue.receive())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.message_id(), *identity);
        queue.ack(&message).await.unwrap();
        let message = tokio::time::timeout(Duration::from_secs(20), stream.receive())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.message_id(), *identity);
        stream.ack_cumulative(&message).await.unwrap();
    }
    let report = transfer_retry(
        fixture.running.get(&1).unwrap(),
        Id::new_v7().unwrap(),
        shard,
        2,
    )
    .await;
    assert_eq!(report.stage, tidemq::metadata::TransferStage::Retired);
    assert_eq!(report.target.group_id, 2);
    producer.close().await.unwrap();
    queue.close().await.unwrap();
    stream.close().await.unwrap();
    client.close().await.unwrap();
    fixture.stop(1).await;
    fixture.restart(1).await;
    let client = fixture.client(1).await;
    let producer = client.new_producer(topic).build().await.unwrap();
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .build()
        .await
        .unwrap();
    let stream = client
        .new_stream_consumer(topic, "events")
        .build()
        .await
        .unwrap();
    let identity = producer
        .send(tidemq_sdk::Message::new("after-restart").key("same-key"))
        .await
        .unwrap()
        .message_id();
    let message = tokio::time::timeout(Duration::from_secs(15), queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.message_id(), identity);
    queue.ack(&message).await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(15), stream.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.message_id(), identity);
    stream.ack_cumulative(&message).await.unwrap();
    producer.close().await.unwrap();
    queue.close().await.unwrap();
    stream.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
