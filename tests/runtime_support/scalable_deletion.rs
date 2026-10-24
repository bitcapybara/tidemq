use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletion_rejects_online_members_and_releases_only_the_original_subscription() {
    let fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/delete";
    client.admin().create_logical_topic(topic).await.unwrap();
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
    let queue_id = client
        .admin()
        .queue_subscription(topic, "jobs")
        .await
        .unwrap()
        .id();
    let rejected = client
        .admin()
        .delete_queue_subscription(topic, "jobs")
        .await
        .unwrap_err();
    assert!(!rejected.outcome_unknown());
    assert_eq!(rejected.kind(), tidemq_sdk::ErrorKind::Conflict);
    assert!(
        client
            .admin()
            .pending_subscription_deletions()
            .unwrap()
            .is_empty()
    );
    let producer = client.new_producer(topic).build().await.unwrap();
    producer.send("protected-backlog").await.unwrap();
    queue.close().await.unwrap();
    let principal = client
        .admin()
        .new_principal(tenant, "reader", [tidemq_sdk::Permission::Consume])
        .build()
        .await
        .unwrap();
    let token = client
        .admin()
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    let reader = token_client(&fixture, token.expose_secret()).await;
    assert!(
        reader
            .admin()
            .queue_subscription(topic, "jobs")
            .await
            .is_ok()
    );
    let forbidden = reader
        .admin()
        .delete_queue_subscription(topic, "jobs")
        .await
        .unwrap_err();
    assert!(
        !forbidden.outcome_unknown(),
        "A pre-admission refusal became unknown: {forbidden:?}"
    );
    assert_eq!(forbidden.kind(), tidemq_sdk::ErrorKind::PermissionDenied);
    assert!(
        reader
            .admin()
            .pending_subscription_deletions()
            .unwrap()
            .is_empty()
    );
    reader.close().await.unwrap();
    let removed = client
        .admin()
        .delete_queue_subscription(topic, "jobs")
        .await
        .unwrap();
    assert_eq!(removed.subscription_id(), queue_id);
    assert!(
        client
            .admin()
            .queue_subscription(topic, "jobs")
            .await
            .is_err()
    );
    let kept = tokio::time::timeout(Duration::from_secs(15), stream.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kept.payload(), b"protected-backlog");
    stream.ack_cumulative(&kept).await.unwrap();
    stream.close().await.unwrap();
    client
        .admin()
        .delete_stream_subscription(topic, "events")
        .await
        .unwrap();
    let replacement = client
        .new_queue_consumer(topic, "jobs")
        .initial_position(tidemq_sdk::InitialPosition::Latest)
        .build()
        .await
        .unwrap();
    assert_ne!(
        client
            .admin()
            .queue_subscription(topic, "jobs")
            .await
            .unwrap()
            .id(),
        queue_id
    );
    producer.send("new-identity").await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(15), replacement.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.payload(), b"new-identity");
    replacement.ack(&message).await.unwrap();
    replacement.close().await.unwrap();
    producer.close().await.unwrap();
    assert_eq!(client.close().await.unwrap().unresolved_deletions(), 0);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_domain_deletion_resumes_after_restart_and_old_replay_cannot_delete_a_new_name() {
    let mut fixture = Fixture::new(&[(1, Role::Combined), (2, Role::Storage)], &[1], &[1]).await;
    configure_dynamic_fixture(&mut fixture).await;
    for config in fixture.configs.values_mut() {
        config.runtime.dynamic_auth = true;
        config.runtime.groups.retain(|group| group.group_id != 3);
        config.runtime.groups.push(GroupConfig {
            group_id: 3,
            voters: vec![2],
            standby: Vec::new(),
        });
    }
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/delete-recovery";
    client.admin().create_logical_topic(topic).await.unwrap();
    client.admin().split_topic_at(topic, 1 << 63).await.unwrap();
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .build()
        .await
        .unwrap();
    let old = client
        .admin()
        .queue_subscription(topic, "jobs")
        .await
        .unwrap()
        .id();
    queue.close().await.unwrap();
    fixture.stop(2).await;
    let operation = Id::new_v7().unwrap();
    let before = fixture
        .running
        .get(&1)
        .unwrap()
        .delete_consumer_subscription(
            operation,
            old.to_string().parse().unwrap(),
            tidemq_protocol::topic::SubscriptionKind::Queue,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(!before.complete);
    assert!(
        !before.deleted.is_empty(),
        "No available domain committed its deletion"
    );
    assert!(
        client
            .admin()
            .queue_subscription(topic, "jobs")
            .await
            .unwrap()
            .is_deleting()
    );
    assert!(
        client
            .new_queue_consumer(topic, "jobs")
            .build()
            .await
            .is_err()
    );
    let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(1)).unwrap();
    let interrupted = context
        .scope(client.admin().delete_queue_subscription(topic, "jobs"))
        .await
        .unwrap_err();
    assert!(interrupted.outcome_unknown());
    let pending = client
        .admin()
        .pending_subscription_deletions()
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(pending.subscription_id(), old);
    fixture.stop(1).await;
    fixture.running.insert(
        2,
        RunningNode::start(fixture.configs.get(&2).unwrap().clone())
            .await
            .unwrap(),
    );
    fixture.restart(1).await;
    let node = fixture.running.get(&1).unwrap();
    let until = Instant::now() + Duration::from_secs(20);
    let after = loop {
        let plan = node
            .delete_consumer_subscription(
                operation,
                before.subscription,
                tidemq_protocol::topic::SubscriptionKind::Queue,
                until,
            )
            .await
            .unwrap();
        if plan.complete {
            break plan;
        }
        assert!(
            Instant::now() < until,
            "The original deletion did not resume"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(after.id, before.id);
    assert_eq!(after.domains, before.domains);
    assert_eq!(after.deleted.len(), after.domains.len());
    assert_eq!(pending.retry().await.unwrap().subscription_id(), old);
    assert!(
        client
            .admin()
            .pending_subscription_deletions()
            .unwrap()
            .is_empty()
    );
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .build()
        .await
        .unwrap();
    let new_id = client
        .admin()
        .queue_subscription(topic, "jobs")
        .await
        .unwrap()
        .id();
    assert_ne!(old, new_id);
    let replay = node
        .delete_consumer_subscription(
            operation,
            before.subscription,
            tidemq_protocol::topic::SubscriptionKind::Queue,
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(replay, after);
    assert_eq!(pending.retry().await.unwrap().subscription_id(), old);
    assert_eq!(
        client
            .admin()
            .queue_subscription(topic, "jobs")
            .await
            .unwrap()
            .id(),
        new_id
    );
    tokio::fs::write(fixture.directory.join("subscription-delete-recovery.json"),
        serde_json::to_vec_pretty(&serde_json::json!({"before": before, "after": after, "replacement": new_id.to_string()})).unwrap()).await.unwrap();
    queue.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
