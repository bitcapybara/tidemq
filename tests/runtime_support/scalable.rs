use super::*;

async fn dynamic_fixture() -> Fixture {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    let bootstrap = fixture.directory.join("bootstrap.token");
    tokio::fs::write(&bootstrap, TOKEN).await.unwrap();
    private_file(bootstrap.clone()).await;
    let config = fixture.configs.get_mut(&1).unwrap();
    config.runtime.dynamic_auth = true;
    config.runtime.bootstrap_admin_token_file = Some(bootstrap);
    config.runtime.groups.push(GroupConfig {
        group_id: 3,
        voters: vec![1],
        standby: Vec::new(),
    });
    fixture.start_all().await;
    fixture
}

async fn token_client(fixture: &Fixture, token: &str) -> Client {
    let config = fixture.configs.get(&1).unwrap();
    Client::with_target(
        ClientConfig::new("node-1.local:7443"),
        ClientTls {
            trust_roots: fixture.roots.clone(),
            identity: None,
            expected_nodes: Vec::new(),
        },
        PeerTarget {
            address: config.listen,
            server_name: "node-1.local".into(),
            local_role: "sdk".into(),
            remote_role: "combined".into(),
            cluster_id: None,
            node_id: None,
        },
        token.into(),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_topic_split_merge_and_credentials_survive_a_real_runtime_restart() {
    let mut fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    let namespace = client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let principal = client
        .admin()
        .new_principal(tenant, "writer", [tidemq_sdk::Permission::Produce])
        .namespace(namespace)
        .build()
        .await
        .unwrap();
    let token = client
        .admin()
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    let topic = client
        .admin()
        .create_logical_topic("acme/app/orders")
        .await
        .unwrap();
    assert_eq!(topic.shard_count(), 1);
    let split = client
        .admin()
        .split_topic_at("acme/app/orders", 1 << 63)
        .await
        .unwrap();
    assert_eq!(split.shard_count(), 2);
    assert_eq!(split.id(), topic.id());
    let merged = client
        .admin()
        .merge_topic_at("acme/app/orders", 1 << 63)
        .await
        .unwrap();
    assert_eq!(merged.shard_count(), 1);
    assert!(merged.revision() > split.revision());
    let app = token_client(&fixture, token.expose_secret()).await;
    assert_eq!(
        app.admin()
            .topic_info("acme/app/orders")
            .await
            .unwrap()
            .id(),
        topic.id()
    );
    assert!(app.admin().create_tenant("forbidden").await.is_err());
    client.close().await.unwrap();
    app.close().await.unwrap();
    fixture.stop(1).await;
    tokio::fs::remove_file(fixture.directory.join("bootstrap.token"))
        .await
        .unwrap();
    fixture.restart(1).await;
    let app = token_client(&fixture, token.expose_secret()).await;
    assert_eq!(
        app.admin()
            .topic_info("acme/app/orders")
            .await
            .unwrap()
            .id(),
        topic.id()
    );
    let root = fixture.client(1).await;
    root.admin().revoke_token(token.id()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        app.admin().topic_info("acme/app/orders").await.is_err(),
        "Revocation must affect an already authenticated connection"
    );
    app.close().await.unwrap();
    root.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_namespace_authorization_is_checked_on_each_resource() {
    let fixture = dynamic_fixture().await;
    let root = fixture.client(1).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let other = root.admin().create_tenant("other").await.unwrap();
    root.admin().create_namespace(other, "app").await.unwrap();
    root.admin()
        .create_logical_topic("acme/app/orders")
        .await
        .unwrap();
    root.admin()
        .create_logical_topic("other/app/orders")
        .await
        .unwrap();
    let principal = root
        .admin()
        .new_principal(tenant, "reader", [tidemq_sdk::Permission::Consume])
        .namespace(namespace)
        .build()
        .await
        .unwrap();
    let token = root
        .admin()
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    let app = token_client(&fixture, token.expose_secret()).await;
    assert!(app.admin().topic_info("acme/app/orders").await.is_ok());
    assert!(app.admin().topic_info("other/app/orders").await.is_err());
    root.admin()
        .set_tenant_enabled(tenant, false)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(app.admin().topic_info("acme/app/orders").await.is_err());
    app.close().await.unwrap();
    root.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumers_finish_paginated_domain_installation_and_retire_cleanly() {
    let fixture = dynamic_fixture().await;
    let client = fixture
        .client_with_timeouts(1, Duration::from_secs(60), Duration::from_secs(30))
        .await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client
        .admin()
        .create_logical_topic("acme/app/paged")
        .await
        .unwrap();
    for boundary in 1..=17_u64 {
        client
            .admin()
            .split_topic_at("acme/app/paged", boundary << 58)
            .await
            .unwrap();
    }
    assert_eq!(
        client
            .admin()
            .topic_info("acme/app/paged")
            .await
            .unwrap()
            .shard_count(),
        18
    );
    let queue = client
        .new_queue_consumer("acme/app/paged", "jobs")
        .build()
        .await
        .unwrap();
    let stream = client
        .new_stream_consumer("acme/app/paged", "state")
        .build()
        .await
        .unwrap();
    assert!(queue.try_receive().unwrap().is_none());
    assert!(stream.try_receive().unwrap().is_none());
    assert_eq!(queue.close().await.unwrap().members_left(), 1);
    assert_eq!(stream.close().await.unwrap().members_left(), 1);
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_members_share_messages_and_a_persistent_nack_redelivers_after_leave() {
    let fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client
        .admin()
        .create_logical_topic("acme/app/jobs")
        .await
        .unwrap();
    let first = client
        .new_queue_consumer("acme/app/jobs", "workers")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let second = client
        .new_queue_consumer("acme/app/jobs", "workers")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let topic = client.admin().topic("acme/app/jobs").await.unwrap();
    let producer = Producer::open(
        client.clone(),
        Id::from_slice(&topic.topic_id).unwrap(),
        0,
        Id::new_v7().unwrap(),
    )
    .await
    .unwrap();
    producer
        .produce(vec![
            public::Message {
                key: Bytes::from_static(b"a"),
                body: Bytes::from_static(b"first"),
            },
            public::Message {
                key: Bytes::from_static(b"b"),
                body: Bytes::from_static(b"second"),
            },
        ])
        .await
        .unwrap();
    let a = tokio::time::timeout(Duration::from_secs(15), first.receive())
        .await
        .unwrap()
        .unwrap();
    let b = tokio::time::timeout(Duration::from_secs(15), second.receive())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(a.message_id(), b.message_id());
    first.clone().ack(&a).await.unwrap();
    let retry_id = b.message_id();
    assert!(matches!(
        second.nack(&b, Duration::from_millis(100)).await.unwrap(),
        tidemq_sdk::AckReceipt::RedeliveryScheduled { .. }
    ));
    drop(a);
    drop(b);
    second.close().await.unwrap();
    let retry = tokio::time::timeout(Duration::from_secs(15), first.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry.message_id(), retry_id);
    first.ack(&retry).await.unwrap();
    drop(retry);
    assert_eq!(first.close().await.unwrap().members_left(), 1);
    producer
        .produce(vec![public::Message {
            key: Bytes::from_static(b"c"),
            body: Bytes::from_static(b"offline backlog"),
        }])
        .await
        .unwrap();
    let restored = client
        .new_queue_consumer("acme/app/jobs", "workers")
        .build()
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(15), restored.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.payload(), b"offline backlog");
    restored.ack(&message).await.unwrap();
    drop(message);
    restored.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_clones_share_an_application_prefix_and_reject_a_queue_model_reuse() {
    let fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client
        .admin()
        .create_logical_topic("acme/app/events")
        .await
        .unwrap();
    let stream = client
        .new_stream_consumer("acme/app/events", "state")
        .build()
        .await
        .unwrap();
    assert!(
        client
            .new_queue_consumer("acme/app/events", "state")
            .build()
            .await
            .is_err()
    );
    let clone = stream.clone();
    let topic = client.admin().topic("acme/app/events").await.unwrap();
    let producer = Producer::open(
        client.clone(),
        Id::from_slice(&topic.topic_id).unwrap(),
        0,
        Id::new_v7().unwrap(),
    )
    .await
    .unwrap();
    producer
        .produce(
            (0..3)
                .map(|index| public::Message {
                    key: Bytes::from_static(b"same"),
                    body: Bytes::from(format!("event-{index}")),
                })
                .collect(),
        )
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(15), stream.receive())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(15), clone.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.payload(), b"event-0");
    assert_eq!(second.payload(), b"event-1");
    clone.ack_cumulative(&second).await.unwrap();
    drop(first);
    drop(second);
    stream.close().await.unwrap();
    assert!(matches!(
        clone.receive().await,
        Err(tidemq_sdk::Error::Closed)
    ));
    let restored = client
        .new_stream_consumer("acme/app/events", "state")
        .build()
        .await
        .unwrap();
    let last = tokio::time::timeout(Duration::from_secs(15), restored.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(last.payload(), b"event-2");
    restored.ack_cumulative(&last).await.unwrap();
    drop(last);
    restored.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_a_client_close_waiter_does_not_cancel_member_cleanup() {
    let fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client
        .admin()
        .create_logical_topic("acme/app/events")
        .await
        .unwrap();
    let stream = client
        .new_stream_consumer("acme/app/events", "state")
        .build()
        .await
        .unwrap();
    let queue = client
        .new_queue_consumer("acme/app/events", "jobs")
        .build()
        .await
        .unwrap();
    {
        let mut closing = std::pin::pin!(client.close());
        std::future::poll_fn(|context| {
            let _ = std::future::Future::poll(closing.as_mut(), context);
            std::task::Poll::Ready(())
        })
        .await;
    }
    let report = client.close().await.unwrap();
    assert!(report.local_cleanup_complete());
    assert_eq!(report.members_left(), 2);
    assert_eq!(client.close().await.unwrap().members_left(), 2);
    assert!(matches!(
        stream.try_receive(),
        Err(tidemq_sdk::Error::Closed)
    ));
    assert!(matches!(
        queue.try_receive(),
        Err(tidemq_sdk::Error::Closed)
    ));
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_queue_member_rejoins_after_restart_and_fences_its_old_ack() {
    let mut fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client
        .admin()
        .create_logical_topic("acme/app/jobs")
        .await
        .unwrap();
    let queue = client
        .new_queue_consumer("acme/app/jobs", "workers")
        .prefetch_messages(4)
        .build()
        .await
        .unwrap();
    let topic = client.admin().topic("acme/app/jobs").await.unwrap();
    let producer = Producer::open(
        client.clone(),
        Id::from_slice(&topic.topic_id).unwrap(),
        0,
        Id::new_v7().unwrap(),
    )
    .await
    .unwrap();
    producer
        .produce(vec![public::Message {
            key: Bytes::from_static(b"job"),
            body: Bytes::from_static(b"recover me"),
        }])
        .await
        .unwrap();
    let old = tokio::time::timeout(Duration::from_secs(15), queue.receive())
        .await
        .unwrap()
        .unwrap();
    fixture.stop(1).await;
    tokio::time::sleep(Duration::from_secs(16)).await;
    fixture.restart(1).await;
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut failures = Vec::new();
    let recovered = loop {
        match tokio::time::timeout_at(deadline, queue.receive())
            .await
            .unwrap()
        {
            Ok(message) => break message,
            Err(error) => {
                let recoverable = recoverable_member_error(&error);
                let detail = format!("{:#}", anyhow::Error::new(error));
                assert!(recoverable, "Unexpected member recovery failure: {detail}");
                if failures.len() < 64 {
                    failures.push(detail);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    assert_eq!(old.message_id(), recovered.message_id());
    assert!(matches!(
        queue.ack(&old).await,
        Err(tidemq_sdk::Error::StaleDelivery)
    ));
    queue.ack(&recovered).await.unwrap();
    tokio::fs::write(fixture.directory.join("consumer-rejoin.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "original_message_id": old.message_id().to_string(), "recovered_message_id": recovered.message_id().to_string(),
        "old_ack_rejected": true, "recovery_errors": failures,
    })).unwrap()).await.unwrap();
    drop(old);
    drop(recovered);
    queue.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

fn recoverable_member_error(error: &tidemq_sdk::Error) -> bool {
    use tidemq_sdk::{Error, common::ErrorCode};
    error.outcome_unknown()
        || match error {
            Error::Background { source } => recoverable_member_error(source),
            Error::SessionExpired => true,
            Error::Remote {
                code:
                    ErrorCode::Unavailable
                    | ErrorCode::Fenced
                    | ErrorCode::StaleGeneration
                    | ErrorCode::StaleDelivery
                    | ErrorCode::SessionExpired,
                ..
            } => true,
            _ => false,
        }
}
