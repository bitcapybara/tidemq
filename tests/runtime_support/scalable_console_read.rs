use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use tidemq_protocol::console::{Decimal, Query, Reply};
use tidemq_sdk::{Permission, ResourceScope};

fn message_query(topic: Id, shard: Id, start: u64, observed_end: u64) -> Query {
    Query::Messages {
        topic,
        shard,
        start: Some(Decimal(start)),
        end: None,
        observed_end: Some(Decimal(observed_end)),
        limit: 32,
        scan_limit: 32,
        key_base64: None,
        properties_base64: BTreeMap::new(),
    }
}

async fn progress(
    client: &Client,
    topic: Id,
    subscription: Id,
) -> tidemq_protocol::console::Progress {
    let Reply::Progress(page) = client
        .console_read(Query::Progress {
            topic,
            subscription,
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected a subscription progress page");
    };
    assert_eq!(page.items.len(), 1);
    assert!(!page.observation.partial);
    let row = page.items.into_iter().next().unwrap();
    assert!(row.component_id.is_none());
    assert!(row.observed_at_ms.is_some());
    assert!(row.revision.is_some());
    assert_eq!(row.source, "dataLinearizableGroupCursor");
    row
}

fn progress_state(row: &tidemq_protocol::console::Progress) -> serde_json::Value {
    let mut state = serde_json::to_value(row).unwrap();
    let fields = state.as_object_mut().unwrap();
    fields.remove("observedAtMs");
    fields.remove("revision");
    state
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_reads_scope_hot_messages_members_and_ack_holes_without_consuming_or_renewing() {
    let fixture = dynamic_fixture().await;
    let root = fixture.client(1).await;
    let admin = root.admin();
    let tenant = admin.create_tenant("acme").await.unwrap();
    let namespace = admin.create_namespace(tenant, "app").await.unwrap();
    let topic_name = "acme/app/visible";
    admin.create_topic(topic_name).await.unwrap();
    let private: Id = admin
        .create_topic("acme/app/private")
        .await
        .unwrap()
        .id()
        .into();
    let private_consumer = root
        .new_stream_consumer("acme/app/private", "private-reader")
        .build()
        .await
        .unwrap();
    private_consumer.close().await.unwrap();
    let topic: Id = admin
        .topic_info(topic_name)
        .await
        .unwrap()
        .id()
        .to_string()
        .parse()
        .unwrap();
    let scope = ResourceScope::Topic {
        tenant,
        namespace,
        topic: topic.into(),
    };
    let principal = admin
        .new_principal(tenant, "console-reader", [])
        .build()
        .await
        .unwrap();
    for permission in [Permission::Produce, Permission::Consume] {
        admin
            .grant_permission(principal, scope, permission)
            .await
            .unwrap();
    }
    let token = admin
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    let reader = token_client(&fixture, token.expose_secret()).await;
    let Reply::Topics(page) = reader
        .console_read(Query::Topics {
            tenant: None,
            namespace: None,
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected an authorized topic directory");
    };
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, topic);
    let Reply::Topic(detail) = reader.console_read(Query::Topic { topic }).await.unwrap() else {
        panic!("Expected real topic configuration and shards");
    };
    assert_eq!(detail.shard_details.len(), 1);
    assert!(!detail.configuration.is_empty());
    let shard = detail.shard_details[0].id;
    assert!(detail.shard_details[0].group.is_none());
    assert!(matches!(
        reader.console_read(message_query(topic, shard, 0, 0)).await,
        Err(tidemq_sdk::Error::Remote {
            code: tidemq_protocol::common::ErrorCode::Forbidden,
            ..
        })
    ));
    assert!(matches!(
        reader
            .console_read(Query::Cluster {
                after: None,
                limit: 32
            })
            .await,
        Err(tidemq_sdk::Error::Remote {
            code: tidemq_protocol::common::ErrorCode::Forbidden,
            ..
        })
    ));
    assert!(matches!(
        reader
            .console_read(Query::Subscriptions {
                topic: Some(private),
                after: None,
                limit: 32,
            })
            .await,
        Err(tidemq_sdk::Error::Remote {
            code: tidemq_protocol::common::ErrorCode::Forbidden,
            ..
        })
    ));
    let dormant = reader
        .new_stream_consumer(topic_name, "dormant")
        .build()
        .await
        .unwrap();
    dormant.close().await.unwrap();
    let queue = reader
        .new_queue_consumer(topic_name, "work")
        .prefetch_messages(2)
        .build()
        .await
        .unwrap();
    let producer = reader.new_producer(topic_name).build().await.unwrap();
    let mut identities = Vec::new();
    for (index, key) in [None, Some(""), Some("k")].into_iter().enumerate() {
        let message = tidemq_sdk::Message::new(format!("body-{index}"))
            .property("binary", Bytes::from_static(&[0, 255]))
            .property("source", "hot");
        let message = key.map_or_else(|| message.clone(), |key| message.clone().key(key));
        identities.push(producer.send(message).await.unwrap().message_id());
    }
    admin
        .grant_permission(principal, scope, Permission::InspectMessageContent)
        .await
        .unwrap();
    let mut query = message_query(topic, shard, 0, 3);
    if let Query::Messages {
        scan_limit,
        key_base64,
        properties_base64,
        ..
    } = &mut query
    {
        *scan_limit = 1;
        *key_base64 = Some(String::new());
        properties_base64.insert("binary".into(), STANDARD.encode([0, 255]));
        properties_base64.insert("source".into(), STANDARD.encode("hot"));
    }
    let Reply::Messages(first) = reader.console_read(query.clone()).await.unwrap() else {
        panic!("Expected a bounded message page");
    };
    assert!(first.items.is_empty());
    assert_eq!(first.scanned_start, Decimal(0));
    assert_eq!(first.scanned_end, Decimal(1));
    assert_eq!(first.next_offset, Some(Decimal(1)));
    assert_eq!(first.stop_reason, "budget");
    if let Query::Messages { start, .. } = &mut query {
        *start = first.next_offset;
    }
    let Reply::Messages(second) = reader.console_read(query).await.unwrap() else {
        panic!("Expected a continued message page");
    };
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].offset, Decimal(1));
    assert_eq!(second.items[0].key_base64.as_deref(), Some(""));
    assert_eq!(second.items[0].id, identities[1].to_string());
    assert_eq!(second.items[0].payload_base64, STANDARD.encode("body-1"));
    assert_eq!(
        second.items[0].properties_base64.get("binary"),
        Some(&STANDARD.encode([0, 255]))
    );
    assert_eq!(second.items[0].tier, "hot");
    producer
        .send(tidemq_sdk::Message::new("after-snapshot"))
        .await
        .unwrap();
    let Reply::Messages(snapshot) = reader
        .console_read(message_query(topic, shard, 0, 3))
        .await
        .unwrap()
    else {
        panic!("Expected a fixed observed end");
    };
    assert_eq!(snapshot.observed_end, Decimal(3));
    assert_eq!(snapshot.scanned_end, Decimal(3));
    assert_eq!(snapshot.items.len(), 3);
    assert!(snapshot.items[0].key_base64.is_none());
    assert_eq!(snapshot.items[2].id, identities[2].to_string());
    let Reply::Subscriptions(subscriptions) = reader
        .console_read(Query::Subscriptions {
            topic: Some(topic),
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected durable subscriptions");
    };
    assert_eq!(subscriptions.items.len(), 2);
    assert_eq!(
        subscriptions
            .items
            .iter()
            .find(|row| row.name == "dormant")
            .unwrap()
            .members,
        Decimal(0)
    );
    let subscription = subscriptions
        .items
        .iter()
        .find(|row| row.name == "work")
        .unwrap()
        .id;
    let first = queue.receive().await.unwrap();
    let second = queue.receive().await.unwrap();
    assert_eq!(first.message_id(), identities[0]);
    assert_eq!(second.message_id(), identities[1]);
    queue.ack(&second).await.unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    let before = loop {
        let row = progress(&reader, topic, subscription).await;
        if row.acked == Some(Decimal(1))
            && row.in_flight == Some(Decimal(2))
            && row.window_end.is_some_and(|end| end.0 >= 3)
        {
            break row;
        }
        assert!(
            Instant::now() < until,
            "Queue did not stabilize its persisted window"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(before.frontier, Some(Decimal(0)));
    assert_eq!(before.unconfirmed, Some(Decimal(3)));
    assert_eq!(before.tail, Some(Decimal(4 - before.window_end.unwrap().0)));
    for _ in 0..3 {
        let observed = progress(&reader, topic, subscription).await;
        assert_eq!(progress_state(&observed), progress_state(&before));
        assert!(observed.observed_at_ms >= before.observed_at_ms);
        assert!(observed.revision >= before.revision);
    }
    for filter in [Some(topic), None] {
        let Reply::Subscriptions(summary) = reader
            .console_read(Query::Subscriptions {
                topic: filter,
                after: None,
                limit: 32,
            })
            .await
            .unwrap()
        else {
            panic!("Expected authorized subscription summaries");
        };
        assert_eq!(summary.items.len(), 2);
        assert!(summary.items.iter().all(|row| row.topic_id == topic));
        assert!(summary.observation.revision.is_some());
        assert_eq!(
            summary.observation.source,
            "metadataDirectoryWithPerShardGroupCursorSamples"
        );
        let work = summary
            .items
            .iter()
            .find(|row| row.id == subscription)
            .unwrap();
        assert_eq!(work.unconfirmed, Some(Decimal(3)));
        let observation = &work.progress_observation;
        assert_eq!(observation.source, "perShardLinearizableGroupCursorSamples");
        assert_eq!(observation.sampled_shards, Some(Decimal(1)));
        assert!(!observation.partial);
        assert!(observation.reason.is_none());
        assert!(observation.observed_from_ms.is_some());
        assert!(observation.observed_from_ms <= observation.observed_until_ms);
        assert!(observation.observed_until_ms >= before.observed_at_ms);
    }
    let after_summary = progress(&reader, topic, subscription).await;
    assert_eq!(progress_state(&after_summary), progress_state(&before));
    queue.ack(&first).await.unwrap();
    let third = queue.receive().await.unwrap();
    assert_eq!(third.message_id(), identities[2]);
    queue.ack(&third).await.unwrap();
    let last = queue.receive().await.unwrap();
    assert_eq!(last.payload(), b"after-snapshot");
    queue.ack(&last).await.unwrap();
    let members = Query::Members {
        topic,
        subscription,
        after: None,
        limit: 32,
    };
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let Reply::Members(first) = reader.console_read(members.clone()).await.unwrap() else {
            panic!("Expected member leases");
        };
        let Reply::Members(second) = reader.console_read(members.clone()).await.unwrap() else {
            panic!("Expected member leases");
        };
        assert_eq!(first.items.len(), 1);
        assert_eq!(second.items.len(), 1);
        assert!(first.items[0].lease_valid);
        assert!(second.items[0].last_heartbeat_ms.is_none());
        if first.observation.revision == second.observation.revision {
            assert_eq!(first.items[0].expires_at_ms, second.items[0].expires_at_ms);
            break;
        }
        assert!(
            Instant::now() < until,
            "Read-only member queries did not preserve metadata state between heartbeats"
        );
    }
    queue.close().await.unwrap();
    producer.close().await.unwrap();
    let Reply::Members(retired) = reader.console_read(members).await.unwrap() else {
        panic!("Expected the retired member directory");
    };
    assert!(retired.items.is_empty());
    let Reply::Subscriptions(durable) = reader
        .console_read(Query::Subscriptions {
            topic: Some(topic),
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected subscriptions to survive graceful member retirement");
    };
    assert_eq!(durable.items.len(), 2);
    assert!(durable.items.iter().all(|row| row.members == Decimal(0)));
    let Reply::Producers(page) = reader
        .console_read(Query::Producers {
            topic,
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected producer leases");
    };
    assert_eq!(page.items.len(), 1);
    assert!(!page.items[0].lease_valid);
    assert!(page.items[0].last_heartbeat_ms.is_none());
    let Reply::Cluster(nodes) = root
        .console_read(Query::Cluster {
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected registered nodes with real runtime probes");
    };
    let node = nodes
        .items
        .iter()
        .find(|node| node.id == Decimal(1))
        .unwrap();
    assert_eq!(node.live, Some(true));
    assert_eq!(node.version.as_deref(), Some(env!("CARGO_PKG_VERSION")));
    assert!(node.instance.is_some());
    let metadata = node
        .groups
        .iter()
        .find(|group| group.role == "metadata")
        .unwrap();
    assert_eq!(metadata.leader, Some(Decimal(1)));
    assert_eq!(metadata.voters, vec![Decimal(1)]);
    assert_eq!(metadata.quorum_confirmed, Some(true));
    reader.close().await.unwrap();
    root.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_shard_pages_retain_sealed_history_after_layout_changes() {
    let fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let admin = client.admin();
    let tenant = admin.create_tenant("acme").await.unwrap();
    admin.create_namespace(tenant, "app").await.unwrap();
    let name = "acme/app/history";
    let topic: Id = admin.create_topic(name).await.unwrap().id().into();
    let producer = client.new_producer(name).build().await.unwrap();
    let identity = producer
        .send(tidemq_sdk::Message::new("sealed-message"))
        .await
        .unwrap()
        .message_id();
    producer.close().await.unwrap();
    admin.split_topic_at(name, 1 << 63).await.unwrap();
    admin.merge_topic_at(name, 1 << 63).await.unwrap();
    let Reply::Topic(detail) = client.console_read(Query::Topic { topic }).await.unwrap() else {
        panic!("Expected the current layout and complete small shard history");
    };
    assert_eq!(detail.topic.shards, Decimal(1));
    assert_eq!(detail.total_shards, Decimal(4));
    assert_eq!(detail.shard_details.len(), 4);
    assert_eq!(
        detail
            .shard_details
            .iter()
            .filter(|shard| shard.sealed)
            .count(),
        3
    );
    assert!(detail.next_shard_cursor.is_none());
    let mut after = None;
    let mut observed = BTreeSet::new();
    loop {
        let Reply::Shards(page) = client
            .console_read(Query::Shards {
                topic,
                after,
                limit: 1,
            })
            .await
            .unwrap()
        else {
            panic!("Expected one bounded historical shard page");
        };
        assert_eq!(page.items.len(), 1);
        assert!(observed.insert(page.items[0].id));
        assert!(page.items[0].group.is_some());
        let Some(next) = page.next_cursor else {
            break;
        };
        after = Some(next.parse().unwrap());
    }
    assert_eq!(
        observed,
        detail.shard_details.iter().map(|shard| shard.id).collect()
    );
    let original = detail
        .shard_details
        .iter()
        .find(|shard| identity.to_string().contains(&shard.id.to_string()))
        .unwrap();
    let Reply::Messages(page) = client
        .console_read(message_query(topic, original.id, 0, 1))
        .await
        .unwrap()
    else {
        panic!("Expected message inspection to use the original sealed shard");
    };
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, identity.to_string());
    assert_eq!(
        page.items[0].payload_base64,
        STANDARD.encode("sealed-message")
    );
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_metadata_stays_visible_with_unknown_shard_progress_when_storage_is_unavailable() {
    let mut fixture = Fixture::new(&[(1, Role::Combined), (2, Role::Storage)], &[1], &[2]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let admin = client.admin();
    let tenant = admin.create_tenant("acme").await.unwrap();
    admin.create_namespace(tenant, "app").await.unwrap();
    let name = "acme/app/partial";
    let topic: Id = admin.create_topic(name).await.unwrap().id().into();
    let queue = client
        .new_queue_consumer(name, "durable")
        .build()
        .await
        .unwrap();
    queue.close().await.unwrap();
    let producer = client.new_producer(name).build().await.unwrap();
    producer
        .send(tidemq_sdk::Message::new("retained"))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let Reply::Subscriptions(subscriptions) = client
        .console_read(Query::Subscriptions {
            topic: Some(topic),
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected the persisted subscription before stopping storage");
    };
    let subscription = subscriptions.items.first().unwrap().id;
    let summary = subscriptions.items.first().unwrap();
    assert_eq!(summary.unconfirmed, Some(Decimal(1)));
    assert!(!summary.progress_observation.partial);
    assert_eq!(
        summary.progress_observation.sampled_shards,
        Some(Decimal(1))
    );
    assert!(summary.progress_observation.observed_from_ms.is_some());
    let Reply::Topic(healthy) = client.console_read(Query::Topic { topic }).await.unwrap() else {
        panic!("Expected a healthy bounded data observation");
    };
    assert!(!healthy.observation.partial);
    assert_eq!(healthy.shard_details[0].committed_end, Some(Decimal(1)));
    for index in 0..3 {
        let queue = client
            .new_queue_consumer(name, format!("unavailable-{index}"))
            .build()
            .await
            .unwrap();
        queue.close().await.unwrap();
    }
    fixture.stop(2).await;
    let began = Instant::now();
    let Reply::Topic(partial) = client.console_read(Query::Topic { topic }).await.unwrap() else {
        panic!("Expected available metadata despite an unavailable data source");
    };
    assert!(began.elapsed() < Duration::from_secs(3));
    assert!(partial.observation.partial);
    assert_eq!(partial.topic.id, topic);
    assert!(partial.shard_details[0].retained_start.is_none());
    assert!(partial.shard_details[0].committed_end.is_none());
    assert_eq!(
        partial.shard_details[0].reason.as_deref(),
        Some("dataObservationUnavailable")
    );
    let Reply::Progress(page) = client
        .console_read(Query::Progress {
            topic,
            subscription,
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected unknown progress rather than fabricated empty backlog");
    };
    assert!(page.observation.partial);
    let row = page.items.first().unwrap();
    assert!(row.frontier.is_none());
    assert!(row.unconfirmed.is_none());
    assert!(row.observed_at_ms.is_none());
    assert!(row.revision.is_none());
    assert_eq!(row.source, "dataObservationUnavailable");
    let began = Instant::now();
    let Reply::Subscriptions(unknown) = client
        .console_read(Query::Subscriptions {
            topic: Some(topic),
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected bounded unknown summaries despite multiple unavailable cursors");
    };
    assert!(began.elapsed() < Duration::from_secs(3));
    assert_eq!(unknown.items.len(), 4);
    assert!(unknown.observation.partial);
    assert!(unknown.items.iter().all(|row| {
        row.unconfirmed.is_none()
            && row.progress_observation.partial
            && row.progress_observation.reason.is_some()
            && row.progress_observation.observed_from_ms.is_none()
            && row.progress_observation.observed_until_ms.is_none()
    }));
    client.close().await.unwrap();
    fixture.close().await;
}
