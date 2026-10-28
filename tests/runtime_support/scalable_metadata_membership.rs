use super::*;

#[cfg(feature = "test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_metadata_waiter_and_controller_restart_resume_one_original_membership_plan() {
    let mut fixture = fixture_without_candidate(
        &[
            (1, Role::Metadata),
            (2, Role::Metadata),
            (3, Role::Gateway),
            (5, Role::Broker),
            (4, Role::Storage),
        ],
        &[1],
        &[4],
        2,
    )
    .await;
    let client = fixture
        .client_with_timeouts(3, Duration::from_secs(30), Duration::from_secs(5))
        .await;
    client
        .admin()
        .register_node(registry::registration(&fixture, 2).await)
        .await
        .unwrap();
    fixture.running.insert(
        2,
        RunningNode::start(fixture.configs.get(&2).unwrap().clone())
            .await
            .unwrap(),
    );
    fixture.wait_ready().await;
    let cluster = fixture.configs.get(&1).unwrap().cluster_id.parse().unwrap();
    let hook =
        tidemq::runtime::test_hooks::InstalledPause::metadata_membership(cluster, 2).unwrap();
    let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(30)).unwrap();
    let request_context = context.clone();
    let request_client = client.clone();
    let request = tokio::spawn(async move {
        request_context
            .scope(request_client.admin().replace_metadata_replica(1, 2))
            .await
    });
    hook.wait(deadline()).await.unwrap();
    context.cancellation_token().cancel();
    let error = request.await.unwrap().unwrap_err();
    assert!(error.outcome_unknown());
    let pending = client.admin().pending_operations().unwrap();
    assert_eq!(pending.len(), 1);
    fixture.stop(1).await;
    drop(hook);
    fixture.restart(1).await;
    let tidemq_sdk::AdminReceipt::MetadataMembershipChanged(result) =
        pending[0].retry().await.unwrap()
    else {
        panic!("Expected the original metadata replacement receipt");
    };
    assert_eq!(result.previous_voters, [1]);
    assert_eq!(result.voters, [2]);
    assert!(client.admin().pending_operations().unwrap().is_empty());
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf3_metadata_replacement_keeps_three_voters_and_checkpoints_every_target_before_source_drain()
 {
    let mut fixture = fixture_without_candidate(
        &[
            (1, Role::Metadata),
            (2, Role::Metadata),
            (3, Role::Metadata),
            (4, Role::Metadata),
            (5, Role::Gateway),
            (9, Role::Broker),
            (6, Role::Storage),
            (7, Role::Storage),
            (8, Role::Storage),
        ],
        &[1, 2, 3],
        &[6, 7, 8],
        4,
    )
    .await;
    let client = fixture
        .client_with_timeouts(5, Duration::from_secs(30), Duration::from_secs(5))
        .await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/rf3-control-move";
    client.admin().create_topic(topic).await.unwrap();
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    let original = producer
        .send("before-replacement")
        .await
        .unwrap()
        .message_id();
    let held = queue.receive().await.unwrap();
    assert_eq!(held.message_id(), original);
    client
        .admin()
        .register_node(registry::registration(&fixture, 4).await)
        .await
        .unwrap();
    fixture.running.insert(
        4,
        RunningNode::start(fixture.configs.get(&4).unwrap().clone())
            .await
            .unwrap(),
    );
    fixture.wait_ready().await;
    let result = client.admin().replace_metadata_replica(1, 4).await.unwrap();
    assert_eq!(result.previous_voters, [1, 2, 3]);
    assert_eq!(result.voters, [2, 3, 4]);
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        let mut complete = true;
        for node in [2, 3, 4] {
            let response = http(
                fixture.running.get(&node).unwrap().management_address,
                "/diagnostics",
            )
            .await;
            let value: serde_json::Value =
                serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap();
            complete &= value
                .pointer("/metadata_storage/storage/snapshot_log/index")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|index| index >= result.membership_index);
            complete &=
                value.pointer("/registry/metadata_voters") == Some(&serde_json::json!([2, 3, 4]));
        }
        if complete {
            break;
        }
        assert!(
            Instant::now() < until,
            "Replacement returned before every target published its durable checkpoint and current directory"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client.admin().begin_node_drain(1).await.unwrap();
    assert!(
        client
            .admin()
            .node_drain_status(1)
            .await
            .unwrap()
            .durable_state_drained()
    );
    client.admin().retire_node(1).await.unwrap();
    fixture.stop(1).await;
    queue.ack(&held).await.unwrap();
    let next = logical_send(&producer, b"after-replacement").await;
    let received = tokio::time::timeout(Duration::from_secs(15), queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.message_id(), next);
    queue.ack(&received).await.unwrap();
    producer.close().await.unwrap();
    queue.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

pub(super) async fn fixture_without_candidate(
    nodes: &[(u64, Role)],
    voters: &[u64],
    storage: &[u64],
    added: u64,
) -> Fixture {
    let mut fixture = Fixture::new(nodes, voters, storage).await;
    for (id, config) in &mut fixture.configs {
        if *id != added {
            config.runtime.peers.retain(|peer| peer.node_id != added);
        }
    }
    fixture
        .configs
        .get_mut(&added)
        .unwrap()
        .runtime
        .bootstrap_admin_token_file = None;
    for id in nodes.iter().map(|(id, _)| *id).filter(|id| *id != added) {
        fixture.running.insert(
            id,
            RunningNode::start(fixture.configs.get(&id).unwrap().clone())
                .await
                .unwrap(),
        );
    }
    fixture.wait_ready().await;
    fixture
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_replacement_checkpoints_the_new_voter_and_old_seed_restart_keeps_original_sdk_working()
 {
    maintenance_test_logging();
    let mut fixture = fixture_without_candidate(
        &[
            (1, Role::Metadata),
            (2, Role::Metadata),
            (3, Role::Gateway),
            (5, Role::Broker),
            (4, Role::Storage),
        ],
        &[1],
        &[4],
        2,
    )
    .await;
    let client = fixture
        .client_with_timeouts(3, Duration::from_secs(30), Duration::from_secs(5))
        .await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "acme/app/control-plane-move";
    client.admin().create_topic(topic).await.unwrap();
    let stream = client
        .new_stream_consumer(topic, "events")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    let original = producer
        .send(tidemq_sdk::Message::new("before-move").key("key"))
        .await
        .unwrap()
        .message_id();
    let held = stream.receive().await.unwrap();
    assert_eq!(held.message_id(), original);
    let registration = registry::registration(&fixture, 2).await;
    client.admin().register_node(registration).await.unwrap();
    fixture.running.insert(
        2,
        RunningNode::start(fixture.configs.get(&2).unwrap().clone())
            .await
            .unwrap(),
    );
    fixture.wait_ready().await;
    #[cfg(feature = "test-hooks")]
    let routing = {
        let cluster = fixture.configs.get(&1).unwrap().cluster_id.parse().unwrap();
        let hook = tidemq::runtime::test_hooks::InstalledPause::metadata_route(cluster, 1).unwrap();
        let query_client = client.clone();
        let query = tokio::spawn(async move { query_client.admin().topic_info(topic).await });
        hook.wait(deadline()).await.unwrap();
        (hook, query)
    };
    let moved = client.admin().replace_metadata_replica(1, 2).await.unwrap();
    assert_eq!(moved.previous_voters, [1]);
    assert_eq!(moved.voters, [2]);
    assert!(moved.membership_index > 0);
    #[cfg(feature = "test-hooks")]
    {
        assert!(
            !routing.1.is_finished(),
            "The old metadata route must remain in flight"
        );
        routing.0.resume();
        assert_eq!(routing.1.await.unwrap().unwrap().name(), topic);
    }
    client.admin().begin_node_drain(1).await.unwrap();
    assert!(
        client
            .admin()
            .node_drain_status(1)
            .await
            .unwrap()
            .durable_state_drained()
    );
    client.admin().retire_node(1).await.unwrap();
    fixture.stop(1).await;
    let until = deadline();
    let mut acknowledged = stream.ack_cumulative(&held).await;
    while let Err(error) = acknowledged {
        assert!(
            Instant::now() < until,
            "The original ACK did not recover: {error:?}"
        );
        assert!(
            error.outcome_unknown() || error.kind() == tidemq_sdk::ErrorKind::Unavailable,
            "Metadata replacement invalidated an admitted delivery: {error:?}"
        );
        let pending = stream.pending_acks().await.unwrap();
        acknowledged = if let Some(original) = pending.first() {
            assert_eq!(pending.len(), 1);
            tidemq_sdk::RequestContext::with_deadline(until)
                .scope(original.retry())
                .await
        } else {
            assert!(
                !error.outcome_unknown(),
                "An unknown ACK needs its original handle"
            );
            tidemq_sdk::RequestContext::with_deadline(until)
                .scope(stream.ack_cumulative(&held))
                .await
        };
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(acknowledged.unwrap(), tidemq_sdk::AckReceipt::Acknowledged);
    fixture.stop(4).await;
    fixture.restart(4).await;
    let next = producer
        .send(tidemq_sdk::Message::new("after-move").key("key"))
        .await
        .unwrap()
        .message_id();
    let message = tokio::time::timeout(Duration::from_secs(15), stream.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.message_id(), next);
    stream.ack_cumulative(&message).await.unwrap();
    producer.close().await.unwrap();
    stream.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
