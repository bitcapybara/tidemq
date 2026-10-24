use super::*;

async fn resources(
    client: &Client,
) -> (
    tidemq_sdk::Producer,
    tidemq_sdk::QueueConsumer,
    tidemq_sdk::ReceivedMessage,
) {
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client
        .admin()
        .create_topic("acme/app/replicas")
        .await
        .unwrap();
    let consumer = client
        .new_queue_consumer("acme/app/replicas", "jobs")
        .build()
        .await
        .unwrap();
    let producer = client
        .new_producer("acme/app/replicas")
        .build()
        .await
        .unwrap();
    let id = producer
        .send("before-replacement")
        .await
        .unwrap()
        .message_id();
    let held = consumer.receive().await.unwrap();
    assert_eq!(held.message_id(), id);
    (producer, consumer, held)
}

async fn verify_original_handles(
    client: &Client,
    producer: tidemq_sdk::Producer,
    consumer: tidemq_sdk::QueueConsumer,
    held: tidemq_sdk::ReceivedMessage,
) {
    consumer.ack(&held).await.unwrap();
    let id = producer
        .send("after-replacement")
        .await
        .unwrap()
        .message_id();
    let message = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(10))
        .unwrap()
        .scope(consumer.receive())
        .await
        .unwrap();
    assert_eq!(message.message_id(), id);
    consumer.ack(&message).await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf3_operator_replaces_a_storage_voter_and_retires_the_source_with_original_sdk_handles() {
    let nodes = [
        (1, Role::Combined),
        (2, Role::Storage),
        (3, Role::Storage),
        (4, Role::Storage),
        (5, Role::Storage),
        (6, Role::Metadata),
        (8, Role::Metadata),
    ];
    let mut fixture = Fixture::new(&nodes, &[1, 6, 8], &[2, 3, 4]).await;
    for (node, config) in &mut fixture.configs {
        if *node == 5 {
            config.runtime.groups.first_mut().unwrap().standby = vec![5];
        } else {
            config.runtime.peers.retain(|peer| peer.node_id != 5);
        }
    }
    for (node, config) in &fixture.configs {
        if *node != 5 {
            fixture
                .running
                .insert(*node, RunningNode::start(config.clone()).await.unwrap());
        }
    }
    fixture.wait_ready().await;
    let client = fixture.client(1).await;
    let (producer, consumer, held) = resources(&client).await;
    let status = http(
        fixture.running.get(&2).unwrap().management_address,
        "/diagnostics",
    )
    .await;
    let status: serde_json::Value =
        serde_json::from_str(status.split_once("\r\n\r\n").unwrap().1).unwrap();
    let removed = status["data_groups"][0]["leader"].as_u64().unwrap();
    assert!([2, 3, 4].contains(&removed));
    let zone = fixture
        .configs
        .get(&removed)
        .unwrap()
        .runtime
        .peer(removed)
        .unwrap()
        .zone
        .clone();
    let target = fixture.configs.get_mut(&5).unwrap();
    target
        .runtime
        .peers
        .iter_mut()
        .find(|peer| peer.node_id == 5)
        .unwrap()
        .zone = zone;
    client
        .admin()
        .register_node(registry::registration(&fixture, 5).await)
        .await
        .unwrap();
    fixture.running.insert(
        5,
        RunningNode::start(fixture.configs.get(&5).unwrap().clone())
            .await
            .unwrap(),
    );
    fixture.wait_ready().await;
    let result = client.admin().replace_data_replica(2, removed, 5).await;
    if result.is_err() {
        evidence(&fixture).await;
    }
    let result = result.unwrap();
    let expected: Vec<_> = [2, 3, 4, 5]
        .into_iter()
        .filter(|node| *node != removed)
        .collect();
    assert_eq!(result.voters, expected);
    assert_eq!(result.generation, 2);
    assert_eq!(
        client
            .admin()
            .replace_data_replica(2, removed, 5)
            .await
            .unwrap(),
        result
    );
    client.admin().begin_node_drain(removed).await.unwrap();
    assert!(
        client
            .admin()
            .node_drain_status(removed)
            .await
            .unwrap()
            .durable_state_drained()
    );
    assert!(client.admin().retire_node(removed).await.unwrap().retired);
    fixture.stop(removed).await;
    verify_original_handles(&client, producer, consumer, held).await;
    fixture.close().await;
}

#[cfg(feature = "test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_data_replacement_survives_controller_restart_before_native_prepare_and_keeps_one_original_plan()
 {
    let mut fixture = Fixture::new(
        &[
            (1, Role::Combined),
            (2, Role::Storage),
            (3, Role::Storage),
            (4, Role::Storage),
        ],
        &[1],
        &[2],
    )
    .await;
    for config in fixture.configs.values_mut() {
        config.runtime.groups.first_mut().unwrap().standby = vec![3, 4];
    }
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let (producer, consumer, held) = resources(&client).await;
    let cluster = fixture.configs.get(&1).unwrap().cluster_id.parse().unwrap();
    let hook = tidemq::runtime::test_hooks::InstalledPause::data_membership(cluster, 2).unwrap();
    let caller = client.clone();
    let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(30)).unwrap();
    let cancellation = context.cancellation_token();
    let waiting = tokio::spawn(async move {
        context
            .scope(caller.admin().replace_data_replica(2, 2, 3))
            .await
    });
    hook.wait(deadline()).await.unwrap();
    cancellation.cancel();
    assert!(waiting.await.unwrap().unwrap_err().outcome_unknown());
    let pending = client.admin().pending_operations().unwrap();
    assert_eq!(pending.len(), 1);
    assert!(client.admin().begin_node_drain(3).await.is_err());
    assert!(client.admin().replace_data_replica(2, 2, 4).await.is_err());
    fixture.stop(1).await;
    drop(hook);
    fixture.restart(1).await;
    let result = pending[0].retry().await;
    if result.is_err() {
        evidence(&fixture).await;
    }
    let result = result.unwrap();
    let tidemq_sdk::AdminReceipt::DataGroupReconfigured(result) = result else {
        panic!("Expected original data replica receipt");
    };
    assert_eq!(result.voters, [3]);
    assert_eq!(result.generation, 2);
    assert!(client.admin().pending_operations().unwrap().is_empty());
    client.admin().begin_node_drain(2).await.unwrap();
    assert!(
        client
            .admin()
            .node_drain_status(2)
            .await
            .unwrap()
            .durable_state_drained()
    );
    client.admin().retire_node(2).await.unwrap();
    fixture.stop(2).await;
    verify_original_handles(&client, producer, consumer, held).await;
    fixture.close().await;
}

async fn evidence(fixture: &Fixture) {
    let mut nodes = BTreeMap::new();
    for (node, running) in &fixture.running {
        nodes.insert(
            *node,
            http(running.management_address, "/diagnostics").await,
        );
    }
    tokio::fs::write(
        fixture.directory.join("data-replacement-failure.json"),
        serde_json::to_vec_pretty(&nodes).unwrap(),
    )
    .await
    .unwrap();
}
