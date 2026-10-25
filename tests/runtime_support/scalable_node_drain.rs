use super::*;
use tidemq::metadata::PartitionKey;
use tidemq_sdk::Message;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_broker_receives_existing_topic_ownership_and_drain_bypasses_balance_cooldown() {
    let mut fixture = metadata_membership::fixture_without_candidate(
        &[(1, Role::Combined), (2, Role::Broker)],
        &[1],
        &[1],
        2,
    )
    .await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let mut topics = Vec::new();
    let mut producers = Vec::new();
    let mut streams = Vec::new();
    let mut held = Vec::new();
    for index in 0..2 {
        let name = format!("acme/app/balance-{index}");
        let topic = client.admin().create_topic(&name).await.unwrap();
        let stream = client
            .new_stream_consumer(&name, "events")
            .build()
            .await
            .unwrap();
        let producer = client.new_producer(&name).build().await.unwrap();
        let id = producer
            .send(Message::new("before").key("one"))
            .await
            .unwrap()
            .message_id();
        let message = stream.receive().await.unwrap();
        assert_eq!(message.message_id(), id);
        topics.push(PartitionKey {
            topic_id: topic.id().to_string().parse().unwrap(),
            partition: 0,
        });
        producers.push(producer);
        streams.push(stream);
        held.push(message);
    }
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
    let until = deadline();
    loop {
        let mut moved = 0;
        let mut observed = true;
        for partition in &topics {
            let result = fixture
                .running
                .get(&1)
                .unwrap()
                .route(*partition, until)
                .await;
            let route = match result {
                Ok(route) => route,
                Err(error) => {
                    assert!(
                        error.failure().retryable,
                        "Ownership observation failed: {error:#}"
                    );
                    observed = false;
                    break;
                }
            };
            moved += usize::from(route.owner_node == 2);
        }
        if observed && moved == 1 {
            break;
        }
        assert!(
            Instant::now() < until,
            "The newly registered broker did not receive existing ownership"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for ((producer, stream), held) in producers.iter().zip(&streams).zip(&held) {
        let id = producer
            .send(Message::new("after").key("one"))
            .await
            .unwrap()
            .message_id();
        stream.ack_cumulative(held).await.unwrap();
        let message = stream.receive().await.unwrap();
        assert_eq!(message.message_id(), id);
        assert_eq!(message.payload(), b"after");
        stream.ack_cumulative(&message).await.unwrap();
    }
    client.admin().begin_node_drain(2).await.unwrap();
    let until = deadline();
    loop {
        if client
            .admin()
            .node_drain_status(2)
            .await
            .unwrap()
            .durable_state_drained()
        {
            break;
        }
        assert!(
            Instant::now() < until,
            "Planned drain was delayed by balance cooldown"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client.admin().retire_node(2).await.unwrap();
    for stream in streams {
        stream.close().await.unwrap();
    }
    for producer in producers {
        producer.close().await.unwrap();
    }
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_drain_proves_local_cleanup_and_retirement_rejects_the_original_identity() {
    let mut fixture = Fixture::new(&[(1, Role::Combined), (2, Role::Gateway)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let connected = fixture.client(2).await;
    let begin = client.admin().begin_node_drain(2).await.unwrap();
    assert!(begin.draining && !begin.schedulable && !begin.retired);
    let status = client.admin().node_drain_status(2).await.unwrap();
    assert!(status.durable_state_drained() && !status.retired);
    assert!(client.admin().set_node_schedulable(2, true).await.is_err());
    let tenant = client.admin().create_tenant("restricted").await.unwrap();
    let principal = client
        .admin()
        .new_principal(tenant, "reader", [tidemq_sdk::Permission::Inspect])
        .build()
        .await
        .unwrap();
    let token = client
        .admin()
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    let restricted = token_client(&fixture, token.expose_secret()).await;
    assert!(restricted.admin().retire_node(2).await.is_err());
    assert!(
        !fixture
            .configs
            .get(&2)
            .unwrap()
            .data_dir
            .join("node-retirement.json")
            .is_file()
    );
    restricted.close().await.unwrap();
    let retired = client.admin().retire_node(2).await.unwrap();
    assert!(retired.retired && retired.draining && !retired.schedulable);
    connected.close().await.unwrap();
    assert!(client.admin().node_drain_status(2).await.unwrap().retired);
    assert!(
        fixture
            .configs
            .get(&2)
            .unwrap()
            .data_dir
            .join("node-retirement.json")
            .is_file()
    );
    fixture.stop(2).await;
    match RunningNode::start(fixture.configs.get(&2).unwrap().clone()).await {
        Ok(restarted) => {
            assert!(restarted.external_address.is_none());
            restarted.shutdown().await.unwrap();
        }
        Err(error) => {
            assert!(error.to_string().contains("Retired node identities"));
        }
    }
    assert!(
        client
            .admin()
            .register_node(registry::registration(&fixture, 2).await)
            .await
            .is_err()
    );
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn draining_broker_hands_off_existing_ownership_before_reporting_clear_blockers() {
    let mut fixture = Fixture::new(&[(1, Role::Combined), (2, Role::Broker)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = client.admin().create_topic("acme/app/drain").await.unwrap();
    let partition = PartitionKey {
        topic_id: topic.id().to_string().parse().unwrap(),
        partition: 0,
    };
    let route = fixture
        .running
        .get(&1)
        .unwrap()
        .route(partition, deadline())
        .await
        .unwrap();
    fixture
        .running
        .get(&1)
        .unwrap()
        .takeover(partition, 2, route.epoch, route.generation, deadline())
        .await
        .unwrap();
    let consumer = client
        .new_stream_consumer("acme/app/drain", "events")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer("acme/app/drain").build().await.unwrap();
    let first = producer
        .send(Message::new("before").key("one"))
        .await
        .unwrap()
        .message_id();
    let held = consumer.receive().await.unwrap();
    assert_eq!(held.message_id(), first);
    client.admin().begin_node_drain(2).await.unwrap();
    let until = deadline();
    loop {
        let status = client.admin().node_drain_status(2).await.unwrap();
        if status.durable_state_drained() {
            break;
        }
        assert!(
            Instant::now() < until,
            "Broker ownership did not leave the draining node"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    consumer.ack_cumulative(&held).await.unwrap();
    client.admin().retire_node(2).await.unwrap();
    fixture.stop(2).await;
    let second = producer
        .send(Message::new("after").key("one"))
        .await
        .unwrap()
        .message_id();
    let message = consumer.receive().await.unwrap();
    assert_eq!(message.message_id(), second);
    consumer.ack_cumulative(&message).await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[cfg(feature = "test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_local_retirement_proof_survives_a_restart_without_reopening_business_admission() {
    let mut fixture = Fixture::new(&[(1, Role::Combined), (2, Role::Gateway)], &[1], &[1]).await;
    let internal = fixture.configs.get(&2).unwrap().runtime.internal_listen;
    let proxy = udp_proxy::Proxy::start([internal, internal]).await;
    for config in fixture.configs.values_mut() {
        config
            .runtime
            .peers
            .iter_mut()
            .find(|peer| peer.node_id == 2)
            .unwrap()
            .internal_address = proxy.address;
    }
    fixture.start_all().await;
    let client = fixture.client(1).await;
    client.admin().begin_node_drain(2).await.unwrap();
    let cluster = fixture.configs.get(&1).unwrap().cluster_id.parse().unwrap();
    let hook = tidemq::runtime::test_hooks::InstalledPause::node_retirement(cluster, 2).unwrap();
    let context = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(1)).unwrap();
    let caller = client.clone();
    let waiting = tokio::spawn(async move { context.scope(caller.admin().retire_node(2)).await });
    hook.wait(deadline()).await.unwrap();
    proxy.set_response_mtu(0);
    hook.resume();
    assert!(waiting.await.unwrap().unwrap_err().outcome_unknown());
    let pending = client.admin().pending_operations().unwrap();
    assert_eq!(pending.len(), 1);
    fixture.stop(2).await;
    drop(hook);
    let resumed = RunningNode::start(fixture.configs.get(&2).unwrap().clone())
        .await
        .unwrap();
    assert!(resumed.external_address.is_none());
    assert!(
        http(resumed.management_address, "/readyz")
            .await
            .starts_with("HTTP/1.1 503")
    );
    fixture.running.insert(2, resumed);
    proxy.set_response_mtu(1200);
    let receipt = pending[0].retry().await.unwrap();
    let tidemq_sdk::AdminReceipt::NodeConfigured(node) = receipt else {
        panic!("Expected original node retirement receipt");
    };
    assert!(node.retired);
    assert!(client.admin().pending_operations().unwrap().is_empty());
    client.close().await.unwrap();
    fixture.close().await;
    proxy.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_node_retires_all_roles_and_rejects_retirement_through_its_own_gateway() {
    let mut fixture = Fixture::new(&[(1, Role::Combined), (2, Role::Combined)], &[1], &[1]).await;
    for config in fixture.configs.values_mut() {
        config.runtime.groups.first_mut().unwrap().standby = vec![2];
    }
    fixture.start_all().await;
    let ingress = fixture.client(1).await;
    let client = fixture.client(2).await;
    create_logical_resources(&client).await;
    let stream = client
        .new_stream_consumer(RUNTIME_TOPIC, "ordered")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let first = producer
        .send(Message::new("before").key("key"))
        .await
        .unwrap()
        .message_id();
    let held = stream.receive().await.unwrap();
    assert_eq!(held.message_id(), first);
    client.admin().replace_data_replica(2, 1, 2).await.unwrap();
    client.admin().replace_metadata_replica(1, 2).await.unwrap();
    client.admin().begin_node_drain(1).await.unwrap();
    let until = deadline();
    loop {
        if client
            .admin()
            .node_drain_status(1)
            .await
            .unwrap()
            .durable_state_drained()
        {
            break;
        }
        assert!(
            Instant::now() < until,
            "Combined node retained a role blocker"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let rejected = ingress.admin().retire_node(1).await.unwrap_err();
    assert!(
        !rejected.outcome_unknown(),
        "Retirement through the retiring gateway must reject before quiescing it"
    );
    assert!(
        !fixture
            .configs
            .get(&1)
            .unwrap()
            .data_dir
            .join("node-retirement.json")
            .is_file()
    );
    ingress.close().await.unwrap();
    client.admin().retire_node(1).await.unwrap();
    fixture.stop(1).await;
    stream.ack_cumulative(&held).await.unwrap();
    let next = producer
        .send(Message::new("after").key("key"))
        .await
        .unwrap()
        .message_id();
    let message = tidemq_sdk::RequestContext::with_timeout(Duration::from_secs(10))
        .unwrap()
        .scope(stream.receive())
        .await
        .unwrap();
    assert_eq!(message.message_id(), next);
    stream.ack_cumulative(&message).await.unwrap();
    stream.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
