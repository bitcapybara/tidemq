use super::*;
use tidemq_protocol::common::{ErrorCode, Outcome};
use tidemq_sdk::{AdminReceipt, DataGroupInfo, Error, RequestContext};

fn retryable_data_replacement_error(error: &Error) -> bool {
    match error {
        Error::Administration { source, .. } => retryable_data_replacement_error(source),
        Error::RequestUnknown { source } => retryable_data_replacement_error(source),
        Error::Remote {
            code: ErrorCode::Unavailable,
            outcome: Outcome::Rejected | Outcome::Unknown,
            retryable: true,
            original_outcome_known: false,
            ..
        } => true,
        _ => false,
    }
}

#[test]
fn data_replacement_recovery_accepts_only_unresolved_retryable_unavailability() {
    for (code, outcome, retryable, original_outcome_known, expected) in [
        (ErrorCode::Unavailable, Outcome::Rejected, true, false, true),
        (ErrorCode::Unavailable, Outcome::Unknown, true, false, true),
        (
            ErrorCode::Unavailable,
            Outcome::Unknown,
            false,
            false,
            false,
        ),
        (ErrorCode::Unavailable, Outcome::Rejected, true, true, false),
        (ErrorCode::Forbidden, Outcome::Rejected, false, false, false),
    ] {
        let error = Error::RequestUnknown {
            source: Box::new(Error::Remote {
                code,
                message: String::new(),
                outcome,
                retryable,
                expected_sequence: None,
                expected_registration_epoch: None,
                retry_after_ms: None,
                original_outcome_known,
            }),
        };
        assert_eq!(retryable_data_replacement_error(&error), expected);
    }
    assert!(!retryable_data_replacement_error(&Error::RequestUnknown {
        source: Box::new(Error::ResponseMismatch {
            reason: "The reply did not match the original operation".into(),
        }),
    }));
}

async fn replace_original_replica(
    client: &Client,
    removed: u64,
) -> tidemq_sdk::Result<DataGroupInfo> {
    // The fixture caps each SDK call at ten seconds; leave time to recover the same handle.
    let until = Instant::now() + Duration::from_secs(20);
    let context = RequestContext::with_deadline(until);
    let initial = context
        .scope(client.admin().replace_data_replica(2, removed, 5))
        .await;
    let pending = match initial {
        Ok(receipt) => return Ok(receipt),
        Err(Error::Administration { pending, source })
            if retryable_data_replacement_error(&source) && Instant::now() < until =>
        {
            eprintln!("Recovering the original data replacement: {pending:?}; {source:#}");
            pending
        }
        Err(error) => return Err(error),
    };
    loop {
        match context.scope(pending.retry()).await {
            Ok(AdminReceipt::DataGroupReconfigured(receipt)) => return Ok(receipt),
            Ok(_) => {
                return Err(Error::ResponseMismatch {
                    reason: "The original data replacement returned another receipt kind".into(),
                });
            }
            Err(error) if retryable_data_replacement_error(&error) && Instant::now() < until => {
                tokio::time::sleep_until(until.min(Instant::now() + Duration::from_millis(25)))
                    .await;
                if Instant::now() >= until {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

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
        (1, Role::Metadata),
        (9, Role::Broker),
        (10, Role::Gateway),
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
    let client = fixture.client(10).await;
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
    let result = replace_original_replica(&client, removed).await;
    if result.is_err() {
        evidence(&fixture).await;
    }
    let result = result.expect("The original data replacement must complete");
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
            (1, Role::Metadata),
            (9, Role::Broker),
            (10, Role::Gateway),
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
    let client = fixture.client(10).await;
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
