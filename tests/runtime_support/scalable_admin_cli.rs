use super::*;

pub(super) async fn output(
    fixture: &Fixture,
    gateway: u64,
    arguments: &[&str],
) -> std::process::Output {
    let address = fixture.configs.get(&gateway).unwrap().listen.to_string();
    tokio::time::timeout(
        Duration::from_secs(60),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_tidemq"))
            .env("TOKIO_WORKER_THREADS", "2")
            .arg("admin")
            .arg("--endpoint")
            .arg(address)
            .arg("--ca")
            .arg(fixture.directory.join("ca.pem"))
            .arg("--token-file")
            .arg(fixture.directory.join("bootstrap.token"))
            .args(arguments)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap()
}

async fn command(fixture: &Fixture, arguments: &[&str]) -> serde_json::Value {
    let result = output(fixture, 1, arguments).await;
    assert!(
        result.status.success(),
        "Administration failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(json["status"], "completed");
    assert_eq!(json["close"]["local_cleanup_complete"], true);
    assert_eq!(json["close"]["unresolved_administrations"], 0);
    json["result"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_cli_lists_transfers_changes_layout_and_retires_a_broker_before_shutdown() {
    let mut fixture = Fixture::new(&[(1, Role::Combined), (2, Role::Broker)], &[1], &[1]).await;
    configure_dynamic_fixture(&mut fixture).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    create_logical_resources(&client).await;
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let queue = client
        .new_queue_consumer(RUNTIME_TOPIC, "cli-jobs")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let id = producer
        .send("before-operator-cli")
        .await
        .unwrap()
        .message_id();
    let held = tokio::time::timeout_at(deadline(), queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.message_id(), id);
    assert_eq!(
        command(&fixture, &["nodes"])
            .await
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        command(&fixture, &["groups"])
            .await
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let shards = command(&fixture, &["shards", RUNTIME_TOPIC]).await;
    let shard = shards.as_array().unwrap().first().unwrap()["id"]
        .as_str()
        .unwrap();
    let moved = command(&fixture, &["transfer-shard", shard, "--target-group", "3"]).await;
    assert_eq!(moved["stage"], "Retired");
    let current = command(&fixture, &["shards", RUNTIME_TOPIC]).await;
    assert_eq!(
        current.as_array().unwrap().first().unwrap()["authority_group"],
        3
    );
    let operation = moved["operation"].as_str().unwrap();
    assert_eq!(
        command(&fixture, &["transfer-status", operation]).await,
        moved
    );
    let split = command(
        &fixture,
        &["split-topic", RUNTIME_TOPIC, "--at", "9223372036854775808"],
    )
    .await;
    assert_eq!(split["shard_count"], 2);
    let merged = command(
        &fixture,
        &[
            "merge-topic",
            RUNTIME_TOPIC,
            "--boundary",
            "9223372036854775808",
        ],
    )
    .await;
    assert_eq!(merged["shard_count"], 1);
    queue.ack(&held).await.unwrap();
    let next = producer
        .send("after-operator-cli")
        .await
        .unwrap()
        .message_id();
    let message = tokio::time::timeout_at(deadline(), queue.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.message_id(), next);
    queue.ack(&message).await.unwrap();
    let disabled = command(
        &fixture,
        &["set-schedulable", "--node-id", "2", "--enabled", "false"],
    )
    .await;
    assert_eq!(disabled["schedulable"], false);
    assert_eq!(
        command(&fixture, &["drain-node", "--node-id", "2"]).await["draining"],
        true
    );
    let until = deadline();
    while command(&fixture, &["drain-status", "--node-id", "2"]).await["durable_state_drained"]
        != true
    {
        assert!(Instant::now() < until, "Broker drain did not complete");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        command(&fixture, &["retire-node", "--node-id", "2"]).await["retired"],
        true
    );
    fixture.stop(2).await;
    producer.close().await.unwrap();
    queue.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
