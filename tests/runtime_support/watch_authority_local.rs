use super::*;
use crate::config::Role;
use crate::metadata::TokenDigest;
use tidemq_protocol::common::ErrorCode;

#[tokio::test]
async fn local_watch_authority_rejection_is_independent_of_remote_envelope_limits() {
    let mut fixture = crate::runtime::data::consumer_tests::fixture::Fixture::new(
        &[(1, Role::Combined)],
        &[1],
        &[1],
    )
    .await;
    fixture.start_all().await;
    let service = fixture.running.get(&1).unwrap().service.clone();
    let credential = service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .try_with_application(|state| {
            state
                .auth()
                .authenticate(
                    TokenDigest::of("runtime-test-access-token-with-at-least-32-characters")
                        .unwrap(),
                    crate::runtime::data::unix_millis().unwrap(),
                )
                .unwrap()
                .id
        })
        .await
        .unwrap();
    let mut rpc = service.rpc.clone();
    // Local semantic authorization has no transport envelope to fit in this limit.
    rpc.limits.max_control_frame_bytes = 1;
    let request = Membership {
        topic: Id::new_v7().unwrap(),
        subscription: Id::new_v7().unwrap(),
        member: Id::new_v7().unwrap(),
        epoch: 1,
        plan: 1,
        client: Id::new_v7().unwrap(),
        credential,
        targets: vec![(Id::new_v7().unwrap(), 0)],
    };
    let result = rpc
        .authorize_watch(
            1,
            service.config.runtime.metadata_group,
            Id::new_v7().unwrap(),
            &request,
            Instant::now() + std::time::Duration::from_secs(2),
        )
        .await;
    fixture.close().await;
    assert_eq!(
        result.err().unwrap().failure().code,
        i32::from(ErrorCode::Forbidden)
    );
}

#[tokio::test]
async fn native_combined_idle_watch_reauthorizes_and_delivers_with_real_actor_correlation() {
    use serde_json::Value;
    use std::time::Duration;
    let buffer = crate::observability::logging::tests::Buffer(std::sync::Arc::default());
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(move || writer.clone())
        .finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let mut fixture = crate::runtime::data::consumer_tests::fixture::Fixture::new(
        &[(1, Role::Combined)],
        &[1],
        &[1],
    )
    .await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("watch").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client
        .admin()
        .create_topic("watch/app/events")
        .await
        .unwrap();
    let queue = client
        .new_queue_consumer("watch/app/events", "workers")
        .build()
        .await
        .unwrap();
    let receiver = queue.clone();
    let receive = tokio::spawn(async move { receiver.receive().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
            if output.contains("watch_authority_local_completed") {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let producer = client
        .new_producer("watch/app/events")
        .build()
        .await
        .unwrap();
    let sent = producer.send("typed-watch").await.unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(5), receive)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(delivery.message_id(), sent.message_id());
    queue.ack(&delivery).await.unwrap();
    drop(delivery);
    queue.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
    let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    assert!(!output.contains("runtime-test-access-token-with-at-least-32-characters"));
    let event = output
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["fields"]["event_code"] == "watch_authority_local_completed")
        .unwrap();
    for field in ["request_id", "operation_id", "actor", "credential"] {
        assert!(
            event["fields"][field]
                .as_str()
                .unwrap()
                .parse::<Id>()
                .is_ok()
        );
    }
    assert!(event["fields"].get("error_code").is_none());
}
