use super::*;
use crate::metadata::{self, Command, Response};

fn request(kind: FrameKind, operation: Id, payload: &impl prost::Message) -> Frame {
    tidemq_protocol::framing::new_frame(
        kind,
        tidemq_protocol::common::TrafficClass::Control,
        Id::new_v7().unwrap(),
        operation,
        payload,
        10_000,
        &tidemq_protocol::ProtocolLimits::default(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replayed_topic_creation_preserves_the_original_result() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let service = fixture.running.get(&1).unwrap().service.clone();
    let access = service
        .access
        .delegated("runtime-test", "*", FrameKind::CreateTopic)
        .unwrap();
    let operation = Id::new_v7().unwrap();
    let payload = public::CreateTopic {
        name: "replayed-topic".into(),
        partitions: 1,
    };
    let original = service
        .metadata_public(
            access.clone(),
            &request(FrameKind::CreateTopic, operation, &payload),
            deadline(),
        )
        .await
        .unwrap();
    let replay = service
        .metadata_public(
            access.clone(),
            &request(FrameKind::CreateTopic, operation, &payload),
            deadline(),
        )
        .await;
    let changed = service
        .metadata_public(
            access,
            &request(
                FrameKind::CreateTopic,
                operation,
                &public::CreateTopic {
                    name: "changed-topic".into(),
                    ..payload
                },
            ),
            deadline(),
        )
        .await;
    fixture.close().await;
    assert_eq!(
        replay
            .expect("Replaying the original topic request must return its durable result")
            .payload,
        original.payload
    );
    assert!(matches!(changed, Err(Error::Rejected { failure })
        if failure.code == i32::from(ErrorCode::ContentConflict)
            && failure.outcome == i32::from(Outcome::Rejected) && !failure.retryable));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replayed_subscription_creation_preserves_the_original_result() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let topic = client
        .admin()
        .create_topic("subscription-replay", 1)
        .await
        .unwrap();
    let service = fixture.running.get(&1).unwrap().service.clone();
    let access = service
        .access
        .delegated("runtime-test", "*", FrameKind::CreateSubscription)
        .unwrap();
    let operation = Id::new_v7().unwrap();
    let payload = public::CreateSubscription {
        topic_id: topic.topic_id,
        name: "replayed-subscription".into(),
        start_offset: 0,
        delivery_mode: public::DeliveryMode::Parallel.into(),
    };
    let original = service
        .metadata_public(
            access.clone(),
            &request(FrameKind::CreateSubscription, operation, &payload),
            deadline(),
        )
        .await
        .unwrap();
    let replay = service
        .metadata_public(
            access.clone(),
            &request(FrameKind::CreateSubscription, operation, &payload),
            deadline(),
        )
        .await;
    let changed = service
        .metadata_public(
            access,
            &request(
                FrameKind::CreateSubscription,
                operation,
                &public::CreateSubscription {
                    name: "changed-subscription".into(),
                    ..payload
                },
            ),
            deadline(),
        )
        .await;
    client.close().await.unwrap();
    fixture.close().await;
    assert_eq!(
        replay
            .expect("Replaying the original subscription request must return its durable result")
            .payload,
        original.payload
    );
    assert!(matches!(changed, Err(Error::Rejected { failure })
        if failure.code == i32::from(ErrorCode::ContentConflict)
            && failure.outcome == i32::from(Outcome::Rejected) && !failure.retryable));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cached_rejection_cannot_become_an_applied_creation() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let service = fixture.running.get(&1).unwrap().service.clone();
    let replica = service.metadata.as_ref().unwrap();
    let access = service
        .access
        .delegated("runtime-test", "*", FrameKind::CreateTopic)
        .unwrap();
    let operation = Id::new_v7().unwrap();
    let payload = public::CreateTopic {
        name: "rejected-topic".into(),
        partitions: 1,
    };
    let rejected = service
        .meta_write(
            Command::create_topic(
                operation,
                service.meta_revision().await.unwrap(),
                operation,
                payload.name.clone(),
                vec![999],
            ),
            deadline(),
        )
        .await
        .unwrap_err();
    assert!(matches!(rejected, Error::Rejected { failure }
        if failure.code == i32::from(ErrorCode::NotFound)
            && failure.outcome == i32::from(Outcome::Rejected)));
    let previous = replica
        .state
        .try_with_application(|state| state.lookup_operation(operation))
        .await
        .unwrap();
    let replay = service
        .metadata_public(
            access,
            &request(FrameKind::CreateTopic, operation, &payload),
            deadline(),
        )
        .await;
    let (after, created) = replica
        .state
        .try_with_application(|state| {
            (
                state.lookup_operation(operation),
                state.topic(operation).is_some(),
            )
        })
        .await
        .unwrap();
    fixture.close().await;
    assert!(matches!(
        previous,
        metadata::OperationResult::Known(Response::Rejected {
            reason: metadata::Rejection::NotFound,
            ..
        })
    ));
    assert!(matches!(replay, Err(Error::Rejected { failure })
        if failure.outcome == i32::from(Outcome::Rejected)));
    assert_eq!(after, previous);
    assert!(!created);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unadmitted_revision_conflict_can_retry_with_the_original_resource_identity() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let service = fixture.running.get(&1).unwrap().service.clone();
    let access = service
        .access
        .delegated("runtime-test", "*", FrameKind::CreateTopic)
        .unwrap();
    let operation = Id::new_v7().unwrap();
    let payload = public::CreateTopic {
        name: "cas-retry-topic".into(),
        partitions: 1,
    };
    let error = service
        .meta_write(
            Command::create_topic(operation, 0, operation, payload.name.clone(), vec![2]),
            deadline(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Rejected { failure }
        if failure.code == i32::from(ErrorCode::Unavailable)
            && failure.outcome == i32::from(Outcome::Rejected)));
    let metadata = service.metadata.as_ref().unwrap();
    assert_eq!(
        metadata
            .state
            .try_with_application(|state| state.lookup_operation(operation))
            .await
            .unwrap(),
        metadata::OperationResult::UnknownOrRetired
    );
    let response = service
        .metadata_public(
            access,
            &request(FrameKind::CreateTopic, operation, &payload),
            deadline(),
        )
        .await;
    let (result, created) = metadata
        .state
        .try_with_application(|state| {
            (
                state.lookup_operation(operation),
                state.topic(operation).map(|topic| topic.name.clone()),
            )
        })
        .await
        .unwrap();
    fixture.close().await;
    response.unwrap();
    assert!(matches!(
        result,
        metadata::OperationResult::Known(Response::Applied { .. })
    ));
    assert_eq!(created.as_deref(), Some(payload.name.as_str()));
}
