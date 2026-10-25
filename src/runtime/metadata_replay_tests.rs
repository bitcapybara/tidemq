use super::*;
use crate::metadata::{self, Command, Response};

fn request(kind: FrameKind, operation: Id, payload: &impl prost::Message) -> Frame {
    tidemq_protocol::framing::new_frame(
        kind,
        TrafficClass::Control,
        Id::new_v7().unwrap(),
        operation,
        payload,
        10_000,
        &ProtocolLimits::default(),
    )
    .unwrap()
}

async fn dispatch(
    service: &Service,
    kind: FrameKind,
    operation: Id,
    payload: &impl prost::Message,
) -> super::super::Result<Frame> {
    let frame = request(kind, operation, payload);
    let until = deadline();
    loop {
        match service
            .metadata_public(access(service, &frame).await, &frame, until)
            .await
        {
            Err(error) if error.failure().retryable && Instant::now() < until => {
                tokio::time::sleep_until(until.min(Instant::now() + Duration::from_millis(10)))
                    .await;
            }
            result => return result,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replayed_logical_topic_creation_preserves_the_original_result() {
    let (fixture, client) = setup().await;
    let service = fixture.running.get(&1).unwrap().service.clone();
    let operation = Id::new_v7().unwrap();
    let payload = public::LogicalTopic {
        name: "test/app/replayed-topic".into(),
    };
    let original = dispatch(&service, FrameKind::CreateLogicalTopic, operation, &payload)
        .await
        .unwrap();
    let replay = dispatch(&service, FrameKind::CreateLogicalTopic, operation, &payload)
        .await
        .unwrap();
    assert_eq!(replay.payload, original.payload);
    let changed = dispatch(
        &service,
        FrameKind::CreateLogicalTopic,
        operation,
        &public::LogicalTopic {
            name: "test/app/changed-topic".into(),
        },
    )
    .await;
    assert!(matches!(changed, Err(Error::Rejected { failure })
        if failure.code == i32::from(ErrorCode::ContentConflict)
            && failure.outcome == i32::from(Outcome::Rejected) && !failure.retryable));
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replayed_queue_creation_preserves_the_fixed_subscription_identity() {
    let (fixture, client) = setup().await;
    let topic = client
        .admin()
        .create_topic("test/app/subscription-replay")
        .await
        .unwrap();
    let service = fixture.running.get(&1).unwrap().service.clone();
    let operation = Id::new_v7().unwrap();
    let payload = public::ConsumerControl {
        topic_id: Bytes::copy_from_slice(topic.id().to_string().parse::<Id>().unwrap().as_bytes()),
        name: "replayed-subscription".into(),
        model: public::ConsumerModel::QueueConsumer.into(),
        action: public::ConsumerAction::EnsureConsumerSubscription.into(),
        max_messages: 8,
        max_bytes: 64 * 1024,
        ..Default::default()
    };
    let until = deadline();
    let original = loop {
        let frame = dispatch(&service, FrameKind::ConsumerControl, operation, &payload)
            .await
            .unwrap();
        let result: public::ConsumerControlResult =
            decode_payload(frame.payload, &ProtocolLimits::default()).unwrap();
        if result.plan_active {
            break result;
        }
        assert!(
            Instant::now() < until,
            "The original subscription installation did not complete"
        );
        tokio::task::yield_now().await;
    };
    let replay = dispatch(&service, FrameKind::ConsumerControl, operation, &payload)
        .await
        .unwrap();
    let replay: public::ConsumerControlResult =
        decode_payload(replay.payload, &ProtocolLimits::default()).unwrap();
    assert_eq!(replay.subscription_id, original.subscription_id);
    assert!(replay.plan_active);
    let changed = dispatch(
        &service,
        FrameKind::ConsumerControl,
        operation,
        &public::ConsumerControl {
            name: "changed-subscription".into(),
            ..payload
        },
    )
    .await;
    assert!(matches!(changed, Err(Error::Rejected { failure })
        if failure.code == i32::from(ErrorCode::ContentConflict)
            && failure.outcome == i32::from(Outcome::Rejected) && !failure.retryable));
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cached_rejection_cannot_become_an_applied_creation() {
    let (fixture, client) = setup().await;
    let service = fixture.running.get(&1).unwrap().service.clone();
    let replica = service.metadata.as_ref().unwrap();
    let operation = Id::new_v7().unwrap();
    let payload = public::LogicalTopic {
        name: "test/app/rejected-topic".into(),
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
        .with_application(|state| state.lookup_operation(operation))
        .await;
    let replay = dispatch(&service, FrameKind::CreateLogicalTopic, operation, &payload).await;
    let (after, created) = replica
        .state
        .with_application(|state| {
            (
                state.lookup_operation(operation),
                state.topic(operation).is_some(),
            )
        })
        .await;
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
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unadmitted_revision_conflict_can_retry_with_the_original_resource_identity() {
    let (fixture, client) = setup().await;
    let service = fixture.running.get(&1).unwrap().service.clone();
    let operation = Id::new_v7().unwrap();
    let payload = public::LogicalTopic {
        name: "test/app/cas-retry-topic".into(),
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
            .with_application(|state| state.lookup_operation(operation))
            .await,
        metadata::OperationResult::UnknownOrRetired
    );
    dispatch(&service, FrameKind::CreateLogicalTopic, operation, &payload)
        .await
        .unwrap();
    let (result, created) = metadata
        .state
        .with_application(|state| {
            (
                state.lookup_operation(operation),
                state.topic(operation).map(|topic| topic.name.clone()),
            )
        })
        .await;
    assert!(matches!(
        result,
        metadata::OperationResult::Known(Response::Applied { .. })
    ));
    assert_eq!(created.as_deref(), Some(payload.name.as_str()));
    client.close().await.unwrap();
    fixture.close().await;
}
