use super::*;
use crate::runtime::wire::semantic_local_tests::Fixture;
use std::sync::Arc;

async fn local_queue_cursor() -> (
    crate::runtime::console_local::tests::Fixture,
    PartitionKey,
    Id,
) {
    use tidemq_protocol::{
        common::TrafficClass,
        console_operation::{Status, WriteCommand, WriteRequest},
        framing::new_frame,
    };

    let fixture = crate::runtime::LocalConsole::fixture().await;
    let (tenant, namespace) = fixture.tenant("typed").await;
    let root = fixture.root().await;
    let topic = root
        .write(&WriteRequest {
            operation_id: Id::new_v7().unwrap(),
            command: WriteCommand::CreateTopic {
                tenant,
                namespace,
                name: "events".into(),
            },
        })
        .await
        .unwrap();
    assert_eq!(topic.status, Status::Succeeded);
    let topic = topic.resource_id.unwrap();
    let identity = root
        .identities(&public::ListIdentities {
            kind: public::IdentityKind::CurrentPrincipalIdentity.into(),
            limit: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    let operation = Id::new_v7().unwrap();
    let access = Access {
        principal: Arc::from(format!(
            "credential:{}",
            Id::from_slice(&identity.current_credential_id).unwrap()
        )),
        scope: Arc::from("*"),
        session: Some(operation),
        tenant: None,
    };
    let service = fixture.service();
    let request = new_frame(
        FrameKind::ConsumerControl,
        TrafficClass::Control,
        operation,
        operation,
        &public::ConsumerControl {
            topic_id: Bytes::copy_from_slice(topic.as_bytes()),
            name: "bounded".into(),
            model: public::ConsumerModel::QueueConsumer.into(),
            action: public::ConsumerAction::EnsureConsumerSubscription.into(),
            ..Default::default()
        },
        3_000,
        &service.config.protocol,
    )
    .unwrap();
    let reply = service
        .consumer_control_public(
            &access,
            &request,
            Instant::now() + std::time::Duration::from_secs(3),
        )
        .await
        .unwrap();
    let reply: public::ConsumerControlResult =
        decode_payload(reply.payload, &service.config.protocol).unwrap();
    (
        fixture,
        PartitionKey {
            topic_id: topic,
            partition: 0,
        },
        Id::from_slice(&reply.subscription_id).unwrap(),
    )
}

#[tokio::test]
async fn local_consumer_control_wait_obeys_the_original_deadline_behind_a_cache_writer() {
    use std::future::{Future, poll_fn};
    use std::time::Duration;
    // The native SDK lifecycle has separate integration coverage; this setup uses real local auth.
    let (fixture, partition, subscription) =
        wire::without_remote_encoding(local_queue_cursor()).await;
    let service = fixture.service();
    assert!(
        service
            .transport_metrics
            .iter()
            .all(|metrics| metrics.snapshot().connections_total == 0),
        "Local cache contention setup must not depend on an SDK connection"
    );
    let guard = service.groups.write().await;
    let until = Instant::now() + Duration::from_millis(250);
    let mut pending = Box::pin(wire::without_remote_encoding(service.consumer_control(
        2,
        Control::Inspect {
            partition,
            subscription,
        },
        until,
    )));
    let mut first_poll = true;
    let mut released_bytes = None;
    let observation = tokio::time::timeout_at(
        until + Duration::from_millis(200),
        poll_fn(|context| {
            // Compare within each poll, so unrelated background work cannot change this ledger.
            let before = service.budget.available_bytes();
            let result = pending.as_mut().poll(context);
            let after = service.budget.available_bytes();
            if first_poll {
                assert!(
                    result.is_pending(),
                    "The cache writer must block the control"
                );
                assert_eq!(before.checked_sub(after), Some(65_536));
                first_poll = false;
            }
            if result.is_ready() {
                released_bytes = after.checked_sub(before);
            }
            result
        }),
    )
    .await;
    let respected_deadline = observation.is_ok();
    drop(guard);
    let late_success = match observation {
        Ok(result) => {
            assert_eq!(
                result.err().unwrap().failure().outcome,
                i32::from(tidemq_protocol::common::Outcome::Unknown)
            );
            drop(pending);
            false
        }
        Err(_) => pending.await.is_ok(),
    };
    eprintln!(
        "Typed consumer deadline ledger: respected_deadline={respected_deadline} late_success={late_success} released_bytes={released_bytes:?}"
    );
    service
        .consumer_control(
            2,
            Control::Inspect {
                partition,
                subscription,
            },
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    fixture.close().await;
    assert!(
        respected_deadline,
        "A local consumer control wait outlived its original deadline"
    );
    assert_eq!(released_bytes, Some(65_536));
}

#[tokio::test]
async fn local_consumer_progress_uses_the_durable_cursor_without_remote_encoding() {
    let fixture = Fixture::new().await;
    let consumer = fixture
        .client
        .new_stream_consumer("typed/app/events", "workers")
        .build()
        .await
        .unwrap();
    let (row, shard) = fixture
        .service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .try_with_application(|state| {
            (
                state
                    .consumer_subscriptions()
                    .find(|row| row.topic == fixture.partition.topic_id)
                    .unwrap()
                    .clone(),
                state.topology().shard(fixture.shard).unwrap().clone(),
            )
        })
        .await
        .unwrap();
    let result = wire::without_remote_encoding(async {
        let deadline = Instant::now() + std::time::Duration::from_secs(3);
        let plan = row.members.plan();
        let version = plan.version * 2 + u64::from(plan.fenced);
        let grants = row
            .members
            .grants(super::super::data::unix_millis()?)
            .unwrap();
        fixture
            .service
            .install_consumer_domain(&row, shard.route.id, version, grants, deadline)
            .await?;
        fixture
            .service
            .consumer_control(
                shard.route.group,
                Control::Ensure {
                    partition: shard.partition,
                    shard: shard.route.id,
                    subscription: row.id,
                    model: row.members.model(),
                    initial: row.members.initial_position(),
                    blocked: false,
                },
                deadline,
            )
            .await?;
        fixture
            .service
            .consumer_control(
                shard.route.group,
                Control::OpenGate {
                    partition: shard.partition,
                    subscription: row.id,
                },
                deadline,
            )
            .await?;
        let deletion = fixture
            .service
            .consumer_control(
                shard.route.group,
                Control::Delete {
                    partition: shard.partition,
                    domain: shard.route.id,
                    subscription: row.id,
                    deletion: Id::new_v7().unwrap(),
                },
                deadline,
            )
            .await;
        assert!(
            deletion.is_err(),
            "A deletion needs the current durable plan"
        );
        assert!(
            fixture
                .service
                .consumer_domain_progress(&shard, row.id, Instant::now())
                .await
                .is_err(),
            "Expired semantic work must not be admitted"
        );
        fixture
            .service
            .consumer_domain_progress(&shard, row.id, deadline)
            .await
    })
    .await;
    if let Ok(progress) = &result {
        assert_eq!(progress.frontier, 0);
        assert!(progress.gate_open);
    }
    consumer.close().await.unwrap();
    fixture.close().await;
    result.unwrap();
}
