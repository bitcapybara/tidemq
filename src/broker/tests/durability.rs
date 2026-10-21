use std::time::{Duration, Instant};

use tidemq_protocol::Id;

use crate::metadata::PartitionKey;
use crate::producer::{RegisterProducer, Rejection};
use crate::subscription::{self, Generations, SigningKey};

use super::super::{
    BrokerLimits, DataCommand, DataRejection, DataResponse, Error, ProducerCommand,
};
use super::support::{Cluster, batch, context, lease, writer};

#[tokio::test]
async fn rf1_snapshot_reopen_preserves_producer_results_ack_authority_and_retirement() {
    let mut cluster = Cluster::new(&[1]).await;
    let partition = PartitionKey {
        topic_id: Id::new_v7().unwrap(),
        partition: 0,
    };
    let grant = cluster.grant(partition, 1);
    cluster.fence_and_recover(&grant).await;
    let broker = cluster.broker(BrokerLimits::default());
    let authorization = lease(&grant);
    let session = broker
        .register(
            &authorization,
            context(),
            partition,
            RegisterProducer {
                producer_id: Id::new_v7().unwrap(),
                expected_registration_epoch: 1,
            },
        )
        .await
        .unwrap();
    let original = batch(partition, &session, 0, b"first-payload");
    assert_eq!(
        broker
            .produce(&authorization, context(), original.clone())
            .await
            .unwrap()
            .first_offset,
        0
    );
    let second = batch(partition, &session, 1, b"second-payload");
    broker
        .produce(&authorization, context(), second)
        .await
        .unwrap();

    let subscription_id = Id::new_v7().unwrap();
    let command = |command| DataCommand::Subscription {
        partition,
        writer: writer(&grant),
        command,
    };
    assert!(matches!(
        cluster
            .write(command(subscription::Command::Create {
                subscription_id,
                operation_id: Id::new_v7().unwrap(),
                start_offset: 0,
                signing_key: SigningKey::try_from([7; 32]).unwrap(),
            }))
            .await,
        DataResponse::Subscription(subscription::Outcome::Created(_))
    ));
    let assignment = match cluster
        .write(command(subscription::Command::Claim {
            subscription_id,
            expected: Generations {
                cursor: 1,
                assignment: 0,
            },
            session_id: Id::new_v7().unwrap(),
            replace_session_id: None,
            cold_before: 0,
        }))
        .await
    {
        DataResponse::Subscription(subscription::Outcome::Assigned(assignment)) => assignment,
        response => panic!("Unexpected assignment response: {response:?}"),
    };
    let bytes = original.messages.first().unwrap().logical_bytes().unwrap() as u32;
    let delivery = match cluster
        .write(command(subscription::Command::RecordDelivery {
            subscription_id,
            generations: assignment.generations,
            session_id: assignment.session_id,
            operation_id: Id::new_v7().unwrap(),
            offset: 0,
            expected_attempt: 0,
            body_bytes: bytes,
            now_ms: 100,
            lane: subscription::DeliveryLane::Realtime,
        }))
        .await
    {
        DataResponse::Subscription(subscription::Outcome::Delivery(delivery)) => delivery,
        response => panic!("Unexpected delivery response: {response:?}"),
    };
    let ack = subscription::Command::Ack {
        subscription_id,
        generations: assignment.generations,
        tokens: vec![delivery.token],
    };
    // The application commits the ACK; discard its response before retrying after recovery.
    let _lost_ack_response = cluster.write(command(ack.clone())).await;
    let (revision, now_ms) = cluster
        .node(1)
        .state
        .with_application(|state| {
            let retention = state.retention(partition).unwrap();
            assert!(state.capacity_usage().unwrap().logical_backlog_upper_bytes > 0);
            (
                retention.revision(),
                state.producer(partition).unwrap().logical_time_ms()
                    + retention.policy().minimum_age_ms
                    + 1_000,
            )
        })
        .await;
    assert!(matches!(
        cluster
            .write(DataCommand::Retention {
                partition,
                writer: writer(&grant),
                command: crate::retention::Command {
                    expected_revision: revision,
                    before: 2,
                    now_ms
                }
            })
            .await,
        DataResponse::Rejected(DataRejection::Retention(
            crate::retention::Rejection::Protected { maximum: 1 }
        ))
    ));
    assert!(matches!(
        cluster
            .write(DataCommand::Retention {
                partition,
                writer: writer(&grant),
                command: crate::retention::Command {
                    expected_revision: revision,
                    before: 1,
                    now_ms
                }
            })
            .await,
        DataResponse::Retention(crate::retention::Status { floor: 1, .. })
    ));
    assert!(matches!(
        cluster
            .write(command(subscription::Command::Seek {
                subscription_id,
                operation_id: Id::new_v7().unwrap(),
                expected_cursor_generation: assignment.generations.cursor,
                offset: 0
            }))
            .await,
        DataResponse::Rejected(DataRejection::Subscription(
            subscription::Rejection::OutOfRetention
        ))
    ));
    let last = cluster
        .node(1)
        .raft
        .metrics()
        .borrow()
        .last_applied
        .unwrap();
    cluster.node(1).raft.trigger().snapshot().await.unwrap();
    cluster
        .node(1)
        .raft
        .wait(Some(Duration::from_secs(5)))
        .snapshot(last, "Data snapshot is durable")
        .await
        .unwrap();
    cluster
        .node(1)
        .raft
        .trigger()
        .purge_log(last.index)
        .await
        .unwrap();
    cluster
        .node(1)
        .raft
        .wait(Some(Duration::from_secs(5)))
        .purged(Some(last), "Data snapshot protects purged commands")
        .await
        .unwrap();
    broker
        .drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    drop(broker);
    cluster.restart(1).await;
    let broker = cluster.broker(BrokerLimits::default());
    let authorization = lease(&grant);
    let duplicate = broker
        .produce(&authorization, context(), original.clone())
        .await
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.first_offset, 0);
    let ack_retry = cluster.write(command(ack)).await;
    assert!(
        matches!(ack_retry, DataResponse::Subscription(subscription::Outcome::Acknowledged(receipt)) if receipt.acknowledged_before == 1 && receipt.already_acknowledged == 1)
    );
    cluster
        .node(1)
        .state
        .with_application(|application| {
            assert_eq!(application.producer(partition).unwrap().committed_end(), 2);
            assert_eq!(application.retention(partition).unwrap().floor(), 1);
            assert_eq!(application.producer(partition).unwrap().retained_floor(), 1);
            crate::replication::Application::validate(application).unwrap();
        })
        .await;
    assert_eq!(
        cluster
            .write(DataCommand::Producer {
                partition,
                writer: writer(&grant),
                now_ms: session.expires_at_ms,
                command: ProducerCommand::AdvanceTime
            })
            .await,
        DataResponse::TimeAdvanced
    );
    broker
        .drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    drop(broker);
    cluster.restart(1).await;
    let broker = cluster.broker(BrokerLimits::default());
    assert!(matches!(
        broker.produce(&lease(&grant), context(), original).await,
        Err(Error::Rejected {
            reason: DataRejection::Producer(Rejection::SessionExpired)
        })
    ));
    drop(broker);
    cluster.shutdown().await;
}

#[tokio::test]
async fn rf3_lost_quorum_and_caller_cancel_hold_budget_until_the_original_commit_finishes() {
    let cluster = Cluster::new(&[1, 2, 3]).await;
    let partition = PartitionKey {
        topic_id: Id::new_v7().unwrap(),
        partition: 1,
    };
    let grant = cluster.grant(partition, 1);
    cluster.fence_and_recover(&grant).await;
    let broker = cluster.broker(BrokerLimits {
        produce_operations: 1,
        ..BrokerLimits::default()
    });
    let authorization = lease(&grant);
    let session = broker
        .register(
            &authorization,
            context(),
            partition,
            RegisterProducer {
                producer_id: Id::new_v7().unwrap(),
                expected_registration_epoch: 1,
            },
        )
        .await
        .unwrap();
    let original = batch(partition, &session, 0, b"quorum-payload");
    cluster.router.isolated.write().await.extend([2, 3]);
    let canceled = tokio::time::timeout(
        Duration::from_millis(150),
        broker.produce(&authorization, context(), original.clone()),
    )
    .await;
    assert!(
        canceled.is_err(),
        "A missing majority cannot return producer success"
    );
    assert_eq!(broker.metrics().snapshot().pending_operations, 1);
    assert!(broker.metrics().snapshot().pending_bytes > 0);
    assert_eq!(broker.metrics().snapshot().unknown_attempts_total, 1);
    assert!(matches!(
        broker
            .produce(&authorization, context(), original.clone())
            .await,
        Err(Error::Capacity)
    ));
    cluster
        .node(1)
        .state
        .with_application(|application| {
            assert_eq!(application.producer(partition).unwrap().committed_end(), 0)
        })
        .await;
    cluster.router.isolated.write().await.clear();
    tokio::time::timeout(Duration::from_secs(5), async {
        while broker.metrics().snapshot().pending_operations != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let recovered = broker
        .produce(&authorization, context(), original)
        .await
        .unwrap();
    assert!(recovered.duplicate);
    assert_eq!(recovered.first_offset, 0);
    assert_eq!(broker.metrics().snapshot().new_messages_total, 1);
    assert_eq!(broker.metrics().snapshot().duplicate_attempts_total, 1);
    drop(broker);
    cluster.shutdown().await;
}

#[tokio::test]
async fn ordered_submissions_pipeline_without_waiting_for_previous_durable_results() {
    let cluster = Cluster::new(&[1, 2, 3]).await;
    let partition = PartitionKey {
        topic_id: Id::new_v7().unwrap(),
        partition: 2,
    };
    let grant = cluster.grant(partition, 1);
    cluster.fence_and_recover(&grant).await;
    let broker = cluster.broker(BrokerLimits {
        produce_operations: 2,
        ..BrokerLimits::default()
    });
    let authorization = lease(&grant);
    let session = broker
        .register(
            &authorization,
            context(),
            partition,
            RegisterProducer {
                producer_id: Id::new_v7().unwrap(),
                expected_registration_epoch: 1,
            },
        )
        .await
        .unwrap();
    cluster.router.isolated.write().await.extend([2, 3]);
    let first = tokio::time::timeout(
        Duration::from_millis(100),
        broker.submit_produce(
            &authorization,
            context(),
            batch(partition, &session, 0, b"first"),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let second = tokio::time::timeout(
        Duration::from_millis(100),
        broker.submit_produce(
            &authorization,
            context(),
            batch(partition, &session, 1, b"second"),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(broker.metrics().snapshot().pending_operations, 2);
    cluster
        .node(1)
        .state
        .with_application(|state| assert_eq!(state.producer(partition).unwrap().committed_end(), 0))
        .await;
    cluster.router.isolated.write().await.clear();
    let (first, second) = tokio::join!(first.receive(), second.receive());
    assert_eq!(first.unwrap().first_offset, 0);
    assert_eq!(second.unwrap().first_offset, 1);
    assert_eq!(broker.metrics().snapshot().new_messages_total, 2);
    drop(broker);
    cluster.shutdown().await;
}
