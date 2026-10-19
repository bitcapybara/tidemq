use std::time::{Duration, Instant};

use tidemq_protocol::Id;

use crate::ownership::{ControlLease, FinalizationKind, Grant, Work};
use crate::subscription::{self, Generations, SigningKey};

use super::super::{DataCommand, DataResponse};
use super::support::{Cluster, writer};

pub(super) async fn commit_first_ack(
    cluster: &Cluster,
    grant: &Grant,
    lease: &ControlLease,
    logical_bytes: u32,
) -> subscription::Command {
    let fence = cluster
        .node(1)
        .state
        .with_application(|state| {
            state
                .ownership()
                .unwrap()
                .fence(grant.partition)
                .unwrap()
                .clone()
        })
        .await;
    lease.check(&fence, Work::ControlMutation).unwrap();
    let subscription_id = Id::random().unwrap();
    let command = |command| DataCommand::Subscription {
        partition: grant.partition,
        writer: writer(grant),
        command,
    };
    assert!(matches!(
        cluster
            .write(command(subscription::Command::Create {
                subscription_id,
                operation_id: Id::random().unwrap(),
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
            session_id: Id::random().unwrap(),
            replace_session_id: None,
            cold_before: 0,
        }))
        .await
    {
        DataResponse::Subscription(subscription::Outcome::Assigned(assignment)) => assignment,
        response => panic!("Unexpected assignment response: {response:?}"),
    };
    lease.check(&fence, Work::Deliver).unwrap();
    let delivery = match cluster
        .write(command(subscription::Command::RecordDelivery {
            subscription_id,
            generations: assignment.generations,
            session_id: assignment.session_id,
            operation_id: Id::random().unwrap(),
            offset: 0,
            expected_attempt: 0,
            body_bytes: logical_bytes,
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
    let finalization = lease
        .begin_finalization(
            &fence,
            FinalizationKind::Ack,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    finalization.check(&fence).unwrap();
    assert!(matches!(cluster.write(command(ack.clone())).await,
        DataResponse::Subscription(subscription::Outcome::Acknowledged(receipt)) if receipt.acknowledged_before == 1 && receipt.accepted == 1));
    ack
}
