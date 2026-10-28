use super::*;
use crate::metadata::{ConsumerCommand, ConsumerOutcome, Outcome, RetentionControl};
use tidemq_protocol::topic::{InitialPosition, SubscriptionKind};

fn transition(state: &mut MetadataState, action: Action) -> Response {
    let command = Command::new(Id::new_v7().unwrap(), state.revision(), action);
    let response = state.apply(command);
    state.validate_snapshot().unwrap();
    response
}

fn create(topic: Id) -> Action {
    Action::Consumer(ConsumerCommand::Create {
        topic,
        name: "history".into(),
        model: SubscriptionKind::Stream,
        initial: InitialPosition::EarliestAvailable,
    })
}

#[test]
fn subscription_creation_and_floor_reservations_are_ordered_and_survive_restart() {
    let (mut state, partition) = fixture();
    let command = crate::retention::Command {
        expected_revision: 1,
        before: 2,
        now_ms: 2_000,
    };
    let prepare = Action::Retention(RetentionControl::Prepare {
        partition,
        generation: 0,
        command,
    });
    assert!(transition(&mut state, prepare.clone()).is_applied());
    rejected(
        transition(&mut state, create(partition.topic_id)),
        Rejection::RecoveryRequired,
    );
    state = postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    assert!(state.pending_retention(partition.topic_id));
    rejected(
        transition(
            &mut state,
            Action::Retention(RetentionControl::Finish {
                partition,
                generation: 1,
                revision: 1,
            }),
        ),
        Rejection::InvalidEvidence,
    );
    assert!(
        transition(
            &mut state,
            Action::Retention(RetentionControl::Finish {
                partition,
                generation: 1,
                revision: 2,
            })
        )
        .is_applied()
    );
    let Response::Applied {
        outcome: Outcome::Consumer(ConsumerOutcome::Created { subscription }),
        ..
    } = transition(&mut state, create(partition.topic_id))
    else {
        panic!("Expected a durable subscription plan");
    };
    assert!(!state.consumer_subscription(subscription).unwrap().prepared);
    let next = Action::Retention(RetentionControl::Prepare {
        partition,
        generation: 1,
        command: crate::retention::Command {
            expected_revision: 2,
            before: 3,
            now_ms: 3_000,
        },
    });
    rejected(
        transition(&mut state, next.clone()),
        Rejection::RecoveryRequired,
    );
    state = postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    state.validate_snapshot().unwrap();
    rejected(
        transition(&mut state, next.clone()),
        Rejection::RecoveryRequired,
    );
    let plan = state
        .consumer_subscription(subscription)
        .unwrap()
        .members
        .plan()
        .clone();
    for fenced in [false, true] {
        for domain in &plan.domains {
            assert!(
                transition(
                    &mut state,
                    Action::Consumer(ConsumerCommand::Observe {
                        subscription,
                        version: plan.version,
                        fenced,
                        domain: *domain,
                        now_ms: 1,
                    })
                )
                .is_applied()
            );
        }
    }
    assert!(state.consumer_subscription(subscription).unwrap().prepared);
    assert!(transition(&mut state, next).is_applied());
    assert_eq!(state.retention_plan(partition).unwrap().generation, 2);
    rejected(transition(&mut state, prepare), Rejection::RecoveryRequired);
}

#[test]
fn a_superseded_retention_cas_cannot_execute_after_its_reservation_releases() {
    let (mut state, partition) = fixture();
    let mut data = crate::retention::RetentionState::new(crate::retention::Policy {
        minimum_age_ms: 0,
        ..Default::default()
    })
    .unwrap();
    data.record_append(0, 2, 1_000, 10).unwrap();
    let command = crate::retention::Command {
        expected_revision: 1,
        before: 2,
        now_ms: 2_000,
    };
    assert!(
        transition(
            &mut state,
            Action::Retention(RetentionControl::Prepare {
                partition,
                generation: 0,
                command
            })
        )
        .is_applied()
    );
    data.record_append(2, 3, 2_000, 10).unwrap();
    assert!(
        transition(
            &mut state,
            Action::Retention(RetentionControl::Finish {
                partition,
                generation: 1,
                revision: data.status(2_000, None).revision,
            })
        )
        .is_applied()
    );
    assert!(transition(&mut state, create(partition.topic_id)).is_applied());
    assert!(matches!(
        data.advance(command, 3, None),
        Err(crate::retention::Rejection::StaleRevision { .. })
    ));
    assert_eq!(data.status(2_000, None).floor, 0);
}

#[test]
fn new_subscriptions_and_layout_protection_cannot_pass_each_other() {
    let prepare_layout = |state: &MetadataState, topic| Action::PrepareLayout {
        topic,
        revision: 1,
        change: crate::metadata::LayoutChange::Split {
            source: state
                .topology()
                .layout(topic)
                .unwrap()
                .shards
                .first()
                .unwrap()
                .id,
            at: 1 << 63,
            groups: [2, 3],
        },
    };
    let (mut state, partition) = fixture();
    assert!(
        state
            .apply(Command::create_group(
                Id::new_v7().unwrap(),
                state.revision(),
                3,
                crate::metadata::GroupPlacement::Raft { voters: [1].into() }
            ))
            .is_applied()
    );
    let layout = prepare_layout(&state, partition.topic_id);
    assert!(transition(&mut state, create(partition.topic_id)).is_applied());
    rejected(transition(&mut state, layout), Rejection::RecoveryRequired);

    let (mut state, partition) = fixture();
    assert!(
        state
            .apply(Command::create_group(
                Id::new_v7().unwrap(),
                state.revision(),
                3,
                crate::metadata::GroupPlacement::Raft { voters: [1].into() }
            ))
            .is_applied()
    );
    let layout = prepare_layout(&state, partition.topic_id);
    assert!(transition(&mut state, layout).is_applied());
    rejected(
        transition(&mut state, create(partition.topic_id)),
        Rejection::RecoveryRequired,
    );
    assert!(state.consumer_subscriptions().next().is_none());
}
