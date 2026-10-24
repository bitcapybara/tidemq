use super::*;
use crate::metadata::TopicDeletionCommand;

fn delete(state: &mut MetadataState, request: TopicDeletionCommand) -> Response {
    state.apply(Command::topic_deletion(
        Id::new_v7().unwrap(),
        state.revision(),
        request,
    ))
}

#[test]
fn topic_deletion_requires_every_durable_seal_and_survives_restart() {
    let (mut state, partition) = fixture();
    let topic = partition.topic_id;
    let source = state.topology().domains(topic).next().unwrap().route.id;
    let operation = Id::new_v7().unwrap();
    assert!(
        state
            .apply(Command::topic_deletion(
                operation,
                state.revision(),
                TopicDeletionCommand::Begin {
                    topic,
                    generation: 1
                }
            ))
            .is_applied()
    );
    assert!(!state.topic_writable(topic));
    assert!(state.topic_by_name("orders").is_some());
    rejected(
        delete(
            &mut state,
            TopicDeletionCommand::Finish {
                topic,
                deletion: operation,
            },
        ),
        Rejection::RecoveryRequired,
    );
    let plan = state.topic_deletion(topic).unwrap().clone();
    rejected(
        delete(
            &mut state,
            TopicDeletionCommand::ObserveSeal {
                topic,
                deletion: operation,
                domain: id(999),
                end: 0,
            },
        ),
        Rejection::InvalidEvidence,
    );
    assert_eq!(state.topic_deletion(topic).unwrap(), &plan);
    assert!(
        delete(
            &mut state,
            TopicDeletionCommand::ObserveSeal {
                topic,
                deletion: operation,
                domain: source,
                end: 5
            }
        )
        .is_applied()
    );
    state.validate_snapshot().unwrap();
    let mut restored: MetadataState =
        serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    restored.validate_snapshot().unwrap();
    assert!(
        delete(
            &mut restored,
            TopicDeletionCommand::Finish {
                topic,
                deletion: operation
            }
        )
        .is_applied()
    );
    assert!(restored.topic_deletion(topic).unwrap().complete);
    assert!(restored.topic_by_name("orders").is_none());
    assert_eq!(
        restored.topic(topic).unwrap().status,
        super::super::RecordStatus::Retired
    );
    assert!(
        delete(
            &mut restored,
            TopicDeletionCommand::Finish {
                topic,
                deletion: operation
            }
        )
        .is_applied()
    );
    restored.validate_snapshot().unwrap();
}

#[test]
fn direct_topic_retirement_cannot_skip_data_fencing_or_delete_children() {
    let (mut state, partition) = fixture();
    rejected(
        state.apply(Command::retire_topic(
            id(901),
            state.revision(),
            partition.topic_id,
            1,
        )),
        Rejection::RecoveryRequired,
    );
    let subscription = id(902);
    assert!(
        state
            .apply(Command::put_subscription(
                id(903),
                state.revision(),
                subscription,
                partition.topic_id,
                "jobs".into(),
                0,
                Default::default()
            ))
            .is_applied()
    );
    rejected(
        delete(
            &mut state,
            TopicDeletionCommand::Begin {
                topic: partition.topic_id,
                generation: 1,
            },
        ),
        Rejection::SubscriptionInUse,
    );
    assert!(state.topic_writable(partition.topic_id));
    assert!(state.topic_deletion(partition.topic_id).is_none());
}

#[test]
fn a_deleting_topic_rejects_new_layout_plans() {
    let (mut state, partition) = fixture();
    let topic = partition.topic_id;
    let source = state.topology().domains(topic).next().unwrap().route.id;
    assert!(
        delete(
            &mut state,
            TopicDeletionCommand::Begin {
                topic,
                generation: 1
            }
        )
        .is_applied()
    );
    let revision = state.topology().layout(topic).unwrap().revision;
    rejected(
        state.apply(Command::prepare_layout(
            id(910),
            state.revision(),
            topic,
            revision,
            super::super::LayoutChange::Split {
                source,
                at: 1 << 63,
                groups: [2, 2],
            },
        )),
        Rejection::IdentityRetired,
    );
    assert!(matches!(state.topic_deletion(topic), Some(plan) if !plan.complete));
    assert!(matches!(
        state.lookup_operation(id(910)),
        super::super::OperationResult::Known(Response::Rejected { .. })
    ));
}
