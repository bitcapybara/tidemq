use super::*;
use crate::metadata::{ConsumerCommand, ConsumerDeletion, ConsumerOutcome, MemberJoin, Outcome};
use tidemq_protocol::topic::{InitialPosition, SubscriptionKind};

fn consumer(state: &mut MetadataState, command: ConsumerCommand) -> Response {
    let result = state.apply(Command::consumer(
        Id::new_v7().unwrap(),
        state.revision(),
        command,
    ));
    state.validate_snapshot().unwrap();
    result
}

fn finish_membership(state: &mut MetadataState, subscription: Id, now_ms: u64) {
    let plan = state
        .consumer_subscription(subscription)
        .unwrap()
        .members
        .plan()
        .clone();
    for fenced in [false, true] {
        for domain in &plan.domains {
            let response = consumer(
                state,
                ConsumerCommand::Observe {
                    subscription,
                    version: plan.version,
                    fenced,
                    domain: *domain,
                    now_ms,
                },
            );
            assert!(
                response.is_applied(),
                "Membership observation was rejected: {response:?}"
            );
        }
    }
}

fn empty_subscription(
    state: &mut MetadataState,
    topic: Id,
    name: &str,
    model: SubscriptionKind,
) -> Id {
    let response = consumer(
        state,
        ConsumerCommand::Create {
            topic,
            name: name.into(),
            model,
            initial: InitialPosition::EarliestAvailable,
        },
    );
    let Response::Applied {
        outcome: Outcome::Consumer(ConsumerOutcome::Created { subscription }),
        ..
    } = response
    else {
        panic!("Subscription creation was rejected: {response:?}");
    };
    finish_membership(state, subscription, 1);
    subscription
}

fn deletion(response: Response) -> ConsumerDeletion {
    match response {
        Response::Applied {
            outcome: Outcome::Consumer(ConsumerOutcome::Deletion(plan)),
            ..
        } => plan,
        response => panic!("Deletion transition was rejected: {response:?}"),
    }
}

#[test]
fn subscription_deletion_requires_online_members_and_handoffs_to_finish() {
    let (mut state, partition) = fixture();
    let subscription = empty_subscription(
        &mut state,
        partition.topic_id,
        "jobs",
        SubscriptionKind::Queue,
    );
    let join = MemberJoin {
        member: Id::new_v7().unwrap(),
        expected_epoch: 1,
        client: Id::new_v7().unwrap(),
        credential: Id::new_v7().unwrap(),
        now_ms: 2,
        credential_expires_ms: 100_000,
        max_messages: 8,
        max_bytes: 4096,
        digest: [7; 32],
    };
    assert!(
        consumer(
            &mut state,
            ConsumerCommand::Join {
                subscription,
                request: join.clone()
            }
        )
        .is_applied()
    );
    rejected(
        consumer(
            &mut state,
            ConsumerCommand::Delete {
                subscription,
                model: SubscriptionKind::Queue,
                now_ms: 3,
            },
        ),
        Rejection::SubscriptionInUse,
    );
    finish_membership(&mut state, subscription, 4);
    rejected(
        consumer(
            &mut state,
            ConsumerCommand::Delete {
                subscription,
                model: SubscriptionKind::Queue,
                now_ms: 5,
            },
        ),
        Rejection::SubscriptionInUse,
    );
    assert!(
        consumer(
            &mut state,
            ConsumerCommand::Leave {
                subscription,
                member: join.member,
                epoch: 1,
                client: join.client,
                credential: join.credential,
                now_ms: 6
            }
        )
        .is_applied()
    );
    rejected(
        consumer(
            &mut state,
            ConsumerCommand::Delete {
                subscription,
                model: SubscriptionKind::Queue,
                now_ms: 7,
            },
        ),
        Rejection::SubscriptionInUse,
    );
    finish_membership(&mut state, subscription, 20_000);
    let plan = deletion(consumer(
        &mut state,
        ConsumerCommand::Delete {
            subscription,
            model: SubscriptionKind::Queue,
            now_ms: 20_001,
        },
    ));
    assert!(!plan.complete);
    assert!(plan.deleted.is_empty());
    assert!(
        state
            .consumer_subscription(subscription)
            .unwrap()
            .is_deleting()
    );
}

#[test]
fn partial_subscription_deletion_blocks_recreation_and_replays_only_the_original_identity() {
    let (mut state, _) = fixture();
    let topic = Id::new_v7().unwrap();
    assert!(
        state
            .apply(Command::create_topic(
                Id::new_v7().unwrap(),
                state.revision(),
                topic,
                "parallel".into(),
                vec![2, 2]
            ))
            .is_applied()
    );
    let subscription = empty_subscription(&mut state, topic, "jobs", SubscriptionKind::Queue);
    let command = Command::consumer(
        Id::new_v7().unwrap(),
        state.revision(),
        ConsumerCommand::Delete {
            subscription,
            model: SubscriptionKind::Queue,
            now_ms: 10,
        },
    );
    let original = state.apply(command.clone());
    let plan = deletion(original.clone());
    rejected(
        state.apply(Command::consumer(
            command.operation_id(),
            state.revision(),
            ConsumerCommand::Delete {
                subscription,
                model: SubscriptionKind::Stream,
                now_ms: 10,
            },
        )),
        Rejection::OperationConflict,
    );
    assert_eq!(plan.domains.len(), 2);
    let first = *plan.domains.first().unwrap();
    let last = *plan.domains.last().unwrap();
    let partial = deletion(consumer(
        &mut state,
        ConsumerCommand::ObserveDeleted {
            subscription,
            deletion: plan.id,
            domain: first,
        },
    ));
    assert_eq!(partial.deleted.len(), 1);
    rejected(
        consumer(
            &mut state,
            ConsumerCommand::FinishDelete {
                subscription,
                deletion: plan.id,
            },
        ),
        Rejection::RecoveryRequired,
    );
    rejected(
        consumer(
            &mut state,
            ConsumerCommand::Create {
                topic,
                name: "jobs".into(),
                model: SubscriptionKind::Queue,
                initial: InitialPosition::EarliestAvailable,
            },
        ),
        Rejection::IdentityRetired,
    );
    rejected(
        consumer(
            &mut state,
            ConsumerCommand::Join {
                subscription,
                request: MemberJoin {
                    member: Id::new_v7().unwrap(),
                    expected_epoch: 1,
                    client: Id::new_v7().unwrap(),
                    credential: Id::new_v7().unwrap(),
                    now_ms: 11,
                    credential_expires_ms: 100_000,
                    max_messages: 8,
                    max_bytes: 4096,
                    digest: [8; 32],
                },
            },
        ),
        Rejection::IdentityRetired,
    );
    let layout = state.topology().layout(topic).unwrap();
    let source = layout.shards.first().unwrap();
    let split = Command::prepare_layout(
        Id::new_v7().unwrap(),
        state.revision(),
        topic,
        layout.revision,
        crate::metadata::LayoutChange::Split {
            source: source.id,
            at: 1 << 62,
            groups: [2, 2],
        },
    );
    rejected(state.apply(split), Rejection::RecoveryRequired);
    let mut state: MetadataState =
        postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    state.validate_snapshot().unwrap();
    assert_eq!(state.apply(command.clone()), original);
    rejected(
        consumer(
            &mut state,
            ConsumerCommand::ObserveDeleted {
                subscription,
                deletion: Id::new_v7().unwrap(),
                domain: last,
            },
        ),
        Rejection::InvalidEvidence,
    );
    let installed = deletion(consumer(
        &mut state,
        ConsumerCommand::ObserveDeleted {
            subscription,
            deletion: plan.id,
            domain: last,
        },
    ));
    assert!(!installed.complete);
    let done = deletion(consumer(
        &mut state,
        ConsumerCommand::FinishDelete {
            subscription,
            deletion: plan.id,
        },
    ));
    assert!(done.complete);
    assert!(state.consumer_subscription_by_name(topic, "jobs").is_none());
    let replacement = empty_subscription(&mut state, topic, "jobs", SubscriptionKind::Stream);
    assert_ne!(replacement, subscription);
    assert_eq!(state.apply(command), original);
    assert_eq!(
        deletion(consumer(
            &mut state,
            ConsumerCommand::Delete {
                subscription,
                model: SubscriptionKind::Queue,
                now_ms: 20
            }
        ))
        .id,
        plan.id
    );
    assert_eq!(
        state
            .consumer_subscription_by_name(topic, "jobs")
            .unwrap()
            .id,
        replacement
    );
    assert!(
        state
            .consumer_subscription(subscription)
            .unwrap()
            .is_deleted()
    );
    state.validate_snapshot().unwrap();
}
