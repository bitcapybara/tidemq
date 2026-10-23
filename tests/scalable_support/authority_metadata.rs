use super::*;
use crate::metadata::{
    ConsumerCommand, ImportEvidence, Outcome, TransferCheckpoint, TransferCommand,
    TransferManifest, TransferStage, TransferStep,
};
use tidemq_protocol::topic::{InitialPosition, SubscriptionKind};

fn advance(
    state: &mut MetadataState,
    plan: Id,
    step: TransferStep,
) -> crate::metadata::AuthorityTransfer {
    let response = state.apply(Command::transfer(
        Id::new_v7().unwrap(),
        state.revision(),
        TransferCommand::Advance {
            plan,
            step: Box::new(step),
        },
    ));
    state.validate_snapshot().unwrap();
    let Response::Applied {
        outcome: Outcome::AuthorityTransfer(plan),
        ..
    } = response
    else {
        panic!("Expected a durable authority transfer step")
    };
    *plan
}

#[test]
fn cross_group_publication_installs_only_target_control_proofs_and_preserves_birth_identity() {
    let (mut state, partition) = fixture();
    let (fence, recovery) = evidence(&state, partition);
    assert!(apply(&mut state, 0, Action::ObserveFence(fence)).is_applied());
    assert!(apply(&mut state, 0, Action::ObserveRecovery(recovery.clone())).is_applied());
    assert!(apply(&mut state, 0, Action::ActivateRoute(recovery)).is_applied());
    assert!(
        state
            .apply(Command::put_node(
                id(700),
                state.revision(),
                NodeSpec {
                    node_id: 2,
                    zone: "target".into(),
                    endpoint: "127.0.0.1:7453".parse().unwrap(),
                    roles: [NodeRole::Storage, NodeRole::Broker].into(),
                    schedulable: true,
                },
                0
            ))
            .is_applied()
    );
    assert!(
        state
            .apply(Command::create_group(
                id(701),
                state.revision(),
                3,
                [2].into()
            ))
            .is_applied()
    );
    let shard = state
        .topology
        .domains(partition.topic_id)
        .next()
        .unwrap()
        .route
        .id;
    let plan_id = id(702);
    let request = TransferCommand::Prepare {
        shard,
        layout_revision: 1,
        target_group: 3,
        target_owner: 2,
    };
    assert!(
        state
            .apply(Command::transfer(
                plan_id,
                state.revision(),
                request.clone()
            ))
            .is_applied()
    );
    state.validate_snapshot().unwrap();
    let original = state.topology.transfer(plan_id).unwrap().clone();
    assert_eq!(original.source.group_id, 2);
    assert_eq!(original.target.group_id, 3);
    let grant = Command::grant_ownership(
        state.revision(),
        state.cluster_id().unwrap(),
        partition,
        2,
        1,
        1,
    )
    .unwrap();
    rejected(state.apply(grant), Rejection::RecoveryRequired);
    rejected(
        state.apply(Command::consumer(
            id(703),
            state.revision(),
            ConsumerCommand::Create {
                topic: partition.topic_id,
                name: "blocked".into(),
                model: SubscriptionKind::Queue,
                initial: InitialPosition::EarliestAvailable,
            },
        )),
        Rejection::RecoveryRequired,
    );
    state.validate_snapshot().unwrap();
    let source_cut = CommitPosition {
        term: 1,
        leader_node: 1,
        index: 1000,
    };
    advance(&mut state, plan_id, TransferStep::Frozen(source_cut));
    let bytes = postcard::to_stdvec(&state).unwrap();
    let mut state: MetadataState = postcard::from_bytes(&bytes).unwrap();
    state.validate_snapshot().unwrap();
    let manifest = TransferManifest {
        bytes: 2048,
        digest: [9; 32],
        source_cut,
        committed_end: 9,
    };
    advance(
        &mut state,
        plan_id,
        TransferStep::Exported(manifest.clone()),
    );
    let committed = CommitPosition {
        term: 1,
        leader_node: 2,
        index: 5,
    };
    let recovery = RecoveryEvidence {
        recovered: RecoveredOwnership {
            fence: FenceRecord {
                grant: original.target.clone(),
                fence_generation: 1,
            },
            checkpoint: RecoveryCheckpoint {
                fence_generation: 1,
                producer_state_revision: 1,
                ack_state_revision: 1,
                committed_end: 9,
                state_digest: [3; 32],
            },
        },
        committed,
    };
    advance(
        &mut state,
        plan_id,
        TransferStep::Imported(ImportEvidence {
            manifest: manifest.clone(),
            recovery,
        }),
    );
    advance(&mut state, plan_id, TransferStep::Published);
    assert_eq!(state.route(partition).unwrap().group_id, 3);
    assert_eq!(state.route(partition).unwrap().generation, 2);
    let binding = state.topology.binding(partition).unwrap();
    assert_eq!(binding.shard, shard);
    assert_eq!(binding.origin_group, 2);
    assert_eq!(binding.authority_group, 3);
    assert_eq!(binding.generation, 2);
    assert!(binding.accepts_object(2) && binding.accepts_object(3));
    assert_eq!(
        state.topology.layout(partition.topic_id).unwrap().revision,
        2
    );
    assert_eq!(state.topology.shard(shard).unwrap().route.group, 3);
    assert!(
        state
            .apply(Command::transfer(plan_id, state.revision(), request))
            .is_applied()
    );
    assert!(
        state
            .apply(Command::put_node(
                id(704),
                state.revision(),
                NodeSpec {
                    node_id: 3,
                    zone: "replacement".into(),
                    endpoint: "127.0.0.1:7463".parse().unwrap(),
                    roles: [NodeRole::Storage].into(),
                    schedulable: true,
                },
                0
            ))
            .is_applied()
    );
    // Membership tests cover the Raft replacement; model its completed metadata view here.
    let target = state.groups.get_mut(&3).unwrap();
    target.voters = [3].into();
    target.generation += 1;
    state.validate_snapshot().unwrap();
    let checkpoint = TransferCheckpoint {
        voter: 3,
        position: CommitPosition {
            index: 6,
            ..committed
        },
        manifest_digest: manifest.digest,
    };
    advance(
        &mut state,
        plan_id,
        TransferStep::Checkpointed(vec![checkpoint]),
    );
    assert_eq!(
        state.topology.transfer(plan_id).unwrap().target_voters,
        BTreeSet::from([2])
    );
    assert_eq!(
        state.topology.transfer(plan_id).unwrap().checkpoint_voters,
        BTreeSet::from([3])
    );
    let retired = advance(
        &mut state,
        plan_id,
        TransferStep::Retired(CommitPosition {
            index: 1001,
            ..source_cut
        }),
    );
    assert_eq!(retired.stage, TransferStage::Retired);
    let restored: MetadataState =
        postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    restored.validate_snapshot().unwrap();
    assert_eq!(restored.route(partition).unwrap().group_id, 3);
    let mut corrupted = restored;
    corrupted
        .topology
        .bindings
        .get_mut(&partition)
        .unwrap()
        .authority_group = 2;
    corrupted
        .topology
        .shards
        .get_mut(&shard)
        .unwrap()
        .route
        .group = 2;
    corrupted
        .topology
        .layouts
        .get_mut(&partition.topic_id)
        .unwrap()
        .shards
        .first_mut()
        .unwrap()
        .group = 2;
    assert_eq!(
        corrupted.validate_snapshot(),
        Err(Rejection::InvalidEvidence)
    );
}
