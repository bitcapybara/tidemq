use super::*;
use crate::lifecycle::{CatalogCommand, MetadataOperation, PinRecord, Plan};
use crate::metadata::{
    ConsumerCommand, ImportEvidence, Outcome, TransferCheckpoint, TransferCommand,
    TransferManifest, TransferStage, TransferStep,
};
use std::collections::BTreeMap;
use tidemq_protocol::topic::{InitialPosition, SubscriptionKind};

fn catalog(state: &mut MetadataState, command: CatalogCommand) {
    let response = state.apply(Command::new(
        Id::new_v7().unwrap(),
        state.revision(),
        Action::Lifecycle(MetadataOperation { command }),
    ));
    assert!(
        response.is_applied(),
        "Catalog operation was rejected: {response:?}"
    );
    state.validate_snapshot().unwrap();
}

fn archived_history(state: &mut MetadataState, partition: PartitionKey) -> PinRecord {
    use crate::archive::{CommittedPartition, Pack, PackIdentity, PackLimits, Record};
    use bytes::Bytes;
    use tidemq_protocol::state::OffsetRange;
    let identity = PackIdentity {
        cluster_id: state.cluster_id().unwrap(),
        group_id: 2,
        object_id: id(710),
    };
    let record = Record {
        offset: 0,
        context: Bytes::new(),
        key: Bytes::new(),
        body: Bytes::from_static(b"history"),
    };
    let pack = Pack::build(
        identity,
        &[CommittedPartition {
            group_id: 2,
            topic_id: partition.topic_id,
            partition_id: partition.partition,
            offsets: OffsetRange::new(0, 1).unwrap(),
            committed_end: 1,
            records: &[record],
        }],
        PackLimits::default(),
        &crate::budget::MemoryBudget::new(&crate::config::MemoryConfig::default()).unwrap(),
    )
    .unwrap();
    let object_id = pack.descriptor().identity.object_id;
    let grant = state.grant(partition).unwrap().clone();
    catalog(
        state,
        CatalogCommand::Register(Plan {
            descriptor: pack.descriptor().clone(),
            grants: vec![grant],
            expected_generations: BTreeMap::from([(partition, 0)]),
            replaces: vec![],
        }),
    );
    catalog(state, CatalogCommand::Uploaded { object_id });
    catalog(state, CatalogCommand::Publish { object_id });
    let pin = PinRecord {
        id: id(711),
        group_id: 2,
        partitions: vec![partition],
        objects: vec![object_id],
        ttl_millis: 1000,
    };
    catalog(state, CatalogCommand::Pin(pin.clone()));
    pin
}

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
    let source_pin = archived_history(&mut state, partition);
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
    catalog(&mut state, CatalogCommand::Pin(source_pin.clone()));
    let late = PinRecord {
        id: id(714),
        ..source_pin.clone()
    };
    rejected(
        state.apply(Command::new(
            Id::new_v7().unwrap(),
            state.revision(),
            Action::Lifecycle(MetadataOperation {
                command: CatalogCommand::Pin(late),
            }),
        )),
        Rejection::Lifecycle(crate::lifecycle::Rejection::StaleGeneration),
    );
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
    let target_pin = PinRecord {
        id: id(712),
        group_id: 3,
        ..source_pin.clone()
    };
    catalog(&mut state, CatalogCommand::Pin(target_pin));
    rejected(
        state.apply(Command::new(
            Id::new_v7().unwrap(),
            state.revision(),
            Action::Lifecycle(MetadataOperation {
                command: CatalogCommand::Checkpoint {
                    object_id: id(710),
                    generation: 1,
                    voters: vec![1],
                    snapshots: vec![(1, "wrong-authority-snapshot".into(), 10)],
                },
            }),
        )),
        Rejection::PlacementViolation,
    );
    let pins = state
        .lifecycle()
        .related_pins_page(3, None, 64, |partition| state.partition_group(partition))
        .unwrap();
    assert_eq!(pins.pins, vec![source_pin.id, id(712)]);
    let objects = state
        .lifecycle()
        .related_objects_page(3, None, 64, |partition| state.partition_group(partition))
        .unwrap();
    assert_eq!(
        objects.objects,
        vec![(id(710), crate::lifecycle::ObjectPhase::Published)]
    );
    catalog(&mut state, CatalogCommand::Pin(source_pin.clone()));
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
    rejected(
        state.apply(Command::transfer(
            id(713),
            state.revision(),
            TransferCommand::Advance {
                plan: plan_id,
                step: Box::new(TransferStep::Retired(CommitPosition {
                    index: 1001,
                    ..source_cut
                })),
            },
        )),
        Rejection::RecoveryRequired,
    );
    catalog(&mut state, CatalogCommand::ReleasePin { pin: source_pin });
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
