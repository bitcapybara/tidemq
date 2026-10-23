use std::collections::{BTreeMap, BTreeSet};
use tidemq::metadata::{
    AuthorityTransfer, Command, HistoricalBinding, ImportEvidence, PartitionKey, Rejection,
    TransferCheckpoint, TransferManifest, TransferStage, TransferStep,
};
use tidemq::ownership::{
    CommitPosition, FenceRecord, Grant, RecoveredOwnership, RecoveryCheckpoint, RecoveryEvidence,
};
use tidemq_protocol::Id;

fn id() -> Id {
    Id::new_v7().unwrap()
}
fn position(index: u64, leader_node: u64) -> CommitPosition {
    CommitPosition {
        term: 1,
        leader_node,
        index,
    }
}
fn plan() -> AuthorityTransfer {
    let cluster = id();
    let partition = PartitionKey {
        topic_id: id(),
        partition: 0,
    };
    let grant = |group_id, epoch, owner| Grant {
        cluster_id: cluster,
        partition,
        group_id,
        operation_id: Command::grant_ownership(1, cluster, partition, owner, epoch - 1, epoch - 1)
            .unwrap()
            .operation_id(),
        epoch,
        owner_node: owner,
        expected_route_generation: epoch - 1,
        metadata_revision: epoch,
    };
    AuthorityTransfer {
        id: id(),
        shard: id(),
        partition,
        layout_revision: 1,
        binding_generation: 1,
        source: grant(2, 1, 1),
        target: grant(3, 2, 2),
        source_voters: [1].into(),
        target_voters: [2].into(),
        stage: TransferStage::Prepare,
        frozen_cut: None,
        manifest: None,
        imported: None,
        checkpoints: BTreeMap::new(),
        retired_cut: None,
    }
}
fn export(plan: &mut AuthorityTransfer) -> TransferManifest {
    let cut = position(1_000, 1);
    plan.advance(TransferStep::Frozen(cut)).unwrap();
    let manifest = TransferManifest {
        bytes: 1024,
        digest: [7; 32],
        source_cut: cut,
        committed_end: 9,
    };
    plan.advance(TransferStep::Exported(manifest.clone()))
        .unwrap();
    manifest
}
fn imported(plan: &AuthorityTransfer, manifest: TransferManifest) -> ImportEvidence {
    ImportEvidence {
        manifest,
        recovery: RecoveryEvidence {
            recovered: RecoveredOwnership {
                fence: FenceRecord {
                    grant: plan.target.clone(),
                    fence_generation: 1,
                },
                checkpoint: RecoveryCheckpoint {
                    fence_generation: 1,
                    producer_state_revision: 1,
                    ack_state_revision: 1,
                    committed_end: 9,
                    state_digest: [8; 32],
                },
            },
            committed: position(5, 2),
        },
    }
}

#[test]
fn source_and_target_log_positions_remain_independent_and_retries_keep_the_original_cut() {
    let mut plan = plan();
    plan.validate().unwrap();
    assert!(plan.advance(TransferStep::Published).is_err());
    let manifest = export(&mut plan);
    assert_eq!(
        plan.advance(TransferStep::Frozen(position(1_001, 1))),
        Err(Rejection::OperationConflict)
    );
    let evidence = imported(&plan, manifest.clone());
    plan.advance(TransferStep::Imported(evidence.clone()))
        .unwrap();
    plan.advance(TransferStep::Imported(evidence)).unwrap();
    plan.advance(TransferStep::Published).unwrap();
    let checkpoint = TransferCheckpoint {
        voter: 2,
        position: position(6, 2),
        manifest_digest: manifest.digest,
    };
    plan.advance(TransferStep::Checkpointed(vec![checkpoint]))
        .unwrap();
    plan.advance(TransferStep::Retired(position(1_001, 1)))
        .unwrap();
    plan.advance(TransferStep::Exported(manifest)).unwrap();
    plan.validate().unwrap();
    let restored: AuthorityTransfer =
        postcard::from_bytes(&postcard::to_stdvec(&plan).unwrap()).unwrap();
    restored.validate().unwrap();
    assert_eq!(restored, plan);
}

#[test]
fn import_checks_the_frozen_digest_destination_fence_and_all_required_checkpoints() {
    let mut plan = plan();
    plan.source_voters = [1, 5, 6].into();
    plan.target_voters = [2, 3, 4].into();
    let manifest = export(&mut plan);
    let mut forged = imported(&plan, manifest.clone());
    forged.recovery.recovered.fence.grant.group_id = 2;
    assert!(plan.advance(TransferStep::Imported(forged)).is_err());
    let mut forged = imported(&plan, manifest.clone());
    forged.manifest.digest = [9; 32];
    assert!(plan.advance(TransferStep::Imported(forged)).is_err());
    plan.advance(TransferStep::Imported(imported(&plan, manifest.clone())))
        .unwrap();
    plan.advance(TransferStep::Published).unwrap();
    let checkpoint = |voter| TransferCheckpoint {
        voter,
        position: position(6, 2),
        manifest_digest: manifest.digest,
    };
    assert!(
        plan.advance(TransferStep::Checkpointed(vec![
            checkpoint(2),
            checkpoint(3)
        ]))
        .is_err()
    );
    assert!(
        plan.advance(TransferStep::Checkpointed(vec![
            checkpoint(2),
            checkpoint(3),
            checkpoint(3)
        ]))
        .is_err()
    );
    assert!(
        plan.advance(TransferStep::Retired(position(1_001, 1)))
            .is_err()
    );
    plan.advance(TransferStep::Checkpointed(vec![
        checkpoint(2),
        checkpoint(3),
        checkpoint(4),
    ]))
    .unwrap();
    plan.validate().unwrap();
}

#[test]
fn historical_binding_preserves_every_original_object_group_across_authority_changes() {
    let mut binding = HistoricalBinding {
        shard: id(),
        partition: PartitionKey {
            topic_id: id(),
            partition: 7,
        },
        origin_group: 2,
        authority_group: 2,
        generation: 1,
        object_groups: BTreeSet::from([2]),
    };
    binding.move_to(1, 3).unwrap();
    binding.move_to(2, 4).unwrap();
    assert!(binding.accepts_object(2) && binding.accepts_object(3) && binding.accepts_object(4));
    assert_eq!(binding.origin_group, 2);
    assert_eq!(binding.authority_group, 4);
    assert_eq!(binding.move_to(1, 5), Err(Rejection::GenerationConflict));
}
