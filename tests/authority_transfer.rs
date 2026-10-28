use std::collections::{BTreeMap, BTreeSet};
use tidemq::metadata::{
    AuthorityTransfer, Command, HistoricalBinding, ImportEvidence, PartitionKey, Rejection,
    TransferCheckpoint, TransferManifest, TransferStage, TransferStep,
};
use tidemq::ownership::{
    CommitPosition, FenceRecord, Grant, RecoveredOwnership, RecoveryCheckpoint, RecoveryEvidence,
};
use tidemq_protocol::Id;

#[path = "replication_support/quic.rs"]
mod quic;
#[path = "replication_support/mod.rs"]
mod support;

fn id() -> Id {
    Id::new_v7().unwrap()
}
fn position(index: u64, leader_node: u64) -> CommitPosition {
    CommitPosition::Raft {
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
        checkpoint_voters: [2].into(),
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
            placement: tidemq::metadata::GroupPlacement::Raft {
                voters: plan.target_voters.clone(),
            },
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
    plan.checkpoint_voters = [2, 3, 4].into();
    let manifest = export(&mut plan);
    let mut forged = imported(&plan, manifest.clone());
    forged.recovery.recovered.fence.grant.group_id = 2;
    assert!(plan.advance(TransferStep::Imported(forged)).is_err());
    let mut forged = imported(&plan, manifest.clone());
    forged.manifest.digest = [9; 32];
    assert!(plan.advance(TransferStep::Imported(forged)).is_err());
    let mut forged = imported(&plan, manifest.clone());
    forged.recovery.committed = CommitPosition::Local { sequence: 5 };
    assert_eq!(
        plan.advance(TransferStep::Imported(forged)),
        Err(Rejection::InvalidEvidence)
    );
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
fn checkpoints_can_cover_a_replacement_voter_without_rewriting_the_original_import_proof() {
    let mut plan = plan();
    let manifest = export(&mut plan);
    let evidence = imported(&plan, manifest.clone());
    plan.advance(TransferStep::Imported(evidence.clone()))
        .unwrap();
    plan.advance(TransferStep::Published).unwrap();
    plan.checkpoint_voters = [3].into();
    assert!(
        plan.advance(TransferStep::Checkpointed(vec![TransferCheckpoint {
            voter: 2,
            position: position(6, 2),
            manifest_digest: manifest.digest,
        }]))
        .is_err()
    );
    for position in [
        CommitPosition::Local { sequence: 6 },
        CommitPosition::Raft {
            term: 0,
            leader_node: 3,
            index: 6,
        },
        CommitPosition::Raft {
            term: 2,
            leader_node: 0,
            index: 6,
        },
        CommitPosition::Raft {
            term: 2,
            leader_node: 3,
            index: 4,
        },
    ] {
        assert_eq!(
            plan.advance(TransferStep::Checkpointed(vec![TransferCheckpoint {
                voter: 3,
                position,
                manifest_digest: manifest.digest,
            }])),
            Err(Rejection::InvalidEvidence)
        );
    }
    let mut rewritten = plan.clone();
    rewritten.imported.as_mut().unwrap().recovery.committed = position(5, 3);
    assert_eq!(rewritten.validate(), Err(Rejection::InvalidEvidence));
    let mut rewritten = plan.clone();
    rewritten.imported.as_mut().unwrap().recovery.placement =
        tidemq::metadata::GroupPlacement::Raft { voters: [3].into() };
    assert_eq!(rewritten.validate(), Err(Rejection::InvalidEvidence));
    plan.advance(TransferStep::Checkpointed(vec![TransferCheckpoint {
        voter: 3,
        position: position(6, 2),
        manifest_digest: manifest.digest,
    }]))
    .unwrap();
    assert_eq!(plan.imported, Some(evidence));
    plan.validate().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_rf1_leaders_real_snapshot_can_checkpoint_the_original_import() {
    use openraft::BasicNode;
    use tidemq::execution::GroupHandle;
    use tidemq::replication::rpc::GroupRole;
    use tokio::time::Instant;

    let mut plan = plan();
    let manifest = export(&mut plan);
    let cluster = quic::Cluster::<support::State>::with_identity(
        &[2, 3],
        plan.target.cluster_id,
        plan.target.group_id,
        GroupRole::Data,
    )
    .await;
    cluster
        .node(2)
        .raft
        .initialize(BTreeMap::from([(2, BasicNode::new("configured-node-2"))]))
        .await
        .unwrap();
    cluster
        .node(2)
        .raft
        .wait(Some(quic::WAIT))
        .current_leader(2, "The original RF1 voter becomes leader")
        .await
        .unwrap();
    let original = GroupHandle::from_raft(
        cluster.node(2).raft.clone(),
        cluster.node(2).state.clone(),
        cluster.node(2).log.clone(),
    );
    let committed = original
        .commit(
            support::Command::Initialize {
                cluster: plan.target.cluster_id,
                group: plan.target.group_id,
            },
            Instant::now() + quic::WAIT,
        )
        .await
        .unwrap();
    let mut evidence = imported(&plan, manifest.clone());
    evidence.recovery.committed = committed.position.into();
    assert!(matches!(
        evidence.recovery.committed,
        CommitPosition::Raft { leader_node: 2, .. }
    ));
    plan.advance(TransferStep::Imported(evidence.clone()))
        .unwrap();
    plan.advance(TransferStep::Published).unwrap();

    cluster
        .node(2)
        .raft
        .add_learner(3, BasicNode::new("configured-node-3"), true)
        .await
        .unwrap();
    let membership = cluster
        .node(2)
        .raft
        .change_membership(BTreeSet::from([3]), false)
        .await
        .unwrap();
    cluster.applied(&[3], membership.log_id.index).await;
    cluster
        .node(3)
        .raft
        .wait(Some(quic::WAIT))
        .current_leader(3, "The replacement RF1 voter becomes leader")
        .await
        .unwrap();
    let replacement = GroupHandle::from_raft(
        cluster.node(3).raft.clone(),
        cluster.node(3).state.clone(),
        cluster.node(3).log.clone(),
    );
    replacement
        .ensure_read(Instant::now() + quic::WAIT)
        .await
        .unwrap();
    replacement
        .commit(support::Command::Fence(1), Instant::now() + quic::WAIT)
        .await
        .unwrap();
    let snapshot = replacement
        .snapshot(Instant::now() + quic::WAIT)
        .await
        .unwrap()
        .unwrap();
    let checkpoint = TransferCheckpoint {
        voter: 3,
        position: snapshot.position.into(),
        manifest_digest: manifest.digest,
    };
    assert!(matches!(
        checkpoint.position,
        CommitPosition::Raft { leader_node: 3, .. }
    ));
    assert_eq!(
        cluster
            .node(3)
            .raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([3])
    );
    println!(
        "Historical import: {:?}; replacement snapshot: {:?}",
        evidence.recovery.committed, checkpoint.position
    );
    plan.checkpoint_voters = [3].into();
    let result = plan.advance(TransferStep::Checkpointed(vec![checkpoint]));
    drop((original, replacement));
    cluster.shutdown().await;

    assert_eq!(
        result,
        Ok(()),
        "The current RF1 snapshot must cover the historical import"
    );
    assert_eq!(plan.stage, TransferStage::Checkpointed);
    assert_eq!(plan.imported, Some(evidence));
    assert_eq!(plan.target_voters, BTreeSet::from([2]));
    plan.advance(TransferStep::Retired(position(1_001, 1)))
        .unwrap();
    plan.validate().unwrap();
    let restored: AuthorityTransfer =
        postcard::from_bytes(&postcard::to_stdvec(&plan).unwrap()).unwrap();
    restored.validate().unwrap();
    assert_eq!(restored, plan);
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
