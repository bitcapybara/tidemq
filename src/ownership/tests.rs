use tidemq_protocol::Id;

use crate::metadata::PartitionKey;

use super::{FenceCommand, FenceRejection, FenceResponse, FenceState, Grant, RecoveryCheckpoint};

pub(super) fn grant(epoch: u64) -> Grant {
    let cluster_id: Id = "00000000-0000-4000-8000-000000000001".parse().unwrap();
    let topic_id: Id = "00000000-0000-4000-8000-000000000002".parse().unwrap();
    let operation_id = crate::metadata::grant_operation_id(
        cluster_id,
        PartitionKey {
            topic_id,
            partition: 0,
        },
        epoch,
        epoch,
        epoch - 1,
    )
    .unwrap();
    Grant {
        cluster_id,
        partition: PartitionKey {
            topic_id,
            partition: 0,
        },
        group_id: 2,
        operation_id,
        epoch,
        owner_node: epoch,
        expected_route_generation: epoch - 1,
        metadata_revision: epoch,
    }
}

pub(super) fn checkpoint(generation: u64) -> RecoveryCheckpoint {
    RecoveryCheckpoint {
        fence_generation: generation,
        producer_state_revision: 7,
        ack_state_revision: 9,
        committed_end: 42,
        state_digest: [7; 32],
    }
}

#[test]
fn committed_fence_replay_preserves_recovery_and_old_writers_stop_after_new_fence() {
    let first = grant(1);
    let mut state = FenceState::new(first.cluster_id, 2, 2).unwrap();
    let command = FenceCommand {
        grant: first.clone(),
    };
    let response = state.apply(command.clone());
    assert!(matches!(response, FenceResponse::Installed(_)));
    assert_eq!(
        state.check_writer(first.partition, 1, 1),
        Err(FenceRejection::RecoveryRequired)
    );
    let recovered = state
        .complete_recovery(first.partition, checkpoint(1))
        .unwrap();
    assert!(state.check_writer(first.partition, 1, 1).is_ok());
    assert_eq!(state.apply(command), response);
    assert_eq!(state.recovered(first.partition), Some(recovered));
    let second = grant(2);
    assert!(matches!(
        state.apply(FenceCommand {
            grant: second.clone()
        }),
        FenceResponse::Installed(_)
    ));
    assert_eq!(
        state.check_writer(first.partition, 1, 1),
        Err(FenceRejection::StaleEpoch)
    );
    assert_eq!(
        state.check_writer(first.partition, 2, 2),
        Err(FenceRejection::RecoveryRequired)
    );
    assert_eq!(
        state.apply(FenceCommand { grant: first }),
        FenceResponse::Rejected(FenceRejection::StaleEpoch)
    );
    state
        .complete_recovery(second.partition, checkpoint(2))
        .unwrap();
    let bytes = serde_json::to_vec(&state).unwrap();
    let restored: FenceState = serde_json::from_slice(&bytes).unwrap();
    restored.validate_snapshot().unwrap();
    assert_eq!(restored, state);
    assert!(restored.check_writer(second.partition, 2, 2).is_ok());
}

#[test]
fn fence_identity_conflicts_and_snapshot_capacity_cannot_bypass_checks() {
    let first = grant(1);
    let mut state = FenceState::new(first.cluster_id, 2, 1).unwrap();
    assert!(matches!(
        state.apply(FenceCommand {
            grant: first.clone()
        }),
        FenceResponse::Installed(_)
    ));
    let mut conflict = first.clone();
    conflict.owner_node = 999;
    conflict.operation_id = crate::metadata::grant_operation_id(
        conflict.cluster_id,
        conflict.partition,
        conflict.epoch,
        conflict.owner_node,
        conflict.expected_route_generation,
    )
    .unwrap();
    assert_eq!(
        state.apply(FenceCommand { grant: conflict }),
        FenceResponse::Rejected(FenceRejection::EpochConflict)
    );
    let mut foreign = grant(2);
    foreign.group_id = 3;
    assert_eq!(
        state.apply(FenceCommand { grant: foreign }),
        FenceResponse::Rejected(FenceRejection::InvalidIdentity)
    );
    let mut another = grant(2);
    another.partition.partition = 1;
    another.operation_id = crate::metadata::grant_operation_id(
        another.cluster_id,
        another.partition,
        another.epoch,
        another.owner_node,
        another.expected_route_generation,
    )
    .unwrap();
    assert_eq!(
        state.apply(FenceCommand { grant: another }),
        FenceResponse::Rejected(FenceRejection::Capacity)
    );
    assert_eq!(
        state.complete_recovery(first.partition, checkpoint(2)),
        Err(FenceRejection::InvalidRecovery)
    );
    state.validate_snapshot().unwrap();
}

#[test]
fn execution_positions_round_trip_without_ordering_across_kinds() {
    use super::CommitPosition;
    let local = CommitPosition::Local { sequence: 9 };
    let raft = CommitPosition::Raft {
        term: 2,
        leader_node: 1,
        index: 9,
    };
    for position in [local, raft] {
        assert_eq!(
            postcard::from_bytes::<CommitPosition>(&postcard::to_stdvec(&position).unwrap())
                .unwrap(),
            position
        );
        assert_eq!(
            serde_json::from_value::<CommitPosition>(serde_json::to_value(position).unwrap())
                .unwrap(),
            position
        );
    }
    assert!(!local.at_least(raft));
    assert!(!raft.at_least(local));
    assert!(!CommitPosition::Local { sequence: 0 }.validate());
}
