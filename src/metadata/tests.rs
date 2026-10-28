use std::collections::BTreeSet;

use tidemq_protocol::Id;

#[path = "../../tests/scalable_support/consumer_deletion.rs"]
mod consumer_deletion_tests;
#[path = "../../tests/scalable_support/subscription_retention.rs"]
mod subscription_retention_tests;
#[path = "../../tests/scalable_support/tenant_quotas.rs"]
mod tenant_quota_tests;
#[path = "../../tests/scalable_support/topic_deletion.rs"]
mod topic_deletion_tests;
#[path = "../../tests/scalable_support/authority_metadata.rs"]
mod transfer_tests;

use crate::ownership::{
    CommitPosition, FenceEvidence, FenceRecord, OwnershipPhase, RecoveredOwnership,
    RecoveryCheckpoint, RecoveryEvidence,
};

use super::command::Action;
use super::{
    Command, DeploymentMode, MetadataLimits, MetadataState, NodeRole, NodeSpec, PartitionKey,
    Rejection, Response,
};

fn id(value: u64) -> Id {
    format!("00000000-0000-4000-8000-{value:012x}")
        .parse()
        .unwrap()
}

fn fixture() -> (MetadataState, PartitionKey) {
    fixture_with_mode(DeploymentMode::SeparateRf1)
}

fn fixture_with_mode(mode: DeploymentMode) -> (MetadataState, PartitionKey) {
    let mut state = MetadataState::default();
    assert!(
        state
            .apply(Command::bootstrap(
                id(1),
                id(2),
                1,
                mode,
                MetadataLimits::default()
            ))
            .is_applied()
    );
    let spec = NodeSpec {
        node_id: 1,
        zone: "local".into(),
        endpoint: "127.0.0.1:7443".parse().unwrap(),
        roles: BTreeSet::from([NodeRole::Storage, NodeRole::Broker]),
        schedulable: true,
    };
    assert!(
        state
            .apply(Command::put_node(id(3), state.revision(), spec, 0))
            .is_applied()
    );
    assert!(
        state
            .apply(Command::create_group(
                id(4),
                state.revision(),
                2,
                if mode == DeploymentMode::Local {
                    super::GroupPlacement::Local { node_id: 1 }
                } else {
                    super::GroupPlacement::Raft { voters: [1].into() }
                }
            ))
            .is_applied()
    );
    assert!(
        state
            .apply(Command::create_topic(
                id(5),
                state.revision(),
                id(6),
                "orders".into(),
                vec![2]
            ))
            .is_applied()
    );
    let partition = PartitionKey {
        topic_id: id(6),
        partition: 0,
    };
    assert!(
        state
            .apply(Command::grant_ownership(state.revision(), id(2), partition, 1, 0, 0).unwrap())
            .is_applied()
    );
    (state, partition)
}

fn evidence(state: &MetadataState, partition: PartitionKey) -> (FenceEvidence, RecoveryEvidence) {
    let fence = FenceRecord {
        grant: state.grant(partition).unwrap().clone(),
        fence_generation: 1,
    };
    let position = if state.mode == Some(DeploymentMode::Local) {
        CommitPosition::Local {
            sequence: 10 * fence.grant.epoch,
        }
    } else {
        CommitPosition::Raft {
            term: 1,
            leader_node: 1,
            index: 10 * fence.grant.epoch,
        }
    };
    let checkpoint = RecoveryCheckpoint {
        fence_generation: 1,
        producer_state_revision: 10,
        ack_state_revision: 10,
        committed_end: 0,
        state_digest: [1; 32],
    };
    (
        FenceEvidence {
            placement: state.group(fence.grant.group_id).unwrap().placement.clone(),
            fence: fence.clone(),
            committed: position,
        },
        RecoveryEvidence {
            placement: state.group(fence.grant.group_id).unwrap().placement.clone(),
            recovered: RecoveredOwnership { fence, checkpoint },
            committed: position,
        },
    )
}

fn apply(state: &mut MetadataState, _operation: u64, action: Action) -> Response {
    let operation = action
        .expected_control_id(state.cluster_id().unwrap())
        .unwrap();
    let response = state.apply(Command::new(operation, state.revision(), action));
    state.validate_snapshot().unwrap();
    response
}

fn rejected(response: Response, reason: Rejection) {
    assert!(matches!(response, Response::Rejected { reason: actual, .. } if actual == reason));
}

#[test]
fn a_stale_metadata_cas_does_not_bind_an_unadmitted_operation_identity() {
    let (mut state, _) = fixture();
    let operation = Id::new_v7().unwrap();
    let topic = Id::new_v7().unwrap();
    let command =
        |revision| Command::create_topic(operation, revision, topic, "cas-retry".into(), vec![2]);
    let stale = command(state.revision());
    assert!(
        state
            .apply(Command::create_topic(
                Id::new_v7().unwrap(),
                state.revision(),
                Id::new_v7().unwrap(),
                "other-cas".into(),
                vec![2]
            ))
            .is_applied()
    );
    rejected(state.apply(stale), Rejection::RevisionConflict);
    assert_eq!(
        state.lookup_operation(operation),
        super::OperationResult::UnknownOrRetired
    );
    let admitted = command(state.revision());
    let outcome = state.apply(admitted.clone());
    assert!(outcome.is_applied());
    let mut state: MetadataState =
        postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    state.validate_snapshot().unwrap();
    assert_eq!(state.apply(admitted), outcome);
    assert_eq!(state.topic(topic).unwrap().name, "cas-retry");
}

#[test]
fn activation_requires_matching_fence_and_rebuilt_state_before_route_publication() {
    let (mut state, partition) = fixture();
    let (fence, recovery) = evidence(&state, partition);
    rejected(
        apply(&mut state, 20, Action::ActivateRoute(recovery.clone())),
        Rejection::RecoveryRequired,
    );
    rejected(
        apply(&mut state, 21, Action::ObserveRecovery(recovery.clone())),
        Rejection::RecoveryRequired,
    );
    let mut forged = fence.clone();
    forged.committed = CommitPosition::Raft {
        term: 1,
        leader_node: 999,
        index: 10,
    };
    rejected(
        apply(&mut state, 22, Action::ObserveFence(forged)),
        Rejection::InvalidEvidence,
    );
    assert!(apply(&mut state, 23, Action::ObserveFence(fence)).is_applied());
    assert_eq!(
        state.diagnose_ownership(partition).unwrap().phase,
        OwnershipPhase::Fenced
    );
    let mut malformed = recovery.clone();
    malformed.recovered.checkpoint.state_digest = [0; 32];
    rejected(
        apply(&mut state, 24, Action::ObserveRecovery(malformed)),
        Rejection::InvalidEvidence,
    );
    rejected(
        apply(&mut state, 25, Action::ActivateRoute(recovery.clone())),
        Rejection::RecoveryRequired,
    );
    assert!(apply(&mut state, 26, Action::ObserveRecovery(recovery.clone())).is_applied());
    assert_eq!(
        state.diagnose_ownership(partition).unwrap().phase,
        OwnershipPhase::Recovered
    );
    assert!(state.route(partition).is_none());
    assert!(apply(&mut state, 27, Action::ActivateRoute(recovery.clone())).is_applied());
    assert_eq!(state.route(partition).unwrap().generation, 1);
    assert!(apply(&mut state, 28, Action::ActivateRoute(recovery)).is_applied());
    assert_eq!(state.route(partition).unwrap().generation, 1);
}

#[test]
fn snapshot_between_fence_and_activation_can_finish_but_superseded_operation_cannot() {
    let (mut state, partition) = fixture();
    let (fence, recovery) = evidence(&state, partition);
    assert!(apply(&mut state, 20, Action::ObserveFence(fence.clone())).is_applied());
    let bytes = serde_json::to_vec(&state).unwrap();
    let mut restored: MetadataState = serde_json::from_slice(&bytes).unwrap();
    restored.validate_snapshot().unwrap();
    assert!(apply(&mut restored, 21, Action::ObserveRecovery(recovery.clone())).is_applied());
    assert!(apply(&mut restored, 22, Action::ActivateRoute(recovery.clone())).is_applied());
    assert!(
        restored
            .apply(
                Command::grant_ownership(restored.revision(), id(2), partition, 1, 1, 1).unwrap()
            )
            .is_applied()
    );
    rejected(
        apply(&mut restored, 31, Action::ObserveFence(fence)),
        Rejection::Superseded,
    );
    rejected(
        apply(&mut restored, 32, Action::ActivateRoute(recovery)),
        Rejection::Superseded,
    );
    assert!(restored.route(partition).is_none());
    assert_eq!(restored.grant(partition).unwrap().epoch, 2);
}

#[test]
fn malformed_snapshot_cannot_create_an_active_route_or_discard_retirement_index() {
    let (mut state, partition) = fixture();
    state.partitions.get_mut(&partition).unwrap().phase = OwnershipPhase::Active;
    assert_eq!(state.validate_snapshot(), Err(Rejection::InvalidEvidence));
    let (mut state, _) = fixture();
    state.topic_names.clear();
    assert!(state.validate_snapshot().is_err());
}

#[test]
fn higher_read_barrier_does_not_change_a_frozen_logical_stage_result() {
    let (mut state, partition) = fixture();
    let (fence, recovery) = evidence(&state, partition);
    let original = apply(&mut state, 20, Action::ObserveFence(fence.clone()));
    let mut advanced = fence;
    advanced.committed = CommitPosition::Raft {
        term: 2,
        leader_node: 1,
        index: 60,
    };
    let before = state.revision();
    assert_eq!(
        apply(&mut state, 21, Action::ObserveFence(advanced.clone())),
        original
    );
    assert_eq!(state.revision(), before);
    advanced.committed = CommitPosition::Raft {
        term: 2,
        leader_node: 1,
        index: 1,
    };
    rejected(
        apply(&mut state, 22, Action::ObserveFence(advanced)),
        Rejection::InvalidEvidence,
    );
    let original = apply(&mut state, 23, Action::ObserveRecovery(recovery.clone()));
    let mut advanced = recovery;
    advanced.committed = CommitPosition::Raft {
        term: 1,
        leader_node: 1,
        index: 100,
    };
    assert_eq!(
        apply(&mut state, 24, Action::ObserveRecovery(advanced.clone())),
        original
    );
    let original = apply(&mut state, 25, Action::ActivateRoute(advanced.clone()));
    advanced.committed = CommitPosition::Raft {
        term: 1,
        leader_node: 1,
        index: 110,
    };
    assert_eq!(
        apply(&mut state, 26, Action::ActivateRoute(advanced)),
        original
    );
}

#[test]
fn full_crud_ledger_reserves_per_partition_capacity_for_repeated_takeover() {
    let (mut state, partition) = fixture();
    state.limits.operations = state.decisions.len();
    let first = state.grant(partition).unwrap().clone();
    let old_command = Command::grant_ownership(4, id(2), partition, 1, 0, 0).unwrap();
    for epoch in 1..=20 {
        if epoch > 1 {
            let command = Command::grant_ownership(
                state.revision(),
                id(2),
                partition,
                1,
                epoch - 1,
                epoch - 1,
            )
            .unwrap();
            assert!(state.apply(command).is_applied());
        }
        let (mut fence, mut recovery) = evidence(&state, partition);
        fence.fence.fence_generation = epoch;
        recovery.recovered.fence.fence_generation = epoch;
        recovery.recovered.checkpoint.fence_generation = epoch;
        assert!(apply(&mut state, 20, Action::ObserveFence(fence)).is_applied());
        assert!(apply(&mut state, 21, Action::ObserveRecovery(recovery.clone())).is_applied());
        assert!(apply(&mut state, 22, Action::ActivateRoute(recovery)).is_applied());
        assert_eq!(state.summary().remaining_operations, 0);
        assert_eq!(state.summary().ownership_result_slots, 4);
        rejected(
            state.apply(Command::retire_topic(
                id(500 + epoch),
                state.revision(),
                partition.topic_id,
                1,
            )),
            Rejection::Capacity,
        );
    }
    rejected(state.apply(old_command.clone()), Rejection::Superseded);
    assert_eq!(
        state.lookup_operation(first.operation_id),
        super::OperationResult::UnknownOrRetired
    );
    let mut forged =
        Command::grant_ownership(state.revision(), id(2), partition, 1, 20, 20).unwrap();
    forged.operation_id = old_command.operation_id;
    rejected(state.apply(forged), Rejection::OperationConflict);
    rejected(
        state.apply(Command::retire_topic(
            old_command.operation_id,
            state.revision(),
            partition.topic_id,
            1,
        )),
        Rejection::InvalidCommand,
    );
    state.validate_snapshot().unwrap();
    let bytes = serde_json::to_vec(&state).unwrap();
    let restored: MetadataState = serde_json::from_slice(&bytes).unwrap();
    restored.validate_snapshot().unwrap();
    assert_eq!(restored, state);
}

#[test]
fn local_ownership_proofs_keep_fencing_recovery_and_retry_positions() {
    let (mut state, partition) = fixture_with_mode(DeploymentMode::Local);
    let (mut fence, mut recovery) = evidence(&state, partition);
    let mut raft_fence = fence.clone();
    raft_fence.placement = super::GroupPlacement::Raft { voters: [1].into() };
    raft_fence.committed = CommitPosition::Raft {
        term: 1,
        leader_node: 1,
        index: 10,
    };
    rejected(
        apply(&mut state, 401, Action::ObserveFence(raft_fence)),
        Rejection::InvalidEvidence,
    );
    fence.placement = super::GroupPlacement::Local { node_id: 1 };
    fence.committed = CommitPosition::Local { sequence: 10 };
    recovery.placement = super::GroupPlacement::Local { node_id: 1 };
    recovery.committed = CommitPosition::Local { sequence: 11 };
    let original = apply(&mut state, 402, Action::ObserveFence(fence.clone()));
    assert!(original.is_applied());
    fence.committed = CommitPosition::Local { sequence: 20 };
    assert_eq!(
        apply(&mut state, 403, Action::ObserveFence(fence.clone())),
        original
    );
    fence.committed = CommitPosition::Local { sequence: 9 };
    rejected(
        apply(&mut state, 404, Action::ObserveFence(fence)),
        Rejection::InvalidEvidence,
    );
    let mut stale = recovery.clone();
    stale.committed = CommitPosition::Local { sequence: 9 };
    rejected(
        apply(&mut state, 405, Action::ObserveRecovery(stale)),
        Rejection::InvalidEvidence,
    );
    assert!(apply(&mut state, 406, Action::ObserveRecovery(recovery.clone())).is_applied());
    assert!(apply(&mut state, 407, Action::ActivateRoute(recovery)).is_applied());
    state.validate_snapshot().unwrap();
    let bytes = serde_json::to_vec(&state).unwrap();
    let restored: MetadataState = serde_json::from_slice(&bytes).unwrap();
    restored.validate_snapshot().unwrap();
    assert_eq!(restored.route(partition).unwrap().epoch, 1);
}

#[test]
fn proof_mode_identity_and_zero_sequence_cannot_be_repaired_by_retries() {
    let (mut state, partition) = fixture();
    let (mut fence, _) = evidence(&state, partition);
    fence.placement = super::GroupPlacement::Local { node_id: 1 };
    fence.committed = CommitPosition::Local { sequence: 10 };
    rejected(
        apply(&mut state, 410, Action::ObserveFence(fence.clone())),
        Rejection::InvalidEvidence,
    );
    let (mut state, partition) = fixture_with_mode(DeploymentMode::Local);
    let (mut fence, _) = evidence(&state, partition);
    fence.committed = CommitPosition::Local { sequence: 0 };
    rejected(
        apply(&mut state, 411, Action::ObserveFence(fence.clone())),
        Rejection::InvalidEvidence,
    );
    fence.committed = CommitPosition::Local { sequence: 10 };
    fence.placement = super::GroupPlacement::Local { node_id: 99 };
    rejected(
        apply(&mut state, 412, Action::ObserveFence(fence.clone())),
        Rejection::InvalidEvidence,
    );
    fence.placement = super::GroupPlacement::Local { node_id: 1 };
    assert!(apply(&mut state, 412, Action::ObserveFence(fence.clone())).is_applied());
    let mut foreign = fence.clone();
    foreign.fence.grant.group_id = 99;
    rejected(
        apply(&mut state, 413, Action::ObserveFence(foreign)),
        Rejection::OperationConflict,
    );
    fence.committed = CommitPosition::Raft {
        term: 1,
        leader_node: 1,
        index: 1000,
    };
    rejected(
        apply(&mut state, 414, Action::ObserveFence(fence)),
        Rejection::InvalidEvidence,
    );
    state.mode = Some(DeploymentMode::SeparateRf1);
    assert!(state.validate_snapshot().is_err());
}

#[test]
fn deployment_modes_have_distinct_current_serializations() {
    for (mode, name, factor) in [
        (DeploymentMode::Local, "local", 1),
        (DeploymentMode::SeparateRf1, "separate_rf1", 1),
        (DeploymentMode::SeparateRf3, "separate_rf3", 3),
    ] {
        assert_eq!(serde_json::to_value(mode).unwrap(), name);
        assert_eq!(mode.replication_factor(), factor);
    }
    for obsolete in ["local_rf1", "three_replica"] {
        assert!(serde_json::from_value::<DeploymentMode>(serde_json::json!(obsolete)).is_err());
    }
}

#[test]
fn local_groups_keep_logical_topics_but_reject_quorum_placement_and_replacement() {
    let (mut state, partition) = fixture_with_mode(DeploymentMode::Local);
    let group = state.group(2).unwrap();
    assert_eq!(group.placement, super::GroupPlacement::Local { node_id: 1 });
    assert!(group.placement.raft_voters().is_none());
    assert!(state.topic(partition.topic_id).is_some());
    assert_eq!(
        crate::scaling::MembershipPlan::new(id(2), group, [2].into()),
        Err(Rejection::Unsupported)
    );
    let command = Command::create_group(
        id(501),
        state.revision(),
        3,
        super::GroupPlacement::Raft { voters: [1].into() },
    );
    rejected(state.apply(command), Rejection::PlacementViolation);
    state.validate_snapshot().unwrap();
}

#[test]
fn local_combined_identity_accepts_real_wildcard_listeners_and_ipv6_origins_only_in_local_mode() {
    let (mut state, _) = fixture_with_mode(DeploymentMode::Local);
    let roles = [
        NodeRole::Gateway,
        NodeRole::Broker,
        NodeRole::Storage,
        NodeRole::Metadata,
    ]
    .into();
    let mut spec = NodeSpec {
        node_id: 1,
        zone: "local".into(),
        endpoint: "0.0.0.0:7443".parse().unwrap(),
        roles,
        schedulable: true,
    };
    let mut identity = super::NodeIdentity {
        certificate_name: "[::1]".into(),
        certificate_pin: [7; 32],
        role: "combined".into(),
        external_endpoint: None,
        retired: false,
        drain_operation: None,
    };
    for endpoint in ["0.0.0.0:7443", "[::]:7443"] {
        spec.endpoint = endpoint.parse().unwrap();
        assert!(spec.validate(DeploymentMode::Local).is_ok());
        assert!(identity.validate(&spec, DeploymentMode::Local).is_ok());
        assert!(
            identity
                .validate(&spec, DeploymentMode::SeparateRf1)
                .is_err()
        );
    }
    assert!(
        state
            .apply(Command::put_node(
                id(601),
                state.revision(),
                spec.clone(),
                1
            ))
            .is_applied()
    );
    state.validate_snapshot().unwrap();
    spec.endpoint = "[::1]:7443".parse().unwrap();
    assert!(
        identity
            .validate(&spec, DeploymentMode::SeparateRf1)
            .is_err()
    );
    identity.certificate_name = "::1".into();
    assert!(identity.validate(&spec, DeploymentMode::Local).is_ok());
    identity.certificate_name = "[invalid::ip]".into();
    assert!(identity.validate(&spec, DeploymentMode::Local).is_err());
    identity.certificate_name = "localhost".into();
    identity.certificate_pin = [0; 32];
    assert!(identity.validate(&spec, DeploymentMode::Local).is_err());
    spec.endpoint.set_port(0);
    assert!(spec.validate(DeploymentMode::Local).is_err());
}

#[test]
fn local_metadata_membership_cannot_create_raft_history_or_mutate_before_rejection() {
    let (mut state, _) = fixture_with_mode(DeploymentMode::Local);
    let change = super::MetadataChange {
        id: Id::new_v7().unwrap(),
        previous: [1].into(),
        target: [2].into(),
        expected_membership_index: 1,
        completed_membership_index: None,
    };
    assert_eq!(
        state.prepare_metadata_change(change.clone()),
        Err(Rejection::Unsupported)
    );
    assert_eq!(
        state.complete_metadata_change(change.id, 2),
        Err(Rejection::Unsupported)
    );
    assert!(state.metadata_changes.is_empty());
    state.validate_snapshot().unwrap();
}
