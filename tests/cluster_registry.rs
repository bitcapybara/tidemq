use tidemq::metadata::{
    AuthAction, AuthCommand, AuthRejection, ClusterAction, ClusterCommand, Command, DeploymentMode,
    MetadataLimits, MetadataState, NodeIdentity, NodeRole, NodeSpec, Rejection, Response,
    TokenDigest,
};
use tidemq_protocol::Id;

const NOW: u64 = 1_000;

#[test]
fn data_replacement_intent_survives_snapshot_and_blocks_conflicting_plans_and_target_drain() {
    let (mut state, root) = fixture();
    for node in [2, 3] {
        assert!(apply(&mut state, Some(root), registration(node)).is_applied());
    }
    assert!(
        apply(
            &mut state,
            Some(root),
            ClusterAction::CreateGroup {
                group: 2,
                voters: [1].into()
            }
        )
        .is_applied()
    );
    let plan = tidemq::scaling::MembershipPlan::new(
        state.cluster_id().unwrap(),
        state.group(2).unwrap(),
        [2].into(),
    )
    .unwrap();
    assert!(
        apply(
            &mut state,
            Some(root),
            ClusterAction::ReplaceData(plan.clone())
        )
        .is_applied()
    );
    let mut restored: MetadataState =
        postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    restored.validate_snapshot().unwrap();
    assert_eq!(
        restored.pending_data_replacements().collect::<Vec<_>>(),
        [&plan]
    );
    let conflicting = tidemq::scaling::MembershipPlan::new(
        restored.cluster_id().unwrap(),
        restored.group(2).unwrap(),
        [3].into(),
    )
    .unwrap();
    let command = Command::prepare_membership(restored.revision(), conflicting).unwrap();
    assert!(matches!(
        restored.apply(command),
        Response::Rejected {
            reason: Rejection::OperationConflict,
            ..
        }
    ));
    assert!(matches!(
        apply(
            &mut restored,
            Some(root),
            ClusterAction::SetSchedulable {
                node: 2,
                enabled: false
            }
        ),
        Response::Rejected {
            reason: Rejection::NodeInUse,
            ..
        }
    ));
    let prepared = Command::prepare_membership(restored.revision(), plan.clone()).unwrap();
    assert!(restored.apply(prepared).is_applied());
    assert_eq!(restored.membership_change(2).unwrap().plan, plan);
    restored.validate_snapshot().unwrap();
}

#[test]
fn uninitialized_snapshot_rejects_control_membership_history() {
    let mut snapshot = serde_json::to_value(MetadataState::default()).unwrap();
    let operation = Id::random().unwrap();
    snapshot["metadata_changes"] = serde_json::json!({ operation.to_string(): {
        "id": operation, "previous": [1], "target": [2], "expected_membership_index": 0, "completed_membership_index": null
    }});
    let restored: MetadataState = serde_json::from_value(snapshot).unwrap();
    assert!(restored.validate_snapshot().is_err());
}

#[test]
fn beginning_node_drain_survives_snapshot_and_forbids_new_scheduling() {
    let (mut state, root) = fixture();
    assert!(apply(&mut state, Some(root), registration(2)).is_applied());
    let operation = Id::random().unwrap();
    let command = Command::cluster(
        operation,
        state.revision(),
        ClusterCommand {
            actor: Some(root),
            now_ms: NOW,
            request_digest: [7; 32],
            action: ClusterAction::BeginDrain { node: 2, operation },
        },
    );
    let receipt = state.apply(command.clone());
    assert!(receipt.is_applied());
    let mut restored: MetadataState =
        postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    restored.validate_snapshot().unwrap();
    assert_eq!(
        restored.node_identity(2).unwrap().drain_operation,
        Some(operation)
    );
    assert!(!restored.node(2).unwrap().spec.schedulable);
    assert_eq!(restored.apply(command), receipt);
    assert!(matches!(
        apply(
            &mut restored,
            Some(root),
            ClusterAction::SetSchedulable {
                node: 2,
                enabled: true
            }
        ),
        Response::Rejected {
            reason: Rejection::NodeInUse,
            ..
        }
    ));
}

#[test]
fn metadata_replacement_keeps_one_original_plan_and_blocks_retirement_until_completion() {
    let (mut state, root) = fixture();
    let ClusterAction::RegisterNode {
        mut spec,
        mut identity,
    } = registration(2)
    else {
        panic!("Expected registration");
    };
    identity.role = "metadata".into();
    spec.roles = identity.roles().unwrap();
    assert!(
        apply(
            &mut state,
            Some(root),
            ClusterAction::RegisterNode { spec, identity }
        )
        .is_applied()
    );
    let change = tidemq::metadata::MetadataChange {
        id: Id::random().unwrap(),
        previous: [1].into(),
        target: [2].into(),
        expected_membership_index: 0,
        completed_membership_index: None,
    };
    let original = Command::cluster(
        change.id,
        state.revision(),
        ClusterCommand {
            actor: Some(root),
            now_ms: NOW,
            request_digest: [9; 32],
            action: ClusterAction::ReplaceMetadata(change.clone()),
        },
    );
    let accepted = state.apply(original.clone());
    assert!(accepted.is_applied());
    assert!(
        apply(
            &mut state,
            Some(root),
            ClusterAction::SetSchedulable {
                node: 2,
                enabled: false
            }
        )
        .is_applied()
    );
    assert!(matches!(
        apply(
            &mut state,
            Some(root),
            ClusterAction::RetireNode {
                node: 2,
                metadata_voters: [1].into()
            }
        ),
        Response::Rejected {
            reason: Rejection::NodeInUse,
            ..
        }
    ));
    let mut restored: MetadataState =
        postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    restored.validate_snapshot().unwrap();
    assert_eq!(restored.pending_metadata_change(), Some(&change));
    assert!(
        !restored
            .apply(Command::complete_metadata_change(
                Id::random().unwrap(),
                restored.revision(),
                change.id,
                0
            ))
            .is_applied()
    );
    assert!(
        restored
            .apply(Command::complete_metadata_change(
                Id::random().unwrap(),
                restored.revision(),
                change.id,
                9
            ))
            .is_applied()
    );
    assert!(restored.pending_metadata_change().is_none());
    assert_eq!(
        restored
            .metadata_change(change.id)
            .unwrap()
            .completed_membership_index,
        Some(9)
    );
    assert_eq!(restored.apply(original), accepted);
    assert!(
        apply(
            &mut restored,
            Some(root),
            ClusterAction::SetSchedulable {
                node: 1,
                enabled: false
            }
        )
        .is_applied()
    );
    assert!(
        apply(
            &mut restored,
            Some(root),
            ClusterAction::RetireNode {
                node: 1,
                metadata_voters: [2].into()
            }
        )
        .is_applied()
    );
    restored.validate_snapshot().unwrap();
}

fn registration(node: u64) -> ClusterAction {
    let mut identity = NodeIdentity {
        certificate_name: format!("node-{node}.local"),
        certificate_pin: [node as u8; 32],
        role: if node == 1 { "combined" } else { "storage" }.into(),
        external_endpoint: None,
        retired: false,
        drain_operation: None,
    };
    if node == 1 {
        identity.external_endpoint = Some("127.0.0.1:7443".parse().unwrap());
    }
    ClusterAction::RegisterNode {
        spec: NodeSpec {
            node_id: node,
            zone: format!("zone-{node}"),
            endpoint: format!("127.0.0.1:{}", 7444 + node).parse().unwrap(),
            roles: identity.roles().unwrap(),
            schedulable: true,
        },
        identity,
    }
}

fn apply(state: &mut MetadataState, actor: Option<Id>, action: ClusterAction) -> Response {
    state.apply(Command::cluster(
        Id::random().unwrap(),
        state.revision(),
        ClusterCommand {
            actor,
            now_ms: NOW,
            request_digest: [7; 32],
            action,
        },
    ))
}

fn fixture() -> (MetadataState, Id) {
    let mut state = MetadataState::default();
    assert!(
        state
            .apply(Command::bootstrap(
                Id::random().unwrap(),
                Id::random().unwrap(),
                1,
                DeploymentMode::LocalRf1,
                MetadataLimits::default()
            ))
            .is_applied()
    );
    assert!(apply(&mut state, None, registration(1)).is_applied());
    let root = Id::random().unwrap();
    assert!(
        state
            .apply(Command::identity(
                Id::random().unwrap(),
                state.revision(),
                AuthCommand {
                    actor: None,
                    now_ms: NOW,
                    request_digest: [1; 32],
                    action: AuthAction::Bootstrap {
                        principal: Id::random().unwrap(),
                        credential: root,
                        digest: TokenDigest::of("registry-root-token-at-least-32-characters")
                            .unwrap(),
                        expires_ms: 100_000,
                    },
                }
            ))
            .is_applied()
    );
    (state, root)
}

#[test]
fn node_identity_and_certificate_tombstones_survive_restart_without_implicit_reregistration() {
    let (mut state, root) = fixture();
    assert!(apply(&mut state, Some(root), registration(2)).is_applied());
    assert!(matches!(
        apply(&mut state, None, registration(3)),
        Response::Rejected {
            reason: Rejection::Authorization(AuthRejection::Forbidden),
            ..
        }
    ));
    let ClusterAction::RegisterNode {
        mut spec,
        mut identity,
    } = registration(2)
    else {
        panic!("Expected a node registration");
    };
    spec.endpoint = "127.0.0.1:8444".parse().unwrap();
    assert!(
        !apply(
            &mut state,
            Some(root),
            ClusterAction::RegisterNode {
                spec,
                identity: identity.clone()
            }
        )
        .is_applied()
    );
    let ClusterAction::RegisterNode { spec, .. } = registration(3) else {
        panic!("Expected a node registration");
    };
    identity.certificate_name = "node-3.local".into();
    assert!(matches!(
        apply(
            &mut state,
            Some(root),
            ClusterAction::RegisterNode { spec, identity }
        ),
        Response::Rejected {
            reason: Rejection::InvalidNode,
            ..
        }
    ));
    assert!(
        !apply(
            &mut state,
            Some(root),
            ClusterAction::RetireNode {
                node: 2,
                metadata_voters: [1].into()
            }
        )
        .is_applied()
    );
    assert!(
        apply(
            &mut state,
            Some(root),
            ClusterAction::SetSchedulable {
                node: 2,
                enabled: false
            }
        )
        .is_applied()
    );
    assert!(
        apply(
            &mut state,
            Some(root),
            ClusterAction::RetireNode {
                node: 2,
                metadata_voters: [1].into()
            }
        )
        .is_applied()
    );
    assert!(state.node_identity(2).unwrap().retired);
    assert!(matches!(
        apply(&mut state, Some(root), registration(2)),
        Response::Rejected {
            reason: Rejection::IdentityRetired,
            ..
        }
    ));
    assert!(
        !apply(
            &mut state,
            Some(root),
            ClusterAction::CreateGroup {
                group: 2,
                voters: [2].into()
            }
        )
        .is_applied()
    );
    assert!(
        apply(
            &mut state,
            Some(root),
            ClusterAction::SetSchedulable {
                node: 1,
                enabled: false
            }
        )
        .is_applied()
    );
    assert!(
        !apply(
            &mut state,
            Some(root),
            ClusterAction::RetireNode {
                node: 1,
                metadata_voters: [1].into()
            }
        )
        .is_applied()
    );
    let recovered: MetadataState =
        postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    recovered.validate_snapshot().unwrap();
    assert_eq!(recovered.node_identity(2), state.node_identity(2));
    assert!(
        recovered
            .node(1)
            .unwrap()
            .spec
            .roles
            .contains(&NodeRole::Metadata)
    );
}

#[test]
fn node_pages_only_restart_for_registry_changes_and_groups_require_registered_replicas() {
    let (mut state, root) = fixture();
    for node in 2..=11 {
        assert!(apply(&mut state, Some(root), registration(node)).is_applied());
    }
    assert!(
        apply(
            &mut state,
            Some(root),
            ClusterAction::CreateGroup {
                group: 2,
                voters: [2].into()
            }
        )
        .is_applied()
    );
    let first = state.registry_page(Some(0), Some(0), 0).unwrap();
    assert_eq!(first.nodes.len(), 8);
    assert_eq!(first.groups.len(), 1);
    let revision = first.revision;
    assert!(
        state
            .apply(Command::identity(
                Id::random().unwrap(),
                state.revision(),
                AuthCommand {
                    actor: Some(root),
                    now_ms: NOW,
                    request_digest: [4; 32],
                    action: AuthAction::CreateTenant(tidemq::metadata::Tenant {
                        id: Id::random().unwrap(),
                        name: "acme".into(),
                        enabled: true,
                        limits: Default::default(),
                    }),
                }
            ))
            .is_applied()
    );
    let next = state
        .registry_page(first.next_nodes, first.next_groups, revision)
        .unwrap();
    assert_eq!(next.nodes.len(), 3);
    assert!(next.groups.is_empty());
    assert!(next.next_nodes.is_none());
    assert!(next.next_groups.is_none());
    assert!(apply(&mut state, Some(root), registration(12)).is_applied());
    assert_eq!(
        state.registry_page(Some(0), Some(0), revision),
        Err(Rejection::Superseded)
    );
    assert!(
        apply(
            &mut state,
            Some(root),
            ClusterAction::SetSchedulable {
                node: 2,
                enabled: false
            }
        )
        .is_applied()
    );
    assert!(matches!(
        apply(
            &mut state,
            Some(root),
            ClusterAction::RetireNode {
                node: 2,
                metadata_voters: [1].into()
            }
        ),
        Response::Rejected {
            reason: Rejection::NodeInUse,
            ..
        }
    ));
    state.validate_snapshot().unwrap();
}
