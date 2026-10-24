use tidemq::metadata::{
    AuthAction, AuthCommand, AuthRejection, ClusterAction, ClusterCommand, Command, DeploymentMode,
    MetadataLimits, MetadataState, NodeIdentity, NodeRole, NodeSpec, Rejection, Response,
    TokenDigest,
};
use tidemq_protocol::Id;

const NOW: u64 = 1_000;

fn registration(node: u64) -> ClusterAction {
    let mut identity = NodeIdentity {
        certificate_name: format!("node-{node}.local"),
        certificate_pin: [node as u8; 32],
        role: if node == 1 { "combined" } else { "storage" }.into(),
        external_endpoint: None,
        retired: false,
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
