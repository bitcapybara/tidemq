use tidemq::metadata::{
    AuthAction, AuthCommand, AuthRejection, Command, DeploymentMode, MetadataLimits, MetadataState,
    Namespace, NodeRole, NodeSpec, Permission, Principal, Rejection, ResourceGrant, ResourceScope,
    Response, Tenant, TokenDigest,
};
use tidemq_protocol::{Id, public};

const NOW: u64 = 1000;

fn id() -> Id {
    Id::new_v7().unwrap()
}

fn apply(state: &mut MetadataState, actor: Id, action: AuthAction) -> Response {
    state.apply(Command::identity(
        id(),
        state.revision(),
        AuthCommand {
            actor: Some(actor),
            now_ms: NOW,
            request_digest: [7; 32],
            action,
        },
    ))
}

struct Fixture {
    state: MetadataState,
    root: Id,
    tenant: Id,
    namespace: Id,
    topic: Id,
}

impl Fixture {
    fn new() -> Self {
        let mut state = MetadataState::default();
        assert!(
            state
                .apply(Command::bootstrap(
                    id(),
                    id(),
                    1,
                    DeploymentMode::LocalRf1,
                    MetadataLimits::default()
                ))
                .is_applied()
        );
        assert!(
            state
                .apply(Command::put_node(
                    id(),
                    state.revision(),
                    NodeSpec {
                        node_id: 1,
                        zone: "local".into(),
                        endpoint: "127.0.0.1:7444".parse().unwrap(),
                        roles: [NodeRole::Broker, NodeRole::Storage].into(),
                        schedulable: true,
                    },
                    0
                ))
                .is_applied()
        );
        assert!(
            state
                .apply(Command::create_group(id(), state.revision(), 2, [1].into()))
                .is_applied()
        );
        let root = id();
        assert!(
            state
                .apply(Command::identity(
                    id(),
                    state.revision(),
                    AuthCommand {
                        actor: None,
                        now_ms: NOW,
                        request_digest: [1; 32],
                        action: AuthAction::Bootstrap {
                            principal: id(),
                            credential: root,
                            digest: TokenDigest::of(
                                "root-token-with-more-than-thirty-two-characters"
                            )
                            .unwrap(),
                            expires_ms: 100_000
                        },
                    }
                ))
                .is_applied()
        );
        let tenant = id();
        assert!(
            apply(
                &mut state,
                root,
                AuthAction::CreateTenant(Tenant {
                    id: tenant,
                    name: "acme".into(),
                    enabled: true,
                    limits: Default::default()
                })
            )
            .is_applied()
        );
        let namespace = id();
        assert!(
            apply(
                &mut state,
                root,
                AuthAction::CreateNamespace(Namespace {
                    id: namespace,
                    tenant,
                    name: "app".into(),
                    enabled: true,
                    limits: None
                })
            )
            .is_applied()
        );
        let topic = id();
        assert!(
            state
                .apply(Command::create_topic(
                    id(),
                    state.revision(),
                    topic,
                    "acme/app/orders".into(),
                    vec![2]
                ))
                .is_applied()
        );
        Self {
            state,
            root,
            tenant,
            namespace,
            topic,
        }
    }

    fn principal(&mut self, name: &str, permissions: &[Permission]) -> (Id, Id) {
        let principal = id();
        assert!(
            apply(
                &mut self.state,
                self.root,
                AuthAction::PutPrincipal(Principal {
                    id: principal,
                    tenant: Some(self.tenant),
                    namespace: None,
                    name: name.into(),
                    enabled: true,
                    permissions: permissions.iter().copied().collect(),
                })
            )
            .is_applied()
        );
        let credential = id();
        assert!(
            apply(
                &mut self.state,
                self.root,
                AuthAction::CreateCredential {
                    id: credential,
                    principal,
                    digest: TokenDigest::of(&format!(
                        "credential-for-{name}-with-more-than-thirty-two-characters"
                    ))
                    .unwrap(),
                    expires_ms: 100_000,
                }
            )
            .is_applied()
        );
        (principal, credential)
    }

    fn scope(&self) -> ResourceScope {
        ResourceScope::Topic {
            tenant: self.tenant,
            namespace: self.namespace,
            topic: self.topic,
        }
    }

    fn set(&mut self, principal: Id, scope: ResourceScope, permission: Permission, allow: bool) {
        assert!(
            apply(
                &mut self.state,
                self.root,
                AuthAction::SetResourcePermission {
                    principal,
                    scope,
                    permission,
                    allow
                }
            )
            .is_applied()
        );
    }

    fn allowed(&self, credential: Id, permission: Permission) -> bool {
        self.state
            .auth()
            .authorize_topic(
                credential,
                NOW,
                permission,
                self.state.topic(self.topic).unwrap(),
            )
            .is_ok()
    }
}

#[test]
fn resource_grants_inherit_and_message_content_is_independent() {
    let mut fixture = Fixture::new();
    let (principal, credential) = fixture.principal("application", &[]);
    let tenant = ResourceScope::Tenant(fixture.tenant);
    let namespace = ResourceScope::Namespace {
        tenant: fixture.tenant,
        namespace: fixture.namespace,
    };
    let topic = fixture.scope();
    fixture.set(principal, tenant, Permission::Produce, true);
    fixture.set(principal, namespace, Permission::Consume, true);
    assert!(fixture.allowed(credential, Permission::Produce));
    assert!(fixture.allowed(credential, Permission::Consume));
    assert!(fixture.allowed(credential, Permission::Inspect));
    assert!(!fixture.allowed(credential, Permission::InspectMessageContent));
    fixture.set(principal, topic, Permission::InspectMessageContent, true);
    assert!(fixture.allowed(credential, Permission::InspectMessageContent));
    fixture.set(principal, topic, Permission::Produce, true);
    fixture.set(principal, topic, Permission::Produce, false);
    assert!(fixture.allowed(credential, Permission::Produce));
    fixture.set(principal, tenant, Permission::Produce, false);
    assert!(!fixture.allowed(credential, Permission::Produce));
    assert!(fixture.allowed(credential, Permission::Consume));
    fixture.state.validate_snapshot().unwrap();
    let bytes = postcard::to_stdvec(&fixture.state).unwrap();
    let restored: MetadataState = postcard::from_bytes(&bytes).unwrap();
    restored.validate_snapshot().unwrap();
    assert!(
        restored
            .auth()
            .authorize_topic(
                credential,
                NOW,
                Permission::InspectMessageContent,
                restored.topic(fixture.topic).unwrap()
            )
            .is_ok()
    );
}

#[test]
fn topic_grants_never_bind_recreated_names_or_forged_parents() {
    let mut fixture = Fixture::new();
    let (principal, credential) = fixture.principal("writer", &[]);
    let scope = fixture.scope();
    fixture.set(principal, scope, Permission::Produce, true);
    assert!(fixture.allowed(credential, Permission::Produce));
    let mut recreated = fixture.state.topic(fixture.topic).unwrap().clone();
    recreated.topic_id = id();
    assert!(
        fixture
            .state
            .auth()
            .authorize_topic(credential, NOW, Permission::Produce, &recreated)
            .is_err()
    );
    let absent = ResourceScope::Topic {
        tenant: fixture.tenant,
        namespace: fixture.namespace,
        topic: recreated.topic_id,
    };
    assert!(matches!(
        apply(
            &mut fixture.state,
            fixture.root,
            AuthAction::SetResourcePermission {
                principal,
                scope: absent,
                permission: Permission::Produce,
                allow: true,
            }
        ),
        Response::Rejected {
            reason: Rejection::NotFound,
            ..
        }
    ));
    let forged = ResourceScope::Topic {
        tenant: id(),
        namespace: fixture.namespace,
        topic: fixture.topic,
    };
    assert!(matches!(
        apply(
            &mut fixture.state,
            fixture.root,
            AuthAction::SetResourcePermission {
                principal,
                scope: forged,
                permission: Permission::Produce,
                allow: true,
            }
        ),
        Response::Rejected {
            reason: Rejection::Authorization(AuthRejection::Forbidden),
            ..
        }
    ));
    fixture.state.validate_snapshot().unwrap();
}

#[test]
fn grant_delegation_and_credential_issuance_cannot_expand_authority() {
    let mut fixture = Fixture::new();
    let (manager, actor) = fixture.principal("manager", &[Permission::ManageCredentials]);
    let (target, _) = fixture.principal("target", &[]);
    let scope = fixture.scope();
    fixture.set(manager, scope, Permission::Produce, true);
    assert!(
        apply(
            &mut fixture.state,
            actor,
            AuthAction::SetResourcePermission {
                principal: target,
                scope,
                permission: Permission::Produce,
                allow: true,
            }
        )
        .is_applied()
    );
    for (scope, permission) in [
        (ResourceScope::Tenant(fixture.tenant), Permission::Produce),
        (scope, Permission::InspectMessageContent),
    ] {
        assert!(matches!(
            apply(
                &mut fixture.state,
                actor,
                AuthAction::SetResourcePermission {
                    principal: target,
                    scope,
                    permission,
                    allow: true,
                }
            ),
            Response::Rejected {
                reason: Rejection::Authorization(AuthRejection::Forbidden),
                ..
            }
        ));
    }
    assert!(
        apply(
            &mut fixture.state,
            actor,
            AuthAction::CreateCredential {
                id: id(),
                principal: target,
                digest: TokenDigest::of(
                    "delegated-credential-with-more-than-thirty-two-characters"
                )
                .unwrap(),
                expires_ms: 100_000,
            }
        )
        .is_applied()
    );
    fixture.set(target, scope, Permission::InspectMessageContent, true);
    assert!(matches!(
        apply(
            &mut fixture.state,
            actor,
            AuthAction::CreateCredential {
                id: id(),
                principal: target,
                digest: TokenDigest::of("elevated-credential-with-more-than-thirty-two-characters")
                    .unwrap(),
                expires_ms: 100_000,
            }
        ),
        Response::Rejected {
            reason: Rejection::Authorization(AuthRejection::Forbidden),
            ..
        }
    ));
    assert!(
        apply(
            &mut fixture.state,
            fixture.root,
            AuthAction::SetPrincipalEnabled {
                id: target,
                enabled: false
            }
        )
        .is_applied()
    );
    assert!(matches!(
        apply(
            &mut fixture.state,
            actor,
            AuthAction::SetPrincipalEnabled {
                id: target,
                enabled: true
            }
        ),
        Response::Rejected {
            reason: Rejection::Authorization(AuthRejection::Forbidden),
            ..
        }
    ));
    fixture.state.validate_snapshot().unwrap();
}

#[test]
fn grant_replacement_uses_auth_revision_and_original_operation_identity() {
    let mut fixture = Fixture::new();
    let (principal, credential) = fixture.principal(
        "editor",
        &[Permission::Consume, Permission::ManageCredentials],
    );
    let stale = fixture.state.auth().revision();
    let scope = fixture.scope();
    fixture.set(principal, scope, Permission::Produce, true);
    let current = fixture.state.auth().revision();
    assert!(matches!(
        apply(
            &mut fixture.state,
            fixture.root,
            AuthAction::ReplaceResourceGrants {
                principal,
                expected_revision: stale,
                grants: Vec::new()
            }
        ),
        Response::Rejected {
            reason: Rejection::Authorization(AuthRejection::Conflict),
            ..
        }
    ));
    assert_eq!(fixture.state.auth().revision(), current);
    assert!(fixture.allowed(credential, Permission::Consume));
    let command = Command::identity(
        id(),
        fixture.state.revision(),
        AuthCommand {
            actor: Some(fixture.root),
            now_ms: NOW,
            request_digest: [9; 32],
            action: AuthAction::ReplaceResourceGrants {
                principal,
                expected_revision: current,
                grants: vec![ResourceGrant {
                    scope,
                    permissions: [Permission::Produce].into(),
                }],
            },
        },
    );
    let response = fixture.state.apply(command.clone());
    assert!(response.is_applied());
    assert!(!fixture.allowed(credential, Permission::Consume));
    assert!(fixture.allowed(credential, Permission::Produce));
    assert!(
        fixture
            .state
            .auth()
            .authorize(credential, NOW, Permission::ManageCredentials, None)
            .is_ok()
    );
    fixture.set(principal, scope, Permission::InspectMessageContent, true);
    let revision = fixture.state.auth().revision();
    assert_eq!(fixture.state.apply(command), response);
    assert_eq!(fixture.state.auth().revision(), revision);
    assert!(fixture.allowed(credential, Permission::InspectMessageContent));
    fixture.state.validate_snapshot().unwrap();
}

#[test]
fn authorization_queries_allow_self_without_management_and_protect_inverse_views() {
    let mut fixture = Fixture::new();
    let (principal, credential) = fixture.principal("viewer", &[]);
    let scope = fixture.scope();
    fixture.set(principal, scope, Permission::InspectMessageContent, true);
    let query = public::ListIdentities {
        kind: public::IdentityKind::CurrentPrincipalIdentity.into(),
        limit: 32,
        resource_scope: Some(scope.to_wire()),
        ..Default::default()
    };
    let page = fixture
        .state
        .auth()
        .identity_page(credential, NOW, &query)
        .unwrap();
    let own = page.entries.first().unwrap();
    assert_eq!(Id::from_slice(&own.id).unwrap(), principal);
    assert_eq!(own.effective_permissions, vec!["inspect_message_content"]);
    assert_eq!(own.effective_grants.len(), 1);
    let inverse = public::ListIdentities {
        kind: public::IdentityKind::ResourceAuthorizations.into(),
        ..query.clone()
    };
    assert!(
        fixture
            .state
            .auth()
            .identity_page(credential, NOW, &inverse)
            .is_err()
    );
    let (manager, actor) = fixture.principal("directory-admin", &[Permission::ManageCredentials]);
    let page = fixture
        .state
        .auth()
        .identity_page(actor, NOW, &inverse)
        .unwrap();
    assert!(
        page.entries
            .iter()
            .any(|row| Id::from_slice(&row.id).unwrap() == principal)
    );
    assert!(
        page.entries
            .iter()
            .any(|row| Id::from_slice(&row.id).unwrap() == manager)
    );
    let tenant = id();
    assert!(
        apply(
            &mut fixture.state,
            fixture.root,
            AuthAction::CreateTenant(Tenant {
                id: tenant,
                name: "other".into(),
                enabled: true,
                limits: Default::default()
            })
        )
        .is_applied()
    );
    for scope in [ResourceScope::Tenant(tenant), ResourceScope::Tenant(id())] {
        for (credential, base) in [(credential, &query), (actor, &inverse)] {
            let foreign = public::ListIdentities {
                resource_scope: Some(scope.to_wire()),
                ..base.clone()
            };
            assert!(matches!(
                fixture
                    .state
                    .auth()
                    .identity_page(credential, NOW, &foreign),
                Err(Rejection::Authorization(AuthRejection::Forbidden))
            ));
        }
        let before = fixture.state.auth().clone();
        for action in [
            AuthAction::SetResourcePermission {
                principal,
                scope,
                permission: Permission::Produce,
                allow: true,
            },
            AuthAction::ReplaceResourceGrants {
                principal,
                expected_revision: before.revision(),
                grants: vec![ResourceGrant {
                    scope,
                    permissions: [Permission::Produce].into(),
                }],
            },
        ] {
            assert!(matches!(
                apply(&mut fixture.state, actor, action),
                Response::Rejected {
                    reason: Rejection::Authorization(AuthRejection::Forbidden),
                    ..
                }
            ));
            assert_eq!(fixture.state.auth(), &before);
        }
    }
    fixture.state.validate_snapshot().unwrap();
}
