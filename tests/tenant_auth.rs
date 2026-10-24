use tidemq::metadata::{
    AuthAction, AuthCommand, AuthRejection, Command, DeploymentMode, MetadataLimits, MetadataState,
    Namespace, NodeRole, NodeSpec, Permission, Principal, Rejection, Response, Tenant, TokenDigest,
};
use tidemq_protocol::Id;

fn id() -> Id {
    Id::new_v7().unwrap()
}
const NOW: u64 = 1000;
const ROOT_TOKEN: &str = "root-token-with-at-least-32-ascii-characters";
const APP_TOKEN: &str = "application-token-with-at-least-32-ascii-characters";

fn apply(state: &mut MetadataState, actor: Option<Id>, action: AuthAction) -> Response {
    state.apply(Command::identity(
        id(),
        state.revision(),
        AuthCommand {
            actor,
            now_ms: NOW,
            request_digest: [7; 32],
            action,
        },
    ))
}

fn tenant(name: &str) -> Tenant {
    Tenant {
        id: id(),
        name: name.into(),
        enabled: true,
        limits: tidemq::metadata::TenantLimits {
            max_topics: 4,
            max_subscriptions: 8,
            max_principals: 4,
            max_credentials: 8,
            capacity: tidemq::metadata::QuotaCapacity {
                backlog_bytes: 1024 * 1024,
                ..tidemq::metadata::TenantLimits::default().capacity
            },
            ..Default::default()
        },
    }
}

#[test]
fn topics_persist_real_parent_ids_and_share_one_tenant_limit_across_namespaces() {
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
        apply(
            &mut state,
            None,
            AuthAction::Bootstrap {
                principal: id(),
                credential: root,
                digest: TokenDigest::of(ROOT_TOKEN).unwrap(),
                expires_ms: 100_000,
            }
        )
        .is_applied()
    );
    let mut tenant = tenant("acme");
    tenant.limits.max_topics = 1;
    tenant.limits.max_members = 1;
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::CreateTenant(tenant.clone())
        )
        .is_applied()
    );
    let app = id();
    for (namespace, name) in [(app, "app"), (id(), "other")] {
        assert!(
            apply(
                &mut state,
                Some(root),
                AuthAction::CreateNamespace(Namespace {
                    id: namespace,
                    tenant: tenant.id,
                    name: name.into(),
                    enabled: true,
                    limits: None,
                })
            )
            .is_applied()
        );
    }
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
    let record = state.topic(topic).unwrap();
    assert_eq!(record.tenant, Some(tenant.id));
    assert_eq!(record.namespace, Some(app));
    assert_shared_member_limit(&mut state, topic);
    assert!(matches!(
        state.apply(Command::create_topic(
            id(),
            state.revision(),
            id(),
            "acme/other/orders".into(),
            vec![2]
        )),
        Response::Rejected {
            reason: Rejection::Capacity,
            ..
        }
    ));
    let allocation = tidemq::metadata::AllocationRequest {
        partition: tidemq::metadata::PartitionKey {
            topic_id: topic,
            partition: 0,
        },
        tenant: tenant.id,
        namespace: app,
        expected_version: 0,
        capacity: tidemq::metadata::QuotaCapacity {
            backlog_bytes: 512 * 1024,
            ..Default::default()
        },
        frozen: false,
    };
    let mut forged = allocation.clone();
    forged.namespace = id();
    assert!(matches!(
        state.apply(Command::quota(
            id(),
            state.revision(),
            tidemq::metadata::QuotaCommand::Prepare {
                request: forged,
                now_ms: NOW
            }
        )),
        Response::Rejected {
            reason: Rejection::InvalidEvidence,
            ..
        }
    ));
    let remaining = state.summary().remaining_operations;
    let Response::Applied {
        outcome: tidemq::metadata::Outcome::Quota(grant),
        ..
    } = state.apply(Command::quota(
        id(),
        state.revision(),
        tidemq::metadata::QuotaCommand::Prepare {
            request: allocation,
            now_ms: NOW,
        },
    ))
    else {
        panic!("Validated tenant quota was not reserved");
    };
    assert_eq!(state.summary().remaining_operations, remaining);
    assert!(
        state
            .quotas()
            .allocation(grant.partition)
            .unwrap()
            .pending
            .is_some()
    );
    assert!(
        state
            .apply(Command::quota(
                id(),
                state.revision(),
                tidemq::metadata::QuotaCommand::Installed { grant, now_ms: NOW }
            ))
            .is_applied()
    );
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::SetTenantEnabled {
                id: tenant.id,
                enabled: false
            }
        )
        .is_applied()
    );
    state.validate_snapshot().unwrap();
    let restored: MetadataState =
        postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    restored.validate_snapshot().unwrap();
    assert_eq!(restored.topic(topic).unwrap().tenant, Some(tenant.id));
    assert_eq!(restored.topic(topic).unwrap().namespace, Some(app));
}

fn assert_shared_member_limit(state: &mut MetadataState, topic: Id) {
    use tidemq::metadata::{ConsumerCommand, MemberJoin};
    use tidemq_protocol::topic::{InitialPosition, SubscriptionKind};
    let mutate = |state: &mut MetadataState, operation, command| {
        state.apply(Command::consumer(operation, state.revision(), command))
    };
    let subscriptions = [id(), id()];
    for (index, subscription) in subscriptions.iter().enumerate() {
        assert!(
            mutate(
                state,
                *subscription,
                ConsumerCommand::Create {
                    topic,
                    name: format!("jobs-{index}"),
                    model: SubscriptionKind::Queue,
                    initial: InitialPosition::EarliestAvailable,
                }
            )
            .is_applied()
        );
        let plan = state
            .consumer_subscription(*subscription)
            .unwrap()
            .members
            .plan()
            .clone();
        for fenced in [false, true] {
            for domain in &plan.domains {
                assert!(
                    mutate(
                        state,
                        id(),
                        ConsumerCommand::Observe {
                            subscription: *subscription,
                            version: plan.version,
                            fenced,
                            domain: *domain,
                            now_ms: NOW,
                        }
                    )
                    .is_applied()
                );
            }
        }
    }
    let join = MemberJoin {
        member: id(),
        expected_epoch: 1,
        client: id(),
        credential: id(),
        now_ms: NOW,
        credential_expires_ms: 100_000,
        max_messages: 8,
        max_bytes: 4096,
        digest: [5; 32],
    };
    let operation = id();
    let first = ConsumerCommand::Join {
        subscription: subscriptions[0],
        request: join.clone(),
    };
    assert!(mutate(state, operation, first.clone()).is_applied());
    assert!(mutate(state, operation, first).is_applied());
    assert!(matches!(
        mutate(
            state,
            id(),
            ConsumerCommand::Join {
                subscription: subscriptions[1],
                request: MemberJoin {
                    member: id(),
                    client: id(),
                    ..join
                },
            }
        ),
        Response::Rejected {
            reason: Rejection::Capacity,
            ..
        }
    ));
}

#[test]
fn durable_credentials_enforce_tenant_namespace_and_live_revocation() {
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
    let root = id();
    let root_principal = id();
    assert!(
        apply(
            &mut state,
            None,
            AuthAction::Bootstrap {
                principal: root_principal,
                credential: root,
                digest: TokenDigest::of(ROOT_TOKEN).unwrap(),
                expires_ms: 100000
            }
        )
        .is_applied()
    );
    let acme = tenant("acme");
    let other = tenant("other");
    for tenant in [&acme, &other] {
        assert!(
            apply(
                &mut state,
                Some(root),
                AuthAction::CreateTenant(tenant.clone())
            )
            .is_applied()
        );
    }
    let namespace = Namespace {
        id: id(),
        tenant: acme.id,
        name: "app".into(),
        enabled: true,
        limits: None,
    };
    let other_namespace = Namespace {
        id: id(),
        tenant: other.id,
        name: "app".into(),
        enabled: true,
        limits: None,
    };
    for namespace in [&namespace, &other_namespace] {
        assert!(
            apply(
                &mut state,
                Some(root),
                AuthAction::CreateNamespace(namespace.clone())
            )
            .is_applied()
        );
    }
    let app = Principal {
        id: id(),
        tenant: Some(acme.id),
        namespace: Some(namespace.id),
        name: "writer".into(),
        enabled: true,
        permissions: [Permission::Produce].into(),
    };
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::PutPrincipal(app.clone())
        )
        .is_applied()
    );
    let credential = id();
    let digest = TokenDigest::of(APP_TOKEN).unwrap();
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::CreateCredential {
                id: credential,
                principal: app.id,
                digest,
                expires_ms: 100000
            }
        )
        .is_applied()
    );
    assert_eq!(
        state.auth().authenticate(digest, NOW).unwrap().id,
        credential
    );
    assert!(
        state
            .auth()
            .authorize(
                credential,
                NOW,
                Permission::Produce,
                Some("acme/app/orders")
            )
            .is_ok()
    );
    assert!(
        state
            .auth()
            .authorize(
                credential,
                NOW,
                Permission::Inspect,
                Some("acme/app/orders")
            )
            .is_ok()
    );
    assert!(
        state
            .auth()
            .authorize(
                credential,
                NOW,
                Permission::Produce,
                Some("other/app/orders")
            )
            .is_err()
    );
    assert!(
        state
            .auth()
            .authorize(
                credential,
                NOW,
                Permission::Consume,
                Some("acme/app/orders")
            )
            .is_err()
    );
    assert!(state.auth().authenticate(digest, 100000).is_err());
    let bytes = postcard::to_stdvec(&state).unwrap();
    assert!(
        !bytes
            .windows(APP_TOKEN.len())
            .any(|bytes| bytes == APP_TOKEN.as_bytes())
    );
    let mut restored: MetadataState = postcard::from_bytes(&bytes).unwrap();
    restored.validate_snapshot().unwrap();
    assert!(restored.auth().authenticate(digest, NOW).is_ok());
    assert!(
        apply(
            &mut restored,
            Some(root),
            AuthAction::DisableCredential { id: credential }
        )
        .is_applied()
    );
    assert!(restored.auth().authenticate(digest, NOW).is_err());
    assert!(!format!("{restored:?}").contains(APP_TOKEN));
}

#[test]
fn tenant_administrator_cannot_create_platform_privileges_or_credentials_for_stronger_principals() {
    let other = tenant("other");
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
    let root = id();
    assert!(
        apply(
            &mut state,
            None,
            AuthAction::Bootstrap {
                principal: id(),
                credential: root,
                digest: TokenDigest::of(ROOT_TOKEN).unwrap(),
                expires_ms: 100000
            }
        )
        .is_applied()
    );
    let tenant = tenant("acme");
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::CreateTenant(tenant.clone())
        )
        .is_applied()
    );
    let principal = Principal {
        id: id(),
        tenant: Some(tenant.id),
        namespace: None,
        name: "admin".into(),
        enabled: true,
        permissions: [Permission::ManageCredentials].into(),
    };
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::PutPrincipal(principal.clone())
        )
        .is_applied()
    );
    let credential = id();
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::CreateCredential {
                id: credential,
                principal: principal.id,
                digest: TokenDigest::of(APP_TOKEN).unwrap(),
                expires_ms: 100000
            }
        )
        .is_applied()
    );
    let mut elevated = principal.clone();
    elevated.permissions.insert(Permission::ManageTenants);
    assert!(matches!(
        apply(
            &mut state,
            Some(credential),
            AuthAction::PutPrincipal(elevated)
        ),
        Response::Rejected {
            reason: Rejection::Authorization(AuthRejection::Forbidden),
            ..
        }
    ));
    assert!(matches!(
        apply(
            &mut state,
            Some(credential),
            AuthAction::CreateTenant(other)
        ),
        Response::Rejected {
            reason: Rejection::Authorization(AuthRejection::Forbidden),
            ..
        }
    ));
    state.validate_snapshot().unwrap();
}
