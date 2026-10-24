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
    tenant.limits.capacity.cold_reads = 1;
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
    assert_shared_cold_read_leases(&mut state, topic);
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
    assert_quota_policy_fencing(&mut state, root, tenant.id);
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

fn assert_quota_policy_fencing(state: &mut MetadataState, actor: Id, tenant: Id) {
    use tidemq::metadata::{AllocationRequest, AuthOutcome, Outcome, QuotaCommand};
    let mut limits = state.auth().tenant(tenant).unwrap().limits;
    limits.capacity.backlog_bytes = 128 * 1024;
    let Response::Applied {
        outcome: Outcome::Identity(result),
        ..
    } = apply(
        state,
        Some(actor),
        AuthAction::SetTenantLimits { id: tenant, limits },
    )
    else {
        panic!("Expected the new quota policy to be committed");
    };
    let AuthOutcome::Policy(policy) = result.outcome else {
        panic!("Expected a quota policy identity");
    };
    assert!(!state.quota_policy_ready(policy, NOW).unwrap());
    let grant = state
        .quotas()
        .allocations()
        .next()
        .unwrap()
        .1
        .installed
        .clone()
        .unwrap();
    let Response::Applied {
        outcome: Outcome::Quota(frozen),
        ..
    } = state.apply(Command::quota(
        id(),
        state.revision(),
        QuotaCommand::Prepare {
            request: AllocationRequest {
                partition: grant.partition,
                tenant: grant.tenant,
                namespace: grant.namespace,
                expected_version: grant.version,
                capacity: grant.capacity,
                frozen: true,
            },
            now_ms: NOW,
        },
    ))
    else {
        panic!("Expected the original data quota to be frozen");
    };
    assert!(!state.quota_policy_ready(policy, NOW).unwrap());
    assert!(
        state
            .apply(Command::quota(
                id(),
                state.revision(),
                QuotaCommand::Installed {
                    grant: frozen,
                    now_ms: NOW
                }
            ))
            .is_applied()
    );
    assert!(state.quota_policy_ready(policy, NOW).unwrap());
    assert!(
        state
            .apply(Command::observe_quota_policy(
                id(),
                state.revision(),
                policy,
                NOW
            ))
            .is_applied()
    );
    assert!(state.quota_policy(policy).unwrap().complete);
    limits.capacity.backlog_bytes = 64 * 1024;
    let Response::Applied {
        outcome: Outcome::Identity(older),
        ..
    } = apply(
        state,
        Some(actor),
        AuthAction::SetTenantLimits { id: tenant, limits },
    )
    else {
        panic!("Expected a second quota policy");
    };
    let AuthOutcome::Policy(older) = older.outcome else {
        panic!("Expected a quota policy identity");
    };
    limits.capacity.backlog_bytes = 256 * 1024;
    assert!(
        apply(
            state,
            Some(actor),
            AuthAction::SetTenantLimits { id: tenant, limits }
        )
        .is_applied()
    );
    assert!(state.quota_policy(older).unwrap().superseded);
    assert!(state.quota_policy(policy).unwrap().complete);
    state.validate_snapshot().unwrap();
}

fn assert_shared_cold_read_leases(state: &mut MetadataState, topic: Id) {
    use tidemq::metadata::{
        Outcome, PartitionKey, ResourceLeaseCommand as Lease, ResourceLeaseTarget,
    };
    let apply = |state: &mut MetadataState, request| {
        state.apply(Command::resource_lease(id(), state.revision(), request))
    };
    let target = ResourceLeaseTarget::ColdRead(PartitionKey {
        topic_id: topic,
        partition: 0,
    });
    let first = id();
    let remaining = state.summary().remaining_operations;
    let response = apply(
        state,
        Lease::Acquire {
            id: first,
            target,
            owner: 1,
            now_ms: NOW,
        },
    );
    assert!(matches!(
        response,
        Response::Applied {
            outcome: Outcome::ResourceLease(Some(_)),
            ..
        }
    ));
    assert_eq!(
        apply(
            state,
            Lease::Acquire {
                id: first,
                target,
                owner: 1,
                now_ms: NOW
            }
        ),
        Response::Applied {
            revision: state.revision(),
            outcome: match response {
                Response::Applied { outcome, .. } => outcome,
                _ => panic!("Expected the original lease"),
            }
        }
    );
    assert!(matches!(
        apply(
            state,
            Lease::Acquire {
                id: id(),
                target,
                owner: 1,
                now_ms: NOW
            }
        ),
        Response::Rejected {
            reason: Rejection::Authorization(AuthRejection::Capacity),
            ..
        }
    ));
    let mut recovered: MetadataState =
        postcard::from_bytes(&postcard::to_stdvec(state).unwrap()).unwrap();
    recovered.validate_snapshot().unwrap();
    let second = id();
    assert!(
        apply(
            &mut recovered,
            Lease::Acquire {
                id: second,
                target,
                owner: 1,
                now_ms: NOW + 30_000
            }
        )
        .is_applied()
    );
    assert!(matches!(
        apply(
            &mut recovered,
            Lease::Renew {
                id: first,
                owner: 1,
                now_ms: NOW + 30_000
            }
        ),
        Response::Rejected {
            reason: Rejection::NotFound,
            ..
        }
    ));
    assert!(
        apply(
            &mut recovered,
            Lease::Release {
                id: first,
                owner: 1,
                now_ms: NOW + 30_000
            }
        )
        .is_applied()
    );
    assert_eq!(recovered.resource_leases().live(NOW + 30_000).count(), 1);
    assert!(
        apply(
            state,
            Lease::Release {
                id: first,
                owner: 1,
                now_ms: NOW
            }
        )
        .is_applied()
    );
    assert_eq!(state.resource_leases().live(NOW).count(), 0);
    assert_eq!(state.summary().remaining_operations, remaining);
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
    let mut resource = tidemq::metadata::TopicRecord {
        topic_id: id(),
        tenant: Some(acme.id),
        namespace: Some(namespace.id),
        name: "acme/app/orders".into(),
        generation: 1,
        partition_count: 1,
        status: tidemq::metadata::RecordStatus::Active,
    };
    assert!(
        state
            .auth()
            .authorize_topic(credential, NOW, Permission::Produce, &resource)
            .is_ok()
    );
    resource.tenant = Some(other.id);
    resource.namespace = Some(other_namespace.id);
    assert!(
        state
            .auth()
            .authorize_topic(credential, NOW, Permission::Produce, &resource)
            .is_err()
    );
    resource.name = "other/app/orders".into();
    assert!(
        state
            .auth()
            .authorize_topic(credential, NOW, Permission::Produce, &resource)
            .is_err()
    );
    resource.tenant = Some(acme.id);
    resource.namespace = Some(namespace.id);
    resource.name = "acme/app/orders".into();
    let mut disabled = state.clone();
    assert!(
        apply(
            &mut disabled,
            Some(root),
            AuthAction::SetTenantEnabled {
                id: acme.id,
                enabled: false
            }
        )
        .is_applied()
    );
    assert!(
        disabled
            .auth()
            .authorize_topic(root, NOW, Permission::Produce, &resource)
            .is_err()
    );
    resource.status = tidemq::metadata::RecordStatus::Retired;
    assert!(
        disabled
            .auth()
            .authorize_topic(root, NOW, Permission::Produce, &resource)
            .is_ok()
    );
    assert!(
        disabled
            .auth()
            .authorize_topic(credential, NOW, Permission::Produce, &resource)
            .is_err()
    );
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

#[test]
fn recreated_parent_names_never_authorize_a_new_principal_for_old_topic_identities() {
    use tidemq::metadata::{RecordStatus, TopicRecord};
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
                expires_ms: 100000,
            }
        )
        .is_applied()
    );
    let original = tenant("acme");
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::CreateTenant(original.clone())
        )
        .is_applied()
    );
    let old_namespace = id();
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::CreateNamespace(Namespace {
                id: old_namespace,
                tenant: original.id,
                name: "app".into(),
                enabled: true,
                limits: None,
            })
        )
        .is_applied()
    );
    let resource = TopicRecord {
        topic_id: id(),
        tenant: Some(original.id),
        namespace: Some(old_namespace),
        name: "acme/app/orders".into(),
        generation: 1,
        partition_count: 1,
        status: RecordStatus::Retired,
    };
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::DeleteTenant { id: original.id }
        )
        .eq(&Response::Rejected {
            revision: state.revision(),
            reason: Rejection::Authorization(AuthRejection::Conflict),
        })
    );
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::DeleteNamespace { id: old_namespace }
        )
        .is_applied()
    );
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::DeleteTenant { id: original.id }
        )
        .is_applied()
    );
    let current = tenant("acme");
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::CreateTenant(current.clone())
        )
        .is_applied()
    );
    let namespace = id();
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::CreateNamespace(Namespace {
                id: namespace,
                tenant: current.id,
                name: "app".into(),
                enabled: true,
                limits: None,
            })
        )
        .is_applied()
    );
    let principal = id();
    assert!(
        apply(
            &mut state,
            Some(root),
            AuthAction::PutPrincipal(Principal {
                id: principal,
                tenant: Some(current.id),
                namespace: Some(namespace),
                name: "writer".into(),
                enabled: true,
                permissions: [Permission::Produce].into(),
            })
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
                principal,
                digest: TokenDigest::of(APP_TOKEN).unwrap(),
                expires_ms: 100000,
            }
        )
        .is_applied()
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
            .authorize_topic(credential, NOW, Permission::Produce, &resource)
            .is_err()
    );
    assert!(
        state
            .auth()
            .authorize_topic(root, NOW, Permission::Produce, &resource)
            .is_ok()
    );
    let restored: MetadataState =
        postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    restored.validate_snapshot().unwrap();
    assert!(restored.auth().tenant_deleted(original.id));
    assert!(restored.auth().namespace_deleted(old_namespace));
    assert!(!restored.auth().tenant_deleted(current.id));
}

#[test]
fn original_management_refusals_remain_bound_to_their_own_request_after_restart() {
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
                expires_ms: 100_000,
            }
        )
        .is_applied()
    );
    let operation = id();
    let refused = state.apply(Command::identity(
        operation,
        state.revision(),
        AuthCommand {
            actor: Some(root),
            now_ms: NOW,
            request_digest: [9; 32],
            action: AuthAction::CreateNamespace(Namespace {
                id: operation,
                tenant: id(),
                name: "missing-parent".into(),
                enabled: true,
                limits: None,
            }),
        },
    ));
    assert!(matches!(
        refused,
        Response::Rejected {
            reason: Rejection::Authorization(AuthRejection::NotFound),
            ..
        }
    ));
    let recovered: MetadataState =
        postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    recovered.validate_snapshot().unwrap();
    assert_eq!(
        recovered.management_result(operation, [9; 32]).unwrap(),
        tidemq::metadata::OperationResult::Known(refused)
    );
    assert_eq!(
        recovered.management_result(operation, [8; 32]),
        Err(Rejection::OperationConflict)
    );
}
