use tidemq::metadata::{
    AuthAction, AuthCommand, AuthRejection, Command, DeploymentMode, MetadataLimits, MetadataState,
    Namespace, Permission, Principal, Rejection, Response, Tenant, TokenDigest,
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
        max_topics: 4,
        max_subscriptions: 8,
        max_principals: 4,
        max_credentials: 8,
        max_backlog_bytes: 1024 * 1024,
    }
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
    };
    let other_namespace = Namespace {
        id: id(),
        tenant: other.id,
        name: "app".into(),
        enabled: true,
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
