use super::*;
use tidemq_protocol::{
    common::{ErrorCode, FrameKind, TrafficClass},
    console_operation::WriteCommand,
    framing::{decode_payload, new_frame},
};
use tracing::instrument::WithSubscriber;

const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";

pub(crate) struct Fixture {
    inner: crate::runtime::data::consumer_tests::fixture::Fixture,
    pub(crate) backend: LocalConsole,
}

impl Fixture {
    pub(crate) async fn new() -> Self {
        let mut inner = crate::runtime::data::consumer_tests::fixture::Fixture::new(
            &[(1, crate::config::Role::Combined)],
            &[1],
            &[1],
        )
        .await;
        assert!(
            inner
                .configs
                .values()
                .all(|config| config.tls.trust_roots.as_os_str().is_empty())
        );
        inner.start_all().await;
        let service = inner.running.get(&1).unwrap().service.clone();
        Self {
            inner,
            backend: LocalConsole::new(service).unwrap(),
        }
    }

    pub(crate) async fn root(&self) -> LocalContext {
        self.backend.authenticate(TOKEN).await.unwrap()
    }
    pub(crate) fn token(&self) -> &'static str {
        TOKEN
    }

    pub(crate) fn connections(&self) -> Vec<u64> {
        self.backend
            .service
            .transport_metrics
            .iter()
            .map(|metrics| metrics.snapshot().connections_total)
            .collect()
    }

    pub(crate) fn process(&self) -> super::super::process::Process {
        self.backend.service.process.clone()
    }

    pub(crate) fn console_config(&self) -> crate::console::EmbeddedConsoleConfig {
        self.backend
            .service
            .config
            .console
            .as_ref()
            .unwrap()
            .clone()
    }

    pub(crate) fn https_client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .tls_certs_only(vec![
                reqwest::Certificate::from_pem(&self.inner.roots).unwrap(),
            ])
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap()
    }

    pub(crate) async fn manage(&self, request: public::ManageIdentity) -> public::IdentityResult {
        let root = self.root().await;
        let operation = Id::new_v7().unwrap();
        let frame = new_frame(
            FrameKind::ManageIdentity,
            TrafficClass::Control,
            operation,
            operation,
            &request,
            5000,
            &self.backend.service.config.protocol,
        )
        .unwrap();
        let access = crate::gateway::Access {
            principal: Arc::from(format!("credential:{}", root.credential)),
            scope: Arc::from("*"),
            session: Some(operation),
            tenant: root.tenant,
        };
        let reply = self
            .backend
            .service
            .identity_public(&access, &frame, root.deadline)
            .await
            .unwrap();
        decode_payload(reply.payload, &self.backend.service.config.protocol).unwrap()
    }

    pub(crate) async fn tenant(&self, name: &str) -> (Id, Id) {
        let tenant = self
            .manage(public::ManageIdentity {
                action: public::IdentityAction::CreateTenantIdentity.into(),
                name: name.into(),
                ..Default::default()
            })
            .await;
        let tenant = Id::from_slice(&tenant.id).unwrap();
        let namespace = self
            .manage(public::ManageIdentity {
                action: public::IdentityAction::CreateNamespaceIdentity.into(),
                parent_id: bytes(tenant),
                name: "app".into(),
                ..Default::default()
            })
            .await;
        (tenant, Id::from_slice(&namespace.id).unwrap())
    }

    pub(crate) async fn principal(
        &self,
        tenant: Id,
        namespace: Id,
        permissions: &[&str],
    ) -> (Id, Id, String) {
        let principal = self
            .manage(public::ManageIdentity {
                action: public::IdentityAction::PutPrincipalIdentity.into(),
                parent_id: bytes(tenant),
                namespace_id: bytes(namespace),
                name: "operator".into(),
                enabled: true,
                permissions: permissions.iter().map(|name| (*name).to_owned()).collect(),
                ..Default::default()
            })
            .await;
        let principal = Id::from_slice(&principal.id).unwrap();
        let token = self
            .manage(public::ManageIdentity {
                action: public::IdentityAction::IssueCredential.into(),
                parent_id: bytes(principal),
                lifetime_ms: 600_000,
                ..Default::default()
            })
            .await;
        (
            principal,
            Id::from_slice(&token.id).unwrap(),
            token.access_token.unwrap(),
        )
    }

    pub(crate) async fn close(self) {
        self.inner.close().await;
    }
}

fn bytes(id: Id) -> Bytes {
    Bytes::copy_from_slice(id.as_bytes())
}
fn topics() -> Query {
    Query::Topics {
        tenant: None,
        namespace: None,
        after: None,
        limit: 32,
    }
}

#[tokio::test]
async fn local_login_queries_writes_and_original_receipts_open_no_sdk_connections() {
    let fixture = Fixture::new().await;
    let before = fixture.connections();
    let (tenant, namespace) = fixture.tenant("local").await;
    let context = fixture.root().await;
    assert_eq!(
        context.actor,
        context
            .project(|state, credential, now| Ok(state
                .auth()
                .current_principal(credential, now)
                .unwrap()
                .id))
            .await
            .unwrap()
    );
    let identity = context
        .identities(&public::ListIdentities {
            kind: public::IdentityKind::CurrentPrincipalIdentity.into(),
            limit: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        Id::from_slice(&identity.current_credential_id).unwrap(),
        context.credential
    );
    let request = WriteRequest {
        operation_id: Id::new_v7().unwrap(),
        command: WriteCommand::CreatePrincipal {
            tenant,
            namespace: Some(namespace),
            name: "created-locally".into(),
            permissions: vec!["inspect".into()],
        },
    };
    let result = context.write(&request).await.unwrap();
    assert_eq!(result.status, Status::Succeeded);
    let recovered = context.operation(request.operation_id).await.unwrap();
    assert_eq!(recovered.status, result.status);
    assert_eq!(recovered.actor, result.actor);
    assert_eq!(recovered.resource_id, result.resource_id);
    let issued = context
        .write(&WriteRequest {
            operation_id: Id::new_v7().unwrap(),
            command: WriteCommand::IssueCredential {
                principal: result.resource_id.unwrap(),
                lifetime_ms: tidemq_protocol::console::Decimal(60_000),
            },
        })
        .await
        .unwrap();
    assert_eq!(issued.status, Status::Succeeded);
    assert!(issued.secret.is_some());
    let recovered_credential = context.operation(issued.id).await.unwrap();
    assert!(recovered_credential.secret.is_none());
    assert!(recovered_credential.secret_unavailable);
    assert!(matches!(
        context.read(topics()).await.unwrap(),
        Reply::Topics(_)
    ));
    let mut changed = request.clone();
    changed.command = WriteCommand::CreatePrincipal {
        tenant,
        namespace: Some(namespace),
        name: "different-content".into(),
        permissions: vec!["inspect".into()],
    };
    assert_eq!(
        context.write(&changed).await.err().unwrap().failure().code,
        i32::from(ErrorCode::ContentConflict)
    );
    assert_eq!(fixture.connections(), before);
    assert!(
        fixture
            .backend
            .service
            .metrics
            .encode()
            .unwrap()
            .contains("operation=\"Recover\",outcome=\"Committed\"} 2")
    );
    fixture.close().await;
}

#[tokio::test]
async fn local_context_rechecks_revocation_permissions_tenant_and_namespace_boundaries() {
    let fixture = Fixture::new().await;
    let (tenant, namespace) = fixture.tenant("first").await;
    let (foreign, foreign_namespace) = fixture.tenant("second").await;
    let (principal, credential, token) = fixture
        .principal(tenant, namespace, &["inspect", "manage_credentials"])
        .await;
    let context = fixture.backend.authenticate(&token).await.unwrap();
    let other_namespace = fixture
        .manage(public::ManageIdentity {
            action: public::IdentityAction::CreateNamespaceIdentity.into(),
            parent_id: bytes(tenant),
            name: "other".into(),
            ..Default::default()
        })
        .await;
    assert_eq!(
        context
            .identities(&public::ListIdentities {
                kind: public::IdentityKind::CurrentPrincipalIdentity.into(),
                limit: 1,
                resource_scope: Some(public::ResourceScope {
                    tenant_id: bytes(tenant),
                    namespace_id: other_namespace.id,
                    topic_id: Bytes::new()
                }),
                ..Default::default()
            })
            .await
            .unwrap_err()
            .failure()
            .code,
        i32::from(ErrorCode::Forbidden)
    );
    let request = public::ListIdentities {
        kind: public::IdentityKind::NamespaceIdentities.into(),
        parent_id: bytes(foreign),
        limit: 32,
        ..Default::default()
    };
    assert_eq!(
        context
            .identities(&request)
            .await
            .unwrap_err()
            .failure()
            .code,
        i32::from(ErrorCode::Forbidden)
    );
    let write = WriteRequest {
        operation_id: Id::new_v7().unwrap(),
        command: WriteCommand::CreatePrincipal {
            tenant: foreign,
            namespace: Some(foreign_namespace),
            name: "foreign".into(),
            permissions: vec!["inspect".into()],
        },
    };
    assert_eq!(
        context.write(&write).await.err().unwrap().failure().code,
        i32::from(ErrorCode::Forbidden)
    );
    let root_write = WriteRequest {
        operation_id: Id::new_v7().unwrap(),
        command: WriteCommand::CreatePrincipal {
            tenant,
            namespace: Some(namespace),
            name: "root-operation".into(),
            permissions: vec!["inspect".into()],
        },
    };
    fixture.root().await.write(&root_write).await.unwrap();
    assert_eq!(
        context
            .operation(root_write.operation_id)
            .await
            .err()
            .unwrap()
            .failure()
            .code,
        i32::from(ErrorCode::Forbidden)
    );
    let current = context
        .identities(&public::ListIdentities {
            kind: public::IdentityKind::CurrentPrincipalIdentity.into(),
            limit: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    fixture
        .manage(public::ManageIdentity {
            action: public::IdentityAction::PutPrincipalIdentity.into(),
            target_id: bytes(principal),
            parent_id: bytes(tenant),
            namespace_id: bytes(namespace),
            name: "operator".into(),
            enabled: true,
            permissions: vec!["inspect".into()],
            expected_auth_revision: current.revision,
            ..Default::default()
        })
        .await;
    assert_eq!(
        context
            .identities(&public::ListIdentities {
                kind: public::IdentityKind::CredentialIdentities.into(),
                parent_id: bytes(principal),
                limit: 32,
                ..Default::default()
            })
            .await
            .unwrap_err()
            .failure()
            .code,
        i32::from(ErrorCode::Forbidden)
    );
    fixture
        .manage(public::ManageIdentity {
            action: public::IdentityAction::SetPrincipalEnabled.into(),
            target_id: bytes(principal),
            enabled: false,
            ..Default::default()
        })
        .await;
    assert_eq!(
        context.read(topics()).await.unwrap_err().failure().code,
        i32::from(ErrorCode::Unauthenticated)
    );
    fixture
        .manage(public::ManageIdentity {
            action: public::IdentityAction::SetPrincipalEnabled.into(),
            target_id: bytes(principal),
            enabled: true,
            ..Default::default()
        })
        .await;
    fixture
        .manage(public::ManageIdentity {
            action: public::IdentityAction::RevokeCredential.into(),
            target_id: bytes(credential),
            ..Default::default()
        })
        .await;
    assert_eq!(
        context
            .identities(&public::ListIdentities {
                kind: public::IdentityKind::CurrentPrincipalIdentity.into(),
                limit: 1,
                ..Default::default()
            })
            .await
            .unwrap_err()
            .failure()
            .code,
        i32::from(ErrorCode::Unauthenticated)
    );
    assert!(fixture.backend.authenticate(&token).await.is_err());
    fixture.close().await;
}

#[tokio::test]
async fn local_context_rejects_expired_credentials_and_disabled_parent_scope() {
    let fixture = Fixture::new().await;
    let (tenant, namespace) = fixture.tenant("expiry").await;
    let (principal, _, token) = fixture.principal(tenant, namespace, &["inspect"]).await;
    let short = fixture
        .manage(public::ManageIdentity {
            action: public::IdentityAction::IssueCredential.into(),
            parent_id: bytes(principal),
            lifetime_ms: 1000,
            ..Default::default()
        })
        .await;
    let expiring = fixture
        .backend
        .authenticate(short.access_token.as_deref().unwrap())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        expiring.read(topics()).await.unwrap_err().failure().code,
        i32::from(ErrorCode::Unauthenticated)
    );
    let active = fixture.backend.authenticate(&token).await.unwrap();
    fixture
        .manage(public::ManageIdentity {
            action: public::IdentityAction::SetNamespaceEnabled.into(),
            target_id: bytes(namespace),
            enabled: false,
            ..Default::default()
        })
        .await;
    assert_eq!(
        active.read(topics()).await.unwrap_err().failure().code,
        i32::from(ErrorCode::Unauthenticated)
    );
    fixture.close().await;
}

#[tokio::test]
async fn local_process_drain_blocks_new_writes_and_keeps_original_operation_queries() {
    let fixture = Fixture::new().await;
    let (tenant, namespace) = fixture.tenant("drain").await;
    let context = fixture.root().await;
    let operation = Id::new_v7().unwrap();
    let request = WriteRequest {
        operation_id: operation,
        command: WriteCommand::CreatePrincipal {
            tenant,
            namespace: Some(namespace),
            name: "before-drain".into(),
            permissions: vec!["inspect".into()],
        },
    };
    assert_eq!(
        context.write(&request).await.unwrap().status,
        Status::Succeeded
    );
    fixture.process().drain();
    assert_eq!(
        context.operation(operation).await.unwrap().status,
        Status::Succeeded
    );
    assert!(
        context
            .write(&WriteRequest {
                operation_id: Id::new_v7().unwrap(),
                ..request
            })
            .await
            .is_err()
    );
    let snapshot = context.observation().await.unwrap();
    assert_eq!(snapshot["items"][0]["ready"], false);
    assert!(!snapshot.to_string().contains("leader"));
    fixture.close().await;
}

#[tokio::test]
async fn local_request_metrics_keep_cancellation_and_unknown_operation_results_unknown() {
    let fixture = Fixture::new().await;
    let context = fixture.root().await;
    let cancelled = LocalMetrics::new(&fixture.backend.service, MetricOperation::Read);
    drop(cancelled);
    let unknown = context.operation(Id::new_v7().unwrap()).await.unwrap();
    assert_eq!(unknown.status, Status::Unknown);
    let metrics = fixture.backend.service.metrics.encode().unwrap();
    assert!(metrics.contains("operation=\"Read\",outcome=\"Unknown\"} 1"));
    assert!(metrics.contains("operation=\"Recover\",outcome=\"Unknown\"} 1"));
    assert!(metrics.contains("operation=\"Recover\",stage=\"Total\"} 1"));
    fixture.close().await;
}

#[tokio::test]
async fn combined_https_authentication_business_writes_and_recovery_use_the_local_backend() {
    let fixture = Fixture::new().await;
    let (tenant, namespace) = fixture.tenant("https").await;
    let config = fixture.console_config();
    let client = fixture.https_client();
    let before = fixture.connections();
    let response = client
        .post(format!("{}/api/v1/session", config.session.public_origin))
        .header("origin", &config.session.public_origin)
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&json!({"token":TOKEN})).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let cookie = response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let identity: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(identity["capabilities"]["execution"], "local");
    let operation = Id::new_v7().unwrap();
    let response = client.post(format!("{}/api/v1/principals", config.session.public_origin))
        .header("origin", &config.session.public_origin).header("cookie", &cookie)
        .header("x-csrf-token", identity["csrfToken"].as_str().unwrap()).header("content-type", "application/json")
        .body(serde_json::to_vec(&json!({"operationId":operation,"name":"native-https","tenantId":tenant,"namespaceId":namespace,"permissions":["inspect"]})).unwrap())
        .send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let created: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(created["status"], "succeeded");
    let response = client
        .get(format!(
            "{}/api/v1/operations/{operation}",
            config.session.public_origin
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let recovered: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(recovered["resourceId"], created["resourceId"]);
    assert_eq!(recovered["actor"], created["actor"]);
    assert_eq!(fixture.connections(), before);
    fixture.close().await;
}

#[tokio::test]
async fn local_message_content_refusal_keeps_audit_identity_without_logging_secrets_or_filters() {
    let fixture = Fixture::new().await;
    let (tenant, namespace) = fixture.tenant("audit").await;
    let created = fixture
        .root()
        .await
        .write(&WriteRequest {
            operation_id: Id::new_v7().unwrap(),
            command: WriteCommand::CreateTopic {
                tenant,
                namespace,
                name: "orders".into(),
            },
        })
        .await
        .unwrap();
    assert_eq!(created.status, Status::Succeeded);
    let (_, _, token) = fixture.principal(tenant, namespace, &["inspect"]).await;
    let context = fixture.backend.authenticate(&token).await.unwrap();
    let buffer = crate::observability::logging::tests::Buffer(Arc::default());
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(move || writer.clone())
        .finish();
    let private_filter = "cHJpdmF0ZS1rZXktZmlsdGVy";
    let result = context
        .read(Query::Messages {
            topic: created.resource_id.unwrap(),
            shard: Id::new_v7().unwrap(),
            start: None,
            end: None,
            observed_end: None,
            limit: 32,
            scan_limit: 512,
            key_base64: Some(private_filter.into()),
            properties_base64: Default::default(),
        })
        .with_subscriber(subscriber)
        .await;
    assert_eq!(
        result.unwrap_err().failure().code,
        i32::from(ErrorCode::Forbidden)
    );
    let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    assert!(!output.contains(&token));
    assert!(!output.contains(private_filter));
    let audit = output
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|event| event["fields"]["event_code"] == "console_message_inspection")
        .collect::<Vec<_>>();
    assert_eq!(audit.len(), 2);
    assert_eq!(audit[0]["fields"]["result"], "started");
    assert_eq!(audit[1]["fields"]["result"], "rejected");
    assert_eq!(
        audit[1]["fields"]["credential"],
        context.credential.to_string()
    );
    assert_eq!(
        audit[1]["fields"]["operation_id"],
        context.operation.to_string()
    );
    fixture.close().await;
}
