//! The two Console business backends share the existing protocol and SDK identity types.

use std::time::Duration;

use bytes::Bytes;
use serde_json::{Value, json};
use tidemq_protocol::{
    Id,
    console::{Query, Reply},
    console_operation::{Operation, WriteRequest},
    public,
};
use tidemq_sdk::{
    Client, Credentials, Error, ErrorKind, IdentityStatus, Permission, PrincipalAuthorization,
    PrincipalInfo, ResourceGrant, ResourceList, ResourceScope, Result, TlsTrust,
};

use super::{ClusterConfig, ConsoleConfig};
use crate::runtime::{LocalConsole, LocalContext};

pub(super) enum Backend {
    Remote {
        cluster: ClusterConfig,
        trust: Bytes,
    },
    Local(LocalConsole),
}

pub(super) enum Connection {
    Remote(Client),
    Local(Box<LocalContext>),
}

impl Backend {
    pub(super) async fn load(config: &ConsoleConfig) -> crate::Result<Self> {
        let trust = tokio::fs::read(&config.cluster.trust_roots)
            .await
            .map_err(|source| crate::Error::Io {
                operation: "read console trust roots",
                path: config.cluster.trust_roots.clone(),
                source,
            })?;
        Ok(Self::Remote {
            cluster: config.cluster.clone(),
            trust: trust.into(),
        })
    }

    pub(super) async fn connect(&self, token: &str) -> Result<Connection> {
        match self {
            Self::Local(backend) => backend
                .authenticate(token)
                .await
                .map(|context| Connection::Local(Box::new(context)))
                .map_err(local_error),
            Self::Remote { cluster, trust } => {
                let mut result = connect(cluster, trust, token).await;
                if result.as_ref().is_err_and(|error| {
                    matches!(
                        error.kind(),
                        ErrorKind::DeadlineExceeded | ErrorKind::Unavailable | ErrorKind::Closed
                    )
                }) {
                    // Connecting has not submitted a business operation.
                    result = connect(cluster, trust, token).await;
                }
                result.map(Connection::Remote)
            }
        }
    }
}

async fn connect(cluster: &ClusterConfig, trust: &Bytes, token: &str) -> Result<Client> {
    Client::builder(
        cluster.address.clone(),
        TlsTrust::ca_pem(trust.clone()),
        Credentials::token(token),
    )
    .server_name(cluster.server_name.clone())
    .request_timeout(Duration::from_secs(8))
    .connect_timeout(Duration::from_secs(3))
    .close_timeout(Duration::from_millis(250))
    .request_lanes(1)
    .pending_requests(8)
    .build()
    .await
}

impl Connection {
    pub(super) async fn close(&self) {
        if let Self::Remote(client) = self {
            let _ = client.close().await;
        }
    }

    pub(super) fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    pub(super) fn capabilities(&self) -> Value {
        let remote = !self.is_local();
        json!({"execution": if remote { "remote" } else { "local" },
            "clusterAdministration": remote, "replicaMembership": remote,
            "nodeMigration": remote, "raftDiagnostics": remote, "businessShardScaling": true})
    }

    pub(super) async fn local_observation(&self) -> Result<Option<Value>> {
        match self {
            Self::Local(context) => context.observation().await.map(Some).map_err(local_error),
            Self::Remote(_) => Ok(None),
        }
    }

    pub(super) async fn console_read(&self, query: Query) -> Result<Reply> {
        match self {
            Self::Remote(client) => client.console_read(query).await,
            Self::Local(context) => {
                // Keep the SDK request bound without creating a transport envelope.
                let bytes = serde_json::to_vec(&query).map_err(|_| Error::Configuration {
                    reason: "console query encoding failed",
                })?;
                if bytes.len() > 12 * 1024 {
                    return Err(Error::Configuration {
                        reason: "console query exceeds its finite bound",
                    });
                }
                context.read(query).await.map_err(local_error)
            }
        }
    }

    pub(super) async fn console_write(&self, request: &WriteRequest) -> Result<Operation> {
        match self {
            Self::Remote(client) => client.console_write(request).await,
            Self::Local(context) => context.write(request).await.map_err(local_error),
        }
    }

    pub(super) async fn console_operation(&self, id: Id) -> Result<Operation> {
        match self {
            Self::Remote(client) => client.console_operation(id).await,
            Self::Local(context) => context.operation(id).await.map_err(local_error),
        }
    }

    pub(super) async fn current_identity(&self) -> Result<PrincipalAuthorization> {
        self.authorization(None, None).await
    }

    pub(super) async fn effective_permissions(
        &self,
        scope: ResourceScope,
    ) -> Result<PrincipalAuthorization> {
        self.authorization(None, Some(scope)).await
    }

    pub(super) async fn principal_identity(
        &self,
        principal: tidemq_sdk::PrincipalId,
        scope: Option<ResourceScope>,
    ) -> Result<PrincipalAuthorization> {
        self.authorization(Some(principal), scope).await
    }

    async fn authorization(
        &self,
        principal: Option<tidemq_sdk::PrincipalId>,
        scope: Option<ResourceScope>,
    ) -> Result<PrincipalAuthorization> {
        if let Self::Remote(client) = self {
            return match (principal, scope) {
                (Some(principal), scope) => client.principal_identity(principal, scope).await,
                (None, Some(scope)) => client.effective_permissions(scope).await,
                (None, None) => client.current_identity().await,
            };
        }
        let request = public::ListIdentities {
            kind: if principal.is_some() {
                public::IdentityKind::PrincipalIdentity
            } else {
                public::IdentityKind::CurrentPrincipalIdentity
            }
            .into(),
            parent_id: principal.map(|id| bytes(id.into())).unwrap_or_default(),
            resource_scope: scope.map(scope_wire),
            limit: 1,
            ..Default::default()
        };
        let page = self.identities(&request).await?;
        if page.revision == 0 || page.entries.len() != 1 || !page.next_after_id.is_empty() {
            return Err(mismatch());
        }
        Ok(PrincipalAuthorization {
            principal: principal_info(page.entries.into_iter().next().ok_or_else(mismatch)?)?,
            revision: page.revision,
            cluster_id: Id::from_slice(&page.cluster_id)?,
            credential_id: Id::from_slice(&page.current_credential_id)?.into(),
            credential_expires_at_unix_ms: page.credential_expires_at_ms,
        })
    }

    pub(super) async fn identities(
        &self,
        request: &public::ListIdentities,
    ) -> Result<public::IdentityPage> {
        if !(1..=32).contains(&request.limit) {
            return Err(mismatch());
        }
        match self {
            Self::Local(context) => context.identities(request).await.map_err(|error| {
                if error.failure().code
                    == i32::from(tidemq_protocol::common::ErrorCode::StaleGeneration)
                {
                    Error::CatalogChanged
                } else {
                    local_error(error)
                }
            }),
            Self::Remote(client) => {
                let admin = client.admin();
                let parent = optional_id(&request.parent_id)?;
                match public::IdentityKind::try_from(request.kind).map_err(|_| mismatch())? {
                    public::IdentityKind::TenantIdentities => {
                        remote_page(admin.tenants(), request, |row| public::IdentityInfo {
                            id: bytes(row.id.into()),
                            name: row.name,
                            enabled: row.status == IdentityStatus::Active,
                            deleted: row.status == IdentityStatus::Deleted,
                            ..Default::default()
                        })
                        .await
                    }
                    public::IdentityKind::NamespaceIdentities => {
                        remote_page(
                            admin.namespaces(parent.ok_or_else(mismatch)?.into()),
                            request,
                            |row| public::IdentityInfo {
                                id: bytes(row.id.into()),
                                parent_id: bytes(row.tenant.into()),
                                name: row.name,
                                enabled: row.status == IdentityStatus::Active,
                                deleted: row.status == IdentityStatus::Deleted,
                                ..Default::default()
                            },
                        )
                        .await
                    }
                    public::IdentityKind::PrincipalIdentities => {
                        let listing = if let Some(parent) = parent {
                            admin.principals(parent.into())
                        } else {
                            admin.platform_principals()
                        };
                        remote_page(listing, request, principal_wire).await
                    }
                    public::IdentityKind::CredentialIdentities => {
                        remote_page(
                            admin.credentials(parent.ok_or_else(mismatch)?.into()),
                            request,
                            |row| public::IdentityInfo {
                                id: bytes(row.id.into()),
                                parent_id: bytes(row.principal.into()),
                                enabled: row.enabled,
                                expires_at_ms: row.expires_at_unix_ms,
                                ..Default::default()
                            },
                        )
                        .await
                    }
                    public::IdentityKind::ResourceAuthorizations => {
                        remote_page(
                            admin.resource_authorizations(scope_from_wire(
                                request.resource_scope.as_ref().ok_or_else(mismatch)?,
                            )?),
                            request,
                            principal_wire,
                        )
                        .await
                    }
                    _ => Err(mismatch()),
                }
            }
        }
    }
}

async fn remote_page<T>(
    mut listing: ResourceList<T>,
    request: &public::ListIdentities,
    encode: fn(T) -> public::IdentityInfo,
) -> Result<public::IdentityPage> {
    listing = listing.page_size(request.limit);
    if let Some(after) = optional_id(&request.after_id)? {
        listing.set_continuation(after, request.expected_revision);
    }
    let entries = listing
        .next_page()
        .await?
        .unwrap_or_default()
        .into_iter()
        .map(encode)
        .collect();
    Ok(public::IdentityPage {
        entries,
        next_after_id: listing
            .continuation()?
            .map(|(after, _)| bytes(after))
            .unwrap_or_default(),
        revision: listing.revision().ok_or_else(mismatch)?,
        ..Default::default()
    })
}

pub(super) fn principal_info(row: public::IdentityInfo) -> Result<PrincipalInfo> {
    if row.deleted || row.resource_grants.len() > 33 || row.effective_grants.len() > 33 {
        return Err(mismatch());
    }
    Ok(PrincipalInfo {
        id: Id::from_slice(&row.id)?.into(),
        tenant: optional_id(&row.parent_id)?.map(Into::into),
        namespace: optional_id(&row.namespace_id)?.map(Into::into),
        name: row.name,
        enabled: row.enabled,
        permissions: permissions(&row.permissions)?,
        resource_grants: row
            .resource_grants
            .into_iter()
            .map(grant_from_wire)
            .collect::<Result<_>>()?,
        effective_grants: row
            .effective_grants
            .into_iter()
            .map(grant_from_wire)
            .collect::<Result<_>>()?,
        effective_permissions: permissions(&row.effective_permissions)?,
    })
}

fn principal_wire(row: PrincipalInfo) -> public::IdentityInfo {
    public::IdentityInfo {
        id: bytes(row.id.into()),
        parent_id: row.tenant.map(|id| bytes(id.into())).unwrap_or_default(),
        namespace_id: row.namespace.map(|id| bytes(id.into())).unwrap_or_default(),
        name: row.name,
        enabled: row.enabled,
        permissions: row
            .permissions
            .into_iter()
            .map(|permission| permission.as_str().to_owned())
            .collect(),
        resource_grants: row.resource_grants.into_iter().map(grant_wire).collect(),
        effective_grants: row.effective_grants.into_iter().map(grant_wire).collect(),
        effective_permissions: row
            .effective_permissions
            .into_iter()
            .map(|permission| permission.as_str().to_owned())
            .collect(),
        ..Default::default()
    }
}

fn permissions(names: &[String]) -> Result<std::collections::BTreeSet<Permission>> {
    let result = names
        .iter()
        .map(|name| Permission::from_name(name))
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    if names.len() > 7 || result.len() != names.len() {
        return Err(mismatch());
    }
    Ok(result)
}

pub(super) fn scope_wire(scope: ResourceScope) -> public::ResourceScope {
    let (tenant, namespace, topic) = match scope {
        ResourceScope::Cluster => (None, None, None),
        ResourceScope::Tenant(tenant) => (Some(tenant), None, None),
        ResourceScope::Namespace { tenant, namespace } => (Some(tenant), Some(namespace), None),
        ResourceScope::Topic {
            tenant,
            namespace,
            topic,
        } => (Some(tenant), Some(namespace), Some(topic)),
    };
    public::ResourceScope {
        tenant_id: tenant.map(|id| bytes(id.into())).unwrap_or_default(),
        namespace_id: namespace.map(|id| bytes(id.into())).unwrap_or_default(),
        topic_id: topic.map(|id| bytes(id.into())).unwrap_or_default(),
    }
}

fn scope_from_wire(scope: &public::ResourceScope) -> Result<ResourceScope> {
    Ok(
        match (
            optional_id(&scope.tenant_id)?,
            optional_id(&scope.namespace_id)?,
            optional_id(&scope.topic_id)?,
        ) {
            (None, None, None) => ResourceScope::Cluster,
            (Some(tenant), None, None) => ResourceScope::Tenant(tenant.into()),
            (Some(tenant), Some(namespace), None) => ResourceScope::Namespace {
                tenant: tenant.into(),
                namespace: namespace.into(),
            },
            (Some(tenant), Some(namespace), Some(topic)) => ResourceScope::Topic {
                tenant: tenant.into(),
                namespace: namespace.into(),
                topic: topic.into(),
            },
            _ => return Err(mismatch()),
        },
    )
}

fn grant_wire(grant: ResourceGrant) -> public::ResourceGrant {
    public::ResourceGrant {
        scope: Some(scope_wire(grant.scope)),
        permissions: grant
            .permissions
            .into_iter()
            .map(|permission| permission.as_str().to_owned())
            .collect(),
    }
}

fn grant_from_wire(grant: public::ResourceGrant) -> Result<ResourceGrant> {
    let permissions = permissions(&grant.permissions)?;
    if permissions.is_empty() {
        return Err(mismatch());
    }
    Ok(ResourceGrant {
        scope: scope_from_wire(grant.scope.as_ref().ok_or_else(mismatch)?)?,
        permissions,
    })
}

fn bytes(id: Id) -> Bytes {
    Bytes::copy_from_slice(id.as_bytes())
}
fn optional_id(value: &[u8]) -> Result<Option<Id>> {
    if value.is_empty() {
        Ok(None)
    } else {
        Id::from_slice(value).map(Some).map_err(Into::into)
    }
}
fn mismatch() -> Error {
    Error::ResponseMismatch {
        reason: "Console identity response exceeded its identity, scope, or finite bounds".into(),
    }
}

fn local_error(error: crate::runtime::Error) -> Error {
    let failure = error.failure();
    let code = tidemq_protocol::common::ErrorCode::try_from(failure.code)
        .unwrap_or(tidemq_protocol::common::ErrorCode::Unavailable);
    Error::Remote {
        code,
        message: failure.message,
        outcome: tidemq_protocol::common::Outcome::try_from(failure.outcome)
            .unwrap_or(tidemq_protocol::common::Outcome::Unknown),
        retryable: failure.retryable,
        expected_sequence: failure.expected_sequence,
        expected_registration_epoch: failure.expected_registration_epoch,
        retry_after_ms: failure.retry_after_ms,
        original_outcome_known: failure.original_outcome_known,
    }
}
