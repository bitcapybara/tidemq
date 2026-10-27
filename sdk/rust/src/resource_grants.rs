use std::collections::BTreeSet;

use crate::{
    Admin, AdminReceipt, Client, Error, NamespaceId, Permission, PrincipalId, PrincipalInfo,
    Result, TenantId, TopicId, admin::bytes,
};
use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    public,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ResourceScope {
    /// A read-only source for existing platform authority; resource grant mutations reject it.
    Cluster,
    Tenant(TenantId),
    Namespace {
        tenant: TenantId,
        namespace: NamespaceId,
    },
    Topic {
        tenant: TenantId,
        namespace: NamespaceId,
        topic: TopicId,
    },
}

impl ResourceScope {
    pub(crate) fn to_wire(self) -> public::ResourceScope {
        let (tenant, namespace, topic) = match self {
            Self::Cluster => (None, None, None),
            Self::Tenant(tenant) => (Some(tenant), None, None),
            Self::Namespace { tenant, namespace } => (Some(tenant), Some(namespace), None),
            Self::Topic {
                tenant,
                namespace,
                topic,
            } => (Some(tenant), Some(namespace), Some(topic)),
        };
        public::ResourceScope {
            tenant_id: tenant.map(|id| bytes(id.0)).unwrap_or_default(),
            namespace_id: namespace.map(|id| bytes(id.0)).unwrap_or_default(),
            topic_id: topic.map(|id| bytes(id.0)).unwrap_or_default(),
        }
    }

    pub(crate) fn from_wire(scope: &public::ResourceScope) -> Result<Self> {
        match (
            scope.tenant_id.is_empty(),
            scope.namespace_id.is_empty(),
            scope.topic_id.is_empty(),
        ) {
            (true, true, true) => Ok(Self::Cluster),
            (false, true, true) => Ok(Self::Tenant(TenantId(Id::from_slice(&scope.tenant_id)?))),
            (false, false, true) => Ok(Self::Namespace {
                tenant: TenantId(Id::from_slice(&scope.tenant_id)?),
                namespace: NamespaceId(Id::from_slice(&scope.namespace_id)?),
            }),
            (false, false, false) => Ok(Self::Topic {
                tenant: TenantId(Id::from_slice(&scope.tenant_id)?),
                namespace: NamespaceId(Id::from_slice(&scope.namespace_id)?),
                topic: TopicId(Id::from_slice(&scope.topic_id)?),
            }),
            _ => Err(mismatch()),
        }
    }

    pub(crate) fn covers(self, other: Self) -> bool {
        let granted = self.to_wire();
        let target = other.to_wire();
        (granted.tenant_id.is_empty() || granted.tenant_id == target.tenant_id)
            && (granted.namespace_id.is_empty() || granted.namespace_id == target.namespace_id)
            && (granted.topic_id.is_empty() || granted.topic_id == target.topic_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceGrant {
    pub scope: ResourceScope,
    pub permissions: BTreeSet<Permission>,
}

impl ResourceGrant {
    pub(crate) fn to_wire(&self) -> public::ResourceGrant {
        public::ResourceGrant {
            scope: Some(self.scope.to_wire()),
            permissions: self
                .permissions
                .iter()
                .map(|permission| permission.as_str().to_owned())
                .collect(),
        }
    }

    pub(crate) fn from_wire(grant: public::ResourceGrant) -> Result<Self> {
        if grant.permissions.is_empty() {
            return Err(mismatch());
        }
        Ok(Self {
            scope: ResourceScope::from_wire(grant.scope.as_ref().ok_or_else(mismatch)?)?,
            permissions: permission_set(&grant.permissions)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct PrincipalAuthorization {
    pub principal: PrincipalInfo,
    pub revision: u64,
    pub cluster_id: Id,
    pub credential_id: crate::CredentialId,
    pub credential_expires_at_unix_ms: u64,
}

impl Client {
    pub async fn principal_identity(
        &self,
        principal: PrincipalId,
        scope: Option<ResourceScope>,
    ) -> Result<PrincipalAuthorization> {
        self.identity_query(public::ListIdentities {
            kind: public::IdentityKind::PrincipalIdentity.into(),
            parent_id: bytes(principal.0),
            limit: 1,
            resource_scope: scope.map(ResourceScope::to_wire),
            ..Default::default()
        })
        .await
    }
    /// This query authenticates the caller without requiring identity-management permissions.
    pub async fn current_identity(&self) -> Result<PrincipalAuthorization> {
        self.identity_context(None).await
    }

    pub async fn effective_permissions(
        &self,
        scope: ResourceScope,
    ) -> Result<PrincipalAuthorization> {
        self.identity_context(Some(scope)).await
    }

    async fn identity_context(
        &self,
        scope: Option<ResourceScope>,
    ) -> Result<PrincipalAuthorization> {
        let request = public::ListIdentities {
            kind: public::IdentityKind::CurrentPrincipalIdentity.into(),
            limit: 1,
            resource_scope: scope.map(ResourceScope::to_wire),
            ..Default::default()
        };
        self.identity_query(request).await
    }

    async fn identity_query(
        &self,
        request: public::ListIdentities,
    ) -> Result<PrincipalAuthorization> {
        let operation = Id::new_v7()?;
        let reply: public::IdentityPage = self
            .operation(|deadline| {
                self.request(
                    FrameKind::ListIdentities,
                    FrameKind::IdentityPage,
                    TrafficClass::Control,
                    operation,
                    &request,
                    deadline,
                )
            })
            .await?;
        if reply.revision == 0 || reply.entries.len() != 1 || !reply.next_after_id.is_empty() {
            return Err(mismatch());
        }
        let row = reply.entries.into_iter().next().ok_or_else(mismatch)?;
        validate_effective_grants(&request, &row)?;
        Ok(PrincipalAuthorization {
            principal: crate::identity_list::principal_info(row)?,
            revision: reply.revision,
            cluster_id: Id::from_slice(&reply.cluster_id)?,
            credential_id: crate::CredentialId(Id::from_slice(&reply.current_credential_id)?),
            credential_expires_at_unix_ms: reply.credential_expires_at_ms,
        })
    }
}

impl Admin {
    pub async fn grant_permission(
        &self,
        principal: PrincipalId,
        scope: ResourceScope,
        permission: Permission,
    ) -> Result<()> {
        self.set_resource_permission(principal, scope, permission, true)
            .await
    }

    pub async fn revoke_permission(
        &self,
        principal: PrincipalId,
        scope: ResourceScope,
        permission: Permission,
    ) -> Result<()> {
        self.set_resource_permission(principal, scope, permission, false)
            .await
    }

    async fn set_resource_permission(
        &self,
        principal: PrincipalId,
        scope: ResourceScope,
        permission: Permission,
        allow: bool,
    ) -> Result<()> {
        validate_resource_permissions(scope, &[permission].into())?;
        self.resource_grant_request(
            principal,
            public::ManageIdentity {
                action: if allow {
                    public::IdentityAction::GrantResourcePermission.into()
                } else {
                    public::IdentityAction::RevokeResourcePermission.into()
                },
                target_id: bytes(principal.0),
                resource_scope: Some(scope.to_wire()),
                permissions: vec![permission.as_str().to_owned()],
                ..Default::default()
            },
        )
        .await
    }

    /// Replaces resource permissions only; identity-management permissions retain their scope.
    pub async fn replace_resource_grants(
        &self,
        principal: PrincipalId,
        expected_revision: u64,
        grants: &[ResourceGrant],
    ) -> Result<()> {
        if expected_revision == 0 || grants.len() > 32 {
            return Err(Error::Configuration {
                reason: "grant replacement needs a revision and at most 32 scopes",
            });
        }
        let mut scopes = BTreeSet::new();
        for grant in grants {
            validate_resource_permissions(grant.scope, &grant.permissions)?;
            if !scopes.insert(grant.scope) {
                return Err(Error::Configuration {
                    reason: "resource grant scopes must be unique",
                });
            }
        }
        self.resource_grant_request(
            principal,
            public::ManageIdentity {
                action: public::IdentityAction::ReplaceResourceGrants.into(),
                target_id: bytes(principal.0),
                expected_auth_revision: expected_revision,
                resource_grants: grants.iter().map(ResourceGrant::to_wire).collect(),
                ..Default::default()
            },
        )
        .await
    }

    async fn resource_grant_request(
        &self,
        principal: PrincipalId,
        request: public::ManageIdentity,
    ) -> Result<()> {
        match self.manage_identity(&request).await? {
            AdminReceipt::PrincipalConfigured(id) if id == principal => Ok(()),
            _ => Err(mismatch()),
        }
    }
}

pub(crate) fn permission_set(names: &[String]) -> Result<BTreeSet<Permission>> {
    if names.len() > 7 {
        return Err(mismatch());
    }
    let permissions = names
        .iter()
        .map(|permission| Permission::from_name(permission))
        .collect::<Result<BTreeSet<_>>>()?;
    if permissions.len() != names.len() {
        return Err(mismatch());
    }
    Ok(permissions)
}

pub(crate) fn validate_effective_grants(
    request: &public::ListIdentities,
    row: &public::IdentityInfo,
) -> Result<()> {
    let Some(scope) = request.resource_scope.as_ref() else {
        return if row.effective_grants.is_empty() && row.effective_permissions.is_empty() {
            Ok(())
        } else {
            Err(mismatch())
        };
    };
    let scope = ResourceScope::from_wire(scope)?;
    let mut permissions = BTreeSet::new();
    let mut scopes = BTreeSet::new();
    if row.effective_grants.len() > 33 {
        return Err(mismatch());
    }
    for grant in &row.effective_grants {
        let grant = ResourceGrant::from_wire(grant.clone())?;
        if !grant.scope.covers(scope) || !scopes.insert(grant.scope) {
            return Err(mismatch());
        }
        permissions.extend(grant.permissions);
    }
    if permissions.iter().any(|permission| {
        matches!(
            permission,
            Permission::Produce | Permission::Consume | Permission::ManageTopics
        )
    }) {
        permissions.insert(Permission::Inspect);
    }
    if permissions != permission_set(&row.effective_permissions)? {
        return Err(mismatch());
    }
    Ok(())
}

fn validate_resource_permissions(
    scope: ResourceScope,
    permissions: &BTreeSet<Permission>,
) -> Result<()> {
    if scope == ResourceScope::Cluster
        || permissions.is_empty()
        || permissions.iter().any(|permission| {
            matches!(
                permission,
                Permission::ManageTenants | Permission::ManageCredentials
            )
        })
    {
        return Err(Error::Configuration {
            reason: "resource grants require a tenant, namespace, or topic and resource permissions",
        });
    }
    Ok(())
}

fn mismatch() -> Error {
    Error::ResponseMismatch {
        reason: "authorization response changed its identity, scope, permissions, or finite bounds"
            .into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_sources_must_match_the_resource_and_cannot_invent_content_access() {
        let scope = ResourceScope::Topic {
            tenant: TenantId(Id::new_v7().unwrap()),
            namespace: NamespaceId(Id::new_v7().unwrap()),
            topic: TopicId(Id::new_v7().unwrap()),
        };
        let request = public::ListIdentities {
            resource_scope: Some(scope.to_wire()),
            ..Default::default()
        };
        let mut row = public::IdentityInfo {
            effective_grants: vec![
                ResourceGrant {
                    scope,
                    permissions: [Permission::Consume].into(),
                }
                .to_wire(),
            ],
            effective_permissions: vec!["consume".into(), "inspect".into()],
            ..Default::default()
        };
        validate_effective_grants(&request, &row).unwrap();
        row.effective_permissions
            .push("inspect_message_content".into());
        assert!(validate_effective_grants(&request, &row).is_err());
        row.effective_permissions.pop();
        row.effective_grants
            .push(row.effective_grants.first().unwrap().clone());
        assert!(validate_effective_grants(&request, &row).is_err());
        row.effective_grants.pop();
        let foreign = ResourceScope::Tenant(TenantId(Id::new_v7().unwrap()));
        row.effective_grants = vec![
            ResourceGrant {
                scope: foreign,
                permissions: [Permission::Consume].into(),
            }
            .to_wire(),
        ];
        assert!(validate_effective_grants(&request, &row).is_err());
        let malformed = public::ResourceScope {
            topic_id: bytes(Id::new_v7().unwrap()),
            ..Default::default()
        };
        assert!(ResourceScope::from_wire(&malformed).is_err());
    }
}
