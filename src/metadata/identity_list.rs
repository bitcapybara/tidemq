use super::*;
use crate::metadata::Rejection;
use bytes::Bytes;
use tidemq_protocol::public;

impl AuthState {
    pub fn identity_page(
        &self,
        credential: Id,
        now_ms: u64,
        request: &public::ListIdentities,
    ) -> Result<public::IdentityPage, Rejection> {
        let actor = self
            .authorize(credential, now_ms, Permission::ManageCredentials, None)
            .map_err(Rejection::Authorization)?;
        if !(1..=32).contains(&request.limit) {
            return Err(Rejection::InvalidLimit);
        }
        if request.expected_revision != 0 && request.expected_revision != self.revision {
            return Err(Rejection::Superseded);
        }
        let after = optional_id(&request.after_id)?;
        let parent = optional_id(&request.parent_id)?;
        let kind =
            public::IdentityKind::try_from(request.kind).map_err(|_| Rejection::InvalidCommand)?;
        let limit = request.limit as usize;
        let mut entries = self
            .identity_entries(actor, kind, parent, after, limit + 1)
            .map_err(Rejection::Authorization)?;
        let next = if entries.len() > limit {
            entries.pop();
            entries
                .last()
                .map(|entry| entry.id.clone())
                .unwrap_or_default()
        } else {
            Bytes::new()
        };
        Ok(public::IdentityPage {
            entries,
            next_after_id: next,
            revision: self.revision,
        })
    }

    fn identity_entries(
        &self,
        actor: &Principal,
        kind: public::IdentityKind,
        parent: Option<Id>,
        after: Option<Id>,
        limit: usize,
    ) -> Result<Vec<public::IdentityInfo>, AuthRejection> {
        match kind {
            public::IdentityKind::TenantIdentities if parent.is_none() => {
                self.tenant_entries(actor, after, limit)
            }
            public::IdentityKind::NamespaceIdentities => {
                self.namespace_entries(actor, parent.ok_or(AuthRejection::Invalid)?, after, limit)
            }
            public::IdentityKind::PrincipalIdentities => {
                self.principal_entries(actor, parent, after, limit)
            }
            public::IdentityKind::CredentialIdentities => {
                self.credential_entries(actor, parent.ok_or(AuthRejection::Invalid)?, after, limit)
            }
            _ => Err(AuthRejection::Invalid),
        }
    }

    fn tenant_entries(
        &self,
        actor: &Principal,
        after: Option<Id>,
        limit: usize,
    ) -> Result<Vec<public::IdentityInfo>, AuthRejection> {
        self.tenants
            .values()
            .filter(|row| {
                after.is_none_or(|after| row.id > after)
                    && actor.tenant.is_none_or(|tenant| row.id == tenant)
            })
            .take(limit)
            .map(|row| {
                Ok(public::IdentityInfo {
                    id: bytes(row.id),
                    name: row.name.clone(),
                    enabled: row.enabled,
                    deleted: self.tenant_deleted(row.id),
                    limits: Some(row.limits.to_wire()?),
                    ..Default::default()
                })
            })
            .collect()
    }

    fn namespace_entries(
        &self,
        actor: &Principal,
        tenant: Id,
        after: Option<Id>,
        limit: usize,
    ) -> Result<Vec<public::IdentityInfo>, AuthRejection> {
        self.manage_tenant(actor, tenant)?;
        self.namespaces
            .values()
            .filter(|row| {
                after.is_none_or(|after| row.id > after)
                    && row.tenant == tenant
                    && actor.namespace.is_none_or(|namespace| row.id == namespace)
            })
            .take(limit)
            .map(|row| {
                Ok(public::IdentityInfo {
                    id: bytes(row.id),
                    parent_id: bytes(row.tenant),
                    name: row.name.clone(),
                    enabled: row.enabled,
                    deleted: self.namespace_deleted(row.id),
                    limits: Some(self.limits(row.tenant, row.id)?.1.to_wire()?),
                    ..Default::default()
                })
            })
            .collect()
    }

    fn principal_entries(
        &self,
        actor: &Principal,
        parent: Option<Id>,
        after: Option<Id>,
        limit: usize,
    ) -> Result<Vec<public::IdentityInfo>, AuthRejection> {
        if let Some(tenant) = parent {
            self.manage_tenant(actor, tenant)?;
        } else if actor.tenant.is_some() {
            return Err(AuthRejection::Forbidden);
        }
        Ok(self
            .principals
            .values()
            .filter(|row| {
                after.is_none_or(|after| row.id > after)
                    && row.tenant == parent
                    && actor
                        .namespace
                        .is_none_or(|namespace| row.namespace == Some(namespace))
            })
            .take(limit)
            .map(|row| public::IdentityInfo {
                id: bytes(row.id),
                parent_id: row.tenant.map(bytes).unwrap_or_default(),
                namespace_id: row.namespace.map(bytes).unwrap_or_default(),
                name: row.name.clone(),
                enabled: row.enabled,
                permissions: row
                    .permissions
                    .iter()
                    .map(|permission| permission_name(*permission).to_owned())
                    .collect(),
                ..Default::default()
            })
            .collect())
    }

    fn credential_entries(
        &self,
        actor: &Principal,
        principal: Id,
        after: Option<Id>,
        limit: usize,
    ) -> Result<Vec<public::IdentityInfo>, AuthRejection> {
        self.manage_principal(
            actor,
            self.principals
                .get(&principal)
                .ok_or(AuthRejection::NotFound)?,
        )?;
        Ok(self
            .credentials
            .values()
            .filter(|row| after.is_none_or(|after| row.id > after) && row.principal == principal)
            .take(limit)
            .map(|row| public::IdentityInfo {
                id: bytes(row.id),
                parent_id: bytes(row.principal),
                enabled: row.enabled,
                expires_at_ms: row.expires_ms,
                ..Default::default()
            })
            .collect())
    }
}

fn optional_id(value: &[u8]) -> Result<Option<Id>, Rejection> {
    if value.is_empty() {
        Ok(None)
    } else {
        Id::from_slice(value)
            .map(Some)
            .map_err(|_| Rejection::InvalidCommand)
    }
}

fn bytes(id: Id) -> Bytes {
    Bytes::copy_from_slice(id.as_bytes())
}

fn permission_name(permission: Permission) -> &'static str {
    match permission {
        Permission::ManageTenants => "manage_tenants",
        Permission::ManageCredentials => "manage_credentials",
        Permission::ManageTopics => "manage_topics",
        Permission::Produce => "produce",
        Permission::Consume => "consume",
        Permission::Inspect => "inspect",
    }
}
