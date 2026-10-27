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
        let kind =
            public::IdentityKind::try_from(request.kind).map_err(|_| Rejection::InvalidCommand)?;
        let actor = self
            .current_principal(credential, now_ms)
            .map_err(Rejection::Authorization)?;
        let own_lookup = kind == public::IdentityKind::PrincipalIdentity
            && request.parent_id.as_ref() == actor.id.as_bytes();
        if kind != public::IdentityKind::CurrentPrincipalIdentity && !own_lookup {
            self.authorize(credential, now_ms, Permission::ManageCredentials, None)
                .map_err(Rejection::Authorization)?;
        }
        if !(1..=32).contains(&request.limit) {
            return Err(Rejection::InvalidLimit);
        }
        if kind == public::IdentityKind::PrincipalIdentity
            && (request.limit != 1 || !request.after_id.is_empty())
        {
            return Err(Rejection::InvalidCommand);
        }
        if request.expected_revision != 0 && request.expected_revision != self.revision {
            return Err(Rejection::Superseded);
        }
        let after = optional_id(&request.after_id)?;
        let parent = optional_id(&request.parent_id)?;
        let scope = request
            .resource_scope
            .as_ref()
            .map(ResourceScope::from_wire)
            .transpose()
            .map_err(Rejection::Authorization)?;
        if scope.is_some()
            && !matches!(
                kind,
                public::IdentityKind::CurrentPrincipalIdentity
                    | public::IdentityKind::ResourceAuthorizations
                    | public::IdentityKind::PrincipalIdentity
            )
        {
            return Err(Rejection::InvalidCommand);
        }
        let limit = request.limit as usize;
        let mut entries = self
            .authorization_entries(actor, kind, parent, after, scope, limit + 1)
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
            cluster_id: Bytes::new(),
            current_credential_id: if matches!(
                kind,
                public::IdentityKind::CurrentPrincipalIdentity
                    | public::IdentityKind::PrincipalIdentity
            ) {
                bytes(credential)
            } else {
                Bytes::new()
            },
            credential_expires_at_ms: if matches!(
                kind,
                public::IdentityKind::CurrentPrincipalIdentity
                    | public::IdentityKind::PrincipalIdentity
            ) {
                self.credential(credential).map_or(0, |row| row.expires_ms)
            } else {
                0
            },
        })
    }

    fn authorization_entries(
        &self,
        actor: &Principal,
        kind: public::IdentityKind,
        parent: Option<Id>,
        after: Option<Id>,
        scope: Option<ResourceScope>,
        limit: usize,
    ) -> Result<Vec<public::IdentityInfo>, AuthRejection> {
        if kind == public::IdentityKind::PrincipalIdentity {
            let id = parent.ok_or(AuthRejection::Invalid)?;
            let target = self
                .principals
                .get(&id)
                .filter(|target| self.manage_principal(actor, target).is_ok())
                .ok_or(if actor.tenant.is_some() {
                    AuthRejection::Forbidden
                } else {
                    AuthRejection::NotFound
                })?;
            if let Some(scope) = scope {
                if !actor.resource_scope().covers(scope) || !target.resource_scope().covers(scope) {
                    return Err(AuthRejection::Forbidden);
                }
                self.validate_scope(scope)?;
            }
            return Ok(vec![self.principal_entry(target, scope)]);
        }
        if kind == public::IdentityKind::CurrentPrincipalIdentity {
            if parent.is_some() {
                return Err(AuthRejection::Invalid);
            }
            if let Some(scope) = scope {
                if !actor.resource_scope().covers(scope) {
                    return Err(AuthRejection::Forbidden);
                }
                self.validate_scope(scope)?;
            }
            return Ok(after
                .is_none_or(|after| actor.id > after)
                .then(|| self.principal_entry(actor, scope))
                .into_iter()
                .collect());
        }
        if kind != public::IdentityKind::ResourceAuthorizations {
            return self.identity_entries(actor, kind, parent, after, limit);
        }
        if parent.is_some() {
            return Err(AuthRejection::Invalid);
        }
        let scope = scope.ok_or(AuthRejection::Invalid)?;
        if !actor.resource_scope().covers(scope) {
            return Err(AuthRejection::Forbidden);
        }
        self.validate_scope(scope)?;
        Ok(self
            .principals
            .values()
            .filter(|row| {
                after.is_none_or(|after| row.id > after)
                    && self.manage_principal(actor, row).is_ok()
                    && row.resource_scope().covers(scope)
                    && !self.effective_resource_grants(row.id, scope).is_empty()
            })
            .take(limit)
            .map(|row| self.principal_entry(row, Some(scope)))
            .collect())
    }

    fn principal_entry(
        &self,
        row: &Principal,
        scope: Option<ResourceScope>,
    ) -> public::IdentityInfo {
        let effective = scope
            .map(|scope| self.effective_resource_grants(row.id, scope))
            .unwrap_or_default();
        let mut permissions: BTreeSet<_> = effective
            .iter()
            .flat_map(|grant| grant.permissions.iter().copied())
            .collect();
        if Self::permission_implied(&permissions, Permission::Inspect) {
            permissions.insert(Permission::Inspect);
        }
        public::IdentityInfo {
            id: bytes(row.id),
            parent_id: row.tenant.map(bytes).unwrap_or_default(),
            namespace_id: row.namespace.map(bytes).unwrap_or_default(),
            name: row.name.clone(),
            enabled: row.enabled,
            permissions: row
                .permissions
                .iter()
                .map(|permission| permission.name().to_owned())
                .collect(),
            resource_grants: self
                .direct_resource_grants(row.id)
                .iter()
                .map(ResourceGrant::to_wire)
                .collect(),
            effective_grants: effective.iter().map(ResourceGrant::to_wire).collect(),
            effective_permissions: permissions
                .iter()
                .map(|permission| permission.name().to_owned())
                .collect(),
            ..Default::default()
        }
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
            .map(|row| self.principal_entry(row, None))
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
