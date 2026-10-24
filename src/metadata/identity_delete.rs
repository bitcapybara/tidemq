use super::*;
use crate::metadata::{MetadataState, RecordStatus, Rejection};

impl MetadataState {
    pub(crate) fn validate_identity_deletion(
        &self,
        command: &AuthCommand,
    ) -> Result<(), Rejection> {
        let permission = match command.action {
            AuthAction::DeleteTenant { .. } => Permission::ManageTenants,
            AuthAction::DeleteNamespace { .. } => Permission::ManageCredentials,
            _ => return Ok(()),
        };
        let actor = self
            .auth
            .authorize(
                command
                    .actor
                    .ok_or(Rejection::Authorization(AuthRejection::Forbidden))?,
                command.now_ms,
                permission,
                None,
            )
            .map_err(Rejection::Authorization)?;
        let (tenant, namespace) = match command.action {
            AuthAction::DeleteTenant { id } => {
                if actor.tenant.is_some() {
                    return Err(Rejection::Authorization(AuthRejection::Forbidden));
                }
                self.auth.tenant(id).ok_or(Rejection::NotFound)?;
                (id, None)
            }
            AuthAction::DeleteNamespace { id } => {
                let row = self.auth.namespace(id).ok_or(Rejection::NotFound)?;
                self.auth
                    .manage_namespace_deletion(actor, row)
                    .map_err(Rejection::Authorization)?;
                (row.tenant, Some(id))
            }
            _ => return Ok(()),
        };
        if self.topics.values().any(|topic| {
            topic.tenant == Some(tenant)
                && namespace.is_none_or(|namespace| topic.namespace == Some(namespace))
                && topic.status != RecordStatus::Retired
        }) {
            return Err(Rejection::RecoveryRequired);
        }
        Ok(())
    }
}

impl AuthState {
    pub(super) fn manage_namespace_deletion(
        &self,
        actor: &Principal,
        row: &Namespace,
    ) -> Result<(), AuthRejection> {
        if actor.namespace.is_some() {
            return Err(AuthRejection::Forbidden);
        }
        self.manage_tenant(actor, row.tenant)
    }

    pub(super) fn delete_namespace(
        &mut self,
        actor: &Principal,
        id: Id,
        now_ms: u64,
    ) -> Result<AuthOutcome, AuthRejection> {
        let row = self.namespaces.get(&id).ok_or(AuthRejection::NotFound)?;
        self.manage_namespace_deletion(actor, row)?;
        self.require_inactive_principals(row.tenant, Some(id), now_ms)?;
        self.namespaces
            .get_mut(&id)
            .ok_or(AuthRejection::NotFound)?
            .enabled = false;
        self.retired_namespaces.insert(id);
        Ok(AuthOutcome::Namespace(id))
    }

    pub(super) fn delete_tenant(
        &mut self,
        actor: &Principal,
        id: Id,
        now_ms: u64,
    ) -> Result<AuthOutcome, AuthRejection> {
        if actor.tenant.is_some() {
            return Err(AuthRejection::Forbidden);
        }
        self.tenants.get(&id).ok_or(AuthRejection::NotFound)?;
        if self
            .namespaces
            .values()
            .any(|row| row.tenant == id && !self.namespace_deleted(row.id))
        {
            return Err(AuthRejection::Conflict);
        }
        self.require_inactive_principals(id, None, now_ms)?;
        self.tenants
            .get_mut(&id)
            .ok_or(AuthRejection::NotFound)?
            .enabled = false;
        self.retired_tenants.insert(id);
        Ok(AuthOutcome::Tenant(id))
    }

    fn require_inactive_principals(
        &self,
        tenant: Id,
        namespace: Option<Id>,
        now_ms: u64,
    ) -> Result<(), AuthRejection> {
        let in_scope = |row: &&Principal| {
            row.tenant == Some(tenant)
                && namespace.is_none_or(|namespace| row.namespace == Some(namespace))
        };
        if self
            .principals
            .values()
            .filter(in_scope)
            .any(|row| row.enabled)
            || self.credentials.values().any(|credential| {
                credential.enabled
                    && credential.expires_ms > now_ms
                    && self
                        .principals
                        .get(&credential.principal)
                        .is_some_and(|row| in_scope(&row))
            })
        {
            return Err(AuthRejection::Conflict);
        }
        Ok(())
    }

    pub(super) fn validate_live_parents(&self, principal: &Principal) -> Result<(), AuthRejection> {
        if principal.tenant.is_some_and(|id| self.tenant_deleted(id))
            || principal
                .namespace
                .is_some_and(|id| self.namespace_deleted(id))
        {
            return Err(AuthRejection::NotFound);
        }
        Ok(())
    }

    pub(super) fn validate_deletions(&self) -> Result<(), AuthRejection> {
        if self
            .retired_tenants
            .iter()
            .any(|id| self.tenants.get(id).is_none_or(|row| row.enabled))
            || self
                .retired_namespaces
                .iter()
                .any(|id| self.namespaces.get(id).is_none_or(|row| row.enabled))
            || self
                .namespaces
                .values()
                .any(|row| self.tenant_deleted(row.tenant) && !self.namespace_deleted(row.id))
        {
            return Err(AuthRejection::Invalid);
        }
        let mut tenants = BTreeSet::new();
        let mut namespaces = BTreeSet::new();
        if self
            .tenants
            .values()
            .any(|row| !self.tenant_deleted(row.id) && !tenants.insert(&row.name))
            || self.namespaces.values().any(|row| {
                !self.namespace_deleted(row.id) && !namespaces.insert((row.tenant, &row.name))
            })
            || self
                .principals
                .values()
                .any(|row| row.enabled && self.validate_live_parents(row).is_err())
        {
            return Err(AuthRejection::Invalid);
        }
        Ok(())
    }
}
