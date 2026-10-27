use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tidemq_protocol::Id;

use super::{
    AuthAction, AuthCommand, AuthOutcome, AuthResult, MetadataState, Rejection, TenantLimits,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuotaPolicy {
    pub id: Id,
    pub tenant: Id,
    pub namespace: Option<Id>,
    pub revision: u64,
    pub limits: TenantLimits,
    pub complete: bool,
    pub superseded: bool,
}

impl MetadataState {
    pub fn quota_policy(&self, policy: Id) -> Option<&QuotaPolicy> {
        self.policies.get(&policy)
    }

    pub(super) fn identity_command(
        &mut self,
        operation: Id,
        command: &AuthCommand,
    ) -> Result<AuthResult, Rejection> {
        self.validate_identity_deletion(command)?;
        let mut auth = self.auth.clone();
        let mut outcome = auth.apply(command).map_err(Rejection::Authorization)?;
        match &command.action {
            AuthAction::SetResourcePermission { scope, allow, .. } => {
                self.validate_authorization_scope(*scope, *allow)?;
            }
            AuthAction::ReplaceResourceGrants { grants, .. } => {
                for grant in grants {
                    self.validate_authorization_scope(grant.scope, true)?;
                }
            }
            _ => {}
        }
        self.auth = auth;
        let target = match command.action {
            AuthAction::SetTenantLimits { id, limits } => Some((id, None, limits)),
            AuthAction::SetNamespaceLimits { id, limits } => Some((
                self.auth.namespace(id).ok_or(Rejection::NotFound)?.tenant,
                Some(id),
                limits,
            )),
            _ => None,
        };
        if let Some((tenant, namespace, limits)) = target {
            for previous in self.policies.values_mut().filter(|policy| {
                policy.tenant == tenant && policy.namespace == namespace && !policy.complete
            }) {
                previous.superseded = true;
            }
            self.policies.insert(
                operation,
                QuotaPolicy {
                    id: operation,
                    tenant,
                    namespace,
                    revision: self.auth.revision(),
                    limits,
                    complete: false,
                    superseded: false,
                },
            );
            outcome = AuthOutcome::Policy(operation);
        }
        Ok(AuthResult {
            request_digest: command.request_digest,
            outcome,
        })
    }

    pub(crate) fn validate_authorization_scope(
        &self,
        scope: super::ResourceScope,
        require_active: bool,
    ) -> Result<(), Rejection> {
        if let Some(topic) = scope.topic() {
            let row = self
                .topic(topic)
                .filter(|row| row.tenant == scope.tenant() && row.namespace == scope.namespace())
                .ok_or(Rejection::NotFound)?;
            if require_active && row.status != super::RecordStatus::Active {
                return Err(Rejection::Authorization(super::AuthRejection::Forbidden));
            }
        }
        Ok(())
    }

    pub fn quota_suspended(
        &self,
        tenant: Id,
        namespace: Id,
        now_ms: u64,
    ) -> Result<bool, Rejection> {
        let (parent, child) = self
            .auth
            .limits(tenant, namespace)
            .map_err(Rejection::Authorization)?;
        Ok(!self
            .quotas
            .reserved(tenant, None, now_ms)?
            .fits(parent.capacity)
            || !self
                .quotas
                .reserved(tenant, Some(namespace), now_ms)?
                .fits(child.capacity))
    }

    pub fn quota_policy_ready(&self, policy: Id, now_ms: u64) -> Result<bool, Rejection> {
        let policy = self.policies.get(&policy).ok_or(Rejection::NotFound)?;
        if policy.complete || policy.superseded {
            return Ok(true);
        }
        let mut scopes = BTreeMap::<Id, (super::QuotaCapacity, bool, bool)>::new();
        for (_, record) in self.quotas.allocations() {
            let Some(grant) = record.installed.as_ref().or(record.pending.as_ref()) else {
                continue;
            };
            if grant.tenant != policy.tenant
                || policy
                    .namespace
                    .is_some_and(|namespace| grant.namespace != namespace)
            {
                continue;
            }
            if record.pending.is_some() {
                return Ok(false);
            }
            let (total, frozen, any_frozen) = scopes
                .entry(grant.namespace)
                .or_insert_with(|| (Default::default(), true, false));
            *total = total.checked_add(record.reserved(now_ms))?;
            *frozen &= grant.frozen;
            *any_frozen |= grant.frozen;
        }
        let parent = self
            .auth
            .tenant(policy.tenant)
            .ok_or(Rejection::NotFound)?
            .limits;
        let parent_suspended = !self
            .quotas
            .reserved(policy.tenant, None, now_ms)?
            .fits(parent.capacity);
        for (namespace, (used, frozen, any_frozen)) in scopes {
            let (_, child) = self
                .auth
                .limits(policy.tenant, namespace)
                .map_err(Rejection::Authorization)?;
            let suspended = parent_suspended || !used.fits(child.capacity);
            if suspended && !frozen || !suspended && any_frozen {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn observe_quota_policy(
        &mut self,
        policy: Id,
        now_ms: u64,
    ) -> Result<QuotaPolicy, Rejection> {
        if !self.quota_policy_ready(policy, now_ms)? {
            return Err(Rejection::RecoveryRequired);
        }
        let policy = self.policies.get_mut(&policy).ok_or(Rejection::NotFound)?;
        if !policy.superseded {
            policy.complete = true;
        }
        Ok(policy.clone())
    }

    pub(super) fn validate_policies(&self) -> Result<(), Rejection> {
        if self.policies.len() > self.limits.operations {
            return Err(Rejection::Capacity);
        }
        for (id, policy) in &self.policies {
            if *id != policy.id
                || policy.revision == 0
                || policy.revision > self.auth.revision()
                || policy.complete && policy.superseded
                || self.auth.tenant(policy.tenant).is_none()
            {
                return Err(Rejection::InvalidEvidence);
            }
            policy.limits.validate().map_err(Rejection::Authorization)?;
            if let Some(namespace) = policy.namespace {
                self.auth
                    .limits(policy.tenant, namespace)
                    .map_err(Rejection::Authorization)?;
            }
        }
        Ok(())
    }
}
