use std::collections::BTreeSet;
use tidemq_protocol::{
    Id,
    common::{ErrorCode, FrameKind, TrafficClass},
    public,
};

use crate::{
    Admin, Client, CredentialId, Error, NamespaceId, Permission, PrincipalId, ResourceLimits,
    Result, TenantId, admin::bytes,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityStatus {
    Active,
    Disabled,
    Deleted,
}

#[derive(Clone, Debug)]
pub struct TenantInfo {
    pub id: TenantId,
    pub name: String,
    pub status: IdentityStatus,
    pub limits: ResourceLimits,
}
#[derive(Clone, Debug)]
pub struct NamespaceInfo {
    pub id: NamespaceId,
    pub tenant: TenantId,
    pub name: String,
    pub status: IdentityStatus,
    pub limits: ResourceLimits,
}
#[derive(Clone, Debug)]
pub struct PrincipalInfo {
    pub id: PrincipalId,
    pub tenant: Option<TenantId>,
    pub namespace: Option<NamespaceId>,
    pub name: String,
    pub enabled: bool,
    pub permissions: BTreeSet<Permission>,
}
#[derive(Clone, Debug)]
pub struct CredentialInfo {
    pub id: CredentialId,
    pub principal: PrincipalId,
    pub enabled: bool,
    pub expires_at_unix_ms: u64,
}

pub struct ResourceList<T> {
    client: Client,
    request: public::ListIdentities,
    decode: fn(public::IdentityInfo) -> Result<T>,
    complete: bool,
}

impl<T> ResourceList<T> {
    pub fn page_size(mut self, count: u32) -> Self {
        self.request.limit = count;
        self
    }

    /// None means this catalog enumeration ended; create another listing to observe later changes.
    pub async fn next_page(&mut self) -> Result<Option<Vec<T>>> {
        if !(1..=32).contains(&self.request.limit) {
            return Err(Error::Configuration {
                reason: "identity page size must be between 1 and 32",
            });
        }
        if self.complete {
            return Ok(None);
        }
        let operation = Id::new_v7()?;
        let reply: public::IdentityPage = self
            .client
            .operation(|deadline| {
                self.client.request(
                    FrameKind::ListIdentities,
                    FrameKind::IdentityPage,
                    TrafficClass::Control,
                    operation,
                    &self.request,
                    deadline,
                )
            })
            .await
            .map_err(|error| match error {
                Error::Remote {
                    code: ErrorCode::StaleGeneration,
                    ..
                } => Error::CatalogChanged,
                error => error,
            })?;
        Self::validate_page(&self.request, &reply)?;
        let entries = reply
            .entries
            .into_iter()
            .map(self.decode)
            .collect::<Result<Vec<_>>>()?;
        self.complete = reply.next_after_id.is_empty();
        self.request.after_id = reply.next_after_id;
        self.request.expected_revision = reply.revision;
        Ok((!entries.is_empty()).then_some(entries))
    }

    fn validate_page(request: &public::ListIdentities, reply: &public::IdentityPage) -> Result<()> {
        if reply.revision == 0
            || request.expected_revision != 0 && request.expected_revision != reply.revision
            || reply.entries.len() > request.limit as usize
        {
            return Err(mismatch());
        }
        let mut after = optional_id(&request.after_id)?;
        for entry in &reply.entries {
            let id = Id::from_slice(&entry.id)?;
            if after.is_some_and(|after| id <= after) || entry.parent_id != request.parent_id {
                return Err(mismatch());
            }
            after = Some(id);
        }
        if !reply.next_after_id.is_empty()
            && (reply.entries.len() != request.limit as usize
                || optional_id(&reply.next_after_id)? != after)
        {
            return Err(mismatch());
        }
        Ok(())
    }
}

impl Admin {
    pub fn tenants(&self) -> ResourceList<TenantInfo> {
        self.identity_listing(public::IdentityKind::TenantIdentities, None, tenant_info)
    }
    pub fn namespaces(&self, tenant: TenantId) -> ResourceList<NamespaceInfo> {
        self.identity_listing(
            public::IdentityKind::NamespaceIdentities,
            Some(tenant.0),
            namespace_info,
        )
    }
    pub fn principals(&self, tenant: TenantId) -> ResourceList<PrincipalInfo> {
        self.identity_listing(
            public::IdentityKind::PrincipalIdentities,
            Some(tenant.0),
            principal_info,
        )
    }
    pub fn platform_principals(&self) -> ResourceList<PrincipalInfo> {
        self.identity_listing(
            public::IdentityKind::PrincipalIdentities,
            None,
            principal_info,
        )
    }
    pub fn credentials(&self, principal: PrincipalId) -> ResourceList<CredentialInfo> {
        self.identity_listing(
            public::IdentityKind::CredentialIdentities,
            Some(principal.0),
            credential_info,
        )
    }
    fn identity_listing<T>(
        &self,
        kind: public::IdentityKind,
        parent: Option<Id>,
        decode: fn(public::IdentityInfo) -> Result<T>,
    ) -> ResourceList<T> {
        ResourceList {
            client: self.client.clone(),
            request: public::ListIdentities {
                kind: kind.into(),
                parent_id: parent.map(bytes).unwrap_or_default(),
                limit: 32,
                ..Default::default()
            },
            decode,
            complete: false,
        }
    }
}

fn mismatch() -> Error {
    Error::ResponseMismatch {
        reason: "identity page changed its revision, scope, order, or finite bounds".into(),
    }
}
fn optional_id(value: &[u8]) -> Result<Option<Id>> {
    if value.is_empty() {
        Ok(None)
    } else {
        Id::from_slice(value).map(Some).map_err(Into::into)
    }
}

fn effective_limits(limits: Option<public::ResourceLimits>) -> Result<ResourceLimits> {
    let limits = limits.ok_or_else(mismatch)?;
    limits.validate()?;
    if [
        u64::from(limits.topics),
        u64::from(limits.subscriptions),
        u64::from(limits.principals),
        u64::from(limits.credentials),
        u64::from(limits.connections),
        u64::from(limits.members),
        limits.backlog_bytes,
        limits.inflight_bytes,
        limits.inflight_messages,
        limits.publish_bytes_per_second,
        limits.consume_bytes_per_second,
        limits.cold_reads,
    ]
    .contains(&0)
    {
        return Err(mismatch());
    }
    Ok(ResourceLimits(limits))
}

fn tenant_info(row: public::IdentityInfo) -> Result<TenantInfo> {
    crate::admin::validate_name(&row.name)?;
    Ok(TenantInfo {
        id: TenantId(Id::from_slice(&row.id)?),
        name: row.name,
        status: identity_status(row.enabled, row.deleted)?,
        limits: effective_limits(row.limits)?,
    })
}
fn namespace_info(row: public::IdentityInfo) -> Result<NamespaceInfo> {
    crate::admin::validate_name(&row.name)?;
    Ok(NamespaceInfo {
        id: NamespaceId(Id::from_slice(&row.id)?),
        tenant: TenantId(Id::from_slice(&row.parent_id)?),
        name: row.name,
        status: identity_status(row.enabled, row.deleted)?,
        limits: effective_limits(row.limits)?,
    })
}
fn principal_info(row: public::IdentityInfo) -> Result<PrincipalInfo> {
    crate::admin::validate_name(&row.name)?;
    if row.deleted || row.permissions.is_empty() || row.permissions.len() > 6 {
        return Err(mismatch());
    }
    let permissions = row
        .permissions
        .iter()
        .map(|permission| match permission.as_str() {
            "manage_tenants" => Ok(Permission::ManageTenants),
            "manage_credentials" => Ok(Permission::ManageCredentials),
            "manage_topics" => Ok(Permission::ManageTopics),
            "produce" => Ok(Permission::Produce),
            "consume" => Ok(Permission::Consume),
            "inspect" => Ok(Permission::Inspect),
            _ => Err(mismatch()),
        })
        .collect::<Result<_>>()?;
    Ok(PrincipalInfo {
        id: PrincipalId(Id::from_slice(&row.id)?),
        tenant: optional_id(&row.parent_id)?.map(TenantId),
        namespace: optional_id(&row.namespace_id)?.map(NamespaceId),
        name: row.name,
        enabled: row.enabled,
        permissions,
    })
}
fn credential_info(row: public::IdentityInfo) -> Result<CredentialInfo> {
    if row.deleted || row.expires_at_ms == 0 {
        return Err(mismatch());
    }
    Ok(CredentialInfo {
        id: CredentialId(Id::from_slice(&row.id)?),
        principal: PrincipalId(Id::from_slice(&row.parent_id)?),
        enabled: row.enabled,
        expires_at_unix_ms: row.expires_at_ms,
    })
}

fn identity_status(enabled: bool, deleted: bool) -> Result<IdentityStatus> {
    match (enabled, deleted) {
        (true, false) => Ok(IdentityStatus::Active),
        (false, false) => Ok(IdentityStatus::Disabled),
        (false, true) => Ok(IdentityStatus::Deleted),
        (true, true) => Err(mismatch()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pages_reject_foreign_parents_repeated_ids_and_revision_changes_before_advancing() {
        let parent = Id::from_slice(&[1; 16]).unwrap();
        let id = Id::from_slice(&[2; 16]).unwrap();
        let mut request = public::ListIdentities {
            parent_id: bytes(parent),
            limit: 2,
            expected_revision: 7,
            ..Default::default()
        };
        let mut reply = public::IdentityPage {
            revision: 7,
            entries: vec![public::IdentityInfo {
                id: bytes(id),
                parent_id: bytes(parent),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(ResourceList::<TenantInfo>::validate_page(&request, &reply).is_ok());
        reply.revision = 8;
        assert!(ResourceList::<TenantInfo>::validate_page(&request, &reply).is_err());
        reply.revision = 7;
        request.parent_id = bytes(id);
        assert!(ResourceList::<TenantInfo>::validate_page(&request, &reply).is_err());
        request.parent_id = bytes(parent);
        reply.entries.push(reply.entries.first().unwrap().clone());
        assert!(ResourceList::<TenantInfo>::validate_page(&request, &reply).is_err());
    }
}
