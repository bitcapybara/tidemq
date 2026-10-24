use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use super::{AuthRejection, MetadataState, NodeRole, PartitionKey, Rejection};

pub const RESOURCE_LEASE_MS: u64 = 30_000;
const MAX_RESOURCE_LEASES: usize = 16_384;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ResourceLeaseTarget {
    Connection(Id),
    ColdRead(PartitionKey),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceLease {
    pub id: Id,
    pub target: ResourceLeaseTarget,
    pub owner: u64,
    pub tenant: Id,
    pub namespace: Option<Id>,
    pub expires_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ResourceLeaseCommand {
    Acquire {
        id: Id,
        target: ResourceLeaseTarget,
        owner: u64,
        now_ms: u64,
    },
    Renew {
        id: Id,
        owner: u64,
        now_ms: u64,
    },
    Release {
        id: Id,
        owner: u64,
        now_ms: u64,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceLeases {
    clock_ms: u64,
    leases: BTreeMap<Id, ResourceLease>,
}

impl ResourceLeases {
    pub fn is_empty(&self) -> bool {
        self.leases.is_empty() && self.clock_ms == 0
    }
    pub fn validate(&self) -> Result<(), Rejection> {
        if self.leases.len() > MAX_RESOURCE_LEASES
            || self
                .leases
                .iter()
                .any(|(id, lease)| *id != lease.id || lease.owner == 0 || lease.expires_ms == 0)
        {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(())
    }

    pub fn live(&self, now_ms: u64) -> impl Iterator<Item = &ResourceLease> {
        let now_ms = now_ms.max(self.clock_ms);
        self.leases
            .values()
            .filter(move |lease| lease.expires_ms > now_ms)
    }
}

impl MetadataState {
    pub(super) fn validate_resource_leases(&self) -> Result<(), Rejection> {
        self.resource_leases.validate()?;
        for lease in self.resource_leases.leases.values() {
            if self.node(lease.owner).is_none() || self.auth.tenant(lease.tenant).is_none() {
                return Err(Rejection::InvalidEvidence);
            }
            if let Some(namespace) = lease.namespace {
                self.auth
                    .limits(lease.tenant, namespace)
                    .map_err(Rejection::Authorization)?;
            }
            match lease.target {
                ResourceLeaseTarget::Connection(credential) => {
                    let credential = self
                        .auth
                        .credential(credential)
                        .ok_or(Rejection::InvalidEvidence)?;
                    let principal = self
                        .auth
                        .principal(credential.principal)
                        .ok_or(Rejection::InvalidEvidence)?;
                    if principal.tenant != Some(lease.tenant)
                        || principal.namespace != lease.namespace
                    {
                        return Err(Rejection::InvalidEvidence);
                    }
                }
                ResourceLeaseTarget::ColdRead(partition) => {
                    let topic = self
                        .topic(partition.topic_id)
                        .ok_or(Rejection::InvalidEvidence)?;
                    if topic.tenant != Some(lease.tenant) || topic.namespace != lease.namespace {
                        return Err(Rejection::InvalidEvidence);
                    }
                }
            }
        }
        Ok(())
    }
    pub fn resource_leases(&self) -> &ResourceLeases {
        &self.resource_leases
    }

    pub(super) fn resource_lease_command(
        &mut self,
        command: &ResourceLeaseCommand,
    ) -> Result<Option<ResourceLease>, Rejection> {
        let (id, owner, now_ms) = match *command {
            ResourceLeaseCommand::Acquire {
                id, owner, now_ms, ..
            }
            | ResourceLeaseCommand::Renew { id, owner, now_ms }
            | ResourceLeaseCommand::Release { id, owner, now_ms } => {
                (id, owner, now_ms.max(self.resource_leases.clock_ms))
            }
        };
        if self.node(owner).is_none() {
            return Err(Rejection::NotFound);
        }
        self.resource_leases.clock_ms = now_ms;
        self.resource_leases
            .leases
            .retain(|_, lease| lease.expires_ms > now_ms);
        if let ResourceLeaseCommand::Release { .. } = command {
            if self
                .resource_leases
                .leases
                .get(&id)
                .is_some_and(|lease| lease.owner != owner)
            {
                return Err(Rejection::InvalidEvidence);
            }
            self.resource_leases.leases.remove(&id);
            return Ok(None);
        }
        let target = match *command {
            ResourceLeaseCommand::Acquire { target, .. } => target,
            ResourceLeaseCommand::Renew { .. } => {
                self.resource_leases
                    .leases
                    .get(&id)
                    .filter(|lease| lease.owner == owner)
                    .ok_or(Rejection::NotFound)?
                    .target
            }
            ResourceLeaseCommand::Release { .. } => return Err(Rejection::InvalidCommand),
        };
        let Some((tenant, namespace, expires_ms)) =
            self.resource_lease_scope(target, owner, now_ms)?
        else {
            return Ok(None);
        };
        if let Some(previous) = self.resource_leases.leases.get_mut(&id) {
            if previous.owner != owner
                || previous.target != target
                || previous.tenant != tenant
                || previous.namespace != namespace
            {
                return Err(Rejection::InvalidEvidence);
            }
            if matches!(command, ResourceLeaseCommand::Renew { .. }) {
                previous.expires_ms = expires_ms;
            }
            return Ok(Some(previous.clone()));
        }
        self.check_resource_lease_capacity(target, tenant, namespace, now_ms)?;
        let lease = ResourceLease {
            id,
            target,
            owner,
            tenant,
            namespace,
            expires_ms,
        };
        self.resource_leases.leases.insert(id, lease.clone());
        Ok(Some(lease))
    }

    fn resource_lease_scope(
        &self,
        target: ResourceLeaseTarget,
        owner: u64,
        now_ms: u64,
    ) -> Result<Option<(Id, Option<Id>, u64)>, Rejection> {
        let node = self.node(owner).ok_or(Rejection::NotFound)?;
        let expires_ms = now_ms
            .checked_add(RESOURCE_LEASE_MS)
            .ok_or(Rejection::CounterExhausted)?;
        match target {
            ResourceLeaseTarget::Connection(credential) => {
                if !node.spec.roles.contains(&NodeRole::Gateway)
                    && !node.spec.roles.contains(&NodeRole::Broker)
                    && !node.spec.roles.contains(&NodeRole::Storage)
                {
                    return Err(Rejection::InvalidEvidence);
                }
                let (tenant, namespace, credential_expiry) = self
                    .auth
                    .credential_scope(credential, now_ms)
                    .map_err(Rejection::Authorization)?;
                Ok(tenant.map(|tenant| (tenant, namespace, expires_ms.min(credential_expiry))))
            }
            ResourceLeaseTarget::ColdRead(partition) => {
                let group = self
                    .partition_group(partition)
                    .and_then(|group| self.group(group))
                    .ok_or(Rejection::NotFound)?;
                if !group.voters.contains(&owner) || !node.spec.roles.contains(&NodeRole::Storage) {
                    return Err(Rejection::InvalidEvidence);
                }
                let topic = self.topic(partition.topic_id).ok_or(Rejection::NotFound)?;
                Ok(Some((
                    topic.tenant.ok_or(Rejection::InvalidEvidence)?,
                    Some(topic.namespace.ok_or(Rejection::InvalidEvidence)?),
                    expires_ms,
                )))
            }
        }
    }

    fn check_resource_lease_capacity(
        &self,
        target: ResourceLeaseTarget,
        tenant: Id,
        namespace: Option<Id>,
        now_ms: u64,
    ) -> Result<(), Rejection> {
        let parent = self.auth.tenant(tenant).ok_or(Rejection::NotFound)?.limits;
        let child = namespace
            .map(|namespace| self.auth.limits(tenant, namespace))
            .transpose()
            .map_err(Rejection::Authorization)?
            .map_or(parent, |(_, child)| child);
        let same_kind = |lease: &&ResourceLease| {
            matches!(
                (lease.target, target),
                (
                    ResourceLeaseTarget::Connection(_),
                    ResourceLeaseTarget::Connection(_)
                ) | (
                    ResourceLeaseTarget::ColdRead(_),
                    ResourceLeaseTarget::ColdRead(_)
                )
            )
        };
        let limit = |limits: super::TenantLimits| match target {
            ResourceLeaseTarget::Connection(_) => limits.max_connections as u64,
            ResourceLeaseTarget::ColdRead(_) => limits.capacity.cold_reads,
        };
        if self.resource_leases.leases.len() >= MAX_RESOURCE_LEASES
            || self
                .resource_leases
                .live(now_ms)
                .filter(same_kind)
                .filter(|lease| lease.tenant == tenant)
                .count() as u64
                >= limit(parent)
            || namespace.is_some()
                && self
                    .resource_leases
                    .live(now_ms)
                    .filter(same_kind)
                    .filter(|lease| lease.namespace == namespace)
                    .count() as u64
                    >= limit(child)
        {
            return Err(Rejection::Authorization(AuthRejection::Capacity));
        }
        Ok(())
    }
}
