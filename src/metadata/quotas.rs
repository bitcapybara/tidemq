use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use super::{MetadataState, PartitionKey, Rejection};

// Burst credit covers one second; retain retired rates through that window and bounded clock skew.
const RATE_REUSE_DELAY_MS: u64 = 2_000;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuotaCapacity {
    pub backlog_bytes: u64,
    pub inflight_bytes: u64,
    pub inflight_messages: u64,
    pub publish_bytes_per_second: u64,
    pub consume_bytes_per_second: u64,
    pub cold_reads: u64,
}

impl QuotaCapacity {
    pub(super) fn values(self) -> [u64; 6] {
        [
            self.backlog_bytes,
            self.inflight_bytes,
            self.inflight_messages,
            self.publish_bytes_per_second,
            self.consume_bytes_per_second,
            self.cold_reads,
        ]
    }

    pub fn fits(self, maximum: Self) -> bool {
        self.values()
            .into_iter()
            .zip(maximum.values())
            .all(|(used, limit)| used <= limit)
    }

    pub fn maximum(self, other: Self) -> Self {
        Self {
            backlog_bytes: self.backlog_bytes.max(other.backlog_bytes),
            inflight_bytes: self.inflight_bytes.max(other.inflight_bytes),
            inflight_messages: self.inflight_messages.max(other.inflight_messages),
            publish_bytes_per_second: self
                .publish_bytes_per_second
                .max(other.publish_bytes_per_second),
            consume_bytes_per_second: self
                .consume_bytes_per_second
                .max(other.consume_bytes_per_second),
            cold_reads: self.cold_reads.max(other.cold_reads),
        }
    }

    pub fn minimum(self, other: Self) -> Self {
        Self {
            backlog_bytes: self.backlog_bytes.min(other.backlog_bytes),
            inflight_bytes: self.inflight_bytes.min(other.inflight_bytes),
            inflight_messages: self.inflight_messages.min(other.inflight_messages),
            publish_bytes_per_second: self
                .publish_bytes_per_second
                .min(other.publish_bytes_per_second),
            consume_bytes_per_second: self
                .consume_bytes_per_second
                .min(other.consume_bytes_per_second),
            cold_reads: self.cold_reads.min(other.cold_reads),
        }
    }

    pub fn saturating_sub(self, other: Self) -> Self {
        Self {
            backlog_bytes: self.backlog_bytes.saturating_sub(other.backlog_bytes),
            inflight_bytes: self.inflight_bytes.saturating_sub(other.inflight_bytes),
            inflight_messages: self
                .inflight_messages
                .saturating_sub(other.inflight_messages),
            publish_bytes_per_second: self
                .publish_bytes_per_second
                .saturating_sub(other.publish_bytes_per_second),
            consume_bytes_per_second: self
                .consume_bytes_per_second
                .saturating_sub(other.consume_bytes_per_second),
            cold_reads: self.cold_reads.saturating_sub(other.cold_reads),
        }
    }

    fn rates(self) -> Self {
        Self {
            publish_bytes_per_second: self.publish_bytes_per_second,
            consume_bytes_per_second: self.consume_bytes_per_second,
            ..Self::default()
        }
    }

    pub fn checked_add(self, other: Self) -> Result<Self, Rejection> {
        let add =
            |left: u64, right: u64| left.checked_add(right).ok_or(Rejection::CounterExhausted);
        Ok(Self {
            backlog_bytes: add(self.backlog_bytes, other.backlog_bytes)?,
            inflight_bytes: add(self.inflight_bytes, other.inflight_bytes)?,
            inflight_messages: add(self.inflight_messages, other.inflight_messages)?,
            publish_bytes_per_second: add(
                self.publish_bytes_per_second,
                other.publish_bytes_per_second,
            )?,
            consume_bytes_per_second: add(
                self.consume_bytes_per_second,
                other.consume_bytes_per_second,
            )?,
            cold_reads: add(self.cold_reads, other.cold_reads)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AllocationRequest {
    pub partition: PartitionKey,
    pub tenant: Id,
    pub namespace: Id,
    pub expected_version: u64,
    pub capacity: QuotaCapacity,
    pub frozen: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuotaGrant {
    pub partition: PartitionKey,
    pub tenant: Id,
    pub namespace: Id,
    pub version: u64,
    pub capacity: QuotaCapacity,
    pub frozen: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum QuotaCommand {
    Prepare {
        request: AllocationRequest,
        now_ms: u64,
    },
    Installed {
        grant: QuotaGrant,
        now_ms: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct RetiringRates {
    capacity: QuotaCapacity,
    reusable_ms: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuotaAllocation {
    pub installed: Option<QuotaGrant>,
    pub pending: Option<QuotaGrant>,
    retiring_rates: Option<RetiringRates>,
}

impl QuotaAllocation {
    pub fn reserved(&self, now_ms: u64) -> QuotaCapacity {
        self.installed
            .as_ref()
            .map_or_default(|grant| grant.capacity)
            .maximum(self.pending.as_ref().map_or_default(|grant| grant.capacity))
            .maximum(
                self.retiring_rates
                    .as_ref()
                    .filter(|hold| hold.reusable_ms > now_ms)
                    .map_or_default(|hold| hold.capacity),
            )
    }

    fn binding(&self) -> Option<&QuotaGrant> {
        self.installed.as_ref().or(self.pending.as_ref())
    }

    fn replay(&self, request: &AllocationRequest) -> Result<Option<QuotaGrant>, Rejection> {
        let version = request
            .expected_version
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let matches = |grant: &&QuotaGrant| {
            grant.version == version
                && grant.capacity == request.capacity
                && grant.frozen == request.frozen
        };
        if let Some(grant) = self
            .pending
            .iter()
            .chain(self.installed.iter())
            .find(matches)
        {
            return Ok(Some(grant.clone()));
        }
        if self.pending.is_some() {
            return Err(Rejection::RecoveryRequired);
        }
        if self.installed.as_ref().map_or(0, |grant| grant.version) != request.expected_version {
            return Err(Rejection::Superseded);
        }
        Ok(None)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuotaLedger {
    clock_ms: u64,
    allocations: BTreeMap<PartitionKey, QuotaAllocation>,
}

impl QuotaLedger {
    pub fn allocation(&self, partition: PartitionKey) -> Option<&QuotaAllocation> {
        self.allocations.get(&partition)
    }

    pub fn allocations(&self) -> impl Iterator<Item = (&PartitionKey, &QuotaAllocation)> {
        self.allocations.iter()
    }

    pub fn reserved(
        &self,
        tenant: Id,
        namespace: Option<Id>,
        now_ms: u64,
    ) -> Result<QuotaCapacity, Rejection> {
        self.allocations
            .values()
            .filter(|record| {
                record.binding().is_some_and(|grant| {
                    grant.tenant == tenant
                        && namespace.is_none_or(|namespace| grant.namespace == namespace)
                })
            })
            .try_fold(QuotaCapacity::default(), |sum, record| {
                sum.checked_add(record.reserved(now_ms.max(self.clock_ms)))
            })
    }

    pub fn prepare(
        &mut self,
        request: AllocationRequest,
        tenant_limit: QuotaCapacity,
        namespace_limit: QuotaCapacity,
        maximum_allocations: usize,
        now_ms: u64,
    ) -> Result<QuotaGrant, Rejection> {
        let now_ms = now_ms.max(self.clock_ms);
        let record = self.allocations.get(&request.partition);
        if let Some(record) = record {
            if record.binding().is_none_or(|grant| {
                grant.tenant != request.tenant || grant.namespace != request.namespace
            }) {
                return Err(Rejection::InvalidEvidence);
            }
            if let Some(grant) = record.replay(&request)? {
                return Ok(grant);
            }
        } else if request.expected_version != 0 {
            return Err(Rejection::Superseded);
        } else if self.allocations.len() >= maximum_allocations {
            return Err(Rejection::Capacity);
        }
        let previous = record.and_then(|record| record.installed.as_ref());
        let capacity = previous.map_or_default(|grant| grant.capacity);
        let thawing = previous.is_some_and(|grant| grant.frozen && !request.frozen);
        if thawing || !request.capacity.fits(capacity) {
            self.validate_allocation_capacity(&request, tenant_limit, namespace_limit, now_ms)?;
        }
        let grant = QuotaGrant {
            partition: request.partition,
            tenant: request.tenant,
            namespace: request.namespace,
            version: request
                .expected_version
                .checked_add(1)
                .ok_or(Rejection::CounterExhausted)?,
            capacity: request.capacity,
            frozen: request.frozen,
        };
        self.clock_ms = now_ms;
        self.allocations
            .entry(request.partition)
            .or_default()
            .pending = Some(grant.clone());
        Ok(grant)
    }

    fn validate_allocation_capacity(
        &self,
        request: &AllocationRequest,
        tenant_limit: QuotaCapacity,
        namespace_limit: QuotaCapacity,
        now_ms: u64,
    ) -> Result<(), Rejection> {
        let mut tenant = request.capacity;
        let mut namespace = request.capacity;
        for (partition, record) in &self.allocations {
            if *partition == request.partition {
                continue;
            }
            let grant = record.binding().ok_or(Rejection::InvalidEvidence)?;
            if grant.tenant != request.tenant {
                continue;
            }
            tenant = tenant.checked_add(record.reserved(now_ms))?;
            if grant.namespace == request.namespace {
                namespace = namespace.checked_add(record.reserved(now_ms))?;
            }
        }
        if !tenant.fits(tenant_limit) || !namespace.fits(namespace_limit) {
            return Err(Rejection::Capacity);
        }
        Ok(())
    }

    // Releasing old credit requires confirmation of the data group's durable installation.
    pub fn observe(&mut self, grant: &QuotaGrant, now_ms: u64) -> Result<(), Rejection> {
        let now_ms = now_ms.max(self.clock_ms);
        let record = self
            .allocations
            .get_mut(&grant.partition)
            .ok_or(Rejection::NotFound)?;
        if record.installed.as_ref() == Some(grant) {
            return Ok(());
        }
        if record.pending.as_ref() != Some(grant) {
            return Err(Rejection::Superseded);
        }
        let rates = record.reserved(now_ms).rates();
        let retiring = if rates != grant.capacity.rates() {
            Some(RetiringRates {
                capacity: rates,
                reusable_ms: now_ms
                    .checked_add(RATE_REUSE_DELAY_MS)
                    .ok_or(Rejection::CounterExhausted)?,
            })
        } else {
            record.retiring_rates.clone()
        };
        record.installed = record.pending.take();
        record.retiring_rates = retiring;
        self.clock_ms = now_ms;
        Ok(())
    }

    pub fn validate(&self, maximum_allocations: usize) -> Result<(), Rejection> {
        if self.allocations.len() > maximum_allocations {
            return Err(Rejection::Capacity);
        }
        for (partition, record) in &self.allocations {
            if record
                .retiring_rates
                .as_ref()
                .is_some_and(|hold| hold.reusable_ms == 0 || hold.capacity != hold.capacity.rates())
            {
                return Err(Rejection::InvalidEvidence);
            }
            let binding = record.binding().ok_or(Rejection::InvalidEvidence)?;
            if record
                .installed
                .iter()
                .chain(record.pending.iter())
                .any(|grant| {
                    grant.partition != *partition
                        || grant.tenant != binding.tenant
                        || grant.namespace != binding.namespace
                        || grant.version == 0
                })
            {
                return Err(Rejection::InvalidEvidence);
            }
            let version = record.installed.as_ref().map_or(0, |grant| grant.version);
            if record
                .pending
                .as_ref()
                .is_some_and(|grant| version.checked_add(1) != Some(grant.version))
            {
                return Err(Rejection::InvalidEvidence);
            }
        }
        Ok(())
    }
}

impl MetadataState {
    pub fn quotas(&self) -> &QuotaLedger {
        &self.quotas
    }

    pub(super) fn quota_command(
        &mut self,
        command: &QuotaCommand,
    ) -> Result<QuotaGrant, Rejection> {
        let (partition, tenant, namespace) = match command {
            QuotaCommand::Prepare { request, .. } => {
                (request.partition, request.tenant, request.namespace)
            }
            QuotaCommand::Installed { grant, .. } => {
                (grant.partition, grant.tenant, grant.namespace)
            }
        };
        let topic = self.topic(partition.topic_id).ok_or(Rejection::NotFound)?;
        if topic.tenant != Some(tenant)
            || topic.namespace != Some(namespace)
            || self.partition_group(partition).is_none()
        {
            return Err(Rejection::InvalidEvidence);
        }
        match command {
            QuotaCommand::Prepare { request, now_ms } => {
                if let Some(record) = self.quotas.allocation(partition)
                    && let Some(grant) = record.replay(request)?
                {
                    return Ok(grant);
                }
                let previous = self
                    .quotas
                    .allocation(partition)
                    .and_then(|record| record.installed.as_ref());
                if !self.topic_writable(topic.topic_id)
                    && !request
                        .capacity
                        .fits(previous.map_or_default(|grant| grant.capacity))
                {
                    return Err(Rejection::IdentityRetired);
                }
                if self
                    .topology
                    .pending_transfers()
                    .any(|plan| plan.partition == partition)
                {
                    return Err(Rejection::RecoveryRequired);
                }
                let (tenant_limit, namespace_limit) = self
                    .auth
                    .limits(tenant, namespace)
                    .map_err(Rejection::Authorization)?;
                self.quotas.prepare(
                    request.clone(),
                    tenant_limit.capacity,
                    namespace_limit.capacity,
                    self.limits.partitions,
                    *now_ms,
                )
            }
            QuotaCommand::Installed { grant, now_ms } => {
                self.quotas.observe(grant, *now_ms)?;
                Ok(grant.clone())
            }
        }
    }

    pub(super) fn validate_quotas(&self) -> Result<(), Rejection> {
        self.quotas.validate(self.limits.partitions)?;
        for (partition, record) in self.quotas.allocations() {
            let grant = record.binding().ok_or(Rejection::InvalidEvidence)?;
            let topic = self.topic(partition.topic_id).ok_or(Rejection::NotFound)?;
            if topic.tenant != Some(grant.tenant)
                || topic.namespace != Some(grant.namespace)
                || self.partition_group(*partition).is_none()
            {
                return Err(Rejection::InvalidEvidence);
            }
            self.auth
                .limits(grant.tenant, grant.namespace)
                .map_err(Rejection::Authorization)?;
        }
        Ok(())
    }
}
