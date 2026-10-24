#[cfg(test)]
#[path = "../../tests/scalable_support/data_quotas.rs"]
mod tests;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use super::{DataApplication, DataRejection, DataResponse, PartitionState, WriterFence};
use crate::metadata::{PartitionKey, QuotaCapacity, QuotaGrant};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Snafu)]
pub enum QuotaRejection {
    #[snafu(display("Tenant quota has not been installed in the data authority"))]
    Missing,
    #[snafu(display("Tenant quota grant changed its immutable identity or content"))]
    InvalidGrant,
    #[snafu(display("Tenant quota grant is stale or skips an installation version"))]
    StaleGrant,
    #[snafu(display("Protected backlog or live deliveries still occupy the requested quota"))]
    Occupied,
    #[snafu(display("The tenant allocation is exhausted or suspended"))]
    Exhausted,
    #[snafu(display("Tenant quota arithmetic overflow"))]
    Arithmetic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct RateState {
    available: u64,
    last_ms: u64,
    remainder: u64,
}

impl RateState {
    fn new(rate: u64, now_ms: u64) -> Self {
        Self {
            available: rate,
            last_ms: now_ms,
            remainder: 0,
        }
    }

    fn refill(self, rate: u64, now_ms: u64) -> Self {
        let now_ms = now_ms.max(self.last_ms);
        let refill = u128::from(rate) * u128::from(now_ms.saturating_sub(self.last_ms))
            + u128::from(self.remainder);
        let available = u128::from(self.available) + refill / 1_000;
        Self {
            available: available.min(u128::from(rate)) as u64,
            last_ms: now_ms,
            remainder: if available >= u128::from(rate) {
                0
            } else {
                (refill % 1_000) as u64
            },
        }
    }

    fn reserve(self, rate: u64, bytes: u64, now_ms: u64) -> Result<Self, QuotaRejection> {
        let mut next = self.refill(rate, now_ms);
        next.available = next
            .available
            .checked_sub(bytes)
            .ok_or(QuotaRejection::Exhausted)?;
        Ok(next)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct TenantQuotaState {
    pub grant: QuotaGrant,
    last_activity_ms: u64,
    publish: RateState,
    consume: RateState,
    publish_activated: bool,
    consume_activated: bool,
}

impl TenantQuotaState {
    pub fn new(grant: QuotaGrant, now_ms: u64) -> Result<Self, QuotaRejection> {
        if grant.version != 1 {
            return Err(QuotaRejection::StaleGrant);
        }
        Ok(Self {
            last_activity_ms: now_ms,
            publish: RateState::new(grant.capacity.publish_bytes_per_second, now_ms),
            consume: RateState::new(grant.capacity.consume_bytes_per_second, now_ms),
            publish_activated: grant.capacity.publish_bytes_per_second > 0,
            consume_activated: grant.capacity.consume_bytes_per_second > 0,
            grant,
        })
    }

    pub fn update(
        &self,
        grant: QuotaGrant,
        usage: QuotaCapacity,
        now_ms: u64,
    ) -> Result<Self, QuotaRejection> {
        if grant == self.grant {
            return Ok(self.clone());
        }
        if grant.partition != self.grant.partition
            || grant.tenant != self.grant.tenant
            || grant.namespace != self.grant.namespace
        {
            return Err(QuotaRejection::InvalidGrant);
        }
        if self.grant.version.checked_add(1) != Some(grant.version) {
            return Err(QuotaRejection::StaleGrant);
        }
        if !usage.fits(grant.capacity) {
            return Err(QuotaRejection::Occupied);
        }
        let mut publish = self
            .publish
            .refill(self.grant.capacity.publish_bytes_per_second, now_ms)
            .refill(grant.capacity.publish_bytes_per_second, now_ms);
        let mut consume = self
            .consume
            .refill(self.grant.capacity.consume_bytes_per_second, now_ms)
            .refill(grant.capacity.consume_bytes_per_second, now_ms);
        // Each dimension receives one initial burst, including subscriptions created before traffic.
        if !self.publish_activated {
            publish = RateState::new(grant.capacity.publish_bytes_per_second, now_ms);
        }
        if !self.consume_activated {
            consume = RateState::new(grant.capacity.consume_bytes_per_second, now_ms);
        }
        let publish_activated =
            self.publish_activated || grant.capacity.publish_bytes_per_second > 0;
        let consume_activated =
            self.consume_activated || grant.capacity.consume_bytes_per_second > 0;
        Ok(Self {
            grant,
            last_activity_ms: self.last_activity_ms,
            publish,
            consume,
            publish_activated,
            consume_activated,
        })
    }

    pub fn publish(
        &self,
        backlog: u64,
        protected: bool,
        bytes: u64,
        now_ms: u64,
    ) -> Result<RateState, QuotaRejection> {
        if self.grant.frozen
            || protected
                && backlog
                    .checked_add(bytes)
                    .ok_or(QuotaRejection::Arithmetic)?
                    > self.grant.capacity.backlog_bytes
        {
            return Err(QuotaRejection::Exhausted);
        }
        self.publish
            .reserve(self.grant.capacity.publish_bytes_per_second, bytes, now_ms)
    }

    pub fn commit_publish(&mut self, next: RateState) {
        self.last_activity_ms = self.last_activity_ms.max(next.last_ms);
        self.publish = next;
    }

    pub fn read_budget(
        &self,
        usage: QuotaCapacity,
        now_ms: u64,
    ) -> Result<crate::subscription::ReadBudget, QuotaRejection> {
        if self.grant.frozen {
            return Err(QuotaRejection::Exhausted);
        }
        Ok(crate::subscription::ReadBudget {
            bytes: self
                .grant
                .capacity
                .inflight_bytes
                .saturating_sub(usage.inflight_bytes)
                .min(
                    self.consume
                        .refill(self.grant.capacity.consume_bytes_per_second, now_ms)
                        .available,
                ),
            messages: self
                .grant
                .capacity
                .inflight_messages
                .saturating_sub(usage.inflight_messages),
            results: u32::MAX,
        })
    }

    pub fn commit_consume(&mut self, bytes: u64, now_ms: u64) {
        self.consume = self
            .consume
            .refill(self.grant.capacity.consume_bytes_per_second, now_ms);
        // Selection is bounded by read_budget in the same data Raft application step.
        self.consume.available = self.consume.available.saturating_sub(bytes);
        if bytes > 0 {
            self.last_activity_ms = self.last_activity_ms.max(now_ms);
        }
    }

    pub fn validate(&self) -> Result<(), QuotaRejection> {
        if self.grant.version == 0
            || self.publish.available > self.grant.capacity.publish_bytes_per_second
            || self.consume.available > self.grant.capacity.consume_bytes_per_second
            || self.publish.remainder >= 1_000
            || self.consume.remainder >= 1_000
        {
            return Err(QuotaRejection::InvalidGrant);
        }
        Ok(())
    }
}

impl PartitionState {
    pub(super) fn quota_usage(&self) -> Result<QuotaCapacity, DataRejection> {
        let mut usage = QuotaCapacity {
            backlog_bytes: self
                .retention
                .logical_backlog_upper_bytes(self.protected_before())
                .map_err(DataRejection::Retention)?,
            ..Default::default()
        };
        for cursor in self.groups.values() {
            usage.inflight_bytes = usage
                .inflight_bytes
                .checked_add(cursor.in_flight_bytes())
                .ok_or(DataRejection::InvalidConfiguration)?;
            usage.inflight_messages = usage
                .inflight_messages
                .checked_add(cursor.in_flight_messages())
                .ok_or(DataRejection::InvalidConfiguration)?;
        }
        Ok(usage)
    }
}

impl DataApplication {
    pub(crate) fn quota_partitions(&self, after: Option<PartitionKey>) -> Vec<PartitionKey> {
        self.partitions
            .keys()
            .filter(|partition| {
                after.is_none_or(|after| **partition > after)
                    && !self.authority.is_frozen(**partition)
            })
            .take(4)
            .copied()
            .collect()
    }

    pub fn quota_last_activity(&self, partition: PartitionKey) -> Option<u64> {
        Some(
            self.partitions
                .get(&partition)?
                .quota
                .as_ref()?
                .last_activity_ms,
        )
    }
    pub fn publish_quota_demand(
        &self,
        partition: PartitionKey,
        request: &crate::producer::PublishRequest,
    ) -> Result<Option<QuotaCapacity>, DataRejection> {
        let state = self
            .partitions
            .get(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        if state.seal.is_some()
            || state.producer.publisher_closed(request.identity)
            || state
                .producer
                .publish_result(request.identity, request.digest)
                .map_err(DataRejection::Producer)?
                .is_some()
        {
            return Ok(None);
        }
        let bytes = request
            .message
            .logical_bytes()
            .ok_or(DataRejection::Quota(QuotaRejection::Arithmetic))?;
        let backlog_bytes = match state.protected_before() {
            Some(before) => state
                .retention
                .logical_backlog_upper_bytes(Some(before))
                .map_err(DataRejection::Retention)?
                .checked_add(bytes)
                .ok_or(DataRejection::Quota(QuotaRejection::Arithmetic))?,
            None => 0,
        };
        Ok(Some(QuotaCapacity {
            backlog_bytes,
            publish_bytes_per_second: bytes,
            ..Default::default()
        }))
    }

    pub fn protection_quota_demand(
        &self,
        partition: PartitionKey,
        start: u64,
    ) -> Result<QuotaCapacity, DataRejection> {
        let state = self
            .partitions
            .get(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        let before = state
            .protected_before()
            .map_or(start, |previous| previous.min(start));
        Ok(QuotaCapacity {
            backlog_bytes: state
                .retention
                .logical_backlog_upper_bytes(Some(before))
                .map_err(DataRejection::Retention)?,
            ..Default::default()
        })
    }

    pub fn tenant_quota(&self, partition: PartitionKey) -> Option<&QuotaGrant> {
        Some(&self.partitions.get(&partition)?.quota.as_ref()?.grant)
    }

    pub fn tenant_quota_usage(
        &self,
        partition: PartitionKey,
    ) -> Result<QuotaCapacity, DataRejection> {
        self.partitions
            .get(&partition)
            .ok_or(DataRejection::PartitionMissing)?
            .quota_usage()
    }

    pub(super) fn install_tenant_quota(
        &mut self,
        partition: PartitionKey,
        writer: WriterFence,
        grant: QuotaGrant,
        now_ms: u64,
    ) -> Result<DataResponse, DataRejection> {
        self.check_writer(partition, writer)?;
        if grant.partition != partition {
            return Err(DataRejection::Quota(QuotaRejection::InvalidGrant));
        }
        let state = self
            .partitions
            .get_mut(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        let usage = state.quota_usage()?;
        if !usage.fits(grant.capacity) {
            return Err(DataRejection::Quota(QuotaRejection::Occupied));
        }
        let quota = match &state.quota {
            Some(quota) => quota.update(grant.clone(), usage, now_ms),
            None => TenantQuotaState::new(grant.clone(), now_ms),
        }
        .map_err(DataRejection::Quota)?;
        state.quota = Some(quota);
        Ok(DataResponse::QuotaInstalled(grant))
    }
}
