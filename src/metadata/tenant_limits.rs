use serde::{Deserialize, Serialize};

use super::{AuthRejection, MetadataState, QuotaCapacity, RecordStatus, Rejection};
use tidemq_protocol::Id;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantLimits {
    pub max_topics: usize,
    pub max_subscriptions: usize,
    pub max_principals: usize,
    pub max_credentials: usize,
    pub max_connections: usize,
    pub max_members: usize,
    pub capacity: QuotaCapacity,
}

impl MetadataState {
    pub(super) fn check_member_capacity(
        &self,
        subscription: Id,
        request: &super::MemberJoin,
    ) -> Result<(), Rejection> {
        let row = self
            .consumer_subscription(subscription)
            .ok_or(Rejection::NotFound)?;
        if !self.auth.initialized()
            || row
                .members
                .member(request.member, request.expected_epoch)
                .is_some()
        {
            return Ok(());
        }
        let topic = self.topic(row.topic).ok_or(Rejection::NotFound)?;
        let tenant = topic.tenant.ok_or(Rejection::InvalidEvidence)?;
        let namespace = topic.namespace.ok_or(Rejection::InvalidEvidence)?;
        let (parent, child) = self
            .auth
            .limits(tenant, namespace)
            .map_err(Rejection::Authorization)?;
        let mut tenant_count = 0_usize;
        let mut namespace_count = 0_usize;
        for row in self.consumer_subscriptions() {
            let owner = self.topic(row.topic).ok_or(Rejection::NotFound)?;
            let count = row.members.live_count(request.now_ms);
            if owner.tenant == Some(tenant) {
                tenant_count = tenant_count.saturating_add(count);
            }
            if owner.namespace == Some(namespace) {
                namespace_count = namespace_count.saturating_add(count);
            }
        }
        if tenant_count >= parent.max_members || namespace_count >= child.max_members {
            return Err(Rejection::Capacity);
        }
        Ok(())
    }

    pub(super) fn check_topic_capacity(&self, tenant: Id, namespace: Id) -> Result<(), Rejection> {
        let (parent, child) = self
            .auth
            .limits(tenant, namespace)
            .map_err(Rejection::Authorization)?;
        let topics = || {
            self.topics
                .values()
                .filter(|topic| topic.status == RecordStatus::Active)
        };
        if topics()
            .filter(|topic| topic.tenant == Some(tenant))
            .count()
            >= parent.max_topics
            || topics()
                .filter(|topic| topic.namespace == Some(namespace))
                .count()
                >= child.max_topics
        {
            return Err(Rejection::Capacity);
        }
        Ok(())
    }

    pub(super) fn check_subscription_capacity(&self, topic: Id) -> Result<(), Rejection> {
        if !self.auth.initialized() {
            return Ok(());
        }
        let topic = self.topic(topic).ok_or(Rejection::NotFound)?;
        let tenant = topic.tenant.ok_or(Rejection::InvalidEvidence)?;
        let namespace = topic.namespace.ok_or(Rejection::InvalidEvidence)?;
        let (parent, child) = self
            .auth
            .limits(tenant, namespace)
            .map_err(Rejection::Authorization)?;
        let topics = || {
            self.consumer_subscriptions
                .values()
                .filter(|row| !row.is_deleted())
                .map(|row| row.topic)
                .chain(
                    self.subscriptions
                        .values()
                        .filter(|row| row.status == RecordStatus::Active)
                        .map(|row| row.topic_id),
                )
                .filter_map(|id| self.topic(id))
        };
        if topics()
            .filter(|topic| topic.tenant == Some(tenant))
            .count()
            >= parent.max_subscriptions
            || topics()
                .filter(|topic| topic.namespace == Some(namespace))
                .count()
                >= child.max_subscriptions
        {
            return Err(Rejection::Capacity);
        }
        Ok(())
    }
}

impl Default for TenantLimits {
    fn default() -> Self {
        let limits = tidemq_protocol::public::ResourceLimits::standard();
        Self {
            max_topics: limits.topics as usize,
            max_subscriptions: limits.subscriptions as usize,
            max_principals: limits.principals as usize,
            max_credentials: limits.credentials as usize,
            max_connections: limits.connections as usize,
            max_members: limits.members as usize,
            capacity: QuotaCapacity {
                backlog_bytes: limits.backlog_bytes,
                inflight_bytes: limits.inflight_bytes,
                inflight_messages: limits.inflight_messages,
                publish_bytes_per_second: limits.publish_bytes_per_second,
                consume_bytes_per_second: limits.consume_bytes_per_second,
                cold_reads: limits.cold_reads,
            },
        }
    }
}

impl TenantLimits {
    pub fn from_wire(
        limits: &tidemq_protocol::public::ResourceLimits,
        previous: Self,
    ) -> Result<Self, AuthRejection> {
        limits.validate().map_err(|_| AuthRejection::Invalid)?;
        let count = |value, previous| {
            if value == 0 {
                Ok(previous)
            } else {
                usize::try_from(value).map_err(|_| AuthRejection::Invalid)
            }
        };
        let bytes = |value, previous| if value == 0 { previous } else { value };
        let limits = Self {
            max_topics: count(limits.topics, previous.max_topics)?,
            max_subscriptions: count(limits.subscriptions, previous.max_subscriptions)?,
            max_principals: count(limits.principals, previous.max_principals)?,
            max_credentials: count(limits.credentials, previous.max_credentials)?,
            max_connections: count(limits.connections, previous.max_connections)?,
            max_members: count(limits.members, previous.max_members)?,
            capacity: QuotaCapacity {
                backlog_bytes: bytes(limits.backlog_bytes, previous.capacity.backlog_bytes),
                inflight_bytes: bytes(limits.inflight_bytes, previous.capacity.inflight_bytes),
                inflight_messages: bytes(
                    limits.inflight_messages,
                    previous.capacity.inflight_messages,
                ),
                publish_bytes_per_second: bytes(
                    limits.publish_bytes_per_second,
                    previous.capacity.publish_bytes_per_second,
                ),
                consume_bytes_per_second: bytes(
                    limits.consume_bytes_per_second,
                    previous.capacity.consume_bytes_per_second,
                ),
                cold_reads: bytes(limits.cold_reads, previous.capacity.cold_reads),
            },
        };
        limits.validate()?;
        Ok(limits)
    }
    pub fn validate(self) -> Result<(), AuthRejection> {
        let maxima = Self {
            max_connections: 4096,
            max_members: 65_536,
            capacity: QuotaCapacity {
                backlog_bytes: 1 << 50,
                inflight_bytes: 1 << 40,
                inflight_messages: 1 << 30,
                publish_bytes_per_second: 1 << 40,
                consume_bytes_per_second: 1 << 40,
                cold_reads: 4096,
            },
            ..Self::default()
        };
        if !self.fits(maxima) || self.counts().contains(&0) || self.capacity.values().contains(&0) {
            return Err(AuthRejection::Invalid);
        }
        Ok(())
    }

    fn counts(self) -> [usize; 6] {
        [
            self.max_topics,
            self.max_subscriptions,
            self.max_principals,
            self.max_credentials,
            self.max_connections,
            self.max_members,
        ]
    }

    pub fn fits(self, parent: Self) -> bool {
        self.counts()
            .into_iter()
            .zip(parent.counts())
            .all(|(value, limit)| value <= limit)
            && self.capacity.fits(parent.capacity)
    }

    pub fn minimum(self, parent: Self) -> Self {
        Self {
            max_topics: self.max_topics.min(parent.max_topics),
            max_subscriptions: self.max_subscriptions.min(parent.max_subscriptions),
            max_principals: self.max_principals.min(parent.max_principals),
            max_credentials: self.max_credentials.min(parent.max_credentials),
            max_connections: self.max_connections.min(parent.max_connections),
            max_members: self.max_members.min(parent.max_members),
            capacity: self.capacity.minimum(parent.capacity),
        }
    }
}
