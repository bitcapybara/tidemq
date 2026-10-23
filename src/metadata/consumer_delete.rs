use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use tidemq_protocol::{Id, topic::SubscriptionKind};

use super::{ConsumerOutcome, ConsumerSubscription, MetadataState, RecordStatus, Rejection};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConsumerDeletion {
    pub id: Id,
    pub subscription: Id,
    pub topic: Id,
    pub domains: Vec<Id>,
    pub deleted: BTreeSet<Id>,
    pub complete: bool,
}

impl ConsumerDeletion {
    pub(super) fn validate(&self, row: &ConsumerSubscription) -> Result<(), Rejection> {
        let domains: BTreeSet<_> = self.domains.iter().copied().collect();
        if self.subscription != row.id
            || self.topic != row.topic
            || self.domains.is_empty()
            || self.domains.len() > 256
            || domains.len() != self.domains.len()
            || self.domains != row.members.plan().domains
            || !self.deleted.is_subset(&domains)
            || self.complete && self.deleted != domains
            || !row.members.can_delete(u64::MAX)
        {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(())
    }
}

impl MetadataState {
    // Domain capture and join fencing must commit as one metadata transition.
    pub(super) fn begin_consumer_delete(
        &mut self,
        operation: Id,
        subscription: Id,
        model: SubscriptionKind,
        now: u64,
    ) -> Result<ConsumerOutcome, Rejection> {
        let row = self
            .consumer_subscriptions
            .get(&subscription)
            .ok_or(Rejection::NotFound)?;
        if row.members.model() != model {
            return Err(Rejection::InvalidSubscription);
        }
        if let Some(plan) = &row.deletion {
            return Ok(ConsumerOutcome::Deletion(plan.clone()));
        }
        if !row.members.can_delete(now) {
            return Err(Rejection::SubscriptionInUse);
        }
        let topic = row.topic;
        if self
            .topic(topic)
            .is_none_or(|topic| topic.status != RecordStatus::Active)
            || self.topology.pending().any(|plan| plan.topic == topic)
            || self
                .topology
                .pending_transfers()
                .any(|plan| plan.partition.topic_id == topic)
            || self
                .topology
                .layout(topic)
                .is_none_or(|layout| layout.revision != row.members.plan().layout_revision)
        {
            return Err(Rejection::RecoveryRequired);
        }
        let domains: Vec<_> = self
            .topology
            .domains(topic)
            .map(|birth| birth.route.id)
            .collect();
        let plan = ConsumerDeletion {
            id: operation,
            subscription,
            topic,
            domains,
            deleted: BTreeSet::new(),
            complete: false,
        };
        plan.validate(row)?;
        self.consumer_subscriptions
            .get_mut(&subscription)
            .ok_or(Rejection::NotFound)?
            .deletion = Some(plan.clone());
        Ok(ConsumerOutcome::Deletion(plan))
    }

    pub(super) fn observe_consumer_delete(
        &mut self,
        subscription: Id,
        deletion: Id,
        domain: Id,
    ) -> Result<ConsumerOutcome, Rejection> {
        let plan = self
            .consumer_subscriptions
            .get_mut(&subscription)
            .and_then(|row| row.deletion.as_mut())
            .ok_or(Rejection::RecoveryRequired)?;
        if plan.id != deletion || !plan.domains.contains(&domain) {
            return Err(Rejection::InvalidEvidence);
        }
        plan.deleted.insert(domain);
        Ok(ConsumerOutcome::Deletion(plan.clone()))
    }

    pub(super) fn finish_consumer_delete(
        &mut self,
        subscription: Id,
        deletion: Id,
    ) -> Result<ConsumerOutcome, Rejection> {
        let plan = self
            .consumer_subscriptions
            .get_mut(&subscription)
            .and_then(|row| row.deletion.as_mut())
            .ok_or(Rejection::RecoveryRequired)?;
        if plan.id != deletion {
            return Err(Rejection::OperationConflict);
        }
        if plan.deleted.len() != plan.domains.len() {
            return Err(Rejection::RecoveryRequired);
        }
        plan.complete = true;
        Ok(ConsumerOutcome::Deletion(plan.clone()))
    }
}
