use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use super::{MetadataState, RecordStatus, Rejection};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TopicDeletionCommand {
    Begin {
        topic: Id,
        generation: u64,
    },
    ObserveSeal {
        topic: Id,
        deletion: Id,
        domain: Id,
        end: u64,
    },
    Finish {
        topic: Id,
        deletion: Id,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicDeletion {
    pub id: Id,
    pub topic: Id,
    pub generation: u64,
    pub domains: BTreeSet<Id>,
    pub sealed: BTreeMap<Id, u64>,
    pub complete: bool,
}

impl MetadataState {
    pub fn topic_writable(&self, topic: Id) -> bool {
        self.topic(topic)
            .is_some_and(|row| row.status == RecordStatus::Active)
            && !self.topic_deletions.contains_key(&topic)
    }

    /// Sealed tombstones keep ownership routes for original results, retirement, and safe maintenance.
    pub fn topic_serviceable(&self, topic: Id) -> bool {
        self.topic(topic)
            .is_some_and(|row| row.status == RecordStatus::Active)
            || self.topic_deletion(topic).is_some_and(|plan| plan.complete)
    }

    pub fn topic_deletion(&self, topic: Id) -> Option<&TopicDeletion> {
        self.topic_deletions.get(&topic)
    }

    pub fn pending_topic_deletions(&self) -> impl Iterator<Item = &TopicDeletion> {
        self.topic_deletions.values().filter(|plan| !plan.complete)
    }

    pub(super) fn delete_topic(
        &mut self,
        operation: Id,
        command: &TopicDeletionCommand,
    ) -> Result<TopicDeletion, Rejection> {
        match *command {
            TopicDeletionCommand::Begin { topic, generation } => {
                self.begin_topic_delete(operation, topic, generation)
            }
            TopicDeletionCommand::ObserveSeal {
                topic,
                deletion,
                domain,
                end,
            } => {
                let plan = self
                    .topic_deletions
                    .get_mut(&topic)
                    .ok_or(Rejection::NotFound)?;
                if plan.id != deletion
                    || !plan.domains.contains(&domain)
                    || plan.sealed.get(&domain).is_some_and(|saved| *saved != end)
                {
                    return Err(Rejection::InvalidEvidence);
                }
                plan.sealed.insert(domain, end);
                Ok(plan.clone())
            }
            TopicDeletionCommand::Finish { topic, deletion } => {
                self.finish_topic_delete(topic, deletion)
            }
        }
    }

    fn begin_topic_delete(
        &mut self,
        operation: Id,
        topic: Id,
        generation: u64,
    ) -> Result<TopicDeletion, Rejection> {
        if let Some(plan) = self.topic_deletions.get(&topic) {
            return if plan.generation == generation {
                Ok(plan.clone())
            } else {
                Err(Rejection::GenerationConflict)
            };
        }
        self.validate_topic_delete_begin(topic, generation)?;
        let domains: BTreeSet<_> = self
            .topology
            .domains(topic)
            .map(|birth| birth.route.id)
            .collect();
        if domains.is_empty() || domains.len() > self.limits.partitions_per_topic {
            return Err(Rejection::InvalidEvidence);
        }
        let sealed = self
            .topology
            .domains(topic)
            .filter_map(|birth| birth.sealed_end.map(|end| (birth.route.id, end)))
            .collect();
        let plan = TopicDeletion {
            id: operation,
            topic,
            generation,
            domains,
            sealed,
            complete: false,
        };
        self.topic_deletions.insert(topic, plan.clone());
        Ok(plan)
    }

    fn validate_topic_delete_begin(&self, topic: Id, generation: u64) -> Result<(), Rejection> {
        let row = self.topic(topic).ok_or(Rejection::NotFound)?;
        if row.generation != generation {
            return Err(Rejection::GenerationConflict);
        }
        if row.status != RecordStatus::Active {
            return Err(Rejection::IdentityRetired);
        }
        generation
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        if self.consumer_subscriptions.values().any(|row| {
            row.topic == topic && row.deletion.as_ref().is_none_or(|plan| !plan.complete)
        }) || self
            .subscriptions
            .values()
            .any(|row| row.topic_id == topic && row.status == RecordStatus::Active)
        {
            return Err(Rejection::SubscriptionInUse);
        }
        if self.topology.pending().any(|plan| plan.topic == topic)
            || self.pending_retention(topic)
            || self
                .topology
                .pending_transfers()
                .any(|plan| plan.partition.topic_id == topic)
        {
            return Err(Rejection::RecoveryRequired);
        }
        Ok(())
    }

    fn finish_topic_delete(&mut self, topic: Id, deletion: Id) -> Result<TopicDeletion, Rejection> {
        let plan = self
            .topic_deletions
            .get(&topic)
            .ok_or(Rejection::NotFound)?;
        if plan.id != deletion {
            return Err(Rejection::OperationConflict);
        }
        if plan.complete {
            return Ok(plan.clone());
        }
        if plan.sealed.len() != plan.domains.len() {
            return Err(Rejection::RecoveryRequired);
        }
        let row = self.topics.get_mut(&topic).ok_or(Rejection::NotFound)?;
        if row.generation != plan.generation || row.status != RecordStatus::Active {
            return Err(Rejection::GenerationConflict);
        }
        row.generation = row
            .generation
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        row.status = RecordStatus::Retired;
        self.topic_names.remove(&row.name);
        let plan = self
            .topic_deletions
            .get_mut(&topic)
            .ok_or(Rejection::NotFound)?;
        plan.complete = true;
        Ok(plan.clone())
    }

    pub(super) fn validate_topic_deletions(&self) -> Result<(), Rejection> {
        if self.topic_deletions.len() > self.topics.len() {
            return Err(Rejection::Capacity);
        }
        for (topic, plan) in &self.topic_deletions {
            let row = self.topic(*topic).ok_or(Rejection::NotFound)?;
            let domains: BTreeSet<_> = self
                .topology
                .domains(*topic)
                .map(|birth| birth.route.id)
                .collect();
            if plan.topic != *topic
                || plan.domains != domains
                || domains.is_empty()
                || domains.len() > self.limits.partitions_per_topic
                || plan.sealed.keys().any(|domain| !domains.contains(domain))
                || plan.complete && plan.sealed.len() != domains.len()
                || plan.complete != (row.status == RecordStatus::Retired)
                || plan.generation.checked_add(u64::from(plan.complete)) != Some(row.generation)
                || self.consumer_subscriptions.values().any(|row| {
                    row.topic == *topic && row.deletion.as_ref().is_none_or(|plan| !plan.complete)
                })
            {
                return Err(Rejection::InvalidEvidence);
            }
        }
        Ok(())
    }
}
