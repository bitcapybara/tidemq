use serde::{Deserialize, Serialize};

use super::{MetadataState, PartitionKey, Rejection};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetentionPlan {
    pub partition: PartitionKey,
    pub generation: u64,
    pub command: crate::retention::Command,
    pub completed_revision: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RetentionControl {
    Prepare {
        partition: PartitionKey,
        generation: u64,
        command: crate::retention::Command,
    },
    Finish {
        partition: PartitionKey,
        generation: u64,
        revision: u64,
    },
}

impl MetadataState {
    pub fn retention_plan(&self, partition: PartitionKey) -> Option<&RetentionPlan> {
        self.retention_plans.get(&partition)
    }

    pub(crate) fn pending_retention(&self, topic: tidemq_protocol::Id) -> bool {
        self.retention_plans
            .values()
            .any(|plan| plan.partition.topic_id == topic && plan.completed_revision.is_none())
    }

    pub(super) fn retention_control(
        &mut self,
        control: &RetentionControl,
    ) -> Result<RetentionPlan, Rejection> {
        match control {
            RetentionControl::Prepare {
                partition,
                generation,
                command,
            } => self.prepare_retention(*partition, *generation, *command),
            RetentionControl::Finish {
                partition,
                generation,
                revision,
            } => {
                let plan = self
                    .retention_plans
                    .get_mut(partition)
                    .ok_or(Rejection::NotFound)?;
                if plan.generation != *generation || *revision <= plan.command.expected_revision {
                    return Err(Rejection::InvalidEvidence);
                }
                if plan.completed_revision.is_none() {
                    plan.completed_revision = Some(*revision);
                }
                Ok(plan.clone())
            }
        }
    }

    fn prepare_retention(
        &mut self,
        partition: PartitionKey,
        generation: u64,
        command: crate::retention::Command,
    ) -> Result<RetentionPlan, Rejection> {
        if let Some(plan) = self.retention_plans.get(&partition) {
            if plan.command == command {
                return Ok(plan.clone());
            }
            if plan.completed_revision.is_none() {
                return Err(Rejection::RecoveryRequired);
            }
        }
        if self
            .retention_plans
            .get(&partition)
            .map_or(0, |plan| plan.generation)
            != generation
        {
            return Err(Rejection::GenerationConflict);
        }
        if command.before == 0
            || command.expected_revision == 0
            || command.now_ms == 0
            || !self.partitions.contains_key(&partition)
            || !self.topic_serviceable(partition.topic_id)
        {
            return Err(Rejection::InvalidCommand);
        }
        if self.topology.transfer_blocks(partition)
            || self
                .topology
                .pending()
                .any(|plan| plan.topic == partition.topic_id)
            || self
                .consumer_subscriptions
                .values()
                .any(|row| row.topic == partition.topic_id && !row.prepared && !row.is_deleted())
        {
            return Err(Rejection::RecoveryRequired);
        }
        let plan = RetentionPlan {
            partition,
            generation: generation
                .checked_add(1)
                .ok_or(Rejection::CounterExhausted)?,
            command,
            completed_revision: None,
        };
        self.retention_plans.insert(partition, plan.clone());
        Ok(plan)
    }

    pub(super) fn validate_retention_plans(&self) -> Result<(), Rejection> {
        if self.retention_plans.len() > self.partitions.len() {
            return Err(Rejection::Capacity);
        }
        for (partition, plan) in &self.retention_plans {
            if *partition != plan.partition
                || !self.partitions.contains_key(partition)
                || plan.generation == 0
                || plan.command.before == 0
                || plan.command.expected_revision == 0
                || plan.command.now_ms == 0
                || plan
                    .completed_revision
                    .is_some_and(|revision| revision <= plan.command.expected_revision)
            {
                return Err(Rejection::InvalidEvidence);
            }
        }
        Ok(())
    }
}
