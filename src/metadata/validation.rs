use std::collections::BTreeMap;

use tidemq_protocol::Id;

use crate::ownership::{Grant, OwnershipPhase};

use super::state::PartitionRecord;
use super::types::{valid_name, valid_topic_name};
use super::{MetadataState, NodeRole, Outcome, PartitionKey, RecordStatus, Rejection, Response};

impl MetadataState {
    /// Rejects corrupt or foreign decoded snapshots before they become authoritative.
    pub fn validate_snapshot(&self) -> Result<(), Rejection> {
        self.limits.validate()?;
        if self.topics.len() > self.limits.topics
            || self.partitions.len() > self.limits.partitions
            || self.subscriptions.len() > self.limits.subscriptions
            || self.nodes.len() > self.limits.nodes
            || self.groups.len() > self.limits.groups
            || self.decisions.len() > self.limits.operations
        {
            return Err(Rejection::Capacity);
        }
        if self.cluster_id.is_none() {
            return self.validate_uninitialized();
        }
        if self.revision == 0
            || self.mode.is_none()
            || self.metadata_group_id.is_none_or(|id| id == 0)
        {
            return Err(Rejection::InvalidGroup);
        }
        self.lifecycle.validate().map_err(Rejection::Lifecycle)?;
        self.topology.validate(self.limits.partitions_per_topic)?;
        if self
            .topology
            .shards
            .values()
            .any(|shard| self.partition_group(shard.partition) != Some(shard.route.group))
        {
            return Err(Rejection::InvalidEvidence);
        }
        self.auth.validate().map_err(Rejection::Authorization)?;
        self.publishers.validate()?;
        if self
            .publishers
            .records()
            .any(|record| !self.topics.contains_key(&record.permit.topic))
        {
            return Err(Rejection::InvalidEvidence);
        }
        self.validate_nodes_groups()?;
        self.validate_node_registry()?;
        self.validate_metadata_changes()?;
        self.validate_migrations()?;
        self.validate_topics()?;
        self.validate_subscriptions()?;
        self.validate_consumer_subscriptions()?;
        self.validate_retention_plans()?;
        self.validate_topic_deletions()?;
        self.validate_quotas()?;
        self.validate_resource_leases()?;
        self.validate_policies()?;
        for (partition, record) in &self.partitions {
            self.validate_partition(*partition, record)?;
        }
        self.validate_decisions()?;
        self.validate_control_decisions()
    }

    fn validate_uninitialized(&self) -> Result<(), Rejection> {
        if self.revision != 0
            || self.mode.is_some()
            || self.metadata_group_id.is_some()
            || !self.topics.is_empty()
            || !self.partitions.is_empty()
            || !self.subscriptions.is_empty()
            || !self.consumer_subscriptions.is_empty()
            || !self.retention_plans.is_empty()
            || !self.topic_deletions.is_empty()
            || self.quotas.allocations().next().is_some()
            || !self.resource_leases.is_empty()
            || !self.policies.is_empty()
            || !self.topic_names.is_empty()
            || !self.subscription_names.is_empty()
            || !self.nodes.is_empty()
            || !self.node_identities.is_empty()
            || self.registry_revision != 0
            || !self.groups.is_empty()
            || !self.migrations.is_empty()
            || !self.metadata_changes.is_empty()
            || self.lifecycle.object_count() != 0
            || self.lifecycle.pin_count() != 0
            || !self.topology.layouts.is_empty()
            || !self.topology.shards.is_empty()
            || !self.topology.plans.is_empty()
            || !self.topology.bindings.is_empty()
            || !self.topology.transfers.is_empty()
            || self.auth.initialized()
            || !self.control_operations.is_empty()
            || self.decisions.values().any(|decision| {
                !matches!(decision.response, Response::Rejected { revision: 0, .. })
            })
        {
            return Err(Rejection::Uninitialized);
        }
        Ok(())
    }

    fn validate_nodes_groups(&self) -> Result<(), Rejection> {
        for (id, node) in &self.nodes {
            node.spec.validate()?;
            if *id != node.spec.node_id || node.generation == 0 {
                return Err(Rejection::InvalidNode);
            }
        }
        for (id, group) in &self.groups {
            if *id == 0
                || *id != group.group_id
                || self.metadata_group_id == Some(*id)
                || group.generation == 0
            {
                return Err(Rejection::InvalidGroup);
            }
            self.validate_placement(&group.voters, false)?;
        }
        Ok(())
    }

    fn validate_topics(&self) -> Result<(), Rejection> {
        let mut names = BTreeMap::new();
        let mut partitions = 0_usize;
        for (id, topic) in &self.topics {
            self.auth
                .validate_topic_parent(topic)
                .map_err(Rejection::Authorization)?;
            if *id != topic.topic_id
                || !valid_topic_name(&topic.name)
                || topic.generation == 0
                || topic.partition_count == 0
                || topic.partition_count as usize > self.limits.partitions_per_topic
            {
                return Err(Rejection::InvalidName);
            }
            if topic.status == RecordStatus::Active
                && names.insert(topic.name.clone(), *id).is_some()
            {
                return Err(Rejection::NameInUse);
            }
            for partition in 0..topic.partition_count {
                let key = PartitionKey {
                    topic_id: *id,
                    partition,
                };
                let record = self.partitions.get(&key).ok_or(Rejection::NotFound)?;
                if !self.groups.contains_key(&record.group_id) {
                    return Err(Rejection::NotFound);
                }
                partitions = partitions
                    .checked_add(1)
                    .ok_or(Rejection::CounterExhausted)?;
            }
        }
        if names != self.topic_names || partitions != self.partitions.len() {
            return Err(Rejection::InvalidGroup);
        }
        Ok(())
    }

    fn validate_subscriptions(&self) -> Result<(), Rejection> {
        let mut names: BTreeMap<Id, BTreeMap<String, Id>> = BTreeMap::new();
        for (id, record) in &self.subscriptions {
            record.config.validate()?;
            if *id != record.subscription_id
                || !valid_name(&record.name, 128)
                || record.generation == 0
                || !self.topics.contains_key(&record.topic_id)
            {
                return Err(Rejection::InvalidSubscription);
            }
            if record.status == RecordStatus::Active
                && names
                    .entry(record.topic_id)
                    .or_default()
                    .insert(record.name.clone(), *id)
                    .is_some()
            {
                return Err(Rejection::NameInUse);
            }
        }
        if names != self.subscription_names {
            return Err(Rejection::InvalidSubscription);
        }
        Ok(())
    }

    fn validate_partition(
        &self,
        partition: PartitionKey,
        record: &PartitionRecord,
    ) -> Result<(), Rejection> {
        let Some(grant) = &record.grant else {
            return if record.highest_epoch == 0
                && record.route_generation == 0
                && record.phase == OwnershipPhase::Unassigned
                && record.fence.is_none()
                && record.recovery.is_none()
                && record.route.is_none()
                && record.control_decisions.is_empty()
            {
                Ok(())
            } else {
                Err(Rejection::InvalidEvidence)
            };
        };
        self.validate_grant(grant)?;
        if grant.partition != partition
            || grant.group_id != record.group_id
            || grant.epoch != record.highest_epoch
            || !self
                .nodes
                .get(&grant.owner_node)
                .is_some_and(|node| node.spec.roles.contains(&NodeRole::Broker))
        {
            return Err(Rejection::InvalidEvidence);
        }
        self.validate_partition_evidence(record, grant)?;
        match record.phase {
            OwnershipPhase::Granted
                if record.fence.is_none()
                    && record.recovery.is_none()
                    && record.route.is_none() => {}
            OwnershipPhase::Fenced
                if record.fence.is_some()
                    && record.recovery.is_none()
                    && record.route.is_none() => {}
            OwnershipPhase::Recovered
                if record.fence.is_some()
                    && record.recovery.is_some()
                    && record.route.is_none() => {}
            OwnershipPhase::Active
                if record.fence.is_some()
                    && record.recovery.is_some()
                    && record.route.is_some() => {}
            _ => return Err(Rejection::InvalidEvidence),
        }
        Ok(())
    }

    fn validate_partition_evidence(
        &self,
        record: &PartitionRecord,
        grant: &Grant,
    ) -> Result<(), Rejection> {
        if let Some(fence) = &record.fence {
            self.validate_recorded_evidence_identity(grant, fence.committed)?;
            if fence.fence.grant != *grant || fence.fence.fence_generation == 0 {
                return Err(Rejection::InvalidEvidence);
            }
        }
        if let Some(recovery) = &record.recovery {
            self.validate_recorded_evidence_identity(grant, recovery.committed)?;
            super::state::validate_recovery(record, recovery)?;
        }
        let expected_generation = if let Some(route) = &record.route {
            if route.partition != grant.partition
                || route.group_id != grant.group_id
                || route.owner_node != grant.owner_node
                || route.epoch != grant.epoch
                || route.ownership_operation != grant.operation_id
                || route.generation != record.route_generation
            {
                return Err(Rejection::InvalidEvidence);
            }
            grant
                .expected_route_generation
                .checked_add(1)
                .ok_or(Rejection::CounterExhausted)?
        } else {
            grant.expected_route_generation
        };
        if expected_generation != record.route_generation {
            return Err(Rejection::GenerationConflict);
        }
        Ok(())
    }

    fn validate_grant(&self, grant: &Grant) -> Result<(), Rejection> {
        if self.cluster_id != Some(grant.cluster_id)
            || !grant.valid_identity()
            || grant.metadata_revision > self.revision
            || !self.nodes.contains_key(&grant.owner_node)
            || self
                .partitions
                .get(&grant.partition)
                .is_none_or(|record| record.group_id != grant.group_id)
        {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(())
    }

    fn validate_decisions(&self) -> Result<(), Rejection> {
        for (operation_id, decision) in &self.decisions {
            if decision.digest == [0; 32]
                || decision.operation_id != *operation_id
                || !super::control::is_random_operation_id(*operation_id)
                || decision.proof_position.is_some()
            {
                return Err(Rejection::InvalidCommand);
            }
            let (revision, outcome) = match &decision.response {
                Response::Rejected { revision, .. } => (*revision, None),
                Response::Applied { revision, outcome } if *revision > 0 => {
                    (*revision, Some(outcome))
                }
                _ => return Err(Rejection::InvalidCommand),
            };
            if revision > self.revision {
                return Err(Rejection::InvalidCommand);
            }
            if let Some(outcome) = outcome {
                let binding = match outcome {
                    Outcome::Identity(result) => Some(result.request_digest),
                    Outcome::Cluster(result) => Some(result.request_digest),
                    Outcome::TopicConfigured(_) | Outcome::LayoutPrepared(_) => {
                        decision.management_digest
                    }
                    _ => None,
                };
                if decision.management_digest != binding {
                    return Err(Rejection::InvalidCommand);
                }
                self.validate_outcome(*operation_id, outcome)?;
            }
        }
        Ok(())
    }

    fn validate_outcome(&self, operation_id: Id, outcome: &Outcome) -> Result<(), Rejection> {
        match outcome {
            Outcome::Cluster(result) => match &result.outcome {
                super::ClusterOutcome::DataChange(plan) => {
                    self.validate_data_replacement_history(plan)?
                }
                super::ClusterOutcome::MetadataChange(id) => {
                    if *id != operation_id || self.metadata_change(*id).is_none() {
                        return Err(Rejection::InvalidEvidence);
                    }
                }
                super::ClusterOutcome::Node(node) => {
                    node.identity.validate(&node.record.spec)?;
                    if node.record.generation == 0 {
                        return Err(Rejection::InvalidNode);
                    }
                }
                super::ClusterOutcome::Group(group) => {
                    if group.group_id == 0 || group.generation == 0 {
                        return Err(Rejection::InvalidGroup);
                    }
                    self.validate_placement(&group.voters, false)?;
                }
            },
            Outcome::MetadataChange(change) => {
                if self
                    .metadata_change(change.id)
                    .is_none_or(|stored| stored != change.as_ref())
                {
                    return Err(Rejection::InvalidEvidence);
                }
            }
            Outcome::NodeConfigured(node) => {
                node.spec.validate()?;
                if node.generation == 0 {
                    return Err(Rejection::InvalidNode);
                }
            }
            Outcome::GroupCreated(group) => {
                if group.group_id == 0 || group.generation == 0 {
                    return Err(Rejection::InvalidGroup);
                }
                self.validate_placement(&group.voters, false)?;
            }
            Outcome::TopicConfigured(topic) => {
                if !valid_topic_name(&topic.name)
                    || topic.generation == 0
                    || topic.partition_count == 0
                    || topic.partition_count as usize > self.limits.partitions_per_topic
                {
                    return Err(Rejection::InvalidName);
                }
            }
            Outcome::SubscriptionConfigured(subscription) => {
                subscription.config.validate()?;
                if !valid_name(&subscription.name, 128) || subscription.generation == 0 {
                    return Err(Rejection::InvalidSubscription);
                }
            }
            Outcome::OwnershipGranted(grant) => {
                self.validate_grant(grant)?;
                if grant.operation_id != operation_id {
                    return Err(Rejection::InvalidEvidence);
                }
            }
            Outcome::RouteActivated(route)
                if route.generation == 0
                    || route.epoch == 0
                    || route.owner_node == 0
                    || !self.groups.contains_key(&route.group_id) =>
            {
                return Err(Rejection::InvalidEvidence);
            }
            _ => {}
        }
        Ok(())
    }
}
