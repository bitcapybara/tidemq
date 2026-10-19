use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use crate::ownership::{FenceEvidence, Grant, OwnershipPhase, RecoveryEvidence};

use super::command::Action;
use super::control::{ControlLocation, ControlStage, is_random_operation_id};
use super::types::valid_name;
use super::{
    Command, DataGroup, DeploymentMode, MetadataLimits, NodeRecord, NodeRole, Outcome,
    PartitionKey, RecordStatus, Rejection, Response, Route, SubscriptionConfig, SubscriptionRecord,
    TopicRecord,
};

const MAX_COMMAND_BYTES: usize = 65_536;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PartitionRecord {
    pub group_id: u64,
    pub highest_epoch: u64,
    pub route_generation: u64,
    pub grant: Option<Grant>,
    pub phase: OwnershipPhase,
    pub fence: Option<FenceEvidence>,
    pub recovery: Option<RecoveryEvidence>,
    pub route: Option<Route>,
    pub(super) control_decisions: BTreeMap<ControlStage, Decision>,
}

impl PartitionRecord {
    fn new(group_id: u64) -> Self {
        Self {
            group_id,
            highest_epoch: 0,
            route_generation: 0,
            grant: None,
            phase: OwnershipPhase::Unassigned,
            fence: None,
            recovery: None,
            route: None,
            control_decisions: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Decision {
    pub(super) operation_id: Id,
    pub(super) digest: [u8; 32],
    pub(super) response: Response,
    pub(super) proof_position: Option<crate::ownership::CommitPosition>,
}

/// Complete authoritative metadata, including identity tombstones and original decisions.
///
/// CRUD retirement never frees an identity or decision slot. Existing partitions retain
/// four replaceable ownership decisions, so CRUD exhaustion cannot disable takeover.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataState {
    pub(crate) cluster_id: Option<Id>,
    pub(crate) metadata_group_id: Option<u64>,
    pub(crate) mode: Option<DeploymentMode>,
    pub(crate) revision: u64,
    pub(crate) lifecycle: crate::lifecycle::Catalog,
    pub(crate) limits: MetadataLimits,
    pub(crate) topics: BTreeMap<Id, TopicRecord>,
    pub(crate) topic_names: BTreeMap<String, Id>,
    pub(crate) nodes: BTreeMap<u64, NodeRecord>,
    pub(crate) groups: BTreeMap<u64, DataGroup>,
    pub(crate) migrations: BTreeMap<u64, crate::scaling::MigrationRecord>,
    pub(crate) partitions: BTreeMap<PartitionKey, PartitionRecord>,
    pub(crate) subscriptions: BTreeMap<Id, SubscriptionRecord>,
    pub(crate) subscription_names: BTreeMap<Id, BTreeMap<String, Id>>,
    // shortcut: Retain CRUD decisions to the hard cap; review retirement fences before raising lifecycle capacity.
    pub(super) decisions: BTreeMap<Id, Decision>,
    pub(super) control_operations: BTreeMap<Id, ControlLocation>,
}

impl MetadataState {
    pub fn lifecycle(&self) -> &crate::lifecycle::Catalog {
        &self.lifecycle
    }

    pub fn subscription_by_name(&self, topic_id: Id, name: &str) -> Option<&SubscriptionRecord> {
        self.subscription_names
            .get(&topic_id)
            .and_then(|names| names.get(name))
            .and_then(|id| self.subscriptions.get(id))
            .filter(|record| record.status == RecordStatus::Active)
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn cluster_id(&self) -> Option<Id> {
        self.cluster_id
    }

    pub fn metadata_group_id(&self) -> Option<u64> {
        self.metadata_group_id
    }

    pub(crate) fn remaining_operations(&self) -> usize {
        self.limits.operations.saturating_sub(self.decisions.len())
    }

    pub fn topic(&self, topic_id: Id) -> Option<&TopicRecord> {
        self.topics.get(&topic_id)
    }

    pub fn topic_by_name(&self, name: &str) -> Option<&TopicRecord> {
        self.topic_names
            .get(name)
            .and_then(|id| self.topics.get(id))
    }

    pub fn subscription(&self, subscription_id: Id) -> Option<&SubscriptionRecord> {
        self.subscriptions.get(&subscription_id)
    }

    pub fn node(&self, node_id: u64) -> Option<&NodeRecord> {
        self.nodes.get(&node_id)
    }

    pub fn group(&self, group_id: u64) -> Option<&DataGroup> {
        self.groups.get(&group_id)
    }

    pub fn partition_group(&self, partition: PartitionKey) -> Option<u64> {
        self.partitions
            .get(&partition)
            .map(|record| record.group_id)
    }

    pub fn grant(&self, partition: PartitionKey) -> Option<&Grant> {
        self.partitions
            .get(&partition)
            .and_then(|record| record.grant.as_ref())
    }

    pub fn route(&self, partition: PartitionKey) -> Option<&Route> {
        if self.topic(partition.topic_id)?.status != RecordStatus::Active {
            return None;
        }
        self.partitions
            .get(&partition)
            .and_then(|record| record.route.as_ref())
    }

    pub fn operation_result(&self, operation_id: Id) -> Option<&Response> {
        self.decision(operation_id)
            .map(|decision| &decision.response)
    }

    pub fn lookup_operation(&self, operation_id: Id) -> super::OperationResult {
        self.operation_result(operation_id).cloned().map_or(
            super::OperationResult::UnknownOrRetired,
            super::OperationResult::Known,
        )
    }

    /// Deterministic business decisions; successful Raft transport alone is not acceptance.
    pub fn apply(&mut self, command: Command) -> Response {
        if matches!(
            command.action,
            Action::PrepareMembership { .. } | Action::CompleteMembership(_)
        ) {
            return self.apply_membership(&command);
        }
        if matches!(command.action, Action::Lifecycle(_)) {
            return self.apply_new(&command);
        }
        let digest = match self.command_digest(&command) {
            Ok(digest) => digest,
            Err(reason) => return self.reject(reason),
        };
        if let Some(location) = command.action.control_location() {
            return self.apply_control(&command, digest, location);
        }
        if !is_random_operation_id(command.operation_id) {
            return self.reject(Rejection::InvalidCommand);
        }
        if let Some(decision) = self.decision(command.operation_id) {
            return if decision.digest == digest {
                decision.response.clone()
            } else {
                self.reject(Rejection::OperationConflict)
            };
        }
        if self.decisions.len() >= self.limits.operations {
            return self.reject(Rejection::Capacity);
        }
        let response = self.apply_new(&command);
        self.decisions.insert(
            command.operation_id,
            Decision {
                operation_id: command.operation_id,
                digest,
                response: response.clone(),
                proof_position: None,
            },
        );
        response
    }

    pub(super) fn apply_new(&mut self, command: &Command) -> Response {
        if command.expected_revision != self.revision {
            return self.reject(Rejection::RevisionConflict);
        }
        let Some(next_revision) = self.revision.checked_add(1) else {
            return self.reject(Rejection::CounterExhausted);
        };
        match self.execute(command, next_revision) {
            Ok(outcome) => {
                self.revision = next_revision;
                Response::Applied {
                    revision: next_revision,
                    outcome,
                }
            }
            Err(reason) => self.reject(reason),
        }
    }

    pub(super) fn reject(&self, reason: Rejection) -> Response {
        Response::Rejected {
            revision: self.revision,
            reason,
        }
    }

    fn command_digest(&self, command: &Command) -> Result<[u8; 32], Rejection> {
        validate_command_shape(&command.action, &self.limits)?;
        if command.action.control_location().is_some() {
            return command.action.logical_digest();
        }
        let bytes = serde_json::to_vec(command).map_err(|_| Rejection::InvalidCommand)?;
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(Rejection::InvalidCommand);
        }
        Ok(*blake3::hash(&bytes).as_bytes())
    }

    fn execute(&mut self, command: &Command, revision: u64) -> Result<Outcome, Rejection> {
        if let Action::Bootstrap {
            cluster_id,
            metadata_group_id,
            mode,
            limits,
        } = &command.action
        {
            if self.cluster_id.is_some() {
                return Err(Rejection::AlreadyInitialized);
            }
            limits.validate()?;
            if *metadata_group_id == 0 {
                return Err(Rejection::InvalidGroup);
            }
            if self.decisions.len() >= limits.operations {
                return Err(Rejection::Capacity);
            }
            self.cluster_id = Some(*cluster_id);
            self.metadata_group_id = Some(*metadata_group_id);
            self.mode = Some(*mode);
            self.limits = limits.clone();
            return Ok(Outcome::Bootstrapped);
        }
        if self.cluster_id.is_none() {
            return Err(Rejection::Uninitialized);
        }
        match &command.action {
            Action::PutNode {
                spec,
                expected_generation,
            } => self.put_node(spec, *expected_generation),
            Action::CreateGroup { group_id, voters } => self.create_group(*group_id, voters),
            Action::CreateTopic {
                topic_id,
                name,
                groups,
            } => self.create_topic(*topic_id, name, groups),
            Action::RetireTopic {
                topic_id,
                expected_generation,
            } => self.retire_topic(*topic_id, *expected_generation),
            Action::PutSubscription {
                subscription_id,
                topic_id,
                name,
                expected_generation,
                config,
            } => self.put_subscription(
                *subscription_id,
                *topic_id,
                name,
                *expected_generation,
                config,
            ),
            Action::RetireSubscription {
                subscription_id,
                expected_generation,
            } => self.retire_subscription(*subscription_id, *expected_generation),
            Action::GrantOwnership {
                partition,
                owner_node,
                expected_epoch,
                expected_route_generation,
            } => self.grant_ownership(
                *partition,
                *owner_node,
                *expected_epoch,
                *expected_route_generation,
                command.operation_id,
                revision,
            ),
            Action::PrepareMembership { plan, copy_bytes } => {
                self.prepare_membership(plan, *copy_bytes, revision)
            }
            Action::CompleteMembership(proof) => self.complete_membership(proof, revision),
            Action::ObserveFence(evidence) => self.observe_fence(evidence),
            Action::ObserveRecovery(evidence) => self.observe_recovery(evidence),
            Action::ActivateRoute(evidence) => self.activate_route(evidence),
            Action::Lifecycle(operation) => self.apply_lifecycle(operation.clone()),
            Action::Bootstrap { .. } => Err(Rejection::AlreadyInitialized),
        }
    }

    fn put_node(&mut self, spec: &super::NodeSpec, expected: u64) -> Result<Outcome, Rejection> {
        spec.validate()?;
        let previous = self.nodes.get(&spec.node_id);
        let generation = previous.map_or(0, |node| node.generation);
        if generation != expected {
            return Err(Rejection::GenerationConflict);
        }
        if previous.is_none() && self.nodes.len() >= self.limits.nodes {
            return Err(Rejection::Capacity);
        }
        let storage_in_use = previous
            .is_some_and(|node| node.spec.roles.contains(&NodeRole::Storage))
            || self
                .groups
                .values()
                .any(|group| group.voters.contains(&spec.node_id));
        let broker_in_use = self.partitions.values().any(|partition| {
            partition
                .grant
                .as_ref()
                .is_some_and(|grant| grant.owner_node == spec.node_id)
        });
        if storage_in_use
            && (!spec.roles.contains(&NodeRole::Storage)
                || previous.is_some_and(|node| node.spec.zone != spec.zone))
            || broker_in_use && !spec.roles.contains(&NodeRole::Broker)
        {
            return Err(Rejection::NodeInUse);
        }
        let generation = generation
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let record = NodeRecord {
            spec: spec.clone(),
            generation,
        };
        self.nodes.insert(spec.node_id, record.clone());
        Ok(Outcome::NodeConfigured(record))
    }

    fn create_group(
        &mut self,
        group_id: u64,
        voters: &BTreeSet<u64>,
    ) -> Result<Outcome, Rejection> {
        if group_id == 0
            || self.metadata_group_id == Some(group_id)
            || self.groups.contains_key(&group_id)
        {
            return Err(Rejection::InvalidGroup);
        }
        if self.groups.len() >= self.limits.groups {
            return Err(Rejection::Capacity);
        }
        self.validate_placement(voters, true)?;
        let group = DataGroup {
            group_id,
            generation: 1,
            voters: voters.clone(),
        };
        self.groups.insert(group_id, group.clone());
        Ok(Outcome::GroupCreated(group))
    }

    pub(super) fn validate_placement(
        &self,
        voters: &BTreeSet<u64>,
        require_schedulable: bool,
    ) -> Result<(), Rejection> {
        let mode = self.mode.ok_or(Rejection::Uninitialized)?;
        if voters.len() != mode.replication_factor() {
            return Err(Rejection::PlacementViolation);
        }
        let mut zones = BTreeSet::new();
        for voter in voters {
            let node = self.nodes.get(voter).ok_or(Rejection::NotFound)?;
            if !node.spec.roles.contains(&NodeRole::Storage) {
                return Err(Rejection::PlacementViolation);
            }
            if require_schedulable && !node.spec.schedulable {
                return Err(Rejection::NotSchedulable);
            }
            zones.insert(&node.spec.zone);
        }
        if mode == DeploymentMode::ThreeReplica && zones.len() != 3 {
            return Err(Rejection::PlacementViolation);
        }
        Ok(())
    }

    fn create_topic(
        &mut self,
        topic_id: Id,
        name: &str,
        groups: &[u64],
    ) -> Result<Outcome, Rejection> {
        if self.topics.contains_key(&topic_id) {
            return Err(Rejection::IdentityRetired);
        }
        if self.topic_names.contains_key(name) {
            return Err(Rejection::NameInUse);
        }
        if self.topics.len() >= self.limits.topics
            || self
                .partitions
                .len()
                .checked_add(groups.len())
                .is_none_or(|count| count > self.limits.partitions)
        {
            return Err(Rejection::Capacity);
        }
        let mut partitions = Vec::new();
        for (partition, group_id) in groups.iter().enumerate() {
            if !self.groups.contains_key(group_id) {
                return Err(Rejection::NotFound);
            }
            let partition = u32::try_from(partition).map_err(|_| Rejection::InvalidGroup)?;
            partitions.push((
                PartitionKey {
                    topic_id,
                    partition,
                },
                PartitionRecord::new(*group_id),
            ));
        }
        let partition_count = u32::try_from(groups.len()).map_err(|_| Rejection::InvalidGroup)?;
        let topic = TopicRecord {
            topic_id,
            name: name.to_owned(),
            generation: 1,
            partition_count,
            status: RecordStatus::Active,
        };
        self.partitions.extend(partitions);
        self.topic_names.insert(name.to_owned(), topic_id);
        self.topics.insert(topic_id, topic.clone());
        Ok(Outcome::TopicConfigured(topic))
    }

    fn retire_topic(&mut self, topic_id: Id, expected: u64) -> Result<Outcome, Rejection> {
        let topic = self.topics.get_mut(&topic_id).ok_or(Rejection::NotFound)?;
        if topic.generation != expected {
            return Err(Rejection::GenerationConflict);
        }
        if topic.status == RecordStatus::Retired {
            return Err(Rejection::IdentityRetired);
        }
        let generation = topic
            .generation
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        topic.generation = generation;
        topic.status = RecordStatus::Retired;
        self.topic_names.remove(&topic.name);
        Ok(Outcome::TopicConfigured(topic.clone()))
    }

    fn put_subscription(
        &mut self,
        id: Id,
        topic_id: Id,
        name: &str,
        expected: u64,
        config: &SubscriptionConfig,
    ) -> Result<Outcome, Rejection> {
        let topic = self.topics.get(&topic_id).ok_or(Rejection::NotFound)?;
        if topic.status != RecordStatus::Active {
            return Err(Rejection::IdentityRetired);
        }
        config.validate()?;
        let previous = self.subscriptions.get(&id);
        if previous.is_some_and(|record| {
            record.status == RecordStatus::Retired
                || record.topic_id != topic_id
                || record.name != name
        }) {
            return Err(Rejection::IdentityRetired);
        }
        let generation = previous.map_or(0, |record| record.generation);
        if generation != expected {
            return Err(Rejection::GenerationConflict);
        }
        if previous.is_none() && self.subscriptions.len() >= self.limits.subscriptions {
            return Err(Rejection::Capacity);
        }
        if self
            .subscription_names
            .get(&topic_id)
            .and_then(|names| names.get(name))
            .is_some_and(|other| *other != id)
        {
            return Err(Rejection::NameInUse);
        }
        let generation = generation
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let record = SubscriptionRecord {
            subscription_id: id,
            topic_id,
            name: name.to_owned(),
            generation,
            config: config.clone(),
            status: RecordStatus::Active,
        };
        self.subscription_names
            .entry(topic_id)
            .or_default()
            .insert(name.to_owned(), id);
        self.subscriptions.insert(id, record.clone());
        Ok(Outcome::SubscriptionConfigured(record))
    }

    fn retire_subscription(&mut self, id: Id, expected: u64) -> Result<Outcome, Rejection> {
        let record = self.subscriptions.get_mut(&id).ok_or(Rejection::NotFound)?;
        if record.generation != expected {
            return Err(Rejection::GenerationConflict);
        }
        if record.status == RecordStatus::Retired {
            return Err(Rejection::IdentityRetired);
        }
        let generation = record
            .generation
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        record.generation = generation;
        record.status = RecordStatus::Retired;
        if let Some(names) = self.subscription_names.get_mut(&record.topic_id) {
            names.remove(&record.name);
            if names.is_empty() {
                self.subscription_names.remove(&record.topic_id);
            }
        }
        Ok(Outcome::SubscriptionConfigured(record.clone()))
    }

    fn grant_ownership(
        &mut self,
        partition: PartitionKey,
        owner_node: u64,
        expected_epoch: u64,
        expected_route: u64,
        operation_id: Id,
        revision: u64,
    ) -> Result<Outcome, Rejection> {
        if self
            .topics
            .get(&partition.topic_id)
            .ok_or(Rejection::NotFound)?
            .status
            != RecordStatus::Active
        {
            return Err(Rejection::IdentityRetired);
        }
        let owner = self.nodes.get(&owner_node).ok_or(Rejection::NotFound)?;
        if !owner.spec.roles.contains(&NodeRole::Broker) || !owner.spec.schedulable {
            return Err(Rejection::NotSchedulable);
        }
        let cluster_id = self.cluster_id.ok_or(Rejection::Uninitialized)?;
        let record = self
            .partitions
            .get_mut(&partition)
            .ok_or(Rejection::NotFound)?;
        if record.highest_epoch != expected_epoch || record.route_generation != expected_route {
            return Err(Rejection::GenerationConflict);
        }
        let epoch = record
            .highest_epoch
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let grant = Grant {
            cluster_id,
            partition,
            group_id: record.group_id,
            operation_id,
            epoch,
            owner_node,
            expected_route_generation: expected_route,
            metadata_revision: revision,
        };
        record.highest_epoch = epoch;
        record.grant = Some(grant.clone());
        record.phase = OwnershipPhase::Granted;
        record.fence = None;
        record.recovery = None;
        record.route = None;
        Ok(Outcome::OwnershipGranted(grant))
    }

    fn observe_fence(&mut self, evidence: &FenceEvidence) -> Result<Outcome, Rejection> {
        self.validate_evidence_identity(&evidence.fence.grant, evidence.committed)?;
        if !evidence.committed.validate() || evidence.fence.fence_generation == 0 {
            return Err(Rejection::InvalidEvidence);
        }
        let record = self.matching_grant(&evidence.fence.grant)?;
        if let Some(previous) = &record.fence {
            return if previous.fence == evidence.fence {
                Ok(Outcome::FenceObserved)
            } else {
                Err(Rejection::InvalidEvidence)
            };
        }
        record.fence = Some(evidence.clone());
        record.phase = OwnershipPhase::Fenced;
        Ok(Outcome::FenceObserved)
    }

    fn observe_recovery(&mut self, evidence: &RecoveryEvidence) -> Result<Outcome, Rejection> {
        self.validate_evidence_identity(&evidence.recovered.fence.grant, evidence.committed)?;
        let record = self.matching_grant(&evidence.recovered.fence.grant)?;
        validate_recovery(record, evidence)?;
        if let Some(previous) = &record.recovery {
            return if previous.recovered == evidence.recovered {
                Ok(Outcome::RecoveryObserved)
            } else {
                Err(Rejection::InvalidEvidence)
            };
        }
        record.recovery = Some(evidence.clone());
        record.phase = OwnershipPhase::Recovered;
        Ok(Outcome::RecoveryObserved)
    }

    fn activate_route(&mut self, evidence: &RecoveryEvidence) -> Result<Outcome, Rejection> {
        self.validate_evidence_identity(&evidence.recovered.fence.grant, evidence.committed)?;
        let record = self.matching_grant(&evidence.recovered.fence.grant)?;
        validate_recovery(record, evidence)?;
        if record
            .recovery
            .as_ref()
            .is_none_or(|previous| previous.recovered != evidence.recovered)
        {
            return Err(Rejection::RecoveryRequired);
        }
        if let Some(route) = &record.route {
            return Ok(Outcome::RouteActivated(route.clone()));
        }
        let grant = &evidence.recovered.fence.grant;
        if record.route_generation != grant.expected_route_generation {
            return Err(Rejection::GenerationConflict);
        }
        let generation = record
            .route_generation
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let route = Route {
            partition: grant.partition,
            group_id: grant.group_id,
            owner_node: grant.owner_node,
            epoch: grant.epoch,
            generation,
            ownership_operation: grant.operation_id,
        };
        record.route_generation = generation;
        record.route = Some(route.clone());
        record.phase = OwnershipPhase::Active;
        Ok(Outcome::RouteActivated(route))
    }

    fn matching_grant(&mut self, grant: &Grant) -> Result<&mut PartitionRecord, Rejection> {
        if self
            .topics
            .get(&grant.partition.topic_id)
            .ok_or(Rejection::NotFound)?
            .status
            != RecordStatus::Active
        {
            return Err(Rejection::IdentityRetired);
        }
        let record = self
            .partitions
            .get_mut(&grant.partition)
            .ok_or(Rejection::NotFound)?;
        if record.grant.as_ref() != Some(grant) {
            return Err(Rejection::Superseded);
        }
        Ok(record)
    }

    pub(super) fn validate_evidence_identity(
        &self,
        grant: &Grant,
        position: crate::ownership::CommitPosition,
    ) -> Result<(), Rejection> {
        if !position.validate()
            || self.cluster_id != Some(grant.cluster_id)
            || !self
                .groups
                .get(&grant.group_id)
                .is_some_and(|group| group.voters.contains(&position.leader_node))
        {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(())
    }
}

fn validate_command_shape(action: &Action, limits: &MetadataLimits) -> Result<(), Rejection> {
    match action {
        Action::Bootstrap { limits, .. } => limits.validate(),
        Action::PutNode { spec, .. } => spec.validate(),
        Action::CreateGroup { voters, .. } if voters.len() > 3 => Err(Rejection::InvalidGroup),
        Action::CreateTopic { name, groups, .. } => {
            if !valid_name(name, 128) {
                Err(Rejection::InvalidName)
            } else if groups.is_empty() || groups.len() > limits.partitions_per_topic {
                Err(Rejection::InvalidGroup)
            } else {
                Ok(())
            }
        }
        Action::PutSubscription { name, config, .. } => {
            if !valid_name(name, 128) {
                return Err(Rejection::InvalidName);
            }
            config.validate()
        }
        _ => Ok(()),
    }
}

pub(super) fn validate_recovery(
    record: &PartitionRecord,
    evidence: &RecoveryEvidence,
) -> Result<(), Rejection> {
    let fence = record.fence.as_ref().ok_or(Rejection::RecoveryRequired)?;
    if fence.fence != evidence.recovered.fence
        || !evidence.committed.validate()
        || evidence.committed.index < fence.committed.index
        || !evidence
            .recovered
            .checkpoint
            .validate(&evidence.recovered.fence)
    {
        return Err(Rejection::InvalidEvidence);
    }
    Ok(())
}

impl MetadataState {
    fn apply_lifecycle(
        &mut self,
        operation: crate::lifecycle::MetadataOperation,
    ) -> Result<Outcome, Rejection> {
        use crate::lifecycle::CatalogCommand;
        operation
            .validate_context(self)
            .map_err(Rejection::Lifecycle)?;
        if let CatalogCommand::Register(plan) = &operation.command {
            if self.cluster_id != Some(plan.descriptor.identity.cluster_id) {
                return Err(Rejection::InvalidEvidence);
            }
            for grant in &plan.grants {
                if self.grant(grant.partition) != Some(grant)
                    || self.route(grant.partition).is_none()
                {
                    return Err(Rejection::Superseded);
                }
            }
        }
        if let CatalogCommand::Checkpoint {
            object_id, voters, ..
        } = &operation.command
        {
            let object = self
                .lifecycle
                .object(*object_id)
                .ok_or(Rejection::NotFound)?;
            let group = self
                .group(object.descriptor().identity.group_id)
                .ok_or(Rejection::NotFound)?;
            if group.voters.iter().copied().collect::<Vec<_>>() != *voters {
                return Err(Rejection::PlacementViolation);
            }
        }
        self.lifecycle
            .apply(operation.command)
            .map(|outcome| Outcome::Lifecycle(crate::lifecycle::MetadataResult { outcome }))
            .map_err(Rejection::Lifecycle)
    }
}
