use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use super::{MetadataState, NodeRole, Rejection};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataChange {
    pub id: Id,
    pub previous: BTreeSet<u64>,
    pub target: BTreeSet<u64>,
    pub expected_membership_index: u64,
    pub completed_membership_index: Option<u64>,
}

impl MetadataChange {
    pub fn complete(&self) -> bool {
        self.completed_membership_index.is_some()
    }

    fn validate(&self, state: &MetadataState) -> Result<(), Rejection> {
        if state.mode == Some(super::DeploymentMode::Local) {
            return Err(Rejection::Unsupported);
        }
        let replicas = state
            .mode
            .ok_or(Rejection::Uninitialized)?
            .replication_factor();
        if !super::control::is_random_operation_id(self.id)
            || self.previous.len() != replicas
            || self.target.len() != replicas
            || self.previous.difference(&self.target).count() != 1
            || self.target.difference(&self.previous).count() != 1
            || self
                .completed_membership_index
                .is_some_and(|index| index <= self.expected_membership_index)
        {
            return Err(Rejection::InvalidEvidence);
        }
        let mut previous_zones = BTreeSet::new();
        let mut target_zones = BTreeSet::new();
        for node in self.previous.union(&self.target) {
            let record = state.nodes.get(node).ok_or(Rejection::InvalidNode)?;
            if !record.spec.roles.contains(&NodeRole::Metadata)
                || !state.node_identities.contains_key(node)
            {
                return Err(Rejection::InvalidNode);
            }
            if self.previous.contains(node) {
                previous_zones.insert(&record.spec.zone);
            }
            if self.target.contains(node) {
                target_zones.insert(&record.spec.zone);
            }
        }
        if replicas == 3 && (previous_zones.len() != 3 || target_zones.len() != 3) {
            return Err(Rejection::PlacementViolation);
        }
        Ok(())
    }
}

impl MetadataState {
    pub fn metadata_change(&self, id: Id) -> Option<&MetadataChange> {
        self.metadata_changes.get(&id)
    }
    pub fn pending_metadata_change(&self) -> Option<&MetadataChange> {
        self.metadata_changes
            .values()
            .find(|change| !change.complete())
    }

    pub(super) fn prepare_metadata_change(
        &mut self,
        change: MetadataChange,
    ) -> Result<Id, Rejection> {
        change.validate(self)?;
        if change.complete() {
            return Err(Rejection::InvalidCommand);
        }
        if self.pending_metadata_change().is_some() {
            return Err(Rejection::OperationConflict);
        }
        if self.metadata_changes.len() >= 128 {
            return Err(Rejection::Capacity);
        }
        if self.metadata_changes.contains_key(&change.id)
            || self.metadata_changes.values().any(|old| {
                old.completed_membership_index
                    .is_some_and(|index| index > change.expected_membership_index)
            })
        {
            return Err(Rejection::OperationConflict);
        }
        for node in &change.target {
            if self
                .nodes
                .get(node)
                .is_none_or(|node| !node.spec.schedulable)
                || self
                    .node_identities
                    .get(node)
                    .is_none_or(|identity| identity.retired)
            {
                return Err(Rejection::PlacementViolation);
            }
        }
        let id = change.id;
        self.metadata_changes.insert(id, change);
        Ok(id)
    }

    pub(super) fn complete_metadata_change(
        &mut self,
        id: Id,
        membership_index: u64,
    ) -> Result<MetadataChange, Rejection> {
        if self.mode == Some(super::DeploymentMode::Local) {
            return Err(Rejection::Unsupported);
        }
        let change = self
            .metadata_changes
            .get_mut(&id)
            .ok_or(Rejection::NotFound)?;
        if membership_index <= change.expected_membership_index {
            return Err(Rejection::InvalidEvidence);
        }
        if let Some(previous) = change.completed_membership_index {
            if previous != membership_index {
                return Err(Rejection::InvalidEvidence);
            }
        } else {
            change.completed_membership_index = Some(membership_index);
        }
        Ok(change.clone())
    }

    pub(super) fn validate_metadata_changes(&self) -> Result<(), Rejection> {
        if self.metadata_changes.len() > 128
            || self
                .metadata_changes
                .values()
                .filter(|change| !change.complete())
                .count()
                > 1
        {
            return Err(Rejection::Capacity);
        }
        for (id, change) in &self.metadata_changes {
            if *id != change.id {
                return Err(Rejection::InvalidEvidence);
            }
            change.validate(self)?;
        }
        Ok(())
    }
}
