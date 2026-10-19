use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use crate::ownership::{CommitPosition, Grant};

use super::command::Action;
use super::state::Decision;
use super::{Command, MetadataState, PartitionKey, Rejection, Response};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) enum ControlStage {
    Grant,
    Fence,
    Recovery,
    Activate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ControlLocation {
    pub partition: PartitionKey,
    pub stage: ControlStage,
}

pub(crate) fn grant_operation_id(
    cluster: Id,
    partition: PartitionKey,
    epoch: u64,
    owner: u64,
    expected_route: u64,
) -> Result<Id, Rejection> {
    if epoch == 0 || owner == 0 {
        return Err(Rejection::InvalidCommand);
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq:ownership:grant:v1");
    hash.update(cluster.as_bytes());
    hash.update(partition.topic_id.as_bytes());
    hash.update(&partition.partition.to_be_bytes());
    hash.update(&epoch.to_be_bytes());
    hash.update(&owner.to_be_bytes());
    hash.update(&expected_route.to_be_bytes());
    derived_id(hash.finalize())
}

pub(crate) fn stage_operation_id(grant: &Grant, stage: ControlStage) -> Result<Id, Rejection> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq:ownership:stage:v1");
    hash.update(grant.operation_id.as_bytes());
    hash.update(&[match stage {
        ControlStage::Grant => 0,
        ControlStage::Fence => 1,
        ControlStage::Recovery => 2,
        ControlStage::Activate => 3,
    }]);
    derived_id(hash.finalize())
}

fn derived_id(hash: blake3::Hash) -> Result<Id, Rejection> {
    let mut bytes = [0_u8; 16];
    for (destination, source) in bytes.iter_mut().zip(hash.as_bytes()) {
        *destination = *source;
    }
    let version = bytes.get_mut(6).ok_or(Rejection::InvalidCommand)?;
    *version = (*version & 0x0f) | 0x80;
    let variant = bytes.get_mut(8).ok_or(Rejection::InvalidCommand)?;
    *variant = (*variant & 0x3f) | 0x80;
    Id::from_slice(&bytes).map_err(|_| Rejection::InvalidCommand)
}

pub(super) fn is_random_operation_id(id: Id) -> bool {
    id.as_bytes().get(6).is_some_and(|byte| byte >> 4 == 4)
        && id.as_bytes().get(8).is_some_and(|byte| byte & 0xc0 == 0x80)
}

impl Action {
    pub(super) fn control_location(&self) -> Option<ControlLocation> {
        let (partition, stage) = match self {
            Self::GrantOwnership { partition, .. } => (*partition, ControlStage::Grant),
            Self::ObserveFence(evidence) => (evidence.fence.grant.partition, ControlStage::Fence),
            Self::ObserveRecovery(evidence) => (
                evidence.recovered.fence.grant.partition,
                ControlStage::Recovery,
            ),
            Self::ActivateRoute(evidence) => (
                evidence.recovered.fence.grant.partition,
                ControlStage::Activate,
            ),
            _ => return None,
        };
        Some(ControlLocation { partition, stage })
    }

    pub(super) fn expected_control_id(&self, cluster: Id) -> Result<Id, Rejection> {
        match self {
            Self::GrantOwnership {
                partition,
                owner_node,
                expected_epoch,
                expected_route_generation,
            } => grant_operation_id(
                cluster,
                *partition,
                expected_epoch
                    .checked_add(1)
                    .ok_or(Rejection::CounterExhausted)?,
                *owner_node,
                *expected_route_generation,
            ),
            Self::ObserveFence(evidence) => {
                stage_operation_id(&evidence.fence.grant, ControlStage::Fence)
            }
            Self::ObserveRecovery(evidence) => {
                stage_operation_id(&evidence.recovered.fence.grant, ControlStage::Recovery)
            }
            Self::ActivateRoute(evidence) => {
                stage_operation_id(&evidence.recovered.fence.grant, ControlStage::Activate)
            }
            _ => Err(Rejection::InvalidCommand),
        }
    }

    pub(super) fn logical_digest(&self) -> Result<[u8; 32], Rejection> {
        let bytes = match self {
            Self::ObserveFence(evidence) => serde_json::to_vec(&evidence.fence),
            Self::ObserveRecovery(evidence) | Self::ActivateRoute(evidence) => {
                serde_json::to_vec(&evidence.recovered)
            }
            _ => serde_json::to_vec(self),
        }
        .map_err(|_| Rejection::InvalidCommand)?;
        Ok(*blake3::hash(&bytes).as_bytes())
    }

    pub(super) fn proof_position(&self) -> Option<CommitPosition> {
        match self {
            Self::ObserveFence(evidence) => Some(evidence.committed),
            Self::ObserveRecovery(evidence) | Self::ActivateRoute(evidence) => {
                Some(evidence.committed)
            }
            _ => None,
        }
    }
}

impl MetadataState {
    pub(super) fn decision(&self, operation: Id) -> Option<&Decision> {
        self.decisions.get(&operation).or_else(|| {
            let location = self.control_operations.get(&operation)?;
            self.partitions
                .get(&location.partition)?
                .control_decisions
                .get(&location.stage)
        })
    }

    pub(super) fn apply_control(
        &mut self,
        command: &Command,
        digest: [u8; 32],
        location: ControlLocation,
    ) -> Response {
        let Some(cluster) = self.cluster_id else {
            return self.reject(Rejection::Uninitialized);
        };
        if command.action.expected_control_id(cluster) != Ok(command.operation_id) {
            return self.reject(Rejection::OperationConflict);
        }
        if let Some(decision) = self.decision(command.operation_id) {
            return match self.validate_control_retry(command, decision, digest) {
                Ok(()) => decision.response.clone(),
                Err(reason) => self.reject(reason),
            };
        }
        let Some(record) = self.partitions.get(&location.partition) else {
            return self.reject(Rejection::NotFound);
        };
        if let Action::GrantOwnership { expected_epoch, .. } = command.action
            && expected_epoch < record.highest_epoch
        {
            return self.reject(Rejection::Superseded);
        }
        let response = self.apply_new(command);
        if response.is_applied() {
            self.record_control_decision(command, digest, location, response.clone());
        }
        response
    }

    fn validate_control_retry(
        &self,
        command: &Command,
        decision: &Decision,
        digest: [u8; 32],
    ) -> Result<(), Rejection> {
        if decision.digest != digest {
            return Err(Rejection::OperationConflict);
        }
        if let Some(position) = command.action.proof_position() {
            let minimum = decision.proof_position.ok_or(Rejection::InvalidEvidence)?;
            if !position.validate() || position.index < minimum.index {
                return Err(Rejection::InvalidEvidence);
            }
            let grant = match &command.action {
                Action::ObserveFence(evidence) => &evidence.fence.grant,
                Action::ObserveRecovery(evidence) | Action::ActivateRoute(evidence) => {
                    &evidence.recovered.fence.grant
                }
                _ => return Err(Rejection::InvalidEvidence),
            };
            self.validate_evidence_identity(grant, position)?;
        }
        Ok(())
    }

    fn record_control_decision(
        &mut self,
        command: &Command,
        digest: [u8; 32],
        location: ControlLocation,
        response: Response,
    ) {
        let Some(record) = self.partitions.get_mut(&location.partition) else {
            return;
        };
        if location.stage == ControlStage::Grant {
            for decision in record.control_decisions.values() {
                self.control_operations.remove(&decision.operation_id);
            }
            record.control_decisions.clear();
        }
        record.control_decisions.insert(
            location.stage,
            Decision {
                operation_id: command.operation_id,
                digest,
                proof_position: command.action.proof_position(),
                response,
            },
        );
        self.control_operations
            .insert(command.operation_id, location);
    }

    pub(super) fn validate_control_decisions(&self) -> Result<(), Rejection> {
        let mut index = std::collections::BTreeMap::new();
        for (partition, record) in &self.partitions {
            let required = match record.phase {
                crate::ownership::OwnershipPhase::Unassigned => 0,
                crate::ownership::OwnershipPhase::Granted => 1,
                crate::ownership::OwnershipPhase::Fenced => 2,
                crate::ownership::OwnershipPhase::Recovered => 3,
                crate::ownership::OwnershipPhase::Active => 4,
            };
            if record.control_decisions.len() != required {
                return Err(Rejection::InvalidEvidence);
            }
            for (stage, decision) in &record.control_decisions {
                self.validate_control_slot(record, *stage, decision)?;
                if index
                    .insert(
                        decision.operation_id,
                        ControlLocation {
                            partition: *partition,
                            stage: *stage,
                        },
                    )
                    .is_some()
                {
                    return Err(Rejection::OperationConflict);
                }
            }
        }
        if index != self.control_operations {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(())
    }

    fn validate_control_slot(
        &self,
        record: &super::state::PartitionRecord,
        stage: ControlStage,
        decision: &Decision,
    ) -> Result<(), Rejection> {
        let grant = record.grant.as_ref().ok_or(Rejection::InvalidEvidence)?;
        let (action, expected_outcome) = match stage {
            ControlStage::Grant => (
                Action::GrantOwnership {
                    partition: grant.partition,
                    owner_node: grant.owner_node,
                    expected_epoch: grant
                        .epoch
                        .checked_sub(1)
                        .ok_or(Rejection::InvalidEvidence)?,
                    expected_route_generation: grant.expected_route_generation,
                },
                super::Outcome::OwnershipGranted(grant.clone()),
            ),
            ControlStage::Fence => (
                Action::ObserveFence(record.fence.clone().ok_or(Rejection::InvalidEvidence)?),
                super::Outcome::FenceObserved,
            ),
            ControlStage::Recovery => (
                Action::ObserveRecovery(record.recovery.clone().ok_or(Rejection::InvalidEvidence)?),
                super::Outcome::RecoveryObserved,
            ),
            ControlStage::Activate => (
                Action::ActivateRoute(record.recovery.clone().ok_or(Rejection::InvalidEvidence)?),
                super::Outcome::RouteActivated(
                    record.route.clone().ok_or(Rejection::InvalidEvidence)?,
                ),
            ),
        };
        if decision.operation_id != action.expected_control_id(grant.cluster_id)?
            || decision.digest != action.logical_digest()?
        {
            return Err(Rejection::InvalidEvidence);
        }
        match &decision.response {
            Response::Applied { revision, outcome }
                if *revision >= grant.metadata_revision
                    && *revision <= self.revision
                    && *outcome == expected_outcome
                    && (stage != ControlStage::Grant || *revision == grant.metadata_revision) => {}
            _ => return Err(Rejection::InvalidEvidence),
        }
        match (stage, decision.proof_position) {
            (ControlStage::Grant, None) => {}
            (ControlStage::Grant, Some(_)) | (_, None) => return Err(Rejection::InvalidEvidence),
            (_, Some(position)) => {
                self.validate_evidence_identity(grant, position)?;
                let minimum = action.proof_position().ok_or(Rejection::InvalidEvidence)?;
                if position.index < minimum.index {
                    return Err(Rejection::InvalidEvidence);
                }
            }
        }
        Ok(())
    }
}
