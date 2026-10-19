use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use crate::metadata::PartitionKey;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Grant {
    pub cluster_id: Id,
    pub partition: PartitionKey,
    pub group_id: u64,
    pub operation_id: Id,
    pub epoch: u64,
    pub owner_node: u64,
    pub expected_route_generation: u64,
    pub metadata_revision: u64,
}

impl Grant {
    pub(crate) fn valid_identity(&self) -> bool {
        self.group_id > 0
            && self.metadata_revision > 0
            && self.expected_route_generation < self.epoch
            && crate::metadata::grant_operation_id(
                self.cluster_id,
                self.partition,
                self.epoch,
                self.owner_node,
                self.expected_route_generation,
            ) == Ok(self.operation_id)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnershipPhase {
    #[default]
    Unassigned,
    Granted,
    Fenced,
    Recovered,
    Active,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitPosition {
    pub term: u64,
    pub leader_node: u64,
    pub index: u64,
}

impl CommitPosition {
    pub(crate) fn validate(self) -> bool {
        self.term > 0 && self.leader_node > 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FenceRecord {
    pub grant: Grant,
    pub fence_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FenceEvidence {
    pub fence: FenceRecord,
    pub committed: CommitPosition,
}

/// A checkpoint rebuilt from the same authoritative data-group state as the fence.
///
/// These are application-state revisions and a canonical state digest, not message offsets.
/// A wire caller cannot certify recovery by supplying this structure.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecoveryCheckpoint {
    pub fence_generation: u64,
    pub producer_state_revision: u64,
    pub ack_state_revision: u64,
    pub committed_end: u64,
    pub state_digest: [u8; 32],
}

impl std::fmt::Debug for RecoveryCheckpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RecoveryCheckpoint")
            .field("fence_generation", &self.fence_generation)
            .field("producer_state_revision", &self.producer_state_revision)
            .field("ack_state_revision", &self.ack_state_revision)
            .field("committed_end", &self.committed_end)
            .field("state_digest", &"[redacted]")
            .finish()
    }
}

impl RecoveryCheckpoint {
    pub(crate) fn validate(&self, fence: &FenceRecord) -> bool {
        self.fence_generation == fence.fence_generation
            && self.producer_state_revision > 0
            && self.ack_state_revision > 0
            && self.state_digest != [0; 32]
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecoveredOwnership {
    pub fence: FenceRecord,
    pub checkpoint: RecoveryCheckpoint,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecoveryEvidence {
    pub recovered: RecoveredOwnership,
    pub committed: CommitPosition,
}
