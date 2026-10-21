use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use crate::metadata::PartitionKey;

use super::{FenceRecord, Grant, RecoveredOwnership, RecoveryCheckpoint};

/// A private-constructed Raft command, not a deserializable network endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FenceCommand {
    pub(crate) grant: Grant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FenceRejection {
    InvalidIdentity,
    Capacity,
    StaleEpoch,
    EpochConflict,
    CounterExhausted,
    RecoveryRequired,
    InvalidRecovery,
}

impl std::fmt::Display for FenceRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidIdentity => {
                "Fence identity does not match this cluster, group, or partition"
            }
            Self::Capacity => "Fence partition capacity is exhausted",
            Self::StaleEpoch => "Fence epoch is older than the committed epoch",
            Self::EpochConflict => "Fence epoch is already bound to a different ownership grant",
            Self::CounterExhausted => "Fence generation cannot advance without overflow",
            Self::RecoveryRequired => "Ownership recovery must complete before writes are admitted",
            Self::InvalidRecovery => "Recovery evidence does not match the committed fence",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FenceResponse {
    Installed(FenceRecord),
    Rejected(FenceRejection),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PartitionFence {
    fence: FenceRecord,
    recovered: Option<RecoveryCheckpoint>,
}

/// Embed this complete state in the data-group application snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FenceState {
    cluster_id: Id,
    group_id: u64,
    max_partitions: usize,
    partitions: BTreeMap<PartitionKey, PartitionFence>,
}

impl FenceState {
    pub fn new(
        cluster_id: Id,
        group_id: u64,
        max_partitions: usize,
    ) -> Result<Self, FenceRejection> {
        if group_id == 0 || max_partitions == 0 || max_partitions > 8_192 {
            return Err(FenceRejection::InvalidIdentity);
        }
        Ok(Self {
            cluster_id,
            group_id,
            max_partitions,
            partitions: BTreeMap::new(),
        })
    }

    pub fn cluster_id(&self) -> Id {
        self.cluster_id
    }

    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Call only for a committed command admitted through `VerifiedGrant`.
    pub fn apply(&mut self, command: FenceCommand) -> FenceResponse {
        let grant = command.grant;
        if grant.cluster_id != self.cluster_id
            || grant.group_id != self.group_id
            || !grant.valid_identity()
        {
            return FenceResponse::Rejected(FenceRejection::InvalidIdentity);
        }
        let previous = self.partitions.get(&grant.partition);
        if let Some(previous) = previous {
            if grant.epoch < previous.fence.grant.epoch {
                return FenceResponse::Rejected(FenceRejection::StaleEpoch);
            }
            if grant.epoch == previous.fence.grant.epoch {
                return if previous.fence.grant == grant {
                    FenceResponse::Installed(previous.fence.clone())
                } else {
                    FenceResponse::Rejected(FenceRejection::EpochConflict)
                };
            }
        } else if self.partitions.len() >= self.max_partitions {
            return FenceResponse::Rejected(FenceRejection::Capacity);
        }
        let Some(generation) = previous
            .map_or(0, |previous| previous.fence.fence_generation)
            .checked_add(1)
        else {
            return FenceResponse::Rejected(FenceRejection::CounterExhausted);
        };
        let fence = FenceRecord {
            grant,
            fence_generation: generation,
        };
        self.partitions.insert(
            fence.grant.partition,
            PartitionFence {
                fence: fence.clone(),
                recovered: None,
            },
        );
        FenceResponse::Installed(fence)
    }

    pub fn fence(&self, partition: PartitionKey) -> Option<&FenceRecord> {
        self.partitions.get(&partition).map(|record| &record.fence)
    }

    pub fn recovered(&self, partition: PartitionKey) -> Option<RecoveredOwnership> {
        let record = self.partitions.get(&partition)?;
        Some(RecoveredOwnership {
            fence: record.fence.clone(),
            checkpoint: record.recovered.clone()?,
        })
    }

    /// The data application calls this after rebuilding producer and ACK state from
    /// its authoritative committed snapshot/tail. It is deliberately not a wire command.
    pub fn complete_recovery(
        &mut self,
        partition: PartitionKey,
        checkpoint: RecoveryCheckpoint,
    ) -> Result<RecoveredOwnership, FenceRejection> {
        let record = self
            .partitions
            .get_mut(&partition)
            .ok_or(FenceRejection::RecoveryRequired)?;
        if !checkpoint.validate(&record.fence) {
            return Err(FenceRejection::InvalidRecovery);
        }
        if record
            .recovered
            .as_ref()
            .is_some_and(|previous| *previous != checkpoint)
        {
            return Err(FenceRejection::InvalidRecovery);
        }
        record.recovered = Some(checkpoint.clone());
        Ok(RecoveredOwnership {
            fence: record.fence.clone(),
            checkpoint,
        })
    }

    pub fn check_writer(
        &self,
        partition: PartitionKey,
        node_id: u64,
        epoch: u64,
    ) -> Result<(), FenceRejection> {
        let record = self
            .partitions
            .get(&partition)
            .ok_or(FenceRejection::RecoveryRequired)?;
        if epoch < record.fence.grant.epoch {
            return Err(FenceRejection::StaleEpoch);
        }
        if epoch != record.fence.grant.epoch || node_id != record.fence.grant.owner_node {
            return Err(FenceRejection::EpochConflict);
        }
        if record.recovered.is_none() {
            return Err(FenceRejection::RecoveryRequired);
        }
        Ok(())
    }

    pub fn validate_snapshot(&self) -> Result<(), FenceRejection> {
        if self.group_id == 0
            || self.max_partitions == 0
            || self.max_partitions > 8_192
            || self.partitions.len() > self.max_partitions
        {
            return Err(FenceRejection::InvalidIdentity);
        }
        for (partition, record) in &self.partitions {
            let grant = &record.fence.grant;
            if grant.cluster_id != self.cluster_id
                || grant.group_id != self.group_id
                || grant.partition != *partition
                || !grant.valid_identity()
                || record.fence.fence_generation == 0
                || record
                    .recovered
                    .as_ref()
                    .is_some_and(|checkpoint| !checkpoint.validate(&record.fence))
            {
                return Err(FenceRejection::InvalidIdentity);
            }
        }
        Ok(())
    }
}
