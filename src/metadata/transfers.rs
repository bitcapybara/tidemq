use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use super::{PartitionKey, Rejection};
use crate::ownership::{CommitPosition, Grant, RecoveryEvidence};

pub const MAX_AUTHORITY_TRANSFERS: usize = 256;
pub const MAX_TRANSFER_BYTES: u64 = 256 * 1024 * 1024;
pub const TRANSFER_PAGE_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum TransferStage {
    Prepare,
    Frozen,
    Exported,
    Imported,
    Published,
    Checkpointed,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HistoricalBinding {
    pub shard: Id,
    pub partition: PartitionKey,
    pub origin_group: u64,
    pub authority_group: u64,
    pub generation: u64,
    pub object_groups: BTreeSet<u64>,
}

impl HistoricalBinding {
    pub(crate) fn initial(shard: Id, partition: PartitionKey, group: u64) -> Self {
        Self {
            shard,
            partition,
            origin_group: group,
            authority_group: group,
            generation: 1,
            object_groups: BTreeSet::from([group]),
        }
    }
    pub fn accepts_object(&self, group: u64) -> bool {
        self.object_groups.contains(&group)
    }
    pub fn move_to(&mut self, expected: u64, group: u64) -> Result<(), Rejection> {
        if self.generation != expected || group == 0 || group == self.authority_group {
            return Err(Rejection::GenerationConflict);
        }
        if !self.object_groups.contains(&group) && self.object_groups.len() >= 64 {
            return Err(Rejection::Capacity);
        }
        let next = self
            .generation
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        self.object_groups.insert(group);
        self.authority_group = group;
        self.generation = next;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferManifest {
    pub bytes: u64,
    pub digest: [u8; 32],
    pub source_cut: CommitPosition,
    pub committed_end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImportEvidence {
    pub manifest: TransferManifest,
    pub recovery: RecoveryEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferCheckpoint {
    pub voter: u64,
    pub position: CommitPosition,
    pub manifest_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthorityTransfer {
    pub id: Id,
    pub shard: Id,
    pub partition: PartitionKey,
    pub layout_revision: u64,
    pub binding_generation: u64,
    pub source: Grant,
    pub target: Grant,
    pub source_voters: BTreeSet<u64>,
    pub target_voters: BTreeSet<u64>,
    pub stage: TransferStage,
    pub frozen_cut: Option<CommitPosition>,
    pub manifest: Option<TransferManifest>,
    pub imported: Option<ImportEvidence>,
    pub checkpoints: BTreeMap<u64, TransferCheckpoint>,
    pub retired_cut: Option<CommitPosition>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransferStep {
    Frozen(CommitPosition),
    Exported(TransferManifest),
    Imported(ImportEvidence),
    Published,
    Checkpointed(Vec<TransferCheckpoint>),
    Retired(CommitPosition),
}

impl AuthorityTransfer {
    pub fn validate(&self) -> Result<(), Rejection> {
        if self.partition != self.source.partition
            || self.partition != self.target.partition
            || self.source.cluster_id != self.target.cluster_id
            || self.source.group_id == self.target.group_id
            || !self.source.valid_identity()
            || !self.target.valid_identity()
            || self.target.epoch <= self.source.epoch
            || self.target.expected_route_generation
                != self
                    .source
                    .expected_route_generation
                    .checked_add(1)
                    .ok_or(Rejection::CounterExhausted)?
            || self.target.metadata_revision <= self.source.metadata_revision
            || self.layout_revision == 0
            || self.binding_generation == 0
            || !matches!(self.source_voters.len(), 1 | 3)
            || self.target_voters.len() != self.source_voters.len()
            || self.source_voters.contains(&0)
            || self.target_voters.contains(&0)
            || self
                .checkpoints
                .iter()
                .any(|(node, row)| *node != row.voter)
        {
            return Err(Rejection::InvalidEvidence);
        }
        if (self.stage >= TransferStage::Frozen) != self.frozen_cut.is_some()
            || (self.stage >= TransferStage::Exported) != self.manifest.is_some()
            || (self.stage >= TransferStage::Imported) != self.imported.is_some()
            || (self.stage >= TransferStage::Checkpointed) != !self.checkpoints.is_empty()
            || (self.stage == TransferStage::Retired) != self.retired_cut.is_some()
        {
            return Err(Rejection::InvalidEvidence);
        }
        if let Some(cut) = self.frozen_cut {
            self.source_position(cut)?;
        }
        if let Some(manifest) = &self.manifest {
            self.check_manifest(manifest)?;
        }
        if let Some(imported) = &self.imported {
            self.check_import(imported)?;
        }
        if !self.checkpoints.is_empty() {
            self.check_checkpoints(&self.checkpoints.values().cloned().collect::<Vec<_>>())?;
        }
        if let Some(cut) = self.retired_cut {
            self.check_retirement(cut)?;
        }
        Ok(())
    }

    // Keep original evidence replay and its persisted stage transition in the same dispatch.
    pub fn advance(&mut self, step: TransferStep) -> Result<(), Rejection> {
        self.validate()?;
        match step {
            TransferStep::Frozen(cut) => {
                self.source_position(cut)?;
                if let Some(previous) = self.frozen_cut {
                    return exact(previous == cut);
                }
                self.require(TransferStage::Prepare)?;
                self.frozen_cut = Some(cut);
                self.stage = TransferStage::Frozen;
            }
            TransferStep::Exported(manifest) => {
                self.check_manifest(&manifest)?;
                if let Some(previous) = &self.manifest {
                    return exact(*previous == manifest);
                }
                self.require(TransferStage::Frozen)?;
                self.manifest = Some(manifest);
                self.stage = TransferStage::Exported;
            }
            TransferStep::Imported(imported) => {
                self.check_import(&imported)?;
                if let Some(previous) = &self.imported {
                    return exact(*previous == imported);
                }
                self.require(TransferStage::Exported)?;
                self.imported = Some(imported);
                self.stage = TransferStage::Imported;
            }
            TransferStep::Published => {
                if self.stage >= TransferStage::Published {
                    return Ok(());
                }
                self.require(TransferStage::Imported)?;
                self.stage = TransferStage::Published;
            }
            TransferStep::Checkpointed(checkpoints) => {
                self.check_checkpoints(&checkpoints)?;
                let checkpoints: BTreeMap<_, _> = checkpoints
                    .into_iter()
                    .map(|row| (row.voter, row))
                    .collect();
                if !self.checkpoints.is_empty() {
                    return exact(self.checkpoints == checkpoints);
                }
                self.require(TransferStage::Published)?;
                self.checkpoints = checkpoints;
                self.stage = TransferStage::Checkpointed;
            }
            TransferStep::Retired(cut) => {
                self.check_retirement(cut)?;
                if let Some(previous) = self.retired_cut {
                    return exact(previous == cut);
                }
                self.require(TransferStage::Checkpointed)?;
                self.retired_cut = Some(cut);
                self.stage = TransferStage::Retired;
            }
        }
        Ok(())
    }

    fn require(&self, stage: TransferStage) -> Result<(), Rejection> {
        if self.stage == stage {
            Ok(())
        } else {
            Err(Rejection::RecoveryRequired)
        }
    }
    fn source_position(&self, cut: CommitPosition) -> Result<(), Rejection> {
        if cut.validate() && cut.index > 0 && self.source_voters.contains(&cut.leader_node) {
            Ok(())
        } else {
            Err(Rejection::InvalidEvidence)
        }
    }
    fn check_manifest(&self, manifest: &TransferManifest) -> Result<(), Rejection> {
        if manifest.bytes == 0
            || manifest.bytes > MAX_TRANSFER_BYTES
            || manifest.digest == [0; 32]
            || self.frozen_cut != Some(manifest.source_cut)
        {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(())
    }
    fn check_import(&self, imported: &ImportEvidence) -> Result<(), Rejection> {
        let recovered = &imported.recovery.recovered;
        if self.manifest.as_ref() != Some(&imported.manifest)
            || recovered.fence.grant != self.target
            || !recovered.checkpoint.validate(&recovered.fence)
            || recovered.checkpoint.committed_end != imported.manifest.committed_end
            || !imported.recovery.committed.validate()
            || imported.recovery.committed.index == 0
            || !self
                .target_voters
                .contains(&imported.recovery.committed.leader_node)
        {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(())
    }
    fn check_checkpoints(&self, checkpoints: &[TransferCheckpoint]) -> Result<(), Rejection> {
        let imported = self.imported.as_ref().ok_or(Rejection::RecoveryRequired)?;
        let voters: BTreeSet<_> = checkpoints.iter().map(|row| row.voter).collect();
        if voters != self.target_voters
            || checkpoints.len() != voters.len()
            || checkpoints.iter().any(|row| {
                row.manifest_digest != imported.manifest.digest
                    || !row.position.validate()
                    || !self.target_voters.contains(&row.position.leader_node)
                    || row.position.index < imported.recovery.committed.index
            })
        {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(())
    }
    fn check_retirement(&self, cut: CommitPosition) -> Result<(), Rejection> {
        self.source_position(cut)?;
        if self
            .frozen_cut
            .is_none_or(|frozen| cut.index < frozen.index)
        {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(())
    }
}

fn exact(matches: bool) -> Result<(), Rejection> {
    if matches {
        Ok(())
    } else {
        Err(Rejection::OperationConflict)
    }
}
