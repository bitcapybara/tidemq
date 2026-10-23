use super::{
    Manifest, Rejection, catalog,
    data::{DataState, Intent},
};
use crate::metadata::PartitionKey;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct AuthoritySlice {
    pub partition: PartitionKey,
    pub intents: BTreeMap<tidemq_protocol::Id, Intent>,
    pub manifest: Option<Manifest>,
    pub reclaim_end: Option<u64>,
    pub checkpointed_floor: Option<u64>,
    pub source_checkpoint_generation: u64,
}

impl DataState {
    pub(crate) fn authority_slice(
        &self,
        partition: PartitionKey,
        limit: u64,
    ) -> Result<AuthoritySlice, Rejection> {
        // Bound every allocation before cloning; the fixed header needs at most 128 bytes.
        let mut remaining = limit.checked_sub(128).ok_or(Rejection::Capacity)?;
        let manifest = self.manifests.get(&partition);
        if let Some(manifest) = manifest {
            remaining = remaining
                .checked_sub(
                    crate::budget::encoded_size(manifest, remaining)
                        .map_err(|_| Rejection::Capacity)?,
                )
                .ok_or(Rejection::Capacity)?;
        }
        let mut intents = BTreeMap::new();
        for (id, intent) in self.intents.iter().filter(|(_, intent)| {
            intent
                .plan
                .descriptor
                .partitions
                .iter()
                .any(|index| catalog::key(index) == partition)
        }) {
            if self.pending.contains(id) {
                return Err(Rejection::RecoveryDependency);
            }
            remaining = remaining
                .checked_sub(
                    crate::budget::encoded_size(&(id, intent), remaining)
                        .map_err(|_| Rejection::Capacity)?,
                )
                .ok_or(Rejection::Capacity)?;
            intents.insert(*id, intent.clone());
        }
        Ok(AuthoritySlice {
            partition,
            intents,
            manifest: manifest.cloned(),
            reclaim_end: self.reclaim_ends.get(&partition).copied(),
            checkpointed_floor: self.checkpointed_floors.get(&partition).copied(),
            source_checkpoint_generation: self.checkpoint_generation,
        })
    }
}

impl AuthoritySlice {
    pub(crate) fn validate(&self) -> Result<(), Rejection> {
        if self.intents.values().any(|intent| {
            !intent
                .plan
                .descriptor
                .partitions
                .iter()
                .any(|index| catalog::key(index) == self.partition)
        }) {
            return Err(Rejection::InvalidSnapshot);
        }
        DataState {
            intents: self.intents.clone(),
            pending: Default::default(),
            manifests: self
                .manifest
                .iter()
                .cloned()
                .map(|manifest| (self.partition, manifest))
                .collect(),
            reclaim_ends: self
                .reclaim_end
                .map(|end| (self.partition, end))
                .into_iter()
                .collect(),
            checkpointed_floors: self
                .checkpointed_floor
                .map(|floor| (self.partition, floor))
                .into_iter()
                .collect(),
            checkpoint_generation: self.source_checkpoint_generation,
        }
        .validate()
    }
}
