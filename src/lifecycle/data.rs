use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use crate::metadata::PartitionKey;

use super::{
    catalog::{key, validate_plan},
    types::{DataCommand, DataOutcome, IntentPhase, MAX_OBJECTS, Plan, Rejection},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Intent {
    pub plan: Plan,
    pub phase: IntentPhase,
    pub generation: u64,
    pub checkpoint_generation: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataState {
    pub(super) intents: BTreeMap<Id, Intent>,
    pub(super) pending: std::collections::BTreeSet<Id>,
    pub(super) manifests: BTreeMap<PartitionKey, super::Manifest>,
    pub(super) reclaim_ends: BTreeMap<PartitionKey, u64>,
    pub(super) checkpoint_generation: u64,
    pub(super) checkpointed_floors: BTreeMap<PartitionKey, u64>,
}

impl DataState {
    pub fn recovery_digest(&self) -> Result<[u8; 32], Rejection> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"tidemq-lifecycle-data-v1\0");
        postcard::to_io(self, &mut hash).map_err(|_| Rejection::InvalidSnapshot)?;
        Ok(*hash.finalize().as_bytes())
    }
    pub fn checkpointed_floor(&self, partition: PartitionKey) -> u64 {
        self.checkpointed_floors
            .get(&partition)
            .copied()
            .unwrap_or(0)
    }

    pub fn object_count(&self) -> usize {
        self.intents.len()
    }

    pub fn pending_objects(&self, after: Option<Id>, limit: usize) -> Result<Vec<Id>, Rejection> {
        use std::ops::Bound::{Excluded, Unbounded};
        if limit == 0 || limit > 64 {
            return Err(Rejection::Capacity);
        }
        Ok(self
            .pending
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .take(limit)
            .copied()
            .collect())
    }
    pub fn published_end(&self, partition: PartitionKey) -> u64 {
        self.manifests
            .get(&partition)
            .map_or(0, |m| m.published_end)
    }
    pub fn local_reclaim_end(&self, partition: PartitionKey) -> u64 {
        self.reclaim_ends.get(&partition).copied().unwrap_or(0)
    }
    pub fn manifest(&self, partition: PartitionKey) -> Option<&super::Manifest> {
        self.manifests.get(&partition)
    }
    pub(crate) fn intent(&self, id: Id) -> Option<&Intent> {
        self.intents.get(&id)
    }

    pub(crate) fn verify_delivery(
        &self,
        proof: &super::VerifiedColdDelivery,
    ) -> Result<(), Rejection> {
        let manifest = self
            .manifests
            .get(&proof.partition)
            .ok_or(Rejection::NotPublished)?;
        if manifest.generation != proof.manifest_generation
            || !manifest.segments.iter().any(|s| {
                s.object_id == proof.object_id
                    && s.offsets.start <= proof.offset
                    && proof.offset < s.offsets.end
            })
        {
            return Err(Rejection::StaleGeneration);
        }
        Ok(())
    }

    pub(crate) fn apply(&mut self, command: DataCommand) -> Result<DataOutcome, Rejection> {
        match command {
            DataCommand::FloorCheckpoint { partition, before } => {
                self.checkpointed_floors
                    .entry(partition)
                    .and_modify(|floor| *floor = (*floor).max(before))
                    .or_insert(before);
                if let Some(manifest) = self.manifests.get_mut(&partition) {
                    manifest
                        .segments
                        .retain(|segment| segment.offsets.end > before);
                }
                Ok(DataOutcome::FloorCheckpoint)
            }
            DataCommand::Authorize(plan) => self.authorize(plan),
            DataCommand::Abort(plan) => self.abort(plan),
            DataCommand::Published { plan, generations } => self.published(plan, &generations),
            DataCommand::Checkpoint {
                object_id,
                partitions,
            } => self.checkpoint(object_id, &partitions),
            DataCommand::Reclaim {
                partition,
                end,
                checkpoint_generation,
            } => self.reclaim(partition, end, checkpoint_generation),
        }
    }

    fn authorize(&mut self, plan: Plan) -> Result<DataOutcome, Rejection> {
        validate_plan(&plan)?;
        let id = plan.descriptor.identity.object_id;
        if let Some(intent) = self.intents.get(&id) {
            if intent.plan != plan {
                return Err(Rejection::Conflict);
            }
            return if intent.phase == IntentPhase::Aborted {
                Err(Rejection::Aborted)
            } else {
                Ok(DataOutcome::Authorized)
            };
        }
        if self.intents.len() >= MAX_OBJECTS {
            return Err(Rejection::Capacity);
        }
        for old_id in &plan.replaces {
            let old = self
                .intents
                .get(old_id)
                .filter(|old| old.phase == IntentPhase::Published)
                .ok_or(Rejection::NotPublished)?;
            for index in &old.plan.descriptor.partitions {
                let partition = key(index);
                let replacement = plan
                    .descriptor
                    .partitions
                    .iter()
                    .find(|new| key(new) == partition);
                if replacement.map_or(index.offsets.end, |new| {
                    new.offsets.start.min(index.offsets.end)
                }) > self.checkpointed_floor(partition).max(index.offsets.start)
                {
                    return Err(Rejection::InvalidRange);
                }
            }
        }
        self.intents.insert(
            id,
            Intent {
                plan,
                phase: IntentPhase::Authorized,
                generation: 1,
                checkpoint_generation: None,
            },
        );
        self.pending.insert(id);
        Ok(DataOutcome::Authorized)
    }

    fn abort(&mut self, plan: Plan) -> Result<DataOutcome, Rejection> {
        validate_plan(&plan)?;
        let id = plan.descriptor.identity.object_id;
        if let Some(intent) = self.intents.get(&id) {
            if intent.plan != plan {
                return Err(Rejection::Conflict);
            }
            if matches!(intent.phase, IntentPhase::Published | IntentPhase::Retired) {
                return Err(Rejection::AlreadyPublished);
            }
        } else if self.intents.len() >= MAX_OBJECTS {
            return Err(Rejection::Capacity);
        }
        let intent = self.intents.entry(id).or_insert(Intent {
            plan,
            phase: IntentPhase::Authorized,
            generation: 1,
            checkpoint_generation: None,
        });
        if intent.phase != IntentPhase::Aborted {
            intent.phase = IntentPhase::Aborted;
            intent.generation += 1;
        }
        self.pending.remove(&id);
        Ok(DataOutcome::Aborted {
            generation: intent.generation,
        })
    }

    fn published(
        &mut self,
        plan: Plan,
        generations: &BTreeMap<PartitionKey, u64>,
    ) -> Result<DataOutcome, Rejection> {
        let id = plan.descriptor.identity.object_id;
        let intent = self.intents.get(&id).ok_or(Rejection::NotFound)?;
        if intent.plan != plan {
            return Err(Rejection::Conflict);
        }
        if matches!(intent.phase, IntentPhase::Published | IntentPhase::Retired) {
            return Ok(DataOutcome::Published);
        }
        if intent.phase == IntentPhase::Aborted {
            return Err(Rejection::Aborted);
        }
        let mut updates = Vec::new();
        for index in &plan.descriptor.partitions {
            let partition = key(index);
            let generation = *generations
                .get(&partition)
                .ok_or(Rejection::StaleGeneration)?;
            let mut manifest = self.manifests.get(&partition).cloned().unwrap_or_default();
            if generation
                != manifest
                    .generation
                    .checked_add(1)
                    .ok_or(Rejection::Capacity)?
            {
                return Err(Rejection::StaleGeneration);
            }
            if plan.replaces.is_empty() && index.offsets.start != manifest.published_end {
                return Err(Rejection::InvalidRange);
            }
            manifest
                .segments
                .retain(|s| !plan.replaces.contains(&s.object_id));
            manifest.segments.push(super::types::Segment {
                object_id: id,
                offsets: index.offsets,
            });
            manifest.segments.sort_by_key(|s| s.offsets.start);
            manifest.published_end = manifest.published_end.max(index.offsets.end);
            manifest.generation = generation;
            manifest.publication_generation = generation;
            updates.push((partition, manifest));
        }
        for old_id in &plan.replaces {
            if self
                .intents
                .get(old_id)
                .is_none_or(|old| old.phase != IntentPhase::Published)
            {
                return Err(Rejection::NotPublished);
            }
        }
        for (partition, manifest) in self.manifests.iter().filter(|_| !plan.replaces.is_empty()) {
            if plan
                .descriptor
                .partitions
                .iter()
                .any(|index| key(index) == *partition)
            {
                continue;
            }
            let removed = manifest
                .segments
                .iter()
                .filter(|segment| plan.replaces.contains(&segment.object_id));
            if removed
                .clone()
                .any(|segment| segment.offsets.end > self.checkpointed_floor(*partition))
            {
                return Err(Rejection::InvalidRange);
            }
            if removed.count() > 0 {
                let mut retired = manifest.clone();
                retired
                    .segments
                    .retain(|segment| !plan.replaces.contains(&segment.object_id));
                updates.push((*partition, retired));
            }
        }
        for (partition, manifest) in updates {
            self.manifests.insert(partition, manifest);
        }
        for old_id in &plan.replaces {
            let old = self
                .intents
                .get_mut(old_id)
                .ok_or(Rejection::InvalidSnapshot)?;
            old.phase = IntentPhase::Retired;
            old.generation += 1;
        }
        let intent = self
            .intents
            .get_mut(&id)
            .ok_or(Rejection::InvalidSnapshot)?;
        intent.phase = IntentPhase::Published;
        intent.generation += 1;
        self.pending.remove(&id);
        Ok(DataOutcome::Published)
    }

    fn checkpoint(
        &mut self,
        id: Id,
        partitions: &[PartitionKey],
    ) -> Result<DataOutcome, Rejection> {
        let intent = self.intents.get(&id).ok_or(Rejection::NotFound)?;
        if intent.phase != IntentPhase::Published {
            return Err(Rejection::NotPublished);
        }
        if partitions.is_empty()
            || partitions.len() > 64
            || partitions
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != partitions.len()
            || partitions.iter().any(|partition| {
                intent
                    .plan
                    .descriptor
                    .partitions
                    .iter()
                    .find(|index| key(index) == *partition)
                    .is_none_or(|index| {
                        self.manifests
                            .get(partition)
                            .is_none_or(|manifest| manifest.published_end < index.offsets.end)
                    })
            })
        {
            return Err(Rejection::InvalidRange);
        }
        if let Some(generation) = intent.checkpoint_generation {
            return Ok(DataOutcome::Checkpoint { generation });
        }
        let generation = self
            .checkpoint_generation
            .checked_add(1)
            .ok_or(Rejection::Capacity)?;
        self.checkpoint_generation = generation;
        let intent = self
            .intents
            .get_mut(&id)
            .ok_or(Rejection::InvalidSnapshot)?;
        intent.checkpoint_generation = Some(generation);
        Ok(DataOutcome::Checkpoint { generation })
    }

    fn reclaim(
        &mut self,
        partition: PartitionKey,
        end: u64,
        generation: u64,
    ) -> Result<DataOutcome, Rejection> {
        let previous = self.local_reclaim_end(partition);
        if end < previous || end > self.published_end(partition) {
            return Err(Rejection::InvalidRange);
        }
        if end <= self.checkpointed_floor(partition) {
            self.reclaim_ends.insert(partition, end);
            return Ok(DataOutcome::Reclaimed { bytes: 0 });
        }
        if generation == 0 {
            return Err(Rejection::CheckpointRequired);
        }
        let manifest = self
            .manifests
            .get(&partition)
            .ok_or(Rejection::NotPublished)?;
        for segment in manifest
            .segments
            .iter()
            .filter(|s| s.offsets.start < end && s.offsets.end > previous)
        {
            let intent = self
                .intents
                .get(&segment.object_id)
                .ok_or(Rejection::InvalidSnapshot)?;
            if intent
                .checkpoint_generation
                .is_none_or(|checkpoint| checkpoint > generation)
            {
                return Err(Rejection::CheckpointRequired);
            }
        }
        self.reclaim_ends.insert(partition, end);
        Ok(DataOutcome::Reclaimed { bytes: 0 })
    }

    pub(crate) fn validate(&self) -> Result<(), Rejection> {
        if self.intents.len() > MAX_OBJECTS {
            return Err(Rejection::InvalidSnapshot);
        }
        for (id, intent) in &self.intents {
            validate_plan(&intent.plan)?;
            if *id != intent.plan.descriptor.identity.object_id
                || intent.generation == 0
                || intent
                    .checkpoint_generation
                    .is_some_and(|g| g == 0 || g > self.checkpoint_generation)
            {
                return Err(Rejection::InvalidSnapshot);
            }
        }
        if self.pending.len()
            != self
                .intents
                .values()
                .filter(|intent| intent.phase == IntentPhase::Authorized)
                .count()
            || self.pending.iter().any(|id| {
                self.intents
                    .get(id)
                    .is_none_or(|intent| intent.phase != IntentPhase::Authorized)
            })
        {
            return Err(Rejection::InvalidSnapshot);
        }
        for (partition, manifest) in &self.manifests {
            if manifest.generation == 0 || manifest.publication_generation != manifest.generation {
                return Err(Rejection::InvalidSnapshot);
            }
            let mut next = manifest
                .segments
                .first()
                .map_or(manifest.published_end, |segment| segment.offsets.start);
            if next > self.checkpointed_floor(*partition) {
                return Err(Rejection::InvalidSnapshot);
            }
            for segment in &manifest.segments {
                let intent = self
                    .intents
                    .get(&segment.object_id)
                    .ok_or(Rejection::InvalidSnapshot)?;
                if intent.phase != IntentPhase::Published
                    || segment.offsets.start != next
                    || !intent
                        .plan
                        .descriptor
                        .partitions
                        .iter()
                        .any(|i| key(i) == *partition && i.offsets == segment.offsets)
                {
                    return Err(Rejection::InvalidSnapshot);
                }
                next = segment.offsets.end;
            }
            if next != manifest.published_end || self.local_reclaim_end(*partition) > next {
                return Err(Rejection::InvalidSnapshot);
            }
        }
        if self.checkpointed_floors.len() > 8_192 {
            return Err(Rejection::InvalidSnapshot);
        }
        Ok(())
    }
}
