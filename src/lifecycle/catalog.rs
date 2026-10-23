use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use tidemq_protocol::{Id, state::OffsetRange};

use crate::{archive::PackLimits, metadata::PartitionKey};

use super::types::{
    Blocker, CatalogCommand, CatalogOutcome, CheckpointScope, Diagnostic, MAX_OBJECTS,
    MAX_PARTITIONS, MAX_PIN_OBJECTS, MAX_PIN_TTL, MAX_PINS, MAX_SEGMENTS, Manifest,
    ObjectCheckpoint, ObjectPhase, ObjectRecord, PinRecord, Plan, Rejection, Segment,
};

fn validate_checkpoint_scopes(
    object: &ObjectRecord,
    group: u64,
    scopes: &[CheckpointScope],
    generation: u64,
) -> Result<(), Rejection> {
    if object.phase != ObjectPhase::Published {
        return Err(Rejection::NotPublished);
    }
    if group == 0
        || generation == 0
        || scopes.is_empty()
        || scopes.len() > MAX_PARTITIONS
        || scopes
            .iter()
            .map(|scope| scope.partition)
            .collect::<BTreeSet<_>>()
            .len()
            != scopes.len()
    {
        return Err(Rejection::Identity);
    }
    for scope in scopes {
        if scope.binding_generation == 0
            || !object
                .descriptor()
                .partitions
                .iter()
                .any(|index| key(index) == scope.partition)
        {
            return Err(Rejection::Identity);
        }
        if let Some(old) = object.checkpoints.get(&scope.partition) {
            if scope.binding_generation < old.binding_generation
                || scope.binding_generation == old.binding_generation
                    && group == old.group_id
                    && generation < old.generation
            {
                return Err(Rejection::StaleGeneration);
            }
            if scope.binding_generation == old.binding_generation && group != old.group_id {
                return Err(Rejection::Conflict);
            }
        }
    }
    Ok(())
}

/// Terminal records are permanent identity fences; reaching the cap stops registration.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Catalog {
    objects: BTreeMap<Id, ObjectRecord>,
    by_group: BTreeMap<u64, BTreeSet<Id>>,
    manifests: BTreeMap<PartitionKey, Manifest>,
    pins: BTreeMap<Id, PinRecord>,
    pins_by_group: BTreeMap<u64, BTreeSet<Id>>,
    retired_floors: BTreeMap<PartitionKey, u64>,
    checkpointed_floors: BTreeMap<PartitionKey, u64>,
}

impl Catalog {
    pub(crate) fn transfer_has_pending(&self, partition: PartitionKey) -> bool {
        self.objects.values().any(|object| {
            object
                .descriptor()
                .partitions
                .iter()
                .any(|index| key(index) == partition)
                && !matches!(
                    object.phase,
                    ObjectPhase::Published | ObjectPhase::Aborted | ObjectPhase::Deleted
                )
        })
    }
    pub(crate) fn transfer_has_pins(&self, partition: PartitionKey, source_group: u64) -> bool {
        self.pins
            .values()
            .any(|pin| pin.group_id == source_group && pin.partitions.contains(&partition))
    }
    pub fn object(&self, id: Id) -> Option<&ObjectRecord> {
        self.objects.get(&id)
    }
    pub fn manifest(&self, partition: PartitionKey) -> Option<&Manifest> {
        self.manifests.get(&partition)
    }
    pub fn object_count(&self) -> usize {
        self.objects.len()
    }
    pub fn pin_count(&self) -> usize {
        self.pins.len()
    }
    pub fn checkpointed_floor(&self, partition: PartitionKey) -> u64 {
        self.checkpointed_floors
            .get(&partition)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn compaction_ranges(
        &self,
        id: Id,
    ) -> Result<Option<Vec<super::RangeRequest>>, Rejection> {
        let object = self.objects.get(&id).ok_or(Rejection::NotFound)?;
        if object.phase != ObjectPhase::Published
            || object.recovery_replacement.is_some()
            || object.manifest_references == 0
        {
            return Ok(None);
        }
        let mut changed = false;
        let mut ranges = Vec::new();
        for index in &object.descriptor().partitions {
            let partition = key(index);
            let segment = self.manifests.get(&partition).and_then(|manifest| {
                manifest
                    .segments
                    .iter()
                    .find(|segment| segment.object_id == id)
            });
            let Some(segment) = segment else {
                if self.checkpointed_floor(partition) < index.offsets.end {
                    return Ok(None);
                }
                changed = true;
                continue;
            };
            let start = segment
                .offsets
                .start
                .max(self.checkpointed_floor(partition));
            if start >= segment.offsets.end {
                return Ok(None);
            }
            changed |= start != index.offsets.start;
            ranges.push(super::RangeRequest {
                partition,
                offsets: OffsetRange {
                    start,
                    end: segment.offsets.end,
                },
            });
        }
        Ok((changed && !ranges.is_empty()).then_some(ranges))
    }

    pub fn capacity_usage(&self) -> super::CapacityUsage {
        self.usage(self.objects.values(), self.pins.len())
    }

    pub fn capacity_usage_for(&self, group: u64) -> super::CapacityUsage {
        self.usage(
            self.by_group
                .get(&group)
                .into_iter()
                .flat_map(|ids| ids.iter())
                .filter_map(|id| self.objects.get(id)),
            self.pins_by_group.get(&group).map_or(0, BTreeSet::len),
        )
    }

    fn usage<'a>(
        &self,
        objects: impl Iterator<Item = &'a ObjectRecord>,
        read_pins: usize,
    ) -> super::CapacityUsage {
        let mut usage = super::CapacityUsage {
            read_pins,
            ..super::CapacityUsage::default()
        };
        for object in objects {
            usage.objects += 1;
            if object.phase == ObjectPhase::Deleted {
                usage.deleted_objects += 1;
                continue;
            }
            let bytes = object.descriptor().total_bytes;
            usage.registered_physical_upper_bytes =
                usage.registered_physical_upper_bytes.saturating_add(bytes);
            if matches!(
                object.phase,
                ObjectPhase::Registered | ObjectPhase::Uploaded | ObjectPhase::CancelRequested
            ) {
                usage.pending_upload_upper_bytes =
                    usage.pending_upload_upper_bytes.saturating_add(bytes);
            }
            let eligible_range_bytes = object
                .descriptor()
                .partitions
                .iter()
                .filter(|index| {
                    self.retired_floors
                        .get(&key(index))
                        .is_some_and(|floor| *floor > index.offsets.start)
                })
                .fold(0_u64, |sum, index| {
                    sum.saturating_add(index.byte_end.saturating_sub(index.byte_start))
                });
            let unreferenced = if object.manifest_references == 0
                && matches!(
                    object.phase,
                    ObjectPhase::Published | ObjectPhase::Aborted | ObjectPhase::Deleting
                ) {
                bytes
            } else {
                0
            };
            usage.reclaim_lag_upper_bytes = usage
                .reclaim_lag_upper_bytes
                .saturating_add(unreferenced.max(eligible_range_bytes).min(bytes));
        }
        usage
    }

    pub fn objects_page(
        &self,
        group: u64,
        after: Option<Id>,
        limit: usize,
    ) -> Result<super::ObjectPage, Rejection> {
        use std::ops::Bound::{Excluded, Unbounded};
        if limit == 0 || limit > 64 {
            return Err(Rejection::Capacity);
        }
        let Some(ids) = self.by_group.get(&group) else {
            return Ok(super::ObjectPage {
                objects: vec![],
                next_after: None,
            });
        };
        let mut iterator = ids.range((after.map_or(Unbounded, Excluded), Unbounded));
        let objects = iterator
            .by_ref()
            .take(limit)
            .map(|id| {
                self.objects
                    .get(id)
                    .map(|o| (*id, o.phase))
                    .ok_or(Rejection::InvalidSnapshot)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let next_after = if iterator.next().is_some() {
            objects.last().map(|(id, _)| *id)
        } else {
            None
        };
        Ok(super::ObjectPage {
            objects,
            next_after,
        })
    }
    pub(crate) fn pin(&self, id: Id) -> Option<&PinRecord> {
        self.pins.get(&id)
    }

    pub(crate) fn related_objects_page(
        &self,
        group: u64,
        after: Option<Id>,
        limit: usize,
        authority: impl Fn(PartitionKey) -> Option<u64>,
    ) -> Result<super::ObjectPage, Rejection> {
        use std::ops::Bound::{Excluded, Unbounded};
        if limit == 0 || limit > 64 {
            return Err(Rejection::Capacity);
        }
        let mut entries = self
            .objects
            .range((after.map_or(Unbounded, Excluded), Unbounded));
        let mut objects = Vec::new();
        let mut scanned = None;
        for _ in 0..64 {
            let Some((id, object)) = entries.next() else {
                break;
            };
            scanned = Some(*id);
            if object.descriptor().identity.group_id == group
                || object
                    .descriptor()
                    .partitions
                    .iter()
                    .any(|index| authority(key(index)) == Some(group))
            {
                objects.push((*id, object.phase));
                if objects.len() == limit {
                    break;
                }
            }
        }
        Ok(super::ObjectPage {
            objects,
            next_after: entries.next().and(scanned),
        })
    }

    pub(crate) fn related_pins_page(
        &self,
        group: u64,
        after: Option<Id>,
        limit: usize,
        authority: impl Fn(PartitionKey) -> Option<u64>,
    ) -> Result<super::PinPage, Rejection> {
        use std::ops::Bound::{Excluded, Unbounded};
        if limit == 0 || limit > 64 {
            return Err(Rejection::Capacity);
        }
        let mut entries = self
            .pins
            .range((after.map_or(Unbounded, Excluded), Unbounded));
        let mut pins = Vec::new();
        let mut scanned = None;
        for _ in 0..64 {
            let Some((id, pin)) = entries.next() else {
                break;
            };
            scanned = Some(*id);
            if pin.group_id == group
                || pin
                    .partitions
                    .iter()
                    .any(|partition| authority(*partition) == Some(group))
            {
                pins.push(*id);
                if pins.len() == limit {
                    break;
                }
            }
        }
        Ok(super::PinPage {
            pins,
            next_after: entries.next().and(scanned),
        })
    }

    pub fn pins_page(
        &self,
        group: u64,
        after: Option<Id>,
        limit: usize,
    ) -> Result<super::PinPage, Rejection> {
        use std::ops::Bound::{Excluded, Unbounded};
        if limit == 0 || limit > 64 {
            return Err(Rejection::Capacity);
        }
        let mut pins = Vec::new();
        pins.try_reserve_exact(limit)
            .map_err(|_| Rejection::Capacity)?;
        let Some(ids) = self.pins_by_group.get(&group) else {
            return Ok(super::PinPage {
                pins,
                next_after: None,
            });
        };
        let mut iterator = ids.range((after.map_or(Unbounded, Excluded), Unbounded));
        pins.extend(iterator.by_ref().take(limit).copied());
        let next_after = if iterator.next().is_some() {
            pins.last().copied()
        } else {
            None
        };
        Ok(super::PinPage { pins, next_after })
    }

    pub fn diagnose(&self, object_id: Id) -> Option<Diagnostic> {
        let object = self.object(object_id)?;
        let references = self.references(object_id);
        let pins = self
            .pins
            .values()
            .filter(|pin| pin.objects.contains(&object_id))
            .count();
        let blocker = match object.phase {
            ObjectPhase::Registered | ObjectPhase::Uploaded => Blocker::PendingTask,
            ObjectPhase::CancelRequested => Blocker::CancelNeedsDataAbort,
            ObjectPhase::Deleting => Blocker::DeletionRetry,
            _ if references > 0 => Blocker::LiveManifest,
            _ if pins > 0 => Blocker::ReadPin,
            _ if object.recovery_replacement.is_some() || !object.floor_holds.is_empty() => {
                Blocker::RecoverySnapshot
            }
            _ => Blocker::None,
        };
        Some(Diagnostic {
            object_id,
            phase: object.phase,
            blocker,
            manifest_references: references,
            read_pins: pins,
            checkpoint_generation: None,
            authority_scopes: Vec::new(),
            recovery_replacement: object.recovery_replacement,
        })
    }

    pub(crate) fn apply(&mut self, command: CatalogCommand) -> Result<CatalogOutcome, Rejection> {
        match command {
            CatalogCommand::Register(plan) => self.register(plan),
            CatalogCommand::Uploaded { object_id } => self.uploaded(object_id),
            CatalogCommand::Publish { object_id } => self.publish(object_id),
            CatalogCommand::RequestCancel { object_id } => self.request_cancel(object_id),
            CatalogCommand::FinishCancel {
                object_id,
                data_generation,
            } => self.finish_cancel(object_id, data_generation),
            CatalogCommand::Pin(pin) => self.acquire_pin(pin),
            CatalogCommand::ReleasePin { pin } => self.release_pin(pin),
            CatalogCommand::Checkpoint {
                object_id,
                group_id,
                scopes,
                generation,
                voters,
                snapshots,
            } => self.checkpoint(
                object_id, group_id, &scopes, generation, &voters, &snapshots,
            ),
            CatalogCommand::BeginDelete { object_id } => self.begin_delete(object_id),
            CatalogCommand::Deleted { object_id } => self.deleted(object_id),
            CatalogCommand::RetireBefore(proof) => self.retire_before(proof),
            CatalogCommand::FloorCheckpoint { proof, snapshots } => {
                self.floor_checkpoint(proof, &snapshots)
            }
        }
    }

    fn register(&mut self, plan: Plan) -> Result<CatalogOutcome, Rejection> {
        validate_plan(&plan)?;
        let id = plan.descriptor.identity.object_id;
        if let Some(object) = self.objects.get(&id) {
            return if object.plan == plan {
                Ok(CatalogOutcome::Object(Box::new(object.clone())))
            } else {
                Err(Rejection::Conflict)
            };
        }
        if self.objects.len() >= MAX_OBJECTS {
            return Err(Rejection::Capacity);
        }
        self.check_generations(&plan)?;
        self.replacement_manifests(&plan)?;
        let object = ObjectRecord {
            plan,
            phase: ObjectPhase::Registered,
            generation: 1,
            manifest_references: 0,
            checkpoints: BTreeMap::new(),
            published_generations: BTreeMap::new(),
            recovery_replacement: None,
            floor_holds: BTreeMap::new(),
        };
        self.by_group
            .entry(object.descriptor().identity.group_id)
            .or_default()
            .insert(id);
        for old_id in &object.plan.replaces {
            self.objects
                .get_mut(old_id)
                .ok_or(Rejection::InvalidSnapshot)?
                .recovery_replacement = Some(id);
        }
        self.objects.insert(id, object.clone());
        Ok(CatalogOutcome::Object(Box::new(object)))
    }

    fn uploaded(&mut self, id: Id) -> Result<CatalogOutcome, Rejection> {
        let object = self.objects.get_mut(&id).ok_or(Rejection::NotFound)?;
        match object.phase {
            ObjectPhase::Registered => {
                object.phase = ObjectPhase::Uploaded;
                object.generation += 1;
            }
            ObjectPhase::Uploaded | ObjectPhase::Published => {}
            ObjectPhase::CancelRequested => return Err(Rejection::CancelRequested),
            _ => return Err(Rejection::Aborted),
        }
        Ok(CatalogOutcome::Object(Box::new(object.clone())))
    }

    fn publish(&mut self, id: Id) -> Result<CatalogOutcome, Rejection> {
        let object = self.objects.get(&id).ok_or(Rejection::NotFound)?;
        if object.phase == ObjectPhase::Published {
            return Ok(CatalogOutcome::Object(Box::new(object.clone())));
        }
        if object.phase != ObjectPhase::Uploaded {
            return Err(Rejection::NotPublished);
        }
        let plan = object.plan.clone();
        self.check_generations(&plan)?;
        let replacements = self.replacement_manifests(&plan)?;
        let published_generations = replacements
            .iter()
            .map(|(partition, manifest)| (*partition, manifest.publication_generation))
            .collect();
        for (partition, manifest) in replacements {
            self.manifests.insert(partition, manifest);
        }
        for old_id in &plan.replaces {
            let old = self
                .objects
                .get_mut(old_id)
                .ok_or(Rejection::InvalidSnapshot)?;
            old.recovery_replacement = Some(id);
            old.manifest_references = 0;
            old.checkpoints.clear();
        }
        let object = self
            .objects
            .get_mut(&id)
            .ok_or(Rejection::InvalidSnapshot)?;
        object.published_generations = published_generations;
        object.manifest_references = plan.descriptor.partitions.len();
        object.phase = ObjectPhase::Published;
        object.generation += 1;
        Ok(CatalogOutcome::Object(Box::new(object.clone())))
    }

    fn check_generations(&self, plan: &Plan) -> Result<(), Rejection> {
        for (partition, expected) in &plan.expected_generations {
            if self.manifests.get(partition).map_or(0, |m| m.generation) != *expected {
                return Err(Rejection::StaleGeneration);
            }
        }
        Ok(())
    }

    fn replacement_manifests(
        &self,
        plan: &Plan,
    ) -> Result<BTreeMap<PartitionKey, Manifest>, Rejection> {
        let replaced: BTreeSet<_> = plan.replaces.iter().copied().collect();
        if replaced.len() != plan.replaces.len() {
            return Err(Rejection::Conflict);
        }
        for id in &replaced {
            let object = self.objects.get(id).ok_or(Rejection::NotFound)?;
            if object.phase != ObjectPhase::Published
                || object
                    .recovery_replacement
                    .is_some_and(|id| id != plan.descriptor.identity.object_id)
            {
                return Err(Rejection::References);
            }
            if object.plan.descriptor.identity.group_id != plan.descriptor.identity.group_id {
                return Err(Rejection::Identity);
            }
            for index in &object.plan.descriptor.partitions {
                let partition = key(index);
                if !plan.expected_generations.contains_key(&partition)
                    && self.checkpointed_floor(partition) < index.offsets.end
                {
                    return Err(Rejection::InvalidRange);
                }
            }
        }
        let mut result = BTreeMap::new();
        for index in &plan.descriptor.partitions {
            let partition = key(index);
            let mut manifest = self.manifests.get(&partition).cloned().unwrap_or_default();
            let removed: Vec<_> = manifest
                .segments
                .iter()
                .filter(|s| replaced.contains(&s.object_id))
                .cloned()
                .collect();
            if replaced.is_empty() {
                if index.offsets.start != manifest.published_end {
                    return Err(Rejection::InvalidRange);
                }
                manifest.published_end = index.offsets.end;
            } else {
                let first = removed.first().ok_or(Rejection::InvalidRange)?;
                let last = removed.last().ok_or(Rejection::InvalidRange)?;
                let mut end = first.offsets.start;
                for segment in &removed {
                    if segment.offsets.start != end {
                        return Err(Rejection::InvalidRange);
                    }
                    end = segment.offsets.end;
                }
                if index.offsets
                    != (OffsetRange {
                        start: first.offsets.start.max(self.checkpointed_floor(partition)),
                        end: last.offsets.end,
                    })
                {
                    return Err(Rejection::InvalidRange);
                }
            }
            manifest
                .segments
                .retain(|s| !replaced.contains(&s.object_id));
            if manifest.segments.len() >= MAX_SEGMENTS {
                return Err(Rejection::Capacity);
            }
            manifest.segments.push(Segment {
                object_id: plan.descriptor.identity.object_id,
                offsets: index.offsets,
            });
            manifest.segments.sort_by_key(|s| s.offsets.start);
            manifest.generation = manifest
                .generation
                .checked_add(1)
                .ok_or(Rejection::Capacity)?;
            manifest.publication_generation = manifest
                .publication_generation
                .checked_add(1)
                .ok_or(Rejection::Capacity)?;
            result.insert(partition, manifest);
        }
        Ok(result)
    }

    fn request_cancel(&mut self, id: Id) -> Result<CatalogOutcome, Rejection> {
        let object = self.objects.get_mut(&id).ok_or(Rejection::NotFound)?;
        match object.phase {
            ObjectPhase::Registered | ObjectPhase::Uploaded => {
                object.phase = ObjectPhase::CancelRequested;
                object.generation += 1;
            }
            ObjectPhase::Published => return Err(Rejection::AlreadyPublished),
            _ => {}
        }
        Ok(CatalogOutcome::Object(Box::new(object.clone())))
    }

    fn finish_cancel(&mut self, id: Id, generation: u64) -> Result<CatalogOutcome, Rejection> {
        if generation == 0 {
            return Err(Rejection::Conflict);
        }
        let object = self.objects.get_mut(&id).ok_or(Rejection::NotFound)?;
        match object.phase {
            ObjectPhase::CancelRequested => {
                object.phase = ObjectPhase::Aborted;
                object.generation += 1;
            }
            ObjectPhase::Aborted | ObjectPhase::Deleting | ObjectPhase::Deleted => {}
            _ => return Err(Rejection::Conflict),
        }
        let result = object.clone();
        for source in &result.plan.replaces {
            if let Some(old) = self.objects.get_mut(source)
                && old.recovery_replacement == Some(id)
            {
                old.recovery_replacement = None;
            }
        }
        Ok(CatalogOutcome::Object(Box::new(result)))
    }

    fn acquire_pin(&mut self, pin: PinRecord) -> Result<CatalogOutcome, Rejection> {
        if let Some(old) = self.pins.get(&pin.id) {
            return if *old == pin {
                Ok(CatalogOutcome::Pinned)
            } else {
                Err(Rejection::Conflict)
            };
        }
        if self.pins.len() >= MAX_PINS {
            return Err(Rejection::Capacity);
        }
        self.validate_pin_scope(&pin)?;
        for id in &pin.objects {
            if self.references(*id) == 0 {
                return Err(Rejection::References);
            }
        }
        self.pins_by_group
            .entry(pin.group_id)
            .or_default()
            .insert(pin.id);
        self.pins.insert(pin.id, pin);
        Ok(CatalogOutcome::Pinned)
    }

    pub(super) fn validate_pin_scope(&self, pin: &PinRecord) -> Result<(), Rejection> {
        if pin.group_id == 0
            || pin.objects.is_empty()
            || pin.objects.len() > MAX_PIN_OBJECTS
            || pin.partitions.is_empty()
            || pin.partitions.len() > MAX_PARTITIONS
            || pin.ttl_millis == 0
            || u128::from(pin.ttl_millis) > MAX_PIN_TTL.as_millis()
        {
            return Err(Rejection::Capacity);
        }
        let unique: BTreeSet<_> = pin.objects.iter().copied().collect();
        if unique.len() != pin.objects.len()
            || pin
                .partitions
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != pin.partitions.len()
        {
            return Err(Rejection::Conflict);
        }
        for id in &pin.objects {
            let object = self.objects.get(id).ok_or(Rejection::NotFound)?;
            if object.phase != ObjectPhase::Published
                || !object
                    .descriptor()
                    .partitions
                    .iter()
                    .any(|index| pin.partitions.contains(&key(index)))
            {
                return Err(Rejection::References);
            }
        }
        if pin.partitions.iter().any(|partition| {
            !pin.objects.iter().any(|id| {
                self.objects.get(id).is_some_and(|object| {
                    object
                        .descriptor()
                        .partitions
                        .iter()
                        .any(|index| key(index) == *partition)
                })
            })
        }) {
            return Err(Rejection::References);
        }
        Ok(())
    }

    fn release_pin(&mut self, pin: PinRecord) -> Result<CatalogOutcome, Rejection> {
        if self
            .pins
            .get(&pin.id)
            .is_some_and(|current| *current != pin)
        {
            return Err(Rejection::Conflict);
        }
        self.pins.remove(&pin.id);
        if let Some(ids) = self.pins_by_group.get_mut(&pin.group_id) {
            ids.remove(&pin.id);
            if ids.is_empty() {
                self.pins_by_group.remove(&pin.group_id);
            }
        }
        Ok(CatalogOutcome::Released)
    }

    fn checkpoint(
        &mut self,
        id: Id,
        group_id: u64,
        scopes: &[CheckpointScope],
        generation: u64,
        voters: &[u64],
        snapshots: &[(u64, String, u64)],
    ) -> Result<CatalogOutcome, Rejection> {
        if !matches!(voters.len(), 1 | 3) || snapshots.len() != voters.len() {
            return Err(Rejection::Membership);
        }
        let expected: BTreeSet<_> = voters.iter().copied().collect();
        let actual: BTreeSet<_> = snapshots.iter().map(|(node, _, _)| *node).collect();
        if !matches!(expected.len(), 1 | 3)
            || expected.contains(&0)
            || actual != expected
            || expected.len() != voters.len()
            || generation == 0
            || snapshots.iter().any(|(_, snapshot, index)| {
                snapshot.is_empty() || snapshot.len() > 128 || *index == 0
            })
        {
            return Err(Rejection::Membership);
        }
        let object = self.objects.get(&id).ok_or(Rejection::NotFound)?;
        validate_checkpoint_scopes(object, group_id, scopes, generation)?;
        let object = self.objects.get_mut(&id).ok_or(Rejection::NotFound)?;
        for scope in scopes {
            object.checkpoints.insert(
                scope.partition,
                ObjectCheckpoint {
                    group_id,
                    binding_generation: scope.binding_generation,
                    generation,
                },
            );
        }
        let complete = object
            .descriptor()
            .partitions
            .iter()
            .all(|index| object.checkpoints.contains_key(&key(index)));
        if complete {
            for old in self
                .objects
                .values_mut()
                .filter(|old| old.recovery_replacement == Some(id))
            {
                old.recovery_replacement = None;
            }
        }
        Ok(CatalogOutcome::Object(Box::new(
            self.objects
                .get(&id)
                .ok_or(Rejection::InvalidSnapshot)?
                .clone(),
        )))
    }

    fn begin_delete(&mut self, id: Id) -> Result<CatalogOutcome, Rejection> {
        let diagnostic = self.diagnose(id).ok_or(Rejection::NotFound)?;
        if diagnostic.manifest_references > 0 {
            return Err(Rejection::References);
        }
        if diagnostic.read_pins > 0 {
            return Err(Rejection::Pinned);
        }
        if diagnostic.recovery_replacement.is_some() {
            return Err(Rejection::RecoveryDependency);
        }
        if self
            .objects
            .get(&id)
            .is_some_and(|object| !object.floor_holds.is_empty())
        {
            return Err(Rejection::RecoveryDependency);
        }
        let object = self.objects.get_mut(&id).ok_or(Rejection::NotFound)?;
        match object.phase {
            ObjectPhase::Aborted | ObjectPhase::Published => {
                object.phase = ObjectPhase::Deleting;
                object.generation += 1;
            }
            ObjectPhase::Deleting | ObjectPhase::Deleted => {}
            _ => return Err(Rejection::CancelRequested),
        }
        Ok(CatalogOutcome::Object(Box::new(object.clone())))
    }

    fn deleted(&mut self, id: Id) -> Result<CatalogOutcome, Rejection> {
        let object = self.objects.get_mut(&id).ok_or(Rejection::NotFound)?;
        if object.phase == ObjectPhase::Deleted {
            return Ok(CatalogOutcome::Object(Box::new(object.clone())));
        }
        if object.phase != ObjectPhase::Deleting {
            return Err(Rejection::Conflict);
        }
        object.phase = ObjectPhase::Deleted;
        object.generation += 1;
        Ok(CatalogOutcome::Object(Box::new(object.clone())))
    }

    fn retire_before(
        &mut self,
        proof: super::types::FloorEvidence,
    ) -> Result<CatalogOutcome, Rejection> {
        if proof.before
            < self
                .retired_floors
                .get(&proof.partition)
                .copied()
                .unwrap_or(0)
        {
            return Err(Rejection::StaleGeneration);
        }
        let Some(manifest) = self.manifests.get(&proof.partition) else {
            self.retired_floors.insert(proof.partition, proof.before);
            return Ok(CatalogOutcome::Retired);
        };
        if proof.manifest_generation != manifest.publication_generation {
            return Err(Rejection::StaleGeneration);
        }
        let generation = manifest
            .generation
            .checked_add(1)
            .ok_or(Rejection::Capacity)?;
        let removed = manifest
            .segments
            .iter()
            .filter(|segment| {
                segment.offsets.end <= proof.before
                    && segment.offsets.end <= proof.published_end
                    && self
                        .objects
                        .get(&segment.object_id)
                        .and_then(|object| object.published_generations.get(&proof.partition))
                        .is_some_and(|generation| *generation <= proof.manifest_generation)
            })
            .map(|segment| segment.object_id)
            .collect::<BTreeSet<_>>();
        let manifest = self
            .manifests
            .get_mut(&proof.partition)
            .ok_or(Rejection::InvalidSnapshot)?;
        if !removed.is_empty() {
            manifest.generation = generation;
            manifest
                .segments
                .retain(|segment| !removed.contains(&segment.object_id));
        }
        for id in removed {
            let object = self
                .objects
                .get_mut(&id)
                .ok_or(Rejection::InvalidSnapshot)?;
            object.manifest_references = object
                .manifest_references
                .checked_sub(1)
                .ok_or(Rejection::InvalidSnapshot)?;
            object
                .floor_holds
                .entry(proof.partition)
                .and_modify(|generation| *generation = (*generation).max(proof.manifest_generation))
                .or_insert(proof.manifest_generation);
        }
        self.retired_floors.insert(proof.partition, proof.before);
        Ok(CatalogOutcome::Retired)
    }

    fn floor_checkpoint(
        &mut self,
        proof: super::types::FloorEvidence,
        snapshots: &[(u64, String, u64)],
    ) -> Result<CatalogOutcome, Rejection> {
        let nodes = snapshots
            .iter()
            .map(|(node, _, _)| *node)
            .collect::<BTreeSet<_>>();
        if nodes != proof.voters
            || snapshots.len() != proof.voters.len()
            || snapshots.iter().any(|(_, id, index)| {
                id.is_empty() || id.len() > 128 || *index < proof.position.index
            })
        {
            return Err(Rejection::CheckpointRequired);
        }
        for object in self.objects.values_mut() {
            if object
                .floor_holds
                .get(&proof.partition)
                .is_some_and(|generation| *generation <= proof.manifest_generation)
                && object
                    .plan
                    .descriptor
                    .partitions
                    .iter()
                    .any(|index| key(index) == proof.partition && index.offsets.end <= proof.before)
            {
                object.floor_holds.remove(&proof.partition);
            }
        }
        self.checkpointed_floors
            .entry(proof.partition)
            .and_modify(|before| *before = (*before).max(proof.before))
            .or_insert(proof.before);
        Ok(CatalogOutcome::Retired)
    }

    fn references(&self, id: Id) -> usize {
        self.objects
            .get(&id)
            .map_or(0, |object| object.manifest_references)
    }

    pub(crate) fn validate(&self) -> Result<(), Rejection> {
        if self.objects.len() > MAX_OBJECTS
            || self.pins.len() > MAX_PINS
            || self.retired_floors.len() > 8_192
        {
            return Err(Rejection::InvalidSnapshot);
        }
        for (id, object) in &self.objects {
            validate_plan(&object.plan)?;
            if *id != object.object_id()
                || object.generation == 0
                || object.checkpoints.len() > MAX_PARTITIONS
                || object.checkpoints.iter().any(|(partition, proof)| {
                    proof.group_id == 0
                        || proof.binding_generation == 0
                        || proof.generation == 0
                        || !object
                            .descriptor()
                            .partitions
                            .iter()
                            .any(|index| key(index) == *partition)
                })
                || object.floor_holds.len() > MAX_PARTITIONS
                || object.floor_holds.iter().any(|(partition, generation)| {
                    *generation == 0
                        || !object
                            .descriptor()
                            .partitions
                            .iter()
                            .any(|index| key(index) == *partition)
                })
            {
                return Err(Rejection::InvalidSnapshot);
            }
            if object.recovery_replacement.is_some_and(|replacement| {
                object.phase != ObjectPhase::Published
                    || self.objects.get(&replacement).is_none_or(|target| {
                        !target.plan.replaces.contains(id)
                            || !matches!(
                                target.phase,
                                ObjectPhase::Registered
                                    | ObjectPhase::Uploaded
                                    | ObjectPhase::Published
                                    | ObjectPhase::CancelRequested
                            )
                    })
            }) {
                return Err(Rejection::InvalidSnapshot);
            }
            if matches!(object.phase, ObjectPhase::Deleting | ObjectPhase::Deleted)
                && (self.references(*id) != 0
                    || object.recovery_replacement.is_some()
                    || !object.floor_holds.is_empty()
                    || self.pins.values().any(|pin| pin.objects.contains(id)))
            {
                return Err(Rejection::InvalidSnapshot);
            }
        }
        if self.by_group.values().map(BTreeSet::len).sum::<usize>() != self.objects.len()
            || self.objects.iter().any(|(id, object)| {
                self.by_group
                    .get(&object.descriptor().identity.group_id)
                    .is_none_or(|ids| !ids.contains(id))
            })
        {
            return Err(Rejection::InvalidSnapshot);
        }
        let mut references = BTreeMap::<Id, usize>::new();
        for (partition, manifest) in &self.manifests {
            let mut next = manifest
                .segments
                .first()
                .map_or(manifest.published_end, |s| s.offsets.start);
            if manifest.generation == 0
                || manifest.publication_generation == 0
                || manifest.publication_generation > manifest.generation
                || manifest.segments.len() > MAX_SEGMENTS
            {
                return Err(Rejection::InvalidSnapshot);
            }
            for segment in &manifest.segments {
                let object = self
                    .objects
                    .get(&segment.object_id)
                    .ok_or(Rejection::InvalidSnapshot)?;
                if object.phase != ObjectPhase::Published
                    || segment.offsets.start != next
                    || !object
                        .plan
                        .descriptor
                        .partitions
                        .iter()
                        .any(|index| key(index) == *partition && index.offsets == segment.offsets)
                {
                    return Err(Rejection::InvalidSnapshot);
                }
                *references.entry(segment.object_id).or_default() += 1;
                next = segment.offsets.end;
            }
            if next != manifest.published_end {
                return Err(Rejection::InvalidSnapshot);
            }
        }
        if self.objects.iter().any(|(id, object)| {
            references.get(id).copied().unwrap_or(0) != object.manifest_references
        }) {
            return Err(Rejection::InvalidSnapshot);
        }
        for (id, pin) in &self.pins {
            if *id != pin.id
                || self.validate_pin_scope(pin).is_err()
                || self
                    .pins_by_group
                    .get(&pin.group_id)
                    .is_none_or(|ids| !ids.contains(id))
            {
                return Err(Rejection::InvalidSnapshot);
            }
        }
        if self.pins_by_group.values().any(BTreeSet::is_empty)
            || self
                .pins_by_group
                .values()
                .map(BTreeSet::len)
                .sum::<usize>()
                != self.pins.len()
        {
            return Err(Rejection::InvalidSnapshot);
        }
        Ok(())
    }
}

pub(crate) fn key(index: &crate::archive::PartitionIndex) -> PartitionKey {
    PartitionKey {
        topic_id: index.topic_id,
        partition: index.partition_id,
    }
}

pub(crate) fn validate_plan(plan: &Plan) -> Result<(), Rejection> {
    plan.descriptor
        .validate(PackLimits::default())
        .map_err(|_| Rejection::InvalidRange)?;
    if plan.descriptor.partitions.len() > MAX_PARTITIONS
        || plan.replaces.len() > MAX_PIN_OBJECTS
        || plan.grants.len() != plan.descriptor.partitions.len()
        || plan.expected_generations.len() != plan.descriptor.partitions.len()
    {
        return Err(Rejection::Capacity);
    }
    let mut keys = BTreeSet::new();
    for index in &plan.descriptor.partitions {
        let partition = key(index);
        let grant = plan
            .grants
            .iter()
            .find(|g| g.partition == partition)
            .ok_or(Rejection::Identity)?;
        if !keys.insert(partition)
            || !plan.expected_generations.contains_key(&partition)
            || grant.cluster_id != plan.descriptor.identity.cluster_id
            || grant.group_id != plan.descriptor.identity.group_id
            || grant.epoch == 0
            || grant.owner_node == 0
        {
            return Err(Rejection::Identity);
        }
    }
    Ok(())
}
