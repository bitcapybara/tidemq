use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use tidemq_protocol::{Id, state::OffsetRange};

use crate::{archive::PackLimits, metadata::PartitionKey};

use super::types::{
    Blocker, CatalogCommand, CatalogOutcome, Diagnostic, MAX_OBJECTS, MAX_PARTITIONS,
    MAX_PIN_OBJECTS, MAX_PIN_TTL, MAX_PINS, MAX_SEGMENTS, Manifest, ObjectPhase, ObjectRecord,
    PinRecord, Plan, Rejection, Segment,
};

/// Terminal records are permanent identity fences; reaching the cap stops registration.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Catalog {
    objects: BTreeMap<Id, ObjectRecord>,
    manifests: BTreeMap<PartitionKey, Manifest>,
    pins: BTreeMap<Id, PinRecord>,
}

impl Catalog {
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
    pub(crate) fn pin(&self, id: Id) -> Option<&PinRecord> {
        self.pins.get(&id)
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
            _ if object.recovery_replacement.is_some() => Blocker::RecoverySnapshot,
            _ => Blocker::None,
        };
        Some(Diagnostic {
            object_id,
            phase: object.phase,
            blocker,
            manifest_references: references,
            read_pins: pins,
            checkpoint_generation: object.checkpoint_generation,
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
                generation,
                voters,
                snapshots,
            } => self.checkpoint(object_id, generation, &voters, &snapshots),
            CatalogCommand::BeginDelete { object_id } => self.begin_delete(object_id),
            CatalogCommand::Deleted { object_id } => self.deleted(object_id),
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
            checkpoint_generation: None,
            published_generations: BTreeMap::new(),
            recovery_replacement: None,
        };
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
            .map(|(partition, manifest)| (*partition, manifest.generation))
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
            old.checkpoint_generation = None;
        }
        let object = self
            .objects
            .get_mut(&id)
            .ok_or(Rejection::InvalidSnapshot)?;
        object.published_generations = published_generations;
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
            if object.phase != ObjectPhase::Published || object.recovery_replacement.is_some() {
                return Err(Rejection::References);
            }
            if object.plan.descriptor.identity.group_id != plan.descriptor.identity.group_id {
                return Err(Rejection::Identity);
            }
            for index in &object.plan.descriptor.partitions {
                if !plan.expected_generations.contains_key(&key(index)) {
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
                        start: first.offsets.start,
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
        Ok(CatalogOutcome::Object(Box::new(object.clone())))
    }

    fn acquire_pin(&mut self, pin: PinRecord) -> Result<CatalogOutcome, Rejection> {
        if let Some(old) = self.pins.get(&pin.id) {
            return if *old == pin {
                Ok(CatalogOutcome::Pinned)
            } else {
                Err(Rejection::Conflict)
            };
        }
        if self.pins.len() >= MAX_PINS
            || pin.objects.is_empty()
            || pin.objects.len() > MAX_PIN_OBJECTS
            || pin.ttl_millis == 0
            || u128::from(pin.ttl_millis) > MAX_PIN_TTL.as_millis()
        {
            return Err(Rejection::Capacity);
        }
        let unique: BTreeSet<_> = pin.objects.iter().copied().collect();
        if unique.len() != pin.objects.len() {
            return Err(Rejection::Conflict);
        }
        for id in &pin.objects {
            let object = self.objects.get(id).ok_or(Rejection::NotFound)?;
            if object.phase != ObjectPhase::Published || self.references(*id) == 0 {
                return Err(Rejection::References);
            }
        }
        self.pins.insert(pin.id, pin);
        Ok(CatalogOutcome::Pinned)
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
        Ok(CatalogOutcome::Released)
    }

    fn checkpoint(
        &mut self,
        id: Id,
        generation: u64,
        voters: &[u64],
        snapshots: &[(u64, String, u64)],
    ) -> Result<CatalogOutcome, Rejection> {
        let expected: BTreeSet<_> = voters.iter().copied().collect();
        let actual: BTreeSet<_> = snapshots.iter().map(|(node, _, _)| *node).collect();
        if !matches!(expected.len(), 1 | 3)
            || expected.contains(&0)
            || actual != expected
            || snapshots.len() != voters.len()
            || generation == 0
            || snapshots.iter().any(|(_, snapshot, index)| {
                snapshot.is_empty() || snapshot.len() > 128 || *index == 0
            })
        {
            return Err(Rejection::Membership);
        }
        let object = self.objects.get_mut(&id).ok_or(Rejection::NotFound)?;
        if object.phase != ObjectPhase::Published {
            return Err(Rejection::NotPublished);
        }
        object.checkpoint_generation = Some(generation);
        for old in self
            .objects
            .values_mut()
            .filter(|old| old.recovery_replacement == Some(id))
        {
            old.recovery_replacement = None;
            old.checkpoint_generation = Some(generation);
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

    fn references(&self, id: Id) -> usize {
        self.manifests
            .values()
            .map(|m| m.segments.iter().filter(|s| s.object_id == id).count())
            .sum()
    }

    pub(crate) fn validate(&self) -> Result<(), Rejection> {
        if self.objects.len() > MAX_OBJECTS || self.pins.len() > MAX_PINS {
            return Err(Rejection::InvalidSnapshot);
        }
        for (id, object) in &self.objects {
            validate_plan(&object.plan)?;
            if *id != object.object_id() || object.generation == 0 {
                return Err(Rejection::InvalidSnapshot);
            }
            if matches!(object.phase, ObjectPhase::Deleting | ObjectPhase::Deleted)
                && (self.references(*id) != 0
                    || object.recovery_replacement.is_some()
                    || self.pins.values().any(|pin| pin.objects.contains(id)))
            {
                return Err(Rejection::InvalidSnapshot);
            }
        }
        for (partition, manifest) in &self.manifests {
            let mut next = manifest
                .segments
                .first()
                .map_or(manifest.published_end, |s| s.offsets.start);
            if manifest.generation == 0 || manifest.segments.len() > MAX_SEGMENTS {
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
                next = segment.offsets.end;
            }
            if next != manifest.published_end {
                return Err(Rejection::InvalidSnapshot);
            }
        }
        for (id, pin) in &self.pins {
            if *id != pin.id
                || pin.objects.len() > MAX_PIN_OBJECTS
                || pin.objects.is_empty()
                || pin.ttl_millis == 0
                || u128::from(pin.ttl_millis) > MAX_PIN_TTL.as_millis()
                || pin.objects.iter().any(|id| {
                    self.objects
                        .get(id)
                        .is_none_or(|o| o.phase != ObjectPhase::Published)
                })
            {
                return Err(Rejection::InvalidSnapshot);
            }
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
