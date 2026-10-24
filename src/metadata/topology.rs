use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tidemq_protocol::{
    Id,
    topic::{HashRange, ShardRoute, TopicLayout},
};

use super::{HistoricalBinding, MetadataState, PartitionKey, Rejection};

type LayoutTargets = (Vec<Id>, Vec<(HashRange, u64)>);

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Topology {
    pub(crate) layouts: BTreeMap<Id, TopicLayout>,
    pub(crate) shards: BTreeMap<Id, BirthShard>,
    pub(crate) plans: BTreeMap<Id, LayoutPlan>,
    pub(crate) bindings: BTreeMap<PartitionKey, HistoricalBinding>,
    pub(crate) transfers: BTreeMap<Id, super::AuthorityTransfer>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BirthShard {
    pub route: ShardRoute,
    pub partition: PartitionKey,
    pub predecessors: Vec<Id>,
    pub sealed_end: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LayoutStage {
    Prepare,
    Sealed,
    Active,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LayoutPlan {
    pub id: Id,
    pub topic: Id,
    pub revision: u64,
    pub sources: Vec<Id>,
    pub successors: Vec<BirthShard>,
    pub sealed: BTreeMap<Id, u64>,
    pub stage: LayoutStage,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LayoutChange {
    Split {
        source: Id,
        at: u64,
        groups: [u64; 2],
    },
    Merge {
        left: Id,
        right: Id,
        group: u64,
    },
}

impl Topology {
    pub fn transfer(&self, id: Id) -> Option<&super::AuthorityTransfer> {
        self.transfers.get(&id)
    }
    pub fn pending_transfers(&self) -> impl Iterator<Item = &super::AuthorityTransfer> {
        self.transfers
            .values()
            .filter(|plan| plan.stage != super::TransferStage::Retired)
    }
    pub(crate) fn transfer_blocks(&self, partition: PartitionKey) -> bool {
        self.pending_transfers()
            .any(|plan| plan.partition == partition && plan.stage < super::TransferStage::Published)
    }
    pub fn binding(&self, partition: PartitionKey) -> Option<&HistoricalBinding> {
        self.bindings.get(&partition)
    }
    pub fn layout(&self, topic: Id) -> Option<&TopicLayout> {
        self.layouts.get(&topic)
    }
    pub fn shard(&self, shard: Id) -> Option<&BirthShard> {
        self.shards.get(&shard)
    }
    pub fn plan(&self, plan: Id) -> Option<&LayoutPlan> {
        self.plans.get(&plan)
    }

    pub fn history(&self, topic: Id) -> impl Iterator<Item = &BirthShard> {
        self.shards
            .values()
            .filter(move |shard| shard.partition.topic_id == topic && shard.sealed_end.is_some())
    }

    pub fn domains(&self, topic: Id) -> impl Iterator<Item = &BirthShard> {
        self.shards
            .values()
            .filter(move |shard| shard.partition.topic_id == topic)
    }

    pub fn pending(&self) -> impl Iterator<Item = &LayoutPlan> {
        self.plans
            .values()
            .filter(|plan| plan.stage != LayoutStage::Active)
    }

    pub(crate) fn initial(
        topic: Id,
        groups: &[u64],
    ) -> Result<(TopicLayout, Vec<BirthShard>), Rejection> {
        if groups.is_empty() {
            return Err(Rejection::InvalidGroup);
        }
        let count = u128::try_from(groups.len()).map_err(|_| Rejection::Capacity)?;
        let mut shards = Vec::new();
        for (index, group) in groups.iter().enumerate() {
            let ordinal = u128::try_from(index).map_err(|_| Rejection::Capacity)?;
            let start = u64::try_from((1_u128 << 64) * ordinal / count)
                .map_err(|_| Rejection::CounterExhausted)?;
            let end = if ordinal + 1 == count {
                None
            } else {
                Some(
                    u64::try_from((1_u128 << 64) * (ordinal + 1) / count)
                        .map_err(|_| Rejection::CounterExhausted)?,
                )
            };
            let index = u32::try_from(index).map_err(|_| Rejection::Capacity)?;
            shards.push(BirthShard {
                route: ShardRoute {
                    id: identity(topic, index)?,
                    range: HashRange { start, end },
                    group: *group,
                    authority_epoch: 1,
                },
                partition: PartitionKey {
                    topic_id: topic,
                    partition: index,
                },
                predecessors: Vec::new(),
                sealed_end: None,
            });
        }
        let layout = TopicLayout {
            topic,
            revision: 1,
            shards: shards.iter().map(|shard| shard.route.clone()).collect(),
        };
        layout
            .validate(groups.len())
            .map_err(|_| Rejection::InvalidGroup)?;
        Ok((layout, shards))
    }

    pub(crate) fn install_initial(&mut self, layout: TopicLayout, shards: Vec<BirthShard>) {
        for shard in shards {
            self.bindings.insert(
                shard.partition,
                HistoricalBinding::initial(shard.route.id, shard.partition, shard.route.group),
            );
            self.shards.insert(shard.route.id, shard);
        }
        self.layouts.insert(layout.topic, layout);
    }

    pub fn validate(&self, maximum: usize) -> Result<(), Rejection> {
        if self.transfers.len() > super::MAX_AUTHORITY_TRANSFERS {
            return Err(Rejection::Capacity);
        }
        for (id, plan) in &self.transfers {
            plan.validate()?;
            if plan.id != *id
                || self
                    .shards
                    .get(&plan.shard)
                    .is_none_or(|shard| shard.partition != plan.partition)
            {
                return Err(Rejection::InvalidEvidence);
            }
        }
        if self.bindings.len() != self.shards.len() {
            return Err(Rejection::InvalidEvidence);
        }
        for (id, layout) in &self.layouts {
            if *id != layout.topic {
                return Err(Rejection::InvalidEvidence);
            }
            layout
                .validate(maximum)
                .map_err(|_| Rejection::InvalidEvidence)?;
            for route in &layout.shards {
                let shard = self.shards.get(&route.id).ok_or(Rejection::NotFound)?;
                if shard.route != *route
                    || shard.partition.topic_id != *id
                    || shard.sealed_end.is_some()
                {
                    return Err(Rejection::InvalidEvidence);
                }
            }
        }
        for (id, shard) in &self.shards {
            let binding = self
                .bindings
                .get(&shard.partition)
                .ok_or(Rejection::InvalidEvidence)?;
            if binding.partition != shard.partition
                || binding.shard != *id
                || binding.authority_group != shard.route.group
                || binding.generation != shard.route.authority_epoch
                || binding.origin_group == 0
                || !binding.accepts_object(binding.origin_group)
                || !binding.accepts_object(binding.authority_group)
                || binding.object_groups.len() > 64
                || binding.object_groups.contains(&0)
            {
                return Err(Rejection::InvalidEvidence);
            }
            if *id != shard.route.id || shard.route.group == 0 || shard.route.authority_epoch == 0 {
                return Err(Rejection::InvalidEvidence);
            }
            shard
                .route
                .range
                .validate()
                .map_err(|_| Rejection::InvalidEvidence)?;
            for predecessor in &shard.predecessors {
                let previous = self.shards.get(predecessor).ok_or(Rejection::NotFound)?;
                if previous.partition.topic_id != shard.partition.topic_id
                    || previous.sealed_end.is_none()
                    || previous.partition.partition >= shard.partition.partition
                {
                    return Err(Rejection::InvalidEvidence);
                }
            }
        }
        for (id, plan) in &self.plans {
            if *id != plan.id
                || plan.sources.is_empty()
                || plan.sources.len() > 2
                || plan.successors.is_empty()
                || plan.successors.len() > 2
            {
                return Err(Rejection::InvalidEvidence);
            }
            for source in &plan.sources {
                if !self.shards.contains_key(source) {
                    return Err(Rejection::NotFound);
                }
            }
            if plan.stage != LayoutStage::Prepare && plan.sealed.len() != plan.sources.len() {
                return Err(Rejection::InvalidEvidence);
            }
        }
        Ok(())
    }
}

fn identity(seed: Id, ordinal: u32) -> Result<Id, Rejection> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq/birth-shard/v1\0");
    hash.update(seed.as_bytes());
    hash.update(&ordinal.to_be_bytes());
    Id::from_slice(
        hash.finalize()
            .as_bytes()
            .get(..16)
            .ok_or(Rejection::InvalidEvidence)?,
    )
    .map_err(|_| Rejection::InvalidEvidence)
}

impl MetadataState {
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    pub(super) fn prepare_layout(
        &mut self,
        id: Id,
        topic: Id,
        expected: u64,
        change: &LayoutChange,
    ) -> Result<LayoutPlan, Rejection> {
        if !self.topic_writable(topic) {
            return Err(Rejection::IdentityRetired);
        }
        if self
            .topology
            .pending_transfers()
            .any(|plan| plan.partition.topic_id == topic)
            || self.consumer_subscriptions.values().any(|row| {
                row.topic == topic && (row.is_deleting() || !row.prepared && !row.is_deleted())
            })
            || self.pending_retention(topic)
        {
            return Err(Rejection::RecoveryRequired);
        }
        let layout = self.topology.layout(topic).ok_or(Rejection::NotFound)?;
        if layout.revision != expected {
            return Err(Rejection::RevisionConflict);
        }
        if self
            .topology
            .plans
            .values()
            .any(|plan| plan.topic == topic && plan.stage != LayoutStage::Active)
        {
            return Err(Rejection::RecoveryRequired);
        }
        if self.topology.plans.len() >= self.limits.operations {
            return Err(Rejection::Capacity);
        }
        let (sources, targets) = self.layout_targets(layout, change)?;
        let record = self.topics.get(&topic).ok_or(Rejection::NotFound)?;
        if record.status != super::RecordStatus::Active {
            return Err(Rejection::IdentityRetired);
        }
        let mut ordinal = record.partition_count;
        let mut successors = Vec::new();
        for (range, group) in targets {
            if !self.groups.contains_key(&group) {
                return Err(Rejection::NotFound);
            }
            successors.push(BirthShard {
                route: ShardRoute {
                    id: identity(id, ordinal)?,
                    range,
                    group,
                    authority_epoch: 1,
                },
                partition: PartitionKey {
                    topic_id: topic,
                    partition: ordinal,
                },
                predecessors: sources.clone(),
                sealed_end: None,
            });
            ordinal = ordinal.checked_add(1).ok_or(Rejection::CounterExhausted)?;
        }
        if ordinal as usize > self.limits.partitions_per_topic
            || self
                .partitions
                .len()
                .checked_add(successors.len())
                .is_none_or(|count| count > self.limits.partitions)
        {
            return Err(Rejection::Capacity);
        }
        let plan = LayoutPlan {
            id,
            topic,
            revision: expected,
            sources,
            successors,
            sealed: BTreeMap::new(),
            stage: LayoutStage::Prepare,
        };
        for shard in &plan.successors {
            self.partitions.insert(
                shard.partition,
                super::state::PartitionRecord::new(shard.route.group),
            );
        }
        self.topics
            .get_mut(&topic)
            .ok_or(Rejection::NotFound)?
            .partition_count = ordinal;
        self.topology.plans.insert(id, plan.clone());
        Ok(plan)
    }

    fn layout_targets(
        &self,
        layout: &TopicLayout,
        change: &LayoutChange,
    ) -> Result<LayoutTargets, Rejection> {
        let find = |id| {
            layout
                .shards
                .iter()
                .find(|shard| shard.id == id)
                .ok_or(Rejection::NotFound)
        };
        match change {
            LayoutChange::Split { source, at, groups } => {
                let source = find(*source)?;
                let (left, right) = source
                    .range
                    .split(*at)
                    .map_err(|_| Rejection::InvalidCommand)?;
                if groups.first() == groups.get(1) {
                    return Err(Rejection::PlacementViolation);
                }
                Ok((
                    vec![source.id],
                    vec![
                        (left, *groups.first().ok_or(Rejection::InvalidGroup)?),
                        (right, *groups.get(1).ok_or(Rejection::InvalidGroup)?),
                    ],
                ))
            }
            LayoutChange::Merge { left, right, group } => {
                let left = find(*left)?;
                let right = find(*right)?;
                let range = left
                    .range
                    .merge(right.range)
                    .map_err(|_| Rejection::InvalidCommand)?;
                Ok((vec![left.id, right.id], vec![(range, *group)]))
            }
        }
    }

    pub(super) fn observe_layout_seal(
        &mut self,
        plan: Id,
        source: Id,
        end: u64,
    ) -> Result<LayoutPlan, Rejection> {
        let plan = self
            .topology
            .plans
            .get_mut(&plan)
            .ok_or(Rejection::NotFound)?;
        if !plan.sources.contains(&source) {
            return Err(Rejection::InvalidEvidence);
        }
        if let Some(previous) = plan.sealed.get(&source) {
            return if *previous == end {
                Ok(plan.clone())
            } else {
                Err(Rejection::InvalidEvidence)
            };
        }
        if plan.stage != LayoutStage::Prepare {
            return Err(Rejection::Superseded);
        }
        plan.sealed.insert(source, end);
        if plan.sealed.len() == plan.sources.len() {
            plan.stage = LayoutStage::Sealed;
        }
        Ok(plan.clone())
    }

    pub(super) fn activate_layout(&mut self, plan_id: Id) -> Result<TopicLayout, Rejection> {
        let plan = self
            .topology
            .plans
            .get(&plan_id)
            .ok_or(Rejection::NotFound)?;
        let old = self
            .topology
            .layouts
            .get(&plan.topic)
            .ok_or(Rejection::NotFound)?;
        if plan.stage == LayoutStage::Active {
            return Ok(old.clone());
        }
        if plan.stage != LayoutStage::Sealed || old.revision != plan.revision {
            return Err(Rejection::RecoveryRequired);
        }
        for successor in &plan.successors {
            if self.route(successor.partition).is_none() {
                return Err(Rejection::RecoveryRequired);
            }
        }
        let mut shards: Vec<_> = old
            .shards
            .iter()
            .filter(|shard| !plan.sources.contains(&shard.id))
            .cloned()
            .collect();
        shards.extend(plan.successors.iter().map(|shard| shard.route.clone()));
        shards.sort_by_key(|shard| shard.range.start);
        let revision = old
            .revision
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let layout = TopicLayout {
            topic: old.topic,
            revision,
            shards,
        };
        layout
            .validate(self.limits.partitions_per_topic)
            .map_err(|_| Rejection::InvalidEvidence)?;
        for (id, end) in &plan.sealed {
            self.topology
                .shards
                .get_mut(id)
                .ok_or(Rejection::NotFound)?
                .sealed_end = Some(*end);
        }
        for shard in &plan.successors {
            self.topology.bindings.insert(
                shard.partition,
                HistoricalBinding::initial(shard.route.id, shard.partition, shard.route.group),
            );
            self.topology.shards.insert(shard.route.id, shard.clone());
        }
        self.topology.layouts.insert(layout.topic, layout.clone());
        self.topology
            .plans
            .get_mut(&plan_id)
            .ok_or(Rejection::NotFound)?
            .stage = LayoutStage::Active;
        Ok(layout)
    }
}
