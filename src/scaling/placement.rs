use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use crate::metadata::{DataGroup, MetadataState, NodeRole};

use super::{Error, MembershipPlan, Result};

#[derive(Clone, Copy, Debug)]
pub struct Capacity {
    pub node_id: u64,
    pub observed_at: Instant,
    pub free_bytes: u64,
    pub reserved_bytes: u64,
    pub bytes_per_second: u64,
    pub cpu_per_mille: u16,
    pub connections: u32,
    pub ack_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct PlacementPolicy {
    pub reserve_bytes: u64,
    pub max_rate_bytes_per_second: u64,
    pub max_connections: u32,
    pub max_ack_bytes: u64,
    pub max_observation_age: Duration,
    pub cooldown: Duration,
}
impl Default for PlacementPolicy {
    fn default() -> Self {
        Self {
            reserve_bytes: 1024 * 1024 * 1024,
            max_rate_bytes_per_second: 256 * 1024 * 1024,
            max_connections: 4096,
            max_ack_bytes: 256 * 1024 * 1024,
            max_observation_age: Duration::from_secs(5),
            cooldown: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Replacement {
    pub removed: u64,
    pub copy_bytes: u64,
    pub repair: bool,
}

/// Bounded observations and group cooldowns; repairs may bypass balance cooldown only.
pub struct Placement {
    policy: PlacementPolicy,
    completed: BTreeMap<u64, Instant>,
}

impl Placement {
    pub fn new(policy: PlacementPolicy) -> Result<Self> {
        if policy.max_rate_bytes_per_second == 0
            || policy.max_connections == 0
            || policy.max_ack_bytes == 0
            || policy.max_observation_age.is_zero()
            || policy.max_observation_age > Duration::from_secs(30)
            || policy.cooldown > Duration::from_secs(3600)
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            policy,
            completed: BTreeMap::new(),
        })
    }

    pub fn replacement(
        &self,
        state: &MetadataState,
        group: &DataGroup,
        replacement: Replacement,
        observations: &[Capacity],
        now: Instant,
    ) -> Result<MembershipPlan> {
        let Replacement {
            removed,
            copy_bytes,
            repair,
        } = replacement;
        let required = copy_bytes
            .checked_add(self.policy.reserve_bytes)
            .ok_or(Error::Placement)?;
        if observations.len() > 256
            || !group.voters.contains(&removed)
            || state.group(group.group_id) != Some(group)
            || !repair
                && self
                    .completed
                    .get(&group.group_id)
                    .is_some_and(|last| now.saturating_duration_since(*last) < self.policy.cooldown)
        {
            return Err(Error::Placement);
        }
        let mut seen = BTreeSet::new();
        let mut selected: Option<(u64, u64)> = None;
        let occupied: BTreeSet<_> = group
            .voters
            .iter()
            .filter(|id| **id != removed)
            .filter_map(|id| state.node(*id).map(|node| node.spec.zone.as_str()))
            .collect();
        for report in observations {
            if !seen.insert(report.node_id) {
                return Err(Error::Configuration);
            }
            let Some(node) = state.node(report.node_id) else {
                continue;
            };
            if group.voters.contains(&report.node_id)
                || !node.spec.schedulable
                || !node.spec.roles.contains(&NodeRole::Storage)
                || occupied.contains(node.spec.zone.as_str())
                || report.observed_at > now
                || now.duration_since(report.observed_at) > self.policy.max_observation_age
                || report.free_bytes.saturating_sub(report.reserved_bytes) < required
                || report.cpu_per_mille >= 900
                || report.bytes_per_second >= self.policy.max_rate_bytes_per_second
                || report.connections >= self.policy.max_connections
                || report.ack_bytes >= self.policy.max_ack_bytes
            {
                continue;
            }
            let score = u64::from(report.cpu_per_mille)
                .saturating_add(
                    report.bytes_per_second.saturating_mul(1000)
                        / self.policy.max_rate_bytes_per_second,
                )
                .saturating_add(
                    u64::from(report.connections).saturating_mul(1000)
                        / u64::from(self.policy.max_connections),
                )
                .saturating_add(report.ack_bytes.saturating_mul(1000) / self.policy.max_ack_bytes);
            let candidate = (score, report.node_id);
            if selected.is_none_or(|current| candidate < current) {
                selected = Some(candidate);
            }
        }
        let (_, node) = selected.ok_or(Error::Placement)?;
        let mut target = group.voters.clone();
        target.remove(&removed);
        target.insert(node);
        MembershipPlan::new(state.cluster_id().ok_or(Error::Authority)?, group, target)
            .map_err(|reason| Error::Rejected { reason })
    }

    pub fn completed(&mut self, group: u64, now: Instant) -> Result<()> {
        if group == 0 || self.completed.len() >= 256 && !self.completed.contains_key(&group) {
            return Err(Error::Overloaded);
        }
        self.completed.insert(group, now);
        Ok(())
    }
}
