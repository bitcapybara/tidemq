use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use crate::metadata::{DataGroup, MetadataState, NodeRole, NodeSpec};
use tidemq_protocol::Id;

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
    cursor: Option<u64>,
    failed: BTreeMap<u64, (Instant, u32)>,
    diagnostic: PlacementDiagnostic,
    diagnosed_at: Instant,
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
            cursor: None,
            failed: BTreeMap::new(),
            diagnostic: PlacementDiagnostic::default(),
            diagnosed_at: Instant::now(),
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
        if group.placement.raft_voters().is_none() {
            return Err(Error::Unsupported);
        }
        if state.group(group.group_id) != Some(group) || observations.len() > 256 {
            return Err(Error::Placement);
        }
        let nodes = group
            .placement
            .hosts()
            .iter()
            .copied()
            .chain(observations.iter().map(|sample| sample.node_id))
            .filter_map(|id| state.node(id).map(|node| (id, node.spec.clone())))
            .collect();
        self.replacement_in(
            state.cluster_id().ok_or(Error::Authority)?,
            group,
            &nodes,
            replacement,
            observations,
            now,
        )
    }

    pub(crate) fn replacement_in(
        &self,
        cluster: Id,
        group: &DataGroup,
        nodes: &BTreeMap<u64, NodeSpec>,
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
            || !group.placement.hosts().contains(&removed)
            || !repair && self.cooling(group.group_id, now)
        {
            return Err(Error::Placement);
        }
        let mut seen = BTreeSet::new();
        let mut selected: Option<(u64, u64)> = None;
        let mut blocked = BlockReason::Topology;
        let occupied: BTreeSet<_> = group
            .placement
            .hosts()
            .iter()
            .filter(|id| **id != removed)
            .filter_map(|id| nodes.get(id).map(|node| node.zone.as_str()))
            .collect();
        for report in observations {
            if !seen.insert(report.node_id) {
                return Err(Error::Configuration);
            }
            let Some(node) = nodes.get(&report.node_id) else {
                continue;
            };
            if group.placement.hosts().contains(&report.node_id)
                || !node.schedulable
                || !node.roles.contains(&NodeRole::Storage)
                || occupied.contains(node.zone.as_str())
            {
                continue;
            }
            if let Some(reason) = self.capacity_block(report, required, now) {
                blocked = reason;
                continue;
            }
            let score = self.score(report);
            let candidate = (score, report.node_id);
            if selected.is_none_or(|current| candidate < current) {
                selected = Some(candidate);
            }
        }
        let (_, node) = selected.ok_or(Error::Capacity { reason: blocked })?;
        let mut target = group.placement.hosts().clone();
        target.remove(&removed);
        target.insert(node);
        MembershipPlan::new(cluster, group, target).map_err(|reason| Error::Rejected { reason })
    }

    pub fn diagnose(&self) -> PlacementDiagnostic {
        PlacementDiagnostic {
            age_ms: u64::try_from(self.diagnosed_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            ..self.diagnostic
        }
    }

    pub(crate) fn note(&mut self, group: u64, blocked: Option<BlockReason>, operation: Option<Id>) {
        self.diagnostic = PlacementDiagnostic {
            group: Some(group),
            blocked,
            operation,
            age_ms: 0,
        };
        self.diagnosed_at = Instant::now();
    }

    pub(crate) fn next_group(&mut self, groups: &BTreeMap<u64, BTreeSet<u64>>) -> Option<u64> {
        use std::ops::Bound::{Excluded, Unbounded};
        let next = self
            .cursor
            .and_then(|cursor| groups.range((Excluded(cursor), Unbounded)).next())
            .or_else(|| groups.first_key_value())
            .map(|(group, _)| *group);
        self.cursor = next;
        next
    }

    pub(crate) fn cooling(&self, group: u64, now: Instant) -> bool {
        self.completed
            .get(&group)
            .is_some_and(|last| now.saturating_duration_since(*last) < self.policy.cooldown)
    }

    pub(crate) fn failed(&self, node: u64, now: Instant) -> bool {
        self.failed.get(&node).is_some_and(|(since, count)| {
            *count >= 3 && now.saturating_duration_since(*since) >= Duration::from_secs(3)
        })
    }

    pub(crate) fn unreachable(&mut self, node: u64, failed: bool, now: Instant) -> bool {
        if !failed {
            self.failed.remove(&node);
            return false;
        }
        if !self.failed.contains_key(&node) && self.failed.len() >= 256 {
            return false;
        }
        let (since, attempts) = self.failed.entry(node).or_insert((now, 0));
        *attempts = attempts.saturating_add(1);
        *attempts >= 3 && now.saturating_duration_since(*since) >= Duration::from_secs(3)
    }

    pub(crate) fn score(&self, report: &Capacity) -> u64 {
        u64::from(report.cpu_per_mille)
            .saturating_add(
                report.bytes_per_second.saturating_mul(1000)
                    / self.policy.max_rate_bytes_per_second,
            )
            .saturating_add(
                u64::from(report.connections).saturating_mul(1000)
                    / u64::from(self.policy.max_connections),
            )
            .saturating_add(report.ack_bytes.saturating_mul(1000) / self.policy.max_ack_bytes)
    }

    pub(crate) fn capacity_block(
        &self,
        report: &Capacity,
        required: u64,
        now: Instant,
    ) -> Option<BlockReason> {
        if report.observed_at > now
            || now.duration_since(report.observed_at) > self.policy.max_observation_age
        {
            Some(BlockReason::ExpiredObservation)
        } else if report.free_bytes.saturating_sub(report.reserved_bytes) < required {
            Some(BlockReason::FreeCapacity)
        } else if report.cpu_per_mille >= 900 {
            Some(BlockReason::Cpu)
        } else if report.bytes_per_second >= self.policy.max_rate_bytes_per_second {
            Some(BlockReason::ByteRate)
        } else if report.connections >= self.policy.max_connections {
            Some(BlockReason::Connections)
        } else if report.ack_bytes >= self.policy.max_ack_bytes {
            Some(BlockReason::AckState)
        } else {
            None
        }
    }

    pub fn completed(&mut self, group: u64, now: Instant) -> Result<()> {
        if group == 0 || self.completed.len() >= 256 && !self.completed.contains_key(&group) {
            return Err(Error::Overloaded);
        }
        self.completed.insert(group, now);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub enum BlockReason {
    NoStandby,
    Topology,
    MissingObservation,
    ExpiredObservation,
    ReachabilityPending,
    FreeCapacity,
    Cpu,
    ByteRate,
    Connections,
    AckState,
    Cooldown,
    Balanced,
    Authority,
    Admission,
    Unknown,
}

impl std::fmt::Display for BlockReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::NoStandby => "No eligible standby replica is configured",
            Self::Topology => "Replica placement violates the stable topology",
            Self::MissingObservation => "Candidate has no capacity observation",
            Self::ExpiredObservation => "Candidate capacity observation has expired",
            Self::ReachabilityPending => "Candidate reachability has not been confirmed",
            Self::FreeCapacity => "Candidate has insufficient free storage or memory capacity",
            Self::Cpu => "Candidate CPU pressure exceeds the placement limit",
            Self::ByteRate => "Candidate byte rate exceeds the placement limit",
            Self::Connections => "Candidate connection pressure exceeds the placement limit",
            Self::AckState => "Required durable ACK state is not ready for movement",
            Self::Cooldown => "Placement cooldown has not elapsed",
            Self::Balanced => "Current placement is balanced; no move is needed",
            Self::Authority => "Required scaling authority is unavailable",
            Self::Admission => "Scaling admission capacity is exhausted",
            Self::Unknown => "Scaling operation outcome is unknown; reconcile the original plan",
        })
    }
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct PlacementDiagnostic {
    pub group: Option<u64>,
    pub blocked: Option<BlockReason>,
    pub operation: Option<Id>,
    pub age_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (DataGroup, BTreeMap<u64, NodeSpec>, Capacity) {
        let group = DataGroup {
            group_id: 2,
            generation: 1,
            placement: crate::metadata::GroupPlacement::Raft {
                voters: [1, 2, 3].into(),
            },
        };
        let nodes = (1..=4)
            .map(|id| {
                (
                    id,
                    NodeSpec {
                        node_id: id,
                        zone: format!("zone-{}", if id == 4 { 3 } else { id }),
                        endpoint: ([127, 0, 0, 1], 7000 + id as u16).into(),
                        roles: [NodeRole::Storage].into(),
                        schedulable: true,
                    },
                )
            })
            .collect();
        let report = Capacity {
            node_id: 4,
            observed_at: Instant::now(),
            free_bytes: 10 << 30,
            reserved_bytes: 0,
            bytes_per_second: 1,
            cpu_per_mille: 1,
            connections: 1,
            ack_bytes: 1,
        };
        (group, nodes, report)
    }

    #[test]
    fn repairs_bypass_cooldown_without_bypassing_capacity_or_zone() {
        let (group, mut nodes, mut report) = fixture();
        let mut placement = Placement::new(PlacementPolicy::default()).unwrap();
        let now = report.observed_at;
        placement.completed(group.group_id, now).unwrap();
        let request = Replacement {
            removed: 3,
            copy_bytes: 1 << 30,
            repair: false,
        };
        let cluster = Id::new_v7().unwrap();
        assert!(
            placement
                .replacement_in(cluster, &group, &nodes, request, &[report], now)
                .is_err()
        );
        let repair = Replacement {
            repair: true,
            ..request
        };
        assert!(
            placement
                .replacement_in(cluster, &group, &nodes, repair, &[report], now)
                .is_ok()
        );
        report.reserved_bytes = report.free_bytes;
        assert!(matches!(
            placement.replacement_in(cluster, &group, &nodes, repair, &[report], now),
            Err(Error::Capacity {
                reason: BlockReason::FreeCapacity
            })
        ));
        report.reserved_bytes = 0;
        nodes.get_mut(&4).unwrap().zone = "zone-2".into();
        assert!(matches!(
            placement.replacement_in(cluster, &group, &nodes, repair, &[report], now),
            Err(Error::Capacity {
                reason: BlockReason::Topology
            })
        ));
    }

    #[test]
    fn round_robin_and_confirmed_failure_state_remain_bounded_and_reset_on_unavailable() {
        let mut placement = Placement::new(PlacementPolicy::default()).unwrap();
        let groups = BTreeMap::from([(2, BTreeSet::new()), (3, BTreeSet::new())]);
        assert_eq!(placement.next_group(&groups), Some(2));
        assert_eq!(placement.next_group(&groups), Some(3));
        assert_eq!(placement.next_group(&groups), Some(2));
        let now = Instant::now();
        assert!(!placement.unreachable(1, true, now));
        assert!(!placement.unreachable(1, true, now + Duration::from_secs(2)));
        assert!(placement.unreachable(1, true, now + Duration::from_secs(3)));
        assert!(!placement.unreachable(1, false, now + Duration::from_secs(4)));
        assert!(!placement.failed(1, now + Duration::from_secs(5)));
    }
}
