use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use tokio::{task::JoinSet, time::Instant};

use super::{CapacitySource, Coordinator, MembershipApplication, Query, Reply, Rpc, Service};
use crate::{
    metadata::{DataGroup, NodeSpec},
    scaling::{
        BlockReason, Capacity, Error, MembershipPlan, MigrationRecord, Placement, Replacement,
        Result,
    },
};

#[derive(Clone, Debug, serde::Serialize)]
pub struct TickReport {
    pub group: u64,
    pub completed: Option<MembershipPlan>,
    pub blocked: Option<BlockReason>,
}
struct Topology {
    group: DataGroup,
    nodes: BTreeMap<u64, NodeSpec>,
    pending: Option<MigrationRecord>,
    standby: BTreeSet<u64>,
}
enum Sample {
    Measured(Capacity),
    Unavailable,
    Unreachable,
    Fatal,
}

impl<R: Rpc, C: CapacitySource, A: MembershipApplication> Service<R, C, A> {
    /// One owned runtime ticker supplies a bounded window of registered candidates and an original deadline.
    pub async fn tick(
        &self,
        placement: &mut Placement,
        groups: &BTreeMap<u64, BTreeSet<u64>>,
        deadline: Instant,
    ) -> Result<TickReport> {
        if groups.is_empty()
            || groups.len() > 16
            || groups
                .values()
                .any(|nodes| nodes.len() > 32 || nodes.contains(&0))
        {
            return Err(Error::Configuration);
        }
        let first = placement.next_group(groups).ok_or(Error::Configuration)?;
        let topology = match self.topology(groups, first, placement, deadline).await {
            Ok(topology) => topology,
            Err(error) => {
                placement.note(first, Some(BlockReason::Authority), None);
                return Err(error);
            }
        };
        let group = topology.group.group_id;
        let result = self.tick_group(placement, topology, deadline).await;
        if let Err(error) = &result {
            let blocked = match error {
                Error::Unknown | Error::Consensus { .. } | Error::Unreachable { .. } => {
                    BlockReason::Unknown
                }
                Error::Overloaded => BlockReason::Admission,
                Error::Capacity { reason } => *reason,
                Error::Placement => BlockReason::Topology,
                _ => BlockReason::Authority,
            };
            let operation = placement.diagnose().operation;
            placement.note(group, Some(blocked), operation);
        }
        result
    }

    async fn tick_group(
        &self,
        placement: &mut Placement,
        topology: Topology,
        deadline: Instant,
    ) -> Result<TickReport> {
        let group = topology.group.group_id;
        if let Some(record) = &topology.pending {
            return self
                .execute_tick(placement, record.plan.clone(), deadline)
                .await;
        }
        let candidates: BTreeSet<_> = topology
            .standby
            .difference(&topology.group.voters)
            .copied()
            .collect();
        if candidates.is_empty() {
            return Ok(block(placement, group, BlockReason::NoStandby));
        }
        let observations = samples(
            self.rpc.clone(),
            group,
            topology.nodes.keys().copied(),
            deadline,
        )
        .await?;
        let (removed, repair) = match select_removed(placement, &topology, &observations) {
            Ok(selection) => selection,
            Err(reason) => return Ok(block(placement, group, reason)),
        };
        if !repair && placement.cooling(group, std::time::Instant::now()) {
            return Ok(block(placement, group, BlockReason::Cooldown));
        }
        let reports = reports(&observations);
        let replacement = Replacement {
            removed,
            copy_bytes: 0,
            repair,
        };
        let first = placement.replacement_in(
            self.config.cluster_id,
            &topology.group,
            &topology.nodes,
            replacement,
            &reports,
            std::time::Instant::now(),
        )?;
        if !repair && !improves(placement, &first, &reports) {
            return Ok(block(placement, group, BlockReason::Balanced));
        }
        let (copy_bytes, reports) = self.snapshot_reports(&topology, first, deadline).await?;
        if !repair && reports.len() != topology.nodes.len() {
            return Ok(block(placement, group, BlockReason::MissingObservation));
        }
        let plan = placement.replacement_in(
            self.config.cluster_id,
            &topology.group,
            &topology.nodes,
            Replacement {
                copy_bytes,
                ..replacement
            },
            &reports,
            std::time::Instant::now(),
        )?;
        if !repair && !improves(placement, &plan, &reports) {
            return Ok(block(placement, group, BlockReason::Balanced));
        }
        self.execute_tick(placement, plan, deadline).await
    }

    async fn snapshot_reports(
        &self,
        topology: &Topology,
        plan: MembershipPlan,
        deadline: Instant,
    ) -> Result<(u64, Vec<Capacity>)> {
        let (_, reply) = self
            .call_leader(topology.group.group_id, Query::Estimate { plan }, deadline)
            .await?;
        let Reply::Estimate { copy_bytes } = reply else {
            return Err(Error::Authority);
        };
        let fresh = samples(
            self.rpc.clone(),
            topology.group.group_id,
            topology.nodes.keys().copied(),
            deadline,
        )
        .await?;
        Ok((copy_bytes, reports(&fresh)))
    }

    async fn execute_tick(
        &self,
        placement: &mut Placement,
        plan: MembershipPlan,
        deadline: Instant,
    ) -> Result<TickReport> {
        placement.note(plan.group_id, None, Some(plan.operation_id));
        Coordinator::new(self.rpc.clone(), self.config.metadata_group)?
            .execute(&plan, deadline)
            .await?;
        placement.completed(plan.group_id, std::time::Instant::now())?;
        placement.note(plan.group_id, None, Some(plan.operation_id));
        Ok(TickReport {
            group: plan.group_id,
            completed: Some(plan),
            blocked: None,
        })
    }

    async fn topology(
        &self,
        groups: &BTreeMap<u64, BTreeSet<u64>>,
        first: u64,
        placement: &Placement,
        deadline: Instant,
    ) -> Result<Topology> {
        let (raft, state) = self.replicas.metadata.as_ref().ok_or(Error::Authority)?;
        tokio::time::timeout_at(
            deadline.min(Instant::now() + Duration::from_secs(3)),
            async {
                raft.ensure_linearizable()
                    .await
                    .map_err(|source| Error::Quorum {
                        source: Box::new(source),
                    })?;
                state
                    .try_with_membership(|state, applied, membership| {
                        if applied.is_none()
                            || state.cluster_id() != Some(self.config.cluster_id)
                            || state.metadata_group_id() != Some(self.config.metadata_group)
                            || state.mode != Some(self.config.mode)
                            || membership.membership().get_joint_config().len() != 1
                            || membership.membership().voter_ids().collect::<BTreeSet<_>>()
                                != self.metadata_voters()?
                        {
                            return Err(Error::Authority);
                        }
                        let ordered = groups
                            .range(first..)
                            .chain(groups.range(..first))
                            .map(|(group, _)| *group);
                        let selected = ordered
                            .into_iter()
                            .enumerate()
                            .filter_map(|(offset, group)| {
                                let data = state.group(group)?;
                                let pending = state
                                    .membership_change(group)
                                    .is_some_and(|record| !record.is_complete());
                                let repair = data.voters.iter().any(|node| {
                                    state.node(*node).is_some_and(|node| !node.spec.schedulable)
                                        || placement.failed(*node, std::time::Instant::now())
                                });
                                Some((
                                    if pending {
                                        0
                                    } else if repair {
                                        1
                                    } else {
                                        2
                                    },
                                    offset,
                                    group,
                                ))
                            })
                            .min();
                        let (_, _, selected) = selected.ok_or(Error::Authority)?;
                        let group = state.group(selected).cloned().ok_or(Error::Authority)?;
                        let standby = groups.get(&selected).cloned().ok_or(Error::Configuration)?;
                        if standby.union(&group.voters).count() > 32 {
                            return Err(Error::Overloaded);
                        }
                        let mut nodes = BTreeMap::new();
                        for id in standby.union(&group.voters) {
                            nodes
                                .insert(*id, state.node(*id).ok_or(Error::Authority)?.spec.clone());
                        }
                        let pending = state
                            .membership_change(selected)
                            .filter(|record| !record.is_complete())
                            .cloned();
                        Ok(Topology {
                            group,
                            standby,
                            nodes,
                            pending,
                        })
                    })
                    .await
                    .map_err(|source| Error::Storage { source })?
            },
        )
        .await
        .map_err(|_| Error::Deadline)?
    }
}

fn block(placement: &mut Placement, group: u64, reason: BlockReason) -> TickReport {
    placement.note(group, Some(reason), None);
    TickReport {
        group,
        completed: None,
        blocked: Some(reason),
    }
}

fn select_removed(
    placement: &mut Placement,
    topology: &Topology,
    samples: &BTreeMap<u64, Sample>,
) -> std::result::Result<(u64, bool), BlockReason> {
    let now = std::time::Instant::now();
    let mut repair = None;
    let mut missing = false;
    let mut worst = None;
    for node in &topology.group.voters {
        let reachable = samples.get(node);
        let failed =
            placement.unreachable(*node, matches!(reachable, Some(Sample::Unreachable)), now);
        let fatal = matches!(reachable, Some(Sample::Fatal));
        let draining = topology
            .nodes
            .get(node)
            .is_some_and(|node| !node.schedulable);
        if failed || draining || fatal {
            if repair.replace(*node).is_some() {
                return Err(BlockReason::Authority);
            }
            continue;
        }
        if let Some(Sample::Measured(sample)) = reachable {
            let score = (placement.score(sample), *node);
            if worst.is_none_or(|old| score > old) {
                worst = Some(score);
            }
        } else {
            missing = true;
        }
    }
    if let Some(removed) = repair {
        if missing {
            return Err(BlockReason::MissingObservation);
        }
        return Ok((removed, true));
    }
    if missing
        || topology
            .standby
            .difference(&topology.group.voters)
            .any(|node| !matches!(samples.get(node), Some(Sample::Measured(_))))
    {
        return Err(BlockReason::MissingObservation);
    }
    worst
        .map(|(_, node)| (node, false))
        .ok_or(BlockReason::MissingObservation)
}

fn improves(placement: &Placement, plan: &MembershipPlan, reports: &[Capacity]) -> bool {
    let removed = plan.previous.difference(&plan.target).next();
    let added = plan.target.difference(&plan.previous).next();
    let old = reports
        .iter()
        .find(|report| Some(&report.node_id) == removed);
    let new = reports.iter().find(|report| Some(&report.node_id) == added);
    match (old, new) {
        (Some(old), Some(new)) => placement.score(old) > placement.score(new).saturating_add(200),
        _ => false,
    }
}

async fn samples<R: Rpc>(
    rpc: Arc<R>,
    group: u64,
    nodes: impl Iterator<Item = u64>,
    deadline: Instant,
) -> Result<BTreeMap<u64, Sample>> {
    let mut nodes = nodes;
    let mut tasks = JoinSet::new();
    let mut result = BTreeMap::new();
    loop {
        while tasks.len() < 4 {
            let Some(node) = nodes.next() else {
                break;
            };
            let rpc = rpc.clone();
            tasks.spawn(async move {
                let started = Instant::now();
                let attempt = deadline.min(started + Duration::from_millis(500));
                let joined = deadline.min(attempt + Duration::from_millis(10));
                let sample = match tokio::time::timeout_at(
                    joined,
                    rpc.call(node, Query::Capacity { group, node }, attempt),
                )
                .await
                {
                    Ok(Ok(Reply::Capacity(sample))) => {
                        match sample.measured(node, started.elapsed()) {
                            Ok(sample) => Sample::Measured(sample),
                            Err(_) => Sample::Unavailable,
                        }
                    }
                    Ok(Ok(Reply::ReplicaFailed {
                        group: observed,
                        node: source,
                        ..
                    })) if observed == group && source == node => Sample::Fatal,
                    Ok(Err(Error::Unreachable { .. })) => Sample::Unreachable,
                    _ => Sample::Unavailable,
                };
                (node, sample)
            });
        }
        if tasks.is_empty() {
            return Ok(result);
        }
        let (node, sample) = tokio::time::timeout_at(deadline, tasks.join_next())
            .await
            .map_err(|_| Error::Deadline)?
            .ok_or(Error::Authority)?
            .map_err(|source| Error::Worker { source })?;
        result.insert(node, sample);
    }
}

fn reports(samples: &BTreeMap<u64, Sample>) -> Vec<Capacity> {
    samples
        .values()
        .filter_map(|sample| match sample {
            Sample::Measured(report) => Some(*report),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaling::remote::ReplicaFailure;

    struct FailureReply;
    impl Rpc for FailureReply {
        async fn leader(&self, _group: u64, _deadline: Instant) -> Result<u64> {
            Err(Error::Authority)
        }
        async fn call(&self, node: u64, query: Query, _deadline: Instant) -> Result<Reply> {
            let Query::Capacity { group, .. } = query else {
                return Err(Error::Authority);
            };
            Ok(Reply::ReplicaFailed {
                group: if node == 2 { group + 1 } else { group },
                node: if node == 3 { node + 1 } else { node },
                failure: ReplicaFailure::RaftStopped,
            })
        }
    }

    #[tokio::test]
    async fn local_failure_must_match_the_requested_group_and_authenticated_target() {
        let result = samples(
            Arc::new(FailureReply),
            17,
            [1, 2, 3].into_iter(),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(matches!(result.get(&1), Some(Sample::Fatal)));
        assert!(matches!(result.get(&2), Some(Sample::Unavailable)));
        assert!(matches!(result.get(&3), Some(Sample::Unavailable)));
    }
}
