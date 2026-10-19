use super::{
    CapacitySource, Error, MembershipApplication, MembershipPlan, Query, Reply, Result, Rpc,
    Service, wire,
};
use crate::{
    metadata::{self, DeploymentMode, NodeRole, Outcome, Response},
    scaling::{StableMembership, Stage},
};
use openraft::BasicNode;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use tokio::time::Instant;

impl<R: Rpc, C: CapacitySource, A: MembershipApplication> Service<R, C, A> {
    pub(super) async fn group(&self, group: u64, deadline: Instant) -> Result<Reply> {
        let (raft, state) = self.replicas.metadata.as_ref().ok_or(Error::Authority)?;
        let expires = (Instant::now() + Duration::from_secs(3)).min(deadline);
        tokio::time::timeout_at(expires, async {
            raft.ensure_linearizable()
                .await
                .map_err(|source| Error::Quorum {
                    source: Box::new(source),
                })?;
            state
                .try_with_membership(|state, _, membership| {
                    if state.cluster_id() != Some(self.config.cluster_id)
                        || state.metadata_group_id() != Some(self.config.metadata_group)
                        || state.mode != Some(self.config.mode)
                        || membership.membership().get_joint_config().len() != 1
                        || membership.membership().voter_ids().collect::<BTreeSet<_>>()
                            != self.config.metadata_voters
                    {
                        return Err(Error::Authority);
                    }
                    Ok(Reply::Group {
                        group: state.group(group).cloned().ok_or(Error::Authority)?,
                        migration: state.membership_change(group).cloned().map(Box::new),
                    })
                })
                .await
                .map_err(|source| Error::Storage { source })?
        })
        .await
        .map_err(|_| Error::Authority)?
    }

    pub(super) async fn view(
        &self,
        plan: &MembershipPlan,
        deadline: Instant,
    ) -> Result<wire::View> {
        let (raft, state) = self.replicas.metadata.as_ref().ok_or(Error::Authority)?;
        let expires = (Instant::now() + Duration::from_secs(3)).min(deadline);
        tokio::time::timeout_at(expires, async {
            raft.ensure_linearizable()
                .await
                .map_err(|source| Error::Quorum {
                    source: Box::new(source),
                })?;
            state
                .try_with_membership(|state, applied, membership| {
                    if state.cluster_id() != Some(self.config.cluster_id)
                        || state.metadata_group_id() != Some(self.config.metadata_group)
                        || state.mode != Some(self.config.mode)
                        || membership.membership().get_joint_config().len() != 1
                    {
                        return Err(Error::Authority);
                    }
                    let voters: BTreeSet<_> = membership.membership().voter_ids().take(4).collect();
                    if voters != self.config.metadata_voters {
                        return Err(Error::Authority);
                    }
                    let mut metadata_nodes = BTreeMap::new();
                    for id in &voters {
                        let node = state.node(*id).ok_or(Error::Authority)?;
                        metadata_nodes.insert(*id, node.spec.clone());
                    }
                    let mut nodes = BTreeMap::new();
                    for id in plan.previous.union(&plan.target) {
                        let node = state.node(*id).ok_or(Error::Authority)?;
                        nodes.insert(*id, node.spec.clone());
                    }
                    let target = plan
                        .target
                        .difference(&plan.previous)
                        .next()
                        .ok_or(Error::Configuration)?;
                    let mut reserved_copy_bytes = 0u64;
                    for record in state
                        .migrations
                        .values()
                        .filter(|record| !record.is_complete() && record.plan != *plan)
                    {
                        if record
                            .plan
                            .target
                            .difference(&record.plan.previous)
                            .any(|id| id == target)
                        {
                            reserved_copy_bytes = reserved_copy_bytes
                                .checked_add(record.copy_bytes)
                                .ok_or(Error::Placement)?;
                        }
                    }
                    Ok(wire::View {
                        cluster: self.config.cluster_id,
                        source: self.config.node_id,
                        metadata_group: self.config.metadata_group,
                        mode: self.config.mode,
                        metadata_nodes,
                        metadata_voters: voters,
                        applied: applied.ok_or(Error::Authority)?,
                        revision: state.revision(),
                        group: state
                            .group(plan.group_id)
                            .cloned()
                            .ok_or(Error::Authority)?,
                        migration: state.membership_change(plan.group_id).cloned(),
                        nodes,
                        reserved_copy_bytes,
                    })
                })
                .await
                .map_err(|source| Error::Storage { source })?
        })
        .await
        .map_err(|_| Error::Authority)?
    }

    pub(super) async fn prepare(
        &self,
        plan: &MembershipPlan,
        requested_copy: u64,
        deadline: Instant,
    ) -> Result<Reply> {
        let first = self.view(plan, deadline).await?;
        self.validate_view(plan, self.config.node_id, &first)?;
        if let Some(record) = &first.migration
            && record.plan == *plan
        {
            if requested_copy > record.copy_bytes {
                return Err(Error::Configuration);
            }
            return Ok(Reply::Done);
        }
        let (_, estimate) = self
            .call_leader(
                plan.group_id,
                Query::Estimate { plan: plan.clone() },
                deadline,
            )
            .await?;
        let Reply::Estimate { copy_bytes } = estimate else {
            return Err(Error::Authority);
        };
        if copy_bytes == 0 {
            return Err(Error::Authority);
        }
        let copy_bytes = copy_bytes.max(requested_copy);
        let target = plan
            .target
            .difference(&plan.previous)
            .next()
            .copied()
            .ok_or(Error::Configuration)?;
        let started = Instant::now();
        let reply = self
            .rpc
            .call(
                target,
                Query::Capacity {
                    group: plan.group_id,
                    node: target,
                },
                deadline,
            )
            .await?;
        let Reply::Capacity(capacity) = reply else {
            return Err(Error::Authority);
        };
        let view = self.view(plan, deadline).await?;
        self.validate_view(plan, self.config.node_id, &view)?;
        let needed = copy_bytes
            .checked_add(view.reserved_copy_bytes)
            .and_then(|v| v.checked_add(1024 * 1024 * 1024))
            .ok_or(Error::Placement)?;
        let age = Duration::from_millis(capacity.age_ms).saturating_add(started.elapsed());
        if capacity.node != target
            || age > Duration::from_secs(5)
            || capacity.free_bytes.saturating_sub(capacity.reserved_bytes) < needed
            || capacity.cpu_per_mille >= 900
            || capacity.bytes_per_second >= 256 * 1024 * 1024
            || capacity.connections >= 4096
            || capacity.ack_bytes >= 256 * 1024 * 1024
        {
            return Err(Error::Placement);
        }
        let command = metadata::Command::prepare_membership_with_capacity(
            view.revision,
            plan.clone(),
            copy_bytes,
        )
        .map_err(|reason| Error::Rejected { reason })?;
        self.meta_write(command, deadline).await?;
        Ok(Reply::Done)
    }

    pub(super) async fn complete(&self, plan: &MembershipPlan, deadline: Instant) -> Result<Reply> {
        let view = self.view(plan, deadline).await?;
        self.validate_view(plan, self.config.node_id, &view)?;
        if view
            .migration
            .as_ref()
            .is_some_and(|record| record.plan == *plan && record.is_complete())
        {
            return Ok(Reply::Done);
        }
        let started = Instant::now();
        let (node, reply) = self
            .call_leader(
                plan.group_id,
                Query::ObserveStable { plan: plan.clone() },
                deadline,
            )
            .await?;
        let Reply::Stable(observation) = reply else {
            return Err(Error::Authority);
        };
        if observation.source != node
            || !plan.target.contains(&node)
            || observation.voters != plan.target
            || observation.evidence.plan != *plan
            || observation.evidence.applied_log.index < observation.evidence.membership_log.index
        {
            return Err(Error::Authority);
        }
        let proof = StableMembership::from_remote(observation.evidence, started, deadline)?;
        let latest = self.view(plan, deadline).await?;
        self.validate_view(plan, self.config.node_id, &latest)?;
        self.meta_write(proof.into_command(latest.revision)?, deadline)
            .await?;
        Ok(Reply::Done)
    }

    pub(super) async fn fresh_view(
        &self,
        plan: &MembershipPlan,
        deadline: Instant,
    ) -> Result<wire::View> {
        let started = Instant::now();
        let (source, reply) = self
            .call_leader(
                self.config.metadata_group,
                Query::View { plan: plan.clone() },
                deadline,
            )
            .await?;
        if started.elapsed() >= Duration::from_secs(3) {
            return Err(Error::Authority);
        }
        let Reply::View(view) = reply else {
            return Err(Error::Authority);
        };
        self.validate_view(plan, source, &view)?;
        Ok(*view)
    }

    pub(super) fn validate_view(
        &self,
        plan: &MembershipPlan,
        source: u64,
        view: &wire::View,
    ) -> Result<()> {
        if view.cluster != self.config.cluster_id
            || view.source != source
            || view.metadata_group != self.config.metadata_group
            || view.mode != self.config.mode
            || view.metadata_voters != self.config.metadata_voters
            || !view.metadata_voters.contains(&source)
            || view.metadata_nodes.len() != self.config.metadata_voters.len()
            || view.group.group_id != plan.group_id
            || view.nodes.len() > 4
            || view.applied.leader_id.term == 0
        {
            return Err(Error::Authority);
        }
        let mut zones = BTreeSet::new();
        for id in &view.metadata_voters {
            let node = view.metadata_nodes.get(id).ok_or(Error::Authority)?;
            node.validate().map_err(|_| Error::Authority)?;
            if node.node_id != *id || !node.roles.contains(&NodeRole::Metadata) {
                return Err(Error::Authority);
            }
            zones.insert(&node.zone);
        }
        if view.mode == DeploymentMode::ThreeReplica && zones.len() != 3 {
            return Err(Error::Authority);
        }
        let mut target_zones = BTreeSet::new();
        for id in plan.previous.union(&plan.target) {
            let node = view.nodes.get(id).ok_or(Error::Authority)?;
            node.validate().map_err(|_| Error::Authority)?;
            if node.node_id != *id || !node.roles.contains(&NodeRole::Storage) {
                return Err(Error::Authority);
            }
            if plan.target.contains(id) {
                target_zones.insert(&node.zone);
            }
        }
        if view.mode == DeploymentMode::ThreeReplica && target_zones.len() != 3 {
            return Err(Error::Authority);
        }
        let old = view.group.generation == plan.generation && view.group.voters == plan.previous;
        let complete = view.group.generation == plan.generation.saturating_add(1)
            && view.group.voters == plan.target;
        if !old && !complete {
            return Err(Error::Rejected {
                reason: metadata::Rejection::Superseded,
            });
        }
        if let Some(record) = &view.migration
            && !record.is_complete()
            && record.plan != *plan
        {
            return Err(Error::Authority);
        }
        Ok(())
    }

    async fn meta_write(&self, command: metadata::Command, deadline: Instant) -> Result<()> {
        let (raft, _) = self.replicas.metadata.as_ref().ok_or(Error::Authority)?;
        let started = Instant::now();
        let response = tokio::time::timeout_at(deadline, raft.client_write(command))
            .await
            .map_err(|_| Error::Unknown)?
            .map_err(|source| Error::Consensus {
                source: Box::new(source),
            })?;
        self.metrics.observe(Stage::Metadata, started.elapsed());
        match response.data {
            Response::Applied {
                outcome: Outcome::MembershipPrepared(_) | Outcome::MembershipCompleted(_),
                ..
            } => Ok(()),
            Response::Rejected { reason, .. } => Err(Error::Rejected { reason }),
            _ => Err(Error::Authority),
        }
    }

    pub(super) fn target_nodes(
        &self,
        view: &wire::View,
        plan: &MembershipPlan,
    ) -> Result<BTreeMap<u64, BasicNode>> {
        let mut targets = BTreeMap::new();
        for id in &plan.target {
            let node = view.nodes.get(id).ok_or(Error::Authority)?;
            if !plan.previous.contains(id) && !node.schedulable {
                return Err(Error::Placement);
            }
            targets.insert(*id, BasicNode::new(node.endpoint.to_string()));
        }
        Ok(targets)
    }
}
