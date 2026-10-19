use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, StoredMembership};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::metadata::{self, DeploymentMode, MetadataState, NodeRole, Outcome, Response};
use crate::replication::{Application, DiskStateMachine, Raft};

use super::{
    Capacity, Error, MembershipEvidence, MembershipPlan, RepairBudget, RepairLease, Result,
    ScalingMetrics, Stage,
};

const VALIDITY: Duration = Duration::from_secs(3);

/// Runtime authority uses the actual initialized application identity.
pub trait MembershipApplication: Application {
    fn membership_identity(&self) -> Option<(tidemq_protocol::Id, u64)>;
}
impl MembershipApplication for crate::broker::DataApplication {
    fn membership_identity(&self) -> Option<(tidemq_protocol::Id, u64)> {
        use crate::ownership::OwnershipApplication;
        self.ownership_state()
            .map(|state| (state.cluster_id(), state.group_id()))
    }
}

/// Only a fresh quorum barrier and a stable applied membership construct this capability.
///
/// ```compile_fail
/// let _: tidemq::scaling::StableMembership = serde_json::from_str("{}").unwrap();
/// ```
pub struct StableMembership {
    evidence: MembershipEvidence,
    expires: Instant,
}

impl StableMembership {
    pub fn into_command(self, expected_revision: u64) -> Result<metadata::Command> {
        if Instant::now() >= self.expires {
            return Err(Error::Authority);
        }
        Ok(metadata::Command::new(
            self.evidence
                .plan
                .derived_id(true)
                .map_err(|reason| Error::Rejected { reason })?,
            expected_revision,
            metadata::Action::CompleteMembership(self.evidence),
        ))
    }
}

struct InFlight {
    plan: MembershipPlan,
    _repair: RepairLease,
    started: Instant,
}

/// The runtime forwards to the actual metadata/data leaders; neither leader is hard-coded.
/// A timed-out attempt retains its repair lease and resumes the same durable plan.
pub struct MembershipCoordinator<A: MembershipApplication> {
    metadata: Raft<MetadataState>,
    meta_state: DiskStateMachine<MetadataState>,
    data: Raft<A>,
    data_state: DiskStateMachine<A>,
    budget: RepairBudget,
    metrics: Arc<ScalingMetrics>,
    in_flight: Mutex<Option<InFlight>>,
}

impl<A: MembershipApplication> MembershipCoordinator<A> {
    pub fn new(
        metadata: Raft<MetadataState>,
        meta_state: DiskStateMachine<MetadataState>,
        data: Raft<A>,
        data_state: DiskStateMachine<A>,
        budget: RepairBudget,
        metrics: Arc<ScalingMetrics>,
    ) -> Self {
        Self {
            metadata,
            meta_state,
            data,
            data_state,
            budget,
            metrics,
            in_flight: Mutex::new(None),
        }
    }

    pub async fn execute(
        &self,
        plan: &MembershipPlan,
        copy_bytes: u64,
        capacity: &[Capacity],
        deadline: Instant,
    ) -> Result<()> {
        let queue = Instant::now();
        let mut flight = tokio::time::timeout_at(deadline, self.in_flight.lock())
            .await
            .map_err(|_| Error::Deadline)?;
        self.metrics.observe(Stage::Queue, queue.elapsed());
        let result = self
            .execute_locked(plan, copy_bytes, capacity, deadline, &mut flight)
            .await;
        if matches!(result, Err(Error::Unknown | Error::Consensus { .. })) {
            self.metrics.unknown();
        }
        result
    }

    async fn execute_locked(
        &self,
        plan: &MembershipPlan,
        copy_bytes: u64,
        capacity: &[Capacity],
        deadline: Instant,
        flight: &mut Option<InFlight>,
    ) -> Result<()> {
        plan.validate()
            .map_err(|reason| Error::Rejected { reason })?;
        let (revision, complete, nodes) = self
            .metadata_view(plan, capacity, copy_bytes, deadline)
            .await?;
        if complete {
            // Metadata is authoritative only after the actual stable data membership is rechecked.
            self.verify(plan, deadline).await?;
            *flight = None;
            return Ok(());
        }
        if flight.as_ref().is_some_and(|old| old.plan != *plan) {
            return Err(Error::Authority);
        }
        if flight.is_none() {
            let target = plan
                .target
                .difference(&plan.previous)
                .next()
                .copied()
                .ok_or(Error::Configuration)?;
            let repair = self
                .budget
                .register(target, plan.group_id, deadline)
                .await?;
            *flight = Some(InFlight {
                plan: plan.clone(),
                _repair: repair,
                started: Instant::now(),
            });
        }
        let command = metadata::Command::prepare_membership(revision, plan.clone())
            .map_err(|reason| Error::Rejected { reason })?;
        self.meta_write(command, deadline).await?;
        let lease = &mut flight.as_mut().ok_or(Error::Configuration)?._repair;
        self.reconfigure(plan, &nodes, capacity, copy_bytes, deadline, lease)
            .await?;
        let proof = self.verify(plan, deadline).await?;
        let (revision, _, _) = self
            .metadata_view(plan, capacity, copy_bytes, deadline)
            .await?;
        self.meta_write(proof.into_command(revision)?, deadline)
            .await?;
        if let Some(completed) = flight.take() {
            self.metrics
                .observe(Stage::ReplicaRepair, completed.started.elapsed());
            tracing::info!(event = "membership_replacement_complete", operation_id = %plan.operation_id,
                group_id = plan.group_id, generation = plan.generation,
                "Stable membership replacement was recorded in metadata");
        }
        Ok(())
    }

    async fn reconfigure(
        &self,
        plan: &MembershipPlan,
        nodes: &BTreeMap<u64, BasicNode>,
        capacity: &[Capacity],
        copy_bytes: u64,
        deadline: Instant,
        lease: &mut RepairLease,
    ) -> Result<()> {
        let actual = self.current_membership(deadline).await?;
        let configurations = actual.membership().get_joint_config();
        let valid_old = configurations.first() == Some(&plan.previous);
        let valid_target = configurations.last() == Some(&plan.target);
        if configurations.len() == 1 && valid_target {
            lease.set_learner(false)?;
            return Ok(());
        }
        if !(configurations.len() == 1 && valid_old
            || configurations.len() == 2 && valid_old && valid_target)
        {
            return Err(Error::Authority);
        }
        if configurations.len() == 1 {
            lease.set_learner(true)?;
            let started = Instant::now();
            for added in plan.target.difference(&plan.previous) {
                let node = nodes.get(added).cloned().ok_or(Error::Authority)?;
                tokio::time::timeout_at(deadline, self.data.add_learner(*added, node, true))
                    .await
                    .map_err(|_| Error::Unknown)?
                    .map_err(|source| Error::Consensus {
                        source: Box::new(source),
                    })?;
            }
            self.metrics.observe(Stage::Learner, started.elapsed());
        }
        // Catch-up can outlive one control window; refresh authority immediately before joint change.
        self.metadata_view(plan, capacity, copy_bytes, deadline)
            .await?;
        lease.set_learner(false)?;
        let started = Instant::now();
        tokio::time::timeout_at(
            deadline,
            self.data.change_membership(plan.target.clone(), false),
        )
        .await
        .map_err(|_| Error::Unknown)?
        .map_err(|source| Error::Consensus {
            source: Box::new(source),
        })?;
        self.metrics.observe(Stage::Membership, started.elapsed());
        Ok(())
    }

    pub async fn verify(
        &self,
        plan: &MembershipPlan,
        deadline: Instant,
    ) -> Result<StableMembership> {
        let identity = self.data_state.storage_identity();
        if identity.group.cluster_id != plan.cluster_id || identity.group.group_id != plan.group_id
        {
            return Err(Error::Authority);
        }
        let started = Instant::now();
        let expires = started
            .checked_add(VALIDITY)
            .ok_or(Error::Configuration)?
            .min(deadline);
        tokio::time::timeout_at(expires, async {
            self.data
                .ensure_linearizable()
                .await
                .map_err(|source| Error::Quorum {
                    source: Box::new(source),
                })?;
            self.data_state
                .try_with_membership(|application, applied, membership| {
                    if application.membership_identity() != Some((plan.cluster_id, plan.group_id))
                        || membership.membership().get_joint_config().len() != 1
                        || membership.membership().get_joint_config().first() != Some(&plan.target)
                    {
                        return Err(Error::Authority);
                    }
                    let membership_log = (*membership.log_id()).ok_or(Error::Authority)?;
                    let applied_log = applied.ok_or(Error::Authority)?;
                    if applied_log.index < membership_log.index {
                        return Err(Error::Authority);
                    }
                    Ok(StableMembership {
                        evidence: MembershipEvidence {
                            plan: plan.clone(),
                            membership_log,
                            applied_log,
                        },
                        expires,
                    })
                })
                .await
                .map_err(|source| Error::Storage { source })?
        })
        .await
        .map_err(|_| Error::Authority)?
    }

    async fn current_membership(
        &self,
        deadline: Instant,
    ) -> Result<StoredMembership<u64, BasicNode>> {
        tokio::time::timeout_at(deadline, async {
            self.data
                .ensure_linearizable()
                .await
                .map_err(|source| Error::Quorum {
                    source: Box::new(source),
                })?;
            self.data_state
                .try_with_membership(|_, _, membership| membership.clone())
                .await
                .map_err(|source| Error::Storage { source })
        })
        .await
        .map_err(|_| Error::Unknown)?
    }

    async fn metadata_view(
        &self,
        plan: &MembershipPlan,
        capacity: &[Capacity],
        copy_bytes: u64,
        deadline: Instant,
    ) -> Result<(u64, bool, BTreeMap<u64, BasicNode>)> {
        let expires = Instant::now()
            .checked_add(VALIDITY)
            .ok_or(Error::Configuration)?
            .min(deadline);
        tokio::time::timeout_at(expires, async {
            self.metadata
                .ensure_linearizable()
                .await
                .map_err(|source| Error::Quorum {
                    source: Box::new(source),
                })?;
            self.meta_state
                .try_with_membership(|state, _, membership| {
                    let identity = self.meta_state.storage_identity();
                    if identity.group.cluster_id != plan.cluster_id
                        || state.metadata_group_id() != Some(identity.group.group_id)
                    {
                        return Err(Error::Authority);
                    }
                    validate_metadata(state, membership, plan)?;
                    let group = state.group(plan.group_id).ok_or(Error::Authority)?;
                    let complete = group.generation == plan.generation.saturating_add(1)
                        && group.voters == plan.target;
                    if !complete
                        && (group.generation != plan.generation || group.voters != plan.previous)
                    {
                        return Err(Error::Authority);
                    }
                    if let Some(record) = state.membership_change(plan.group_id)
                        && !record.is_complete()
                        && record.plan != *plan
                    {
                        return Err(Error::Authority);
                    }
                    let mut nodes = BTreeMap::new();
                    for node_id in &plan.target {
                        let node = state.node(*node_id).ok_or(Error::Authority)?;
                        if !node.spec.roles.contains(&NodeRole::Storage) {
                            return Err(Error::Authority);
                        }
                        if !complete && !plan.previous.contains(node_id) {
                            validate_capacity(
                                node.spec.schedulable,
                                *node_id,
                                capacity,
                                copy_bytes,
                            )?;
                        }
                        nodes.insert(*node_id, BasicNode::new(node.spec.endpoint.to_string()));
                    }
                    Ok((state.revision(), complete, nodes))
                })
                .await
                .map_err(|source| Error::Storage { source })?
        })
        .await
        .map_err(|_| Error::Authority)?
    }

    async fn meta_write(&self, command: metadata::Command, deadline: Instant) -> Result<()> {
        let started = Instant::now();
        let response = tokio::time::timeout_at(deadline, self.metadata.client_write(command))
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
}

fn validate_metadata(
    state: &MetadataState,
    membership: &StoredMembership<u64, BasicNode>,
    plan: &MembershipPlan,
) -> Result<()> {
    if state.cluster_id() != Some(plan.cluster_id)
        || state.metadata_group_id().is_none()
        || membership.membership().get_joint_config().len() != 1
    {
        return Err(Error::Authority);
    }
    let factor = state.mode.ok_or(Error::Authority)?.replication_factor();
    let voters: BTreeSet<_> = membership.membership().voter_ids().take(4).collect();
    let mut zones = BTreeSet::new();
    for id in &voters {
        let node = state.node(*id).ok_or(Error::Authority)?;
        if !node.spec.roles.contains(&NodeRole::Metadata) {
            return Err(Error::Authority);
        }
        zones.insert(&node.spec.zone);
    }
    let target_zones: BTreeSet<_> = plan
        .target
        .iter()
        .filter_map(|id| state.node(*id).map(|n| &n.spec.zone))
        .collect();
    if voters.len() != factor
        || plan.target.len() != factor
        || state.mode == Some(DeploymentMode::ThreeReplica)
            && (zones.len() != 3 || target_zones.len() != 3)
    {
        return Err(Error::Authority);
    }
    Ok(())
}

fn validate_capacity(
    schedulable: bool,
    node: u64,
    capacity: &[Capacity],
    copy_bytes: u64,
) -> Result<()> {
    if !schedulable || capacity.len() > 256 {
        return Err(Error::Placement);
    }
    let mut matching = capacity.iter().filter(|report| report.node_id == node);
    let report = matching.next().ok_or(Error::Placement)?;
    let now = std::time::Instant::now();
    if matching.next().is_some()
        || report.observed_at > now
        || now.duration_since(report.observed_at) > Duration::from_secs(5)
        || report.free_bytes.saturating_sub(report.reserved_bytes)
            < copy_bytes
                .checked_add(1024 * 1024 * 1024)
                .ok_or(Error::Placement)?
        || report.cpu_per_mille >= 900
    {
        return Err(Error::Placement);
    }
    Ok(())
}
