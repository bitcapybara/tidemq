//! Closed semantic scaling RPCs. The transport must authenticate and correlate every peer call.

mod data;
mod metadata;
mod tick;
mod wire;

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
};
use tidemq_protocol::{Id, common::TrafficClass};
use tokio::{
    sync::{Mutex, Semaphore},
    time::Instant,
};

use super::{
    Capacity, Error, MembershipApplication, MembershipPlan, MigrationRecord, RepairBudget,
    RepairLease, Result, ScalingMetrics, StableMembership,
};
use crate::{
    metadata::{DataGroup, DeploymentMode, MetadataState},
    replication::{DiskLog, DiskStateMachine, Raft},
    transport::NodeGrant,
};

pub use tick::TickReport;
pub(crate) use wire::CapacityReport;
pub use wire::{
    Query, ReplicaFailure, Reply, decode_query, decode_reply, encode_query, encode_reply,
};

/// Calls use the configured certificate-bound target, fresh request IDs and the original deadline.
pub trait Rpc: Send + Sync + 'static {
    fn leader(&self, group: u64, deadline: Instant) -> impl Future<Output = Result<u64>> + Send;
    fn call(
        &self,
        node: u64,
        query: Query,
        deadline: Instant,
    ) -> impl Future<Output = Result<Reply>> + Send;
}

/// Reads the local cached, measured capacity. Missing or expired samples are unavailable.
pub trait CapacitySource: Send + Sync + 'static {
    fn snapshot(&self) -> Option<Capacity>;
}
impl<F: Fn() -> Option<Capacity> + Send + Sync + 'static> CapacitySource for F {
    fn snapshot(&self) -> Option<Capacity> {
        self()
    }
}

#[derive(Clone)]
pub struct ServiceConfig {
    pub cluster_id: Id,
    pub node_id: u64,
    pub metadata_group: u64,
    pub mode: DeploymentMode,
    pub metadata_voters: BTreeSet<u64>,
}

pub struct LocalReplicas<A: MembershipApplication = crate::broker::DataApplication> {
    metadata: Option<(Raft<MetadataState>, DiskStateMachine<MetadataState>)>,
    data: BTreeMap<u64, DataNode<A>>,
}
impl<A: MembershipApplication> LocalReplicas<A> {
    pub fn new(
        metadata: Option<(Raft<MetadataState>, DiskStateMachine<MetadataState>)>,
        data: Vec<(Raft<A>, DiskLog<A>, DiskStateMachine<A>)>,
    ) -> Result<Self> {
        if data.len() > 256 {
            return Err(Error::Overloaded);
        }
        let mut nodes = BTreeMap::new();
        for (raft, log, state) in data {
            let group = state.storage_identity().group.group_id;
            if nodes.insert(group, DataNode { raft, log, state }).is_some() {
                return Err(Error::Configuration);
            }
        }
        Ok(Self {
            metadata,
            data: nodes,
        })
    }
}

#[derive(Clone)]
struct DataNode<A: MembershipApplication> {
    raft: Raft<A>,
    log: DiskLog<A>,
    state: DiskStateMachine<A>,
}
struct Flight {
    plan: MembershipPlan,
    repair: RepairLease,
    started: Instant,
}

pub struct Service<
    R: Rpc,
    C: CapacitySource,
    A: MembershipApplication = crate::broker::DataApplication,
> {
    config: ServiceConfig,
    replicas: LocalReplicas<A>,
    rpc: Arc<R>,
    capacity: Arc<C>,
    budget: RepairBudget,
    metrics: Arc<ScalingMetrics>,
    metadata_voters: std::sync::RwLock<BTreeSet<u64>>,
    flights: std::sync::Mutex<BTreeMap<u64, Arc<Mutex<Option<Flight>>>>>,
    control_slots: Arc<Semaphore>,
    read_slots: Arc<Semaphore>,
    estimates: Arc<Semaphore>,
}

impl<R: Rpc, C: CapacitySource, A: MembershipApplication> Service<R, C, A> {
    pub fn new(
        config: ServiceConfig,
        replicas: LocalReplicas<A>,
        rpc: Arc<R>,
        capacity: Arc<C>,
        budget: RepairBudget,
        metrics: Arc<ScalingMetrics>,
    ) -> Result<Self> {
        if config.node_id == 0
            || config.metadata_group == 0
            || config.metadata_voters.len() != config.mode.replication_factor()
            || config.metadata_voters.contains(&0)
        {
            return Err(Error::Configuration);
        }
        if let Some((_, state)) = &replicas.metadata {
            let id = state.storage_identity();
            if id.group.cluster_id != config.cluster_id
                || id.group.group_id != config.metadata_group
                || id.node_id != config.node_id
            {
                return Err(Error::Configuration);
            }
        }
        for node in replicas.data.values() {
            let id = node.state.storage_identity();
            if id.group.cluster_id != config.cluster_id
                || id.group.group_id == config.metadata_group
                || id.node_id != config.node_id
            {
                return Err(Error::Configuration);
            }
        }
        Ok(Self {
            metadata_voters: std::sync::RwLock::new(config.metadata_voters.clone()),
            config,
            replicas,
            rpc,
            capacity,
            budget,
            metrics,
            flights: std::sync::Mutex::new(BTreeMap::new()),
            estimates: Arc::new(Semaphore::new(1)),
            control_slots: Arc::new(Semaphore::new(1)),
            read_slots: Arc::new(Semaphore::new(2)),
        })
    }

    pub(crate) fn replace_metadata_voters(&self, voters: BTreeSet<u64>) -> Result<()> {
        if voters.len() != self.config.mode.replication_factor() || voters.contains(&0) {
            return Err(Error::Authority);
        }
        *self.metadata_voters.write().map_err(|_| Error::Authority)? = voters;
        Ok(())
    }

    fn metadata_voters(&self) -> Result<BTreeSet<u64>> {
        self.metadata_voters
            .read()
            .map(|voters| voters.clone())
            .map_err(|_| Error::Authority)
    }

    pub async fn handle(&self, peer: &NodeGrant, query: Query, deadline: Instant) -> Result<Reply> {
        self.authorize(peer, &query)?;
        let _slot = match &query {
            Query::Prepare { .. } | Query::Complete { .. } => Some(
                self.control_slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::Overloaded)?,
            ),
            Query::Capacity { .. }
            | Query::View { .. }
            | Query::ObserveStable { .. }
            | Query::Finish { .. }
            | Query::Group { .. } => Some(
                self.read_slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::Overloaded)?,
            ),
            _ => None,
        };
        let result = match query {
            Query::Capacity { group, node } => self.capacity_reply(group, node, deadline).await,
            Query::Group { group } => self.group(group, deadline).await,
            Query::View { plan } => self
                .view(&plan, deadline)
                .await
                .map(|view| Reply::View(Box::new(view))),
            Query::Estimate { plan } => self.estimate(&plan, deadline).await,
            Query::Prepare { plan, copy_bytes } => self.prepare(&plan, copy_bytes, deadline).await,
            Query::Change { plan } => self.change(&plan, deadline).await,
            Query::ObserveStable { plan } => self
                .observe(&plan, deadline)
                .await
                .map(|observation| Reply::Stable(Box::new(observation))),
            Query::Complete { plan } => self.complete(&plan, deadline).await,
            Query::Finish { plan } => self.finish(&plan, deadline).await,
        };
        if matches!(result, Err(Error::Unknown | Error::Consensus { .. })) {
            self.metrics.unknown();
        }
        result
    }

    fn authorize(&self, peer: &NodeGrant, query: &Query) -> Result<()> {
        let group = query.group_id(self.config.metadata_group);
        let reader = matches!(
            query,
            Query::View { .. } | Query::Capacity { .. } | Query::Group { .. }
        );
        let allowed_role = matches!(peer.role.as_str(), "metadata" | "combined")
            || reader && matches!(peer.role.as_str(), "storage" | "broker");
        if peer.cluster_id != self.config.cluster_id
            || peer.node_id == 0
            || !peer.allowed_classes.contains(&TrafficClass::Control)
            || !peer.allowed_groups.contains(&group)
            || !allowed_role
        {
            return Err(Error::Authority);
        }
        if let Some(plan) = query.plan() {
            plan.validate()
                .map_err(|reason| Error::Rejected { reason })?;
            if plan.cluster_id != self.config.cluster_id
                || plan.group_id == self.config.metadata_group
            {
                return Err(Error::Authority);
            }
        }
        Ok(())
    }

    async fn capacity_reply(&self, group: u64, node: u64, deadline: Instant) -> Result<Reply> {
        if node != self.config.node_id {
            return Err(Error::Authority);
        }
        let replica = self.replicas.data.get(&group).ok_or(Error::Authority)?;
        // This local failed gate is neither quorum nor membership-completion evidence.
        let application_failed = tokio::time::timeout_at(deadline, async {
            replica.state.0.applied.read().await.failed
        })
        .await;
        match application_failed {
            Ok(true) => {
                return Ok(Reply::ReplicaFailed {
                    group,
                    node,
                    failure: ReplicaFailure::ApplicationFailed,
                });
            }
            Err(_) => return Ok(Reply::CapacityUnavailable),
            Ok(false) => {}
        }
        if replica.raft.metrics().borrow().running_state.is_err() {
            return Ok(Reply::ReplicaFailed {
                group,
                node,
                failure: ReplicaFailure::RaftStopped,
            });
        }
        let Some(report) = self.capacity.snapshot() else {
            return Ok(Reply::CapacityUnavailable);
        };
        if report.node_id != node {
            return Err(Error::Authority);
        }
        match wire::CapacityReport::from_sample(report) {
            Ok(report) => Ok(Reply::Capacity(report)),
            Err(Error::Placement) => Ok(Reply::CapacityUnavailable),
            Err(error) => Err(error),
        }
    }

    async fn call_leader(
        &self,
        group: u64,
        query: Query,
        deadline: Instant,
    ) -> Result<(u64, Reply)> {
        let node = self.rpc.leader(group, deadline).await?;
        Ok((node, self.rpc.call(node, query, deadline).await?))
    }
}

/// Management executes on an authorized metadata service identity and keeps one logical plan.
pub struct Coordinator<R: Rpc> {
    rpc: Arc<R>,
    metadata_group: u64,
}
impl<R: Rpc> Coordinator<R> {
    pub fn new(rpc: Arc<R>, metadata_group: u64) -> Result<Self> {
        if metadata_group == 0 {
            return Err(Error::Configuration);
        }
        Ok(Self {
            rpc,
            metadata_group,
        })
    }
    pub async fn plan(
        &self,
        cluster: Id,
        group: u64,
        removed: u64,
        added: u64,
        deadline: Instant,
    ) -> Result<MembershipPlan> {
        let (current, migration) = self.group(group, deadline).await?;
        if let Some(record) = migration {
            let same = record.plan.cluster_id == cluster
                && record
                    .plan
                    .previous
                    .difference(&record.plan.target)
                    .copied()
                    .eq([removed])
                && record
                    .plan
                    .target
                    .difference(&record.plan.previous)
                    .copied()
                    .eq([added]);
            if same
                && (!record.is_complete()
                    || current.generation
                        == record
                            .plan
                            .generation
                            .checked_add(1)
                            .ok_or(Error::Configuration)?
                        && current.voters == record.plan.target)
            {
                return Ok(record.plan);
            }
            if !record.is_complete() {
                return Err(Error::Authority);
            }
        }
        let mut target = current.voters.clone();
        if !target.remove(&removed) || !target.insert(added) {
            return Err(Error::Configuration);
        }
        MembershipPlan::new(cluster, &current, target).map_err(|reason| Error::Rejected { reason })
    }

    pub async fn execute(&self, plan: &MembershipPlan, deadline: Instant) -> Result<()> {
        plan.validate()
            .map_err(|reason| Error::Rejected { reason })?;
        if !self.verify_completed(plan, deadline).await? {
            for (phase, query) in [
                (
                    "prepare membership",
                    Query::Prepare {
                        plan: plan.clone(),
                        copy_bytes: 0,
                    },
                ),
                ("change membership", Query::Change { plan: plan.clone() }),
                ("complete metadata", Query::Complete { plan: plan.clone() }),
            ] {
                let group = query.group_id(self.metadata_group);
                let node = self
                    .rpc
                    .leader(group, deadline)
                    .await
                    .map_err(|error| phase_failure(phase, error))?;
                if !matches!(
                    self.rpc
                        .call(node, query, deadline)
                        .await
                        .map_err(|error| phase_failure(phase, error))?,
                    Reply::Done
                ) {
                    return Err(Error::Authority);
                }
            }
        }
        // Old leaders may retain their node-local repair lease after an unknown response.
        for node in plan.previous.union(&plan.target) {
            let cleanup_deadline =
                (Instant::now() + std::time::Duration::from_millis(250)).min(deadline);
            let _ = self
                .rpc
                .call(
                    *node,
                    Query::Finish { plan: plan.clone() },
                    cleanup_deadline,
                )
                .await;
        }
        Ok(())
    }

    async fn group(
        &self,
        group: u64,
        deadline: Instant,
    ) -> Result<(DataGroup, Option<MigrationRecord>)> {
        let node = self
            .rpc
            .leader(self.metadata_group, deadline)
            .await
            .map_err(|error| phase_failure("discover metadata leader", error))?;
        let Reply::Group {
            group: current,
            migration,
        } = self
            .rpc
            .call(node, Query::Group { group }, deadline)
            .await
            .map_err(|error| phase_failure("read canonical metadata group", error))?
        else {
            return Err(Error::Authority);
        };
        if current.group_id != group {
            return Err(Error::Authority);
        }
        Ok((current, migration.map(|record| *record)))
    }

    async fn verify_completed(&self, plan: &MembershipPlan, deadline: Instant) -> Result<bool> {
        let (group, record) = self.group(plan.group_id, deadline).await?;
        if !completed_record(plan, &group, record.as_ref())? {
            return Ok(false);
        }
        // Completed replays verify fresh authority without acquiring a mutation slot.
        let started = Instant::now();
        let node = self
            .rpc
            .leader(plan.group_id, deadline)
            .await
            .map_err(|error| phase_failure("discover completed data leader", error))?;
        let Reply::Stable(observation) = self
            .rpc
            .call(node, Query::ObserveStable { plan: plan.clone() }, deadline)
            .await
            .map_err(|error| phase_failure("verify completed stable membership", error))?
        else {
            return Err(Error::Authority);
        };
        observation.validate_for(plan, node)?;
        let (latest, record) = self.group(plan.group_id, deadline).await?;
        if !completed_record(plan, &latest, record.as_ref())? {
            return Err(Error::Authority);
        }
        drop(StableMembership::from_remote(
            observation.evidence,
            started,
            deadline,
        )?);
        Ok(true)
    }
}

fn completed_record(
    plan: &MembershipPlan,
    group: &DataGroup,
    record: Option<&MigrationRecord>,
) -> Result<bool> {
    if let Some(record) = record
        && record.plan == *plan
        && record.is_complete()
    {
        if group.generation != plan.generation.checked_add(1).ok_or(Error::Configuration)?
            || group.voters != plan.target
        {
            return Err(Error::Authority);
        }
        return Ok(true);
    }
    if group.generation > plan.generation {
        return Err(Error::Rejected {
            reason: crate::metadata::Rejection::Superseded,
        });
    }
    if group.generation != plan.generation
        || group.voters != plan.previous
        || record.is_some_and(|record| !record.is_complete() && record.plan != *plan)
    {
        return Err(Error::Authority);
    }
    Ok(false)
}

#[derive(Debug, snafu::Snafu)]
#[snafu(display("Scaling RPC failed during {phase}"))]
struct PhaseFailure {
    phase: &'static str,
    source: Box<dyn std::error::Error + Send + Sync>,
}

fn phase_failure(phase: &'static str, error: Error) -> Error {
    match error {
        Error::Remote { source } => Error::Remote {
            source: Box::new(PhaseFailure { phase, source }),
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_phase_context_preserves_the_original_typed_source_and_unknown_policy() {
        let original = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
        let error = phase_failure(
            "verify completed stable membership",
            Error::Remote {
                source: Box::new(original),
            },
        );
        let Error::Remote { source } = error else {
            panic!("The remote error class must be preserved");
        };
        let phase = source.downcast_ref::<PhaseFailure>().unwrap();
        assert_eq!(phase.phase, "verify completed stable membership");
        assert_eq!(
            phase
                .source
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::ConnectionReset
        );
        assert!(matches!(
            phase_failure("prepare membership", Error::Unknown),
            Error::Unknown
        ));
    }
}
