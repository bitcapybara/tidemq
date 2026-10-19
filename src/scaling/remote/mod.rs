//! Closed semantic scaling RPCs. The transport must authenticate and correlate every peer call.

mod data;
mod metadata;
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
    Capacity, Error, MembershipApplication, MembershipPlan, RepairBudget, RepairLease, Result,
    ScalingMetrics,
};
use crate::{
    metadata::{DeploymentMode, MetadataState},
    replication::{DiskLog, DiskStateMachine, Raft},
    transport::NodeGrant,
};

pub use wire::{Query, Reply, decode_query, decode_reply, encode_query, encode_reply};

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

    pub async fn handle(&self, peer: &NodeGrant, query: Query, deadline: Instant) -> Result<Reply> {
        self.authorize(peer, &query)?;
        let _slot = match &query {
            Query::Prepare { .. } | Query::Complete { .. } => Some(
                self.control_slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::Overloaded)?,
            ),
            Query::View { .. }
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
            Query::Capacity { group, node } => self.capacity_reply(group, node),
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

    fn capacity_reply(&self, group: u64, node: u64) -> Result<Reply> {
        if node != self.config.node_id || !self.replicas.data.contains_key(&group) {
            return Err(Error::Authority);
        }
        let report = self.capacity.snapshot().ok_or(Error::Placement)?;
        if report.node_id != node {
            return Err(Error::Authority);
        }
        wire::CapacityReport::from_sample(report).map(Reply::Capacity)
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
        let node = self.rpc.leader(self.metadata_group, deadline).await?;
        let Reply::Group {
            group: current,
            migration,
        } = self
            .rpc
            .call(node, Query::Group { group }, deadline)
            .await?
        else {
            return Err(Error::Authority);
        };
        if current.group_id != group {
            return Err(Error::Authority);
        }
        if let Some(record) = migration
            && !record.is_complete()
        {
            if record
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
                    .eq([added])
            {
                return Ok(record.plan);
            }
            return Err(Error::Authority);
        }
        let mut target = current.voters.clone();
        if !target.remove(&removed) || !target.insert(added) {
            return Err(Error::Configuration);
        }
        MembershipPlan::new(cluster, &current, target).map_err(|reason| Error::Rejected { reason })
    }

    pub async fn execute(&self, plan: &MembershipPlan, deadline: Instant) -> Result<()> {
        for query in [
            Query::Prepare {
                plan: plan.clone(),
                copy_bytes: 0,
            },
            Query::Change { plan: plan.clone() },
            Query::Complete { plan: plan.clone() },
        ] {
            let group = query.group_id(self.metadata_group);
            let node = self.rpc.leader(group, deadline).await?;
            if !matches!(self.rpc.call(node, query, deadline).await?, Reply::Done) {
                return Err(Error::Authority);
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
}
