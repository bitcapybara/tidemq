use std::future::Future;

use serde::Serialize;
use tokio::time::Instant;

use crate::metadata::{MetadataState, NodeRole, PartitionKey, Route};

use super::{Error, Result, ScalingMetrics, Stage};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Handoff {
    pub partition: PartitionKey,
    pub target_owner: u64,
    pub expected_epoch: u64,
    pub expected_route_generation: u64,
}

/// Runtime implementations authenticate and execute semantic operations, never raw commands.
pub trait DrainActions: Sync {
    fn prewarm(
        &self,
        handoff: &Handoff,
        deadline: Instant,
    ) -> impl Future<Output = Result<()>> + Send;
    fn drain_owner(
        &self,
        handoff: &Handoff,
        deadline: Instant,
    ) -> impl Future<Output = Result<()>> + Send;
    fn takeover(
        &self,
        handoff: &Handoff,
        deadline: Instant,
    ) -> impl Future<Output = Result<Route>> + Send;
    fn reconnect_gateway(
        &self,
        node_id: u64,
        deadline: Instant,
    ) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, serde::Deserialize)]
pub struct DrainProgress {
    pub node_id: u64,
    pub owned_partitions: usize,
    pub storage_groups: usize,
    pub pending_membership_changes: usize,
    pub metadata_role: bool,
}
impl DrainProgress {
    /// Gateway connections and local in-flight permits must additionally be drained by runtime.
    pub fn durable_state_drained(&self) -> bool {
        self.owned_partitions == 0
            && self.storage_groups == 0
            && self.pending_membership_changes == 0
            && !self.metadata_role
    }
}

pub struct DrainPlan;
impl DrainPlan {
    pub fn inspect(state: &MetadataState, node_id: u64) -> Result<DrainProgress> {
        let node = state.node(node_id).ok_or(Error::Configuration)?;
        if node.spec.schedulable {
            return Err(Error::Placement);
        }
        Ok(DrainProgress {
            node_id,
            owned_partitions: state
                .partitions
                .values()
                .filter(|p| p.grant.as_ref().is_some_and(|g| g.owner_node == node_id))
                .count(),
            storage_groups: state
                .groups
                .values()
                .filter(|g| g.voters.contains(&node_id))
                .count(),
            pending_membership_changes: state
                .migrations
                .values()
                .filter(|migration| !migration.is_complete())
                .map(|migration| &migration.plan)
                .chain(state.pending_data_replacements())
                .filter(|plan| {
                    plan.previous().contains(&node_id) || plan.target().contains(&node_id)
                })
                .map(|plan| plan.group_id())
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            metadata_role: node.spec.roles.contains(&NodeRole::Metadata),
        })
    }

    pub async fn handoff(
        actions: &impl DrainActions,
        handoff: &Handoff,
        metrics: &ScalingMetrics,
        deadline: Instant,
    ) -> Result<Route> {
        let started = Instant::now();
        actions.prewarm(handoff, deadline).await?;
        metrics.observe(Stage::Prewarm, started.elapsed());
        let drain = Instant::now();
        actions.drain_owner(handoff, deadline).await?;
        metrics.observe(Stage::Drain, drain.elapsed());
        let route = actions.takeover(handoff, deadline).await?;
        if route.partition != handoff.partition
            || route.owner_node != handoff.target_owner
            || route.epoch
                != handoff
                    .expected_epoch
                    .checked_add(1)
                    .ok_or(Error::Configuration)?
            || route.generation
                != handoff
                    .expected_route_generation
                    .checked_add(1)
                    .ok_or(Error::Configuration)?
        {
            return Err(Error::Authority);
        }
        metrics.observe(Stage::ServiceRecovery, started.elapsed());
        Ok(route)
    }
}
