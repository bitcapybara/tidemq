use tokio::time::Instant;

use super::{Error, Result, service::Service};
use crate::{
    metadata::{PartitionKey, Route},
    scaling::{DrainPlan, Handoff},
};

impl Service {
    pub(super) async fn select_initial_broker(
        &self,
        partition: PartitionKey,
        deadline: Instant,
    ) -> Result<u64> {
        use std::sync::atomic::Ordering;
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
        let mut candidates = replica
            .state
            .try_with_application(|state| super::node_retirement::drain_targets(state).1)
            .await
            .map_err(|source| {
                Error::operation("select registered initial broker ownership", source)
            })?;
        candidates.sort_unstable();
        let after = self.initial_broker_probe_after.load(Ordering::Relaxed);
        let start = candidates
            .iter()
            .position(|(_, node)| *node == after)
            .map_or(0, |index| (index + 1) % candidates.len().max(1));
        // Continue after failed probes so unavailable low-count nodes cannot hide later capacity.
        let mut responsive = 0_usize;
        let mut unreachable = 0_usize;
        for (_, node) in candidates
            .iter()
            .cycle()
            .skip(start)
            .take(candidates.len().min(32))
        {
            if Instant::now() >= deadline {
                break;
            }
            self.initial_broker_probe_after
                .store(*node, Ordering::Relaxed);
            let probe = self
                .probe_broker(
                    *node,
                    deadline.min(Instant::now() + std::time::Duration::from_millis(300)),
                )
                .await;
            match probe {
                super::failover::Probe::Eligible => {
                    self.initial_broker_probe_after.store(0, Ordering::Relaxed);
                    return Ok(*node);
                }
                super::failover::Probe::Responsive => responsive = responsive.saturating_add(1),
                super::failover::Probe::Unreachable => unreachable = unreachable.saturating_add(1),
            }
        }
        self.log_initial_broker_failure(
            partition,
            deadline,
            candidates.len(),
            responsive,
            unreachable,
        );
        Err(Error::unavailable())
    }

    fn log_initial_broker_failure(
        &self,
        partition: PartitionKey,
        deadline: Instant,
        candidates: usize,
        responsive: usize,
        unreachable: usize,
    ) {
        let quota = self
            .quota
            .lock()
            .ok()
            .map(|quota| quota.diagnose(std::time::Instant::now()));
        let container = quota.and_then(|quota| quota.container);
        tracing::warn!(
            event_code = "initial_route_broker_unavailable",
            topic_creation_operation_id = %partition.topic_id,
            topic_id = %partition.topic_id,
            birth_partition = partition.partition,
            stage = "select_initial_broker",
            probe_reason = "no_eligible_broker",
            candidates,
            responsive,
            unreachable,
            deadline_expired = Instant::now() >= deadline,
            caller_container_blocked = ?container.map(|sample| sample.blocked),
            caller_container_current_bytes = ?container.and_then(|sample| sample.current_bytes),
            caller_container_limit = ?container.map(|sample| sample.limit),
            caller_container_sample_age_ms = ?container.map(|sample| sample.sample_age_ms),
            caller_capacity_sample_age_ms = ?quota.and_then(|sample| sample.sample_age_ms),
            "Initial ownership could not select an eligible Broker"
        );
    }

    pub(super) async fn balance_broker_ownership(&self, deadline: Instant) -> Result<bool> {
        self.require_cluster()?;
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
        let (routes, candidates) = replica
            .state
            .try_with_application(balance_targets)
            .await
            .map_err(|source| Error::operation("select broker ownership balance", source))?;
        for route in routes {
            for (count, node) in candidates.iter().take(32) {
                let source = candidates
                    .iter()
                    .find(|(_, node)| *node == route.owner_node)
                    .map_or(0, |(count, _)| *count);
                if !improves(source, *count)
                    || self
                        .probe_broker(
                            *node,
                            deadline.min(Instant::now() + std::time::Duration::from_millis(300)),
                        )
                        .await
                        != super::failover::Probe::Eligible
                {
                    continue;
                }
                let Ok(_repair) = self.repair.try_register(*node, route.group_id) else {
                    continue;
                };
                DrainPlan::handoff(
                    self,
                    &Handoff {
                        partition: route.partition,
                        target_owner: *node,
                        expected_epoch: route.epoch,
                        expected_route_generation: route.generation,
                    },
                    &self.scaling.metrics,
                    deadline,
                )
                .await
                .map_err(|source| Error::operation("balance existing broker ownership", source))?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn improves(source: usize, target: usize) -> bool {
    source.saturating_sub(target) > 1
}

fn balance_targets(state: &crate::metadata::MetadataState) -> (Vec<Route>, Vec<(usize, u64)>) {
    let (_, mut candidates) = super::node_retirement::drain_targets(state);
    candidates.sort_unstable();
    let Some((maximum, _)) = candidates.last() else {
        return (Vec::new(), candidates);
    };
    if candidates
        .first()
        .is_none_or(|(minimum, _)| !improves(*maximum, *minimum))
    {
        return (Vec::new(), candidates);
    }
    let routes = state
        .partitions
        .keys()
        .filter_map(|partition| {
            let route = state.route(*partition)?;
            let count = candidates
                .iter()
                .find(|(_, node)| *node == route.owner_node)?
                .0;
            if count != *maximum
                || state.topology().transfer_blocks(*partition)
                || state
                    .topology()
                    .pending()
                    .any(|plan| plan.topic == partition.topic_id)
            {
                return None;
            }
            Some(route.clone())
        })
        .take(4)
        .collect();
    (routes, candidates)
}

#[cfg(test)]
mod tests {
    #[test]
    fn moving_one_owner_strictly_reduces_imbalance_and_cannot_oscillate() {
        for source in 0_usize..128 {
            for target in 0_usize..128 {
                if super::improves(source, target) {
                    assert!(
                        source.abs_diff(target) > source.saturating_sub(1).abs_diff(target + 1)
                    );
                    assert!(!super::improves(target + 1, source - 1));
                }
            }
        }
        assert!(!super::improves(1, 0));
        assert!(!super::improves(usize::MAX, usize::MAX));
    }
}
