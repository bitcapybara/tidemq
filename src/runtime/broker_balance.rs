use tokio::time::Instant;

use super::{Error, Result, service::Service};
use crate::{
    metadata::Route,
    scaling::{DrainPlan, Handoff},
};

impl Service {
    pub(super) async fn balance_broker_ownership(&self, deadline: Instant) -> Result<bool> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        super::wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
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
