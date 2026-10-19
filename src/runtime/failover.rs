use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::Duration;

use prometheus_client::{metrics::counter::Counter, registry::Registry};
use tokio::time::Instant;

use crate::metadata::{NodeRole, OwnershipDiagnostic, PartitionKey, RecordStatus};
use crate::ownership::OwnershipPhase;

use super::service::Service;
use super::{Result, wire};

const PROBE_TIMEOUT: Duration = Duration::from_millis(250);
const MIN_OBSERVATION: Duration = Duration::from_secs(1);
const PROBE_FRESHNESS: Duration = Duration::from_secs(3);
const PARTITIONS_PER_ROUND: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Probe {
    Eligible,
    Responsive,
    Unreachable,
}

#[derive(Default)]
struct Evidence {
    first_failure: Option<Instant>,
    latest_failure: Option<Instant>,
    failures: u8,
    eligible_at: Option<Instant>,
    suspected: bool,
}

impl Evidence {
    fn observe(&mut self, probe: Probe, now: Instant) -> bool {
        if probe != Probe::Unreachable {
            self.first_failure = None;
            self.latest_failure = None;
            self.failures = 0;
            self.suspected = false;
            self.eligible_at = (probe == Probe::Eligible).then_some(now);
            return false;
        }
        self.eligible_at = None;
        self.latest_failure = Some(now);
        let first = *self.first_failure.get_or_insert(now);
        self.failures = self.failures.saturating_add(1);
        let newly_suspected = !self.suspected
            && self.failures >= 3
            && now.saturating_duration_since(first) >= MIN_OBSERVATION;
        self.suspected |= newly_suspected;
        newly_suspected
    }

    fn eligible(&self, now: Instant) -> bool {
        self.eligible_at
            .is_some_and(|observed| now.saturating_duration_since(observed) < PROBE_FRESHNESS)
    }

    fn confirmed(&self, now: Instant) -> bool {
        self.suspected
            && self
                .latest_failure
                .is_some_and(|observed| now.saturating_duration_since(observed) < PROBE_FRESHNESS)
    }
}

pub(super) struct State {
    peers: BTreeMap<u64, Evidence>,
    peer_after: Option<u64>,
    partition_after: Option<PartitionKey>,
    leader_term: Option<u64>,
}

impl State {
    pub(super) fn new(config: &crate::config::Config) -> Self {
        Self {
            peers: config
                .runtime
                .peers
                .iter()
                .filter(|peer| peer.role.serves_broker())
                .map(|peer| (peer.node_id, Evidence::default()))
                .collect(),
            peer_after: None,
            partition_after: None,
            leader_term: None,
        }
    }

    fn reset(&mut self) {
        for evidence in self.peers.values_mut() {
            *evidence = Evidence::default();
        }
        self.peer_after = None;
        self.partition_after = None;
        self.leader_term = None;
    }

    fn next_peers(&mut self) -> Vec<u64> {
        let peers: Vec<_> = self
            .peers
            .range((
                self.peer_after.map_or(Bound::Unbounded, Bound::Excluded),
                Bound::Unbounded,
            ))
            .take(4)
            .map(|(node, _)| *node)
            .collect();
        self.peer_after = if peers.len() == 4 {
            peers.last().copied()
        } else {
            None
        };
        peers
    }
}

#[derive(Default)]
pub(super) struct Metrics {
    probes: Counter,
    unreachable: Counter,
    suspected: Counter,
    started: Counter,
    resumed: Counter,
    activated: Counter,
}

impl Metrics {
    pub(super) fn register(registry: &mut Registry) -> Self {
        let metrics = Self::default();
        let registry = registry.sub_registry_with_prefix("tidemq_failover");
        for (name, help, counter) in [
            (
                "probes",
                "Authenticated read-only Broker liveness probe attempts",
                &metrics.probes,
            ),
            (
                "unreachable",
                "Broker probes without native transport progress",
                &metrics.unreachable,
            ),
            (
                "suspected",
                "Broker observations crossing the count and duration thresholds",
                &metrics.suspected,
            ),
            (
                "takeover_attempts",
                "Bounded automatic ownership takeover attempts",
                &metrics.started,
            ),
            (
                "resume_attempts",
                "Attempts resuming an existing durable ownership grant",
                &metrics.resumed,
            ),
            (
                "activated",
                "Automatic ownership activations completed with fresh quorum evidence",
                &metrics.activated,
            ),
        ] {
            registry.register(name, help, counter.clone());
        }
        metrics
    }
}

impl Service {
    pub(super) async fn reconcile_owners(
        &self,
        state: &mut State,
        deadline: Instant,
    ) -> Result<()> {
        let Some(replica) = &self.metadata else {
            return Ok(());
        };
        if self.require_leader(replica).is_err() {
            state.reset();
            return Ok(());
        }
        let term = replica.raft.metrics().borrow().current_term;
        if state.leader_term != Some(term) {
            state.reset();
            state.leader_term = Some(term);
        }
        if !matches!(
            wire::within(deadline, replica.raft.ensure_linearizable()).await,
            Ok(Ok(_))
        ) {
            state.reset();
            return Ok(());
        }
        self.probe_owners(state, deadline).await;
        let page = replica
            .state
            .try_with_application(|metadata| {
                metadata
                    .partitions
                    .range((
                        state
                            .partition_after
                            .map_or(Bound::Unbounded, Bound::Excluded),
                        Bound::Unbounded,
                    ))
                    .take(PARTITIONS_PER_ROUND)
                    .filter_map(|(partition, _)| metadata.diagnose_ownership(*partition))
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(|source| {
                super::Error::operation("read bounded owner failover page", source)
            })?;
        state.partition_after = if page.len() == PARTITIONS_PER_ROUND {
            page.last().map(|record| record.partition)
        } else {
            None
        };
        for record in page {
            if self.failover_partition(state, record, deadline).await? {
                break;
            }
        }
        Ok(())
    }

    async fn probe_owners(&self, state: &mut State, deadline: Instant) {
        let peers = state.next_peers();
        let mut peers = peers.into_iter();
        let probe_deadline = deadline.min(Instant::now() + PROBE_TIMEOUT);
        let results = tokio::join!(
            self.probe_one(peers.next(), probe_deadline),
            self.probe_one(peers.next(), probe_deadline),
            self.probe_one(peers.next(), probe_deadline),
            self.probe_one(peers.next(), probe_deadline),
        );
        for (node, probe) in [results.0, results.1, results.2, results.3]
            .into_iter()
            .flatten()
        {
            self.failover_metrics.probes.inc();
            if probe == Probe::Unreachable {
                self.failover_metrics.unreachable.inc();
            }
            if let Some(evidence) = state.peers.get_mut(&node)
                && evidence.observe(probe, Instant::now())
            {
                self.failover_metrics.suspected.inc();
                tracing::warn!(
                    event_code = "broker_owner_unreachable",
                    node_id = node,
                    "Broker failed repeated authenticated transport probes over the observation window"
                );
            }
        }
    }

    async fn probe_one(&self, node: Option<u64>, deadline: Instant) -> Option<(u64, Probe)> {
        let node = node?;
        Some((node, self.probe_broker(node, deadline).await))
    }

    async fn failover_partition(
        &self,
        state: &State,
        record: OwnershipDiagnostic,
        deadline: Instant,
    ) -> Result<bool> {
        let Some(target) = self.failover_target(state, &record).await? else {
            return Ok(false);
        };
        let expected_epoch = if record.phase == OwnershipPhase::Active {
            self.failover_metrics.started.inc();
            record.epoch
        } else {
            self.failover_metrics.resumed.inc();
            record
                .epoch
                .checked_sub(1)
                .ok_or_else(|| super::Error::invalid("Ownership grant epoch is invalid"))?
        };
        let route = self
            .execute_takeover(
                record.partition,
                target,
                expected_epoch,
                record.route_generation,
                deadline,
            )
            .await?;
        self.failover_metrics.activated.inc();
        tracing::info!(
            event_code = "broker_owner_activated",
            node_id = target,
            epoch = route.epoch,
            "Broker ownership activated after durable fencing and recovery"
        );
        Ok(true)
    }

    async fn failover_target(
        &self,
        state: &State,
        record: &OwnershipDiagnostic,
    ) -> Result<Option<u64>> {
        let Some(owner) = record.owner_node else {
            return Ok(None);
        };
        let now = Instant::now();
        let moving = record.phase == OwnershipPhase::Active;
        if record.phase == OwnershipPhase::Unassigned
            || moving
                && !state
                    .peers
                    .get(&owner)
                    .is_some_and(|evidence| evidence.confirmed(now))
        {
            return Ok(None);
        }
        self.metadata
            .as_ref()
            .ok_or_else(super::Error::unavailable)?
            .state
            .try_with_application(|metadata| {
                if !metadata
                    .topic(record.partition.topic_id)
                    .is_some_and(|topic| topic.status == RecordStatus::Active)
                {
                    return None;
                }
                state.peers.iter().find_map(|(node, evidence)| {
                    let desired_owner = if moving {
                        *node != owner
                    } else {
                        *node == owner
                    };
                    (evidence.eligible(now)
                        && desired_owner
                        && metadata.node(*node).is_some_and(|node| {
                            node.spec.schedulable && node.spec.roles.contains(&NodeRole::Broker)
                        }))
                    .then_some(*node)
                })
            })
            .await
            .map_err(|source| super::Error::operation("validate Broker failover target", source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_requires_count_and_duration_and_responsive_errors_clear_evidence() {
        let now = Instant::now();
        let mut evidence = Evidence::default();
        assert!(!evidence.observe(Probe::Unreachable, now));
        assert!(!evidence.observe(Probe::Unreachable, now + Duration::from_millis(500)));
        assert!(!evidence.observe(Probe::Unreachable, now + Duration::from_millis(900)));
        assert!(evidence.observe(Probe::Unreachable, now + MIN_OBSERVATION));
        assert!(evidence.confirmed(now + MIN_OBSERVATION));
        assert!(!evidence.confirmed(now + MIN_OBSERVATION + PROBE_FRESHNESS));
        assert!(!evidence.observe(Probe::Unreachable, now + MIN_OBSERVATION));
        assert!(!evidence.observe(Probe::Responsive, now + MIN_OBSERVATION));
        assert!(!evidence.suspected);
        assert!(evidence.first_failure.is_none());
        assert!(!evidence.eligible(now + MIN_OBSERVATION));
        evidence.observe(Probe::Eligible, now + MIN_OBSERVATION);
        assert!(evidence.eligible(now + MIN_OBSERVATION));
        assert!(!evidence.eligible(now + MIN_OBSERVATION + PROBE_FRESHNESS));
    }

    #[test]
    fn probe_pages_rotate_only_over_configured_brokers_and_reset_on_authority_loss() {
        let mut state = State {
            peers: (31..=39).map(|node| (node, Evidence::default())).collect(),
            peer_after: None,
            partition_after: None,
            leader_term: Some(7),
        };
        assert_eq!(state.next_peers(), vec![31, 32, 33, 34]);
        assert_eq!(state.next_peers(), vec![35, 36, 37, 38]);
        assert_eq!(state.next_peers(), vec![39]);
        assert_eq!(state.next_peers(), vec![31, 32, 33, 34]);
        state
            .peers
            .get_mut(&31)
            .unwrap()
            .observe(Probe::Unreachable, Instant::now());
        state.reset();
        assert!(state.leader_term.is_none());
        assert!(state.peers.get(&31).unwrap().first_failure.is_none());
        assert_eq!(state.peers.len(), 9);
        assert_eq!(state.next_peers(), vec![31, 32, 33, 34]);
    }
}
