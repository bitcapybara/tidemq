mod observe;
mod placement;
mod policy;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use serde::Serialize;
use tidemq_protocol::{Id, common::TrafficClass, topic::TopicLayout};
use tokio::{sync::watch, time::Instant};

use super::{Error, Result, service::Service, wire};
use crate::metadata::{BirthShard, MetadataState, RecordStatus};
use observe::{Evaluation, Sample, Window};
pub use policy::{TopicScalingPolicy, WorkCapacity};

const MAX_DOMAINS: usize = 256;
const OBSERVATION_BYTES: u32 = 1024 * 1024;

#[derive(Clone)]
struct Domain {
    shard: BirthShard,
    group: u64,
    generation: u64,
    layout: u64,
    active: bool,
    owner_epoch: u64,
}

struct Scan {
    domains: Vec<Domain>,
    layouts: Vec<TopicLayout>,
    groups: BTreeMap<u64, crate::metadata::DataGroup>,
    registry_revision: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
enum Status {
    Disabled,
    #[default]
    Collecting,
    MissingObservation,
    ObservationCapacity,
    Cooling,
    SplitReady,
    HotKeyOrNarrowRange,
    ConsumerCapacity,
    ArchiveBehind,
    StorageCapacityRequired,
    RepairCapacityRequired,
    LayoutLimit,
    Split,
    Merge,
}

#[derive(Clone, Copy, Default, Serialize)]
pub(super) struct Diagnostic {
    status: Status,
    topic: Option<Id>,
    shard: Option<Id>,
    observed_domains: usize,
    required_independent_groups: u8,
}

impl Diagnostic {
    pub(super) fn configured(enabled: bool) -> Self {
        Self {
            status: if enabled {
                Status::Collecting
            } else {
                Status::Disabled
            },
            ..Default::default()
        }
    }
}

impl Service {
    pub(crate) async fn scale_topics(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let Some(policy) = self.config.runtime.topic_scaling.clone() else {
            return Ok(());
        };
        if self.metadata.is_none() {
            return Ok(());
        }
        // Covers the fixed window book, snapshot, in-flight pages, and selection scratch together.
        let mut memory = None;
        let mut windows = BTreeMap::new();
        let mut round = 0;
        let mut ticker = tokio::time::interval(policy.interval());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = shutdown.changed() => return Ok(()), _ = ticker.tick() => {} }
            if memory.is_none() {
                memory = self
                    .budget
                    .try_reserve(TrafficClass::Control, OBSERVATION_BYTES)
                    .ok();
                if memory.is_none() {
                    if let Ok(mut stored) = self.topic_scaling_diagnostic.lock() {
                        *stored = Diagnostic {
                            status: Status::ObservationCapacity,
                            ..Default::default()
                        };
                    }
                    continue;
                }
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            let result = tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                result = self.topic_scaling_tick(&policy, &mut windows, round, deadline) => result,
            };
            round = (round + 1) % MAX_DOMAINS;
            let diagnostic = match result {
                Ok(diagnostic) => diagnostic,
                Err(_) => {
                    windows.clear();
                    Diagnostic {
                        status: Status::MissingObservation,
                        ..Default::default()
                    }
                }
            };
            if let Ok(mut stored) = self.topic_scaling_diagnostic.lock() {
                *stored = diagnostic;
            }
        }
    }

    async fn topic_scaling_tick(
        &self,
        policy: &TopicScalingPolicy,
        windows: &mut BTreeMap<Id, Window>,
        round: usize,
        deadline: Instant,
    ) -> Result<Diagnostic> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        let Some(mut scan) = replica
            .state
            .try_with_application(scan)
            .await
            .map_err(|source| Error::operation("read bounded topic scaling domains", source))??
        else {
            windows.clear();
            return Ok(Diagnostic {
                status: Status::ObservationCapacity,
                ..Default::default()
            });
        };
        if !scan.layouts.is_empty() {
            let shift = round % scan.layouts.len();
            scan.layouts.rotate_left(shift);
        }
        let samples = self.sample_domains(&scan.domains, deadline).await?;
        let ids: BTreeSet<_> = samples
            .iter()
            .map(|sample| sample.domain.shard.route.id)
            .collect();
        windows.retain(|id, _| ids.contains(id));
        let mut evaluations = BTreeMap::new();
        let mut diagnostic = Diagnostic {
            observed_domains: samples.len(),
            ..Default::default()
        };
        for sample in &samples {
            let id = sample.domain.shard.route.id;
            if !sample.valid() {
                windows.remove(&id);
                diagnostic.status = Status::MissingObservation;
                continue;
            }
            let now = Instant::now();
            let evaluation = if let Some(window) = windows.get_mut(&id) {
                window.observe(sample, policy, now, self.config.runtime.objects.is_some())
            } else {
                windows.insert(id, Window::new(sample, policy, now));
                None
            };
            if let Some(evaluation) = evaluation {
                evaluations.insert(id, evaluation);
            }
        }
        if let Some(decision) = self
            .decide_topic_layout(policy, &scan, &evaluations, deadline)
            .await?
        {
            if matches!(decision.status, Status::Split | Status::Merge) {
                windows.clear();
            }
            return Ok(decision);
        }
        for (id, evaluation) in evaluations {
            if evaluation.status != Status::Collecting {
                diagnostic.status = evaluation.status;
                diagnostic.shard = Some(id);
            }
        }
        Ok(diagnostic)
    }

    async fn sample_domains(&self, domains: &[Domain], deadline: Instant) -> Result<Vec<Sample>> {
        let mut groups = BTreeMap::<u64, Vec<&Domain>>::new();
        for domain in domains {
            groups.entry(domain.group).or_default().push(domain);
        }
        let started = Instant::now();
        let mut samples = Vec::new();
        for (group, domains) in groups {
            for page in domains.chunks(16) {
                let partitions: Vec<_> = page.iter().map(|domain| domain.shard.partition).collect();
                let reply = self.query_shard_loads(group, &partitions, deadline).await?;
                for (domain, report) in page.iter().zip(reply.reports) {
                    let report = report.ok_or_else(Error::unavailable)?;
                    samples.push(Sample {
                        domain: (*domain).clone(),
                        node: reply.node,
                        instance: reply.instance,
                        report,
                    });
                }
            }
        }
        if started.elapsed() > Duration::from_secs(5) {
            return Err(Error::unavailable());
        }
        Ok(samples)
    }
}

fn scan(state: &MetadataState) -> Result<Option<Scan>> {
    let mut domains = Vec::new();
    let mut layouts = Vec::new();
    for layout in state.topology.layouts.values() {
        if !state.topic_serviceable(layout.topic) {
            continue;
        }
        let writable = state
            .topic(layout.topic)
            .is_some_and(|topic| topic.status == RecordStatus::Active);
        for shard in state.topology.domains(layout.topic) {
            if domains.len() == MAX_DOMAINS {
                return Ok(None);
            }
            let group = state
                .topology
                .binding(shard.partition)
                .ok_or_else(Error::unavailable)?
                .authority_group;
            domains.push(Domain {
                shard: shard.clone(),
                group,
                generation: state
                    .group(group)
                    .ok_or_else(Error::unavailable)?
                    .generation,
                layout: layout.revision,
                active: writable
                    && layout
                        .shards
                        .iter()
                        .any(|active| active.id == shard.route.id),
                owner_epoch: state
                    .route(shard.partition)
                    .ok_or_else(Error::unavailable)?
                    .epoch,
            });
        }
        if writable
            && !state
                .topology
                .pending()
                .any(|plan| plan.topic == layout.topic)
            && !state
                .topology
                .pending_transfers()
                .any(|plan| plan.partition.topic_id == layout.topic)
        {
            layouts.push(layout.clone());
        }
    }
    Ok(Some(Scan {
        domains,
        layouts,
        groups: state.groups.clone(),
        registry_revision: state.registry_revision(),
    }))
}

#[cfg(test)]
#[path = "../../../tests/scalable_support/topic_policy.rs"]
mod tests;
