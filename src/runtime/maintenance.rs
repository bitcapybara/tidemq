use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use tidemq_protocol::Id;
use tidemq_protocol::common::{ErrorCode, Outcome};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::broker::{DataCommand, DataResponse, WriterFence};
use crate::lifecycle::{Coordinator, ObjectPhase, RangeRequest};
use crate::metadata::PartitionKey;
use crate::ownership::Work;

use super::service::Service;
use super::{Error, Result};

#[derive(Default)]
struct Progress {
    objects: Option<Id>,
    compactions: Option<Id>,
    partitions: Option<PartitionKey>,
    inspected: bool,
    blocked: bool,
}

impl Service {
    pub(crate) async fn rebalance(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let groups = self
            .config
            .runtime
            .groups
            .iter()
            .map(|group| (group.group_id, group.standby.iter().copied().collect()))
            .collect::<BTreeMap<_, _>>();
        let mut placement =
            crate::scaling::Placement::new(crate::scaling::PlacementPolicy::default())
                .map_err(|source| Error::operation("initialize placement ticker", source))?;
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = shutdown.changed() => return Ok(()), _ = ticker.tick() => {} }
            let Some(replica) = &self.metadata else {
                continue;
            };
            if self.require_leader(replica).is_err() {
                continue;
            }
            let deadline = Instant::now() + Duration::from_secs(30);
            let result = tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                result = self.scaling.service.tick(&mut placement, &groups, deadline) => result,
            };
            let diagnostic = serde_json::json!({"placement":placement.diagnose(),"last_tick":result.as_ref().ok(),"successful":result.is_ok()});
            if let Ok(mut cached) = self.scaling_diagnostic.lock() {
                *cached = diagnostic;
            }
        }
    }

    pub(crate) fn sample_ownership_validity(&self) {
        let now = std::time::Instant::now();
        let remaining = self
            .leases
            .lock()
            .ok()
            .and_then(|leases| {
                leases
                    .values()
                    .map(|slot| {
                        slot.try_lock()
                            .ok()
                            .and_then(|lease| {
                                lease
                                    .as_ref()
                                    .map(|lease| lease.expires_at().saturating_duration_since(now))
                            })
                            .unwrap_or_default()
                    })
                    .min()
            })
            .unwrap_or_default();
        self.ownership_metrics.validity_remaining(remaining);
    }

    pub(crate) async fn expire_pins(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut tasks = tokio::task::JoinSet::new();
        let mut active = BTreeSet::new();
        let mut cursors = BTreeMap::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = shutdown.changed() => break, _ = ticker.tick() => {} }
            while let Some(Ok(key)) = tasks.try_join_next() {
                active.remove(&key);
            }
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = async {
                    for (group, coordinator) in &self.objects {
                        if active.len() >= 4 { break; }
                        let Some(replica) = self.data.get(group) else { continue; };
                        if self.require_leader(replica).is_err() { continue; }
                        let deadline = Instant::now() + Duration::from_millis(500);
                        let cursor = cursors.get(group).copied().flatten();
                        let Ok(page) = coordinator.pending_pins(cursor, 64, deadline.into_std()).await else { continue; };
                        cursors.insert(*group, page.next_after);
                        for pin in page.pins {
                            let key = (*group, pin);
                            if active.len() >= 4 { break; }
                            if !active.insert(key) { continue; }
                            let coordinator = coordinator.clone();
                            tasks.spawn(async move {
                                let _ = coordinator.expire_pin(pin, std::time::Instant::now() + Duration::from_secs(15)).await;
                                key
                            });
                        }
                    }
                } => {}
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    }

    pub(crate) fn admit_produce(&self) -> Result<()> {
        let available = self
            .disk_capacity
            .lock()
            .map_err(|_| Error::unavailable())?;
        let Some((disk, sampled)) = available.as_ref() else {
            return Err(Error::unavailable());
        };
        if sampled.elapsed() > Duration::from_millis(self.config.runtime.capacity.max_sample_age_ms)
        {
            return Err(Error::unavailable());
        }
        if disk.available_bytes < 4_194_304 {
            return Err(Error::reject(
                ErrorCode::QuotaExceeded,
                Outcome::Rejected,
                true,
            ));
        }
        drop(available);
        self.quota
            .lock()
            .map_err(|_| Error::unavailable())?
            .admit(crate::quota::Work::Produce, std::time::Instant::now())
            .map_err(|reason| match reason {
                crate::quota::Error::Pressure { .. } => {
                    Error::reject(ErrorCode::QuotaExceeded, Outcome::Rejected, true)
                }
                _ => Error::unavailable(),
            })
    }

    pub(crate) async fn maintain(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut progress = BTreeMap::<u64, Progress>::new();
        let mut ticker = tokio::time::interval(Duration::from_millis(
            self.config.runtime.archive_interval_ms,
        ));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = shutdown.changed() => return Ok(()), _ = ticker.tick() => {} }
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                _ = async {
            for (group, replica) in &self.data {
                if self.require_leader(replica).is_err() {
                    progress.remove(group);
                    continue;
                }
                let position = progress.entry(*group).or_default();
                let deadline = Instant::now() + Duration::from_secs(5);
                if let Some(coordinator) = self.objects.get(group)
                    && self
                        .reconcile_objects(*group, coordinator, position, deadline)
                        .await
                        .is_err()
                {
                    position.blocked = true;
                }
                let _ = self.maintain_partitions(*group, position, deadline).await;
            }
                } => {}
            }
        }
    }

    async fn reconcile_objects(
        &self,
        group: u64,
        coordinator: &Coordinator,
        progress: &mut Progress,
        deadline: Instant,
    ) -> Result<()> {
        let page = coordinator
            .pending_objects(progress.objects, 64, deadline.into_std())
            .await
            .map_err(|source| Error::operation("discover original archive tasks", source))?;
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        let pending = replica
            .state
            .try_with_application(|state| state.lifecycle().pending_objects(progress.objects, 64))
            .await
            .map_err(|source| Error::operation("read local archive task index", source))?
            .map_err(|_| Error::unavailable())?
            .into_iter()
            .collect::<BTreeSet<_>>();
        for (id, phase) in page.objects {
            if phase == ObjectPhase::Published {
                let diagnostic = coordinator
                    .diagnose(id, deadline.into_std())
                    .await
                    .map_err(|source| Error::operation("read archive checkpoint status", source))?;
                if diagnostic.manifest_references == 0 {
                    match coordinator.collect(id, deadline.into_std()).await {
                        Ok(())
                        | Err(crate::lifecycle::Error::Rejected {
                            reason:
                                crate::lifecycle::Rejection::Pinned
                                | crate::lifecycle::Rejection::RecoveryDependency,
                        }) => {}
                        Err(_) => progress.blocked = true,
                    }
                    continue;
                }
                if pending.contains(&id)
                    && coordinator.complete(id, deadline.into_std()).await.is_err()
                {
                    progress.blocked = true;
                    continue;
                }
                if diagnostic.checkpoint_generation.is_none()
                    && coordinator
                        .checkpoint(id, deadline.into_std())
                        .await
                        .is_err()
                {
                    progress.blocked = true;
                }
                continue;
            }
            if coordinator
                .reconcile(id, deadline.into_std())
                .await
                .is_err()
            {
                progress.blocked = true;
            }
        }
        progress.objects = page.next_after;
        if progress.objects.is_none() {
            progress.inspected = !progress.blocked;
            progress.blocked = false;
        }
        Ok(())
    }

    async fn maintain_partitions(
        &self,
        group: u64,
        progress: &mut Progress,
        deadline: Instant,
    ) -> Result<()> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        let page = replica
            .state
            .try_with_application(|state| state.archive_candidates(progress.partitions, 16))
            .await
            .map_err(|source| Error::operation("read bounded archive candidates", source))?
            .map_err(super::data::data_rejection)?;
        progress.partitions = page.next;
        let mut checked_compaction = false;
        for candidate in page.partitions {
            self.advance_retention(group, candidate.partition, deadline)
                .await?;
            let Some(coordinator) = self.objects.get(&group) else {
                continue;
            };
            let reclaimed = replica
                .state
                .try_with_application(|state| {
                    state.lifecycle().local_reclaim_end(candidate.partition)
                })
                .await
                .map_err(|source| {
                    Error::operation("read local archive reclaim progress", source)
                })?;
            if candidate.published_end > reclaimed {
                coordinator
                    .reclaim(
                        candidate.partition,
                        candidate.published_end,
                        deadline.into_std(),
                    )
                    .await
                    .map_err(|source| {
                        Error::operation("resume checkpointed local reclaim", source)
                    })?;
            }
            let (floor, checkpointed) = replica
                .state
                .try_with_application(|state| {
                    (
                        state
                            .producer(candidate.partition)
                            .map_or(0, |producer| producer.retained_floor()),
                        state.lifecycle().checkpointed_floor(candidate.partition),
                    )
                })
                .await
                .map_err(|source| Error::operation("read retention recovery floor", source))?;
            if floor > checkpointed {
                coordinator
                    .retire_before(candidate.partition, deadline.into_std())
                    .await
                    .map_err(|source| {
                        Error::operation("checkpoint logical retention floor", source)
                    })?;
            }
            if !progress.inspected || progress.blocked {
                continue;
            }
            let allowed = self
                .quota
                .lock()
                .map_err(|_| Error::unavailable())?
                .admit(
                    crate::quota::Work::BackgroundSnapshot,
                    std::time::Instant::now(),
                )
                .is_ok();
            if !allowed {
                continue;
            }
            if !checked_compaction {
                checked_compaction = true;
                progress.inspected = false;
                let step = coordinator
                    .compact_next(progress.compactions, deadline.into_std())
                    .await
                    .map_err(|source| {
                        Error::operation("compact one bounded archive candidate", source)
                    })?;
                progress.compactions = step.next_after;
                progress.inspected = true;
                if step.replacement.is_some() {
                    break;
                }
            }
            if candidate.published_end >= candidate.committed_end {
                continue;
            }
            let end = candidate
                .committed_end
                .min(candidate.published_end.saturating_add(16));
            let prepared = coordinator
                .prepare(
                    vec![RangeRequest {
                        partition: candidate.partition,
                        offsets: tidemq_protocol::state::OffsetRange {
                            start: candidate.published_end,
                            end,
                        },
                    }],
                    Vec::new(),
                    deadline.into_std(),
                )
                .await
                .map_err(|source| Error::operation("prepare bounded background archive", source))?;
            let id = prepared.object_id();
            // Remember uncertainty before the first durable registration; restart discovers it by ID.
            progress.inspected = false;
            coordinator
                .register(&prepared, deadline.into_std())
                .await
                .map_err(|source| Error::operation("register background archive", source))?;
            coordinator
                .authorize(id, deadline.into_std())
                .await
                .map_err(|source| Error::operation("authorize background archive", source))?;
            coordinator
                .upload(&prepared, deadline.into_std())
                .await
                .map_err(|source| Error::operation("upload background archive", source))?;
            coordinator
                .publish(id, deadline.into_std())
                .await
                .map_err(|source| Error::operation("publish background archive", source))?;
            coordinator
                .checkpoint(id, deadline.into_std())
                .await
                .map_err(|source| Error::operation("checkpoint background archive", source))?;
            coordinator
                .reclaim(candidate.partition, end, deadline.into_std())
                .await
                .map_err(|source| Error::operation("reclaim published hot bodies", source))?;
            break;
        }
        Ok(())
    }

    async fn advance_retention(
        &self,
        group: u64,
        partition: PartitionKey,
        deadline: Instant,
    ) -> Result<()> {
        let lease = self.data_lease(group, partition, deadline).await?;
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        let now_ms = super::data::unix_millis()?;
        let (status, fence) = replica
            .state
            .try_with_application(|state| {
                let protected = state
                    .subscriptions(partition)
                    .and_then(|subscriptions| subscriptions.protected_before());
                (
                    state
                        .retention(partition)
                        .map(|retention| retention.status(now_ms, protected)),
                    state
                        .ownership()
                        .and_then(|ownership| ownership.fence(partition))
                        .cloned(),
                )
            })
            .await
            .map_err(|source| Error::operation("read same-lock retention status", source))?;
        let status = status.ok_or_else(Error::unavailable)?;
        if status.maximum_floor <= status.floor {
            return Ok(());
        }
        lease
            .check(
                &fence.ok_or_else(Error::unavailable)?,
                Work::ControlMutation,
            )
            .map_err(|_| Error::unavailable())?;
        let response = self
            .data_write(
                group,
                DataCommand::Retention {
                    partition,
                    writer: WriterFence {
                        node_id: lease.grant().owner_node,
                        owner_epoch: lease.grant().epoch,
                    },
                    command: crate::retention::Command {
                        expected_revision: status.revision,
                        before: status.maximum_floor,
                        now_ms,
                    },
                },
                deadline,
            )
            .await?;
        if !matches!(response, DataResponse::Retention(_)) {
            return Err(Error::invalid("Unexpected retention command outcome"));
        }
        Ok(())
    }
}
