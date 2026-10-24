use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidemq_protocol::Id;
use tidemq_protocol::common::{ErrorCode, Outcome, TrafficClass};
use tidemq_protocol::state::OffsetRange;
use tokio::sync::{Notify, watch};
use tokio::time::Instant;

use crate::broker::{DataCommand, DataResponse, WriterFence};
use crate::budget::MemoryBudget;
use crate::lifecycle::{Coordinator, ObjectPhase, RangeRequest};
use crate::metadata::PartitionKey;
use crate::ownership::Work;
use crate::producer::{MessageData, ProducerState, Rejection};

use super::service::Service;
use super::{Error, Result};

const ARCHIVE_MAX_BYTES: u64 = 8 * 1024 * 1024;
const ARCHIVE_MAX_MESSAGES: usize = 8192;
const ARCHIVE_PAGE_BYTES: u64 = 1024 * 1024;
const ARCHIVE_PAGE_MESSAGES: usize = 1024;

fn select_archive_range(
    producer: &ProducerState,
    start: u64,
    committed_end: u64,
    budget: &MemoryBudget,
    deadline: Instant,
) -> Result<OffsetRange> {
    let reservation = ARCHIVE_PAGE_MESSAGES
        .checked_mul(std::mem::size_of::<MessageData>())
        .and_then(|bytes| bytes.checked_add(ARCHIVE_PAGE_BYTES as usize))
        .and_then(|bytes| u32::try_from(bytes).ok())
        .ok_or_else(|| Error::invalid("Archive page reservation exceeds its finite bound"))?;
    let _permit = budget
        .try_reserve(TrafficClass::Bulk, reservation)
        .map_err(|source| Error::operation("reserve bounded archive selection page", source))?;
    let count = u64::try_from(ARCHIVE_MAX_MESSAGES)
        .map_err(|_| Error::invalid("Archive message bound does not fit an offset"))?;
    let end = start.saturating_add(count).min(committed_end);
    if start >= end || committed_end > producer.committed_end() {
        return Err(Error::invalid(
            "Archive candidate is not a committed nonempty range",
        ));
    }
    let mut next = start;
    let mut bytes = 0_u64;
    while next < end && bytes < ARCHIVE_MAX_BYTES {
        if Instant::now() >= deadline {
            return Err(Error::unavailable());
        }
        let messages = usize::try_from(end.saturating_sub(next))
            .map_err(|_| Error::invalid("Archive message count does not fit the platform"))?
            .min(ARCHIVE_PAGE_MESSAGES)
            .min(producer.limits().max_batch_messages);
        let max_bytes = ARCHIVE_MAX_BYTES
            .saturating_sub(bytes)
            .min(ARCHIVE_PAGE_BYTES)
            .min(producer.limits().max_batch_bytes);
        let page = match producer.archive_read(next, messages, max_bytes) {
            Ok(page) => page,
            // The remaining byte allowance may be smaller than the next complete message.
            Err(Rejection::ReadLimit) if next > start => break,
            Err(source) => {
                return Err(Error::operation(
                    "select bounded committed archive page",
                    source,
                ));
            }
        };
        if page.messages.is_empty() {
            return Err(Error::invalid(
                "Archive page did not advance its committed range",
            ));
        }
        for message in &page.messages {
            bytes = message
                .logical_bytes()
                .and_then(|length| bytes.checked_add(length))
                .ok_or_else(|| Error::invalid("Archive byte counter overflowed"))?;
        }
        next = page.next_offset;
        // Each page contains Bytes references only; release them before reading another page.
    }
    Ok(OffsetRange { start, end: next })
}

pub(super) async fn background_admitted(
    quota: &Mutex<crate::quota::Quota>,
    observed: &Notify,
    deadline: Instant,
) -> Result<bool> {
    loop {
        if Instant::now() >= deadline {
            return Err(Error::unavailable());
        }
        let changed = observed.notified();
        tokio::pin!(changed);
        // Register before checking freshness so a concurrent real observation cannot be lost.
        changed.as_mut().enable();
        let admission = quota.lock().map_err(|_| Error::unavailable())?.admit(
            crate::quota::Work::BackgroundSnapshot,
            std::time::Instant::now(),
        );
        match admission {
            Ok(()) => return Ok(true),
            Err(crate::quota::Error::Stale) => super::wire::within(deadline, changed).await?,
            Err(_) => return Ok(false),
        }
    }
}

#[derive(Default)]
struct Progress {
    objects: Option<Id>,
    compactions: Option<Id>,
    partitions: Option<PartitionKey>,
    publishers: Option<(PartitionKey, Id, u64)>,
    quotas: Option<PartitionKey>,
    inspected: bool,
    blocked: bool,
}

impl Service {
    pub(crate) async fn rebalance(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut group_after = None;
        let mut round = 0_usize;
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
            let groups = self.registry.placement_groups(group_after, round)?;
            group_after = groups.keys().next_back().copied();
            round = round.saturating_add(1);
            let deadline = Instant::now() + Duration::from_secs(30);
            let result = tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                result = self.scaling.service.tick(&mut placement, &groups, deadline) => result,
            };
            let successful = result.is_ok();
            let (last_tick, last_error) = match result {
                Ok(report) => (Some(report), None),
                Err(source) => {
                    let error = anyhow::Error::new(source);
                    (
                        None,
                        Some(format!("{error:#}").chars().take(4096).collect::<String>()),
                    )
                }
            };
            let diagnostic = serde_json::json!({"placement":placement.diagnose(),"last_tick":last_tick,"last_error":last_error,"successful":successful});
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
                    for coordinator in self.objects.values() {
                        coordinator.expire_read_cache().await;
                    }
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
                let _ = self.maintain_tenant_quotas(*group, &mut position.quotas, deadline).await;
                let _ = self.maintain_publishers(*group, position, deadline).await;
                if let Some(coordinator) = self.objects.get(group)
                    && self
                        .reconcile_objects(*group, coordinator, position, deadline)
                        .await
                        .is_err()
                {
                    position.blocked = true;
                }
                if let Err(error) = self.maintain_partitions(*group, position, deadline).await {
                    let error = anyhow::Error::new(error);
                    tracing::debug!(event_code = "partition_maintenance_wait", group_id = group,
                        error = %format_args!("{error:#}"), "Partition maintenance will resume from durable progress");
                }
            }
                } => {}
            }
        }
    }

    async fn maintain_publishers(
        &self,
        group: u64,
        progress: &mut Progress,
        deadline: Instant,
    ) -> Result<()> {
        let now_ms = super::data::unix_millis()?;
        let page = self
            .data
            .get(&group)
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.publisher_cleanup_page(progress.publishers, now_ms))
            .await
            .map_err(|source| Error::operation("page publisher recovery history", source))?;
        progress.publishers = page
            .last()
            .map(|(partition, permit)| (*partition, permit.producer, permit.registration));
        for (partition, mut permit) in page {
            if let Some(current) = self.publisher_recovery_permit(&permit, deadline).await? {
                permit = current;
            }
            if permit.live {
                continue;
            }
            let lease = self.data_lease(group, partition, deadline).await?;
            self.data_write(
                group,
                DataCommand::CleanupPublisher {
                    partition,
                    writer: WriterFence {
                        node_id: lease.grant().owner_node,
                        owner_epoch: lease.grant().epoch,
                    },
                    permit,
                    now_ms,
                },
                deadline,
            )
            .await?;
        }
        Ok(())
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
                if pending.contains(&id)
                    && coordinator.complete(id, deadline.into_std()).await.is_err()
                {
                    progress.blocked = true;
                    continue;
                }
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
                if diagnostic.authority_scopes.is_empty() {
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
            if let Err(source) = coordinator.reconcile(id, deadline.into_std()).await {
                progress.blocked = true;
                if coordinator.metrics().may_warn() {
                    let err = anyhow::Error::new(source);
                    tracing::warn!(event = "object_reconcile_blocked", object_id = %id,
                        error = %format_args!("{err:#}"), "Object lifecycle reconciliation failed");
                }
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
        let candidate_count = page.partitions.len();
        let mut checked_compaction = false;
        for (index, candidate) in page.partitions.into_iter().enumerate() {
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
            if !background_admitted(&self.quota, &self.quota_observed, deadline).await? {
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
            let offsets = super::wire::within(
                deadline,
                replica.state.try_with_application(|state| {
                    select_archive_range(
                        state
                            .producer(candidate.partition)
                            .ok_or_else(Error::unavailable)?,
                        candidate.published_end,
                        candidate.committed_end,
                        &self.budget,
                        deadline,
                    )
                }),
            )
            .await?
            .map_err(|source| Error::operation("read byte-bounded archive range", source))??;
            let end = offsets.end;
            let prepared = coordinator
                .prepare(
                    vec![RangeRequest {
                        partition: candidate.partition,
                        offsets,
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
            // Resume after this partition so a hot backlog cannot starve the rest of the page.
            progress.partitions =
                if index.saturating_add(1) < candidate_count || page.next.is_some() {
                    Some(candidate.partition)
                } else {
                    None
                };
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
                (
                    state.retention_status(partition, now_ms),
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::budget::MemoryLimits;
    use crate::producer::{AppendBatch, ProducerLimits, RegisterProducer};

    use super::*;

    fn observed_quota(age: Duration, memory: u64) -> Mutex<crate::quota::Quota> {
        let mut quota = crate::quota::Quota::new(Default::default(), Default::default()).unwrap();
        quota.observe(
            crate::quota::Usage {
                process_memory_bytes: memory,
                ..Default::default()
            },
            std::time::Instant::now().checked_sub(age).unwrap(),
        );
        Mutex::new(quota)
    }

    fn budget() -> MemoryBudget {
        MemoryBudget::new(MemoryLimits {
            total_bytes: 192 * 1024 * 1024,
            control_reserved_bytes: 32 * 1024 * 1024,
        })
        .unwrap()
    }

    fn producer(partition: u32, count: usize, message_bytes: usize) -> ProducerState {
        let mut state =
            ProducerState::new(Id::new_v7().unwrap(), partition, ProducerLimits::default())
                .unwrap();
        let session = state
            .register(
                1,
                RegisterProducer {
                    producer_id: Id::new_v7().unwrap(),
                    expected_registration_epoch: 1,
                },
            )
            .unwrap();
        let message = MessageData {
            context: Bytes::new(),
            key: Bytes::from_static(b"k"),
            body: Bytes::from(vec![b'x'; message_bytes - 1]),
        };
        let messages = vec![message; count];
        let batch_size = state
            .limits()
            .max_batch_messages
            .min(state.limits().max_batch_bytes as usize / message_bytes);
        let mut first_sequence = 0;
        for messages in messages.chunks(batch_size) {
            let content_digest = tidemq_protocol::producer::canonical_produce_digest_parts(
                state.topic_id(),
                partition,
                session.producer_id,
                session.producer_epoch,
                first_sequence,
                messages
                    .iter()
                    .map(|message| (message.key.as_ref(), message.body.as_ref())),
            )
            .unwrap();
            let result = state
                .append(
                    2,
                    AppendBatch {
                        topic_id: state.topic_id(),
                        partition_id: partition,
                        producer_id: session.producer_id,
                        producer_epoch: session.producer_epoch,
                        first_sequence,
                        messages: messages.to_vec(),
                        content_digest,
                    },
                )
                .unwrap();
            first_sequence += u64::from(result.message_count);
        }
        state
    }

    #[test]
    fn archive_tick_advances_small_messages_past_sixteen_with_a_finite_count_and_deadline() {
        let budget = budget();
        let available = budget.available_bytes();
        let state = producer(0, ARCHIVE_MAX_MESSAGES + 1, 8);
        let until = Instant::now() + Duration::from_secs(1);
        let range = select_archive_range(&state, 0, state.committed_end(), &budget, until).unwrap();
        assert_eq!(
            range,
            OffsetRange {
                start: 0,
                end: 8192
            }
        );
        assert_eq!(budget.available_bytes(), available);
        let remaining =
            select_archive_range(&state, range.end, state.committed_end(), &budget, until).unwrap();
        assert_eq!(
            remaining,
            OffsetRange {
                start: 8192,
                end: 8193
            }
        );
        let snapshot = select_archive_range(&state, 0, 33, &budget, until).unwrap();
        assert_eq!(snapshot.end, 33);
        assert!(
            select_archive_range(&state, 0, state.committed_end(), &budget, Instant::now())
                .is_err()
        );
        assert_eq!(budget.available_bytes(), available);
    }

    #[test]
    fn archive_tick_bounds_large_messages_and_shares_the_original_budget_across_partitions() {
        let budget = budget();
        let available = budget.available_bytes();
        let states = [producer(0, 33, 256 * 1024 - 7), producer(1, 33, 256 * 1024)];
        for state in &states {
            let range = select_archive_range(
                state,
                0,
                state.committed_end(),
                &budget,
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap();
            assert_eq!(range.end, 32);
            let bytes = (range.start..range.end)
                .map(|offset| state.message_bytes_at(offset).unwrap())
                .sum::<u64>();
            assert!(bytes <= ARCHIVE_MAX_BYTES);
            assert!(bytes + state.message_bytes_at(range.end).unwrap() > ARCHIVE_MAX_BYTES);
            assert_eq!(budget.available_bytes(), available);
        }
        let busy = budget
            .try_reserve(
                TrafficClass::Bulk,
                u32::try_from(budget.available_data_bytes()).unwrap(),
            )
            .unwrap();
        assert!(
            select_archive_range(
                &states[0],
                0,
                states[0].committed_end(),
                &budget,
                Instant::now() + Duration::from_secs(1),
            )
            .is_err()
        );
        assert!(budget.available_core_bytes() > 0);
        drop(busy);
        assert_eq!(budget.available_bytes(), available);
    }

    #[tokio::test]
    async fn stale_background_admission_waits_for_a_real_observation_without_losing_notifications()
    {
        let quota = observed_quota(Duration::from_millis(1100), 64 * 1024 * 1024);
        let observed = Notify::new();
        let waiting =
            background_admitted(&quota, &observed, Instant::now() + Duration::from_secs(1));
        tokio::pin!(waiting);
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        observed.notify_waiters();
        // A notification alone is not evidence: the stale quota must still refuse admission.
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        quota.lock().unwrap().observe(
            crate::quota::Usage {
                process_memory_bytes: 64 * 1024 * 1024,
                ..Default::default()
            },
            std::time::Instant::now(),
        );
        observed.notify_waiters();
        assert!(waiting.await.unwrap());
    }

    #[tokio::test]
    async fn background_admission_keeps_the_original_deadline_and_pressure_rejection() {
        let observed = Notify::new();
        let quota = observed_quota(Duration::from_millis(1100), 64 * 1024 * 1024);
        assert!(
            background_admitted(
                &quota,
                &observed,
                Instant::now() + Duration::from_millis(20)
            )
            .await
            .is_err()
        );
        let pressured = observed_quota(
            Duration::ZERO,
            crate::quota::Limits::default().process_memory.high,
        );
        assert!(
            !background_admitted(
                &pressured,
                &observed,
                Instant::now() + Duration::from_secs(1)
            )
            .await
            .unwrap()
        );
        let unknown =
            Mutex::new(crate::quota::Quota::new(Default::default(), Default::default()).unwrap());
        assert!(
            !background_admitted(&unknown, &observed, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn shutdown_cancels_a_stale_background_admission_wait() {
        let quota = observed_quota(Duration::from_millis(1100), 64 * 1024 * 1024);
        let observed = Notify::new();
        let (shutdown, mut receiver) = watch::channel(false);
        let running = async {
            tokio::select! {
                _ = receiver.changed() => Ok(false),
                admitted = background_admitted(&quota, &observed, Instant::now() + Duration::from_secs(5)) => admitted,
            }
        };
        tokio::pin!(running);
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(running.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        shutdown.send(true).unwrap();
        assert!(
            !tokio::time::timeout(Duration::from_millis(100), running)
                .await
                .unwrap()
                .unwrap()
        );
    }
}
