use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::Instrument;
use tracing::instrument::WithSubscriber;

use crate::observability::metrics::Stage;
use crate::wal::{self, RecordId, SegmentedDurability};

use super::local::{LocalHealth, Payload, QueueEntry, Queued, Shared};
use super::options::{RECORD_RESERVE, SEGMENT_HEADER};
use super::storage::{FORMAT, boundary, encode_record, lock};
use super::{Application, Applied, Error, ExecutionPosition, Result};

const MAINTENANCE_RETRY: Duration = Duration::from_millis(25);
const BATCH_COLLECT_YIELDS: usize = 8;
const MAX_IO_RETRIES: usize = 3;

struct InFlight<A: Application> {
    payload: Payload<A>,
    sequence: u64,
}

type Durability = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

struct PendingBatch<A: Application> {
    inflight: Vec<InFlight<A>>,
    durability: Durability,
    commit_started: Instant,
}

const PIPELINE_BATCHES: usize = 64;

pub(super) async fn run<A: Application>(
    shared: Arc<Shared<A>>,
    mut receiver: mpsc::Receiver<QueueEntry<A>>,
    mut retained: VecDeque<(u64, RecordId)>,
) -> Result<()> {
    let mut carry: Option<Arc<Queued<A>>> = None;
    let mut blocked = false;
    let mut closed = false;
    let mut next_sequence = shared.sequence.load(Ordering::Acquire);
    let mut pending = VecDeque::<PendingBatch<A>>::new();
    let mut writer = shared.storage.journal.health();
    loop {
        match &*writer.borrow_and_update() {
            wal::JournalHealth::Ready => {}
            wal::JournalHealth::Failed { reason } => {
                return checked(&shared, Err(Error::fatal(reason.clone())));
            }
            wal::JournalHealth::Stopped => {
                return checked(
                    &shared,
                    Err(Error::fatal("Local writer stopped unexpectedly")),
                );
            }
        }
        let ready = matches!(*lock(&shared.phase), LocalHealth::Ready);
        if !ready && pending.is_empty() {
            if let Some(queued) = carry.take() {
                queued.reject("Group stopped before handoff");
            }
            receiver.close();
            while let Some(entry) = receiver.recv().await {
                entry.queued.reject("Group stopped before handoff");
            }
            return Ok(());
        }
        let trigger = pending
            .is_empty()
            .then(|| maintenance_trigger(&shared, retained.len()))
            .flatten();
        if trigger.is_some()
            || (pending.is_empty() && shared.snapshot_requested.load(Ordering::Acquire))
        {
            lock(&shared.stats).maintenance_trigger = trigger.or(Some("requested"));
            blocked = !checked(&shared, maintain(&shared, &mut retained).await)?;
        }
        let pipeline_full = pending.len() >= PIPELINE_BATCHES;
        if let Some(front) = pending.front_mut()
            && (pipeline_full || !ready || carry.is_none() && closed)
        {
            let result = (&mut front.durability).await;
            let pending_batch = pending
                .pop_front()
                .ok_or_else(|| Error::fatal("Durability pipeline lost its front batch"))?;
            apply_durable(
                &shared,
                pending_batch.inflight,
                result,
                pending_batch.commit_started,
            )
            .await?;
            continue;
        }
        let pipeline_has_room = pending.len() < PIPELINE_BATCHES;
        if let Some(front) = pending.front_mut()
            && carry.is_none()
            && !closed
            && pipeline_has_room
        {
            tokio::select! {
                result = &mut front.durability => {
                    let pending_batch = pending
                        .pop_front()
                        .ok_or_else(|| Error::fatal("Durability pipeline lost its front batch"))?;
                    apply_durable(
                        &shared,
                        pending_batch.inflight,
                        result,
                        pending_batch.commit_started,
                    )
                    .await?;
                }
                entry = receiver.recv() => {
                    match entry {
                        Some(entry) => carry = Some(entry.queued),
                        None => closed = true,
                    }
                }
            }
            continue;
        }
        let first = match carry.take() {
            Some(queued) => queued,
            None if !closed => tokio::select! {
                biased;
                _ = shared.wake.notified() => continue,
                _ = shared.storage.notify.notified() => continue,
                _ = writer.changed() => continue,
                _ = tokio::time::sleep(MAINTENANCE_RETRY), if blocked && pending.is_empty() => continue,
                entry = receiver.recv() => match entry {
                    Some(entry) => entry.queued,
                    None => { closed = true; continue; }
                },
            },
            None => {
                if pending.is_empty() {
                    return Ok(());
                }
                continue;
            }
        };
        let (batch, next, bytes) = collect(&shared, first, &mut receiver).await;
        carry = next;
        if batch.is_empty() {
            continue;
        }
        let completion = batch.iter().all(|queued| queued.completion);
        if !can_append(&shared, bytes, batch.len(), completion) {
            blocked = !checked(&shared, maintain(&shared, &mut retained).await)?;
            if !can_append(&shared, bytes, batch.len(), completion) {
                for queued in batch {
                    queued.reject(
                        "Local capacity is protected; maintenance must advance before submission",
                    );
                }
                lock(&shared.stats).maintenance_blocked = Some("protected capacity");
                tokio::select! {
                    _ = tokio::time::sleep(MAINTENANCE_RETRY) => {},
                    _ = shared.storage.notify.notified() => {},
                    _ = shared.wake.notified() => {},
                    _ = writer.changed() => {},
                }
                continue;
            }
        }
        let Some((inflight, durability)) =
            append(&shared, batch, &mut retained, &mut next_sequence).await?
        else {
            continue;
        };
        pending.push_back(PendingBatch {
            inflight,
            durability: Box::pin(async move { durability.durable().await.map_err(Into::into) }),
            commit_started: Instant::now(),
        });
    }
}

type Batch<A> = (Vec<Arc<Queued<A>>>, Option<Arc<Queued<A>>>, u64);

async fn collect<A: Application>(
    shared: &Shared<A>,
    first: Arc<Queued<A>>,
    receiver: &mut mpsc::Receiver<QueueEntry<A>>,
) -> Batch<A> {
    let mut batch = Vec::new();
    let mut bytes = 0u64;
    let mut next = Some(first);
    let mut empty_yields = 0;
    let batch_limit = shared
        .storage
        .options
        .batch_bytes()
        .min(shared.storage.options.journal.max_record_bytes as u64);
    while let Some(queued) = next.take() {
        let size = lock(&queued.payload)
            .as_ref()
            .map_or(0, |p| p.bytes.len().saturating_add(RECORD_RESERVE)) as u64;
        if size != 0 {
            if !batch.is_empty() && bytes.saturating_add(size) > batch_limit {
                return (batch, Some(queued), bytes);
            }
            bytes = bytes.saturating_add(size);
            batch.push(queued);
        }
        if batch.len() >= shared.storage.options.batch_records() {
            break;
        }
        next = loop {
            match receiver.try_recv() {
                Ok(entry) => {
                    empty_yields = 0;
                    break Some(entry.queued);
                }
                Err(mpsc::error::TryRecvError::Empty)
                    if !batch.is_empty() && empty_yields < BATCH_COLLECT_YIELDS =>
                {
                    empty_yields += 1;
                    tokio::task::yield_now().await;
                }
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break None;
                }
            }
        };
    }
    (batch, next, bytes)
}

fn can_append<A: Application>(
    shared: &Shared<A>,
    bytes: u64,
    count: usize,
    completion: bool,
) -> bool {
    let stats = shared.storage.journal.statistics();
    let options = &shared.storage.options.journal;
    let rotate = stats.active_bytes.saturating_add(bytes) > options.segment_target_bytes;
    let staging = if rotate {
        SEGMENT_HEADER + 600 + (options.max_segments as u64) * 80
    } else {
        0
    };
    let completion_bytes = if completion {
        0
    } else {
        options.max_record_bytes as u64 + RECORD_RESERVE as u64
    };
    let completion_records = if completion {
        0
    } else {
        (options.max_retained_records / 4).max(1)
    };
    stats
        .current_file_bytes
        .saturating_add(bytes)
        .saturating_add(staging)
        <= options
            .max_file_bytes
            .saturating_sub(options.control_reserve_bytes)
            .saturating_sub(completion_bytes)
        && stats.retained_records.saturating_add(count)
            <= options
                .max_retained_records
                .saturating_sub(completion_records)
        // Ordinary appends leave one completion rotation in addition to two checkpoint slots.
        && (!rotate || stats.segment_count.saturating_add(3 + usize::from(!completion)) <= options.max_segments)
}

fn maintenance_trigger<A: Application>(shared: &Shared<A>, records: usize) -> Option<&'static str> {
    let stats = shared.storage.journal.statistics();
    let options = &shared.storage.options.journal;
    if stats.retired_bytes != 0 {
        return Some("retired files");
    }
    if records == 0 {
        return None;
    }
    if stats.current_file_bytes
        >= options
            .max_file_bytes
            .saturating_sub(options.control_reserve_bytes)
            / 2
    {
        return Some("bytes");
    }
    if records >= (options.max_retained_records / 2).max(1) {
        return Some("records");
    }
    if stats.segment_count >= options.max_segments.saturating_sub(3).max(1) {
        return Some("segments");
    }
    (!can_append(
        shared,
        shared.storage.options.batch_bytes(),
        shared.storage.options.batch_records(),
        false,
    ))
    .then_some("capacity reserve")
}

#[tracing::instrument(name = "local_submit_batch", level = "debug", skip_all, fields(group_id = shared.storage.identity.group.group_id, commands = batch.len()))]
async fn append<A: Application>(
    shared: &Shared<A>,
    batch: Vec<Arc<Queued<A>>>,
    retained: &mut VecDeque<(u64, RecordId)>,
    next_sequence: &mut u64,
) -> Result<Option<(Vec<InFlight<A>>, SegmentedDurability)>> {
    let mut records = Vec::new();
    let mut inflight = Vec::new();
    let mut sequence = *next_sequence;
    for queued in batch {
        checked(
            shared,
            boundary(&shared.storage.directory, "before_handoff"),
        )?;
        let Some(mut payload) = ({
            let phase = lock(&shared.phase);
            if *phase != LocalHealth::Ready || Instant::now() >= queued.deadline {
                queued.reject("Group stopped or deadline elapsed before handoff");
                None
            } else {
                queued.handoff()
            }
        }) else {
            continue;
        };
        lock(&shared.stats).handed_off += 1;
        if let Some(metrics) = &shared.storage.options.metrics {
            metrics.observe(Stage::Queue, payload.enqueued.elapsed().as_secs_f64());
        }
        sequence = checked(
            shared,
            sequence
                .checked_add(1)
                .ok_or_else(|| Error::fatal("Local command sequence overflow")),
        )?;
        let bytes = checked(
            shared,
            encode_record(
                FORMAT,
                shared.storage.hash,
                sequence,
                &payload.bytes,
                shared.storage.options.journal.max_record_bytes,
            )
            .map_err(Into::into),
        )?;
        checked(shared, boundary(&shared.storage.directory, "before_append"))?;
        records.push(bytes);
        payload.bytes = Vec::new();
        inflight.push(InFlight { payload, sequence });
    }
    *next_sequence = sequence;
    if records.is_empty() {
        return Ok(None);
    }
    let first = inflight
        .first()
        .ok_or_else(|| Error::fatal("Retained append batch has no in-flight records"))?;
    let (ids, durability) = checked(
        shared,
        shared
            .storage
            .journal
            .append_retained_batch(records)
            .instrument(first.payload.span.clone())
            .with_subscriber(first.payload.subscriber.clone())
            .await
            .map_err(Into::into),
    )?;
    if ids.len() != inflight.len() {
        return Err(Error::fatal(
            "Retained append returned mismatched record identities",
        ));
    }
    for (item, id) in inflight.iter().zip(ids) {
        retained.push_back((item.sequence, id));
    }
    Ok(Some((inflight, durability)))
}

async fn apply_durable<A: Application>(
    shared: &Shared<A>,
    inflight: Vec<InFlight<A>>,
    durable: Result<()>,
    commit_started: Instant,
) -> Result<()> {
    let mut durable = Some(durable);
    for item in inflight {
        let InFlight { payload, sequence } = item;
        checked(
            shared,
            boundary(&shared.storage.directory, "before_durable"),
        )?;
        let start = Instant::now();
        if let Some(result) = durable.take() {
            checked(shared, result)?;
            if let Some(metrics) = &shared.storage.options.metrics {
                metrics.observe(Stage::Commit, commit_started.elapsed().as_secs_f64());
            }
        } else if let Some(metrics) = &shared.storage.options.metrics {
            metrics.observe(Stage::Commit, start.elapsed().as_secs_f64());
        }
        lock(&shared.stats).durable_sequence = sequence;
        checked(shared, boundary(&shared.storage.directory, "after_durable"))?;
        let start = Instant::now();
        let (response, observations) = async {
            let mut view = shared.view.write().await;
            if view.sequence.checked_add(1) != Some(sequence) {
                return checked(shared, Err(Error::fatal("Local apply sequence differs")));
            }
            checked(shared, boundary(&shared.storage.directory, "before_apply"))?;
            let response = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::broker::apply_committed(&mut view.application, payload.command)
            })) {
                Ok(Ok(response)) => response,
                Ok(Err(source)) => {
                    view.failed = true;
                    return checked(shared, Err(Error::Application { source }));
                }
                Err(_) => {
                    view.failed = true;
                    return checked(
                        shared,
                        Err(Error::fatal("Application panicked during local apply")),
                    );
                }
            };
            view.sequence = sequence;
            shared.sequence.store(sequence, Ordering::Release);
            lock(&shared.stats).applied_sequence = sequence;
            Ok(response)
        }
        .instrument(payload.span)
        .with_subscriber(payload.subscriber)
        .await?;
        if let Some(metrics) = &shared.storage.options.metrics {
            metrics.observe(Stage::Apply, start.elapsed().as_secs_f64());
        }
        checked(shared, boundary(&shared.storage.directory, "after_apply"))?;
        checked(shared, shared.ensure_readable())?;
        if let Some(observations) = observations {
            observations.notify();
        }
        let _ = payload.reply.send(Ok(Applied {
            position: ExecutionPosition::Local { sequence },
            response,
        }));
        // Command and completion permits stay with this executor until apply and reply finish.
        drop(payload._permits);
    }
    Ok(())
}

fn checked<A: Application, T>(shared: &Shared<A>, result: Result<T>) -> Result<T> {
    result.map_err(|error| {
        let error = anyhow::Error::new(error);
        let reason = format!("{error:#}");
        shared.fail(reason.clone());
        Error::fatal(reason)
    })
}

#[tracing::instrument(name = "local_maintenance", level = "debug", skip_all, fields(group_id = shared.storage.identity.group.group_id))]
async fn maintain<A: Application>(
    shared: &Shared<A>,
    retained: &mut VecDeque<(u64, RecordId)>,
) -> Result<bool> {
    lock(&shared.stats).maintenance_in_progress = true;
    let mut attempts = 0;
    loop {
        match maintain_once(shared, retained).await {
            Ok(progress) => {
                let mut stats = lock(&shared.stats);
                stats.maintenance_in_progress = false;
                stats.maintenance_blocked = (!progress).then_some("snapshot readers");
                if progress && shared.snapshot_requested.swap(false, Ordering::AcqRel) {
                    shared
                        .snapshot_done
                        .send_modify(|generation| *generation = generation.saturating_add(1));
                }
                return Ok(progress);
            }
            Err(error) => {
                attempts += 1;
                lock(&shared.stats).maintenance_failures += 1;
                // Uncertain publication and corruption must never be retried or cleaned away.
                if !matches!(
                    error,
                    Error::Storage {
                        source: wal::Error::Io { .. }
                    }
                ) || attempts >= MAX_IO_RETRIES
                {
                    lock(&shared.stats).maintenance_in_progress = false;
                    return Err(error);
                }
                tracing::warn!(
                    event = "local_maintenance_retry",
                    attempt = attempts,
                    group_id = shared.storage.identity.group.group_id,
                    "Retrying local storage maintenance within its finite budget"
                );
                tokio::time::sleep(MAINTENANCE_RETRY).await;
            }
        }
    }
}

async fn maintain_once<A: Application>(
    shared: &Shared<A>,
    retained: &mut VecDeque<(u64, RecordId)>,
) -> Result<bool> {
    let sequence = shared.sequence.load(Ordering::Acquire);
    if sequence == 0 {
        return Ok(true);
    }
    let snapshot = lock(&shared.stats).snapshot_sequence;
    if snapshot < sequence {
        let start = Instant::now();
        let view = shared.view.read().await;
        shared.ensure_readable()?;
        if shared
            .storage
            .publish(&view.application, view.sequence)
            .await?
            .is_none()
        {
            return Ok(false);
        }
        drop(view);
        lock(&shared.stats).snapshot_sequence = sequence;
        if let Some(metrics) = &shared.storage.options.metrics {
            metrics.observe(Stage::WalCheckpoint, start.elapsed().as_secs_f64());
        }
    }
    shared.storage.checkpoint(sequence, retained).await?;
    retained.retain(|(seq, _)| *seq > sequence);
    let stats = shared.storage.journal.statistics();
    if stats.retired_bytes != 0 {
        return Err(wal::Error::Io {
            path: shared.storage.directory.clone(),
            source: std::io::Error::other("Retired WAL files remain charged after cleanup"),
        }
        .into());
    }
    let mut local = lock(&shared.stats);
    if snapshot < sequence {
        local.maintenance_runs = local.maintenance_runs.saturating_add(1);
    }
    tracing::debug!(
        event = "local_maintenance_completed",
        sequence,
        reclaimed_bytes = stats.reclaimed_bytes,
        remaining_bytes = stats.current_file_bytes,
        "Published local snapshot and advanced the reference checkpoint"
    );
    Ok(true)
}
