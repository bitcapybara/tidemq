use super::*;
use std::sync::{Weak, atomic::AtomicBool};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckReceipt {
    Acknowledged,
    RedeliveryScheduled { due_ms: u64 },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AckProgress {
    pub acknowledged: usize,
    pub redelivery_scheduled: usize,
    pub rejected: usize,
    pub pending: usize,
}

#[derive(Clone)]
pub struct PendingAck {
    pub(super) work: Arc<Work>,
}

impl std::fmt::Debug for PendingAck {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingAck")
            .field("operation", &self.work.operation)
            .finish_non_exhaustive()
    }
}

struct Batch {
    operation: Id,
    request: public::GroupAck,
    entries: Vec<u64>,
    outcome: Option<Result<AckReceipt, Arc<Error>>>,
    uncertain: bool,
    settled: bool,
    retired: bool,
}

pub(super) struct Work {
    _memory: tokio::sync::OwnedSemaphorePermit,
    _slot: tokio::sync::OwnedSemaphorePermit,
    core: Weak<Core>,
    operation: Id,
    nack: Option<u64>,
    dependencies: Mutex<Vec<PendingAck>>,
    batches: Mutex<Vec<Batch>>,
    // Original receipt cells remain reachable without retaining message bodies.
    deliveries: BTreeMap<u64, Weak<ReceivedData>>,
    result: Mutex<Option<Result<AckReceipt, Arc<Error>>>>,
    inflight: AtomicBool,
    complete: AtomicBool,
    changed: Notify,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl PendingAck {
    pub fn progress(&self) -> Result<AckProgress> {
        let batches = self.work.batches.lock().map_err(|_| Error::Closed)?;
        let mut progress = AckProgress::default();
        for batch in batches.iter() {
            let count = batch.entries.len();
            match &batch.outcome {
                Some(Ok(AckReceipt::Acknowledged)) => progress.acknowledged += count,
                Some(Ok(AckReceipt::RedeliveryScheduled { .. })) => {
                    progress.redelivery_scheduled += count
                }
                Some(Err(_)) => progress.rejected += count,
                None => progress.pending += count,
            }
        }
        Ok(progress)
    }

    pub async fn retry(&self) -> Result<AckReceipt> {
        let known = {
            let result = self.work.result.lock().map_err(|_| Error::Closed)?;
            self.is_complete().then(|| result.clone()).flatten()
        };
        if let Some(result) = known {
            if let Some(core) = self.work.core.upgrade() {
                core.state()?.pending.remove(&self.work.operation);
            }
            return self.expose(result, true);
        }
        let core = self.work.core.upgrade().ok_or(Error::Closed)?;
        if core.stop.is_cancelled() && core.state()?.closed.is_none() {
            return self.expose(
                Err(Arc::new(Error::RequestUnknown {
                    source: Box::new(Error::Closed),
                })),
                true,
            );
        }
        core.client
            .operation(|deadline| self.retry_until(deadline, true))
            .await
    }

    pub(super) fn is_complete(&self) -> bool {
        self.work.complete.load(Ordering::Acquire)
    }

    fn expose(&self, result: Result<AckReceipt, Arc<Error>>, claim: bool) -> Result<AckReceipt> {
        result.map_err(|source| {
            if claim {
                Error::Acknowledgement {
                    pending: self.clone(),
                    source,
                }
            } else {
                Error::Background { source }
            }
        })
    }

    // Erasure breaks the Send-checking cycle through spawned ACKs and their dependencies.
    fn retry_until(
        &self,
        deadline: Instant,
        claim: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<AckReceipt>> + Send + '_>> {
        Box::pin(async move {
            let launch = {
                let mut result = self.work.result.lock().map_err(|_| Error::Closed)?;
                if !self.is_complete()
                    && self
                        .work
                        .inflight
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    *result = None;
                    true
                } else {
                    false
                }
            };
            if launch {
                let mut owned_task = self.work.task.lock().map_err(|_| Error::Closed)?;
                let work = self.work.clone();
                let handle = tokio::spawn(async move {
                    let result = drive(&work, deadline).await.map_err(Arc::new);
                    let complete = work
                        .dependencies
                        .lock()
                        .is_ok_and(|dependencies| dependencies.is_empty())
                        && work.batches.lock().is_ok_and(|batches| {
                            batches
                                .iter()
                                .all(|batch| batch.outcome.is_some() && batch.retired)
                        });
                    if let Ok(mut stored) = work.result.lock() {
                        *stored = Some(result);
                        work.complete.store(complete, Ordering::Release);
                    }
                    work.inflight.store(false, Ordering::Release);
                    work.changed.notify_waiters();
                });
                *owned_task = Some(handle);
            }
            loop {
                let notified = self.work.changed.notified();
                let (result, complete) = {
                    let result = self.work.result.lock().map_err(|_| Error::Closed)?;
                    (result.clone(), self.is_complete())
                };
                if let Some(result) = result {
                    if claim
                        && complete
                        && let Some(core) = self.work.core.upgrade()
                    {
                        core.state()?.pending.remove(&self.work.operation);
                    }
                    return self.expose(result, claim);
                }
                tokio::time::timeout_at(deadline, notified)
                    .await
                    .map_err(|_| Error::Acknowledgement {
                        pending: self.clone(),
                        source: Arc::new(Error::RequestUnknown {
                            source: Box::new(Error::Deadline),
                        }),
                    })?;
            }
        })
    }
}

pub(super) async fn start(
    core: &Arc<Core>,
    message: &ReceivedMessage,
    nack: Option<u64>,
    cumulative: bool,
) -> Result<AckReceipt> {
    core.client
        .operation(|deadline| async move {
            if core.stop.is_cancelled() {
                return Err(Error::Closed);
            }
            if message.data.consumer != core.member
                || message.data.epoch != core.epoch.load(Ordering::Acquire)
            {
                return Err(Error::StaleDelivery);
            }
            if message.data.acknowledgement.get() == Some(&AckReceipt::Acknowledged)
                && nack.is_none()
            {
                return Ok(AckReceipt::Acknowledged);
            }
            let slot = tokio::time::timeout_at(
                deadline,
                core.client.inner.control_requests.clone().acquire_owned(),
            )
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|_| Error::Closed)?;
            let bytes = if cumulative {
                core.max_messages.saturating_mul(512).saturating_add(4096)
            } else {
                4480
            };
            if u64::from(bytes) > core.client.inner.config.max_pending_bytes / 8 {
                return Err(Error::Configuration {
                    reason: "control memory cannot hold this acknowledgement prefix",
                });
            }
            let memory = tokio::time::timeout_at(
                deadline,
                core.client
                    .inner
                    .control_memory
                    .clone()
                    .acquire_many_owned(bytes),
            )
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|_| Error::Closed)?;
            let pending = admit(core, message, nack, cumulative, memory, slot)?;
            pending.retry_until(deadline, true).await
        })
        .await
}

fn admit(
    core: &Arc<Core>,
    message: &ReceivedMessage,
    nack: Option<u64>,
    cumulative: bool,
    memory: tokio::sync::OwnedSemaphorePermit,
    slot: tokio::sync::OwnedSemaphorePermit,
) -> Result<PendingAck> {
    if message.data.consumer != core.member
        || message.data.member != core.member
        || message.data.epoch != core.epoch.load(Ordering::Acquire)
    {
        return Err(Error::StaleDelivery);
    }
    let mut state = core.state()?;
    if core.stop.is_cancelled() || core.client.is_closing() {
        return Err(Error::Closed);
    }
    if !state
        .ledger
        .get(&message.delivery_sequence)
        .is_some_and(|entry| Arc::ptr_eq(&entry.data, &message.data))
    {
        return Err(Error::StaleDelivery);
    }
    if state.pending.len() >= core.max_messages as usize {
        return Err(Error::Admission {
            resource: "pending acknowledgements",
        });
    }
    let entries: Vec<_> = if cumulative {
        state
            .ledger
            .range(..=message.delivery_sequence)
            .map(|(sequence, entry)| (*sequence, entry.clone()))
            .collect()
    } else {
        vec![(message.delivery_sequence, message.clone())]
    };
    let mut dependencies = BTreeMap::new();
    let mut own = Vec::new();
    for (sequence, message) in entries {
        if let Some(work) = state.claims.get(&sequence).and_then(Weak::upgrade) {
            if !cumulative && work.nack != nack {
                return Err(Error::AcknowledgementInProgress);
            }
            dependencies.insert(work.operation, PendingAck { work });
        } else {
            own.push((sequence, message));
        }
    }
    if own.is_empty() && dependencies.len() == 1 {
        return dependencies
            .into_values()
            .next()
            .ok_or(Error::StaleDelivery);
    }
    let operation = Id::new_v7()?;
    let mut grouped: BTreeMap<(Id, u32, u64), Vec<(u64, public::GroupProof)>> = BTreeMap::new();
    for (sequence, message) in &own {
        grouped
            .entry((
                message.data.id.shard,
                message.data.partition,
                message.data.epoch,
            ))
            .or_default()
            .push((*sequence, message.data.proof.clone()));
    }
    let mut batches = Vec::new();
    for ((shard, partition, epoch), proofs) in grouped {
        for proofs in proofs.chunks(64) {
            let sequence = state.ack_sequences.entry((shard, epoch)).or_insert(1);
            let current = *sequence;
            *sequence = current.checked_add(1).ok_or(Error::SessionExpired)?;
            batches.push(Batch {
                operation: Id::new_v7()?,
                request: public::GroupAck {
                    topic_id: bytes(core.topic_id),
                    partition_id: partition,
                    subscription_id: bytes(core.subscription),
                    member_id: bytes(core.member),
                    member_epoch: epoch,
                    sequence: current,
                    proofs: proofs.iter().map(|(_, proof)| proof.clone()).collect(),
                    nack_delay_ms: nack,
                },
                entries: proofs.iter().map(|(sequence, _)| *sequence).collect(),
                outcome: None,
                uncertain: false,
                settled: false,
                retired: false,
            });
        }
    }
    let work = Arc::new(Work {
        deliveries: own
            .iter()
            .map(|(sequence, message)| (*sequence, Arc::downgrade(&message.data)))
            .collect(),
        _memory: memory,
        _slot: slot,
        core: Arc::downgrade(core),
        operation,
        nack,
        dependencies: Mutex::new(dependencies.into_values().collect()),
        batches: Mutex::new(batches),
        result: Mutex::new(None),
        inflight: AtomicBool::new(false),
        complete: AtomicBool::new(false),
        changed: Notify::new(),
        task: Mutex::new(None),
    });
    for (sequence, _) in own {
        state.claims.insert(sequence, Arc::downgrade(&work));
    }
    let pending = PendingAck { work };
    state.pending.insert(operation, pending.clone());
    crate::request::mark_sent();
    Ok(pending)
}

async fn drive(work: &Arc<Work>, deadline: Instant) -> Result<AckReceipt> {
    let core = work.core.upgrade().ok_or(Error::Closed)?;
    let dependencies = work.dependencies.lock().map_err(|_| Error::Closed)?.clone();
    for dependency in dependencies {
        dependency.retry_until(deadline, false).await?;
    }
    work.dependencies.lock().map_err(|_| Error::Closed)?.clear();
    let count = work.batches.lock().map_err(|_| Error::Closed)?.len();
    let mut receipt = AckReceipt::Acknowledged;
    let mut rejected = None;
    for index in 0..count {
        let (operation, request, known, retired, uncertain) = {
            let batches = work.batches.lock().map_err(|_| Error::Closed)?;
            let batch = batches.get(index).ok_or(Error::Closed)?;
            (
                batch.operation,
                batch.request.clone(),
                batch.outcome.clone(),
                batch.retired,
                batch.uncertain,
            )
        };
        let outcome = match known {
            Some(outcome) => outcome,
            None => {
                let result: Result<public::GroupAckResult> = core
                    .client
                    .request(
                        FrameKind::GroupAck,
                        FrameKind::GroupAckResult,
                        TrafficClass::Control,
                        operation,
                        &request,
                        deadline,
                    )
                    .await;
                let result = result
                    .map(|result| {
                        if result.redelivery_scheduled {
                            AckReceipt::RedeliveryScheduled {
                                due_ms: result.due_ms,
                            }
                        } else {
                            AckReceipt::Acknowledged
                        }
                    })
                    .map_err(|error| error.preserve_uncertainty(uncertain));
                let mut batches = work.batches.lock().map_err(|_| Error::Closed)?;
                let batch = batches.get_mut(index).ok_or(Error::Closed)?;
                match result {
                    Err(error) if error.outcome_unknown() => {
                        batch.uncertain = true;
                        return Err(error);
                    }
                    result => {
                        let outcome = result.map_err(Arc::new);
                        batch.outcome = Some(outcome.clone());
                        outcome
                    }
                }
            }
        };
        match &outcome {
            Ok(known) => receipt = *known,
            Err(error) => {
                rejected.get_or_insert_with(|| error.clone());
            }
        }
        settle_batch(&core, work, index, &outcome)?;
        if !retired {
            let retire = public::GroupRetire {
                topic_id: request.topic_id,
                partition_id: request.partition_id,
                subscription_id: request.subscription_id,
                member_id: request.member_id,
                member_epoch: request.member_epoch,
                sequence: request.sequence,
            };
            let _: public::GroupRetired = core
                .client
                .request(
                    FrameKind::GroupRetireAck,
                    FrameKind::GroupRetired,
                    TrafficClass::Control,
                    operation,
                    &retire,
                    deadline,
                )
                .await
                .map_err(|error| error.preserve_uncertainty(true))?;
            work.batches
                .lock()
                .map_err(|_| Error::Closed)?
                .get_mut(index)
                .ok_or(Error::Closed)?
                .retired = true;
        }
    }
    match rejected {
        Some(source) => Err(Error::Background { source }),
        None => Ok(receipt),
    }
}

fn settle_batch(
    core: &Core,
    work: &Work,
    index: usize,
    outcome: &Result<AckReceipt, Arc<Error>>,
) -> Result<()> {
    let mut batches = work.batches.lock().map_err(|_| Error::Closed)?;
    let batch = batches.get_mut(index).ok_or(Error::Closed)?;
    if batch.settled {
        return Ok(());
    }
    let mut state = core.state()?;
    for sequence in &batch.entries {
        if let Ok(receipt) = outcome {
            state.ledger.remove(sequence);
            if let Some(data) = work.deliveries.get(sequence).and_then(Weak::upgrade) {
                let _ = data.acknowledgement.set(*receipt);
                data.release_credit()?;
            }
        }
        state.claims.remove(sequence);
    }
    batch.settled = true;
    core.changed.notify_waiters();
    Ok(())
}

pub(super) async fn drain(core: &Core, deadline: Instant) {
    let pending = core
        .state()
        .map(|state| state.pending.values().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    for pending in pending {
        let _ = pending.retry_until(deadline, false).await;
    }
}

pub(super) async fn stop_tasks(core: &Core) {
    let pending = core
        .state()
        .map(|state| state.pending.values().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    for pending in pending {
        let task = pending
            .work
            .task
            .lock()
            .ok()
            .and_then(|mut task| task.take());
        if let Some(task) = task {
            if !task.is_finished() {
                task.abort();
                if let Ok(mut result) = pending.work.result.lock() {
                    *result = Some(Err(Arc::new(Error::RequestUnknown {
                        source: Box::new(Error::Closed),
                    })));
                }
            }
            let _ = task.await;
        }
        pending.work.inflight.store(false, Ordering::Release);
        pending.work.changed.notify_waiters();
    }
}
