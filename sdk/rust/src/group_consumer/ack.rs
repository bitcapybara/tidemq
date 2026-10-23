use super::*;
use std::sync::{Weak, atomic::AtomicBool};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckReceipt {
    Acknowledged,
    RedeliveryScheduled { due_ms: u64 },
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
    receipt: Option<AckReceipt>,
    retired: bool,
}

pub(super) struct Work {
    core: Weak<Core>,
    operation: Id,
    entries: Vec<u64>,
    nack: Option<u64>,
    dependencies: Mutex<Vec<PendingAck>>,
    batches: Mutex<Vec<Batch>>,
    result: Mutex<Option<Result<AckReceipt, Arc<Error>>>>,
    inflight: AtomicBool,
    changed: Notify,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl PendingAck {
    pub async fn retry(&self) -> Result<AckReceipt> {
        let core = self.work.core.upgrade().ok_or(Error::Closed)?;
        core.client
            .operation(|deadline| self.retry_until(deadline, true))
            .await
    }

    pub(super) fn is_complete(&self) -> bool {
        self.work
            .result
            .lock()
            .ok()
            .is_some_and(|result| result.as_ref().is_some_and(Result::is_ok))
    }

    fn retry_until(
        &self,
        deadline: Instant,
        claim: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<AckReceipt>> + Send + '_>> {
        Box::pin(async move {
            if !self.is_complete()
                && self
                    .work
                    .inflight
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                let work = self.work.clone();
                if let Ok(mut result) = work.result.lock() {
                    *result = None;
                }
                let handle = tokio::spawn(async move {
                    let result = drive(&work, deadline).await.map_err(Arc::new);
                    if let Ok(mut stored) = work.result.lock() {
                        *stored = Some(result);
                    }
                    work.inflight.store(false, Ordering::Release);
                    work.changed.notify_waiters();
                });
                *self.work.task.lock().map_err(|_| Error::Closed)? = Some(handle);
            }
            loop {
                let notified = self.work.changed.notified();
                let result = self.work.result.lock().map_err(|_| Error::Closed)?.clone();
                if let Some(result) = result {
                    if claim
                        && result.is_ok()
                        && let Some(core) = self.work.core.upgrade()
                    {
                        core.state()?.pending.remove(&self.work.operation);
                    }
                    return result.map_err(|source| Error::Background { source });
                }
                tokio::time::timeout_at(deadline, notified)
                    .await
                    .map_err(|_| Error::RequestUnknown {
                        source: Box::new(Error::Deadline),
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
            let pending = admit(core, message, nack, cumulative)?;
            pending.retry_until(deadline, true).await
        })
        .await
}

fn admit(
    core: &Arc<Core>,
    message: &ReceivedMessage,
    nack: Option<u64>,
    cumulative: bool,
) -> Result<PendingAck> {
    if message.data.consumer != core.member
        || message.data.member != core.member
        || message.data.epoch != core.epoch.load(Ordering::Acquire)
    {
        return Err(Error::StaleDelivery);
    }
    let mut state = core.state()?;
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
    let mut grouped: BTreeMap<(Id, u32, u64), Vec<public::GroupProof>> = BTreeMap::new();
    for (_, message) in &own {
        grouped
            .entry((
                message.data.id.shard,
                message.data.partition,
                message.data.epoch,
            ))
            .or_default()
            .push(message.data.proof.clone());
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
                    proofs: proofs.to_vec(),
                    nack_delay_ms: nack,
                },
                receipt: None,
                retired: false,
            });
        }
    }
    let work = Arc::new(Work {
        core: Arc::downgrade(core),
        operation,
        entries: own.iter().map(|(sequence, _)| *sequence).collect(),
        nack,
        dependencies: Mutex::new(dependencies.into_values().collect()),
        batches: Mutex::new(batches),
        result: Mutex::new(None),
        inflight: AtomicBool::new(false),
        changed: Notify::new(),
        task: Mutex::new(None),
    });
    for (sequence, _) in own {
        state.claims.insert(sequence, Arc::downgrade(&work));
    }
    let pending = PendingAck { work };
    state.pending.insert(operation, pending.clone());
    Ok(pending)
}

async fn drive(work: &Arc<Work>, deadline: Instant) -> Result<AckReceipt> {
    let core = work.core.upgrade().ok_or(Error::Closed)?;
    let dependencies = work.dependencies.lock().map_err(|_| Error::Closed)?.clone();
    for dependency in dependencies {
        dependency.retry_until(deadline, false).await?;
    }
    work.dependencies.lock().map_err(|_| Error::Closed)?.clear();
    let _guard = tokio::time::timeout_at(deadline, core.ack_gate.lock())
        .await
        .map_err(|_| Error::Deadline)?;
    let count = work.batches.lock().map_err(|_| Error::Closed)?.len();
    let mut receipt = AckReceipt::Acknowledged;
    for index in 0..count {
        let (operation, request, known, retired) = {
            let batches = work.batches.lock().map_err(|_| Error::Closed)?;
            let batch = batches.get(index).ok_or(Error::Closed)?;
            (
                batch.operation,
                batch.request.clone(),
                batch.receipt,
                batch.retired,
            )
        };
        if let Some(known) = known {
            receipt = known;
        } else {
            let result: public::GroupAckResult = core
                .client
                .request(
                    FrameKind::GroupAck,
                    FrameKind::GroupAckResult,
                    TrafficClass::Control,
                    operation,
                    &request,
                    deadline,
                )
                .await?;
            receipt = if result.redelivery_scheduled {
                AckReceipt::RedeliveryScheduled {
                    due_ms: result.due_ms,
                }
            } else {
                AckReceipt::Acknowledged
            };
            work.batches
                .lock()
                .map_err(|_| Error::Closed)?
                .get_mut(index)
                .ok_or(Error::Closed)?
                .receipt = Some(receipt);
        }
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
                .await?;
            work.batches
                .lock()
                .map_err(|_| Error::Closed)?
                .get_mut(index)
                .ok_or(Error::Closed)?
                .retired = true;
        }
    }
    let mut state = core.state()?;
    for sequence in &work.entries {
        state.ledger.remove(sequence);
        state.claims.remove(sequence);
    }
    drop(state);
    core.changed.notify_waiters();
    Ok(receipt)
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
