use super::*;
use std::sync::{Weak, atomic::AtomicBool};
use tidemq_protocol::topic::{SendIdentity, message_digest, routing_hash};
use tokio::sync::OwnedSemaphorePermit;

struct Credit {
    slots: OwnedSemaphorePermit,
    requests: OwnedSemaphorePermit,
    memory: OwnedSemaphorePermit,
    bytes: OwnedSemaphorePermit,
}

pub(super) struct Work {
    core: Weak<Core>,
    sequence: u64,
    key: Option<u64>,
    operation: Id,
    request: Mutex<public::Publish>,
    credit: Mutex<Credit>,
    dependency: Mutex<Option<PendingSend>>,
    uncertain: AtomicBool,
    complete: AtomicBool,
    inflight: AtomicBool,
    result: Mutex<Option<std::result::Result<SendReceipt, Arc<Error>>>>,
    changed: Notify,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    created: Instant,
}

#[derive(Clone)]
pub struct PendingSend {
    work: Arc<Work>,
}
impl std::fmt::Debug for PendingSend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingSend")
            .field("sequence", &self.work.sequence)
            .field("complete", &self.complete())
            .finish()
    }
}

impl PendingSend {
    pub(super) fn complete(&self) -> bool {
        self.work.complete.load(Ordering::Acquire)
    }
    pub async fn retry(&self) -> Result<SendReceipt> {
        if self.complete() {
            return self.take(true);
        }
        let core = self.work.core.upgrade().ok_or(Error::Closed)?;
        core.client
            .operation(|deadline| self.retry_until(deadline, true))
            .await
    }
    fn take(&self, claim: bool) -> Result<SendReceipt> {
        let (result, complete) = {
            let result = self.work.result.lock().map_err(|_| Error::Closed)?;
            (
                result.clone().ok_or(Error::ProducerBlocked)?,
                self.complete(),
            )
        };
        if claim
            && complete
            && let Some(core) = self.work.core.upgrade()
        {
            core.state()?.pending.remove(&self.work.sequence);
        }
        self.expose(result, claim)
    }
    fn expose(
        &self,
        result: std::result::Result<SendReceipt, Arc<Error>>,
        claim: bool,
    ) -> Result<SendReceipt> {
        result.map_err(|source| {
            if claim {
                Error::SendUnresolved {
                    pending: self.clone(),
                    source,
                }
            } else {
                Error::Background { source }
            }
        })
    }
    fn retry_until(
        &self,
        deadline: Instant,
        claim: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SendReceipt>> + Send + '_>> {
        Box::pin(async move {
            self.ensure_running(deadline)?;
            loop {
                let notified = self.work.changed.notified();
                if self
                    .work
                    .result
                    .lock()
                    .map_err(|_| Error::Closed)?
                    .is_some()
                {
                    return self.take(claim);
                }
                if tokio::time::timeout_at(deadline, notified).await.is_err() {
                    return self.expose(
                        Err(Arc::new(Error::RequestUnknown {
                            source: Box::new(Error::Deadline),
                        })),
                        claim,
                    );
                }
            }
        })
    }

    fn ensure_running(&self, deadline: Instant) -> Result<()> {
        let mut owned_task = self.work.task.lock().map_err(|_| Error::Closed)?;
        let launch = {
            let mut result = self.work.result.lock().map_err(|_| Error::Closed)?;
            if !self.complete()
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
            let work = self.work.clone();
            let task = tokio::spawn(async move {
                let (result, complete) = match drive(&work, deadline).await {
                    Ok(receipt) => (Ok(receipt), true),
                    Err((source, known)) => (Err(Arc::new(source)), known),
                };
                if complete {
                    release_payload(&work);
                }
                if let Ok(mut stored) = work.result.lock() {
                    *stored = Some(result);
                    work.complete.store(complete, Ordering::Release);
                }
                work.inflight.store(false, Ordering::Release);
                work.changed.notify_waiters();
            });
            *owned_task = Some(task);
        }
        Ok(())
    }
}

pub(super) async fn one(core: &Arc<Core>, message: Message) -> Result<SendReceipt> {
    let admitted = Mutex::new(None::<PendingSend>);
    let admitted_ref = &admitted;
    let result = core
        .client
        .operation(|deadline| async move {
            let size = message.memory_bytes(core.client.inner.config.protocol.max_message_bytes)?;
            let mut credit = reserve(core, 1, size, deadline).await?;
            let pending = admit(core, message, split_credit(&mut credit, size)?)?;
            *admitted_ref.lock().map_err(|_| Error::Closed)? = Some(pending.clone());
            pending.ensure_running(deadline)?;
            pending.retry_until(deadline, true).await
        })
        .await;
    match result {
        Err(source) if !matches!(source, Error::SendUnresolved { .. }) => {
            let pending = admitted.lock().ok().and_then(|pending| pending.clone());
            match pending {
                Some(pending) => Err(Error::SendUnresolved {
                    pending,
                    source: Arc::new(source),
                }),
                None => Err(source),
            }
        }
        result => result,
    }
}

pub(super) async fn batch(core: &Arc<Core>, messages: Vec<Message>) -> Result<SendBatchResult> {
    let admitted = Mutex::new(Vec::new());
    let admitted_ref = &admitted;
    let result = core
        .client
        .operation(|deadline| async move {
            if messages.is_empty() || messages.len() > core.max_sends as usize {
                return Err(Error::Configuration {
                    reason: "batch must fit the producer's pending window",
                });
            }
            let sizes = messages
                .iter()
                .map(|message| {
                    message.memory_bytes(core.client.inner.config.protocol.max_message_bytes)
                })
                .collect::<Result<Vec<_>>>()?;
            let total = sizes.iter().try_fold(0_u32, |sum, size| {
                sum.checked_add(*size).ok_or(Error::Admission {
                    resource: "batch bytes",
                })
            })?;
            let mut credit = reserve(core, messages.len() as u32, total, deadline).await?;
            let mut pending = Vec::new();
            for (message, size) in messages.into_iter().zip(sizes) {
                let send = admit(core, message, split_credit(&mut credit, size)?)?;
                admitted_ref
                    .lock()
                    .map_err(|_| Error::Closed)?
                    .push(send.clone());
                send.ensure_running(deadline)?;
                pending.push(send);
            }
            let mut outcomes = Vec::new();
            for send in pending {
                outcomes.push(send.retry_until(deadline, true).await);
            }
            Ok(SendBatchResult { outcomes })
        })
        .await;
    result.map_err(|source| {
        let pending = admitted
            .lock()
            .map(|pending| pending.clone())
            .unwrap_or_default();
        if pending.is_empty() {
            source
        } else {
            Error::BatchSendInterrupted {
                pending,
                source: Arc::new(source),
            }
        }
    })
}

async fn acquire(
    semaphore: &Arc<Semaphore>,
    count: u32,
    deadline: Instant,
) -> Result<OwnedSemaphorePermit> {
    tokio::time::timeout_at(deadline, semaphore.clone().acquire_many_owned(count))
        .await
        .map_err(|_| Error::Deadline)?
        .map_err(|_| Error::Closed)
}
async fn reserve(core: &Core, count: u32, size: u32, deadline: Instant) -> Result<Credit> {
    if size > core.max_bytes || u64::from(size) > core.client.inner.config.max_pending_bytes / 2 {
        return Err(Error::Configuration {
            reason: "message or batch cannot fit the shared producer budget",
        });
    }
    Ok(Credit {
        slots: acquire(&core.slots, count, deadline).await?,
        requests: acquire(&core.client.inner.requests, count, deadline).await?,
        memory: acquire(&core.memory, size, deadline).await?,
        bytes: acquire(&core.client.inner.bytes, size, deadline).await?,
    })
}
fn split_credit(credit: &mut Credit, size: u32) -> Result<Credit> {
    Ok(Credit {
        slots: credit.slots.split(1).ok_or(Error::Closed)?,
        requests: credit.requests.split(1).ok_or(Error::Closed)?,
        memory: credit.memory.split(size as usize).ok_or(Error::Closed)?,
        bytes: credit.bytes.split(size as usize).ok_or(Error::Closed)?,
    })
}

fn admit(core: &Arc<Core>, mut message: Message, credit: Credit) -> Result<PendingSend> {
    if core.stop.is_cancelled() {
        return Err(Error::Closed);
    }
    if *core.lease_until.lock().map_err(|_| Error::Closed)? <= Instant::now() {
        return Err(Error::SessionExpired);
    }
    let mut state = core.state()?;
    let sequence = state.next_sequence;
    let next = sequence.checked_add(1).ok_or(Error::SessionExpired)?;
    let identity = SendIdentity {
        topic: core.topic_id,
        producer: core.producer,
        registration: core.registration.load(Ordering::Acquire),
        sequence,
    };
    let key = message
        .wire
        .key
        .as_ref()
        .map(|key| routing_hash(Some(key), core.producer, sequence))
        .transpose()?;
    let hash = routing_hash(message.wire.key.as_deref(), core.producer, sequence)?;
    let digest = message_digest(
        identity,
        message.wire.key.as_deref(),
        &message.wire.payload,
        &message.wire.properties,
    )?;
    let target = state
        .layout
        .wire
        .active
        .iter()
        .find(|shard| shard.hash_start <= hash && shard.hash_end.is_none_or(|end| hash < end))
        .cloned()
        .ok_or(Error::ResponseMismatch {
            reason: "topic layout omitted the routing hash".into(),
        })?;
    let dependency = key.and_then(|key| {
        state
            .pending
            .values()
            .rev()
            .find(|pending| pending.work.key == Some(key) && !pending.complete())
            .cloned()
    });
    message.wire.key = message
        .wire
        .key
        .map(|key| crate::Bytes::copy_from_slice(&key));
    message.wire.payload = crate::Bytes::copy_from_slice(&message.wire.payload);
    message.wire.properties = message
        .wire
        .properties
        .into_iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                crate::Bytes::copy_from_slice(&value),
            )
        })
        .collect();
    let request = public::Publish {
        topic_id: bytes(core.topic_id),
        shard_id: target.shard_id.clone(),
        partition_id: target.birth_partition,
        producer_id: bytes(core.producer),
        registration: identity.registration,
        sequence,
        digest: crate::Bytes::copy_from_slice(&digest),
        message: Some(message.wire),
        retired_before: state
            .pending
            .first_key_value()
            .map_or(sequence, |(sequence, _)| *sequence),
    };
    let pending = PendingSend {
        work: Arc::new(Work {
            core: Arc::downgrade(core),
            sequence,
            key,
            operation: Id::new_v7()?,
            request: Mutex::new(request),
            credit: Mutex::new(credit),
            dependency: Mutex::new(dependency),
            uncertain: AtomicBool::new(false),
            complete: AtomicBool::new(false),
            inflight: AtomicBool::new(false),
            result: Mutex::new(None),
            changed: Notify::new(),
            task: Mutex::new(None),
            created: Instant::now(),
        }),
    };
    state.next_sequence = next;
    state
        .touched
        .insert(Id::from_slice(&target.shard_id)?, target);
    state.pending.insert(sequence, pending.clone());
    crate::request::mark_sent();
    Ok(pending)
}

fn release_payload(work: &Work) {
    if let Ok(mut request) = work.request.lock() {
        request.message = None;
    }
    if let Ok(mut credit) = work.credit.lock() {
        let memory = credit.memory.num_permits().saturating_sub(4096);
        drop(credit.memory.split(memory));
        let bytes = credit.bytes.num_permits().saturating_sub(4096);
        drop(credit.bytes.split(bytes));
    }
    if let Ok(mut dependency) = work.dependency.lock() {
        dependency.take();
    }
}

async fn drive(
    work: &Arc<Work>,
    deadline: Instant,
) -> std::result::Result<SendReceipt, (Error, bool)> {
    let core = work.core.upgrade().ok_or((Error::Closed, false))?;
    if work.created.elapsed() > Duration::from_secs(600) {
        core.stop.cancel();
        return Err((Error::SendRecoveryExpired, false));
    }
    let dependency = work
        .dependency
        .lock()
        .map_err(|_| (Error::Closed, false))?
        .clone();
    if let Some(dependency) = dependency {
        if let Err(error) = dependency.retry_until(deadline, false).await
            && error.outcome_unknown()
        {
            return Err((Error::ProducerBlocked, true));
        }
        work.dependency
            .lock()
            .map_err(|_| (Error::Closed, false))?
            .take();
    }
    drive_requests(&core, work, deadline).await
}

async fn drive_requests(
    core: &Core,
    work: &Work,
    deadline: Instant,
) -> std::result::Result<SendReceipt, (Error, bool)> {
    loop {
        if Instant::now() >= deadline {
            return Err((
                Error::Deadline.preserve_uncertainty(work.uncertain.load(Ordering::Acquire)),
                !work.uncertain.load(Ordering::Acquire),
            ));
        }
        let request = work
            .request
            .lock()
            .map_err(|_| (Error::Closed, false))?
            .clone();
        let result: Result<public::PublishResult> = core
            .client
            .request_affinity(
                FrameKind::Publish,
                FrameKind::PublishResult,
                TrafficClass::Produce,
                work.operation,
                &request,
                deadline,
                crate::client::affinity(&(request.partition_id, request.sequence)),
            )
            .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let error = error.preserve_uncertainty(work.uncertain.load(Ordering::Acquire));
                let known = !error.outcome_unknown();
                work.uncertain.store(!known, Ordering::Release);
                return Err((error, known));
            }
        };
        if result.topic_id != request.topic_id
            || result.shard_id != request.shard_id
            || result.producer_id != request.producer_id
            || result.registration != request.registration
            || result.sequence != request.sequence
            || result.digest != request.digest
        {
            work.uncertain.store(true, Ordering::Release);
            return Err((
                Error::RequestUnknown {
                    source: Box::new(Error::ResponseMismatch {
                        reason: "publish result differs from its original identity".into(),
                    }),
                },
                false,
            ));
        }
        if let Some(offset) = result.offset {
            if result.permanently_closed || !result.permanent_seal.is_empty() || offset == u64::MAX
            {
                work.uncertain.store(true, Ordering::Release);
                return Err((
                    Error::RequestUnknown {
                        source: Box::new(Error::ResponseMismatch {
                            reason: "publish result has conflicting outcomes".into(),
                        }),
                    },
                    false,
                ));
            }
            let shard =
                Id::from_slice(&request.shard_id).map_err(|source| (source.into(), false))?;
            return Ok(SendReceipt {
                id: MessageId {
                    topic: core.topic_id,
                    shard,
                    offset,
                },
            });
        }
        if result.permanently_closed && result.permanent_seal.is_empty() {
            return Err((Error::SessionExpired, true));
        }
        Id::from_slice(&result.permanent_seal).map_err(|source| {
            work.uncertain.store(true, Ordering::Release);
            (
                Error::RequestUnknown {
                    source: Box::new(source.into()),
                },
                false,
            )
        })?;
        work.uncertain.store(false, Ordering::Release);
        if core.stop.is_cancelled() {
            return Err((Error::Closed, true));
        }
        reroute(core, work, &request, deadline)
            .await
            .map_err(|source| {
                (
                    Error::SendNotSubmitted {
                        source: Arc::new(source),
                    },
                    true,
                )
            })?;
    }
}

async fn reroute(
    core: &Core,
    work: &Work,
    original: &public::Publish,
    deadline: Instant,
) -> Result<()> {
    loop {
        let context = crate::RequestContext::with_deadline(deadline);
        let layout = context
            .scope(core.client.admin().topic_info(&core.topic))
            .await?;
        let message = original.message.as_ref().ok_or(Error::Closed)?;
        let hash = routing_hash(message.key.as_deref(), core.producer, original.sequence)?;
        let target = layout
            .wire
            .active
            .iter()
            .find(|shard| shard.hash_start <= hash && shard.hash_end.is_none_or(|end| hash < end))
            .cloned()
            .ok_or(Error::Closed)?;
        if target.shard_id != original.shard_id {
            let mut state = core.state()?;
            state.layout = layout;
            state
                .touched
                .insert(Id::from_slice(&target.shard_id)?, target.clone());
            drop(state);
            let mut request = work.request.lock().map_err(|_| Error::Closed)?;
            request.shard_id = target.shard_id;
            request.partition_id = target.birth_partition;
            return Ok(());
        }
        tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(10)))
            .await
            .map_err(|_| Error::Deadline)?;
    }
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
