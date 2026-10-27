mod ack;
mod assignment;
mod credit;
mod driver;
mod watch;
pub use credit::ConsumerMode;

#[cfg(test)]
#[path = "../../../tests/scalable_support/consumer_views.rs"]
mod view_tests;

struct JoinAttempt {
    operation: Id,
    request: public::ConsumerControl,
    heartbeat: Option<(Id, public::ConsumerControl)>,
}

use bytes::Bytes;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    public,
    topic::SubscriptionKind,
};
use tokio::{
    sync::{Notify, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{
    Client, Error, InitialPosition, ReceivedMessage, Result,
    admin::bytes,
    close::{CloseError, CloseReport, CloseResult},
    message::ReceivedData,
};
pub use ack::{AckProgress, AckReceipt, PendingAck};

/// Bounded local scheduling state; saturation is also reported by receive().
#[derive(Clone, Copy, Debug)]
pub struct ConsumerDiagnostics {
    pub mode: ConsumerMode,
    pub ordinary_reads: usize,
    pub protected_reads: usize,
    pub ordinary_unknown: usize,
    pub protected_unknown: bool,
    pub recovery_queue: usize,
    pub ordinary_rpc: usize,
    pub protected_rpc: usize,
    pub recovery_rpc: usize,
    pub long_watches: usize,
    pub immediate_probes: usize,
    pub ready_targets: usize,
    pub assigned_targets: usize,
    pub observed_targets: usize,
    pub fallback_overdue: usize,
    pub observation_overdue: usize,
    pub protected_blocked: bool,
    pub ordinary_slots_free: usize,
    pub ordinary_bytes_free: usize,
    pub protected_slots_free: usize,
    pub protected_bytes_free: usize,
}

struct Builder {
    client: Client,
    topic: String,
    name: String,
    model: SubscriptionKind,
    initial: InitialPosition,
    messages: u32,
    bytes: u32,
}

pub struct StreamConsumerBuilder(Builder);
pub struct QueueConsumerBuilder(Builder);

macro_rules! builder {
    ($builder:ident, $consumer:ident) => {
        impl $builder {
            pub fn initial_position(mut self, initial: InitialPosition) -> Self {
                self.0.initial = initial;
                self
            }
            pub fn prefetch_messages(mut self, messages: u32) -> Self {
                self.0.messages = messages;
                self
            }
            pub fn prefetch_bytes(mut self, bytes: u32) -> Self {
                self.0.bytes = bytes;
                self
            }
            pub async fn build(self) -> Result<$consumer> {
                Ok($consumer {
                    owner: self.0.build().await?,
                })
            }
        }
    };
}
builder!(StreamConsumerBuilder, StreamConsumer);
builder!(QueueConsumerBuilder, QueueConsumer);

#[derive(Clone)]
pub struct StreamConsumer {
    owner: Arc<Owner>,
}
#[derive(Clone)]
pub struct QueueConsumer {
    owner: Arc<Owner>,
}

struct Owner {
    core: Arc<Core>,
    _client: Client,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.core.stop.cancel();
    }
}

struct State {
    queue: VecDeque<ReceivedMessage>,
    ledger: BTreeMap<u64, ReceivedMessage>,
    revoked: Vec<(u64, Arc<ReceivedData>)>,
    next_delivery: u64,
    domains: BTreeMap<Id, driver::Domain>,
    read_after: Option<Id>,
    recovery_after: Option<Id>,
    watch_after: Option<Id>,
    watches: BTreeMap<Id, watch::Watch>,
    next_watch: Instant,
    saturated_since: Option<Instant>,
    last_saturation_error: Option<Instant>,
    pending: BTreeMap<Id, PendingAck>,
    claims: BTreeMap<u64, std::sync::Weak<ack::Work>>,
    ack_sequences: BTreeMap<(Id, u64), u64>,
    error: Option<Arc<Error>>,
    closed: Option<CloseResult>,
}

pub(crate) struct Core {
    client: Client,
    topic: String,
    topic_id: Id,
    subscription: Id,
    model: SubscriptionKind,
    member: Id,
    epoch: AtomicU64,
    max_messages: u32,
    max_bytes: u32,
    info: Mutex<public::ConsumerControlResult>,
    state: Mutex<State>,
    changed: Notify,
    credit: credit::Pools,
    stop: CancellationToken,
    close_deadline: Mutex<Option<Instant>>,
    lease_until: Mutex<Instant>,
    lease_fence_after: Mutex<Instant>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    quarantine: Mutex<Option<tokio::task::JoinHandle<()>>>,
    resource_memory: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
}

impl Client {
    pub fn new_stream_consumer(
        &self,
        topic: impl Into<String>,
        subscription: impl Into<String>,
    ) -> StreamConsumerBuilder {
        StreamConsumerBuilder(Builder::new(
            self.clone(),
            topic.into(),
            subscription.into(),
            SubscriptionKind::Stream,
        ))
    }
    pub fn new_queue_consumer(
        &self,
        topic: impl Into<String>,
        subscription: impl Into<String>,
    ) -> QueueConsumerBuilder {
        QueueConsumerBuilder(Builder::new(
            self.clone(),
            topic.into(),
            subscription.into(),
            SubscriptionKind::Queue,
        ))
    }
}

impl Builder {
    fn new(client: Client, topic: String, name: String, model: SubscriptionKind) -> Self {
        Self {
            client,
            topic,
            name,
            model,
            initial: InitialPosition::EarliestAvailable,
            messages: 256,
            bytes: 8 * 1024 * 1024,
        }
    }

    async fn build(self) -> Result<Arc<Owner>> {
        if self.messages == 0
            || self.messages > 256
            || self.bytes < 4096
            || self.bytes > 8 * 1024 * 1024
            || self.name.is_empty()
            || self.name.len() > 128
        {
            return Err(Error::Configuration {
                reason: "consumer name or prefetch budget is invalid",
            });
        }
        if u64::from(self.bytes)
            < minimum_prefetch_bytes(self.client.inner.config.protocol.max_message_bytes)
        {
            return Err(Error::Configuration {
                reason: "consumer prefetch_bytes must hold a maximum-sized message plus 512 bytes of delivery metadata",
            });
        }
        let client = self.client.clone();
        client
            .operation(|deadline| self.build_until(deadline))
            .await
    }

    async fn build_until(self, deadline: Instant) -> Result<Arc<Owner>> {
        let resource_memory = self.client.reserve_resource(deadline).await?;
        let credit = credit::Pools::new(&self.client, self.messages, self.bytes)?;
        if credit.mode == ConsumerMode::Isolated {
            credit.native.register(&self.client, deadline).await?;
        }
        if credit.mode == ConsumerMode::Serial {
            tracing::warn!(target: "tidemq_sdk::consumer", event_code = "consumer_serial_degradation",
                "Consumer credit or client request capacity permits only one original read; an unknown result blocks further reads");
        }
        watch::validate_minimum(&self.client)?;
        let info = self.client.admin().topic_info(&self.topic).await?;
        let mut request = public::ConsumerControl {
            topic_id: bytes(info.id),
            name: self.name,
            subscription_id: Bytes::new(),
            model: match self.model {
                SubscriptionKind::Stream => public::ConsumerModel::StreamConsumer,
                SubscriptionKind::Queue => public::ConsumerModel::QueueConsumer,
            }
            .into(),
            start_at_latest: self.initial == InitialPosition::Latest,
            action: public::ConsumerAction::EnsureConsumerSubscription.into(),
            member_id: Bytes::new(),
            member_epoch: 0,
            heartbeat_sequence: 0,
            max_messages: self.messages,
            max_bytes: u64::from(self.bytes),
        };
        let ensure_operation = Id::new_v7()?;
        let status = loop {
            let status = match control(&self.client, ensure_operation, &request, deadline).await {
                Ok(status) => status,
                Err(error)
                    if (error.outcome_unknown()
                        || error.retry_admission()
                        || error.retry_rejected())
                        && Instant::now() < deadline =>
                {
                    pause_control(deadline).await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if status.plan_active {
                break status;
            }
            pause_control(deadline).await?;
        };
        request.subscription_id = status.subscription_id.clone();
        request.member_id = bytes(Id::new_v7()?);
        request.member_epoch = status.next_member_epoch;
        request.action = public::ConsumerAction::JoinConsumer.into();
        let member = Id::from_slice(&request.member_id)?;
        let operation = Id::new_v7()?;
        let core = Arc::new(Core {
            client: self.client.background(),
            topic: self.topic,
            topic_id: info.id,
            subscription: Id::from_slice(&status.subscription_id)?,
            model: self.model,
            member,
            epoch: AtomicU64::new(request.member_epoch),
            max_messages: self.messages,
            max_bytes: self.bytes,
            info: Mutex::new(status),
            state: Mutex::new(State {
                queue: VecDeque::new(),
                ledger: BTreeMap::new(),
                revoked: Vec::new(),
                next_delivery: 1,
                domains: BTreeMap::new(),
                read_after: None,
                recovery_after: None,
                watch_after: None,
                watches: BTreeMap::new(),
                next_watch: Instant::now(),
                saturated_since: None,
                last_saturation_error: None,
                pending: BTreeMap::new(),
                claims: BTreeMap::new(),
                ack_sequences: BTreeMap::new(),
                error: None,
                closed: None,
            }),
            changed: Notify::new(),
            credit,
            stop: CancellationToken::new(),
            close_deadline: Mutex::new(None),
            lease_until: Mutex::new(Instant::now()),
            lease_fence_after: Mutex::new(deadline + Duration::from_secs(15)),
            task: Mutex::new(None),
            quarantine: Mutex::new(None),
            resource_memory: Mutex::new(Some(resource_memory)),
        });
        self.client.register_consumer(&core)?;
        let owner = Arc::new(Owner {
            core: core.clone(),
            _client: self.client,
        });
        let (ready, result) = tokio::sync::oneshot::channel();
        let worker = core.clone();
        let task = tokio::spawn(async move {
            let core = worker;
            let mut joining = JoinAttempt {
                operation,
                request,
                heartbeat: None,
            };
            let result = join_control(&core.client, &mut joining, deadline).await;
            core.epoch
                .store(joining.request.member_epoch, Ordering::Release);
            match result {
                Ok((status, started)) => {
                    core.epoch.store(status.member_epoch, Ordering::Release);
                    if let Err(error) = core.update_info(status, started) {
                        core.record_error(error);
                        core.stop.cancel();
                    }
                    let _ = ready.send(Ok(()));
                    driver::run(core).await;
                }
                Err(error) => {
                    let error = Arc::new(error);
                    let _ = ready.send(Err(error.clone()));
                    driver::finish(core, Some(error)).await;
                }
            }
        });
        *core.task.lock().map_err(|_| Error::Closed)? = Some(task);
        result
            .await
            .map_err(|_| Error::Closed)?
            .map_err(|source| Error::Background { source })?;
        Ok(owner)
    }
}

async fn control(
    client: &Client,
    operation: Id,
    request: &public::ConsumerControl,
    deadline: Instant,
) -> Result<public::ConsumerControlResult> {
    client
        .request(
            FrameKind::ConsumerControl,
            FrameKind::ConsumerControlResult,
            TrafficClass::Control,
            operation,
            request,
            deadline,
        )
        .await
}

async fn join_control(
    client: &Client,
    joining: &mut JoinAttempt,
    deadline: Instant,
) -> Result<(public::ConsumerControlResult, Instant)> {
    let JoinAttempt {
        operation,
        request,
        heartbeat: pending_heartbeat,
    } = joining;
    loop {
        let started = Instant::now();
        match control(client, *operation, request, deadline).await {
            Ok(result)
                if result.member_status == 2
                    && result.plan_active
                    && result.member_epoch > 0
                    && fresh_view(&result, started) =>
            {
                return Ok((result, started));
            }
            Ok(result) if matches!(result.member_status, 1 | 2) && result.member_epoch > 0 => {
                if pending_heartbeat.as_ref().is_some_and(|(_, request)| {
                    request.heartbeat_sequence < result.next_heartbeat_sequence
                }) {
                    *pending_heartbeat = None;
                }
                if pending_heartbeat.is_none() {
                    let mut heartbeat = request.clone();
                    heartbeat.action = public::ConsumerAction::HeartbeatConsumer.into();
                    heartbeat.heartbeat_sequence = result.next_heartbeat_sequence;
                    *pending_heartbeat = Some((Id::new_v7()?, heartbeat));
                }
                let (heartbeat_operation, heartbeat) =
                    pending_heartbeat.as_ref().ok_or(Error::SessionExpired)?;
                let started = Instant::now();
                let status = match control(client, *heartbeat_operation, heartbeat, deadline).await
                {
                    Ok(status) => status,
                    Err(error)
                        if (error.outcome_unknown()
                            || error.retry_admission()
                            || error.retry_rejected())
                            && Instant::now() < deadline =>
                    {
                        pause_control(deadline).await?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                *pending_heartbeat = None;
                if status.member_status == 2 && status.plan_active && fresh_view(&status, started) {
                    return Ok((status, started));
                }
                pause_control(deadline).await?;
            }
            Ok(result) if matches!(result.member_status, 5 | 7) && result.plan_active => {
                request.member_epoch = result.next_member_epoch;
                *operation = Id::new_v7()?;
                *pending_heartbeat = None;
                pause_control(deadline).await?;
            }
            Ok(_) => return Err(Error::SessionExpired),
            Err(Error::Remote {
                code: tidemq_protocol::common::ErrorCode::RegistrationConflict,
                outcome: tidemq_protocol::common::Outcome::Rejected,
                expected_registration_epoch: Some(expected),
                ..
            }) => {
                request.member_epoch = expected;
                *operation = Id::new_v7()?;
                *pending_heartbeat = None;
                pause_control(deadline).await?;
            }
            Err(error)
                if (error.outcome_unknown()
                    || error.retry_admission()
                    || error.retry_rejected())
                    && Instant::now() < deadline =>
            {
                pause_control(deadline).await?;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn pause_control(deadline: Instant) -> Result<()> {
    tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(10)))
        .await
        .map_err(|_| Error::Deadline)
}

fn fresh_view(status: &public::ConsumerControlResult, started: Instant) -> bool {
    status.remaining_lease_ms <= 15_000
        && started
            .checked_add(Duration::from_millis(status.remaining_lease_ms))
            .is_some_and(|until| until > Instant::now())
}

impl Core {
    pub(crate) fn retains_work(&self) -> bool {
        self.state().is_ok_and(|state| {
            state.closed.is_none()
                || !state.pending.is_empty()
                || self
                    .resource_memory
                    .lock()
                    .is_ok_and(|memory| memory.is_some())
        })
    }
    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state.lock().map_err(|_| Error::Closed)
    }
    fn request(&self, action: public::ConsumerAction) -> public::ConsumerControl {
        public::ConsumerControl {
            topic_id: bytes(self.topic_id),
            subscription_id: bytes(self.subscription),
            name: String::new(),
            model: match self.model {
                SubscriptionKind::Stream => public::ConsumerModel::StreamConsumer,
                SubscriptionKind::Queue => public::ConsumerModel::QueueConsumer,
            }
            .into(),
            start_at_latest: false,
            action: action.into(),
            member_id: bytes(self.member),
            member_epoch: self.epoch.load(Ordering::Acquire),
            heartbeat_sequence: 0,
            max_messages: self.max_messages,
            max_bytes: u64::from(self.max_bytes),
        }
    }

    fn try_receive(&self) -> Result<Option<ReceivedMessage>> {
        let mut state = self.state()?;
        if let Some(source) = state.error.take() {
            return Err(Error::Background { source });
        }
        if self.stop.is_cancelled() {
            return Err(Error::Closed);
        }
        if *self.lease_until.lock().map_err(|_| Error::Closed)? <= Instant::now() {
            return Err(Error::SessionExpired);
        }
        for _ in 0..state.queue.len() {
            let Some(mut message) = state.queue.pop_front() else {
                break;
            };
            if message.data.epoch != self.epoch.load(Ordering::Acquire) {
                continue;
            }
            if self.model == SubscriptionKind::Stream {
                let info = self.info.lock().map_err(|_| Error::Closed)?;
                if !info.lanes.iter().any(|lane| {
                    message.data.hash >= lane.hash_start
                        && lane.hash_end.is_none_or(|end| message.data.hash < end)
                }) {
                    state.queue.push_back(message);
                    continue;
                }
            }
            let sequence = state.next_delivery;
            state.next_delivery = sequence.checked_add(1).ok_or(Error::SessionExpired)?;
            message.delivery_sequence = sequence;
            state.ledger.insert(sequence, message.clone());
            self.client.inner.metrics.record_deliveries(1);
            return Ok(Some(message));
        }
        Ok(None)
    }

    async fn receive(&self) -> Result<ReceivedMessage> {
        loop {
            let notified = self.changed.notified();
            if let Some(message) = self.try_receive()? {
                return Ok(message);
            }
            notified.await;
        }
    }

    async fn close(&self) -> CloseResult {
        self.close_until(Instant::now() + self.client.inner.config.close_timeout)
            .await
    }

    pub(crate) async fn close_until(&self, deadline: Instant) -> CloseResult {
        if let Ok(mut original) = self.close_deadline.lock()
            && original.is_none()
        {
            *original = Some(deadline);
        }
        self.stop.cancel();
        if let Ok(mut state) = self.state() {
            state.error = None;
        }
        self.changed.notify_waiters();
        loop {
            let notified = self.changed.notified();
            let closed = match self.state() {
                Ok(state) => state.closed.clone(),
                Err(source) => {
                    return Err(CloseError {
                        report: CloseReport::default(),
                        source: Arc::new(source),
                    });
                }
            };
            if let Some(result) = closed {
                ack::stop_tasks(self).await;
                return self.join_closed(result, deadline).await;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(CloseError {
                    report: CloseReport::default(),
                    source: Arc::new(Error::Deadline),
                });
            }
        }
    }

    async fn join_closed(&self, mut result: CloseResult, deadline: Instant) -> CloseResult {
        if self.client.is_closing() {
            let task = self.quarantine.lock().ok().and_then(|mut task| task.take());
            if let Some(mut task) = task {
                match tokio::time::timeout_at(deadline, &mut task).await {
                    Ok(Ok(())) => match &mut result {
                        Ok(report) => report.local_cleanup_complete = true,
                        Err(error) => error.report.local_cleanup_complete = true,
                    },
                    status => {
                        let mut report = match result {
                            Ok(report) => report,
                            Err(error) => error.report,
                        };
                        report.local_cleanup_complete = false;
                        let source = match status {
                            Ok(Err(source)) => Error::Task { source },
                            _ => {
                                if let Ok(mut stored) = self.quarantine.lock() {
                                    *stored = Some(task);
                                }
                                Error::Deadline
                            }
                        };
                        return Err(CloseError {
                            report,
                            source: Arc::new(source),
                        });
                    }
                }
            }
        }
        let task = self.task.lock().ok().and_then(|mut task| task.take());
        let Some(mut task) = task else {
            return result;
        };
        match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(Ok(())) => result,
            status => {
                let mut report = match result {
                    Ok(report) => report,
                    Err(error) => error.report,
                };
                report.local_cleanup_complete = false;
                let source = match status {
                    Ok(Err(source)) => Error::Task { source },
                    _ => {
                        if let Ok(mut stored) = self.task.lock() {
                            *stored = Some(task);
                        }
                        Error::Deadline
                    }
                };
                Err(CloseError {
                    report,
                    source: Arc::new(source),
                })
            }
        }
    }

    fn record_error(&self, error: Error) {
        if self.stop.is_cancelled() {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            state.error = Some(Arc::new(error));
        }
        self.changed.notify_waiters();
    }
}

macro_rules! consumer {
    ($ty:ident) => {
        impl $ty {
            pub fn diagnostics(&self) -> Result<ConsumerDiagnostics> {
                driver::diagnostics(&self.owner.core)
            }
            pub async fn receive(&self) -> Result<ReceivedMessage> {
                self.owner.core.receive().await
            }
            pub fn try_receive(&self) -> Result<Option<ReceivedMessage>> {
                self.owner.core.try_receive()
            }
            pub async fn pending_acks(&self) -> Result<Vec<PendingAck>> {
                Ok(self.owner.core.state()?.pending.values().cloned().collect())
            }
            pub async fn close(&self) -> Result<CloseReport, CloseError> {
                self.owner.core.close().await
            }
        }
    };
}
consumer!(StreamConsumer);
consumer!(QueueConsumer);

impl StreamConsumer {
    pub async fn ack_cumulative(&self, message: &ReceivedMessage) -> Result<AckReceipt> {
        ack::start(&self.owner.core, message, None, true).await
    }
}
impl QueueConsumer {
    pub async fn ack(&self, message: &ReceivedMessage) -> Result<AckReceipt> {
        ack::start(&self.owner.core, message, None, false).await
    }
    pub async fn nack(&self, message: &ReceivedMessage, delay: Duration) -> Result<AckReceipt> {
        let delay = u64::try_from(delay.as_millis()).map_err(|_| Error::Configuration {
            reason: "redelivery delay is too large",
        })?;
        if delay > 300_000 {
            return Err(Error::Configuration {
                reason: "redelivery delay must be at most five minutes",
            });
        }
        ack::start(&self.owner.core, message, Some(delay), false).await
    }
}

fn minimum_prefetch_bytes(maximum_message: u32) -> u64 {
    u64::from(maximum_message) + 512
}
