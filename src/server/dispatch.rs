use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
#[cfg(test)]
use prost::Message;
use tidemq_protocol::common::{ErrorCode, ErrorReply, Frame, FrameKind, Outcome, TrafficClass};
use tidemq_protocol::framing::{decode_payload, frame_limit, new_frame};
use tidemq_protocol::{Id, ProtocolLimits};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout_at};

use crate::budget::{MemoryBudget, MemoryPermit};
use crate::transport::{
    Authorization, FramedStream, ReceivedFrame, Server, Session, StreamPurpose,
};

use super::telemetry::{DispatchMetrics, HandlerGauge};
use super::tenant_admission::{TenantAdmission, TenantPermit};

pub trait WireService: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    fn handle(
        &self,
        peer: Authorization,
        frame: &Frame,
        deadline: Instant,
        order: Option<SubmissionOrder>,
    ) -> impl Future<Output = std::result::Result<Option<Frame>, Self::Error>> + Send;

    /// Transfer a compact, independently admitted request out of ordinary handler ownership.
    fn defer(
        self: &Arc<Self>,
        _peer: Authorization,
        _request: &Frame,
        _deadline: Instant,
    ) -> Option<DeferredReply<Self::Error>> {
        None
    }

    /// Nested business envelopes must not occupy the control handlers they depend on.
    fn execution_purpose(
        &self,
        _peer: &Authorization,
        _request: &Frame,
        transport_purpose: StreamPurpose,
    ) -> StreamPurpose {
        transport_purpose
    }

    /// The authority supplies the tenant identity; connections and credentials share its window.
    fn scheduling_key(&self, peer: &Authorization, _request: &Frame) -> Option<[u8; 16]> {
        match peer {
            Authorization::Credential { tenant, .. } => {
                Some(tenant.map_or([0; 16], |id| *id.as_bytes()))
            }
            _ => None,
        }
    }

    /// Opt in only for replies that must reach the peer before closing the endpoint.
    fn response_requires_flush(&self, _peer: &Authorization, _request: &Frame) -> bool {
        false
    }

    /// Called after a requested native flush; transport receipt does not establish business commit.
    fn response_sent(&self, _peer: &Authorization, _request: &Frame, _response: &Frame) {}
}

pub type DeferredReply<E> =
    Pin<Box<dyn Future<Output = std::result::Result<Option<Frame>, E>> + Send>>;

/// A producer lane retains order only until submission, then waits for its reply independently.
pub struct SubmissionOrder {
    _guard: OwnedMutexGuard<()>,
}

#[derive(Clone, Debug)]
pub struct DispatchConfig {
    pub raft_handlers: usize,
    pub core_handlers: usize,
    pub ack_handlers: usize,
    pub data_handlers: usize,
    pub replies_per_stream: usize,
    /// Assembly limit after the first frame byte; idle lanes remain owned until native close/drain.
    pub idle_timeout: Duration,
    pub drain_timeout: Duration,
    pub protocol: ProtocolLimits,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
            raft_handlers: 2,
            core_handlers: 8,
            ack_handlers: 32,
            data_handlers: 128,
            replies_per_stream: 8,
            idle_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(5),
            protocol: ProtocolLimits::default(),
        }
    }
}

struct Reply {
    frame: Frame,
    deadline: Instant,
    notification: Option<(Authorization, ReceivedFrame)>,
}
struct Job {
    peer: Authorization,
    frame: AdmittedFrame,
    reply: mpsc::Sender<Reply>,
    _slot: OwnedSemaphorePermit,
    memory: MemoryPermit,
    order: Option<SubmissionOrder>,
    tenant: Option<TenantPermit>,
}
struct ClassQueue {
    slots: Arc<Semaphore>,
    sender: mpsc::Sender<Job>,
}
struct Dispatcher {
    raft: Option<ClassQueue>,
    core: ClassQueue,
    ack: ClassQueue,
    data: ClassQueue,
    budget: MemoryBudget,
    config: DispatchConfig,
    health: Arc<DispatchMetrics>,
    tenants: Arc<TenantAdmission>,
}

enum AdmittedFrame {
    Remote(ReceivedFrame),
    #[cfg(test)]
    Local {
        frame: Frame,
        deadline: Instant,
    },
}

impl AdmittedFrame {
    fn frame(&self) -> &Frame {
        match self {
            Self::Remote(frame) => frame.frame(),
            #[cfg(test)]
            Self::Local { frame, .. } => frame,
        }
    }

    fn deadline(&self) -> Instant {
        match self {
            Self::Remote(frame) => frame.deadline(),
            #[cfg(test)]
            Self::Local { deadline, .. } => *deadline,
        }
    }

    fn received(self) -> Option<ReceivedFrame> {
        match self {
            Self::Remote(frame) => Some(frame),
            #[cfg(test)]
            Self::Local { .. } => None,
        }
    }
}

impl ClassQueue {
    fn new(capacity: usize) -> (Self, mpsc::Receiver<Job>) {
        let (sender, receiver) = mpsc::channel(capacity);
        (
            Self {
                slots: Arc::new(Semaphore::new(capacity)),
                sender,
            },
            receiver,
        )
    }
}

impl Dispatcher {
    fn rejected_frame(
        &self,
        frame: AdmittedFrame,
        purpose: StreamPurpose,
        reason: &'static str,
    ) -> Box<AdmittedFrame> {
        if self.health.may_log_rejection(std::time::Instant::now()) {
            tracing::info!(
                target: "tidemq::server::admission",
                event_code = "dispatch_admission_rejected",
                reason,
                purpose = ?purpose,
                frame_kind = frame.frame().kind,
                traffic_class = frame.frame().traffic_class,
                request_id = ?Id::from_slice(&frame.frame().request_id).ok(),
                operation_id = ?Id::from_slice(&frame.frame().operation_id).ok(),
                core_slots_available = self.core.slots.available_permits(),
                ack_slots_available = self.ack.slots.available_permits(),
                data_slots_available = self.data.slots.available_permits(),
                raft_slots_available = ?self.raft.as_ref().map(|queue| queue.slots.available_permits()),
                "The dispatcher refused a frame before handler admission"
            );
        }
        Box::new(frame)
    }

    async fn submit(
        &self,
        peer: Authorization,
        purpose: StreamPurpose,
        frame: AdmittedFrame,
        reply: mpsc::Sender<Reply>,
        order: Option<SubmissionOrder>,
        tenant: Option<[u8; 16]>,
    ) -> std::result::Result<(), Box<AdmittedFrame>> {
        let raft_control = purpose == StreamPurpose::Core
            && matches!(&peer, Authorization::Internal(_))
            && matches!(
                FrameKind::try_from(frame.frame().kind),
                Ok(FrameKind::RaftVote | FrameKind::RaftAppend)
            );
        let queue = match purpose {
            StreamPurpose::Core if raft_control => match &self.raft {
                Some(queue) => queue,
                None => return Err(self.rejected_frame(frame, purpose, "raft_queue_unavailable")),
            },
            StreamPurpose::Core => &self.core,
            StreamPurpose::Ack => &self.ack,
            StreamPurpose::Request => &self.data,
        };
        let Ok(class) = TrafficClass::try_from(frame.frame().traffic_class) else {
            return Err(self.rejected_frame(frame, purpose, "invalid_traffic_class"));
        };
        let Some(bytes) = frame_limit(class, &self.config.protocol).checked_mul(2) else {
            return Err(self.rejected_frame(frame, purpose, "frame_charge_overflow"));
        };
        let tenant = if purpose == StreamPurpose::Request {
            match tenant {
                Some(tenant) => match self.tenants.try_reserve(tenant, bytes as usize) {
                    Some(permit) => Some(permit),
                    None => return Err(self.rejected_frame(frame, purpose, "tenant_admission")),
                },
                None => None,
            }
        } else {
            None
        };
        let slot = if raft_control {
            // The reader retains one transport-charged frame while existing Raft work completes.
            match timeout_at(frame.deadline(), queue.slots.clone().acquire_owned()).await {
                Ok(Ok(slot)) => slot,
                _ => return Err(self.rejected_frame(frame, purpose, "raft_slot_deadline")),
            }
        } else {
            match queue.slots.clone().try_acquire_owned() {
                Ok(slot) => slot,
                Err(_) => return Err(self.rejected_frame(frame, purpose, "handler_slots")),
            }
        };
        let memory = match purpose {
            StreamPurpose::Core => self.budget.try_reserve_core(bytes),
            StreamPurpose::Ack => self.budget.try_reserve_ack(bytes),
            StreamPurpose::Request => self.budget.try_reserve(class, bytes),
        };
        let Ok(memory) = memory else {
            return Err(self.rejected_frame(frame, purpose, "handler_memory"));
        };
        let job = Job {
            peer,
            frame,
            reply,
            _slot: slot,
            memory,
            order,
            tenant,
        };
        self.health.admitted.inc();
        queue.sender.try_send(job).map_err(|error| {
            self.rejected_frame(error.into_inner().frame, purpose, "handler_queue")
        })
    }
}

#[derive(Clone)]
pub(crate) struct InProcess {
    dispatcher: Arc<Dispatcher>,
    readonly: mpsc::Sender<ReadOnlyJob>,
    stop: watch::Sender<Option<Instant>>,
    finished: watch::Receiver<bool>,
}

pub(crate) struct LocalRequests {
    core: mpsc::Receiver<Job>,
    ack: mpsc::Receiver<Job>,
    data: mpsc::Receiver<Job>,
    readonly: mpsc::Receiver<ReadOnlyJob>,
    stop: watch::Receiver<Option<Instant>>,
    finished: watch::Sender<bool>,
    dispatcher: Arc<Dispatcher>,
}

#[cfg(test)]
pub(crate) struct LocalResponse {
    response: mpsc::Receiver<Reply>,
    deadline: Instant,
}

struct ReadOnlyJob(Pin<Box<dyn Future<Output = ()> + Send>>);

struct ReadOnlyReply<T> {
    value: T,
    _memory: MemoryPermit,
    _slot: OwnedSemaphorePermit,
}

pub(crate) struct LocalReadOnlyResponse<T> {
    response: mpsc::Receiver<ReadOnlyReply<T>>,
    deadline: Instant,
}

pub(crate) struct LocalAdmission {
    _slot: OwnedSemaphorePermit,
    _memory: MemoryPermit,
    _active: HandlerGauge,
}

impl InProcess {
    /// Direct semantic handlers retain the same class slots and memory as queued work.
    pub(crate) fn admit_local(
        &self,
        purpose: StreamPurpose,
        bytes: u32,
        deadline: Instant,
    ) -> crate::Result<LocalAdmission> {
        if self.stop.borrow().is_some()
            || Instant::now() >= deadline
            || !(1..=65_536).contains(&bytes)
        {
            return Err(crate::Error::Configuration {
                reason: "local semantic work is stopped, expired or exceeds its byte bound",
            });
        }
        let queue = match purpose {
            StreamPurpose::Core => &self.dispatcher.core,
            StreamPurpose::Ack => &self.dispatcher.ack,
            StreamPurpose::Request => &self.dispatcher.data,
        };
        let slot =
            queue
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| crate::Error::Configuration {
                    reason: "local semantic handler slots are exhausted",
                })?;
        let memory = match purpose {
            StreamPurpose::Core => self.dispatcher.budget.try_reserve_core(bytes),
            StreamPurpose::Ack => self.dispatcher.budget.try_reserve_ack(bytes),
            StreamPurpose::Request => self
                .dispatcher
                .budget
                .try_reserve(TrafficClass::Control, bytes),
        }
        .map_err(|source| crate::Error::LocalMemory { source })?;
        self.dispatcher.health.admitted.inc();
        self.dispatcher.health.active_handlers.inc();
        Ok(LocalAdmission {
            _slot: slot,
            _memory: memory,
            _active: HandlerGauge(self.dispatcher.health.active_handlers.clone()),
        })
    }

    pub fn new(
        config: DispatchConfig,
        budget: MemoryBudget,
        health: Arc<DispatchMetrics>,
    ) -> crate::Result<(Self, LocalRequests)> {
        validate(&config)?;
        let (core, core_rx) = ClassQueue::new(config.core_handlers);
        let (ack, ack_rx) = ClassQueue::new(config.ack_handlers);
        let (data, data_rx) = ClassQueue::new(config.data_handlers);
        let (readonly, readonly_rx) = mpsc::channel(config.data_handlers);
        let maximum_frame = [
            TrafficClass::Control,
            TrafficClass::Produce,
            TrafficClass::Delivery,
        ]
        .into_iter()
        .map(|class| frame_limit(class, &config.protocol) as usize)
        .max()
        .unwrap_or(0)
        .saturating_mul(2);
        let tenants = TenantAdmission::new(budget.clone(), config.data_handlers, maximum_frame);
        let dispatcher = Arc::new(Dispatcher {
            raft: None,
            core,
            ack,
            data,
            budget,
            config,
            health,
            tenants,
        });
        let (stop, stopped) = watch::channel(None);
        let (finished, completion) = watch::channel(false);
        Ok((
            Self {
                dispatcher: dispatcher.clone(),
                readonly,
                stop,
                finished: completion,
            },
            LocalRequests {
                core: core_rx,
                ack: ack_rx,
                data: data_rx,
                readonly: readonly_rx,
                stop: stopped,
                finished,
                dispatcher,
            },
        ))
    }

    /// Read-only semantic work shares ordinary slots and memory without a wire envelope.
    pub(crate) async fn submit_readonly<T: Send + 'static>(
        &self,
        bytes: u32,
        deadline: Instant,
        work: impl Future<Output = T> + Send + 'static,
    ) -> crate::Result<LocalReadOnlyResponse<T>> {
        if self.stop.borrow().is_some()
            || Instant::now() >= deadline
            || !(1..=65_536).contains(&bytes)
        {
            return Err(crate::Error::Configuration {
                reason: "local read-only work is stopped, expired or exceeds its byte bound",
            });
        }
        let slot = self
            .dispatcher
            .data
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| crate::Error::Configuration {
                reason: "local read-only handler slots are exhausted",
            })?;
        let memory = self
            .dispatcher
            .budget
            .try_reserve(TrafficClass::Control, bytes)
            .map_err(|source| crate::Error::LocalMemory { source })?;
        let (reply, response) = mpsc::channel(1);
        let health = self.dispatcher.health.clone();
        let job = ReadOnlyJob(Box::pin(async move {
            if reply.is_closed() || Instant::now() >= deadline {
                return;
            }
            health.active_handlers.inc();
            let _active = HandlerGauge(health.active_handlers.clone());
            // No mutation is handed off by this entry; cancellation can release the read.
            let result = tokio::select! {
                biased;
                _ = reply.closed() => return,
                result = timeout_at(deadline, work) => result,
            };
            if let Ok(value) = result {
                let _ = reply.try_send(ReadOnlyReply {
                    value,
                    _memory: memory,
                    _slot: slot,
                });
            }
        }));
        self.readonly
            .try_send(job)
            .map_err(|_| crate::Error::Configuration {
                reason: "local read-only queue is unavailable",
            })?;
        self.dispatcher.health.admitted.inc();
        Ok(LocalReadOnlyResponse { response, deadline })
    }

    // This frame fixture exercises the shared remote queues without opening a transport.
    #[cfg(test)]
    async fn submit<S: WireService>(
        &self,
        service: &Arc<S>,
        peer: Authorization,
        mut frame: Frame,
        purpose: StreamPurpose,
        deadline: Instant,
    ) -> crate::Result<LocalResponse> {
        if self.stop.borrow().is_some() || Instant::now() >= deadline {
            return Err(crate::Error::Configuration {
                reason: "local dispatch is stopped or its deadline expired",
            });
        }
        let class = TrafficClass::try_from(frame.traffic_class).map_err(|_| {
            crate::Error::Configuration {
                reason: "local request traffic class is invalid",
            }
        })?;
        if class == TrafficClass::Replication || frame.kind != i32::from(FrameKind::InternalCommand)
        {
            return Err(crate::Error::Configuration {
                reason: "local dispatch accepts semantic commands only",
            });
        }
        let bytes = u32::try_from(frame.encoded_len())
            .ok()
            .filter(|bytes| {
                *bytes > 0 && *bytes <= frame_limit(class, &self.dispatcher.config.protocol)
            })
            .ok_or(crate::Error::Configuration {
                reason: "local request exceeds its frame limit",
            })?;
        let memory = match purpose {
            StreamPurpose::Core => self.dispatcher.budget.try_reserve_core(bytes),
            StreamPurpose::Ack => self.dispatcher.budget.try_reserve_ack(bytes),
            StreamPurpose::Request => self.dispatcher.budget.try_reserve(class, bytes),
        }
        .map_err(|source| crate::Error::LocalMemory { source })?;
        frame.payload = Bytes::from_owner(ReplyMemory {
            bytes: frame.payload,
            _permit: memory,
            _tenant: None,
        });
        let execution = service.execution_purpose(&peer, &frame, purpose);
        let tenant = service.scheduling_key(&peer, &frame);
        let (reply, response) = mpsc::channel(1);
        self.dispatcher
            .submit(
                peer,
                execution,
                AdmittedFrame::Local { frame, deadline },
                reply,
                None,
                tenant,
            )
            .await
            .map_err(|_| crate::Error::Configuration {
                reason: "local handler slots or memory are exhausted",
            })?;
        Ok(LocalResponse { response, deadline })
    }

    pub fn begin_drain(&self, deadline: Instant) {
        self.dispatcher.core.slots.close();
        self.dispatcher.ack.slots.close();
        self.dispatcher.data.slots.close();
        self.stop.send_modify(|current| {
            *current = Some(current.map_or(deadline, |current| current.min(deadline)))
        });
    }

    pub async fn close(&self, deadline: Instant) -> crate::Result<()> {
        self.begin_drain(deadline);
        let mut finished = self.finished.clone();
        while !*finished.borrow() {
            timeout_at(deadline, finished.changed())
                .await
                .map_err(|_| crate::Error::Configuration {
                    reason: "local dispatch drain deadline elapsed",
                })?
                .map_err(|_| crate::Error::Configuration {
                    reason: "local dispatch completion is unavailable",
                })?;
        }
        Ok(())
    }
}

#[cfg(test)]
impl LocalResponse {
    pub async fn receive(mut self) -> crate::Result<Frame> {
        timeout_at(self.deadline, self.response.recv())
            .await
            .map_err(|_| crate::Error::Configuration {
                reason: "local response deadline elapsed",
            })?
            .map(|reply| reply.frame)
            .ok_or(crate::Error::Configuration {
                reason: "local handler ended without a response",
            })
    }
}

impl<T> LocalReadOnlyResponse<T> {
    pub(crate) async fn receive(mut self) -> crate::Result<T> {
        timeout_at(self.deadline, self.response.recv())
            .await
            .map_err(|_| crate::Error::Configuration {
                reason: "local read-only response deadline elapsed",
            })?
            .map(|reply| reply.value)
            .ok_or(crate::Error::Configuration {
                reason: "local read-only work ended without a response",
            })
    }
}

impl LocalRequests {
    pub async fn run<S: WireService>(mut self, service: Arc<S>) -> crate::Result<()> {
        let mut handlers = JoinSet::new();
        loop {
            if self.stop.borrow().is_some() {
                break;
            }
            tokio::select! {
                changed = self.stop.changed() => { if changed.is_err() { break; } },
                result = handlers.join_next(), if !handlers.is_empty() => {
                    if let Some(result) = result { result.map_err(|source| crate::Error::Task { source })?; }
                },
                Some(job) = self.core.recv() => { handlers.spawn(execute(service.clone(), job, self.dispatcher.config.protocol.clone(), self.dispatcher.health.clone())); },
                Some(job) = self.ack.recv() => { handlers.spawn(execute(service.clone(), job, self.dispatcher.config.protocol.clone(), self.dispatcher.health.clone())); },
                Some(job) = self.data.recv() => { handlers.spawn(execute(service.clone(), job, self.dispatcher.config.protocol.clone(), self.dispatcher.health.clone())); },
                Some(job) = self.readonly.recv() => { handlers.spawn(job.0); },
            }
        }
        let deadline = self.stop.borrow().unwrap_or_else(Instant::now);
        for receiver in [&mut self.core, &mut self.ack, &mut self.data] {
            receiver.close();
            while let Ok(job) = receiver.try_recv() {
                reject_local_queued(job, &self.dispatcher.config.protocol);
            }
        }
        self.readonly.close();
        while self.readonly.try_recv().is_ok() {}
        while !handlers.is_empty() {
            timeout_at(deadline, handlers.join_next())
                .await
                .map_err(|_| crate::Error::Configuration {
                    reason: "local handlers exceeded the process drain deadline",
                })?
                .transpose()
                .map_err(|source| crate::Error::Task { source })?;
        }
        Ok(())
    }
}

impl Drop for LocalRequests {
    fn drop(&mut self) {
        self.finished.send_replace(true);
    }
}

fn reject_local_queued(job: Job, limits: &ProtocolLimits) {
    if let Some(mut response) = overload(job.frame.frame(), limits, ErrorCode::Unavailable) {
        response.payload = Bytes::from_owner(ReplyMemory {
            bytes: response.payload,
            _permit: job.memory,
            _tenant: job.tenant,
        });
        let _ = job.reply.try_send(Reply {
            frame: response,
            deadline: job.frame.deadline(),
            notification: None,
        });
    }
}

/// Shutdown stops admission first, then drains owned handlers for a finite interval.
pub async fn run<S: WireService>(
    mut server: Server,
    service: Arc<S>,
    config: DispatchConfig,
    budget: MemoryBudget,
    health: Arc<DispatchMetrics>,
    mut shutdown: watch::Receiver<bool>,
) -> crate::Result<()> {
    validate(&config)?;
    // Semantic handlers can await Raft; they must never occupy its own control execution slots.
    let (raft, mut raft_rx) = ClassQueue::new(config.raft_handlers);
    let (core, mut core_rx) = ClassQueue::new(config.core_handlers);
    let (ack, mut ack_rx) = ClassQueue::new(config.ack_handlers);
    let (data, mut data_rx) = ClassQueue::new(config.data_handlers);
    let maximum_frame = [
        TrafficClass::Control,
        TrafficClass::Produce,
        TrafficClass::Delivery,
    ]
    .into_iter()
    .map(|class| frame_limit(class, &config.protocol) as usize)
    .max()
    .unwrap_or(0)
    .saturating_mul(2);
    let tenants = TenantAdmission::new(budget.clone(), config.data_handlers, maximum_frame);
    let dispatcher = Arc::new(Dispatcher {
        raft: Some(raft),
        core,
        ack,
        data,
        budget,
        config,
        health,
        tenants,
    });
    let mut sessions = JoinSet::new();
    let mut handlers = JoinSet::new();
    while !*shutdown.borrow() {
        while handlers.try_join_next().is_some() {}
        while sessions.try_join_next().is_some() {}
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break } },
            _ = sessions.join_next(), if !sessions.is_empty() => {},
            _ = handlers.join_next(), if !handlers.is_empty() => {},
            Some(job) = raft_rx.recv() => { handlers.spawn(execute(service.clone(), job, dispatcher.config.protocol.clone(), dispatcher.health.clone())); },
            Some(job) = core_rx.recv() => { handlers.spawn(execute(service.clone(), job, dispatcher.config.protocol.clone(), dispatcher.health.clone())); },
            Some(job) = ack_rx.recv() => { handlers.spawn(execute(service.clone(), job, dispatcher.config.protocol.clone(), dispatcher.health.clone())); },
            Some(job) = data_rx.recv() => { handlers.spawn(execute(service.clone(), job, dispatcher.config.protocol.clone(), dispatcher.health.clone())); },
            session = server.accept() => match session {
                Some(session) => { sessions.spawn(serve_session(session, service.clone(), dispatcher.clone(), shutdown.clone())); },
                None => break,
            }
        }
    }
    let deadline = Instant::now()
        .checked_add(dispatcher.config.drain_timeout)
        .ok_or(crate::Error::Configuration {
            reason: "server drain deadline overflow",
        })?;
    let listener_result = server.stop_accepting(deadline).await;
    for receiver in [&mut raft_rx, &mut core_rx, &mut ack_rx, &mut data_rx] {
        receiver.close();
        while let Ok(job) = receiver.try_recv() {
            handlers.spawn(execute(
                service.clone(),
                job,
                dispatcher.config.protocol.clone(),
                dispatcher.health.clone(),
            ));
        }
    }
    while !handlers.is_empty() {
        if timeout_at(deadline, handlers.join_next()).await.is_err() {
            handlers.abort_all();
            break;
        }
    }
    while handlers.join_next().await.is_some() {}
    while !sessions.is_empty() {
        if timeout_at(deadline, sessions.join_next()).await.is_err() {
            sessions.abort_all();
            break;
        }
    }
    while sessions.join_next().await.is_some() {}
    let endpoint_result = server.shutdown_by(deadline).await;
    listener_result
        .and(endpoint_result)
        .map_err(|source| crate::Error::Transport { source })
}

fn validate(config: &DispatchConfig) -> crate::Result<()> {
    config
        .protocol
        .validate()
        .map_err(|source| crate::Error::Protocol { source })?;
    for (value, reason) in [
        (
            config.raft_handlers,
            "dispatch.raft_handlers must be between 1 and 4096",
        ),
        (
            config.core_handlers,
            "dispatch.core_handlers must be between 1 and 4096",
        ),
        (
            config.ack_handlers,
            "dispatch.ack_handlers must be between 1 and 4096",
        ),
        (
            config.data_handlers,
            "dispatch.data_handlers must be between 1 and 4096",
        ),
        (
            config.replies_per_stream,
            "dispatch.replies_per_stream must be between 1 and 4096",
        ),
    ] {
        if !(1..=4096).contains(&value) {
            return Err(crate::Error::Configuration { reason });
        }
    }
    if config.idle_timeout.is_zero() || config.idle_timeout > Duration::from_secs(300) {
        return Err(crate::Error::Configuration {
            reason: "dispatch.idle_timeout must be greater than zero and at most 300 seconds",
        });
    }
    if config.drain_timeout.is_zero() || config.drain_timeout > Duration::from_secs(60) {
        return Err(crate::Error::Configuration {
            reason: "dispatch.drain_timeout must be greater than zero and at most 60 seconds",
        });
    }
    Ok(())
}

async fn execute<S: WireService>(
    service: Arc<S>,
    job: Job,
    limits: ProtocolLimits,
    health: Arc<DispatchMetrics>,
) {
    if job.reply.is_closed() {
        return;
    }
    health.active_handlers.inc();
    let _active = HandlerGauge(health.active_handlers.clone());
    let Job {
        peer,
        frame,
        reply,
        _slot,
        memory,
        order,
        tenant,
    } = job;
    let deadline = frame.deadline();
    if let Some(deferred) = service.defer(peer.clone(), frame.frame(), deadline) {
        // The deferred owner charged its compact request and reply before this handoff.
        drop((frame, _slot, memory, order, tenant, _active));
        let result = tokio::select! {
            result = timeout_at(deadline, deferred) => result,
            _ = reply.closed() => return,
        };
        if let Ok(Ok(Some(response))) = result {
            let _ = timeout_at(
                deadline,
                reply.send(Reply {
                    frame: response,
                    deadline,
                    notification: None,
                }),
            )
            .await;
        }
        return;
    }
    let notification_peer = service
        .response_requires_flush(&peer, frame.frame())
        .then(|| peer.clone());
    let result = if Instant::now() >= deadline {
        overload(frame.frame(), &limits, ErrorCode::DeadlineExceeded)
    } else {
        // Keep business future layout outside the bounded dispatcher task's state.
        match Box::pin(service.handle(peer, frame.frame(), deadline, order)).await {
            Ok(response) => response,
            Err(error) => {
                health.handler_errors.inc();
                if health.may_log(std::time::Instant::now()) {
                    let error = anyhow::Error::new(error);
                    tracing::warn!(
                        event_code = "wire_handler_failed",
                        stage = "handler_execution",
                        command_kind = FrameKind::try_from(frame.frame().kind).map(|kind| kind.as_str_name()).unwrap_or("UNSPECIFIED"),
                        request_id = %Id::from_slice(&frame.frame().request_id).map(|id| id.to_string()).unwrap_or_else(|_| "invalid".into()),
                        operation_id = %Id::from_slice(&frame.frame().operation_id).map(|id| id.to_string()).unwrap_or_else(|_| "invalid".into()),
                        error = %format_args!("{error:#}"),
                        "Wire request handler failed"
                    );
                }
                failure(frame.frame(), &limits)
            }
        }
    };
    log_unknown_reply(frame.frame(), result.as_ref(), &limits, &health);
    if let Some(mut response) = result {
        response.payload = Bytes::from_owner(ReplyMemory {
            bytes: response.payload,
            _permit: memory,
            _tenant: tenant,
        });
        // Keep the bounded handler slot while the writer catches up; a full queue is not a lost reply.
        let _ = timeout_at(
            deadline,
            reply.send(Reply {
                frame: response,
                deadline,
                notification: notification_peer
                    .and_then(|peer| frame.received().map(|frame| (peer, frame))),
            }),
        )
        .await;
    }
}

fn log_unknown_reply(
    request: &Frame,
    response: Option<&Frame>,
    limits: &ProtocolLimits,
    health: &DispatchMetrics,
) {
    let Some(error) = response.and_then(|frame| unknown_reply(frame, limits)) else {
        return;
    };
    if !health.may_log(std::time::Instant::now()) {
        return;
    }
    let command_kind = FrameKind::try_from(request.kind)
        .map(|kind| kind.as_str_name())
        .unwrap_or("UNSPECIFIED");
    let request_id = Id::from_slice(&request.request_id)
        .map(|id| id.to_string())
        .unwrap_or_else(|_| "invalid".into());
    let operation_id = Id::from_slice(&request.operation_id)
        .map(|id| id.to_string())
        .unwrap_or_else(|_| "invalid".into());
    tracing::warn!(
        event_code = "wire_request_unknown",
        stage = "handler_reply",
        command_kind,
        request_id,
        operation_id,
        code = error.code,
        reason = error.message,
        retryable = error.retryable,
        outcome = "Unknown",
        "Handler returned an unknown operation result"
    );
}

fn unknown_reply(frame: &Frame, limits: &ProtocolLimits) -> Option<ErrorReply> {
    if frame.kind != i32::from(FrameKind::Error) {
        return None;
    }
    decode_payload::<ErrorReply>(frame.payload.clone(), limits)
        .ok()
        .filter(|error| error.outcome == i32::from(Outcome::Unknown))
}

struct ReplyMemory {
    bytes: Bytes,
    _permit: MemoryPermit,
    _tenant: Option<TenantPermit>,
}
impl AsRef<[u8]> for ReplyMemory {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

async fn serve_session<S: WireService>(
    session: Arc<Session>,
    service: Arc<S>,
    dispatcher: Arc<Dispatcher>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut streams = JoinSet::new();
    let peer = session.connection().authorization().clone();
    let (stream_stop, stream_shutdown) = watch::channel(false);
    for stream in session.take_initial_streams().await {
        streams.spawn(serve_stream(
            stream,
            service.clone(),
            peer.clone(),
            dispatcher.clone(),
            stream_shutdown.clone(),
        ));
    }
    while !*shutdown.borrow() {
        while streams.try_join_next().is_some() {}
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = streams.join_next(), if !streams.is_empty() => {},
            stream = session.accept() => match stream {
                Some(stream) => { streams.spawn(serve_stream(stream, service.clone(), peer.clone(), dispatcher.clone(), stream_shutdown.clone())); },
                None => break,
            }
        }
    }
    // Native connection closure must release reader-owned admission waiters before handler drain.
    let _ = stream_stop.send(true);
    if let Some(deadline) = Instant::now().checked_add(dispatcher.config.drain_timeout) {
        while !streams.is_empty() {
            if timeout_at(deadline, streams.join_next()).await.is_err() {
                break;
            }
        }
    }
    session.close();
}

async fn serve_stream<S: WireService>(
    stream: FramedStream,
    service: Arc<S>,
    peer: Authorization,
    dispatcher: Arc<Dispatcher>,
    mut shutdown: watch::Receiver<bool>,
) {
    let purpose = stream.purpose();
    let (mut sender, mut receiver) = stream.split();
    let (replies, mut outgoing) = mpsc::channel::<Reply>(dispatcher.config.replies_per_stream);
    let sequence = Arc::new(AsyncMutex::new(()));
    let admission = service.clone();
    let read = async move {
        loop {
            let received = tokio::select! {
                received = receiver.receive_next(dispatcher.config.idle_timeout) => received,
                _ = shutdown.changed() => { drop(replies); return true },
            };
            let frame = match received {
                Ok(Some(frame)) => frame,
                Ok(None) => {
                    drop(replies);
                    return true;
                }
                Err(_) => return false,
            };
            let order = if frame.frame().traffic_class == i32::from(TrafficClass::Produce) {
                match timeout_at(frame.deadline(), sequence.clone().lock_owned()).await {
                    Ok(guard) => Some(SubmissionOrder { _guard: guard }),
                    Err(_) => {
                        if let Some(response) = overload(
                            frame.frame(),
                            &dispatcher.config.protocol,
                            ErrorCode::DeadlineExceeded,
                        ) {
                            let _ = timeout_at(
                                frame.deadline(),
                                replies.send(Reply {
                                    frame: response,
                                    deadline: frame.deadline(),
                                    notification: None,
                                }),
                            )
                            .await;
                        }
                        continue;
                    }
                }
            } else {
                None
            };
            let execution = admission.execution_purpose(&peer, frame.frame(), purpose);
            let tenant = admission.scheduling_key(&peer, frame.frame());
            let submitted = tokio::select! {
                submitted = dispatcher.submit(peer.clone(), execution, AdmittedFrame::Remote(frame), replies.clone(), order, tenant) => submitted,
                _ = shutdown.changed() => { drop(replies); return true },
            };
            if let Err(frame) = submitted
                && let Some(response) = overload(
                    frame.frame(),
                    &dispatcher.config.protocol,
                    ErrorCode::QuotaExceeded,
                )
            {
                dispatcher.health.rejected.inc();
                let _ = timeout_at(
                    frame.deadline(),
                    replies.send(Reply {
                        frame: response,
                        deadline: frame.deadline(),
                        notification: None,
                    }),
                )
                .await;
            }
        }
    };
    let write = async {
        while let Some(reply) = outgoing.recv().await {
            // An expired reply must not reset other in-flight RPCs sharing this stream.
            if Instant::now() >= reply.deadline {
                continue;
            }
            if let Err(error) = sender.send(&reply.frame, reply.deadline).await {
                if matches!(error, crate::transport::Error::Deadline { .. }) && sender.is_usable() {
                    continue;
                }
                return;
            }
            if let Some((peer, request)) = reply.notification {
                if sender.flush(reply.deadline).await.is_err() {
                    return;
                }
                service.response_sent(&peer, request.frame(), &reply.frame);
            }
        }
        let _ = sender.finish();
    };
    tokio::pin!(write);
    tokio::select! {
        clean = read => { if clean { write.await } },
        _ = &mut write => {},
    }
}

fn overload(request: &Frame, limits: &ProtocolLimits, code: ErrorCode) -> Option<Frame> {
    error_frame(
        request,
        limits,
        code,
        Outcome::Rejected,
        match code {
            ErrorCode::DeadlineExceeded => "Request deadline expired before handler admission",
            ErrorCode::QuotaExceeded => "Handler slots or memory admission capacity are exhausted",
            _ => "Service stopped accepting requests before handler admission",
        },
    )
}
fn failure(request: &Frame, limits: &ProtocolLimits) -> Option<Frame> {
    error_frame(
        request,
        limits,
        ErrorCode::Unavailable,
        Outcome::Unknown,
        "Handler failed; recover the result using the original operation identity",
    )
}
fn error_frame(
    request: &Frame,
    limits: &ProtocolLimits,
    code: ErrorCode,
    outcome: Outcome,
    message: &'static str,
) -> Option<Frame> {
    let class = TrafficClass::try_from(request.traffic_class).ok()?;
    new_frame(
        FrameKind::Error,
        class,
        Id::from_slice(&request.request_id).ok()?,
        Id::from_slice(&request.operation_id).ok()?,
        &ErrorReply {
            code: code.into(),
            message: message.into(),
            retryable: true,
            outcome: outcome.into(),
            ..Default::default()
        },
        request.remaining_timeout_ms,
        limits,
    )
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct LocalService {
        blocked: Id,
        entered: tokio::sync::Notify,
        release: Semaphore,
        calls: AtomicUsize,
    }

    impl WireService for LocalService {
        type Error = std::io::Error;

        async fn handle(
            &self,
            _peer: Authorization,
            frame: &Frame,
            _deadline: Instant,
            _order: Option<SubmissionOrder>,
        ) -> std::io::Result<Option<Frame>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if frame.operation_id.as_ref() == self.blocked.as_bytes() {
                self.entered.notify_one();
                let _release = self.release.acquire().await.unwrap();
            }
            let mut reply = frame.clone();
            reply.kind = FrameKind::InternalResult.into();
            Ok(Some(reply))
        }
    }

    fn local_fixture() -> (InProcess, LocalRequests, Arc<LocalService>, MemoryBudget) {
        let config = DispatchConfig {
            data_handlers: 1,
            core_handlers: 1,
            ack_handlers: 1,
            ..Default::default()
        };
        let budget = MemoryBudget::new(&crate::config::MemoryConfig::default()).unwrap();
        let (dispatch, requests) =
            InProcess::new(config, budget.clone(), Arc::new(DispatchMetrics::default())).unwrap();
        let service = Arc::new(LocalService {
            blocked: Id::new_v7().unwrap(),
            entered: tokio::sync::Notify::new(),
            release: Semaphore::new(0),
            calls: AtomicUsize::new(0),
        });
        (dispatch, requests, service, budget)
    }

    fn local_request(operation: Id) -> Frame {
        new_frame(
            FrameKind::InternalCommand,
            TrafficClass::Control,
            Id::new_v7().unwrap(),
            operation,
            &tidemq_protocol::internal::InternalCommand::default(),
            2_000,
            &ProtocolLimits::default(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn local_normal_pressure_preserves_core_and_completion_capacity() {
        let (dispatch, requests, service, budget) = local_fixture();
        let before = budget.available_bytes();
        let runner = tokio::spawn(requests.run(service.clone()));
        let deadline = Instant::now() + Duration::from_secs(2);
        let normal = dispatch
            .submit(
                &service,
                Authorization::Server,
                local_request(service.blocked),
                StreamPurpose::Request,
                deadline,
            )
            .await
            .unwrap();
        timeout_at(deadline, service.entered.notified())
            .await
            .unwrap();
        assert!(
            dispatch
                .submit(
                    &service,
                    Authorization::Server,
                    local_request(Id::new_v7().unwrap()),
                    StreamPurpose::Request,
                    deadline
                )
                .await
                .is_err()
        );
        for purpose in [StreamPurpose::Core, StreamPurpose::Ack] {
            let response = dispatch
                .submit(
                    &service,
                    Authorization::Server,
                    local_request(Id::new_v7().unwrap()),
                    purpose,
                    deadline,
                )
                .await
                .unwrap();
            assert_eq!(
                response.receive().await.unwrap().kind,
                i32::from(FrameKind::InternalResult)
            );
        }
        service.release.add_permits(1);
        drop(normal.receive().await.unwrap());
        dispatch.close(deadline).await.unwrap();
        runner.await.unwrap().unwrap();
        assert_eq!(budget.available_bytes(), before);
    }

    #[tokio::test]
    async fn local_queued_cancellation_does_not_execute_and_releases_admission() {
        let (dispatch, requests, service, budget) = local_fixture();
        let before = budget.available_bytes();
        let deadline = Instant::now() + Duration::from_secs(2);
        let pending = dispatch
            .submit(
                &service,
                Authorization::Server,
                local_request(service.blocked),
                StreamPurpose::Request,
                deadline,
            )
            .await
            .unwrap();
        drop(pending);
        let runner = tokio::spawn(requests.run(service.clone()));
        timeout_at(deadline, async {
            while dispatch.dispatcher.data.slots.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(service.calls.load(Ordering::Relaxed), 0);
        dispatch.close(deadline).await.unwrap();
        runner.await.unwrap().unwrap();
        assert_eq!(budget.available_bytes(), before);
    }

    #[tokio::test]
    async fn local_drain_rejects_queued_commands_before_handoff() {
        let (dispatch, requests, service, budget) = local_fixture();
        let before = budget.available_bytes();
        let deadline = Instant::now() + Duration::from_secs(2);
        let pending = dispatch
            .submit(
                &service,
                Authorization::Server,
                local_request(service.blocked),
                StreamPurpose::Request,
                deadline,
            )
            .await
            .unwrap();
        dispatch.begin_drain(deadline);
        requests.run(service.clone()).await.unwrap();
        let response = pending.receive().await.unwrap();
        let error: ErrorReply =
            decode_payload(response.payload.clone(), &ProtocolLimits::default()).unwrap();
        assert_eq!(error.outcome, i32::from(Outcome::Rejected));
        assert_eq!(service.calls.load(Ordering::Relaxed), 0);
        drop(response);
        assert_eq!(budget.available_bytes(), before);
    }

    #[tokio::test]
    async fn local_dispatch_rejects_replication_without_allocating_a_raft_queue() {
        let (dispatch, requests, service, budget) = local_fixture();
        let before = budget.available_bytes();
        let mut request = local_request(Id::new_v7().unwrap());
        request.kind = FrameKind::RaftAppend.into();
        request.traffic_class = TrafficClass::Replication.into();
        assert!(
            dispatch
                .submit(
                    &service,
                    Authorization::Server,
                    request,
                    StreamPurpose::Core,
                    Instant::now() + Duration::from_secs(1)
                )
                .await
                .is_err()
        );
        assert!(dispatch.dispatcher.raft.is_none());
        drop(requests);
        assert_eq!(budget.available_bytes(), before);
    }

    #[tokio::test]
    async fn typed_direct_admission_shares_normal_capacity_and_preserves_reserved_classes() {
        let (dispatch, requests, _service, budget) = local_fixture();
        let before = budget.available_bytes();
        let deadline = Instant::now() + Duration::from_secs(2);
        let normal = dispatch
            .admit_local(StreamPurpose::Request, 65_536, deadline)
            .unwrap();
        assert!(
            dispatch
                .admit_local(StreamPurpose::Request, 1, deadline)
                .is_err()
        );
        assert!(
            dispatch
                .submit_readonly(1, deadline, async {})
                .await
                .is_err()
        );
        let core = dispatch
            .admit_local(StreamPurpose::Core, 1024, deadline)
            .unwrap();
        let ack = dispatch
            .admit_local(StreamPurpose::Ack, 1024, deadline)
            .unwrap();
        assert_eq!(budget.available_bytes() + 65_536 + 2048, before);
        drop((normal, core, ack));
        assert_eq!(budget.available_bytes(), before);
        assert!(
            dispatch
                .admit_local(StreamPurpose::Request, 65_537, deadline)
                .is_err()
        );
        assert!(
            dispatch
                .admit_local(StreamPurpose::Core, 1, Instant::now())
                .is_err()
        );
        dispatch.begin_drain(deadline);
        for purpose in [
            StreamPurpose::Request,
            StreamPurpose::Core,
            StreamPurpose::Ack,
        ] {
            assert!(dispatch.admit_local(purpose, 1, deadline).is_err());
        }
        drop(requests);
        assert_eq!(budget.available_bytes(), before);
    }

    #[tokio::test]
    async fn typed_readonly_replies_retain_shared_slots_and_memory_without_blocking_core_or_ack() {
        let (dispatch, requests, service, budget) = local_fixture();
        let before = budget.available_bytes();
        let deadline = Instant::now() + Duration::from_secs(2);
        let (entered, completed) = tokio::sync::oneshot::channel();
        let pending = dispatch
            .submit_readonly(65_536, deadline, async move {
                let _ = entered.send(());
                42_u64
            })
            .await
            .unwrap();
        let runner = tokio::spawn(requests.run(service.clone()));
        completed.await.unwrap();
        assert_eq!(budget.available_bytes() + 65_536, before);
        assert!(
            dispatch
                .submit_readonly(1, deadline, async { 7_u64 })
                .await
                .is_err()
        );
        assert!(
            dispatch
                .submit(
                    &service,
                    Authorization::Server,
                    local_request(Id::new_v7().unwrap()),
                    StreamPurpose::Request,
                    deadline,
                )
                .await
                .is_err()
        );
        for purpose in [StreamPurpose::Core, StreamPurpose::Ack] {
            let response = dispatch
                .submit(
                    &service,
                    Authorization::Server,
                    local_request(Id::new_v7().unwrap()),
                    purpose,
                    deadline,
                )
                .await
                .unwrap();
            drop(response.receive().await.unwrap());
        }
        assert_eq!(pending.receive().await.unwrap(), 42);
        dispatch.close(deadline).await.unwrap();
        runner.await.unwrap().unwrap();
        assert_eq!(budget.available_bytes(), before);
    }

    #[tokio::test]
    async fn typed_readonly_queued_cancellation_skips_execution_and_releases_admission() {
        let (dispatch, requests, service, budget) = local_fixture();
        let before = budget.available_bytes();
        let deadline = Instant::now() + Duration::from_secs(2);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let pending = dispatch
            .submit_readonly(65_536, deadline, async move {
                observed.fetch_add(1, Ordering::Relaxed);
            })
            .await
            .unwrap();
        drop(pending);
        let runner = tokio::spawn(requests.run(service));
        timeout_at(deadline, async {
            while dispatch.dispatcher.data.slots.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        dispatch.close(deadline).await.unwrap();
        runner.await.unwrap().unwrap();
        assert_eq!(budget.available_bytes(), before);
    }

    #[tokio::test]
    async fn typed_readonly_active_cancellation_and_drain_release_all_owned_work() {
        let (dispatch, requests, service, budget) = local_fixture();
        let before = budget.available_bytes();
        let deadline = Instant::now() + Duration::from_secs(2);
        let (entered, running) = tokio::sync::oneshot::channel();
        let pending = dispatch
            .submit_readonly(65_536, deadline, async move {
                let _ = entered.send(());
                std::future::pending::<()>().await;
            })
            .await
            .unwrap();
        let runner = tokio::spawn(requests.run(service));
        running.await.unwrap();
        drop(pending);
        timeout_at(deadline, async {
            while budget.available_bytes() != before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        dispatch.close(deadline).await.unwrap();
        runner.await.unwrap().unwrap();
        assert!(
            dispatch
                .submit_readonly(1, deadline, async {})
                .await
                .is_err()
        );

        let (dispatch, requests, service, budget) = local_fixture();
        let before = budget.available_bytes();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let pending = dispatch
            .submit_readonly(65_536, deadline, async move {
                observed.fetch_add(1, Ordering::Relaxed);
            })
            .await
            .unwrap();
        dispatch.begin_drain(deadline);
        requests.run(service).await.unwrap();
        assert!(pending.receive().await.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(budget.available_bytes(), before);
    }

    #[test]
    fn only_valid_unknown_error_replies_are_classified_for_logging() {
        let limits = ProtocolLimits::default();
        let id = Id::new_v7().unwrap();
        let success = new_frame(
            FrameKind::TopicResult,
            TrafficClass::Control,
            id,
            id,
            &tidemq_protocol::public::TopicResult::default(),
            1_000,
            &limits,
        )
        .unwrap();
        assert!(unknown_reply(&success, &limits).is_none());
        let rejected = overload(&success, &limits, ErrorCode::Unavailable).unwrap();
        assert!(unknown_reply(&rejected, &limits).is_none());
        let mut unknown = failure(&success, &limits).unwrap();
        let classified = unknown_reply(&unknown, &limits).unwrap();
        assert_eq!(classified.outcome, i32::from(Outcome::Unknown));
        assert_eq!(classified.code, i32::from(ErrorCode::Unavailable));
        unknown.payload = Bytes::from_static(&[0xff]);
        assert!(unknown_reply(&unknown, &limits).is_none());
    }
}
