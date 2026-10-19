use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tidemq_protocol::common::{ErrorCode, ErrorReply, Frame, FrameKind, Outcome, TrafficClass};
use tidemq_protocol::framing::{frame_limit, new_frame};
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

pub trait WireService: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    fn handle(
        &self,
        peer: Authorization,
        frame: &Frame,
        deadline: Instant,
        order: Option<SubmissionOrder>,
    ) -> impl Future<Output = std::result::Result<Option<Frame>, Self::Error>> + Send;
}

/// A producer lane retains order only until submission, then waits for its reply independently.
pub struct SubmissionOrder {
    _guard: OwnedMutexGuard<()>,
}

#[derive(Clone, Debug)]
pub struct DispatchConfig {
    pub core_handlers: usize,
    pub ack_handlers: usize,
    pub data_handlers: usize,
    pub replies_per_stream: usize,
    pub idle_timeout: Duration,
    pub drain_timeout: Duration,
    pub protocol: ProtocolLimits,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
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
}
struct Job {
    peer: Authorization,
    frame: ReceivedFrame,
    reply: mpsc::Sender<Reply>,
    _slot: OwnedSemaphorePermit,
    memory: MemoryPermit,
    order: Option<SubmissionOrder>,
}
struct ClassQueue {
    slots: Arc<Semaphore>,
    sender: mpsc::Sender<Job>,
}
struct Dispatcher {
    core: ClassQueue,
    ack: ClassQueue,
    data: ClassQueue,
    budget: MemoryBudget,
    config: DispatchConfig,
    health: Arc<DispatchMetrics>,
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
    fn submit(
        &self,
        peer: Authorization,
        purpose: StreamPurpose,
        frame: ReceivedFrame,
        reply: mpsc::Sender<Reply>,
        order: Option<SubmissionOrder>,
    ) -> std::result::Result<(), Box<ReceivedFrame>> {
        let queue = match purpose {
            StreamPurpose::Core => &self.core,
            StreamPurpose::Ack => &self.ack,
            StreamPurpose::Request => &self.data,
        };
        let Ok(slot) = queue.slots.clone().try_acquire_owned() else {
            return Err(Box::new(frame));
        };
        let Ok(class) = TrafficClass::try_from(frame.frame().traffic_class) else {
            return Err(Box::new(frame));
        };
        let Some(bytes) = frame_limit(class, &self.config.protocol).checked_mul(2) else {
            return Err(Box::new(frame));
        };
        let memory = if purpose == StreamPurpose::Core {
            self.budget.try_reserve_core(bytes)
        } else {
            self.budget.try_reserve(class, bytes)
        };
        let Ok(memory) = memory else {
            return Err(Box::new(frame));
        };
        let job = Job {
            peer,
            frame,
            reply,
            _slot: slot,
            memory,
            order,
        };
        self.health.admitted.inc();
        queue
            .sender
            .try_send(job)
            .map_err(|error| Box::new(error.into_inner().frame))
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
    let (core, mut core_rx) = ClassQueue::new(config.core_handlers);
    let (ack, mut ack_rx) = ClassQueue::new(config.ack_handlers);
    let (data, mut data_rx) = ClassQueue::new(config.data_handlers);
    let dispatcher = Arc::new(Dispatcher {
        core,
        ack,
        data,
        budget,
        config,
        health,
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
            Some(job) = core_rx.recv() => { handlers.spawn(execute(service.clone(), job, dispatcher.config.protocol.clone(), dispatcher.health.clone())); },
            Some(job) = ack_rx.recv() => { handlers.spawn(execute(service.clone(), job, dispatcher.config.protocol.clone(), dispatcher.health.clone())); },
            Some(job) = data_rx.recv() => { handlers.spawn(execute(service.clone(), job, dispatcher.config.protocol.clone(), dispatcher.health.clone())); },
            session = server.accept() => match session {
                Some(session) => { sessions.spawn(serve_session(session, dispatcher.clone(), shutdown.clone())); },
                None => break,
            }
        }
    }
    server.shutdown().await;
    let deadline = Instant::now()
        .checked_add(dispatcher.config.drain_timeout)
        .ok_or(crate::Error::Configuration {
            reason: "server drain deadline overflow",
        })?;
    for receiver in [&mut core_rx, &mut ack_rx, &mut data_rx] {
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
    Ok(())
}

fn validate(config: &DispatchConfig) -> crate::Result<()> {
    config
        .protocol
        .validate()
        .map_err(|source| crate::Error::Protocol { source })?;
    if [
        config.core_handlers,
        config.ack_handlers,
        config.data_handlers,
        config.replies_per_stream,
    ]
    .iter()
    .any(|limit| *limit == 0 || *limit > 4096)
        || config.idle_timeout.is_zero()
        || config.idle_timeout > Duration::from_secs(300)
        || config.drain_timeout.is_zero()
        || config.drain_timeout > Duration::from_secs(60)
    {
        return Err(crate::Error::Configuration {
            reason: "wire dispatch limits are invalid",
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
    health.active_handlers.inc();
    let _active = HandlerGauge(health.active_handlers.clone());
    let Job {
        peer,
        frame,
        reply,
        _slot,
        memory,
        order,
    } = job;
    let deadline = frame.deadline();
    let result = if Instant::now() >= deadline {
        overload(frame.frame(), &limits, ErrorCode::DeadlineExceeded)
    } else {
        match service.handle(peer, frame.frame(), deadline, order).await {
            Ok(response) => response,
            Err(error) => {
                health.handler_errors.inc();
                if health.may_log(std::time::Instant::now()) {
                    let error = anyhow::Error::new(error);
                    tracing::warn!(event_code = "wire_handler_failed", error = %format_args!("{error:#}"));
                }
                failure(frame.frame(), &limits)
            }
        }
    };
    if let Some(mut frame) = result {
        frame.payload = Bytes::from_owner(ReplyMemory {
            bytes: frame.payload,
            _permit: memory,
        });
        let _ = reply.try_send(Reply { frame, deadline });
    }
}

struct ReplyMemory {
    bytes: Bytes,
    _permit: MemoryPermit,
}
impl AsRef<[u8]> for ReplyMemory {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

async fn serve_session(
    session: Arc<Session>,
    dispatcher: Arc<Dispatcher>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut streams = JoinSet::new();
    let peer = session.connection().authorization().clone();
    for stream in session.take_initial_streams().await {
        streams.spawn(serve_stream(
            stream,
            peer.clone(),
            dispatcher.clone(),
            shutdown.clone(),
        ));
    }
    loop {
        while streams.try_join_next().is_some() {}
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = streams.join_next(), if !streams.is_empty() => {},
            stream = session.accept() => match stream {
                Some(stream) => { streams.spawn(serve_stream(stream, peer.clone(), dispatcher.clone(), shutdown.clone())); },
                None => break,
            }
        }
    }
    if let Some(deadline) = Instant::now().checked_add(dispatcher.config.drain_timeout) {
        while !streams.is_empty() {
            if timeout_at(deadline, streams.join_next()).await.is_err() {
                break;
            }
        }
    }
    session.close();
}

async fn serve_stream(
    stream: FramedStream,
    peer: Authorization,
    dispatcher: Arc<Dispatcher>,
    mut shutdown: watch::Receiver<bool>,
) {
    let purpose = stream.purpose();
    let (mut sender, mut receiver) = stream.split();
    let (replies, mut outgoing) = mpsc::channel::<Reply>(dispatcher.config.replies_per_stream);
    let sequence = Arc::new(AsyncMutex::new(()));
    let read = async move {
        while let Some(deadline) = Instant::now().checked_add(dispatcher.config.idle_timeout) {
            let received = tokio::select! {
                received = receiver.receive(deadline) => received,
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
                            let _ = replies.try_send(Reply {
                                frame: response,
                                deadline: frame.deadline(),
                            });
                        }
                        continue;
                    }
                }
            } else {
                None
            };
            if let Err(frame) =
                dispatcher.submit(peer.clone(), purpose, frame, replies.clone(), order)
                && let Some(response) = overload(
                    frame.frame(),
                    &dispatcher.config.protocol,
                    ErrorCode::QuotaExceeded,
                )
            {
                dispatcher.health.rejected.inc();
                let _ = replies.try_send(Reply {
                    frame: response,
                    deadline: frame.deadline(),
                });
            }
        }
        false
    };
    let write = async {
        while let Some(reply) = outgoing.recv().await {
            if sender.send(&reply.frame, reply.deadline).await.is_err() {
                return;
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
        "Request was rejected before handler admission",
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
