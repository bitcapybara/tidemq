use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
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

pub trait WireService: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    fn handle(
        &self,
        peer: Authorization,
        frame: &Frame,
        deadline: Instant,
        order: Option<SubmissionOrder>,
    ) -> impl Future<Output = std::result::Result<Option<Frame>, Self::Error>> + Send;

    /// Nested business envelopes must not occupy the control handlers they depend on.
    fn execution_purpose(
        &self,
        _peer: &Authorization,
        _request: &Frame,
        transport_purpose: StreamPurpose,
    ) -> StreamPurpose {
        transport_purpose
    }

    /// Opt in only for replies that must reach the peer before closing the endpoint.
    fn response_requires_flush(&self, _peer: &Authorization, _request: &Frame) -> bool {
        false
    }

    /// Called after a requested native flush; transport receipt does not establish business commit.
    fn response_sent(&self, _peer: &Authorization, _request: &Frame, _response: &Frame) {}
}

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
    raft: ClassQueue,
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
    async fn submit(
        &self,
        peer: Authorization,
        purpose: StreamPurpose,
        frame: ReceivedFrame,
        reply: mpsc::Sender<Reply>,
        order: Option<SubmissionOrder>,
    ) -> std::result::Result<(), Box<ReceivedFrame>> {
        let raft_control = purpose == StreamPurpose::Core
            && matches!(&peer, Authorization::Internal(_))
            && matches!(
                FrameKind::try_from(frame.frame().kind),
                Ok(FrameKind::RaftVote | FrameKind::RaftAppend)
            );
        let queue = match purpose {
            StreamPurpose::Core if raft_control => &self.raft,
            StreamPurpose::Core => &self.core,
            StreamPurpose::Ack => &self.ack,
            StreamPurpose::Request => &self.data,
        };
        let slot = if raft_control {
            // The reader retains one transport-charged frame while existing Raft work completes.
            match timeout_at(frame.deadline(), queue.slots.clone().acquire_owned()).await {
                Ok(Ok(slot)) => slot,
                _ => return Err(Box::new(frame)),
            }
        } else {
            match queue.slots.clone().try_acquire_owned() {
                Ok(slot) => slot,
                Err(_) => {
                    if self.health.may_log(std::time::Instant::now()) {
                        tracing::debug!(event_code = "dispatch_admission_wait", reason = "slots",
                            purpose = ?purpose, frame_kind = frame.frame().kind,
                            internal = matches!(&peer, Authorization::Internal(_)),
                            "Dispatch is waiting for handler capacity");
                    }
                    return Err(Box::new(frame));
                }
            }
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
            if self.health.may_log(std::time::Instant::now()) {
                tracing::debug!(event_code = "dispatch_admission_wait", reason = "memory",
                    purpose = ?purpose, frame_kind = frame.frame().kind, requested_bytes = bytes,
                    available_data_bytes = self.budget.available_data_bytes(),
                    internal = matches!(&peer, Authorization::Internal(_)),
                    "Dispatch is waiting for request and reply memory");
            }
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
    // Semantic handlers can await Raft; they must never occupy its own control execution slots.
    let (raft, mut raft_rx) = ClassQueue::new(config.raft_handlers);
    let (core, mut core_rx) = ClassQueue::new(config.core_handlers);
    let (ack, mut ack_rx) = ClassQueue::new(config.ack_handlers);
    let (data, mut data_rx) = ClassQueue::new(config.data_handlers);
    let dispatcher = Arc::new(Dispatcher {
        raft,
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
    let notification_peer = service
        .response_requires_flush(&peer, frame.frame())
        .then(|| peer.clone());
    let result = if Instant::now() >= deadline {
        overload(frame.frame(), &limits, ErrorCode::DeadlineExceeded)
    } else {
        match service.handle(peer, frame.frame(), deadline, order).await {
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
        });
        // Keep the bounded handler slot while the writer catches up; a full queue is not a lost reply.
        let _ = timeout_at(
            deadline,
            reply.send(Reply {
                frame: response,
                deadline,
                notification: notification_peer.map(|peer| (peer, frame)),
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
            let submitted = tokio::select! {
                submitted = dispatcher.submit(peer.clone(), execution, frame, replies.clone(), order) => submitted,
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
            if sender.send(&reply.frame, reply.deadline).await.is_err() {
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
