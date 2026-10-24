//! Bounded request multiplexing over independently driven reusable lanes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use prost::Message;
use snafu::Snafu;
use tidemq_protocol::{
    Id,
    common::{Frame, FrameKind, TrafficClass},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

use crate::{Connection, FrameReceiver, FrameSender, FramedStream, ReceivedFrame, Session};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid RPC configuration: {reason}"))]
    Configuration { reason: &'static str },
    #[snafu(display("RPC admission failed for {resource}"))]
    Admission { resource: &'static str },
    #[snafu(display("RPC client is closed; the request outcome may be unknown"))]
    Closed,
    #[snafu(display("RPC deadline exceeded; may have been sent: {may_have_been_sent}"))]
    Deadline { may_have_been_sent: bool },
    #[snafu(display("Duplicate in-flight RPC request identity"))]
    DuplicateRequest,
    #[snafu(display("Invalid RPC request identity"))]
    Protocol { source: tidemq_protocol::Error },
    #[snafu(display("RPC lane setup failed"))]
    Transport { source: crate::Error },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct RpcConfig {
    pub max_pending_requests: usize,
    pub max_pending_bytes: u32,
    /// Assembly limit after the first frame byte; native connection timers govern idle lanes.
    pub read_idle_timeout: Duration,
    pub liveness_timeout: Duration,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            max_pending_requests: 256,
            max_pending_bytes: 16 * 1024 * 1024,
            read_idle_timeout: Duration::from_secs(30),
            liveness_timeout: Duration::from_secs(1),
        }
    }
}

impl RpcConfig {
    pub fn validate(&self) -> Result<()> {
        for (valid, reason) in [
            (
                (1..=65_536).contains(&self.max_pending_requests),
                "max_pending_requests must be between 1 and 65536",
            ),
            (
                self.max_pending_bytes > 0,
                "max_pending_bytes must be greater than zero",
            ),
            (
                usize::try_from(self.max_pending_bytes)
                    .ok()
                    .is_some_and(|bytes| bytes <= Semaphore::MAX_PERMITS),
                "max_pending_bytes must fit the platform's semaphore capacity",
            ),
            (
                !self.read_idle_timeout.is_zero()
                    && self.read_idle_timeout <= Duration::from_secs(300),
                "read_idle_timeout must be greater than zero and at most 300 seconds",
            ),
            (
                (Duration::from_millis(100)..=Duration::from_secs(10))
                    .contains(&self.liveness_timeout),
                "liveness_timeout must be between 100 milliseconds and 10 seconds",
            ),
        ] {
            if !valid {
                return Err(Error::Configuration { reason });
            }
        }
        Ok(())
    }
}

struct Pending {
    result: oneshot::Sender<ReceivedFrame>,
    operation: Id,
    _slots: Arc<OwnedSemaphorePermit>,
    _bytes: Arc<OwnedSemaphorePermit>,
}

struct Outbound {
    frame: Frame,
    deadline: Instant,
    _slots: Arc<OwnedSemaphorePermit>,
    _bytes: Arc<OwnedSemaphorePermit>,
}

#[derive(Clone, Copy)]
enum Priority {
    Request,
    Core,
    Completion,
}

struct Capacity {
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_bytes: u32,
}

struct State {
    pending: Mutex<HashMap<Id, Pending>>,
    closed: AtomicBool,
    draining: AtomicBool,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    connection: Option<Connection>,
    completion: Option<Capacity>,
    core: Option<Capacity>,
}

impl State {
    fn available_requests(&self) -> usize {
        self.slots.available_permits()
            + [&self.core, &self.completion]
                .into_iter()
                .flatten()
                .map(|limit| limit.slots.available_permits())
                .sum::<usize>()
    }
    fn available_bytes(&self) -> usize {
        self.bytes.available_permits()
            + [&self.core, &self.completion]
                .into_iter()
                .flatten()
                .map(|limit| limit.bytes.available_permits())
                .sum::<usize>()
    }
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.slots.close();
        self.bytes.close();
        for capacity in [&self.completion, &self.core].into_iter().flatten() {
            capacity.slots.close();
            capacity.bytes.close();
        }
        if let Some(connection) = &self.connection {
            connection.close();
        }
        if let Ok(mut pending) = self.pending.lock() {
            pending.clear();
        }
    }
}

struct Inner {
    state: Arc<State>,
    writers: Vec<mpsc::Sender<Outbound>>,
    core_writers: Vec<mpsc::Sender<Outbound>>,
    request_writer: Mutex<Option<mpsc::Sender<Outbound>>>,
    request_open: tokio::sync::Mutex<()>,
    protocol: tidemq_protocol::ProtocolLimits,
    next_lane: AtomicUsize,
    ack_writer: Mutex<Option<mpsc::Sender<Outbound>>>,
    ack_open: tokio::sync::Mutex<()>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    _session: Option<Arc<Session>>,
    max_pending_requests: usize,
    max_pending_bytes: u32,
    read_idle_timeout: Duration,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.state.close();
        if let Ok(tasks) = self.tasks.get_mut() {
            for task in tasks {
                task.abort();
            }
        }
    }
}

/// Clone handles share queues, identities and limits; cloning opens no connection.
#[derive(Clone)]
pub struct RpcClient {
    inner: Arc<Inner>,
}

#[derive(Clone, Debug)]
pub struct RpcSnapshot {
    pub lanes: usize,
    pub admitted_requests: usize,
    pub admitted_bytes: usize,
    pub pending_responses: Option<usize>,
    pub closed: bool,
}

impl RpcClient {
    pub fn start(streams: Vec<FramedStream>, config: RpcConfig) -> Result<Self> {
        Self::start_inner(streams, config, None)
    }

    pub async fn from_session(session: Arc<Session>, config: RpcConfig) -> Result<Self> {
        let streams = session.take_initial_streams().await;
        Self::start_inner(streams, config, Some(session))
    }

    /// All control cores are driven; other classes get a fixed reusable lane set.
    pub async fn from_session_with_lanes(
        session: Arc<Session>,
        config: RpcConfig,
        request_lanes: usize,
        deadline: Instant,
    ) -> Result<Self> {
        if request_lanes == 0 || request_lanes > 64 {
            return Err(Error::Configuration {
                reason: "request_lanes must be between 1 and 64",
            });
        }
        config.validate()?;
        let mut streams = session.take_initial_streams().await;
        if session.connection().class() != tidemq_protocol::common::TrafficClass::Control {
            while streams.len() < request_lanes {
                match session
                    .connection()
                    .open(crate::StreamPurpose::Request, deadline)
                    .await
                {
                    Ok(stream) => streams.push(stream),
                    Err(source) => {
                        session.close();
                        return Err(Error::Transport { source });
                    }
                }
            }
        }
        Self::start_inner(streams, config, Some(session))
    }

    fn start_inner(
        streams: Vec<FramedStream>,
        config: RpcConfig,
        session: Option<Arc<Session>>,
    ) -> Result<Self> {
        config.validate()?;
        if streams.is_empty() || streams.len() > 64 {
            return Err(Error::Configuration {
                reason: "streams must contain between 1 and 64 RPC lanes",
            });
        }
        let completion = control_capacity(&streams, session.as_deref(), &config)?;
        let core = control_capacity(&streams, session.as_deref(), &config)?;
        let reserved_requests = completion
            .as_ref()
            .map_or(0, |limit| limit.slots.available_permits())
            .saturating_add(
                core.as_ref()
                    .map_or(0, |limit| limit.slots.available_permits()),
            );
        let reserved_bytes = completion
            .as_ref()
            .map_or(0, |limit| limit.bytes.available_permits())
            .saturating_add(
                core.as_ref()
                    .map_or(0, |limit| limit.bytes.available_permits()),
            );
        let requests = config
            .max_pending_requests
            .saturating_sub(reserved_requests);
        let bytes = (config.max_pending_bytes as usize).saturating_sub(reserved_bytes);
        let protocol = streams
            .first()
            .ok_or(Error::Closed)?
            .protocol_limits()
            .clone();
        let state = Arc::new(State {
            pending: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            draining: AtomicBool::new(false),
            slots: Arc::new(Semaphore::new(requests)),
            bytes: Arc::new(Semaphore::new(bytes)),
            connection: session.as_ref().map(|session| session.connection()),
            completion,
            core,
        });
        let mut writers = Vec::new();
        let mut core_writers = Vec::new();
        let mut tasks = Vec::new();
        let mut ack_writer = None;
        if session.is_some() {
            tasks.push(tokio::spawn(watch_liveness(
                Arc::downgrade(&state),
                config.liveness_timeout,
            )));
        }
        for stream in streams {
            let purpose = stream.purpose();
            let (sender, receiver) = stream.split();
            let (tx, rx) = mpsc::channel(config.max_pending_requests);
            match purpose {
                crate::StreamPurpose::Ack => ack_writer = Some(tx),
                crate::StreamPurpose::Core => core_writers.push(tx),
                _ => writers.push(tx),
            }
            tasks.push(tokio::spawn(write_loop(sender, rx, Arc::downgrade(&state))));
            tasks.push(tokio::spawn(read_loop(
                receiver,
                Arc::downgrade(&state),
                config.read_idle_timeout,
            )));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                state,
                writers,
                core_writers,
                protocol,
                request_writer: Mutex::new(None),
                request_open: tokio::sync::Mutex::new(()),
                next_lane: AtomicUsize::new(0),
                ack_writer: Mutex::new(ack_writer),
                ack_open: tokio::sync::Mutex::new(()),
                tasks: Mutex::new(tasks),
                _session: session,
                max_pending_requests: config.max_pending_requests,
                max_pending_bytes: config.max_pending_bytes,
                read_idle_timeout: config.read_idle_timeout,
            }),
        })
    }

    pub fn is_closed(&self) -> bool {
        self.inner.state.closed.load(Ordering::Acquire)
    }

    pub fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn lane_count(&self) -> usize {
        self.inner.writers.len()
            + self.inner.core_writers.len()
            + usize::from(
                self.inner
                    .ack_writer
                    .lock()
                    .is_ok_and(|writer| writer.is_some()),
            )
            + usize::from(
                self.inner
                    .request_writer
                    .lock()
                    .is_ok_and(|writer| writer.is_some()),
            )
    }

    pub fn snapshot(&self) -> RpcSnapshot {
        RpcSnapshot {
            lanes: self.lane_count(),
            admitted_requests: self
                .inner
                .max_pending_requests
                .saturating_sub(self.inner.state.available_requests()),
            admitted_bytes: (self.inner.max_pending_bytes as usize)
                .saturating_sub(self.inner.state.available_bytes()),
            pending_responses: self
                .inner
                .state
                .pending
                .lock()
                .ok()
                .map(|pending| pending.len()),
            closed: self.is_closed(),
        }
    }

    /// A caller deadline removes its waiter, never resets a lane shared with other requests.
    pub async fn call(&self, frame: Frame, deadline: Instant) -> Result<ReceivedFrame> {
        self.submit(frame, deadline).await?.receive().await
    }

    pub async fn call_affinity(
        &self,
        frame: Frame,
        deadline: Instant,
        affinity: u64,
    ) -> Result<ReceivedFrame> {
        self.submit_affinity(frame, deadline, affinity)
            .await?
            .receive()
            .await
    }

    /// Ordered enqueue is separate from waiting for the durable application response.
    pub async fn submit(&self, frame: Frame, deadline: Instant) -> Result<RpcResponse> {
        let priority = self.priority(&frame)?;
        self.submit_inner(frame, deadline, None, priority).await
    }

    pub async fn submit_affinity(
        &self,
        frame: Frame,
        deadline: Instant,
        affinity: u64,
    ) -> Result<RpcResponse> {
        let priority = self.priority(&frame)?;
        self.submit_inner(frame, deadline, Some(affinity), priority)
            .await
    }

    /// Trusted semantic forwarders use this only for validated completion envelopes.
    pub async fn submit_completion(&self, frame: Frame, deadline: Instant) -> Result<RpcResponse> {
        let kind = FrameKind::try_from(frame.kind).map_err(|_| Error::Configuration {
            reason: "unknown completion frame kind",
        })?;
        if !tidemq_protocol::framing::is_completion(kind) && kind != FrameKind::InternalCommand {
            return Err(Error::Configuration {
                reason: "completion lane requires a completion or internal envelope",
            });
        }
        self.submit_inner(frame, deadline, None, Priority::Completion)
            .await
    }

    /// Trusted forwarders select this for validated authority or membership maintenance envelopes.
    pub async fn submit_core(&self, frame: Frame, deadline: Instant) -> Result<RpcResponse> {
        if frame.kind != i32::from(FrameKind::InternalCommand)
            && !tidemq_protocol::framing::is_core_control(&frame, &self.inner.protocol)
                .map_err(|source| Error::Protocol { source })?
        {
            return Err(Error::Configuration {
                reason: "core lane requires a control query or internal envelope",
            });
        }
        self.submit_inner(frame, deadline, None, Priority::Core)
            .await
    }

    fn priority(&self, frame: &Frame) -> Result<Priority> {
        if frame.traffic_class != i32::from(TrafficClass::Control) {
            return Ok(Priority::Request);
        }
        if completion_frame(frame) {
            return Ok(Priority::Completion);
        }
        if tidemq_protocol::framing::is_core_control(frame, &self.inner.protocol)
            .map_err(|source| Error::Protocol { source })?
        {
            return Ok(Priority::Core);
        }
        Ok(Priority::Request)
    }

    async fn submit_inner(
        &self,
        frame: Frame,
        deadline: Instant,
        affinity: Option<u64>,
        priority: Priority,
    ) -> Result<RpcResponse> {
        let id = Id::from_slice(&frame.request_id).map_err(|source| Error::Protocol { source })?;
        let operation =
            Id::from_slice(&frame.operation_id).map_err(|source| Error::Protocol { source })?;
        let (slots, bytes) = self.reserve(&frame, priority, deadline).await?;
        let (result, receiver) = oneshot::channel();
        {
            let mut pending = self.inner.state.pending.lock().map_err(|_| Error::Closed)?;
            if self.is_closed() || self.inner.state.draining.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            if pending.contains_key(&id) {
                return Err(Error::DuplicateRequest);
            }
            pending.insert(
                id,
                Pending {
                    result,
                    operation,
                    _slots: slots.clone(),
                    _bytes: bytes.clone(),
                },
            );
        }
        let waiter = Waiter {
            state: self.inner.state.clone(),
            id,
            ticket: slots.clone(),
        };
        self.enqueue(
            Outbound {
                frame,
                deadline,
                _slots: slots,
                _bytes: bytes,
            },
            deadline,
            affinity,
            priority,
        )
        .await?;
        Ok(RpcResponse {
            receiver,
            deadline,
            _waiter: waiter,
        })
    }

    /// Sends a protocol notification without creating a response waiter.
    pub async fn notify(&self, frame: Frame, deadline: Instant) -> Result<()> {
        let priority = self.priority(&frame)?;
        let (slots, bytes) = self.reserve(&frame, priority, deadline).await?;
        self.enqueue(
            Outbound {
                frame,
                deadline,
                _slots: slots,
                _bytes: bytes,
            },
            deadline,
            None,
            priority,
        )
        .await
    }

    async fn reserve(
        &self,
        frame: &Frame,
        priority: Priority,
        deadline: Instant,
    ) -> Result<(Arc<OwnedSemaphorePermit>, Arc<OwnedSemaphorePermit>)> {
        if self.is_closed() || self.inner.state.draining.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let length = u32::try_from(frame.encoded_len()).map_err(|_| Error::Admission {
            resource: "encoded RPC frame length exceeds u32",
        })?;
        let (slots, bytes) = match priority {
            Priority::Request => (&self.inner.state.slots, &self.inner.state.bytes),
            Priority::Core | Priority::Completion => {
                let capacity = match priority {
                    Priority::Core => &self.inner.state.core,
                    _ => &self.inner.state.completion,
                };
                let capacity = capacity.as_ref().ok_or(Error::Admission {
                    resource: "RPC priority capacity",
                })?;
                if length > capacity.max_bytes {
                    return Err(Error::Admission {
                        resource: "max_pending_bytes",
                    });
                }
                // Maintenance must wait for its own bounded quota instead of failing a consumer on a transient collision.
                let slots = timeout_at(deadline, capacity.slots.clone().acquire_owned())
                    .await
                    .map_err(|_| Error::Deadline {
                        may_have_been_sent: false,
                    })?
                    .map_err(|_| Error::Closed)?;
                let bytes = timeout_at(deadline, capacity.bytes.clone().acquire_many_owned(length))
                    .await
                    .map_err(|_| Error::Deadline {
                        may_have_been_sent: false,
                    })?
                    .map_err(|_| Error::Closed)?;
                return Ok((Arc::new(slots), Arc::new(bytes)));
            }
        };
        let slots = slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Admission {
                resource: "max_pending_requests",
            })?;
        let bytes = bytes
            .clone()
            .try_acquire_many_owned(length)
            .map_err(|_| Error::Admission {
                resource: "max_pending_bytes",
            })?;
        Ok((Arc::new(slots), Arc::new(bytes)))
    }

    async fn enqueue(
        &self,
        outbound: Outbound,
        deadline: Instant,
        affinity: Option<u64>,
        priority: Priority,
    ) -> Result<()> {
        let writer = match priority {
            Priority::Completion => {
                self.special_writer(crate::StreamPurpose::Ack, deadline)
                    .await?
            }
            Priority::Request if self.inner.writers.is_empty() => {
                self.special_writer(crate::StreamPurpose::Request, deadline)
                    .await?
            }
            priority => {
                let writers = if matches!(priority, Priority::Core) {
                    &self.inner.core_writers
                } else {
                    &self.inner.writers
                };
                let count = writers.len();
                if count == 0 {
                    return Err(Error::Configuration {
                        reason: "RPC has no matching stream lane",
                    });
                }
                let lane = match affinity {
                    Some(affinity) => usize::try_from(affinity % count as u64).map_err(|_| {
                        Error::Configuration {
                            reason: "RPC affinity exceeds the platform address space",
                        }
                    })?,
                    None => self.inner.next_lane.fetch_add(1, Ordering::Relaxed) % count,
                };
                writers.get(lane).cloned().ok_or(Error::Closed)?
            }
        };
        timeout_at(deadline, writer.send(outbound))
            .await
            .map_err(|_| Error::Deadline {
                may_have_been_sent: false,
            })?
            .map_err(|_| Error::Closed)
    }

    async fn special_writer(
        &self,
        purpose: crate::StreamPurpose,
        deadline: Instant,
    ) -> Result<mpsc::Sender<Outbound>> {
        let (cached, opening) = if purpose == crate::StreamPurpose::Ack {
            (&self.inner.ack_writer, &self.inner.ack_open)
        } else {
            (&self.inner.request_writer, &self.inner.request_open)
        };
        if let Some(writer) = cached.lock().map_err(|_| Error::Closed)?.clone() {
            return Ok(writer);
        }
        let _opening = timeout_at(deadline, opening.lock())
            .await
            .map_err(|_| Error::Deadline {
                may_have_been_sent: false,
            })?;
        if let Some(writer) = cached.lock().map_err(|_| Error::Closed)?.clone() {
            return Ok(writer);
        }
        let session = self.inner._session.as_ref().ok_or(Error::Configuration {
            reason: "RPC without a session requires supplied lanes for each traffic priority",
        })?;
        let stream = session
            .connection()
            .open(purpose, deadline)
            .await
            .map_err(|source| Error::Transport { source })?;
        // Pair task registration with the cached lane before shutdown can take the task list.
        let mut writer = cached.lock().map_err(|_| Error::Closed)?;
        let mut tasks = self.inner.tasks.lock().map_err(|_| Error::Closed)?;
        if self.is_closed() || self.inner.state.draining.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let (sender, receiver) = stream.split();
        let (tx, rx) = mpsc::channel(self.inner.max_pending_requests);
        tasks.push(tokio::spawn(write_loop(
            sender,
            rx,
            Arc::downgrade(&self.inner.state),
        )));
        tasks.push(tokio::spawn(read_loop(
            receiver,
            Arc::downgrade(&self.inner.state),
            self.inner.read_idle_timeout,
        )));
        *writer = Some(tx.clone());
        Ok(tx)
    }

    /// Waits only until the supplied deadline, then releases all tasks and waiters.
    pub async fn close(&self, deadline: Instant) {
        self.inner.state.draining.store(true, Ordering::Release);
        loop {
            let empty = self
                .inner
                .state
                .pending
                .lock()
                .map(|p| p.is_empty())
                .unwrap_or(true);
            if empty || Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep_until(
                deadline.min(
                    Instant::now()
                        .checked_add(Duration::from_millis(2))
                        .unwrap_or(deadline),
                ),
            )
            .await;
        }
        self.shutdown(deadline).await;
    }

    /// Stop owned tasks after the caller has stopped work; unresolved replies remain unknown.
    pub async fn shutdown(&self, deadline: Instant) {
        self.inner.state.draining.store(true, Ordering::Release);
        self.inner.state.close();
        let tasks = self
            .inner
            .tasks
            .lock()
            .map(|mut tasks| std::mem::take(&mut *tasks))
            .unwrap_or_default();
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = timeout_at(deadline, task).await;
        }
        if let Some(session) = &self.inner._session {
            let _ = session.shutdown(deadline).await;
        }
    }
}

fn control_capacity(
    streams: &[FramedStream],
    session: Option<&Session>,
    config: &RpcConfig,
) -> Result<Option<Capacity>> {
    let control = streams
        .iter()
        .any(|stream| stream.purpose() == crate::StreamPurpose::Core)
        || session.is_some_and(|session| session.connection().class() == TrafficClass::Control);
    let supplied_ack = streams
        .iter()
        .filter(|stream| stream.purpose() == crate::StreamPurpose::Ack)
        .count();
    if supplied_ack > 1
        || streams
            .iter()
            .all(|stream| stream.purpose() == crate::StreamPurpose::Ack)
    {
        return Err(Error::Configuration {
            reason: "RPC requires normal lanes and at most one supplied ACK lane",
        });
    }
    if control || supplied_ack > 0 {
        if config.max_pending_requests < 3 || config.max_pending_bytes < 3 {
            return Err(Error::Configuration {
                reason: "control RPC capacity must fit request, core, and completion work",
            });
        }
        Ok(Some(Capacity {
            slots: Arc::new(Semaphore::new((config.max_pending_requests / 4).max(1))),
            bytes: Arc::new(Semaphore::new(
                (config.max_pending_bytes as usize / 4).max(1),
            )),
            max_bytes: (config.max_pending_bytes / 4).max(1),
        }))
    } else {
        Ok(None)
    }
}

fn completion_frame(frame: &Frame) -> bool {
    FrameKind::try_from(frame.kind).is_ok_and(tidemq_protocol::framing::is_completion)
}

pub struct RpcResponse {
    receiver: oneshot::Receiver<ReceivedFrame>,
    deadline: Instant,
    _waiter: Waiter,
}

impl RpcResponse {
    pub async fn receive(self) -> Result<ReceivedFrame> {
        timeout_at(self.deadline, self.receiver)
            .await
            .map_err(|_| Error::Deadline {
                may_have_been_sent: true,
            })?
            .map_err(|_| Error::Closed)
    }
}

struct Waiter {
    state: Arc<State>,
    id: Id,
    ticket: Arc<OwnedSemaphorePermit>,
}

impl Drop for Waiter {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.state.pending.lock()
            && pending
                .get(&self.id)
                .is_some_and(|current| Arc::ptr_eq(&current._slots, &self.ticket))
        {
            pending.remove(&self.id);
        }
    }
}

async fn write_loop(
    mut sender: FrameSender,
    mut queue: mpsc::Receiver<Outbound>,
    state: Weak<State>,
) {
    while let Some(outbound) = queue.recv().await {
        if Instant::now() >= outbound.deadline {
            continue;
        }
        if sender
            .send_owned(outbound.frame, outbound.deadline)
            .await
            .is_err()
        {
            break;
        }
    }
    if let Some(state) = state.upgrade() {
        state.close();
    }
}

async fn read_loop(mut receiver: FrameReceiver, state: Weak<State>, idle: Duration) {
    loop {
        let Ok(Some(response)) = receiver.receive_next(idle).await else {
            break;
        };
        let Ok(id) = Id::from_slice(&response.frame().request_id) else {
            break;
        };
        let Some(state) = state.upgrade() else { break };
        let waiter = state
            .pending
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(&id));
        if let Some(waiter) = waiter {
            if response.frame().operation_id.as_ref() != waiter.operation.as_bytes() {
                state.close();
                break;
            }
            let _ = waiter.result.send(response);
        }
    }
    if let Some(state) = state.upgrade() {
        state.close();
    }
}

async fn watch_liveness(state: Weak<State>, timeout: Duration) {
    let mut ticker = tokio::time::interval(timeout);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let Some(state) = state.upgrade() else {
            return;
        };
        if state.closed.load(Ordering::Acquire) {
            return;
        }
        let Some(connection) = state.connection.as_ref() else {
            return;
        };
        let pending = state
            .pending
            .lock()
            .map_or(true, |pending| !pending.is_empty());
        let native_pending = connection
            .recovery_snapshot()
            .ok()
            .flatten()
            .is_some_and(|snapshot| snapshot.bytes_in_flight > 0);
        if !pending && !connection.has_ack_streams() && !native_pending {
            continue;
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            state.close();
            return;
        };
        if connection.probe(deadline).await.is_err() {
            state.close();
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Client, ClientCredentials, ClientTls, PeerTarget, Server, ServerAuthorization, ServerTls,
        TlsIdentity, TokenGrant, TransportConfig,
        budget::{MemoryBudget, MemoryLimits},
    };
    use bytes::Bytes;
    use rcgen::{CertificateParams, KeyPair};
    use tidemq_protocol::{
        ProtocolLimits,
        common::{FrameKind, TrafficClass},
        framing::new_frame,
        public::PublishResult,
    };

    const TOKEN: &str = "rpc-test-token-with-at-least-32-characters";

    fn budget() -> MemoryBudget {
        MemoryBudget::new(MemoryLimits {
            total_bytes: 64 * 1024 * 1024,
            control_reserved_bytes: 8 * 1024 * 1024,
        })
        .unwrap()
    }

    fn request() -> Frame {
        new_frame(
            FrameKind::PublishResult,
            TrafficClass::Produce,
            Id::new_v7().unwrap(),
            Id::new_v7().unwrap(),
            &PublishResult {
                offset: Some(42),
                ..Default::default()
            },
            2000,
            &ProtocolLimits::default(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn dropping_an_old_waiter_cannot_remove_a_new_waiter_with_the_same_id() {
        let state = Arc::new(State {
            pending: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            draining: AtomicBool::new(false),
            slots: Arc::new(Semaphore::new(2)),
            bytes: Arc::new(Semaphore::new(100)),
            connection: None,
            completion: None,
            core: None,
        });
        let id = Id::new_v7().unwrap();
        let old_ticket = Arc::new(state.slots.clone().try_acquire_owned().unwrap());
        let old = Waiter {
            state: state.clone(),
            id,
            ticket: old_ticket,
        };
        let new_ticket = Arc::new(state.slots.clone().try_acquire_owned().unwrap());
        state.pending.lock().unwrap().insert(
            id,
            Pending {
                result: oneshot::channel().0,
                operation: id,
                _slots: new_ticket,
                _bytes: Arc::new(state.bytes.clone().try_acquire_owned().unwrap()),
            },
        );
        drop(old);
        assert!(state.pending.lock().unwrap().contains_key(&id));
    }

    #[tokio::test]
    async fn pipelines_requests_and_correlates_reversed_responses() {
        let key = KeyPair::generate().unwrap();
        let cert = CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let mut server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            ServerTls {
                identity: TlsIdentity::new(
                    Bytes::from(cert.pem()),
                    Bytes::from(key.serialize_pem()),
                )
                .unwrap(),
                client_trust_roots: None,
            },
            ServerAuthorization::External(vec![
                TokenGrant::new(
                    TOKEN,
                    "test".into(),
                    "test".into(),
                    vec![FrameKind::PublishResult],
                )
                .unwrap(),
            ]),
            TransportConfig::default(),
            budget(),
        )
        .unwrap();
        let client = Client::bind(
            "127.0.0.1:0".parse().unwrap(),
            ClientTls {
                trust_roots: Bytes::from(cert.pem()),
                identity: None,
                expected_nodes: vec![],
            },
            TransportConfig::default(),
            budget(),
        )
        .unwrap();
        let target = PeerTarget {
            address: server.local_addr(),
            server_name: "localhost".into(),
            local_role: "sdk".into(),
            remote_role: "gateway".into(),
            cluster_id: None,
            node_id: None,
        };
        let task = tokio::spawn(async move {
            let session = server.accept().await.unwrap();
            let stream = session.take_initial_streams().await.pop().unwrap();
            let (mut sender, mut receiver) = stream.split();
            let first = receiver
                .receive(Instant::now() + Duration::from_secs(3))
                .await
                .unwrap()
                .unwrap();
            let second = receiver
                .receive(Instant::now() + Duration::from_secs(3))
                .await
                .unwrap()
                .unwrap();
            sender
                .send(second.frame(), Instant::now() + Duration::from_secs(3))
                .await
                .unwrap();
            sender
                .send(first.frame(), Instant::now() + Duration::from_secs(3))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            session.close();
            server.shutdown().await.unwrap();
        });
        let session = client
            .connect(
                target,
                &ClientCredentials::External {
                    session_id: Id::new_v7().unwrap(),
                    access_token: TOKEN.into(),
                },
                TrafficClass::Produce,
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap();
        let rpc = RpcClient::from_session(session, RpcConfig::default())
            .await
            .unwrap();
        let first = request();
        let mut second = request();
        let mut large_payload = second.payload.to_vec();
        // Unknown field 100 carries padding while preserving a valid PublishResult payload.
        large_payload.extend_from_slice(&[0xa2, 0x06]);
        prost::encode_length_delimiter(64 * 1024, &mut large_payload).unwrap();
        large_payload.extend_from_slice(&vec![7; 64 * 1024]);
        second.payload = Bytes::from(large_payload);
        let second_digest = blake3::hash(&second.payload);
        let first_id = first.request_id.clone();
        let second_id = second.request_id.clone();
        let (a, b) = tokio::join!(
            rpc.call(first, Instant::now() + Duration::from_secs(2)),
            rpc.call(second, Instant::now() + Duration::from_secs(2))
        );
        assert_eq!(a.unwrap().frame().request_id, first_id);
        let second_response = b.unwrap();
        assert_eq!(second_response.frame().request_id, second_id);
        assert_eq!(
            blake3::hash(&second_response.frame().payload),
            second_digest
        );
        assert!(!rpc.is_closed());
        rpc.close(Instant::now() + Duration::from_millis(10)).await;
        task.await.unwrap();
    }
}
