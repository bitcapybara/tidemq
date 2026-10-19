//! Bounded request multiplexing over independently driven reusable lanes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use prost::Message;
use snafu::Snafu;
use tidemq_protocol::{Id, common::Frame};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

use crate::{Connection, FrameReceiver, FrameSender, FramedStream, ReceivedFrame, Session};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid RPC limits"))]
    Configuration,
    #[snafu(display("RPC admission budget exhausted"))]
    Admission,
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
    pub read_idle_timeout: Duration,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            max_pending_requests: 256,
            max_pending_bytes: 16 * 1024 * 1024,
            read_idle_timeout: Duration::from_secs(30),
        }
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

struct State {
    pending: Mutex<HashMap<Id, Pending>>,
    closed: AtomicBool,
    draining: AtomicBool,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    connection: Option<Connection>,
}

impl State {
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.slots.close();
        self.bytes.close();
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
    next_lane: AtomicUsize,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    _session: Option<Arc<Session>>,
    max_pending_requests: usize,
    max_pending_bytes: u32,
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
            return Err(Error::Configuration);
        }
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
        if streams.is_empty()
            || streams.len() > 64
            || config.max_pending_requests == 0
            || config.max_pending_requests > 65_536
            || config.max_pending_bytes == 0
            || usize::try_from(config.max_pending_bytes)
                .ok()
                .is_none_or(|bytes| bytes > Semaphore::MAX_PERMITS)
            || config.read_idle_timeout.is_zero()
            || config.read_idle_timeout > Duration::from_secs(300)
        {
            return Err(Error::Configuration);
        }
        let state = Arc::new(State {
            pending: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            draining: AtomicBool::new(false),
            slots: Arc::new(Semaphore::new(config.max_pending_requests)),
            bytes: Arc::new(Semaphore::new(config.max_pending_bytes as usize)),
            connection: session.as_ref().map(|session| session.connection()),
        });
        let mut writers = Vec::new();
        let mut tasks = Vec::new();
        for stream in streams {
            let (sender, receiver) = stream.split();
            let (tx, rx) = mpsc::channel(config.max_pending_requests);
            writers.push(tx);
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
                next_lane: AtomicUsize::new(0),
                tasks: Mutex::new(tasks),
                _session: session,
                max_pending_requests: config.max_pending_requests,
                max_pending_bytes: config.max_pending_bytes,
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
    }

    pub fn snapshot(&self) -> RpcSnapshot {
        RpcSnapshot {
            lanes: self.lane_count(),
            admitted_requests: self
                .inner
                .max_pending_requests
                .saturating_sub(self.inner.state.slots.available_permits()),
            admitted_bytes: (self.inner.max_pending_bytes as usize)
                .saturating_sub(self.inner.state.bytes.available_permits()),
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
        self.submit_inner(frame, deadline, None).await
    }

    pub async fn submit_affinity(
        &self,
        frame: Frame,
        deadline: Instant,
        affinity: u64,
    ) -> Result<RpcResponse> {
        self.submit_inner(frame, deadline, Some(affinity)).await
    }

    async fn submit_inner(
        &self,
        frame: Frame,
        deadline: Instant,
        affinity: Option<u64>,
    ) -> Result<RpcResponse> {
        let id = Id::from_slice(&frame.request_id).map_err(|source| Error::Protocol { source })?;
        let operation =
            Id::from_slice(&frame.operation_id).map_err(|source| Error::Protocol { source })?;
        let (slots, bytes) = self.reserve(&frame)?;
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
        let (slots, bytes) = self.reserve(&frame)?;
        self.enqueue(
            Outbound {
                frame,
                deadline,
                _slots: slots,
                _bytes: bytes,
            },
            deadline,
            None,
        )
        .await
    }

    fn reserve(
        &self,
        frame: &Frame,
    ) -> Result<(Arc<OwnedSemaphorePermit>, Arc<OwnedSemaphorePermit>)> {
        if self.is_closed() || self.inner.state.draining.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let length = u32::try_from(frame.encoded_len()).map_err(|_| Error::Admission)?;
        let slots = self
            .inner
            .state
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Admission)?;
        let bytes = self
            .inner
            .state
            .bytes
            .clone()
            .try_acquire_many_owned(length)
            .map_err(|_| Error::Admission)?;
        Ok((Arc::new(slots), Arc::new(bytes)))
    }

    async fn enqueue(
        &self,
        outbound: Outbound,
        deadline: Instant,
        affinity: Option<u64>,
    ) -> Result<()> {
        let lane = match affinity {
            Some(affinity) => usize::try_from(affinity % self.inner.writers.len() as u64)
                .map_err(|_| Error::Configuration)?,
            None => self.inner.next_lane.fetch_add(1, Ordering::Relaxed) % self.inner.writers.len(),
        };
        let writer = self.inner.writers.get(lane).ok_or(Error::Closed)?;
        timeout_at(deadline, writer.send(outbound))
            .await
            .map_err(|_| Error::Deadline {
                may_have_been_sent: false,
            })?
            .map_err(|_| Error::Closed)
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
            .send(&outbound.frame, outbound.deadline)
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
    while let Some(deadline) = Instant::now().checked_add(idle) {
        let Ok(Some(response)) = receiver.receive(deadline).await else {
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
        public::ProduceResult,
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
            FrameKind::ProduceResult,
            TrafficClass::Produce,
            Id::random().unwrap(),
            Id::random().unwrap(),
            &ProduceResult {
                first_offset: 42,
                message_count: 1,
                duplicate: false,
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
        });
        let id = Id::random().unwrap();
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
                    vec![FrameKind::ProduceResult],
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
            server.shutdown().await;
        });
        let session = client
            .connect(
                target,
                &ClientCredentials::External {
                    session_id: Id::random().unwrap(),
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
        let second = request();
        let first_id = first.request_id.clone();
        let second_id = second.request_id.clone();
        let (a, b) = tokio::join!(
            rpc.call(first, Instant::now() + Duration::from_secs(2)),
            rpc.call(second, Instant::now() + Duration::from_secs(2))
        );
        assert_eq!(a.unwrap().frame().request_id, first_id);
        assert_eq!(b.unwrap().frame().request_id, second_id);
        assert!(!rpc.is_closed());
        rpc.close(Instant::now() + Duration::from_millis(10)).await;
        task.await.unwrap();
    }
}
