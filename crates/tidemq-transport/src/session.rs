use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use s2n_quic::{
    connection::{Handle, StreamAcceptor},
    stream::BidirectionalStream,
};
use tidemq_protocol::{
    Id,
    common::{Frame, FrameKind, TrafficClass},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
    time::{Instant, timeout_at},
};

use crate::budget::{MemoryBudget, MemoryPermit};

use super::{
    Authorization, Error, FramedStream, Result, TransportConfig, TransportMetrics, telemetry::add,
};

const PRELUDE_MAGIC: u8 = 0x71;
const READY_MAGIC: u8 = 0xa1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamPurpose {
    Core,
    Request,
    Ack,
}

impl StreamPurpose {
    fn wire(self) -> u8 {
        match self {
            Self::Core => 1,
            Self::Request => 2,
            Self::Ack => 3,
        }
    }
    fn from_wire(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Core),
            2 => Ok(Self::Request),
            3 => Ok(Self::Ack),
            _ => Err(Error::Prelude),
        }
    }
}

pub(super) struct ConnectionState {
    pub handle: Handle,
    pub class: TrafficClass,
    pub authorization: Authorization,
    pub session_id: Id,
    pub config: Arc<TransportConfig>,
    pub budget: MemoryBudget,
    pub metrics: Arc<TransportMetrics>,
    core: Arc<Semaphore>,
    ordinary: Arc<Semaphore>,
    ack: Arc<Semaphore>,
    core_live: AtomicUsize,
    closed: AtomicBool,
    _memory: MemoryPermit,
    _endpoint_slot: Option<OwnedSemaphorePermit>,
}

pub(super) struct StreamLease {
    pub state: Arc<ConnectionState>,
    pub purpose: StreamPurpose,
    _slot: OwnedSemaphorePermit,
    core_live: AtomicBool,
}

impl StreamLease {
    pub fn invalidate(&self) {
        if self.purpose == StreamPurpose::Core && self.core_live.swap(false, Ordering::AcqRel) {
            let _ = self
                .state
                .core_live
                .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_sub(1)
                });
        }
    }
    pub fn validate_kind(&self, frame: &Frame) -> Result<()> {
        let kind = FrameKind::try_from(frame.kind).map_err(|_| Error::Prelude)?;
        let ack = tidemq_protocol::framing::is_completion(kind);
        let internal = matches!(kind, FrameKind::InternalCommand | FrameKind::InternalResult);
        let common = matches!(kind, FrameKind::Error | FrameKind::Cancel);
        let core = self.purpose == StreamPurpose::Core
            && (internal
                || tidemq_protocol::framing::is_core_control(frame, &self.state.config.protocol)
                    .map_err(|source| Error::Protocol { source })?);
        if (self.purpose == StreamPurpose::Core && !core && !common)
            || (self.purpose == StreamPurpose::Ack && !ack && !common && !internal)
        {
            return Err(Error::Authorization {
                reason: "frame does not match the stream purpose",
            });
        }
        Ok(())
    }
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        self.invalidate();
    }
}

impl Drop for ConnectionState {
    fn drop(&mut self) {
        self.handle.close(0_u32.into());
        let _ = self.metrics.active_connections.try_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |count| count.checked_sub(1),
        );
    }
}

#[derive(Clone)]
pub struct Connection {
    pub(super) state: Arc<ConnectionState>,
}

impl Connection {
    pub fn class(&self) -> TrafficClass {
        self.state.class
    }
    pub fn authorization(&self) -> &Authorization {
        &self.state.authorization
    }
    pub fn session_id(&self) -> Id {
        self.state.session_id
    }
    pub fn physical_id(&self) -> u64 {
        self.state.handle.id()
    }
    pub fn recovery_snapshot(&self) -> Result<Option<super::RecoverySnapshot>> {
        super::endpoint::recovery_snapshot(&self.state.handle)
    }
    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }
    pub(super) fn has_ack_streams(&self) -> bool {
        self.state.ack.available_permits() < self.state.config.max_ack_streams
    }
    /// Proves new authenticated transport progress, independently of application response latency.
    pub async fn probe(&self, deadline: Instant) -> Result<()> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        add(&self.state.metrics.liveness_probes, 1);
        let result = super::endpoint::probe_connection(&self.state.handle, deadline).await;
        if result.is_err() {
            add(&self.state.metrics.liveness_failures, 1);
        }
        result
    }
    pub fn core_streams_ready(&self) -> bool {
        self.state.core_live.load(Ordering::Acquire) >= self.state.config.core_streams
    }

    pub fn close(&self) {
        self.state.closed.store(true, Ordering::Release);
        self.state.handle.close(0_u32.into());
    }

    pub async fn open(&self, purpose: StreamPurpose, deadline: Instant) -> Result<FramedStream> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let lease = self.state.admit(purpose)?;
        let mut handle = self.state.handle.clone();
        let result =
            timeout_at(deadline, async {
                let mut stream = handle.open_bidirectional_stream().await.map_err(|source| {
                    Error::Connection {
                        operation: "open stream",
                        source,
                    }
                })?;
                let opened = exchange_prelude(&mut stream, purpose, true).await;
                if opened.is_err() {
                    let _ = stream.reset(0x102_u32.into());
                }
                opened?;
                Ok(FramedStream::new(stream, lease))
            })
            .await;
        match result {
            Ok(result) => result,
            Err(_) => {
                add(&self.state.metrics.deadlines, 1);
                Err(Error::Deadline {
                    operation: "open stream",
                })
            }
        }
    }
}

pub struct Session {
    connection: Connection,
    initial: Mutex<Vec<FramedStream>>,
    incoming: Mutex<IncomingStreams>,
    pump: StdMutex<Option<JoinHandle<()>>>,
}

struct IncomingStreams {
    core: mpsc::Receiver<FramedStream>,
    ordinary: mpsc::Receiver<FramedStream>,
}

impl Session {
    pub fn connection(&self) -> Connection {
        self.connection.clone()
    }
    pub(super) async fn add_initial(&self, stream: FramedStream) {
        self.initial.lock().await.push(stream);
    }
    /// A single application dispatcher owns the pre-established lanes.
    pub async fn take_initial_streams(&self) -> Vec<FramedStream> {
        std::mem::take(&mut *self.initial.lock().await)
    }

    /// Accept work independently of stream readers and business execution.
    pub async fn accept(&self) -> Option<FramedStream> {
        let mut incoming = self.incoming.lock().await;
        let IncomingStreams { core, ordinary } = &mut *incoming;
        tokio::select! { biased; stream = core.recv(), if !core.is_closed() || !core.is_empty() => stream, stream = ordinary.recv() => stream }
    }

    pub fn close(&self) {
        self.connection.close();
        if let Ok(pump) = self.pump.lock()
            && let Some(pump) = pump.as_ref()
        {
            pump.abort();
        }
    }

    pub async fn shutdown(&self, deadline: Instant) -> Result<()> {
        self.close();
        let pump = self.pump.lock().map_err(|_| Error::Poisoned)?.take();
        if let Some(pump) = pump {
            match timeout_at(deadline, pump).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => {
                    return Err(Error::Io {
                        operation: "join stream acceptance pump",
                        source: std::io::Error::other(error),
                    });
                }
                Err(_) => {
                    return Err(Error::Deadline {
                        operation: "stop stream acceptance pump",
                    });
                }
            }
        }
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.connection.close();
        if let Ok(pump) = self.pump.get_mut()
            && let Some(pump) = pump.take()
        {
            pump.abort();
        }
    }
}

impl ConnectionState {
    #[allow(clippy::too_many_arguments)] // The binding owns every security and capacity input together.
    pub(super) fn new(
        handle: Handle,
        class: TrafficClass,
        authorization: Authorization,
        session_id: Id,
        config: Arc<TransportConfig>,
        budget: MemoryBudget,
        metrics: Arc<TransportMetrics>,
        memory: MemoryPermit,
        endpoint_slot: Option<OwnedSemaphorePermit>,
    ) -> Arc<Self> {
        add(&metrics.active_connections, 1);
        Arc::new(Self {
            handle,
            class,
            authorization,
            session_id,
            core: Arc::new(Semaphore::new(config.core_streams)),
            ordinary: Arc::new(Semaphore::new(
                ordinary_capacity(class, &config).saturating_sub(
                    if class == TrafficClass::Control {
                        config.max_ack_streams
                    } else {
                        0
                    },
                ),
            )),
            ack: Arc::new(Semaphore::new(config.max_ack_streams)),
            config,
            budget,
            metrics,
            core_live: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            _memory: memory,
            _endpoint_slot: endpoint_slot,
        })
    }

    // Core, ordinary, and ACK leases are acquired together without lending core slots to ACKs.
    pub(super) fn admit(self: &Arc<Self>, purpose: StreamPurpose) -> Result<StreamLease> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if self.class != TrafficClass::Control && purpose != StreamPurpose::Request {
            return Err(Error::Authorization {
                reason: "stream purpose does not match the connection class",
            });
        }
        if purpose == StreamPurpose::Ack
            && self.core_live.load(Ordering::Acquire) < self.config.core_streams
        {
            return Err(Error::Admission {
                resource: "core control lanes must be restored before ACK admission",
            });
        }
        let semaphore = match purpose {
            StreamPurpose::Core => &self.core,
            StreamPurpose::Ack => &self.ack,
            StreamPurpose::Request => &self.ordinary,
        };
        let slot = semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Admission {
                resource: "connection stream slots",
            })?;
        if purpose == StreamPurpose::Core {
            self.core_live.fetch_add(1, Ordering::AcqRel);
        }
        add(&self.metrics.streams, 1);
        Ok(StreamLease {
            state: self.clone(),
            purpose,
            _slot: slot,
            core_live: AtomicBool::new(purpose == StreamPurpose::Core),
        })
    }
}

pub(super) fn start_session(
    state: Arc<ConnectionState>,
    acceptor: StreamAcceptor,
    initial: FramedStream,
) -> Arc<Session> {
    let (core_tx, core_rx) = mpsc::channel(state.config.core_streams);
    let (ordinary_tx, ordinary_rx) = mpsc::channel(ordinary_capacity(state.class, &state.config));
    let pump = tokio::spawn(pump_streams(state.clone(), acceptor, core_tx, ordinary_tx));
    Arc::new(Session {
        connection: Connection {
            state: state.clone(),
        },
        initial: Mutex::new(vec![initial]),
        incoming: Mutex::new(IncomingStreams {
            core: core_rx,
            ordinary: ordinary_rx,
        }),
        pump: StdMutex::new(Some(pump)),
    })
}

async fn pump_streams(
    state: Arc<ConnectionState>,
    mut acceptor: StreamAcceptor,
    core: mpsc::Sender<FramedStream>,
    ordinary: mpsc::Sender<FramedStream>,
) {
    let mut pending = JoinSet::new();
    let expiry = super::auth::credential_expired(&state.authorization);
    tokio::pin!(expiry);
    loop {
        tokio::select! {
            _ = &mut expiry => break,
            _ = core.closed() => break,
            _ = ordinary.closed() => break,
            _ = pending.join_next(), if !pending.is_empty() => {},
            accepted = acceptor.accept_bidirectional_stream(), if pending.len() < state.config.max_streams => {
                match accepted {
                    Ok(Some(stream)) => { pending.spawn(accept_stream(stream, state.clone(), core.clone(), ordinary.clone())); }
                    _ => break,
                }
            }
        }
    }
    state.closed.store(true, Ordering::Release);
    state.handle.close(0_u32.into());
}

async fn accept_stream(
    mut stream: BidirectionalStream,
    state: Arc<ConnectionState>,
    core: mpsc::Sender<FramedStream>,
    ordinary: mpsc::Sender<FramedStream>,
) {
    let Ok(deadline) = super::policy::deadline_after(state.config.stream_prelude_timeout) else {
        let _ = stream.reset(0x102_u32.into());
        return;
    };
    let result = timeout_at(deadline, async {
        let purpose = read_prelude(&mut stream).await?;
        let lease = state.admit(purpose)?;
        stream
            .write_all(&[READY_MAGIC, purpose.wire()])
            .await
            .map_err(|source| Error::Io {
                operation: "send stream readiness",
                source,
            })?;
        Ok::<_, Error>((purpose, lease))
    })
    .await;
    match result {
        Ok(Ok((purpose, lease))) => {
            let framed = FramedStream::new(stream, lease);
            let sender = if purpose == StreamPurpose::Core {
                core
            } else {
                ordinary
            };
            if let Err(error) = sender.try_send(framed) {
                let mut rejected = error.into_inner();
                let _ = rejected.sender.reset();
                add(&state.metrics.rejected, 1);
            }
        }
        _ => {
            let _ = stream.reset(0x102_u32.into());
            let _ = stream.stop_sending(0x102_u32.into());
            add(&state.metrics.rejected, 1);
        }
    }
}

pub(super) async fn exchange_prelude(
    stream: &mut BidirectionalStream,
    purpose: StreamPurpose,
    initiator: bool,
) -> Result<()> {
    if initiator {
        stream
            .write_all(&[PRELUDE_MAGIC, purpose.wire()])
            .await
            .map_err(|source| Error::Io {
                operation: "write stream prelude",
                source,
            })?;
        let mut ready = [0_u8; 2];
        stream
            .read_exact(&mut ready)
            .await
            .map_err(|source| Error::Io {
                operation: "read stream readiness",
                source,
            })?;
        if ready != [READY_MAGIC, purpose.wire()] {
            return Err(Error::Prelude);
        }
    } else {
        if read_prelude(stream).await? != purpose {
            return Err(Error::Prelude);
        }
        stream
            .write_all(&[READY_MAGIC, purpose.wire()])
            .await
            .map_err(|source| Error::Io {
                operation: "write stream readiness",
                source,
            })?;
    }
    Ok(())
}

async fn read_prelude(stream: &mut BidirectionalStream) -> Result<StreamPurpose> {
    let mut prelude = [0_u8; 2];
    stream
        .read_exact(&mut prelude)
        .await
        .map_err(|source| Error::Io {
            operation: "read stream prelude",
            source,
        })?;
    if prelude.first().copied() != Some(PRELUDE_MAGIC) {
        return Err(Error::Prelude);
    }
    StreamPurpose::from_wire(prelude.get(1).copied().ok_or(Error::Prelude)?)
}

fn ordinary_capacity(class: TrafficClass, config: &TransportConfig) -> usize {
    if class == TrafficClass::Control {
        config.max_streams.saturating_sub(config.core_streams)
    } else {
        config.max_streams
    }
}
