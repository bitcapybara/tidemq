use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use s2n_quic::stream::{ReceiveStream, SendStream};
use tidemq_protocol::{
    common::{Frame, FrameKind, TrafficClass},
    framing::{decode_frame, encode_frame, frame_length},
};
use tokio::{
    io::AsyncReadExt,
    time::{Instant, timeout_at},
};

use crate::budget::{MemoryBudget, MemoryPermit};

use super::{
    Error, Result, TransportConfig,
    session::{StreamLease, StreamPurpose},
    telemetry::{TransportMetrics, add},
};

const RESET_INVALID: u32 = 0x101;

#[derive(Clone, Copy)]
pub(super) enum FrameAdmission {
    Handshake(Option<TrafficClass>),
    Core,
    Normal(TrafficClass),
}

impl FrameAdmission {
    fn class(self) -> Option<TrafficClass> {
        match self {
            Self::Handshake(class) => class,
            Self::Core => Some(TrafficClass::Control),
            Self::Normal(class) => Some(class),
        }
    }
    fn reserve(self, budget: &MemoryBudget, bytes: u32) -> Result<MemoryPermit> {
        match self {
            Self::Handshake(_) => budget.try_reserve_handshake(bytes),
            Self::Core => budget.try_reserve_core(bytes),
            Self::Normal(class) => budget.try_reserve(class, bytes),
        }
        .map_err(|source| Error::Memory { source })
    }
}

/// Keep this lease alive while application code retains payload clones.
pub struct ReceivedFrame {
    frame: Frame,
    deadline: Instant,
}

impl ReceivedFrame {
    pub fn frame(&self) -> &Frame {
        &self.frame
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
}

pub struct FramedStream {
    pub sender: FrameSender,
    pub receiver: FrameReceiver,
}

pub struct FrameSender {
    pub(super) stream: SendStream,
    lease: Arc<StreamLease>,
    usable: bool,
    clean_end: bool,
}

pub struct FrameReceiver {
    stream: ReceiveStream,
    lease: Arc<StreamLease>,
    usable: bool,
    clean_end: bool,
}

struct ChargedBytes {
    bytes: Bytes,
    _memory: MemoryPermit,
}

struct LaneOperation {
    lease: Arc<StreamLease>,
    completed: bool,
}
impl Drop for LaneOperation {
    fn drop(&mut self) {
        if !self.completed {
            self.lease.invalidate();
        }
    }
}

impl AsRef<[u8]> for ChargedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl FramedStream {
    pub(super) fn new(stream: s2n_quic::stream::BidirectionalStream, lease: StreamLease) -> Self {
        let (receive, send) = stream.split();
        Self::from_halves(receive, send, lease)
    }
    pub(super) fn from_halves(
        receive: ReceiveStream,
        send: SendStream,
        lease: StreamLease,
    ) -> Self {
        let lease = Arc::new(lease);
        Self {
            sender: FrameSender {
                stream: send,
                lease: lease.clone(),
                usable: true,
                clean_end: false,
            },
            receiver: FrameReceiver {
                stream: receive,
                lease,
                usable: true,
                clean_end: false,
            },
        }
    }
    pub fn id(&self) -> u64 {
        self.sender.stream.id()
    }
    pub fn purpose(&self) -> StreamPurpose {
        self.sender.lease.purpose
    }
    pub fn split(self) -> (FrameSender, FrameReceiver) {
        (self.sender, self.receiver)
    }
}

impl FrameSender {
    /// Enqueue without waiting for a transport ACK, allowing durable-ACK-independent pipelining.
    pub async fn send(&mut self, frame: &Frame, deadline: Instant) -> Result<()> {
        if !self.usable {
            return Err(Error::Poisoned);
        }
        self.lease.validate_kind(frame)?;
        let state = self.lease.state.clone();
        if frame.traffic_class != i32::from(state.class) {
            return Err(Error::Authorization {
                reason: "frame differs from the bound connection class",
            });
        }
        self.usable = false;
        let mut operation = LaneOperation {
            lease: self.lease.clone(),
            completed: false,
        };
        let started = Instant::now();
        let deadline = deadline.min(
            started
                .checked_add(std::time::Duration::from_millis(
                    frame
                        .remaining_timeout_ms
                        .min(tidemq_protocol::framing::MAX_TIMEOUT_MS),
                ))
                .ok_or(Error::Configuration {
                    reason: "request deadline overflow",
                })?,
        );
        let result = timeout_at(deadline, self.enqueue(frame, deadline)).await;
        add(
            &state.metrics.send_wait_ns,
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        );
        match result {
            Ok(Ok(length)) => {
                self.usable = true;
                operation.completed = true;
                add(&state.metrics.frames_sent, 1);
                add(&state.metrics.bytes_sent, u64::from(length));
                Ok(())
            }
            Ok(Err(error)) => {
                let _ = self.stream.reset(RESET_INVALID.into());
                Err(error)
            }
            Err(_) => {
                let _ = self.stream.reset(RESET_INVALID.into());
                add(&state.metrics.deadlines, 1);
                Err(Error::Deadline {
                    operation: "send frame",
                })
            }
        }
    }

    /// Wait for native receipt before a deliberate endpoint shutdown; this is not a durable ACK.
    pub async fn flush(&mut self, deadline: Instant) -> Result<()> {
        if !self.usable {
            return Err(Error::Poisoned);
        }
        self.usable = false;
        let mut operation = LaneOperation {
            lease: self.lease.clone(),
            completed: false,
        };
        let result = timeout_at(deadline, self.stream.flush()).await;
        match result {
            Ok(Ok(())) => {
                self.usable = true;
                operation.completed = true;
                Ok(())
            }
            Ok(Err(source)) => Err(Error::Stream {
                operation: "flush lane",
                source,
            }),
            Err(_) => {
                add(&self.lease.state.metrics.deadlines, 1);
                Err(Error::Deadline {
                    operation: "flush lane",
                })
            }
        }
    }

    async fn enqueue(&mut self, frame: &Frame, deadline: Instant) -> Result<u32> {
        std::future::poll_fn(|context| self.stream.poll_send_ready(context))
            .await
            .map_err(|source| Error::Stream {
                operation: "wait for send readiness",
                source,
            })?;
        let state = &self.lease.state;
        let (bytes, length) = encode_charged(
            frame,
            if self.lease.purpose == StreamPurpose::Core {
                FrameAdmission::Core
            } else {
                FrameAdmission::Normal(state.class)
            },
            &state.config,
            &state.budget,
            deadline,
        )?;
        self.stream
            .send(bytes)
            .await
            .map_err(|source| Error::Stream {
                operation: "enqueue frame",
                source,
            })?;
        Ok(length)
    }

    /// Finish the lane; no business outcome can be inferred from success or failure.
    pub fn finish(&mut self) -> Result<()> {
        if !self.usable {
            return Err(Error::Poisoned);
        }
        self.stream.finish().map_err(|source| Error::Stream {
            operation: "finish lane",
            source,
        })?;
        self.usable = false;
        self.clean_end = true;
        self.lease.invalidate();
        Ok(())
    }

    /// Reset the whole lane only when every unresolved operation is recovered by identity.
    pub fn reset(&mut self) -> Result<()> {
        self.usable = false;
        self.lease.invalidate();
        self.stream
            .reset(RESET_INVALID.into())
            .map_err(|source| Error::Stream {
                operation: "reset lane",
                source,
            })
    }
}

impl Drop for FrameSender {
    fn drop(&mut self) {
        self.lease.invalidate();
        if !self.usable && !self.clean_end {
            let _ = self.stream.reset(RESET_INVALID.into());
        }
    }
}

impl FrameReceiver {
    /// A canceled/expired receive poisons this lane because a prefix/body may be partial.
    pub async fn receive(&mut self, deadline: Instant) -> Result<Option<ReceivedFrame>> {
        self.receive_frame(Some(deadline), None).await
    }

    /// Wait without an application idle timer; once the first byte arrives, assembly is bounded.
    /// Canceling before that byte consumes nothing. Canceling a started frame poisons the lane.
    pub async fn receive_next(
        &mut self,
        assembly_timeout: std::time::Duration,
    ) -> Result<Option<ReceivedFrame>> {
        if !self.usable {
            return Err(Error::Poisoned);
        }
        if assembly_timeout.is_zero() || Instant::now().checked_add(assembly_timeout).is_none() {
            return Err(Error::Configuration {
                reason: "frame assembly timeout is invalid",
            });
        }
        match read_first(&mut self.stream).await {
            Ok(Some(first)) => {
                let deadline = first.1.checked_add(assembly_timeout);
                self.receive_frame(deadline, Some(first)).await
            }
            Ok(None) => {
                self.usable = false;
                self.clean_end = true;
                self.lease.invalidate();
                Ok(None)
            }
            Err(error) => {
                self.usable = false;
                self.lease.invalidate();
                add(&self.lease.state.metrics.malformed, 1);
                let _ = self.stream.stop_sending(RESET_INVALID.into());
                Err(error)
            }
        }
    }

    async fn receive_frame(
        &mut self,
        deadline: Option<Instant>,
        first: Option<(u8, Instant)>,
    ) -> Result<Option<ReceivedFrame>> {
        if !self.usable {
            return Err(Error::Poisoned);
        }
        self.usable = false;
        let mut operation = LaneOperation {
            lease: self.lease.clone(),
            completed: false,
        };
        let deadline = deadline.ok_or(Error::Configuration {
            reason: "frame assembly deadline overflow",
        })?;
        let state = &self.lease.state;
        let result = read_wire_started(
            &mut self.stream,
            if self.lease.purpose == StreamPurpose::Core {
                FrameAdmission::Core
            } else {
                FrameAdmission::Normal(state.class)
            },
            &state.config,
            &state.budget,
            &state.metrics,
            deadline,
            first,
        )
        .await;
        match result {
            Ok(Some(frame)) => {
                let validation = state
                    .authorization
                    .validate_frame(&frame.frame, &state.config.protocol)
                    .and_then(|()| self.lease.validate_kind(&frame.frame));
                if let Err(error) = validation {
                    add(&state.metrics.rejected, 1);
                    let _ = self.stream.stop_sending(RESET_INVALID.into());
                    return Err(error);
                }
                self.usable = true;
                operation.completed = true;
                Ok(Some(frame))
            }
            Ok(None) => {
                self.clean_end = true;
                Ok(None)
            }
            Err(error) => {
                let _ = self.stream.stop_sending(RESET_INVALID.into());
                Err(error)
            }
        }
    }
}

impl Drop for FrameReceiver {
    fn drop(&mut self) {
        self.lease.invalidate();
        if !self.usable && !self.clean_end {
            let _ = self.stream.stop_sending(RESET_INVALID.into());
        }
    }
}

pub(super) async fn read_wire(
    stream: &mut ReceiveStream,
    admission: FrameAdmission,
    config: &TransportConfig,
    budget: &MemoryBudget,
    metrics: &TransportMetrics,
    deadline: Instant,
) -> Result<Option<ReceivedFrame>> {
    read_wire_started(stream, admission, config, budget, metrics, deadline, None).await
}

async fn read_wire_started(
    stream: &mut ReceiveStream,
    admission: FrameAdmission,
    config: &TransportConfig,
    budget: &MemoryBudget,
    metrics: &TransportMetrics,
    deadline: Instant,
    first: Option<(u8, Instant)>,
) -> Result<Option<ReceivedFrame>> {
    let result = timeout_at(
        deadline,
        read_body(stream, admission, config, budget, first),
    )
    .await;
    match result {
        Ok(Ok(frame)) => {
            if let Some(frame) = &frame {
                add(&metrics.frames_received, 1);
                add(&metrics.bytes_received, frame.frame.encoded_len() as u64);
            }
            Ok(frame)
        }
        Ok(Err(error)) => {
            add(&metrics.malformed, 1);
            Err(error)
        }
        Err(_) => {
            add(&metrics.deadlines, 1);
            Err(Error::Deadline {
                operation: "receive frame",
            })
        }
    }
}

// A one-byte read is cancellation safe; the caller arms lane poisoning before the next await.
async fn read_first(stream: &mut ReceiveStream) -> Result<Option<(u8, Instant)>> {
    let mut first = [0_u8; 1];
    if stream.read(&mut first).await.map_err(|source| Error::Io {
        operation: "read prefix",
        source,
    })? == 0
    {
        return Ok(None);
    }
    let [byte] = first;
    Ok(Some((byte, Instant::now())))
}

// Started prefix/body consumption and budget deduction share one cancellation boundary.
async fn read_body(
    stream: &mut ReceiveStream,
    admission: FrameAdmission,
    config: &TransportConfig,
    budget: &MemoryBudget,
    first: Option<(u8, Instant)>,
) -> Result<Option<ReceivedFrame>> {
    let Some((byte, received_at)) = (match first {
        Some(first) => Some(first),
        None => read_first(stream).await?,
    }) else {
        return Ok(None);
    };
    let mut prefix = [byte, 0, 0, 0];
    let rest = prefix.get_mut(1..).ok_or(Error::Prelude)?;
    stream.read_exact(rest).await.map_err(|source| Error::Io {
        operation: "read prefix",
        source,
    })?;
    let class = admission.class();
    let max = if matches!(admission, FrameAdmission::Handshake(_)) {
        config.protocol.max_control_frame_bytes.min(16_384)
    } else if let Some(class) = class {
        frame_limit_for(class, config)
    } else {
        config.protocol.max_control_frame_bytes.min(16_384)
    };
    let length_u32 = u32::from_be_bytes(prefix);
    if length_u32 == 0 || length_u32 > max {
        return Err(Error::Prelude);
    }
    let expected = class.unwrap_or(TrafficClass::Bulk);
    let length = frame_length(prefix, expected, &config.protocol)
        .map_err(|source| Error::Protocol { source })?;
    // Reserve decoding overhead too; the payload remains zero-copy in the leased Bytes.
    let charge = length_u32.checked_mul(2).ok_or(Error::Prelude)?;
    let memory = admission.reserve(budget, charge)?;
    let mut body = Vec::new();
    body.try_reserve_exact(length)
        .map_err(|source| Error::Allocation { source })?;
    body.resize(length, 0);
    stream
        .read_exact(&mut body)
        .await
        .map_err(|source| Error::Io {
            operation: "read frame body",
            source,
        })?;
    let body = Bytes::from_owner(ChargedBytes {
        bytes: Bytes::from(body),
        _memory: memory,
    });
    let mut frame = decode_bound(body, class, config)?;
    let request_deadline = received_at
        .checked_add(std::time::Duration::from_millis(frame.remaining_timeout_ms))
        .ok_or(Error::Prelude)?;
    if request_deadline <= Instant::now() {
        return Err(Error::Deadline {
            operation: "read request budget",
        });
    }
    frame.remaining_timeout_ms = u64::try_from(
        request_deadline
            .saturating_duration_since(Instant::now())
            .as_millis(),
    )
    .unwrap_or(0);
    if frame.remaining_timeout_ms == 0 {
        return Err(Error::Deadline {
            operation: "decode request budget",
        });
    }
    Ok(Some(ReceivedFrame {
        frame,
        deadline: request_deadline,
    }))
}

fn decode_bound(
    body: Bytes,
    class: Option<TrafficClass>,
    config: &TransportConfig,
) -> Result<Frame> {
    if let Some(class) = class {
        return decode_frame(body, class, &config.protocol)
            .map_err(|source| Error::Protocol { source });
    }
    for class in [
        TrafficClass::Control,
        TrafficClass::Produce,
        TrafficClass::Delivery,
        TrafficClass::Bulk,
        TrafficClass::Replication,
    ] {
        if let Ok(frame) = decode_frame(body.clone(), class, &config.protocol) {
            if FrameKind::try_from(frame.kind).ok() == Some(FrameKind::Hello) {
                return Ok(frame);
            }
            return Err(Error::Authentication {
                reason: "first frame must be HELLO",
            });
        }
    }
    Err(Error::Authentication {
        reason: "HELLO is malformed",
    })
}

pub(super) fn frame_limit_for(class: TrafficClass, config: &TransportConfig) -> u32 {
    tidemq_protocol::framing::frame_limit(class, &config.protocol)
}

pub(super) fn encode_charged(
    frame: &Frame,
    admission: FrameAdmission,
    config: &TransportConfig,
    budget: &MemoryBudget,
    deadline: Instant,
) -> Result<(Bytes, u32)> {
    let mut frame = frame.clone();
    let remaining = u64::try_from(
        deadline
            .saturating_duration_since(Instant::now())
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    if remaining == 0 {
        return Err(Error::Deadline {
            operation: "encode frame",
        });
    }
    frame.remaining_timeout_ms = frame.remaining_timeout_ms.min(remaining);
    let length = frame
        .encoded_len()
        .checked_add(4)
        .and_then(|length| u32::try_from(length).ok())
        .ok_or(Error::Configuration {
            reason: "encoded frame length overflow",
        })?;
    let permit = admission.reserve(budget, length)?;
    let bytes =
        encode_frame(&frame, &config.protocol).map_err(|source| Error::Protocol { source })?;
    Ok((
        Bytes::from_owner(ChargedBytes {
            bytes,
            _memory: permit,
        }),
        length,
    ))
}
