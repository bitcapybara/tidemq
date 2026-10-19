use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use tidemq_protocol::{
    Id, ProtocolLimits,
    common::{ErrorCode, ErrorReply, Frame, FrameKind, Outcome, TrafficClass},
    framing::{decode_payload, new_frame},
};
use tidemq_transport::{
    ClientCredentials, ClientTls, PeerTarget, ReceivedFrame, TransportConfig,
    budget::{MemoryBudget, MemoryLimits},
    rpc::{RpcClient, RpcConfig},
};
use tokio::{
    sync::{Mutex, Semaphore},
    time::{Instant, timeout_at},
};

use crate::{Admin, Error, Metrics, Result, metrics::increment};

/// Queues, reconnects and native transport all have finite application-owned bounds.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub endpoint: String,
    pub request_timeout_ms: u64,
    pub max_pending_requests: u32,
    pub max_pending_bytes: u64,
    pub max_reconnect_attempts: u32,
    pub request_lanes: usize,
    pub retry_backoff: Duration,
    pub protocol: ProtocolLimits,
    pub transport: TransportConfig,
    pub transport_memory_bytes: u64,
    pub transport_control_reserved_bytes: u64,
}

impl ClientConfig {
    fn producer_bytes(&self) -> Result<usize> {
        let total = usize::try_from(self.max_pending_bytes).map_err(|_| Error::Admission)?;
        let one_delivery = (self.protocol.max_message_bytes as usize)
            .checked_add(crate::consumer::delivery_overhead())
            .ok_or(Error::Admission)?;
        total
            .checked_sub((total / 4).max(one_delivery))
            .filter(|available| *available > 256)
            .ok_or(Error::Configuration {
                reason: "pending bytes must reserve room for production and one maximum delivery",
            })
    }

    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            request_timeout_ms: 30_000,
            max_pending_requests: 256,
            max_pending_bytes: 16 * 1024 * 1024,
            max_reconnect_attempts: 3,
            request_lanes: 4,
            retry_backoff: Duration::from_millis(25),
            protocol: ProtocolLimits::default(),
            transport: TransportConfig::default(),
            transport_memory_bytes: 128 * 1024 * 1024,
            transport_control_reserved_bytes: 16 * 1024 * 1024,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.endpoint.is_empty()
            || self.endpoint.len() > 512
            || self.endpoint.contains(char::is_whitespace)
            || self.request_timeout_ms == 0
            || self.request_timeout_ms > tidemq_protocol::framing::MAX_TIMEOUT_MS
            || self.max_pending_requests == 0
            || self.max_pending_requests > 65_536
            || self.max_pending_bytes < u64::from(self.protocol.max_frame_bytes)
            || self.max_pending_bytes > u64::from(u32::MAX)
            || usize::try_from(self.max_pending_bytes)
                .ok()
                .is_none_or(|bytes| bytes > Semaphore::MAX_PERMITS)
            || self.max_reconnect_attempts == 0
            || self.max_reconnect_attempts > 16
            || self.request_lanes == 0
            || self.request_lanes > self.transport.max_streams
            || self.request_lanes > 64
            || self.retry_backoff.is_zero()
            || self.retry_backoff > Duration::from_secs(5)
        {
            return Err(Error::Configuration {
                reason: "endpoint, deadline, queue or retry bound is invalid",
            });
        }
        self.protocol.validate()?;
        self.producer_bytes()?;
        self.transport.validate()?;
        MemoryBudget::new(MemoryLimits {
            total_bytes: self.transport_memory_bytes,
            control_reserved_bytes: self.transport_control_reserved_bytes,
        })
        .map_err(|_| Error::Configuration {
            reason: "transport memory bounds are invalid",
        })?;
        Ok(())
    }
}

struct Pool {
    control: Mutex<Option<RpcClient>>,
    produce: Mutex<Option<RpcClient>>,
    delivery: Mutex<Option<RpcClient>>,
    bulk: Mutex<Option<RpcClient>>,
}

impl Pool {
    fn slot(&self, class: TrafficClass) -> Result<&Mutex<Option<RpcClient>>> {
        match class {
            TrafficClass::Control => Ok(&self.control),
            TrafficClass::Produce => Ok(&self.produce),
            TrafficClass::Delivery => Ok(&self.delivery),
            TrafficClass::Bulk => Ok(&self.bulk),
            _ => Err(Error::Configuration {
                reason: "SDK traffic class is invalid",
            }),
        }
    }
}

pub(crate) struct Inner {
    pub config: ClientConfig,
    transport: tidemq_transport::Client,
    credentials: ClientCredentials,
    target: PeerTarget,
    pool: Pool,
    pub bytes: Arc<Semaphore>,
    pub producer_bytes: Arc<Semaphore>,
    pub requests: Arc<Semaphore>,
    pub metrics: Arc<Metrics>,
    pub consumer_claims: std::sync::Mutex<HashSet<(Id, u32, Id)>>,
    closed: AtomicBool,
}

/// One logical session shared across independently authenticated C/P/D/B connections.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<Inner>,
}

impl Client {
    /// Resolves the stable endpoint once; each reconnect uses the same total request deadline.
    pub async fn connect(
        config: ClientConfig,
        trust_roots: Bytes,
        access_token: String,
    ) -> Result<Self> {
        config.validate()?;
        let deadline = deadline_after(Duration::from_millis(config.request_timeout_ms))?;
        let addresses = timeout_at(deadline, tokio::net::lookup_host(config.endpoint.as_str()))
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|source| Error::Resolve { source })?;
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(32)
            .map_err(|_| Error::Admission)?;
        candidates.extend(addresses.take(32));
        let address = candidates
            .iter()
            .find(|address| address.is_ipv4())
            .or_else(|| candidates.first())
            .copied()
            .ok_or(Error::Configuration {
                reason: "endpoint resolved to no address",
            })?;
        let host = config
            .endpoint
            .rsplit_once(':')
            .map(|(host, _)| host.trim_matches(['[', ']']))
            .ok_or(Error::Configuration {
                reason: "endpoint requires a host and port",
            })?
            .to_owned();
        let target = PeerTarget {
            address,
            server_name: host,
            local_role: "sdk".into(),
            remote_role: "gateway".into(),
            cluster_id: None,
            node_id: None,
        };
        Self::with_target(
            config.clone(),
            ClientTls {
                trust_roots,
                identity: None,
                expected_nodes: vec![],
            },
            target,
            access_token,
            deadline,
        )
        .await
    }

    pub async fn with_target(
        mut config: ClientConfig,
        tls: ClientTls,
        target: PeerTarget,
        access_token: String,
        deadline: Instant,
    ) -> Result<Self> {
        config.validate()?;
        config.transport.protocol = config.protocol.clone();
        let bind: SocketAddr = if target.address.is_ipv6() {
            "[::]:0"
        } else {
            "0.0.0.0:0"
        }
        .parse()
        .map_err(|_| Error::Configuration {
            reason: "local bind address is invalid",
        })?;
        let budget = MemoryBudget::new(MemoryLimits {
            total_bytes: config.transport_memory_bytes,
            control_reserved_bytes: config.transport_control_reserved_bytes,
        })
        .map_err(|_| Error::Configuration {
            reason: "transport memory bounds are invalid",
        })?;
        let transport =
            tidemq_transport::Client::bind(bind, tls, config.transport.clone(), budget)?;
        let client = Self {
            inner: Arc::new(Inner {
                producer_bytes: Arc::new(Semaphore::new(config.producer_bytes()?)),
                bytes: Arc::new(Semaphore::new(config.max_pending_bytes as usize)),
                requests: Arc::new(Semaphore::new(config.max_pending_requests as usize)),
                config,
                transport,
                credentials: ClientCredentials::External {
                    session_id: Id::random()?,
                    access_token,
                },
                target,
                pool: Pool {
                    control: Mutex::new(None),
                    produce: Mutex::new(None),
                    delivery: Mutex::new(None),
                    bulk: Mutex::new(None),
                },
                metrics: Arc::new(Metrics::default()),
                consumer_claims: std::sync::Mutex::new(HashSet::new()),
                closed: AtomicBool::new(false),
            }),
        };
        client.rpc(TrafficClass::Control, deadline).await?;
        Ok(client)
    }

    pub fn admin(&self) -> Admin {
        Admin {
            client: self.clone(),
        }
    }
    pub fn metrics(&self) -> Arc<Metrics> {
        self.inner.metrics.clone()
    }
    pub fn session_id(&self) -> Id {
        match &self.inner.credentials {
            ClientCredentials::External { session_id, .. } => *session_id,
            ClientCredentials::Internal { session_id, .. } => *session_id,
        }
    }
    pub fn deadline(&self) -> Result<Instant> {
        deadline_after(Duration::from_millis(self.inner.config.request_timeout_ms))
    }

    pub(crate) async fn rpc(&self, class: TrafficClass, deadline: Instant) -> Result<RpcClient> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let mut slot = timeout_at(deadline, self.inner.pool.slot(class)?.lock())
            .await
            .map_err(|_| Error::Deadline)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if let Some(rpc) = slot.as_ref().filter(|rpc| !rpc.is_closed()) {
            return Ok(rpc.clone());
        }
        *slot = None;
        let session = self
            .inner
            .transport
            .pooled(
                self.inner.target.clone(),
                &self.inner.credentials,
                class,
                deadline,
            )
            .await?;
        if self.inner.closed.load(Ordering::Acquire) {
            session.close();
            return Err(Error::Closed);
        }
        let rpc = RpcClient::from_session_with_lanes(
            session,
            RpcConfig {
                max_pending_requests: self.inner.config.max_pending_requests as usize,
                max_pending_bytes: self.inner.config.max_pending_bytes as u32,
                read_idle_timeout: self.inner.config.transport.idle_timeout,
            },
            self.inner.config.request_lanes,
            deadline,
        )
        .await?;
        *slot = Some(rpc.clone());
        Ok(rpc)
    }

    pub(crate) fn frame<M: Message>(
        &self,
        kind: FrameKind,
        class: TrafficClass,
        request: Id,
        operation: Id,
        payload: &M,
        deadline: Instant,
    ) -> Result<Frame> {
        let remaining = u64::try_from(
            deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )
        .map_err(|_| Error::Deadline)?
        .min(tidemq_protocol::framing::MAX_TIMEOUT_MS);
        if remaining == 0 {
            return Err(Error::Deadline);
        }
        let mut frame = new_frame(
            kind,
            class,
            request,
            operation,
            payload,
            remaining,
            &self.inner.config.protocol,
        )?;
        frame.traceparent = crate::context::current_traceparent()?;
        if frame.traceparent.len() > self.inner.config.protocol.max_traceparent_bytes as usize
            || frame.encoded_len()
                > tidemq_protocol::framing::frame_limit(class, &self.inner.config.protocol) as usize
        {
            return Err(Error::Protocol {
                source: tidemq_protocol::Error::Limit {
                    name: "frame with trace context",
                },
            });
        }
        Ok(frame)
    }

    pub(crate) async fn request<M: Message, R: Message + Default>(
        &self,
        kind: FrameKind,
        response_kind: FrameKind,
        class: TrafficClass,
        operation: Id,
        payload: &M,
        deadline: Instant,
    ) -> Result<R> {
        self.request_inner(
            kind,
            response_kind,
            class,
            operation,
            payload,
            deadline,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)] // Keep the protocol envelope and one optional lane route together.
    pub(crate) async fn request_affinity<M: Message, R: Message + Default>(
        &self,
        kind: FrameKind,
        response_kind: FrameKind,
        class: TrafficClass,
        operation: Id,
        payload: &M,
        deadline: Instant,
        affinity: u64,
    ) -> Result<R> {
        self.request_inner(
            kind,
            response_kind,
            class,
            operation,
            payload,
            deadline,
            Some(affinity),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)] // Both entry points share identical deadline and retry behavior.
    #[tracing::instrument(skip_all, fields(kind = kind.as_str_name(), class = class.as_str_name(), operation_id = ?operation))]
    async fn request_inner<M: Message, R: Message + Default>(
        &self,
        kind: FrameKind,
        response_kind: FrameKind,
        class: TrafficClass,
        operation: Id,
        payload: &M,
        deadline: Instant,
        affinity: Option<u64>,
    ) -> Result<R> {
        let _timer = self.inner.metrics.timer();
        for attempt in 0..self.inner.config.max_reconnect_attempts {
            let frame = self.frame(kind, class, Id::random()?, operation, payload, deadline)?;
            increment(&self.inner.metrics.requests, 1);
            let response = if kind == FrameKind::AckBatch {
                self.ack_attempt(frame, deadline).await
            } else {
                match self.rpc(class, deadline).await {
                    Ok(rpc) => match affinity {
                        Some(affinity) => rpc
                            .call_affinity(frame, deadline, affinity)
                            .await
                            .map_err(Error::from),
                        None => rpc.call(frame, deadline).await.map_err(Error::from),
                    },
                    Err(error) => Err(error),
                }
            };
            match response.and_then(|response| self.decode(response, response_kind)) {
                Ok(response) => return Ok(response),
                Err(error)
                    if (error.retry_transport() || error.retry_rejected())
                        && attempt + 1 < self.inner.config.max_reconnect_attempts =>
                {
                    increment(&self.inner.metrics.retries, 1);
                    if self.backoff(operation, attempt, deadline).await.is_err() {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(Error::Deadline)
    }

    async fn ack_attempt(&self, frame: Frame, deadline: Instant) -> Result<ReceivedFrame> {
        self.rpc(TrafficClass::Control, deadline).await?;
        let session = self
            .inner
            .transport
            .pooled(
                self.inner.target.clone(),
                &self.inner.credentials,
                TrafficClass::Control,
                deadline,
            )
            .await?;
        let mut stream = session
            .connection()
            .open(tidemq_transport::StreamPurpose::Ack, deadline)
            .await?;
        stream.sender.send(&frame, deadline).await?;
        stream.sender.finish()?;
        let response = stream
            .receiver
            .receive(deadline)
            .await?
            .ok_or(Error::Closed)?;
        if response.frame().request_id != frame.request_id
            || response.frame().operation_id != frame.operation_id
        {
            return Err(Error::ResponseMismatch);
        }
        Ok(response)
    }

    pub(crate) fn decode<R: Message + Default>(
        &self,
        response: ReceivedFrame,
        expected: FrameKind,
    ) -> Result<R> {
        let frame = response.frame();
        if frame.kind == i32::from(FrameKind::Error) {
            let reply: ErrorReply =
                decode_payload(frame.payload.clone(), &self.inner.config.protocol)?;
            let code = ErrorCode::try_from(reply.code).map_err(|_| Error::ResponseMismatch)?;
            let outcome = Outcome::try_from(reply.outcome).map_err(|_| Error::ResponseMismatch)?;
            if code == ErrorCode::Unspecified || outcome == Outcome::Unspecified {
                return Err(Error::ResponseMismatch);
            }
            return Err(Error::Remote {
                code,
                outcome,
                retryable: reply.retryable,
                expected_sequence: reply.expected_sequence,
                expected_registration_epoch: reply.expected_registration_epoch,
                retry_after_ms: reply.retry_after_ms,
            });
        }
        if frame.kind != i32::from(expected) {
            return Err(Error::ResponseMismatch);
        }
        Ok(decode_payload(
            frame.payload.clone(),
            &self.inner.config.protocol,
        )?)
    }

    pub(crate) async fn backoff(
        &self,
        operation: Id,
        attempt: u32,
        deadline: Instant,
    ) -> Result<()> {
        let jitter = operation
            .as_bytes()
            .first()
            .copied()
            .map(u64::from)
            .unwrap_or(0)
            % 17;
        let delay = self
            .inner
            .config
            .retry_backoff
            .saturating_mul(1_u32.checked_shl(attempt).unwrap_or(u32::MAX))
            .min(Duration::from_secs(1))
            .saturating_add(Duration::from_millis(jitter));
        let wake = Instant::now().checked_add(delay).ok_or(Error::Deadline)?;
        if wake >= deadline {
            return Err(Error::Deadline);
        }
        tokio::time::sleep_until(wake).await;
        Ok(())
    }

    /// Cancellation addresses one request and does not reset its shared lane.
    pub async fn cancel(&self, request_id: Id, deadline: Instant) -> Result<()> {
        let frame = self.frame(
            FrameKind::Cancel,
            TrafficClass::Control,
            Id::random()?,
            Id::random()?,
            &tidemq_protocol::common::CancelRequest {
                request_id: Bytes::copy_from_slice(request_id.as_bytes()),
            },
            deadline,
        )?;
        self.rpc(TrafficClass::Control, deadline)
            .await?
            .notify(frame, deadline)
            .await?;
        Ok(())
    }

    pub async fn close(&self, deadline: Instant) {
        self.inner.closed.store(true, Ordering::Release);
        for class in [
            TrafficClass::Control,
            TrafficClass::Produce,
            TrafficClass::Delivery,
            TrafficClass::Bulk,
        ] {
            if let Ok(slot) = self.inner.pool.slot(class)
                && let Ok(mut slot) = timeout_at(deadline, slot.lock()).await
                && let Some(rpc) = slot.take()
            {
                rpc.close(deadline).await;
            }
        }
        let _ = self.inner.transport.close(deadline).await;
    }
}

pub(crate) fn deadline_after(duration: Duration) -> Result<Instant> {
    Instant::now().checked_add(duration).ok_or(Error::Deadline)
}

pub(crate) fn affinity(value: &impl std::hash::Hash) -> u64 {
    use std::hash::Hasher;
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hash);
    hash.finish()
}
