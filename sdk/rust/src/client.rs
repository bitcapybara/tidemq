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
use tokio_util::sync::CancellationToken;

use crate::{Admin, Error, Metrics, Result};

/// Queues, reconnects and native transport all have finite application-owned bounds.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub endpoint: String,
    pub namespace: Option<String>,
    pub close_timeout: Duration,
    pub request_timeout_ms: u64,
    pub max_pending_requests: u32,
    pub max_pending_bytes: u64,
    pub max_reconnect_attempts: u32,
    pub request_lanes: usize,
    pub retry_backoff: Duration,
    pub liveness_timeout: Duration,
    pub protocol: ProtocolLimits,
    pub transport: TransportConfig,
    pub transport_memory_bytes: u64,
    pub transport_control_reserved_bytes: u64,
}

impl ClientConfig {
    pub(crate) fn producer_bytes(&self) -> Result<usize> {
        let total = usize::try_from(self.max_pending_bytes).map_err(|_| Error::Configuration {
            reason: "max_pending_bytes must fit the platform's address space",
        })?;
        let normal = total.saturating_sub((total / 8).saturating_mul(2));
        normal
            .checked_sub((normal / 4).max(self.protocol.max_frame_bytes as usize))
            .filter(|available| *available >= 4096)
            .ok_or(Error::Configuration {
                reason: "max_pending_bytes must reserve control memory and one full delivery frame",
            })
    }

    pub(crate) fn producer_request_limit(&self) -> u32 {
        self.max_pending_requests
            .saturating_sub((self.max_pending_requests / 4).max(1))
            .saturating_sub(1)
    }

    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            namespace: None,
            close_timeout: Duration::from_secs(5),
            request_timeout_ms: 30_000,
            max_pending_requests: 256,
            max_pending_bytes: 16 * 1024 * 1024,
            max_reconnect_attempts: 3,
            request_lanes: 4,
            retry_backoff: Duration::from_millis(25),
            liveness_timeout: Duration::from_secs(1),
            protocol: ProtocolLimits::default(),
            transport: TransportConfig::default(),
            transport_memory_bytes: 128 * 1024 * 1024,
            transport_control_reserved_bytes: 16 * 1024 * 1024,
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_endpoint()?;
        self.protocol.validate()?;
        self.transport.validate()?;
        self.validate_queues()?;
        for (valid, reason) in [
            (
                (!self.close_timeout.is_zero() && self.close_timeout <= Duration::from_secs(300)),
                "close timeout must be between one nanosecond and five minutes",
            ),
            (
                (1..=tidemq_protocol::framing::MAX_TIMEOUT_MS).contains(&self.request_timeout_ms),
                "request_timeout_ms must be between 1 and 300000 milliseconds",
            ),
            (
                (1..=16).contains(&self.max_reconnect_attempts),
                "max_reconnect_attempts must be between 1 and 16",
            ),
            (
                !self.retry_backoff.is_zero() && self.retry_backoff <= Duration::from_secs(5),
                "retry_backoff must be greater than zero and at most 5 seconds",
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
        self.producer_bytes()?;
        MemoryBudget::new(MemoryLimits {
            total_bytes: self.transport_memory_bytes,
            control_reserved_bytes: self.transport_control_reserved_bytes,
        })
        .map_err(|source| Error::Memory { source })?;
        Ok(())
    }

    fn validate_endpoint(&self) -> Result<()> {
        for (valid, reason) in [
            (!self.endpoint.is_empty(), "endpoint must not be empty"),
            (
                self.endpoint.len() <= 512,
                "endpoint must not exceed 512 bytes",
            ),
            (
                !self.endpoint.contains(char::is_whitespace),
                "endpoint must not contain whitespace",
            ),
            (
                !self.endpoint.contains('@'),
                "endpoint must not contain user credentials",
            ),
            (
                !self.endpoint.chars().any(char::is_control),
                "endpoint must not contain control characters",
            ),
        ] {
            if !valid {
                return Err(Error::Configuration { reason });
            }
        }
        let (host, port) = self.endpoint.rsplit_once(':').ok_or(Error::Configuration {
            reason: "endpoint must contain a host and port, for example localhost:7443",
        })?;
        if host.trim_matches(['[', ']']).is_empty() {
            return Err(Error::Configuration {
                reason: "endpoint host must not be empty",
            });
        }
        if port.parse::<u16>().ok().is_none_or(|port| port == 0) {
            return Err(Error::Configuration {
                reason: "endpoint port must be an integer between 1 and 65535",
            });
        }
        Ok(())
    }

    fn validate_queues(&self) -> Result<()> {
        for (valid, reason) in [
            (
                (3..=65_536).contains(&self.max_pending_requests),
                "max_pending_requests must be between 3 and 65536",
            ),
            (
                self.max_pending_bytes >= u64::from(self.protocol.max_frame_bytes),
                "max_pending_bytes must be at least protocol.max_frame_bytes",
            ),
            (
                self.max_pending_bytes <= u64::from(u32::MAX),
                "max_pending_bytes must not exceed 4294967295 bytes",
            ),
            (
                usize::try_from(self.max_pending_bytes)
                    .ok()
                    .is_some_and(|bytes| bytes <= Semaphore::MAX_PERMITS),
                "max_pending_bytes must fit the platform's semaphore capacity",
            ),
            (
                (1..=64).contains(&self.request_lanes),
                "request_lanes must be between 1 and 64",
            ),
            (
                self.request_lanes <= self.transport.max_streams,
                "request_lanes must not exceed transport.max_streams",
            ),
        ] {
            if !valid {
                return Err(Error::Configuration { reason });
            }
        }
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
    pub producer_requests: Arc<Semaphore>,
    pub requests: Arc<Semaphore>,
    pub control_requests: Arc<Semaphore>,
    pub control_memory: Arc<Semaphore>,
    pub resource_memory: Arc<Semaphore>,
    _reserved_memory: tokio::sync::OwnedSemaphorePermit,
    pub metrics: Arc<Metrics>,
    pub consumer_claims: std::sync::Mutex<HashSet<(Id, u32, Id)>>,
    pub(crate) consumers: std::sync::Mutex<Vec<Arc<crate::group_consumer::Core>>>,
    pub(crate) deletions: crate::subscriptions::Deletions,
    producers: std::sync::Mutex<Vec<Arc<crate::logical_producer::Core>>>,
    closing: AtomicBool,
    close_result: std::sync::Mutex<Option<crate::close::CloseResult>>,
    close_changed: tokio::sync::Notify,
    closed: AtomicBool,
    cancellation: CancellationToken,
}

/// One logical session shared across independently authenticated C/P/D/B connections.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<Inner>,
    _owner: Option<Arc<ClientOwner>>,
}

struct ClientOwner(std::sync::Weak<Inner>);

impl Drop for ClientOwner {
    fn drop(&mut self) {
        if let Some(inner) = self.0.upgrade()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                let _ = Client {
                    inner,
                    _owner: None,
                }
                .close()
                .await;
            });
        }
    }
}

impl Client {
    /// Resolves the stable endpoint once; each reconnect uses the same total request deadline.
    pub async fn connect(
        config: ClientConfig,
        trust_roots: Bytes,
        access_token: String,
    ) -> Result<Self> {
        config.validate()?;
        crate::request::run(
            Duration::from_millis(config.request_timeout_ms),
            &CancellationToken::new(),
            |deadline| Self::connect_inner(config, trust_roots, access_token, deadline),
        )
        .await
    }

    async fn connect_inner(
        config: ClientConfig,
        trust_roots: Bytes,
        access_token: String,
        deadline: Instant,
    ) -> Result<Self> {
        let addresses = timeout_at(deadline, tokio::net::lookup_host(config.endpoint.as_str()))
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|source| Error::Resolve {
                endpoint: config.endpoint.clone(),
                source,
            })?;
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(32)
            .map_err(|_| Error::Admission {
                resource: "DNS candidate address allocation",
            })?;
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
        Self::with_target_inner(
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
        config: ClientConfig,
        tls: ClientTls,
        target: PeerTarget,
        access_token: String,
    ) -> Result<Self> {
        config.validate()?;
        crate::request::run(
            Duration::from_millis(config.request_timeout_ms),
            &CancellationToken::new(),
            |deadline| Self::with_target_inner(config, tls, target, access_token, deadline),
        )
        .await
    }

    async fn with_target_inner(
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
        .map_err(|source| Error::Memory { source })?;
        let transport =
            tidemq_transport::Client::bind(bind, tls, config.transport.clone(), budget)?;
        let bytes = Arc::new(Semaphore::new(config.max_pending_bytes as usize));
        let reserved = (config.max_pending_bytes / 8) as u32;
        let reserved_memory = bytes
            .clone()
            .try_acquire_many_owned(reserved.checked_mul(2).ok_or(Error::Configuration {
                reason: "reserved memory exceeds its supported bound",
            })?)
            .map_err(|_| Error::Admission {
                resource: "reserved client control memory",
            })?;
        let control_requests = (config.max_pending_requests / 4).max(1);
        let inner = Arc::new(Inner {
            producer_bytes: Arc::new(Semaphore::new(config.producer_bytes()?)),
            producer_requests: Arc::new(Semaphore::new(config.producer_request_limit() as usize)),
            bytes,
            requests: Arc::new(Semaphore::new(
                config
                    .max_pending_requests
                    .saturating_sub(control_requests)
                    .max(1) as usize,
            )),
            control_requests: Arc::new(Semaphore::new(control_requests as usize)),
            control_memory: Arc::new(Semaphore::new(reserved as usize)),
            resource_memory: Arc::new(Semaphore::new(reserved as usize)),
            _reserved_memory: reserved_memory,
            config,
            transport,
            credentials: ClientCredentials::External {
                session_id: Id::new_v7()?,
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
            consumers: std::sync::Mutex::new(Vec::new()),
            deletions: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            producers: std::sync::Mutex::new(Vec::new()),
            closing: AtomicBool::new(false),
            close_result: std::sync::Mutex::new(None),
            close_changed: tokio::sync::Notify::new(),
            closed: AtomicBool::new(false),
            cancellation: CancellationToken::new(),
        });
        let client = Self {
            _owner: Some(Arc::new(ClientOwner(Arc::downgrade(&inner)))),
            inner,
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
    /// Register the fixed native transport collector in the host registry alongside SDK metrics.
    pub fn transport_metrics(&self) -> Arc<tidemq_transport::TransportMetrics> {
        self.inner.transport.metrics.clone()
    }
    pub fn session_id(&self) -> Id {
        match &self.inner.credentials {
            ClientCredentials::External { session_id, .. } => *session_id,
            ClientCredentials::Internal { session_id, .. } => *session_id,
        }
    }
    pub(crate) async fn operation<T, F: std::future::Future<Output = Result<T>>>(
        &self,
        operation: impl FnOnce(Instant) -> F,
    ) -> Result<T> {
        if self.inner.closing.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        crate::request::run(
            Duration::from_millis(self.inner.config.request_timeout_ms),
            &self.inner.cancellation,
            operation,
        )
        .await
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
                liveness_timeout: self.inner.config.liveness_timeout,
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
    #[tracing::instrument(target = "tidemq_trace", name = "tidemq.sdk.request", skip_all, fields(kind = kind.as_str_name(), class = class.as_str_name(), operation_id = ?operation))]
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
        let mut uncertain = false;
        for attempt in 0..self.inner.config.max_reconnect_attempts {
            let frame = Id::new_v7()
                .map_err(Error::from)
                .and_then(|request| self.frame(kind, class, request, operation, payload, deadline))
                .map_err(|error| error.preserve_uncertainty(uncertain))?;
            self.inner.metrics.record_request();
            let response = if kind == FrameKind::AckBatch {
                self.ack_attempt(frame, deadline).await
            } else {
                match self.rpc(class, deadline).await {
                    Ok(rpc) => {
                        crate::request::mark_sent();
                        match affinity {
                            Some(affinity) => rpc
                                .call_affinity(frame, deadline, affinity)
                                .await
                                .map_err(Error::from),
                            None => rpc.call(frame, deadline).await.map_err(Error::from),
                        }
                    }
                    Err(error) => Err(error),
                }
            };
            let result = response.and_then(|response| self.decode(response, response_kind));
            if let Err(error) = &result {
                uncertain |= error.outcome_unknown()
                    || matches!(
                        error,
                        Error::ResponseMismatch { .. } | Error::Protocol { .. }
                    );
            }
            match result {
                Ok(response) => return Ok(response),
                Err(error)
                    if (error.retry_transport()
                        || error.retry_rejected()
                        || error.retry_admission()
                            && matches!(
                                kind,
                                FrameKind::PublisherControl
                                    | FrameKind::Publish
                                    | FrameKind::RetirePublishes
                                    | FrameKind::ConsumerControl
                                    | FrameKind::GroupRead
                                    | FrameKind::GroupAck
                                    | FrameKind::GroupRetireAck
                                    | FrameKind::GroupRetireRead
                                    | FrameKind::GetTopic
                                    | FrameKind::GetTopicLayout
                            ))
                        && attempt + 1 < self.inner.config.max_reconnect_attempts =>
                {
                    if attempt == 0
                        && let Error::Remote { code, message, .. } = &error
                    {
                        tracing::debug!(target: "tidemq_sdk::retry", event_code = "sdk_request_retry",
                            error_code = code.as_str_name(), reason = message,
                            "The original SDK request will retry a server refusal");
                    }
                    self.inner.metrics.record_retry();
                    if self.backoff(operation, attempt, deadline).await.is_err() {
                        return Err(error.preserve_uncertainty(uncertain));
                    }
                }
                Err(error) => return Err(error.preserve_uncertainty(uncertain)),
            }
        }
        Err(Error::Deadline.preserve_uncertainty(uncertain))
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
        crate::request::mark_sent();
        stream.sender.send(&frame, deadline).await?;
        stream.sender.finish()?;
        let response = stream
            .receiver
            .receive(deadline)
            .await?
            .ok_or(Error::Closed)?;
        if response.frame().request_id != frame.request_id {
            return Err(Error::ResponseMismatch {
                reason: "ACK response request_id differs from the request".into(),
            });
        }
        if response.frame().operation_id != frame.operation_id {
            return Err(Error::ResponseMismatch {
                reason: "ACK response operation_id differs from the request".into(),
            });
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
            if reply.message.len() > 1024 {
                return Err(Error::ResponseMismatch {
                    reason: "server error diagnostic exceeds its finite bound".into(),
                });
            }
            let code = ErrorCode::try_from(reply.code).map_err(|_| Error::ResponseMismatch {
                reason: format!("error reply code {} is not recognized", reply.code),
            })?;
            let outcome =
                Outcome::try_from(reply.outcome).map_err(|_| Error::ResponseMismatch {
                    reason: format!("error reply outcome {} is not recognized", reply.outcome),
                })?;
            if code == ErrorCode::Unspecified {
                return Err(Error::ResponseMismatch {
                    reason: "error reply code must not be unspecified".into(),
                });
            }
            if outcome == Outcome::Unspecified {
                return Err(Error::ResponseMismatch {
                    reason: "error reply outcome must not be unspecified".into(),
                });
            }
            return Err(Error::Remote {
                code,
                message: if reply.message.is_empty() {
                    "The server omitted a diagnostic message".into()
                } else {
                    reply.message
                },
                outcome,
                retryable: reply.retryable,
                expected_sequence: reply.expected_sequence,
                expected_registration_epoch: reply.expected_registration_epoch,
                retry_after_ms: reply.retry_after_ms,
                original_outcome_known: reply.original_outcome_known,
            });
        }
        if frame.kind != i32::from(expected) {
            return Err(Error::ResponseMismatch {
                reason: format!(
                    "expected {}, received frame.kind {}",
                    expected.as_str_name(),
                    frame.kind
                ),
            });
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
        let delay = backoff_delay(self.inner.config.retry_backoff, operation, attempt);
        let wake = Instant::now().checked_add(delay).ok_or(Error::Deadline)?;
        if wake >= deadline {
            return Err(Error::Deadline);
        }
        tokio::time::sleep_until(wake).await;
        Ok(())
    }

    /// Cancellation addresses one request and does not reset its shared lane.
    pub async fn cancel(&self, request_id: Id) -> Result<()> {
        self.operation(|deadline| self.cancel_inner(request_id, deadline))
            .await
    }

    async fn cancel_inner(&self, request_id: Id, deadline: Instant) -> Result<()> {
        let frame = self.frame(
            FrameKind::Cancel,
            TrafficClass::Control,
            Id::new_v7()?,
            Id::new_v7()?,
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

    pub(crate) fn background(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _owner: None,
        }
    }

    pub(crate) fn background_from(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            _owner: None,
        }
    }

    pub(crate) fn register_consumer(&self, core: &Arc<crate::group_consumer::Core>) -> Result<()> {
        let mut resources = self.inner.consumers.lock().map_err(|_| Error::Closed)?;
        if self.inner.closing.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        resources.retain(|resource| resource.retains_work());
        if resources.len() >= 128 {
            return Err(Error::Admission {
                resource: "logical consumers",
            });
        }
        resources.push(core.clone());
        Ok(())
    }

    pub(crate) fn register_producer(
        &self,
        core: &Arc<crate::logical_producer::Core>,
    ) -> Result<()> {
        let mut resources = self.inner.producers.lock().map_err(|_| Error::Closed)?;
        if self.inner.closing.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        resources.retain(|resource| resource.retains_work());
        if resources.len() >= 128 {
            return Err(Error::Admission {
                resource: "logical producers",
            });
        }
        resources.push(core.clone());
        Ok(())
    }

    pub async fn close(&self) -> Result<crate::CloseReport, crate::CloseError> {
        if self
            .inner
            .closing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.inner.cancellation.cancel();
            let client = self.background();
            tokio::spawn(async move {
                let result = client.close_owned().await;
                if let Ok(mut stored) = client.inner.close_result.lock() {
                    *stored = Some(result);
                }
                client.inner.close_changed.notify_waiters();
            });
        }
        loop {
            let notified = self.inner.close_changed.notified();
            match self.inner.close_result.lock() {
                Ok(result) => {
                    if let Some(result) = result.as_ref() {
                        return result.clone();
                    }
                }
                Err(_) => {
                    return Err(crate::CloseError {
                        report: crate::CloseReport::default(),
                        source: Arc::new(Error::Closed),
                    });
                }
            }
            notified.await;
        }
    }

    pub(crate) async fn reserve_resource(
        &self,
        deadline: Instant,
    ) -> Result<tokio::sync::OwnedSemaphorePermit> {
        const BYTES: u32 = 64 * 1024;
        if self.inner.config.max_pending_bytes / 8 < u64::from(BYTES) {
            return Err(Error::Configuration {
                reason: "pending bytes must reserve at least 64 KiB for each logical resource",
            });
        }
        timeout_at(
            deadline,
            self.inner.resource_memory.clone().acquire_many_owned(BYTES),
        )
        .await
        .map_err(|_| Error::Deadline)?
        .map_err(|_| Error::Closed)
    }

    pub(crate) fn closing_token(&self) -> CancellationToken {
        self.inner.cancellation.clone()
    }
    pub(crate) fn is_closing(&self) -> bool {
        self.inner.closing.load(Ordering::Acquire)
    }

    async fn close_owned(&self) -> crate::close::CloseResult {
        let deadline = Instant::now() + self.inner.config.close_timeout;
        let mut report = crate::CloseReport {
            local_cleanup_complete: true,
            ..crate::CloseReport::default()
        };
        let mut failure = None;
        let consumers = self
            .inner
            .consumers
            .lock()
            .map(|mut resources| std::mem::take(&mut *resources))
            .unwrap_or_default();
        let mut jobs = tokio::task::JoinSet::new();
        for consumer in consumers {
            jobs.spawn(async move { consumer.close_until(deadline).await });
        }
        let producers = self
            .inner
            .producers
            .lock()
            .map(|mut resources| std::mem::take(&mut *resources))
            .unwrap_or_default();
        for producer in producers {
            jobs.spawn(async move { producer.close_until(deadline).await });
        }
        while let Some(result) = jobs.join_next().await {
            match result {
                Ok(Ok(child)) => report.merge(&child),
                Ok(Err(error)) => {
                    report.merge(error.report());
                    failure.get_or_insert(error.source);
                }
                Err(_) => {
                    report.local_cleanup_complete = false;
                    failure.get_or_insert_with(|| Arc::new(Error::Closed));
                }
            }
        }
        match self.close_subscription_deletions(deadline).await {
            Ok(child) => report.merge(&child),
            Err(error) => {
                report.merge(error.report());
                failure.get_or_insert(error.source);
            }
        }
        if let Err(error) = self.close_transport(deadline).await {
            report.local_cleanup_complete = false;
            failure.get_or_insert_with(|| Arc::new(error));
        }
        match failure {
            Some(source) => Err(crate::CloseError { report, source }),
            None => Ok(report),
        }
    }

    async fn close_transport(&self, deadline: Instant) -> Result<()> {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.cancellation.cancel();
        self.inner.bytes.close();
        self.inner.producer_bytes.close();
        self.inner.producer_requests.close();
        self.inner.requests.close();
        self.inner.control_requests.close();
        self.inner.control_memory.close();
        self.inner.resource_memory.close();
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
        self.inner.transport.close(deadline).await?;
        Ok(())
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

fn backoff_delay(base: Duration, operation: Id, attempt: u32) -> Duration {
    // UUID v7 prefixes encode time; random tail bytes spread retries created together.
    let jitter = operation
        .as_bytes()
        .last()
        .copied()
        .map(u64::from)
        .unwrap_or(0)
        % 17;
    base.saturating_mul(1_u32.checked_shl(attempt).unwrap_or(u32::MAX))
        .min(Duration::from_secs(1))
        .saturating_add(Duration::from_millis(jitter))
}

#[cfg(test)]
mod tests;
