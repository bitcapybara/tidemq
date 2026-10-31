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
    rpc::{RpcBudget, RpcClient, RpcConfig},
};

#[cfg(test)]
use tidemq_transport::rpc::RpcSnapshot;
use tokio::{
    sync::{Mutex, Semaphore},
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;

use crate::{Admin, Error, Metrics, Result};

pub(crate) mod admission;
pub(crate) mod protected;

/// Queues, reconnects and native transport all have finite application-owned bounds.
#[derive(Clone, Debug)]
pub(crate) struct ClientConfig {
    pub endpoint: String,
    pub server_name: Option<String>,
    pub namespace: Option<String>,
    pub close_timeout: Duration,
    pub connect_timeout: Duration,
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
            server_name: None,
            namespace: None,
            close_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
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
                !self.connect_timeout.is_zero() && self.connect_timeout <= Duration::from_secs(300),
                "connect_timeout must be greater than zero and at most five minutes",
            ),
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

/// Effective finite bounds, suitable for host diagnostics without exposing transport configuration.
#[derive(Clone, Copy, Debug)]
pub struct ClientLimits {
    pub request_lanes: usize,
    pub max_pending_requests: u32,
    pub max_pending_bytes: u64,
    pub transport_memory_bytes: u64,
    pub transport_control_reserved_bytes: u64,
    pub max_connections: usize,
    pub max_handshakes: usize,
    pub max_streams: usize,
    pub core_streams: usize,
    pub max_ack_streams: usize,
    pub connection_window_bytes: u32,
    pub stream_window_bytes: u32,
    pub send_buffer_bytes: u32,
    pub liveness_timeout: Duration,
}

impl From<&ClientConfig> for ClientLimits {
    fn from(config: &ClientConfig) -> Self {
        Self {
            request_lanes: config.request_lanes,
            max_pending_requests: config.max_pending_requests,
            max_pending_bytes: config.max_pending_bytes,
            transport_memory_bytes: config.transport_memory_bytes,
            transport_control_reserved_bytes: config.transport_control_reserved_bytes,
            max_connections: config.transport.max_connections,
            max_handshakes: config.transport.max_handshakes,
            max_streams: config.transport.max_streams,
            core_streams: config.transport.core_streams,
            max_ack_streams: config.transport.max_ack_streams,
            connection_window_bytes: config.transport.connection_window_bytes,
            stream_window_bytes: config.transport.stream_window_bytes,
            send_buffer_bytes: config.transport.send_buffer_bytes,
            liveness_timeout: config.liveness_timeout,
        }
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
    server_name: String,
    dual_stack: bool,
    pool: Pool,
    delivery_budget: std::sync::Mutex<Option<RpcBudget>>,
    pub bytes: Arc<Semaphore>,
    pub producer_bytes: Arc<Semaphore>,
    pub producer_requests: Arc<Semaphore>,
    pub requests: Arc<Semaphore>,
    pub control_requests: Arc<Semaphore>,
    pub control_memory: Arc<Semaphore>,
    pub resource_memory: Arc<Semaphore>,
    pub admission: admission::Admission,
    _reserved_memory: tokio::sync::OwnedSemaphorePermit,
    pub metrics: Arc<Metrics>,
    pub(crate) consumers: std::sync::Mutex<Vec<Arc<crate::group_consumer::Core>>>,
    pub(crate) deletions: crate::subscriptions::Deletions,
    pub(crate) administrations: crate::admin::pending::Operations,
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
    #[cfg(test)]
    pub(crate) fn rpc_snapshots(&self) -> Vec<(&'static str, Option<RpcSnapshot>)> {
        let pools = [
            ("control", &self.inner.pool.control),
            ("produce", &self.inner.pool.produce),
            ("delivery", &self.inner.pool.delivery),
            ("bulk", &self.inner.pool.bulk),
        ];
        pools
            .into_iter()
            .map(|(name, pool)| {
                let snapshot = pool
                    .try_lock()
                    .ok()
                    .and_then(|guard| guard.as_ref().and_then(|rpc| rpc.try_snapshot()));
                (name, snapshot)
            })
            .collect()
    }

    /// Connects within the configured total connect budget; reconnects resolve the endpoint again.
    pub(crate) async fn connect(
        config: ClientConfig,
        trust_roots: Bytes,
        access_token: String,
    ) -> Result<Self> {
        config.validate()?;
        crate::request::run(
            config.connect_timeout,
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
        let client = Self::unconnected(config, trust_roots, access_token)?;
        client.rpc(TrafficClass::Control, deadline).await?;
        Ok(client)
    }

    fn unconnected(
        mut config: ClientConfig,
        trust_roots: Bytes,
        access_token: String,
    ) -> Result<Self> {
        let server_name = config
            .server_name
            .clone()
            .or_else(|| {
                config
                    .endpoint
                    .rsplit_once(':')
                    .map(|(host, _)| host.trim_matches(['[', ']']).to_owned())
            })
            .ok_or(Error::Configuration {
                reason: "endpoint requires a host and port",
            })?;
        config.transport.protocol = config.protocol.clone();
        let (transport, dual_stack) = bind_transport(&config, trust_roots, &server_name)?;
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
            admission: admission::Admission::new(&config),
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
            server_name,
            dual_stack,
            pool: Pool {
                control: Mutex::new(None),
                produce: Mutex::new(None),
                delivery: Mutex::new(None),
                bulk: Mutex::new(None),
            },
            delivery_budget: std::sync::Mutex::new(None),
            metrics: Arc::new(Metrics::default()),
            consumers: std::sync::Mutex::new(Vec::new()),
            deletions: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            administrations: std::sync::Mutex::new(std::collections::BTreeMap::new()),
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
        Ok(client)
    }

    pub fn admin(&self) -> Admin {
        Admin {
            client: self.clone(),
        }
    }
    pub fn limits(&self) -> ClientLimits {
        (&self.inner.config).into()
    }
    pub fn metrics(&self) -> Arc<Metrics> {
        self.inner.metrics.clone()
    }
    /// Register the fixed native transport collector in the host registry alongside SDK metrics.
    pub fn transport_metrics(&self) -> Arc<tidemq_transport::TransportMetrics> {
        self.inner.transport.metrics.clone()
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
        let connect_deadline = deadline.min(deadline_after(self.inner.config.connect_timeout)?);
        let rpc = self.connect_rpc(class, connect_deadline).await?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        *slot = Some(rpc.clone());
        Ok(rpc)
    }

    async fn connect_rpc(&self, class: TrafficClass, deadline: Instant) -> Result<RpcClient> {
        let candidates = resolve(&self.inner.config.endpoint, deadline).await?;
        let mut last = Error::Deadline;
        for (index, address) in candidates.iter().enumerate() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let count = u32::try_from(candidates.len().saturating_sub(index)).map_err(|_| {
                Error::Configuration {
                    reason: "DNS candidate count is excessive",
                }
            })?;
            let budget = remaining.checked_div(count).ok_or(Error::Deadline)?;
            if budget.is_zero() {
                return Err(last);
            }
            let attempt = deadline.min(Instant::now().checked_add(budget).ok_or(Error::Deadline)?);
            match self.connect_address(*address, class, attempt).await {
                Ok(rpc) => return Ok(rpc),
                Err(error) if error.retry_transport() => last = error,
                Err(error) => return Err(error),
            }
        }
        Err(last)
    }

    async fn connect_address(
        &self,
        address: SocketAddr,
        class: TrafficClass,
        deadline: Instant,
    ) -> Result<RpcClient> {
        let address = if self.inner.dual_stack {
            match address {
                SocketAddr::V4(address) => {
                    SocketAddr::from((address.ip().to_ipv6_mapped(), address.port()))
                }
                address => address,
            }
        } else {
            address
        };
        let target = PeerTarget {
            address,
            server_name: self.inner.server_name.clone(),
            local_role: "sdk".into(),
            remote_role: "gateway".into(),
            cluster_id: None,
            node_id: None,
        };
        let session = self
            .inner
            .transport
            .pooled(target, &self.inner.credentials, class, deadline)
            .await?;
        if self.inner.closed.load(Ordering::Acquire) {
            session.close();
            return Err(Error::Closed);
        }
        let config = RpcConfig {
            max_pending_requests: self.inner.config.max_pending_requests as usize,
            max_pending_bytes: self.inner.config.max_pending_bytes as u32,
            read_idle_timeout: self.inner.config.transport.idle_timeout,
            liveness_timeout: self.inner.config.liveness_timeout,
        };
        let result = if class == TrafficClass::Delivery {
            let budget = {
                let mut stored = self
                    .inner
                    .delivery_budget
                    .lock()
                    .map_err(|_| Error::Closed)?;
                match stored.as_ref() {
                    Some(budget) => budget.clone(),
                    None => {
                        let budget = RpcBudget::for_connection(
                            &config,
                            &session.connection(),
                            self.inner.config.request_lanes,
                        )?;
                        *stored = Some(budget.clone());
                        budget
                    }
                }
            };
            RpcClient::from_session_with_lanes_and_budget(
                session.clone(),
                config,
                self.inner.config.request_lanes,
                budget,
                deadline,
            )
            .await
        } else {
            RpcClient::from_session_with_lanes(
                session.clone(),
                config,
                self.inner.config.request_lanes,
                deadline,
            )
            .await
        };
        match result {
            Ok(rpc) => Ok(rpc),
            Err(source) => {
                session.close();
                Err(source.into())
            }
        }
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
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)] // An admitted attempt uses the same wire envelope without reserving twice.
    pub(crate) async fn request_reserved<M: Message, R: Message + Default>(
        &self,
        kind: FrameKind,
        response_kind: FrameKind,
        class: TrafficClass,
        operation: Id,
        payload: &M,
        deadline: Instant,
        attempt: &admission::Attempt,
    ) -> Result<R> {
        self.request_inner(
            kind,
            response_kind,
            class,
            operation,
            payload,
            deadline,
            None,
            Some(attempt),
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
        admitted: Option<&admission::Attempt>,
    ) -> Result<R> {
        let _timer = self.inner.metrics.timer();
        let mut uncertain = false;
        for attempt in 0..self.inner.config.max_reconnect_attempts {
            let frame = Id::new_v7()
                .map_err(Error::from)
                .and_then(|request| self.frame(kind, class, request, operation, payload, deadline))
                .map_err(|error| error.preserve_uncertainty(uncertain))?;
            let read_only = read_only_request(&frame, &self.inner.config.protocol);
            let critical = tidemq_protocol::framing::is_completion(kind)
                || tidemq_protocol::framing::is_core_control(&frame, &self.inner.config.protocol)?;
            let _execution = if admitted.is_none() && !critical {
                Some(
                    self.inner
                        .admission
                        .attempt(admission::frame_bytes(&self.inner.config, class), deadline)
                        .await
                        .map_err(|error| error.preserve_uncertainty(uncertain))?,
                )
            } else {
                None
            };
            self.inner.metrics.record_request();
            let rpc = if kind == FrameKind::GroupRead
                && let Some(native) = admitted.and_then(|attempt| attempt.native.as_ref())
            {
                native.rpc(self, deadline).await
            } else {
                self.rpc(class, deadline).await
            };
            let response = match rpc {
                Ok(rpc) => {
                    if !read_only {
                        crate::request::mark_sent();
                    }
                    match affinity {
                        Some(affinity) => rpc
                            .call_affinity(frame, deadline, affinity)
                            .await
                            .map_err(Error::from),
                        None => rpc.call(frame, deadline).await.map_err(Error::from),
                    }
                }
                Err(error) => Err(error),
            };
            let result = response
                .and_then(|response| self.decode(response, response_kind))
                .map_err(|error| error.read_only(read_only));
            if let Err(error) = &result {
                if error.outcome_unknown() {
                    self.inner.metrics.record_unknown();
                }
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
                        && kind != FrameKind::GroupWatch
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
        match self.close_administrations(deadline).await {
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

#[cfg(test)]
pub(crate) fn test_client(config: ClientConfig) -> Client {
    let key = rcgen::KeyPair::generate().unwrap();
    let certificate = rcgen::CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let mut client = Client::unconnected(
        config,
        Bytes::from(certificate.pem()),
        "test-token-with-at-least-32-characters".into(),
    )
    .unwrap();
    client._owner = None;
    client
}

async fn resolve(endpoint: &str, deadline: Instant) -> Result<Vec<SocketAddr>> {
    let addresses = timeout_at(deadline, tokio::net::lookup_host(endpoint))
        .await
        .map_err(|_| Error::Deadline)?
        .map_err(|source| Error::Resolve {
            endpoint: endpoint.to_owned(),
            source,
        })?;
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(32)
        .map_err(|_| Error::Admission {
            resource: "DNS candidate address allocation",
        })?;
    for address in addresses.take(32) {
        if !candidates.contains(&address) {
            candidates.push(address);
        }
    }
    candidates.sort_by_key(|address| !address.is_ipv4());
    if candidates.is_empty() {
        return Err(Error::Resolve {
            endpoint: endpoint.to_owned(),
            source: std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                "Endpoint resolved to no address",
            ),
        });
    }
    Ok(candidates)
}

fn read_only_request(frame: &Frame, limits: &ProtocolLimits) -> bool {
    use tidemq_protocol::public;
    match FrameKind::try_from(frame.kind) {
        Ok(
            FrameKind::GetTopic
            | FrameKind::GetTopicLayout
            | FrameKind::GetClusterRegistry
            | FrameKind::ConsoleRead
            | FrameKind::GetConsoleOperation
            | FrameKind::ListIdentities
            | FrameKind::GroupWatch,
        ) => true,
        Ok(FrameKind::ConsumerControl) => {
            decode_payload::<public::ConsumerControl>(frame.payload.clone(), limits).is_ok_and(
                |request| request.action == i32::from(public::ConsumerAction::ConsumerStatus),
            )
        }
        Ok(FrameKind::PublisherControl) => {
            decode_payload::<public::PublisherControl>(frame.payload.clone(), limits).is_ok_and(
                |request| request.action == i32::from(public::PublisherAction::PublisherStatus),
            )
        }
        Ok(FrameKind::SubscriptionAdmin) => decode_payload::<public::SubscriptionAdmin>(
            frame.payload.clone(),
            limits,
        )
        .is_ok_and(|request| {
            request.action == i32::from(public::SubscriptionAdminAction::InspectSubscription)
        }),
        Ok(FrameKind::ManageCluster) => {
            decode_payload::<public::ManageCluster>(frame.payload.clone(), limits).is_ok_and(
                |request| request.action == i32::from(public::ClusterAction::InspectNodeDrain),
            )
        }
        _ => false,
    }
}

fn bind_transport(
    config: &ClientConfig,
    trust_roots: Bytes,
    server_name: &str,
) -> Result<(tidemq_transport::Client, bool)> {
    let budget = MemoryBudget::new(MemoryLimits {
        total_bytes: config.transport_memory_bytes,
        control_reserved_bytes: config.transport_control_reserved_bytes,
    })
    .map_err(|source| Error::Memory { source })?;
    let tls = || ClientTls {
        trust_roots: trust_roots.clone(),
        identity: None,
        expected_nodes: Vec::new(),
    };
    let ipv4 = SocketAddr::from(([0, 0, 0, 0], 0));
    let prefer_dual = server_name.parse::<std::net::Ipv4Addr>().is_err();
    // One dual-stack endpoint keeps DNS address-family changes within the same budget and session.
    let result = if prefer_dual {
        match tidemq_transport::Client::bind(
            SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, 0)),
            tls(),
            config.transport.clone(),
            budget.clone(),
        ) {
            Ok(transport) => (transport, true),
            Err(_) => (
                tidemq_transport::Client::bind(ipv4, tls(), config.transport.clone(), budget)?,
                false,
            ),
        }
    } else {
        (
            tidemq_transport::Client::bind(ipv4, tls(), config.transport.clone(), budget)?,
            false,
        )
    };
    Ok(result)
}

#[cfg(test)]
mod console_query_tests {
    use super::*;

    #[test]
    fn console_reads_and_operation_lookups_are_queries_while_console_writes_are_mutations() {
        for (kind, read_only) in [
            (FrameKind::ConsoleRead, true),
            (FrameKind::GetConsoleOperation, true),
            (FrameKind::ConsoleWrite, false),
            (FrameKind::GroupWatch, true),
            (FrameKind::GroupRead, false),
            (FrameKind::GroupRetireRead, false),
        ] {
            let frame = Frame {
                kind: kind.into(),
                ..Default::default()
            };
            assert_eq!(
                read_only_request(&frame, &ProtocolLimits::default()),
                read_only
            );
        }
    }
}
