use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use s2n_quic::{
    Connection as QuicConnection,
    provider::{
        endpoint_limits::{ConnectionAttempt, Limiter, Outcome},
        event::{Subscriber, events},
        tls::s2n_tls,
    },
};
use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    framing::{ALPN, decode_payload, new_frame},
    public::HelloResult,
};
use tokio::{
    sync::{Mutex, RwLock, Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
    time::{Instant, timeout_at},
};

use crate::budget::{MemoryBudget, MemoryPermit};

use super::{
    Authorization, ClientCredentials, Error, FramedStream, NodeGrant, PeerTarget, Result,
    ServerAuthorization, Session, StreamPurpose, TransportConfig, TransportMetrics,
    error::setup,
    frame::{FrameAdmission, read_wire},
    session::{ConnectionState, start_session},
    telemetry::add,
};

#[derive(Clone)]
pub struct TlsIdentity {
    certificate: Bytes,
    private_key: Bytes,
}

impl TlsIdentity {
    pub fn new(certificate_pem: Bytes, private_key_pem: Bytes) -> Result<Self> {
        if certificate_pem.is_empty()
            || certificate_pem.len() > 65_536
            || private_key_pem.is_empty()
            || private_key_pem.len() > 16_384
        {
            return Err(Error::Configuration {
                reason: "TLS PEM identity is empty or excessive",
            });
        }
        pem_text(&certificate_pem)?;
        pem_text(&private_key_pem)?;
        Ok(Self {
            certificate: certificate_pem,
            private_key: private_key_pem,
        })
    }
}

pub struct ServerTls {
    pub identity: TlsIdentity,
    pub client_trust_roots: Option<Bytes>,
}
pub struct ClientTls {
    pub trust_roots: Bytes,
    pub identity: Option<TlsIdentity>,
    pub expected_nodes: Vec<NodeGrant>,
}

#[derive(Clone, Default)]
struct TlsEvidence {
    completed: bool,
    peer_pin: Option<[u8; 32]>,
    recovery: Option<super::RecoverySnapshot>,
}
struct EvidenceSubscriber(Arc<TransportMetrics>);

struct InternalClientNames(Vec<String>);
impl s2n_tls::callbacks::VerifyHostNameCallback for InternalClientNames {
    fn verify_host_name(&self, name: &str) -> bool {
        self.0
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(name))
    }
}

impl Subscriber for EvidenceSubscriber {
    type ConnectionContext = TlsEvidence;
    fn create_connection_context(
        &mut self,
        _: &events::ConnectionMeta,
        _: &events::ConnectionInfo,
    ) -> Self::ConnectionContext {
        TlsEvidence::default()
    }
    fn on_tls_exporter_ready(
        &mut self,
        context: &mut Self::ConnectionContext,
        _: &events::ConnectionMeta,
        event: &events::TlsExporterReady,
    ) {
        context.completed = true;
        // This pinned upstream API is doc-hidden; mTLS pin tests are required on every upgrade.
        context.peer_pin = event
            .session
            .peer_cert_chain_der()
            .ok()
            .and_then(|chain| chain.first().map(|leaf| NodeGrant::certificate_pin(leaf)));
    }
    fn on_tls_handshake_failed(
        &mut self,
        _: &mut Self::ConnectionContext,
        _: &events::ConnectionMeta,
        _: &events::TlsHandshakeFailed,
    ) {
        add(&self.0.tls_failed, 1);
    }
    fn on_packet_lost(
        &mut self,
        _: &mut Self::ConnectionContext,
        _: &events::ConnectionMeta,
        _: &events::PacketLost,
    ) {
        add(&self.0.packets_lost, 1);
    }
    fn on_frame_sent(
        &mut self,
        _: &mut Self::ConnectionContext,
        _: &events::ConnectionMeta,
        event: &events::FrameSent,
    ) {
        match &event.frame {
            events::Frame::DataBlocked { .. } => add(&self.0.data_blocked, 1),
            events::Frame::StreamDataBlocked { .. } => add(&self.0.stream_data_blocked, 1),
            events::Frame::StreamsBlocked { .. } => add(&self.0.streams_blocked, 1),
            _ => {}
        }
    }
    fn on_recovery_metrics(
        &mut self,
        context: &mut Self::ConnectionContext,
        _: &events::ConnectionMeta,
        event: &events::RecoveryMetrics,
    ) {
        context.recovery = Some(super::RecoverySnapshot {
            min_rtt_us: u64::try_from(event.min_rtt.as_micros()).unwrap_or(u64::MAX),
            smoothed_rtt_us: u64::try_from(event.smoothed_rtt.as_micros()).unwrap_or(u64::MAX),
            latest_rtt_us: u64::try_from(event.latest_rtt.as_micros()).unwrap_or(u64::MAX),
            pto_count: event.pto_count,
            congestion_window_bytes: event.congestion_window,
            bytes_in_flight: event.bytes_in_flight,
            congestion_limited: event.congestion_limited,
        });
    }
}

struct EndpointLimits {
    connections: usize,
    handshakes: usize,
}

struct CloseOnDrop(Option<s2n_quic::connection::Handle>);
impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.close(0x103_u32.into());
        }
    }
}
impl Limiter for EndpointLimits {
    fn on_connection_attempt(&mut self, attempt: &ConnectionAttempt) -> Outcome {
        if attempt.connection_count >= self.connections {
            Outcome::close()
        } else if attempt.inflight_handshakes >= self.handshakes {
            Outcome::retry()
        } else {
            Outcome::allow()
        }
    }
}

pub struct Server {
    address: SocketAddr,
    incoming: mpsc::Receiver<Arc<Session>>,
    task: JoinHandle<()>,
    pub metrics: Arc<TransportMetrics>,
}

impl Server {
    // Publish the listener only after its TLS, authorization, and resource policy all succeed.
    pub fn bind(
        address: SocketAddr,
        tls: ServerTls,
        authorization: ServerAuthorization,
        config: TransportConfig,
        budget: MemoryBudget,
    ) -> Result<Self> {
        config.validate()?;
        authorization.validate()?;
        let metrics = Arc::new(TransportMetrics::default());
        let mut tls_builder = s2n_tls::Server::builder()
            .with_application_protocols([ALPN])
            .map_err(|source| setup("set server ALPN", source))?
            .with_certificate(
                pem_text(&tls.identity.certificate)?,
                pem_text(&tls.identity.private_key)?,
            )
            .map_err(|source| setup("load server identity", source))?;
        if authorization.internal() {
            let roots = tls.client_trust_roots.ok_or(Error::Configuration {
                reason: "internal listeners require client trust roots",
            })?;
            validate_roots(&roots)?;
            tls_builder = tls_builder
                .with_empty_trust_store()
                .map_err(|source| setup("clear server trust", source))?
                .with_trusted_certificate(pem_text(&roots)?)
                .map_err(|source| setup("load client trust roots", source))?
                .with_client_authentication()
                .map_err(|source| setup("require mutual TLS", source))?;
            if let ServerAuthorization::Internal(grants) = &authorization {
                tls_builder = tls_builder
                    .with_verify_host_name_callback(InternalClientNames(
                        grants
                            .iter()
                            .map(|grant| grant.certificate_name.clone())
                            .collect(),
                    ))
                    .map_err(|source| setup("configure client certificate names", source))?;
            }
        }
        tls_builder
            .config_mut()
            .set_max_cert_chain_depth(8)
            .map_err(|source| setup("bound client certificate chain depth", source))?;
        let endpoint = s2n_quic::Server::builder()
            .with_tls(
                tls_builder
                    .build()
                    .map_err(|source| setup("build server TLS", source))?,
            )
            .map_err(|source| setup("configure server TLS", source))?
            .with_io(address)
            .map_err(|source| setup("bind server socket", source))?
            .with_limits(config.limits()?)
            .map_err(|source| setup("configure server limits", source))?
            .with_endpoint_limits(EndpointLimits {
                connections: config.max_connections,
                handshakes: config.max_handshakes,
            })
            .map_err(|source| setup("configure endpoint admission", source))?
            .with_event(EvidenceSubscriber(metrics.clone()))
            .map_err(|source| setup("configure TLS evidence", source))?
            .start()
            .map_err(|source| setup("start server", source))?;
        let address = endpoint.local_addr().map_err(|source| Error::Io {
            operation: "read server address",
            source,
        })?;
        let (sender, incoming) = mpsc::channel(config.max_connections);
        let task = tokio::spawn(server_loop(
            endpoint,
            sender,
            Arc::new(authorization),
            Arc::new(config),
            budget,
            metrics.clone(),
        ));
        Ok(Self {
            address,
            incoming,
            task,
            metrics,
        })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    pub async fn accept(&mut self) -> Option<Arc<Session>> {
        self.incoming.recv().await
    }
    /// Stops accepting and cancels active handshakes; callers drain/close accepted sessions first.
    pub async fn shutdown(mut self) -> Result<()> {
        self.task.abort();
        let _ = (&mut self.task).await;
        let mut address = self.address;
        let bind: SocketAddr = if address.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()
        .map_err(|_| Error::Configuration {
            reason: "shutdown socket address is invalid",
        })?;
        if address.ip().is_unspecified() {
            address.set_ip(if address.is_ipv4() {
                std::net::Ipv4Addr::LOCALHOST.into()
            } else {
                std::net::Ipv6Addr::LOCALHOST.into()
            });
        }
        // s2n-quic 1.91 acceptor Drop does not wake idle I/O; a local datagram releases it.
        let deadline = super::policy::deadline_after(std::time::Duration::from_secs(1))?;
        timeout_at(deadline, async {
            let socket = tokio::net::UdpSocket::bind(bind).await?;
            socket.send_to(&[0], address).await?;
            Ok::<_, std::io::Error>(())
        })
        .await
        .map_err(|_| Error::Deadline {
            operation: "wake idle server endpoint for shutdown",
        })?
        .map_err(|source| Error::Io {
            operation: "wake idle server endpoint for shutdown",
            source,
        })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct PoolKey {
    target: PeerTarget,
    scope: [u8; 32],
    session: Id,
    class: i32,
}
type PoolSlot = Arc<Mutex<Option<Arc<Session>>>>;

pub struct Client {
    endpoint: s2n_quic::Client,
    config: Arc<TransportConfig>,
    budget: MemoryBudget,
    expected_nodes: Vec<NodeGrant>,
    has_identity: bool,
    slots: Arc<Semaphore>,
    pool: Mutex<HashMap<PoolKey, PoolSlot>>,
    closed: AtomicBool,
    lifecycle: RwLock<()>,
    sessions: std::sync::Mutex<Vec<Weak<Session>>>,
    pub metrics: Arc<TransportMetrics>,
}

impl Client {
    // Construct the native endpoint once so certificate and transport settings cannot diverge.
    pub fn bind(
        address: SocketAddr,
        tls: ClientTls,
        config: TransportConfig,
        budget: MemoryBudget,
    ) -> Result<Self> {
        config.validate()?;
        validate_roots(&tls.trust_roots)?;
        let metrics = Arc::new(TransportMetrics::default());
        if tls.expected_nodes.len() > 128 {
            return Err(Error::Configuration {
                reason: "server authorization list is excessive",
            });
        }
        super::auth::validate_node_grants(&tls.expected_nodes)?;
        let has_identity = tls.identity.is_some();
        let mut tls_builder = s2n_tls::Client::builder()
            .with_application_protocols([ALPN])
            .map_err(|source| setup("set client ALPN", source))?
            .with_empty_trust_store()
            .map_err(|source| setup("clear client trust", source))?
            .with_certificate(pem_text(&tls.trust_roots)?)
            .map_err(|source| setup("load server trust roots", source))?;
        if let Some(identity) = tls.identity {
            tls_builder = tls_builder
                .with_client_identity(
                    pem_text(&identity.certificate)?,
                    pem_text(&identity.private_key)?,
                )
                .map_err(|source| setup("load client identity", source))?;
        }
        tls_builder = tls_builder
            .with_max_cert_chain_depth(8)
            .map_err(|source| setup("bound server certificate chain depth", source))?;
        let endpoint = s2n_quic::Client::builder()
            .with_tls(
                tls_builder
                    .build()
                    .map_err(|source| setup("build client TLS", source))?,
            )
            .map_err(|source| setup("configure client TLS", source))?
            .with_io(address)
            .map_err(|source| setup("bind client socket", source))?
            .with_limits(config.limits()?)
            .map_err(|source| setup("configure client limits", source))?
            .with_event(EvidenceSubscriber(metrics.clone()))
            .map_err(|source| setup("configure client evidence", source))?
            .start()
            .map_err(|source| setup("start client", source))?;
        Ok(Self {
            endpoint,
            slots: Arc::new(Semaphore::new(config.max_connections)),
            config: Arc::new(config),
            budget,
            expected_nodes: tls.expected_nodes,
            has_identity,
            pool: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            lifecycle: RwLock::new(()),
            sessions: std::sync::Mutex::new(Vec::new()),
            metrics,
        })
    }

    /// One independently authenticated physical connection per target/role/credential/session/class.
    pub async fn pooled(
        &self,
        target: PeerTarget,
        credentials: &ClientCredentials,
        class: TrafficClass,
        deadline: Instant,
    ) -> Result<Arc<Session>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        target.validate()?;
        let key = PoolKey {
            target: target.clone(),
            scope: *credentials.digest()?.as_bytes(),
            session: credentials.session(),
            class: class.into(),
        };
        let slot = {
            let mut pool = self.pool.lock().await;
            if !pool.contains_key(&key) && pool.len() >= self.config.max_connections {
                pool.retain(|_, slot| match slot.try_lock() {
                    Ok(entry) => entry
                        .as_ref()
                        .is_some_and(|session| !session.connection().is_closed()),
                    Err(_) => true,
                });
                if pool.len() >= self.config.max_connections {
                    return Err(Error::Admission {
                        resource: "connection pool keys",
                    });
                }
            }
            pool.entry(key)
                .or_insert_with(|| Arc::new(Mutex::new(None)))
                .clone()
        };
        let mut entry = timeout_at(deadline, slot.lock())
            .await
            .map_err(|_| Error::Deadline {
                operation: "wait for pooled connection",
            })?;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if let Some(session) = entry
            .as_ref()
            .filter(|session| !session.connection().is_closed())
        {
            return Ok(session.clone());
        }
        *entry = None;
        let session = self.connect(target, credentials, class, deadline).await?;
        *entry = Some(session.clone());
        Ok(session)
    }

    pub async fn connect(
        &self,
        target: PeerTarget,
        credentials: &ClientCredentials,
        class: TrafficClass,
        deadline: Instant,
    ) -> Result<Arc<Session>> {
        let _connecting = timeout_at(deadline, self.lifecycle.read())
            .await
            .map_err(|_| Error::Deadline {
                operation: "wait for client lifecycle",
            })?;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        target.validate()?;
        credentials.digest()?;
        if class == TrafficClass::Unspecified
            || (credentials.internal() && (!self.has_identity || self.expected_nodes.is_empty()))
            || (credentials.internal() && (target.cluster_id.is_none() || target.node_id.is_none()))
        {
            return Err(Error::Configuration {
                reason: "internal clients require an identity and an authorized server pin",
            });
        }
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Admission {
                resource: "client connection slots",
            })?;
        let memory = self
            .budget
            .try_reserve(class, self.config.transport_reservation()?)
            .map_err(|source| Error::Memory { source })?;
        let deadline = deadline.min(super::policy::deadline_after(
            self.config.handshake_timeout,
        )?);
        let result = timeout_at(
            deadline,
            self.connect_inner(target, credentials, class, memory, slot, deadline),
        )
        .await;
        match result {
            Ok(Ok(session)) => {
                let mut sessions = self.sessions.lock().map_err(|_| Error::Closed)?;
                sessions.retain(|entry| entry.strong_count() != 0);
                if let Err(source) = sessions.try_reserve(1) {
                    session.close();
                    return Err(Error::Allocation { source });
                }
                sessions.push(Arc::downgrade(&session));
                Ok(session)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => {
                add(&self.metrics.deadlines, 1);
                Err(Error::Deadline {
                    operation: "connect and authenticate",
                })
            }
        }
    }

    /// Closes cached and direct sessions, then waits for native endpoint connections to drain.
    pub async fn close(&self, deadline: Instant) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        let _closing = timeout_at(deadline, self.lifecycle.write())
            .await
            .map_err(|_| Error::Deadline {
                operation: "drain client connection attempts",
            })?;
        let sessions = {
            let mut sessions = self.sessions.lock().map_err(|_| Error::Closed)?;
            std::mem::take(&mut *sessions)
        };
        for session in sessions.iter().filter_map(Weak::upgrade) {
            session.close();
        }
        let mut pool =
            timeout_at(deadline, self.pool.lock())
                .await
                .map_err(|_| Error::Deadline {
                    operation: "drain client connection cache",
                })?;
        pool.clear();
        drop(pool);
        for session in sessions.iter().filter_map(Weak::upgrade) {
            session.shutdown(deadline).await?;
        }
        let mut endpoint = self.endpoint.clone();
        timeout_at(deadline, endpoint.wait_idle())
            .await
            .map_err(|_| Error::Deadline {
                operation: "drain native client endpoint",
            })?
            .map_err(|source| Error::Connection {
                operation: "drain native client endpoint",
                source,
            })
    }

    // Keep authentication and initial lanes under one close-on-failure transaction.
    async fn connect_inner(
        &self,
        target: PeerTarget,
        credentials: &ClientCredentials,
        class: TrafficClass,
        memory: MemoryPermit,
        slot: tokio::sync::OwnedSemaphorePermit,
        deadline: Instant,
    ) -> Result<Arc<Session>> {
        let connect = s2n_quic::client::Connect::new(target.address)
            .with_server_name(target.server_name.as_str());
        let mut connection =
            self.endpoint
                .connect(connect)
                .await
                .map_err(|source| Error::Connection {
                    operation: "TLS connect",
                    source,
                })?;
        let mut closing = CloseOnDrop(Some(connection.handle()));
        let authorization = verify_server(
            &connection,
            credentials,
            &self.expected_nodes,
            class,
            &target,
        )?;
        let stream = connection
            .open_bidirectional_stream()
            .await
            .map_err(|source| Error::Connection {
                operation: "open HELLO stream",
                source,
            })?;
        let (mut receive, mut send) = stream.split();
        let timeout_ms = remaining_ms(deadline)?;
        let hello = credentials.hello(class, timeout_ms, &self.config.protocol)?;
        let (hello_bytes, _) = super::frame::encode_charged(
            &hello,
            FrameAdmission::Handshake(Some(class)),
            &self.config,
            &self.budget,
            deadline,
        )?;
        send.send(hello_bytes)
            .await
            .map_err(|source| Error::Stream {
                operation: "send HELLO",
                source,
            })?;
        let result = read_wire(
            &mut receive,
            FrameAdmission::Handshake(Some(class)),
            &self.config,
            &self.budget,
            &self.metrics,
            deadline,
        )
        .await?
        .ok_or(Error::Closed)?;
        validate_hello_result(result.frame(), &hello, credentials.session(), &self.config)?;
        let (handle, acceptor) = connection.split();
        let state = ConnectionState::new(
            handle,
            class,
            authorization,
            credentials.session(),
            self.config.clone(),
            self.budget.clone(),
            self.metrics.clone(),
            memory,
            Some(slot),
        );
        let purpose = initial_purpose(class);
        let initial = FramedStream::from_halves(receive, send, state.admit(purpose)?);
        let session = start_session(state, acceptor, initial);
        if class == TrafficClass::Control {
            for _ in 1..self.config.core_streams {
                let stream = session
                    .connection()
                    .open(StreamPurpose::Core, deadline)
                    .await?;
                session.add_initial(stream).await;
            }
        }
        add(&self.metrics.connections, 1);
        closing.0 = None;
        tracing::debug!(
            traffic_class = crate::budget::class_name(class),
            "QUIC connection authenticated"
        );
        Ok(session)
    }
}

async fn server_loop(
    mut endpoint: s2n_quic::Server,
    sender: mpsc::Sender<Arc<Session>>,
    authorization: Arc<ServerAuthorization>,
    config: Arc<TransportConfig>,
    budget: MemoryBudget,
    metrics: Arc<TransportMetrics>,
) {
    let mut handshakes = JoinSet::new();
    loop {
        tokio::select! {
            _ = sender.closed() => break,
            _ = handshakes.join_next(), if !handshakes.is_empty() => {},
            next = endpoint.accept(), if handshakes.len() < config.max_handshakes => {
                let Some(connection) = next else { break; };
                let authorization = authorization.clone(); let config = config.clone(); let budget = budget.clone(); let metrics = metrics.clone(); let sender = sender.clone();
                handshakes.spawn(async move {
                    let Ok(deadline) = super::policy::deadline_after(config.handshake_timeout) else { add(&metrics.rejected, 1); return; };
                    match timeout_at(deadline, accept_connection(connection, authorization, config, budget, metrics.clone(), deadline)).await {
                        Ok(Ok(session)) => { if let Err(error) = sender.try_send(session) { error.into_inner().close(); add(&metrics.rejected, 1); } }
                        _ => { add(&metrics.rejected, 1); }
                    }
                });
            }
        }
    }
}

// The close guard must cover every bootstrap stage before ownership reaches the session pump.
async fn accept_connection(
    mut connection: QuicConnection,
    authorization: Arc<ServerAuthorization>,
    config: Arc<TransportConfig>,
    budget: MemoryBudget,
    metrics: Arc<TransportMetrics>,
    deadline: Instant,
) -> Result<Arc<Session>> {
    let mut closing = CloseOnDrop(Some(connection.handle()));
    let handshake_memory = budget
        .try_reserve_handshake(config.transport_reservation()?)
        .map_err(|source| Error::Memory { source })?;
    let evidence = connection
        .query_event_context(|context: &TlsEvidence| context.clone())
        .map_err(|_| Error::Authentication {
            reason: "TLS evidence context unavailable",
        })?;
    if !evidence.completed || connection.application_protocol().ok().as_deref() != Some(ALPN) {
        return Err(Error::Authentication {
            reason: "TLS handshake or ALPN not verified",
        });
    }
    let stream = connection
        .accept_bidirectional_stream()
        .await
        .map_err(|source| Error::Connection {
            operation: "accept HELLO stream",
            source,
        })?
        .ok_or(Error::Closed)?;
    let (mut receive, mut send) = stream.split();
    let hello = read_wire(
        &mut receive,
        FrameAdmission::Handshake(None),
        &config,
        &budget,
        &metrics,
        deadline,
    )
    .await?
    .ok_or(Error::Closed)?;
    let (session_id, class, peer) =
        authorization.authorize(hello.frame(), evidence.peer_pin, &config.protocol)?;
    let memory = budget
        .try_reserve(class, config.transport_reservation()?)
        .map_err(|source| Error::Memory { source })?;
    drop(handshake_memory);
    let request =
        Id::from_slice(&hello.frame().request_id).map_err(|source| Error::Protocol { source })?;
    let response = new_frame(
        FrameKind::HelloResult,
        class,
        request,
        session_id,
        &HelloResult {
            session_id: Bytes::copy_from_slice(session_id.as_bytes()),
            max_frame_bytes: config.protocol.max_frame_bytes,
            max_batch_messages: config.protocol.max_batch_messages,
            max_in_flight_bytes: u64::from(config.connection_window_bytes),
        },
        remaining_ms(deadline)?,
        &config.protocol,
    )
    .map_err(|source| Error::Protocol { source })?;
    let (response_bytes, _) = super::frame::encode_charged(
        &response,
        FrameAdmission::Handshake(Some(class)),
        &config,
        &budget,
        deadline,
    )?;
    send.send(response_bytes)
        .await
        .map_err(|source| Error::Stream {
            operation: "send HELLO result",
            source,
        })?;
    drop(hello);
    let (handle, acceptor) = connection.split();
    let state = ConnectionState::new(
        handle,
        class,
        peer,
        session_id,
        config,
        budget,
        metrics.clone(),
        memory,
        None,
    );
    let initial = FramedStream::from_halves(receive, send, state.admit(initial_purpose(class))?);
    add(&metrics.connections, 1);
    let session = start_session(state, acceptor, initial);
    closing.0 = None;
    Ok(session)
}

fn verify_server(
    connection: &QuicConnection,
    credentials: &ClientCredentials,
    expected: &[NodeGrant],
    class: TrafficClass,
    target: &PeerTarget,
) -> Result<Authorization> {
    let evidence = connection
        .query_event_context(|context: &TlsEvidence| context.clone())
        .map_err(|_| Error::Authentication {
            reason: "server TLS evidence unavailable",
        })?;
    if !evidence.completed || connection.application_protocol().ok().as_deref() != Some(ALPN) {
        return Err(Error::Authentication {
            reason: "server handshake or ALPN not verified",
        });
    }
    if credentials.internal() {
        let expected = expected
            .iter()
            .find(|expected| {
                evidence.peer_pin == Some(expected.certificate_pin)
                    && target.cluster_id == Some(expected.cluster_id)
                    && target.node_id == Some(expected.node_id)
                    && expected.role == target.remote_role
                    && expected
                        .certificate_name
                        .eq_ignore_ascii_case(&target.server_name)
                    && expected.allowed_classes.contains(&class)
            })
            .ok_or(Error::Authentication {
                reason: "server certificate pin, role, or class rejected",
            })?;
        if let ClientCredentials::Internal {
            cluster_id, role, ..
        } = credentials
            && (*cluster_id != expected.cluster_id || *role != target.local_role)
        {
            return Err(Error::Authorization {
                reason: "local role or cluster differs from the expected peer",
            });
        }
        Ok(Authorization::Internal(expected.clone()))
    } else {
        Ok(Authorization::Server)
    }
}

fn validate_hello_result(
    frame: &tidemq_protocol::common::Frame,
    hello: &tidemq_protocol::common::Frame,
    session: Id,
    config: &TransportConfig,
) -> Result<()> {
    if FrameKind::try_from(frame.kind).ok() != Some(FrameKind::HelloResult)
        || frame.request_id != hello.request_id
        || frame.operation_id != hello.operation_id
    {
        return Err(Error::Authentication {
            reason: "HELLO result binding is invalid",
        });
    }
    let result: HelloResult = decode_payload(frame.payload.clone(), &config.protocol)
        .map_err(|source| Error::Protocol { source })?;
    if result.session_id.as_ref() != session.as_bytes()
        || result.max_frame_bytes == 0
        || result.max_batch_messages == 0
        || result.max_in_flight_bytes == 0
    {
        return Err(Error::Authentication {
            reason: "HELLO limits or session are invalid",
        });
    }
    Ok(())
}

fn initial_purpose(class: TrafficClass) -> StreamPurpose {
    if class == TrafficClass::Control {
        StreamPurpose::Core
    } else {
        StreamPurpose::Request
    }
}
fn remaining_ms(deadline: Instant) -> Result<u64> {
    u64::try_from(
        deadline
            .saturating_duration_since(Instant::now())
            .as_millis(),
    )
    .ok()
    .filter(|remaining| *remaining > 0)
    .ok_or(Error::Deadline {
        operation: "authenticate",
    })
}
fn validate_roots(roots: &[u8]) -> Result<()> {
    if roots.is_empty() || roots.len() > 1_048_576 {
        return Err(Error::Configuration {
            reason: "trust roots are empty or excessive",
        });
    }
    pem_text(roots)?;
    Ok(())
}

pub(super) fn recovery_snapshot(
    handle: &s2n_quic::connection::Handle,
) -> Result<Option<super::RecoverySnapshot>> {
    handle
        .query_event_context(|context: &TlsEvidence| context.recovery.clone())
        .map_err(|_| Error::Closed)
}

fn pem_text(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes).map_err(|_| Error::Configuration {
        reason: "TLS PEM input must be UTF-8 text",
    })
}
