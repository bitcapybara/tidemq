use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use prometheus_client::metrics::gauge::Gauge;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{
    self,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject},
};
use tokio_util::task::TaskTracker;
use zeroize::Zeroizing;

use super::lifecycle::Metrics;
use crate::runtime::process::Process;
use crate::{Error, Result};

const MAX_CONNECTIONS: usize = 256;
const MAX_HANDSHAKES: usize = 32;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) async fn load(
    config: &crate::config::TlsConfig,
    public_origin: &str,
) -> Result<TlsAcceptor> {
    let certificates = super::certificate_chain(&config.certificate).await?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    verify_server_material(&certificates, public_origin, &provider)?;
    let key = Zeroizing::new(super::files::read_bounded(&config.private_key, 65_536).await?);
    let key = PrivateKeyDer::from_pem_slice(&key)
        .map_err(|_| invalid("console TLS private key PEM is invalid"))?;
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| invalid("console TLS protocol configuration is invalid"))?
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|_| invalid("console TLS certificate and private key must match"))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn verify_server_material(
    certificates: &[CertificateDer<'static>],
    public_origin: &str,
    provider: &rustls::crypto::CryptoProvider,
) -> Result<()> {
    let origin = reqwest::Url::parse(public_origin)
        .map_err(|_| invalid("console public origin is invalid"))?;
    let host = origin
        .host_str()
        .filter(|_| origin.scheme() == "https")
        .ok_or_else(|| invalid("console public origin must have an HTTPS host"))?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    let name =
        ServerName::try_from(host).map_err(|_| invalid("console public origin host is invalid"))?;
    let leaf = certificates
        .first()
        .ok_or_else(|| invalid("console TLS certificate chain must not be empty"))?;
    let certificate = rustls::server::ParsedCertificate::try_from(leaf)
        .map_err(|_| invalid("console TLS leaf certificate is invalid"))?;
    rustls::client::verify_server_name(&certificate, &name)
        .map_err(|_| invalid("console TLS certificate SAN must cover session.public_origin"))?;
    // Pinned WebPKI checks leaf validity, constraints and ServerAuth before issuer lookup.
    // Empty roots/intermediates isolate material checks without requiring a private CA root.
    let verified = rustls::client::verify_server_cert_signed_by_trust_anchor(
        &certificate,
        &rustls::RootCertStore::empty(),
        &[],
        UnixTime::now(),
        provider.signature_verification_algorithms.all,
    );
    match verified {
        Err(rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer)) => Ok(()),
        _ => Err(invalid(
            "console TLS leaf must be currently valid for server authentication",
        )),
    }
}

pub(crate) async fn certificate_chain(
    path: &std::path::Path,
) -> Result<Vec<CertificateDer<'static>>> {
    let pem = super::files::read_bounded(path, 1_048_576).await?;
    let certificates = CertificateDer::pem_slice_iter(&pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| invalid("console TLS certificate PEM is invalid"))?;
    if certificates.is_empty() {
        return Err(invalid("console TLS certificate chain must not be empty"));
    }
    let leaf = certificates
        .first()
        .ok_or_else(|| invalid("console TLS certificate chain must not be empty"))?;
    rustls::server::ParsedCertificate::try_from(leaf)
        .map_err(|_| invalid("console TLS leaf certificate is invalid"))?;
    Ok(certificates)
}

pub(super) async fn serve(
    listener: TcpListener,
    acceptor: Option<TlsAcceptor>,
    router: Router,
    mut shutdown: watch::Receiver<Option<Instant>>,
    process: Process,
    metrics: Arc<Metrics>,
    tracker: TaskTracker,
) -> Result<()> {
    let admission = Admission {
        connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        handshakes: Arc::new(Semaphore::new(MAX_HANDSHAKES)),
        tracker,
    };
    let mut tasks = JoinSet::new();
    let outcome = loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break Ok(()),
            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(source)) = result {
                    process.fail("console_connection_task");
                    break Err(Error::Task { source });
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(source) if !matches!(source.kind(), std::io::ErrorKind::InvalidInput | std::io::ErrorKind::NotConnected | std::io::ErrorKind::Unsupported) => {
                        metrics.connection_rejections.inc();
                        tokio::select! {
                            _ = shutdown.changed() => break Ok(()),
                            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                        }
                        continue;
                    }
                    Err(source) => {
                        process.fail("console_listener");
                        break Err(super::http_error(source));
                    }
                };
                spawn_connection(&mut tasks, stream, router.clone(), acceptor.clone(), shutdown.clone(), metrics.clone(), &admission);
            }
        }
    };
    drop(listener);
    if outcome.is_err() {
        tasks.abort_all();
    }
    let deadline = *shutdown.borrow();
    let drained = drain_connections(&mut tasks, deadline, &process).await;
    outcome.and(drained)
}

struct Admission {
    connections: Arc<Semaphore>,
    handshakes: Arc<Semaphore>,
    tracker: TaskTracker,
}

fn spawn_connection(
    tasks: &mut JoinSet<bool>,
    stream: TcpStream,
    router: Router,
    acceptor: Option<TlsAcceptor>,
    shutdown: watch::Receiver<Option<Instant>>,
    metrics: Arc<Metrics>,
    admission: &Admission,
) {
    let Ok(permit) = admission.connections.clone().try_acquire_owned() else {
        metrics.connection_rejections.inc();
        return;
    };
    let handshake = if acceptor.is_some() {
        match admission.handshakes.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                metrics.connection_rejections.inc();
                return;
            }
        }
    } else {
        None
    };
    tasks.spawn(admission.tracker.track_future(async move {
        let _permit = permit;
        let _connection = GaugeGuard::new(&metrics.connections);
        if let Some(acceptor) = acceptor {
            let handshake_gauge = GaugeGuard::new(&metrics.handshakes);
            if let Some(stream) =
                handshake_stream(stream, acceptor, shutdown.clone(), &metrics).await
            {
                drop(handshake_gauge);
                drop(handshake);
                serve_connection(stream, router, shutdown, &metrics).await
            } else {
                false
            }
        } else {
            serve_connection(stream, router, shutdown, &metrics).await
        }
    }));
}

async fn drain_connections(
    tasks: &mut JoinSet<bool>,
    deadline: Option<Instant>,
    process: &Process,
) -> Result<()> {
    let mut timed_out = false;
    while !tasks.is_empty() {
        if let Some(deadline) = deadline {
            match tokio::time::timeout_at(deadline, tasks.join_next()).await {
                Ok(Some(Ok(expired))) => timed_out |= expired,
                Ok(Some(Err(source))) if source.is_panic() => {
                    process.fail("console_connection_task");
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    return Err(Error::Task { source });
                }
                Err(_) => {
                    timed_out = true;
                    tasks.abort_all();
                    break;
                }
                _ => {}
            }
        } else {
            let _ = tasks.join_next().await;
        }
    }
    while tasks.join_next().await.is_some() {}
    if timed_out {
        return Err(invalid("console connection shutdown deadline elapsed"));
    }
    Ok(())
}

async fn handshake_stream(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    mut shutdown: watch::Receiver<Option<Instant>>,
    metrics: &Metrics,
) -> Option<tokio_rustls::server::TlsStream<TcpStream>> {
    tokio::select! {
        biased;
        _ = shutdown.changed() => None,
        result = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)) => {
            match result {
                Ok(Ok(stream)) => Some(stream),
                Ok(Err(_)) => { metrics.tls_errors.inc(); None }
                Err(_) => { metrics.tls_timeouts.inc(); None }
            }
        }
    }
}

async fn serve_connection<I>(
    stream: I,
    router: Router,
    mut shutdown: watch::Receiver<Option<Instant>>,
    metrics: &Metrics,
) -> bool
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(5));
    let connection =
        builder.serve_connection(TokioIo::new(stream), TowerToHyperService::new(router));
    tokio::pin!(connection);
    tokio::select! {
        result = &mut connection => {
            if result.is_err() { metrics.connection_errors.inc(); }
            false
        }
        _ = tokio::time::sleep(CONNECTION_TIMEOUT) => { metrics.connection_errors.inc(); false }
        _ = shutdown.changed() => {
            connection.as_mut().graceful_shutdown();
            let deadline = *shutdown.borrow();
            if let Some(deadline) = deadline {
                tokio::time::timeout_at(deadline, &mut connection).await.is_err()
            } else { false }
        }
    }
}

struct GaugeGuard(Gauge);

impl GaugeGuard {
    fn new(gauge: &Gauge) -> Self {
        gauge.inc();
        Self(gauge.clone())
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

fn invalid(reason: &'static str) -> Error {
    Error::Configuration { reason }
}
