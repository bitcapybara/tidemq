//! Native HTTPS Console with process-owned connections and required asset supervision.

pub(crate) mod api;
mod backend;
mod api_identity;
mod api_query;
mod api_write;
mod assets;
mod config;
mod files;
mod http;
mod lifecycle;
mod observability;
pub mod session;
mod tls;

use std::net::SocketAddr;
use std::sync::Arc;

use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::task::TaskTracker;

use crate::runtime::process::{Phase, Process};
use crate::{Error, Result};

pub use config::{ClusterConfig, ConsoleConfig, ConsoleRole, EmbeddedConsoleConfig, SessionConfig};
pub(crate) use lifecycle::Metrics as ConsoleMetrics;
pub(crate) use tls::{certificate_chain, load as load_https_identity};

/// Reads required startup material without binding listeners or creating keys.
pub async fn validate_embedded(
    config: &EmbeddedConsoleConfig,
    tls: &crate::config::TlsConfig,
) -> Result<()> {
    config.validate()?;
    files::validate_embedded_resources(config).await?;
    session::SessionManager::load(&config.session).await?;
    load_https_identity(tls, &config.session.public_origin).await?;
    Ok(())
}

/// Validates standalone resources, remote trust, session keys, and native HTTPS.
pub async fn validate_config(config: &ConsoleConfig) -> Result<()> {
    config.validate()?;
    files::validate_remote_resources(config).await?;
    files::StaticFiles::load(&config.static_dir).await?;
    session::SessionManager::load(&config.session).await?;
    load_https_identity(&config.tls, &config.session.public_origin).await?;
    Ok(())
}

pub struct RunningConsole {
    pub address: SocketAddr,
    pub management_address: Option<SocketAddr>,
    shutdown: watch::Sender<Option<Instant>>,
    tasks: JoinSet<Result<()>>,
    state: Arc<http::StateData>,
    connections: TaskTracker,
}

impl RunningConsole {
    pub async fn start(config: ConsoleConfig) -> Result<Self> {
        config.validate()?;
        files::validate_remote_resources(&config).await?;
        let management = bind(config.management_listen).await?;
        let api = api::ApiState::load(&config).await?;
        let process = Process::default();
        process.recovering();
        let metrics = Arc::new(ConsoleMetrics::default());
        let mut registry = Registry::default();
        process.register(&mut registry);
        metrics.register(&mut registry);
        let embedded = EmbeddedConsoleConfig {
            listen: config.listen,
            static_dir: config.static_dir,
            session: config.session,
            observability: config.observability,
        };
        let mut running =
            Self::start_embedded(&embedded, &config.tls, api, process.clone(), metrics).await?;
        running.management_address = Some(management.local_addr().map_err(http_error)?);
        spawn_owned(
            &mut running.tasks,
            process.clone(),
            running.shutdown.subscribe(),
            "console_management",
            tls::serve(
                management,
                None,
                http::management_router(running.state.clone(), Arc::new(registry)),
                running.shutdown.subscribe(),
                process.clone(),
                running.state.metrics.clone(),
                running.connections.clone(),
            ),
        );
        process.ready();
        Ok(running)
    }

    pub(crate) async fn start_embedded(
        config: &EmbeddedConsoleConfig,
        tls_config: &crate::config::TlsConfig,
        api: Arc<api::ApiState>,
        process: Process,
        metrics: Arc<ConsoleMetrics>,
    ) -> Result<Self> {
        config.validate()?;
        let files = files::validate_embedded_resources(config).await?;
        let acceptor = load_https_identity(tls_config, &config.session.public_origin).await?;
        let public = bind(config.listen).await?;
        Self::assemble(public, files, api, acceptor, process, metrics)
    }

    fn assemble(
        public: TcpListener,
        files: files::StaticFiles,
        api: Arc<api::ApiState>,
        acceptor: tokio_rustls::TlsAcceptor,
        process: Process,
        metrics: Arc<ConsoleMetrics>,
    ) -> Result<Self> {
        let address = public.local_addr().map_err(http_error)?;
        let (state, failures) = http::StateData::build(files, Some(api), process.clone(), metrics);
        let (shutdown, receive) = watch::channel(None);
        let connections = TaskTracker::new();
        let mut tasks = JoinSet::new();
        spawn_owned(
            &mut tasks,
            process.clone(),
            receive.clone(),
            "console_https",
            tls::serve(
                public,
                Some(acceptor),
                http::public_router(state.clone()),
                receive.clone(),
                process.clone(),
                state.metrics.clone(),
                connections.clone(),
            ),
        );
        let assets = state.assets.clone();
        spawn_owned(
            &mut tasks,
            process,
            receive.clone(),
            "console_assets",
            async move {
                assets.run(failures, receive).await;
                Ok(())
            },
        );
        Ok(Self {
            address,
            management_address: None,
            shutdown,
            tasks,
            state,
            connections,
        })
    }

    pub async fn wait_for_exit(&mut self) -> Result<()> {
        let mut changed = self.state.process.subscribe();
        loop {
            if self.state.process.phase() == Phase::Failed {
                return Err(Error::Configuration {
                    reason: "a required Console or process dependency failed",
                });
            }
            tokio::select! {
                _ = changed.changed() => {},
                result = self.tasks.join_next() => {
                    let expected = self.shutdown.borrow().is_some()
                        || matches!(self.state.process.phase(), Phase::Draining | Phase::Stopped);
                    if !expected { self.state.process.fail("console_task"); }
                    if let Some(result) = result { result.map_err(|source| Error::Task { source })??; }
                    if expected { return Ok(()); }
                    return Err(Error::Configuration { reason: "console required task exited unexpectedly" });
                }
            }
        }
    }

    pub async fn shutdown_until(mut self, deadline: Instant) -> Result<()> {
        self.state.process.drain();
        self.shutdown.send_replace(Some(deadline));
        let mut failure = None;
        loop {
            match tokio::time::timeout_at(deadline, self.tasks.join_next()).await {
                Ok(Some(result)) => {
                    if let Err(error) = result
                        .map_err(|source| Error::Task { source })
                        .and_then(|result| result)
                    {
                        failure.get_or_insert(error);
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    failure.get_or_insert(Error::Configuration {
                        reason: "console shutdown deadline elapsed",
                    });
                    self.tasks.abort_all();
                    while self.tasks.join_next().await.is_some() {}
                    break;
                }
            }
        }
        self.connections.close();
        self.connections.wait().await;
        if failure.is_some() {
            self.state.process.fail("console_shutdown");
        }
        if self.management_address.is_some() {
            self.state.process.stopped();
        }
        failure.map_or(Ok(()), Err)
    }
}

impl Drop for RunningConsole {
    fn drop(&mut self) {
        if self.shutdown.borrow().is_none() {
            self.state.process.fail("console_owner");
            self.shutdown.send_replace(Some(Instant::now()));
        }
    }
}

async fn bind(address: SocketAddr) -> Result<TcpListener> {
    TcpListener::bind(address).await.map_err(http_error)
}

fn spawn_owned(
    tasks: &mut JoinSet<Result<()>>,
    process: Process,
    shutdown: watch::Receiver<Option<Instant>>,
    dependency: &'static str,
    future: impl std::future::Future<Output = Result<()>> + Send + 'static,
) {
    tasks.spawn(async move {
        let _guard = lifecycle::TaskGuard {
            process,
            shutdown,
            dependency,
        };
        future.await
    });
}

fn http_error(source: std::io::Error) -> Error {
    Error::Io {
        operation: "run console HTTP listener",
        path: "console".into(),
        source,
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tls_tests;
