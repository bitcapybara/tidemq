//! Stateless HTTP process; all MQ authority remains behind authenticated public APIs.

pub(crate) mod api;
mod api_identity;
mod api_query;
mod api_write;
mod backend;
mod config;
mod files;
mod http;
mod observability;
pub mod session;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::{Error, Result};

pub use config::{ClusterConfig, ConsoleConfig, ConsoleRole, EmbeddedConsoleConfig, SessionConfig};

pub struct RunningConsole {
    pub address: SocketAddr,
    pub management_address: SocketAddr,
    shutdown: watch::Sender<bool>,
    tasks: JoinSet<Result<()>>,
    state: Arc<http::StateData>,
}

impl RunningConsole {
    pub async fn start(config: ConsoleConfig) -> Result<Self> {
        config.validate()?;
        let files = files::validate_resources(&config).await?;
        let public = bind(config.listen).await?;
        let management = bind(config.management_listen).await?;
        let address = public.local_addr().map_err(http_error)?;
        let management_address = management.local_addr().map_err(http_error)?;
        let api = api::ApiState::load(&config).await?;
        let state = http::StateData::with_api(files, api);
        let (shutdown, receive) = watch::channel(false);
        let mut tasks = JoinSet::new();
        spawn_server(
            &mut tasks,
            public,
            http::public_router(state.clone()),
            receive.clone(),
        );
        spawn_server(
            &mut tasks,
            management,
            http::management_router(state.clone()),
            receive,
        );
        Ok(Self {
            address,
            management_address,
            shutdown,
            tasks,
            state,
        })
    }

    pub async fn wait_for_exit(&mut self) -> Result<()> {
        if let Some(result) = self.tasks.join_next().await {
            result.map_err(|source| Error::Task { source })??;
        }
        Err(Error::Configuration {
            reason: "console HTTP listener exited unexpectedly",
        })
    }

    pub async fn shutdown_until(mut self, deadline: Instant) -> Result<()> {
        self.state.ready.store(false, Ordering::Release);
        let _ = self.shutdown.send(true);
        loop {
            match tokio::time::timeout_at(deadline, self.tasks.join_next()).await {
                Ok(Some(result)) => result.map_err(|source| Error::Task { source })??,
                Ok(None) => return Ok(()),
                Err(_) => {
                    self.tasks.abort_all();
                    return Err(Error::Configuration {
                        reason: "console HTTP shutdown deadline elapsed",
                    });
                }
            }
        }
    }
}

async fn bind(address: SocketAddr) -> Result<TcpListener> {
    TcpListener::bind(address).await.map_err(http_error)
}

fn spawn_server(
    tasks: &mut JoinSet<Result<()>>,
    listener: TcpListener,
    router: Router,
    mut shutdown: watch::Receiver<bool>,
) {
    tasks.spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                if !*shutdown.borrow() {
                    let _ = shutdown.changed().await;
                }
            })
            .await
            .map_err(http_error)
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
