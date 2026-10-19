use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use s2n_quic::provider::io::{Provider as IoProvider, tokio::Provider as NativeIo};
use s2n_quic_core::{endpoint::Endpoint, inet::SocketAddress};
use tokio::{
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use crate::{Error, Result, error::setup};

type Task = (JoinHandle<()>, SocketAddr);

pub(super) struct Owner(Arc<Mutex<Option<Task>>>);

pub(super) struct Provider {
    native: NativeIo,
    task: Arc<Mutex<Option<Task>>>,
}

impl Owner {
    pub(super) fn bind(address: SocketAddr) -> Result<(Self, Provider)> {
        let native = NativeIo::builder()
            .with_receive_address(address)
            .and_then(|builder| builder.build())
            .map_err(|source| setup("configure owned native I/O", source))?;
        let task = Arc::new(Mutex::new(None));
        Ok((Self(task.clone()), Provider { native, task }))
    }

    pub(super) async fn shutdown(&self, deadline: Instant) -> Result<()> {
        let task = self.0.lock().map_err(|_| Error::Closed)?.take();
        let Some((mut task, address)) = task else {
            return Ok(());
        };
        // Upstream's default adapter discards this handle; connection close may retain 3*PTO.
        task.abort();
        let joined = timeout_at(deadline, &mut task)
            .await
            .map_err(|_| Error::Deadline {
                operation: "stop native endpoint I/O",
            })?;
        if let Err(source) = joined
            && !source.is_cancelled()
        {
            return Err(setup("join native endpoint I/O", source));
        }
        release_socket(address, deadline).await
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        if let Ok(mut task) = self.0.lock()
            && let Some((task, _)) = task.take()
        {
            task.abort();
        }
    }
}

impl IoProvider for Provider {
    type PathHandle = <NativeIo as IoProvider>::PathHandle;
    type Error = std::io::Error;

    fn start<E: Endpoint<PathHandle = Self::PathHandle>>(
        self,
        endpoint: E,
    ) -> std::io::Result<SocketAddress> {
        let (task, address) = self.native.start(endpoint)?;
        let Ok(mut slot) = self.task.lock() else {
            task.abort();
            return Err(std::io::Error::other("Native I/O ownership is poisoned"));
        };
        *slot = Some((task, address.into()));
        Ok(address)
    }
}

async fn release_socket(address: SocketAddr, deadline: Instant) -> Result<()> {
    timeout_at(deadline, async {
        let mut delay = Duration::from_millis(1);
        loop {
            // Ring closure wakes upstream's auxiliary RX/TX tasks; require their sockets to close.
            match tokio::net::UdpSocket::bind(address).await {
                Ok(socket) => {
                    drop(socket);
                    return Ok(());
                }
                Err(source) if source.kind() == std::io::ErrorKind::AddrInUse => {
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2).min(Duration::from_millis(32));
                }
                Err(source) => {
                    return Err(Error::Io {
                        operation: "verify native socket release",
                        source,
                    });
                }
            }
        }
    })
    .await
    .map_err(|_| Error::Deadline {
        operation: "release native endpoint socket",
    })?
}
