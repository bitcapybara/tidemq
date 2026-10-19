//! Process-owned listeners and authenticated semantic routing to durable authorities.

mod config;
mod data;
mod error;
mod management;
mod metadata;
mod node;
mod service;
mod telemetry;
pub(crate) mod wire;

use std::path::Path;

use bytes::Bytes;
use tokio::io::AsyncReadExt;

pub use config::{GroupConfig, PeerConfig, RuntimeConfig};
pub use error::{Error, Failure, Result};
pub use node::RunningNode;

pub(crate) async fn read_bounded(path: &Path, max: u64) -> Result<Bytes> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|source| Error::operation("open runtime file", source))?;
    let mut bytes = Vec::new();
    file.take(max.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| Error::operation("read runtime file", source))?;
    if bytes.len() as u64 > max {
        return Err(Error::invalid("Runtime file exceeds its finite bound"));
    }
    Ok(bytes.into())
}
