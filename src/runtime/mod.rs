//! Process-owned listeners and authenticated semantic routing to durable authorities.

mod auth;
mod authority_transfer;
mod config;
mod consumer_control;
mod consumer_data;
mod consumer_delete;
mod consumer_read;
mod data;
mod drain;
mod error;
mod failover;
mod identity;
mod maintenance;
mod management;
mod metadata;
mod node;
mod objects;
pub(crate) mod physical;
mod publishing;
pub(crate) mod scaling;
mod service;
mod telemetry;
mod tenant_quota;
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub mod test_hooks;
mod topic_delete;
mod topology;
pub(crate) mod wire;

use std::path::Path;

use bytes::Bytes;
use tokio::io::AsyncReadExt;

pub use config::{GroupConfig, ObjectConfig, PeerConfig, RuntimeConfig};
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

pub(crate) async fn read_secret(path: &Path, max: u64) -> Result<Bytes> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|source| Error::operation("open private runtime file", source))?;
    let metadata = file
        .metadata()
        .await
        .map_err(|source| Error::operation("inspect private runtime file", source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::invalid(
                "Secret files must have owner-only permissions",
            ));
        }
    }
    if !metadata.is_file() || metadata.len() > max {
        return Err(Error::invalid(
            "Private runtime file is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(max.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| Error::operation("read private runtime file", source))?;
    if bytes.len() as u64 > max {
        return Err(Error::invalid(
            "Private runtime file exceeds its finite bound",
        ));
    }
    Ok(bytes.into())
}
