//! Ordered application execution through local durable storage or real Raft consensus.

mod error;
mod group;
mod local;
mod options;
mod storage;
mod types;
mod worker;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tracing_tests;

pub use error::{Error, Result};
pub use group::{GroupHandle, GroupState};
pub use local::{LocalGroup, LocalHealth, LocalStatistics};
pub use options::LocalOptions;
pub use storage::LocalSnapshotReader;
pub use types::{
    Application, Applied, ExecutionPosition, GroupIdentity, SnapshotInfo, StorageIdentity,
};
