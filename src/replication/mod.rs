//! OpenRaft storage-v2 integration for deterministic, bounded application state.
//!
//! Raft log indices are consensus positions and must never be used as message offsets.

mod log;
pub mod rpc;
mod snapshot;
mod snapshot_registry;
mod state_machine;
mod store;
mod types;

pub use snapshot::SnapshotFile;
pub use snapshot_registry::SnapshotDiagnostics;
pub use store::{DiskLog, DiskStateMachine, DiskStore, StorageDiagnostics, StorageOptions};
pub use types::{Application, GroupIdentity, StorageIdentity, TypeConfig};

pub type Raft<A> = openraft::Raft<TypeConfig<A>>;
pub type LogEntry<A> = openraft::Entry<TypeConfig<A>>;
