//! Durable object publication, replacement, pins and reclamation.

mod backend;
mod catalog;
mod coordinator;
mod data;
pub(crate) mod remote;
pub use remote::ReplicaCheckpoint;
mod telemetry;
mod types;

pub use backend::MetadataReplica;
pub use catalog::Catalog;
pub use coordinator::{CompactionStep, Coordinator, DataReplica, GuardedPage, PreparedObject};
pub use data::DataState;
pub use telemetry::LifecycleMetrics;
pub use types::VerifiedColdDelivery;
pub use types::{Blocker, Diagnostic, Manifest, ObjectPhase, ObjectRecord, RangeRequest, ReadPin};
pub use types::{CapacityUsage, ObjectPage, PinPage};
pub use types::{DataOperation, DataResult, MetadataOperation, MetadataResult};

pub use types::Rejection;
pub(crate) use types::{CatalogCommand, DataCommand};

use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Object lifecycle configuration is invalid"))]
    Configuration,
    #[snafu(display("Object lifecycle operation exceeded its deadline"))]
    Deadline,
    #[snafu(display(
        "Object lifecycle consensus outcome is unknown; reconcile the original object identity"
    ))]
    Unknown,
    #[snafu(display("Object lifecycle operation is blocked: {reason:?}"))]
    Rejected { reason: Rejection },
    #[snafu(display("Object lifecycle consensus operation failed: {reason}"))]
    Consensus { reason: String },
    #[snafu(display("Object lifecycle storage is unavailable"))]
    Storage { source: crate::wal::Error },
    #[snafu(display("Object lifecycle control authorization failed"))]
    Authority {
        source: crate::ownership::AuthorityError,
    },
    #[snafu(display("Object lifecycle control authorization expired"))]
    Lease {
        source: crate::ownership::LeaseError,
    },
    #[snafu(display("Object lifecycle S3 request failed"))]
    Object { source: crate::object_store::Error },
    #[snafu(display("Object lifecycle archive validation failed"))]
    Archive { source: crate::archive::Error },
    #[snafu(display("Object lifecycle worker failed"))]
    Worker { source: tokio::task::JoinError },
    #[snafu(display("Object lifecycle admission is exhausted"))]
    Overloaded,
    #[snafu(display("Object lifecycle wire request is invalid"))]
    Wire,
    #[snafu(display("Object lifecycle peer is not authorized"))]
    Authorization,
    #[snafu(display("Object lifecycle allocation failed"))]
    Allocation {
        source: std::collections::TryReserveError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests;
