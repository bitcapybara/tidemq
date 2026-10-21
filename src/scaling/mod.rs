//! Bounded migration scheduling and evidence-driven consensus membership changes.

mod budget;
mod drain;
mod membership;
mod placement;
mod pressure;
pub mod remote;
mod telemetry;
mod types;

pub use budget::{RepairBudget, RepairDiagnostic, RepairLease, RepairLimits};
pub use drain::{DrainActions, DrainPlan, DrainProgress, Handoff};
pub use membership::{MembershipApplication, MembershipCoordinator, StableMembership};
pub use placement::{
    BlockReason, Capacity, Placement, PlacementDiagnostic, PlacementPolicy, Replacement,
};
pub use pressure::{PressureSample, PressureSampler};
pub use telemetry::{ScalingMetrics, Stage};
pub(crate) use types::MembershipEvidence;
pub use types::{MembershipPlan, MigrationRecord};

use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Scaling configuration or plan is invalid"))]
    Configuration,
    #[snafu(display("Scaling is blocked by placement, capacity, or cooldown"))]
    Placement,
    #[snafu(display("Scaling capacity is blocked: {reason}"))]
    Capacity { reason: BlockReason },
    #[snafu(display("Scaling admission is exhausted"))]
    Overloaded,
    #[snafu(display("Scaling operation exceeded its deadline"))]
    Deadline,
    #[snafu(display("Scaling consensus outcome is unknown; reconcile the original plan"))]
    Unknown,
    #[snafu(display("Scaling consensus result requires reconciliation"))]
    Consensus {
        source: Box<
            openraft::error::RaftError<
                u64,
                openraft::error::ClientWriteError<u64, openraft::BasicNode>,
            >,
        >,
    },
    #[snafu(display("Scaling quorum read failed"))]
    Quorum {
        source: Box<
            openraft::error::RaftError<
                u64,
                openraft::error::CheckIsLeaderError<u64, openraft::BasicNode>,
            >,
        >,
    },
    #[snafu(display("Scaling plan was rejected: {reason}"))]
    Rejected { reason: crate::metadata::Rejection },
    #[snafu(display("Scaling application storage is unavailable"))]
    Storage { source: crate::wal::Error },
    #[snafu(display("Scaling authority is unavailable or does not match the stable topology"))]
    Authority,
    #[snafu(display("Scaling peer transport is unreachable"))]
    Unreachable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Scaling trusted peer request failed"))]
    Remote {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Scaling native snapshot observation failed"))]
    Snapshot {
        source: Box<openraft::StorageError<u64>>,
    },
    #[snafu(display("Scaling snapshot file observation failed"))]
    SnapshotIo { source: std::io::Error },
    #[snafu(display("Scaling pressure observation is unavailable"))]
    Observation { source: std::io::Error },
    #[snafu(display("Scaling observation worker failed"))]
    Worker { source: tokio::task::JoinError },
    #[snafu(display("Scaling state lock is poisoned"))]
    Poisoned,
}

pub type Result<T> = std::result::Result<T, Error>;
