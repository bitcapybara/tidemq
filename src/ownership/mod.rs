//! Ownership grants, durable fences, and process-local control validity.

mod authority;
mod fence;
mod lease;
pub(crate) mod remote;
mod telemetry;
mod types;

#[cfg(test)]
mod tests;

pub use authority::{
    DataAuthority, Error as AuthorityError, MetadataAuthority, OwnershipApplication, VerifiedFence,
    VerifiedGrant, VerifiedRecovery,
};
pub use fence::{FenceCommand, FenceRejection, FenceResponse, FenceState};
pub use lease::{
    ControlLease, ControlPolicy, Error as LeaseError, Finalization, FinalizationKind,
    LeaseDiagnostic, ValidityReason, Work,
};
pub use telemetry::{OwnershipMetrics, Transition, TransitionOutcome, emit_transition};
pub use types::{
    CommitPosition, FenceEvidence, FenceRecord, Grant, OwnershipPhase, RecoveredOwnership,
    RecoveryCheckpoint, RecoveryEvidence,
};

#[cfg(test)]
pub(crate) use lease::test_lease;
