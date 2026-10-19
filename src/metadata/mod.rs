//! Deterministic control-plane state. Raft owns persistence and authoritative reads.
//!
//! The serialized command is a Raft storage format, never an unrestricted wire RPC.
//! In particular, ownership evidence must enter through the verified authority adapters.

mod command;
mod control;
mod diagnostics;
mod replicated;
mod state;
mod types;
mod validation;

#[cfg(test)]
mod tests;

pub(crate) use command::Action;
pub use command::{Command, OperationResult, Outcome, Rejection, Response};
pub(crate) use control::{ControlStage, grant_operation_id, stage_operation_id};
pub use diagnostics::{MetadataSummary, OwnershipDiagnostic, TopicPage, TopicSummary};
pub use state::MetadataState;
pub use types::{
    DataGroup, DeploymentMode, MetadataLimits, NodeRecord, NodeRole, NodeSpec, Ordering,
    PartitionKey, RecordStatus, Route, SubscriptionConfig, SubscriptionRecord, TopicRecord,
};
