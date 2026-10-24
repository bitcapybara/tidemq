//! Deterministic control-plane state. Raft owns persistence and authoritative reads.
//!
//! The serialized command is a Raft storage format, never an unrestricted wire RPC.
//! In particular, ownership evidence must enter through the verified authority adapters.

mod auth;
mod command;
mod consumer_delete;
mod consumers;
mod control;
mod data_membership;
mod diagnostics;
mod members;
mod metadata_membership;
mod nodes;
mod policies;
mod publishers;
mod quotas;
mod replicated;
mod resource_leases;
mod scaling;
mod state;
mod tenant_limits;
mod topic_delete;
mod topology;
mod transfer_state;
mod transfers;
mod types;
mod validation;

#[cfg(test)]
mod tests;

pub use auth::{
    AuthAction, AuthCommand, AuthOutcome, AuthRejection, AuthResult, AuthState, Credential,
    Namespace, Permission, Principal, Tenant, TokenDigest,
};
pub(crate) use command::Action;
pub use command::{Command, OperationResult, Outcome, Rejection, Response};
pub use consumer_delete::ConsumerDeletion;
pub use consumers::{ConsumerCommand, ConsumerOutcome, ConsumerSubscription};
pub(crate) use control::{ControlStage, grant_operation_id, stage_operation_id};
pub use diagnostics::{MetadataSummary, OwnershipDiagnostic, TopicPage, TopicSummary};
pub use members::{
    MEMBER_LEASE_MS, MEMBER_RECOVERY_MS, MemberHeartbeat, MemberInfo, MemberJoin, MemberStatus,
    MembersBook, MembersPlan,
};
pub use metadata_membership::MetadataChange;
pub use nodes::{
    ClusterAction, ClusterCommand, ClusterOutcome, ClusterResult, NodeIdentity, RegisteredNode,
    RegistryPage,
};
pub use policies::QuotaPolicy;
pub use publishers::{
    PUBLISHER_LEASE_MS, PUBLISHER_RECOVERY_MS, PublisherCommand, PublisherRecord, PublishersBook,
};
pub use quotas::{
    AllocationRequest, QuotaAllocation, QuotaCapacity, QuotaCommand, QuotaGrant, QuotaLedger,
};
pub use resource_leases::{
    RESOURCE_LEASE_MS, ResourceLease, ResourceLeaseCommand, ResourceLeaseTarget, ResourceLeases,
};
pub use state::MetadataState;
pub use tenant_limits::TenantLimits;
pub use topic_delete::{TopicDeletion, TopicDeletionCommand};
pub use topology::{BirthShard, LayoutChange, LayoutPlan, LayoutStage, Topology};
pub use transfers::{
    AuthorityTransfer, HistoricalBinding, ImportEvidence, MAX_AUTHORITY_TRANSFERS,
    MAX_TRANSFER_BYTES, TRANSFER_PAGE_BYTES, TransferCheckpoint, TransferCommand, TransferManifest,
    TransferStage, TransferStep,
};
pub use types::{
    DataGroup, DeploymentMode, MetadataLimits, NodeRecord, NodeRole, NodeSpec, Ordering,
    PartitionKey, RecordStatus, Route, SubscriptionConfig, SubscriptionRecord, TopicRecord,
};
