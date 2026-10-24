use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use crate::ownership::{FenceEvidence, Grant, RecoveryEvidence};

use super::{
    DataGroup, DeploymentMode, MetadataLimits, NodeRecord, NodeSpec, PartitionKey, Route,
    SubscriptionConfig, SubscriptionRecord, TopicRecord,
};

/// Only Raft storage and trusted in-process dispatchers deserialize this type.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub(crate) operation_id: Id,
    pub(crate) expected_revision: u64,
    pub(crate) action: Action,
    pub(crate) management_binding: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum Action {
    Cluster(super::ClusterCommand),
    Transfer(super::TransferCommand),
    Bootstrap {
        cluster_id: Id,
        metadata_group_id: u64,
        mode: DeploymentMode,
        limits: MetadataLimits,
    },
    PutNode {
        spec: NodeSpec,
        expected_generation: u64,
    },
    CreateGroup {
        group_id: u64,
        voters: BTreeSet<u64>,
    },
    CreateTopic {
        topic_id: Id,
        name: String,
        groups: Vec<u64>,
    },
    RetireTopic {
        topic_id: Id,
        expected_generation: u64,
    },
    PutSubscription {
        subscription_id: Id,
        topic_id: Id,
        name: String,
        expected_generation: u64,
        config: SubscriptionConfig,
    },
    RetireSubscription {
        subscription_id: Id,
        expected_generation: u64,
    },
    GrantOwnership {
        partition: PartitionKey,
        owner_node: u64,
        expected_epoch: u64,
        expected_route_generation: u64,
    },
    ObserveFence(FenceEvidence),
    ObserveRecovery(RecoveryEvidence),
    ActivateRoute(RecoveryEvidence),
    PrepareMembership {
        plan: crate::scaling::MembershipPlan,
        copy_bytes: u64,
    },
    CompleteMembership(crate::scaling::MembershipEvidence),
    Lifecycle(crate::lifecycle::MetadataOperation),
    PrepareLayout {
        topic: Id,
        revision: u64,
        change: super::LayoutChange,
    },
    ObserveLayoutSeal {
        plan: Id,
        source: Id,
        end: u64,
    },
    ActivateLayout {
        plan: Id,
    },
    Identity(super::AuthCommand),
    Consumer(super::ConsumerCommand),
    Publisher(super::PublisherCommand),
    TopicDeletion(super::TopicDeletionCommand),
    Quota(super::QuotaCommand),
    ResourceLease(super::ResourceLeaseCommand),
    ObserveQuotaPolicy {
        policy: Id,
        now_ms: u64,
    },
}

impl Command {
    pub(crate) fn bind_management(mut self, digest: [u8; 32]) -> Self {
        self.management_binding = Some(digest);
        self
    }
    pub fn cluster(operation: Id, expected_revision: u64, command: super::ClusterCommand) -> Self {
        Self::new(operation, expected_revision, Action::Cluster(command))
    }
    pub fn observe_quota_policy(operation: Id, revision: u64, policy: Id, now_ms: u64) -> Self {
        Self::new(
            operation,
            revision,
            Action::ObserveQuotaPolicy { policy, now_ms },
        )
    }
    pub fn resource_lease(
        operation: Id,
        revision: u64,
        command: super::ResourceLeaseCommand,
    ) -> Self {
        Self::new(operation, revision, Action::ResourceLease(command))
    }
    pub fn quota(operation: Id, expected_revision: u64, command: super::QuotaCommand) -> Self {
        Self::new(operation, expected_revision, Action::Quota(command))
    }
    pub fn topic_deletion(
        operation: Id,
        expected_revision: u64,
        command: super::TopicDeletionCommand,
    ) -> Self {
        Self::new(operation, expected_revision, Action::TopicDeletion(command))
    }
    pub fn transfer(
        operation: Id,
        expected_revision: u64,
        command: super::TransferCommand,
    ) -> Self {
        Self::new(operation, expected_revision, Action::Transfer(command))
    }
    pub fn publisher(
        operation: Id,
        expected_revision: u64,
        command: super::PublisherCommand,
    ) -> Self {
        Self::new(operation, expected_revision, Action::Publisher(command))
    }
    pub fn consumer(
        operation: Id,
        expected_revision: u64,
        command: super::ConsumerCommand,
    ) -> Self {
        Self::new(operation, expected_revision, Action::Consumer(command))
    }
    pub fn bootstrap(
        operation_id: Id,
        cluster_id: Id,
        metadata_group_id: u64,
        mode: DeploymentMode,
        limits: MetadataLimits,
    ) -> Self {
        Self::new(
            operation_id,
            0,
            Action::Bootstrap {
                cluster_id,
                metadata_group_id,
                mode,
                limits,
            },
        )
    }

    pub fn put_node(
        operation_id: Id,
        expected_revision: u64,
        spec: NodeSpec,
        expected_generation: u64,
    ) -> Self {
        Self::new(
            operation_id,
            expected_revision,
            Action::PutNode {
                spec,
                expected_generation,
            },
        )
    }

    pub fn create_group(
        operation_id: Id,
        expected_revision: u64,
        group_id: u64,
        voters: BTreeSet<u64>,
    ) -> Self {
        Self::new(
            operation_id,
            expected_revision,
            Action::CreateGroup { group_id, voters },
        )
    }

    pub fn create_topic(
        operation_id: Id,
        expected_revision: u64,
        topic_id: Id,
        name: String,
        groups: Vec<u64>,
    ) -> Self {
        Self::new(
            operation_id,
            expected_revision,
            Action::CreateTopic {
                topic_id,
                name,
                groups,
            },
        )
    }

    pub fn retire_topic(
        operation_id: Id,
        expected_revision: u64,
        topic_id: Id,
        expected_generation: u64,
    ) -> Self {
        Self::new(
            operation_id,
            expected_revision,
            Action::RetireTopic {
                topic_id,
                expected_generation,
            },
        )
    }

    pub fn put_subscription(
        operation_id: Id,
        expected_revision: u64,
        subscription_id: Id,
        topic_id: Id,
        name: String,
        expected_generation: u64,
        config: SubscriptionConfig,
    ) -> Self {
        Self::new(
            operation_id,
            expected_revision,
            Action::PutSubscription {
                subscription_id,
                topic_id,
                name,
                expected_generation,
                config,
            },
        )
    }

    pub fn retire_subscription(
        operation_id: Id,
        expected_revision: u64,
        subscription_id: Id,
        expected_generation: u64,
    ) -> Self {
        Self::new(
            operation_id,
            expected_revision,
            Action::RetireSubscription {
                subscription_id,
                expected_generation,
            },
        )
    }

    pub fn grant_ownership(
        expected_revision: u64,
        cluster_id: Id,
        partition: PartitionKey,
        owner_node: u64,
        expected_epoch: u64,
        expected_route_generation: u64,
    ) -> Result<Self, Rejection> {
        let target_epoch = expected_epoch
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let operation_id = super::control::grant_operation_id(
            cluster_id,
            partition,
            target_epoch,
            owner_node,
            expected_route_generation,
        )?;
        Ok(Self::new(
            operation_id,
            expected_revision,
            Action::GrantOwnership {
                partition,
                owner_node,
                expected_epoch,
                expected_route_generation,
            },
        ))
    }

    pub fn operation_id(&self) -> Id {
        self.operation_id
    }

    pub fn identity(operation: Id, expected_revision: u64, command: super::AuthCommand) -> Self {
        Self::new(operation, expected_revision, Action::Identity(command))
    }

    pub fn prepare_layout(
        operation: Id,
        expected_revision: u64,
        topic: Id,
        revision: u64,
        change: super::LayoutChange,
    ) -> Self {
        Self::new(
            operation,
            expected_revision,
            Action::PrepareLayout {
                topic,
                revision,
                change,
            },
        )
    }

    /// The trusted coordinator must obtain the immutable end from the source Raft leader.
    pub fn observe_layout_seal(
        operation: Id,
        expected_revision: u64,
        plan: Id,
        source: Id,
        end: u64,
    ) -> Self {
        Self::new(
            operation,
            expected_revision,
            Action::ObserveLayoutSeal { plan, source, end },
        )
    }

    pub fn activate_layout(operation: Id, expected_revision: u64, plan: Id) -> Self {
        Self::new(
            operation,
            expected_revision,
            Action::ActivateLayout { plan },
        )
    }

    pub(crate) fn new(operation_id: Id, expected_revision: u64, action: Action) -> Self {
        Self {
            operation_id,
            expected_revision,
            action,
            management_binding: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Rejection {
    Uninitialized,
    AlreadyInitialized,
    InvalidLimit,
    InvalidName,
    InvalidNode,
    InvalidGroup,
    InvalidSubscription,
    InvalidEvidence,
    InvalidCommand,
    Capacity,
    RevisionConflict,
    GenerationConflict,
    MemberEpochConflict { expected: u64 },
    OperationConflict,
    IdentityRetired,
    NameInUse,
    NotFound,
    NodeInUse,
    NotSchedulable,
    PlacementViolation,
    Superseded,
    RecoveryRequired,
    CounterExhausted,
    Lifecycle(crate::lifecycle::Rejection),
    Authorization(super::AuthRejection),
    SubscriptionInUse,
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self {
            Self::SubscriptionInUse => {
                "Subscription still has online members or an unfinished membership handoff"
            }
            Self::Authorization(reason) => return write!(formatter, "{reason}"),
            Self::Uninitialized => "Metadata group has not been bootstrapped",
            Self::AlreadyInitialized => "Metadata group has already been bootstrapped",
            Self::InvalidLimit => "Metadata request exceeds its configured item or byte limit",
            Self::InvalidName => {
                "Resource name violates its ASCII component or length rules; a topic path must be tenant/namespace/topic"
            }
            Self::InvalidNode => {
                "Node configuration violates the node identity or role constraints"
            }
            Self::InvalidGroup => {
                "Data group configuration violates the group identity or replica constraints"
            }
            Self::InvalidSubscription => {
                "Subscription configuration violates its topic or delivery mode constraints"
            }
            Self::InvalidEvidence => {
                "Ownership or recovery evidence does not match the committed state"
            }
            Self::InvalidCommand => {
                "Metadata command does not match the requested state transition"
            }
            Self::Capacity => "Metadata identity or operation-result capacity is exhausted",
            Self::RevisionConflict => {
                "Metadata revision changed; reread the current revision before retrying"
            }
            Self::GenerationConflict => {
                "Resource generation changed; reread the current resource before retrying"
            }
            Self::MemberEpochConflict { expected } => {
                return write!(
                    formatter,
                    "Member epoch was allocated to another member; next epoch is {expected}"
                );
            }
            Self::OperationConflict => {
                "Operation identity was already used for a different metadata command"
            }
            Self::IdentityRetired => "Resource identity has been retired and cannot be reused",
            Self::NameInUse => {
                "Resource name is already assigned to a different immutable identity"
            }
            Self::NotFound => "The requested metadata resource does not exist",
            Self::NodeInUse => "Node still hosts a group and cannot be removed",
            Self::NotSchedulable => "Selected node is not schedulable",
            Self::PlacementViolation => {
                "Replica placement violates the configured replication or zone constraints"
            }
            Self::Superseded => "Operation was superseded by a newer committed operation",
            Self::RecoveryRequired => {
                "Group recovery must complete before this transition can proceed"
            }
            Self::CounterExhausted => "Metadata counter cannot advance without overflow",
            Self::Lifecycle(reason) => {
                return write!(
                    formatter,
                    "Object lifecycle rejected the metadata transition: {reason}"
                );
            }
        };
        formatter.write_str(reason)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Outcome {
    Cluster(Box<super::ClusterResult>),
    QuotaPolicy(super::QuotaPolicy),
    ResourceLease(Option<super::ResourceLease>),
    Quota(super::QuotaGrant),
    TopicDeletion(super::TopicDeletion),
    AuthorityTransfer(Box<super::AuthorityTransfer>),
    Bootstrapped,
    NodeConfigured(NodeRecord),
    GroupCreated(DataGroup),
    TopicConfigured(TopicRecord),
    SubscriptionConfigured(SubscriptionRecord),
    OwnershipGranted(Grant),
    FenceObserved,
    RecoveryObserved,
    RouteActivated(Route),
    MembershipPrepared(crate::scaling::MembershipPlan),
    MembershipCompleted(crate::scaling::MembershipPlan),
    Lifecycle(crate::lifecycle::MetadataResult),
    LayoutPrepared(super::LayoutPlan),
    LayoutActivated(tidemq_protocol::topic::TopicLayout),
    Identity(super::AuthResult),
    Consumer(super::ConsumerOutcome),
    Publisher(crate::producer::PublisherPermit),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Response {
    Applied { revision: u64, outcome: Outcome },
    Rejected { revision: u64, reason: Rejection },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationResult {
    Known(Response),
    UnknownOrRetired,
}

impl Default for Response {
    fn default() -> Self {
        Self::Rejected {
            revision: 0,
            reason: Rejection::Uninitialized,
        }
    }
}

impl Response {
    pub fn is_applied(&self) -> bool {
        matches!(self, Self::Applied { .. })
    }
}
