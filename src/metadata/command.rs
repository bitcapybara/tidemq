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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum Action {
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
}

impl Command {
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

    pub(crate) fn new(operation_id: Id, expected_revision: u64, action: Action) -> Self {
        Self {
            operation_id,
            expected_revision,
            action,
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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Outcome {
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
