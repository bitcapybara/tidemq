use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use openraft::error::{CheckIsLeaderError, RaftError};
use openraft::{BasicNode, LogId};
use snafu::{ResultExt, Snafu};
use tidemq_protocol::Id;
use tokio::sync::Semaphore;

use crate::metadata::{
    Action, Command, ControlStage, MetadataState, NodeRole, RecordStatus, stage_operation_id,
};
use crate::replication::{Application, DiskStateMachine, Raft};

use super::{
    CommitPosition, ControlLease, ControlPolicy, FenceCommand, FenceEvidence, FenceState, Grant,
    LeaseError, RecoveryEvidence,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Ownership authority configuration is invalid"))]
    InvalidConfiguration,
    #[snafu(display("The authoritative linearizable read failed"))]
    LinearizableRead {
        source: Box<RaftError<u64, CheckIsLeaderError<u64, BasicNode>>>,
    },
    #[snafu(display("The authority read exceeded its validity window"))]
    ReadDeadline { source: tokio::time::error::Elapsed },
    #[snafu(display("The authority view belongs to a different cluster or group"))]
    IdentityMismatch,
    #[snafu(display("Actual Raft voters do not match the authorized stable topology"))]
    MembershipMismatch,
    #[snafu(display("The requested ownership grant is absent, retired, or superseded"))]
    GrantMismatch,
    #[snafu(display("The committed fence is absent or does not match the grant"))]
    FenceRequired,
    #[snafu(display("Authoritative producer and ACK recovery is not complete"))]
    RecoveryRequired,
    #[snafu(display("The route is not active for this ownership grant"))]
    RouteRequired,
    #[snafu(display("The applied view has no committed consensus position"))]
    MissingCommitPosition,
    #[snafu(display("Control validity cannot be established"))]
    Validity { source: LeaseError },
}

/// An application supplies its actual durable ownership state, not caller booleans.
pub trait OwnershipApplication: Application {
    fn ownership_state(&self) -> Option<&FenceState>;
}

/// Local authoritative adapter. A remote implementation must additionally authenticate
/// the configured metadata identity and prohibit cached responses and raw command RPCs.
pub struct MetadataAuthority {
    raft: Raft<MetadataState>,
    state: DiskStateMachine<MetadataState>,
    cluster_id: Id,
    group_id: u64,
    policy: ControlPolicy,
    finalizations: Arc<Semaphore>,
}

impl MetadataAuthority {
    pub fn new(
        raft: Raft<MetadataState>,
        state: DiskStateMachine<MetadataState>,
        cluster_id: Id,
        group_id: u64,
        policy: ControlPolicy,
    ) -> Result<Self, Error> {
        policy.validate().context(ValiditySnafu)?;
        if group_id == 0 {
            return Err(Error::InvalidConfiguration);
        }
        Ok(Self {
            raft,
            state,
            cluster_id,
            group_id,
            policy,
            finalizations: Arc::new(Semaphore::new(policy.max_finalizations)),
        })
    }

    pub async fn verify_grant(&self, expected: &Grant) -> Result<VerifiedGrant, Error> {
        let (grant, started) = self
            .read(|state, _| {
                let grant = state
                    .grant(expected.partition)
                    .filter(|grant| *grant == expected)
                    .ok_or(Error::GrantMismatch)?;
                if state
                    .topic(expected.partition.topic_id)
                    .is_none_or(|topic| topic.status != RecordStatus::Active)
                {
                    return Err(Error::GrantMismatch);
                }
                Ok(grant.clone())
            })
            .await?;
        Ok(VerifiedGrant {
            grant,
            freshness: Freshness::new(started, self.policy)?,
        })
    }

    pub async fn active_lease(&self, fence: &super::FenceRecord) -> Result<ControlLease, Error> {
        let ((grant, generation), started) = self
            .read(|state, _| {
                let grant = state
                    .grant(fence.grant.partition)
                    .filter(|grant| *grant == &fence.grant)
                    .ok_or(Error::GrantMismatch)?;
                let route = state
                    .route(grant.partition)
                    .filter(|route| {
                        route.epoch == grant.epoch
                            && route.owner_node == grant.owner_node
                            && route.ownership_operation == grant.operation_id
                    })
                    .ok_or(Error::RouteRequired)?;
                Ok((grant.clone(), route.generation))
            })
            .await?;
        ControlLease::from_verified(
            grant,
            generation,
            started,
            self.policy,
            self.finalizations.clone(),
        )
        .context(ValiditySnafu)
    }

    async fn read<R: Send>(
        &self,
        query: impl FnOnce(&MetadataState, Option<LogId<u64>>) -> Result<R, Error> + Send,
    ) -> Result<(R, Instant), Error> {
        let started = Instant::now();
        let result = tokio::time::timeout(self.policy.ttl, async {
            self.raft
                .ensure_linearizable()
                .await
                .map_err(|source| Error::LinearizableRead {
                    source: Box::new(source),
                })?;
            let voters = stable_voters(&self.raft)?;
            self.state
                .with_applied(|state, position| {
                    validate_metadata_identity(state, self.cluster_id, self.group_id, &voters)?;
                    query(state, position)
                })
                .await
        })
        .await
        .context(ReadDeadlineSnafu)??;
        Ok((result, started))
    }
}

/// Authoritative reads from the owning data group, after a real quorum barrier.
pub struct DataAuthority<A: OwnershipApplication> {
    raft: Raft<A>,
    state: DiskStateMachine<A>,
    cluster_id: Id,
    group_id: u64,
    expected_voters: BTreeSet<u64>,
    policy: ControlPolicy,
}

impl<A: OwnershipApplication> DataAuthority<A> {
    pub fn new(
        raft: Raft<A>,
        state: DiskStateMachine<A>,
        cluster_id: Id,
        group_id: u64,
        expected_voters: BTreeSet<u64>,
        policy: ControlPolicy,
    ) -> Result<Self, Error> {
        policy.validate().context(ValiditySnafu)?;
        if group_id == 0 || !matches!(expected_voters.len(), 1 | 3) || expected_voters.contains(&0)
        {
            return Err(Error::InvalidConfiguration);
        }
        Ok(Self {
            raft,
            state,
            cluster_id,
            group_id,
            expected_voters,
            policy,
        })
    }

    pub async fn verify_fence(&self, expected: &Grant) -> Result<VerifiedFence, Error> {
        let (evidence, started) = self
            .read(|state, position| {
                let fence = state
                    .fence(expected.partition)
                    .filter(|fence| fence.grant == *expected)
                    .ok_or(Error::FenceRequired)?;
                Ok(FenceEvidence {
                    fence: fence.clone(),
                    committed: commit_position(position)?,
                })
            })
            .await?;
        Ok(VerifiedFence {
            evidence,
            freshness: Freshness::new(started, self.policy)?,
        })
    }

    pub async fn verify_recovery(&self, expected: &Grant) -> Result<VerifiedRecovery, Error> {
        let (evidence, started) = self
            .read(|state, position| {
                let recovered = state
                    .recovered(expected.partition)
                    .filter(|recovered| {
                        recovered.fence.grant == *expected
                            && recovered.checkpoint.validate(&recovered.fence)
                    })
                    .ok_or(Error::RecoveryRequired)?;
                Ok(RecoveryEvidence {
                    recovered,
                    committed: commit_position(position)?,
                })
            })
            .await?;
        Ok(VerifiedRecovery {
            evidence,
            freshness: Freshness::new(started, self.policy)?,
        })
    }

    async fn read<R: Send>(
        &self,
        query: impl FnOnce(&FenceState, Option<LogId<u64>>) -> Result<R, Error> + Send,
    ) -> Result<(R, Instant), Error> {
        let started = Instant::now();
        let result = tokio::time::timeout(self.policy.ttl, async {
            self.raft
                .ensure_linearizable()
                .await
                .map_err(|source| Error::LinearizableRead {
                    source: Box::new(source),
                })?;
            if stable_voters(&self.raft)? != self.expected_voters {
                return Err(Error::MembershipMismatch);
            }
            self.state
                .with_applied(|state, position| {
                    let state = state.ownership_state().ok_or(Error::FenceRequired)?;
                    if state.cluster_id() != self.cluster_id || state.group_id() != self.group_id {
                        return Err(Error::IdentityMismatch);
                    }
                    query(state, position)
                })
                .await
        })
        .await
        .context(ReadDeadlineSnafu)??;
        Ok((result, started))
    }
}

struct Freshness {
    expires_at: Instant,
}

impl Freshness {
    fn new(started: Instant, policy: ControlPolicy) -> Result<Self, Error> {
        let expires_at = started
            .checked_add(policy.ttl)
            .ok_or(Error::InvalidConfiguration)?;
        let freshness = Self { expires_at };
        freshness.check()?;
        Ok(freshness)
    }

    fn check(&self) -> Result<(), Error> {
        if Instant::now() >= self.expires_at {
            return Err(Error::Validity {
                source: LeaseError::Expired,
            });
        }
        Ok(())
    }
}

/// Only a successful fresh metadata quorum read can create this capability.
///
/// ```compile_fail
/// let _: tidemq::ownership::VerifiedGrant = serde_json::from_str("{}").unwrap();
/// ```
pub struct VerifiedGrant {
    grant: Grant,
    freshness: Freshness,
}

impl VerifiedGrant {
    pub fn grant(&self) -> &Grant {
        &self.grant
    }

    pub fn into_fence_command(self) -> Result<FenceCommand, Error> {
        self.freshness.check()?;
        Ok(FenceCommand { grant: self.grant })
    }
}

pub struct VerifiedFence {
    evidence: FenceEvidence,
    freshness: Freshness,
}

impl VerifiedFence {
    pub fn into_observe_command(self, expected_revision: u64) -> Result<Command, Error> {
        self.freshness.check()?;
        let operation_id = stage_operation_id(&self.evidence.fence.grant, ControlStage::Fence)
            .map_err(|_| Error::InvalidConfiguration)?;
        Ok(Command::new(
            operation_id,
            expected_revision,
            Action::ObserveFence(self.evidence),
        ))
    }
}

pub struct VerifiedRecovery {
    evidence: RecoveryEvidence,
    freshness: Freshness,
}

impl VerifiedRecovery {
    pub fn into_observe_command(self, expected_revision: u64) -> Result<Command, Error> {
        self.freshness.check()?;
        let operation_id =
            stage_operation_id(&self.evidence.recovered.fence.grant, ControlStage::Recovery)
                .map_err(|_| Error::InvalidConfiguration)?;
        Ok(Command::new(
            operation_id,
            expected_revision,
            Action::ObserveRecovery(self.evidence),
        ))
    }

    pub fn into_activate_command(self, expected_revision: u64) -> Result<Command, Error> {
        self.freshness.check()?;
        let operation_id =
            stage_operation_id(&self.evidence.recovered.fence.grant, ControlStage::Activate)
                .map_err(|_| Error::InvalidConfiguration)?;
        Ok(Command::new(
            operation_id,
            expected_revision,
            Action::ActivateRoute(self.evidence),
        ))
    }
}

fn stable_voters<A: Application>(raft: &Raft<A>) -> Result<BTreeSet<u64>, Error> {
    let receiver = raft.metrics();
    let metrics = receiver.borrow();
    let membership = metrics.membership_config.membership();
    // shortcut: Pause new capabilities during joint membership until its authorization protocol is verified.
    if membership.get_joint_config().len() != 1 {
        return Err(Error::MembershipMismatch);
    }
    let voters: BTreeSet<_> = membership.voter_ids().take(4).collect();
    if !matches!(voters.len(), 1 | 3) {
        return Err(Error::MembershipMismatch);
    }
    Ok(voters)
}

fn validate_metadata_identity(
    state: &MetadataState,
    cluster: Id,
    group: u64,
    voters: &BTreeSet<u64>,
) -> Result<(), Error> {
    if state.cluster_id() != Some(cluster) || state.metadata_group_id() != Some(group) {
        return Err(Error::IdentityMismatch);
    }
    let factor = state
        .mode
        .ok_or(Error::IdentityMismatch)?
        .replication_factor();
    if factor != voters.len() {
        return Err(Error::MembershipMismatch);
    }
    let mut zones = BTreeSet::new();
    for voter in voters {
        let node = state.node(*voter).ok_or(Error::MembershipMismatch)?;
        if !node.spec.roles.contains(&NodeRole::Metadata) {
            return Err(Error::MembershipMismatch);
        }
        zones.insert(&node.spec.zone);
    }
    if factor == 3 && zones.len() != 3 {
        return Err(Error::MembershipMismatch);
    }
    Ok(())
}

fn commit_position(position: Option<LogId<u64>>) -> Result<CommitPosition, Error> {
    let position = position.ok_or(Error::MissingCommitPosition)?;
    Ok(CommitPosition {
        term: position.leader_id.term,
        leader_node: position.leader_id.node_id,
        index: position.index,
    })
}
