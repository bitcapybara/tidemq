use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use snafu::{ResultExt, Snafu};
use tidemq_protocol::Id;
use tokio::sync::Semaphore;

use crate::execution::{Application, GroupHandle};
use crate::metadata::{
    Action, Command, ControlStage, GroupPlacement, MetadataState, NodeRole, stage_operation_id,
};

use super::{
    CommitPosition, ControlLease, ControlPolicy, FenceCommand, FenceEvidence, FenceState, Grant,
    LeaseError, RecoveryEvidence,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The authoritative application is stopped after a local failure"))]
    ApplicationStopped { source: crate::execution::Error },
    #[snafu(display("Ownership authority configuration is invalid"))]
    InvalidConfiguration,
    #[snafu(display("The authoritative linearizable read failed"))]
    LinearizableRead {
        source: Box<crate::execution::Error>,
    },
    #[snafu(display("The authority read exceeded its validity window"))]
    ReadDeadline { source: tokio::time::error::Elapsed },
    #[snafu(display("The authority view belongs to a different cluster or group"))]
    IdentityMismatch,
    #[snafu(display("The actual group placement does not match the authorized topology"))]
    MembershipMismatch,
    #[snafu(display("The requested ownership grant is absent, retired, or superseded"))]
    GrantMismatch,
    #[snafu(display("The committed fence is absent or does not match the grant"))]
    FenceRequired,
    #[snafu(display("Authoritative producer and ACK recovery is not complete"))]
    RecoveryRequired,
    #[snafu(display("The route is not active for this ownership grant"))]
    RouteRequired,
    #[snafu(display("The applied view has no committed execution position"))]
    MissingCommitPosition,
    #[snafu(display("Control validity cannot be established"))]
    Validity { source: LeaseError },
}

/// An application supplies its actual durable ownership state, not caller booleans.
pub trait OwnershipApplication: Application {
    fn ownership_state(&self) -> Option<&FenceState>;
}

/// In-process authoritative adapter. A remote implementation must additionally authenticate
/// the configured metadata identity and prohibit cached responses and raw command RPCs.
pub struct MetadataAuthority {
    pub(super) group: GroupHandle<MetadataState>,
    cluster_id: Id,
    group_id: u64,
    policy: ControlPolicy,
    finalizations: Arc<Semaphore>,
}

impl MetadataAuthority {
    pub fn new(
        group: GroupHandle<MetadataState>,
        cluster_id: Id,
        group_id: u64,
        policy: ControlPolicy,
    ) -> Result<Self, Error> {
        policy.validate().context(ValiditySnafu)?;
        if group_id == 0
            || group.identity().group.cluster_id != cluster_id
            || group.identity().group.group_id != group_id
        {
            return Err(Error::InvalidConfiguration);
        }
        Ok(Self {
            group,
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
                if !state.topic_serviceable(expected.partition.topic_id) {
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
                if !state.topic_serviceable(grant.partition.topic_id) {
                    return Err(Error::GrantMismatch);
                }
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

    pub(super) async fn read<R: Send>(
        &self,
        query: impl FnOnce(&MetadataState, Option<CommitPosition>) -> Result<R, Error> + Send,
    ) -> Result<(R, Instant), Error> {
        let deadline = Instant::now()
            .checked_add(self.policy.ttl)
            .ok_or(Error::InvalidConfiguration)?;
        self.read_until(deadline, query).await
    }

    /// Bound a remote authority projection by the caller's deadline as well as its freshness TTL.
    pub(super) async fn read_until<R: Send>(
        &self,
        deadline: Instant,
        query: impl FnOnce(&MetadataState, Option<CommitPosition>) -> Result<R, Error> + Send,
    ) -> Result<(R, Instant), Error> {
        let started = Instant::now();
        let deadline = deadline.min(
            started
                .checked_add(self.policy.ttl)
                .ok_or(Error::InvalidConfiguration)?,
        );
        if deadline <= started {
            let expired = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>()).await;
            let Err(source) = expired else {
                return Err(Error::InvalidConfiguration);
            };
            return Err(Error::ReadDeadline { source });
        }
        let result = tokio::time::timeout(deadline.saturating_duration_since(started), async {
            self.group
                .ensure_read(tokio::time::Instant::from_std(deadline))
                .await
                .map_err(|source| Error::LinearizableRead {
                    source: Box::new(source),
                })?;
            let placement = authority_placement(&self.group)?;
            self.group
                .state()
                .try_with_applied(|state, position| {
                    validate_metadata_identity(
                        state,
                        self.cluster_id,
                        self.group_id,
                        &self.group,
                        &placement,
                    )?;
                    query(state, position.map(CommitPosition::from))
                })
                .await
                .context(ApplicationStoppedSnafu)?
        })
        .await
        .context(ReadDeadlineSnafu)??;
        Ok((result, started))
    }
}

/// Authoritative reads from the owning data group, after the execution barrier.
pub struct DataAuthority<A: OwnershipApplication> {
    pub(super) group: GroupHandle<A>,
    cluster_id: Id,
    group_id: u64,
    expected_placement: GroupPlacement,
    policy: ControlPolicy,
}

impl<A: OwnershipApplication> DataAuthority<A> {
    pub fn new(
        group: GroupHandle<A>,
        cluster_id: Id,
        group_id: u64,
        expected_placement: GroupPlacement,
        policy: ControlPolicy,
    ) -> Result<Self, Error> {
        policy.validate().context(ValiditySnafu)?;
        if group_id == 0
            || group.identity().group.cluster_id != cluster_id
            || group.identity().group.group_id != group_id
            || expected_placement.mode().is_err()
            || match &expected_placement {
                GroupPlacement::Local { node_id } => {
                    !group.is_local() || *node_id != group.local_node()
                }
                GroupPlacement::Raft { .. } => group.is_local(),
            }
        {
            return Err(Error::InvalidConfiguration);
        }
        Ok(Self {
            group,
            cluster_id,
            group_id,
            expected_placement,
            policy,
        })
    }

    pub async fn verify_fence(&self, expected: &Grant) -> Result<VerifiedFence, Error> {
        let (evidence, started) = self
            .read(|state, position, placement| {
                let fence = state
                    .fence(expected.partition)
                    .filter(|fence| fence.grant == *expected)
                    .ok_or(Error::FenceRequired)?;
                Ok(FenceEvidence {
                    placement: placement.clone(),
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
            .read(|state, position, placement| {
                let recovered = state
                    .recovered(expected.partition)
                    .filter(|recovered| {
                        recovered.fence.grant == *expected
                            && recovered.checkpoint.validate(&recovered.fence)
                    })
                    .ok_or(Error::RecoveryRequired)?;
                Ok(RecoveryEvidence {
                    placement: placement.clone(),
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

    pub(super) async fn read<R: Send>(
        &self,
        query: impl FnOnce(&FenceState, Option<CommitPosition>, &GroupPlacement) -> Result<R, Error>
        + Send,
    ) -> Result<(R, Instant), Error> {
        let started = Instant::now();
        let result = tokio::time::timeout(self.policy.ttl, async {
            self.group
                .ensure_read(tokio::time::Instant::now() + self.policy.ttl)
                .await
                .map_err(|source| Error::LinearizableRead {
                    source: Box::new(source),
                })?;
            let placement = authority_placement(&self.group)?;
            if placement != self.expected_placement {
                return Err(Error::MembershipMismatch);
            }
            self.group
                .state()
                .try_with_applied(|state, position| {
                    let state = state.ownership_state().ok_or(Error::FenceRequired)?;
                    if state.cluster_id() != self.cluster_id || state.group_id() != self.group_id {
                        return Err(Error::IdentityMismatch);
                    }
                    query(state, position.map(CommitPosition::from), &placement)
                })
                .await
                .context(ApplicationStoppedSnafu)?
        })
        .await
        .context(ReadDeadlineSnafu)??;
        Ok((result, started))
    }
}

pub(super) struct Freshness {
    expires_at: Instant,
}

impl Freshness {
    pub(super) fn new(started: Instant, policy: ControlPolicy) -> Result<Self, Error> {
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

/// Only a successful fresh metadata authority read can create this capability.
///
/// ```compile_fail
/// let _: tidemq::ownership::VerifiedGrant = serde_json::from_str("{}").unwrap();
/// ```
pub struct VerifiedGrant {
    pub(super) grant: Grant,
    pub(super) freshness: Freshness,
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
    pub(super) evidence: FenceEvidence,
    pub(super) freshness: Freshness,
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
    pub(super) evidence: RecoveryEvidence,
    pub(super) freshness: Freshness,
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

pub(crate) fn authority_placement<A: Application>(
    group: &GroupHandle<A>,
) -> Result<GroupPlacement, Error> {
    let Some(raft) = group.raft() else {
        return Ok(GroupPlacement::Local {
            node_id: group.local_node(),
        });
    };
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
    Ok(GroupPlacement::Raft { voters })
}

fn validate_metadata_identity(
    state: &MetadataState,
    cluster: Id,
    group: u64,
    handle: &GroupHandle<MetadataState>,
    placement: &GroupPlacement,
) -> Result<(), Error> {
    if state.cluster_id() != Some(cluster) || state.metadata_group_id() != Some(group) {
        return Err(Error::IdentityMismatch);
    }
    let mode = state.mode.ok_or(Error::IdentityMismatch)?;
    if (mode == crate::metadata::DeploymentMode::Local) != handle.is_local() {
        return Err(Error::IdentityMismatch);
    }
    let factor = mode.replication_factor();
    if placement.mode() != Ok(mode) {
        return Err(Error::MembershipMismatch);
    }
    let mut zones = BTreeSet::new();
    for voter in &placement.hosts() {
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

fn commit_position(position: Option<CommitPosition>) -> Result<CommitPosition, Error> {
    position
        .filter(|position| position.validate())
        .ok_or(Error::MissingCommitPosition)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn metadata_read_honors_an_expired_caller_deadline() {
        let directory = std::env::temp_dir().join(format!(
            "tidemq-authority-deadline-{}",
            Id::new_v7().unwrap()
        ));
        let cluster = Id::new_v7().unwrap();
        let group = GroupHandle::Local(
            crate::execution::LocalGroup::open(
                &directory,
                crate::execution::StorageIdentity::new::<MetadataState>(cluster, 1, 1),
                Default::default(),
                MetadataState::default(),
            )
            .await
            .unwrap(),
        );
        let authority =
            MetadataAuthority::new(group.clone(), cluster, 1, ControlPolicy::default()).unwrap();
        let deadline = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .unwrap();
        let result = authority.read_until(deadline, |_, _| Ok(())).await;
        assert!(matches!(
            result,
            Err(Error::ReadDeadline { .. }) | Err(Error::LinearizableRead { .. })
        ));
        group
            .close(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}
