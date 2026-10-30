use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant as MonoInstant;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::metadata::{PartitionKey, RecordStatus, Route, SubscriptionRecord, TopicRecord};
use crate::runtime::wire::{self, PeerRpc, Request};
use crate::runtime::{Error, Result};

use super::authority::Freshness;
use super::{
    CommitPosition, ControlLease, ControlPolicy, FenceEvidence, FenceRecord, Grant,
    MetadataAuthority, RecoveryEvidence, VerifiedFence, VerifiedGrant, VerifiedRecovery,
};

impl<A: super::OwnershipApplication> super::DataAuthority<A> {
    pub(crate) async fn remote_proof(
        &self,
        expected: &Grant,
        recovery: bool,
        voters: BTreeSet<u64>,
    ) -> std::result::Result<DataProof, super::AuthorityError> {
        if self.group.is_local() {
            return Err(super::AuthorityError::IdentityMismatch);
        }
        let (evidence, _) = self
            .read(|state, position, placement| {
                let position = position.ok_or(super::AuthorityError::MissingCommitPosition)?;
                let committed = position;
                if recovery {
                    let recovered = state
                        .recovered(expected.partition)
                        .filter(|recovered| {
                            recovered.fence.grant == *expected
                                && recovered.checkpoint.validate(&recovered.fence)
                        })
                        .ok_or(super::AuthorityError::RecoveryRequired)?;
                    Ok(DataEvidence::Recovery(RecoveryEvidence {
                        placement: placement.clone(),
                        recovered,
                        committed,
                    }))
                } else {
                    let fence = state
                        .fence(expected.partition)
                        .filter(|fence| fence.grant == *expected)
                        .cloned()
                        .ok_or(super::AuthorityError::FenceRequired)?;
                    Ok(DataEvidence::Fence(FenceEvidence {
                        placement: placement.clone(),
                        fence,
                        committed,
                    }))
                }
            })
            .await?;
        Ok(DataProof {
            cluster: expected.cluster_id,
            group: expected.group_id,
            voters,
            evidence,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Query {
    Group(u64),
    TopicName(String),
    Topic(Id),
    SubscriptionName { topic: Id, name: String },
    Subscription(Id),
    Partition(PartitionKey),
    Grant(Grant),
    Lease(FenceRecord),
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Value {
    Group(crate::metadata::DataGroup),
    Topic(Option<TopicRecord>),
    Subscription(Option<SubscriptionRecord>),
    Partition {
        topic: TopicRecord,
        group: u64,
        grant: Option<Grant>,
        route: Option<Route>,
    },
    Grant(Grant),
    Lease {
        grant: Grant,
        generation: u64,
    },
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Proof {
    cluster: Id,
    group: u64,
    voters: BTreeSet<u64>,
    position: CommitPosition,
    value: Value,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct DataProof {
    pub cluster: Id,
    pub group: u64,
    pub voters: BTreeSet<u64>,
    pub evidence: DataEvidence,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum DataEvidence {
    Fence(FenceEvidence),
    Recovery(RecoveryEvidence),
}

/// Local reads use the same bounded projections and freshness rules without an RPC envelope.
pub(crate) enum Authority {
    Local(MetadataAuthority),
    Remote(RemoteMetadataAuthority),
}

impl Authority {
    pub fn new_local(
        group: crate::execution::GroupHandle<crate::metadata::MetadataState>,
        cluster: Id,
        metadata_group: u64,
        policy: ControlPolicy,
    ) -> Result<Self> {
        if !group.is_local() {
            return Err(Error::invalid("Local authority requires a local group"));
        }
        MetadataAuthority::new(group, cluster, metadata_group, policy)
            .map(Self::Local)
            .map_err(|source| Error::operation("create local metadata authority", source))
    }

    pub fn new(
        rpc: PeerRpc,
        cluster: Id,
        group: u64,
        placement: crate::metadata::GroupPlacement,
        policy: ControlPolicy,
    ) -> Result<Self> {
        let crate::metadata::GroupPlacement::Raft { voters } = placement else {
            return Err(Error::invalid(
                "Local authority must be constructed from its local group",
            ));
        };
        RemoteMetadataAuthority::new(rpc, cluster, group, voters, policy).map(Self::Remote)
    }

    pub async fn query(&self, query: Query, deadline: Instant) -> Result<Value> {
        match self {
            Self::Remote(authority) => authority.query(query, deadline).await,
            Self::Local(authority) => {
                tokio::time::timeout_at(deadline, authority.read(|state, _| project(state, query)))
                    .await
                    .map_err(|_| Error::unavailable())?
                    .map(|(value, _)| value)
                    .map_err(|source| Error::operation("read local metadata authority", source))
            }
        }
    }

    pub fn replace_voters(&self, voters: BTreeSet<u64>) -> Result<()> {
        match self {
            Self::Remote(authority) => authority.replace_voters(voters),
            Self::Local(_) => Err(Error::invalid("Local authority has no voting membership")),
        }
    }

    pub async fn verify_grant(&self, expected: &Grant, deadline: Instant) -> Result<VerifiedGrant> {
        match self {
            Self::Remote(authority) => authority.verify_grant(expected, deadline).await,
            Self::Local(authority) => {
                tokio::time::timeout_at(deadline, authority.verify_grant(expected))
                    .await
                    .map_err(|_| Error::unavailable())?
                    .map_err(|source| Error::operation("verify local metadata grant", source))
            }
        }
    }

    pub async fn active_lease(
        &self,
        fence: &FenceRecord,
        deadline: Instant,
    ) -> Result<ControlLease> {
        match self {
            Self::Remote(authority) => authority.active_lease(fence, deadline).await,
            Self::Local(authority) => {
                tokio::time::timeout_at(deadline, authority.active_lease(fence))
                    .await
                    .map_err(|_| Error::unavailable())?
                    .map_err(|source| Error::operation("verify local control lease", source))
            }
        }
    }

    pub fn verified_fence(
        &self,
        proof: DataProof,
        expected: &Grant,
        voters: &BTreeSet<u64>,
        started: MonoInstant,
    ) -> Result<VerifiedFence> {
        match self {
            Self::Remote(authority) => authority.verified_fence(proof, expected, voters, started),
            Self::Local(_) => Err(Error::invalid(
                "Local authority cannot accept a remote quorum proof",
            )),
        }
    }

    pub fn verified_recovery(
        &self,
        proof: DataProof,
        expected: &Grant,
        voters: &BTreeSet<u64>,
        started: MonoInstant,
    ) -> Result<VerifiedRecovery> {
        match self {
            Self::Remote(authority) => {
                authority.verified_recovery(proof, expected, voters, started)
            }
            Self::Local(_) => Err(Error::invalid(
                "Local authority cannot accept a remote quorum proof",
            )),
        }
    }
}

/// Only this adapter constructs capabilities from a fresh, certificate-bound authority reply.
pub(crate) struct RemoteMetadataAuthority {
    rpc: PeerRpc,
    cluster: Id,
    group: u64,
    voters: std::sync::RwLock<BTreeSet<u64>>,
    preferred: AtomicU64,
    policy: ControlPolicy,
    finalizations: Arc<Semaphore>,
}

impl RemoteMetadataAuthority {
    pub fn new(
        rpc: PeerRpc,
        cluster: Id,
        group: u64,
        voters: BTreeSet<u64>,
        policy: ControlPolicy,
    ) -> Result<Self> {
        policy
            .validate()
            .map_err(|source| Error::operation("validate remote authority policy", source))?;
        if group == 0 || !matches!(voters.len(), 1 | 3) || voters.contains(&0) {
            return Err(Error::invalid("Remote metadata topology is invalid"));
        }
        Ok(Self {
            rpc,
            cluster,
            group,
            voters: std::sync::RwLock::new(voters),
            preferred: AtomicU64::new(0),
            policy,
            finalizations: Arc::new(Semaphore::new(policy.max_finalizations)),
        })
    }

    pub async fn query(&self, query: Query, deadline: Instant) -> Result<Value> {
        self.fresh(query, deadline).await.map(|(value, _)| value)
    }

    pub(crate) fn replace_voters(&self, voters: BTreeSet<u64>) -> Result<()> {
        if !matches!(voters.len(), 1 | 3) || voters.contains(&0) {
            return Err(Error::unavailable());
        }
        *self.voters.write().map_err(|_| Error::unavailable())? = voters;
        Ok(())
    }

    async fn fresh(&self, query: Query, deadline: Instant) -> Result<(Value, MonoInstant)> {
        let started = MonoInstant::now();
        let expires = Instant::from_std(started)
            .checked_add(self.policy.ttl)
            .ok_or_else(|| Error::invalid("Authority deadline overflow"))?;
        let deadline = deadline.min(expires);
        let voters = self
            .voters
            .read()
            .map_err(|_| Error::unavailable())?
            .clone();
        // A verified responder is a routing hint; every reply still needs a fresh quorum proof.
        let preferred = self.preferred.load(Ordering::Relaxed);
        let candidates = voters
            .get(&preferred)
            .copied()
            .into_iter()
            .chain(voters.iter().copied().filter(|node| *node != preferred));
        for node in candidates {
            let request = Request::control(wire::AUTHORITY, &query)?;
            let attempt_deadline =
                deadline.min(Instant::now() + std::time::Duration::from_millis(500));
            let result = self
                .rpc
                .call(node, self.group, request, attempt_deadline)
                .await;
            let bytes = match result {
                Ok(bytes) => bytes,
                Err(_) if Instant::now() < deadline => continue,
                Err(error) => return Err(error),
            };
            let proof: Proof = wire::decode_json(&bytes)?;
            proof.validate(self.cluster, self.group, &voters, started, self.policy.ttl)?;
            if let Query::Group(expected) = query
                && !matches!(&proof.value, Value::Group(group) if group.group_id == expected)
            {
                return Err(Error::invalid(
                    "Metadata proof describes a different data group",
                ));
            }
            self.preferred.store(node, Ordering::Relaxed);
            return Ok((proof.value, started));
        }
        Err(Error::unavailable())
    }

    pub async fn verify_grant(&self, expected: &Grant, deadline: Instant) -> Result<VerifiedGrant> {
        let (value, started) = self.fresh(Query::Grant(expected.clone()), deadline).await?;
        let Value::Grant(grant) = value else {
            return Err(Error::invalid(
                "Authority grant response has the wrong kind",
            ));
        };
        if grant != *expected {
            return Err(Error::invalid("Authority returned a different grant"));
        }
        let freshness = Freshness::new(started, self.policy)
            .map_err(|source| Error::operation("validate grant freshness", source))?;
        Ok(VerifiedGrant { grant, freshness })
    }

    pub async fn active_lease(
        &self,
        fence: &FenceRecord,
        deadline: Instant,
    ) -> Result<ControlLease> {
        let (value, started) = self.fresh(Query::Lease(fence.clone()), deadline).await?;
        let Value::Lease { grant, generation } = value else {
            return Err(Error::invalid(
                "Authority lease response has the wrong kind",
            ));
        };
        if grant != fence.grant || generation == 0 {
            return Err(Error::invalid("Authority returned a different lease"));
        }
        ControlLease::from_verified(
            grant,
            generation,
            started,
            self.policy,
            self.finalizations.clone(),
        )
        .map_err(|source| Error::operation("validate lease freshness", source))
    }

    pub fn verified_fence(
        &self,
        proof: DataProof,
        expected: &Grant,
        voters: &BTreeSet<u64>,
        started: MonoInstant,
    ) -> Result<VerifiedFence> {
        let DataEvidence::Fence(evidence) = proof.evidence else {
            return Err(Error::invalid("Data proof has the wrong kind"));
        };
        if evidence.placement
            != (crate::metadata::GroupPlacement::Raft {
                voters: proof.voters.clone(),
            })
        {
            return Err(Error::unavailable());
        }
        validate_data_proof(
            proof.cluster,
            proof.group,
            &proof.voters,
            evidence.committed,
            &evidence.fence.grant,
            self.cluster,
            expected,
            voters,
        )?;
        Ok(VerifiedFence {
            evidence,
            freshness: Freshness::new(started, self.policy)
                .map_err(|source| Error::operation("validate fence freshness", source))?,
        })
    }

    pub fn verified_recovery(
        &self,
        proof: DataProof,
        expected: &Grant,
        voters: &BTreeSet<u64>,
        started: MonoInstant,
    ) -> Result<VerifiedRecovery> {
        let DataEvidence::Recovery(evidence) = proof.evidence else {
            return Err(Error::invalid("Data proof has the wrong kind"));
        };
        if evidence.placement
            != (crate::metadata::GroupPlacement::Raft {
                voters: proof.voters.clone(),
            })
        {
            return Err(Error::unavailable());
        }
        validate_data_proof(
            proof.cluster,
            proof.group,
            &proof.voters,
            evidence.committed,
            &evidence.recovered.fence.grant,
            self.cluster,
            expected,
            voters,
        )?;
        if !evidence
            .recovered
            .checkpoint
            .validate(&evidence.recovered.fence)
        {
            return Err(Error::invalid("Data recovery proof is inconsistent"));
        }
        Ok(VerifiedRecovery {
            evidence,
            freshness: Freshness::new(started, self.policy)
                .map_err(|source| Error::operation("validate recovery freshness", source))?,
        })
    }
}

impl Proof {
    fn validate(
        &self,
        cluster: Id,
        group: u64,
        voters: &BTreeSet<u64>,
        started: MonoInstant,
        ttl: std::time::Duration,
    ) -> Result<()> {
        let mode = crate::metadata::GroupPlacement::Raft {
            voters: voters.clone(),
        }
        .mode()
        .map_err(|_| Error::invalid("Metadata proof topology is invalid"))?;
        if self.cluster != cluster
            || self.group != group
            || !self.position.matches_mode(mode)
            || matches!(&self.value, Value::Group(data) if data.placement.mode() != Ok(mode))
        {
            return Err(Error::invalid(
                "Fresh metadata proof does not match its configured authority",
            ));
        }
        // Discovery and in-flight quorum reads can straddle a membership change or lease expiry.
        if self.voters != *voters
            || !self
                .position
                .raft_leader()
                .is_some_and(|leader| voters.contains(&leader))
            || started.elapsed() >= ttl
        {
            return Err(Error::unavailable());
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/scalable_support/metadata_proof.rs"]
mod tests;

// Compare each independent authority identity before constructing a process-local capability.
#[allow(clippy::too_many_arguments)]
fn validate_data_proof(
    cluster: Id,
    group: u64,
    actual_voters: &BTreeSet<u64>,
    position: CommitPosition,
    grant: &Grant,
    expected_cluster: Id,
    expected: &Grant,
    voters: &BTreeSet<u64>,
) -> Result<()> {
    if cluster != expected_cluster
        || group != expected.group_id
        || !position.validate()
        || grant != expected
    {
        return Err(Error::invalid(
            "Data proof does not match its configured consensus authority",
        ));
    }
    if actual_voters != voters
        || !position
            .raft_leader()
            .is_some_and(|leader| voters.contains(&leader))
    {
        return Err(Error::unavailable());
    }
    Ok(())
}

impl MetadataAuthority {
    pub(crate) async fn remote_query(
        &self,
        query: Query,
        deadline: std::time::Instant,
    ) -> std::result::Result<Proof, super::AuthorityError> {
        if self.group.is_local() {
            return Err(super::AuthorityError::IdentityMismatch);
        }
        let ((value, position, cluster, group, voters), _) = self
            .read_until(deadline, |state, position| {
                let position = position.ok_or(super::AuthorityError::MissingCommitPosition)?;
                let cluster = state
                    .cluster_id()
                    .ok_or(super::AuthorityError::IdentityMismatch)?;
                let group = state
                    .metadata_group_id()
                    .ok_or(super::AuthorityError::IdentityMismatch)?;
                let voters = super::authority::authority_placement(&self.group)?
                    .raft_voters()
                    .cloned()
                    .ok_or(super::AuthorityError::MembershipMismatch)?;
                let value = project(state, query)?;
                Ok((value, position, cluster, group, voters))
            })
            .await?;
        Ok(Proof {
            cluster,
            group,
            voters,
            position,
            value,
        })
    }
}

pub(crate) fn current_voters<A: crate::replication::Application>(
    group: &crate::execution::GroupHandle<A>,
) -> Result<BTreeSet<u64>> {
    if group.is_local() {
        return Err(Error::unavailable());
    }
    super::authority::authority_placement(group)
        .map_err(|_| Error::unavailable())?
        .raft_voters()
        .cloned()
        .ok_or_else(Error::unavailable)
}

fn project(
    state: &crate::metadata::MetadataState,
    query: Query,
) -> std::result::Result<Value, super::AuthorityError> {
    let serviceable_topic = |topic| {
        state
            .topic(topic)
            .filter(|_| state.topic_serviceable(topic))
            .cloned()
            .ok_or(super::AuthorityError::GrantMismatch)
    };
    Ok(match query {
        Query::Group(group) => Value::Group(
            state
                .group(group)
                .cloned()
                .ok_or(super::AuthorityError::MembershipMismatch)?,
        ),
        Query::TopicName(name) => Value::Topic(
            state
                .topic_by_name(&name)
                .filter(|topic| topic.status == RecordStatus::Active)
                .cloned(),
        ),
        Query::Topic(topic) => Value::Topic(
            state
                .topic(topic)
                .filter(|topic| topic.status == RecordStatus::Active)
                .cloned(),
        ),
        Query::SubscriptionName { topic, name } => {
            Value::Subscription(state.subscription_by_name(topic, &name).cloned())
        }
        Query::Subscription(subscription) => Value::Subscription(
            state
                .subscription(subscription)
                .filter(|record| record.status == RecordStatus::Active)
                .cloned(),
        ),
        Query::Partition(partition) => Value::Partition {
            topic: serviceable_topic(partition.topic_id)?,
            group: state
                .partition_group(partition)
                .ok_or(super::AuthorityError::GrantMismatch)?,
            grant: state.grant(partition).cloned(),
            route: state.route(partition).cloned(),
        },
        Query::Grant(expected) => {
            serviceable_topic(expected.partition.topic_id)?;
            let grant = state
                .grant(expected.partition)
                .filter(|grant| *grant == &expected)
                .cloned()
                .ok_or(super::AuthorityError::GrantMismatch)?;
            Value::Grant(grant)
        }
        Query::Lease(fence) => {
            serviceable_topic(fence.grant.partition.topic_id)?;
            let grant = state
                .grant(fence.grant.partition)
                .filter(|grant| *grant == &fence.grant)
                .cloned()
                .ok_or(super::AuthorityError::GrantMismatch)?;
            let route = state
                .route(grant.partition)
                .filter(|route| {
                    route.epoch == grant.epoch
                        && route.owner_node == grant.owner_node
                        && route.ownership_operation == grant.operation_id
                })
                .ok_or(super::AuthorityError::RouteRequired)?;
            Value::Lease {
                grant,
                generation: route.generation,
            }
        }
    })
}
