use std::collections::BTreeSet;
use std::sync::Arc;
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
        let (evidence, _) = self
            .read(|state, position| {
                let position = position.ok_or(super::AuthorityError::MissingCommitPosition)?;
                let committed = CommitPosition {
                    term: position.leader_id.term,
                    leader_node: position.leader_id.node_id,
                    index: position.index,
                };
                if recovery {
                    let recovered = state
                        .recovered(expected.partition)
                        .filter(|recovered| {
                            recovered.fence.grant == *expected
                                && recovered.checkpoint.validate(&recovered.fence)
                        })
                        .ok_or(super::AuthorityError::RecoveryRequired)?;
                    Ok(DataEvidence::Recovery(RecoveryEvidence {
                        recovered,
                        committed,
                    }))
                } else {
                    let fence = state
                        .fence(expected.partition)
                        .filter(|fence| fence.grant == *expected)
                        .cloned()
                        .ok_or(super::AuthorityError::FenceRequired)?;
                    Ok(DataEvidence::Fence(FenceEvidence { fence, committed }))
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

/// Only this adapter constructs capabilities from a fresh, certificate-bound authority reply.
pub(crate) struct RemoteMetadataAuthority {
    rpc: PeerRpc,
    cluster: Id,
    group: u64,
    voters: BTreeSet<u64>,
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
            voters,
            policy,
            finalizations: Arc::new(Semaphore::new(policy.max_finalizations)),
        })
    }

    pub async fn query(&self, query: Query, deadline: Instant) -> Result<Value> {
        self.fresh(query, deadline).await.map(|(value, _)| value)
    }

    async fn fresh(&self, query: Query, deadline: Instant) -> Result<(Value, MonoInstant)> {
        let started = MonoInstant::now();
        let expires = Instant::from_std(started)
            .checked_add(self.policy.ttl)
            .ok_or_else(|| Error::invalid("Authority deadline overflow"))?;
        let deadline = deadline.min(expires);
        for node in &self.voters {
            let request = Request::control(wire::AUTHORITY, &query)?;
            let attempt_deadline =
                deadline.min(Instant::now() + std::time::Duration::from_millis(500));
            let result = self
                .rpc
                .call(*node, self.group, request, attempt_deadline)
                .await;
            let bytes = match result {
                Ok(bytes) => bytes,
                Err(_) if Instant::now() < deadline => continue,
                Err(error) => return Err(error),
            };
            let proof: Proof = wire::decode_json(&bytes)?;
            if proof.cluster != self.cluster
                || proof.group != self.group
                || proof.voters != self.voters
                || !proof.position.validate()
                || !self.voters.contains(&proof.position.leader_node)
                || MonoInstant::now().saturating_duration_since(started) >= self.policy.ttl
            {
                return Err(Error::invalid(
                    "Fresh metadata proof does not match its configured authority",
                ));
            }
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
        || actual_voters != voters
        || !position.validate()
        || !voters.contains(&position.leader_node)
        || grant != expected
    {
        return Err(Error::invalid(
            "Data proof does not match its configured consensus authority",
        ));
    }
    Ok(())
}

impl MetadataAuthority {
    pub(crate) async fn remote_query(
        &self,
        query: Query,
    ) -> std::result::Result<Proof, super::AuthorityError> {
        let ((value, position, cluster, group, voters), _) = self
            .read(|state, position| {
                let position = position.ok_or(super::AuthorityError::MissingCommitPosition)?;
                let cluster = state
                    .cluster_id()
                    .ok_or(super::AuthorityError::IdentityMismatch)?;
                let group = state
                    .metadata_group_id()
                    .ok_or(super::AuthorityError::IdentityMismatch)?;
                let voters = super::authority::stable_voters(&self.raft)?;
                let active_topic = |topic| {
                    state
                        .topic(topic)
                        .filter(|topic| topic.status == RecordStatus::Active)
                        .cloned()
                        .ok_or(super::AuthorityError::GrantMismatch)
                };
                let value = match query {
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
                        topic: active_topic(partition.topic_id)?,
                        group: state
                            .partition_group(partition)
                            .ok_or(super::AuthorityError::GrantMismatch)?,
                        grant: state.grant(partition).cloned(),
                        route: state.route(partition).cloned(),
                    },
                    Query::Grant(expected) => {
                        active_topic(expected.partition.topic_id)?;
                        let grant = state
                            .grant(expected.partition)
                            .filter(|grant| *grant == &expected)
                            .cloned()
                            .ok_or(super::AuthorityError::GrantMismatch)?;
                        Value::Grant(grant)
                    }
                    Query::Lease(fence) => {
                        active_topic(fence.grant.partition.topic_id)?;
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
                };
                Ok((
                    value,
                    CommitPosition {
                        term: position.leader_id.term,
                        leader_node: position.leader_id.node_id,
                        index: position.index,
                    },
                    cluster,
                    group,
                    voters,
                ))
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
    raft: &crate::replication::Raft<A>,
) -> Result<BTreeSet<u64>> {
    super::authority::stable_voters(raft).map_err(|_| Error::unavailable())
}
