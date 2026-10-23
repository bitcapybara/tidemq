//! Closed internal RPCs. Only the authenticated runtime may construct remote authority adapters.

use std::{
    collections::BTreeSet,
    io::Write,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use openraft::{LogId, storage::RaftSnapshotBuilder};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tidemq_protocol::{Id, common::TrafficClass};

use crate::{
    metadata::{DeploymentMode, MetadataState, NodeRole},
    ownership::{ControlLease, FenceRecord},
    replication::{DiskStateMachine, Raft},
    transport::{Authorization, NodeGrant},
};

use super::{
    DataReplica, Error, Rejection, Result,
    backend::{
        BoxFuture, LocalMetadata, MetadataBackend, MetadataReplica, Query, QuorumView, View,
    },
    types::{CatalogCommand, CatalogOutcome},
};

pub(crate) const LIFECYCLE_QUERY: u32 = 6;
pub(crate) const LIFECYCLE_COMMAND: u32 = 7;
pub(crate) const LIFECYCLE_CHECKPOINT: u32 = 8;
const MAX_BODY: usize = 512 * 1024;
const MAX_CONTROL_BODY: usize = 12 * 1024;

/// Implementations bind the request to the exact configured TLS node, cluster, role and group.
pub(crate) trait LifecycleRpc: Send + Sync {
    fn request(
        &self,
        target: u64,
        group: u64,
        class: TrafficClass,
        kind: u32,
        body: Bytes,
        deadline: Instant,
    ) -> BoxFuture<'_, Bytes>;
    fn control_lease(&self, fence: FenceRecord, deadline: Instant) -> BoxFuture<'_, ControlLease>;
}

pub(crate) type RpcFuture<'a, T> = BoxFuture<'a, T>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct FloorTarget {
    partition: crate::metadata::PartitionKey,
    before: u64,
    manifest_generation: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SnapshotRequest {
    cluster_id: Id,
    group_id: u64,
    object_id: Option<Id>,
    generation: u64,
    partitions: Vec<crate::metadata::PartitionKey>,
    floor: Option<FloorTarget>,
    voters: BTreeSet<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotEvidence {
    pub(crate) cluster_id: Id,
    pub(crate) group_id: u64,
    pub(crate) object_id: Option<Id>,
    pub(crate) generation: u64,
    partitions: Vec<crate::metadata::PartitionKey>,
    floor: Option<FloorTarget>,
    pub(crate) voters: BTreeSet<u64>,
    pub(crate) node_id: u64,
    pub(crate) snapshot_id: String,
    pub(crate) position: LogId<u64>,
}

#[derive(Clone)]
pub struct ReplicaCheckpoint {
    node_id: u64,
    pub(super) cluster_id: Id,
    pub(super) group_id: u64,
    backend: CheckpointBackend,
}

#[derive(Clone)]
enum CheckpointBackend {
    Local(DataReplica),
    Remote(Arc<dyn LifecycleRpc>),
}

impl ReplicaCheckpoint {
    pub fn local(replica: DataReplica) -> Self {
        Self {
            node_id: replica.node_id(),
            cluster_id: replica.state.0.identity.group.cluster_id,
            group_id: replica.state.0.identity.group.group_id,
            backend: CheckpointBackend::Local(replica),
        }
    }
    pub(crate) fn remote(
        cluster_id: Id,
        group_id: u64,
        node_id: u64,
        rpc: Arc<dyn LifecycleRpc>,
    ) -> Result<Self> {
        if node_id == 0 || group_id == 0 {
            return Err(Error::Configuration);
        }
        Ok(Self {
            node_id,
            cluster_id,
            group_id,
            backend: CheckpointBackend::Remote(rpc),
        })
    }
    pub(crate) fn node_id(&self) -> u64 {
        self.node_id
    }
    pub(crate) async fn build(
        &self,
        object_id: Id,
        generation: u64,
        position: LogId<u64>,
        voters: &BTreeSet<u64>,
        partitions: &[crate::metadata::PartitionKey],
        deadline: Instant,
    ) -> Result<SnapshotEvidence> {
        let request = SnapshotRequest {
            cluster_id: self.cluster_id,
            group_id: self.group_id,
            object_id: Some(object_id),
            generation,
            partitions: partitions.to_vec(),
            floor: None,
            voters: voters.clone(),
        };
        self.build_request(request, position, deadline).await
    }

    pub(crate) async fn build_floor(
        &self,
        proof: &super::types::FloorEvidence,
        deadline: Instant,
    ) -> Result<SnapshotEvidence> {
        let request = SnapshotRequest {
            cluster_id: self.cluster_id,
            group_id: self.group_id,
            object_id: None,
            generation: 0,
            partitions: Vec::new(),
            floor: Some(FloorTarget {
                partition: proof.partition,
                before: proof.before,
                manifest_generation: proof.manifest_generation,
            }),
            voters: proof.voters.clone(),
        };
        self.build_request(request, proof.position, deadline).await
    }

    async fn build_request(
        &self,
        request: SnapshotRequest,
        position: LogId<u64>,
        deadline: Instant,
    ) -> Result<SnapshotEvidence> {
        let proof = match &self.backend {
            CheckpointBackend::Local(replica) => {
                build_snapshot(replica, &request, deadline).await?
            }
            CheckpointBackend::Remote(rpc) => {
                let body = rpc
                    .request(
                        self.node_id,
                        self.group_id,
                        TrafficClass::Bulk,
                        LIFECYCLE_CHECKPOINT,
                        encode(&request)?,
                        deadline,
                    )
                    .await?;
                decode::<SnapshotEvidence>(&body)?
            }
        };
        if proof.cluster_id != self.cluster_id
            || proof.group_id != self.group_id
            || proof.object_id != request.object_id
            || proof.generation != request.generation
            || proof.partitions != request.partitions
            || proof.floor != request.floor
            || proof.voters != request.voters
            || proof.node_id != self.node_id
            || proof.position < position
            || proof.snapshot_id.is_empty()
            || proof.snapshot_id.len() > 128
        {
            return Err(Error::Authorization);
        }
        Ok(proof)
    }
}

pub(crate) struct SnapshotHandler {
    replica: DataReplica,
}
impl SnapshotHandler {
    pub(crate) fn new(replica: DataReplica) -> Self {
        Self { replica }
    }
    pub(crate) async fn handle(
        &self,
        peer: &Authorization,
        class: TrafficClass,
        body: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let Authorization::Internal(peer) = peer else {
            return Err(Error::Authorization);
        };
        let request: SnapshotRequest = decode(body)?;
        if class != TrafficClass::Bulk
            || peer.cluster_id != request.cluster_id
            || !peer.allowed_groups.contains(&request.group_id)
            || !matches!(peer.role.as_str(), "storage" | "metadata" | "combined")
        {
            return Err(Error::Authorization);
        }
        let evidence = build_snapshot(&self.replica, &request, deadline).await?;
        encode(&evidence)
    }
}

async fn build_snapshot(
    replica: &DataReplica,
    request: &SnapshotRequest,
    deadline: Instant,
) -> Result<SnapshotEvidence> {
    if replica.state.0.identity.group.cluster_id != request.cluster_id
        || replica.state.0.identity.group.group_id != request.group_id
        || !request.voters.contains(&replica.node_id())
        || !matches!(request.voters.len(), 1 | 3)
        || request.partitions.len() > 64
        || request.object_id.is_some() == request.floor.is_some()
        || request
            .partitions
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != request.partitions.len()
        || request.object_id.is_some() && (request.partitions.is_empty() || request.generation == 0)
        || request.floor.is_some() && !request.partitions.is_empty()
    {
        return Err(Error::Authorization);
    }
    let applied = loop {
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        if super::coordinator::stable_voters(&replica.raft)? != request.voters {
            return Err(Error::Rejected {
                reason: Rejection::Membership,
            });
        }
        let position = replica
            .state
            .try_with_applied(
                |state, applied| {
                    if request.ready(state) { applied } else { None }
                },
            )
            .await
            .map_err(|source| Error::Storage { source })?;
        if let Some(position) = position {
            break position;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    let mut state = replica.state.clone();
    let snapshot = tokio::time::timeout_at(deadline.into(), state.build_snapshot())
        .await
        .map_err(|_| Error::Deadline)?
        .map_err(|source| Error::Consensus {
            reason: source.to_string(),
        })?;
    let position = snapshot
        .meta
        .last_log_id
        .filter(|position| *position >= applied)
        .ok_or(Error::Rejected {
            reason: Rejection::CheckpointRequired,
        })?;
    if super::coordinator::stable_voters(&replica.raft)? != request.voters {
        return Err(Error::Rejected {
            reason: Rejection::Membership,
        });
    }
    Ok(SnapshotEvidence {
        cluster_id: request.cluster_id,
        group_id: request.group_id,
        object_id: request.object_id,
        generation: request.generation,
        partitions: request.partitions.clone(),
        floor: request.floor,
        voters: request.voters.clone(),
        node_id: replica.node_id(),
        snapshot_id: snapshot.meta.snapshot_id,
        position,
    })
}

impl SnapshotRequest {
    fn ready(&self, state: &crate::broker::DataApplication) -> bool {
        match (self.object_id, self.floor) {
            (Some(id), None) => state.lifecycle().intent(id).is_some_and(|intent| {
                intent.phase == super::types::IntentPhase::Published
                    && intent.checkpoint_generation == Some(self.generation)
                    && self.partitions.iter().all(|partition| {
                        intent
                            .plan
                            .descriptor
                            .partitions
                            .iter()
                            .find(|index| super::catalog::key(index) == *partition)
                            .is_some_and(|index| {
                                state.producer(*partition).is_some_and(|producer| {
                                    producer.committed_end() >= index.offsets.end
                                }) && state.lifecycle().manifest(*partition).is_some_and(
                                    |manifest| manifest.published_end >= index.offsets.end,
                                )
                            })
                    })
            }),
            (None, Some(floor)) => {
                state
                    .producer(floor.partition)
                    .is_some_and(|producer| producer.retained_floor() >= floor.before)
                    && state
                        .lifecycle()
                        .manifest(floor.partition)
                        .map_or(0, |manifest| manifest.generation)
                        >= floor.manifest_generation
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Envelope<T> {
    cluster_id: Id,
    group_id: u64,
    data_group: u64,
    request: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Mutation {
    Object(Id),
    Pinned,
    Released,
    Retired,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Reply {
    Query(Box<QuorumView>),
    Mutation {
        proof: Box<QuorumView>,
        outcome: Mutation,
    },
    Rejected(Rejection),
    Redirect(Option<u64>),
    Unavailable,
}

struct RemoteMetadata {
    rpc: Arc<dyn LifecycleRpc>,
    cluster_id: Id,
    group_id: u64,
    nodes: Vec<u64>,
    mode: DeploymentMode,
}

impl MetadataReplica {
    pub(crate) fn remote(
        rpc: Arc<dyn LifecycleRpc>,
        cluster_id: Id,
        group_id: u64,
        nodes: Vec<u64>,
        mode: DeploymentMode,
    ) -> Result<Self> {
        let unique: BTreeSet<_> = nodes.iter().copied().collect();
        if group_id == 0
            || nodes.len() > 16
            || unique.contains(&0)
            || unique.len() != nodes.len()
            || nodes.len() < mode.replication_factor()
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            backend: Arc::new(RemoteMetadata {
                rpc,
                cluster_id,
                group_id,
                nodes,
                mode,
            }),
            cluster_id,
            group_id,
        })
    }
}

impl RemoteMetadata {
    async fn call<T: Serialize>(
        &self,
        kind: u32,
        data_group: u64,
        request: T,
        deadline: Instant,
    ) -> Result<(u64, Reply)> {
        let body = encode(&Envelope {
            cluster_id: self.cluster_id,
            group_id: self.group_id,
            data_group,
            request,
        })?;
        let class = if kind == LIFECYCLE_QUERY || body.len() > MAX_CONTROL_BODY {
            TrafficClass::Bulk
        } else {
            TrafficClass::Control
        };
        let mut target = self.nodes.first().copied().ok_or(Error::Configuration)?;
        for round in 0..self.nodes.len().saturating_add(2).min(18) {
            if Instant::now() >= deadline {
                return Err(if kind == LIFECYCLE_COMMAND {
                    Error::Unknown
                } else {
                    Error::Deadline
                });
            }
            match self
                .rpc
                .request(
                    target,
                    self.group_id,
                    class,
                    kind,
                    body.clone(),
                    deadline.min(Instant::now() + Duration::from_millis(500)),
                )
                .await
            {
                Ok(bytes) => match decode::<Reply>(&bytes)? {
                    Reply::Redirect(Some(next)) if self.nodes.contains(&next) && next != target => {
                        target = next;
                    }
                    Reply::Redirect(_) | Reply::Unavailable => {
                        target = *self
                            .nodes
                            .get((round + 1) % self.nodes.len())
                            .ok_or(Error::Configuration)?;
                    }
                    reply => return Ok((target, reply)),
                },
                Err(_) => {
                    target = *self
                        .nodes
                        .get((round + 1) % self.nodes.len())
                        .ok_or(Error::Configuration)?;
                }
            }
        }
        if kind == LIFECYCLE_COMMAND {
            Err(Error::Unknown)
        } else {
            Err(Error::Deadline)
        }
    }

    fn verify(&self, target: u64, proof: &QuorumView) -> Result<()> {
        let voters: BTreeSet<_> = proof.voters.iter().copied().collect();
        if proof.cluster_id != self.cluster_id
            || proof.group_id != self.group_id
            || proof.node_id != target
            || proof.mode != self.mode
            || proof.revision == 0
            || proof.position.index == 0
            || voters.len() != self.mode.replication_factor()
            || voters.len() != proof.voters.len()
            || !voters.contains(&target)
            || voters.iter().any(|node| !self.nodes.contains(node))
            || proof.nodes.len() != voters.len()
        {
            return Err(Error::Authorization);
        }
        let mut zones = BTreeSet::new();
        let mut described = BTreeSet::new();
        for node in &proof.nodes {
            if !voters.contains(&node.spec.node_id)
                || !node.spec.roles.contains(&NodeRole::Metadata)
                || !described.insert(node.spec.node_id)
                || node.spec.zone.is_empty()
                || node.spec.zone.len() > 64
            {
                return Err(Error::Authorization);
            }
            zones.insert(&node.spec.zone);
        }
        if self.mode == DeploymentMode::ThreeReplica && zones.len() != 3 {
            return Err(Error::Authorization);
        }
        Ok(())
    }
}

impl MetadataBackend for RemoteMetadata {
    fn query(&self, data_group: u64, query: Query, deadline: Instant) -> BoxFuture<'_, QuorumView> {
        Box::pin(async move {
            let (target, reply) = self
                .call(LIFECYCLE_QUERY, data_group, query.clone(), deadline)
                .await?;
            match reply {
                Reply::Query(proof) => {
                    self.verify(target, &proof)?;
                    validate_view(&query, &proof.view, self.cluster_id, data_group)?;
                    Ok(*proof)
                }
                Reply::Rejected(reason) => Err(Error::Rejected { reason }),
                _ => Err(Error::Wire),
            }
        })
    }
    fn execute(&self, command: CatalogCommand, deadline: Instant) -> BoxFuture<'_, CatalogOutcome> {
        Box::pin(async move {
            let data_group = command_group(&command);
            let expected = command.clone();
            let (target, reply) = self
                .call(LIFECYCLE_COMMAND, data_group, command, deadline)
                .await?;
            match reply {
                Reply::Mutation { proof, outcome } => {
                    self.verify(target, &proof)?;
                    if !matches_mutation(&expected, &outcome) {
                        return Err(Error::Authorization);
                    }
                    match outcome {
                        Mutation::Object(id) => match self
                            .query(data_group, Query::Object(id), deadline)
                            .await?
                            .view
                        {
                            View::Object(object) => Ok(CatalogOutcome::Object(object)),
                            _ => Err(Error::Wire),
                        },
                        Mutation::Pinned => Ok(CatalogOutcome::Pinned),
                        Mutation::Released => Ok(CatalogOutcome::Released),
                        Mutation::Retired => Ok(CatalogOutcome::Retired),
                    }
                }
                Reply::Rejected(reason) => Err(Error::Rejected { reason }),
                _ => Err(Error::Unknown),
            }
        })
    }
    fn lease(&self, fence: FenceRecord, deadline: Instant) -> BoxFuture<'_, ControlLease> {
        self.rpc.control_lease(fence, deadline)
    }
}

pub(crate) struct MetadataHandler {
    local: LocalMetadata,
}
impl MetadataHandler {
    pub(crate) fn new(raft: Raft<MetadataState>, state: DiskStateMachine<MetadataState>) -> Self {
        Self {
            local: LocalMetadata { raft, state },
        }
    }

    pub(crate) async fn handle(
        &self,
        peer: &Authorization,
        kind: u32,
        class: TrafficClass,
        body: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let peer = self.authorize(peer)?;
        if !matches!(class, TrafficClass::Control | TrafficClass::Bulk)
            || body.len()
                > if class == TrafficClass::Control {
                    MAX_CONTROL_BODY
                } else {
                    MAX_BODY
                }
        {
            return Err(Error::Wire);
        }
        let result = match kind {
            LIFECYCLE_QUERY if class == TrafficClass::Bulk => {
                let envelope: Envelope<Query> = decode(body)?;
                self.check_identity(&envelope)?;
                self.query_scope(peer, envelope.data_group, &envelope.request)
                    .await?;
                self.local
                    .read(envelope.data_group, envelope.request, deadline)
                    .await
                    .map(|proof| Reply::Query(Box::new(proof)))
            }
            LIFECYCLE_COMMAND => {
                let envelope: Envelope<CatalogCommand> = decode(body)?;
                self.check_identity(&envelope)?;
                self.command_scope(peer, envelope.data_group, &envelope.request)
                    .await?;
                self.mutate(envelope, deadline).await
            }
            _ => return Err(Error::Wire),
        };
        let reply = match result {
            Ok(reply) => reply,
            Err(Error::Rejected { reason }) => Reply::Rejected(reason),
            Err(Error::Consensus { .. }) => {
                Reply::Redirect(self.local.raft.metrics().borrow().current_leader)
            }
            Err(_) => Reply::Unavailable,
        };
        encode(&reply)
    }

    async fn mutate(&self, envelope: Envelope<CatalogCommand>, deadline: Instant) -> Result<Reply> {
        let outcome = self.local.execute(envelope.request, deadline).await?;
        let outcome = match outcome {
            CatalogOutcome::Object(object) => Mutation::Object(object.object_id()),
            CatalogOutcome::Pinned => Mutation::Pinned,
            CatalogOutcome::Released => Mutation::Released,
            CatalogOutcome::Retired => Mutation::Retired,
        };
        let proof = self
            .local
            .read(envelope.data_group, Query::Health, deadline)
            .await?;
        Ok(Reply::Mutation {
            proof: Box::new(proof),
            outcome,
        })
    }

    fn authorize<'a>(&self, authorization: &'a Authorization) -> Result<&'a NodeGrant> {
        let Authorization::Internal(peer) = authorization else {
            return Err(Error::Authorization);
        };
        if peer.cluster_id != self.local.state.0.identity.group.cluster_id
            || !peer
                .allowed_groups
                .contains(&self.local.state.0.identity.group.group_id)
            || !matches!(peer.role.as_str(), "storage" | "metadata" | "combined")
        {
            return Err(Error::Authorization);
        }
        Ok(peer)
    }
    async fn query_scope(&self, peer: &NodeGrant, group: u64, query: &Query) -> Result<()> {
        let (objects, partitions) = match query {
            Query::Object(id) | Query::Diagnose(id) | Query::Compaction(id) => (vec![*id], vec![]),
            Query::Prepare(ranges) => {
                if ranges.len() > 64 {
                    return Err(Error::Wire);
                }
                (vec![], ranges.iter().map(|range| range.partition).collect())
            }
            Query::Read { partition, .. } | Query::Reclaim { partition, .. } => {
                (vec![], vec![*partition])
            }
            Query::Pin(id) => {
                let objects = self
                    .local
                    .state
                    .try_with_application(|state| {
                        state
                            .lifecycle()
                            .pin(*id)
                            .map(|pin| pin.objects.clone())
                            .unwrap_or_default()
                    })
                    .await
                    .map_err(|source| Error::Storage { source })?;
                (objects, vec![])
            }
            _ => (vec![], vec![]),
        };
        self.scope(peer, group, &objects, &partitions).await
    }

    async fn command_scope(
        &self,
        peer: &NodeGrant,
        group: u64,
        command: &CatalogCommand,
    ) -> Result<()> {
        let objects = match command {
            CatalogCommand::Checkpoint {
                object_id,
                group_id,
                scopes,
                ..
            } => {
                return self
                    .scope(
                        peer,
                        *group_id,
                        &[*object_id],
                        &scopes
                            .iter()
                            .map(|scope| scope.partition)
                            .collect::<Vec<_>>(),
                    )
                    .await;
            }
            CatalogCommand::RetireBefore(proof) | CatalogCommand::FloorCheckpoint { proof, .. } => {
                return self
                    .scope(peer, proof.group_id, &[], &[proof.partition])
                    .await;
            }
            CatalogCommand::Register(plan) => {
                if plan.descriptor.partitions.len() > 64 || plan.replaces.len() > 64 {
                    return Err(Error::Wire);
                }
                if !peer
                    .allowed_groups
                    .contains(&plan.descriptor.identity.group_id)
                {
                    return Err(Error::Authorization);
                }
                plan.replaces.clone()
            }
            CatalogCommand::Pin(pin) => {
                return self
                    .scope(peer, pin.group_id, &pin.objects, &pin.partitions)
                    .await;
            }
            CatalogCommand::ReleasePin { pin } => pin.objects.clone(),
            CatalogCommand::Uploaded { object_id }
            | CatalogCommand::Publish { object_id }
            | CatalogCommand::RequestCancel { object_id }
            | CatalogCommand::FinishCancel { object_id, .. }
            | CatalogCommand::BeginDelete { object_id }
            | CatalogCommand::Deleted { object_id } => vec![*object_id],
        };
        self.scope(peer, group, &objects, &[]).await
    }

    async fn scope(
        &self,
        peer: &NodeGrant,
        group: u64,
        objects: &[Id],
        partitions: &[crate::metadata::PartitionKey],
    ) -> Result<()> {
        if objects.len() > 64
            || partitions.len() > 64
            || (group != 0 && !peer.allowed_groups.contains(&group))
        {
            return Err(Error::Authorization);
        }
        self.local
            .state
            .try_with_application(|state| {
                for id in objects {
                    let object = state.lifecycle().object(*id).ok_or(Error::Rejected {
                        reason: Rejection::NotFound,
                    })?;
                    if !peer
                        .allowed_groups
                        .contains(&object.descriptor().identity.group_id)
                        && !object.descriptor().partitions.iter().any(|index| {
                            let partition = super::catalog::key(index);
                            state
                                .partition_group(partition)
                                .is_some_and(|group| peer.allowed_groups.contains(&group))
                                && state.topology().binding(partition).is_some_and(|binding| {
                                    binding.accepts_object(object.descriptor().identity.group_id)
                                })
                        })
                    {
                        return Err(Error::Authorization);
                    }
                }
                for partition in partitions {
                    let group = state.partition_group(*partition).ok_or(Error::Rejected {
                        reason: Rejection::NotFound,
                    })?;
                    if !peer.allowed_groups.contains(&group) {
                        return Err(Error::Authorization);
                    }
                }
                Ok(())
            })
            .await
            .map_err(|source| Error::Storage { source })?
    }

    fn check_identity<T>(&self, envelope: &Envelope<T>) -> Result<()> {
        if envelope.cluster_id != self.local.state.0.identity.group.cluster_id
            || envelope.group_id != self.local.state.0.identity.group.group_id
        {
            return Err(Error::Authorization);
        }
        Ok(())
    }
}

fn command_group(command: &CatalogCommand) -> u64 {
    match command {
        CatalogCommand::Register(plan) => plan.descriptor.identity.group_id,
        CatalogCommand::Checkpoint { group_id, .. } => *group_id,
        _ => 0,
    }
}

fn matches_mutation(command: &CatalogCommand, outcome: &Mutation) -> bool {
    match (command, outcome) {
        (CatalogCommand::Pin(_), Mutation::Pinned)
        | (CatalogCommand::ReleasePin { .. }, Mutation::Released)
        | (
            CatalogCommand::RetireBefore(_) | CatalogCommand::FloorCheckpoint { .. },
            Mutation::Retired,
        ) => true,
        (CatalogCommand::Register(plan), Mutation::Object(id)) => {
            plan.descriptor.identity.object_id == *id
        }
        (
            CatalogCommand::Uploaded { object_id }
            | CatalogCommand::Publish { object_id }
            | CatalogCommand::RequestCancel { object_id }
            | CatalogCommand::FinishCancel { object_id, .. }
            | CatalogCommand::Checkpoint { object_id, .. }
            | CatalogCommand::BeginDelete { object_id }
            | CatalogCommand::Deleted { object_id },
            Mutation::Object(id),
        ) => *object_id == *id,
        _ => false,
    }
}

fn validate_view(query: &Query, view: &View, cluster: Id, data_group: u64) -> Result<()> {
    let valid = match (query, view) {
        (Query::Capacity, View::Capacity(usage)) => {
            usage.objects <= super::types::MAX_OBJECTS
                && usage.deleted_objects <= usage.objects
                && usage.read_pins <= super::types::MAX_PINS
        }
        (Query::Objects { after, limit }, View::Objects(page)) => {
            *limit > 0
                && page.objects.len() <= *limit
                && *limit <= 64
                && page
                    .objects
                    .iter()
                    .all(|(id, _)| after.is_none_or(|after| *id > after))
                && page.objects.windows(2).all(|pair| {
                    pair.first()
                        .zip(pair.get(1))
                        .is_some_and(|(a, b)| a.0 < b.0)
                })
                && valid_page_cursor(
                    *after,
                    page.next_after,
                    page.objects.last().map(|(id, _)| *id),
                )
        }
        (Query::Pins { after, limit }, View::Pins(page)) => {
            *limit > 0
                && *limit <= 64
                && page.pins.len() <= *limit
                && page
                    .pins
                    .iter()
                    .all(|id| after.is_none_or(|after| *id > after))
                && page
                    .pins
                    .windows(2)
                    .all(|pair| pair.first().zip(pair.get(1)).is_some_and(|(a, b)| a < b))
                && valid_page_cursor(*after, page.next_after, page.pins.last().copied())
        }
        (Query::Health, View::Health) => true,
        (Query::Pin(id), View::Pin(pin)) => pin.as_ref().is_none_or(|pin| {
            pin.id == *id
                && pin.group_id > 0
                && !pin.objects.is_empty()
                && pin.objects.len() <= 64
                && !pin.partitions.is_empty()
                && pin.partitions.len() <= super::types::MAX_PARTITIONS
                && pin.ttl_millis > 0
                && pin.ttl_millis <= 10_000
        }),
        (Query::Compaction(_), View::Compaction(ranges)) => ranges.as_ref().is_none_or(|ranges| {
            !ranges.is_empty()
                && ranges.len() <= super::types::MAX_PARTITIONS
                && ranges
                    .iter()
                    .all(|range| range.offsets.start < range.offsets.end)
                && ranges
                    .iter()
                    .map(|range| range.partition)
                    .collect::<BTreeSet<_>>()
                    .len()
                    == ranges.len()
        }),
        (Query::Diagnose(id), View::Diagnostic(diagnostic)) => {
            diagnostic.object_id == *id
                && diagnostic.authority_scopes.len() <= 64
                && diagnostic
                    .authority_scopes
                    .iter()
                    .all(|scope| scope.binding_generation > 0)
                && diagnostic
                    .authority_scopes
                    .iter()
                    .map(|scope| scope.partition)
                    .collect::<BTreeSet<_>>()
                    .len()
                    == diagnostic.authority_scopes.len()
        }
        (Query::DataVoters, View::DataVoters(group)) => {
            group.group_id == data_group && matches!(group.voters.len(), 1 | 3)
        }
        (Query::Object(id), View::Object(object)) => {
            object.object_id() == *id && object.descriptor().identity.cluster_id == cluster
        }
        (Query::Read { partition, first }, View::Read { object, generation }) => {
            *generation > 0
                && object.descriptor().identity.cluster_id == cluster
                && object.descriptor().partitions.iter().any(|i| {
                    i.topic_id == partition.topic_id
                        && i.partition_id == partition.partition
                        && i.offsets.start <= *first
                        && *first < i.offsets.end
                })
        }
        (
            Query::Prepare(ranges),
            View::Prepared {
                grants,
                generations,
            },
        ) => {
            ranges.len() == grants.len()
                && ranges.len() == generations.len()
                && ranges.iter().all(|range| {
                    grants.iter().any(|grant| {
                        grant.partition == range.partition
                            && grant.cluster_id == cluster
                            && grant.group_id == data_group
                    }) && generations.contains_key(&range.partition)
                })
        }
        (Query::Reclaim { .. }, View::Reclaim { .. }) => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(Error::Authorization)
    }
}

fn valid_page_cursor(after: Option<Id>, next: Option<Id>, last: Option<Id>) -> bool {
    next.is_none_or(|cursor| {
        after.is_none_or(|after| cursor > after) && last.is_none_or(|last| cursor >= last)
    })
}

struct Bounded {
    bytes: Vec<u8>,
}
impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|size| size > MAX_BODY)
        {
            return Err(std::io::Error::other("Lifecycle payload exceeds its bound"));
        }
        self.bytes
            .try_reserve(bytes.len())
            .map_err(std::io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encode(value: &impl Serialize) -> Result<Bytes> {
    let mut output = Bounded { bytes: Vec::new() };
    serde_json::to_writer(&mut output, value).map_err(|_| Error::Wire)?;
    Ok(Bytes::from(output.bytes))
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.is_empty() || bytes.len() > MAX_BODY {
        return Err(Error::Wire);
    }
    serde_json::from_slice(bytes).map_err(|_| Error::Wire)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{NodeRecord, NodeSpec};

    struct NoRpc;
    impl LifecycleRpc for NoRpc {
        fn request(
            &self,
            _: u64,
            _: u64,
            _: TrafficClass,
            _: u32,
            _: Bytes,
            _: Instant,
        ) -> RpcFuture<'_, Bytes> {
            Box::pin(async { Err(Error::Wire) })
        }
        fn control_lease(&self, _: FenceRecord, _: Instant) -> RpcFuture<'_, ControlLease> {
            Box::pin(async { Err(Error::Wire) })
        }
    }

    fn proof(cluster_id: Id) -> QuorumView {
        QuorumView {
            cluster_id,
            group_id: 77,
            node_id: 2,
            revision: 5,
            mode: DeploymentMode::LocalRf1,
            voters: vec![2],
            nodes: vec![NodeRecord {
                spec: NodeSpec {
                    node_id: 2,
                    zone: "zone-a".into(),
                    endpoint: "127.0.0.1:19002".parse().unwrap(),
                    roles: BTreeSet::from([NodeRole::Metadata]),
                    schedulable: true,
                },
                generation: 1,
            }],
            position: LogId::new(openraft::CommittedLeaderId::new(1, 2), 8),
            view: View::Health,
        }
    }

    #[test]
    fn remote_proof_requires_exact_target_and_never_downgrades_three_voters() {
        let cluster = Id::new_v7().unwrap();
        let mut proof = proof(cluster);
        let remote = RemoteMetadata {
            rpc: Arc::new(NoRpc),
            cluster_id: cluster,
            group_id: 77,
            nodes: vec![2],
            mode: DeploymentMode::LocalRf1,
        };
        remote.verify(2, &proof).unwrap();
        assert!(remote.verify(1, &proof).is_err());
        proof.cluster_id = Id::new_v7().unwrap();
        assert!(remote.verify(2, &proof).is_err());
        proof.cluster_id = cluster;
        let production = RemoteMetadata {
            rpc: Arc::new(NoRpc),
            cluster_id: cluster,
            group_id: 77,
            nodes: vec![1, 2, 3],
            mode: DeploymentMode::ThreeReplica,
        };
        assert!(production.verify(2, &proof).is_err());
    }

    #[test]
    fn bounded_wire_codec_and_result_identity_reject_malformed_payloads() {
        assert!(encode(&vec![b'x'; MAX_BODY]).is_err());
        assert!(decode::<Reply>(&vec![b' '; MAX_BODY + 1]).is_err());
        assert!(decode::<Reply>(br#"{"Unknown":null}"#).is_err());
        let id = Id::new_v7().unwrap();
        let other = Id::new_v7().unwrap();
        assert!(matches_mutation(
            &CatalogCommand::Deleted { object_id: id },
            &Mutation::Object(id)
        ));
        assert!(!matches_mutation(
            &CatalogCommand::Deleted { object_id: id },
            &Mutation::Object(other)
        ));
        let query = Query::Objects {
            after: Some(id),
            limit: 1,
        };
        let page = View::Objects(super::super::ObjectPage {
            objects: vec![(id, super::super::ObjectPhase::Deleted)],
            next_after: Some(id),
        });
        assert!(validate_view(&query, &page, Id::new_v7().unwrap(), 9).is_err());
    }
}
