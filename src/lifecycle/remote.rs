//! Closed internal RPCs. Only the authenticated runtime may construct remote authority adapters.

#![allow(dead_code)] // The separately developed runtime assembly consumes these private adapters.

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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SnapshotRequest {
    cluster_id: Id,
    group_id: u64,
    object_id: Id,
    generation: u64,
    voters: BTreeSet<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotEvidence {
    pub(crate) cluster_id: Id,
    pub(crate) group_id: u64,
    pub(crate) object_id: Id,
    pub(crate) generation: u64,
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
        deadline: Instant,
    ) -> Result<SnapshotEvidence> {
        let request = SnapshotRequest {
            cluster_id: self.cluster_id,
            group_id: self.group_id,
            object_id,
            generation,
            voters: voters.clone(),
        };
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
            || proof.object_id != object_id
            || proof.generation != generation
            || proof.voters != *voters
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
            .try_with_applied(|state, applied| {
                state
                    .lifecycle()
                    .intent(request.object_id)
                    .filter(|intent| {
                        intent.phase == super::types::IntentPhase::Published
                            && intent.checkpoint_generation == Some(request.generation)
                    })
                    .and(applied)
            })
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
        voters: request.voters.clone(),
        node_id: replica.node_id(),
        snapshot_id: snapshot.meta.snapshot_id,
        position,
    })
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
            let (target, reply) = self
                .call(LIFECYCLE_COMMAND, data_group, command, deadline)
                .await?;
            match reply {
                Reply::Mutation { proof, outcome } => {
                    self.verify(target, &proof)?;
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
            Query::Object(id) | Query::Diagnose(id) => (vec![*id], vec![]),
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
            CatalogCommand::Pin(pin) | CatalogCommand::ReleasePin { pin } => pin.objects.clone(),
            CatalogCommand::Uploaded { object_id }
            | CatalogCommand::Publish { object_id }
            | CatalogCommand::RequestCancel { object_id }
            | CatalogCommand::FinishCancel { object_id, .. }
            | CatalogCommand::Checkpoint { object_id, .. }
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
        _ => 0,
    }
}

fn validate_view(query: &Query, view: &View, cluster: Id, data_group: u64) -> Result<()> {
    let valid = match (query, view) {
        (Query::Health, View::Health)
        | (Query::Pin(_), View::Pin(_))
        | (Query::Diagnose(_), View::Diagnostic(_)) => true,
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
        (Query::Reclaim { partition, end }, View::Reclaim { generation, plan }) => {
            *generation > 0
                && plan.descriptor.identity.cluster_id == cluster
                && plan.descriptor.partitions.iter().any(|i| {
                    i.topic_id == partition.topic_id
                        && i.partition_id == partition.partition
                        && i.offsets.start < *end
                        && i.offsets.end >= *end
                })
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(Error::Authorization)
    }
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
