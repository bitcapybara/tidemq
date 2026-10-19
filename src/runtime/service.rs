use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tidemq_protocol::Id;
use tidemq_protocol::common::{CancelRequest, ErrorCode, Frame, FrameKind, Outcome, TrafficClass};
use tidemq_protocol::framing::{decode_frame, decode_payload, encode_payload, frame_limit};
use tidemq_protocol::internal::{InternalCommand, RaftRequest, SnapshotChunk};
use tokio::sync::{Mutex, RwLock, watch};
use tokio::time::Instant;

use crate::broker::{Broker, BrokerLimits, DataApplication, ProducerMetrics};
use crate::budget::MemoryBudget;
use crate::config::Config;
use crate::gateway::{Access, AccessTable};
use crate::metadata::{MetadataState, PartitionKey, Route};
use crate::observability::metrics::{Metrics, Operation};
use crate::ownership::remote::{Query, RemoteMetadataAuthority, Value};
use crate::replication::rpc::{GroupRole, RaftService};
use crate::server::{SubmissionOrder, WireService};
use crate::transport::Authorization;

use super::node::{Replica, data_config};
use super::wire::{self, PeerRpc, Request};
use super::{Error, Result};

pub(crate) struct CachedRoute {
    pub route: Route,
    pub topic_name: String,
    pub expires: Instant,
}
type CancelKey = (String, String, Id);

pub(crate) struct Service {
    pub config: Config,
    pub cluster: Id,
    pub metadata: Option<Replica<MetadataState>>,
    pub data: BTreeMap<u64, Replica<DataApplication>>,
    pub brokers: BTreeMap<(u64, u64), Broker>,
    pub rpc: PeerRpc,
    pub authority: RemoteMetadataAuthority,
    pub access: AccessTable,
    pub delivery_key: [u8; 32],
    pub budget: MemoryBudget,
    pub metrics: Metrics,
    pub runtime_metrics: super::telemetry::RuntimeMetrics,
    pub log_health: std::sync::Mutex<Option<crate::observability::logging::LogHealth>>,
    pub draining: AtomicBool,
    pub routes: RwLock<BTreeMap<PartitionKey, CachedRoute>>,
    pub leaders: RwLock<BTreeMap<u64, (u64, Instant)>>,
    pub leases: Mutex<BTreeMap<PartitionKey, Arc<crate::ownership::ControlLease>>>,
    pub metadata_mutation: Mutex<()>,
    cancellations: Arc<std::sync::Mutex<BTreeMap<CancelKey, watch::Sender<bool>>>>,
}

impl Service {
    // Keep process-owned facilities explicit at the single runtime assembly boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        cluster: Id,
        metadata: Option<Replica<MetadataState>>,
        data: BTreeMap<u64, Replica<DataApplication>>,
        rpc: PeerRpc,
        authority: RemoteMetadataAuthority,
        access: AccessTable,
        delivery_key: [u8; 32],
        budget: MemoryBudget,
        producer: ProducerMetrics,
        metrics: Metrics,
        runtime_metrics: super::telemetry::RuntimeMetrics,
    ) -> Result<Self> {
        let mut brokers = BTreeMap::new();
        for (group, replica) in &data {
            let group = config.runtime.group(*group)?;
            for owner in config
                .runtime
                .peers
                .iter()
                .filter(|peer| peer.role.serves_broker())
            {
                let mut broker = Broker::new(
                    owner.node_id,
                    replica.raft.clone(),
                    replica.state.clone(),
                    data_config(cluster, group),
                    budget.clone(),
                    BrokerLimits::default(),
                    producer.clone(),
                )
                .map_err(|source| {
                    Error::operation("create bounded data authority adapter", source)
                })?;
                if let Some(stages) = metrics.operation(Operation::Produce) {
                    broker = broker.with_stages(stages);
                }
                brokers.insert((group.group_id, owner.node_id), broker);
            }
        }
        Ok(Self {
            config,
            cluster,
            metadata,
            data,
            brokers,
            rpc,
            authority,
            access,
            delivery_key,
            budget,
            metrics,
            runtime_metrics,
            log_health: std::sync::Mutex::new(None),
            draining: AtomicBool::new(false),
            routes: RwLock::new(BTreeMap::new()),
            leaders: RwLock::new(BTreeMap::new()),
            leases: Mutex::new(BTreeMap::new()),
            metadata_mutation: Mutex::new(()),
            cancellations: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        })
    }

    pub async fn close(&self) -> Result<()> {
        for broker in self.brokers.values() {
            let _ = broker
                .drain(std::time::Instant::now() + Duration::from_secs(5))
                .await;
        }
        for replica in self.data.values() {
            replica
                .raft
                .shutdown()
                .await
                .map_err(|source| Error::operation("stop data Raft replica", source))?;
            replica
                .log
                .close()
                .await
                .map_err(|source| Error::operation("close data WAL", source))?;
        }
        if let Some(replica) = &self.metadata {
            replica
                .raft
                .shutdown()
                .await
                .map_err(|source| Error::operation("stop metadata Raft replica", source))?;
            replica
                .log
                .close()
                .await
                .map_err(|source| Error::operation("close metadata WAL", source))?;
        }
        self.rpc
            .pool
            .close(Instant::now() + Duration::from_secs(2))
            .await;
        Ok(())
    }

    pub async fn route(&self, partition: PartitionKey, deadline: Instant) -> Result<Route> {
        if let Some(entry) = self
            .routes
            .read()
            .await
            .get(&partition)
            .filter(|entry| entry.expires > Instant::now())
        {
            return Ok(entry.route.clone());
        }
        let Value::Partition {
            topic,
            route: Some(route),
            ..
        } = self
            .authority
            .query(Query::Partition(partition), deadline)
            .await?
        else {
            return Err(Error::unavailable());
        };
        let mut cache = self.routes.write().await;
        if cache.len() >= 2048 && !cache.contains_key(&partition) {
            cache.retain(|_, entry| entry.expires > Instant::now());
        }
        if cache.len() >= 2048 && !cache.contains_key(&partition) {
            return Err(Error::reject(
                ErrorCode::QuotaExceeded,
                Outcome::Rejected,
                true,
            ));
        }
        cache.insert(
            partition,
            CachedRoute {
                route: route.clone(),
                topic_name: topic.name,
                expires: Instant::now() + Duration::from_millis(500),
            },
        );
        Ok(route)
    }

    pub async fn authorize_partition(
        &self,
        access: &Access,
        partition: PartitionKey,
        deadline: Instant,
    ) -> Result<Route> {
        let route = self.route(partition, deadline).await?;
        let routes = self.routes.read().await;
        let topic = routes.get(&partition).ok_or_else(Error::unavailable)?;
        access.authorize_topic(&topic.topic_name)?;
        Ok(route)
    }

    pub async fn leader(&self, group: u64, deadline: Instant) -> Result<u64> {
        if let Some((leader, expires)) = self
            .leaders
            .read()
            .await
            .get(&group)
            .filter(|(_, expires)| *expires > Instant::now())
        {
            let _ = expires;
            return Ok(*leader);
        }
        let voters = if group == self.config.runtime.metadata_group {
            &self.config.runtime.metadata_voters
        } else {
            &self.config.runtime.group(group)?.voters
        };
        for node in voters {
            let request = Request::control(wire::LEADER, &())?;
            let attempt_deadline = deadline.min(Instant::now() + Duration::from_millis(500));
            if let Ok(bytes) = self.rpc.call(*node, group, request, attempt_deadline).await {
                let leader: Option<u64> = wire::decode_json(&bytes)?;
                if let Some(leader) = leader.filter(|leader| voters.contains(leader)) {
                    self.leaders
                        .write()
                        .await
                        .insert(group, (leader, Instant::now() + Duration::from_millis(200)));
                    return Ok(leader);
                }
            }
        }
        Err(Error::unavailable())
    }

    pub fn require_leader<A: crate::replication::Application>(
        &self,
        replica: &Replica<A>,
    ) -> Result<()> {
        if replica.raft.metrics().borrow().current_leader != Some(self.config.node_id) {
            return Err(Error::unavailable());
        }
        Ok(())
    }

    async fn public(
        &self,
        access: Access,
        frame: &Frame,
        deadline: Instant,
        order: Option<SubmissionOrder>,
        hops: u32,
    ) -> Result<Frame> {
        if self.draining.load(Ordering::Acquire) {
            return Err(Error::unavailable());
        }
        let kind = frame_kind(frame)?;
        if matches!(
            kind,
            FrameKind::CreateTopic
                | FrameKind::GetTopic
                | FrameKind::CreateSubscription
                | FrameKind::GetSubscription
        ) {
            let leader = self
                .leader(self.config.runtime.metadata_group, deadline)
                .await?;
            if leader == self.config.node_id {
                drop(order);
                return self.metadata_public(access, frame, deadline).await;
            }
            return self
                .forward(
                    leader,
                    self.config.runtime.metadata_group,
                    access,
                    frame,
                    deadline,
                    order,
                    hops + 1,
                )
                .await;
        }
        let partition = super::data::partition(frame, &self.config.protocol)?;
        let route = self
            .authorize_partition(&access, partition, deadline)
            .await?;
        let mut forwarded = frame.clone();
        super::data::set_owner_epoch(
            &mut forwarded,
            route.epoch,
            &self.config.protocol,
            hops == 0,
        )?;
        if route.owner_node != self.config.node_id && hops < 2 {
            return self
                .forward(
                    route.owner_node,
                    route.group_id,
                    access,
                    &forwarded,
                    deadline,
                    order,
                    hops + 1,
                )
                .await;
        }
        if hops < 2 && !self.config.role.serves_broker() {
            return Err(forbidden());
        }
        let leader = self.leader(route.group_id, deadline).await?;
        if leader != self.config.node_id {
            return self
                .forward(
                    leader,
                    route.group_id,
                    access,
                    &forwarded,
                    deadline,
                    order,
                    2,
                )
                .await;
        }
        self.data_public(access, &forwarded, route, deadline, order)
            .await
    }

    // Explicit routing and deadline fields prevent accidental loss at a trust boundary.
    #[allow(clippy::too_many_arguments)]
    async fn forward(
        &self,
        target: u64,
        group: u64,
        access: Access,
        frame: &Frame,
        deadline: Instant,
        order: Option<SubmissionOrder>,
        hops: u32,
    ) -> Result<Frame> {
        if hops > 2 {
            return Err(Error::invalid("Forwarding hop budget is exhausted"));
        }
        let class = TrafficClass::try_from(frame.traffic_class)
            .map_err(|_| Error::invalid("Forwarded traffic class is invalid"))?;
        let request = Request {
            kind: wire::FORWARD,
            body: encode_payload(frame, frame_limit(class, &self.config.protocol))
                .map_err(|source| Error::operation("encode delegated public frame", source))?,
            principal: access.principal,
            scope: access.scope,
            hops,
        };
        let affinity = super::data::affinity(frame, &self.config.protocol)?;
        let pending = self
            .rpc
            .submit(
                target,
                group,
                class,
                wire::id(&frame.operation_id)?,
                request,
                deadline,
                affinity,
            )
            .await?;
        drop(order);
        let bytes = pending.receive().await?;
        let response = decode_frame(bytes, class, &self.config.protocol)
            .map_err(|source| Error::operation("validate forwarded response", source))?;
        if response.request_id != frame.request_id || response.operation_id != frame.operation_id {
            return Err(Error::invalid(
                "Forwarded response does not match the original request",
            ));
        }
        Ok(response)
    }

    async fn internal(
        &self,
        peer: &crate::transport::NodeGrant,
        frame: &Frame,
        deadline: Instant,
        order: Option<SubmissionOrder>,
    ) -> Result<Frame> {
        if peer.cluster_id != self.cluster {
            return Err(forbidden());
        }
        let envelope: InternalCommand =
            decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode semantic envelope", source))?;
        if envelope.operation_id != frame.operation_id
            || !peer.allowed_groups.contains(&envelope.group_id)
        {
            return Err(forbidden());
        }
        let request: Request = decode_payload(envelope.encoded_command, &self.config.protocol)
            .map_err(|source| Error::operation("decode semantic operation", source))?;
        let group = envelope.group_id;
        let value = match request.kind {
            wire::LEADER => {
                drop(order);
                if frame.traffic_class != i32::from(TrafficClass::Control) {
                    return Err(forbidden());
                }
                let leader = if group == self.config.runtime.metadata_group {
                    self.metadata
                        .as_ref()
                        .and_then(|replica| replica.raft.metrics().borrow().current_leader)
                } else {
                    self.data
                        .get(&group)
                        .and_then(|replica| replica.raft.metrics().borrow().current_leader)
                };
                wire::json(&leader)?
            }
            wire::AUTHORITY => {
                drop(order);
                if group != self.config.runtime.metadata_group
                    || frame.traffic_class != i32::from(TrafficClass::Control)
                {
                    return Err(forbidden());
                }
                let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
                self.require_leader(replica)?;
                let authority = crate::ownership::MetadataAuthority::new(
                    replica.raft.clone(),
                    replica.state.clone(),
                    self.cluster,
                    group,
                    crate::ownership::ControlPolicy::default(),
                )
                .map_err(|source| Error::operation("create metadata authority", source))?;
                let query: Query = wire::decode_json(&request.body)?;
                if peer.role == "gateway" && matches!(query, Query::Grant(_) | Query::Lease(_)) {
                    return Err(forbidden());
                }
                let proof = authority
                    .remote_query(query)
                    .await
                    .map_err(|source| Error::operation("perform metadata quorum read", source))?;
                wire::json(&proof)?
            }
            wire::OWNERSHIP => {
                drop(order);
                if !matches!(peer.role.as_str(), "metadata" | "combined") {
                    return Err(forbidden());
                }
                self.ownership_command(group, &request.body, deadline)
                    .await?
            }
            wire::CREATE_SUBSCRIPTION => {
                drop(order);
                if !matches!(peer.role.as_str(), "metadata" | "combined") {
                    return Err(forbidden());
                }
                self.create_data_subscription(group, &request.body, deadline)
                    .await?
            }
            wire::FORWARD => {
                if !matches!(peer.role.as_str(), "gateway" | "broker" | "combined")
                    || request.hops == 0
                    || request.hops > 2
                    || request.principal.len() > 128
                    || request.scope.len() > 128
                {
                    return Err(forbidden());
                }
                let class = TrafficClass::try_from(frame.traffic_class)
                    .map_err(|_| Error::invalid("Delegated class is invalid"))?;
                let inner =
                    decode_frame(request.body, class, &self.config.protocol).map_err(|source| {
                        Error::operation("validate delegated public frame", source)
                    })?;
                let access = self.access.delegated(
                    &request.principal,
                    &request.scope,
                    frame_kind(&inner)?,
                )?;
                if request.hops == 2 {
                    let partition = super::data::partition(&inner, &self.config.protocol)?;
                    let route = self
                        .authorize_partition(&access, partition, deadline)
                        .await?;
                    if route.group_id != group
                        || route.owner_node != peer.node_id
                        || !matches!(peer.role.as_str(), "broker" | "combined")
                    {
                        return Err(forbidden());
                    }
                }
                let reply = self
                    .public(access, &inner, deadline, order, request.hops)
                    .await?;
                encode_payload(&reply, frame_limit(class, &self.config.protocol))
                    .map_err(|source| Error::operation("encode delegated public result", source))?
            }
            _ => return Err(Error::invalid("Unknown semantic operation")),
        };
        wire::internal_response(frame, value, &self.config.protocol)
    }

    async fn raft(&self, peer: &Authorization, frame: &Frame) -> Result<Frame> {
        let group = if frame.kind == i32::from(FrameKind::RaftSnapshot) {
            decode_payload::<SnapshotChunk>(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode snapshot group", source))?
                .group_id
        } else {
            decode_payload::<RaftRequest>(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode Raft group", source))?
                .group_id
        };
        if group == self.config.runtime.metadata_group {
            let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
            RaftService::new(
                replica.raft.clone(),
                self.cluster,
                group,
                GroupRole::Metadata,
                replica.voters.clone(),
                self.config.protocol.clone(),
            )
            .map_err(|source| Error::operation("create metadata Raft dispatch", source))?
            .handle(peer, frame)
            .await
            .map_err(|source| Error::operation("handle metadata Raft RPC", source))
        } else {
            let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
            RaftService::new(
                replica.raft.clone(),
                self.cluster,
                group,
                GroupRole::Data,
                replica.voters.clone(),
                self.config.protocol.clone(),
            )
            .map_err(|source| Error::operation("create data Raft dispatch", source))?
            .handle(peer, frame)
            .await
            .map_err(|source| Error::operation("handle data Raft RPC", source))
        }
    }
}

impl WireService for Service {
    type Error = Error;
    async fn handle(
        &self,
        peer: Authorization,
        frame: &Frame,
        deadline: Instant,
        order: Option<SubmissionOrder>,
    ) -> Result<Option<Frame>> {
        let result = match &peer {
            Authorization::External { .. } => {
                let kind = frame_kind(frame)?;
                let access = self.access.external(&peer, kind)?;
                if kind == FrameKind::Cancel {
                    let cancel: CancelRequest =
                        decode_payload(frame.payload.clone(), &self.config.protocol)
                            .map_err(|source| Error::operation("decode cancellation", source))?;
                    let key = (
                        access.principal,
                        access.scope,
                        wire::id(&cancel.request_id)?,
                    );
                    if let Ok(cancellations) = self.cancellations.lock()
                        && let Some(cancelled) = cancellations.get(&key)
                    {
                        let _ = cancelled.send(true);
                    }
                    return Ok(None);
                }
                let key = (
                    access.principal.clone(),
                    access.scope.clone(),
                    wire::id(&frame.request_id)?,
                );
                let (cancel, mut cancelled) = watch::channel(false);
                {
                    let mut pending = self
                        .cancellations
                        .lock()
                        .map_err(|_| Error::invalid("Cancellation table is unavailable"))?;
                    if pending.len() >= 256 || pending.contains_key(&key) {
                        return wire::error_response(
                            frame,
                            &Error::reject(ErrorCode::QuotaExceeded, Outcome::Rejected, true),
                            &self.config.protocol,
                        )
                        .map(Some);
                    }
                    pending.insert(key.clone(), cancel);
                }
                let _registration = CancelRegistration {
                    table: self.cancellations.clone(),
                    key,
                };
                tokio::select! { result = self.public(access, frame, deadline, order, 0) => result, _ = cancelled.changed() => Err(Error::unknown()) }
            }
            Authorization::Internal(node)
                if frame.kind == i32::from(FrameKind::InternalCommand) =>
            {
                self.internal(node, frame, deadline, order).await
            }
            Authorization::Internal(_) => {
                drop(order);
                self.raft(&peer, frame).await
            }
            Authorization::Server => Err(forbidden()),
        };
        match result {
            Ok(frame) => Ok(Some(frame)),
            Err(error) => wire::error_response(frame, &error, &self.config.protocol).map(Some),
        }
    }
}

struct CancelRegistration {
    table: Arc<std::sync::Mutex<BTreeMap<CancelKey, watch::Sender<bool>>>>,
    key: CancelKey,
}
impl Drop for CancelRegistration {
    fn drop(&mut self) {
        if let Ok(mut table) = self.table.lock() {
            table.remove(&self.key);
        }
    }
}

pub(crate) fn frame_kind(frame: &Frame) -> Result<FrameKind> {
    FrameKind::try_from(frame.kind).map_err(|_| Error::invalid("Unknown public frame kind"))
}
pub(crate) fn forbidden() -> Error {
    Error::reject(ErrorCode::Forbidden, Outcome::Rejected, false)
}
