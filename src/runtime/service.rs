use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tidemq_protocol::Id;
use tidemq_protocol::common::{
    CancelRequest, ErrorCode, ErrorReply, Frame, FrameKind, Outcome, TrafficClass,
};
use tidemq_protocol::framing::{decode_frame, decode_payload, encode_payload, frame_limit};
use tidemq_protocol::internal::{InternalCommand, RaftRequest, SnapshotChunk};
use tokio::sync::{Mutex, Notify, RwLock, watch};
use tokio::time::Instant;

use crate::broker::{DataApplication, ProducerMetrics};
use crate::budget::MemoryBudget;
use crate::config::Config;
use crate::gateway::Access;
use crate::metadata::{DataGroup, MetadataState, PartitionKey, Route};
use crate::observability::metrics::{Metrics, Operation};
use crate::ownership::remote::{Query, RemoteMetadataAuthority, Value};
use crate::replication::rpc::{GroupRole, RaftService};
use crate::server::{SubmissionOrder, WireService};
use crate::transport::{Authorization, StreamPurpose};

use super::node::Replica;
use super::wire::{self, PeerRpc, Request};
use super::{Error, Result};

pub(crate) struct CachedRoute {
    pub route: Route,
    pub topic_name: String,
    pub expires: Instant,
}
type CancelKey = (Arc<str>, Arc<str>, Option<Id>, Id);
type LeaseSlot = Arc<Mutex<Option<Arc<crate::ownership::ControlLease>>>>;

pub(crate) struct Service {
    pub config: Config,
    pub cluster: Id,
    pub metadata: Option<Replica<MetadataState>>,
    pub data: BTreeMap<u64, Replica<DataApplication>>,
    pub objects: BTreeMap<u64, crate::lifecycle::Coordinator>,
    pub rpc: PeerRpc,
    pub authority: Arc<RemoteMetadataAuthority>,
    pub(super) auth_cache: super::auth::AuthCache,
    pub(super) consumer_after: std::sync::Mutex<Option<Id>>,
    pub(super) consumer_delete_after: std::sync::Mutex<Option<Id>>,
    pub(super) topic_delete_after: std::sync::Mutex<Option<Id>>,
    pub budget: MemoryBudget,
    pub metrics: Metrics,
    pub(super) producer_metrics: ProducerMetrics,
    pub dispatch_metrics: Arc<crate::server::DispatchMetrics>,
    pub runtime_metrics: super::telemetry::RuntimeMetrics,
    pub ownership_metrics: crate::ownership::OwnershipMetrics,
    pub(super) failover_metrics: super::failover::Metrics,
    pub scaling_diagnostic: std::sync::Mutex<serde_json::Value>,
    pub transport_metrics: Vec<Arc<crate::transport::TransportMetrics>>,
    pub scaling: super::scaling::RuntimeScaling,
    pub(super) transfer_exports: super::authority_transfer::ExportCache,
    pub(super) repair: crate::scaling::RepairBudget,
    pub quota: std::sync::Mutex<crate::quota::Quota>,
    pub quota_observed: Notify,
    pub disk_capacity: std::sync::Mutex<Option<(crate::quota::disk::Snapshot, Instant)>>,
    pub log_health: std::sync::Mutex<Option<crate::observability::logging::LogHealth>>,
    pub trace_health: std::sync::Mutex<Option<crate::observability::tracing::TraceHealth>>,
    pub draining: AtomicBool,
    pub partition_drains: std::sync::Mutex<BTreeMap<PartitionKey, u64>>,
    pub shutdown_requested: watch::Sender<bool>,
    pub routes: RwLock<BTreeMap<PartitionKey, CachedRoute>>,
    pub leaders: RwLock<BTreeMap<u64, (u64, Instant)>>,
    pub groups: RwLock<BTreeMap<u64, (DataGroup, Instant)>>,
    pub leases: std::sync::Mutex<BTreeMap<PartitionKey, LeaseSlot>>,
    pub metadata_mutation: Mutex<()>,
    pub(super) transfer_serial: Mutex<()>,
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
        objects: BTreeMap<u64, crate::lifecycle::Coordinator>,
        rpc: PeerRpc,
        authority: Arc<RemoteMetadataAuthority>,
        budget: MemoryBudget,
        producer: ProducerMetrics,
        mut metrics: Metrics,
        dispatch_metrics: Arc<crate::server::DispatchMetrics>,
        runtime_metrics: super::telemetry::RuntimeMetrics,
        ownership_metrics: crate::ownership::OwnershipMetrics,
        quota: crate::quota::Quota,
        repair: crate::scaling::RepairBudget,
        transport_metrics: Vec<Arc<crate::transport::TransportMetrics>>,
        shutdown_requested: watch::Sender<bool>,
    ) -> Result<Self> {
        let failover_metrics = super::failover::Metrics::register(metrics.registry_mut());
        let scaling = super::scaling::RuntimeScaling::new(
            &config,
            cluster,
            metadata.as_ref(),
            &data,
            rpc.clone(),
            repair.clone(),
            metrics.registry_mut(),
        )?;
        Ok(Self {
            config,
            cluster,
            metadata,
            data,
            objects,
            rpc,
            authority,
            auth_cache: super::auth::AuthCache::default(),
            consumer_after: std::sync::Mutex::new(None),
            consumer_delete_after: std::sync::Mutex::new(None),
            topic_delete_after: std::sync::Mutex::new(None),
            budget,
            producer_metrics: producer,
            metrics,
            dispatch_metrics,
            runtime_metrics,
            ownership_metrics,
            failover_metrics,
            scaling_diagnostic: std::sync::Mutex::new(serde_json::json!({"status":"starting"})),
            transport_metrics,
            scaling,
            transfer_exports: super::authority_transfer::ExportCache::default(),
            repair,
            quota: std::sync::Mutex::new(quota),
            quota_observed: Notify::new(),
            disk_capacity: std::sync::Mutex::new(None),
            log_health: std::sync::Mutex::new(None),
            trace_health: std::sync::Mutex::new(None),
            draining: AtomicBool::new(false),
            partition_drains: std::sync::Mutex::new(BTreeMap::new()),
            shutdown_requested,
            routes: RwLock::new(BTreeMap::new()),
            leaders: RwLock::new(BTreeMap::new()),
            groups: RwLock::new(BTreeMap::new()),
            leases: std::sync::Mutex::new(BTreeMap::new()),
            metadata_mutation: Mutex::new(()),
            transfer_serial: Mutex::new(()),
            cancellations: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        })
    }

    pub async fn close(&self, deadline: Instant) -> Result<()> {
        let mut failure = None;
        for replica in self.data.values() {
            if let Err(error) = Self::close_replica(replica, deadline).await {
                failure.get_or_insert(error);
            }
        }
        if let Some(replica) = &self.metadata
            && let Err(error) = Self::close_replica(replica, deadline).await
        {
            failure.get_or_insert(error);
        }
        if let Err(source) = self.rpc.pool.close(deadline).await {
            failure.get_or_insert_with(|| Error::operation("drain internal QUIC endpoint", source));
        }
        failure.map_or(Ok(()), Err)
    }

    async fn close_replica<A: crate::replication::Application>(
        replica: &Replica<A>,
        deadline: Instant,
    ) -> Result<()> {
        wire::within(deadline, replica.raft.shutdown())
            .await?
            .map_err(|source| Error::operation("stop Raft replica before WAL closure", source))?;
        wire::within(deadline, replica.log.close())
            .await?
            .map_err(|source| Error::operation("close drained Raft WAL", source))
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

    pub(crate) async fn authoritative_group(
        &self,
        group: u64,
        deadline: Instant,
    ) -> Result<DataGroup> {
        let actual = self
            .data
            .get(&group)
            .map(|replica| crate::ownership::remote::current_voters(&replica.raft))
            .transpose()?;
        if let Some((cached, _)) =
            self.groups
                .read()
                .await
                .get(&group)
                .filter(|(cached, expires)| {
                    *expires > Instant::now()
                        && actual
                            .as_ref()
                            .is_none_or(|voters| *voters == cached.voters)
                })
        {
            return Ok(cached.clone());
        }
        let started = Instant::now();
        let Value::Group(configured) = self.authority.query(Query::Group(group), deadline).await?
        else {
            return Err(Error::unavailable());
        };
        if configured.group_id != group
            || configured.generation == 0
            || configured.voters.len() != usize::from(self.config.replication_factor)
            || configured.voters.iter().any(|node| {
                self.config
                    .runtime
                    .peer(*node)
                    .map_or(true, |peer| !peer.role.stores_data())
            })
            || started.elapsed() >= Duration::from_millis(500)
        {
            return Err(Error::unavailable());
        }
        if let Some(replica) = self.data.get(&group)
            && crate::ownership::remote::current_voters(&replica.raft)? != configured.voters
        {
            return Err(Error::unavailable());
        }
        self.groups.write().await.insert(
            group,
            (configured.clone(), started + Duration::from_millis(500)),
        );
        Ok(configured)
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
            self.config.runtime.metadata_voters.clone()
        } else {
            let config = self.config.runtime.group(group)?;
            config
                .voters
                .iter()
                .chain(&config.standby)
                .copied()
                .collect()
        };
        for node in &voters {
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
        let operation = match frame_kind(frame)? {
            FrameKind::GroupRead => Operation::Read,
            FrameKind::GroupAck => Operation::Ack,
            FrameKind::Publish => Operation::Produce,
            _ => Operation::Control,
        };
        let mut timer = super::telemetry::OperationTimer::new(self.metrics.operation(operation));
        let result = self
            .public_dispatch(access, frame, deadline, order, hops)
            .await;
        timer.finish(&result, &self.config.protocol);
        result
    }

    async fn public_dispatch(
        &self,
        access: Access,
        frame: &Frame,
        deadline: Instant,
        order: Option<SubmissionOrder>,
        hops: u32,
    ) -> Result<Frame> {
        if self.draining.load(Ordering::Acquire)
            && !super::drain::allows_completion(frame, &self.config.protocol)?
        {
            return Err(Error::unavailable());
        }
        if !crate::gateway::external_kind(frame_kind(frame)?) {
            return Err(forbidden());
        }
        let kind = frame_kind(frame)?;
        if matches!(
            kind,
            FrameKind::CreateLogicalTopic
                | FrameKind::GetTopicLayout
                | FrameKind::ChangeTopicLayout
                | FrameKind::ManageIdentity
                | FrameKind::ConsumerControl
                | FrameKind::SubscriptionAdmin
                | FrameKind::RemoveTopic
                | FrameKind::PublisherControl
                | FrameKind::GetTopic
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
        self.admit_partition(partition, route.epoch, kind)?;
        if route.owner_node != self.config.node_id && hops < 2 {
            return self
                .forward(
                    route.owner_node,
                    route.group_id,
                    access,
                    frame,
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
                .forward(leader, route.group_id, access, frame, deadline, order, 2)
                .await;
        }
        let group = route.group_id;
        let result = if matches!(kind, FrameKind::Publish | FrameKind::RetirePublishes) {
            drop(order);
            self.publishing_public(&access, frame, route, deadline)
                .await
        } else if matches!(
            kind,
            FrameKind::GroupRead
                | FrameKind::GroupAck
                | FrameKind::GroupRetireRead
                | FrameKind::GroupRetireAck
        ) {
            drop(order);
            self.group_data_public(&access, frame, route, deadline)
                .await
        } else {
            drop(order);
            Err(forbidden())
        };
        if let Err(error) = &result {
            self.invalidate_forward(frame, group, error.failure().code)
                .await;
        }
        result
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
            principal: access.principal.to_string(),
            scope: access.scope.to_string(),
            hops,
            client_session: access
                .session
                .map(super::metadata::bytes_id)
                .unwrap_or_default(),
        };
        let pending = self
            .rpc
            .submit(
                target,
                group,
                class,
                wire::id(&frame.operation_id)?,
                request,
                deadline,
                0,
            )
            .await?;
        drop(order);
        let bytes = match pending.receive().await {
            Ok(bytes) => bytes,
            Err(error) => {
                self.invalidate_forward(frame, group, error.failure().code)
                    .await;
                return Err(error);
            }
        };
        let response = decode_frame(bytes, class, &self.config.protocol)
            .map_err(|source| Error::operation("validate forwarded response", source))?;
        if response.request_id != frame.request_id || response.operation_id != frame.operation_id {
            return Err(Error::invalid(
                "Forwarded response does not match the original request",
            ));
        }
        if response.kind == i32::from(FrameKind::Error) {
            let error: ErrorReply = decode_payload(response.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode delegated rejection", source))?;
            self.invalidate_forward(frame, group, error.code).await;
        }
        Ok(response)
    }

    async fn invalidate_forward(&self, frame: &Frame, group: u64, code: i32) {
        if matches!(
            ErrorCode::try_from(code),
            Ok(ErrorCode::Fenced | ErrorCode::Unavailable)
        ) {
            if let Ok(partition) = super::data::partition(frame, &self.config.protocol) {
                self.routes.write().await.remove(&partition);
            }
            self.leaders.write().await.remove(&group);
        }
    }

    fn internal_execution_purpose(&self, frame: &Frame) -> Option<StreamPurpose> {
        let envelope: InternalCommand =
            decode_payload(frame.payload.clone(), &self.config.protocol).ok()?;
        let request: Request =
            decode_payload(envelope.encoded_command, &self.config.protocol).ok()?;
        match request.kind {
            wire::AUTHORITY
            | wire::OWNERSHIP
            | wire::LEADER
            | super::auth::AUTH_QUERY
            | crate::lifecycle::remote::LIFECYCLE_QUERY
            | crate::lifecycle::remote::LIFECYCLE_COMMAND => Some(StreamPurpose::Core),
            kind if super::tenant_quota::is_core(kind, &request.body) => Some(StreamPurpose::Core),
            wire::FORWARD => {
                let class = TrafficClass::try_from(frame.traffic_class).ok()?;
                let inner = decode_frame(request.body, class, &self.config.protocol).ok()?;
                Some(business_execution_purpose(&inner, &self.config.protocol))
            }
            _ => Some(StreamPurpose::Request),
        }
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
            super::authority_transfer::AUTHORITY_TRANSFER => {
                drop(order);
                self.authority_transfer_command(
                    peer,
                    group,
                    wire::id(&frame.operation_id)?,
                    frame.traffic_class,
                    &request.body,
                    deadline,
                )
                .await?
            }
            super::consumer_data::GROUP_CONTROL => {
                drop(order);
                if !matches!(peer.role.as_str(), "metadata" | "combined")
                    || frame.traffic_class != i32::from(TrafficClass::Control)
                {
                    return Err(forbidden());
                }
                self.group_control_command(group, &request.body, deadline)
                    .await?
            }
            super::tenant_quota::CONTROL => {
                drop(order);
                if group != self.config.runtime.metadata_group
                    || frame.traffic_class != i32::from(TrafficClass::Control)
                    || !matches!(peer.role.as_str(), "metadata" | "storage" | "combined")
                {
                    return Err(forbidden());
                }
                self.quota_control_command(&request.body, deadline).await?
            }
            super::tenant_quota::DATA => {
                drop(order);
                if frame.traffic_class != i32::from(TrafficClass::Control)
                    || !matches!(peer.role.as_str(), "metadata" | "storage" | "combined")
                {
                    return Err(forbidden());
                }
                self.quota_data_command(group, &request.body, deadline)
                    .await?
            }
            super::auth::AUTH_QUERY => {
                drop(order);
                if group != self.config.runtime.metadata_group
                    || frame.traffic_class != i32::from(TrafficClass::Control)
                {
                    return Err(forbidden());
                }
                self.auth_command(&request.body, deadline).await?
            }
            super::topology::SEAL_SHARD => {
                drop(order);
                if !matches!(peer.role.as_str(), "metadata" | "combined")
                    || frame.traffic_class != i32::from(TrafficClass::Control)
                {
                    return Err(forbidden());
                }
                self.seal_shard_command(group, &request.body, deadline)
                    .await?
            }
            super::drain::KIND => {
                drop(order);
                if frame.traffic_class != i32::from(TrafficClass::Control) {
                    return Err(forbidden());
                }
                self.drain_command(peer, group, &request.body, deadline)
                    .await?
            }
            super::scaling::KIND => {
                drop(order);
                let query = crate::scaling::remote::decode_query(&request.body)
                    .map_err(|source| Error::operation("decode closed scaling request", source))?;
                if query.group_id(self.config.runtime.metadata_group) != group
                    || frame.traffic_class != i32::from(query.traffic_class())
                {
                    return Err(forbidden());
                }
                let reply = self
                    .scaling
                    .service
                    .handle(peer, query, deadline)
                    .await
                    .map_err(|source| {
                        Error::operation("execute authenticated scaling request", source)
                    })?;
                Bytes::from(
                    crate::scaling::remote::encode_reply(&reply).map_err(|source| {
                        Error::operation("encode closed scaling response", source)
                    })?,
                )
            }
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
            crate::lifecycle::remote::LIFECYCLE_QUERY
            | crate::lifecycle::remote::LIFECYCLE_COMMAND => {
                drop(order);
                if group != self.config.runtime.metadata_group {
                    return Err(forbidden());
                }
                let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
                self.require_leader(replica)?;
                let class = TrafficClass::try_from(frame.traffic_class)
                    .map_err(|_| Error::invalid("Lifecycle traffic class is invalid"))?;
                crate::lifecycle::remote::MetadataHandler::new(
                    replica.raft.clone(),
                    replica.state.clone(),
                )
                .handle(
                    &Authorization::Internal(peer.clone()),
                    request.kind,
                    class,
                    &request.body,
                    deadline.into_std(),
                )
                .await
                .map_err(|source| {
                    Error::operation("perform authenticated lifecycle metadata operation", source)
                })?
            }
            crate::lifecycle::remote::LIFECYCLE_CHECKPOINT => {
                drop(order);
                let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
                let class = TrafficClass::try_from(frame.traffic_class)
                    .map_err(|_| Error::invalid("Checkpoint traffic class is invalid"))?;
                let data = crate::lifecycle::DataReplica::local(
                    replica.raft.clone(),
                    replica.state.clone(),
                );
                crate::lifecycle::remote::SnapshotHandler::new(data)
                    .handle(
                        &Authorization::Internal(peer.clone()),
                        class,
                        &request.body,
                        deadline.into_std(),
                    )
                    .await
                    .map_err(|source| {
                        Error::operation("perform authenticated replica checkpoint", source)
                    })?
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
                let credential: Id = request
                    .principal
                    .strip_prefix("credential:")
                    .ok_or_else(forbidden)?
                    .parse()
                    .map_err(|_| forbidden())?;
                let (access, expires) = self
                    .dynamic_access(
                        credential,
                        wire::id(&request.client_session)?,
                        &inner,
                        deadline,
                    )
                    .await?;
                let deadline = super::auth::admit_deadline(deadline, expires)?;
                if request.hops == 2 {
                    let partition = super::data::partition(&inner, &self.config.protocol)?;
                    let mut route = self
                        .authorize_partition(&access, partition, deadline)
                        .await?;
                    if route.owner_node != peer.node_id {
                        self.routes.write().await.remove(&partition);
                        route = self
                            .authorize_partition(&access, partition, deadline)
                            .await?;
                    }
                    if route.group_id != group
                        || !matches!(peer.role.as_str(), "broker" | "combined")
                    {
                        return Err(forbidden());
                    }
                    if route.owner_node != peer.node_id {
                        return Err(Error::reject(ErrorCode::Fenced, Outcome::Rejected, true));
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
    fn execution_purpose(
        &self,
        peer: &Authorization,
        frame: &Frame,
        transport_purpose: StreamPurpose,
    ) -> StreamPurpose {
        if !matches!(peer, Authorization::Internal(_)) {
            return business_execution_purpose(frame, &self.config.protocol);
        }
        if frame.kind != i32::from(FrameKind::InternalCommand) {
            return transport_purpose;
        }
        self.internal_execution_purpose(frame)
            .unwrap_or(transport_purpose)
    }

    fn response_requires_flush(&self, peer: &Authorization, request: &Frame) -> bool {
        self.shutdown_response(peer, request)
    }

    fn response_sent(&self, peer: &Authorization, request: &Frame, response: &Frame) {
        if response.kind == i32::from(FrameKind::InternalResult)
            && response.request_id == request.request_id
            && response.operation_id == request.operation_id
            && self.draining.load(Ordering::Acquire)
            && self.shutdown_response(peer, request)
        {
            let _ = self.shutdown_requested.send(true);
        }
    }

    type Error = Error;
    async fn handle(
        &self,
        peer: Authorization,
        frame: &Frame,
        deadline: Instant,
        order: Option<SubmissionOrder>,
    ) -> Result<Option<Frame>> {
        let span = crate::observability::tracing::request_span(
            frame,
            self.config.node_id,
            self.config.role.name(),
        );
        crate::observability::tracing::observe_until(async {
        let started = tracing::enabled!(tracing::Level::TRACE).then(std::time::Instant::now);
        let result = async {
        let result = match &peer {
            Authorization::External { .. } | Authorization::Credential { .. } => {
                let kind = frame_kind(frame)?;
                let Authorization::Credential { credential, session_id, .. } = &peer else { return Err(forbidden()); };
                let (access, expires) = self.dynamic_access(*credential, *session_id, frame, deadline).await?;
                let deadline = super::auth::admit_deadline(deadline, expires)?;
                if kind == FrameKind::Cancel {
                    let cancel: CancelRequest =
                        decode_payload(frame.payload.clone(), &self.config.protocol)
                            .map_err(|source| Error::operation("decode cancellation", source))?;
                    let key = (
                        access.principal,
                        access.scope,
                        access.session,
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
                    access.session,
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
        result.map(Some)
        }.await;
        if let Some(started) = started {
            tracing::trace!(
                event_code = "runtime_request_completed",
                elapsed_us = started.elapsed().as_micros(),
                succeeded = result.is_ok(),
                "Authenticated runtime request completed"
            );
        }
        match result {
            Ok(frame) => Ok(frame),
            Err(error) => {
                let response = wire::error_response(frame, &error, &self.config.protocol).map(Some);
                super::telemetry::log_unknown_error(frame, error, &self.dispatch_metrics);
                response
            }
        }
        }, span, deadline.into_std()).await
    }
}

fn business_execution_purpose(
    frame: &Frame,
    limits: &tidemq_protocol::ProtocolLimits,
) -> StreamPurpose {
    if FrameKind::try_from(frame.kind).is_ok_and(tidemq_protocol::framing::is_completion) {
        StreamPurpose::Ack
    } else if tidemq_protocol::framing::is_core_control(frame, limits).unwrap_or(false) {
        StreamPurpose::Core
    } else {
        StreamPurpose::Request
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
