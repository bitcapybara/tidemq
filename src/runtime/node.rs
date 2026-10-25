use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use tidemq_protocol::Id;
use tidemq_protocol::common::TrafficClass;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::broker::{DataApplication, DataGroupConfig, ProducerMetrics};
use crate::budget::MemoryBudget;
use crate::config::Config;
use crate::metadata::{MetadataState, NodeRole};
use crate::observability::metrics::{Metrics, Operation};
use crate::ownership::ControlPolicy;
use crate::ownership::remote::RemoteMetadataAuthority;
use crate::producer::ProducerLimits;
use crate::replication::rpc::{NetworkConfig, PeerPool, QuicNetworkFactory};
use crate::replication::{
    Application, DiskLog, DiskStateMachine, DiskStore, Raft, StorageIdentity, StorageOptions,
};
use crate::server::{DispatchConfig, DispatchMetrics};
use crate::transport::{
    Client, ClientCredentials, ClientTls, NodeGrant, Server, ServerAuthorization, ServerTls,
    TlsIdentity, TransportConfig,
};

use super::service::Service;
use super::wire::PeerRpc;
use super::{Error, GroupConfig, Result};

pub(crate) struct Replica<A: Application> {
    pub raft: Raft<A>,
    pub state: DiskStateMachine<A>,
    pub log: DiskLog<A>,
    pub voters: BTreeSet<u64>,
}

pub struct RunningNode {
    pub external_address: Option<SocketAddr>,
    pub internal_address: SocketAddr,
    pub management_address: SocketAddr,
    pub(crate) service: Arc<Service>,
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<Result<()>>>,
}

impl RunningNode {
    pub async fn start(config: Config) -> Result<Self> {
        config.runtime.validate(&config)?;
        let cluster = config
            .cluster_id
            .parse::<Id>()
            .map_err(|source| Error::operation("validate cluster identity", source))?;
        tokio::fs::create_dir_all(&config.data_dir)
            .await
            .map_err(|source| Error::operation("create process data directory", source))?;
        let retirement = super::node_retirement::load(&config, cluster).await?;
        let budget = MemoryBudget::new(&config.memory)
            .map_err(|source| Error::operation("create process memory budget", source))?;
        let identity = TlsIdentity::new(
            super::read_bounded(&config.tls.certificate, 65_536).await?,
            super::read_secret(&config.tls.private_key, 16_384).await?,
        )
        .map_err(|source| Error::operation("load process TLS identity", source))?;
        let roots = super::read_bounded(&config.tls.trust_roots, 65_536).await?;
        let grants = grants(&config, cluster).await?;
        let registry = Arc::new(super::registry::Registry::load(&config, cluster, grants).await?);
        let grants = registry.trust.snapshot().to_vec();
        let auth_service = Arc::new(std::sync::OnceLock::new());
        let transport = transport_config(&config);
        let bind_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let internal = bind_server(
            config.runtime.internal_listen,
            ServerTls {
                identity: identity.clone(),
                client_trust_roots: Some(roots.clone()),
            },
            ServerAuthorization::LiveInternal(registry.trust.clone()),
            transport.clone(),
            budget.clone(),
            bind_deadline,
        )
        .await?;
        let external = if config.role.serves_gateway() && retirement.is_none() {
            Some(
                bind_server(
                    config.listen,
                    ServerTls {
                        identity: identity.clone(),
                        client_trust_roots: None,
                    },
                    ServerAuthorization::Dynamic(Arc::new(super::auth::Authenticator(
                        auth_service.clone(),
                    ))),
                    transport.clone(),
                    budget.clone(),
                    bind_deadline,
                )
                .await?,
            )
        } else {
            None
        };
        let client = Arc::new(
            Client::bind(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
                ClientTls {
                    trust_roots: roots.clone(),
                    identity: Some(identity.clone()),
                    expected_nodes: grants.clone(),
                },
                transport.clone(),
                budget.clone(),
            )
            .map_err(|source| Error::operation("bind internal QUIC client", source))?,
        );
        let mut transport_metrics = vec![client.metrics.clone()];
        let repair = crate::scaling::RepairBudget::new(crate::scaling::RepairLimits::default())
            .map_err(|source| Error::operation("initialize node repair budget", source))?;
        let mut metrics = Metrics::default();
        let dispatch = Arc::new(DispatchMetrics::default());
        dispatch.register(metrics.registry_mut());
        let runtime_metrics = super::telemetry::RuntimeMetrics::default();
        metrics
            .registry_mut()
            .sub_registry_with_prefix("tidemq_runtime")
            .register_collector(Box::new(runtime_metrics.clone()));
        metrics
            .registry_mut()
            .sub_registry_with_prefix("tidemq_internal_client")
            .register_collector(Box::new(client.metrics.clone()));
        let producer = ProducerMetrics::default();
        metrics
            .registry_mut()
            .sub_registry_with_prefix("tidemq_producer")
            .register_collector(Box::new(producer.clone()));
        let ownership = crate::ownership::OwnershipMetrics::register(metrics.registry_mut());
        let quota_metrics = crate::quota::Metrics::default();
        quota_metrics.register(metrics.registry_mut());
        let quota = crate::quota::Quota::new(config.runtime.capacity, quota_metrics)
            .map_err(|source| Error::operation("initialize capacity admission", source))?;
        let targets = registry.targets(&config, cluster)?;
        let pool = Arc::new(
            PeerPool::new(
                client,
                ClientCredentials::Internal {
                    session_id: random_id()?,
                    cluster_id: cluster,
                    node_id: config.node_id,
                    role: config.role.name().into(),
                },
                targets,
                NetworkConfig {
                    protocol: config.protocol.clone(),
                    ..NetworkConfig::default()
                },
                budget.clone(),
                metrics.operation(Operation::Control),
            )
            .map_err(|source| Error::operation("create shared internal peer pool", source))?
            .with_repair_budget(repair.clone()),
        );
        let rpc = PeerRpc {
            pool: pool.clone(),
            limits: config.protocol.clone(),
        };
        let authority = Arc::new(RemoteMetadataAuthority::new(
            rpc.clone(),
            cluster,
            config.runtime.metadata_group,
            config.runtime.metadata_voters.iter().copied().collect(),
            ControlPolicy::default(),
        )?);
        let metadata = if config.role.stores_metadata() {
            Some(
                open_replica::<MetadataState>(
                    &config,
                    config.runtime.metadata_group,
                    &config.runtime.metadata_voters,
                    cluster,
                    pool.clone(),
                    metrics.operation(Operation::Control),
                )
                .await?,
            )
        } else {
            None
        };
        let mut data = BTreeMap::new();
        for group in &config.runtime.groups {
            if group.hosts(config.node_id) {
                data.insert(
                    group.group_id,
                    open_replica::<DataApplication>(
                        &config,
                        group.group_id,
                        &group.voters,
                        cluster,
                        pool.clone(),
                        metrics.operation(Operation::Produce),
                    )
                    .await?,
                );
            }
        }
        let objects = super::objects::coordinators(
            &config,
            cluster,
            &data,
            rpc.clone(),
            authority.clone(),
            budget.clone(),
            metrics.registry_mut(),
        )
        .await?;
        let external_address = external.as_ref().map(Server::local_addr);
        let internal_address = internal.local_addr();
        transport_metrics.push(internal.metrics.clone());
        if let Some(external) = &external {
            transport_metrics.push(external.metrics.clone());
        }
        metrics
            .registry_mut()
            .sub_registry_with_prefix("tidemq_internal_server")
            .register_collector(Box::new(internal.metrics.clone()));
        if let Some(external) = &external {
            metrics
                .registry_mut()
                .sub_registry_with_prefix("tidemq_external_server")
                .register_collector(Box::new(external.metrics.clone()));
        }
        let (shutdown, stopped) = watch::channel(false);
        let (external_stop, _) = watch::channel(false);
        let has_external = external.is_some();
        let service = Arc::new(Service::new(
            config,
            cluster,
            metadata,
            data,
            objects,
            rpc,
            authority,
            registry,
            budget.clone(),
            producer,
            metrics,
            dispatch.clone(),
            runtime_metrics,
            ownership,
            quota,
            repair,
            transport_metrics,
            shutdown.clone(),
            retirement,
            external_stop,
            has_external,
        )?);
        let dispatcher = DispatchConfig {
            // The external listener can authenticate only after its complete service is installed.
            protocol: service.config.protocol.clone(),
            ..DispatchConfig::default()
        };
        auth_service
            .set(Arc::downgrade(&service))
            .map_err(|_| Error::invalid("Authentication service was installed twice"))?;
        let mut tasks = Vec::new();
        let internal_service = service.clone();
        let internal_config = dispatcher.clone();
        let internal_budget = budget.clone();
        let internal_dispatch = dispatch.clone();
        let internal_stopped = stopped.clone();
        tasks.push(tokio::spawn(async move {
            crate::server::run(
                internal,
                internal_service,
                internal_config,
                internal_budget,
                internal_dispatch,
                internal_stopped,
            )
            .await
            .map_err(Error::foundation)
        }));
        if let Some(external) = external {
            let external_service = service.clone();
            let external_stopped = service.external_stop.subscribe();
            tasks.push(tokio::spawn(async move {
                let result = crate::server::run(
                    external,
                    external_service.clone(),
                    dispatcher,
                    budget,
                    dispatch,
                    external_stopped,
                )
                .await
                .map_err(Error::foundation);
                external_service.external_drained.store(
                    if result.is_ok() { 1 } else { 2 },
                    std::sync::atomic::Ordering::Release,
                );
                result
            }));
        }
        let (management_address, task) =
            super::management::start(service.clone(), stopped.clone()).await?;
        tasks.push(task);
        tasks.push(tokio::spawn(service.clone().rebalance(stopped.clone())));
        tasks.push(tokio::spawn(
            service.clone().follow_node_drains(stopped.clone()),
        ));
        tasks.push(tokio::spawn(
            service.clone().follow_registry(stopped.clone()),
        ));
        tasks.push(tokio::spawn(
            service.clone().follow_metadata_membership(stopped.clone()),
        ));
        tasks.push(tokio::spawn(
            service.clone().resume_authority_transfers(stopped.clone()),
        ));
        let maintenance = service.clone();
        tasks.push(tokio::spawn(maintenance.maintain(stopped.clone())));
        tasks.push(tokio::spawn(service.clone().expire_pins(stopped.clone())));
        let bootstrap = service.clone();
        tasks.push(tokio::spawn(
            async move { bootstrap.reconcile(stopped).await },
        ));
        Ok(Self {
            external_address,
            internal_address,
            management_address,
            service,
            shutdown,
            tasks,
        })
    }

    pub fn attach_logging(&self, health: crate::observability::logging::LogHealth) -> Result<()> {
        *self
            .service
            .log_health
            .lock()
            .map_err(|_| Error::invalid("Runtime logging health is unavailable"))? = Some(health);
        Ok(())
    }

    pub fn attach_tracing(&self, health: crate::observability::tracing::TraceHealth) -> Result<()> {
        *self
            .service
            .trace_health
            .lock()
            .map_err(|_| Error::invalid("Runtime trace health is unavailable"))? = Some(health);
        Ok(())
    }

    /// Trusted local control entry point; public clients cannot submit ownership commands.
    pub async fn route(
        &self,
        partition: crate::metadata::PartitionKey,
        deadline: tokio::time::Instant,
    ) -> Result<crate::metadata::Route> {
        self.service.route(partition, deadline).await
    }

    /// Trusted metadata service command that closes a Gateway through its owned shutdown path.
    pub async fn reconnect_gateway(&self, node: u64, deadline: tokio::time::Instant) -> Result<()> {
        crate::scaling::DrainActions::reconnect_gateway(self.service.as_ref(), node, deadline)
            .await
            .map_err(|source| Error::operation("request gateway reconnect", source))
    }

    /// Trusted local control entry point; public clients cannot submit ownership commands.
    pub async fn takeover(
        &self,
        partition: crate::metadata::PartitionKey,
        target_owner: u64,
        expected_epoch: u64,
        expected_route_generation: u64,
        deadline: tokio::time::Instant,
    ) -> Result<crate::metadata::Route> {
        crate::scaling::DrainPlan::handoff(
            self.service.as_ref(),
            &crate::scaling::Handoff {
                partition,
                target_owner,
                expected_epoch,
                expected_route_generation,
            },
            &self.service.scaling.metrics,
            deadline,
        )
        .await
        .map_err(|source| Error::operation("handoff partition through planned drain", source))
    }

    pub async fn wait_for_shutdown_request(&self) {
        let mut stopped = self.shutdown.subscribe();
        if !*stopped.borrow() {
            let _ = stopped.changed().await;
        }
    }

    /// Fresh quorum observations carry a process identity to fence sampling across restarts.
    pub async fn shard_loads(
        &self,
        group: u64,
        partitions: &[crate::metadata::PartitionKey],
        deadline: tokio::time::Instant,
    ) -> Result<super::ShardLoadPage> {
        if !self.service.config.role.stores_metadata() {
            return Err(Error::invalid(
                "Shard load inspection requires a metadata service identity",
            ));
        }
        self.service
            .query_shard_loads(group, partitions, deadline)
            .await
    }

    /// Trusted metadata-node control entry point; resumes the same persisted replacement plan.
    pub async fn replace_replica(
        &self,
        group: u64,
        removed: u64,
        added: u64,
        deadline: tokio::time::Instant,
    ) -> Result<crate::scaling::MembershipPlan> {
        if !self.service.config.role.stores_metadata() {
            return Err(Error::invalid(
                "Replica replacement requires a metadata service identity",
            ));
        }
        let coordinator = crate::scaling::remote::Coordinator::new(
            self.service.scaling.rpc.clone(),
            self.service.config.runtime.metadata_group,
        )
        .map_err(|source| Error::operation("create replica replacement coordinator", source))?;
        let plan = coordinator
            .plan(self.service.cluster, group, removed, added, deadline)
            .await
            .map_err(|source| Error::operation("plan replica replacement", source))?;
        coordinator
            .execute(&plan, deadline)
            .await
            .map_err(|source| Error::operation("execute replica replacement", source))?;
        Ok(plan)
    }

    /// Trusted metadata-node control entry point; retries retain the original operation identity.
    pub async fn transfer_shard(
        &self,
        operation: Id,
        shard: Id,
        target_group: u64,
        deadline: tokio::time::Instant,
    ) -> Result<crate::metadata::AuthorityTransfer> {
        self.service
            .start_authority_transfer(operation, shard, target_group, deadline)
            .await
    }

    /// Quorum-read progress for the original trusted authority transfer operation.
    pub async fn authority_transfer_status(
        &self,
        operation: Id,
        deadline: tokio::time::Instant,
    ) -> Result<Option<crate::metadata::AuthorityTransfer>> {
        self.service
            .authority_transfer_status(operation, deadline)
            .await
    }

    /// Delete one fixed subscription identity; unfinished domain removals resume after restart.
    pub async fn delete_consumer_subscription(
        &self,
        operation: Id,
        subscription: Id,
        model: tidemq_protocol::topic::SubscriptionKind,
        deadline: tokio::time::Instant,
    ) -> Result<crate::metadata::ConsumerDeletion> {
        self.service
            .start_consumer_delete(operation, subscription, model, deadline)
            .await
    }

    /// Archive committed ranges through the authenticated lifecycle service and durable manifest.
    pub async fn archive(
        &self,
        group: u64,
        ranges: Vec<crate::lifecycle::RangeRequest>,
        deadline: tokio::time::Instant,
    ) -> Result<Id> {
        let coordinator = self
            .service
            .objects
            .get(&group)
            .ok_or_else(Error::unavailable)?;
        let prepared = coordinator
            .prepare(ranges, Vec::new(), deadline.into_std())
            .await
            .map_err(|source| Error::operation("prepare committed archive ranges", source))?;
        let id = prepared.object_id();
        coordinator
            .register(&prepared, deadline.into_std())
            .await
            .map_err(|source| Error::operation("register archive task", source))?;
        coordinator
            .authorize(id, deadline.into_std())
            .await
            .map_err(|source| Error::operation("authorize archive task", source))?;
        coordinator
            .upload(&prepared, deadline.into_std())
            .await
            .map_err(|source| Error::operation("upload archive object", source))?;
        coordinator
            .publish(id, deadline.into_std())
            .await
            .map_err(|source| Error::operation("publish archive manifest", source))?;
        coordinator
            .checkpoint(id, deadline.into_std())
            .await
            .map_err(|source| Error::operation("checkpoint published archive", source))?;
        Ok(id)
    }

    /// Reclaim local bodies only after the lifecycle service verifies published recovery evidence.
    pub async fn reclaim_local(
        &self,
        partition: crate::metadata::PartitionKey,
        end: u64,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        let route = self.service.route(partition, deadline).await?;
        self.service
            .objects
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?
            .reclaim(partition, end, deadline.into_std())
            .await
            .map_err(|source| Error::operation("reclaim published local bodies", source))
    }

    pub async fn shutdown(self) -> Result<()> {
        self.shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
    }

    /// Share the service entry point's original total deadline with optional exporters.
    pub async fn shutdown_until(mut self, deadline: tokio::time::Instant) -> Result<()> {
        self.service
            .draining
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.service.external_stop.send(true);
        let _ = self.shutdown.send(true);
        let mut failure = None;
        for mut task in std::mem::take(&mut self.tasks) {
            match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(Ok(result)) => {
                    if let Err(error) = result {
                        failure = Some(error);
                    }
                }
                Ok(Err(source)) => failure = Some(Error::operation("join runtime worker", source)),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    failure = Some(Error::invalid("Runtime worker exceeded its drain deadline"));
                }
            }
        }
        tokio::time::timeout_at(deadline, self.service.close(deadline))
            .await
            .map_err(|_| Error::invalid("Runtime exceeded its total shutdown deadline"))??;
        failure.map_or(Ok(()), Err)
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        let _ = self.service.external_stop.send(true);
        let _ = self.shutdown.send(true);
    }
}

async fn bind_server(
    address: SocketAddr,
    tls: ServerTls,
    authorization: ServerAuthorization,
    config: TransportConfig,
    budget: MemoryBudget,
    deadline: tokio::time::Instant,
) -> Result<Server> {
    let mut backoff = Duration::from_millis(2);
    loop {
        let tls = ServerTls {
            identity: tls.identity.clone(),
            client_trust_roots: tls.client_trust_roots.clone(),
        };
        match Server::bind(
            address,
            tls,
            authorization.clone(),
            config.clone(),
            budget.clone(),
        ) {
            Ok(server) => return Ok(server),
            Err(source) => {
                let occupied = tokio::net::UdpSocket::bind(address)
                    .await
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::AddrInUse);
                if !occupied || tokio::time::Instant::now() >= deadline {
                    return Err(Error::operation("bind authenticated QUIC listener", source));
                }
                tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + backoff)).await;
                backoff = backoff.saturating_mul(2).min(Duration::from_millis(50));
            }
        }
    }
}

async fn open_replica<A: Application>(
    config: &Config,
    group: u64,
    voters: &[u64],
    cluster: Id,
    pool: Arc<PeerPool>,
    stages: Option<Arc<crate::observability::metrics::OperationMetrics>>,
) -> Result<Replica<A>> {
    let store = DiskStore::<A>::open(
        config.data_dir.join(format!("group-{group}")),
        StorageIdentity::new::<A>(cluster, group, config.node_id),
        StorageOptions::default(),
        stages,
    )
    .await
    .map_err(|source| Error::operation("open durable Raft replica", source))?;
    let recovered = store.log.diagnostics().await;
    let pristine = recovered.vote.is_none()
        && recovered.committed_log.is_none()
        && recovered.snapshot_log.is_none()
        && recovered.purged_log.is_none()
        && recovered.retained_log_entries == 0;
    let raft_config = openraft::Config {
        cluster_name: format!("tidemq-{cluster}-{group}"),
        heartbeat_interval: 100,
        election_timeout_min: 400,
        election_timeout_max: 800,
        snapshot_max_chunk_size: 256 * 1024,
        max_in_snapshot_log_to_keep: 1024,
        ..openraft::Config::default()
    }
    .validate()
    .map_err(|source| Error::operation("validate Raft runtime configuration", source))?;
    let network = QuicNetworkFactory::<A>::new(pool, group)
        .map_err(|source| Error::operation("create QUIC Raft network", source))?;
    let raft = Raft::new(
        config.node_id,
        Arc::new(raft_config),
        network,
        store.log.clone(),
        store.state_machine.clone(),
    )
    .await
    .map_err(|source| Error::operation("start Raft replica", source))?;
    if config.runtime.bootstrap && voters.iter().min().copied() == Some(config.node_id) && pristine
    {
        let members: BTreeMap<_, _> = voters
            .iter()
            .map(|id| {
                config
                    .runtime
                    .peer(*id)
                    .map(|peer| (*id, BasicNode::new(peer.internal_address.to_string())))
            })
            .collect::<Result<_>>()?;
        raft.initialize(members)
            .await
            .map_err(|source| Error::operation("bootstrap explicit voter set", source))?;
    }
    Ok(Replica {
        raft,
        state: store.state_machine,
        log: store.log,
        voters: voters.iter().copied().collect(),
    })
}

async fn grants(config: &Config, cluster: Id) -> Result<Vec<NodeGrant>> {
    let mut grants = Vec::new();
    for peer in &config.runtime.peers {
        let der = super::read_bounded(&peer.certificate_der, 32_768).await?;
        let mut groups = vec![config.runtime.metadata_group];
        groups.extend(config.runtime.groups.iter().map(|group| group.group_id));
        grants.push(NodeGrant {
            certificate_pin: NodeGrant::certificate_pin(&der),
            certificate_name: peer.server_name.clone(),
            cluster_id: cluster,
            node_id: peer.node_id,
            role: peer.role.name().into(),
            allowed_groups: groups,
            allowed_classes: classes(peer.role.name()),
        });
    }
    Ok(grants)
}

pub(super) fn classes(role: &str) -> Vec<TrafficClass> {
    let mut classes = vec![
        TrafficClass::Control,
        TrafficClass::Produce,
        TrafficClass::Delivery,
        TrafficClass::Bulk,
    ];
    if matches!(role, "storage" | "metadata" | "combined") {
        classes.push(TrafficClass::Replication);
    }
    classes
}

fn transport_config(config: &Config) -> TransportConfig {
    TransportConfig {
        protocol: config.protocol.clone(),
        max_connections: config.memory.max_connections as usize,
        max_streams: (config.memory.max_streams_per_connection as usize).min(16),
        ..TransportConfig::default()
    }
}

pub(crate) fn data_config(cluster: Id, group: &GroupConfig, config: &Config) -> DataGroupConfig {
    // Public message bytes exclude internal routing context and its bounded encoding overhead.
    let max_message_bytes = u64::from(config.protocol.max_message_bytes)
        .saturating_add(256)
        .min(16 * 1024 * 1024);
    DataGroupConfig {
        cluster_id: cluster,
        group_id: group.group_id,
        max_partitions: 128,
        max_hot_bytes: 32 * 1024 * 1024,
        max_hot_messages: 65_536,
        producer: ProducerLimits {
            max_message_bytes,
            max_batch_bytes: ProducerLimits::default()
                .max_batch_bytes
                .max(max_message_bytes),
            ..ProducerLimits::default()
        },
        subscription: crate::subscription::Limits::default(),
        retention: config.runtime.retention,
    }
}

pub(crate) fn random_id() -> Result<Id> {
    Id::new_v7().map_err(|source| Error::operation("allocate operation identity", source))
}

pub(crate) fn roles(role: crate::config::Role) -> BTreeSet<NodeRole> {
    match role {
        crate::config::Role::Combined => BTreeSet::from([
            NodeRole::Gateway,
            NodeRole::Broker,
            NodeRole::Storage,
            NodeRole::Metadata,
        ]),
        crate::config::Role::Gateway => BTreeSet::from([NodeRole::Gateway]),
        crate::config::Role::Broker => BTreeSet::from([NodeRole::Broker]),
        crate::config::Role::Storage => BTreeSet::from([NodeRole::Storage]),
        crate::config::Role::Metadata => BTreeSet::from([NodeRole::Metadata]),
    }
}
