use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use tidemq_protocol::Id;
use tidemq_protocol::common::TrafficClass;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::broker::{DataApplication, DataGroupConfig};
use crate::budget::MemoryBudget;
use crate::config::Config;
use crate::execution::{
    Application, GroupHandle, GroupState, LocalGroup, LocalOptions, StorageIdentity,
};
use crate::metadata::{MetadataState, NodeRole};
use crate::observability::metrics::Operation;
use crate::ownership::ControlPolicy;
use crate::ownership::remote::Authority;
use crate::producer::ProducerLimits;
use crate::replication::rpc::{NetworkConfig, PeerPool, QuicNetworkFactory};
use crate::replication::{DiskStore, Raft, StorageOptions};
use crate::server::DispatchConfig;
use crate::transport::{
    Client, ClientCredentials, ClientTls, NodeGrant, Server, ServerAuthorization, ServerTls,
    TlsIdentity, TransportConfig,
};

use super::service::Service;
use super::wire::PeerRpc;
use super::{Error, GroupConfig, Result};

#[path = "node_local.rs"]
mod local;

pub(crate) struct Replica<A: Application> {
    pub group: GroupHandle<A>,
    pub state: GroupState<A>,
    pub voters: BTreeSet<u64>,
}

impl<A: Application> Replica<A> {
    pub(crate) fn authority_node(&self) -> Option<u64> {
        match &self.group {
            GroupHandle::Local(group) => {
                let health = group.health();
                matches!(
                    *health.borrow(),
                    crate::execution::LocalHealth::Ready | crate::execution::LocalHealth::Draining
                )
                .then_some(group.identity().node_id)
            }
            GroupHandle::Raft { raft, .. } => super::wire::native_leader(raft),
        }
    }

    pub(crate) fn current_placement(&self) -> Result<crate::metadata::GroupPlacement> {
        match &self.group {
            GroupHandle::Local(group) => Ok(crate::metadata::GroupPlacement::Local {
                node_id: group.identity().node_id,
            }),
            GroupHandle::Raft { .. } => Ok(crate::metadata::GroupPlacement::Raft {
                voters: crate::ownership::remote::current_voters(&self.group)?,
            }),
        }
    }

    pub(crate) async fn storage_bound(&self) -> Result<(u64, bool)> {
        match &self.group {
            GroupHandle::Local(group) => Ok((
                group
                    .file_limit_bytes()
                    .map_err(super::wire::execution_error)?,
                group.statistics().snapshot_sequence > 0,
            )),
            GroupHandle::Raft { log, .. } => {
                let diagnostics = log.diagnostics().await;
                Ok((
                    diagnostics.replica_file_limit_bytes,
                    diagnostics.snapshot_log.is_some(),
                ))
            }
        }
    }
}

pub struct RunningNode {
    pub external_address: Option<SocketAddr>,
    pub internal_address: Option<SocketAddr>,
    pub console_address: Option<SocketAddr>,
    pub management_address: SocketAddr,
    pub(crate) service: Arc<Service>,
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<Result<()>>>,
    console: Option<crate::console::RunningConsole>,
    local_rpc: Option<JoinHandle<Result<()>>>,
}

impl RunningNode {
    pub async fn start(config: Config) -> Result<Self> {
        config.runtime.validate(&config)?;
        if config.role == crate::config::Role::Combined {
            return Self::start_combined(config).await;
        }
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
        let super::facilities::Facilities {
            mut metrics,
            dispatch,
            runtime: runtime_metrics,
            producer,
            ownership,
            quota,
            repair,
        } = super::facilities::Facilities::new(&config)?;
        metrics
            .registry_mut()
            .sub_registry_with_prefix("tidemq_internal_client")
            .register_collector(Box::new(client.metrics.clone()));
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
        let rpc = PeerRpc::remote(pool.clone(), config.protocol.clone());
        let authority = Arc::new(Authority::new(
            rpc.clone(),
            cluster,
            config.runtime.metadata_group,
            crate::metadata::GroupPlacement::Raft {
                voters: config.runtime.metadata_voters.iter().copied().collect(),
            },
            ControlPolicy::default(),
        )?);
        let metadata = if config.role.stores_metadata() {
            Some(
                open_replica::<MetadataState>(
                    &config,
                    config.runtime.metadata_group,
                    &config.runtime.metadata_voters,
                    cluster,
                    Some(pool.clone()),
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
                        Some(pool.clone()),
                        metrics.operation(Operation::Produce),
                    )
                    .await?,
                );
            }
        }
        let objects = super::objects::coordinators(
            &config,
            cluster,
            metadata.as_ref(),
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
        #[cfg(feature = "test-hooks")]
        let dispatcher = DispatchConfig {
            core_handlers: super::watch_test_hooks::core_handlers(
                service.cluster,
                service.config.node_id,
                dispatcher.core_handlers,
            ),
            ..dispatcher
        };
        auth_service
            .set(Arc::downgrade(&service))
            .map_err(|_| Error::invalid("Authentication service was installed twice"))?;
        let mut tasks = RequiredTasks::default();
        let internal_service = service.clone();
        let internal_config = dispatcher.clone();
        let internal_budget = budget.clone();
        let internal_dispatch = dispatch.clone();
        let internal_stopped = stopped.clone();
        tasks.push(
            "internal_quic",
            spawn_required(stopped.clone(), None, async move {
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
            }),
        );
        if let Some(external) = external {
            let external_service = service.clone();
            let external_stopped = service.external_stop.subscribe();
            let expected_stop = external_stopped.clone();
            tasks.push(
                "external_quic",
                spawn_required(stopped.clone(), Some(expected_stop), async move {
                    crate::server::run(
                        external,
                        external_service.clone(),
                        dispatcher,
                        budget,
                        dispatch,
                        external_stopped,
                    )
                    .await
                    .map_err(Error::foundation)
                }),
            );
        }
        let (management_address, task) =
            super::management::start(service.clone(), stopped.clone()).await?;
        tasks.push("management", task);
        if service.metadata.is_some() {
            tasks.push(
                "replica_placement",
                spawn_required(
                    stopped.clone(),
                    None,
                    service.clone().rebalance(stopped.clone()),
                ),
            );
            if service.config.runtime.topic_scaling.is_some() {
                tasks.push(
                    "topic_scaling",
                    spawn_required(
                        stopped.clone(),
                        None,
                        service.clone().scale_topics(stopped.clone()),
                    ),
                );
            }
            tasks.push(
                "node_drains",
                spawn_required(
                    stopped.clone(),
                    None,
                    service.clone().follow_node_drains(stopped.clone()),
                ),
            );
            tasks.push(
                "metadata_membership",
                spawn_required(
                    stopped.clone(),
                    None,
                    service.clone().follow_metadata_membership(stopped.clone()),
                ),
            );
            tasks.push(
                "authority_transfers",
                spawn_required(
                    stopped.clone(),
                    None,
                    service.clone().resume_authority_transfers(stopped.clone()),
                ),
            );
        }
        tasks.push(
            "registry",
            spawn_required(
                stopped.clone(),
                None,
                service.clone().follow_registry(stopped.clone()),
            ),
        );
        if !service.data.is_empty() {
            tasks.push(
                "data_maintenance",
                spawn_required(
                    stopped.clone(),
                    None,
                    service.clone().maintain(stopped.clone()),
                ),
            );
        }
        if !service.objects.is_empty() {
            tasks.push(
                "lifecycle_pins",
                spawn_required(
                    stopped.clone(),
                    None,
                    service.clone().expire_pins(stopped.clone()),
                ),
            );
        }
        let bootstrap = service.clone();
        tasks.push(
            "metadata_reconcile",
            spawn_required(stopped.clone(), None, async move {
                bootstrap.reconcile(stopped).await
            }),
        );
        let tasks = tasks.supervised(&service);
        Ok(Self {
            external_address,
            internal_address: Some(internal_address),
            console_address: None,
            management_address,
            service,
            shutdown,
            tasks,
            console: None,
            local_rpc: None,
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
        let mut process = self.service.process.subscribe();
        loop {
            if *stopped.borrow()
                || matches!(
                    self.service.process.phase(),
                    super::process::Phase::Failed | super::process::Phase::Stopped
                )
            {
                return;
            }
            tokio::select! {
                _ = stopped.changed() => {},
                _ = process.changed() => {},
            }
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

    /// One deadline covers completion admission, listeners, handlers, group writers and locks.
    pub async fn shutdown_until(mut self, deadline: tokio::time::Instant) -> Result<()> {
        self.service.process.drain();
        self.service
            .draining
            .store(true, std::sync::atomic::Ordering::Release);
        self.service.watch_budget.close();
        if self.service.process.phase() != super::process::Phase::Failed
            && self
                .service
                .transport_metrics
                .iter()
                .any(|metrics| metrics.snapshot().active_connections > 0)
        {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let completion = (remaining / 3).min(Duration::from_secs(1));
            tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + completion)).await;
        }
        let _ = self.service.external_stop.send(true);
        let _ = self.shutdown.send(true);
        let console = self.console.take();
        let (mut failure, console_result) = tokio::join!(
            join_workers(std::mem::take(&mut self.tasks), deadline),
            async move {
                if let Some(console) = console {
                    tokio::time::timeout_at(deadline, console.shutdown_until(deadline))
                        .await
                        .map_err(|_| {
                            Error::invalid("Console exceeded the total process shutdown deadline")
                        })?
                        .map_err(Error::foundation)
                } else {
                    Ok(())
                }
            }
        );
        if let Err(error) = console_result {
            failure.get_or_insert(error);
        }
        self.service.rpc.begin_drain(deadline);
        if let Some(task) = self.local_rpc.take()
            && let Some(error) = join_workers(vec![task], deadline).await
        {
            failure.get_or_insert(error);
        }
        match tokio::time::timeout_at(deadline, self.service.close(deadline)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                failure.get_or_insert(error);
            }
            Err(_) => {
                failure.get_or_insert_with(|| {
                    Error::invalid("Runtime exceeded its total shutdown deadline")
                });
            }
        }
        if failure.is_some() {
            self.service.process.fail("shutdown");
        }
        if self.service.process.phase() == super::process::Phase::Failed {
            failure.get_or_insert_with(|| Error::invalid("A required process dependency failed"));
        }
        self.service.process.stopped();
        failure.map_or(Ok(()), Err)
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        self.service.process.drain();
        self.service
            .rpc
            .begin_drain(tokio::time::Instant::now() + Duration::from_secs(5));
        let _ = self.service.external_stop.send(true);
        let _ = self.shutdown.send(true);
    }
}

async fn join_workers(
    tasks: Vec<JoinHandle<Result<()>>>,
    deadline: tokio::time::Instant,
) -> Option<Error> {
    let mut failure = None;
    for mut task in tasks {
        let result = match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(Ok(result)) => result,
            Ok(Err(source)) => Err(Error::operation("join runtime worker", source)),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(Error::invalid("Runtime worker exceeded its drain deadline"))
            }
        };
        if let Err(error) = result {
            failure.get_or_insert(error);
        }
    }
    failure
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExitCause {
    Unexpected,
    ProcessShutdown,
    ExternalAdmission,
}

pub(crate) struct RequiredWorker {
    task: JoinHandle<Result<()>>,
    exit: Arc<std::sync::OnceLock<ExitCause>>,
    external: bool,
    _owner: WorkerOwner,
}

struct WorkerOwner(tokio::task::AbortHandle);

impl Drop for WorkerOwner {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl RequiredWorker {
    pub(super) fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.task.abort_handle()
    }
}

pub(super) fn spawn_required(
    stopped: watch::Receiver<bool>,
    external_stop: Option<watch::Receiver<bool>>,
    future: impl std::future::Future<Output = Result<()>> + Send + 'static,
) -> RequiredWorker {
    let exit = Arc::new(std::sync::OnceLock::new());
    let completed = exit.clone();
    let external = external_stop.is_some();
    let task = tokio::spawn(async move {
        let result = future.await;
        let cause = if result.is_err() {
            ExitCause::Unexpected
        } else if *stopped.borrow() {
            ExitCause::ProcessShutdown
        } else if external_stop.as_ref().is_some_and(|stop| *stop.borrow()) {
            ExitCause::ExternalAdmission
        } else {
            ExitCause::Unexpected
        };
        // The receipt is immutable before the producer JoinHandle becomes finished.
        let _ = completed.set(cause);
        result
    });
    let owner = WorkerOwner(task.abort_handle());
    RequiredWorker {
        task,
        exit,
        external,
        _owner: owner,
    }
}

#[derive(Default)]
struct RequiredTasks(Vec<(&'static str, RequiredWorker)>);

impl RequiredTasks {
    fn push(&mut self, name: &'static str, worker: RequiredWorker) {
        self.0.push((name, worker));
    }

    fn supervised(&mut self, service: &Arc<Service>) -> Vec<JoinHandle<Result<()>>> {
        std::mem::take(&mut self.0)
            .into_iter()
            .map(|(name, worker)| supervise(name, worker, service.clone()))
            .collect()
    }

    async fn abort_and_join(&mut self, deadline: tokio::time::Instant) -> Option<Error> {
        self.abort();
        join_workers(
            std::mem::take(&mut self.0)
                .into_iter()
                .map(|(_, worker)| worker.task)
                .collect(),
            deadline,
        )
        .await
    }

    fn abort(&self) {
        for (_, worker) in &self.0 {
            worker.task.abort();
        }
    }
}

impl Drop for RequiredTasks {
    fn drop(&mut self) {
        self.abort();
    }
}

fn supervise(
    dependency: &'static str,
    worker: RequiredWorker,
    service: Arc<Service>,
) -> JoinHandle<Result<()>> {
    let observed = observe_required(dependency, worker, service);
    tokio::spawn(async move { observed.await.map(|_| ()) })
}

pub(super) async fn observe_required(
    dependency: &'static str,
    worker: RequiredWorker,
    service: Arc<Service>,
) -> Result<ExitCause> {
    let mut worker = worker;
    let result = (&mut worker.task)
        .await
        .map_err(|source| Error::operation("join required runtime task", source))
        .and_then(|result| result);
    let cause = worker.exit.get().copied().unwrap_or(ExitCause::Unexpected);
    let expected = result.is_ok() && cause != ExitCause::Unexpected;
    if worker.external {
        service.external_drained.store(
            if expected { 1 } else { 2 },
            std::sync::atomic::Ordering::Release,
        );
    }
    if !expected {
        service.process.fail(dependency);
        service
            .draining
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = service.external_stop.send(true);
        if let Err(source) = result {
            let error = anyhow::Error::new(source);
            tracing::error!(event_code = "required_runtime_task_failed", dependency,
                    error = %format_args!("{error:#}"), "A required runtime task returned an error");
            return Err(Error::Operation {
                operation: "run required runtime task",
                source: std::io::Error::other(error),
            });
        }
    } else {
        tracing::debug!(event_code = "required_runtime_task_stopped", dependency,
                cause = ?cause, "A required runtime task completed its requested stop");
    }
    result.map(|()| cause)
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
    pool: Option<Arc<PeerPool>>,
    stages: Option<Arc<crate::observability::metrics::OperationMetrics>>,
) -> Result<Replica<A>> {
    if config.role == crate::config::Role::Combined {
        let local = LocalGroup::open(
            config.data_dir.join(format!("group-{group}")),
            StorageIdentity::new::<A>(cluster, group, config.node_id),
            LocalOptions {
                metrics: stages,
                ..LocalOptions::default()
            },
            A::default(),
        )
        .await
        .map_err(|source| Error::operation("recover durable local execution group", source))?;
        let group = GroupHandle::Local(local);
        return Ok(Replica {
            state: group.state(),
            group,
            voters: BTreeSet::new(),
        });
    }
    let pool =
        pool.ok_or_else(|| Error::invalid("Separate execution requires its internal peer pool"))?;
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
    let group = GroupHandle::from_raft(raft, store.state_machine, store.log);
    Ok(Replica {
        state: group.state(),
        group,
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
        subscription: {
            let limits = crate::subscription::Limits::default();
            #[cfg(feature = "test-hooks")]
            let limits =
                super::watch_test_hooks::subscription_limits(cluster, group.group_id, limits);
            limits
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Role;
    use crate::runtime::data::consumer_tests::fixture::Fixture;

    async fn assert_late_stop_is_fatal(main_shutdown: bool) {
        let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        fixture.start_all().await;
        let node = fixture.running.remove(&1).unwrap();
        let stopped = node.shutdown.subscribe();
        let (stop, expected_stop) = watch::channel(false);
        let worker = spawn_required(
            stopped.clone(),
            (!main_shutdown).then(|| expected_stop.clone()),
            async { Ok(()) },
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while !worker.task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!*stopped.borrow() && !*expected_stop.borrow());
        if main_shutdown {
            node.shutdown.send(true).unwrap();
        } else {
            stop.send(true).unwrap();
        }
        let result = supervise("completed_required_worker", worker, node.service.clone())
            .await
            .unwrap();
        let phase = node.service.process.phase();
        let shutdown = node.shutdown().await;
        fixture.close().await;
        assert!(result.is_ok());
        assert_eq!(
            phase,
            super::super::process::Phase::Failed,
            "A late stop must not excuse an already completed unexpected exit"
        );
        assert!(shutdown.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn late_external_stop_cannot_reclassify_completed_worker() {
        assert_late_stop_is_fatal(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn late_main_shutdown_cannot_reclassify_completed_worker() {
        assert_late_stop_is_fatal(true).await;
    }

    async fn assert_required_failure(
        future: impl std::future::Future<Output = Result<()>> + Send + 'static,
        expected_stop: Option<bool>,
        returns_error: bool,
        main_shutdown: bool,
    ) {
        let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        fixture.start_all().await;
        let node = fixture.running.remove(&1).unwrap();
        if main_shutdown {
            node.shutdown.send(true).unwrap();
        }
        let worker = spawn_required(
            node.shutdown.subscribe(),
            expected_stop.map(|signalled| watch::channel(signalled).1),
            future,
        );
        let result = supervise("test_required_worker", worker, node.service.clone())
            .await
            .unwrap();
        assert_eq!(result.is_err(), returns_error);
        assert_eq!(
            node.service.process.phase(),
            super::super::process::Phase::Failed
        );
        assert!(node.shutdown().await.is_err());
        fixture.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn required_worker_return_without_stop_is_fatal() {
        assert_required_failure(async { Ok(()) }, None, false, false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_worker_return_without_its_own_stop_is_fatal() {
        assert_required_failure(async { Ok(()) }, Some(false), false, false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_worker_error_after_expected_stop_is_fatal() {
        assert_required_failure(
            async { Err(Error::invalid("Injected required worker failure")) },
            Some(true),
            true,
            false,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_worker_panic_after_expected_stop_is_fatal() {
        assert_required_failure(
            async { panic!("Injected required worker panic") },
            Some(true),
            true,
            false,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn worker_error_after_main_shutdown_is_fatal() {
        assert_required_failure(
            async { Err(Error::invalid("Injected required worker failure")) },
            None,
            true,
            true,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn worker_panic_after_main_shutdown_is_fatal() {
        assert_required_failure(
            async { panic!("Injected required worker panic") },
            None,
            true,
            true,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn worker_abort_after_requested_stops_is_fatal() {
        let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        fixture.start_all().await;
        let node = fixture.running.remove(&1).unwrap();
        node.shutdown.send(true).unwrap();
        let worker = spawn_required(
            node.shutdown.subscribe(),
            Some(watch::channel(true).1),
            std::future::pending(),
        );
        worker.task.abort();
        let result = supervise("aborted_required_worker", worker, node.service.clone())
            .await
            .unwrap();
        assert!(
            result.is_err(),
            "A required worker cancellation was ignored"
        );
        assert_eq!(
            node.service.process.phase(),
            super::super::process::Phase::Failed
        );
        assert!(node.shutdown().await.is_err());
        fixture.close().await;
    }

    async fn assert_requested_stop_is_clean(main_shutdown: bool) {
        let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        fixture.start_all().await;
        let node = fixture.running.remove(&1).unwrap();
        if main_shutdown {
            node.shutdown.send(true).unwrap();
        }
        let worker = spawn_required(
            node.shutdown.subscribe(),
            (!main_shutdown).then(|| watch::channel(true).1),
            async { Ok(()) },
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while !worker.task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        supervise("expected_worker", worker, node.service.clone())
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            node.service.process.phase(),
            super::super::process::Phase::Failed
        );
        node.shutdown().await.unwrap();
        fixture.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn expected_external_stop_recorded_before_completion_is_clean() {
        assert_requested_stop_is_clean(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn expected_main_shutdown_recorded_before_completion_is_clean() {
        assert_requested_stop_is_clean(true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn expected_external_stop_cannot_clear_an_existing_failure() {
        let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        fixture.start_all().await;
        let node = fixture.running.remove(&1).unwrap();
        node.service.process.fail("another_required_worker");
        let worker = spawn_required(
            node.shutdown.subscribe(),
            Some(watch::channel(true).1),
            async { Ok(()) },
        );
        supervise("external_quic", worker, node.service.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            node.service.process.phase(),
            super::super::process::Phase::Failed
        );
        assert!(node.shutdown().await.is_err());
        fixture.close().await;
    }
}
