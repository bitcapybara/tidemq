use super::*;

use std::sync::atomic::Ordering;
use tokio_rustls::rustls::pki_types::{CertificateDer, pem::PemObject};

impl RunningNode {
    pub(super) async fn start_combined(config: Config) -> Result<Self> {
        let mut startup = LocalStartup::default();
        let result = assemble(config, &mut startup).await;
        if result.is_err() {
            startup
                .cleanup(tokio::time::Instant::now() + Duration::from_secs(5))
                .await;
        }
        result
    }
}

async fn assemble(config: Config, startup: &mut LocalStartup) -> Result<RunningNode> {
    let console_config = config
        .console
        .clone()
        .ok_or_else(|| Error::invalid("Combined HTTPS configuration is absent"))?;
    crate::console::validate_embedded(&console_config, &config.tls)
        .await
        .map_err(Error::foundation)?;
    let cluster = config
        .cluster_id
        .parse::<Id>()
        .map_err(|source| Error::operation("validate local cluster identity", source))?;
    let certificate = super::super::read_bounded(&config.tls.certificate, 65_536).await?;
    let leaf = CertificateDer::from_pem_slice(&certificate)
        .map_err(|source| Error::operation("parse local service certificate", source))?;
    let identity = TlsIdentity::new(
        certificate.clone(),
        super::super::read_secret(&config.tls.private_key, 16_384).await?,
    )
    .map_err(|source| Error::operation("load local service TLS identity", source))?;
    let local_identity = local_identity(&config, cluster, leaf.as_ref())?;
    tokio::fs::create_dir_all(&config.data_dir)
        .await
        .map_err(|source| Error::operation("create local process data directory", source))?;
    if super::super::node_retirement::load(&config, cluster)
        .await?
        .is_some()
    {
        return Err(Error::invalid(
            "Local execution cannot reopen a retired cluster node",
        ));
    }
    let budget = MemoryBudget::new(&config.memory)
        .map_err(|source| Error::operation("create local process memory budget", source))?;
    let super::super::facilities::Facilities {
        mut metrics,
        dispatch,
        runtime: runtime_metrics,
        producer,
        ownership,
        quota,
        repair,
    } = super::super::facilities::Facilities::new(&config)?;
    let console_metrics = Arc::new(crate::console::ConsoleMetrics::default());
    console_metrics.register(metrics.registry_mut());
    let metadata = open_replica::<MetadataState>(
        &config,
        config.runtime.metadata_group,
        &[],
        cluster,
        None,
        metrics.operation(Operation::Control),
    )
    .await?;
    startup.metadata = Some(metadata.group.clone());
    let mut data = BTreeMap::new();
    for group in &config.runtime.groups {
        let replica = open_replica::<DataApplication>(
            &config,
            group.group_id,
            &[],
            cluster,
            None,
            metrics.operation(Operation::Produce),
        )
        .await?;
        startup.data.push(replica.group.clone());
        data.insert(group.group_id, replica);
    }
    let auth_service = Arc::new(std::sync::OnceLock::new());
    let dispatcher = DispatchConfig {
        protocol: config.protocol.clone(),
        ..DispatchConfig::default()
    };
    let (in_process, requests) =
        crate::server::InProcess::new(dispatcher.clone(), budget.clone(), dispatch.clone())
            .map_err(Error::foundation)?;
    let rpc = PeerRpc::local(
        in_process,
        auth_service.clone(),
        local_identity.clone(),
        config.protocol.clone(),
    );
    let authority = Arc::new(Authority::new_local(
        metadata.group.clone(),
        cluster,
        config.runtime.metadata_group,
        ControlPolicy::default(),
    )?);
    let objects = super::super::objects::coordinators(
        &config,
        cluster,
        Some(&metadata),
        &data,
        rpc.clone(),
        authority.clone(),
        budget.clone(),
        metrics.registry_mut(),
    )
    .await?;
    let external = bind_server(
        config.listen,
        ServerTls {
            identity,
            client_trust_roots: None,
        },
        ServerAuthorization::Dynamic(Arc::new(super::super::auth::Authenticator(
            auth_service.clone(),
        ))),
        transport_config(&config),
        budget.clone(),
        tokio::time::Instant::now() + Duration::from_secs(3),
    )
    .await?;
    let external_address = external.local_addr();
    let registry = Arc::new(super::super::registry::Registry::local(
        &config,
        &local_identity,
        external_address,
    )?);
    metrics
        .registry_mut()
        .sub_registry_with_prefix("tidemq_external_server")
        .register_collector(Box::new(external.metrics.clone()));
    let transport_metrics = vec![external.metrics.clone()];
    let (shutdown, stopped) = watch::channel(false);
    let (external_stop, _) = watch::channel(false);
    let service = Arc::new(Service::new(
        config,
        cluster,
        Some(metadata),
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
        None,
        external_stop,
        true,
    )?);
    startup.service = Some(service.clone());
    auth_service
        .set(Arc::downgrade(&service))
        .map_err(|_| Error::invalid("Local service was installed twice"))?;
    startup.local_rpc = Some(supervise(
        tokio::spawn(async move {
            requests
                .run(service.clone())
                .await
                .map_err(Error::foundation)
        }),
        startup
            .service
            .as_ref()
            .cloned()
            .ok_or_else(Error::unavailable)?,
        stopped.clone(),
    ));
    let service = startup
        .service
        .as_ref()
        .cloned()
        .ok_or_else(Error::unavailable)?;
    let api = crate::console::api::ApiState::load_local(
        &console_config,
        super::super::LocalConsole::new(service.clone())?,
    )
    .await
    .map_err(Error::foundation)?;
    startup.console = Some(
        crate::console::RunningConsole::start_embedded(
            &console_config,
            &service.config.tls,
            api,
            service.process.clone(),
            console_metrics,
        )
        .await
        .map_err(Error::foundation)?,
    );
    let console_address = startup.console.as_ref().map(|console| console.address);
    let (management_address, management) =
        super::super::management::start(service.clone(), stopped.clone()).await?;
    startup.tasks.push(management);
    let external_service = service.clone();
    let external_stopped = service.external_stop.subscribe();
    startup.tasks.push(tokio::spawn(async move {
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
        external_service
            .external_drained
            .store(if result.is_ok() { 1 } else { 2 }, Ordering::Release);
        result
    }));
    start_business_workers(startup, &service, stopped.clone());
    for handle in startup
        .metadata
        .iter()
        .cloned()
        .map(LocalHandle::Metadata)
        .chain(startup.data.iter().cloned().map(LocalHandle::Data))
    {
        startup
            .tasks
            .push(tokio::spawn(watch_group(handle, stopped.clone())));
    }
    let tasks = std::mem::take(&mut startup.tasks)
        .into_iter()
        .map(|task| supervise(task, service.clone(), stopped.clone()))
        .collect();
    service.console_ready.store(true, Ordering::Release);
    startup.committed = true;
    Ok(RunningNode {
        external_address: Some(external_address),
        internal_address: None,
        console_address,
        management_address,
        service,
        shutdown,
        tasks,
        console: startup.console.take(),
        local_rpc: startup.local_rpc.take(),
    })
}

fn local_identity(config: &Config, cluster: Id, leaf: &[u8]) -> Result<NodeGrant> {
    let console = config.console.as_ref().ok_or_else(Error::unavailable)?;
    let origin = reqwest::Url::parse(&console.session.public_origin)
        .map_err(|source| Error::operation("parse local HTTPS origin", source))?;
    let name = origin
        .host_str()
        .ok_or_else(|| Error::invalid("Local HTTPS origin has no host"))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let mut groups = vec![config.runtime.metadata_group];
    groups.extend(config.runtime.groups.iter().map(|group| group.group_id));
    Ok(NodeGrant {
        certificate_pin: NodeGrant::certificate_pin(leaf),
        certificate_name: name,
        cluster_id: cluster,
        node_id: config.node_id,
        role: "combined".into(),
        allowed_groups: groups,
        allowed_classes: classes("broker"),
    })
}

fn start_business_workers(
    startup: &mut LocalStartup,
    service: &Arc<Service>,
    stopped: watch::Receiver<bool>,
) {
    startup
        .tasks
        .push(tokio::spawn(service.clone().scale_topics(stopped.clone())));
    startup
        .tasks
        .push(tokio::spawn(service.clone().maintain(stopped.clone())));
    startup
        .tasks
        .push(tokio::spawn(service.clone().expire_pins(stopped.clone())));
    let bootstrap = service.clone();
    startup.tasks.push(tokio::spawn(
        async move { bootstrap.reconcile(stopped).await },
    ));
}

enum LocalHandle {
    Metadata(GroupHandle<MetadataState>),
    Data(GroupHandle<DataApplication>),
}

async fn watch_group(handle: LocalHandle, mut stopped: watch::Receiver<bool>) -> Result<()> {
    let mut health = match &handle {
        LocalHandle::Metadata(GroupHandle::Local(group)) => group.health(),
        LocalHandle::Data(GroupHandle::Local(group)) => group.health(),
        _ => {
            return Err(Error::invalid(
                "Local group supervision received a Raft group",
            ));
        }
    };
    loop {
        if *stopped.borrow() {
            return Ok(());
        }
        let phase = health.borrow().clone();
        if !matches!(phase, crate::execution::LocalHealth::Ready) {
            return Err(Error::invalid(
                "Required local execution group stopped serving authority",
            ));
        }
        tokio::select! {
            _ = stopped.changed() => return Ok(()),
            changed = health.changed() => if changed.is_err() { return Err(Error::invalid("Local group health supervision ended")); },
        }
    }
}

#[derive(Default)]
struct LocalStartup {
    committed: bool,
    metadata: Option<GroupHandle<MetadataState>>,
    data: Vec<GroupHandle<DataApplication>>,
    tasks: Vec<JoinHandle<Result<()>>>,
    local_rpc: Option<JoinHandle<Result<()>>>,
    console: Option<crate::console::RunningConsole>,
    service: Option<Arc<Service>>,
}

impl LocalStartup {
    async fn cleanup(&mut self, deadline: tokio::time::Instant) {
        if let Some(service) = &self.service {
            service.process.fail("startup");
            let _ = service.external_stop.send(true);
            let _ = service.shutdown_requested.send(true);
        }
        if let Some(console) = self.console.take() {
            let _ = tokio::time::timeout_at(deadline, console.shutdown_until(deadline)).await;
        }
        for task in &self.tasks {
            task.abort();
        }
        for mut task in std::mem::take(&mut self.tasks) {
            let _ = tokio::time::timeout_at(deadline, &mut task).await;
        }
        if let Some(service) = &self.service {
            service.rpc.begin_drain(deadline);
        }
        if let Some(mut task) = self.local_rpc.take() {
            if tokio::time::timeout_at(deadline, &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
        for group in &self.data {
            let _ = group.close(deadline).await;
        }
        if let Some(group) = &self.metadata {
            let _ = group.close(deadline).await;
        }
    }
}

impl Drop for LocalStartup {
    fn drop(&mut self) {
        if !self.committed {
            if let Some(service) = &self.service {
                service.process.fail("startup_cancelled");
                service.rpc.begin_drain(tokio::time::Instant::now());
                let _ = service.external_stop.send(true);
                let _ = service.shutdown_requested.send(true);
            }
            for task in &self.tasks {
                task.abort();
            }
            if let Some(task) = &self.local_rpc {
                task.abort();
            }
        }
    }
}
