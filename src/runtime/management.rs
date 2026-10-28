use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout_at};

use crate::ownership::remote::Query;

use super::service::Service;
use super::{Error, Result};

struct Health {
    ready: AtomicBool,
    snapshot: RwLock<Value>,
}

#[derive(serde::Serialize)]
struct CapacitySnapshot {
    usage: crate::quota::Usage,
    data_filesystem: crate::quota::disk::Snapshot,
    reserved_storage_bytes: u64,
    ack_state_logical_bytes: u64,
    sample_started_at_age_ms: u64,
    physical_scope: &'static str,
}

pub(crate) async fn start(
    service: Arc<Service>,
    shutdown: watch::Receiver<bool>,
) -> Result<(std::net::SocketAddr, super::node::RequiredWorker)> {
    let listener = TcpListener::bind(service.config.management_listen)
        .await
        .map_err(|source| Error::operation("bind management listener", source))?;
    let address = listener
        .local_addr()
        .map_err(|source| Error::operation("read management listener address", source))?;
    let health = Arc::new(Health {
        ready: AtomicBool::new(false),
        snapshot: RwLock::new(serde_json::json!({"status":"starting"})),
    });
    let slots = Arc::new(Semaphore::new(4));
    let sampler = spawn_sampler(service.clone(), health.clone(), shutdown.clone());
    let task = super::node::spawn_required(
        shutdown.clone(),
        None,
        run(listener, service, health, slots, shutdown, sampler),
    );
    Ok((address, task))
}

fn spawn_sampler(
    service: Arc<Service>,
    health: Arc<Health>,
    shutdown: watch::Receiver<bool>,
) -> super::node::RequiredWorker {
    super::node::spawn_required(shutdown.clone(), None, async move {
        sample(service, health, shutdown).await;
        Ok(())
    })
}

async fn run(
    listener: TcpListener,
    service: Arc<Service>,
    health: Arc<Health>,
    slots: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
    sampler: super::node::RequiredWorker,
) -> Result<()> {
    let mut connections = JoinSet::new();
    let sampler_abort = sampler.abort_handle();
    let observed = super::node::observe_required("readiness_sampler", sampler, service.clone());
    let mut observed = std::pin::pin!(observed);
    let mut sampler_finished = false;
    let result = loop {
        while connections.try_join_next().is_some() {}
        tokio::select! {
            biased;
            _ = shutdown.changed() => break Ok(()),
            completed = &mut observed => {
                sampler_finished = true;
                break sampler_result(&service, completed);
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(source) => break Err(Error::operation("accept management connection", source)),
                };
                if !peer.ip().is_loopback() { continue; }
                let Ok(permit) = slots.clone().try_acquire_owned() else { continue; };
                let service = service.clone(); let health = health.clone();
                connections.spawn(async move { let _permit = permit; let _ = serve(stream, service, health).await; });
            }
        }
    };
    health.ready.store(false, Ordering::Release);
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    let cleanup = if sampler_finished {
        Ok(())
    } else {
        if result.is_err() {
            service.process.fail("management");
            sampler_abort.abort();
        }
        // The outer worker deadline also bounds this owned sampler drain.
        sampler_result(&service, observed.await)
    };
    result.and(cleanup)
}

fn sampler_result(service: &Service, result: Result<super::node::ExitCause>) -> Result<()> {
    match result {
        Ok(super::node::ExitCause::ProcessShutdown) => Ok(()),
        Ok(_) => {
            service.process.fail("readiness_sampler");
            Err(Error::invalid(
                "Required readiness sampler exited unexpectedly",
            ))
        }
        Err(error) => Err(error),
    }
}

async fn sample(service: Arc<Service>, health: Arc<Health>, mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let sampler = crate::quota::memory::MemorySampler::default();
    let physical = super::physical::PhysicalSampler::new(service.config.data_dir.clone());
    let disk = crate::quota::disk::DiskSampler::new(service.config.data_dir.clone()).ok();
    let pressure =
        crate::scaling::PressureSampler::with_metrics(service.transport_metrics.clone()).ok();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { _ = shutdown.changed() => return, _ = ticker.tick() => {} }
        service.sample_ownership_validity();
        let admitted = u64::from(service.config.memory.total_bytes)
            .saturating_sub(u64::try_from(service.budget.available_bytes()).unwrap_or(u64::MAX));
        service
            .metrics
            .admitted_bytes
            .set(i64::try_from(admitted).unwrap_or(i64::MAX));
        let started = Instant::now();
        let deadline = started + Duration::from_millis(900);
        let mut ready = !service.draining.load(Ordering::Acquire)
            && service.registry.schedulable(service.config.node_id)
            && service
                .authority
                .query(Query::TopicName("__readiness".into()), deadline)
                .await
                .is_ok();
        ready &= service.auth_ready(deadline).await;
        let memory = sampler.sample(deadline).await.ok();
        let capacity = sample_capacity(
            &service,
            &physical,
            disk.as_ref(),
            memory,
            started,
            deadline,
        )
        .await
        .ok();
        if capacity.is_none() {
            let _ = service.scaling.capacity.replace(None);
        }
        let pressure = if let Some(sampler) = &pressure {
            sampler.sample(deadline).await.ok().flatten()
        } else {
            None
        };
        let placement = capacity
            .as_ref()
            .zip(pressure)
            .and_then(|(capacity, pressure)| {
                let allowed = service
                    .quota
                    .lock()
                    .ok()?
                    .admit(
                        crate::quota::Work::BackgroundSnapshot,
                        std::time::Instant::now(),
                    )
                    .is_ok();
                if !allowed {
                    return None;
                }
                Some(crate::scaling::Capacity {
                    node_id: service.config.node_id,
                    observed_at: started.into_std(),
                    free_bytes: capacity.data_filesystem.available_bytes,
                    reserved_bytes: capacity.reserved_storage_bytes,
                    cpu_per_mille: pressure.cpu_per_mille,
                    bytes_per_second: pressure.bytes_per_second,
                    connections: pressure.active_connections,
                    ack_bytes: capacity.ack_state_logical_bytes,
                })
            });
        let placement_available = placement.is_some();
        let _ = service.scaling.capacity.replace(placement);
        if !service.data.is_empty() {
            ready &= capacity.is_some();
        }
        for (group, replica) in &service.data {
            if ready && replica.group.is_local() {
                ready = match service.config.runtime.group(*group) {
                    Ok(config) => {
                        let expected =
                            super::node::data_config(service.cluster, config, &service.config);
                        replica
                            .state
                            .try_with_application(|state| state.config() == Some(&expected))
                            .await
                            .unwrap_or(false)
                    }
                    Err(_) => false,
                };
            }
            if ready && service.require_leader(replica).is_ok() {
                ready = replica.group.ensure_read(deadline).await.is_ok();
            }
        }
        let schedulable = service.registry.schedulable(service.config.node_id);
        let draining = service.draining.load(Ordering::Acquire);
        ready &= schedulable && !draining && service.console_ready.load(Ordering::Acquire);
        if ready {
            service.process.ready();
        }
        ready &= service.process.phase() == super::process::Phase::Ready;
        health.ready.store(ready, Ordering::Release);
        let logging = service
            .log_health
            .lock()
            .ok()
            .and_then(|health| health.clone());
        let sampled = super::telemetry::Snapshot {
            ready,
            sampled_at_ms: super::data::unix_millis().ok(),
            process_memory: memory,
            log_dropped_events: logging.as_ref().map(|health| health.dropped_events()),
            log_sink_errors: logging
                .as_ref()
                .map(|health| health.sink_errors.load(Ordering::Relaxed)),
            log_oversized_events: logging
                .as_ref()
                .map(|health| health.oversized_events.load(Ordering::Relaxed)),
            tracing: service
                .trace_health
                .lock()
                .ok()
                .and_then(|health| health.as_ref().map(|health| health.snapshot())),
        };
        if let Ok(mut metrics) = service.runtime_metrics.0.lock() {
            *metrics = sampled.clone();
        }
        let allocator = crate::allocator::snapshot().await.ok();
        let metadata = if let Some(replica) = &service.metadata {
            Some(
                replica
                    .state
                    .with_application(|state| {
                        let mut summary = serde_json::json!(state.summary());
                        if let Some(fields) = summary.as_object_mut() {
                            fields.insert(
                                "pending_layouts".into(),
                                serde_json::json!(
                                    state.topology().pending().take(8).collect::<Vec<_>>()
                                ),
                            );
                        }
                        summary
                    })
                    .await,
            )
        } else {
            None
        };
        let mut groups = Vec::new();
        for (group, replica) in &service.data {
            groups.push(group_diagnostics(*group, replica, deadline).await);
        }
        let metadata_storage = if let Some(replica) = &service.metadata {
            Some(group_diagnostics(service.config.runtime.metadata_group, replica, deadline).await)
        } else {
            None
        };
        let quota = service
            .quota
            .lock()
            .ok()
            .map(|quota| quota.diagnose(std::time::Instant::now()));
        let placement_diagnostic = service
            .scaling_diagnostic
            .lock()
            .ok()
            .map(|value| value.clone());
        let topic_scaling = service
            .topic_scaling_diagnostic
            .lock()
            .ok()
            .and_then(|value| serde_json::to_value(*value).ok());
        let transport = service
            .transport_metrics
            .iter()
            .map(|metrics| metrics.snapshot())
            .collect::<Vec<_>>();
        let memory_budget = serde_json::json!({
            "available_total_bytes": service.budget.available_bytes(),
            "available_data_bytes": service.budget.available_data_bytes(),
            "available_normal_bytes": service.budget.available_normal_bytes(),
            "available_core_bytes": service.budget.available_core_bytes(),
            "available_ack_bytes": service.budget.available_ack_bytes(),
            "available_handshake_bytes": service.budget.available_handshake_bytes(),
        });
        let retention =
            serde_json::json!({"deferrals":service.retention_deferrals.load(Ordering::Relaxed)});
        *health.snapshot.write().await = serde_json::json!({"version":env!("CARGO_PKG_VERSION"),"scope":"current_process_local_applied","node_id":service.config.node_id,"cluster_id":service.cluster,"role":service.config.role,"ready":ready,"draining":draining,"schedulable":schedulable,"allocator":allocator,"process":sampled,"metadata":metadata,"metadata_storage":metadata_storage,"data_groups":groups,"cached_routes":service.routes.read().await.len(),"capacity":capacity,"quota":quota,"memory_budget":memory_budget,"transport":transport,"placement_capacity_available":placement_available,"placement":placement_diagnostic,"topic_scaling":topic_scaling,"registry":service.registry.diagnostic().ok(),"retention":retention});
    }
}

async fn group_diagnostics<A: crate::execution::Application>(
    group_id: u64,
    replica: &super::node::Replica<A>,
    deadline: Instant,
) -> Value {
    match &replica.group {
        crate::execution::GroupHandle::Local(group) => {
            let state = match &*group.health().borrow() {
                crate::execution::LocalHealth::Ready => "ready",
                crate::execution::LocalHealth::Draining => "draining",
                crate::execution::LocalHealth::Failed { .. } => "failed",
                crate::execution::LocalHealth::Stopped => "stopped",
            };
            serde_json::json!({"execution":"local", "group_id":group_id,
                "node_id":group.identity().node_id,"state":state,"position":group.applied(),
                "storage":group.statistics(),"file_limit_bytes":group.file_limit_bytes().ok()})
        }
        crate::execution::GroupHandle::Raft { raft, log, .. } => {
            let mut status = {
                let metrics = raft.metrics();
                let metrics = metrics.borrow();
                serde_json::json!({"execution":"raft", "group_id":group_id,"leader":metrics.current_leader,
                    "term":metrics.current_term,"last_applied":metrics.last_applied,
                    "voters":metrics.membership_config.membership().voter_ids().take(4).collect::<Vec<_>>(),
                    "joint_configs":metrics.membership_config.membership().get_joint_config(),"bootstrap_voters":replica.voters})
            };
            if let Some(fields) = status.as_object_mut() {
                fields.insert(
                    "storage".into(),
                    serde_json::json!(timeout_at(deadline, log.diagnostics()).await.ok()),
                );
            }
            status
        }
    }
}

async fn sample_capacity(
    service: &Service,
    physical: &super::physical::PhysicalSampler,
    disk: Option<&crate::quota::disk::DiskSampler>,
    memory: Option<crate::quota::memory::Snapshot>,
    started: Instant,
    deadline: Instant,
) -> Result<CapacitySnapshot> {
    let memory = memory.ok_or_else(Error::unavailable)?;
    service
        .quota
        .lock()
        .map_err(|_| Error::unavailable())?
        .observe_container(memory, started.into_std());
    let mut groups = Vec::new();
    let mut reserved_storage_bytes = 0_u64;
    let mut ack_state_logical_bytes = 0_u64;
    let mut usage = crate::quota::Usage {
        process_memory_bytes: memory.process_rss_bytes,
        ..Default::default()
    };
    for (group, replica) in &service.data {
        let data = replica
            .state
            .try_with_application(|state| {
                if state.config().is_none()
                    && state.ownership().is_none()
                    && state.hot_messages() == 0
                    && state.lifecycle().object_count() == 0
                {
                    Ok(crate::broker::CapacityUsage::default())
                } else {
                    state.capacity_usage()
                }
            })
            .await
            .map_err(|source| Error::operation("sample committed local capacity", source))?
            .map_err(super::data::data_rejection)?;
        usage.logical_backlog_bytes = usage
            .logical_backlog_bytes
            .checked_add(data.logical_backlog_upper_bytes)
            .ok_or_else(Error::unavailable)?;
        ack_state_logical_bytes = ack_state_logical_bytes
            .checked_add(data.ack_state_logical_bytes)
            .ok_or_else(Error::unavailable)?;
        let (limit, snapshot) = replica.storage_bound().await?;
        reserved_storage_bytes = reserve_growth(reserved_storage_bytes, limit)?;
        groups.push((*group, snapshot));
        if let Some(coordinator) = service.objects.get(group) {
            let objects = coordinator
                .capacity_usage(deadline.into_std())
                .await
                .map_err(|source| {
                    Error::operation("sample authoritative object capacity", source)
                })?;
            usage.physical_storage_bytes = usage
                .physical_storage_bytes
                .checked_add(objects.registered_physical_upper_bytes)
                .ok_or_else(Error::unavailable)?;
            usage.shared_reclaim_lag_bytes = usage
                .shared_reclaim_lag_bytes
                .checked_add(objects.reclaim_lag_upper_bytes)
                .ok_or_else(Error::unavailable)?;
        }
    }
    if let Some(metadata) = &service.metadata {
        let (limit, snapshot) = metadata.storage_bound().await?;
        reserved_storage_bytes = reserve_growth(reserved_storage_bytes, limit)?;
        groups.push((service.config.runtime.metadata_group, snapshot));
    }
    let (local_bytes, disk) = tokio::join!(physical.sample(groups, deadline), async {
        disk.ok_or_else(Error::unavailable)?
            .sample(deadline)
            .await
            .map_err(|source| Error::operation("sample configured data filesystem", source))
    });
    let disk = disk?;
    let local_bytes = local_bytes?;
    usage.physical_storage_bytes = usage
        .physical_storage_bytes
        .checked_add(local_bytes)
        .ok_or_else(Error::unavailable)?;
    service
        .quota
        .lock()
        .map_err(|_| Error::unavailable())?
        .observe(usage, started.into_std());
    service.quota_observed.notify_waiters();
    *service
        .disk_capacity
        .lock()
        .map_err(|_| Error::unavailable())? = Some((disk, started));
    Ok(CapacitySnapshot {
        usage,
        data_filesystem: disk,
        reserved_storage_bytes: reserved_storage_bytes.saturating_sub(local_bytes),
        ack_state_logical_bytes,
        sample_started_at_age_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        physical_scope: "local_replica_file_lengths_plus_registered_object_upper_bound",
    })
}

fn reserve_growth(previous: u64, file_limit_bytes: u64) -> Result<u64> {
    // Includes WAL checkpoints/staging and bounded named, pinned, and incoming snapshots.
    previous
        .checked_add(file_limit_bytes)
        .ok_or_else(Error::unavailable)
}

async fn serve(mut stream: TcpStream, service: Arc<Service>, health: Arc<Health>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(2);
    let _memory = service
        .budget
        .try_reserve_core(524_288)
        .map_err(|source| Error::operation("admit management response", source))?;
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    while request.len() <= 8192 {
        let read = timeout_at(deadline, stream.read(&mut chunk))
            .await
            .map_err(|_| Error::invalid("Management request deadline expired"))?
            .map_err(|source| Error::operation("read management request", source))?;
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(
            chunk
                .get(..read)
                .ok_or_else(|| Error::invalid("Management read exceeded its buffer"))?,
        );
        if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
            break;
        }
    }
    let path = parse_request(&request);
    let (status, content_type, body) = match path {
        Ok("/healthz") => (200, "application/json", "{\"alive\":true}".into()),
        Ok("/readyz") => {
            let ready = health.ready.load(Ordering::Acquire)
                && service.process.phase() == super::process::Phase::Ready
                && !service.draining.load(Ordering::Acquire)
                && service.registry.schedulable(service.config.node_id);
            (
                if ready { 200 } else { 503 },
                "application/json",
                format!("{{\"ready\":{ready}}}"),
            )
        }
        Ok("/metrics") => (
            200,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
            service.metrics.encode().map_err(Error::foundation)?,
        ),
        Ok("/diagnostics") => (
            200,
            "application/json",
            serde_json::to_string(&*health.snapshot.read().await)
                .map_err(|source| Error::operation("encode bounded diagnostics", source))?,
        ),
        Ok(path) if path.starts_with("/ownership/") => {
            ownership_response(&service, path, deadline).await?
        }
        Ok(_) => (404, "application/json", "{\"error\":\"Unknown management endpoint; use /healthz, /readyz, /metrics, /diagnostics, or /ownership/<topic>/<partition>\"}".into()),
        Err(error) => (
            400,
            "application/json",
            serde_json::json!({"error": error.to_string()}).to_string(),
        ),
    };
    if body.len() > 262_144 {
        return Err(Error::invalid(
            "Management response exceeded its finite bound",
        ));
    }
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Service Unavailable",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n",
        body.len()
    );
    timeout_at(deadline, async {
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(body.as_bytes()).await?;
        stream.shutdown().await
    })
    .await
    .map_err(|_| Error::invalid("Management response deadline expired"))?
    .map_err(|source| Error::operation("write management response", source))
}

async fn ownership_response(
    service: &Service,
    path: &str,
    deadline: Instant,
) -> Result<(u16, &'static str, String)> {
    let key = match ownership_key(path) {
        Ok(key) => key,
        Err(error) => {
            return Ok((
                400,
                "application/json",
                serde_json::json!({"error": error.to_string()}).to_string(),
            ));
        }
    };
    let Some(replica) = &service.metadata else {
        return Ok((
            404,
            "application/json",
            "{\"error\":\"Ownership diagnostics require a metadata or combined node with a local metadata replica\"}".into(),
        ));
    };
    let diagnostic = timeout_at(
        deadline,
        replica
            .state
            .try_with_application(|state| state.diagnose_ownership(key)),
    )
    .await;
    match diagnostic {
        Ok(Ok(Some(diagnostic))) => Ok((
            200,
            "application/json",
            serde_json::to_string(&diagnostic)
                .map_err(|source| Error::operation("encode ownership diagnostic", source))?,
        )),
        Ok(Ok(None)) => Ok((
            404,
            "application/json",
            serde_json::json!({"error": "No ownership state exists for the requested partition", "topic_id": key.topic_id.to_string(), "partition_id": key.partition}).to_string(),
        )),
        Err(_) => Ok((
            503,
            "application/json",
            "{\"error\":\"Ownership diagnostic exceeded the request deadline\"}".into(),
        )),
        Ok(Err(source)) => {
            let err = anyhow::Error::new(Error::operation("read ownership diagnostic state", source));
            tracing::warn!(event_code = "ownership_diagnostic_failed", topic_id = %key.topic_id,
                partition_id = key.partition, error = %format_args!("{err:#}"), "Ownership diagnostic state read failed");
            Ok((503, "application/json", "{\"error\":\"Metadata state is unavailable; inspect the ownership_diagnostic_failed server log\"}".into()))
        }
    }
}

fn ownership_key(path: &str) -> Result<crate::metadata::PartitionKey> {
    path.strip_prefix("/ownership/")
        .ok_or_else(|| Error::invalid("Ownership path must start with /ownership/"))?
        .to_owned()
        .try_into()
        .map_err(|reason| Error::invalid(format!("Invalid ownership key: {reason}")))
}

fn parse_request(bytes: &[u8]) -> Result<&str> {
    if bytes.len() > 8192 {
        return Err(Error::invalid("Management request is too large"));
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| Error::invalid("Management request must be ASCII"))?;
    let (headers, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| Error::invalid("Management headers are incomplete"))?;
    if !body.is_empty() {
        return Err(Error::invalid("Management requests cannot contain a body"));
    }
    let mut lines = headers.split("\r\n");
    let mut first = lines
        .next()
        .ok_or_else(|| Error::invalid("Management request line is absent"))?
        .split(' ');
    if first.next() != Some("GET") {
        return Err(Error::invalid("Management requests must use GET"));
    }
    let path = first
        .next()
        .filter(|path| path.len() <= 64)
        .ok_or_else(|| Error::invalid("Management path is invalid"))?;
    if !matches!(first.next(), Some("HTTP/1.0" | "HTTP/1.1")) || first.next().is_some() {
        return Err(Error::invalid("Management HTTP version is invalid"));
    }
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| Error::invalid("Management header is malformed"))?;
        if name.eq_ignore_ascii_case("transfer-encoding")
            || (name.eq_ignore_ascii_case("content-length") && value.trim() != "0")
        {
            return Err(Error::invalid("Management requests cannot contain a body"));
        }
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Role;
    use crate::runtime::data::consumer_tests::fixture::{Fixture, deadline, http};

    async fn fixture() -> Fixture {
        let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        fixture.start_all().await;
        fixture
    }

    struct SamplerLifetime(Arc<AtomicBool>);

    impl Drop for SamplerLifetime {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }

    fn pending_sampler(
        shutdown: watch::Receiver<bool>,
    ) -> (super::super::node::RequiredWorker, Arc<AtomicBool>) {
        let alive = Arc::new(AtomicBool::new(true));
        let owner = SamplerLifetime(alive.clone());
        let worker = super::super::node::spawn_required(shutdown, None, async move {
            let _owner = owner;
            std::future::pending::<Result<()>>().await
        });
        (worker, alive)
    }

    #[derive(Clone, Copy, Debug)]
    enum SamplerFault {
        Returned,
        Error,
        Panic,
        Abort,
    }

    async fn assert_completed_sampler_failure(fault: SamplerFault, stop_first: bool) {
        let mut fixture = fixture().await;
        let node = fixture.running.remove(&1).unwrap();
        let service = node.service.clone();
        let health = Arc::new(Health {
            ready: AtomicBool::new(true),
            snapshot: RwLock::new(Value::Null),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, shutdown) = watch::channel(false);
        if stop_first {
            stop.send(true).unwrap();
        }
        let sampler = super::super::node::spawn_required(shutdown.clone(), None, async move {
            match fault {
                SamplerFault::Returned => Ok(()),
                SamplerFault::Error => Err(Error::invalid("Injected readiness sampler error")),
                SamplerFault::Panic => panic!("Injected readiness sampler panic"),
                SamplerFault::Abort => std::future::pending().await,
            }
        });
        let completed = sampler.abort_handle();
        if matches!(fault, SamplerFault::Abort) {
            completed.abort();
        }
        timeout_at(deadline(), async {
            while !completed.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(*shutdown.borrow(), stop_first);
        stop.send(true).unwrap();
        let result = run(
            listener,
            service.clone(),
            health.clone(),
            Arc::new(Semaphore::new(4)),
            shutdown,
            sampler,
        )
        .await;
        let phase = service.process.phase();
        let rebound = TcpListener::bind(address).await.unwrap();
        drop(rebound);
        let node_result = node.shutdown().await;
        fixture.close().await;
        assert!(
            result.is_err(),
            "A late shutdown hid sampler fault {fault:?}"
        );
        assert!(!health.ready.load(Ordering::Acquire));
        assert_eq!(phase, super::super::process::Phase::Failed);
        assert!(node_result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn completed_sampler_panic_must_not_be_hidden_by_late_shutdown() {
        assert_completed_sampler_failure(SamplerFault::Panic, false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn completed_sampler_error_return_and_abort_survive_late_shutdown() {
        for fault in [
            SamplerFault::Returned,
            SamplerFault::Error,
            SamplerFault::Abort,
        ] {
            assert_completed_sampler_failure(fault, false).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sampler_errors_panics_and_aborts_after_stop_remain_fatal() {
        for fault in [
            SamplerFault::Error,
            SamplerFault::Panic,
            SamplerFault::Abort,
        ] {
            assert_completed_sampler_failure(fault, true).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sampler_that_observes_stop_closes_cleanly_without_reclassification() {
        let fixture = fixture().await;
        let service = fixture.running.get(&1).unwrap().service.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, shutdown) = watch::channel(false);
        let mut observed_stop = shutdown.clone();
        let sampler = super::super::node::spawn_required(shutdown.clone(), None, async move {
            observed_stop.changed().await.unwrap();
            assert!(*observed_stop.borrow());
            Ok(())
        });
        stop.send(true).unwrap();
        let completed = sampler.abort_handle();
        timeout_at(deadline(), async {
            while !completed.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        run(
            listener,
            service.clone(),
            Arc::new(Health {
                ready: AtomicBool::new(true),
                snapshot: RwLock::new(Value::Null),
            }),
            Arc::new(Semaphore::new(4)),
            shutdown,
            sampler,
        )
        .await
        .unwrap();
        assert_ne!(
            service.process.phase(),
            super::super::process::Phase::Failed
        );
        drop(TcpListener::bind(address).await.unwrap());
        fixture.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sampler_started_with_stop_already_observed_closes_cleanly() {
        let fixture = fixture().await;
        let service = fixture.running.get(&1).unwrap().service.clone();
        let (stop, shutdown) = watch::channel(true);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let health = Arc::new(Health {
            ready: AtomicBool::new(true),
            snapshot: RwLock::new(Value::Null),
        });
        let sampler = spawn_sampler(service.clone(), health.clone(), shutdown.clone());
        let worker = super::super::node::spawn_required(
            shutdown.clone(),
            None,
            run(
                listener,
                service.clone(),
                health,
                Arc::new(Semaphore::new(4)),
                shutdown,
                sampler,
            ),
        );
        let result =
            super::super::node::observe_required("management_probe", worker, service.clone()).await;
        assert_eq!(
            result.unwrap(),
            super::super::node::ExitCause::ProcessShutdown
        );
        assert_ne!(
            service.process.phase(),
            super::super::process::Phase::Failed
        );
        drop(stop);
        drop(TcpListener::bind(address).await.unwrap());
        fixture.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn readiness_sampler_panic_and_unexpected_exit_fail_the_live_process() {
        for panic in [false, true] {
            let mut fixture = fixture().await;
            let node = fixture.running.get(&1).unwrap();
            let service = node.service.clone();
            let management = node.management_address;
            let health = Arc::new(Health {
                ready: AtomicBool::new(true),
                snapshot: RwLock::new(Value::Null),
            });
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (_stop, shutdown) = watch::channel(false);
            let (release, released) = tokio::sync::oneshot::channel();
            let sampler = super::super::node::spawn_required(shutdown.clone(), None, async move {
                released.await.unwrap();
                assert!(!panic, "Injected readiness sampler panic");
                Ok(())
            });
            let task = tokio::spawn(run(
                listener,
                service.clone(),
                health.clone(),
                Arc::new(Semaphore::new(4)),
                shutdown,
                sampler,
            ));
            assert!(http(address, "/readyz").await.starts_with("HTTP/1.1 200"));
            release.send(()).unwrap();
            assert!(
                timeout_at(deadline(), task)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_err()
            );
            assert!(!health.ready.load(Ordering::Acquire));
            assert_eq!(
                service.process.phase(),
                super::super::process::Phase::Failed
            );
            assert!(
                http(management, "/readyz")
                    .await
                    .starts_with("HTTP/1.1 503")
            );
            let rebound = TcpListener::bind(address).await.unwrap();
            drop(rebound);
            let node = fixture.running.remove(&1).unwrap();
            assert!(node.shutdown().await.is_err());
            fixture.close().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelling_management_aborts_its_owned_sampler_and_releases_the_listener() {
        let fixture = fixture().await;
        let service = fixture.running.get(&1).unwrap().service.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_stop, shutdown) = watch::channel(false);
        let (sampler, alive) = pending_sampler(shutdown.clone());
        let task = tokio::spawn(run(
            listener,
            service,
            Arc::new(Health {
                ready: AtomicBool::new(true),
                snapshot: RwLock::new(Value::Null),
            }),
            Arc::new(Semaphore::new(4)),
            shutdown,
            sampler,
        ));
        assert!(http(address, "/readyz").await.starts_with("HTTP/1.1 200"));
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        timeout_at(deadline(), async {
            while alive.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let rebound = TcpListener::bind(address).await.unwrap();
        drop(rebound);
        fixture.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropping_unpolled_management_cancels_sampler_and_releases_listener() {
        let fixture = fixture().await;
        let service = fixture.running.get(&1).unwrap().service.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_stop, shutdown) = watch::channel(false);
        let (sampler, alive) = pending_sampler(shutdown.clone());
        let pending = run(
            listener,
            service,
            Arc::new(Health {
                ready: AtomicBool::new(true),
                snapshot: RwLock::new(Value::Null),
            }),
            Arc::new(Semaphore::new(4)),
            shutdown,
            sampler,
        );
        drop(pending);
        timeout_at(deadline(), async {
            while alive.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(TcpListener::bind(address).await.unwrap());
        fixture.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn caller_deadline_bounds_sampler_drain_without_another_time_budget() {
        let fixture = fixture().await;
        let service = fixture.running.get(&1).unwrap().service.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, shutdown) = watch::channel(false);
        let (sampler, alive) = pending_sampler(shutdown.clone());
        let worker = super::super::node::spawn_required(
            shutdown.clone(),
            None,
            run(
                listener,
                service.clone(),
                Arc::new(Health {
                    ready: AtomicBool::new(true),
                    snapshot: RwLock::new(Value::Null),
                }),
                Arc::new(Semaphore::new(4)),
                shutdown,
                sampler,
            ),
        );
        let observed = super::super::node::observe_required("management_probe", worker, service);
        assert!(http(address, "/readyz").await.starts_with("HTTP/1.1 200"));
        stop.send(true).unwrap();
        let until = Instant::now() + Duration::from_millis(100);
        assert!(timeout_at(until, observed).await.is_err());
        timeout_at(deadline(), async {
            while alive.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(TcpListener::bind(address).await.unwrap());
        fixture.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn combined_stays_recovering_until_its_data_initialization_is_applied() {
        let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        let config = fixture.configs.get(&1).unwrap().clone();
        let directory = config.data_dir.join("group-2");
        let hook = crate::execution::LocalExecutionHook::new(&directory, "before_durable", false);
        let node = crate::runtime::RunningNode::start(config).await.unwrap();
        let service = node.service.clone();
        fixture.running.insert(1, node);
        hook.entered().await;
        timeout_at(deadline(), async {
            while !service.auth_ready(deadline()).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let previous = service.runtime_metrics.0.lock().unwrap().sampled_at_ms;
        timeout_at(deadline(), async {
            loop {
                let observed = service.runtime_metrics.0.lock().unwrap().sampled_at_ms;
                if observed.is_some() && observed != previous {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let node = fixture.running.get(&1).unwrap();
        assert_eq!(
            service.process.phase(),
            super::super::process::Phase::Recovering
        );
        assert!(
            http(node.management_address, "/readyz")
                .await
                .starts_with("HTTP/1.1 503")
        );
        assert!(
            service
                .data
                .get(&2)
                .unwrap()
                .state
                .try_with_application(|state| state.config().is_none())
                .await
                .unwrap()
        );
        hook.release();
        fixture.wait_ready().await;
        assert_eq!(service.process.phase(), super::super::process::Phase::Ready);
        fixture.close().await;
    }

    #[test]
    fn ownership_path_errors_retain_the_invalid_component() {
        let topic = "f098986a-ecf4-4b23-b9ca-1475d2c51d75";
        assert!(
            ownership_key(&format!("/other/{topic}/0"))
                .unwrap_err()
                .to_string()
                .contains("must start with /ownership/")
        );
        assert!(
            ownership_key("/ownership/invalid/0")
                .unwrap_err()
                .to_string()
                .contains("invalid topic identity")
        );
        assert!(
            ownership_key(&format!("/ownership/{topic}/invalid"))
                .unwrap_err()
                .to_string()
                .contains("invalid partition number")
        );
    }
    #[test]
    fn ownership_lookup_accepts_one_bounded_partition_key() {
        let topic = "f098986a-ecf4-4b23-b9ca-1475d2c51d75";
        let key = ownership_key(&format!("/ownership/{topic}/4294967295")).unwrap();
        assert_eq!(key.topic_id.to_string(), topic);
        assert_eq!(key.partition, u32::MAX);
        for suffix in ["", "-1", "+1", "00", "4294967296", "0/1", "0?x=1"] {
            assert!(ownership_key(&format!("/ownership/{topic}/{suffix}")).is_err());
        }
        assert!(ownership_key("/ownership/invalid/0").is_err());
        assert!(ownership_key(&format!("/other/{topic}/0")).is_err());
    }

    #[test]
    fn management_parser_rejects_mutation_and_request_smuggling() {
        assert_eq!(
            parse_request(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap(),
            "/metrics"
        );
        assert!(parse_request(b"POST /diagnostics HTTP/1.1\r\n\r\n").is_err());
        assert!(
            parse_request(b"GET /metrics HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n").is_err()
        );
        assert!(parse_request(b"GET /metrics HTTP/1.1\r\nContent-Length: 1\r\n\r\na").is_err());
        assert!(
            parse_request(b"GET /metrics HTTP/1.1\r\n\r\nGET /diagnostics HTTP/1.1\r\n\r\n")
                .is_err()
        );
        assert!(parse_request(&vec![b'a'; 8193]).is_err());
    }
}
