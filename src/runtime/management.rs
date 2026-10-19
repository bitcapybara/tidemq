use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};
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
    mut shutdown: watch::Receiver<bool>,
) -> Result<(std::net::SocketAddr, JoinHandle<Result<()>>)> {
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
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        let sampler_service = service.clone();
        let sampler_health = health.clone();
        let sampler_shutdown = shutdown.clone();
        let sampler = tokio::spawn(sample(sampler_service, sampler_health, sampler_shutdown));
        loop {
            while connections.try_join_next().is_some() {}
            tokio::select! {
                _ = shutdown.changed() => break,
                accepted = listener.accept() => {
                    let (stream, peer) = accepted.map_err(|source| Error::operation("accept management connection", source))?;
                    if !peer.ip().is_loopback() { continue; }
                    let Ok(permit) = slots.clone().try_acquire_owned() else { continue; };
                    let service = service.clone(); let health = health.clone();
                    connections.spawn(async move { let _permit = permit; let _ = serve(stream, service, health).await; });
                }
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        sampler.abort();
        let _ = sampler.await;
        Ok(())
    });
    Ok((address, task))
}

async fn sample(service: Arc<Service>, health: Arc<Health>, mut shutdown: watch::Receiver<bool>) {
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
            && service
                .authority
                .query(Query::TopicName("__readiness".into()), deadline)
                .await
                .is_ok();
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
        for replica in service.data.values() {
            if ready && service.require_leader(replica).is_ok() {
                ready = tokio::time::timeout_at(deadline, replica.raft.ensure_linearizable())
                    .await
                    .is_ok_and(|result| result.is_ok());
            }
        }
        health.ready.store(ready, Ordering::Release);
        let logging = service
            .log_health
            .lock()
            .ok()
            .and_then(|health| health.clone());
        let sampled = super::telemetry::Snapshot {
            ready,
            process_memory: memory,
            log_dropped_events: logging.as_ref().map(|health| health.dropped_events()),
            log_sink_errors: logging
                .as_ref()
                .map(|health| health.sink_errors.load(Ordering::Relaxed)),
            log_oversized_events: logging
                .as_ref()
                .map(|health| health.oversized_events.load(Ordering::Relaxed)),
        };
        if let Ok(mut metrics) = service.runtime_metrics.0.lock() {
            *metrics = sampled.clone();
        }
        let allocator = crate::allocator::snapshot().await.ok();
        let metadata = if let Some(replica) = &service.metadata {
            Some(
                replica
                    .state
                    .with_application(crate::metadata::MetadataState::summary)
                    .await,
            )
        } else {
            None
        };
        let mut groups = Vec::new();
        for (group, replica) in &service.data {
            let mut native = {
                let metrics = replica.raft.metrics();
                let metrics = metrics.borrow();
                serde_json::json!({"group_id":group,"leader":metrics.current_leader,"term":metrics.current_term,"last_applied":metrics.last_applied,"voters":metrics.membership_config.membership().voter_ids().take(4).collect::<Vec<_>>(),"joint_configs":metrics.membership_config.membership().get_joint_config(),"bootstrap_voters":replica.voters})
            };
            let storage = timeout_at(deadline, replica.log.diagnostics()).await.ok();
            if let Some(fields) = native.as_object_mut() {
                fields.insert("storage".into(), serde_json::json!(storage));
            }
            groups.push(native);
        }
        let metadata_storage = if let Some(replica) = &service.metadata {
            timeout_at(deadline, replica.log.diagnostics()).await.ok()
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
        let transport = service
            .transport_metrics
            .iter()
            .map(|metrics| metrics.snapshot())
            .collect::<Vec<_>>();
        *health.snapshot.write().await = serde_json::json!({"version":env!("CARGO_PKG_VERSION"),"scope":"current_process_local_applied","node_id":service.config.node_id,"cluster_id":service.cluster,"role":service.config.role,"ready":ready,"draining":service.draining.load(Ordering::Acquire),"allocator":allocator,"process":sampled,"metadata":metadata,"metadata_storage":metadata_storage,"data_groups":groups,"cached_routes":service.routes.read().await.len(),"capacity":capacity,"quota":quota,"transport":transport,"placement_capacity_available":placement_available,"placement":placement_diagnostic});
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
        let diagnostics = replica.log.diagnostics().await;
        reserved_storage_bytes = reserve_growth(reserved_storage_bytes, &diagnostics)?;
        groups.push((*group, diagnostics.snapshot_log.is_some()));
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
        let diagnostics = metadata.log.diagnostics().await;
        reserved_storage_bytes = reserve_growth(reserved_storage_bytes, &diagnostics)?;
        groups.push((
            service.config.runtime.metadata_group,
            diagnostics.snapshot_log.is_some(),
        ));
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

fn reserve_growth(
    previous: u64,
    diagnostics: &crate::replication::StorageDiagnostics,
) -> Result<u64> {
    // One live and one atomic-replacement journal/snapshot can coexist within fixed replica bounds.
    diagnostics
        .journal_file_limit_bytes
        .checked_add(diagnostics.snapshot_payload_limit_bytes as u64)
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| previous.checked_add(bytes))
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
            let ready = health.ready.load(Ordering::Acquire);
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
        Ok(_) => (404, "application/json", "{\"error\":\"Not found\"}".into()),
        Err(_) => (
            400,
            "application/json",
            "{\"error\":\"A bounded GET request without a body is required\"}".into(),
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
