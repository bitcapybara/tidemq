use super::*;
use crate::message::{MessageId, ReceivedData};
use tokio::{sync::OwnedSemaphorePermit, task::JoinSet};

pub(super) struct Domain {
    shard: Id,
    partition: u32,
    sequence: u64,
    assigned: bool,
    busy: bool,
    complete: bool,
    empty_reads: u32,
    next_poll: Instant,
    read: Option<Arc<Mutex<ReadAttempt>>>,
}

struct Credit {
    slots: OwnedSemaphorePermit,
    memory: OwnedSemaphorePermit,
    client: OwnedSemaphorePermit,
}
struct ReadAttempt {
    request: public::GroupRead,
    operation: Id,
    applied: bool,
    credit: Option<Credit>,
    created: Instant,
    progress: Option<ReadProgress>,
}

#[derive(Clone, Copy)]
struct ReadProgress {
    empty: bool,
    complete: bool,
}

pub(super) async fn run(core: Arc<Core>) {
    let heartbeat = tokio::spawn(heartbeats(core.clone()));
    let reads = tokio::spawn(reads(core.clone()));
    core.stop.cancelled().await;
    let result = cleanup(&core, None).await;
    heartbeat.abort();
    reads.abort();
    let _ = heartbeat.await;
    let _ = reads.await;
    ack::stop_tasks(&core).await;
    publish_close(&core, result);
}

pub(super) async fn finish(core: Arc<Core>, error: Option<Arc<Error>>) {
    let result = cleanup(&core, error).await;
    publish_close(&core, result);
}

fn publish_close(core: &Core, result: CloseResult) {
    if let Ok(mut state) = core.state.lock() {
        state.queue.clear();
        state.ledger.clear();
        state.domains.clear();
        state.closed = Some(result);
    }
    core.changed.notify_waiters();
}

async fn cleanup(core: &Core, original: Option<Arc<Error>>) -> CloseResult {
    let deadline = core
        .close_deadline
        .lock()
        .ok()
        .and_then(|deadline| *deadline)
        .unwrap_or_else(|| Instant::now() + core.client.inner.config.close_timeout);
    ack::drain(core, deadline).await;
    let request = core.request(public::ConsumerAction::LeaveConsumer);
    let leave = match Id::new_v7() {
        Ok(operation) => control(&core.client, operation, &request, deadline).await,
        Err(error) => Err(error.into()),
    };
    let mut report = CloseReport {
        local_cleanup_complete: true,
        ..CloseReport::default()
    };
    if let Ok(state) = core.state() {
        for pending in state.pending.values() {
            if pending.is_complete() {
                report.unclaimed_results = report.unclaimed_results.saturating_add(1);
            } else {
                report.unresolved_acks = report.unresolved_acks.saturating_add(1);
            }
        }
    }
    match leave {
        Ok(status) if status.member_status == 4 => {
            report.members_left = 1;
            if let Some(source) = original {
                Err(CloseError { report, source })
            } else {
                Ok(report)
            }
        }
        Ok(_) => Err(CloseError {
            report,
            source: Arc::new(Error::SessionExpired),
        }),
        Err(error) => Err(CloseError {
            report,
            source: original.unwrap_or_else(|| Arc::new(error)),
        }),
    }
}

async fn heartbeats(core: Arc<Core>) {
    let operation = match Id::new_v7() {
        Ok(operation) => operation,
        Err(error) => {
            core.record_error(error.into());
            core.stop.cancel();
            return;
        }
    };
    let mut lease = LeaseDriver {
        sequence: 1,
        operation,
        join: None,
        rejoins: 0,
    };
    let mut timer = tokio::time::interval(Duration::from_secs(3));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { _ = core.stop.cancelled() => return, _ = timer.tick() => {} }
        if let Err(error) = maintain_lease(&core, &mut lease).await {
            let terminal = error.permission_denied() || lease.rejoins > 16;
            core.record_error(error);
            if terminal {
                core.stop.cancel();
                return;
            }
        }
    }
}

struct LeaseDriver {
    sequence: u64,
    operation: Id,
    join: Option<(Id, public::ConsumerControl)>,
    rejoins: u32,
}

async fn maintain_lease(core: &Core, lease: &mut LeaseDriver) -> Result<()> {
    let started = Instant::now();
    if let Some((operation, request)) = &mut lease.join {
        let status = core
            .client
            .operation(|deadline| join_control(&core.client, operation, request, deadline))
            .await?;
        core.epoch.store(status.member_epoch, Ordering::Release);
        core.update_info(status, started)?;
        lease.join = None;
        lease.sequence = 1;
        lease.operation = Id::new_v7()?;
        return Ok(());
    }
    let mut request = core.request(public::ConsumerAction::HeartbeatConsumer);
    request.heartbeat_sequence = lease.sequence;
    match core
        .client
        .operation(|deadline| control(&core.client, lease.operation, &request, deadline))
        .await
    {
        Ok(status) => {
            core.update_info(status, started)?;
            lease.sequence = lease.sequence.checked_add(1).ok_or(Error::SessionExpired)?;
            lease.operation = Id::new_v7()?;
            Ok(())
        }
        Err(error) if error.permission_denied() => Err(error),
        Err(error) => {
            let probe = core.request(public::ConsumerAction::ConsumerStatus);
            let probe_operation = Id::new_v7()?;
            let status = core
                .client
                .operation(|deadline| control(&core.client, probe_operation, &probe, deadline))
                .await?;
            if status.member_status == 5 && status.plan_active {
                lease.rejoins = lease.rejoins.saturating_add(1);
                if lease.rejoins > 16 {
                    return Err(Error::SessionExpired);
                }
                // Metadata has observed every data-domain fence before this new join.
                core.epoch.store(0, Ordering::Release);
                let mut state = core.state()?;
                state.queue.clear();
                state.ledger.clear();
                state.claims.clear();
                state.domains.clear();
                drop(state);
                let mut request = core.request(public::ConsumerAction::JoinConsumer);
                request.member_epoch = status.next_member_epoch;
                lease.join = Some((Id::new_v7()?, request));
            }
            Err(error)
        }
    }
}

async fn reads(core: Arc<Core>) {
    let mut jobs: JoinSet<(Id, u64, Result<ReadProgress>)> = JoinSet::new();
    let mut timer = tokio::time::interval(Duration::from_millis(25));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut refreshed = Instant::now()
        .checked_sub(Duration::from_secs(5))
        .unwrap_or_else(Instant::now);
    loop {
        tokio::select! {
            _ = core.stop.cancelled() => return,
            _ = timer.tick() => {},
            result = jobs.join_next(), if !jobs.is_empty() => {
                match result {
                    Some(Ok((domain, epoch, result))) => {
                        if epoch == core.epoch.load(Ordering::Acquire) && let Ok(mut state) = core.state() && let Some(record) = state.domains.get_mut(&domain) {
                            record.busy = false;
                            match &result {
                                Ok(progress) => {
                                    record.read = None; record.sequence = record.sequence.saturating_add(1); record.complete = progress.complete;
                                    record.empty_reads = if progress.empty { record.empty_reads.saturating_add(1).min(6) } else { 0 };
                                    record.next_poll = Instant::now() + Duration::from_millis(10_u64 << record.empty_reads);
                                }
                                Err(_) => record.next_poll = Instant::now() + Duration::from_millis(250),
                            }
                        }
                        if let Err(error) = result && !no_work(&error) { core.record_error(error); }
                    }
                    Some(Err(_)) => { core.record_error(Error::Closed); core.stop.cancel(); return; }
                    None => {},
                }
            }
        }
        if refreshed.elapsed() >= Duration::from_secs(1) {
            match refresh_domains(&core).await {
                Ok(()) => refreshed = Instant::now(),
                Err(error) => {
                    core.record_error(error);
                    continue;
                }
            }
        }
        while jobs.len() < 8 {
            match next_read(&core) {
                Ok(Some((domain, read))) => {
                    let core = core.clone();
                    let epoch = read
                        .lock()
                        .map(|read| read.request.member_epoch)
                        .unwrap_or(0);
                    jobs.spawn(
                        async move { (domain, epoch, poll_read(&core, domain, read).await) },
                    );
                }
                Ok(None) => break,
                Err(error) => {
                    core.record_error(error);
                    break;
                }
            }
        }
    }
}

async fn refresh_domains(core: &Core) -> Result<()> {
    let layout = core.client.admin().topic_info(&core.topic).await?;
    let work = core
        .info
        .lock()
        .map_err(|_| Error::Closed)?
        .work_shards
        .clone();
    let mut state = core.state()?;
    for domain in state.domains.values_mut() {
        domain.assigned = work.iter().any(|id| id.as_ref() == domain.shard.as_bytes());
    }
    for shard in layout.wire.active.iter().chain(&layout.wire.history) {
        let id = Id::from_slice(&shard.shard_id)?;
        let assigned = work.iter().any(|domain| domain == &shard.shard_id);
        state.domains.entry(id).or_insert(Domain {
            shard: id,
            partition: shard.birth_partition,
            sequence: 1,
            assigned,
            busy: false,
            complete: false,
            empty_reads: 0,
            next_poll: Instant::now(),
            read: None,
        });
    }
    Ok(())
}

fn reserve(core: &Core) -> Result<Option<Credit>> {
    let messages = core.slots.available_permits().min(32);
    let maximum = core
        .client
        .inner
        .config
        .protocol
        .max_frame_bytes
        .saturating_sub(8192) as usize;
    let bytes = core
        .memory
        .available_permits()
        .min(core.client.inner.bytes.available_permits())
        .min(maximum);
    if messages == 0 || bytes < 1024 {
        return Ok(None);
    }
    let slots = match core.slots.clone().try_acquire_many_owned(messages as u32) {
        Ok(permit) => permit,
        Err(_) => return Ok(None),
    };
    let memory = match core.memory.clone().try_acquire_many_owned(bytes as u32) {
        Ok(permit) => permit,
        Err(_) => return Ok(None),
    };
    let client = match core
        .client
        .inner
        .bytes
        .clone()
        .try_acquire_many_owned(bytes as u32)
    {
        Ok(permit) => permit,
        Err(_) => return Ok(None),
    };
    Ok(Some(Credit {
        slots,
        memory,
        client,
    }))
}

fn next_read(core: &Core) -> Result<Option<(Id, Arc<Mutex<ReadAttempt>>)>> {
    let lease_valid = *core.lease_until.lock().map_err(|_| Error::Closed)? > Instant::now()
        && core.epoch.load(Ordering::Acquire) > 0;
    let mut state = core.state()?;
    let eligible = |domain: &&Domain| {
        !domain.busy
            && !domain.complete
            && domain.next_poll <= Instant::now()
            && (domain.assigned && lease_valid || domain.read.is_some())
    };
    let selected = state
        .domains
        .values()
        .filter(eligible)
        .find(|domain| state.read_after.is_none_or(|after| domain.shard > after))
        .or_else(|| state.domains.values().find(eligible))
        .map(|domain| domain.shard);
    let Some(selected) = selected else {
        return Ok(None);
    };
    state.read_after = Some(selected);
    let domain = state.domains.get_mut(&selected).ok_or(Error::Closed)?;
    if domain.sequence == u64::MAX {
        return Err(Error::SessionExpired);
    }
    if domain.read.is_none() {
        let Some(credit) = reserve(core)? else {
            return Ok(None);
        };
        let request = public::GroupRead {
            topic_id: bytes(core.topic_id),
            partition_id: domain.partition,
            subscription_id: bytes(core.subscription),
            member_id: bytes(core.member),
            member_epoch: core.epoch.load(Ordering::Acquire),
            sequence: domain.sequence,
            max_messages: credit.slots.num_permits() as u32,
            max_bytes: credit.memory.num_permits() as u64,
        };
        domain.read = Some(Arc::new(Mutex::new(ReadAttempt {
            request,
            operation: Id::new_v7()?,
            applied: false,
            credit: Some(credit),
            created: Instant::now(),
            progress: None,
        })));
    }
    let read = domain.read.as_ref().ok_or(Error::Closed)?.clone();
    domain.busy = true;
    Ok(Some((domain.shard, read)))
}

async fn poll_read(core: &Core, domain: Id, work: Arc<Mutex<ReadAttempt>>) -> Result<ReadProgress> {
    let (request, operation, applied) = {
        let work = work.lock().map_err(|_| Error::Closed)?;
        if work.created.elapsed() > Duration::from_secs(600) {
            core.stop.cancel();
            return Err(Error::SessionExpired);
        }
        (work.request.clone(), work.operation, work.applied)
    };
    core.client
        .operation(|deadline| async move {
            if !applied {
                let result: public::GroupDeliveries = core
                    .client
                    .request(
                        FrameKind::GroupRead,
                        FrameKind::GroupDeliveries,
                        TrafficClass::Delivery,
                        operation,
                        &request,
                        deadline,
                    )
                    .await?;
                accept_read(core, domain, &work, &request, result)?;
            }
            let retire = public::GroupRetire {
                topic_id: request.topic_id,
                partition_id: request.partition_id,
                subscription_id: request.subscription_id,
                member_id: request.member_id,
                member_epoch: request.member_epoch,
                sequence: request.sequence,
            };
            let _: public::GroupRetired = core
                .client
                .request(
                    FrameKind::GroupRetireRead,
                    FrameKind::GroupRetired,
                    TrafficClass::Control,
                    operation,
                    &retire,
                    deadline,
                )
                .await?;
            work.lock()
                .map_err(|_| Error::Closed)?
                .progress
                .ok_or(Error::Closed)
        })
        .await
}

fn no_work(error: &Error) -> bool {
    matches!(
        error,
        Error::Remote {
            code: tidemq_protocol::common::ErrorCode::AssignmentBusy
                | tidemq_protocol::common::ErrorCode::QuotaExceeded,
            outcome: tidemq_protocol::common::Outcome::Rejected,
            ..
        }
    )
}

fn accept_read(
    core: &Core,
    domain: Id,
    work: &Mutex<ReadAttempt>,
    request: &public::GroupRead,
    result: public::GroupDeliveries,
) -> Result<()> {
    if result.sequence != request.sequence
        || result.deliveries.len() > request.max_messages as usize
    {
        return Err(Error::ResponseMismatch {
            reason: "read result differs from its original credit".into(),
        });
    }
    let mut total = 0_usize;
    for delivery in &result.deliveries {
        if delivery
            .proof
            .as_ref()
            .is_none_or(|proof| proof.mac.len() != 32)
        {
            return Err(Error::ResponseMismatch {
                reason: "read result omitted a valid confirmation proof".into(),
            });
        }
        total = total
            .checked_add(delivery.key.len())
            .and_then(|bytes| bytes.checked_add(delivery.body.len()))
            .and_then(|bytes| bytes.checked_add(256))
            .ok_or(Error::Admission {
                resource: "delivery bytes",
            })?;
    }
    if total as u64 > request.max_bytes {
        return Err(Error::ResponseMismatch {
            reason: "read result exceeds its original byte credit".into(),
        });
    }
    let mut attempt = work.lock().map_err(|_| Error::Closed)?;
    if attempt.applied {
        return Ok(());
    }
    if request.member_epoch != core.epoch.load(Ordering::Acquire) {
        attempt.applied = true;
        attempt.credit = None;
        attempt.progress = Some(ReadProgress {
            empty: true,
            complete: false,
        });
        return Ok(());
    }
    let mut credit = attempt.credit.take().ok_or(Error::Closed)?;
    let progress = ReadProgress {
        empty: result.deliveries.is_empty(),
        complete: result.sealed_complete,
    };
    let mut state = core.state()?;
    for delivery in result.deliveries {
        if !delivery.current_attempt {
            continue;
        }
        let length = delivery
            .key
            .len()
            .saturating_add(delivery.body.len())
            .saturating_add(256);
        let proof = delivery.proof.ok_or(Error::Closed)?;
        let data = ReceivedData {
            id: MessageId {
                topic: core.topic_id,
                shard: domain,
                offset: proof.offset,
            },
            hash: delivery.key_hash,
            key: Bytes::copy_from_slice(&delivery.key),
            payload: Bytes::copy_from_slice(&delivery.body),
            proof,
            member: core.member,
            epoch: request.member_epoch,
            consumer: core.member,
            partition: request.partition_id,
            _memory: credit.memory.split(length).ok_or(Error::Closed)?,
            _client_memory: credit.client.split(length).ok_or(Error::Closed)?,
            _slot: credit.slots.split(1).ok_or(Error::Closed)?,
        };
        state.queue.push_back(ReceivedMessage {
            data: Arc::new(data),
            delivery_sequence: 0,
        });
    }
    attempt.applied = true;
    attempt.progress = Some(progress);
    drop(state);
    core.changed.notify_waiters();
    Ok(())
}
