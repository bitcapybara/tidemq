use super::*;
use crate::client::admission::Lane;
use tokio::task::JoinSet;

enum Event {
    Read(Id, Arc<Mutex<ReadAttempt>>, Result<ReadProgress>),
    Watch(Id, u64, Result<public::GroupWatchResult>),
    Layout(Result<()>),
}

pub(super) async fn run(core: Arc<Core>) {
    let mut jobs = JoinSet::new();
    let mut timer = tokio::time::interval(Duration::from_millis(10));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut refresh = Instant::now();
    let mut refreshing = false;
    let mut layout_failed_since = None;
    loop {
        tokio::select! {
            _ = core.stop.cancelled() => break,
            _ = core.changed.notified() => {},
            _ = timer.tick() => {},
            event = jobs.join_next(), if !jobs.is_empty() => {
                match event {
                    Some(Ok(Event::Read(id, read, result))) => {
                        if let Err(error) = finish_read(&core, id, &read, result) { report_error(&core, error); }
                    }
                    Some(Ok(Event::Watch(id, attempt, result))) => {
                        if let Err(error) = watch::finish(&core, id, attempt, result) { report_error(&core, error); }
                    }
                    Some(Ok(Event::Layout(result))) => {
                        refreshing = false;
                        if let Err(error) = result {
                            if no_work(&error) {
                                let since = *layout_failed_since.get_or_insert(Instant::now());
                                if let Err(error) = report_stall(&core, since) { report_error(&core, error); }
                                tracing::debug!(
                                    target: "tidemq_sdk::consumer_recovery",
                                    consumer = %core.member,
                                    error_kind = ?error.kind(),
                                    "Consumer layout hint will retry on the bounded refresh schedule"
                                );
                            } else {
                                report_error(&core, error);
                            }
                        } else {
                            layout_failed_since = None;
                        }
                    }
                    Some(Err(error)) => { core.record_error(Error::Task { source: error }); core.stop.cancel(); }
                    None => {},
                }
            }
        }
        if !refreshing && Instant::now() >= refresh {
            refreshing = true;
            refresh = Instant::now() + Duration::from_secs(1);
            let worker = core.clone();
            jobs.spawn(async move { Event::Layout(refresh_domains(&worker).await) });
        }
        if let Err(error) = schedule(&core, &mut jobs) {
            report_error(&core, error);
        }
    }
    jobs.abort_all();
    while jobs.join_next().await.is_some() {}
    core.client.inner.admission.forget(core.member);
}

fn report_error(core: &Core, error: Error) {
    let terminal =
        error.permission_denied() || matches!(error, Error::SessionExpired | Error::TopicReplaced);
    core.record_error(error);
    if terminal {
        core.stop.cancel();
    }
}

fn schedule(core: &Arc<Core>, jobs: &mut JoinSet<Event>) -> Result<()> {
    enqueue_fallbacks(core)?;
    for recovery in [true, false] {
        for _ in 0..8 {
            let Some((id, read, attempt)) = next_read(core, recovery)? else {
                break;
            };
            let worker = core.clone();
            jobs.spawn(async move {
                let result = poll_read(&worker, id, read.clone(), attempt).await;
                Event::Read(id, read, result)
            });
        }
    }
    for _ in 0..8 {
        let Some((id, request, operation, deadline, permit)) = watch::next(core)? else {
            break;
        };
        let worker = core.clone();
        jobs.spawn(async move {
            let attempt = request.attempt;
            let result = watch::poll(&worker, operation, &request, deadline, &permit).await;
            Event::Watch(id, attempt, result)
        });
    }
    report_saturation(core)
}

pub(super) async fn refresh_domains(core: &Core) -> Result<()> {
    let layout = core.client.admin().topic_info(&core.topic).await?;
    if layout.id != core.topic_id {
        return Err(Error::TopicReplaced);
    }
    let mut state = core.state()?;
    let info = core.info.lock().map_err(|_| Error::Closed)?;
    let now = Instant::now();
    for domain in state.domains.values_mut() {
        let assigned = info
            .work_shards
            .iter()
            .any(|id| id.as_ref() == domain.shard.as_bytes());
        if domain.assigned != assigned {
            domain.failed_since = None;
            if assigned {
                domain.observed_at = now;
                domain.next_fallback = now + fallback_delay(domain.shard);
                domain.next_poll = now;
                domain.dirty = true;
            }
        }
        domain.assigned = assigned;
    }
    // A temporary assignment loss must not reset a birth domain's retired Read sequence.
    state.domains.retain(|id, domain| {
        domain.read.is_some()
            || domain.watching.is_some()
            || layout
                .wire
                .active
                .iter()
                .chain(&layout.wire.history)
                .any(|shard| shard.shard_id.as_ref() == id.as_bytes())
    });
    for shard in layout.wire.active.iter().chain(&layout.wire.history) {
        let id = Id::from_slice(&shard.shard_id)?;
        if !info.work_shards.contains(&shard.shard_id) || state.domains.contains_key(&id) {
            continue;
        }
        let memory = core
            .client
            .inner
            .resource_memory
            .clone()
            .try_acquire_many_owned(512)
            .map_err(|_| Error::Admission {
                resource: "consumer target metadata",
            })?;
        state
            .domains
            .insert(id, Domain::new(id, shard.birth_partition, memory));
    }
    Ok(())
}

impl Domain {
    pub(super) fn new(
        shard: Id,
        partition: u32,
        memory: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self {
            shard,
            partition,
            sequence: 1,
            assigned: true,
            busy: false,
            recovering: false,
            complete: false,
            empty_reads: 0,
            next_poll: Instant::now(),
            next_fallback: Instant::now() + fallback_delay(shard),
            token: Bytes::new(),
            dirty: true,
            watching: None,
            next_watch: Instant::now(),
            observed_at: Instant::now(),
            failed_since: None,
            read: None,
            _memory: memory,
        }
    }
}

pub(super) fn fallback_delay(shard: Id) -> Duration {
    Duration::from_millis(30_000 + crate::client::affinity(&shard) % 1000)
}

pub(super) fn enqueue_fallbacks(core: &Core) -> Result<()> {
    let now = Instant::now();
    let mut state = core.state()?;
    for domain in state.domains.values_mut() {
        if domain.assigned && !domain.complete && domain.next_fallback <= now {
            domain.dirty = true;
        }
    }
    Ok(())
}

#[derive(Default)]
pub(super) struct Counts {
    pub ordinary: usize,
    pub protected: usize,
    pub ordinary_unknown: usize,
    pub protected_unknown: bool,
    pub recovery_queue: usize,
    pub normal_rpc: usize,
    pub protected_rpc: usize,
    pub recovery: usize,
}

pub(super) fn counts(state: &State) -> Result<Counts> {
    let mut counts = Counts::default();
    for domain in state.domains.values() {
        if let Some(read) = &domain.read {
            let read = read.lock().map_err(|_| Error::Closed)?;
            match read.pool {
                Pool::Ordinary => counts.ordinary += 1,
                Pool::Protected => counts.protected += 1,
            }
            if read.uncertain {
                if read.pool == Pool::Protected {
                    counts.protected_unknown = true;
                } else {
                    counts.ordinary_unknown += 1;
                }
            }
            if !domain.busy {
                counts.recovery_queue += 1;
            }
            if domain.busy {
                if domain.recovering {
                    counts.recovery += 1;
                }
                if read.pool == Pool::Protected && !domain.recovering {
                    counts.protected_rpc += 1;
                } else {
                    counts.normal_rpc += 1;
                }
            }
        }
    }
    Ok(counts)
}

type SelectedRead = (Id, Arc<Mutex<ReadAttempt>>, Attempt);

pub(super) fn next_read(core: &Core, recovery: bool) -> Result<Option<SelectedRead>> {
    let lease_live = *core.lease_until.lock().map_err(|_| Error::Closed)? > Instant::now()
        && core.epoch.load(Ordering::Acquire) > 0;
    let mut state = core.state()?;
    let epoch = core.info.lock().map_err(|_| Error::Closed)?.member_epoch;
    let live = lease_live && epoch > 0 && epoch == core.epoch.load(Ordering::Acquire);
    let counts = counts(&state)?;
    if recovery && (counts.recovery >= 2 || counts.normal_rpc >= 7) {
        core.client
            .inner
            .admission
            .withdraw(core.member, Lane::Recovery);
        return Ok(None);
    }
    let after = if recovery {
        state.recovery_after
    } else {
        state.read_after
    };
    let eligible = |domain: &&Domain| {
        !domain.busy
            && domain.next_poll <= Instant::now()
            && if recovery {
                domain.read.is_some()
            } else {
                live && domain.assigned && !domain.complete && domain.read.is_none() && domain.dirty
            }
    };
    let selected = state
        .domains
        .values()
        .filter(eligible)
        .find(|domain| after.is_none_or(|after| domain.shard > after))
        .or_else(|| state.domains.values().find(eligible))
        .map(|domain| domain.shard);
    let lane = if recovery { Lane::Recovery } else { Lane::Read };
    let Some(selected) = selected else {
        core.client.inner.admission.withdraw(core.member, lane);
        return Ok(None);
    };
    let contenders = state.domains.values().filter(eligible).count();
    let serial = core.credit.mode == ConsumerMode::Serial;
    if !recovery && serial && counts.ordinary + counts.protected > 0 {
        core.client
            .inner
            .admission
            .withdraw(core.member, Lane::Read);
        return Ok(None);
    }
    let domain = state.domains.get_mut(&selected).ok_or(Error::Closed)?;
    let reservation = core.client.inner.admission.fair(core.member, lane, || {
        reserve_read(core, domain, recovery, &counts, contenders)
    })?;
    let Some((credit, pool, attempt)) = reservation else {
        return Ok(None);
    };
    if let Some(credit) = credit {
        start_read(core, domain, credit, pool, epoch)?;
    }
    domain.busy = true;
    domain.recovering = recovery;
    let read = domain.read.as_ref().ok_or(Error::Closed)?.clone();
    if recovery {
        state.recovery_after = Some(selected);
    } else {
        state.read_after = Some(selected);
    }
    Ok(Some((selected, read, attempt)))
}

fn reserve_read(
    core: &Core,
    domain: &Domain,
    recovery: bool,
    counts: &Counts,
    contenders: usize,
) -> Result<Option<(Option<Credit>, Pool, Attempt)>> {
    let serial = core.credit.mode == ConsumerMode::Serial;
    if recovery {
        let pool = domain
            .read
            .as_ref()
            .ok_or(Error::Closed)?
            .lock()
            .map_err(|_| Error::Closed)?
            .pool;
        return Ok(core
            .credit
            .attempt(&core.client, pool, true, TrafficClass::Delivery)?
            .map(|attempt| (None, pool, attempt)));
    }
    for pool in [Pool::Ordinary, Pool::Protected] {
        if pool == Pool::Ordinary && counts.ordinary >= 8
            || pool == Pool::Protected
                && (serial || counts.protected > 0 || counts.protected_rpc > 0)
        {
            continue;
        }
        let Some(credit) = core.credit.reserve(&core.client, pool, contenders)? else {
            continue;
        };
        if pool == Pool::Ordinary
            && (counts.normal_rpc >= 7 || core.credit.read_rpc.available_permits() == 0)
        {
            // A temporary RPC ceiling must not spend protection before the eighth ordinary outcome is known.
            return Ok(None);
        }
        if let Some(attempt) =
            core.credit
                .attempt(&core.client, pool, false, TrafficClass::Delivery)?
        {
            return Ok(Some((Some(credit), pool, attempt)));
        }
    }
    Ok(None)
}

fn start_read(
    core: &Core,
    domain: &mut Domain,
    credit: Credit,
    pool: Pool,
    epoch: u64,
) -> Result<()> {
    domain
        .sequence
        .checked_add(1)
        .ok_or(Error::SessionExpired)?;
    let request = public::GroupRead {
        topic_id: bytes(core.topic_id),
        partition_id: domain.partition,
        subscription_id: bytes(core.subscription),
        member_id: bytes(core.member),
        member_epoch: epoch,
        sequence: domain.sequence,
        max_messages: credit.slots.num_permits() as u32,
        max_bytes: credit
            .memory
            .num_permits()
            .saturating_sub(credit.slots.num_permits().saturating_mul(256))
            as u64,
    };
    domain.read = Some(Arc::new(Mutex::new(ReadAttempt {
        request,
        operation: Id::new_v7()?,
        applied: false,
        retiring: false,
        uncertain: false,
        pool,
        credit: Some(credit),
        deadline: Instant::now() + Duration::from_secs(600),
        token: domain.token.clone(),
        progress: None,
    })));
    domain.dirty = false;
    domain.next_fallback = Instant::now() + fallback_delay(domain.shard);
    Ok(())
}

pub(super) fn finish_read(
    core: &Core,
    id: Id,
    work: &Arc<Mutex<ReadAttempt>>,
    result: Result<ReadProgress>,
) -> Result<()> {
    let mut state = core.state()?;
    let Some(domain) = state.domains.get_mut(&id).filter(|domain| {
        domain
            .read
            .as_ref()
            .is_some_and(|read| Arc::ptr_eq(read, work))
    }) else {
        return Ok(());
    };
    domain.busy = false;
    domain.recovering = false;
    match result {
        Ok(progress) => {
            let read = work.lock().map_err(|_| Error::Closed)?;
            // A change observed during the read or retirement must survive completion.
            domain.dirty |= !progress.empty || domain.token != read.token;
            drop(read);
            domain.read = None;
            domain.sequence = domain
                .sequence
                .checked_add(1)
                .ok_or(Error::SessionExpired)?;
            domain.failed_since = None;
            domain.complete = progress.complete;
            domain.empty_reads = if progress.empty {
                domain.empty_reads.saturating_add(1).min(6)
            } else {
                0
            };
            domain.next_poll = Instant::now()
                + Duration::from_millis(if progress.empty {
                    10_u64 << domain.empty_reads
                } else {
                    0
                });
        }
        Err(error) => {
            domain.next_poll = Instant::now() + Duration::from_millis(250);
            if domain.assigned {
                // A never-enqueued attempt can be replaced without erasing the domain's stall.
                domain.failed_since.get_or_insert(Instant::now());
            }
            let mut read = work.lock().map_err(|_| Error::Closed)?;
            if !read.applied && !read.uncertain && unsubmitted(&error) {
                // Native admission refused before enqueue; no original result can exist.
                domain.read = None;
                domain.dirty = true;
                drop(read);
                drop(state);
                if !no_work(&error) {
                    report_error(core, error);
                }
                return Ok(());
            }
            read.uncertain |= error.outcome_unknown();
            // AssignmentBusy from GroupRead means its successor gate is not open yet.
            let waiting_for_gate = !read.applied && !read.retiring && read_gate_pending(&error);
            drop(read);
            if !no_work(&error) && !waiting_for_gate {
                drop(state);
                report_error(core, error);
            }
        }
    }
    Ok(())
}

fn unsubmitted(error: &Error) -> bool {
    matches!(
        error,
        Error::Admission { .. }
            | Error::Rpc {
                source: tidemq_transport::rpc::Error::Admission { .. }
                    | tidemq_transport::rpc::Error::Deadline {
                        may_have_been_sent: false
                    }
            }
    )
}

pub(super) fn report_saturation(core: &Core) -> Result<()> {
    let mut state = core.state()?;
    let now = Instant::now();
    let overdue_progress = state
        .domains
        .values()
        .filter(|domain| domain.assigned && !domain.complete)
        .map(|domain| {
            domain
                .failed_since
                .map_or(domain.observed_at, |since| since.min(domain.observed_at))
        })
        .filter(|since| now.saturating_duration_since(*since) >= Duration::from_secs(30))
        .min();
    let blocked = overdue_progress.is_some()
        || state.domains.values().any(|domain| {
            domain.assigned
                && !domain.complete
                && (domain.dirty && domain.read.is_none()
                    || now.saturating_duration_since(domain.next_fallback)
                        >= Duration::from_secs(30))
                || domain.assigned
                    && !domain.complete
                    && domain.observed_at.elapsed() >= Duration::from_secs(30)
        });
    if !blocked {
        state.saturated_since = None;
        return Ok(());
    }
    let since = state.saturated_since.get_or_insert(now);
    if let Some(progress_at) = overdue_progress {
        *since = (*since).min(progress_at);
    }
    let since = *since;
    drop(state);
    report_stall(core, since)
}

pub(super) fn report_stall(core: &Core, since: Instant) -> Result<()> {
    let mut state = core.state()?;
    let now = Instant::now();
    if now.saturating_duration_since(since) < Duration::from_secs(30)
        || state
            .last_saturation_error
            .is_some_and(|last| now.saturating_duration_since(last) < Duration::from_secs(30))
    {
        return Ok(());
    }
    state.last_saturation_error = Some(now);
    drop(state);
    core.record_error(Error::Admission {
        resource: "consumer observation or read progress remains blocked",
    });
    Ok(())
}
