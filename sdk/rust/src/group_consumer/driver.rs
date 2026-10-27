use super::credit::{Credit, Pool};
use super::*;
use crate::client::admission::Attempt;
use crate::message::{MessageId, ReceivedData};
mod scheduling;
#[cfg(test)]
mod tests;

pub(super) struct Domain {
    pub(super) shard: Id,
    pub(super) partition: u32,
    pub(super) sequence: u64,
    pub(super) assigned: bool,
    pub(super) busy: bool,
    pub(super) recovering: bool,
    pub(super) complete: bool,
    pub(super) empty_reads: u32,
    pub(super) next_poll: Instant,
    pub(super) next_fallback: Instant,
    pub(super) token: Bytes,
    pub(super) dirty: bool,
    pub(super) watching: Option<Id>,
    pub(super) next_watch: Instant,
    pub(super) observed_at: Instant,
    pub(super) failed_since: Option<Instant>,
    pub(super) read: Option<Arc<Mutex<ReadAttempt>>>,
    pub(super) _memory: tokio::sync::OwnedSemaphorePermit,
}

pub(super) struct ReadAttempt {
    request: public::GroupRead,
    operation: Id,
    applied: bool,
    retiring: bool,
    uncertain: bool,
    pool: Pool,
    credit: Option<Credit>,
    deadline: Instant,
    token: Bytes,
    progress: Option<ReadProgress>,
}

#[derive(Clone, Copy, Debug)]
struct ReadProgress {
    empty: bool,
    complete: bool,
}

pub(super) fn diagnostics(core: &Core) -> Result<ConsumerDiagnostics> {
    let state = core.state()?;
    let counts = scheduling::counts(&state)?;
    let (long_watches, immediate_probes) = watch::counts(&state);
    Ok(ConsumerDiagnostics {
        mode: core.credit.mode,
        ordinary_reads: counts.ordinary,
        protected_reads: counts.protected,
        ordinary_unknown: counts.ordinary_unknown,
        protected_unknown: counts.protected_unknown,
        recovery_queue: counts.recovery_queue,
        ordinary_rpc: (if core.credit.mode == ConsumerMode::Isolated {
            7_usize
        } else {
            1
        })
        .saturating_sub(core.credit.read_rpc.available_permits()),
        protected_rpc: usize::from(core.credit.mode == ConsumerMode::Isolated)
            .saturating_sub(core.credit.protected_rpc.available_permits()),
        recovery_rpc: (if core.credit.mode == ConsumerMode::Isolated {
            2_usize
        } else {
            1
        })
        .saturating_sub(core.credit.recovery_rpc.available_permits()),
        long_watches,
        immediate_probes,
        ready_targets: state
            .domains
            .values()
            .filter(|domain| domain.dirty && domain.assigned && !domain.complete)
            .count(),
        assigned_targets: state
            .domains
            .values()
            .filter(|domain| domain.assigned && !domain.complete)
            .count(),
        observed_targets: state
            .domains
            .values()
            .filter(|domain| domain.assigned && !domain.complete && !domain.token.is_empty())
            .count(),
        fallback_overdue: state
            .domains
            .values()
            .filter(|domain| {
                domain.assigned && !domain.complete && domain.next_fallback <= Instant::now()
            })
            .count(),
        observation_overdue: state
            .domains
            .values()
            .filter(|domain| {
                domain.assigned
                    && !domain.complete
                    && domain.observed_at.elapsed() >= Duration::from_secs(30)
            })
            .count(),
        protected_blocked: core.credit.mode == ConsumerMode::Isolated
            && (counts.protected > 0
                || core.credit.protected_slots.available_permits() == 0
                || core.credit.protected_bytes.available_permits() < core.credit.minimum as usize),
        ordinary_slots_free: core.credit.ordinary_slots.available_permits(),
        ordinary_bytes_free: core.credit.ordinary_bytes.available_permits(),
        protected_slots_free: core.credit.protected_slots.available_permits(),
        protected_bytes_free: core.credit.protected_bytes.available_permits(),
    })
}

pub(super) async fn run(core: Arc<Core>) {
    let heartbeat = tokio::spawn(heartbeats(core.clone()));
    let reads = tokio::spawn(scheduling::run(core.clone()));
    core.stop.cancelled().await;
    heartbeat.abort();
    let _ = heartbeat.await;
    let _ = reads.await;
    let result = cleanup(&core, None).await;
    ack::stop_tasks(&core).await;
    publish_close(&core, result);
}

pub(super) async fn finish(core: Arc<Core>, error: Option<Arc<Error>>) {
    let result = cleanup(&core, error).await;
    publish_close(&core, result);
}

fn publish_close(core: &Arc<Core>, mut result: CloseResult) {
    let fenced = match &result {
        Ok(report) => report.members_left > 0,
        Err(error) => error.report.members_left > 0,
    };
    if fenced {
        release_local(core);
    } else {
        if let Err(error) = &mut result {
            error.report.local_cleanup_complete = false;
        }
        let retained = core.clone();
        let until = core
            .lease_fence_after
            .lock()
            .map(|until| *until)
            .unwrap_or_else(|_| Instant::now() + Duration::from_secs(45));
        let task = tokio::spawn(async move {
            let closing = retained.client.closing_token();
            tokio::select! { _ = tokio::time::sleep_until(until) => {}, _ = closing.cancelled() => {} }
            release_local(&retained);
        });
        if let Ok(mut stored) = core.quarantine.lock() {
            *stored = Some(task);
        }
    }
    if let Ok(mut state) = core.state.lock() {
        state.closed = Some(result);
    }
    core.changed.notify_waiters();
}

fn release_local(core: &Core) {
    core.client.inner.admission.forget(core.member);
    if let Ok(mut state) = core.state.lock() {
        for message in state.ledger.values().chain(state.queue.iter()) {
            let _ = message.data.release_credit();
        }
        for (_, data) in &state.revoked {
            let _ = data.release_credit();
        }
        state.revoked.clear();
        state.queue = VecDeque::new();
        state.ledger.clear();
        state.domains.clear();
        state.watches.clear();
        state.ack_sequences.clear();
        state.claims.clear();
    }
    if let Ok(mut info) = core.info.lock() {
        info.work_shards = Vec::new();
        info.lanes = Vec::new();
        info.subscription_id = bytes(core.subscription);
    }
    if let Ok(mut memory) = core.resource_memory.lock() {
        memory.take();
    }
    if let Ok(mut shared) = core.credit.shared.lock() {
        shared.take();
    }
    core.changed.notify_waiters();
}

async fn cleanup(core: &Arc<Core>, mut failure: Option<Arc<Error>>) -> CloseResult {
    let deadline = core
        .close_deadline
        .lock()
        .ok()
        .and_then(|deadline| *deadline)
        .unwrap_or_else(|| Instant::now() + core.client.inner.config.close_timeout);
    ack::drain(core, deadline).await;
    if let Err(error) = core.credit.native.unregister(deadline).await {
        failure.get_or_insert_with(|| Arc::new(error));
    }
    let leave = leave(core, deadline).await;
    let mut report = CloseReport {
        local_cleanup_complete: true,
        ..CloseReport::default()
    };
    report.unresolved_watches = watch::cleanup(core, deadline).await;
    if report.unresolved_watches > 0 {
        failure.get_or_insert_with(|| {
            Arc::new(Error::Admission {
                resource: "remote watch cleanup remains unresolved",
            })
        });
    }
    if let Ok(state) = core.state() {
        for pending in state.pending.values() {
            if pending.is_complete() {
                report.unclaimed_results = report.unclaimed_results.saturating_add(1);
            } else {
                report.unresolved_acks = report.unresolved_acks.saturating_add(1);
            }
        }
    }
    if let Err(error) = leave {
        failure.get_or_insert_with(|| Arc::new(error));
    } else {
        report.members_left = 1;
        if let Err(error) = retire_pending_reads(core, deadline).await {
            failure.get_or_insert_with(|| Arc::new(error));
        } else if let Err(error) = retire_member(core, deadline).await {
            failure.get_or_insert_with(|| Arc::new(error));
        }
    }
    if let Ok(state) = core.state() {
        report.unresolved_reads = state
            .domains
            .values()
            .filter(|domain| domain.read.is_some())
            .count();
    }
    match failure {
        Some(source) => Err(CloseError { report, source }),
        None => Ok(report),
    }
}

async fn retire_member(core: &Core, deadline: Instant) -> Result<()> {
    let request = core.request(public::ConsumerAction::RetireConsumer);
    let operation = Id::new_v7()?;
    crate::request::retry_cleanup(deadline, || {
        control(&core.client, operation, &request, deadline)
    })
    .await?;
    Ok(())
}

async fn retire_pending_reads(core: &Core, deadline: Instant) -> Result<()> {
    let reads: Vec<_> = core
        .state()?
        .domains
        .iter()
        .filter_map(|(domain, state)| state.read.as_ref().map(|read| (*domain, read.clone())))
        .collect();
    for (domain, read) in reads {
        let (request, operation) = {
            let read = read.lock().map_err(|_| Error::Closed)?;
            (read.request.clone(), read.operation)
        };
        let request = public::GroupRetire {
            topic_id: request.topic_id,
            partition_id: request.partition_id,
            subscription_id: request.subscription_id,
            member_id: request.member_id,
            member_epoch: request.member_epoch,
            sequence: request.sequence,
        };
        let _: public::GroupRetired = crate::request::retry_cleanup(deadline, || {
            core.client.request(
                FrameKind::GroupRetireRead,
                FrameKind::GroupRetired,
                TrafficClass::Control,
                operation,
                &request,
                deadline,
            )
        })
        .await?;
        if let Some(state) = core.state()?.domains.get_mut(&domain) {
            state.read = None;
        }
    }
    Ok(())
}

async fn leave(core: &Core, deadline: Instant) -> Result<()> {
    let request = core.request(public::ConsumerAction::LeaveConsumer);
    let operation = Id::new_v7()?;
    loop {
        let status = match control(&core.client, operation, &request, deadline).await {
            Ok(status) => status,
            Err(error) => {
                let probe = core.request(public::ConsumerAction::ConsumerStatus);
                match control(&core.client, Id::new_v7()?, &probe, deadline).await {
                    Ok(status) if matches!(status.member_status, 4..=7) => status,
                    Ok(_) | Err(_)
                        if (error.outcome_unknown()
                            || error.retry_admission()
                            || error.retry_rejected())
                            && Instant::now() < deadline =>
                    {
                        pause_control(deadline).await?;
                        continue;
                    }
                    _ => return Err(error),
                }
            }
        };
        if matches!(status.member_status, 4..=7) && status.plan_active {
            return Ok(());
        }
        pause_control(deadline).await?;
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
        sequence: core
            .info
            .lock()
            .map(|info| info.next_heartbeat_sequence)
            .unwrap_or(1),
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
            let live = core
                .lease_until
                .lock()
                .is_ok_and(|until| *until > Instant::now());
            if terminal || !live || !no_work(&error) {
                core.record_error(error);
            }
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
    join: Option<JoinAttempt>,
    rejoins: u32,
}

async fn maintain_lease(core: &Core, lease: &mut LeaseDriver) -> Result<()> {
    let started = Instant::now();
    let upper = started
        + Duration::from_millis(core.client.inner.config.request_timeout_ms)
        + Duration::from_secs(15);
    {
        let mut fence_after = core.lease_fence_after.lock().map_err(|_| Error::Closed)?;
        *fence_after = (*fence_after).max(upper);
    }
    if let Some(joining) = &mut lease.join {
        let (status, observed) = core
            .client
            .operation(|deadline| join_control(&core.client, joining, deadline))
            .await?;
        core.epoch.store(status.member_epoch, Ordering::Release);
        lease.sequence = status.next_heartbeat_sequence;
        core.update_info(status, observed)?;
        lease.join = None;
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
            let observed = Instant::now();
            let status = core
                .client
                .operation(|deadline| control(&core.client, probe_operation, &probe, deadline))
                .await?;
            if status.member_status == 2
                && status.plan_active
                && status.next_heartbeat_sequence > lease.sequence
                && fresh_view(&status, observed)
            {
                lease.sequence = status.next_heartbeat_sequence;
                lease.operation = Id::new_v7()?;
                core.update_info(status, observed)?;
                return Ok(());
            }
            if matches!(status.member_status, 5 | 7) && status.plan_active {
                lease.rejoins = lease.rejoins.saturating_add(1);
                if lease.rejoins > 16 {
                    return Err(Error::SessionExpired);
                }
                // Metadata has observed every data-domain fence before this new join.
                core.epoch.store(0, Ordering::Release);
                let mut state = core.state()?;
                for message in state.ledger.values().chain(state.queue.iter()) {
                    message.data.release_credit()?;
                }
                for (_, data) in &state.revoked {
                    data.release_credit()?;
                }
                state.revoked.clear();
                state.queue.clear();
                state.ledger.clear();
                state.claims.clear();
                state.domains.clear();
                drop(state);
                let mut request = core.request(public::ConsumerAction::JoinConsumer);
                request.member_epoch = status.next_member_epoch;
                lease.join = Some(JoinAttempt {
                    operation: Id::new_v7()?,
                    request,
                    heartbeat: None,
                });
            }
            Err(error)
        }
    }
}

async fn poll_read(
    core: &Core,
    domain: Id,
    work: Arc<Mutex<ReadAttempt>>,
    permit: Attempt,
) -> Result<ReadProgress> {
    let (request, operation, applied, original_deadline, uncertain) = {
        let mut read = work.lock().map_err(|_| Error::Closed)?;
        read.retiring = read.applied;
        (
            read.request.clone(),
            read.operation,
            read.applied,
            read.deadline,
            read.uncertain,
        )
    };
    if Instant::now() >= original_deadline {
        core.record_error(Error::SessionExpired.preserve_uncertainty(true));
        core.stop.cancel();
        return Err(Error::SessionExpired.preserve_uncertainty(true));
    }
    let deadline = original_deadline.min(
        Instant::now()
            + Duration::from_millis(core.client.inner.config.request_timeout_ms.min(1000)),
    );
    let action = async {
        if !applied {
            let result: public::GroupDeliveries = core
                .client
                .request_reserved(
                    FrameKind::GroupRead,
                    FrameKind::GroupDeliveries,
                    TrafficClass::Delivery,
                    operation,
                    &request,
                    deadline,
                    &permit,
                )
                .await?;
            accept_read(core, domain, &work, &request, result)?;
        }
        let retire = public::GroupRetire {
            topic_id: request.topic_id.clone(),
            partition_id: request.partition_id,
            subscription_id: request.subscription_id.clone(),
            member_id: request.member_id.clone(),
            member_epoch: request.member_epoch,
            sequence: request.sequence,
        };
        // A fenced body can be discarded without setting applied, but retirement is still a mutation.
        work.lock().map_err(|_| Error::Closed)?.retiring = true;
        let _: public::GroupRetired = core
            .client
            .request_reserved(
                FrameKind::GroupRetireRead,
                FrameKind::GroupRetired,
                TrafficClass::Control,
                operation,
                &retire,
                deadline,
                &permit,
            )
            .await?;
        work.lock()
            .map_err(|_| Error::Closed)?
            .progress
            .ok_or(Error::Closed)
    };
    let result = tokio::time::timeout_at(deadline, action)
        .await
        .unwrap_or_else(|_| Err(Error::Deadline.preserve_uncertainty(true)))
        .map_err(|error| error.preserve_uncertainty(uncertain));
    if let Err(error) = &result {
        let mut read = work.lock().map_err(|_| Error::Closed)?;
        tracing::debug!(
            target: "tidemq_sdk::consumer_recovery",
            consumer = %core.member,
            operation = %operation,
            shard = %domain,
            member_epoch = request.member_epoch,
            sequence = request.sequence,
            phase = if read.retiring { "retire" } else { "read" },
            waiting_for_gate = !read.applied && !read.retiring && read_gate_pending(error),
            error_kind = ?error.kind(),
            outcome_unknown = error.outcome_unknown(),
            "Consumer original read attempt failed"
        );
        read.uncertain |= error.outcome_unknown()
            || matches!(
                error,
                Error::ResponseMismatch { .. } | Error::Protocol { .. }
            );
    }
    result
}

fn read_gate_pending(error: &Error) -> bool {
    match error {
        Error::RequestUnknown { source } => read_gate_pending(source),
        Error::Remote {
            code: tidemq_protocol::common::ErrorCode::AssignmentBusy,
            outcome: tidemq_protocol::common::Outcome::Rejected,
            ..
        } => true,
        _ => false,
    }
}

pub(super) fn no_work(error: &Error) -> bool {
    use tidemq_protocol::common::{ErrorCode, Outcome};
    // An unknown outcome preserves identity; it does not make a terminal cause retryable.
    match error {
        Error::Query { source } | Error::RequestUnknown { source } => no_work(source),
        Error::Deadline
        | Error::Admission { .. }
        | Error::Rpc {
            source: tidemq_transport::rpc::Error::Admission { .. },
        }
        | Error::Transport {
            source: tidemq_transport::Error::Admission { .. },
        } => true,
        Error::Remote {
            code,
            outcome,
            retryable,
            ..
        } => {
            *retryable
                && matches!(outcome, Outcome::Rejected | Outcome::Unknown)
                && matches!(
                    code,
                    ErrorCode::Unavailable
                        | ErrorCode::Fenced
                        | ErrorCode::AssignmentBusy
                        | ErrorCode::StaleGeneration
                        | ErrorCode::QuotaExceeded
                        | ErrorCode::ResultUnknown
                        | ErrorCode::DeadlineExceeded
                )
        }
        _ => error.retry_transport(),
    }
}

fn validate_deliveries(
    core: &Core,
    request: &public::GroupRead,
    result: &public::GroupDeliveries,
) -> Result<()> {
    if result.sequence != request.sequence
        || result.assignment_version == 0
        || result.deliveries.len() > request.max_messages as usize
    {
        return Err(Error::ResponseMismatch {
            reason: "read result differs from its original credit".into(),
        });
    }
    let mut total = 0_usize;
    let mut proofs = std::collections::BTreeSet::new();
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
        if let Some(proof) = &delivery.proof
            && !proofs.insert((proof.offset, proof.attempt))
        {
            return Err(Error::ResponseMismatch {
                reason: "read result repeats a delivery proof".into(),
            });
        }
        let charge = tidemq_protocol::topic::delivery_bytes(
            &delivery.key,
            &delivery.body,
            &delivery.properties,
        )?;
        if charge
            > u64::from(core.client.inner.config.protocol.max_message_bytes).saturating_add(256)
            || delivery.properties.len() > 128
            || delivery
                .properties
                .keys()
                .any(|name| name.is_empty() || name.len() > 256)
        {
            return Err(Error::ResponseMismatch {
                reason: "read delivery exceeds the message limits".into(),
            });
        }
        total = total
            .checked_add(usize::try_from(charge).map_err(|_| Error::Admission {
                resource: "delivery bytes",
            })?)
            .ok_or(Error::Admission {
                resource: "delivery bytes",
            })?;
    }
    if total as u64 > request.max_bytes {
        return Err(Error::ResponseMismatch {
            reason: "read result exceeds its original byte credit".into(),
        });
    }
    Ok(())
}

fn accept_read(
    core: &Core,
    domain: Id,
    work: &Mutex<ReadAttempt>,
    request: &public::GroupRead,
    result: public::GroupDeliveries,
) -> Result<()> {
    validate_deliveries(core, request, &result)?;
    let mut state = core.state()?;
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
    if !state.domains.get(&domain).is_some_and(|domain| {
        domain
            .read
            .as_ref()
            .is_some_and(|read| std::ptr::eq(Arc::as_ptr(read), work))
    }) {
        return Err(Error::StaleDelivery);
    }
    let info = core.info.lock().map_err(|_| Error::Closed)?;
    let mut credit = attempt.credit.take().ok_or(Error::Closed)?;
    let progress = ReadProgress {
        empty: result.deliveries.is_empty(),
        complete: result.sealed_complete,
    };
    for delivery in result.deliveries {
        if !delivery.current_attempt {
            continue;
        }
        let message = materialize(
            core,
            domain,
            request,
            result.assignment_version,
            delivery,
            &mut credit,
        )?;
        state.enqueue(core.model, &info, message)?;
    }
    state.release_revoked(&info)?;
    attempt.applied = true;
    attempt.progress = Some(progress);
    drop(state);
    core.changed.notify_waiters();
    Ok(())
}

fn materialize(
    core: &Core,
    domain: Id,
    request: &public::GroupRead,
    assignment_version: u64,
    delivery: public::GroupDelivery,
    credit: &mut Credit,
) -> Result<ReceivedMessage> {
    let length = usize::try_from(tidemq_protocol::topic::delivery_bytes(
        &delivery.key,
        &delivery.body,
        &delivery.properties,
    )?)
    .ok()
    .and_then(|bytes| bytes.checked_add(256))
    .ok_or(Error::Admission {
        resource: "delivery bytes",
    })?;
    let mut proof = delivery.proof.ok_or(Error::Closed)?;
    proof.mac = Bytes::copy_from_slice(&proof.mac);
    let data = ReceivedData {
        _protected: credit.protected.clone(),
        _charge: core.client.inner.metrics.charge(
            u32::try_from(length).map_err(|_| Error::Admission {
                resource: "retained consumer bytes",
            })?,
            true,
        ),
        id: MessageId {
            topic: core.topic_id,
            shard: domain,
            offset: proof.offset,
        },
        hash: delivery.key_hash,
        assignment_version,
        acknowledgement: std::sync::OnceLock::new(),
        key: Bytes::copy_from_slice(&delivery.key),
        key_present: delivery.key_present,
        properties: delivery
            .properties
            .into_iter()
            .map(|(name, value)| (name, Bytes::copy_from_slice(&value)))
            .collect(),
        payload: Bytes::copy_from_slice(&delivery.body),
        proof,
        member: core.member,
        epoch: request.member_epoch,
        consumer: core.member,
        partition: request.partition_id,
        _memory: credit.memory.split(length).ok_or(Error::Closed)?,
        _client_memory: credit.client.split(length).ok_or(Error::Closed)?,
        slot: Mutex::new(Some(credit.slots.split(1).ok_or(Error::Closed)?)),
    };
    Ok(ReceivedMessage {
        data: Arc::new(data),
        delivery_sequence: 0,
    })
}
