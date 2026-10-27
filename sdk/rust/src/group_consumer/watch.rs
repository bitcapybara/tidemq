use super::*;
use crate::client::admission::{Attempt, Lane};
use std::collections::BTreeSet;
use tidemq_protocol::watch::{MAX_TARGETS, MAX_TOKEN_BYTES, MAX_WAIT_MS};

pub(super) struct Watch {
    request: public::GroupWatch,
    operation: Id,
    deadline: Instant,
    active: bool,
    retry_at: Instant,
    failures: u32,
    _memory: tokio::sync::OwnedSemaphorePermit,
}

fn request(core: &Core, operation: Id, wait: bool) -> Result<public::GroupWatch> {
    let info = core.info.lock().map_err(|_| Error::Closed)?;
    let version = info.assignment_version;
    Ok(public::GroupWatch {
        topic_id: bytes(core.topic_id),
        subscription_id: bytes(core.subscription),
        member_id: bytes(core.member),
        member_epoch: info.member_epoch,
        assignment_version: version,
        attempt: 1,
        wait_ms: if wait {
            MAX_WAIT_MS.min(
                core.client
                    .inner
                    .config
                    .request_timeout_ms
                    .saturating_sub(100) as u32,
            )
        } else {
            0
        },
        targets: Vec::new(),
        root_operation_id: bytes(operation),
        parent_operation_id: Bytes::new(),
    })
}

pub(super) fn validate_minimum(client: &Client) -> Result<()> {
    let operation = Id::new_v7()?;
    let id = bytes(operation);
    let request = public::GroupWatch {
        topic_id: id.clone(),
        subscription_id: id.clone(),
        member_id: id.clone(),
        member_epoch: 1,
        assignment_version: 1,
        attempt: 1,
        wait_ms: 0,
        targets: vec![public::WatchTarget {
            shard_id: id.clone(),
            birth_partition: 0,
            observed_token: Bytes::from(vec![0; MAX_TOKEN_BYTES]),
        }],
        root_operation_id: id,
        parent_operation_id: Bytes::new(),
    };
    tidemq_protocol::watch::validate(&request, operation, &client.inner.config.protocol)?;
    client.frame(
        FrameKind::GroupWatch,
        TrafficClass::Control,
        Id::new_v7()?,
        operation,
        &request,
        Instant::now() + Duration::from_secs(1),
    )?;
    Ok(())
}

type SelectedWatch = (Id, public::GroupWatch, Id, Instant, Attempt);

pub(super) fn next(core: &Core) -> Result<Option<SelectedWatch>> {
    let now = Instant::now();
    let mut state = core.state()?;
    expire(&mut state, now);
    let (epoch, version) = {
        let info = core.info.lock().map_err(|_| Error::Closed)?;
        (info.member_epoch, info.assignment_version)
    };
    let live = epoch > 0
        && epoch == core.epoch.load(Ordering::Acquire)
        && *core.lease_until.lock().map_err(|_| Error::Closed)? > now;
    if state.next_watch > now {
        return Ok(None);
    }
    if let Some(id) = state
        .watches
        .iter()
        .find(|(_, watch)| {
            !watch.active
                && watch.retry_at <= now
                && live
                && watch.request.member_epoch == epoch
                && watch.request.assignment_version == version
        })
        .map(|(id, _)| *id)
    {
        let permit = core
            .client
            .inner
            .admission
            .fair(core.member, Lane::Watch, || {
                core.credit.attempt(
                    &core.client,
                    credit::Pool::Ordinary,
                    false,
                    TrafficClass::Control,
                )
            })?;
        let Some(permit) = permit else {
            return Ok(None);
        };
        let watch = state.watches.get_mut(&id).ok_or(Error::Closed)?;
        watch.request.attempt = watch
            .request
            .attempt
            .checked_add(1)
            .ok_or(Error::SessionExpired)?;
        watch.active = true;
        let selected = (
            id,
            watch.request.clone(),
            watch.operation,
            watch.deadline,
            permit,
        );
        state.next_watch = now + Duration::from_micros(15_625);
        return Ok(Some(selected));
    }
    let long_capable = core.credit.mode != ConsumerMode::Serial
        && core.client.inner.config.request_timeout_ms > 100;
    if state.watches.len() >= if !long_capable { 1 } else { 8 } || !live {
        return Ok(None);
    }
    let long = state
        .watches
        .values()
        .filter(|watch| watch.request.wait_ms > 0)
        .count();
    let wait = long < 7 && long_capable;
    let operation = Id::new_v7()?;
    let request = page(core, &state, operation, wait, now)?;
    if request.targets.is_empty() {
        core.client
            .inner
            .admission
            .withdraw(core.member, Lane::Watch);
        return Ok(None);
    }
    let permit = core
        .client
        .inner
        .admission
        .fair(core.member, Lane::Watch, || {
            core.credit.attempt(
                &core.client,
                credit::Pool::Ordinary,
                false,
                TrafficClass::Control,
            )
        })?;
    let Some(permit) = permit else {
        return Ok(None);
    };
    register(core, &mut state, request, operation, permit, now)
}

fn page(
    core: &Core,
    state: &State,
    operation: Id,
    wait: bool,
    now: Instant,
) -> Result<public::GroupWatch> {
    let mut request = request(core, operation, wait)?;
    let eligible = |domain: &&driver::Domain| {
        domain.assigned && !domain.complete && domain.watching.is_none() && domain.next_watch <= now
    };
    let after = state.watch_after;
    let advance = state
        .domains
        .values()
        .filter(eligible)
        .any(|domain| after.is_none_or(|after| domain.shard > after));
    let candidates = state
        .domains
        .values()
        .filter(eligible)
        .filter(|domain| !advance || after.is_none_or(|after| domain.shard > after));
    for domain in candidates.take(MAX_TARGETS) {
        request.targets.push(public::WatchTarget {
            shard_id: bytes(domain.shard),
            birth_partition: domain.partition,
            observed_token: domain.token.clone(),
        });
        // Validate using maximum-size tokens so subsequent pages can always make progress.
        let mut maximum = request.clone();
        for target in &mut maximum.targets {
            target.observed_token = Bytes::from(vec![0; MAX_TOKEN_BYTES]);
        }
        if tidemq_protocol::watch::validate(&maximum, operation, &core.client.inner.config.protocol)
            .is_err()
        {
            request.targets.pop();
            break;
        }
    }
    Ok(request)
}

fn register(
    core: &Core,
    state: &mut State,
    request: public::GroupWatch,
    operation: Id,
    permit: Attempt,
    now: Instant,
) -> Result<Option<SelectedWatch>> {
    let charged = prost::Message::encoded_len(&request)
        .saturating_mul(3)
        .saturating_add(request.targets.len().saturating_mul(256))
        .saturating_add(2048);
    let memory = core
        .client
        .inner
        .resource_memory
        .clone()
        .try_acquire_many_owned(u32::try_from(charged).map_err(|_| Error::Admission {
            resource: "watch metadata",
        })?)
        .map_err(|_| Error::Admission {
            resource: "watch metadata",
        })?;
    for target in &request.targets {
        if let Some(domain) = state.domains.get_mut(&Id::from_slice(&target.shard_id)?) {
            domain.watching = Some(operation);
        }
    }
    state.watch_after = request
        .targets
        .last()
        .map(|target| Id::from_slice(&target.shard_id))
        .transpose()?;
    let deadline = now
        + Duration::from_millis(
            core.client
                .inner
                .config
                .request_timeout_ms
                .min(if request.wait_ms == 0 { 500 } else { 1250 }),
        );
    state.watches.insert(
        operation,
        Watch {
            request: request.clone(),
            operation,
            deadline,
            active: true,
            retry_at: now,
            failures: 0,
            _memory: memory,
        },
    );
    state.next_watch = now + Duration::from_micros(15_625);
    Ok(Some((operation, request, operation, deadline, permit)))
}

pub(super) async fn poll(
    core: &Core,
    operation: Id,
    request: &public::GroupWatch,
    original_deadline: Instant,
    permit: &Attempt,
) -> Result<public::GroupWatchResult> {
    tidemq_protocol::watch::validate(request, operation, &core.client.inner.config.protocol)?;
    let deadline = original_deadline
        .min(Instant::now() + Duration::from_millis(if request.wait_ms == 0 { 500 } else { 1250 }));
    tokio::time::timeout_at(
        deadline,
        core.client.request_reserved(
            FrameKind::GroupWatch,
            FrameKind::GroupWatchResult,
            TrafficClass::Control,
            operation,
            request,
            deadline,
            permit,
        ),
    )
    .await
    .unwrap_or(Err(Error::Deadline))
    .map_err(|error| error.read_only(true))
}

pub(super) fn finish(
    core: &Core,
    id: Id,
    attempt: u64,
    result: Result<public::GroupWatchResult>,
) -> Result<()> {
    let mut state = core.state()?;
    let Some(watch) = state.watches.get_mut(&id) else {
        return Ok(());
    };
    if watch.request.attempt != attempt || !watch.active {
        return Ok(());
    }
    watch.active = false;
    let result = result.and_then(|result| validate_result(&watch.request, result));
    let results = match result {
        Ok(results) => results,
        Err(error) => {
            watch.failures = watch.failures.saturating_add(1);
            watch.retry_at = if watch.failures >= 3 {
                watch.deadline
            } else {
                Instant::now() + Duration::from_millis(250)
            };
            drop(state);
            if driver::no_work(&error) {
                tracing::debug!(
                    target: "tidemq_sdk::consumer_recovery",
                    consumer = %core.member,
                    operation = %id,
                    attempt,
                    error_kind = ?error.kind(),
                    "Consumer watch hint will retry within its original deadline"
                );
                return Ok(());
            }
            return Err(error);
        }
    };
    let watch = state.watches.remove(&id).ok_or(Error::Closed)?;
    let current = core.info.lock().map_err(|_| Error::Closed)?;
    let valid = watch.request.member_epoch == core.epoch.load(Ordering::Acquire)
        && watch.request.assignment_version == current.assignment_version;
    let mut failure = None;
    for (target, result) in watch.request.targets.iter().zip(results) {
        let shard = Id::from_slice(&target.shard_id)?;
        let Some(domain) = state
            .domains
            .get_mut(&shard)
            .filter(|domain| domain.watching == Some(id))
        else {
            continue;
        };
        domain.watching = None;
        if !valid || !domain.assigned {
            continue;
        }
        if let Err(error) = observe(domain, result) {
            failure.get_or_insert(error);
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn observe(domain: &mut driver::Domain, result: public::WatchResult) -> Result<()> {
    let status =
        public::WatchStatus::try_from(result.status).map_err(|_| Error::ResponseMismatch {
            reason: "watch status is invalid".into(),
        })?;
    match status {
        public::WatchStatus::WatchSync | public::WatchStatus::WatchChanged => {
            domain.observed_at = Instant::now();
            domain.token = result.observed_token;
            domain.dirty = true;
        }
        public::WatchStatus::WatchUnchanged => {
            domain.observed_at = Instant::now();
        }
        public::WatchStatus::WatchReset => {
            domain.token = Bytes::new();
            domain.dirty = true;
            domain.next_watch = Instant::now() + Duration::from_millis(250);
        }
        public::WatchStatus::WatchRetry | public::WatchStatus::WatchRejected => {
            domain.next_watch = Instant::now() + Duration::from_millis(250);
            let code =
                tidemq_protocol::common::ErrorCode::try_from(result.error_code).map_err(|_| {
                    Error::ResponseMismatch {
                        reason: "watch error code is invalid".into(),
                    }
                })?;
            let error = Error::Remote {
                code,
                message: "Watch target could not be observed".into(),
                outcome: tidemq_protocol::common::Outcome::Rejected,
                retryable: result.retryable,
                expected_sequence: None,
                expected_registration_epoch: None,
                retry_after_ms: None,
                original_outcome_known: true,
            };
            if status == public::WatchStatus::WatchRetry && driver::no_work(&error) {
                // A retry is neither an unchanged observation nor a business receive failure.
                return Ok(());
            }
            return Err(error);
        }
        public::WatchStatus::Unspecified => {
            return Err(Error::ResponseMismatch {
                reason: "watch status is unspecified".into(),
            });
        }
    }
    Ok(())
}

fn validate_result(
    request: &public::GroupWatch,
    result: public::GroupWatchResult,
) -> Result<Vec<public::WatchResult>> {
    let invalid = || {
        Error::ResponseMismatch { reason: "watch response must cover each original target exactly once with bounded tokens and explicit status".into() }
    };
    if request.targets.len() != result.targets.len() {
        return Err(invalid());
    }
    let mut seen = BTreeSet::new();
    let mut results = BTreeMap::new();
    for result in result.targets {
        if result.observed_token.len() > MAX_TOKEN_BYTES || !seen.insert(result.shard_id.clone()) {
            return Err(invalid());
        }
        let target = request
            .targets
            .iter()
            .find(|target| target.shard_id == result.shard_id)
            .ok_or_else(invalid)?;
        match public::WatchStatus::try_from(result.status).map_err(|_| invalid())? {
            public::WatchStatus::Unspecified => return Err(invalid()),
            public::WatchStatus::WatchSync | public::WatchStatus::WatchChanged
                if result.observed_token.is_empty() =>
            {
                return Err(invalid());
            }
            public::WatchStatus::WatchUnchanged
                if result.observed_token != target.observed_token
                    || result.observed_token.is_empty() =>
            {
                return Err(invalid());
            }
            public::WatchStatus::WatchRetry | public::WatchStatus::WatchRejected
                if result.error_code == 0
                    || tidemq_protocol::common::ErrorCode::try_from(result.error_code).is_err() =>
            {
                return Err(invalid());
            }
            _ => {}
        }
        results.insert(result.shard_id.clone(), result);
    }
    request
        .targets
        .iter()
        .map(|target| results.remove(&target.shard_id).ok_or_else(invalid))
        .collect()
}

fn expire(state: &mut State, now: Instant) {
    let expired: Vec<_> = state
        .watches
        .iter()
        .filter(|(_, watch)| !watch.active && watch.deadline <= now)
        .map(|(id, _)| *id)
        .collect();
    for id in expired {
        remove(state, id);
    }
}

fn remove(state: &mut State, id: Id) {
    if let Some(watch) = state.watches.remove(&id) {
        for target in watch.request.targets {
            if let Ok(shard) = Id::from_slice(&target.shard_id)
                && let Some(domain) = state
                    .domains
                    .get_mut(&shard)
                    .filter(|domain| domain.watching == Some(id))
            {
                domain.watching = None;
            }
        }
    }
}

pub(super) fn counts(state: &State) -> (usize, usize) {
    (
        state
            .watches
            .values()
            .filter(|watch| watch.request.wait_ms > 0)
            .count(),
        state
            .watches
            .values()
            .filter(|watch| watch.request.wait_ms == 0)
            .count(),
    )
}

#[cfg(test)]
pub(super) fn retry_now_for_test(core: &Core, id: Id) {
    let mut state = core.state().unwrap();
    state.next_watch = Instant::now();
    state.watches.get_mut(&id).unwrap().retry_at = Instant::now();
}

pub(super) async fn cleanup(core: &Arc<Core>, deadline: Instant) -> usize {
    let watches = core
        .state()
        .map(|state| {
            state
                .watches
                .values()
                .map(|watch| (watch.operation, watch.request.attempt, watch.deadline))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut jobs = tokio::task::JoinSet::new();
    for (operation, attempt, expires) in watches {
        let core = core.clone();
        jobs.spawn(async move {
            let request = public::GroupWatchCancel {
                root_operation_id: bytes(operation),
                operation_id: bytes(operation),
                attempt,
            };
            let result: Result<public::GroupWatchCancelled> = match Id::new_v7() {
                Ok(cancel) => {
                    core.client
                        .request(
                            FrameKind::GroupWatchCancel,
                            FrameKind::GroupWatchCancelled,
                            TrafficClass::Control,
                            cancel,
                            &request,
                            deadline.min(expires),
                        )
                        .await
                }
                Err(error) => Err(error.into()),
            };
            if result.is_err() && Instant::now() < expires {
                tokio::time::sleep_until(deadline.min(expires)).await;
            }
            (operation, result.is_ok() || Instant::now() >= expires)
        });
    }
    while let Some(result) = jobs.join_next().await {
        if let Ok((operation, true)) = result
            && let Ok(mut state) = core.state()
        {
            remove(&mut state, operation);
        }
    }
    core.state().map_or(1, |state| state.watches.len())
}
