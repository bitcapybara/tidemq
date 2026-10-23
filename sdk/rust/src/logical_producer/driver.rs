use super::*;

pub(super) async fn start(
    core: Arc<Core>,
    mut request: public::PublisherControl,
    deadline: Instant,
    ready: tokio::sync::oneshot::Sender<std::result::Result<(), Arc<Error>>>,
) {
    let opened = open(&core, &mut request, deadline)
        .await
        .and_then(|(status, started)| {
            core.registration
                .store(status.registration, Ordering::Release);
            core.update_lease(&status, started)?;
            Ok(status)
        });
    match opened {
        Ok(status) => {
            let _ = ready.send(Ok(()));
            maintain(&core, status.next_heartbeat_sequence).await;
            finish(core, None).await;
        }
        Err(error) => {
            let source = Arc::new(error);
            let _ = ready.send(Err(source.clone()));
            finish(core, Some(source)).await;
        }
    }
}

async fn open(
    core: &Core,
    request: &mut public::PublisherControl,
    deadline: Instant,
) -> Result<(public::PublisherControlResult, Instant)> {
    let mut operation = Id::new_v7()?;
    loop {
        core.registration
            .store(request.registration, Ordering::Release);
        let started = Instant::now();
        match control(&core.client, operation, request, deadline).await {
            Err(Error::Remote {
                code: tidemq_protocol::common::ErrorCode::RegistrationConflict,
                outcome: tidemq_protocol::common::Outcome::Rejected,
                expected_registration_epoch: Some(expected),
                ..
            }) => {
                request.registration = expected;
                operation = Id::new_v7()?;
            }
            Err(error) if error.outcome_unknown() && Instant::now() < deadline => {
                tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(10)))
                    .await
                    .map_err(|_| Error::Deadline)?;
            }
            result => return result.map(|status| (status, started)),
        }
    }
}

async fn maintain(core: &Core, mut sequence: u64) {
    let mut operation = match Id::new_v7() {
        Ok(id) => id,
        Err(_) => {
            core.stop.cancel();
            return;
        }
    };
    let mut timer = tokio::time::interval(Duration::from_secs(30));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { _ = core.stop.cancelled() => return, _ = timer.tick() => {} }
        let started = Instant::now();
        let mut request = core.request(public::PublisherAction::HeartbeatPublisher);
        request.heartbeat_sequence = sequence;
        let result = tokio::select! {
            _ = core.stop.cancelled() => return,
            result = core.client.operation(|deadline| control(&core.client, operation, &request, deadline)) => result,
        };
        match result {
            Ok(status) => {
                if core.update_lease(&status, started).is_err() {
                    core.stop.cancel();
                    return;
                }
                let Some(next) = sequence.checked_add(1) else {
                    core.stop.cancel();
                    return;
                };
                sequence = next;
                match Id::new_v7() {
                    Ok(id) => operation = id,
                    Err(_) => {
                        core.stop.cancel();
                        return;
                    }
                }
            }
            Err(error) => {
                let expired = core
                    .lease_until
                    .lock()
                    .is_ok_and(|until| *until <= Instant::now());
                if error.permission_denied() || expired {
                    core.stop.cancel();
                    return;
                }
            }
        }
    }
}

async fn finish(core: Arc<Core>, original: Option<Arc<Error>>) {
    let deadline = match core.close_deadline.lock() {
        Ok(mut deadline) => {
            *deadline.get_or_insert_with(|| Instant::now() + core.client.inner.config.close_timeout)
        }
        Err(_) => Instant::now(),
    };
    send::drain(&core, deadline).await;
    send::stop_tasks(&core).await;
    let closed = close_remote(&core, deadline).await;
    let mut report = CloseReport {
        local_cleanup_complete: true,
        ..Default::default()
    };
    if let Ok(state) = core.state() {
        for pending in state.pending.values() {
            if pending.complete() {
                report.unclaimed_results += 1;
            } else {
                report.unresolved_sends += 1;
            }
        }
    }
    let result = match original {
        Some(source) => Err(CloseError { report, source }),
        None => match closed {
            Ok(()) => Ok(report),
            Err(error) => Err(CloseError {
                report,
                source: Arc::new(error),
            }),
        },
    };
    if let Ok(mut resource) = core.resource_memory.lock() {
        resource.take();
    }
    if let Ok(mut state) = core.state() {
        state.layout.wire.active = Vec::new();
        state.layout.wire.history = Vec::new();
        state.touched.clear();
        state.closed = Some(result);
    }
    core.changed.notify_waiters();
}

async fn close_remote(core: &Core, deadline: Instant) -> Result<()> {
    let request = core.request(public::PublisherAction::ClosePublisher);
    control(&core.client, Id::new_v7()?, &request, deadline).await?;
    let (before, domains) = {
        let state = core.state()?;
        (
            state
                .pending
                .first_key_value()
                .map_or(state.next_sequence, |(sequence, _)| *sequence),
            state.touched.values().cloned().collect::<Vec<_>>(),
        )
    };
    for domain in domains {
        let request = public::RetirePublishes {
            topic_id: bytes(core.topic_id),
            shard_id: domain.shard_id,
            partition_id: domain.birth_partition,
            producer_id: bytes(core.producer),
            registration: core.registration.load(Ordering::Acquire),
            before,
            close: true,
        };
        let result: public::PublishesRetired = core
            .client
            .request(
                FrameKind::RetirePublishes,
                FrameKind::PublishesRetired,
                TrafficClass::Control,
                Id::new_v7()?,
                &request,
                deadline,
            )
            .await?;
        if result.before != before {
            return Err(Error::ResponseMismatch {
                reason: "publisher retirement differs from its requested boundary".into(),
            });
        }
    }
    Ok(())
}
