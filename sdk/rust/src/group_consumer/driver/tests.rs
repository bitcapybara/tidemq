use super::*;
mod peer;

fn remote_failure(code: tidemq_protocol::common::ErrorCode, retryable: bool) -> Error {
    Error::Remote {
        code,
        message: "The peer refused the original operation".into(),
        outcome: tidemq_protocol::common::Outcome::Rejected,
        retryable,
        expected_sequence: None,
        expected_registration_epoch: None,
        retry_after_ms: None,
        original_outcome_known: false,
    }
}

#[tokio::test]
async fn a_read_gate_rejection_retries_only_the_same_unapplied_read() {
    use tidemq_protocol::common::ErrorCode;
    let client = client();
    let core = core(&client, 1, 256, 8 * 1024 * 1024);
    let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    let original = {
        let read = read.lock().unwrap();
        (
            read.operation,
            read.request.clone(),
            read.deadline,
            read.pool,
        )
    };
    drop(permit);
    let error = remote_failure(ErrorCode::AssignmentBusy, false);
    assert!(
        !no_work(&error),
        "The exception must not change non-Read recovery"
    );
    scheduling::finish_read(&core, id, &read, Err(error)).unwrap();
    assert!(core.try_receive().unwrap().is_none());
    unknown(&core, id, &read);
    scheduling::finish_read(
        &core,
        id,
        &read,
        Err(remote_failure(ErrorCode::AssignmentBusy, false).preserve_uncertainty(true)),
    )
    .unwrap();
    core.state()
        .unwrap()
        .domains
        .get_mut(&id)
        .unwrap()
        .next_poll = Instant::now();
    let (_, recovered, permit) = scheduling::next_read(&core, true).unwrap().unwrap();
    assert!(Arc::ptr_eq(&read, &recovered));
    let retained = recovered.lock().unwrap();
    assert_eq!(
        (
            retained.operation,
            retained.request.clone(),
            retained.deadline,
            retained.pool
        ),
        original
    );
    assert!(retained.uncertain && retained.credit.is_some());
    drop(retained);
    drop(permit);
    let request = read.lock().unwrap().request.clone();
    accept_read(
        &core,
        id,
        &read,
        &request,
        deliveries(&read, Bytes::from_static(b"gate-open")),
    )
    .unwrap();
    scheduling::finish_read(
        &core,
        id,
        &read,
        Err(remote_failure(ErrorCode::AssignmentBusy, false)),
    )
    .unwrap();
    assert_eq!(
        core.try_receive().unwrap_err().kind(),
        crate::ErrorKind::Conflict,
        "The Read exception must not hide a retirement refusal"
    );
    assert_eq!(core.state().unwrap().queue.len(), 1);
    {
        let mut read = read.lock().unwrap();
        // A fenced response can be discarded before applied is set, then still require retirement.
        read.applied = false;
        read.retiring = true;
    }
    scheduling::finish_read(
        &core,
        id,
        &read,
        Err(remote_failure(ErrorCode::AssignmentBusy, false)),
    )
    .unwrap();
    assert_eq!(
        core.try_receive().unwrap_err().kind(),
        crate::ErrorKind::Conflict
    );
}

fn assert_read_refusal_visible(error: Error) {
    let client = client();
    let core = core(&client, 1, 256, 8 * 1024 * 1024);
    let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    let kind = error.kind();
    drop(permit);
    scheduling::finish_read(&core, id, &read, Err(error)).unwrap();
    assert_eq!(core.try_receive().unwrap_err().kind(), kind);
    assert!(Arc::ptr_eq(
        core.state()
            .unwrap()
            .domains
            .get(&id)
            .unwrap()
            .read
            .as_ref()
            .unwrap(),
        &read
    ));
}

#[tokio::test]
async fn nonretryable_read_refusals_other_than_a_closed_gate_remain_visible() {
    use tidemq_protocol::common::{ErrorCode, Outcome};
    for code in [
        ErrorCode::Forbidden,
        ErrorCode::Unauthenticated,
        ErrorCode::DeduplicationExpired,
        ErrorCode::OffsetExpired,
        ErrorCode::SessionExpired,
        ErrorCode::ContentConflict,
        ErrorCode::QuotaExceeded,
        ErrorCode::Unavailable,
        ErrorCode::InvalidRequest,
    ] {
        assert_read_refusal_visible(remote_failure(code, false));
    }
    assert_read_refusal_visible(Error::Protocol {
        source: tidemq_protocol::Error::Limit {
            name: "read protocol bound",
        },
    });
    assert_read_refusal_visible(Error::ResponseMismatch {
        reason: "Read result is malformed".into(),
    });
    assert_read_refusal_visible(Error::Query {
        source: Box::new(remote_failure(ErrorCode::AssignmentBusy, false)),
    });
    let mut unknown = remote_failure(ErrorCode::AssignmentBusy, false);
    if let Error::Remote { outcome, .. } = &mut unknown {
        *outcome = Outcome::Unknown;
    }
    assert_read_refusal_visible(unknown);
}

#[test]
fn terminal_causes_are_not_made_retryable_by_query_or_unknown_wrappers() {
    use tidemq_protocol::common::ErrorCode;
    for code in [
        ErrorCode::Forbidden,
        ErrorCode::Unauthenticated,
        ErrorCode::DeduplicationExpired,
        ErrorCode::OffsetExpired,
        ErrorCode::SessionExpired,
        ErrorCode::ContentConflict,
        ErrorCode::InvalidRequest,
        ErrorCode::NotFound,
    ] {
        assert!(!no_work(&remote_failure(code, true)));
        assert!(!no_work(
            &remote_failure(code, true).preserve_uncertainty(true)
        ));
        assert!(!no_work(&Error::Query {
            source: Box::new(remote_failure(code, true))
        }));
    }
    assert!(!no_work(&remote_failure(ErrorCode::QuotaExceeded, false)));
    assert!(!no_work(
        &Error::ResponseMismatch {
            reason: "Invalid Watch response".into()
        }
        .preserve_uncertainty(true)
    ));
    assert!(!no_work(
        &Error::Protocol {
            source: tidemq_protocol::Error::Limit {
                name: "test protocol bound"
            }
        }
        .preserve_uncertainty(true)
    ));
    for code in [
        ErrorCode::Unavailable,
        ErrorCode::QuotaExceeded,
        ErrorCode::AssignmentBusy,
        ErrorCode::StaleGeneration,
    ] {
        assert!(no_work(&remote_failure(code, true)));
        assert!(no_work(
            &remote_failure(code, true).preserve_uncertainty(true)
        ));
    }
}

#[tokio::test]
async fn continuous_layout_failure_uses_the_shared_thirty_second_saturation_gate() {
    let client = client();
    let core = core(&client, 0, 256, 8 * 1024 * 1024);
    scheduling::report_stall(&core, Instant::now() - Duration::from_secs(29)).unwrap();
    assert!(core.try_receive().unwrap().is_none());
    let failed_since = Instant::now() - Duration::from_secs(31);
    scheduling::report_stall(&core, failed_since).unwrap();
    assert_eq!(
        core.try_receive().unwrap_err().kind(),
        crate::ErrorKind::ResourceExhausted
    );
    scheduling::report_stall(&core, failed_since).unwrap();
    assert!(core.try_receive().unwrap().is_none());
    assert!(!core.stop.is_cancelled());
}

#[tokio::test]
async fn an_expired_original_read_remains_visible_without_replacing_its_unknown_identity() {
    let client = client();
    let core = core(&client, 1, 256, 8 * 1024 * 1024);
    let (id, read, attempt) = scheduling::next_read(&core, false).unwrap().unwrap();
    let original = {
        let read = read.lock().unwrap();
        (
            read.operation,
            read.request.clone(),
            read.deadline,
            read.pool,
        )
    };
    drop(attempt);
    unknown(&core, id, &read);
    let error = remote_failure(
        tidemq_protocol::common::ErrorCode::DeduplicationExpired,
        false,
    )
    .preserve_uncertainty(true);
    scheduling::finish_read(&core, id, &read, Err(error)).unwrap();
    let error = core.try_receive().unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::RecoveryExpired);
    assert!(error.outcome_unknown());
    let retained = read.lock().unwrap();
    assert_eq!(
        (
            retained.operation,
            retained.request.clone(),
            retained.deadline,
            retained.pool
        ),
        original
    );
    assert!(retained.credit.is_some());
    drop(retained);
    assert!(scheduling::next_read(&core, false).unwrap().is_none());
}

fn core(client: &Client, targets: usize, messages: u32, max_bytes: u32) -> Arc<Core> {
    let member = Id::new_v7().unwrap();
    let credit = credit::Pools::new(client, messages, max_bytes).unwrap();
    let mut domains = BTreeMap::new();
    let mut shards = Vec::new();
    for index in 1..=targets {
        let shard = Id::from_slice(&(index as u128).to_be_bytes()).unwrap();
        shards.push(bytes(shard));
        domains.insert(
            shard,
            Domain::new(
                shard,
                index as u32,
                client
                    .inner
                    .resource_memory
                    .clone()
                    .try_acquire_many_owned(512)
                    .unwrap(),
            ),
        );
    }
    Arc::new(Core {
        client: client.background(),
        topic: "test/app/events".into(),
        topic_id: Id::new_v7().unwrap(),
        subscription: Id::new_v7().unwrap(),
        model: SubscriptionKind::Queue,
        member,
        epoch: AtomicU64::new(1),
        max_messages: messages,
        max_bytes,
        info: Mutex::new(public::ConsumerControlResult {
            member_epoch: 1,
            assignment_version: 1,
            remaining_lease_ms: 15_000,
            work_shards: shards,
            plan_active: true,
            assignment_fenced: true,
            ..Default::default()
        }),
        state: Mutex::new(State {
            queue: VecDeque::new(),
            ledger: BTreeMap::new(),
            revoked: Vec::new(),
            next_delivery: 1,
            domains,
            read_after: None,
            recovery_after: None,
            watch_after: None,
            watches: BTreeMap::new(),
            next_watch: Instant::now(),
            saturated_since: None,
            last_saturation_error: None,
            pending: BTreeMap::new(),
            pending_join: None,
            claims: BTreeMap::new(),
            ack_sequences: BTreeMap::new(),
            error: None,
            closed: None,
        }),
        changed: Notify::new(),
        credit,
        stop: CancellationToken::new(),
        close_deadline: Mutex::new(None),
        lease_until: Mutex::new(Instant::now() + Duration::from_secs(15)),
        lease_fence_after: Mutex::new(Instant::now() + Duration::from_secs(45)),
        task: Mutex::new(None),
        quarantine: Mutex::new(None),
        resource_memory: Mutex::new(Some(
            client
                .inner
                .resource_memory
                .clone()
                .try_acquire_many_owned(64 * 1024)
                .unwrap(),
        )),
    })
}

fn client() -> Client {
    crate::client::test_client(crate::client::ClientConfig::new("localhost:7443"))
}

#[tokio::test]
async fn unknown_eight_keep_original_credit_and_healthy_ninth_uses_protection() {
    let client = client();
    let core = core(&client, 10, 256, 8 * 1024 * 1024);
    let mut identities = Vec::new();
    for _ in 0..8 {
        let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
        let original = read.lock().unwrap();
        assert_eq!(original.pool, Pool::Ordinary);
        identities.push((
            id,
            original.operation,
            original.request.clone(),
            original.deadline,
        ));
        drop(original);
        drop(permit);
        scheduling::finish_read(
            &core,
            id,
            &read,
            Err(Error::RequestUnknown {
                source: Box::new(Error::Deadline),
            }),
        )
        .unwrap();
    }
    let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    assert_eq!(read.lock().unwrap().pool, Pool::Protected);
    assert_eq!(read.lock().unwrap().request.max_messages, 1);
    assert_eq!(core.credit.protected_slots.available_permits(), 0);
    assert_eq!(diagnostics(&core).unwrap().ordinary_reads, 8);
    drop(permit);
    scheduling::finish_read(
        &core,
        id,
        &read,
        Ok(ReadProgress {
            empty: true,
            complete: false,
        }),
    )
    .unwrap();
    drop(read);
    let (_, next, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    assert_eq!(next.lock().unwrap().pool, Pool::Protected);
    drop(permit);
    for (id, operation, request, deadline) in identities {
        let state = core.state().unwrap();
        let original = state
            .domains
            .get(&id)
            .unwrap()
            .read
            .as_ref()
            .unwrap()
            .lock()
            .unwrap();
        assert_eq!(original.operation, operation);
        assert_eq!(original.request, request);
        assert_eq!(original.deadline, deadline);
        assert!(original.credit.is_some());
    }
}

fn deliveries(read: &Arc<Mutex<ReadAttempt>>, body: Bytes) -> public::GroupDeliveries {
    public::GroupDeliveries {
        sequence: read.lock().unwrap().request.sequence,
        assignment_version: 2,
        sealed_complete: false,
        deliveries: vec![public::GroupDelivery {
            proof: Some(public::GroupProof {
                offset: 0,
                attempt: 1,
                mac: Bytes::from(vec![1; 32]),
            }),
            body,
            current_attempt: true,
            ..Default::default()
        }],
    }
}

fn unknown(core: &Core, id: Id, read: &Arc<Mutex<ReadAttempt>>) {
    scheduling::finish_read(
        core,
        id,
        read,
        Err(Error::RequestUnknown {
            source: Box::new(Error::Deadline),
        }),
    )
    .unwrap();
}

#[tokio::test]
async fn protected_unknown_blocks_only_its_own_share_and_recovery_has_two_attempts() {
    let client = client();
    let core = core(&client, 11, 256, 8 * 1024 * 1024);
    for _ in 0..9 {
        let (id, read, attempt) = scheduling::next_read(&core, false).unwrap().unwrap();
        drop(attempt);
        unknown(&core, id, &read);
    }
    assert!(scheduling::next_read(&core, false).unwrap().is_none());
    assert!(diagnostics(&core).unwrap().protected_blocked);
    assert!(core.try_receive().unwrap().is_none());
    for domain in core.state().unwrap().domains.values_mut() {
        if domain.read.is_some() {
            domain.failed_since = Some(Instant::now() - Duration::from_secs(31));
        }
    }
    scheduling::report_saturation(&core).unwrap();
    assert_eq!(
        core.try_receive().unwrap_err().kind(),
        crate::ErrorKind::ResourceExhausted
    );
    for domain in core.state().unwrap().domains.values_mut() {
        domain.next_poll = Instant::now();
    }
    let first = scheduling::next_read(&core, true).unwrap().unwrap();
    let second = scheduling::next_read(&core, true).unwrap().unwrap();
    assert_ne!(first.0, second.0);
    assert!(scheduling::next_read(&core, true).unwrap().is_none());
    let status = diagnostics(&core).unwrap();
    assert_eq!(status.ordinary_reads, 8);
    assert_eq!(status.protected_reads, 1);
    assert_eq!(status.ordinary_unknown, 8);
    assert!(status.protected_unknown);
    assert_eq!(status.recovery_queue, 7);
    assert_eq!(status.ordinary_rpc, 2);
    assert_eq!(status.protected_rpc, 0);
    assert_eq!(status.recovery_rpc, 2);
    drop((first, second));
}

#[tokio::test]
async fn active_rpc_caps_are_seven_ordinary_and_one_protected() {
    let client = client();
    let minimum = minimum_prefetch_bytes(client.inner.config.protocol.max_message_bytes) as u32;
    let core = core(&client, 20, 256, minimum * 8);
    let mut active = Vec::new();
    for _ in 0..8 {
        active.push(scheduling::next_read(&core, false).unwrap().unwrap());
    }
    assert!(scheduling::next_read(&core, false).unwrap().is_none());
    let status = diagnostics(&core).unwrap();
    assert_eq!(status.ordinary_rpc, 7);
    assert_eq!(status.protected_rpc, 1);
    assert_eq!(status.ordinary_reads, 7);
    assert_eq!(status.protected_reads, 1);
    drop(active);
}

#[tokio::test]
async fn fencing_domain_records_does_not_release_old_epoch_rpc_permits() {
    let client = client();
    let minimum = minimum_prefetch_bytes(client.inner.config.protocol.max_message_bytes) as u32;
    let core = core(&client, 20, 256, minimum * 8);
    let mut active = Vec::new();
    for _ in 0..8 {
        active.push(scheduling::next_read(&core, false).unwrap().unwrap());
    }
    core.epoch.store(2, Ordering::Release);
    core.info.lock().unwrap().member_epoch = 2;
    for domain in core.state().unwrap().domains.values_mut() {
        domain.read = None;
        domain.busy = false;
        domain.dirty = true;
    }
    assert!(scheduling::next_read(&core, false).unwrap().is_none());
    assert_eq!(diagnostics(&core).unwrap().ordinary_rpc, 7);
    assert_eq!(diagnostics(&core).unwrap().protected_rpc, 1);
    active.pop();
    assert_eq!(diagnostics(&core).unwrap().protected_rpc, 0);
    let (_, read, _) = scheduling::next_read(&core, false).unwrap().unwrap();
    assert_eq!(read.lock().unwrap().pool, Pool::Protected);
    drop(active);
}

#[tokio::test]
async fn rpc_ceiling_does_not_spend_protection_and_new_epoch_waits_for_its_view() {
    let client = client();
    let core = core(&client, 20, 256, 8 * 1024 * 1024);
    let mut active = Vec::new();
    for _ in 0..7 {
        active.push(scheduling::next_read(&core, false).unwrap().unwrap());
    }
    assert!(scheduling::next_read(&core, false).unwrap().is_none());
    assert_eq!(core.credit.protected_slots.available_permits(), 1);
    let (id, read, attempt) = active.remove(0);
    drop(attempt);
    unknown(&core, id, &read);
    let (_, next, attempt) = scheduling::next_read(&core, false).unwrap().unwrap();
    assert_eq!(next.lock().unwrap().pool, Pool::Ordinary);
    active.push((id, next, attempt));
    let (_, protected, attempt) = scheduling::next_read(&core, false).unwrap().unwrap();
    assert_eq!(protected.lock().unwrap().pool, Pool::Protected);
    drop(attempt);
    core.epoch.store(2, Ordering::Release);
    assert!(scheduling::next_read(&core, false).unwrap().is_none());
    assert!(watch::next(&core).unwrap().is_none());
    drop(active);
}

#[tokio::test]
async fn retirement_unknown_cannot_enqueue_twice_and_clones_retain_protected_bytes() {
    let client = client();
    let core = core(&client, 9, 256, 8 * 1024 * 1024);
    for _ in 0..8 {
        let (id, read, attempt) = scheduling::next_read(&core, false).unwrap().unwrap();
        drop(attempt);
        unknown(&core, id, &read);
    }
    let (id, read, attempt) = scheduling::next_read(&core, false).unwrap().unwrap();
    let original = read.lock().unwrap().request.clone();
    let result = deliveries(
        &read,
        Bytes::from(vec![
            42;
            client.inner.config.protocol.max_message_bytes as usize
        ]),
    );
    accept_read(&core, id, &read, &original, result.clone()).unwrap();
    assert!(read.lock().unwrap().applied);
    assert!(read.lock().unwrap().credit.is_none());
    drop(attempt);
    unknown(&core, id, &read);
    accept_read(&core, id, &read, &original, result).unwrap();
    assert_eq!(core.state().unwrap().queue.len(), 1);
    let message = core.try_receive().unwrap().unwrap();
    let clone = message.clone();
    message.data.release_credit().unwrap();
    core.state().unwrap().ledger.clear();
    assert_eq!(core.credit.protected_slots.available_permits(), 1);
    assert_eq!(core.credit.protected_bytes.available_permits(), 0);
    let progress = read.lock().unwrap().progress.unwrap();
    scheduling::finish_read(&core, id, &read, Ok(progress)).unwrap();
    drop(read);
    drop(message);
    assert_eq!(core.credit.protected_bytes.available_permits(), 0);
    assert!(scheduling::next_read(&core, false).unwrap().is_none());
    drop(clone);
    assert_eq!(
        core.credit.protected_bytes.available_permits(),
        core.credit.minimum as usize
    );
    let (_, read, _) = scheduling::next_read(&core, false).unwrap().unwrap();
    assert_eq!(read.lock().unwrap().pool, Pool::Protected);
}

#[tokio::test]
async fn serial_credit_and_small_client_capacity_are_explicit_and_too_small_is_rejected() {
    let client = client();
    let minimum = minimum_prefetch_bytes(client.inner.config.protocol.max_message_bytes) as u32;
    assert!(credit::Pools::new(&client, 1, minimum - 1).is_err());
    let core = core(&client, 2, 1, minimum);
    assert_eq!(diagnostics(&core).unwrap().mode, ConsumerMode::Serial);
    let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    let original = read.lock().unwrap().request.clone();
    drop(permit);
    unknown(&core, id, &read);
    assert!(scheduling::next_read(&core, false).unwrap().is_none());
    core.state()
        .unwrap()
        .domains
        .get_mut(&id)
        .unwrap()
        .next_poll = Instant::now();
    let (recovered, recovery, _) = scheduling::next_read(&core, true).unwrap().unwrap();
    assert_eq!(recovered, id);
    assert_eq!(recovery.lock().unwrap().request, original);
    let mut config = crate::client::ClientConfig::new("localhost:7443");
    config.max_pending_requests = 3;
    let small = crate::client::test_client(config);
    let pools = credit::Pools::new(&small, 256, 8 * 1024 * 1024).unwrap();
    assert_eq!(pools.mode, ConsumerMode::Serial);
}

#[tokio::test]
async fn pre_read_token_and_coalesced_dirty_survive_retirement() {
    let client = client();
    let core = core(&client, 1, 256, 8 * 1024 * 1024);
    let shard = *core.state().unwrap().domains.keys().next().unwrap();
    core.state().unwrap().domains.get_mut(&shard).unwrap().token =
        Bytes::from_static(b"before-read");
    let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    assert_eq!(
        read.lock().unwrap().token,
        Bytes::from_static(b"before-read")
    );
    let original = read.lock().unwrap().request.clone();
    accept_read(
        &core,
        id,
        &read,
        &original,
        public::GroupDeliveries {
            sequence: 1,
            assignment_version: 2,
            ..Default::default()
        },
    )
    .unwrap();
    for _ in 0..1000 {
        let mut state = core.state().unwrap();
        let domain = state.domains.get_mut(&shard).unwrap();
        domain.token = Bytes::from_static(b"during-retire");
        domain.dirty = true;
    }
    drop(permit);
    scheduling::finish_read(
        &core,
        id,
        &read,
        Ok(ReadProgress {
            empty: true,
            complete: false,
        }),
    )
    .unwrap();
    let state = core.state().unwrap();
    assert!(state.domains.get(&shard).unwrap().dirty);
    assert_eq!(state.domains.len(), 1);
    assert_eq!(state.domains.get(&shard).unwrap().sequence, 2);
}

#[tokio::test]
async fn late_epoch_response_and_old_completion_cannot_change_current_delivery() {
    let client = client();
    let core = core(&client, 1, 256, 8 * 1024 * 1024);
    let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    let original = read.lock().unwrap().request.clone();
    core.epoch.store(2, Ordering::Release);
    accept_read(
        &core,
        id,
        &read,
        &original,
        deliveries(&read, Bytes::from_static(b"old")),
    )
    .unwrap();
    assert_eq!(core.state().unwrap().queue.len(), 0);
    assert!(read.lock().unwrap().credit.is_none());
    core.state().unwrap().domains.get_mut(&id).unwrap().read = None;
    let failed_since = Instant::now();
    core.state()
        .unwrap()
        .domains
        .get_mut(&id)
        .unwrap()
        .failed_since = Some(failed_since);
    let sequence = core.state().unwrap().domains.get(&id).unwrap().sequence;
    drop(permit);
    scheduling::finish_read(
        &core,
        id,
        &read,
        Ok(ReadProgress {
            empty: false,
            complete: true,
        }),
    )
    .unwrap();
    assert_eq!(
        core.state().unwrap().domains.get(&id).unwrap().sequence,
        sequence
    );
    assert!(!core.state().unwrap().domains.get(&id).unwrap().complete);
    assert_eq!(
        core.state().unwrap().domains.get(&id).unwrap().failed_since,
        Some(failed_since)
    );
}

#[tokio::test]
async fn fenced_local_cleanup_returns_all_owned_resources_but_not_live_clone_bytes() {
    let client = client();
    let bytes_before = client.inner.bytes.available_permits();
    let metadata_before = client.inner.resource_memory.available_permits();
    let frames_before = client.inner.admission.frames.available_permits();
    let execution_before = client.inner.admission.execution.available_permits();
    let requests_before = client.inner.requests.available_permits();
    let core = core(&client, 20, 256, 8 * 1024 * 1024);
    let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    let request = read.lock().unwrap().request.clone();
    accept_read(
        &core,
        id,
        &read,
        &request,
        deliveries(&read, Bytes::from_static(b"retained")),
    )
    .unwrap();
    let retained = core.try_receive().unwrap().unwrap();
    drop(permit);
    release_local(&core);
    assert_eq!(core.state().unwrap().queue.len(), 0);
    assert_eq!(core.state().unwrap().domains.len(), 0);
    assert_eq!(
        metadata_before,
        client.inner.resource_memory.available_permits()
    );
    assert_eq!(
        frames_before,
        client.inner.admission.frames.available_permits()
    );
    assert_eq!(
        execution_before,
        client.inner.admission.execution.available_permits()
    );
    assert_eq!(requests_before, client.inner.requests.available_permits());
    assert!(client.inner.bytes.available_permits() < bytes_before);
    drop(retained);
    drop(read);
    assert_eq!(bytes_before, client.inner.bytes.available_permits());
    assert_eq!(client.metrics().snapshot().active_deliveries, 0);
    assert_eq!(client.metrics().snapshot().admitted_bytes, 0);
}

#[tokio::test]
async fn fair_watch_pages_cover_more_than_256_targets_without_overlap_or_credit() {
    for targets in [1, 8, 9, 256, 257, 1000] {
        let client = client();
        let core = core(&client, targets, 256, 8 * 1024 * 1024);
        let bytes_before = client.inner.bytes.available_permits();
        let mut seen = std::collections::BTreeSet::new();
        let mut active = Vec::new();
        for index in 0..(targets.div_ceil(32) + 7) {
            core.state().unwrap().next_watch = Instant::now();
            let Some((id, request, _, _, permit)) = watch::next(&core).unwrap() else {
                break;
            };
            assert!(request.targets.len() <= 32);
            if index >= 7 {
                assert_eq!(request.wait_ms, 0);
            }
            for target in &request.targets {
                assert!(seen.insert(Id::from_slice(&target.shard_id).unwrap()));
            }
            if index < 7 {
                active.push((id, request, permit));
            } else {
                let result = public::GroupWatchResult {
                    targets: request
                        .targets
                        .iter()
                        .map(|target| public::WatchResult {
                            shard_id: target.shard_id.clone(),
                            status: public::WatchStatus::WatchSync.into(),
                            observed_token: Bytes::from_static(b"baseline"),
                            ..Default::default()
                        })
                        .collect(),
                };
                watch::finish(&core, id, request.attempt, Ok(result)).unwrap();
                drop(permit);
            }
            let (long, probes) = watch::counts(&core.state().unwrap());
            assert!(long <= 7 && probes <= 1);
            assert_eq!(core.credit.ordinary_slots.available_permits(), 255);
            assert_eq!(core.credit.protected_slots.available_permits(), 1);
            assert_eq!(client.inner.bytes.available_permits(), bytes_before);
            if seen.len() == targets {
                break;
            }
        }
        assert_eq!(seen.len(), targets);
        drop(active);
    }
}

#[tokio::test]
async fn watch_timeouts_do_not_create_reads_and_pages_are_rate_limited() {
    let client = client();
    let core = core(&client, 64, 256, 8 * 1024 * 1024);
    for domain in core.state().unwrap().domains.values_mut() {
        domain.dirty = false;
        domain.token = Bytes::from_static(b"idle");
    }
    let before = Instant::now();
    let (id, request, operation, _, permit) = watch::next(&core).unwrap().unwrap();
    assert_eq!(bytes(operation), request.root_operation_id);
    assert!(request.parent_operation_id.is_empty());
    assert!(watch::next(&core).unwrap().is_none());
    assert!(core.state().unwrap().next_watch >= before + Duration::from_micros(15_625));
    let result = public::GroupWatchResult {
        targets: request
            .targets
            .iter()
            .map(|target| public::WatchResult {
                shard_id: target.shard_id.clone(),
                status: public::WatchStatus::WatchUnchanged.into(),
                observed_token: target.observed_token.clone(),
                ..Default::default()
            })
            .collect(),
    };
    watch::finish(&core, id, request.attempt, Ok(result)).unwrap();
    drop(permit);
    let state = core.state().unwrap();
    assert!(
        state
            .domains
            .values()
            .all(|domain| domain.read.is_none() && !domain.dirty && domain.sequence == 1)
    );
}

#[tokio::test]
async fn original_watch_retry_keeps_its_targets_and_deadline_with_a_higher_attempt() {
    let client = client();
    let core = core(&client, 2, 256, 8 * 1024 * 1024);
    let (id, request, operation, deadline, permit) = watch::next(&core).unwrap().unwrap();
    drop(permit);
    watch::finish(&core, id, request.attempt, Err(Error::Deadline)).unwrap();
    // Set the bounded scheduler's next wake without relying on wall-clock sleeps.
    watch::retry_now_for_test(&core, id);
    let (_, retry, next_operation, next_deadline, _) = watch::next(&core).unwrap().unwrap();
    assert_eq!(operation, next_operation);
    assert_eq!(deadline, next_deadline);
    assert_eq!(request.targets, retry.targets);
    assert_eq!(request.member_epoch, retry.member_epoch);
    assert_eq!(retry.attempt, request.attempt + 1);
    assert_eq!(
        tidemq_protocol::watch::digest(&request),
        tidemq_protocol::watch::digest(&retry)
    );
    let response = public::GroupWatchResult {
        targets: request
            .targets
            .iter()
            .map(|target| public::WatchResult {
                shard_id: target.shard_id.clone(),
                status: public::WatchStatus::WatchSync.into(),
                observed_token: Bytes::from_static(b"current-attempt"),
                ..Default::default()
            })
            .collect(),
    };
    watch::finish(&core, id, request.attempt, Ok(response.clone())).unwrap();
    assert_eq!(core.state().unwrap().watches.len(), 1);
    assert!(
        core.state()
            .unwrap()
            .domains
            .values()
            .all(|domain| domain.token.is_empty())
    );
    watch::finish(&core, id, retry.attempt, Ok(response)).unwrap();
    assert_eq!(core.state().unwrap().watches.len(), 0);
    assert!(
        core.state()
            .unwrap()
            .domains
            .values()
            .all(|domain| domain.token.as_ref() == b"current-attempt")
    );
}

#[tokio::test]
async fn stale_watch_and_partial_response_cannot_clear_dirty_or_claim_unchanged() {
    let client = client();
    let core = core(&client, 2, 256, 8 * 1024 * 1024);
    let (id, request, _, _, permit) = watch::next(&core).unwrap().unwrap();
    assert!(
        watch::finish(
            &core,
            id,
            request.attempt,
            Ok(public::GroupWatchResult::default())
        )
        .is_err()
    );
    assert_eq!(core.state().unwrap().watches.len(), 1);
    assert!(
        core.state()
            .unwrap()
            .domains
            .values()
            .all(|domain| domain.dirty && domain.token.is_empty())
    );
    core.info.lock().unwrap().assignment_version = 2;
    let result = public::GroupWatchResult {
        targets: request
            .targets
            .iter()
            .map(|target| public::WatchResult {
                shard_id: target.shard_id.clone(),
                status: public::WatchStatus::WatchChanged.into(),
                observed_token: Bytes::from_static(b"old-plan"),
                ..Default::default()
            })
            .collect(),
    };
    watch::finish(&core, id, request.attempt, Ok(result)).unwrap();
    assert!(
        core.state()
            .unwrap()
            .domains
            .values()
            .all(|domain| domain.token.is_empty())
    );
    drop(permit);
}

#[tokio::test]
async fn fallback_and_hot_targets_use_one_shared_fair_rotation() {
    let client = client();
    let core = core(&client, 12, 256, 8 * 1024 * 1024);
    let ordered: Vec<_> = core.state().unwrap().domains.keys().copied().collect();
    for domain in core.state().unwrap().domains.values_mut() {
        domain.dirty = false;
        domain.next_fallback = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
    }
    scheduling::enqueue_fallbacks(&core).unwrap();
    for expected in ordered {
        let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
        assert_eq!(id, expected);
        drop(permit);
        scheduling::finish_read(
            &core,
            id,
            &read,
            Ok(ReadProgress {
                empty: true,
                complete: false,
            }),
        )
        .unwrap();
        let mut state = core.state().unwrap();
        let hot = state.domains.values_mut().next().unwrap();
        hot.dirty = true;
        hot.next_poll = Instant::now();
    }
}

#[tokio::test]
async fn another_consumer_cannot_borrow_shared_protected_bytes_or_frame_capacity() {
    let client = client();
    let first = core(&client, 1, 256, 8 * 1024 * 1024);
    let second = core(&client, 1, 256, 8 * 1024 * 1024);
    let ordinary = client
        .inner
        .bytes
        .clone()
        .try_acquire_many_owned(client.inner.bytes.available_permits() as u32)
        .unwrap();
    let executions = client
        .inner
        .admission
        .execution
        .clone()
        .try_acquire_many_owned(client.inner.admission.execution.available_permits() as u32)
        .unwrap();
    let frames = client
        .inner
        .admission
        .frames
        .clone()
        .try_acquire_many_owned(client.inner.admission.frames.available_permits() as u32)
        .unwrap();
    assert!(
        first
            .credit
            .reserve(&client, Pool::Ordinary, 1)
            .unwrap()
            .is_none()
    );
    let first_credit = first
        .credit
        .reserve(&client, Pool::Protected, 1)
        .unwrap()
        .unwrap();
    let first_attempt = first
        .credit
        .attempt(&client, Pool::Protected, false, TrafficClass::Delivery)
        .unwrap()
        .unwrap();
    let second_credit = second
        .credit
        .reserve(&client, Pool::Protected, 1)
        .unwrap()
        .unwrap();
    let second_attempt = second
        .credit
        .attempt(&client, Pool::Protected, false, TrafficClass::Delivery)
        .unwrap()
        .unwrap();
    assert_eq!(first_credit.slots.num_permits(), 1);
    assert_eq!(second_credit.slots.num_permits(), 1);
    assert!(
        first
            .credit
            .attempt(&client, Pool::Protected, true, TrafficClass::Delivery)
            .unwrap()
            .is_none()
    );
    drop((
        first_credit,
        second_credit,
        first_attempt,
        second_attempt,
        ordinary,
        executions,
        frames,
    ));
}

fn assert_credit_ledger(core: &Core, retained: &[ReceivedMessage]) {
    let mut slots = [0_usize; 2];
    let mut memory = [0_usize; 2];
    let mut client_memory = [0_usize; 2];
    let state = core.state().unwrap();
    for domain in state.domains.values() {
        if let Some(read) = &domain.read {
            let read = read.lock().unwrap();
            if let Some(credit) = &read.credit {
                let pool = usize::from(read.pool == Pool::Protected);
                slots[pool] += credit.slots.num_permits();
                memory[pool] += credit.memory.num_permits();
                client_memory[pool] += credit.client.num_permits();
            }
        }
    }
    let mut seen = std::collections::BTreeSet::new();
    for data in state
        .queue
        .iter()
        .chain(state.ledger.values())
        .chain(retained)
        .map(|message| &message.data)
        .chain(state.revoked.iter().map(|(_, data)| data))
    {
        if !seen.insert(Arc::as_ptr(data) as usize) {
            continue;
        }
        let pool = usize::from(data._protected.is_some());
        slots[pool] += data
            .slot
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |slot| slot.num_permits());
        memory[pool] += data._memory.num_permits();
        client_memory[pool] += data._client_memory.num_permits();
    }
    assert_eq!(
        slots[0] + core.credit.ordinary_slots.available_permits(),
        core.max_messages as usize - 1
    );
    assert_eq!(
        slots[1] + core.credit.protected_slots.available_permits(),
        1
    );
    assert_eq!(
        memory[0] + core.credit.ordinary_bytes.available_permits(),
        core.max_bytes as usize - core.credit.minimum as usize
    );
    assert_eq!(
        memory[1] + core.credit.protected_bytes.available_permits(),
        core.credit.minimum as usize
    );
    assert_eq!(
        client_memory[1]
            + core
                .credit
                .shared
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .bytes
                .available_permits(),
        core.credit.minimum as usize
    );
    let total = core.client.inner.config.max_pending_bytes as usize;
    let normal = total - (total / 8) * 2;
    assert_eq!(
        core.client.inner.bytes.available_permits()
            + client_memory[0]
            + core.credit.minimum as usize,
        normal
    );
}

#[tokio::test]
async fn credit_accounting_is_conserved_through_unknown_accept_ack_and_final_clone() {
    let client = client();
    let core = core(&client, 10, 256, 8 * 1024 * 1024);
    assert_credit_ledger(&core, &[]);
    for _ in 0..8 {
        let (id, read, attempt) = scheduling::next_read(&core, false).unwrap().unwrap();
        drop(attempt);
        unknown(&core, id, &read);
        assert_credit_ledger(&core, &[]);
    }
    let (id, read, attempt) = scheduling::next_read(&core, false).unwrap().unwrap();
    let request = read.lock().unwrap().request.clone();
    accept_read(
        &core,
        id,
        &read,
        &request,
        deliveries(&read, Bytes::from(vec![5; 262144])),
    )
    .unwrap();
    assert_credit_ledger(&core, &[]);
    let message = core.try_receive().unwrap().unwrap();
    let clone = message.clone();
    assert_credit_ledger(&core, std::slice::from_ref(&clone));
    message.data.release_credit().unwrap();
    core.state().unwrap().ledger.clear();
    drop(message);
    assert_credit_ledger(&core, std::slice::from_ref(&clone));
    drop(clone);
    assert_credit_ledger(&core, &[]);
    drop(attempt);
    let progress = read.lock().unwrap().progress.unwrap();
    scheduling::finish_read(&core, id, &read, Ok(progress)).unwrap();
    assert_credit_ledger(&core, &[]);
}

#[tokio::test]
async fn malformed_read_results_cannot_consume_credit_or_partially_enqueue() {
    let client = client();
    let core = core(&client, 1, 256, 8 * 1024 * 1024);
    let (id, read, attempt) = scheduling::next_read(&core, false).unwrap().unwrap();
    let request = read.lock().unwrap().request.clone();
    let valid = deliveries(&read, Bytes::from_static(b"valid"));
    let mut invalid = valid.clone();
    invalid.sequence += 1;
    assert!(accept_read(&core, id, &read, &request, invalid).is_err());
    let mut invalid = valid.clone();
    let mut bad = invalid.deliveries.first().unwrap().clone();
    bad.proof.as_mut().unwrap().mac = Bytes::new();
    invalid.deliveries.push(bad);
    assert!(accept_read(&core, id, &read, &request, invalid).is_err());
    let mut invalid = valid.clone();
    invalid
        .deliveries
        .push(invalid.deliveries.first().unwrap().clone());
    assert!(accept_read(&core, id, &read, &request, invalid).is_err());
    let invalid = deliveries(&read, Bytes::from(vec![0; 262145]));
    assert!(accept_read(&core, id, &read, &request, invalid).is_err());
    assert!(!read.lock().unwrap().applied);
    assert!(read.lock().unwrap().credit.is_some());
    assert!(core.state().unwrap().queue.is_empty());
    assert_credit_ledger(&core, &[]);
    drop(attempt);
}

#[tokio::test]
async fn short_watch_deadlines_allow_only_one_immediate_probe() {
    let mut config = crate::client::ClientConfig::new("localhost:7443");
    config.request_timeout_ms = 50;
    let client = crate::client::test_client(config);
    let core = core(&client, 100, 256, 8 * 1024 * 1024);
    let (_, request, _, _, _permit) = watch::next(&core).unwrap().unwrap();
    assert_eq!(request.wait_ms, 0);
    core.state().unwrap().next_watch = Instant::now();
    assert!(watch::next(&core).unwrap().is_none());
    assert_eq!(watch::counts(&core.state().unwrap()), (0, 1));
}

#[tokio::test]
async fn native_admission_refusal_rolls_back_only_an_original_that_was_never_submitted() {
    let client = client();
    let core = core(&client, 2, 256, 8 * 1024 * 1024);
    let admission = || Error::Rpc {
        source: tidemq_transport::rpc::Error::Admission {
            resource: "native pending",
        },
    };
    let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    drop(permit);
    scheduling::finish_read(&core, id, &read, Err(admission())).unwrap();
    drop(read);
    assert!(
        core.state()
            .unwrap()
            .domains
            .get(&id)
            .unwrap()
            .read
            .is_none()
    );
    assert_eq!(core.state().unwrap().domains.get(&id).unwrap().sequence, 1);
    assert_credit_ledger(&core, &[]);
    let (id, original, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    drop(permit);
    unknown(&core, id, &original);
    scheduling::finish_read(&core, id, &original, Err(admission())).unwrap();
    let state = core.state().unwrap();
    assert!(Arc::ptr_eq(
        state.domains.get(&id).unwrap().read.as_ref().unwrap(),
        &original
    ));
    assert!(original.lock().unwrap().credit.is_some());
}

fn observe_healthy_watch(core: &Core) {
    core.state().unwrap().next_watch = Instant::now();
    let (id, request, _, _, permit) = watch::next(core).unwrap().unwrap();
    let targets = request
        .targets
        .iter()
        .map(|target| public::WatchResult {
            shard_id: target.shard_id.clone(),
            status: public::WatchStatus::WatchUnchanged.into(),
            observed_token: target.observed_token.clone(),
            ..Default::default()
        })
        .collect();
    watch::finish(
        core,
        id,
        request.attempt,
        Ok(public::GroupWatchResult { targets }),
    )
    .unwrap();
    drop(permit);
}

#[tokio::test]
async fn healthy_watches_cannot_hide_repeated_unsubmitted_read_refusals() {
    let client = client();
    let core = core(&client, 1, 256, 8 * 1024 * 1024);
    let id = *core.state().unwrap().domains.keys().next().unwrap();
    core.state().unwrap().domains.get_mut(&id).unwrap().token =
        Bytes::from_static(b"healthy-token");
    let (_, first, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    let short_stall = Instant::now() - Duration::from_secs(5);
    core.state()
        .unwrap()
        .domains
        .get_mut(&id)
        .unwrap()
        .failed_since = Some(short_stall);
    drop(permit);
    scheduling::finish_read(
        &core,
        id,
        &first,
        Err(Error::Admission {
            resource: "native pending",
        }),
    )
    .unwrap();
    drop(first);

    core.state()
        .unwrap()
        .domains
        .get_mut(&id)
        .unwrap()
        .next_poll = Instant::now();
    let (_, second, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    observe_healthy_watch(&core);
    assert_eq!(
        core.state().unwrap().domains.get(&id).unwrap().failed_since,
        Some(short_stall)
    );
    scheduling::report_saturation(&core).unwrap();
    assert!(
        core.try_receive().unwrap().is_none(),
        "A short admission refusal was published"
    );
    let long_stall = Instant::now() - Duration::from_secs(30);
    core.state()
        .unwrap()
        .domains
        .get_mut(&id)
        .unwrap()
        .failed_since = Some(long_stall);
    drop(permit);
    scheduling::finish_read(
        &core,
        id,
        &second,
        Err(Error::Rpc {
            source: tidemq_transport::rpc::Error::Admission {
                resource: "native pending",
            },
        }),
    )
    .unwrap();
    drop(second);

    core.state()
        .unwrap()
        .domains
        .get_mut(&id)
        .unwrap()
        .next_poll = Instant::now();
    let (_, third, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    observe_healthy_watch(&core);
    assert_eq!(
        core.state().unwrap().domains.get(&id).unwrap().failed_since,
        Some(long_stall)
    );
    scheduling::report_saturation(&core).unwrap();
    assert_eq!(
        core.try_receive().unwrap_err().kind(),
        crate::ErrorKind::ResourceExhausted
    );
    scheduling::report_saturation(&core).unwrap();
    assert!(
        core.try_receive().unwrap().is_none(),
        "The shared reporting throttle was bypassed"
    );
    drop(permit);
    scheduling::finish_read(
        &core,
        id,
        &third,
        Err(Error::Rpc {
            source: tidemq_transport::rpc::Error::Deadline {
                may_have_been_sent: false,
            },
        }),
    )
    .unwrap();
    drop(third);

    core.state()
        .unwrap()
        .domains
        .get_mut(&id)
        .unwrap()
        .next_poll = Instant::now();
    let (_, recovered, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    assert_eq!(
        core.state().unwrap().domains.get(&id).unwrap().failed_since,
        Some(long_stall)
    );
    drop(permit);
    scheduling::finish_read(
        &core,
        id,
        &recovered,
        Ok(ReadProgress {
            empty: true,
            complete: false,
        }),
    )
    .unwrap();
    drop(recovered);
    scheduling::report_saturation(&core).unwrap();
    assert!(core.try_receive().unwrap().is_none());
    assert!(core.state().unwrap().saturated_since.is_none());
    assert!(
        core.state()
            .unwrap()
            .domains
            .get(&id)
            .unwrap()
            .failed_since
            .is_none()
    );
    assert_credit_ledger(&core, &[]);
}

#[tokio::test]
async fn retryable_watch_target_preserves_token_and_does_not_fail_business_receive() {
    let client = client();
    let core = core(&client, 1, 256, 8 * 1024 * 1024);
    for domain in core.state().unwrap().domains.values_mut() {
        domain.token = Bytes::from_static(b"known-token");
        domain.dirty = false;
    }
    let (id, request, _, _, _permit) = watch::next(&core).unwrap().unwrap();
    watch::finish(
        &core,
        id,
        request.attempt,
        Ok(public::GroupWatchResult {
            targets: request
                .targets
                .iter()
                .map(|target| public::WatchResult {
                    shard_id: target.shard_id.clone(),
                    status: public::WatchStatus::WatchRetry.into(),
                    error_code: tidemq_protocol::common::ErrorCode::Unavailable.into(),
                    retryable: true,
                    observed_token: Bytes::new(),
                })
                .collect(),
        }),
    )
    .unwrap();
    assert!(core.try_receive().unwrap().is_none());
    assert!(
        core.state()
            .unwrap()
            .domains
            .values()
            .all(|domain| domain.token.as_ref() == b"known-token"
                && !domain.dirty
                && domain.read.is_none()
                && domain.sequence == 1)
    );
}

#[tokio::test]
async fn saturated_consumer_admission_rotates_across_consumers_and_rolls_back_failed_registration()
{
    use crate::client::admission::{Lane, frame_bytes};
    let client = client();
    let first = Id::new_v7().unwrap();
    let second = Id::new_v7().unwrap();
    let admission = &client.inner.admission;
    assert!(
        admission
            .fair::<()>(first, Lane::Read, || Ok(None))
            .unwrap()
            .is_none()
    );
    assert!(
        admission
            .fair::<()>(second, Lane::Read, || Ok(Some(())))
            .unwrap()
            .is_none()
    );
    assert!(
        admission
            .fair::<()>(first, Lane::Read, || Ok(Some(())))
            .unwrap()
            .is_some()
    );
    assert!(
        admission
            .fair::<()>(first, Lane::Read, || Ok(Some(())))
            .unwrap()
            .is_none()
    );
    assert!(
        admission
            .fair::<()>(second, Lane::Read, || Ok(Some(())))
            .unwrap()
            .is_some()
    );
    admission.forget(first);
    admission.forget(second);
    let before = client.inner.bytes.available_permits();
    let request_before = client.inner.requests.available_permits();
    let held = admission
        .frames
        .clone()
        .try_acquire_many_owned(admission.frames.available_permits() as u32)
        .unwrap();
    assert!(
        admission
            .protect(
                &client.inner.bytes,
                &client.inner.requests,
                262656,
                frame_bytes(&client.inner.config, TrafficClass::Delivery)
            )
            .is_err()
    );
    assert_eq!(client.inner.bytes.available_permits(), before);
    assert_eq!(client.inner.requests.available_permits(), request_before);
    drop(held);
}
