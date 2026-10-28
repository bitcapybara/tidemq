use super::*;

#[tokio::test]
async fn late_empty_read_and_unknown_retirement_preserve_identity_and_next_delivery() {
    let peer = peer().await;
    let core = core(&peer.client, 1, 256, 8 * 1024 * 1024);
    peer.oracle.empty_first_read.store(true, Ordering::Release);
    peer.oracle.read_unknown.store(true, Ordering::Release);
    peer.oracle.retire_unknown.store(true, Ordering::Release);
    let free_slots = core.credit.ordinary_slots.available_permits();
    let (shard, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    let (operation, original, original_deadline) = {
        let read = read.lock().unwrap();
        (read.operation, read.request.clone(), read.deadline)
    };
    let error = poll_read(&core, shard, read.clone(), permit)
        .await
        .unwrap_err();
    assert!(error.outcome_unknown());
    scheduling::finish_read(&core, shard, &read, Err(error)).unwrap();
    let (same, permit) = recover(&core, shard, &read);
    let error = poll_read(&core, shard, same, permit).await.unwrap_err();
    assert!(error.outcome_unknown());
    scheduling::finish_read(&core, shard, &read, Err(error)).unwrap();
    let observed = diagnostics(&core).unwrap();
    assert_eq!(observed.sample_read_operation, Some(operation));
    assert_eq!(observed.sample_read_epoch, Some(original.member_epoch));
    assert_eq!(observed.sample_read_sequence, Some(original.sequence));
    assert!(observed.sample_read_applied && observed.sample_read_retiring);
    assert_eq!(observed.sample_read_failures, 2);
    assert!(observed.sample_read_last_error.is_some());
    assert_eq!(observed.sample_read_remote_code, None);
    assert_eq!(observed.queued_deliveries, 0);
    assert_eq!(observed.ordinary_slots_free, free_slots);

    peer.oracle.late.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        while peer.oracle.late_read_replies.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (watch_id, request, watch_operation, until, permit) = watch::next(&core).unwrap().unwrap();
    let result = watch::poll(&core, watch_operation, &request, until, &permit).await;
    watch::finish(&core, watch_id, request.attempt, result).unwrap();
    drop(permit);
    assert!(core.state().unwrap().domains.get(&shard).unwrap().dirty);
    peer.oracle.retire_unknown.store(false, Ordering::Release);
    // Recovery must finish the original mutation even while a fresh lease is unavailable.
    *core.lease_until.lock().unwrap() = Instant::now();
    assert!(!diagnostics(&core).unwrap().lease_live);
    let (same, permit) = recover(&core, shard, &read);
    let progress = poll_read(&core, shard, same, permit).await.unwrap();
    scheduling::finish_read(&core, shard, &read, Ok(progress)).unwrap();
    assert!(core.state().unwrap().domains.get(&shard).unwrap().dirty);
    assert_eq!(read.lock().unwrap().deadline, original_deadline);
    assert_eq!(read.lock().unwrap().request, original);
    assert_eq!(read.lock().unwrap().operation, operation);
    assert_original_frames(&peer, operation);

    assert!(scheduling::next_read(&core, false).unwrap().is_none());
    let mut status = core.info.lock().unwrap().clone();
    status.subscription_id = bytes(core.subscription);
    core.update_info(status, Instant::now()).unwrap();
    assert_eq!(core.epoch.load(Ordering::Acquire), original.member_epoch);
    assert!(diagnostics(&core).unwrap().lease_live);
    core.state()
        .unwrap()
        .domains
        .get_mut(&shard)
        .unwrap()
        .next_poll = Instant::now();
    let (next_shard, next_read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    assert_eq!(next_shard, shard);
    assert_eq!(
        next_read.lock().unwrap().request.sequence,
        original.sequence + 1
    );
    let progress = poll_read(&core, shard, next_read.clone(), permit)
        .await
        .unwrap();
    scheduling::finish_read(&core, shard, &next_read, Ok(progress)).unwrap();
    let message = core.try_receive().unwrap().unwrap();
    assert_eq!(message.payload(), b"data");
    assert_eq!(message.message_id().offset, 1);
    assert_eq!(message.data.epoch, original.member_epoch);
    assert!(core.try_receive().unwrap().is_none());
    assert_eq!(
        ack::start(&core, &message, None, false).await.unwrap(),
        ack::AckReceipt::Acknowledged
    );
    assert_eq!(diagnostics(&core).unwrap().delivered_unacked, 0);
    drop(message);
    drop((read, next_read));
    core.stop.cancel();
    finish_joined(&core).await;
    peer.client.close().await.unwrap();
}

#[tokio::test]
async fn explicit_read_quota_refusal_keeps_its_original_identity_and_visible_category() {
    use tidemq_protocol::common::{ErrorCode, ErrorReply, Outcome};
    let peer = peer().await;
    let core = core(&peer.client, 1, 256, 8 * 1024 * 1024);
    *peer.oracle.read_error.lock().unwrap() = Some(ErrorReply {
        code: ErrorCode::QuotaExceeded.into(),
        outcome: Outcome::Rejected.into(),
        retryable: true,
        message: "An independent read admission budget refused this attempt".into(),
        ..Default::default()
    });
    let (shard, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    let (original, operation, original_deadline) = {
        let read = read.lock().unwrap();
        (read.request.clone(), read.operation, read.deadline)
    };
    let error = poll_read(&core, shard, read.clone(), permit)
        .await
        .unwrap_err();
    assert!(!error.outcome_unknown());
    assert!(no_work(&error));
    scheduling::finish_read(&core, shard, &read, Err(error)).unwrap();
    let snapshot = diagnostics(&core).unwrap();
    assert_eq!(snapshot.sample_read_operation, Some(operation));
    assert_eq!(snapshot.sample_read_failures, 1);
    assert_eq!(
        snapshot.sample_read_last_error,
        Some(crate::ErrorKind::ResourceExhausted)
    );
    assert_eq!(snapshot.sample_read_error_source, Some("remote"));
    assert_eq!(
        snapshot.sample_read_remote_code,
        Some(ErrorCode::QuotaExceeded.into())
    );
    assert!(!snapshot.sample_read_applied && !snapshot.sample_read_retiring);
    assert!(read.lock().unwrap().credit.is_some());
    assert!(core.try_receive().unwrap().is_none());
    *peer.oracle.read_error.lock().unwrap() = None;
    let (same, permit) = recover(&core, shard, &read);
    let progress = poll_read(&core, shard, same, permit).await.unwrap();
    scheduling::finish_read(&core, shard, &read, Ok(progress)).unwrap();
    assert_eq!(read.lock().unwrap().request, original);
    assert_eq!(read.lock().unwrap().deadline, original_deadline);
    let message = core.try_receive().unwrap().unwrap();
    assert_eq!(message.payload(), b"data");
    assert_eq!(
        ack::start(&core, &message, None, false).await.unwrap(),
        ack::AckReceipt::Acknowledged
    );
    drop((message, read));
    core.stop.cancel();
    finish_joined(&core).await;
    peer.client.close().await.unwrap();
}

fn recover(
    core: &Core,
    shard: Id,
    original: &Arc<Mutex<ReadAttempt>>,
) -> (Arc<Mutex<ReadAttempt>>, crate::client::admission::Attempt) {
    core.state()
        .unwrap()
        .domains
        .get_mut(&shard)
        .unwrap()
        .next_poll = Instant::now();
    let (id, read, permit) = scheduling::next_read(core, true).unwrap().unwrap();
    assert_eq!(id, shard);
    assert!(Arc::ptr_eq(&read, original));
    (read, permit)
}

fn assert_original_frames(peer: &Peer, operation: Id) {
    let frames = peer.oracle.frames.lock().unwrap();
    for kind in [FrameKind::GroupRead, FrameKind::GroupRetireRead] {
        let requests: Vec<_> = frames
            .iter()
            .filter(|frame| frame.kind == i32::from(kind))
            .collect();
        assert_eq!(requests.len(), 2);
        let first = requests.first().unwrap();
        let last = requests.last().unwrap();
        assert_eq!(first.operation_id.as_ref(), operation.as_bytes());
        assert_eq!(first.operation_id, last.operation_id);
        assert_eq!(first.payload, last.payload);
        assert_ne!(first.request_id, last.request_id);
    }
}
