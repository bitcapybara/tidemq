use super::*;
use tidemq_protocol::common::ErrorCode;

fn idle_core(peer: &Peer) -> Arc<Core> {
    let core = core(&peer.client, 1, 256, 8 * 1024 * 1024);
    let mut state = core.state().unwrap();
    let domain = state.domains.values_mut().next().unwrap();
    domain.dirty = false;
    domain.token = Bytes::from_static(b"baseline-token");
    *peer.oracle.layout.lock().unwrap() = Some(public::TopicLayoutResult {
        topic_id: bytes(core.topic_id),
        revision: 1,
        total_shards: 1,
        active: vec![public::ShardRoute {
            shard_id: bytes(domain.shard),
            group_id: 1,
            authority_epoch: 1,
            birth_partition: domain.partition,
            ..Default::default()
        }],
        ..Default::default()
    });
    drop(state);
    core
}

async fn observed(counter: &AtomicU64, at_least: u64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while counter.load(Ordering::Acquire) < at_least {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("The real QUIC peer did not observe the expected request");
}

async fn stop(core: Arc<Core>, worker: tokio::task::JoinHandle<()>, peer: &Peer) {
    core.stop.cancel();
    worker.await.unwrap();
    finish(core.clone(), None).await;
    {
        let state = core.state().unwrap();
        let report = state.closed.as_ref().unwrap().as_ref().unwrap();
        assert!(report.local_cleanup_complete());
        assert_eq!(report.unresolved_reads(), 0);
        assert_eq!(report.unresolved_watches(), 0);
        assert!(state.domains.is_empty() && state.watches.is_empty() && state.queue.is_empty());
    }
    peer.client.close().await.unwrap();
}

#[tokio::test]
async fn quic_hint_loss_quota_and_layout_disconnect_do_not_hijack_receive() {
    let peer = peer().await;
    let core = idle_core(&peer);
    native_registered(&core).await;
    peer.oracle.watch_drop.store(true, Ordering::Release);
    peer.oracle.layout_disconnect.store(true, Ordering::Release);
    let worker = tokio::spawn(scheduling::run(core.clone()));
    observed(&peer.oracle.watch_dropped, 1).await;
    observed(&peer.oracle.layout_disconnected, 1).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(1500), core.receive())
            .await
            .is_err(),
        "Read-only hint failures interrupted a healthy business receive"
    );
    assert_eq!(
        core.state()
            .unwrap()
            .domains
            .values()
            .next()
            .unwrap()
            .token
            .as_ref(),
        b"baseline-token"
    );
    assert_eq!(diagnostics(&core).unwrap().ordinary_reads, 0);

    peer.oracle
        .layout_disconnect
        .store(false, Ordering::Release);
    *peer.oracle.watch_error.lock().unwrap() = Some(ErrorCode::QuotaExceeded);
    peer.oracle.watch_drop.store(false, Ordering::Release);
    observed(&peer.oracle.watch_refused, 2).await;
    observed(&peer.oracle.layout_completed, 1).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), core.receive())
            .await
            .is_err(),
        "A retryable whole-frame Watch quota refusal interrupted receive"
    );
    assert_eq!(
        core.state()
            .unwrap()
            .domains
            .values()
            .next()
            .unwrap()
            .token
            .as_ref(),
        b"baseline-token"
    );

    *peer.oracle.watch_error.lock().unwrap() = None;
    let delivery = tokio::time::timeout(Duration::from_secs(5), core.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.payload(), b"data");
    assert_eq!(
        core.state()
            .unwrap()
            .domains
            .values()
            .next()
            .unwrap()
            .token
            .as_ref(),
        b"watch-token"
    );
    stop(core, worker, &peer).await;
    drop(delivery);
    assert_eq!(peer.client.metrics().snapshot().admitted_bytes, 0);
}

#[tokio::test]
async fn quic_hint_permission_denial_remains_visible_and_stops_the_consumer() {
    let peer = peer().await;
    let core = idle_core(&peer);
    *peer.oracle.watch_error.lock().unwrap() = Some(ErrorCode::Forbidden);
    let worker = tokio::spawn(scheduling::run(core.clone()));
    let error = tokio::time::timeout(Duration::from_secs(5), core.receive())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.permission_denied());
    assert!(core.stop.is_cancelled());
    stop(core, worker, &peer).await;
}

#[tokio::test]
async fn sustained_hint_saturation_remains_visible_with_bounded_reporting() {
    let peer = peer().await;
    let core = idle_core(&peer);
    peer.oracle.watch_drop.store(true, Ordering::Release);
    {
        let mut state = core.state().unwrap();
        state.domains.values_mut().next().unwrap().observed_at =
            Instant::now() - Duration::from_secs(31);
    }
    let worker = tokio::spawn(scheduling::run(core.clone()));
    let error = tokio::time::timeout(Duration::from_secs(5), core.receive())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::ResourceExhausted);
    assert!(!core.stop.is_cancelled());
    assert_eq!(diagnostics(&core).unwrap().observation_overdue, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), core.receive())
            .await
            .is_err()
    );
    stop(core, worker, &peer).await;
}

#[tokio::test]
async fn temporary_layout_unassignment_preserves_retired_sequence_on_reassignment() {
    let peer = peer().await;
    let core = idle_core(&peer);
    core.state()
        .unwrap()
        .domains
        .values_mut()
        .next()
        .unwrap()
        .dirty = true;
    let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    let first_operation = read.lock().unwrap().operation;
    let progress = poll_read(&core, id, read.clone(), permit).await.unwrap();
    scheduling::finish_read(&core, id, &read, Ok(progress)).unwrap();
    assert_eq!(core.state().unwrap().domains.get(&id).unwrap().sequence, 2);

    core.info.lock().unwrap().work_shards.clear();
    scheduling::refresh_domains(&core).await.unwrap();
    {
        let state = core.state().unwrap();
        let domain = state
            .domains
            .get(&id)
            .expect("An unassigned domain lost its retired Read identity");
        assert!(!domain.assigned);
        assert_eq!(domain.sequence, 2);
    }
    core.info.lock().unwrap().work_shards.push(bytes(id));
    scheduling::refresh_domains(&core).await.unwrap();
    let (_, next, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    assert_eq!(next.lock().unwrap().request.sequence, 2);
    assert_ne!(next.lock().unwrap().operation, first_operation);
    let progress = poll_read(&core, id, next.clone(), permit).await.unwrap();
    scheduling::finish_read(&core, id, &next, Ok(progress)).unwrap();
    assert_eq!(core.state().unwrap().queue.len(), 2);
    core.stop.cancel();
    finish(core.clone(), None).await;
    drop((read, next));
    peer.client.close().await.unwrap();
    assert_eq!(peer.client.metrics().snapshot().admitted_bytes, 0);
}
