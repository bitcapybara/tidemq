//! Real QUIC boundaries; this peer does not model Raft or server authority.
use super::*;
use prost::Message;
use std::sync::atomic::AtomicBool;
mod acknowledgements;
mod hints;
use tidemq_protocol::{
    common::Frame,
    framing::{decode_payload, new_frame},
};
use tidemq_transport::{
    Server, ServerAuthorization, ServerTls, Session, TlsIdentity, TokenGrant, TransportConfig,
    budget::{MemoryBudget, MemoryLimits},
};

const TOKEN: &str = "sdk-watch-test-token-with-at-least-32-characters";

#[derive(Default)]
struct Oracle {
    read_unknown: AtomicBool,
    retire_unknown: AtomicBool,
    watch_unknown: AtomicBool,
    watch_drop: AtomicBool,
    watch_error: Mutex<Option<tidemq_protocol::common::ErrorCode>>,
    ack_error: Mutex<Option<tidemq_protocol::common::ErrorReply>>,
    watch_dropped: AtomicU64,
    watch_refused: AtomicU64,
    layout_disconnect: AtomicBool,
    layout_disconnected: AtomicU64,
    layout_completed: AtomicU64,
    layout: Mutex<Option<public::TopicLayoutResult>>,
    frames: Mutex<Vec<Frame>>,
    late: Notify,
    watch_ids: Mutex<BTreeMap<Id, u64>>,
    drop_member: Mutex<Option<Id>>,
    delivery_sessions: Mutex<Vec<std::sync::Weak<Session>>>,
}

struct Peer {
    client: Client,
    oracle: Arc<Oracle>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn peer() -> Peer {
    let key = rcgen::KeyPair::generate().unwrap();
    let certificate = rcgen::CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let ca = Bytes::from(certificate.pem());
    let tls = TlsIdentity::new(ca.clone(), Bytes::from(key.serialize_pem())).unwrap();
    let mut server = Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        ServerTls {
            identity: tls,
            client_trust_roots: None,
        },
        ServerAuthorization::External(vec![
            TokenGrant::new(
                TOKEN,
                "sdk-test".into(),
                "sdk-test".into(),
                vec![
                    FrameKind::GroupRead,
                    FrameKind::GroupRetireRead,
                    FrameKind::GroupAck,
                    FrameKind::GroupRetireAck,
                    FrameKind::GroupWatch,
                    FrameKind::GroupWatchCancel,
                    FrameKind::ConsumerControl,
                    FrameKind::GetTopic,
                    FrameKind::GetTopicLayout,
                    FrameKind::Cancel,
                ],
            )
            .unwrap(),
        ]),
        TransportConfig::default(),
        MemoryBudget::new(MemoryLimits {
            total_bytes: 64 * 1024 * 1024,
            control_reserved_bytes: 8 * 1024 * 1024,
        })
        .unwrap(),
    )
    .unwrap();
    let endpoint = format!("localhost:{}", server.local_addr().port());
    let oracle = Arc::new(Oracle::default());
    let observe = oracle.clone();
    let task = tokio::spawn(async move {
        let mut sessions = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                session = server.accept() => match session {
                    Some(session) => {
                        assert!(sessions.len() < 16);
                        sessions.spawn(serve(session, observe.clone()));
                    }
                    None => break,
                },
                _ = sessions.join_next(), if !sessions.is_empty() => {},
            }
        }
    });
    let client = Client::builder(
        &endpoint,
        crate::TlsTrust::ca_pem(ca),
        crate::Credentials::token(TOKEN),
    )
    .build()
    .await
    .unwrap();
    Peer {
        client,
        oracle,
        task,
    }
}

async fn serve(session: Arc<Session>, oracle: Arc<Oracle>) {
    if session.connection().class() == TrafficClass::Delivery {
        let mut sessions = oracle.delivery_sessions.lock().unwrap();
        assert!(sessions.len() < 16);
        sessions.push(Arc::downgrade(&session));
    }
    let mut streams = tokio::task::JoinSet::new();
    for stream in session.take_initial_streams().await {
        streams.spawn(serve_stream(stream, oracle.clone()));
    }
    loop {
        tokio::select! {
            stream = session.accept() => match stream {
                Some(stream) => {
                    assert!(streams.len() < 32);
                    streams.spawn(serve_stream(stream, oracle.clone()));
                }
                None => break,
            },
            _ = streams.join_next(), if !streams.is_empty() => {},
        }
    }
}

async fn serve_stream(stream: tidemq_transport::FramedStream, oracle: Arc<Oracle>) {
    let (mut sender, mut receiver) = stream.split();
    while let Ok(Some(frame)) = receiver
        .receive(Instant::now() + Duration::from_secs(10))
        .await
    {
        let frame = frame.frame().clone();
        {
            let mut frames = oracle.frames.lock().unwrap();
            assert!(frames.len() < 1024);
            frames.push(frame.clone());
        }
        let kind = FrameKind::try_from(frame.kind).unwrap();
        let response = match kind {
            FrameKind::GetTopic => {
                if oracle.layout_disconnect.load(Ordering::Acquire) {
                    oracle.layout_disconnected.fetch_add(1, Ordering::Release);
                    return;
                }
                let layout = oracle.layout.lock().unwrap();
                reply(
                    &frame,
                    FrameKind::TopicResult,
                    &public::TopicResult {
                        topic_id: layout.as_ref().unwrap().topic_id.clone(),
                        name: "test/app/events".into(),
                    },
                )
            }
            FrameKind::GetTopicLayout => {
                oracle.layout_completed.fetch_add(1, Ordering::Release);
                reply(
                    &frame,
                    FrameKind::TopicLayoutResult,
                    oracle.layout.lock().unwrap().as_ref().unwrap(),
                )
            }
            FrameKind::GroupRead => {
                let read: public::GroupRead = decode_payload(
                    frame.payload.clone(),
                    &tidemq_protocol::ProtocolLimits::default(),
                )
                .unwrap();
                if oracle
                    .drop_member
                    .lock()
                    .unwrap()
                    .is_some_and(|member| read.member_id.as_ref() == member.as_bytes())
                {
                    continue;
                }
                if oracle.read_unknown.swap(false, Ordering::AcqRel) {
                    oracle.late.notified().await;
                }
                reply(
                    &frame,
                    FrameKind::GroupDeliveries,
                    &public::GroupDeliveries {
                        sequence: read.sequence,
                        assignment_version: 2,
                        sealed_complete: false,
                        deliveries: vec![public::GroupDelivery {
                            proof: Some(public::GroupProof {
                                offset: read.sequence.saturating_sub(1),
                                attempt: 1,
                                mac: Bytes::from(vec![1; 32]),
                            }),
                            body: Bytes::from_static(b"data"),
                            current_attempt: true,
                            ..Default::default()
                        }],
                    },
                )
            }
            FrameKind::GroupRetireRead => {
                if oracle.retire_unknown.load(Ordering::Acquire) {
                    continue;
                }
                reply(&frame, FrameKind::GroupRetired, &public::GroupRetired {})
            }
            FrameKind::GroupAck => {
                let refusal = oracle.ack_error.lock().unwrap().clone();
                match refusal {
                    Some(refusal) => reply(&frame, FrameKind::Error, &refusal),
                    None => reply(
                        &frame,
                        FrameKind::GroupAckResult,
                        &public::GroupAckResult {
                            redelivery_scheduled: false,
                            due_ms: 0,
                        },
                    ),
                }
            }
            FrameKind::GroupRetireAck => {
                reply(&frame, FrameKind::GroupRetired, &public::GroupRetired {})
            }
            FrameKind::GroupWatch => {
                let watch: public::GroupWatch = decode_payload(
                    frame.payload.clone(),
                    &tidemq_protocol::ProtocolLimits::default(),
                )
                .unwrap();
                oracle
                    .watch_ids
                    .lock()
                    .unwrap()
                    .insert(Id::from_slice(&frame.operation_id).unwrap(), watch.attempt);
                if oracle.watch_drop.load(Ordering::Acquire) {
                    oracle.watch_dropped.fetch_add(1, Ordering::Release);
                    continue;
                }
                let watch_error = *oracle.watch_error.lock().unwrap();
                if let Some(code) = watch_error {
                    oracle.watch_refused.fetch_add(1, Ordering::Release);
                    let error = tidemq_protocol::common::ErrorReply {
                        code: code.into(),
                        outcome: tidemq_protocol::common::Outcome::Rejected.into(),
                        retryable: code == tidemq_protocol::common::ErrorCode::QuotaExceeded,
                        message: "Watch observation was refused".into(),
                        ..Default::default()
                    };
                    sender
                        .send(
                            &reply(&frame, FrameKind::Error, &error),
                            Instant::now() + Duration::from_secs(5),
                        )
                        .await
                        .unwrap();
                    continue;
                }
                if oracle.watch_unknown.load(Ordering::Acquire) {
                    let error = tidemq_protocol::common::ErrorReply {
                        code: tidemq_protocol::common::ErrorCode::Unavailable.into(),
                        outcome: tidemq_protocol::common::Outcome::Unknown.into(),
                        retryable: true,
                        message: "Watch reply is uncertain".into(),
                        ..Default::default()
                    };
                    let response = reply(&frame, FrameKind::Error, &error);
                    sender
                        .send(&response, Instant::now() + Duration::from_secs(5))
                        .await
                        .unwrap();
                    continue;
                }
                reply(
                    &frame,
                    FrameKind::GroupWatchResult,
                    &public::GroupWatchResult {
                        targets: watch
                            .targets
                            .into_iter()
                            .map(|target| public::WatchResult {
                                shard_id: target.shard_id,
                                status: public::WatchStatus::WatchSync.into(),
                                observed_token: Bytes::from_static(b"watch-token"),
                                ..Default::default()
                            })
                            .collect(),
                    },
                )
            }
            FrameKind::GroupWatchCancel => {
                let cancel: public::GroupWatchCancel = decode_payload(
                    frame.payload.clone(),
                    &tidemq_protocol::ProtocolLimits::default(),
                )
                .unwrap();
                assert_ne!(frame.operation_id, cancel.operation_id);
                assert_eq!(cancel.root_operation_id, cancel.operation_id);
                let removed = oracle
                    .watch_ids
                    .lock()
                    .unwrap()
                    .remove(&Id::from_slice(&cancel.operation_id).unwrap());
                assert_eq!(removed, Some(cancel.attempt));
                reply(
                    &frame,
                    FrameKind::GroupWatchCancelled,
                    &public::GroupWatchCancelled {},
                )
            }
            FrameKind::ConsumerControl => reply(
                &frame,
                FrameKind::ConsumerControlResult,
                &public::ConsumerControlResult {
                    member_status: 4,
                    plan_active: true,
                    ..Default::default()
                },
            ),
            FrameKind::Cancel => continue,
            other => panic!("Unexpected SDK frame: {other:?}"),
        };
        if sender
            .send(&response, Instant::now() + Duration::from_secs(5))
            .await
            .is_err()
        {
            break;
        }
    }
}

fn reply<M: Message>(frame: &Frame, kind: FrameKind, payload: &M) -> Frame {
    new_frame(
        kind,
        TrafficClass::try_from(frame.traffic_class).unwrap(),
        Id::from_slice(&frame.request_id).unwrap(),
        Id::from_slice(&frame.operation_id).unwrap(),
        payload,
        5000,
        &tidemq_protocol::ProtocolLimits::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn finite_quic_attempt_releases_execution_and_discards_late_original_response() {
    let peer = peer().await;
    let core = core(&peer.client, 1, 256, 8 * 1024 * 1024);
    let execution = peer.client.inner.admission.execution.available_permits();
    let frames = peer.client.inner.admission.frames.available_permits();
    peer.oracle.read_unknown.store(true, Ordering::Release);
    let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    let request = read.lock().unwrap().request.clone();
    let operation = read.lock().unwrap().operation;
    let original_deadline = read.lock().unwrap().deadline;
    let error = poll_read(&core, id, read.clone(), permit)
        .await
        .unwrap_err();
    assert!(error.outcome_unknown());
    assert_eq!(
        execution,
        peer.client.inner.admission.execution.available_permits()
    );
    assert_eq!(
        frames,
        peer.client.inner.admission.frames.available_permits()
    );
    scheduling::finish_read(&core, id, &read, Err(error)).unwrap();
    assert!(read.lock().unwrap().credit.is_some());
    core.state()
        .unwrap()
        .domains
        .get_mut(&id)
        .unwrap()
        .next_poll = Instant::now();
    let (recovered, same, permit) = scheduling::next_read(&core, true).unwrap().unwrap();
    assert_eq!(recovered, id);
    assert!(Arc::ptr_eq(&same, &read));
    let progress = poll_read(&core, id, same.clone(), permit).await.unwrap();
    scheduling::finish_read(&core, id, &read, Ok(progress)).unwrap();
    peer.oracle.late.notify_one();
    assert_eq!(core.state().unwrap().queue.len(), 1);
    assert_eq!(read.lock().unwrap().request, request);
    assert_eq!(read.lock().unwrap().operation, operation);
    assert_eq!(read.lock().unwrap().deadline, original_deadline);
    {
        let history = peer.oracle.frames.lock().unwrap();
        let requests: Vec<_> = history
            .iter()
            .filter(|frame| frame.kind == i32::from(FrameKind::GroupRead))
            .collect();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests.first().unwrap().operation_id,
            requests.last().unwrap().operation_id
        );
        assert_ne!(
            requests.first().unwrap().request_id,
            requests.last().unwrap().request_id
        );
        assert_eq!(
            requests.first().unwrap().payload,
            requests.last().unwrap().payload
        );
        assert!(
            requests
                .iter()
                .all(|frame| frame.remaining_timeout_ms <= 1000)
        );
    }
    core.stop.cancel();
    finish_joined(&core).await;
    assert_eq!(
        core.state()
            .unwrap()
            .closed
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .unresolved_reads(),
        0
    );
    peer.client.close().await.unwrap();
}

#[tokio::test]
async fn quic_retirement_recovery_only_retires_and_close_cancels_watch_resources() {
    let peer = peer().await;
    let bytes_before = peer.client.inner.bytes.available_permits();
    let resources_before = peer.client.inner.resource_memory.available_permits();
    let core = core(&peer.client, 1, 256, 8 * 1024 * 1024);
    peer.oracle.retire_unknown.store(true, Ordering::Release);
    let (id, read, permit) = scheduling::next_read(&core, false).unwrap().unwrap();
    let error = poll_read(&core, id, read.clone(), permit)
        .await
        .unwrap_err();
    assert!(error.outcome_unknown());
    scheduling::finish_read(&core, id, &read, Err(error)).unwrap();
    assert!(read.lock().unwrap().applied);
    assert_eq!(core.state().unwrap().queue.len(), 1);
    peer.oracle.retire_unknown.store(false, Ordering::Release);
    core.state()
        .unwrap()
        .domains
        .get_mut(&id)
        .unwrap()
        .next_poll = Instant::now();
    let (_, same, permit) = scheduling::next_read(&core, true).unwrap().unwrap();
    let progress = poll_read(&core, id, same, permit).await.unwrap();
    scheduling::finish_read(&core, id, &read, Ok(progress)).unwrap();
    assert_eq!(core.state().unwrap().queue.len(), 1);
    assert_eq!(
        peer.oracle
            .frames
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| frame.kind == i32::from(FrameKind::GroupRead))
            .count(),
        1
    );
    peer.oracle.watch_unknown.store(true, Ordering::Release);
    let (watch_id, request, operation, deadline, permit) = watch::next(&core).unwrap().unwrap();
    let result = watch::poll(&core, operation, &request, deadline, &permit).await;
    assert!(result.is_err());
    assert!(!result.as_ref().unwrap_err().outcome_unknown());
    watch::finish(&core, watch_id, request.attempt, result).unwrap();
    drop(permit);
    core.stop.cancel();
    finish_joined(&core).await;
    {
        let state = core.state().unwrap();
        let report = state.closed.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(report.unresolved_watches(), 0);
        assert_eq!(report.unresolved_reads(), 0);
        assert!(report.local_cleanup_complete());
        assert_eq!(report.members_left(), 1);
        assert!(state.domains.is_empty() && state.watches.is_empty() && state.queue.is_empty());
    }
    drop(read);
    assert!(peer.oracle.watch_ids.lock().unwrap().is_empty());
    assert_eq!(bytes_before, peer.client.inner.bytes.available_permits());
    assert_eq!(
        resources_before,
        peer.client.inner.resource_memory.available_permits()
    );
    peer.client.close().await.unwrap();
}

async fn native_registered(core: &Core) {
    core.credit
        .native
        .register(&core.client, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}

fn eight_unknown(core: &Core) {
    for _ in 0..8 {
        let (id, read, permit) = scheduling::next_read(core, false).unwrap().unwrap();
        assert_eq!(read.lock().unwrap().pool, Pool::Ordinary);
        drop(permit);
        unknown(core, id, &read);
    }
}

async fn protected_delivery(core: &Arc<Core>) -> ReceivedMessage {
    let (id, read, permit) = scheduling::next_read(core, false).unwrap().unwrap();
    assert_eq!(read.lock().unwrap().pool, Pool::Protected);
    let result = poll_read(core, id, read.clone(), permit).await.unwrap();
    scheduling::finish_read(core, id, &read, Ok(result)).unwrap();
    let message = core.try_receive().unwrap().unwrap();
    assert_eq!(message.payload(), b"data");
    assert_eq!(
        ack::start(core, &message, None, false).await.unwrap(),
        ack::AckReceipt::Acknowledged
    );
    message
}

#[tokio::test]
async fn native_protected_reads_survive_shared_pending_exhaustion_for_two_consumers() {
    let peer = peer().await;
    let first = core(&peer.client, 10, 256, 8 * 1024 * 1024);
    let second = core(&peer.client, 10, 256, 8 * 1024 * 1024);
    native_registered(&first).await;
    native_registered(&second).await;
    eight_unknown(&first);
    eight_unknown(&second);
    let until = Instant::now() + Duration::from_secs(10);
    let normal = peer
        .client
        .rpc(TrafficClass::Delivery, until)
        .await
        .unwrap();
    let marker = Id::new_v7().unwrap();
    *peer.oracle.drop_member.lock().unwrap() = Some(marker);
    let mut pending = Vec::new();
    for sequence in 1..=257 {
        let request = public::GroupRead {
            topic_id: bytes(first.topic_id),
            partition_id: 0,
            subscription_id: bytes(first.subscription),
            member_id: bytes(marker),
            member_epoch: 1,
            sequence,
            max_messages: 1,
            max_bytes: 262400,
        };
        let frame = peer
            .client
            .frame(
                FrameKind::GroupRead,
                TrafficClass::Delivery,
                Id::new_v7().unwrap(),
                Id::new_v7().unwrap(),
                &request,
                until,
            )
            .unwrap();
        match normal.submit(frame, until).await {
            Ok(response) => pending.push(response),
            Err(tidemq_transport::rpc::Error::Admission { .. }) => break,
            Err(error) => panic!("Unexpected native admission error: {error:?}"),
        }
    }
    assert_eq!(pending.len(), 254);
    assert_eq!(normal.snapshot().pending_responses, Some(254));
    for core in [&first, &second] {
        let message = protected_delivery(core).await;
        let cloned = message.clone();
        drop(message);
        assert!(diagnostics(core).unwrap().protected_blocked);
        drop(cloned);
        drop(protected_delivery(core).await);
        assert_eq!(diagnostics(core).unwrap().ordinary_unknown, 8);
        assert!(!normal.is_closed());
    }
    drop(pending);
    for core in [&first, &second] {
        core.stop.cancel();
        finish_joined(core).await;
        assert_eq!(
            core.state()
                .unwrap()
                .closed
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .unresolved_reads(),
            0
        );
    }
    assert!(
        !normal.is_closed(),
        "Private shutdown closed the shared Delivery session"
    );
    let registration = normal.reserve_protected().unwrap();
    drop(registration);
    assert_eq!(normal.snapshot().pending_responses, Some(0));
    peer.client.close().await.unwrap();
}

#[tokio::test]
async fn native_private_reopen_keeps_registration_and_drops_old_lane_before_replacement() {
    let peer = peer().await;
    let core = core(&peer.client, 10, 256, 8 * 1024 * 1024);
    native_registered(&core).await;
    eight_unknown(&core);
    let until = Instant::now() + Duration::from_secs(5);
    let normal = peer
        .client
        .rpc(TrafficClass::Delivery, until)
        .await
        .unwrap();
    let old = core.credit.native.rpc(&peer.client, until).await.unwrap();
    old.shutdown(until).await;
    assert!(!normal.is_closed());
    drop(old);
    drop(protected_delivery(&core).await);
    let retained = protected_delivery(&core).await;
    core.stop.cancel();
    finish_joined(&core).await;
    assert!(!normal.is_closed());
    let next_registration = normal.reserve_protected().unwrap();
    drop(next_registration);
    // A host-held body keeps SDK credit, independently of the closed native registration.
    assert!(peer.client.metrics().snapshot().active_deliveries > 0);
    drop(retained);
    assert_eq!(peer.client.metrics().snapshot().active_deliveries, 0);
    peer.client.close().await.unwrap();
}

#[tokio::test]
async fn native_capacity_survives_a_physical_delivery_reconnect() {
    let peer = peer().await;
    let core = core(&peer.client, 10, 256, 8 * 1024 * 1024);
    native_registered(&core).await;
    eight_unknown(&core);
    let until = Instant::now() + Duration::from_secs(5);
    let old_normal = peer
        .client
        .rpc(TrafficClass::Delivery, until)
        .await
        .unwrap();
    let old_private = core.credit.native.rpc(&peer.client, until).await.unwrap();
    let server_session = tokio::time::timeout_at(until, async {
        loop {
            let session = peer
                .oracle
                .delivery_sessions
                .lock()
                .unwrap()
                .iter()
                .find_map(std::sync::Weak::upgrade);
            if let Some(session) = session {
                break session;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server_session.close();
    tokio::time::timeout_at(until, async {
        while !old_normal.is_closed() || !old_private.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(old_private);
    drop(protected_delivery(&core).await);
    let current = peer
        .client
        .rpc(TrafficClass::Delivery, until)
        .await
        .unwrap();
    assert!(!current.same_instance(&old_normal));
    assert!(!current.is_closed());
    assert_eq!(diagnostics(&core).unwrap().ordinary_unknown, 8);
    core.stop.cancel();
    finish_joined(&core).await;
    assert!(!current.is_closed());
    let capacity = current.reserve_protected().unwrap();
    drop(capacity);
    peer.client.close().await.unwrap();
}
