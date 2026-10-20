//! This is a protocol peer on real UDP/TLS, not a durable broker implementation.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use prost::Message;
use rcgen::{CertificateParams, KeyPair};
use tidemq_protocol::{
    Id, ProtocolLimits,
    common::{Frame, FrameKind, TrafficClass},
    framing::{decode_payload, new_frame},
    public,
};
use tidemq_sdk::{
    Client, ClientConfig, Consumer, ConsumerConfig, DeliveryOrdering, Error, Producer,
    RequestContext,
};
use tidemq_transport::{
    ClientTls, FramedStream, PeerTarget, Server, ServerAuthorization, ServerTls, Session,
    StreamPurpose, TlsIdentity, TokenGrant, TransportConfig,
    budget::{MemoryBudget, MemoryLimits},
};
use tokio::{
    task::{JoinHandle, JoinSet},
    time::Instant,
};

const TOKEN: &str = "sdk-test-token-with-at-least-32-characters";
fn id(byte: u8) -> Id {
    Id::from_slice(&[byte; 16]).unwrap()
}
fn bytes(id: Id) -> Bytes {
    Bytes::copy_from_slice(id.as_bytes())
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[derive(Default)]
struct PeerState {
    classes: Vec<TrafficClass>,
    sessions: Vec<Id>,
    traceparents: Vec<String>,
    producers: BTreeMap<(Bytes, u64, u64), (Bytes, u64)>,
    producer_lanes: BTreeMap<Bytes, BTreeSet<u64>>,
    producer_requests: Vec<Bytes>,
    producer_attempts: Vec<(Bytes, public::ProduceBatch)>,
    reject_produces: u32,
    drop_first_produce: bool,
    drop_first_admin: bool,
    hold_registration: bool,
    registrations: Vec<public::OpenProducer>,
    hold_admin: bool,
    admin_requests: usize,
    pair_produces: bool,
    assignment: Option<public::AssignmentResult>,
    claims: Vec<public::ClaimAssignment>,
    unassigned: bool,
    reject_reads: u32,
    other_hot_key: bool,
    idempotent_claim: bool,
    drop_claim_reply: bool,
    bulk_reads: usize,
    hot_reads: usize,
    hot_offset: Option<u64>,
    barriers: Vec<u64>,
    barrier_requests: Vec<(Bytes, Bytes, public::ReceiveBarrier)>,
    barrier_unknowns: u32,
    commit_unknown_barrier: bool,
    acknowledgements: Vec<usize>,
    acknowledged_tokens: BTreeSet<Bytes>,
    seek_count: u64,
    skip_hole: bool,
    fill_credit: bool,
}

struct Peer {
    client: Client,
    state: Arc<Mutex<PeerState>>,
    task: JoinHandle<()>,
    ca: Bytes,
    endpoint: String,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn peer(drop_first_produce: bool, skip_hole: bool) -> Peer {
    let key = KeyPair::generate().unwrap();
    let certificate = CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let budget = || {
        MemoryBudget::new(MemoryLimits {
            total_bytes: 64 * 1024 * 1024,
            control_reserved_bytes: 8 * 1024 * 1024,
        })
        .unwrap()
    };
    let allowed = vec![
        FrameKind::CreateTopic,
        FrameKind::GetTopic,
        FrameKind::CreateSubscription,
        FrameKind::GetSubscription,
        FrameKind::OpenProducer,
        FrameKind::ProducerStatus,
        FrameKind::RetireProducer,
        FrameKind::ProduceBatch,
        FrameKind::SubscriptionStatus,
        FrameKind::ClaimAssignment,
        FrameKind::Read,
        FrameKind::ReceiveBarrier,
        FrameKind::AckBatch,
        FrameKind::SeekSubscription,
        FrameKind::DeleteSubscription,
        FrameKind::Cancel,
    ];
    let mut server = Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        ServerTls {
            identity: TlsIdentity::new(
                Bytes::from(certificate.pem()),
                Bytes::from(key.serialize_pem()),
            )
            .unwrap(),
            client_trust_roots: None,
        },
        ServerAuthorization::External(vec![
            TokenGrant::new(TOKEN, "sdk-test".into(), "sdk-test".into(), allowed).unwrap(),
        ]),
        TransportConfig::default(),
        budget(),
    )
    .unwrap();
    let target = PeerTarget {
        address: server.local_addr(),
        server_name: "localhost".into(),
        local_role: "sdk".into(),
        remote_role: "gateway".into(),
        cluster_id: None,
        node_id: None,
    };
    let state = Arc::new(Mutex::new(PeerState {
        drop_first_produce,
        skip_hole,
        ..PeerState::default()
    }));
    let observed = state.clone();
    let task = tokio::spawn(async move {
        let mut sessions = JoinSet::new();
        loop {
            tokio::select! {
                session = server.accept() => {
                    let Some(session) = session else { break };
                    { let mut state = observed.lock().unwrap(); state.classes.push(session.connection().class()); state.sessions.push(session.connection().session_id()); }
                    sessions.spawn(serve_session(session, observed.clone()));
                }
                _ = sessions.join_next(), if !sessions.is_empty() => {},
            }
        }
    });
    let endpoint = format!("localhost:{}", target.address.port());
    let ca = Bytes::from(certificate.pem());
    let client = Client::with_target(
        ClientConfig::new("localhost:7443"),
        ClientTls {
            trust_roots: ca.clone(),
            identity: None,
            expected_nodes: vec![],
        },
        target,
        TOKEN.into(),
    )
    .await
    .unwrap();
    Peer {
        client,
        state,
        task,
        ca,
        endpoint,
    }
}

async fn serve_session(session: Arc<Session>, state: Arc<Mutex<PeerState>>) {
    let mut streams = JoinSet::new();
    for stream in session.take_initial_streams().await {
        streams.spawn(serve_stream(stream, session.clone(), state.clone()));
    }
    loop {
        tokio::select! {
            stream = session.accept() => { let Some(stream) = stream else { break }; streams.spawn(serve_stream(stream, session.clone(), state.clone())); }
            _ = streams.join_next(), if !streams.is_empty() => {},
        }
    }
}

async fn serve_stream(stream: FramedStream, session: Arc<Session>, state: Arc<Mutex<PeerState>>) {
    let purpose = stream.purpose();
    let lane = stream.id();
    let (mut sender, mut receiver) = stream.split();
    let mut held_produce = None;
    while let Ok(Some(received)) = receiver.receive(deadline()).await {
        let request = received.frame();
        let response = handle(request, purpose, lane, &session, &mut state.lock().unwrap());
        if let Some(response) = response {
            let pair = response.kind == i32::from(FrameKind::ProduceResult)
                && state.lock().unwrap().pair_produces;
            if pair && held_produce.is_none() {
                held_produce = Some(response);
                continue;
            }
            if sender.send(&response, deadline()).await.is_err() {
                break;
            }
            if let Some(held) = held_produce.take() {
                state.lock().unwrap().pair_produces = false;
                if sender.send(&held, deadline()).await.is_err() {
                    break;
                }
            }
        }
    }
}

fn response<M: Message>(request: &Frame, kind: FrameKind, payload: &M) -> Frame {
    new_frame(
        kind,
        TrafficClass::try_from(request.traffic_class).unwrap(),
        Id::from_slice(&request.request_id).unwrap(),
        Id::from_slice(&request.operation_id).unwrap(),
        payload,
        3000,
        &ProtocolLimits::default(),
    )
    .unwrap()
}

fn fenced(request: &Frame) -> Frame {
    response(
        request,
        FrameKind::Error,
        &tidemq_protocol::common::ErrorReply {
            code: tidemq_protocol::common::ErrorCode::Fenced.into(),
            outcome: tidemq_protocol::common::Outcome::Rejected.into(),
            retryable: true,
            ..Default::default()
        },
    )
}

fn advance_owner(state: &mut PeerState, unassigned: bool) {
    let assignment = state.assignment.as_mut().unwrap();
    assignment.assignment_generation += 1;
    assignment.delivery_generation = assignment.assignment_generation;
    assignment.received_before = assignment.acknowledged_before;
    assignment.issued_before = assignment.acknowledged_before;
    state.bulk_reads = 0;
    state.hot_reads = 0;
    state.unassigned = unassigned;
}

fn handle(
    request: &Frame,
    purpose: StreamPurpose,
    lane: u64,
    session: &Session,
    state: &mut PeerState,
) -> Option<Frame> {
    state.traceparents.push(request.traceparent.clone());
    if matches!(
        FrameKind::try_from(request.kind),
        Ok(FrameKind::CreateTopic | FrameKind::GetTopic)
    ) {
        state.admin_requests += 1;
        if state.hold_admin {
            return None;
        }
    }
    if request.kind == i32::from(FrameKind::CreateTopic) && state.drop_first_admin {
        state.drop_first_admin = false;
        session.close();
        return None;
    }
    let limits = ProtocolLimits::default();
    Some(match FrameKind::try_from(request.kind).unwrap() {
        FrameKind::CreateTopic | FrameKind::GetTopic => response(
            request,
            FrameKind::TopicResult,
            &public::TopicResult {
                topic_id: bytes(id(1)),
                name: "events".into(),
                partitions: 1,
            },
        ),
        FrameKind::CreateSubscription | FrameKind::GetSubscription => response(
            request,
            FrameKind::SubscriptionResult,
            &public::SubscriptionResult {
                topic_id: bytes(id(1)),
                subscription_id: bytes(id(2)),
                cursor_generation: 1,
            },
        ),
        FrameKind::ProducerStatus => response(
            request,
            FrameKind::ProducerStatusResult,
            &public::ProducerStatusResult {
                state: public::ProducerSessionState::NotRegistered.into(),
                session: None,
                next_registration_epoch: 1,
            },
        ),
        FrameKind::OpenProducer => {
            let open: public::OpenProducer =
                decode_payload(request.payload.clone(), &limits).unwrap();
            assert_eq!(open.owner_epoch, 0);
            state.registrations.push(open.clone());
            if state.hold_registration {
                return None;
            }
            response(
                request,
                FrameKind::ProducerSessionResult,
                &public::ProducerSessionResult {
                    producer_id: open.producer_id,
                    producer_epoch: open.expected_registration_epoch,
                    next_sequence: 0,
                    expires_at_ms: u64::MAX,
                    next_registration_epoch: 2,
                },
            )
        }
        FrameKind::ProduceBatch => {
            let batch: public::ProduceBatch =
                decode_payload(request.payload.clone(), &limits).unwrap();
            assert_eq!(batch.owner_epoch, 0);
            state.producer_requests.push(request.request_id.clone());
            state
                .producer_attempts
                .push((request.operation_id.clone(), batch.clone()));
            state
                .producer_lanes
                .entry(batch.producer_id.clone())
                .or_default()
                .insert(lane);
            if state.reject_produces != 0 {
                state.reject_produces -= 1;
                return Some(response(
                    request,
                    FrameKind::Error,
                    &tidemq_protocol::common::ErrorReply {
                        code: tidemq_protocol::common::ErrorCode::Unavailable.into(),
                        outcome: tidemq_protocol::common::Outcome::Rejected.into(),
                        retryable: true,
                        ..Default::default()
                    },
                ));
            }
            let identity = (
                batch.producer_id.clone(),
                batch.producer_epoch,
                batch.first_sequence,
            );
            let duplicate = state.producers.contains_key(&identity);
            if let Some((digest, _)) = state.producers.get(&identity) {
                assert_eq!(*digest, batch.content_digest);
            }
            state
                .producers
                .entry(identity)
                .or_insert((batch.content_digest, batch.first_sequence));
            if state.drop_first_produce {
                state.drop_first_produce = false;
                return None;
            }
            response(
                request,
                FrameKind::ProduceResult,
                &public::ProduceResult {
                    first_offset: batch.first_sequence,
                    message_count: batch.messages.len() as u32,
                    duplicate,
                },
            )
        }
        FrameKind::SubscriptionStatus => {
            let previous = state
                .assignment
                .clone()
                .unwrap_or(public::AssignmentResult {
                    topic_id: bytes(id(1)),
                    subscription_id: bytes(id(2)),
                    cursor_generation: 1,
                    cold_before: 2,
                    ..public::AssignmentResult::default()
                });
            response(
                request,
                FrameKind::SubscriptionStatusResult,
                &public::SubscriptionStatusResult {
                    assignment: Some(previous),
                    assigned: state.assignment.is_some() && !state.unassigned,
                },
            )
        }
        FrameKind::ClaimAssignment => {
            let claim: public::ClaimAssignment =
                decode_payload(request.payload.clone(), &limits).unwrap();
            assert_eq!(
                claim.session_id.as_ref(),
                session.connection().session_id().as_bytes()
            );
            state.claims.push(claim.clone());
            if state.idempotent_claim
                && !state.unassigned
                && let Some(previous) = state.assignment.clone()
            {
                state.bulk_reads = 0;
                return Some(response(request, FrameKind::AssignmentResult, &previous));
            }
            let acknowledged_before = state
                .assignment
                .as_ref()
                .map(|previous| previous.acknowledged_before)
                .unwrap_or(if state.seek_count > 0 { 2 } else { 0 });
            let assignment = public::AssignmentResult {
                subscription_id: claim.subscription_id,
                topic_id: claim.topic_id,
                partition_id: claim.partition_id,
                session_id: claim.session_id,
                cursor_generation: claim.expected_cursor_generation,
                assignment_generation: claim.expected_assignment_generation + 1,
                delivery_generation: claim.expected_assignment_generation + 1,
                acknowledged_before,
                next_delivery_offset: acknowledged_before,
                cold_before: 2,
                received_before: acknowledged_before,
                issued_before: acknowledged_before,
            };
            state.assignment = Some(assignment.clone());
            state.unassigned = false;
            state.bulk_reads = 0;
            if state.drop_claim_reply {
                state.drop_claim_reply = false;
                return None;
            }
            response(request, FrameKind::AssignmentResult, &assignment)
        }
        FrameKind::Read => {
            let read: public::ReadRequest =
                decode_payload(request.payload.clone(), &limits).unwrap();
            if state.reject_reads > 0 {
                state.reject_reads -= 1;
                return Some(fenced(request));
            }
            let class = TrafficClass::try_from(request.traffic_class).unwrap();
            let assignment = state.assignment.as_mut().unwrap();
            if state.unassigned
                || read.cursor_generation != assignment.cursor_generation
                || read.assignment_generation != assignment.assignment_generation
                || read.session_id != assignment.session_id
            {
                return Some(fenced(request));
            }
            assert_eq!(read.session_id, assignment.session_id);
            let mut ranges = Vec::new();
            let offset = if class == TrafficClass::Bulk {
                state.bulk_reads += 1;
                assignment.issued_before = assignment.issued_before.max(2);
                if state.skip_hole {
                    ranges.push(public::AcknowledgedRange { start: 1, end: 2 });
                    0
                } else if state.bulk_reads == 1 {
                    1
                } else {
                    0
                }
            } else {
                assert!(assignment.received_before >= assignment.cold_before);
                state.hot_reads += 1;
                let offset = state.hot_offset.unwrap_or(2);
                assignment.issued_before = assignment.issued_before.max(offset + 1);
                offset
            };
            let delivery = public::Delivery {
                offset,
                key: if state.other_hot_key && offset == 2 {
                    Bytes::from_static(b"unrelated-key")
                } else if state.fill_credit {
                    Bytes::new()
                } else {
                    Bytes::from_static(b"same-key")
                },
                body: if state.fill_credit {
                    Bytes::from(vec![
                        b'x';
                        read.max_bytes.min(limits.max_message_bytes) as usize
                    ])
                } else {
                    Bytes::from_static(b"payload")
                },
                delivery_token: Bytes::from(vec![offset as u8; 101]),
            };
            response(
                request,
                FrameKind::DeliveryBatch,
                &public::DeliveryBatch {
                    deliveries: vec![delivery],
                    next_offset: assignment.issued_before,
                    topic_id: assignment.topic_id.clone(),
                    partition_id: assignment.partition_id,
                    subscription_id: assignment.subscription_id.clone(),
                    cursor_generation: assignment.cursor_generation,
                    assignment_generation: assignment.assignment_generation,
                    delivery_generation: assignment.delivery_generation,
                    session_id: assignment.session_id.clone(),
                    cold_before: assignment.cold_before,
                    issued_before: assignment.issued_before,
                    acknowledged_ranges: ranges,
                },
            )
        }
        FrameKind::ReceiveBarrier => {
            let barrier: public::ReceiveBarrier =
                decode_payload(request.payload.clone(), &limits).unwrap();
            state.barrier_requests.push((
                request.operation_id.clone(),
                request.request_id.clone(),
                barrier.clone(),
            ));
            let assignment = state.assignment.as_mut().unwrap();
            if state.unassigned
                || barrier.cursor_generation != assignment.cursor_generation
                || barrier.assignment_generation != assignment.assignment_generation
                || barrier.session_id != assignment.session_id
            {
                return Some(fenced(request));
            }
            assert!(barrier.received_before <= assignment.issued_before);
            let unknown = state.barrier_unknowns > 0;
            if !unknown || state.commit_unknown_barrier {
                state.barriers.push(barrier.received_before);
                assignment.received_before =
                    assignment.received_before.max(barrier.received_before);
            }
            if unknown {
                state.barrier_unknowns -= 1;
                return Some(response(
                    request,
                    FrameKind::Error,
                    &tidemq_protocol::common::ErrorReply {
                        code: tidemq_protocol::common::ErrorCode::Unavailable.into(),
                        outcome: tidemq_protocol::common::Outcome::Unknown.into(),
                        retryable: true,
                        ..Default::default()
                    },
                ));
            }
            response(request, FrameKind::AssignmentResult, assignment)
        }
        FrameKind::AckBatch => {
            assert_eq!(purpose, StreamPurpose::Ack);
            assert!(request.encoded_len() <= limits.max_control_frame_bytes as usize);
            let ack: public::AckBatch = decode_payload(request.payload.clone(), &limits).unwrap();
            let assignment = state.assignment.as_ref().unwrap();
            if ack.cursor_generation != assignment.cursor_generation
                || ack.assignment_generation != assignment.assignment_generation
                || ack.session_id != assignment.session_id
            {
                return Some(fenced(request));
            }
            state.acknowledgements.push(ack.delivery_tokens.len());
            let mut accepted = 0;
            let mut already = 0;
            for token in ack.delivery_tokens {
                if state.acknowledged_tokens.insert(token) {
                    accepted += 1;
                } else {
                    already += 1;
                }
            }
            let mut acknowledged_before = 0_u64;
            while state
                .acknowledged_tokens
                .contains(&Bytes::from(vec![acknowledged_before as u8; 101]))
            {
                acknowledged_before += 1;
            }
            if let Some(assignment) = &mut state.assignment {
                assignment.acknowledged_before = acknowledged_before;
            }
            response(
                request,
                FrameKind::AckResult,
                &public::AckResult {
                    acknowledged_before,
                    accepted,
                    already_acknowledged: already,
                },
            )
        }
        FrameKind::SeekSubscription => {
            let seek: public::SeekSubscription =
                decode_payload(request.payload.clone(), &limits).unwrap();
            state.seek_count += 1;
            state.assignment = None;
            response(
                request,
                FrameKind::SeekResult,
                &public::SeekResult {
                    cursor_generation: seek.expected_cursor_generation + 1,
                    assignment_generation: seek.expected_assignment_generation + 1,
                    delivery_generation: seek.expected_assignment_generation + 1,
                    acknowledged_before: seek.target_offset,
                },
            )
        }
        FrameKind::Cancel => return None,
        kind => panic!("Unexpected test request: {kind:?}"),
    })
}

#[tokio::test]
async fn unresolved_production_leaves_consumption_capacity_and_does_not_retain_host_backing() {
    let peer = peer(false, true).await;
    let mut config = ClientConfig::new(peer.endpoint.clone());
    config.max_pending_bytes = 1_048_576;
    let client = Client::connect(config, peer.ca.clone(), TOKEN.into())
        .await
        .unwrap();
    let mut producers = Vec::new();
    for producer_id in [3, 4] {
        let producer = Producer::open(client.clone(), id(1), 0, id(producer_id))
            .await
            .unwrap();
        peer.state.lock().unwrap().drop_first_produce = true;
        let result = RequestContext::with_timeout(Duration::from_millis(80))
            .unwrap()
            .scope(producer.produce(vec![public::Message {
                key: Bytes::new(),
                body: Bytes::from(vec![b'x'; 262_144]),
            }]))
            .await;
        assert!(matches!(result, Err(Error::ProduceUnknown { .. })));
        producers.push(producer);
    }
    let third = Producer::open(client.clone(), id(1), 0, id(5))
        .await
        .unwrap();
    assert!(matches!(
        third
            .produce(vec![public::Message {
                key: Bytes::new(),
                body: Bytes::from(vec![b'x'; 262_144])
            }])
            .await,
        Err(Error::Admission)
    ));
    assert!(third.pending_batches().await.unwrap().is_empty());
    peer.state.lock().unwrap().fill_credit = true;
    let consumer = Consumer::claim(
        client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig {
            max_buffered_bytes: 1_048_576,
            max_buffered_messages: 1,
            ..ConsumerConfig::default()
        },
    )
    .await
    .unwrap();
    consumer.fill().await.unwrap();
    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(delivery.body().len(), 262_144);
    consumer.ack(&delivery).await.unwrap();
    assert!(client.metrics().snapshot().admitted_bytes <= 1_048_576);

    struct Backing {
        bytes: Vec<u8>,
        released: Arc<std::sync::atomic::AtomicBool>,
    }
    impl AsRef<[u8]> for Backing {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Backing {
        fn drop(&mut self) {
            self.released
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let backing = Bytes::from_owner(Backing {
        bytes: vec![b'y'; 2_097_152],
        released: released.clone(),
    });
    let small = backing.slice(..4);
    drop(backing);
    let producer = Producer::open(peer.client.clone(), id(1), 0, id(6))
        .await
        .unwrap();
    peer.state.lock().unwrap().drop_first_produce = true;
    assert!(matches!(
        RequestContext::with_timeout(Duration::from_millis(80))
            .unwrap()
            .scope(producer.produce(vec![public::Message {
                key: Bytes::new(),
                body: small
            }]))
            .await,
        Err(Error::ProduceUnknown { .. })
    ));
    assert!(released.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(producer.pending_batches().await.unwrap().len(), 1);
}

#[tokio::test]
async fn consumer_credit_includes_metadata_and_last_handle_releases_its_claim() {
    let peer = peer(false, true).await;
    peer.state.lock().unwrap().fill_credit = true;
    let config = ConsumerConfig {
        max_buffered_messages: 1,
        max_buffered_bytes: 1_024,
        ..ConsumerConfig::default()
    };
    let consumer = Consumer::claim(peer.client.clone(), id(1), 0, id(2), config.clone())
        .await
        .unwrap();
    let clone = consumer.clone();
    assert!(
        Consumer::claim(peer.client.clone(), id(1), 0, id(2), config.clone())
            .await
            .is_err()
    );
    assert_eq!(consumer.fill().await.unwrap(), 1);
    let delivery = consumer.next().await.unwrap().unwrap();
    assert!(!delivery.body().is_empty());
    assert!(peer.client.metrics().snapshot().admitted_bytes <= 1_024);
    drop(consumer);
    assert!(
        Consumer::claim(peer.client.clone(), id(1), 0, id(2), config.clone())
            .await
            .is_err()
    );
    drop(clone);
    drop(delivery);
    assert_eq!(peer.client.metrics().snapshot().admitted_bytes, 0);
    // Dropping a handle releases local admission, not the server's durable assignment.
    peer.state.lock().unwrap().assignment = None;
    Consumer::claim(peer.client.clone(), id(1), 0, id(2), config)
        .await
        .unwrap();
}

#[tokio::test]
async fn trace_context_is_request_scoped_validated_and_does_not_leak_across_lanes() {
    use tidemq_sdk::scope_traceparent;
    const FIRST: &str = "00-11111111111111111111111111111111-2222222222222222-01";
    const SECOND: &str = "00-33333333333333333333333333333333-4444444444444444-01";
    let peer = peer(false, false).await;
    let admin = peer.client.admin();
    let (a, b) = tokio::join!(
        scope_traceparent(FIRST, admin.topic("events")),
        scope_traceparent(SECOND, admin.topic("events"))
    );
    a.unwrap();
    b.unwrap();
    admin.topic("events").await.unwrap();
    let polled = std::sync::atomic::AtomicBool::new(false);
    let invalid = scope_traceparent("invalid", async {
        polled.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    })
    .await;
    assert!(invalid.is_err());
    assert!(!polled.load(std::sync::atomic::Ordering::Relaxed));
    let state = peer.state.lock().unwrap();
    assert!(state.traceparents.contains(&FIRST.into()));
    assert!(state.traceparents.contains(&SECOND.into()));
    assert_eq!(state.traceparents.last().map(String::as_str), Some(""));
}

#[tokio::test]
async fn dns_endpoint_connects_to_the_ipv4_local_development_listener() {
    let peer = peer(false, false).await;
    let client = Client::connect(
        ClientConfig::new(peer.endpoint.clone()),
        peer.ca.clone(),
        TOKEN.into(),
    )
    .await
    .unwrap();
    client.admin().topic("events").await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn sdk_producer_pipelines_sequence_order_without_waiting_for_each_response() {
    let peer = peer(false, false).await;
    peer.state.lock().unwrap().pair_produces = true;
    let producer = Producer::open(peer.client.clone(), id(1), 0, id(3))
        .await
        .unwrap();
    let message = public::Message {
        key: Bytes::from_static(b"key"),
        body: Bytes::from_static(b"body"),
    };
    let (first, second) = tokio::join!(
        producer.produce(vec![message.clone()]),
        producer.produce(vec![message])
    );
    assert_eq!(first.unwrap().first_offset, 0);
    assert_eq!(second.unwrap().first_offset, 1);
    assert_eq!(
        peer.state
            .lock()
            .unwrap()
            .producer_lanes
            .get(&bytes(id(3)))
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn many_producer_domains_share_a_fixed_lane_set_and_one_produce_connection() {
    let peer = peer(false, false).await;
    for producer_id in 3..35 {
        let producer = Producer::open(peer.client.clone(), id(1), 0, id(producer_id))
            .await
            .unwrap();
        producer
            .produce(vec![public::Message {
                key: Bytes::new(),
                body: Bytes::from_static(b"body"),
            }])
            .await
            .unwrap();
    }
    let state = peer.state.lock().unwrap();
    let lanes: BTreeSet<_> = state
        .producer_lanes
        .values()
        .flat_map(|lanes| lanes.iter().copied())
        .collect();
    assert!(lanes.len() > 1);
    assert!(lanes.len() <= ClientConfig::new("localhost:7443").request_lanes);
    assert_eq!(
        state.classes,
        vec![TrafficClass::Control, TrafficClass::Produce]
    );
}

#[tokio::test]
async fn reconnect_uses_the_same_logical_session_after_a_physical_connection_closes() {
    let peer = peer(false, false).await;
    peer.state.lock().unwrap().drop_first_admin = true;
    let topic = peer.client.admin().create_topic("events", 1).await.unwrap();
    assert_eq!(topic.topic_id, bytes(id(1)));
    let state = peer.state.lock().unwrap();
    assert_eq!(
        state.classes,
        vec![TrafficClass::Control, TrafficClass::Control]
    );
    assert!(
        state
            .sessions
            .iter()
            .all(|session| *session == peer.client.session_id())
    );
    assert!(peer.client.metrics().snapshot().retries > 0);
}

#[tokio::test]
async fn cancelling_a_produce_future_preserves_its_recoverable_pending_batch() {
    let peer = peer(true, false).await;
    let producer = Producer::open(peer.client.clone(), id(1), 0, id(3))
        .await
        .unwrap();
    let clone = producer.clone();
    let task = tokio::spawn(async move {
        clone
            .produce(vec![public::Message {
                key: Bytes::from_static(b"key"),
                body: Bytes::from_static(b"secret-payload"),
            }])
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !peer.state.lock().unwrap().producer_requests.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let pending = producer.pending_batches().await.unwrap().pop().unwrap();
    assert!(!format!("{pending:?}").contains("secret-payload"));
    assert!(producer.retry(&pending).await.unwrap().duplicate);
}

#[tokio::test]
async fn ack_batches_obey_encoded_control_frame_limits_and_count_limits() {
    let peer = peer(false, true).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    consumer.fill().await.unwrap();
    let delivery = consumer.next().await.unwrap().unwrap();
    let deliveries = vec![delivery; 400];
    tidemq_sdk::scope_traceparent(
        "00-11111111111111111111111111111111-2222222222222222-01",
        consumer.ack_many(&deliveries),
    )
    .await
    .unwrap();
    let state = peer.state.lock().unwrap();
    assert!(state.acknowledgements.len() >= 3);
    assert_eq!(state.acknowledgements.iter().sum::<usize>(), 400);
    assert!(
        state
            .acknowledgements
            .iter()
            .all(|count| *count <= ProtocolLimits::default().max_batch_messages as usize)
    );
}

#[derive(Clone)]
struct HostWriter(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for HostWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_uses_the_existing_host_subscriber_and_omits_credentials() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let sink = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NEW)
        .with_writer(move || HostWriter(sink.clone()))
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let peer = peer(false, false).await;
    peer.client.admin().topic("events").await.unwrap();
    tracing::info!("host-subscriber-survived");
    let text = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(text.contains("host-subscriber-survived"));
    assert!(text.contains("tidemq.sdk.request"));
    assert!(!text.contains(TOKEN));
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn optional_metrics_register_into_the_hosts_registry() {
    let peer = peer(false, false).await;
    peer.client.admin().topic("events").await.unwrap();
    let mut registry = prometheus_client::registry::Registry::default();
    peer.client
        .metrics()
        .register(registry.sub_registry_with_prefix("host_queue_client"));
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    assert!(text.contains("host_queue_client_requests_total 1"));
    assert!(text.contains("host_queue_client_operation_duration_seconds_bucket"));
    assert!(!text.contains(TOKEN));
}

#[tokio::test]
async fn transient_rejection_retries_the_original_batch_with_a_bounded_attempt_budget() {
    let peer = peer(false, false).await;
    peer.state.lock().unwrap().reject_produces = 2;
    let producer = Producer::open(peer.client.clone(), id(1), 0, id(3))
        .await
        .unwrap();
    let message = public::Message {
        key: Bytes::from_static(b"key"),
        body: Bytes::from_static(b"body"),
    };
    let receipt = producer.produce(vec![message.clone()]).await.unwrap();
    assert_eq!(receipt.first_offset, 0);
    assert!(!receipt.duplicate);
    {
        let state = peer.state.lock().unwrap();
        assert_eq!(state.producer_requests.len(), 3);
        assert_eq!(
            state
                .producer_requests
                .iter()
                .collect::<BTreeSet<_>>()
                .len(),
            3
        );
        assert!(
            state
                .producer_attempts
                .windows(2)
                .all(|pair| pair[0] == pair[1])
        );
        assert_eq!(state.producers.len(), 1);
    }
    peer.state.lock().unwrap().reject_produces = u32::MAX;
    assert!(matches!(
        producer.produce(vec![message]).await,
        Err(Error::Remote {
            code: tidemq_protocol::common::ErrorCode::Unavailable,
            outcome: tidemq_protocol::common::Outcome::Rejected,
            ..
        })
    ));
    {
        let state = peer.state.lock().unwrap();
        assert_eq!(state.producer_requests.len(), 6);
        assert_eq!(state.producers.len(), 1);
    }
    let pending = producer.pending_batches().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending.first().unwrap().first_sequence(), 1);
}

#[tokio::test]
async fn unknown_produce_retries_original_identity_without_reassigning_sequence() {
    let peer = peer(true, false).await;
    let producer = Producer::open(peer.client.clone(), id(1), 0, id(3))
        .await
        .unwrap();
    let message = public::Message {
        key: Bytes::from_static(b"key"),
        body: Bytes::from_static(b"body"),
    };
    let result = RequestContext::with_timeout(Duration::from_millis(100))
        .unwrap()
        .scope(producer.produce(vec![message.clone()]))
        .await;
    assert!(matches!(result, Err(Error::ProduceUnknown { .. })));
    assert!(matches!(
        producer.produce(vec![message]).await,
        Err(Error::ProducerBlocked)
    ));
    let pending = producer.pending_batches().await.unwrap().pop().unwrap();
    assert_eq!(pending.first_sequence(), 0);
    assert_eq!(peer.client.metrics().snapshot().pending_batches, 1);
    let receipt = producer.retry(&pending).await.unwrap();
    assert!(receipt.duplicate);
    assert!(producer.pending_batches().await.unwrap().is_empty());
    assert!(peer.client.metrics().snapshot().admitted_bytes > 0);
    drop(pending);
    assert_eq!(peer.client.metrics().snapshot().admitted_bytes, 0);
    let state = peer.state.lock().unwrap();
    assert_eq!(state.producers.len(), 1);
    assert_eq!(state.producer_requests.len(), 2);
    assert_ne!(
        state.producer_requests.first(),
        state.producer_requests.last()
    );
    assert_eq!(
        state.classes,
        vec![TrafficClass::Control, TrafficClass::Produce]
    );
    assert!(
        state
            .sessions
            .iter()
            .all(|session| *session == peer.client.session_id())
    );
}

async fn wait_for_produces(peer: &Peer, count: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if peer.state.lock().unwrap().producer_requests.len() >= count {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancellation_retains_original_batch_and_does_not_reset_other_producers() {
    let peer = peer(true, false).await;
    let producer = Producer::open(peer.client.clone(), id(1), 0, id(3))
        .await
        .unwrap();
    let other = Producer::open(peer.client.clone(), id(1), 0, id(4))
        .await
        .unwrap();
    let context = RequestContext::with_timeout(Duration::from_secs(3)).unwrap();
    let cancellation = context.cancellation_token();
    let clone = producer.clone();
    let task = tokio::spawn(async move {
        context
            .scope(clone.produce(vec![public::Message {
                key: Bytes::from_static(b"key"),
                body: Bytes::from_static(b"cancelled-result"),
            }]))
            .await
    });
    wait_for_produces(&peer, 1).await;
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(Error::ProduceUnknown { .. })));
    assert!(result.unwrap_err().outcome_unknown());
    assert_eq!(peer.client.metrics().snapshot().unknown, 1);
    let pending = producer.pending_batches().await.unwrap().pop().unwrap();
    let cancelled_retry = RequestContext::new(deadline(), cancellation);
    assert!(matches!(
        cancelled_retry.scope(producer.retry(&pending)).await,
        Err(Error::ProduceUnknown { .. })
    ));
    {
        let state = peer.state.lock().unwrap();
        let (operation, payload) = state.producer_attempts.first().unwrap();
        assert_eq!(operation.as_ref(), pending.operation_id().as_bytes());
        assert_eq!(pending.first_sequence(), payload.first_sequence);
        assert_eq!(pending.digest(), payload.content_digest.as_ref());
    }
    other
        .produce(vec![public::Message {
            key: Bytes::new(),
            body: Bytes::from_static(b"unrelated"),
        }])
        .await
        .unwrap();
    assert!(producer.retry(&pending).await.unwrap().duplicate);
    assert!(producer.pending_batches().await.unwrap().is_empty());
    drop(pending);
    assert_eq!(peer.client.metrics().snapshot().admitted_bytes, 0);
}

#[tokio::test]
async fn cancelling_queued_produce_releases_admission_without_creating_an_identity() {
    let peer = peer(true, false).await;
    let mut config = ClientConfig::new(peer.endpoint.clone());
    config.max_pending_requests = 1;
    let client = Client::connect(config, peer.ca.clone(), TOKEN.into())
        .await
        .unwrap();
    let first = Producer::open(client.clone(), id(1), 0, id(3))
        .await
        .unwrap();
    let second = Producer::open(client.clone(), id(1), 0, id(4))
        .await
        .unwrap();
    let message = public::Message {
        key: Bytes::new(),
        body: Bytes::from_static(b"queued"),
    };
    let result = RequestContext::with_timeout(Duration::from_millis(80))
        .unwrap()
        .scope(first.produce(vec![message.clone()]))
        .await;
    assert!(matches!(result, Err(Error::ProduceUnknown { .. })));
    let before = client.metrics().snapshot().admitted_bytes;
    let context = RequestContext::with_timeout(Duration::from_secs(3)).unwrap();
    let cancellation = context.cancellation_token();
    let clone = second.clone();
    let queued = message.clone();
    let task = tokio::spawn(async move { context.scope(clone.produce(vec![queued])).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
    assert_eq!(peer.state.lock().unwrap().producer_requests.len(), 1);
    cancellation.cancel();
    assert!(matches!(task.await.unwrap(), Err(Error::Cancelled)));
    assert!(second.pending_batches().await.unwrap().is_empty());
    assert_eq!(client.metrics().snapshot().admitted_bytes, before);
    let pending = first.pending_batches().await.unwrap().pop().unwrap();
    assert!(first.retry(&pending).await.unwrap().duplicate);
    drop(pending);
    let receipt = second.produce(vec![message]).await.unwrap();
    assert_eq!(receipt.first_offset, 0);
    assert_eq!(client.metrics().snapshot().admitted_bytes, 0);
    client.close().await;
}

#[tokio::test]
async fn closing_client_cancels_in_flight_production_and_preserves_pending_identity() {
    let peer = peer(true, false).await;
    let producer = Producer::open(peer.client.clone(), id(1), 0, id(3))
        .await
        .unwrap();
    let clone = producer.clone();
    let task = tokio::spawn(async move {
        clone
            .produce(vec![public::Message {
                key: Bytes::new(),
                body: Bytes::from_static(b"closing"),
            }])
            .await
    });
    wait_for_produces(&peer, 1).await;
    tokio::time::timeout(Duration::from_secs(2), peer.client.close())
        .await
        .unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(Error::ProduceUnknown { .. })
    ));
    let pending = producer.pending_batches().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending.first().unwrap().first_sequence(), 0);
    assert!(matches!(
        peer.client.admin().topic("events").await,
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn cancelled_registration_keeps_its_epoch_and_admin_submission_remains_unknown() {
    let peer = peer(false, false).await;
    peer.state.lock().unwrap().hold_registration = true;
    let context = RequestContext::with_timeout(Duration::from_secs(3)).unwrap();
    let cancellation = context.cancellation_token();
    let client = peer.client.clone();
    let task =
        tokio::spawn(async move { context.scope(Producer::open(client, id(1), 0, id(3))).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while peer.state.lock().unwrap().registrations.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancellation.cancel();
    assert!(
        matches!(task.await.unwrap(), Err(Error::RegistrationUnknown {
        producer_id, registration_epoch: 1, ..
    }) if producer_id == id(3))
    );
    peer.state.lock().unwrap().hold_registration = false;
    let producer = Producer::open_at(peer.client.clone(), id(1), 0, id(3), 1)
        .await
        .unwrap();
    assert_eq!(producer.epoch(), 1);
    {
        let mut state = peer.state.lock().unwrap();
        assert_eq!(state.registrations.len(), 2);
        assert_eq!(state.registrations.first(), state.registrations.last());
        state.hold_admin = true;
    }
    let context = RequestContext::with_timeout(Duration::from_secs(3)).unwrap();
    let cancellation = context.cancellation_token();
    let client = peer.client.clone();
    let task = tokio::spawn(async move {
        context
            .scope(client.admin().create_topic("cancelled", 1))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while peer.state.lock().unwrap().admin_requests == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancellation.cancel();
    let result = task.await.unwrap();
    assert!(matches!(result, Err(Error::RequestUnknown { .. })));
    assert!(result.unwrap_err().outcome_unknown());
}

#[tokio::test]
async fn default_timeout_bounds_production_and_cancellation_interrupts_retry_backoff() {
    let peer = peer(true, false).await;
    let mut config = ClientConfig::new(peer.endpoint.clone());
    config.request_timeout_ms = 120;
    let client = Client::connect(config, peer.ca.clone(), TOKEN.into())
        .await
        .unwrap();
    let producer = Producer::open(client.clone(), id(1), 0, id(3))
        .await
        .unwrap();
    let message = public::Message {
        key: Bytes::new(),
        body: Bytes::from_static(b"timeout"),
    };
    let started = Instant::now();
    assert!(matches!(
        producer.produce(vec![message.clone()]).await,
        Err(Error::ProduceUnknown { .. })
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
    let pending = producer.pending_batches().await.unwrap().pop().unwrap();
    assert!(producer.retry(&pending).await.unwrap().duplicate);
    drop(pending);
    peer.state.lock().unwrap().reject_produces = u32::MAX;
    let mut config = ClientConfig::new(peer.endpoint.clone());
    config.retry_backoff = Duration::from_secs(1);
    let retry_client = Client::connect(config, peer.ca.clone(), TOKEN.into())
        .await
        .unwrap();
    let retrying = Producer::open(retry_client.clone(), id(1), 0, id(4))
        .await
        .unwrap();
    let context = RequestContext::with_timeout(Duration::from_secs(3)).unwrap();
    let cancellation = context.cancellation_token();
    let clone = retrying.clone();
    let task = tokio::spawn(async move { context.scope(clone.produce(vec![message])).await });
    wait_for_produces(&peer, 3).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    cancellation.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(500), task)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::ProduceUnknown { .. })
    ));
    assert_eq!(peer.state.lock().unwrap().producer_requests.len(), 3);
    assert_eq!(retrying.pending_batches().await.unwrap().len(), 1);
    client.close().await;
    retry_client.close().await;
}

#[tokio::test]
async fn owner_recovery_preserves_failed_keys_and_invalidates_old_tokens_across_clones() {
    for unassigned in [false, true] {
        let peer = peer(false, false).await;
        let consumer = Consumer::claim(
            peer.client.clone(),
            id(1),
            0,
            id(2),
            ConsumerConfig {
                ordering: DeliveryOrdering::KeySerial,
                ..ConsumerConfig::default()
            },
        )
        .await
        .unwrap();
        let clone = consumer.clone();
        consumer.fill().await.unwrap();
        consumer.fill().await.unwrap();
        let old = consumer.next().await.unwrap().unwrap();
        consumer.fail(old.clone()).await.unwrap();
        {
            let mut state = peer.state.lock().unwrap();
            advance_owner(&mut state, unassigned);
            state.other_hot_key = true;
        }
        clone.fill().await.unwrap();
        assert_eq!(peer.client.metrics().snapshot().active_deliveries, 2);
        consumer.fill().await.unwrap();
        assert!(clone.next().await.unwrap().is_none());
        assert!(matches!(
            consumer.ack(&old).await,
            Err(Error::StaleDelivery)
        ));
        drop(old);
        consumer.fill().await.unwrap();
        let unrelated = clone.next().await.unwrap().unwrap();
        assert_eq!(unrelated.offset(), 2);
        clone.ack(&unrelated).await.unwrap();
        assert!(consumer.next().await.unwrap().is_none());
        consumer.retry_failed(0).await.unwrap();
        let first = clone.next().await.unwrap().unwrap();
        assert_eq!(first.offset(), 0);
        clone.ack(&first).await.unwrap();
        let second = consumer.next().await.unwrap().unwrap();
        assert_eq!(second.offset(), 1);
        assert_eq!(consumer.ack(&second).await.unwrap().acknowledged_before, 3);
        let state = peer.state.lock().unwrap();
        assert_eq!(state.claims.len(), 2);
        assert!(
            state
                .claims
                .iter()
                .all(|claim| claim.replace_session_id.is_empty())
        );
        assert_eq!(state.acknowledgements, vec![1, 1, 1]);
    }
}

#[tokio::test]
async fn hot_ack_frontier_can_cross_the_frozen_boundary_before_and_after_owner_recovery() {
    let peer = peer(false, false).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    consumer.fill().await.unwrap();
    consumer.fill().await.unwrap();
    for offset in 0..2 {
        let delivery = consumer.next().await.unwrap().unwrap();
        assert_eq!(delivery.offset(), offset);
        consumer.ack(&delivery).await.unwrap();
    }
    for offset in 2..5 {
        {
            let mut state = peer.state.lock().unwrap();
            state.hot_offset = Some(offset);
            if offset == 4 {
                advance_owner(&mut state, false);
            }
        }
        assert_eq!(consumer.fill().await.unwrap(), 1);
        let delivery = consumer.next().await.unwrap().unwrap();
        assert_eq!(delivery.offset(), offset);
        assert_eq!(
            consumer.ack(&delivery).await.unwrap().acknowledged_before,
            offset + 1
        );
    }
    let state = peer.state.lock().unwrap();
    let assignment = state.assignment.as_ref().unwrap();
    assert_eq!(assignment.cold_before, 2);
    assert_eq!(assignment.acknowledged_before, 5);
    assert_eq!(assignment.received_before, 5);
    assert_eq!(state.claims.len(), 2);
    assert!(state.barriers.contains(&4));
    assert!(state.barriers.contains(&5));
    assert!(state.claims.iter().all(|claim| {
        claim.session_id == bytes(peer.client.session_id()) && claim.replace_session_id.is_empty()
    }));
}

#[tokio::test]
async fn cancelled_reclaim_cannot_resume_its_obsolete_queue_and_recovery_keeps_the_session() {
    let peer = peer(false, false).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    consumer.fill().await.unwrap();
    peer.state.lock().unwrap().drop_claim_reply = true;
    let clone = consumer.clone();
    let task = tokio::spawn(async move { clone.reclaim().await });
    let stop = deadline();
    while peer.state.lock().unwrap().claims.len() < 2 {
        assert!(Instant::now() < stop);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(matches!(consumer.next().await, Err(Error::StaleDelivery)));
    consumer.fill().await.unwrap();
    consumer.fill().await.unwrap();
    let fresh = consumer.next().await.unwrap().unwrap();
    assert_eq!(fresh.offset(), 0);
    let state = peer.state.lock().unwrap();
    assert_eq!(state.claims.len(), 3);
    assert!(
        state
            .claims
            .iter()
            .all(|claim| claim.session_id == bytes(peer.client.session_id())
                && claim.replace_session_id.is_empty())
    );
}

#[tokio::test]
async fn same_generation_refresh_rebuilds_enqueue_evidence_before_unlocking_keys() {
    let peer = peer(false, false).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig {
            ordering: DeliveryOrdering::KeySerial,
            ..ConsumerConfig::default()
        },
    )
    .await
    .unwrap();
    consumer.fill().await.unwrap();
    consumer.fill().await.unwrap();
    let failed = consumer.next().await.unwrap().unwrap();
    consumer.fail(failed).await.unwrap();
    {
        let mut state = peer.state.lock().unwrap();
        state.reject_reads = 3;
        state.idempotent_claim = true;
        state.other_hot_key = true;
    }
    consumer.fill().await.unwrap();
    assert!(consumer.next().await.unwrap().is_none());
    consumer.fill().await.unwrap();
    assert!(consumer.next().await.unwrap().is_none());
    consumer.fill().await.unwrap();
    let unrelated = consumer.next().await.unwrap().unwrap();
    assert_eq!(unrelated.offset(), 2);
    consumer.retry_failed(0).await.unwrap();
    let original = consumer.next().await.unwrap().unwrap();
    assert_eq!(original.offset(), 0);
    assert_eq!(peer.state.lock().unwrap().claims.len(), 2);
}

#[tokio::test]
async fn old_ack_is_not_rewritten_and_recv_recovers_from_the_durable_frontier() {
    let peer = peer(false, false).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    consumer.fill().await.unwrap();
    consumer.fill().await.unwrap();
    let first = consumer.next().await.unwrap().unwrap();
    assert_eq!(first.offset(), 0);
    consumer.ack(&first).await.unwrap();
    let old = consumer.next().await.unwrap().unwrap();
    advance_owner(&mut peer.state.lock().unwrap(), true);
    assert!(matches!(
        consumer.ack(&old).await,
        Err(Error::Remote {
            code: tidemq_protocol::common::ErrorCode::Fenced,
            ..
        })
    ));
    let fresh = consumer.recv().await.unwrap().unwrap();
    assert_eq!(fresh.offset(), 1);
    assert!(matches!(
        consumer.ack(&old).await,
        Err(Error::StaleDelivery)
    ));
    assert_eq!(consumer.ack(&fresh).await.unwrap().acknowledged_before, 2);
    let state = peer.state.lock().unwrap();
    assert_eq!(state.claims.len(), 2);
    assert_eq!(state.assignment.as_ref().unwrap().acknowledged_before, 2);
    assert_eq!(state.acknowledgements, vec![1, 1]);
}

#[tokio::test]
async fn automatic_owner_recovery_never_reuses_explicit_takeover_consent_for_another_session() {
    let peer = peer(false, false).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig {
            replace_session_id: Some(id(9)),
            ..ConsumerConfig::default()
        },
    )
    .await
    .unwrap();
    {
        let mut state = peer.state.lock().unwrap();
        advance_owner(&mut state, false);
        state.assignment.as_mut().unwrap().session_id = bytes(id(9));
    }
    assert!(matches!(
        consumer.fill().await,
        Err(Error::Remote {
            code: tidemq_protocol::common::ErrorCode::AssignmentBusy,
            ..
        })
    ));
    assert_eq!(peer.state.lock().unwrap().claims.len(), 1);
    assert_eq!(
        peer.state
            .lock()
            .unwrap()
            .assignment
            .as_ref()
            .unwrap()
            .session_id,
        bytes(id(9))
    );
    consumer.reclaim().await.unwrap();
    let state = peer.state.lock().unwrap();
    assert_eq!(state.claims.len(), 2);
    assert_eq!(
        state.claims.last().unwrap().replace_session_id,
        bytes(id(9))
    );
}

#[tokio::test]
async fn last_allowed_assignment_refresh_can_issue_its_final_read_without_holding_the_state_lock() {
    let peer = peer(false, false).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    peer.state.lock().unwrap().reject_reads = 9;
    assert_eq!(consumer.fill().await.unwrap(), 1);
    assert_eq!(peer.state.lock().unwrap().claims.len(), 4);
}

#[tokio::test]
async fn repeated_fences_stop_at_the_shared_deadline_and_finite_recovery_budget() {
    let peer = peer(false, false).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    peer.state.lock().unwrap().reject_reads = 50;
    let begin = Instant::now();
    assert!(
        RequestContext::with_deadline(begin + Duration::from_millis(500))
            .scope(consumer.fill())
            .await
            .is_err()
    );
    assert!(begin.elapsed() < Duration::from_secs(1));
    assert!(peer.state.lock().unwrap().claims.len() <= 4);
}

#[tokio::test]
async fn gaps_do_not_cross_bulk_barrier_and_failed_keys_remain_blocked() {
    let peer = peer(false, false).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig {
            ordering: DeliveryOrdering::KeySerial,
            ..ConsumerConfig::default()
        },
    )
    .await
    .unwrap();
    consumer.fill().await.unwrap();
    assert!(consumer.next().await.unwrap().is_none());
    assert_eq!(peer.state.lock().unwrap().hot_reads, 0);
    consumer.fill().await.unwrap();
    let first = consumer.next().await.unwrap().unwrap();
    assert_eq!(first.offset(), 0);
    consumer.fail(first).await.unwrap();
    assert!(consumer.next().await.unwrap().is_none());
    consumer.retry_failed(0).await.unwrap();
    let first = consumer.next().await.unwrap().unwrap();
    consumer.ack(&first).await.unwrap();
    let second = consumer.next().await.unwrap().unwrap();
    assert_eq!(second.offset(), 1);
    consumer.ack(&first).await.unwrap();
    consumer.fill().await.unwrap();
    assert!(consumer.next().await.unwrap().is_none());
    consumer.ack(&second).await.unwrap();
    let hot = consumer.next().await.unwrap().unwrap();
    assert_eq!(hot.offset(), 2);
    consumer.seek(2, Id::random().unwrap()).await.unwrap();
    assert!(matches!(
        consumer.ack(&hot).await,
        Err(Error::StaleDelivery)
    ));
    let state = peer.state.lock().unwrap();
    assert!(state.barriers.contains(&2));
    assert_eq!(state.hot_reads, 1);
    assert_eq!(state.acknowledgements, vec![1, 1, 1]);
    assert_eq!(state.classes.len(), 3);
    assert!(
        state
            .sessions
            .iter()
            .all(|session| *session == peer.client.session_id())
    );
}

#[tokio::test]
async fn receive_barrier_recovers_the_same_progress_after_an_unknown_result() {
    for committed in [false, true] {
        let peer = peer(false, true).await;
        let consumer = Consumer::claim(
            peer.client.clone(),
            id(1),
            0,
            id(2),
            ConsumerConfig::default(),
        )
        .await
        .unwrap();
        {
            let mut state = peer.state.lock().unwrap();
            state.barrier_unknowns = 1;
            state.commit_unknown_barrier = committed;
            assert_eq!(state.hot_reads, 0);
        }
        consumer
            .fill()
            .await
            .expect("An unknown monotonic barrier must be resolved before reading hot data");
        {
            let state = peer.state.lock().unwrap();
            assert_eq!(state.barrier_requests.len(), 2);
            let first = &state.barrier_requests[0];
            let retry = &state.barrier_requests[1];
            assert_eq!(first.0, retry.0);
            assert_ne!(first.1, retry.1);
            assert_eq!(first.2, retry.2);
            assert_eq!(first.2.received_before, 2);
            assert_eq!(state.hot_reads, 0);
            assert!(state.acknowledgements.is_empty());
        }
        consumer.fill().await.unwrap();
        assert_eq!(peer.state.lock().unwrap().hot_reads, 1);
    }
}

#[tokio::test]
async fn receive_barrier_preserves_unknown_when_the_retry_budget_is_exhausted() {
    let peer = peer(false, true).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    peer.state.lock().unwrap().barrier_unknowns = u32::MAX;
    let result = consumer.fill().await;
    assert!(matches!(
        result,
        Err(Error::Remote {
            outcome: tidemq_protocol::common::Outcome::Unknown,
            ..
        })
    ));
    let state = peer.state.lock().unwrap();
    assert_eq!(state.barrier_requests.len(), 3);
    assert_eq!(state.hot_reads, 0);
    assert!(state.acknowledgements.is_empty());
}

#[tokio::test]
async fn persisted_ack_ranges_bridge_receive_holes_without_business_ack() {
    let peer = peer(false, true).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    consumer.fill().await.unwrap();
    consumer.fill().await.unwrap();
    let state = peer.state.lock().unwrap();
    assert!(state.barriers.contains(&2));
    assert_eq!(state.hot_reads, 1);
    assert!(state.acknowledgements.is_empty());
}

#[tokio::test]
async fn bulk_retries_remain_available_after_hot_delivery_starts_without_business_acks() {
    let peer = peer(false, false).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    consumer.fill().await.unwrap();
    consumer.fill().await.unwrap();
    consumer.fill().await.unwrap();
    consumer.fill().await.unwrap();
    let state = peer.state.lock().unwrap();
    assert_eq!(state.hot_reads, 1);
    assert_eq!(state.bulk_reads, 3);
    assert!(state.acknowledgements.is_empty());
}
