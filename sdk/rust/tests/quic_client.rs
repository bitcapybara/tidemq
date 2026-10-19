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
    drop_first_produce: bool,
    drop_first_admin: bool,
    pair_produces: bool,
    assignment: Option<public::AssignmentResult>,
    bulk_reads: usize,
    hot_reads: usize,
    barriers: Vec<u64>,
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
        deadline(),
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

fn handle(
    request: &Frame,
    purpose: StreamPurpose,
    lane: u64,
    session: &Session,
    state: &mut PeerState,
) -> Option<Frame> {
    state.traceparents.push(request.traceparent.clone());
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
                .producer_lanes
                .entry(batch.producer_id.clone())
                .or_default()
                .insert(lane);
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
                    assigned: state.assignment.is_some(),
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
            let assignment = public::AssignmentResult {
                subscription_id: claim.subscription_id,
                topic_id: claim.topic_id,
                partition_id: claim.partition_id,
                session_id: claim.session_id,
                cursor_generation: claim.expected_cursor_generation,
                assignment_generation: claim.expected_assignment_generation + 1,
                delivery_generation: claim.expected_assignment_generation + 1,
                acknowledged_before: if state.seek_count > 0 { 2 } else { 0 },
                next_delivery_offset: if state.seek_count > 0 { 2 } else { 0 },
                cold_before: 2,
                received_before: if state.seek_count > 0 { 2 } else { 0 },
                issued_before: if state.seek_count > 0 { 2 } else { 0 },
            };
            state.assignment = Some(assignment.clone());
            response(request, FrameKind::AssignmentResult, &assignment)
        }
        FrameKind::Read => {
            let read: public::ReadRequest =
                decode_payload(request.payload.clone(), &limits).unwrap();
            let class = TrafficClass::try_from(request.traffic_class).unwrap();
            let assignment = state.assignment.as_mut().unwrap();
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
                assignment.issued_before = 3;
                2
            };
            let delivery = public::Delivery {
                offset,
                key: if state.fill_credit {
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
            let assignment = state.assignment.as_mut().unwrap();
            assert!(barrier.received_before <= assignment.issued_before);
            state.barriers.push(barrier.received_before);
            assignment.received_before = assignment.received_before.max(barrier.received_before);
            response(request, FrameKind::AssignmentResult, assignment)
        }
        FrameKind::AckBatch => {
            assert_eq!(purpose, StreamPurpose::Ack);
            assert!(request.encoded_len() <= limits.max_control_frame_bytes as usize);
            let ack: public::AckBatch = decode_payload(request.payload.clone(), &limits).unwrap();
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
        let producer = Producer::open(client.clone(), id(1), 0, id(producer_id), deadline())
            .await
            .unwrap();
        peer.state.lock().unwrap().drop_first_produce = true;
        let result = producer
            .produce(
                vec![public::Message {
                    key: Bytes::new(),
                    body: Bytes::from(vec![b'x'; 262_144]),
                }],
                Instant::now() + Duration::from_millis(80),
            )
            .await;
        assert!(matches!(result, Err(Error::ProduceUnknown { .. })));
        producers.push(producer);
    }
    let third = Producer::open(client.clone(), id(1), 0, id(5), deadline())
        .await
        .unwrap();
    assert!(matches!(
        third
            .produce(
                vec![public::Message {
                    key: Bytes::new(),
                    body: Bytes::from(vec![b'x'; 262_144])
                }],
                deadline()
            )
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
        deadline(),
    )
    .await
    .unwrap();
    consumer.fill(deadline()).await.unwrap();
    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(delivery.body().len(), 262_144);
    consumer.ack(&delivery, deadline()).await.unwrap();
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
    let producer = Producer::open(peer.client.clone(), id(1), 0, id(6), deadline())
        .await
        .unwrap();
    peer.state.lock().unwrap().drop_first_produce = true;
    assert!(matches!(
        producer
            .produce(
                vec![public::Message {
                    key: Bytes::new(),
                    body: small
                }],
                Instant::now() + Duration::from_millis(80)
            )
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
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        config.clone(),
        deadline(),
    )
    .await
    .unwrap();
    let clone = consumer.clone();
    assert!(
        Consumer::claim(
            peer.client.clone(),
            id(1),
            0,
            id(2),
            config.clone(),
            deadline()
        )
        .await
        .is_err()
    );
    assert_eq!(consumer.fill(deadline()).await.unwrap(), 1);
    let delivery = consumer.next().await.unwrap().unwrap();
    assert!(!delivery.body().is_empty());
    assert!(peer.client.metrics().snapshot().admitted_bytes <= 1_024);
    drop(consumer);
    assert!(
        Consumer::claim(
            peer.client.clone(),
            id(1),
            0,
            id(2),
            config.clone(),
            deadline()
        )
        .await
        .is_err()
    );
    drop(clone);
    drop(delivery);
    assert_eq!(peer.client.metrics().snapshot().admitted_bytes, 0);
    // Dropping a handle releases local admission, not the server's durable assignment.
    peer.state.lock().unwrap().assignment = None;
    Consumer::claim(peer.client.clone(), id(1), 0, id(2), config, deadline())
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
        scope_traceparent(FIRST, admin.topic("events", deadline())),
        scope_traceparent(SECOND, admin.topic("events", deadline()))
    );
    a.unwrap();
    b.unwrap();
    admin.topic("events", deadline()).await.unwrap();
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
    client.admin().topic("events", deadline()).await.unwrap();
    client.close(deadline()).await;
}

#[tokio::test]
async fn sdk_producer_pipelines_sequence_order_without_waiting_for_each_response() {
    let peer = peer(false, false).await;
    peer.state.lock().unwrap().pair_produces = true;
    let producer = Producer::open(peer.client.clone(), id(1), 0, id(3), deadline())
        .await
        .unwrap();
    let message = public::Message {
        key: Bytes::from_static(b"key"),
        body: Bytes::from_static(b"body"),
    };
    let (first, second) = tokio::join!(
        producer.produce(vec![message.clone()], deadline()),
        producer.produce(vec![message], deadline())
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
        let producer = Producer::open(peer.client.clone(), id(1), 0, id(producer_id), deadline())
            .await
            .unwrap();
        producer
            .produce(
                vec![public::Message {
                    key: Bytes::new(),
                    body: Bytes::from_static(b"body"),
                }],
                deadline(),
            )
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
    let topic = peer
        .client
        .admin()
        .create_topic("events", 1, deadline())
        .await
        .unwrap();
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
    let producer = Producer::open(peer.client.clone(), id(1), 0, id(3), deadline())
        .await
        .unwrap();
    let clone = producer.clone();
    let task = tokio::spawn(async move {
        clone
            .produce(
                vec![public::Message {
                    key: Bytes::from_static(b"key"),
                    body: Bytes::from_static(b"secret-payload"),
                }],
                deadline(),
            )
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
    assert!(
        producer
            .retry(&pending, deadline())
            .await
            .unwrap()
            .duplicate
    );
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
        deadline(),
    )
    .await
    .unwrap();
    consumer.fill(deadline()).await.unwrap();
    let delivery = consumer.next().await.unwrap().unwrap();
    let deliveries = vec![delivery; 400];
    tidemq_sdk::scope_traceparent(
        "00-11111111111111111111111111111111-2222222222222222-01",
        consumer.ack_many(&deliveries, deadline()),
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
    peer.client
        .admin()
        .topic("events", deadline())
        .await
        .unwrap();
    tracing::info!("host-subscriber-survived");
    let text = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(text.contains("host-subscriber-survived"));
    assert!(text.contains("request_inner"));
    assert!(!text.contains(TOKEN));
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn optional_metrics_register_into_the_hosts_registry() {
    let peer = peer(false, false).await;
    peer.client
        .admin()
        .topic("events", deadline())
        .await
        .unwrap();
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
async fn unknown_produce_retries_original_identity_without_reassigning_sequence() {
    let peer = peer(true, false).await;
    let producer = Producer::open(peer.client.clone(), id(1), 0, id(3), deadline())
        .await
        .unwrap();
    let message = public::Message {
        key: Bytes::from_static(b"key"),
        body: Bytes::from_static(b"body"),
    };
    let result = producer
        .produce(
            vec![message.clone()],
            Instant::now() + Duration::from_millis(100),
        )
        .await;
    assert!(matches!(result, Err(Error::ProduceUnknown { .. })));
    assert!(matches!(
        producer.produce(vec![message], deadline()).await,
        Err(Error::ProducerBlocked)
    ));
    let pending = producer.pending_batches().await.unwrap().pop().unwrap();
    assert_eq!(pending.first_sequence(), 0);
    assert_eq!(peer.client.metrics().snapshot().pending_batches, 1);
    let receipt = producer.retry(&pending, deadline()).await.unwrap();
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
        deadline(),
    )
    .await
    .unwrap();
    consumer.fill(deadline()).await.unwrap();
    assert!(consumer.next().await.unwrap().is_none());
    assert_eq!(peer.state.lock().unwrap().hot_reads, 0);
    consumer.fill(deadline()).await.unwrap();
    let first = consumer.next().await.unwrap().unwrap();
    assert_eq!(first.offset(), 0);
    consumer.fail(first).await.unwrap();
    assert!(consumer.next().await.unwrap().is_none());
    consumer.retry_failed(0).await.unwrap();
    let first = consumer.next().await.unwrap().unwrap();
    consumer.ack(&first, deadline()).await.unwrap();
    let second = consumer.next().await.unwrap().unwrap();
    assert_eq!(second.offset(), 1);
    consumer.ack(&first, deadline()).await.unwrap();
    consumer.fill(deadline()).await.unwrap();
    assert!(consumer.next().await.unwrap().is_none());
    consumer.ack(&second, deadline()).await.unwrap();
    let hot = consumer.next().await.unwrap().unwrap();
    assert_eq!(hot.offset(), 2);
    consumer
        .seek(2, Id::random().unwrap(), deadline())
        .await
        .unwrap();
    assert!(matches!(
        consumer.ack(&hot, deadline()).await,
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
async fn persisted_ack_ranges_bridge_receive_holes_without_business_ack() {
    let peer = peer(false, true).await;
    let consumer = Consumer::claim(
        peer.client.clone(),
        id(1),
        0,
        id(2),
        ConsumerConfig::default(),
        deadline(),
    )
    .await
    .unwrap();
    consumer.fill(deadline()).await.unwrap();
    consumer.fill(deadline()).await.unwrap();
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
        deadline(),
    )
    .await
    .unwrap();
    consumer.fill(deadline()).await.unwrap();
    consumer.fill(deadline()).await.unwrap();
    consumer.fill(deadline()).await.unwrap();
    consumer.fill(deadline()).await.unwrap();
    let state = peer.state.lock().unwrap();
    assert_eq!(state.hot_reads, 1);
    assert_eq!(state.bulk_reads, 3);
    assert!(state.acknowledgements.is_empty());
}
