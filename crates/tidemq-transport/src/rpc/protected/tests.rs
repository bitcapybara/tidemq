use super::*;
use crate::{
    Client, ClientCredentials, ClientTls, PeerTarget, Server, ServerAuthorization, ServerTls,
    StreamPurpose, TlsIdentity, TokenGrant, TransportConfig, budget::MemoryLimits,
};
use bytes::Bytes;
use tidemq_protocol::{
    ProtocolLimits,
    framing::{decode_payload, new_frame},
    public::{
        ConsumerAction, ConsumerControl, ConsumerControlResult, GroupAck, GroupAckResult,
        GroupDeliveries, GroupDelivery, GroupRead, GroupWatch, GroupWatchResult, WatchResult,
        WatchStatus, WatchTarget,
    },
};

const TOKEN: &str = "protected-rpc-budget-test-token-with-32-characters";
const TOTAL: usize = 2 * 1024 * 1024;
const REQUEST_BYTES: u32 = 516;
const FRAME_BYTES: u32 = 3 * 32_768 + 8;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

fn transport() -> TransportConfig {
    TransportConfig {
        send_buffer_bytes: 1_024,
        max_streams: 6,
        max_ack_streams: 1,
        protocol: ProtocolLimits {
            max_frame_bytes: 32_768,
            max_control_frame_bytes: REQUEST_BYTES - 4,
            max_message_bytes: 16_384,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn config(slots: usize, bytes: u32) -> RpcConfig {
    RpcConfig {
        max_pending_requests: slots,
        max_pending_bytes: bytes,
        ..Default::default()
    }
}

fn memory() -> MemoryBudget {
    MemoryBudget::new(MemoryLimits {
        total_bytes: TOTAL as u64,
        control_reserved_bytes: (TOTAL / 4) as u64,
    })
    .unwrap()
}

fn quotas(budget: &RpcBudget) -> [usize; 3] {
    [
        budget.inner.normal.slots.available_permits(),
        budget.inner.normal.bytes.available_permits(),
        budget.inner.registrations.available_permits(),
    ]
}

fn memory_quotas(memory: &MemoryBudget) -> [usize; 6] {
    [
        memory.available_bytes(),
        memory.available_data_bytes(),
        memory.available_normal_bytes(),
        memory.available_core_bytes(),
        memory.available_ack_bytes(),
        memory.available_handshake_bytes(),
    ]
}

async fn until(message: &'static str, condition: impl Fn() -> bool) {
    timeout_at(deadline(), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(message);
}

async fn arm_pending<F: std::future::Future>(mut future: std::pin::Pin<&mut F>) {
    std::future::poll_fn(|context| {
        assert!(future.as_mut().poll(context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
}

fn request(sequence: u64) -> Frame {
    new_frame(
        FrameKind::GroupRead,
        TrafficClass::Delivery,
        Id::new_v7().unwrap(),
        Id::new_v7().unwrap(),
        &GroupRead {
            topic_id: Bytes::from_static(&[1; 16]),
            subscription_id: Bytes::from_static(&[2; 16]),
            member_id: Bytes::from_static(&[3; 16]),
            member_epoch: 1,
            sequence,
            max_messages: 1,
            max_bytes: 16_640,
            ..Default::default()
        },
        5_000,
        &transport().protocol,
    )
    .unwrap()
}

fn padded_request(sequence: u64, length: usize) -> Frame {
    let mut frame = request(sequence);
    let original = frame.payload.clone();
    let mut padding = length - frame.encoded_len();
    for _ in 0..6 {
        let mut bytes = original.to_vec();
        // A valid unknown field changes wire size without changing the GroupRead semantics.
        bytes.extend_from_slice(&[0xa2, 0x06]);
        prost::encode_length_delimiter(padding, &mut bytes).unwrap();
        bytes.extend(std::iter::repeat_n(7, padding));
        frame.payload = Bytes::from(bytes);
        let actual = frame.encoded_len();
        if actual == length {
            return frame;
        }
        padding = padding
            .checked_add(length)
            .unwrap()
            .checked_sub(actual)
            .unwrap();
    }
    panic!("Could not construct the requested test frame size");
}

fn reply(request: &Frame, body_bytes: usize) -> Frame {
    let read: GroupRead = decode_payload(request.payload.clone(), &transport().protocol).unwrap();
    new_frame(
        FrameKind::GroupDeliveries,
        TrafficClass::Delivery,
        Id::from_slice(&request.request_id).unwrap(),
        Id::from_slice(&request.operation_id).unwrap(),
        &GroupDeliveries {
            sequence: read.sequence,
            deliveries: vec![GroupDelivery {
                body: Bytes::from(vec![7; body_bytes]),
                current_attempt: true,
                ..Default::default()
            }],
            ..Default::default()
        },
        5_000,
        &transport().protocol,
    )
    .unwrap()
}

async fn exchange(
    rpc: &RpcClient,
    peer: &mut FramedStream,
    sequence: u64,
    body: usize,
) -> ReceivedFrame {
    let response = rpc.submit(request(sequence), deadline()).await.unwrap();
    let received = peer.receiver.receive(deadline()).await.unwrap().unwrap();
    assert_eq!(received.frame().kind, i32::from(FrameKind::GroupRead));
    let response_frame = reply(received.frame(), body);
    peer.sender
        .send_owned(response_frame, deadline())
        .await
        .unwrap();
    let response = response.receive().await.unwrap();
    let deliveries: GroupDeliveries =
        decode_payload(response.frame().payload.clone(), &transport().protocol).unwrap();
    assert_eq!(deliveries.sequence, sequence);
    assert_eq!(deliveries.deliveries.first().unwrap().body.len(), body);
    response
}

fn control_frame(kind: FrameKind, payload: &impl Message) -> Frame {
    new_frame(
        kind,
        TrafficClass::Control,
        Id::new_v7().unwrap(),
        Id::new_v7().unwrap(),
        payload,
        5_000,
        &transport().protocol,
    )
    .unwrap()
}

fn control_reply(request: &Frame, kind: FrameKind, payload: &impl Message) -> Frame {
    new_frame(
        kind,
        TrafficClass::Control,
        Id::from_slice(&request.request_id).unwrap(),
        Id::from_slice(&request.operation_id).unwrap(),
        payload,
        5_000,
        &transport().protocol,
    )
    .unwrap()
}

async fn answer_control(
    response: RpcResponse,
    peer: &mut FramedStream,
    request: &ReceivedFrame,
    kind: FrameKind,
    payload: &impl Message,
) {
    peer.sender
        .send_owned(control_reply(request.frame(), kind, payload), deadline())
        .await
        .unwrap();
    let response = response.receive().await.unwrap();
    assert_eq!(response.frame().request_id, request.frame().request_id);
    assert_eq!(response.frame().operation_id, request.frame().operation_id);
    assert_eq!(response.frame().kind, i32::from(kind));
}

struct Harness {
    client: Client,
    server: Server,
    memory: MemoryBudget,
    peer_memory: MemoryBudget,
    session: Arc<Session>,
    peer_session: Arc<Session>,
    normal: RpcClient,
    normal_peer: FramedStream,
    budget: RpcBudget,
}

impl Harness {
    async fn new(config: RpcConfig) -> Self {
        Self::with_transport(config, transport()).await
    }

    async fn with_transport(config: RpcConfig, transport_config: TransportConfig) -> Self {
        Self::with_lanes(config, transport_config, 1).await
    }

    async fn with_lanes(
        config: RpcConfig,
        transport_config: TransportConfig,
        normal_lanes: usize,
    ) -> Self {
        let envelope = transport_config.transport_reservation().unwrap() as usize;
        let memory = memory();
        let peer_memory = self::memory();
        let key = rcgen::KeyPair::generate().unwrap();
        let certificate = rcgen::CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let pem = Bytes::from(certificate.pem());
        let mut server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            ServerTls {
                identity: TlsIdentity::new(pem.clone(), Bytes::from(key.serialize_pem())).unwrap(),
                client_trust_roots: None,
            },
            ServerAuthorization::External(vec![
                TokenGrant::new(
                    TOKEN,
                    "consumer".into(),
                    "consume".into(),
                    vec![
                        FrameKind::GroupRead,
                        FrameKind::GroupDeliveries,
                        FrameKind::Cancel,
                        FrameKind::GroupWatch,
                        FrameKind::GroupWatchResult,
                        FrameKind::ConsumerControl,
                        FrameKind::ConsumerControlResult,
                        FrameKind::GroupAck,
                        FrameKind::GroupAckResult,
                    ],
                )
                .unwrap(),
            ]),
            transport_config.clone(),
            peer_memory.clone(),
        )
        .unwrap();
        let client = Client::bind(
            "127.0.0.1:0".parse().unwrap(),
            ClientTls {
                trust_roots: pem,
                identity: None,
                expected_nodes: vec![],
            },
            transport_config,
            memory.clone(),
        )
        .unwrap();
        let session = client
            .connect(
                Self::target(&server),
                &Self::credentials(),
                TrafficClass::Delivery,
                deadline(),
            )
            .await
            .unwrap();
        let peer_session = server.accept().await.unwrap();
        let budget =
            RpcBudget::for_connection(&config, &session.connection(), normal_lanes).unwrap();
        let normal = RpcClient::from_session_with_lanes_and_budget(
            session.clone(),
            config,
            normal_lanes,
            budget.clone(),
            deadline(),
        )
        .await
        .unwrap();
        let normal_peer = peer_session.take_initial_streams().await.pop().unwrap();
        until("HELLO buffers must release their bootstrap memory", || {
            memory.available_bytes() == TOTAL - envelope
                && peer_memory.available_bytes() == TOTAL - envelope
        })
        .await;
        Self {
            client,
            server,
            memory,
            peer_memory,
            session,
            peer_session,
            normal,
            normal_peer,
            budget,
        }
    }

    fn target(server: &Server) -> PeerTarget {
        PeerTarget {
            address: server.local_addr(),
            server_name: "localhost".into(),
            local_role: "sdk".into(),
            remote_role: "gateway".into(),
            cluster_id: None,
            node_id: None,
        }
    }

    fn credentials() -> ClientCredentials {
        ClientCredentials::External {
            session_id: Id::new_v7().unwrap(),
            access_token: TOKEN.into(),
        }
    }

    async fn private(&self, reservation: &ProtectedCapacity) -> (RpcClient, FramedStream) {
        let rpc = self
            .normal
            .open_protected(reservation, deadline())
            .await
            .unwrap();
        let peer = timeout_at(deadline(), self.peer_session.accept())
            .await
            .unwrap()
            .unwrap();
        (rpc, peer)
    }

    async fn ordinary_lane(&self) -> (FramedStream, FramedStream) {
        let stream = self
            .session
            .connection()
            .open(StreamPurpose::Request, deadline())
            .await
            .unwrap();
        let peer = timeout_at(deadline(), self.peer_session.accept())
            .await
            .unwrap()
            .unwrap();
        (stream, peer)
    }

    async fn finish(self) -> MemoryBudget {
        self.normal.shutdown(deadline()).await;
        drop(self.normal);
        drop(self.normal_peer);
        self.session.shutdown(deadline()).await.unwrap();
        self.peer_session.shutdown(deadline()).await.unwrap();
        drop((self.session, self.peer_session, self.budget));
        self.client.close(deadline()).await.unwrap();
        drop(self.client);
        self.server.shutdown().await.unwrap();
        until(
            "The server fixture must release every native allocation",
            || self.peer_memory.available_bytes() == TOTAL,
        )
        .await;
        self.memory
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_private_reads_survive_normal_pending_slot_and_byte_exhaustion() {
    for exhaust_bytes in [false, true] {
        let rpc_config = config(if exhaust_bytes { 8 } else { 3 }, 3 * REQUEST_BYTES);
        let mut h = Harness::new(rpc_config.clone()).await;
        let reservation = h.normal.reserve_protected().unwrap();
        let (private, mut private_peer) = h.private(&reservation).await;
        let mut pending = Vec::new();
        let mut received = Vec::new();
        let count = if exhaust_bytes { 1 } else { 2 };
        for sequence in 0..count {
            let frame = if exhaust_bytes {
                padded_request(sequence, 2 * REQUEST_BYTES as usize)
            } else {
                request(sequence)
            };
            pending.push(h.normal.submit(frame, deadline()).await.unwrap());
            received.push(
                h.normal_peer
                    .receiver
                    .receive(deadline())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        if exhaust_bytes {
            assert_eq!(h.budget.inner.normal.bytes.available_permits(), 0);
            assert!(h.budget.inner.normal.slots.available_permits() > 0);
        } else {
            assert_eq!(h.budget.inner.normal.slots.available_permits(), 0);
            assert!(h.budget.inner.normal.bytes.available_permits() > request(10).encoded_len());
        }
        let before = quotas(&h.budget);
        assert!(matches!(
            h.normal.submit(request(10), deadline()).await,
            Err(Error::Admission { .. })
        ));
        assert_eq!(quotas(&h.budget), before);
        until(
            "Ordinary sends must finish their native encoding ownership",
            || {
                h.memory.available_bytes()
                    == TOTAL
                        - transport().transport_reservation().unwrap() as usize
                        - FRAME_BYTES as usize
            },
        )
        .await;
        let memory_before = memory_quotas(&h.memory);
        assert!(h.normal.reserve_protected().is_err());
        assert_eq!(quotas(&h.budget), before);
        assert_eq!(memory_quotas(&h.memory), memory_before);
        drop(exchange(&private, &mut private_peer, 11, 16_384).await);
        assert_eq!(h.normal.snapshot().pending_responses, Some(count as usize));
        assert_eq!(quotas(&h.budget), before);
        drop((pending, received));
        until(
            "Canceled ordinary waiters must return their pending admission",
            || {
                h.budget.inner.normal.slots.available_permits()
                    == rpc_config.max_pending_requests - 1
                    && h.budget.inner.normal.bytes.available_permits()
                        == (rpc_config.max_pending_bytes - REQUEST_BYTES) as usize
            },
        )
        .await;
        private.shutdown(deadline()).await;
        drop((private, private_peer, reservation));
        until(
            "The private registration must release its parent quotas",
            || {
                h.budget.inner.normal.slots.available_permits() == rpc_config.max_pending_requests
                    && h.budget.inner.normal.bytes.available_permits()
                        == rpc_config.max_pending_bytes as usize
            },
        )
        .await;
        assert!(!h.session.connection().is_closed());
        drop(exchange(&h.normal, &mut h.normal_peer, 12, 1).await);
        let memory = h.finish().await;
        until("Client memory must return after pending exhaustion", || {
            memory.available_bytes() == TOTAL
        })
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_rpc_uses_carved_frames_when_normal_encode_and_receive_fail() {
    let mut h = Harness::new(config(6, 8 * REQUEST_BYTES)).await;
    let reservation = h.normal.reserve_protected().unwrap();
    let (private, mut private_peer) = h.private(&reservation).await;
    let (mut encode_lane, encode_peer) = h.ordinary_lane().await;
    let (mut receive_lane, mut receive_peer) = h.ordinary_lane().await;
    let ordinary_response = exchange(&h.normal, &mut h.normal_peer, 1, 4_096).await;
    let retained_normal = ordinary_response.frame().payload.clone();
    drop(ordinary_response);
    let data = h
        .memory
        .try_reserve(
            TrafficClass::Delivery,
            u32::try_from(h.memory.available_data_bytes()).unwrap(),
        )
        .unwrap();
    let control = h
        .memory
        .try_reserve(
            TrafficClass::Control,
            u32::try_from(h.memory.available_normal_bytes()).unwrap(),
        )
        .unwrap();
    assert_eq!(h.memory.available_data_bytes(), 0);
    assert_eq!(h.memory.available_normal_bytes(), 0);
    assert!(matches!(
        encode_lane.sender.send(&request(2), deadline()).await,
        Err(crate::Error::Memory { .. })
    ));
    receive_peer
        .sender
        .send_owned(reply(&request(3), 64), deadline())
        .await
        .unwrap();
    assert!(matches!(
        receive_lane.receiver.receive(deadline()).await,
        Err(crate::Error::Memory { .. })
    ));
    let before = memory_quotas(&h.memory);
    drop(exchange(&private, &mut private_peer, 4, 16_384).await);
    until(
        "Private native frames must return to their carved pool",
        || reservation.inner.frames.available_bytes() == FRAME_BYTES as usize,
    )
    .await;
    assert_eq!(memory_quotas(&h.memory), before);
    assert!(!h.normal.is_closed());
    assert!(!h.session.connection().is_closed());
    private.shutdown(deadline()).await;
    drop((
        private,
        private_peer,
        reservation,
        data,
        control,
        retained_normal,
    ));
    drop((encode_lane, encode_peer, receive_lane, receive_peer));
    let memory = h.finish().await;
    until("Client memory must return after frame saturation", || {
        memory.available_bytes() == TOTAL
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_receive_admission_failure_preserves_private_rpc_and_reopens_root() {
    let mut h = Harness::new(config(6, 8 * REQUEST_BYTES)).await;
    let before = quotas(&h.budget);
    let reservation = h.normal.reserve_protected().unwrap();
    let (private, mut private_peer) = h.private(&reservation).await;
    let retained = exchange(&private, &mut private_peer, 1, 16_384)
        .await
        .frame()
        .payload
        .clone();
    let first = h.normal.submit(request(2), deadline()).await.unwrap();
    let first_request = h
        .normal_peer
        .receiver
        .receive(deadline())
        .await
        .unwrap()
        .unwrap();
    let second = h.normal.submit(request(3), deadline()).await.unwrap();
    let second_request = h
        .normal_peer
        .receiver
        .receive(deadline())
        .await
        .unwrap()
        .unwrap();
    let data = h
        .memory
        .try_reserve(
            TrafficClass::Delivery,
            u32::try_from(h.memory.available_data_bytes()).unwrap(),
        )
        .unwrap();
    let control = h
        .memory
        .try_reserve(
            TrafficClass::Control,
            u32::try_from(h.memory.available_normal_bytes()).unwrap(),
        )
        .unwrap();
    drop(exchange(&private, &mut private_peer, 4, 16_384).await);
    let response = reply(first_request.frame(), 16_384);
    assert!(
        h.memory
            .try_reserve(
                TrafficClass::Delivery,
                u32::try_from(response.encoded_len() * 2).unwrap(),
            )
            .is_err()
    );
    // The failed receiver may stop the peer before it queues the complete response.
    let _peer_send = h.normal_peer.sender.send_owned(response, deadline()).await;
    let (first, second) = tokio::join!(first.receive(), second.receive());
    let failed_pending =
        matches!(first, Err(Error::Closed)) && matches!(second, Err(Error::Closed));
    let root_admitting = !h.normal.is_closed() && !h.session.connection().is_closed();
    let mut reopened = None;
    if root_admitting {
        drop(exchange(&private, &mut private_peer, 5, 16_384).await);
    }
    drop((data, control, first_request, second_request));
    if root_admitting {
        let response = h.normal.submit(request(6), deadline()).await.unwrap();
        let mut peer = timeout_at(deadline(), h.peer_session.accept())
            .await
            .unwrap()
            .unwrap();
        let received = peer.receiver.receive(deadline()).await.unwrap().unwrap();
        peer.sender
            .send_owned(reply(received.frame(), 64), deadline())
            .await
            .unwrap();
        drop(response.receive().await.unwrap());
        reopened = Some(peer);
    }
    private.shutdown(deadline()).await;
    drop((private, private_peer, reservation, reopened));
    until(
        "Native registration and pending quotas must return before the retained payload drops",
        || quotas(&h.budget) == before,
    )
    .await;
    let memory = h.finish().await;
    until(
        "Only the actual retained frame region may remain after cleanup",
        || memory.available_bytes() == TOTAL - FRAME_BYTES as usize,
    )
    .await;
    drop(retained);
    assert_eq!(memory.available_bytes(), TOTAL);
    eprintln!(
        "Ordinary receive failure: pending_unknown={failed_pending}, root_admitting={root_admitting}, cleanup_bytes={}",
        memory.available_bytes()
    );
    assert!(
        failed_pending,
        "Every pending request on the failed ordinary lane must remain unknown"
    );
    assert!(
        root_admitting,
        "Ordinary frame admission failure must preserve the root and its protected lane"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_lane_teardown_joins_a_granted_send_before_dropping_the_receiver() {
    struct Payload {
        bytes: Bytes,
        dropped: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl AsRef<[u8]> for Payload {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::AcqRel);
        }
    }

    let mut h = Harness::new(config(8, 8 * REQUEST_BYTES)).await;
    let reservation = h.normal.reserve_protected().unwrap();
    let (private, mut private_peer) = h.private(&reservation).await;
    let before = quotas(&h.budget);
    let failed = h.normal.submit(request(1), deadline()).await.unwrap();
    let first = h
        .normal_peer
        .receiver
        .receive(deadline())
        .await
        .unwrap()
        .unwrap();
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut late = request(2);
    late.payload = Bytes::from_owner(Payload {
        bytes: late.payload,
        dropped: dropped.clone(),
    });
    let (slots, bytes) = h
        .normal
        .reserve(&late, Priority::Request, deadline())
        .await
        .unwrap();
    let (result, late_result) = oneshot::channel();
    let lane = h.normal.inner.writers.first().unwrap().clone();
    let state = h.normal.inner.state.clone();
    state.pending.lock().unwrap().insert(
        Id::from_slice(&late.request_id).unwrap(),
        Pending {
            result,
            operation: Id::from_slice(&late.operation_id).unwrap(),
            lane: Some(lane.identity.clone()),
            _slots: slots.clone(),
            _bytes: bytes.clone(),
        },
    );
    let outbound = Outbound {
        frame: late,
        deadline: deadline(),
        _slots: slots,
        _bytes: bytes,
    };
    let (ready, entered) = oneshot::channel();
    let (release, resumed) = std::sync::mpsc::channel();
    let sending = lane.sender.clone();
    let thread = std::thread::spawn(move || {
        // Model preemption after granting a queue permit inside the enqueue/teardown barrier.
        let barrier = state.pending.lock().unwrap();
        let permit = sending.try_reserve().unwrap();
        ready.send(()).unwrap();
        resumed.recv_timeout(Duration::from_secs(5)).unwrap();
        permit.send(outbound);
        drop(barrier);
    });
    timeout_at(deadline(), entered).await.unwrap().unwrap();
    let data = h
        .memory
        .try_reserve(
            TrafficClass::Delivery,
            u32::try_from(h.memory.available_data_bytes()).unwrap(),
        )
        .unwrap();
    let control = h
        .memory
        .try_reserve(
            TrafficClass::Control,
            u32::try_from(h.memory.available_normal_bytes()).unwrap(),
        )
        .unwrap();
    eprintln!(
        "Granted-send fault armed: memory={:?} root_closed={} queue_closed={}",
        memory_quotas(&h.memory),
        h.normal.is_closed(),
        lane.sender.is_closed()
    );
    assert!(
        h.memory
            .try_reserve(TrafficClass::Delivery, 32_768)
            .is_err()
    );
    {
        let sending = h
            .normal_peer
            .sender
            .send_owned(reply(first.frame(), 16_384), deadline());
        tokio::pin!(sending);
        tokio::select! {
            result = &mut sending => eprintln!("Granted-send fault reply: {result:?}"),
            _ = until("The memory refusal did not close the ordinary queue", || lane.sender.is_closed()) => {},
        }
    }
    until("The failed lane did not stop accepting new sends", || {
        lane.sender.is_closed()
    })
    .await;
    assert!(!h.normal.is_closed());
    assert_eq!(dropped.load(Ordering::Acquire), 0);
    release.send(()).unwrap();
    thread.join().unwrap();
    assert!(matches!(failed.receive().await, Err(Error::Closed)));
    assert!(timeout_at(deadline(), late_result).await.unwrap().is_err());
    until(
        "A late granted send retained its payload or pending quotas",
        || dropped.load(Ordering::Acquire) == 1 && quotas(&h.budget) == before,
    )
    .await;
    assert!(lane.sender.is_closed());
    assert!(!h.normal.is_closed());
    assert!(!h.session.connection().is_closed());
    drop(exchange(&private, &mut private_peer, 3, 16_384).await);
    drop((data, control, first));
    drop(exchange(&private, &mut private_peer, 4, 16_384).await);
    let next = h.normal.submit(request(5), deadline()).await.unwrap();
    let mut reopened = timeout_at(deadline(), h.peer_session.accept())
        .await
        .unwrap()
        .unwrap();
    let received = reopened
        .receiver
        .receive(deadline())
        .await
        .unwrap()
        .unwrap();
    reopened
        .sender
        .send_owned(reply(received.frame(), 64), deadline())
        .await
        .unwrap();
    drop(next.receive().await.unwrap());
    private.shutdown(deadline()).await;
    drop((private, private_peer, reservation, reopened, received, lane));
    let memory = h.finish().await;
    until(
        "The granted-send regression did not return all native memory",
        || memory.available_bytes() == TOTAL,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_receive_failure_preserves_sibling_pending_and_skips_the_failed_writer() {
    let mut h = Harness::with_lanes(config(8, 8 * REQUEST_BYTES), transport(), 2).await;
    let mut sibling = timeout_at(deadline(), h.peer_session.accept())
        .await
        .unwrap()
        .unwrap();
    let failed = h
        .normal
        .submit_affinity(request(1), deadline(), 0)
        .await
        .unwrap();
    let failed_request = h
        .normal_peer
        .receiver
        .receive(deadline())
        .await
        .unwrap()
        .unwrap();
    let healthy = h
        .normal
        .submit_affinity(request(2), deadline(), 1)
        .await
        .unwrap();
    let healthy_request = sibling.receiver.receive(deadline()).await.unwrap().unwrap();
    let data = h
        .memory
        .try_reserve(
            TrafficClass::Delivery,
            u32::try_from(h.memory.available_data_bytes()).unwrap(),
        )
        .unwrap();
    let control = h
        .memory
        .try_reserve(
            TrafficClass::Control,
            u32::try_from(h.memory.available_normal_bytes()).unwrap(),
        )
        .unwrap();
    let _peer_send = h
        .normal_peer
        .sender
        .send_owned(reply(failed_request.frame(), 16_384), deadline())
        .await;
    assert!(matches!(failed.receive().await, Err(Error::Closed)));
    assert!(!h.normal.is_closed());
    assert_eq!(h.normal.snapshot().pending_responses, Some(1));
    drop((data, control, failed_request));
    sibling
        .sender
        .send_owned(reply(healthy_request.frame(), 64), deadline())
        .await
        .unwrap();
    let received = healthy.receive().await.unwrap();
    let result: GroupDeliveries =
        decode_payload(received.frame().payload.clone(), &transport().protocol).unwrap();
    assert_eq!(result.sequence, 2);
    drop((received, healthy_request, result));
    let next = h
        .normal
        .submit_affinity(request(3), deadline(), 0)
        .await
        .unwrap();
    let next_request = sibling.receiver.receive(deadline()).await.unwrap().unwrap();
    sibling
        .sender
        .send_owned(reply(next_request.frame(), 64), deadline())
        .await
        .unwrap();
    let received = next.receive().await.unwrap();
    let result: GroupDeliveries =
        decode_payload(received.frame().payload.clone(), &transport().protocol).unwrap();
    assert_eq!(result.sequence, 3);
    drop((received, next_request, result, sibling));
    let memory = h.finish().await;
    until(
        "Sibling-lane recovery must return every native buffer",
        || memory.available_bytes() == TOTAL,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_send_failures_reopen_bounded_lanes_and_physical_close_still_closes_root() {
    let mut rpc_config = config(6, 8 * REQUEST_BYTES);
    rpc_config.liveness_timeout = Duration::from_millis(100);
    let mut h = Harness::new(rpc_config).await;
    let reservation = h.normal.reserve_protected().unwrap();
    let (private, mut private_peer) = h.private(&reservation).await;
    let data = h
        .memory
        .try_reserve(
            TrafficClass::Delivery,
            u32::try_from(h.memory.available_data_bytes()).unwrap(),
        )
        .unwrap();
    let control = h
        .memory
        .try_reserve(
            TrafficClass::Control,
            u32::try_from(h.memory.available_normal_bytes()).unwrap(),
        )
        .unwrap();
    for sequence in 1..=8 {
        let pending = h
            .normal
            .submit(request(sequence), deadline())
            .await
            .unwrap();
        let mut peer = if sequence > 1 {
            Some(
                timeout_at(deadline(), h.peer_session.accept())
                    .await
                    .unwrap()
                    .unwrap(),
            )
        } else {
            None
        };
        assert!(matches!(pending.receive().await, Err(Error::Closed)));
        let failed_peer = peer.as_mut().unwrap_or(&mut h.normal_peer);
        assert!(!matches!(
            failed_peer.receiver.receive(deadline()).await,
            Ok(Some(_))
        ));
        assert!(!h.normal.is_closed());
        assert!(!h.session.connection().is_closed());
        drop(exchange(&private, &mut private_peer, sequence, 16_384).await);
        until(
            "Both halves of every failed ordinary lane must stop",
            || {
                h.normal
                    .inner
                    .tasks
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|task| !task.is_finished())
                    .count()
                    == 1
            },
        )
        .await;
        assert!(h.normal.inner.tasks.lock().unwrap().len() <= 3);
        assert_eq!(h.normal.snapshot().pending_responses, Some(0));
        assert_eq!(h.normal.lane_count(), 0);
        drop(peer);
    }
    drop((data, control));
    h.session.connection().close();
    until(
        "A real connection close must remain visible after all ordinary readers stopped",
        || h.normal.is_closed(),
    )
    .await;
    assert!(matches!(h.normal.reserve_protected(), Err(Error::Closed)));
    private.shutdown(deadline()).await;
    drop((private, private_peer, reservation));
    let memory = h.finish().await;
    until(
        "Repeated failed-lane replacements must release all native ownership",
        || memory.available_bytes() == TOTAL,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_request_receive_admission_failure_preserves_pending_heartbeat_and_ack() {
    for with_session in [true, false] {
        let mut transport_config = transport();
        transport_config.core_streams = 1;
        let mut h = Harness::with_transport(config(8, 8 * REQUEST_BYTES), transport_config).await;
        let session = h
            .client
            .connect(
                Harness::target(&h.server),
                &Harness::credentials(),
                TrafficClass::Control,
                deadline(),
            )
            .await
            .unwrap();
        let peer_session = h.server.accept().await.unwrap();
        let mut core_peer = peer_session.take_initial_streams().await.pop().unwrap();
        let mut watch_peer = None;
        let mut ack_peer = None;
        let rpc = if with_session {
            RpcClient::from_session(session.clone(), config(8, 8 * REQUEST_BYTES))
                .await
                .unwrap()
        } else {
            let mut streams = session.take_initial_streams().await;
            for purpose in [StreamPurpose::Request, StreamPurpose::Ack] {
                streams.push(
                    session
                        .connection()
                        .open(purpose, deadline())
                        .await
                        .unwrap(),
                );
                let peer = timeout_at(deadline(), peer_session.accept())
                    .await
                    .unwrap()
                    .unwrap();
                if purpose == StreamPurpose::Request {
                    watch_peer = Some(peer);
                } else {
                    ack_peer = Some(peer);
                }
            }
            RpcClient::start(streams, config(8, 8 * REQUEST_BYTES)).unwrap()
        };
        let heartbeat_frame = control_frame(
            FrameKind::ConsumerControl,
            &ConsumerControl {
                action: ConsumerAction::HeartbeatConsumer.into(),
                heartbeat_sequence: 1,
                ..Default::default()
            },
        );
        let heartbeat = rpc
            .submit(heartbeat_frame.clone(), deadline())
            .await
            .unwrap();
        let heartbeat_request = core_peer
            .receiver
            .receive(deadline())
            .await
            .unwrap()
            .unwrap();
        let ack_frame = control_frame(FrameKind::GroupAck, &GroupAck::default());
        let ack = rpc.submit(ack_frame.clone(), deadline()).await.unwrap();
        let mut ack_peer = match ack_peer {
            Some(peer) => peer,
            None => timeout_at(deadline(), peer_session.accept())
                .await
                .unwrap()
                .unwrap(),
        };
        let ack_request = ack_peer
            .receiver
            .receive(deadline())
            .await
            .unwrap()
            .unwrap();
        let watch_frame = control_frame(
            FrameKind::GroupWatch,
            &GroupWatch {
                targets: vec![WatchTarget {
                    shard_id: Bytes::from_static(&[4; 16]),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        let watch = rpc.submit(watch_frame.clone(), deadline()).await.unwrap();
        let mut watch_peer = match watch_peer {
            Some(peer) => peer,
            None => timeout_at(deadline(), peer_session.accept())
                .await
                .unwrap()
                .unwrap(),
        };
        let watch_request = watch_peer
            .receiver
            .receive(deadline())
            .await
            .unwrap()
            .unwrap();
        let normal = h
            .memory
            .try_reserve(
                TrafficClass::Control,
                u32::try_from(h.memory.available_normal_bytes()).unwrap(),
            )
            .unwrap();
        assert_eq!(h.memory.available_normal_bytes(), 0);
        assert!(h.memory.available_core_bytes() > 0);
        assert!(h.memory.available_ack_bytes() > 0);
        let watch_result = GroupWatchResult {
            targets: vec![WatchResult {
                shard_id: Bytes::from_static(&[4; 16]),
                status: WatchStatus::WatchChanged.into(),
                observed_token: Bytes::from(vec![7; 256]),
                ..Default::default()
            }],
        };
        let _peer_send = watch_peer
            .sender
            .send_owned(
                control_reply(
                    watch_request.frame(),
                    FrameKind::GroupWatchResult,
                    &watch_result,
                ),
                deadline(),
            )
            .await;
        let unknown = matches!(watch.receive().await, Err(Error::Closed));
        let root_admitting = !rpc.is_closed() && !session.connection().is_closed();
        let pending_preserved = rpc.snapshot().pending_responses == Some(2);
        if root_admitting {
            assert!(pending_preserved);
            assert!(session.connection().core_streams_ready());
            answer_control(
                heartbeat,
                &mut core_peer,
                &heartbeat_request,
                FrameKind::ConsumerControlResult,
                &ConsumerControlResult::default(),
            )
            .await;
            answer_control(
                ack,
                &mut ack_peer,
                &ack_request,
                FrameKind::GroupAckResult,
                &GroupAckResult::default(),
            )
            .await;
            for (frame, peer, kind) in [
                (
                    heartbeat_frame,
                    &mut core_peer,
                    FrameKind::ConsumerControlResult,
                ),
                (ack_frame, &mut ack_peer, FrameKind::GroupAckResult),
            ] {
                let response = rpc.submit(frame, deadline()).await.unwrap();
                let received = peer.receiver.receive(deadline()).await.unwrap().unwrap();
                if kind == FrameKind::ConsumerControlResult {
                    answer_control(
                        response,
                        peer,
                        &received,
                        kind,
                        &ConsumerControlResult::default(),
                    )
                    .await;
                } else {
                    answer_control(response, peer, &received, kind, &GroupAckResult::default())
                        .await;
                }
            }
        } else {
            drop((heartbeat, ack));
        }
        drop(normal);
        let mut reopened_peer = None;
        if root_admitting {
            if with_session {
                let response = rpc.submit(watch_frame, deadline()).await.unwrap();
                let mut peer = timeout_at(deadline(), peer_session.accept())
                    .await
                    .unwrap()
                    .unwrap();
                let received = peer.receiver.receive(deadline()).await.unwrap().unwrap();
                answer_control(
                    response,
                    &mut peer,
                    &received,
                    FrameKind::GroupWatchResult,
                    &watch_result,
                )
                .await;
                reopened_peer = Some(peer);
            } else {
                assert!(matches!(
                    rpc.submit(watch_frame, deadline()).await,
                    Err(Error::Closed)
                ));
            }
            assert_eq!(rpc.snapshot().pending_responses, Some(0));
            session.connection().close();
            until(
                "A real Control connection close must close the RPC root",
                || rpc.is_closed(),
            )
            .await;
        }
        rpc.shutdown(deadline()).await;
        drop((
            rpc,
            core_peer,
            watch_peer,
            ack_peer,
            reopened_peer,
            heartbeat_request,
            ack_request,
            watch_request,
        ));
        session.shutdown(deadline()).await.unwrap();
        peer_session.shutdown(deadline()).await.unwrap();
        drop((session, peer_session));
        let memory = h.finish().await;
        until(
            "Control lane isolation must release every real buffer",
            || memory.available_bytes() == TOTAL,
        )
        .await;
        eprintln!(
            "Control Request admission: with_session={with_session}, ordinary_unknown={unknown}, root_admitting={root_admitting}, core_ack_pending_preserved={pending_preserved}, cleanup_bytes={}",
            memory.available_bytes()
        );
        assert!(unknown);
        assert!(
            root_admitting,
            "Ordinary Control admission must preserve the physical session"
        );
        assert!(
            pending_preserved,
            "Ordinary failure must not discard pending heartbeat or ACK work"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumers_cannot_borrow_private_pending_bytes_slots_or_an_active_lane() {
    let mut h = Harness::new(config(4, 6 * REQUEST_BYTES)).await;
    let first = h.normal.reserve_protected().unwrap();
    let second = h.normal.reserve_protected().unwrap();
    let (a, mut a_peer) = h.private(&first).await;
    let (b, mut b_peer) = h.private(&second).await;
    assert!(matches!(
        a.submit(padded_request(1, REQUEST_BYTES as usize + 1), deadline())
            .await,
        Err(Error::Admission { .. })
    ));
    assert_eq!(first.inner.capacity.slots.available_permits(), 1);
    assert_eq!(
        first.inner.capacity.bytes.available_permits(),
        REQUEST_BYTES as usize
    );
    let pending = a.submit(request(2), deadline()).await.unwrap();
    let received = a_peer.receiver.receive(deadline()).await.unwrap().unwrap();
    assert_eq!(first.inner.capacity.slots.available_permits(), 0);
    assert_eq!(second.inner.capacity.slots.available_permits(), 1);
    assert!(matches!(
        a.submit(request(3), deadline()).await,
        Err(Error::Admission { .. })
    ));
    let before = quotas(&h.budget);
    assert!(matches!(
        h.normal
            .open_protected(&first, Instant::now() + Duration::from_millis(20))
            .await,
        Err(Error::Deadline {
            may_have_been_sent: false
        })
    ));
    assert_eq!(quotas(&h.budget), before);
    drop(exchange(&b, &mut b_peer, 4, 128).await);
    for kind in [FrameKind::GroupDeliveries, FrameKind::Cancel] {
        let mut frame = request(5);
        frame.kind = kind.into();
        assert!(matches!(
            b.submit(frame, deadline()).await,
            Err(Error::Configuration { .. })
        ));
    }
    let mut wrong_class = request(7);
    wrong_class.traffic_class = TrafficClass::Bulk.into();
    assert!(matches!(
        b.submit(wrong_class, deadline()).await,
        Err(Error::Configuration { .. })
    ));
    drop((pending, received));
    let clone = a.clone();
    drop(a);
    assert_eq!(first.inner.active.available_permits(), 0);
    clone.shutdown(deadline()).await;
    drop((clone, a_peer));
    until(
        "The last private client and task must return active ownership",
        || first.inner.active.available_permits() == 1,
    )
    .await;
    drop((b, b_peer));
    until("Dropping a private RPC must stop its owned tasks", || {
        second.inner.active.available_permits() == 1
    })
    .await;
    assert!(!h.normal.is_closed());
    assert!(!h.session.connection().is_closed());
    drop(exchange(&h.normal, &mut h.normal_peer, 8, 1).await);
    drop((first, second));
    let memory = h.finish().await;
    until("Client memory must return after consumer isolation", || {
        memory.available_bytes() == TOTAL
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registration_failures_roll_back_every_quota_and_preserve_one_legal_normal_read() {
    for rpc_config in [
        config(1, 2 * REQUEST_BYTES),
        config(3, 2 * REQUEST_BYTES - 1),
    ] {
        let h = Harness::new(rpc_config).await;
        let before = (quotas(&h.budget), memory_quotas(&h.memory));
        assert!(h.normal.reserve_protected().is_err());
        assert_eq!((quotas(&h.budget), memory_quotas(&h.memory)), before);
        let memory = h.finish().await;
        until("Rejected registrations must not leak", || {
            memory.available_bytes() == TOTAL
        })
        .await;
    }
    let mut h = Harness::new(config(2, 2 * REQUEST_BYTES)).await;
    let keep = 2 * FRAME_BYTES - 1;
    let filler = h
        .memory
        .try_reserve(
            TrafficClass::Delivery,
            u32::try_from(h.memory.available_data_bytes()).unwrap() - keep,
        )
        .unwrap();
    let before = (quotas(&h.budget), memory_quotas(&h.memory));
    assert!(h.normal.reserve_protected().is_err());
    assert_eq!((quotas(&h.budget), memory_quotas(&h.memory)), before);
    drop(filler);
    let filler = h
        .memory
        .try_reserve(
            TrafficClass::Delivery,
            u32::try_from(h.memory.available_data_bytes()).unwrap() - 2 * FRAME_BYTES,
        )
        .unwrap();
    let reservation = h.normal.reserve_protected().unwrap();
    assert_eq!(h.budget.inner.normal.slots.available_permits(), 1);
    assert_eq!(
        h.budget.inner.normal.bytes.available_permits(),
        REQUEST_BYTES as usize
    );
    assert_eq!(h.memory.available_data_bytes(), FRAME_BYTES as usize);
    let before = (quotas(&h.budget), memory_quotas(&h.memory));
    assert!(h.normal.reserve_protected().is_err());
    assert_eq!((quotas(&h.budget), memory_quotas(&h.memory)), before);
    drop(exchange(&h.normal, &mut h.normal_peer, 1, 16_384).await);
    assert!(!h.normal.is_closed());
    drop((reservation, filler));
    let memory = h.finish().await;
    until("The exact floor must fully restore native memory", || {
        memory.available_bytes() == TOTAL
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_stream_capacity_bounds_long_lived_registrations() {
    let h = Harness::new(config(32, 32 * REQUEST_BYTES)).await;
    let connection = h.session.connection();
    let full = RpcBudget::for_connection(
        &config(32, 32 * REQUEST_BYTES),
        &connection,
        connection.request_capacity(),
    )
    .unwrap();
    assert!(full.reserve(&connection).is_err());
    assert_eq!(quotas(&full), [32, 32 * REQUEST_BYTES as usize, 0]);
    let initial = quotas(&h.budget);
    let mut reservations = Vec::new();
    for _ in 0..connection.request_capacity() - 1 {
        reservations.push(h.normal.reserve_protected().unwrap());
    }
    assert_eq!(h.budget.inner.registrations.available_permits(), 0);
    let before = (quotas(&h.budget), memory_quotas(&h.memory));
    assert!(h.normal.reserve_protected().is_err());
    assert_eq!((quotas(&h.budget), memory_quotas(&h.memory)), before);
    drop(reservations);
    assert_eq!(quotas(&h.budget), initial);
    drop((full, connection));
    let memory = h.finish().await;
    until(
        "Registration limits must release their frame regions",
        || memory.available_bytes() == TOTAL,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_private_stream_open_returns_active_credit_without_losing_registration() {
    let h = Harness::new(config(3, 4 * REQUEST_BYTES)).await;
    let reservation = h.normal.reserve_protected().unwrap();
    let mut lanes = Vec::new();
    for _ in 1..h.session.connection().request_capacity() {
        lanes.push(h.ordinary_lane().await);
    }
    let before = (quotas(&h.budget), memory_quotas(&h.memory));
    assert!(matches!(
        h.normal.open_protected(&reservation, deadline()).await,
        Err(Error::Transport {
            source: crate::Error::Admission { .. }
        })
    ));
    assert_eq!(reservation.inner.active.available_permits(), 1);
    assert_eq!(
        reservation.inner.frames.available_bytes(),
        FRAME_BYTES as usize
    );
    assert_eq!((quotas(&h.budget), memory_quotas(&h.memory)), before);
    assert!(!h.session.connection().is_closed());
    let (mut released, mut released_peer) = lanes.pop().unwrap();
    released.sender.reset().unwrap();
    released_peer.sender.reset().unwrap();
    drop((released, released_peer));
    let (private, mut private_peer) = h.private(&reservation).await;
    drop(exchange(&private, &mut private_peer, 1, 16_384).await);
    private.shutdown(deadline()).await;
    drop((private, private_peer, reservation, lanes));
    let memory = h.finish().await;
    until("Failed opens must leave no native resource charge", || {
        memory.available_bytes() == TOTAL
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiting_private_open_must_recheck_parent_drain_before_starting_new_reads() {
    let mut h = Harness::new(config(3, 4 * REQUEST_BYTES)).await;
    let reservation = h.normal.reserve_protected().unwrap();
    let (old_private, old_peer) = h.private(&reservation).await;
    let pending = h.normal.submit(request(1), deadline()).await.unwrap();
    let received = h
        .normal_peer
        .receiver
        .receive(deadline())
        .await
        .unwrap()
        .unwrap();
    let mut opening = Box::pin(h.normal.open_protected(&reservation, deadline()));
    arm_pending(opening.as_mut()).await;
    let mut closing = Box::pin(h.normal.close(deadline()));
    arm_pending(closing.as_mut()).await;
    assert!(h.normal.inner.state.draining.load(Ordering::Acquire));
    assert!(!h.session.connection().is_closed());
    old_private.shutdown(deadline()).await;
    drop((old_private, old_peer));
    let opened = opening.await;
    let started_new_read = if let Ok(private) = opened {
        let mut peer = timeout_at(deadline(), h.peer_session.accept())
            .await
            .unwrap()
            .unwrap();
        drop(exchange(&private, &mut peer, 2, 1).await);
        private.shutdown(deadline()).await;
        drop((private, peer));
        true
    } else {
        false
    };
    drop(closing);
    drop((pending, received, reservation));
    let memory = h.finish().await;
    until("The drain-race fixture must clean up all resources", || {
        memory.available_bytes() == TOTAL
    })
    .await;
    assert!(
        !started_new_read,
        "A private opener queued before parent drain must not start a new GroupRead after drain begins"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_private_lane_rejects_new_reads_during_parent_drain_but_finishes_admitted_work() {
    let mut h = Harness::new(config(3, 4 * REQUEST_BYTES)).await;
    let reservation = h.normal.reserve_protected().unwrap();
    let (private, mut peer) = h.private(&reservation).await;
    let private_response = private.submit(request(1), deadline()).await.unwrap();
    let private_request = peer.receiver.receive(deadline()).await.unwrap().unwrap();
    let ordinary_response = h.normal.submit(request(2), deadline()).await.unwrap();
    let ordinary_request = h
        .normal_peer
        .receiver
        .receive(deadline())
        .await
        .unwrap()
        .unwrap();
    let mut closing = Box::pin(h.normal.close(deadline()));
    arm_pending(closing.as_mut()).await;
    assert!(h.normal.inner.state.draining.load(Ordering::Acquire));
    assert!(!h.session.connection().is_closed());
    let before = quotas(&h.budget);
    assert!(matches!(
        private.submit(request(3), deadline()).await,
        Err(Error::Closed)
    ));
    assert_eq!(quotas(&h.budget), before);
    peer.sender
        .send_owned(reply(private_request.frame(), 64), deadline())
        .await
        .unwrap();
    let accepted = private_response.receive().await.unwrap();
    assert_eq!(
        accepted.frame().request_id,
        private_request.frame().request_id
    );
    drop(accepted);
    until(
        "The already admitted private request must finish during drain",
        || {
            reservation.inner.capacity.slots.available_permits() == 1
                && reservation.inner.capacity.bytes.available_permits() == REQUEST_BYTES as usize
        },
    )
    .await;
    assert!(matches!(
        private.submit(request(4), deadline()).await,
        Err(Error::Closed)
    ));
    assert!(!h.session.connection().is_closed());
    assert_eq!(h.normal.snapshot().pending_responses, Some(1));
    h.normal_peer
        .sender
        .send_owned(reply(ordinary_request.frame(), 64), deadline())
        .await
        .unwrap();
    let accepted = ordinary_response.receive().await.unwrap();
    assert_eq!(
        accepted.frame().request_id,
        ordinary_request.frame().request_id
    );
    drop(accepted);
    closing.await;
    private.shutdown(deadline()).await;
    drop((
        private,
        peer,
        private_request,
        ordinary_request,
        reservation,
    ));
    let memory = h.finish().await;
    until(
        "Parent drain must preserve only real outstanding ownership",
        || memory.available_bytes() == TOTAL,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_reader_and_writer_failures_release_quotas_without_closing_the_shared_session() {
    for invalid_response in [false, true] {
        let mut h = Harness::new(config(3, 4 * REQUEST_BYTES)).await;
        let reservation = h.normal.reserve_protected().unwrap();
        let (private, mut peer) = h.private(&reservation).await;
        let mut frame = request(1);
        if !invalid_response {
            frame.protocol_version = 0;
        }
        let response = private.submit(frame, deadline()).await.unwrap();
        if invalid_response {
            let received = peer.receiver.receive(deadline()).await.unwrap().unwrap();
            let mut wrong_operation = reply(received.frame(), 16_384);
            wrong_operation.operation_id = Bytes::copy_from_slice(Id::new_v7().unwrap().as_bytes());
            peer.sender
                .send_owned(wrong_operation, deadline())
                .await
                .unwrap();
        }
        assert!(matches!(response.receive().await, Err(Error::Closed)));
        assert!(private.is_closed());
        assert_eq!(private.snapshot().pending_responses, Some(0));
        private.shutdown(deadline()).await;
        drop((private, peer));
        until(
            "Failed private tasks must release active, pending and native frame ownership",
            || {
                reservation.inner.active.available_permits() == 1
                    && reservation.inner.capacity.slots.available_permits() == 1
                    && reservation.inner.capacity.bytes.available_permits()
                        == REQUEST_BYTES as usize
                    && reservation.inner.frames.available_bytes() == FRAME_BYTES as usize
            },
        )
        .await;
        assert!(!h.session.connection().is_closed());
        assert!(!h.normal.is_closed());
        let (replacement, mut peer) = h.private(&reservation).await;
        drop(exchange(&replacement, &mut peer, 2, 1).await);
        replacement.shutdown(deadline()).await;
        drop((replacement, peer, reservation));
        drop(exchange(&h.normal, &mut h.normal_peer, 3, 1).await);
        let memory = h.finish().await;
        until(
            "Both task-failure paths must restore all fixture memory",
            || memory.available_bytes() == TOTAL,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_small_control_limit_must_not_strand_a_legal_protected_read_with_trace_context() {
    let mut transport_config = transport();
    transport_config.protocol.max_control_frame_bytes = 128;
    let mut h = Harness::with_transport(config(4, 4 * 132), transport_config.clone()).await;
    let mut traced = request(1);
    traced.traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into();
    let request_bytes = traced.encoded_len();
    assert!(request_bytes > 132);
    tidemq_protocol::framing::encode_frame(&traced, &transport_config.protocol).unwrap();
    let ordinary = h.normal.submit(traced.clone(), deadline()).await.unwrap();
    let received = h
        .normal_peer
        .receiver
        .receive(deadline())
        .await
        .unwrap()
        .unwrap();
    h.normal_peer
        .sender
        .send_owned(reply(received.frame(), 1), deadline())
        .await
        .unwrap();
    drop((ordinary.receive().await.unwrap(), received));
    until(
        "The traced ordinary request must release its transient memory",
        || {
            h.memory.available_bytes()
                == TOTAL - transport().transport_reservation().unwrap() as usize
        },
    )
    .await;
    let before = (quotas(&h.budget), memory_quotas(&h.memory));
    let reservation = match h.normal.reserve_protected() {
        Ok(reservation) => reservation,
        Err(Error::Configuration { .. }) => {
            assert_eq!((quotas(&h.budget), memory_quotas(&h.memory)), before);
            drop(exchange(&h.normal, &mut h.normal_peer, 2, 1).await);
            let memory = h.finish().await;
            until(
                "An explicit registration rejection must restore its admission",
                || memory.available_bytes() == TOTAL,
            )
            .await;
            return;
        }
        Err(error) => panic!("Unexpected private registration error: {error}"),
    };
    let reserved_bytes = reservation.inner.capacity.max_bytes;
    let (private, mut peer) = h.private(&reservation).await;
    let response = private.submit(traced, deadline()).await;
    let admitted = if let Ok(response) = response {
        let received = peer.receiver.receive(deadline()).await.unwrap().unwrap();
        peer.sender
            .send_owned(reply(received.frame(), 1), deadline())
            .await
            .unwrap();
        drop(response.receive().await.unwrap());
        true
    } else {
        false
    };
    private.shutdown(deadline()).await;
    drop((private, peer, reservation));
    let memory = h.finish().await;
    until(
        "Trace-boundary validation must release all fixture memory",
        || memory.available_bytes() == TOTAL,
    )
    .await;
    assert!(
        admitted,
        "An accepted private registration must fit a legal GroupRead with the SDK's trace context: request bytes={request_bytes}, reserved bytes={reserved_bytes}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maximal_group_read_encoding_fits_the_minimum_private_pending_reservation() {
    let mut transport_config = transport();
    transport_config.protocol.max_control_frame_bytes = 252;
    // List every field explicitly so protocol growth requires updating this bound.
    let payload = GroupRead {
        topic_id: Bytes::from_static(&[1; 16]),
        partition_id: u32::MAX,
        subscription_id: Bytes::from_static(&[2; 16]),
        member_id: Bytes::from_static(&[3; 16]),
        member_epoch: u64::MAX,
        sequence: u64::MAX,
        max_messages: u32::MAX,
        max_bytes: u64::MAX,
    };
    assert_eq!(payload.encoded_len(), 99);
    let mut frame = new_frame(
        FrameKind::GroupRead,
        TrafficClass::Delivery,
        Id::new_v7().unwrap(),
        Id::new_v7().unwrap(),
        &payload,
        tidemq_protocol::framing::MAX_TIMEOUT_MS,
        &transport_config.protocol,
    )
    .unwrap();
    frame.traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into();
    let wire = tidemq_protocol::framing::encode_frame(&frame, &transport_config.protocol).unwrap();
    assert!(
        wire.len() <= 256,
        "Maximal GroupRead wire size grew to {} bytes",
        wire.len()
    );
    eprintln!(
        "Maximal GroupRead: payload={} bytes, frame={} bytes, wire={} bytes, reserved=256 bytes",
        payload.encoded_len(),
        frame.encoded_len(),
        wire.len()
    );
    let h = Harness::with_transport(config(4, 4 * 256), transport_config).await;
    let reservation = h.normal.reserve_protected().unwrap();
    assert_eq!(reservation.inner.capacity.max_bytes, 256);
    let (private, mut peer) = h.private(&reservation).await;
    let response = private.submit(frame, deadline()).await.unwrap();
    let received = peer.receiver.receive(deadline()).await.unwrap().unwrap();
    let decoded: GroupRead =
        decode_payload(received.frame().payload.clone(), &transport().protocol).unwrap();
    assert_eq!(decoded, payload);
    peer.sender
        .send_owned(reply(received.frame(), 1), deadline())
        .await
        .unwrap();
    let response = response.receive().await.unwrap();
    assert_eq!(response.frame().request_id, received.frame().request_id);
    drop((response, received, decoded, private, peer, reservation));
    let memory = h.finish().await;
    until(
        "The minimum protected pending reservation must clean up completely",
        || memory.available_bytes() == TOTAL,
    )
    .await;
}

#[tokio::test]
async fn wrong_rpc_budget_memory_source_class_and_configuration_are_rejected() {
    let rpc_config = config(4, 4 * REQUEST_BYTES);
    let mut h = Harness::new(rpc_config.clone()).await;
    let foreign = RpcBudget::for_connection(&rpc_config, &h.session.connection(), 1).unwrap();
    let foreign_reservation = foreign.reserve(&h.session.connection()).unwrap();
    let before = (quotas(&h.budget), memory_quotas(&h.memory));
    assert!(matches!(
        h.normal
            .open_protected(&foreign_reservation, deadline())
            .await,
        Err(Error::Configuration { .. })
    ));
    assert_eq!((quotas(&h.budget), memory_quotas(&h.memory)), before);
    assert_eq!(foreign_reservation.inner.active.available_permits(), 1);
    for (changed, lanes) in [
        (config(5, 4 * REQUEST_BYTES), 1),
        (config(4, 4 * REQUEST_BYTES + 1), 1),
        (rpc_config.clone(), 2),
    ] {
        assert!(matches!(
            RpcClient::from_session_with_lanes_and_budget(
                h.session.clone(),
                changed,
                lanes,
                h.budget.clone(),
                deadline()
            )
            .await,
            Err(Error::Configuration { .. })
        ));
    }
    for lanes in [0, 65, h.session.connection().request_capacity() + 1] {
        assert!(RpcBudget::for_connection(&rpc_config, &h.session.connection(), lanes).is_err());
    }
    let mut other = Harness::new(rpc_config.clone()).await;
    assert!(matches!(
        RpcClient::from_session_with_lanes_and_budget(
            other.session.clone(),
            rpc_config.clone(),
            1,
            h.budget.clone(),
            deadline()
        )
        .await,
        Err(Error::Configuration { .. })
    ));
    let control = h
        .client
        .connect(
            Harness::target(&h.server),
            &Harness::credentials(),
            TrafficClass::Control,
            deadline(),
        )
        .await
        .unwrap();
    let control_peer = h.server.accept().await.unwrap();
    assert!(RpcBudget::for_connection(&rpc_config, &control.connection(), 1).is_err());
    control.shutdown(deadline()).await.unwrap();
    control_peer.shutdown(deadline()).await.unwrap();
    drop((control, control_peer, foreign, foreign_reservation));
    drop(exchange(&h.normal, &mut h.normal_peer, 1, 1).await);
    drop(exchange(&other.normal, &mut other.normal_peer, 2, 1).await);
    let memory = other.finish().await;
    until("The unrelated source must remain usable and clean", || {
        memory.available_bytes() == TOTAL
    })
    .await;
    let reservation = h.normal.reserve_protected().unwrap();
    let connection = h.session.connection();
    // The single-thread runtime keeps the reader unpolled throughout this classification window.
    connection.close();
    assert!(!h.normal.is_closed());
    assert!(matches!(
        RpcBudget::for_connection(&rpc_config, &connection, 1),
        Err(Error::Closed)
    ));
    assert!(matches!(h.normal.reserve_protected(), Err(Error::Closed)));
    assert!(matches!(
        h.normal.open_protected(&reservation, deadline()).await,
        Err(Error::Closed)
    ));
    assert!(!h.normal.is_closed());
    drop((connection, reservation));
    let memory = h.finish().await;
    until(
        "Source validation must leave no registration charge",
        || memory.available_bytes() == TOTAL,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_reuses_one_root_budget_without_duplicating_registration_or_active_credit() {
    let rpc_config = config(2, 2 * REQUEST_BYTES);
    let mut h = Harness::new(rpc_config.clone()).await;
    let reservation = h.normal.reserve_protected().unwrap();
    let reservation_clone = reservation.clone();
    let (old_private, mut old_peer) = h.private(&reservation).await;
    let response = old_private.submit(request(1), deadline()).await.unwrap();
    let received = old_peer
        .receiver
        .receive(deadline())
        .await
        .unwrap()
        .unwrap();
    let before = quotas(&h.budget);
    h.normal.shutdown(deadline()).await;
    until(
        "The old physical connection must stop its private RPC",
        || old_private.is_closed(),
    )
    .await;
    assert!(matches!(response.receive().await, Err(Error::Closed)));
    let session = h
        .client
        .connect(
            Harness::target(&h.server),
            &Harness::credentials(),
            TrafficClass::Delivery,
            deadline(),
        )
        .await
        .unwrap();
    let peer = h.server.accept().await.unwrap();
    let normal = RpcClient::from_session_with_lanes_and_budget(
        session.clone(),
        rpc_config.clone(),
        1,
        h.budget.clone(),
        deadline(),
    )
    .await
    .unwrap();
    let normal_peer = peer.take_initial_streams().await.pop().unwrap();
    assert!(Arc::ptr_eq(
        &normal.inner.state.slots,
        &h.budget.inner.normal.slots
    ));
    assert!(Arc::ptr_eq(
        &normal.inner.state.bytes,
        &h.budget.inner.normal.bytes
    ));
    assert_eq!(quotas(&h.budget), before);
    assert!(normal.reserve_protected().is_err());
    assert!(matches!(
        normal
            .open_protected(
                &reservation_clone,
                Instant::now() + Duration::from_millis(20)
            )
            .await,
        Err(Error::Deadline {
            may_have_been_sent: false
        })
    ));
    old_private.shutdown(deadline()).await;
    drop((old_private, old_peer, received, reservation_clone));
    until(
        "Old task ownership must end before reconnect borrows the active lane",
        || reservation.inner.active.available_permits() == 1,
    )
    .await;
    let private = normal
        .open_protected(&reservation, deadline())
        .await
        .unwrap();
    let mut private_peer = timeout_at(deadline(), peer.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(quotas(&h.budget), before);
    drop(exchange(&private, &mut private_peer, 2, 16_384).await);
    assert_eq!(quotas(&h.budget), before);
    private.shutdown(deadline()).await;
    drop((private, private_peer, reservation));
    until(
        "Dropping the final registration must restore the common root",
        || {
            quotas(&h.budget)
                == [
                    rpc_config.max_pending_requests,
                    rpc_config.max_pending_bytes as usize,
                    1,
                ]
        },
    )
    .await;
    assert!(!session.connection().is_closed());
    normal.shutdown(deadline()).await;
    drop((normal, normal_peer));
    session.shutdown(deadline()).await.unwrap();
    peer.shutdown(deadline()).await.unwrap();
    drop((session, peer));
    let memory = h.finish().await;
    until(
        "Reconnect generations must not duplicate native memory",
        || memory.available_bytes() == TOTAL,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_and_expired_private_waiters_discard_late_replies_and_keep_clone_charges() {
    let rpc_config = config(4, 4 * REQUEST_BYTES);
    let mut h = Harness::new(rpc_config.clone()).await;
    let reservation = h.normal.reserve_protected().unwrap();
    let (private, mut peer) = h.private(&reservation).await;
    for expired in [false, true] {
        let request_deadline = if expired {
            Instant::now() + Duration::from_millis(50)
        } else {
            deadline()
        };
        let response = private
            .submit(request(if expired { 3 } else { 1 }), request_deadline)
            .await
            .unwrap();
        let received = peer.receiver.receive(deadline()).await.unwrap().unwrap();
        if expired {
            assert!(matches!(
                response.receive().await,
                Err(Error::Deadline {
                    may_have_been_sent: true
                })
            ));
        } else {
            drop(response);
        }
        until(
            "Ended attempts must release native pending admission",
            || {
                reservation.inner.capacity.slots.available_permits() == 1
                    && reservation.inner.capacity.bytes.available_permits()
                        == REQUEST_BYTES as usize
            },
        )
        .await;
        let next = private
            .submit(request(if expired { 4 } else { 2 }), deadline())
            .await
            .unwrap();
        let next_request = peer.receiver.receive(deadline()).await.unwrap().unwrap();
        let received_before = h.client.metrics.snapshot().frames_received_total;
        peer.sender
            .send_owned(reply(received.frame(), 16_384), deadline())
            .await
            .unwrap();
        until(
            "Late replies must receive and release fresh carved frame admission",
            || {
                h.client.metrics.snapshot().frames_received_total > received_before
                    && reservation.inner.frames.available_bytes() == FRAME_BYTES as usize
            },
        )
        .await;
        assert_eq!(private.snapshot().pending_responses, Some(1));
        assert!(
            private
                .inner
                .state
                .pending
                .lock()
                .unwrap()
                .contains_key(&Id::from_slice(&next_request.frame().request_id).unwrap())
        );
        peer.sender
            .send_owned(reply(next_request.frame(), 1), deadline())
            .await
            .unwrap();
        let accepted = next.receive().await.unwrap();
        assert_eq!(accepted.frame().request_id, next_request.frame().request_id);
        drop(accepted);
    }
    let received = exchange(&private, &mut peer, 5, 16_384).await;
    let charged = received.frame().encoded_len() * 2;
    let mut deliveries: GroupDeliveries =
        decode_payload(received.frame().payload.clone(), &transport().protocol).unwrap();
    let body = deliveries.deliveries.pop().unwrap().body;
    let retained = body.slice(..1);
    drop((received, deliveries, body));
    until(
        "Only the decoded native response may remain charged",
        || reservation.inner.frames.available_bytes() == FRAME_BYTES as usize - charged,
    )
    .await;
    private.shutdown(deadline()).await;
    drop((private, peer, reservation));
    until(
        "Native payload ownership must not retain released RPC registration slots",
        || {
            quotas(&h.budget)
                == [
                    rpc_config.max_pending_requests,
                    rpc_config.max_pending_bytes as usize,
                    3,
                ]
        },
    )
    .await;
    assert!(!h.session.connection().is_closed());
    drop(exchange(&h.normal, &mut h.normal_peer, 6, 1).await);
    let memory = h.finish().await;
    assert_eq!(memory.available_bytes(), TOTAL - FRAME_BYTES as usize);
    drop(retained);
    assert_eq!(memory.available_bytes(), TOTAL);
    assert_eq!(memory.available_data_bytes(), memory.data_capacity());
}
