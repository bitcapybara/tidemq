use super::*;
use crate::budget::MemoryLimits;
use crate::{
    Client, ClientCredentials, ClientTls, PeerTarget, Server, ServerAuthorization, ServerTls,
    Session, TlsIdentity, TokenGrant,
};
use std::time::Duration;
use tidemq_protocol::Id;
use tidemq_protocol::framing::{PROTOCOL_VERSION, decode_payload};

fn frame(payload: Bytes) -> Frame {
    Frame {
        protocol_version: PROTOCOL_VERSION,
        request_id: Bytes::from_static(&[1; 16]),
        operation_id: Bytes::from_static(&[2; 16]),
        traffic_class: TrafficClass::Produce.into(),
        kind: FrameKind::Publish.into(),
        payload,
        remaining_timeout_ms: 1_000,
        ..Default::default()
    }
}

fn budget(total_bytes: u64) -> MemoryBudget {
    MemoryBudget::new(MemoryLimits {
        total_bytes,
        control_reserved_bytes: total_bytes / 4,
    })
    .unwrap()
}

fn encode(frame: Frame, budget: &MemoryBudget) -> ([Bytes; 3], u32) {
    encode_charged_owned(
        frame,
        FrameAdmission::Normal(TrafficClass::Produce),
        &TransportConfig::default(),
        budget,
        Instant::now() + std::time::Duration::from_secs(10),
    )
    .unwrap()
}

#[test]
fn owned_payload_shares_storage_and_keeps_all_chunks_charged() {
    let budget = budget(2 * 1024 * 1024);
    let available = budget.available_bytes();
    let payload = Bytes::from(vec![3; 64 * 1024]);
    let pointer = payload.as_ptr();
    let frame = frame(payload);
    let expected = encode_frame(&frame, &TransportConfig::default().protocol).unwrap();
    let (chunks, length) = encode(frame, &budget);
    let [header, payload, trailer] = &chunks;
    assert_eq!(payload.as_ptr(), pointer);
    assert_eq!(usize::try_from(length).unwrap(), expected.len());
    assert_eq!(
        [header.as_ref(), payload.as_ref(), trailer.as_ref()].concat(),
        expected
    );
    let charged = available - budget.available_bytes();
    assert!(charged >= 64 * 1024);
    let retained_payload = payload.slice(..1);
    let retained_header = header.clone();
    drop(chunks);
    assert_eq!(budget.available_bytes(), available - charged);
    drop(retained_payload);
    assert_eq!(budget.available_bytes(), available - charged);
    drop(retained_header);
    assert_eq!(budget.available_bytes(), available);
}

#[test]
fn unique_offset_slice_charges_the_original_allocation_capacity() {
    let budget = budget(2 * 1024 * 1024);
    let original = Bytes::from(vec![3; 512 * 1024]);
    let payload = original.slice(256 * 1024..288 * 1024);
    drop(original);
    let available = budget.available_bytes();
    let (chunks, _) = encode(frame(payload), &budget);
    let [_, payload, _] = &chunks;
    assert_eq!(payload.len(), 32 * 1024);
    assert!(available - budget.available_bytes() >= 512 * 1024);
    drop(chunks);
    assert_eq!(budget.available_bytes(), available);
}

#[test]
fn oversized_or_unknown_shared_owners_fall_back_to_a_bounded_copy() {
    for keep_original in [false, true] {
        let budget = budget(128 * 1024);
        let original = Bytes::from(vec![3; 1024 * 1024]);
        let payload = original.slice(512 * 1024..544 * 1024);
        let original = keep_original.then_some(original);
        let available = budget.available_bytes();
        let (chunks, length) = encode(frame(payload), &budget);
        let [wire, payload, trailer] = &chunks;
        assert!(!wire.is_empty());
        assert!(payload.is_empty() && trailer.is_empty());
        assert_eq!(available - budget.available_bytes(), length as usize);
        drop(original);
        drop(chunks);
        assert_eq!(budget.available_bytes(), available);
    }
}

#[test]
fn nested_decoded_payload_retains_receive_admission_until_its_final_clone() {
    let budget = budget(256 * 1024);
    let available = budget.available_bytes();
    let limits = TransportConfig::default().protocol;
    let message = tidemq_protocol::public::PublishMessage {
        payload: Bytes::from(vec![7; 32 * 1024]),
        ..Default::default()
    };
    let mut frame = frame(Bytes::new());
    frame.payload =
        tidemq_protocol::framing::encode_payload(&message, limits.max_frame_bytes).unwrap();
    let body = tidemq_protocol::framing::encode_payload(&frame, limits.max_frame_bytes).unwrap();
    let charge = u32::try_from(body.len() * 2).unwrap();
    let body = Bytes::from_owner(ChargedBytes {
        bytes: body,
        _memory: budget.try_reserve(TrafficClass::Produce, charge).unwrap(),
    });
    let frame = decode_frame(body, TrafficClass::Produce, &limits).unwrap();
    let nested =
        decode_payload::<tidemq_protocol::public::PublishMessage>(frame.payload.clone(), &limits)
            .unwrap();
    let retained = nested.payload.slice(..1);
    drop(nested);
    drop(frame);
    assert_eq!(budget.available_bytes(), available - charge as usize);
    drop(retained);
    assert_eq!(budget.available_bytes(), available);
}

fn delivery_frame(config: &TransportConfig) -> Frame {
    let size = usize::try_from(config.protocol.max_frame_bytes).unwrap();
    let mut frame = frame(Bytes::from(vec![7; size]));
    frame.traffic_class = TrafficClass::Delivery.into();
    frame.kind = FrameKind::GroupDeliveries.into();
    let overhead = frame.encoded_len() - frame.payload.len();
    frame.payload = Bytes::from(vec![7; size - overhead]);
    assert_eq!(frame.encoded_len(), size);
    frame
}

fn native_config() -> TransportConfig {
    TransportConfig {
        send_buffer_bytes: 1_024,
        protocol: tidemq_protocol::ProtocolLimits {
            max_frame_bytes: 32_768,
            max_message_bytes: 16_384,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn reservation_bytes(config: &TransportConfig) -> u32 {
    config.protocol.max_frame_bytes * 3 + 8
}

fn native_target(server: &Server) -> PeerTarget {
    PeerTarget {
        address: server.local_addr(),
        server_name: "localhost".into(),
        local_role: "sdk".into(),
        remote_role: "gateway".into(),
        cluster_id: None,
        node_id: None,
    }
}

fn native_credentials() -> ClientCredentials {
    ClientCredentials::External {
        session_id: Id::new_v7().unwrap(),
        access_token: "reserved-native-frame-test-token-with-32-characters".into(),
    }
}

fn native_deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

async fn native_pair(
    memory: &MemoryBudget,
    class: TrafficClass,
) -> (Client, Server, Arc<Session>, Arc<Session>) {
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
    let certificate = params.self_signed(&key).unwrap();
    let certificate = Bytes::from(certificate.pem());
    let identity = TlsIdentity::new(certificate.clone(), Bytes::from(key.serialize_pem())).unwrap();
    let grant = TokenGrant::new(
        "reserved-native-frame-test-token-with-32-characters",
        "consumer".into(),
        "consume".into(),
        vec![FrameKind::GroupRead, FrameKind::GroupDeliveries],
    )
    .unwrap();
    let mut server = Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        ServerTls {
            identity,
            client_trust_roots: None,
        },
        ServerAuthorization::External(vec![grant]),
        native_config(),
        memory.clone(),
    )
    .unwrap();
    let client = Client::bind(
        "127.0.0.1:0".parse().unwrap(),
        ClientTls {
            trust_roots: certificate,
            identity: None,
            expected_nodes: vec![],
        },
        native_config(),
        memory.clone(),
    )
    .unwrap();
    let session = client
        .connect(
            native_target(&server),
            &native_credentials(),
            class,
            native_deadline(),
        )
        .await
        .unwrap();
    let accepted = server.accept().await.unwrap();
    (client, server, session, accepted)
}

async fn close_native_pair(
    client: Client,
    server: Server,
    session: Arc<Session>,
    peer: Arc<Session>,
) {
    session.shutdown(native_deadline()).await.unwrap();
    peer.shutdown(native_deadline()).await.unwrap();
    drop((session, peer));
    client.close(native_deadline()).await.unwrap();
    drop(client);
    server.shutdown().await.unwrap();
}

#[test]
fn reserved_encode_failures_restore_capacity_without_falling_back_to_normal_memory() {
    let budget = budget(256 * 1024);
    let config = native_config();
    let reservation = budget
        .reserve_frames(TrafficClass::Delivery, reservation_bytes(&config))
        .unwrap();
    let before = budget.available_bytes();
    let mut invalid = delivery_frame(&config);
    invalid.protocol_version = 0;
    assert!(
        encode_charged_owned(
            invalid,
            FrameAdmission::Reserved(reservation.clone()),
            &config,
            &budget,
            native_deadline(),
        )
        .is_err()
    );
    assert_eq!(
        reservation.available_bytes(),
        reservation_bytes(&config) as usize
    );
    assert_eq!(budget.available_bytes(), before);
    let (outbound, _) = encode_charged_owned(
        delivery_frame(&config),
        FrameAdmission::Reserved(reservation.clone()),
        &config,
        &budget,
        native_deadline(),
    )
    .unwrap();
    let inbound = reservation
        .try_reserve(&budget, 2 * config.protocol.max_frame_bytes)
        .unwrap();
    assert_eq!(reservation.available_bytes(), 4);
    drop((outbound, inbound));
    let receive = reservation
        .try_reserve(&budget, 2 * config.protocol.max_frame_bytes)
        .unwrap();
    let remaining = reservation.available_bytes();
    let (wire, length) = encode_charged_owned(
        delivery_frame(&config),
        FrameAdmission::Reserved(reservation.clone()),
        &config,
        &budget,
        native_deadline(),
    )
    .unwrap();
    let [header, payload, trailer] = &wire;
    assert!(payload.is_empty() && trailer.is_empty());
    assert_eq!(header.len(), length as usize);
    assert_eq!(reservation.available_bytes(), remaining - length as usize);
    assert_eq!(budget.available_bytes(), before);
    assert!(
        encode_charged_owned(
            delivery_frame(&config),
            FrameAdmission::Reserved(reservation.clone()),
            &config,
            &budget,
            native_deadline(),
        )
        .is_err()
    );
    assert!(budget.try_reserve(TrafficClass::Delivery, 1).is_ok());
    drop((wire, receive));
    assert_eq!(
        reservation.available_bytes(),
        reservation_bytes(&config) as usize
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_maximum_receive_and_send_overlap_survives_saturated_normal_memory() {
    let memory = budget(2 * 1024 * 1024);
    let total = memory.available_bytes();
    let config = native_config();
    let region_bytes = reservation_bytes(&config);
    let (client, server, session, peer) = native_pair(&memory, TrafficClass::Delivery).await;
    let connection = session.connection();
    let reservation = connection.reserve_frames(region_bytes).unwrap();
    let peer_reservation = peer.connection().reserve_frames(region_bytes).unwrap();
    let capacity = connection.request_capacity();
    let mut outgoing = connection
        .open(StreamPurpose::Request, native_deadline())
        .await
        .unwrap()
        .with_reserved_frames(&reservation)
        .unwrap();
    let mut incoming = peer
        .accept()
        .await
        .unwrap()
        .with_reserved_frames(&peer_reservation)
        .unwrap();
    assert_eq!(connection.request_capacity(), capacity);
    assert_eq!(capacity, config.max_streams);
    drop(connection);
    drop(session.take_initial_streams().await);
    drop(peer.take_initial_streams().await);
    let ordinary = memory
        .try_reserve(
            TrafficClass::Delivery,
            u32::try_from(memory.available_data_bytes()).unwrap(),
        )
        .unwrap();
    let normal_control = memory
        .try_reserve(
            TrafficClass::Control,
            u32::try_from(memory.available_normal_bytes()).unwrap(),
        )
        .unwrap();
    assert_eq!(memory.available_data_bytes(), 0);
    assert_eq!(memory.available_normal_bytes(), 0);
    assert!(
        encode_charged(
            &delivery_frame(&config),
            FrameAdmission::Normal(TrafficClass::Delivery),
            &config,
            &memory,
            native_deadline()
        )
        .is_err()
    );
    let root_available = memory.available_bytes();
    let (sent, received) = tokio::join!(
        incoming
            .sender
            .send_owned(delivery_frame(&config), native_deadline()),
        outgoing.receiver.receive(native_deadline()),
    );
    sent.unwrap();
    let received = received.unwrap().unwrap();
    assert_eq!(
        received.frame().encoded_len(),
        config.protocol.max_frame_bytes as usize
    );
    incoming.sender.flush(native_deadline()).await.unwrap();
    assert_eq!(
        reservation.available_bytes(),
        config.protocol.max_frame_bytes as usize + 8
    );
    let payload = received.frame().payload.clone();
    let retained = payload.slice(..1);
    drop(received);
    let (sent, response) = tokio::join!(
        outgoing
            .sender
            .send_owned(delivery_frame(&config), native_deadline()),
        incoming.receiver.receive(native_deadline()),
    );
    sent.unwrap();
    let response = response.unwrap().unwrap();
    assert_eq!(response.frame().payload.len(), payload.len());
    assert_eq!(memory.available_bytes(), root_available);
    outgoing.sender.flush(native_deadline()).await.unwrap();
    drop((
        response,
        outgoing,
        incoming,
        reservation,
        peer_reservation,
        ordinary,
        normal_control,
        payload,
    ));
    close_native_pair(client, server, session, peer).await;
    timeout_at(native_deadline(), async {
        while memory.available_bytes() != total - region_bytes as usize {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(retained);
    assert_eq!(memory.available_bytes(), total);
    assert_eq!(memory.available_data_bytes(), memory.data_capacity());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_reservation_reconnects_only_on_its_original_budget_and_delivery_class() {
    let memory = budget(2 * 1024 * 1024);
    let total = memory.available_bytes();
    let (client, mut server, session, peer) = native_pair(&memory, TrafficClass::Delivery).await;
    let reservation = session.connection().reserve_frames(128).unwrap();
    let other = budget(2 * 1024 * 1024);
    let foreign = other.reserve_frames(TrafficClass::Delivery, 128).unwrap();
    let lane = session.take_initial_streams().await.pop().unwrap();
    assert!(matches!(
        lane.with_reserved_frames(&foreign),
        Err(Error::Memory { .. })
    ));
    assert_eq!(foreign.available_bytes(), 128);
    session.shutdown(native_deadline()).await.unwrap();
    peer.shutdown(native_deadline()).await.unwrap();
    drop(peer.take_initial_streams().await);
    drop((session, peer));
    let reconnected = client
        .connect(
            native_target(&server),
            &native_credentials(),
            TrafficClass::Delivery,
            native_deadline(),
        )
        .await
        .unwrap();
    let peer = server.accept().await.unwrap();
    let lane = reconnected
        .take_initial_streams()
        .await
        .pop()
        .unwrap()
        .with_reserved_frames(&reservation)
        .unwrap();
    assert_eq!(reservation.available_bytes(), 128);
    drop((lane, reservation, foreign));
    drop(peer.take_initial_streams().await);
    close_native_pair(client, server, reconnected, peer).await;
    timeout_at(native_deadline(), async {
        while memory.available_bytes() != total {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(other.available_bytes(), total);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_pre_enqueue_deadline_preserves_sender_and_peer() {
    let memory = budget(2 * 1024 * 1024);
    let total = memory.available_bytes();
    let (client, server, session, peer) = native_pair(&memory, TrafficClass::Delivery).await;
    let mut outgoing = session.take_initial_streams().await.pop().unwrap();
    let mut incoming = peer.take_initial_streams().await.pop().unwrap();
    let before = client.metrics.snapshot().frames_sent_total;
    let deadlines_before = client.metrics.snapshot().deadlines_total;
    let expired = outgoing
        .sender
        .send_owned(delivery_frame(&native_config()), Instant::now())
        .await;
    let usable = outgoing.sender.usable;
    let counted_deadline = client.metrics.snapshot().deadlines_total == deadlines_before + 1;
    assert_eq!(client.metrics.snapshot().frames_sent_total, before);
    if usable {
        let (sent, received) = tokio::join!(
            outgoing
                .sender
                .send_owned(delivery_frame(&native_config()), native_deadline()),
            incoming.receiver.receive(native_deadline()),
        );
        sent.unwrap();
        drop(received.unwrap().unwrap());
    }
    drop((outgoing, incoming));
    close_native_pair(client, server, session, peer).await;
    timeout_at(native_deadline(), async {
        while memory.available_bytes() != total {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(expired, Err(Error::Deadline { .. })));
    assert!(
        usable,
        "A deadline before native enqueue poisoned the sender"
    );
    assert!(
        counted_deadline,
        "The pre-enqueue deadline must be counted once"
    );
}

#[tokio::test]
async fn native_cancel_after_enqueue_keeps_sender_poisoned() {
    use tokio::io::AsyncReadExt;
    let memory = budget(2 * 1024 * 1024);
    let total = memory.available_bytes();
    let (client, server, session, peer) = native_pair(&memory, TrafficClass::Delivery).await;
    let mut outgoing = session.take_initial_streams().await.pop().unwrap();
    let mut incoming = peer.take_initial_streams().await.pop().unwrap();
    let mut sending = Box::pin(
        outgoing
            .sender
            .send_owned(delivery_frame(&native_config()), native_deadline()),
    );
    std::future::poll_fn(|context| {
        assert!(
            sending.as_mut().poll(context).is_pending(),
            "The bounded native send did not suspend"
        );
        std::task::Poll::Ready(())
    })
    .await;
    let mut first = [0_u8; 1];
    timeout_at(
        native_deadline(),
        incoming.receiver.stream.read_exact(&mut first),
    )
    .await
    .unwrap()
    .unwrap();
    drop(sending);
    assert!(!outgoing.sender.usable);
    assert!(matches!(
        outgoing
            .sender
            .send_owned(delivery_frame(&native_config()), native_deadline())
            .await,
        Err(Error::Poisoned)
    ));
    drop((outgoing, incoming));
    close_native_pair(client, server, session, peer).await;
    timeout_at(native_deadline(), async {
        while memory.available_bytes() != total {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_core_ack_and_control_request_lanes_reject_delivery_regions() {
    let memory = budget(2 * 1024 * 1024);
    let (client, server, session, peer) = native_pair(&memory, TrafficClass::Control).await;
    assert!(session.connection().reserve_frames(128).is_err());
    let reservation = memory.reserve_frames(TrafficClass::Delivery, 128).unwrap();
    let ack = session
        .connection()
        .open(StreamPurpose::Ack, native_deadline())
        .await
        .unwrap();
    let request = session
        .connection()
        .open(StreamPurpose::Request, native_deadline())
        .await
        .unwrap();
    assert!(matches!(
        ack.with_reserved_frames(&reservation),
        Err(Error::Authorization { .. })
    ));
    assert!(matches!(
        request.with_reserved_frames(&reservation),
        Err(Error::Memory { .. })
    ));
    let initial = session.take_initial_streams().await;
    for lane in initial {
        assert!(matches!(
            lane.with_reserved_frames(&reservation),
            Err(Error::Authorization { .. })
        ));
    }
    assert_eq!(reservation.available_bytes(), 128);
    drop(peer.take_initial_streams().await);
    close_native_pair(client, server, session, peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_truncated_reserved_receive_restores_the_child_and_parent_capacity() {
    use tokio::io::AsyncWriteExt;

    let memory = budget(2 * 1024 * 1024);
    let total = memory.available_bytes();
    let (client, server, session, peer) = native_pair(&memory, TrafficClass::Delivery).await;
    let reservation = session.connection().reserve_frames(1024).unwrap();
    let mut outgoing = session
        .take_initial_streams()
        .await
        .pop()
        .unwrap()
        .with_reserved_frames(&reservation)
        .unwrap();
    let mut incoming = peer.take_initial_streams().await.pop().unwrap();
    incoming
        .sender
        .stream
        .write_all(&128_u32.to_be_bytes())
        .await
        .unwrap();
    incoming.sender.stream.write_all(&[7]).await.unwrap();
    incoming.sender.stream.finish().unwrap();
    assert!(outgoing.receiver.receive(native_deadline()).await.is_err());
    assert_eq!(reservation.available_bytes(), 1024);
    drop((outgoing, incoming, reservation));
    close_native_pair(client, server, session, peer).await;
    timeout_at(native_deadline(), async {
        while memory.available_bytes() != total {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
