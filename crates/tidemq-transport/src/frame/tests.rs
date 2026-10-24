use super::*;
use crate::budget::MemoryLimits;
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
