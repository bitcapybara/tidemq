use super::*;
use crate::{internal, public};

fn envelope<M: Message>(kind: FrameKind, class: TrafficClass, payload: &M) -> Frame {
    Frame {
        protocol_version: PROTOCOL_VERSION,
        request_id: Bytes::from_static(&[1; 16]),
        operation_id: Bytes::from_static(&[2; 16]),
        traffic_class: class.into(),
        kind: kind.into(),
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
        payload: Bytes::from(payload.encode_to_vec()),
        remaining_timeout_ms: MAX_TIMEOUT_MS,
    }
}

fn round_trip(frame: &Frame, core: bool, limits: &ProtocolLimits) {
    let class = TrafficClass::try_from(frame.traffic_class).unwrap();
    assert_eq!(is_core_control(frame, limits).unwrap(), core);
    let wire = encode_frame(frame, limits).unwrap();
    let prefix = wire.get(..4).unwrap().try_into().unwrap();
    assert_eq!(
        frame_length(prefix, class, limits).unwrap(),
        frame.encoded_len()
    );
    let decoded = decode_frame(wire.slice(4..), class, limits).unwrap();
    assert_eq!(&decoded, frame);
    assert_eq!(is_core_control(&decoded, limits).unwrap(), core);
    let (parts, _) = encode_frame_parts(frame.clone(), limits).unwrap();
    assert_eq!(parts.concat(), wire.as_ref());
}

fn assert_wire_bound(frame: &Frame, core: bool, limits: &ProtocolLimits) {
    let class = TrafficClass::try_from(frame.traffic_class).unwrap();
    let length = u32::try_from(frame.encoded_len()).unwrap();
    let mut exact = limits.clone();
    if class == TrafficClass::Control {
        exact.max_control_frame_bytes = length;
    } else {
        exact.max_frame_bytes = length;
        exact.max_message_bytes = exact.max_message_bytes.min(length - 1);
    }
    exact.validate().unwrap();
    round_trip(frame, core, &exact);
    let mut smaller = exact;
    if class == TrafficClass::Control {
        smaller.max_control_frame_bytes -= 1;
    } else {
        smaller.max_frame_bytes -= 1;
        smaller.max_message_bytes = smaller.max_message_bytes.min(length - 2);
    }
    smaller.validate().unwrap();
    assert!(matches!(
        encode_frame(frame, &smaller),
        Err(Error::Limit { .. })
    ));
    assert!(matches!(
        encode_frame_parts(frame.clone(), &smaller),
        Err(Error::Limit { .. })
    ));
    assert!(matches!(
        frame_length(length.to_be_bytes(), class, &smaller),
        Err(Error::Limit { .. })
    ));
    assert!(matches!(
        decode_frame(Bytes::from(frame.encode_to_vec()), class, &smaller),
        Err(Error::Limit { .. })
    ));
}

#[test]
fn large_publish_and_delivery_frames_keep_their_data_bounds() {
    let limits = ProtocolLimits {
        max_frame_bytes: 4 * 1024 * 1024,
        max_message_bytes: 2 * 1024 * 1024,
        ..Default::default()
    };
    limits.validate().unwrap();
    let publish = public::Publish {
        topic_id: Bytes::from_static(&[3; 16]),
        shard_id: Bytes::from_static(&[4; 16]),
        producer_id: Bytes::from_static(&[5; 16]),
        registration: 1,
        sequence: 1,
        digest: Bytes::from_static(&[6; 32]),
        message: Some(public::PublishMessage {
            payload: Bytes::from(vec![7; 2 * 1024 * 1024]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let deliveries = public::GroupDeliveries {
        sequence: 1,
        deliveries: (0..128)
            .map(|offset| public::GroupDelivery {
                proof: Some(public::GroupProof {
                    offset,
                    attempt: 1,
                    mac: Bytes::from_static(&[8; 32]),
                }),
                body: Bytes::from(vec![9; 16 * 1024]),
                current_attempt: true,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let frames = [
        envelope(FrameKind::Publish, TrafficClass::Produce, &publish),
        envelope(
            FrameKind::GroupDeliveries,
            TrafficClass::Delivery,
            &deliveries,
        ),
        envelope(FrameKind::GroupDeliveries, TrafficClass::Bulk, &deliveries),
    ];
    for frame in frames {
        assert!(frame.payload.len() > limits.max_control_frame_bytes as usize);
        assert!(frame.encoded_len() <= limits.max_frame_bytes as usize);
        round_trip(&frame, false, &limits);
        assert_wire_bound(&frame, false, &limits);
    }
}

#[test]
fn large_bulk_results_and_internal_envelopes_are_not_core() {
    let limits = ProtocolLimits::default();
    let body = Bytes::from(vec![b'x'; 64 * 1024]);
    let frames = [
        envelope(
            FrameKind::ConsoleReadResult,
            TrafficClass::Bulk,
            &public::ConsoleReadResult { json: body.clone() },
        ),
        envelope(
            FrameKind::InternalCommand,
            TrafficClass::Produce,
            &internal::InternalCommand {
                group_id: 1,
                operation_id: Bytes::from_static(&[2; 16]),
                encoded_command: body.clone(),
            },
        ),
        envelope(
            FrameKind::RaftSnapshot,
            TrafficClass::Bulk,
            &internal::SnapshotChunk {
                group_id: 1,
                snapshot_id: Bytes::from_static(&[1; 16]),
                data: body,
                ..Default::default()
            },
        ),
    ];
    for frame in frames {
        assert!(frame.payload.len() > limits.max_control_frame_bytes as usize);
        round_trip(&frame, false, &limits);
        assert_wire_bound(&frame, false, &limits);
    }
}

#[test]
fn raft_data_classes_keep_normal_bounds_while_control_stays_core() {
    let limits = ProtocolLimits::default();
    for length in [128, limits.max_control_frame_bytes as usize + 1] {
        let large = length > limits.max_control_frame_bytes as usize;
        let request = internal::RaftRequest {
            group_id: 1,
            rpc_id: Bytes::from_static(&[1; 16]),
            encoded_rpc: Bytes::from(vec![7; length]),
        };
        let append = envelope(FrameKind::RaftAppend, TrafficClass::Replication, &request);
        round_trip(&append, false, &limits);
        if large {
            assert_wire_bound(&append, false, &limits);
        }
        let result = internal::RaftResult {
            rpc_id: request.rpc_id.clone(),
            encoded_result: request.encoded_rpc.clone(),
        };
        for class in [
            TrafficClass::Replication,
            TrafficClass::Bulk,
            TrafficClass::Produce,
            TrafficClass::Delivery,
        ] {
            let result = envelope(FrameKind::RaftResult, class, &result);
            round_trip(&result, false, &limits);
            if large {
                assert_wire_bound(&result, false, &limits);
            }
        }
        for kind in [
            FrameKind::RaftVote,
            FrameKind::RaftAppend,
            FrameKind::RaftResult,
        ] {
            let control = if kind == FrameKind::RaftResult {
                envelope(kind, TrafficClass::Control, &result)
            } else {
                envelope(kind, TrafficClass::Control, &request)
            };
            if !large {
                round_trip(&control, true, &limits);
                assert_wire_bound(&control, true, &limits);
            } else {
                assert!(matches!(
                    is_core_control(&control, &limits),
                    Err(Error::Limit { .. })
                ));
                assert!(encode_frame(&control, &limits).is_err());
                assert!(
                    decode_frame(
                        Bytes::from(control.encode_to_vec()),
                        TrafficClass::Control,
                        &limits
                    )
                    .is_err()
                );
            }
        }
    }
}

#[test]
fn retirements_keep_completion_priority_and_control_wire_bounds() {
    let limits = ProtocolLimits::default();
    let retire = public::GroupRetire {
        topic_id: Bytes::from_static(&[3; 16]),
        subscription_id: Bytes::from_static(&[4; 16]),
        member_id: Bytes::from_static(&[5; 16]),
        member_epoch: u64::MAX,
        sequence: u64::MAX,
        ..Default::default()
    };
    let publishes = public::RetirePublishes {
        topic_id: retire.topic_id.clone(),
        shard_id: retire.subscription_id.clone(),
        producer_id: retire.member_id.clone(),
        registration: u64::MAX,
        before: u64::MAX,
        close: true,
        ..Default::default()
    };
    for frame in [
        envelope(FrameKind::GroupRetireRead, TrafficClass::Control, &retire),
        envelope(FrameKind::GroupRetireAck, TrafficClass::Control, &retire),
        envelope(
            FrameKind::RetirePublishes,
            TrafficClass::Control,
            &publishes,
        ),
    ] {
        assert!(is_completion(FrameKind::try_from(frame.kind).unwrap()));
        round_trip(&frame, false, &limits);
        assert_wire_bound(&frame, false, &limits);
    }
    for frame in [
        envelope(
            FrameKind::GroupRetired,
            TrafficClass::Control,
            &public::GroupRetired {},
        ),
        envelope(
            FrameKind::PublishesRetired,
            TrafficClass::Control,
            &public::PublishesRetired { before: u64::MAX },
        ),
    ] {
        assert!(is_completion(FrameKind::try_from(frame.kind).unwrap()));
        round_trip(&frame, false, &limits);
    }
    let ack = public::GroupAck {
        topic_id: retire.topic_id,
        subscription_id: retire.subscription_id,
        member_id: retire.member_id,
        proofs: (0..1024)
            .map(|offset| public::GroupProof {
                offset,
                attempt: 1,
                mac: Bytes::from_static(&[6; 32]),
            })
            .collect(),
        ..Default::default()
    };
    let oversized = envelope(FrameKind::GroupAck, TrafficClass::Control, &ack);
    assert!(oversized.payload.len() > limits.max_control_frame_bytes as usize);
    assert!(is_completion(FrameKind::GroupAck));
    assert!(!is_core_control(&oversized, &limits).unwrap());
    assert!(encode_frame(&oversized, &limits).is_err());
    assert!(
        decode_frame(
            Bytes::from(oversized.encode_to_vec()),
            TrafficClass::Control,
            &limits
        )
        .is_err()
    );
}

#[test]
fn membership_actions_are_decoded_only_inside_the_control_payload_bound() {
    let limits = ProtocolLimits::default();
    for action in -1..=7 {
        let frame = envelope(
            FrameKind::ConsumerControl,
            TrafficClass::Control,
            &public::ConsumerControl {
                action,
                ..Default::default()
            },
        );
        assert_eq!(
            is_core_control(&frame, &limits).unwrap(),
            (3..=6).contains(&action)
        );
    }
    for action in -1..=5 {
        let frame = envelope(
            FrameKind::PublisherControl,
            TrafficClass::Control,
            &public::PublisherControl {
                action,
                ..Default::default()
            },
        );
        assert_eq!(
            is_core_control(&frame, &limits).unwrap(),
            (2..=4).contains(&action)
        );
    }
    for kind in [FrameKind::ConsumerControl, FrameKind::PublisherControl] {
        let mut frame = envelope(kind, TrafficClass::Control, &public::GroupRetired {});
        frame.payload = Bytes::from(vec![0xff; limits.max_control_frame_bytes as usize + 1]);
        assert!(matches!(
            is_core_control(&frame, &limits),
            Err(Error::Limit {
                name: "core control payload bytes"
            })
        ));
        frame.payload = Bytes::from_static(&[0xff]);
        assert!(is_core_control(&frame, &limits).is_err());
    }
}

#[test]
fn static_core_bounds_and_cross_class_cancellation_are_preserved() {
    let limits = ProtocolLimits::default();
    for kind in [
        FrameKind::ConsumerControlResult,
        FrameKind::PublisherControlResult,
        FrameKind::Cancel,
        FrameKind::GroupWatchCancel,
        FrameKind::GroupWatchCancelled,
        FrameKind::Diagnose,
        FrameKind::DiagnosticResult,
        FrameKind::GetTopic,
        FrameKind::TopicResult,
        FrameKind::GetTopicLayout,
        FrameKind::TopicLayoutResult,
        FrameKind::GetClusterRegistry,
        FrameKind::ClusterRegistryPage,
        FrameKind::RaftVote,
        FrameKind::RaftAppend,
        FrameKind::RaftResult,
    ] {
        let mut frame = envelope(kind, TrafficClass::Control, &public::GroupRetired {});
        frame.payload = Bytes::from(vec![7; limits.max_control_frame_bytes as usize]);
        assert!(is_core_control(&frame, &limits).unwrap());
        assert!(encode_frame(&frame, &limits).is_err());
        frame.payload = Bytes::from(vec![7; limits.max_control_frame_bytes as usize + 1]);
        assert!(matches!(
            is_core_control(&frame, &limits),
            Err(Error::Limit { .. })
        ));
    }
    for class in [
        TrafficClass::Control,
        TrafficClass::Produce,
        TrafficClass::Delivery,
        TrafficClass::Replication,
        TrafficClass::Bulk,
    ] {
        let cancel = envelope(
            FrameKind::Cancel,
            class,
            &crate::common::CancelRequest {
                request_id: Bytes::from_static(&[3; 16]),
            },
        );
        round_trip(&cancel, true, &limits);
    }
}

#[test]
fn classification_does_not_replace_unknown_kind_or_class_validation() {
    let limits = ProtocolLimits::default();
    let mut frame = envelope(
        FrameKind::Publish,
        TrafficClass::Produce,
        &public::PublishMessage {
            payload: Bytes::from(vec![7; 64 * 1024]),
            ..Default::default()
        },
    );
    for kind in [i32::from(FrameKind::Unspecified), i32::MAX] {
        frame.kind = kind;
        assert!(!is_core_control(&frame, &limits).unwrap());
        assert!(encode_frame(&frame, &limits).is_err());
        assert!(
            decode_frame(
                Bytes::from(frame.encode_to_vec()),
                TrafficClass::Produce,
                &limits
            )
            .is_err()
        );
    }
    frame.kind = FrameKind::Publish.into();
    frame.traffic_class = TrafficClass::Delivery.into();
    assert!(!is_core_control(&frame, &limits).unwrap());
    assert!(encode_frame(&frame, &limits).is_err());
    assert!(
        decode_frame(
            Bytes::from(frame.encode_to_vec()),
            TrafficClass::Delivery,
            &limits
        )
        .is_err()
    );
    frame.traffic_class = i32::MAX;
    assert!(encode_frame(&frame, &limits).is_err());
    assert!(
        decode_frame(
            Bytes::from(frame.encode_to_vec()),
            TrafficClass::Produce,
            &limits
        )
        .is_err()
    );
    frame.kind = FrameKind::RaftVote.into();
    frame.traffic_class = TrafficClass::Replication.into();
    assert!(encode_frame(&frame, &limits).is_err());
    assert!(
        decode_frame(
            Bytes::from(frame.encode_to_vec()),
            TrafficClass::Replication,
            &limits
        )
        .is_err()
    );
}
