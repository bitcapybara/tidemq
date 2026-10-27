use bytes::Bytes;
use prost::Message;
use snafu::ResultExt;

use crate::common::{Frame, FrameKind, TrafficClass};
use crate::error::{DecodeSnafu, EncodeSnafu, ReserveSnafu};
use crate::{Error, Id, ProtocolLimits, Result};

pub const PROTOCOL_VERSION: u32 = 2;
pub const ALPN: &[u8] = b"tidemq/2";
pub const MAX_TIMEOUT_MS: u64 = 300_000;

pub fn is_completion(kind: FrameKind) -> bool {
    matches!(
        kind,
        FrameKind::GroupAck
            | FrameKind::GroupAckResult
            | FrameKind::GroupRetireRead
            | FrameKind::GroupRetireAck
            | FrameKind::GroupRetired
            | FrameKind::RetirePublishes
            | FrameKind::PublishesRetired
    )
}

/// Classify Core candidates without imposing their payload bound on ordinary frames.
pub fn is_core_control(frame: &Frame, limits: &ProtocolLimits) -> Result<bool> {
    use crate::public;
    let check_payload = || {
        if frame.payload.len() > limits.max_control_frame_bytes as usize {
            return Err(Error::Limit {
                name: "core control payload bytes",
            });
        }
        Ok(())
    };
    Ok(match FrameKind::try_from(frame.kind) {
        Ok(FrameKind::RaftAppend)
            if frame.traffic_class == i32::from(TrafficClass::Replication) =>
        {
            false
        }
        Ok(FrameKind::RaftResult)
            if matches!(
                TrafficClass::try_from(frame.traffic_class),
                Ok(TrafficClass::Produce
                    | TrafficClass::Delivery
                    | TrafficClass::Bulk
                    | TrafficClass::Replication)
            ) =>
        {
            false
        }
        Ok(FrameKind::ConsumerControl) => {
            check_payload()?;
            let request: public::ConsumerControl = decode_payload(frame.payload.clone(), limits)?;
            matches!(
                public::ConsumerAction::try_from(request.action),
                Ok(public::ConsumerAction::HeartbeatConsumer
                    | public::ConsumerAction::LeaveConsumer
                    | public::ConsumerAction::RetireConsumer
                    | public::ConsumerAction::ConsumerStatus)
            )
        }
        Ok(FrameKind::PublisherControl) => {
            check_payload()?;
            let request: public::PublisherControl = decode_payload(frame.payload.clone(), limits)?;
            matches!(
                public::PublisherAction::try_from(request.action),
                Ok(public::PublisherAction::HeartbeatPublisher
                    | public::PublisherAction::ClosePublisher
                    | public::PublisherAction::PublisherStatus)
            )
        }
        Ok(
            FrameKind::ConsumerControlResult
            | FrameKind::PublisherControlResult
            | FrameKind::Cancel
            | FrameKind::GroupWatchCancel
            | FrameKind::GroupWatchCancelled
            | FrameKind::Diagnose
            | FrameKind::DiagnosticResult
            | FrameKind::GetTopic
            | FrameKind::TopicResult
            | FrameKind::GetTopicLayout
            | FrameKind::TopicLayoutResult
            | FrameKind::GetClusterRegistry
            | FrameKind::ClusterRegistryPage
            | FrameKind::RaftVote
            | FrameKind::RaftAppend
            | FrameKind::RaftResult,
        ) => {
            check_payload()?;
            true
        }
        _ => false,
    })
}

pub fn frame_limit(class: TrafficClass, limits: &ProtocolLimits) -> u32 {
    if class == TrafficClass::Control {
        limits.max_control_frame_bytes
    } else {
        limits.max_frame_bytes
    }
}

pub fn frame_length(
    prefix: [u8; 4],
    class: TrafficClass,
    limits: &ProtocolLimits,
) -> Result<usize> {
    let length = u32::from_be_bytes(prefix);
    if length == 0 || length > frame_limit(class, limits) {
        return Err(Error::Limit {
            name: "frame bytes",
        });
    }
    usize::try_from(length).map_err(|_| Error::Overflow {
        name: "frame bytes",
    })
}

pub fn new_frame<M: Message>(
    kind: FrameKind,
    class: TrafficClass,
    request_id: Id,
    operation_id: Id,
    payload: &M,
    timeout_ms: u64,
    limits: &ProtocolLimits,
) -> Result<Frame> {
    let frame = Frame {
        protocol_version: PROTOCOL_VERSION,
        request_id: Bytes::copy_from_slice(request_id.as_bytes()),
        operation_id: Bytes::copy_from_slice(operation_id.as_bytes()),
        traffic_class: class.into(),
        kind: kind.into(),
        traceparent: String::new(),
        payload: encode_payload(payload, frame_limit(class, limits))?,
        remaining_timeout_ms: timeout_ms,
    };
    validate_frame(&frame, class, limits)?;
    Ok(frame)
}

pub fn encode_frame(frame: &Frame, limits: &ProtocolLimits) -> Result<Bytes> {
    let (length_u32, capacity) = encoded_frame_size(frame, limits)?;
    let mut output = Vec::new();
    output.try_reserve_exact(capacity).context(ReserveSnafu)?;
    output.extend_from_slice(&length_u32.to_be_bytes());
    frame.encode(&mut output).context(EncodeSnafu)?;
    Ok(Bytes::from(output))
}

/// Return exact wire chunks and their metadata allocation capacity without copying the payload.
/// Callers must account for the payload's entire retained allocation, not only its visible length.
pub fn encode_frame_parts(
    mut frame: Frame,
    limits: &ProtocolLimits,
) -> Result<([Bytes; 3], usize)> {
    let (length, capacity) = encoded_frame_size(&frame, limits)?;
    let payload = std::mem::take(&mut frame.payload);
    let timeout = std::mem::take(&mut frame.remaining_timeout_ms);
    let metadata_capacity = capacity.checked_sub(payload.len()).ok_or(Error::Overflow {
        name: "frame metadata capacity",
    })?;
    let mut header = Vec::new();
    header
        .try_reserve_exact(metadata_capacity)
        .context(ReserveSnafu)?;
    header.extend_from_slice(&length.to_be_bytes());
    frame.encode(&mut header).context(EncodeSnafu)?;
    if !payload.is_empty() {
        // Frame.payload is field 7; keep the generated encoder's field order byte-for-byte.
        header.push(0x3a);
        prost::encode_length_delimiter(payload.len(), &mut header).context(EncodeSnafu)?;
    }
    let timeout = Frame {
        remaining_timeout_ms: timeout,
        ..Frame::default()
    };
    let mut trailer = Vec::new();
    trailer
        .try_reserve_exact(timeout.encoded_len())
        .context(ReserveSnafu)?;
    timeout.encode(&mut trailer).context(EncodeSnafu)?;
    let retained_metadata =
        header
            .capacity()
            .checked_add(trailer.capacity())
            .ok_or(Error::Overflow {
                name: "frame metadata allocation",
            })?;
    Ok((
        [Bytes::from(header), payload, Bytes::from(trailer)],
        retained_metadata,
    ))
}

fn encoded_frame_size(frame: &Frame, limits: &ProtocolLimits) -> Result<(u32, usize)> {
    let class = TrafficClass::try_from(frame.traffic_class).map_err(|_| Error::Invalid {
        reason: "unknown traffic class",
    })?;
    validate_frame(frame, class, limits)?;
    let length = frame.encoded_len();
    if length > frame_limit(class, limits) as usize {
        return Err(Error::Limit {
            name: "encoded frame bytes",
        });
    }
    let length_u32 = u32::try_from(length).map_err(|_| Error::Overflow {
        name: "frame bytes",
    })?;
    let capacity = length.checked_add(4).ok_or(Error::Overflow {
        name: "frame capacity",
    })?;
    Ok((length_u32, capacity))
}

pub fn decode_frame(body: Bytes, expected: TrafficClass, limits: &ProtocolLimits) -> Result<Frame> {
    if body.len() > frame_limit(expected, limits) as usize {
        return Err(Error::Limit {
            name: "received frame bytes",
        });
    }
    validate_fields(&body, 32)?;
    let mut frame = Frame::decode(body).context(DecodeSnafu)?;
    if frame.traceparent.len() > limits.max_traceparent_bytes as usize
        || (!frame.traceparent.is_empty() && !valid_traceparent(&frame.traceparent))
    {
        frame.traceparent.clear();
    }
    validate_frame(&frame, expected, limits)?;
    Ok(frame)
}

pub fn encode_payload<M: Message>(message: &M, max_bytes: u32) -> Result<Bytes> {
    let length = message.encoded_len();
    if length > max_bytes as usize {
        return Err(Error::Limit {
            name: "message payload bytes",
        });
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).context(ReserveSnafu)?;
    message.encode(&mut bytes).context(EncodeSnafu)?;
    Ok(Bytes::from(bytes))
}

pub fn decode_payload<M: Message + Default>(payload: Bytes, limits: &ProtocolLimits) -> Result<M> {
    if payload.len() > limits.max_frame_bytes as usize {
        return Err(Error::Limit {
            name: "message payload bytes",
        });
    }
    // Reject repeated empty fields before Prost can allocate an amplified vector.
    validate_fields(&payload, limits.max_payload_fields)?;
    M::decode(payload).context(DecodeSnafu)
}

fn validate_frame(frame: &Frame, expected: TrafficClass, limits: &ProtocolLimits) -> Result<()> {
    if frame.protocol_version != PROTOCOL_VERSION {
        return Err(Error::Invalid {
            reason: "unsupported protocol version",
        });
    }
    if expected == TrafficClass::Unspecified || frame.traffic_class != i32::from(expected) {
        return Err(Error::Invalid {
            reason: "connection traffic class mismatch",
        });
    }
    let kind = FrameKind::try_from(frame.kind).map_err(|_| Error::Invalid {
        reason: "unknown frame kind",
    })?;
    if !kind_allowed(kind, expected) {
        return Err(Error::Invalid {
            reason: "frame kind is not allowed on this connection",
        });
    }
    Id::from_slice(&frame.request_id)?;
    Id::from_slice(&frame.operation_id)?;
    if frame.remaining_timeout_ms == 0 || frame.remaining_timeout_ms > MAX_TIMEOUT_MS {
        return Err(Error::Limit {
            name: "remaining request timeout",
        });
    }
    if frame.traceparent.len() > limits.max_traceparent_bytes as usize {
        return Err(Error::Limit {
            name: "trace context bytes",
        });
    }
    if frame.encoded_len() > frame_limit(expected, limits) as usize {
        return Err(Error::Limit {
            name: "encoded frame bytes",
        });
    }
    Ok(())
}

pub fn kind_allowed(kind: FrameKind, class: TrafficClass) -> bool {
    match kind {
        FrameKind::ConsoleRead
        | FrameKind::ConsoleReadResult
        | FrameKind::ConsoleWrite
        | FrameKind::ConsoleOperationResult
        | FrameKind::GetConsoleOperation => class == TrafficClass::Bulk,
        FrameKind::Hello | FrameKind::HelloResult | FrameKind::Error | FrameKind::Cancel => true,
        FrameKind::TopicResult
        | FrameKind::GetTopic
        | FrameKind::GetTopicLayout
        | FrameKind::TopicLayoutResult
        | FrameKind::ChangeTopicLayout
        | FrameKind::CreateLogicalTopic
        | FrameKind::ManageIdentity
        | FrameKind::IdentityResult
        | FrameKind::ListIdentities
        | FrameKind::IdentityPage
        | FrameKind::ManageCluster
        | FrameKind::ClusterControlResult
        | FrameKind::GetClusterRegistry
        | FrameKind::ClusterRegistryPage
        | FrameKind::ConsumerControl
        | FrameKind::ConsumerControlResult
        | FrameKind::SubscriptionAdmin
        | FrameKind::SubscriptionAdminResult
        | FrameKind::RemoveTopic
        | FrameKind::TopicRemoved
        | FrameKind::PublisherControl
        | FrameKind::PublisherControlResult
        | FrameKind::RetirePublishes
        | FrameKind::PublishesRetired
        | FrameKind::GroupAck
        | FrameKind::GroupAckResult
        | FrameKind::GroupRetireRead
        | FrameKind::GroupRetireAck
        | FrameKind::GroupRetired
        | FrameKind::GroupWatch
        | FrameKind::GroupWatchResult
        | FrameKind::GroupWatchCancel
        | FrameKind::GroupWatchCancelled
        | FrameKind::RaftVote
        | FrameKind::Diagnose
        | FrameKind::DiagnosticResult => class == TrafficClass::Control,
        FrameKind::Publish | FrameKind::PublishResult => class == TrafficClass::Produce,
        FrameKind::GroupRead | FrameKind::GroupDeliveries => {
            matches!(class, TrafficClass::Delivery | TrafficClass::Bulk)
        }
        FrameKind::RaftAppend => matches!(class, TrafficClass::Control | TrafficClass::Replication),
        FrameKind::RaftSnapshot => class == TrafficClass::Bulk,
        FrameKind::RaftResult | FrameKind::InternalCommand | FrameKind::InternalResult => {
            class != TrafficClass::Unspecified
        }
        FrameKind::Unspecified => false,
    }
}

pub fn valid_traceparent(value: &str) -> bool {
    let mut parts = value.split('-');
    let version = parts.next();
    let trace = parts.next();
    let span = parts.next();
    let flags = parts.next();
    version == Some("00")
        && trace.is_some_and(|v| valid_hex(v, 32, true))
        && span.is_some_and(|v| valid_hex(v, 16, true))
        && flags.is_some_and(|v| valid_hex(v, 2, false))
        && parts.next().is_none()
}

fn valid_hex(value: &str, length: usize, nonzero: bool) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        && (!nonzero || value.bytes().any(|byte| byte != b'0'))
}

fn validate_fields(bytes: &[u8], max_fields: u32) -> Result<()> {
    let mut cursor = 0_usize;
    let mut fields = 0_u32;
    while cursor < bytes.len() {
        fields = fields.checked_add(1).ok_or(Error::Overflow {
            name: "field count",
        })?;
        if fields > max_fields {
            return Err(Error::Limit {
                name: "protobuf field count",
            });
        }
        let tag = read_varint(bytes, &mut cursor)?;
        if tag >> 3 == 0 || tag >> 3 > 536_870_911 {
            return Err(Error::Invalid {
                reason: "invalid protobuf field number",
            });
        }
        let skip = match tag & 7 {
            0 => {
                read_varint(bytes, &mut cursor)?;
                0
            }
            1 => 8,
            2 => {
                usize::try_from(read_varint(bytes, &mut cursor)?).map_err(|_| Error::Overflow {
                    name: "protobuf field bytes",
                })?
            }
            5 => 4,
            _ => {
                return Err(Error::Invalid {
                    reason: "unsupported protobuf wire type",
                });
            }
        };
        let end = cursor.checked_add(skip).ok_or(Error::Overflow {
            name: "protobuf cursor",
        })?;
        bytes.get(cursor..end).ok_or(Error::Invalid {
            reason: "truncated protobuf field",
        })?;
        cursor = end;
    }
    Ok(())
}

pub(crate) fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(*cursor).ok_or(Error::Invalid {
            reason: "truncated protobuf varint",
        })?;
        *cursor = cursor.checked_add(1).ok_or(Error::Overflow {
            name: "protobuf cursor",
        })?;
        let bits = u64::from(byte & 0x7f);
        if shift == 63 && bits > 1 {
            return Err(Error::Invalid {
                reason: "protobuf varint overflow",
            });
        }
        value |= bits << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Error::Invalid {
        reason: "protobuf varint overflow",
    })
}

#[cfg(test)]
mod core_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::public::{LogicalTopic, Publish};

    #[test]
    fn watch_waits_use_normal_capacity_but_cancellation_keeps_core_reservation() {
        let limits = ProtocolLimits::default();
        for (kind, core) in [
            (FrameKind::GroupWatch, false),
            (FrameKind::GroupWatchResult, false),
            (FrameKind::GroupWatchCancel, true),
            (FrameKind::GroupWatchCancelled, true),
        ] {
            let frame = Frame {
                kind: kind.into(),
                ..Frame::default()
            };
            assert_eq!(is_core_control(&frame, &limits).unwrap(), core);
            assert!(!is_completion(kind));
        }
    }

    #[test]
    fn bounded_frame_round_trip_and_class_binding() {
        let limits = ProtocolLimits::default();
        let id = Id::new_v7().unwrap();
        let body = LogicalTopic {
            name: "events".into(),
        };
        let frame = new_frame(
            FrameKind::CreateLogicalTopic,
            TrafficClass::Control,
            id,
            id,
            &body,
            1_000,
            &limits,
        )
        .unwrap();
        let encoded = encode_frame(&frame, &limits).unwrap();
        let prefix: [u8; 4] = encoded.get(..4).unwrap().try_into().unwrap();
        assert_eq!(
            frame_length(prefix, TrafficClass::Control, &limits).unwrap(),
            encoded.len() - 4
        );
        let decoded = decode_frame(encoded.slice(4..), TrafficClass::Control, &limits).unwrap();
        assert_eq!(
            decode_payload::<LogicalTopic>(decoded.payload, &limits).unwrap(),
            body
        );
        assert!(
            decode_frame(
                encode_payload(&frame, limits.max_frame_bytes).unwrap(),
                TrafficClass::Produce,
                &limits
            )
            .is_err()
        );
        assert!(frame_length(u32::MAX.to_be_bytes(), TrafficClass::Bulk, &limits).is_err());
    }

    #[test]
    fn removed_protocol_versions_and_operations_are_rejected() {
        let limits = ProtocolLimits::default();
        let mut frame = test_frame(0, 1_000);
        frame.protocol_version = 1;
        assert!(validate_frame(&frame, TrafficClass::Produce, &limits).is_err());
        frame.protocol_version = PROTOCOL_VERSION;
        for kind in [10, 12, 20, 22, 24, 28, 37] {
            frame.kind = kind;
            assert!(validate_frame(&frame, TrafficClass::Produce, &limits).is_err());
        }
    }

    #[test]
    fn malformed_or_amplified_messages_are_rejected_before_decode() {
        let limits = ProtocolLimits::default();
        assert!(decode_payload::<Publish>(Bytes::from_static(&[0x32, 0xff]), &limits).is_err());
        let repeated = Bytes::from([0x32, 0].repeat(1_101));
        assert!(matches!(
            decode_payload::<Publish>(repeated, &limits),
            Err(Error::Limit { .. })
        ));
        assert!(validate_fields(&[0x80; 10], 10).is_err());
        assert!(validate_fields(&[0x0b], 10).is_err());
    }

    #[test]
    fn trace_context_is_validated_without_changing_operation_identity() {
        assert!(valid_traceparent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        ));
        assert!(!valid_traceparent(
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01"
        ));
        assert!(!valid_traceparent("invalid"));
    }

    #[test]
    fn invalid_trace_metadata_is_ignored_inside_a_valid_frame() {
        let limits = ProtocolLimits::default();
        let id = Id::new_v7().unwrap();
        let message = LogicalTopic {
            name: "events".into(),
        };
        let mut frame = new_frame(
            FrameKind::CreateLogicalTopic,
            TrafficClass::Control,
            id,
            id,
            &message,
            1_000,
            &limits,
        )
        .unwrap();
        frame.traceparent = "invalid".repeat(20);
        let raw = encode_payload(&frame, limits.max_control_frame_bytes).unwrap();
        let decoded = decode_frame(raw, TrafficClass::Control, &limits).unwrap();
        assert!(decoded.traceparent.is_empty());
        assert_eq!(decoded.operation_id, frame.operation_id);
    }

    #[test]
    fn segmented_frames_match_generated_wire_encoding_and_share_payload() {
        let limits = ProtocolLimits::default();
        for length in [0, 1, 127, 128, 16_384, 262_144] {
            for timeout in [1, 127, 128, MAX_TIMEOUT_MS] {
                let mut frame = test_frame(length, timeout);
                for traceparent in [
                    "",
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                ] {
                    frame.traceparent = traceparent.into();
                    let expected = encode_frame(&frame, &limits).unwrap();
                    let payload = frame.payload.clone();
                    let ([header, shared, trailer], capacity) =
                        encode_frame_parts(frame.clone(), &limits).unwrap();
                    assert_eq!(shared.as_ptr(), payload.as_ptr());
                    let mut wire = header.to_vec();
                    wire.extend_from_slice(&shared);
                    wire.extend_from_slice(&trailer);
                    assert_eq!(wire.as_slice(), expected.as_ref());
                    assert!(capacity >= header.len() + trailer.len());
                }
            }
        }
    }

    #[test]
    fn nested_protobuf_bytes_share_the_frame_backing() {
        let limits = ProtocolLimits::default();
        let message = crate::public::PublishMessage {
            payload: Bytes::from(vec![7; 64 * 1024]),
            ..Default::default()
        };
        let id = Id::new_v7().unwrap();
        let frame = new_frame(
            FrameKind::Publish,
            TrafficClass::Produce,
            id,
            id,
            &message,
            1_000,
            &limits,
        )
        .unwrap();
        let wire = encode_frame(&frame, &limits).unwrap();
        let decoded = decode_frame(wire.slice(4..), TrafficClass::Produce, &limits).unwrap();
        let inner =
            decode_payload::<crate::public::PublishMessage>(decoded.payload, &limits).unwrap();
        let start = wire.as_ptr() as usize;
        assert!((start..start + wire.len()).contains(&(inner.payload.as_ptr() as usize)));
        assert_eq!(inner.payload, message.payload);
    }

    fn test_frame(length: usize, timeout: u64) -> Frame {
        Frame {
            protocol_version: PROTOCOL_VERSION,
            request_id: Bytes::from_static(&[1; 16]),
            operation_id: Bytes::from_static(&[2; 16]),
            traffic_class: TrafficClass::Produce.into(),
            kind: FrameKind::Publish.into(),
            payload: Bytes::from(vec![7; length]),
            remaining_timeout_ms: timeout,
            ..Default::default()
        }
    }
}
