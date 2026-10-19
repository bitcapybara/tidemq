use bytes::Bytes;
use prost::Message;
use snafu::ResultExt;

use crate::common::{Frame, FrameKind, TrafficClass};
use crate::error::{DecodeSnafu, EncodeSnafu, ReserveSnafu};
use crate::{Error, Id, ProtocolLimits, Result};

pub const PROTOCOL_VERSION: u32 = 1;
pub const ALPN: &[u8] = b"tidemq/1";
pub const MAX_TIMEOUT_MS: u64 = 300_000;

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
    let mut output = Vec::new();
    output.try_reserve_exact(capacity).context(ReserveSnafu)?;
    output.extend_from_slice(&length_u32.to_be_bytes());
    frame.encode(&mut output).context(EncodeSnafu)?;
    Ok(Bytes::from(output))
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
        FrameKind::Hello | FrameKind::HelloResult | FrameKind::Error | FrameKind::Cancel => true,
        FrameKind::CreateTopic
        | FrameKind::TopicResult
        | FrameKind::CreateSubscription
        | FrameKind::SubscriptionResult
        | FrameKind::OpenProducer
        | FrameKind::ProducerSessionResult
        | FrameKind::ProducerStatus
        | FrameKind::ProducerStatusResult
        | FrameKind::RetireProducer
        | FrameKind::ProducerRetired
        | FrameKind::ClaimAssignment
        | FrameKind::AssignmentResult
        | FrameKind::SeekSubscription
        | FrameKind::SeekResult
        | FrameKind::DeleteSubscription
        | FrameKind::SubscriptionDeleted
        | FrameKind::SubscriptionStatus
        | FrameKind::SubscriptionStatusResult
        | FrameKind::GetTopic
        | FrameKind::GetSubscription
        | FrameKind::AckBatch
        | FrameKind::AckResult
        | FrameKind::ReceiveBarrier
        | FrameKind::RaftVote
        | FrameKind::Diagnose
        | FrameKind::DiagnosticResult => class == TrafficClass::Control,
        FrameKind::ProduceBatch | FrameKind::ProduceResult => class == TrafficClass::Produce,
        FrameKind::Read | FrameKind::DeliveryBatch => {
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

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
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
mod tests {
    use super::*;
    use crate::public::{CreateTopic, ProduceBatch};

    #[test]
    fn bounded_frame_round_trip_and_class_binding() {
        let limits = ProtocolLimits::default();
        let id = Id::random().unwrap();
        let body = CreateTopic {
            name: "events".into(),
            partitions: 4,
        };
        let frame = new_frame(
            FrameKind::CreateTopic,
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
            decode_payload::<CreateTopic>(decoded.payload, &limits).unwrap(),
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
    fn malformed_or_amplified_messages_are_rejected_before_decode() {
        let limits = ProtocolLimits::default();
        assert!(
            decode_payload::<ProduceBatch>(Bytes::from_static(&[0x32, 0xff]), &limits).is_err()
        );
        let repeated = Bytes::from([0x32, 0].repeat(1_101));
        assert!(matches!(
            decode_payload::<ProduceBatch>(repeated, &limits),
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
        let id = Id::random().unwrap();
        let message = CreateTopic {
            name: "events".into(),
            partitions: 1,
        };
        let mut frame = new_frame(
            FrameKind::CreateTopic,
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
}
