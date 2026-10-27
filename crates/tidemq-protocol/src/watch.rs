//! Bounded read-only observations. Tokens and branch identities never authorize a delivery.

use crate::{Error, Id, ProtocolLimits, Result, public::GroupWatch};
use prost::Message;

pub const MAX_TARGETS: usize = 32;
pub const MAX_TOKEN_BYTES: usize = 64;
pub const MAX_WAIT_MS: u32 = 1_000;

pub fn decode(payload: bytes::Bytes, limits: &ProtocolLimits) -> Result<GroupWatch> {
    validate_request_payload(&payload, limits)?;
    crate::framing::decode_payload(payload, limits)
}

pub fn decode_result(
    payload: bytes::Bytes,
    limits: &ProtocolLimits,
) -> Result<crate::public::GroupWatchResult> {
    validate_result_payload(&payload, limits)?;
    crate::framing::decode_payload(payload, limits)
}

pub fn validate_request_payload(payload: &[u8], limits: &ProtocolLimits) -> Result<()> {
    preflight(payload, limits, 8)
}
pub fn validate_result_payload(payload: &[u8], limits: &ProtocolLimits) -> Result<()> {
    preflight(payload, limits, 1)
}

fn preflight(payload: &[u8], limits: &ProtocolLimits, target_field: u64) -> Result<()> {
    if payload.len() > limits.max_control_frame_bytes as usize {
        return Err(Error::Limit {
            name: "watch control payload bytes",
        });
    }
    let mut targets = 0_usize;
    visit_bytes(
        payload,
        (MAX_TARGETS as u32 + 16).min(limits.max_payload_fields),
        |field, value| {
            if field == target_field {
                targets = targets.checked_add(1).ok_or(Error::Overflow {
                    name: "watch targets",
                })?;
                if targets > MAX_TARGETS {
                    return Err(Error::Limit {
                        name: "watch targets",
                    });
                }
                visit_bytes(value, 8, |field, value| {
                    if field == 1 && value.len() > 16 || field == 3 && value.len() > MAX_TOKEN_BYTES
                    {
                        return Err(Error::Limit {
                            name: "watch target identity or token bytes",
                        });
                    }
                    Ok(())
                })?;
            }
            Ok(())
        },
    )
}

/// Bound repeated messages before Prost can allocate an amplified target vector.
fn visit_bytes(
    bytes: &[u8],
    maximum: u32,
    mut visitor: impl FnMut(u64, &[u8]) -> Result<()>,
) -> Result<()> {
    let mut cursor = 0_usize;
    let mut fields = 0_u32;
    while cursor < bytes.len() {
        fields = fields.checked_add(1).ok_or(Error::Overflow {
            name: "watch fields",
        })?;
        if fields > maximum {
            return Err(Error::Limit {
                name: "watch fields",
            });
        }
        let tag = crate::framing::read_varint(bytes, &mut cursor)?;
        if tag >> 3 == 0 || tag >> 3 > 536_870_911 {
            return invalid("invalid watch protobuf field");
        }
        let count =
            match tag & 7 {
                0 => {
                    crate::framing::read_varint(bytes, &mut cursor)?;
                    0
                }
                1 => 8,
                5 => 4,
                2 => usize::try_from(crate::framing::read_varint(bytes, &mut cursor)?).map_err(
                    |_| Error::Overflow {
                        name: "watch field bytes",
                    },
                )?,
                _ => return invalid("unsupported watch protobuf wire type"),
            };
        let end = cursor.checked_add(count).ok_or(Error::Overflow {
            name: "watch field bytes",
        })?;
        let value = bytes.get(cursor..end).ok_or(Error::Invalid {
            reason: "truncated watch protobuf field",
        })?;
        if tag & 7 == 2 {
            visitor(tag >> 3, value)?;
        }
        cursor = end;
    }
    Ok(())
}

pub fn validate(request: &GroupWatch, operation: Id, limits: &ProtocolLimits) -> Result<()> {
    for id in [
        &request.topic_id,
        &request.subscription_id,
        &request.member_id,
    ] {
        Id::from_slice(id)?;
    }
    let root = Id::from_slice(&request.root_operation_id)?;
    if request.parent_operation_id.is_empty() {
        if root != operation {
            return invalid("watch root identity differs from its frame");
        }
    } else {
        Id::from_slice(&request.parent_operation_id)?;
        if root == operation {
            return invalid("watch child identity must differ from its root");
        }
    }
    if request.member_epoch == 0 || request.assignment_version == 0 || request.attempt == 0 {
        return invalid("watch epoch, assignment version and attempt must be nonzero");
    }
    if request.wait_ms > MAX_WAIT_MS || !(1..=MAX_TARGETS).contains(&request.targets.len()) {
        return invalid("watch wait or target count exceeds its bound");
    }
    // Include both semantic envelopes and maximum-width frame metadata at every hop.
    if request.encoded_len().saturating_add(1_024) > limits.max_control_frame_bytes as usize
        || request.targets.len().saturating_add(9) > limits.max_payload_fields as usize
    {
        return Err(Error::Limit {
            name: "watch control frame",
        });
    }
    let mut shards = std::collections::BTreeSet::new();
    for target in &request.targets {
        let shard = Id::from_slice(&target.shard_id)?;
        if !shards.insert(shard) || target.observed_token.len() > MAX_TOKEN_BYTES {
            return invalid("watch targets must be distinct and tokens bounded");
        }
    }
    Ok(())
}

fn invalid<T>(reason: &'static str) -> Result<T> {
    Err(Error::Invalid { reason })
}

/// Fixed-width identities and length-prefixed tokens make branch scope unambiguous.
pub fn digest(request: &GroupWatch) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq/group-watch/1\0");
    for bytes in [
        &request.root_operation_id,
        &request.parent_operation_id,
        &request.topic_id,
        &request.subscription_id,
        &request.member_id,
    ] {
        hash.update(&(bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    hash.update(&request.member_epoch.to_be_bytes());
    hash.update(&request.assignment_version.to_be_bytes());
    hash.update(&request.wait_ms.to_be_bytes());
    let mut targets: Vec<_> = request.targets.iter().collect();
    targets.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    hash.update(&(targets.len() as u64).to_be_bytes());
    for target in targets {
        hash.update(&(target.shard_id.len() as u64).to_be_bytes());
        hash.update(&target.shard_id);
        hash.update(&target.birth_partition.to_be_bytes());
        hash.update(&(target.observed_token.len() as u64).to_be_bytes());
        hash.update(&target.observed_token);
    }
    *hash.finalize().as_bytes()
}

pub fn child_operation(
    parent: Id,
    stage: u8,
    group: u64,
    route_epoch: u64,
    route_generation: u64,
    request: &GroupWatch,
) -> Result<Id> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq/group-watch-child/1\0");
    hash.update(parent.as_bytes());
    hash.update(&[stage]);
    hash.update(&group.to_be_bytes());
    hash.update(&route_epoch.to_be_bytes());
    hash.update(&route_generation.to_be_bytes());
    hash.update(&digest(request));
    let bytes = hash.finalize();
    Id::from_slice(bytes.as_bytes().get(..16).ok_or(Error::Invalid {
        reason: "watch branch digest has invalid width",
    })?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::public::WatchTarget;

    #[test]
    fn repeated_targets_and_nested_tokens_are_bounded_before_prost_allocation() {
        let limits = ProtocolLimits::default();
        assert!(decode(Bytes::from([0x42, 0].repeat(MAX_TARGETS + 1)), &limits).is_err());
        assert!(decode_result(Bytes::from([0x0a, 0].repeat(MAX_TARGETS + 1)), &limits).is_err());
        for malformed in [
            vec![0x42, 0xff],
            vec![0x00],
            vec![0x0b],
            vec![0x42, 2, 0x1a, 2],
        ] {
            assert!(validate_request_payload(&malformed, &limits).is_err());
        }
        let (_, mut watch) = request();
        watch.targets.first_mut().unwrap().observed_token =
            Bytes::from(vec![1; MAX_TOKEN_BYTES + 1]);
        let payload =
            crate::framing::encode_payload(&watch, limits.max_control_frame_bytes).unwrap();
        assert!(decode(payload, &limits).is_err());
        watch.targets.first_mut().unwrap().observed_token = Bytes::from(vec![1; MAX_TOKEN_BYTES]);
        let payload =
            crate::framing::encode_payload(&watch, limits.max_control_frame_bytes).unwrap();
        assert_eq!(decode(payload, &limits).unwrap(), watch);
    }
    use bytes::Bytes;

    fn request() -> (Id, GroupWatch) {
        let id = Id::new_v7().unwrap();
        let bytes = Bytes::copy_from_slice(id.as_bytes());
        (
            id,
            GroupWatch {
                topic_id: bytes.clone(),
                subscription_id: bytes.clone(),
                member_id: bytes.clone(),
                member_epoch: 1,
                assignment_version: 1,
                attempt: 1,
                wait_ms: MAX_WAIT_MS,
                targets: vec![WatchTarget {
                    shard_id: bytes.clone(),
                    birth_partition: 0,
                    observed_token: Bytes::new(),
                }],
                root_operation_id: bytes,
                parent_operation_id: Bytes::new(),
            },
        )
    }

    #[test]
    fn validates_real_control_budget_and_target_limits() {
        let (operation, mut watch) = request();
        let limits = ProtocolLimits::default();
        validate(&watch, operation, &limits).unwrap();
        watch.targets.clear();
        assert!(validate(&watch, operation, &limits).is_err());
        for _ in 0..MAX_TARGETS {
            watch.targets.push(WatchTarget {
                shard_id: Bytes::copy_from_slice(Id::new_v7().unwrap().as_bytes()),
                birth_partition: 0,
                observed_token: Bytes::from(vec![1; MAX_TOKEN_BYTES]),
            });
        }
        validate(&watch, operation, &limits).unwrap();
        let small = ProtocolLimits {
            max_control_frame_bytes: 1024,
            ..limits.clone()
        };
        assert!(validate(&watch, operation, &small).is_err());
        watch.targets.push(watch.targets.first().unwrap().clone());
        assert!(validate(&watch, operation, &limits).is_err());
        watch.targets.pop();
        watch.targets.get_mut(0).unwrap().observed_token =
            Bytes::from(vec![1; MAX_TOKEN_BYTES + 1]);
        assert!(validate(&watch, operation, &limits).is_err());
    }

    #[test]
    fn converging_owners_and_groups_have_independent_child_identities() {
        let (parent, mut watch) = request();
        let child = child_operation(parent, 1, 10, 2, 3, &watch).unwrap();
        watch.attempt = 2;
        assert_eq!(child, child_operation(parent, 1, 10, 2, 3, &watch).unwrap());
        for other in [
            child_operation(parent, 1, 11, 2, 3, &watch).unwrap(),
            child_operation(parent, 1, 10, 3, 3, &watch).unwrap(),
            child_operation(Id::new_v7().unwrap(), 1, 10, 2, 3, &watch).unwrap(),
            child_operation(parent, 2, 10, 2, 3, &watch).unwrap(),
        ] {
            assert_ne!(child, other);
        }
        watch.targets.first_mut().unwrap().observed_token = Bytes::from_static(b"changed");
        assert_ne!(child, child_operation(parent, 1, 10, 2, 3, &watch).unwrap());
    }
}
