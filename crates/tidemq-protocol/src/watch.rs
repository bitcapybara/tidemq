//! Bounded read-only observations. Tokens and branch identities never authorize a delivery.

use crate::{Error, Id, ProtocolLimits, Result, public::GroupWatch};
use prost::Message;

pub const MAX_TARGETS: usize = 32;
pub const MAX_TOKEN_BYTES: usize = 64;
pub const MAX_WAIT_MS: u32 = 1_000;

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
