use std::collections::BTreeSet;

use tidemq::metadata::{
    Command, DeploymentMode, LayoutChange, LayoutStage, MetadataLimits, MetadataState, NodeRole,
    NodeSpec, Rejection, Response,
};
use tidemq_protocol::{
    Id,
    topic::{HashRange, ShardRoute, TopicLayout, routing_hash},
};

fn id() -> Id {
    Id::new_v7().unwrap()
}

fn metadata() -> MetadataState {
    let mut state = MetadataState::default();
    assert!(
        state
            .apply(Command::bootstrap(
                id(),
                id(),
                1,
                DeploymentMode::LocalRf1,
                MetadataLimits::default()
            ))
            .is_applied()
    );
    let spec = NodeSpec {
        node_id: 1,
        zone: "local".into(),
        endpoint: "127.0.0.1:7444".parse().unwrap(),
        roles: BTreeSet::from([NodeRole::Broker, NodeRole::Storage]),
        schedulable: true,
    };
    assert!(
        state
            .apply(Command::put_node(id(), state.revision(), spec, 0))
            .is_applied()
    );
    for group in [2, 3] {
        assert!(
            state
                .apply(Command::create_group(
                    id(),
                    state.revision(),
                    group,
                    BTreeSet::from([1])
                ))
                .is_applied()
        );
    }
    state
}

#[test]
fn full_hash_space_includes_the_largest_value_and_split_preserves_boundaries() {
    let (left, right) = HashRange::FULL.split(1 << 63).unwrap();
    for hash in [0, 1, (1 << 63) - 1, 1 << 63, u64::MAX] {
        assert_ne!(left.contains(hash), right.contains(hash));
    }
    assert!(right.contains(u64::MAX));
    assert_eq!(left.merge(right).unwrap(), HashRange::FULL);
    assert!(left.split(0).is_err());
    assert!(left.split(1 << 63).is_err());
    assert!(right.merge(left).is_err());
    assert!(
        HashRange {
            start: u64::MAX,
            end: Some(u64::MAX)
        }
        .validate()
        .is_err()
    );
}

#[test]
fn keyed_hash_is_stable_and_absent_keys_use_the_send_identity() {
    let producer = id();
    let first = routing_hash(Some(b"order-42"), producer, 1).unwrap();
    assert_eq!(first, routing_hash(Some(b"order-42"), id(), 99).unwrap());
    assert_ne!(
        routing_hash(None, producer, 1).unwrap(),
        routing_hash(None, producer, 2).unwrap()
    );
    assert_ne!(
        routing_hash(None, producer, 1).unwrap(),
        routing_hash(Some(b""), producer, 1).unwrap()
    );
}

#[test]
fn layouts_reject_gaps_overlaps_duplicate_ids_and_incomplete_coverage() {
    let topic = id();
    let (left, right) = HashRange::FULL.split(10).unwrap();
    let mut layout = TopicLayout {
        topic,
        revision: 1,
        shards: vec![
            ShardRoute {
                id: id(),
                range: left,
                group: 2,
                authority_epoch: 1,
            },
            ShardRoute {
                id: id(),
                range: right,
                group: 3,
                authority_epoch: 1,
            },
        ],
    };
    layout.validate(2).unwrap();
    assert_eq!(layout.route(9).unwrap().group, 2);
    assert_eq!(layout.route(10).unwrap().group, 3);
    assert_eq!(layout.route(u64::MAX).unwrap().group, 3);
    for start in [9, 11] {
        let mut broken = layout.clone();
        broken.shards.get_mut(1).unwrap().range.start = start;
        assert!(broken.validate(2).is_err());
    }
    layout.shards.get_mut(1).unwrap().range.end = Some(u64::MAX);
    assert!(layout.validate(2).is_err());
}

#[test]
fn prepared_split_replays_original_plan_and_cannot_activate_without_source_seal() {
    let mut state = metadata();
    let topic = id();
    assert!(
        state
            .apply(Command::create_topic(
                id(),
                state.revision(),
                topic,
                "orders".into(),
                vec![2]
            ))
            .is_applied()
    );
    let source = state
        .topology()
        .layout(topic)
        .unwrap()
        .shards
        .first()
        .unwrap()
        .id;
    let operation = id();
    let command = Command::prepare_layout(
        operation,
        state.revision(),
        topic,
        1,
        LayoutChange::Split {
            source,
            at: 1 << 63,
            groups: [2, 3],
        },
    );
    let original = state.apply(command.clone());
    assert!(original.is_applied());
    assert_eq!(state.apply(command), original);
    assert_eq!(
        state.topology().plan(operation).unwrap().stage,
        LayoutStage::Prepare
    );
    assert!(matches!(
        state.apply(Command::activate_layout(id(), state.revision(), operation)),
        Response::Rejected {
            reason: Rejection::RecoveryRequired,
            ..
        }
    ));
    assert_eq!(state.topology().layout(topic).unwrap().revision, 1);
    state.validate_snapshot().unwrap();
    let encoded = postcard::to_stdvec(&state).unwrap();
    let restored: MetadataState = postcard::from_bytes(&encoded).unwrap();
    restored.validate_snapshot().unwrap();
    assert_eq!(state, restored);
}

#[test]
fn source_seal_is_immutable_and_missing_target_readiness_keeps_the_old_layout() {
    let mut state = metadata();
    let topic = id();
    assert!(
        state
            .apply(Command::create_topic(
                id(),
                state.revision(),
                topic,
                "orders".into(),
                vec![2]
            ))
            .is_applied()
    );
    let source = state
        .topology()
        .layout(topic)
        .unwrap()
        .shards
        .first()
        .unwrap()
        .id;
    let plan = id();
    assert!(
        state
            .apply(Command::prepare_layout(
                plan,
                state.revision(),
                topic,
                1,
                LayoutChange::Split {
                    source,
                    at: 20,
                    groups: [2, 3]
                }
            ))
            .is_applied()
    );
    assert!(
        state
            .apply(Command::observe_layout_seal(
                id(),
                state.revision(),
                plan,
                source,
                42
            ))
            .is_applied()
    );
    assert!(matches!(
        state.apply(Command::observe_layout_seal(
            id(),
            state.revision(),
            plan,
            source,
            43
        )),
        Response::Rejected {
            reason: Rejection::InvalidEvidence,
            ..
        }
    ));
    assert!(matches!(
        state.apply(Command::activate_layout(id(), state.revision(), plan)),
        Response::Rejected {
            reason: Rejection::RecoveryRequired,
            ..
        }
    ));
    assert_eq!(
        state.topology().plan(plan).unwrap().sealed.get(&source),
        Some(&42)
    );
    state.validate_snapshot().unwrap();
}

#[test]
fn initial_layouts_cover_the_whole_space_even_when_the_shard_count_is_not_a_power_of_two() {
    let mut state = metadata();
    let topic = id();
    assert!(
        state
            .apply(Command::create_topic(
                id(),
                state.revision(),
                topic,
                "orders".into(),
                vec![2, 3, 2]
            ))
            .is_applied()
    );
    let layout = state.topology().layout(topic).unwrap();
    layout.validate(3).unwrap();
    assert_eq!(layout.shards.len(), 3);
    layout.route(u64::MAX).unwrap();
    state.validate_snapshot().unwrap();
}
