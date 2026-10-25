use super::*;

#[test]
fn differences_preserve_committed_work_and_reject_resets_invalidity_and_overflow() {
    let range = HashRange {
        start: 0,
        end: None,
    };
    let mut before = ShardLoad::default();
    before.publish(range, 1, 100);
    let mut next = before.clone();
    next.publish(range, 1_u64 << 63, 200);
    next.deliver(250, 2);
    next.acknowledge(Some(80), 2);
    let saved: ShardLoad = postcard::from_bytes(&postcard::to_stdvec(&next).unwrap()).unwrap();
    let delta = saved.difference(&before).unwrap();
    assert_eq!(delta.publish_messages, 1);
    assert_eq!(delta.publish_bytes, 200);
    assert_eq!(delta.delivery_bytes, 250);
    assert_eq!(delta.deliveries, 2);
    assert_eq!(delta.read_requests, 1);
    assert_eq!(delta.ack_bytes, 80);
    assert_eq!(delta.acknowledgements, 2);
    assert_eq!(delta.ack_requests, 1);
    assert!(std::mem::size_of::<ShardLoad>() as u64 <= ShardLoad::LOGICAL_BYTES);
    assert!(before.difference(&next).is_none());
    next.publish(range, 0, u64::MAX);
    assert!(next.invalid);
    assert!(next.difference(&saved).is_none());
}

#[test]
fn hash_summary_proves_a_cut_without_claiming_that_a_single_hot_key_can_scale() {
    let range = HashRange {
        start: 0,
        end: None,
    };
    let mut load = ShardLoad::default();
    load.publish(range, 7, 100);
    assert_eq!(load.split_at(range), None);
    load.publish(range, u64::MAX, 100);
    let cut = load.split_at(range).unwrap();
    assert!(7 < cut && cut < u64::MAX);
    let mut invalid = load.clone();
    invalid.acknowledge(None, 1);
    assert_eq!(invalid.split_at(range), None);
    let before = load.clone();
    load.hash_bins.fill(0);
    assert!(load.difference(&before).is_none());
    let mut skewed = ShardLoad::default();
    skewed.publish(range, 0, 99);
    skewed.publish(range, u64::MAX, 1);
    assert_eq!(skewed.split_at(range), None);
}

#[test]
fn normalized_bins_keep_full_and_tiny_half_open_range_boundaries_exact() {
    for range in [
        HashRange {
            start: 0,
            end: None,
        },
        HashRange {
            start: u64::MAX - 1,
            end: None,
        },
        HashRange {
            start: 9,
            end: Some(11),
        },
    ] {
        let mut load = ShardLoad::default();
        let last = range
            .end
            .unwrap_or(u64::MAX)
            .saturating_sub(u64::from(range.end.is_some()));
        load.publish(range, range.start, 1);
        load.publish(range, last, 1);
        assert!(!load.invalid);
        let cut = load.split_at(range).unwrap();
        assert!(cut > range.start && cut <= last);
    }
    let mut load = ShardLoad::default();
    load.publish(
        HashRange {
            start: 10,
            end: Some(11),
        },
        9,
        1,
    );
    assert!(load.invalid);
}
