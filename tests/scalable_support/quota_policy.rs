use super::*;

fn sample() -> (Status, DataStatus, QuotaGrant) {
    let partition = PartitionKey {
        topic_id: Id::from_slice(&[1; 16]).unwrap(),
        partition: 0,
    };
    let grant = QuotaGrant {
        partition,
        tenant: Id::from_slice(&[2; 16]).unwrap(),
        namespace: Id::from_slice(&[3; 16]).unwrap(),
        version: 1,
        capacity: QuotaCapacity {
            backlog_bytes: 1_000,
            inflight_bytes: 500,
            inflight_messages: 10,
            publish_bytes_per_second: 1_000,
            consume_bytes_per_second: 1_000,
            ..Default::default()
        },
        frozen: false,
    };
    let status = Status {
        partition,
        tenant: grant.tenant,
        namespace: grant.namespace,
        group: 2,
        allocation: None,
        available: Default::default(),
        suspended: false,
        now_ms: 10_000,
    };
    let data = DataStatus {
        partition,
        grant: Some(grant.clone()),
        usage: QuotaCapacity {
            backlog_bytes: 300,
            inflight_bytes: 200,
            inflight_messages: 2,
            ..Default::default()
        },
        last_activity_ms: Some(10_000),
        rates: QuotaCapacity {
            publish_bytes_per_second: 100,
            consume_bytes_per_second: 600,
            ..Default::default()
        },
        rate_updated_ms: Some(0),
        now_ms: 10_000,
    };
    (status, data, grant)
}

#[test]
fn active_rate_reclamation_freezes_before_returning_credit_and_preserves_live_usage() {
    let (status, mut data, mut grant) = sample();
    data.now_ms = 9_999;
    assert_eq!(reclaim_allocation(&status, &data, &grant), None);
    data.now_ms = 10_000;
    assert_eq!(
        reclaim_allocation(&status, &data, &grant),
        Some((grant.capacity, true))
    );
    grant.frozen = true;
    data.usage.inflight_bytes = 250;
    let (retained, frozen) = reclaim_allocation(&status, &data, &grant).unwrap();
    assert!(!frozen);
    assert_eq!(retained.backlog_bytes, 300);
    assert_eq!(retained.inflight_bytes, 250);
    assert_eq!(retained.inflight_messages, 2);
    assert_eq!(retained.publish_bytes_per_second, 100);
    assert_eq!(retained.consume_bytes_per_second, 600);
    grant.capacity = retained;
    grant.frozen = false;
    assert_eq!(reclaim_allocation(&status, &data, &grant), None);
}

#[test]
fn suspended_or_idle_domains_release_rates_only_after_fencing() {
    let (mut status, data, mut grant) = sample();
    status.suspended = true;
    assert_eq!(
        reclaim_allocation(&status, &data, &grant),
        Some((grant.capacity, true))
    );
    grant.frozen = true;
    assert_eq!(
        reclaim_allocation(&status, &data, &grant),
        Some((data.usage, true))
    );
    grant.capacity = data.usage;
    assert_eq!(reclaim_allocation(&status, &data, &grant), None);
    status.suspended = false;
    status.now_ms = 15_000;
    assert_eq!(
        reclaim_allocation(&status, &data, &grant),
        Some((data.usage, false))
    );
}

#[test]
fn rate_growth_reuses_small_slices_and_never_exceeds_available_capacity() {
    assert_eq!(grow_rate(0, 100, 2 * 1024 * 1024), 1024 * 1024);
    assert_eq!(grow_rate(100, 101, 1_000), 200);
    assert_eq!(grow_rate(100, 101, 150), 150);
    assert_eq!(grow_rate(100, 80, 1_000), 100);
    let demand = QuotaCapacity {
        publish_bytes_per_second: 151,
        ..Default::default()
    };
    let maximum = QuotaCapacity {
        publish_bytes_per_second: 150,
        ..Default::default()
    };
    assert!(grow(Default::default(), demand, maximum).is_err());
}
