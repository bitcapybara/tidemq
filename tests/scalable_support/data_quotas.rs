use super::*;
use tidemq_protocol::Id;

fn grant(version: u64, rate: u64) -> crate::metadata::QuotaGrant {
    crate::metadata::QuotaGrant {
        partition: crate::metadata::PartitionKey {
            topic_id: Id::from_slice(&[1; 16]).unwrap(),
            partition: 0,
        },
        tenant: Id::from_slice(&[2; 16]).unwrap(),
        namespace: Id::from_slice(&[3; 16]).unwrap(),
        version,
        capacity: crate::metadata::QuotaCapacity {
            backlog_bytes: 100,
            publish_bytes_per_second: rate,
            consume_bytes_per_second: rate,
            inflight_bytes: 100,
            inflight_messages: 10,
            cold_reads: 1,
        },
        frozen: false,
    }
}

#[test]
fn each_rate_dimension_receives_its_initial_burst_only_once_even_after_idle_credit_reclamation() {
    let quota = TenantQuotaState::new(grant(1, 0), 0).unwrap();
    let mut quota = quota.update(grant(2, 100), Default::default(), 0).unwrap();
    quota.commit_publish(quota.publish(0, false, 80, 0).unwrap());
    quota.commit_consume(80, 0);
    assert_eq!(quota.read_budget(Default::default(), 0).unwrap().bytes, 20);
    let quota = quota.update(grant(3, 0), Default::default(), 0).unwrap();
    let quota = quota.update(grant(4, 100), Default::default(), 0).unwrap();
    assert_eq!(
        quota.publish(0, false, 1, 0),
        Err(QuotaRejection::Exhausted)
    );
    assert_eq!(quota.read_budget(Default::default(), 0).unwrap().bytes, 0);
    assert_eq!(
        quota.read_budget(Default::default(), 100).unwrap().bytes,
        10
    );
}

#[test]
fn delivery_admission_shares_byte_message_and_rate_credit_without_double_charging_replays() {
    let mut quota = TenantQuotaState::new(grant(1, 100), 0).unwrap();
    let usage = QuotaCapacity {
        inflight_bytes: 80,
        inflight_messages: 9,
        ..Default::default()
    };
    let budget = quota.read_budget(usage, 0).unwrap();
    assert_eq!(budget.bytes, 20);
    assert_eq!(budget.messages, 1);
    quota.commit_consume(20, 0);
    assert_eq!(quota.read_budget(Default::default(), 0).unwrap().bytes, 80);
    assert_eq!(
        quota.read_budget(Default::default(), 200).unwrap().bytes,
        100
    );
    let mut frozen = grant(2, 100);
    frozen.frozen = true;
    let quota = quota.update(frozen, usage, 0).unwrap();
    assert_eq!(quota.read_budget(usage, 0), Err(QuotaRejection::Exhausted));
}

#[test]
fn rate_reservations_only_commit_with_the_operation_and_quota_replays_do_not_refill() {
    let mut quota = TenantQuotaState::new(grant(1, 100), 1).unwrap();
    let reserved = quota.publish(0, false, 80, 1).unwrap();
    assert!(quota.publish(0, false, 100, 1).is_ok());
    quota.commit_publish(reserved);
    assert_eq!(
        quota.publish(0, false, 21, 1),
        Err(QuotaRejection::Exhausted)
    );
    let same = quota
        .update(grant(1, 100), Default::default(), 5_000)
        .unwrap();
    assert_eq!(same, quota);
    let increased = quota.update(grant(2, 200), Default::default(), 1).unwrap();
    assert_eq!(
        increased.publish(0, false, 21, 1),
        Err(QuotaRejection::Exhausted)
    );
    let recovered: TenantQuotaState =
        postcard::from_bytes(&postcard::to_stdvec(&increased).unwrap()).unwrap();
    assert_eq!(
        recovered.publish(0, false, 21, 0),
        Err(QuotaRejection::Exhausted)
    );
    assert!(recovered.publish(0, false, 40, 101).is_ok());
}

#[test]
fn quota_shrinking_cannot_release_protected_backlog_or_live_delivery_credit() {
    let quota = TenantQuotaState::new(grant(1, 100), 0).unwrap();
    let mut smaller = grant(2, 100);
    smaller.capacity.backlog_bytes = 50;
    let usage = crate::metadata::QuotaCapacity {
        backlog_bytes: 51,
        ..Default::default()
    };
    assert_eq!(
        quota.update(smaller.clone(), usage, 0),
        Err(QuotaRejection::Occupied)
    );
    smaller.capacity.inflight_bytes = 10;
    let usage = crate::metadata::QuotaCapacity {
        inflight_bytes: 11,
        ..Default::default()
    };
    assert_eq!(
        quota.update(smaller.clone(), usage, 0),
        Err(QuotaRejection::Occupied)
    );
    assert!(quota.update(smaller, Default::default(), 0).is_ok());
    assert_eq!(
        quota.publish(90, true, 11, 0),
        Err(QuotaRejection::Exhausted)
    );
    assert!(quota.publish(90, false, 11, 0).is_ok());
}

#[test]
fn grant_parentage_versions_and_freezing_cannot_be_bypassed() {
    let quota = TenantQuotaState::new(grant(1, 100), 0).unwrap();
    let mut changed = grant(2, 100);
    changed.tenant = Id::from_slice(&[9; 16]).unwrap();
    assert_eq!(
        quota.update(changed, Default::default(), 0),
        Err(QuotaRejection::InvalidGrant)
    );
    assert_eq!(
        quota.update(grant(3, 100), Default::default(), 0),
        Err(QuotaRejection::StaleGrant)
    );
    let mut frozen = grant(2, 100);
    frozen.frozen = true;
    let frozen = quota.update(frozen, Default::default(), 0).unwrap();
    assert_eq!(
        frozen.publish(0, false, 1, 0),
        Err(QuotaRejection::Exhausted)
    );
    frozen.validate().unwrap();
}
