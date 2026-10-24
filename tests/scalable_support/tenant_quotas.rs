use super::*;
use crate::metadata::{AllocationRequest, QuotaCapacity, QuotaLedger};

fn capacity(bytes: u64) -> QuotaCapacity {
    QuotaCapacity {
        backlog_bytes: bytes,
        ..QuotaCapacity::default()
    }
}

fn request(partition: u32, namespace: u64, version: u64, bytes: u64) -> AllocationRequest {
    AllocationRequest {
        partition: PartitionKey {
            topic_id: id(80),
            partition,
        },
        tenant: id(81),
        namespace: id(namespace),
        expected_version: version,
        capacity: capacity(bytes),
        frozen: false,
    }
}

#[test]
fn namespace_allocations_share_one_tenant_total_and_pending_shrinks_do_not_release_credit() {
    let mut ledger = QuotaLedger::default();
    let first = ledger
        .prepare(request(0, 82, 0, 80), capacity(100), capacity(100), 8, 0)
        .unwrap();
    ledger.observe(&first, 0).unwrap();
    assert_eq!(
        ledger.prepare(request(1, 83, 0, 30), capacity(100), capacity(100), 8, 0),
        Err(Rejection::Capacity)
    );
    let smaller = ledger
        .prepare(request(0, 82, 1, 20), capacity(100), capacity(100), 8, 0)
        .unwrap();
    assert_eq!(
        ledger.prepare(request(1, 83, 0, 30), capacity(100), capacity(100), 8, 0),
        Err(Rejection::Capacity)
    );
    let mut recovered: QuotaLedger =
        postcard::from_bytes(&postcard::to_stdvec(&ledger).unwrap()).unwrap();
    assert_eq!(recovered, ledger);
    recovered.observe(&smaller, 0).unwrap();
    let second = recovered
        .prepare(request(1, 83, 0, 30), capacity(100), capacity(100), 8, 0)
        .unwrap();
    recovered.observe(&second, 0).unwrap();
    assert_eq!(recovered.reserved(id(81), None, 0).unwrap(), capacity(50));
    assert_eq!(recovered.observe(&first, 0), Err(Rejection::Superseded));
    assert_eq!(recovered.reserved(id(81), None, 0).unwrap(), capacity(50));
}

#[test]
fn quota_retries_keep_the_original_version_and_cannot_change_parentage_or_an_unfinished_plan() {
    let mut ledger = QuotaLedger::default();
    let original = request(0, 82, 0, 30);
    let grant = ledger
        .prepare(original.clone(), capacity(100), capacity(40), 8, 0)
        .unwrap();
    assert_eq!(
        ledger
            .prepare(original.clone(), capacity(100), capacity(40), 8, 0)
            .unwrap(),
        grant
    );
    assert_eq!(
        ledger.prepare(request(0, 82, 0, 31), capacity(100), capacity(40), 8, 0),
        Err(Rejection::RecoveryRequired)
    );
    assert_eq!(
        ledger.prepare(request(0, 83, 0, 30), capacity(100), capacity(40), 8, 0),
        Err(Rejection::InvalidEvidence)
    );
    ledger.observe(&grant, 0).unwrap();
    assert_eq!(
        ledger
            .prepare(original, capacity(100), capacity(40), 8, 0)
            .unwrap(),
        grant
    );
    assert_eq!(
        ledger.prepare(request(1, 82, 0, 20), capacity(100), capacity(40), 8, 0),
        Err(Rejection::Capacity)
    );
    assert!(
        ledger
            .prepare(request(1, 83, 0, 40), capacity(100), capacity(40), 8, 0)
            .is_ok()
    );
}

#[test]
fn lowering_limits_preserves_existing_reservations_and_only_allows_reduction() {
    let mut ledger = QuotaLedger::default();
    let grant = ledger
        .prepare(request(0, 82, 0, 80), capacity(100), capacity(100), 8, 0)
        .unwrap();
    ledger.observe(&grant, 0).unwrap();
    let mut freezing = request(0, 82, 1, 80);
    freezing.frozen = true;
    let frozen = ledger
        .prepare(freezing, capacity(40), capacity(40), 8, 0)
        .unwrap();
    assert!(frozen.frozen);
    ledger.observe(&frozen, 0).unwrap();
    assert_eq!(
        ledger.prepare(request(1, 82, 0, 1), capacity(40), capacity(40), 8, 0),
        Err(Rejection::Capacity)
    );
    let reduced = ledger
        .prepare(request(0, 82, 2, 20), capacity(40), capacity(40), 8, 0)
        .unwrap();
    ledger.observe(&reduced, 0).unwrap();
    assert!(
        ledger
            .prepare(request(1, 82, 0, 20), capacity(40), capacity(40), 8, 0)
            .is_ok()
    );
}

#[test]
fn retiring_rate_credit_cannot_mint_another_burst_by_rapid_redistribution() {
    let rate = |bytes| QuotaCapacity {
        publish_bytes_per_second: bytes,
        ..Default::default()
    };
    let mut ledger = QuotaLedger::default();
    let mut first = request(0, 82, 0, 0);
    first.capacity = rate(80);
    let grant = ledger.prepare(first, rate(100), rate(100), 8, 0).unwrap();
    ledger.observe(&grant, 0).unwrap();
    let mut shrink = request(0, 82, 1, 0);
    shrink.capacity = rate(20);
    let shrink = ledger.prepare(shrink, rate(100), rate(100), 8, 1).unwrap();
    ledger.observe(&shrink, 1).unwrap();
    ledger.observe(&shrink, 1_000).unwrap();
    let mut second = request(1, 83, 0, 0);
    second.capacity = rate(30);
    assert_eq!(
        ledger.prepare(second.clone(), rate(100), rate(100), 8, 2_000),
        Err(Rejection::Capacity)
    );
    assert!(
        ledger
            .prepare(second, rate(100), rate(100), 8, 2_001)
            .is_ok()
    );
    ledger.validate(8).unwrap();
}
