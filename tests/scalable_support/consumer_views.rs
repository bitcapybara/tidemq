use super::*;

#[test]
fn a_completed_assignment_uses_its_last_query_start_and_rejects_late_views() {
    let now = Instant::now();
    let status = public::ConsumerControlResult {
        remaining_lease_ms: 15_000,
        ..Default::default()
    };
    assert!(!fresh_view(
        &status,
        now.checked_sub(Duration::from_secs(30)).unwrap()
    ));
    assert!(fresh_view(&status, now));
    assert!(!fresh_view(
        &public::ConsumerControlResult {
            remaining_lease_ms: 0,
            ..Default::default()
        },
        now
    ));
    assert!(!fresh_view(
        &public::ConsumerControlResult {
            remaining_lease_ms: 15_001,
            ..Default::default()
        },
        now
    ));
}
