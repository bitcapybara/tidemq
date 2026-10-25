use super::*;
use std::time::Duration;

fn proof() -> Proof {
    Proof {
        cluster: Id::new_v7().unwrap(),
        group: 1,
        voters: [1, 2, 4].into(),
        position: CommitPosition {
            term: 7,
            leader_node: 1,
            index: 100,
        },
        value: Value::Topic(None),
    }
}

#[test]
fn topology_changes_and_expired_proofs_fail_closed_with_a_retryable_error() {
    let mut proof = proof();
    let started = MonoInstant::now();
    let ttl = Duration::from_secs(3);
    proof
        .validate(proof.cluster, 1, &proof.voters, started, ttl)
        .unwrap();
    for (voters, ttl) in [
        (BTreeSet::from([1, 2, 3]), ttl),
        (proof.voters.clone(), Duration::ZERO),
    ] {
        let failure = proof
            .validate(proof.cluster, 1, &voters, started, ttl)
            .unwrap_err()
            .failure();
        assert_eq!(
            failure.code,
            i32::from(tidemq_protocol::common::ErrorCode::Unavailable)
        );
        assert!(failure.retryable);
    }
    // A committed log entry can still name the leader removed by a completed replacement.
    proof.position.leader_node = 3;
    let failure = proof
        .validate(proof.cluster, 1, &proof.voters, started, ttl)
        .unwrap_err()
        .failure();
    assert_eq!(
        failure.code,
        i32::from(tidemq_protocol::common::ErrorCode::Unavailable)
    );
    assert!(failure.retryable);
}

#[test]
fn malformed_or_foreign_proofs_remain_permanent_errors() {
    let mut proof = proof();
    let started = MonoInstant::now();
    let ttl = Duration::from_secs(3);
    let foreign = Id::new_v7().unwrap();
    for (cluster, group) in [(foreign, 1), (proof.cluster, 2)] {
        assert!(matches!(
            proof.validate(cluster, group, &proof.voters, started, ttl),
            Err(Error::Invalid { .. })
        ));
    }
    proof.position.term = 0;
    assert!(matches!(
        proof.validate(proof.cluster, 1, &proof.voters, started, ttl),
        Err(Error::Invalid { .. })
    ));
}
