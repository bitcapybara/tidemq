use super::*;
use std::time::Duration;

fn proof() -> Proof {
    Proof {
        cluster: Id::new_v7().unwrap(),
        group: 1,
        voters: [1, 2, 4].into(),
        position: CommitPosition::Raft {
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
    proof.position = CommitPosition::Raft {
        term: 7,
        leader_node: 3,
        index: 100,
    };
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
    proof.position = CommitPosition::Raft {
        term: 0,
        leader_node: 1,
        index: 100,
    };
    assert!(matches!(
        proof.validate(proof.cluster, 1, &proof.voters, started, ttl),
        Err(Error::Invalid { .. })
    ));
}

#[test]
fn data_membership_changes_remain_retryable_without_accepting_foreign_grants() {
    let grant = crate::ownership::tests::grant(1);
    let voters = BTreeSet::from([1, 2, 4]);
    let mut position = CommitPosition::Raft {
        term: 7,
        leader_node: 1,
        index: 100,
    };
    let validate = |actual: &BTreeSet<u64>, position, expected: &Grant| {
        validate_data_proof(
            grant.cluster_id,
            grant.group_id,
            actual,
            position,
            &grant,
            grant.cluster_id,
            expected,
            &voters,
        )
    };
    validate(&voters, position, &grant).unwrap();
    let old = BTreeSet::from([1, 2, 3]);
    for actual in [&old, &voters] {
        if actual == &voters {
            position = CommitPosition::Raft {
                term: 7,
                leader_node: 3,
                index: 100,
            };
        }
        let failure = validate(actual, position, &grant).unwrap_err().failure();
        assert_eq!(
            failure.code,
            i32::from(tidemq_protocol::common::ErrorCode::Unavailable)
        );
        assert!(failure.retryable);
    }
    let foreign = crate::ownership::tests::grant(2);
    assert!(matches!(
        validate(&voters, position, &foreign),
        Err(Error::Invalid { .. })
    ));
}

#[test]
fn remote_quorum_proofs_reject_local_sequences() {
    let mut proof = proof();
    proof.position = CommitPosition::Local { sequence: 100 };
    assert!(
        proof
            .validate(
                proof.cluster,
                proof.group,
                &proof.voters,
                MonoInstant::now(),
                Duration::from_secs(3)
            )
            .is_err()
    );
    let grant = crate::ownership::tests::grant(1);
    assert!(
        validate_data_proof(
            grant.cluster_id,
            grant.group_id,
            &proof.voters,
            proof.position,
            &grant,
            grant.cluster_id,
            &grant,
            &proof.voters
        )
        .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_authority_projects_real_local_placement_without_rpc_or_voting_membership() {
    use crate::execution::{GroupHandle, LocalGroup, StorageIdentity};
    use crate::metadata::{
        Command, DeploymentMode, GroupPlacement, MetadataLimits, MetadataState, NodeRole, NodeSpec,
    };
    let directory =
        std::env::temp_dir().join(format!("tidemq-local-authority-{}", Id::new_v7().unwrap()));
    let cluster = Id::new_v7().unwrap();
    let group = GroupHandle::Local(
        LocalGroup::open(
            &directory,
            StorageIdentity::new::<MetadataState>(cluster, 1, 7),
            Default::default(),
            MetadataState::default(),
        )
        .await
        .unwrap(),
    );
    let deadline = || Instant::now() + Duration::from_secs(5);
    assert!(
        group
            .commit(
                Command::bootstrap(
                    Id::new_v7().unwrap(),
                    cluster,
                    1,
                    DeploymentMode::Local,
                    MetadataLimits::default()
                ),
                deadline()
            )
            .await
            .unwrap()
            .response
            .is_applied()
    );
    let revision = group
        .state()
        .try_with_application(MetadataState::revision)
        .await
        .unwrap();
    assert!(
        group
            .commit(
                Command::put_node(
                    Id::new_v7().unwrap(),
                    revision,
                    NodeSpec {
                        node_id: 7,
                        zone: "local".into(),
                        endpoint: "127.0.0.1:7443".parse().unwrap(),
                        roles: [NodeRole::Metadata, NodeRole::Storage].into(),
                        schedulable: true,
                    },
                    0
                ),
                deadline()
            )
            .await
            .unwrap()
            .response
            .is_applied()
    );
    let revision = group
        .state()
        .try_with_application(MetadataState::revision)
        .await
        .unwrap();
    assert!(
        group
            .commit(
                Command::create_group(
                    Id::new_v7().unwrap(),
                    revision,
                    2,
                    GroupPlacement::Local { node_id: 7 }
                ),
                deadline()
            )
            .await
            .unwrap()
            .response
            .is_applied()
    );
    let authority =
        Authority::new_local(group.clone(), cluster, 1, ControlPolicy::default()).unwrap();
    let Value::Group(data) = authority.query(Query::Group(2), deadline()).await.unwrap() else {
        panic!("Expected a local data group");
    };
    assert_eq!(data.placement, GroupPlacement::Local { node_id: 7 });
    assert!(data.placement.raft_voters().is_none());
    assert!(group.raft().is_none());
    assert!(authority.replace_voters([7].into()).is_err());
    assert!(
        Authority::new_local(
            group.clone(),
            Id::new_v7().unwrap(),
            1,
            ControlPolicy::default()
        )
        .is_err()
    );
    group.close(deadline()).await.unwrap();
    assert!(authority.query(Query::Group(2), deadline()).await.is_err());
    drop((authority, group));
    tokio::fs::remove_dir_all(directory).await.unwrap();
}

#[test]
fn a_raft_metadata_proof_cannot_describe_local_placement() {
    let mut proof = proof();
    proof.value = Value::Group(crate::metadata::DataGroup {
        group_id: 2,
        generation: 1,
        placement: crate::metadata::GroupPlacement::Local { node_id: 1 },
    });
    assert!(
        proof
            .validate(
                proof.cluster,
                proof.group,
                &proof.voters,
                MonoInstant::now(),
                Duration::from_secs(3)
            )
            .is_err()
    );
}
