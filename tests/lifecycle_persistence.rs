#[path = "lifecycle_support/mod.rs"]
mod support;

use std::time::Duration;

use tidemq::{
    lifecycle::{Blocker, Error, ObjectPhase, RangeRequest, Rejection},
    object_store::Limits,
};
use tidemq_protocol::state::OffsetRange;

fn ranges(fixture: &support::Fixture, end: u64) -> Vec<RangeRequest> {
    vec![RangeRequest {
        partition: fixture.partition,
        offsets: OffsetRange::new(0, end).unwrap(),
    }]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the verified RustFS 1.0.3 binary through TIDEMQ_TEST_RUSTFS_BIN"]
async fn raft_restart_at_publication_boundaries_and_compaction_preserve_every_record() {
    let rustfs = support::rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = support::Fixture::new().await;
    fixture.produce(3).await;
    let store = rustfs.store(fixture.cluster, Limits::default(), support::s3::budget());
    let coordinator = fixture.coordinator(store.clone());
    let prepared = coordinator
        .prepare(ranges(&fixture, 3), vec![], support::deadline())
        .await
        .unwrap();
    let object_id = prepared.object_id();
    coordinator
        .register(&prepared, support::deadline())
        .await
        .unwrap();
    assert_eq!(
        coordinator
            .diagnose(object_id, support::deadline())
            .await
            .unwrap()
            .blocker,
        Blocker::PendingTask
    );
    assert_eq!(
        fixture
            .data
            .state
            .with_application(|state| state.lifecycle().published_end(fixture.partition))
            .await,
        0
    );
    drop(coordinator);
    fixture = fixture.restart().await;
    let coordinator = fixture.coordinator(store.clone());
    coordinator
        .authorize(object_id, support::deadline())
        .await
        .unwrap();
    drop(coordinator);
    fixture = fixture.restart().await;
    let coordinator = fixture.coordinator(store.clone());
    coordinator
        .upload(&prepared, support::deadline())
        .await
        .unwrap();
    assert_eq!(
        coordinator
            .diagnose(object_id, support::deadline())
            .await
            .unwrap()
            .phase,
        ObjectPhase::Uploaded
    );
    assert!(matches!(
        coordinator
            .reclaim(fixture.partition, 3, support::deadline())
            .await,
        Err(Error::Rejected {
            reason: Rejection::NotPublished
        })
    ));
    drop(coordinator);
    fixture = fixture.restart().await;
    let coordinator = fixture.coordinator(store.clone());
    coordinator
        .publish_manifest(object_id, support::deadline())
        .await
        .unwrap();
    assert_eq!(
        fixture
            .data
            .state
            .with_application(|state| state.lifecycle().published_end(fixture.partition))
            .await,
        0
    );
    drop(coordinator);
    fixture = fixture.restart().await;
    let coordinator = fixture.coordinator(store.clone());
    coordinator
        .reconcile(object_id, support::deadline())
        .await
        .unwrap();
    assert!(matches!(
        coordinator
            .reclaim(fixture.partition, 3, support::deadline())
            .await,
        Err(Error::Rejected {
            reason: Rejection::CheckpointRequired
        })
    ));
    coordinator
        .checkpoint(object_id, support::deadline())
        .await
        .unwrap();
    coordinator
        .reclaim(fixture.partition, 3, support::deadline())
        .await
        .unwrap();
    fixture
        .data
        .state
        .with_application(|state| {
            assert_eq!(state.hot_bytes(), 0);
            assert_eq!(
                state.producer(fixture.partition).unwrap().retained_floor(),
                0
            );
            assert_eq!(
                state
                    .producer(fixture.partition)
                    .unwrap()
                    .local_reclaim_end(),
                3
            );
            assert_eq!(state.lifecycle().published_end(fixture.partition), 3);
        })
        .await;
    let old_page = coordinator
        .read(fixture.partition, 0, 3, 1024, support::deadline())
        .await
        .unwrap();
    let expected = old_page
        .records()
        .unwrap()
        .iter()
        .map(|record| (record.offset, record.key().to_vec(), record.body().to_vec()))
        .collect::<Vec<_>>();
    assert_eq!(expected.len(), 3);
    let old_proof = old_page.verified_delivery(0).unwrap();
    assert_eq!(old_proof.body_bytes(), 11);
    let replacement = coordinator
        .prepare(ranges(&fixture, 3), vec![object_id], support::deadline())
        .await
        .unwrap();
    let replacement_id = replacement.object_id();
    coordinator
        .register(&replacement, support::deadline())
        .await
        .unwrap();
    coordinator
        .authorize(replacement_id, support::deadline())
        .await
        .unwrap();
    coordinator
        .upload(&replacement, support::deadline())
        .await
        .unwrap();
    coordinator
        .publish(replacement_id, support::deadline())
        .await
        .unwrap();
    assert!(matches!(
        coordinator.collect(object_id, support::deadline()).await,
        Err(Error::Rejected {
            reason: Rejection::Pinned
        })
    ));
    old_page.release(support::deadline()).await.unwrap();
    assert!(matches!(
        coordinator.collect(object_id, support::deadline()).await,
        Err(Error::Rejected {
            reason: Rejection::RecoveryDependency
        })
    ));
    assert_eq!(
        coordinator
            .diagnose(object_id, support::deadline())
            .await
            .unwrap()
            .blocker,
        Blocker::RecoverySnapshot
    );
    coordinator
        .checkpoint(replacement_id, support::deadline())
        .await
        .unwrap();
    coordinator
        .collect(object_id, support::deadline())
        .await
        .unwrap();
    assert_eq!(
        coordinator
            .diagnose(object_id, support::deadline())
            .await
            .unwrap()
            .phase,
        ObjectPhase::Deleted
    );
    coordinator
        .collect(object_id, support::deadline())
        .await
        .unwrap();
    let current = coordinator
        .read(fixture.partition, 0, 3, 1024, support::deadline())
        .await
        .unwrap();
    assert_eq!(
        current
            .records()
            .unwrap()
            .iter()
            .map(|record| (record.offset, record.key().to_vec(), record.body().to_vec()))
            .collect::<Vec<_>>(),
        expected
    );
    current.release(support::deadline()).await.unwrap();
    let pin = coordinator
        .acquire_pin(
            vec![replacement_id],
            Duration::from_millis(20),
            support::deadline(),
        )
        .await
        .unwrap();
    let id = pin.id();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(pin.check().is_err());
    coordinator
        .expire_pin(id, support::deadline())
        .await
        .unwrap();
    drop(pin);
    drop(coordinator);
    fixture = fixture.restart().await;
    let coordinator = fixture.coordinator(store);
    assert_eq!(
        coordinator
            .diagnose(object_id, support::deadline())
            .await
            .unwrap()
            .phase,
        ObjectPhase::Deleted
    );
    let current = coordinator
        .read(fixture.partition, 0, 3, 1024, support::deadline())
        .await
        .unwrap();
    assert_eq!(current.records().unwrap().len(), 3);
    current.release(support::deadline()).await.unwrap();
    drop(coordinator);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the verified RustFS 1.0.3 binary through TIDEMQ_TEST_RUSTFS_BIN"]
async fn aborted_unseen_intent_survives_both_raft_restarts_and_rejects_delayed_upload() {
    let rustfs = support::rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let fixture = support::Fixture::new().await;
    fixture.produce(1).await;
    let store = rustfs.store(fixture.cluster, Limits::default(), support::s3::budget());
    let coordinator = fixture.coordinator(store.clone());
    let prepared = coordinator
        .prepare(ranges(&fixture, 1), vec![], support::deadline())
        .await
        .unwrap();
    let object_id = prepared.object_id();
    coordinator
        .register(&prepared, support::deadline())
        .await
        .unwrap();
    coordinator
        .cancel(object_id, support::deadline())
        .await
        .unwrap();
    assert_eq!(
        coordinator
            .diagnose(object_id, support::deadline())
            .await
            .unwrap()
            .phase,
        ObjectPhase::Aborted
    );
    drop(coordinator);
    let fixture = fixture.restart().await;
    let coordinator = fixture.coordinator(store);
    assert!(
        coordinator
            .authorize(object_id, support::deadline())
            .await
            .is_err()
    );
    assert!(
        coordinator
            .upload(&prepared, support::deadline())
            .await
            .is_err()
    );
    assert!(
        coordinator
            .publish_manifest(object_id, support::deadline())
            .await
            .is_err()
    );
    coordinator
        .collect(object_id, support::deadline())
        .await
        .unwrap();
    coordinator
        .collect(object_id, support::deadline())
        .await
        .unwrap();
    assert_eq!(
        fixture
            .data
            .state
            .with_application(|state| state.hot_messages())
            .await,
        1
    );
    assert_eq!(
        coordinator
            .diagnose(object_id, support::deadline())
            .await
            .unwrap()
            .phase,
        ObjectPhase::Deleted
    );
    drop(coordinator);
    fixture.close().await;
}
