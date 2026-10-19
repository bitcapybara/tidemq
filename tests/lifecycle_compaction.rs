#[path = "lifecycle_support/mod.rs"]
mod support;

use std::time::Duration;

use tidemq::{
    broker::{DataCommand, DataResponse, WriterFence},
    budget::MemoryBudget,
    lifecycle::{Coordinator, Error, ObjectPhase, RangeRequest, Rejection},
    metadata::PartitionKey,
    object_store::Limits,
};
use tidemq_protocol::{Id, state::OffsetRange};
use tidemq_transport::budget::MemoryLimits;

fn retained_budget() -> MemoryBudget {
    MemoryBudget::new(MemoryLimits {
        total_bytes: 192 * 1024 * 1024,
        control_reserved_bytes: 32 * 1024 * 1024,
    })
    .unwrap()
}

fn range(partition: PartitionKey, first: u64, end: u64) -> RangeRequest {
    RangeRequest {
        partition,
        offsets: OffsetRange::new(first, end).unwrap(),
    }
}

async fn advance(fixture: &support::Fixture, partition: PartitionKey, before: u64) {
    let revision = fixture
        .data
        .state
        .with_application(|state| state.retention(partition).unwrap().revision())
        .await;
    let response = fixture
        .data
        .raft
        .client_write(DataCommand::Retention {
            partition,
            writer: WriterFence {
                node_id: 1,
                owner_epoch: 1,
            },
            command: tidemq::retention::Command {
                expected_revision: revision,
                before,
                now_ms: 3_601_000,
            },
        })
        .await
        .unwrap()
        .data;
    assert!(matches!(response, DataResponse::Retention(status) if status.floor == before));
}

async fn publish(coordinator: &Coordinator, ranges: Vec<RangeRequest>) -> Id {
    let prepared = coordinator
        .prepare(ranges, vec![], support::deadline())
        .await
        .unwrap();
    let id = prepared.object_id();
    coordinator
        .register(&prepared, support::deadline())
        .await
        .unwrap();
    coordinator
        .authorize(id, support::deadline())
        .await
        .unwrap();
    coordinator
        .upload(&prepared, support::deadline())
        .await
        .unwrap();
    coordinator.publish(id, support::deadline()).await.unwrap();
    coordinator
        .checkpoint(id, support::deadline())
        .await
        .unwrap();
    id
}

async fn assert_cold(coordinator: &Coordinator, partition: PartitionKey, first: u64, end: u64) {
    let page = coordinator
        .read(partition, first, 16, 1024, support::deadline())
        .await
        .unwrap();
    let observed = page.records().unwrap();
    assert_eq!(observed.len() as u64, end - first);
    for (record, expected) in observed.iter().zip(first..end) {
        assert_eq!(record.offset, expected);
        assert_eq!(record.key(), format!("key-{expected}").as_bytes());
        assert_eq!(record.body(), format!("body-{expected}").as_bytes());
    }
    page.release(support::deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the verified RustFS 1.0.3 binary through TIDEMQ_TEST_RUSTFS_BIN"]
async fn automatic_shared_compaction_reads_reclaimed_s3_and_resumes_original_registered_id() {
    let rustfs = support::rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = support::Fixture::with_partitions(2).await;
    let first = fixture.partition;
    let second = PartitionKey {
        topic_id: first.topic_id,
        partition: 1,
    };
    fixture.produce_partition(first, 4).await;
    fixture.produce_partition(second, 4).await;
    let budget = retained_budget();
    let store = rustfs.store(fixture.cluster, Limits::default(), budget.clone());
    let coordinator = fixture.coordinator_with_budget(store.clone(), budget.clone());
    let old = publish(&coordinator, vec![range(first, 0, 4), range(second, 0, 4)]).await;
    for partition in [first, second] {
        coordinator
            .reclaim(partition, 4, support::deadline())
            .await
            .unwrap();
    }
    assert_eq!(
        fixture
            .data
            .state
            .with_application(|state| state.hot_bytes())
            .await,
        0
    );
    let pin = coordinator
        .acquire_pin(vec![old], Duration::from_secs(10), support::deadline())
        .await
        .unwrap();
    advance(&fixture, first, 2).await;
    coordinator
        .retire_before(first, support::deadline())
        .await
        .unwrap();
    let step = coordinator
        .compact_next(None, support::deadline())
        .await
        .unwrap();
    let compacted = step
        .replacement
        .expect("partial shared object must be selected automatically");
    assert_ne!(compacted, old);
    let indices = fixture
        .metadata
        .state
        .with_application(|state| {
            state
                .lifecycle()
                .object(compacted)
                .unwrap()
                .descriptor()
                .partitions
                .clone()
        })
        .await;
    assert_eq!(indices.len(), 2);
    assert_eq!(
        indices.first().unwrap().offsets,
        OffsetRange::new(2, 4).unwrap()
    );
    assert_eq!(
        indices.get(1).unwrap().offsets,
        OffsetRange::new(0, 4).unwrap()
    );
    assert!(matches!(
        coordinator.collect(old, support::deadline()).await,
        Err(Error::Rejected {
            reason: Rejection::Pinned
        })
    ));
    coordinator
        .release_pin(pin, support::deadline())
        .await
        .unwrap();
    coordinator.collect(old, support::deadline()).await.unwrap();
    assert_cold(&coordinator, first, 2, 4).await;
    assert_cold(&coordinator, second, 0, 4).await;
    assert!(matches!(
        coordinator
            .read(first, 1, 1, 1024, support::deadline())
            .await,
        Err(Error::Rejected {
            reason: Rejection::InvalidRange
        })
    ));
    assert!(
        coordinator
            .compact_next(None, support::deadline())
            .await
            .unwrap()
            .replacement
            .is_none()
    );

    advance(&fixture, second, 1).await;
    coordinator
        .retire_before(second, support::deadline())
        .await
        .unwrap();
    let pending = coordinator
        .prepare(
            vec![range(first, 2, 4), range(second, 1, 4)],
            vec![compacted],
            support::deadline(),
        )
        .await
        .unwrap();
    let pending_id = pending.object_id();
    coordinator
        .register(&pending, support::deadline())
        .await
        .unwrap();
    assert_eq!(
        coordinator
            .diagnose(compacted, support::deadline())
            .await
            .unwrap()
            .recovery_replacement,
        Some(pending_id)
    );
    drop(pending);
    drop(coordinator);
    fixture = fixture.restart().await;
    let coordinator = fixture.coordinator_with_budget(store.clone(), budget);
    let count_before = fixture
        .metadata
        .state
        .with_application(|state| state.lifecycle().object_count())
        .await;
    assert!(
        coordinator
            .compact_next(None, support::deadline())
            .await
            .unwrap()
            .replacement
            .is_none()
    );
    assert_eq!(
        fixture
            .metadata
            .state
            .with_application(|state| state.lifecycle().object_count())
            .await,
        count_before
    );
    coordinator
        .reconcile(pending_id, support::deadline())
        .await
        .unwrap();
    coordinator
        .checkpoint(pending_id, support::deadline())
        .await
        .unwrap();
    assert_eq!(
        coordinator
            .diagnose(pending_id, support::deadline())
            .await
            .unwrap()
            .phase,
        ObjectPhase::Published
    );
    coordinator
        .collect(compacted, support::deadline())
        .await
        .unwrap();
    assert_cold(&coordinator, first, 2, 4).await;
    assert_cold(&coordinator, second, 1, 4).await;

    advance(&fixture, first, 4).await;
    coordinator
        .retire_before(first, support::deadline())
        .await
        .unwrap();
    let single = coordinator
        .compact_next(None, support::deadline())
        .await
        .unwrap()
        .replacement
        .unwrap();
    let indices = fixture
        .metadata
        .state
        .with_application(|state| {
            state
                .lifecycle()
                .object(single)
                .unwrap()
                .descriptor()
                .partitions
                .clone()
        })
        .await;
    assert_eq!(indices.len(), 1);
    assert_eq!(indices.first().unwrap().partition_id, second.partition);
    coordinator
        .collect(pending_id, support::deadline())
        .await
        .unwrap();
    assert_cold(&coordinator, second, 1, 4).await;

    advance(&fixture, second, 2).await;
    coordinator
        .retire_before(second, support::deadline())
        .await
        .unwrap();
    let stale = coordinator
        .prepare(vec![range(second, 2, 4)], vec![single], support::deadline())
        .await
        .unwrap();
    let stale_id = stale.object_id();
    coordinator
        .register(&stale, support::deadline())
        .await
        .unwrap();
    drop(stale);
    advance(&fixture, second, 4).await;
    coordinator
        .retire_before(second, support::deadline())
        .await
        .unwrap();
    assert!(matches!(
        coordinator.collect(single, support::deadline()).await,
        Err(Error::Rejected {
            reason: Rejection::RecoveryDependency
        })
    ));
    coordinator
        .reconcile(stale_id, support::deadline())
        .await
        .unwrap();
    assert_eq!(
        coordinator
            .diagnose(stale_id, support::deadline())
            .await
            .unwrap()
            .phase,
        ObjectPhase::Aborted
    );
    coordinator
        .collect(single, support::deadline())
        .await
        .unwrap();
    coordinator
        .collect(stale_id, support::deadline())
        .await
        .unwrap();
    drop(coordinator);
    fixture.close().await;
}
