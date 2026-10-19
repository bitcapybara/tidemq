#[path = "replication_support/mod.rs"]
mod support;

use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::testing::{StoreBuilder, Suite};
use openraft::{RaftLogReader, RaftSnapshotBuilder, StorageError, Vote};
use tidemq::replication::{DiskLog, DiskStateMachine, DiskStore, StorageOptions};
use tidemq_protocol::Id;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use support::{Command, Config, Directory, Response, State, entry, identity, log_id, store};

struct Builder {
    cluster: Id,
    group: u64,
}

impl StoreBuilder<Config, DiskLog<State>, DiskStateMachine<State>, Directory> for Builder {
    async fn build(
        &self,
    ) -> Result<(Directory, DiskLog<State>, DiskStateMachine<State>), StorageError<u64>> {
        let directory = Directory::new().await;
        let store = store(
            &directory.path().join("replica"),
            identity(self.cluster, self.group, 0),
        )
        .await;
        Ok((directory, store.log, store.state_machine))
    }
}

#[test]
fn official_openraft_storage_v2_suite_passes_on_real_files() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::INFO)
        .try_init();
    Suite::<Config, DiskLog<State>, DiskStateMachine<State>, Builder, Directory>::test_all(
        Builder {
            cluster: Id::random().unwrap(),
            group: 1,
        },
    )
    .unwrap();
}

#[tokio::test]
async fn vote_committed_tail_fence_offsets_and_original_results_survive_reopen() {
    let directory = Directory::new().await;
    let path = directory.path().join("replica");
    let identity = identity(Id::random().unwrap(), 1, 1);
    let mut storage = store(&path, identity.clone()).await;
    storage
        .log
        .save_vote(&Vote::new_committed(7, 1))
        .await
        .unwrap();
    let entries = vec![
        entry(7, 0, Command::Fence(9)),
        entry(
            7,
            1,
            Command::Append {
                epoch: 9,
                request: 41,
                value: 3,
            },
        ),
    ];
    storage.log.blocking_append(entries.clone()).await.unwrap();
    let responses = storage.state_machine.apply(entries).await.unwrap();
    assert_eq!(responses.last(), Some(&Response::Appended { offset: 0 }));
    assert_eq!(
        storage.log.read_committed().await.unwrap(),
        Some(log_id(7, 1))
    );
    storage.log.close().await.unwrap();
    drop(storage);
    let mut reopened = store(&path, identity).await;
    assert_eq!(
        reopened.log.read_vote().await.unwrap(),
        Some(Vote::new_committed(7, 1))
    );
    let recovered = reopened.state_machine.application().await;
    assert_eq!(recovered.writer_epoch, 9);
    assert_eq!(recovered.next_offset, 1);
    assert_eq!(recovered.results.get(&41), Some(&(0, 3)));
    let retry = entry(
        7,
        2,
        Command::Append {
            epoch: 9,
            request: 41,
            value: 3,
        },
    );
    reopened.log.blocking_append([retry.clone()]).await.unwrap();
    assert_eq!(
        reopened.state_machine.apply([retry]).await.unwrap(),
        vec![Response::Appended { offset: 0 }]
    );
    assert_eq!(reopened.state_machine.application().await.next_offset, 1);
    reopened.log.close().await.unwrap();
}

#[tokio::test]
async fn uncommitted_tail_is_not_applied_and_can_be_replaced_after_restart() {
    let directory = Directory::new().await;
    let path = directory.path().join("replica");
    let identity = identity(Id::random().unwrap(), 1, 1);
    let mut storage = store(&path, identity.clone()).await;
    storage
        .log
        .blocking_append([entry(
            1,
            0,
            Command::Append {
                epoch: 0,
                request: 1,
                value: 1,
            },
        )])
        .await
        .unwrap();
    storage.log.close().await.unwrap();
    drop(storage);
    let mut reopened = store(&path, identity).await;
    assert_eq!(reopened.state_machine.application().await.next_offset, 0);
    reopened.log.truncate(log_id(1, 0)).await.unwrap();
    let replacement = entry(
        2,
        0,
        Command::Append {
            epoch: 0,
            request: 2,
            value: 2,
        },
    );
    reopened
        .log
        .blocking_append([replacement.clone()])
        .await
        .unwrap();
    reopened.state_machine.apply([replacement]).await.unwrap();
    assert_eq!(
        reopened.state_machine.application().await.values.get(&0),
        Some(&2)
    );
    reopened.log.close().await.unwrap();
}

#[tokio::test]
async fn durable_snapshot_covers_purged_logs_and_install_validates_group_and_metadata() {
    let directory = Directory::new().await;
    let cluster = Id::random().unwrap();
    let group = 1;
    let first_identity = identity(cluster, group, 1);
    let path = directory.path().join("first");
    let mut first = store(&path, first_identity.clone()).await;
    let entries = vec![
        entry(1, 0, Command::Fence(4)),
        entry(
            1,
            1,
            Command::Append {
                epoch: 4,
                request: 1,
                value: 7,
            },
        ),
    ];
    first.log.blocking_append(entries.clone()).await.unwrap();
    first.state_machine.apply(entries).await.unwrap();
    let snapshot = first.state_machine.build_snapshot().await.unwrap();
    assert_eq!(snapshot.meta.last_log_id, Some(log_id(1, 1)));
    first.log.purge(log_id(1, 1)).await.unwrap();
    first.log.close().await.unwrap();
    drop(first);
    let reopened = store(&path, first_identity).await;
    assert_eq!(reopened.state_machine.application().await.writer_epoch, 4);
    assert_eq!(
        reopened.state_machine.application().await.results.get(&1),
        Some(&(0, 7))
    );
    assert_eq!(reopened.log.diagnostics().await.retained_log_entries, 0);
    let mut follower = store(
        &directory.path().join("follower"),
        identity(cluster, group, 2),
    )
    .await;
    follower
        .state_machine
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(
        follower.state_machine.application().await,
        reopened.state_machine.application().await
    );
    let snapshot = follower
        .state_machine
        .get_current_snapshot()
        .await
        .unwrap()
        .unwrap();
    let mut wrong_meta = snapshot.meta.clone();
    wrong_meta.last_log_id = Some(log_id(2, 99));
    assert!(
        follower
            .state_machine
            .install_snapshot(&wrong_meta, snapshot.snapshot)
            .await
            .is_err()
    );
    let snapshot = follower
        .state_machine
        .get_current_snapshot()
        .await
        .unwrap()
        .unwrap();
    let mut foreign = store(&directory.path().join("foreign"), identity(cluster, 2, 3)).await;
    assert!(
        foreign
            .state_machine
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err()
    );
    reopened.log.close().await.unwrap();
    follower.log.close().await.unwrap();
    foreign.log.close().await.unwrap();
}

#[tokio::test]
async fn writer_lock_identity_snapshot_write_limits_and_invalid_ranges_are_safe() {
    let directory = Directory::new().await;
    let path = directory.path().join("replica");
    let identity = identity(Id::random().unwrap(), 1, 1);
    let options = StorageOptions {
        max_snapshot_bytes: 1024,
        ..StorageOptions::default()
    };
    let mut storage = DiskStore::<State>::open(&path, identity.clone(), options.clone(), None)
        .await
        .unwrap();
    assert!(
        DiskStore::<State>::open(&path, identity.clone(), options.clone(), None)
            .await
            .is_err()
    );
    let mut receiver = storage
        .state_machine
        .begin_receiving_snapshot()
        .await
        .unwrap();
    assert!(receiver.write_all(&vec![0; 1113]).await.is_err());
    assert!(receiver.seek(std::io::SeekFrom::Start(1113)).await.is_err());
    assert!(
        storage
            .log
            .try_get_log_entries((std::ops::Bound::Included(5), std::ops::Bound::Excluded(3)))
            .await
            .unwrap()
            .is_empty()
    );
    storage.log.close().await.unwrap();
    drop(storage);
    let mut wrong = identity;
    wrong.node_id = 2;
    assert!(
        DiskStore::<State>::open(&path, wrong, options, None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn snapshot_corruption_after_compaction_fails_closed() {
    let directory = Directory::new().await;
    let path = directory.path().join("replica");
    let identity = identity(Id::random().unwrap(), 1, 1);
    let mut storage = store(&path, identity.clone()).await;
    let batch = vec![
        entry(1, 0, Command::Fence(1)),
        entry(
            1,
            1,
            Command::Append {
                epoch: 1,
                request: 2,
                value: 4,
            },
        ),
    ];
    storage.log.blocking_append(batch.clone()).await.unwrap();
    storage.state_machine.apply(batch).await.unwrap();
    storage.state_machine.build_snapshot().await.unwrap();
    storage.log.purge(log_id(1, 1)).await.unwrap();
    storage.log.close().await.unwrap();
    drop(storage);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(path.join("snapshot.bin"))
        .await
        .unwrap();
    file.seek(std::io::SeekFrom::Start(88)).await.unwrap();
    file.write_all(b"X").await.unwrap();
    file.sync_all().await.unwrap();
    drop(file);
    assert!(
        DiskStore::<State>::open(&path, identity, StorageOptions::default(), None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn storage_reports_real_stage_samples_capacity_and_local_state_without_copying_the_ledger() {
    use tidemq::observability::metrics::{Metrics, Operation};

    let directory = Directory::new().await;
    let metrics = Metrics::default();
    let mut storage = DiskStore::<State>::open(
        directory.path().join("replica"),
        identity(Id::random().unwrap(), 1, 1),
        StorageOptions::default(),
        metrics.operation(Operation::Control),
    )
    .await
    .unwrap();
    let entry = entry(
        1,
        0,
        Command::Append {
            epoch: 0,
            request: 1,
            value: 6,
        },
    );
    storage.log.blocking_append([entry.clone()]).await.unwrap();
    storage.state_machine.apply([entry]).await.unwrap();
    assert_eq!(
        storage
            .state_machine
            .with_application(|state| state.values.get(&0).copied())
            .await,
        Some(6)
    );
    let diagnostic = storage.log.diagnostics().await;
    assert_eq!(diagnostic.applied_log, diagnostic.committed_log);
    assert_eq!(diagnostic.retained_log_entries, 1);
    assert!(diagnostic.remaining_log_entries < StorageOptions::default().max_log_entries);
    assert!(diagnostic.journal_runtime.current_file_bytes > 0);
    let text = metrics.encode().unwrap();
    for stage in ["Admission", "Queue", "Append", "Sync", "Commit", "Apply"] {
        let count = text
            .lines()
            .find(|line| {
                line.starts_with("tidemq_stage_duration_seconds_count")
                    && line.contains("operation=\"Control\"")
                    && line.contains(&format!("stage=\"{stage}\""))
            })
            .and_then(|line| line.split_whitespace().last())
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert!(
            count > 0,
            "The storage stage records actual completed observations"
        );
    }
    storage.log.close().await.unwrap();
}

#[tokio::test]
async fn copied_journal_and_missing_identity_cannot_rebind_another_replica() {
    let directory = Directory::new().await;
    let cluster = Id::random().unwrap();
    let first_path = directory.path().join("first");
    let second_path = directory.path().join("second");
    let first = store(&first_path, identity(cluster, 1, 1)).await;
    let second_identity = identity(cluster, 2, 2);
    let second = store(&second_path, second_identity.clone()).await;
    first.log.close().await.unwrap();
    second.log.close().await.unwrap();
    drop(first);
    drop(second);
    tokio::fs::copy(first_path.join("raft.wal"), second_path.join("raft.wal"))
        .await
        .unwrap();
    assert!(
        DiskStore::<State>::open(
            &second_path,
            second_identity.clone(),
            StorageOptions::default(),
            None
        )
        .await
        .is_err()
    );
    tokio::fs::remove_file(second_path.join("identity.bin"))
        .await
        .unwrap();
    assert!(
        DiskStore::<State>::open(
            &second_path,
            second_identity,
            StorageOptions::default(),
            None
        )
        .await
        .is_err()
    );
}
