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
            cluster: Id::new_v7().unwrap(),
            group: 1,
        },
    )
    .unwrap();
}

#[tokio::test]
async fn vote_committed_tail_fence_offsets_and_original_results_survive_reopen() {
    let directory = Directory::new().await;
    let path = directory.path().join("replica");
    let identity = identity(Id::new_v7().unwrap(), 1, 1);
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
    let identity = identity(Id::new_v7().unwrap(), 1, 1);
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
async fn repeated_truncate_and_new_terms_never_resurrect_superseded_entries() {
    let directory = Directory::new().await;
    let path = directory.path().join("replica");
    let identity = identity(Id::new_v7().unwrap(), 1, 1);
    let mut storage = store(&path, identity.clone()).await;
    let prefix = vec![
        entry(1, 0, Command::Fence(9)),
        entry(
            1,
            1,
            Command::Append {
                epoch: 9,
                request: 1,
                value: 10,
            },
        ),
    ];
    storage.log.blocking_append(prefix.clone()).await.unwrap();
    storage.state_machine.apply(prefix).await.unwrap();
    for term in 2..8 {
        storage.log.truncate(log_id(term - 1, 2)).await.unwrap();
        let suffix: Vec<_> = (2..8)
            .map(|index| {
                entry(
                    term,
                    index,
                    Command::Append {
                        epoch: 9,
                        request: term * 100 + index,
                        value: term,
                    },
                )
            })
            .collect();
        storage.log.blocking_append(suffix).await.unwrap();
        storage.log.close().await.unwrap();
        drop(storage);
        storage = store(&path, identity.clone()).await;
        let recovered = storage.log.try_get_log_entries(2..8).await.unwrap();
        assert_eq!(recovered.len(), 6);
        assert!(
            recovered
                .iter()
                .all(|entry| entry.log_id.leader_id.term == term)
        );
        assert_eq!(storage.state_machine.application().await.next_offset, 1);
        assert_eq!(
            storage.log.read_committed().await.unwrap(),
            Some(log_id(1, 1))
        );
    }
    let final_suffix = storage.log.try_get_log_entries(2..8).await.unwrap();
    storage.state_machine.apply(final_suffix).await.unwrap();
    storage.state_machine.build_snapshot().await.unwrap();
    storage.log.purge(log_id(7, 4)).await.unwrap();
    storage.log.close().await.unwrap();
    drop(storage);
    let mut reopened = store(&path, identity).await;
    let remaining = reopened.log.try_get_log_entries(0..10).await.unwrap();
    assert_eq!(
        remaining
            .iter()
            .map(|entry| entry.log_id)
            .collect::<Vec<_>>(),
        (5..8).map(|index| log_id(7, index)).collect::<Vec<_>>()
    );
    let application = reopened.state_machine.application().await;
    assert_eq!(application.next_offset, 7);
    assert_eq!(application.results.len(), 7);
    assert!(
        application
            .results
            .keys()
            .all(|request| *request == 1 || (702..708).contains(request))
    );
    reopened.log.close().await.unwrap();
}

#[tokio::test]
async fn new_store_publishes_only_the_segmented_layout() {
    let directory = Directory::new().await;
    let path = directory.path().join("replica");
    let storage = store(&path, identity(Id::new_v7().unwrap(), 1, 1)).await;
    storage.log.close().await.unwrap();
    drop(storage);
    assert!(
        tokio::fs::try_exists(path.join("manifest.bin"))
            .await
            .unwrap()
    );
    assert_eq!(
        support::matching_files(&path, "segment-", ".wal")
            .await
            .len(),
        1
    );
    assert!(!tokio::fs::try_exists(path.join("raft.wal")).await.unwrap());
    assert!(
        !tokio::fs::try_exists(path.join("snapshot.bin"))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn purging_old_segments_preserves_control_state_retained_entries_and_original_results() {
    let directory = Directory::new().await;
    let path = directory.path().join("replica");
    let identity = identity(Id::new_v7().unwrap(), 1, 1);
    let options = StorageOptions {
        journal: tidemq::wal::JournalOptions {
            max_record_bytes: 512,
            max_file_bytes: 128 * 1024,
            max_pending_bytes: 4096,
            group_commit_bytes: 512,
            segment_target_bytes: 2048,
            max_segments: 64,
            max_checkpoint_bytes: 8192,
            max_retained_records: 64,
            control_reserve_bytes: 16 * 1024,
            ..tidemq::wal::JournalOptions::default()
        },
        max_log_entries: 64,
        max_log_bytes: 8192,
        max_snapshot_bytes: 8192,
        max_replication_batch_bytes: 512,
        ..StorageOptions::default()
    };
    let mut storage = DiskStore::<State>::open(&path, identity.clone(), options.clone(), None)
        .await
        .unwrap();
    storage
        .log
        .save_vote(&Vote::new_committed(2, 1))
        .await
        .unwrap();
    for index in 0..33 {
        let command = if index == 0 {
            Command::Fence(5)
        } else {
            Command::Append {
                epoch: 5,
                request: index,
                value: index * 10,
            }
        };
        let entry = entry(2, index, command);
        storage.log.blocking_append([entry.clone()]).await.unwrap();
        storage.state_machine.apply([entry]).await.unwrap();
    }
    storage.state_machine.build_snapshot().await.unwrap();
    let before = support::directory_contents(&path).await;
    storage.log.purge(log_id(2, 17)).await.unwrap();
    let after = support::directory_contents(&path).await;
    assert!(before.keys().any(
        |file| file.extension().is_some_and(|extension| extension == "wal")
            && !after.contains_key(file)
    ));
    for (file, bytes) in &after {
        if file.extension().is_some_and(|extension| extension == "wal")
            && let Some(original) = before.get(file)
        {
            assert_eq!(original, bytes);
        }
    }
    storage.log.close().await.unwrap();
    drop(storage);
    let mut reopened = DiskStore::<State>::open(&path, identity, options, None)
        .await
        .unwrap();
    assert_eq!(
        reopened.log.read_vote().await.unwrap(),
        Some(Vote::new_committed(2, 1))
    );
    assert_eq!(
        reopened.log.read_committed().await.unwrap(),
        Some(log_id(2, 32))
    );
    assert_eq!(
        reopened.log.diagnostics().await.purged_log,
        Some(log_id(2, 17))
    );
    let entries = reopened.log.try_get_log_entries(0..40).await.unwrap();
    assert_eq!(
        entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
        (18..33).map(|index| log_id(2, index)).collect::<Vec<_>>()
    );
    let application = reopened.state_machine.application().await;
    assert_eq!(application.writer_epoch, 5);
    assert_eq!(application.next_offset, 32);
    assert_eq!(application.results.get(&1), Some(&(0, 10)));
    assert_eq!(application.results.get(&32), Some(&(31, 320)));
    reopened.log.close().await.unwrap();
}

#[tokio::test]
async fn durable_snapshot_covers_purged_logs_and_install_validates_group_and_metadata() {
    let directory = Directory::new().await;
    let cluster = Id::new_v7().unwrap();
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
    first.log.close().await.unwrap();
    drop(first);
    let reopened = store(&path, first_identity).await;
    assert_eq!(reopened.state_machine.application().await.writer_epoch, 4);
    assert_eq!(
        reopened.state_machine.application().await.results.get(&1),
        Some(&(0, 7))
    );
    assert_eq!(reopened.log.diagnostics().await.retained_log_entries, 0);
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
async fn snapshot_readers_pin_retired_files_and_the_directory_until_they_are_dropped() {
    let directory = Directory::new().await;
    let path = directory.path().join("replica");
    let identity = identity(Id::new_v7().unwrap(), 1, 1);
    let mut storage = store(&path, identity.clone()).await;
    let first_entry = entry(1, 0, Command::Fence(2));
    storage
        .log
        .blocking_append([first_entry.clone()])
        .await
        .unwrap();
    storage.state_machine.apply([first_entry]).await.unwrap();
    let first = storage.state_machine.build_snapshot().await.unwrap();
    let first_files = support::matching_files(&path, "snapshot-", ".bin").await;
    assert_eq!(first_files.len(), 1);
    let first_path = first_files.first().unwrap();
    let first_bytes = tokio::fs::metadata(first_path).await.unwrap().len();
    let second_entry = entry(
        1,
        1,
        Command::Append {
            epoch: 2,
            request: 1,
            value: 10,
        },
    );
    storage
        .log
        .blocking_append([second_entry.clone()])
        .await
        .unwrap();
    storage.state_machine.apply([second_entry]).await.unwrap();
    let second = storage.state_machine.build_snapshot().await.unwrap();
    let diagnostic = storage.log.diagnostics().await;
    assert_eq!(diagnostic.snapshots.snapshot_files, 2);
    assert_eq!(diagnostic.snapshots.snapshot_retired_bytes, first_bytes);
    assert_eq!(diagnostic.snapshots.snapshot_readers, 2);
    assert!(diagnostic.snapshots.snapshot_file_bytes > first_bytes);
    assert!(tokio::fs::try_exists(first_path).await.unwrap());
    drop(first);
    let third_entry = entry(
        1,
        2,
        Command::Append {
            epoch: 2,
            request: 2,
            value: 11,
        },
    );
    storage
        .log
        .blocking_append([third_entry.clone()])
        .await
        .unwrap();
    storage.state_machine.apply([third_entry]).await.unwrap();
    let third = storage.state_machine.build_snapshot().await.unwrap();
    assert!(!tokio::fs::try_exists(first_path).await.unwrap());
    assert_eq!(storage.log.diagnostics().await.snapshots.snapshot_files, 2);
    storage.log.close().await.unwrap();
    drop(storage);
    assert!(
        DiskStore::<State>::open(&path, identity.clone(), StorageOptions::default(), None)
            .await
            .is_err()
    );
    drop(second);
    drop(third);
    let reopened = store(&path, identity).await;
    assert_eq!(reopened.state_machine.application().await.next_offset, 2);
    reopened.log.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_reader_limit_rejects_new_handles_without_invalidating_existing_readers() {
    let directory = Directory::new().await;
    let options = StorageOptions {
        max_snapshot_readers: 2,
        ..StorageOptions::default()
    };
    let mut storage = DiskStore::<State>::open(
        directory.path().join("replica"),
        identity(Id::new_v7().unwrap(), 1, 1),
        options,
        None,
    )
    .await
    .unwrap();
    let entry = entry(1, 0, Command::Fence(1));
    storage.log.blocking_append([entry.clone()]).await.unwrap();
    storage.state_machine.apply([entry]).await.unwrap();
    let first = storage.state_machine.build_snapshot().await.unwrap();
    let second = storage
        .state_machine
        .get_current_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert!(storage.state_machine.get_current_snapshot().await.is_err());
    assert_eq!(
        storage.log.diagnostics().await.snapshots.snapshot_readers,
        2
    );
    drop(second);
    let recovered = storage
        .state_machine
        .get_current_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.meta, recovered.meta);
    drop(first);
    drop(recovered);
    storage.log.close().await.unwrap();
}

#[tokio::test]
async fn writer_lock_identity_snapshot_write_limits_and_invalid_ranges_are_safe() {
    let directory = Directory::new().await;
    let path = directory.path().join("replica");
    let identity = identity(Id::new_v7().unwrap(), 1, 1);
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
    let identity = identity(Id::new_v7().unwrap(), 1, 1);
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
    let snapshots = support::matching_files(&path, "snapshot-", ".bin").await;
    assert_eq!(snapshots.len(), 1);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(snapshots.first().unwrap())
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
        identity(Id::new_v7().unwrap(), 1, 1),
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
    let count = |stage: &str| {
        text.lines()
            .find(|line| {
                line.starts_with("tidemq_stage_duration_seconds_count")
                    && line.contains("operation=\"Control\"")
                    && line.contains(&format!("stage=\"{stage}\""))
            })
            .and_then(|line| line.split_whitespace().last())
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    for stage in [
        "WalAdmission",
        "WalQueue",
        "Append",
        "Sync",
        "CommitPersist",
        "Apply",
    ] {
        assert!(
            count(stage) > 0,
            "The storage stage records actual completed observations"
        );
    }
    for stage in ["Admission", "Queue", "Commit", "RepairPace"] {
        assert_eq!(
            count(stage),
            0,
            "Storage does not mix unrelated stage samples"
        );
    }
    storage.log.close().await.unwrap();
}

#[tokio::test]
async fn copied_journal_and_missing_identity_cannot_rebind_another_replica() {
    let directory = Directory::new().await;
    let cluster = Id::new_v7().unwrap();
    let first_path = directory.path().join("first");
    let second_path = directory.path().join("second");
    let first = store(&first_path, identity(cluster, 1, 1)).await;
    let second_identity = identity(cluster, 2, 2);
    let second = store(&second_path, second_identity.clone()).await;
    first.log.close().await.unwrap();
    second.log.close().await.unwrap();
    drop(first);
    drop(second);
    let first_segments = support::matching_files(&first_path, "segment-", ".wal").await;
    let second_segments = support::matching_files(&second_path, "segment-", ".wal").await;
    assert_eq!(first_segments.len(), 1);
    assert_eq!(second_segments.len(), 1);
    tokio::fs::copy(
        first_segments.first().unwrap(),
        second_segments.first().unwrap(),
    )
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
