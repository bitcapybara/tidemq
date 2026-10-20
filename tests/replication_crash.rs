#[path = "replication_support/mod.rs"]
mod support;

use std::path::PathBuf;
use std::process::Command as ProcessCommand;
use std::time::Duration;

use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::{RaftLogReader, RaftSnapshotBuilder, Vote};
use tidemq::replication::{DiskStore, StorageOptions};
use tidemq::wal::JournalOptions;
use tidemq_protocol::Id;
use tokio::io::AsyncWriteExt;

use support::{Command, Directory, entry, identity, log_id};

fn options(mode: &str) -> StorageOptions {
    StorageOptions {
        journal: JournalOptions {
            max_record_bytes: 2048,
            max_file_bytes: 64 * 1024,
            max_pending_bytes: 4096,
            group_commit_bytes: 1024,
            segment_target_bytes: 4096,
            max_segments: 16,
            max_checkpoint_bytes: 8192,
            max_retained_records: 64,
            control_reserve_bytes: 16 * 1024,
            ..JournalOptions::default()
        },
        max_log_entries: if mode == "compaction" { 16 } else { 64 },
        max_log_bytes: 8192,
        max_snapshot_bytes: 8192,
        max_replication_batch_bytes: 1024,
        ..StorageOptions::default()
    }
}

#[tokio::test]
async fn crash_writer_child() {
    let Ok(mode) = std::env::var("TIDEMQ_STORAGE_CRASH_MODE") else {
        return;
    };
    let path = PathBuf::from(std::env::var("TIDEMQ_STORAGE_CRASH_PATH").unwrap());
    let ready = PathBuf::from(std::env::var("TIDEMQ_STORAGE_CRASH_READY").unwrap());
    let cluster: Id = std::env::var("TIDEMQ_STORAGE_CRASH_CLUSTER")
        .unwrap()
        .parse()
        .unwrap();
    let mut storage =
        DiskStore::<support::State>::open(&path, identity(cluster, 1, 1), options(&mode), None)
            .await
            .unwrap();
    storage
        .log
        .save_vote(&Vote::new_committed(8, 1))
        .await
        .unwrap();
    let batch = vec![
        entry(8, 0, Command::Fence(12)),
        entry(
            8,
            1,
            Command::Append {
                epoch: 12,
                request: 25,
                value: 33,
            },
        ),
    ];
    storage.log.blocking_append(batch.clone()).await.unwrap();
    storage.state_machine.apply(batch).await.unwrap();
    if mode == "compaction" {
        storage.state_machine.build_snapshot().await.unwrap();
        storage.log.purge(log_id(8, 1)).await.unwrap();
        for round in 0..128 {
            let entry = entry(
                8,
                round + 2,
                Command::Append {
                    epoch: 12,
                    request: round + 1000,
                    value: round,
                },
            );
            storage.log.blocking_append([entry.clone()]).await.unwrap();
            storage.state_machine.apply([entry]).await.unwrap();
            storage.state_machine.build_snapshot().await.unwrap();
            storage.log.purge(log_id(8, round + 2)).await.unwrap();
        }
        let diagnostic = storage.log.diagnostics().await;
        assert!(diagnostic.journal_runtime.written_bytes > 4096);
        assert!(diagnostic.journal_runtime.checkpoint_rewrites >= 129);
        assert!(
            diagnostic.journal_runtime.current_file_bytes < options(&mode).journal.max_file_bytes
        );
    } else if mode == "rotation" {
        for index in 2..50 {
            storage
                .log
                .blocking_append([entry(
                    9,
                    index,
                    Command::Append {
                        epoch: 12,
                        request: 1000 + index,
                        value: index,
                    },
                )])
                .await
                .unwrap();
        }
        assert!(
            support::matching_files(&path, "segment-", ".wal")
                .await
                .len()
                > 1
        );
    } else if mode == "replacement" {
        for term in 9..15 {
            storage
                .log
                .save_vote(&Vote::new_committed(term, 1))
                .await
                .unwrap();
            storage.log.truncate(log_id(term - 1, 2)).await.unwrap();
            let suffix = (2..6)
                .map(|index| {
                    entry(
                        term,
                        index,
                        Command::Append {
                            epoch: 12,
                            request: term * 100 + index,
                            value: term,
                        },
                    )
                })
                .collect::<Vec<_>>();
            storage.log.blocking_append(suffix).await.unwrap();
        }
        let suffix = storage.log.try_get_log_entries(2..6).await.unwrap();
        storage.state_machine.apply(suffix).await.unwrap();
        storage.state_machine.build_snapshot().await.unwrap();
        storage.log.purge(log_id(14, 3)).await.unwrap();
    } else if mode == "snapshot" {
        storage.state_machine.build_snapshot().await.unwrap();
        storage.log.purge(log_id(8, 1)).await.unwrap();
    } else if mode == "partial-tail" {
        let segments = support::matching_files(&path, "segment-", ".wal").await;
        assert_eq!(segments.len(), 1);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(segments.first().unwrap())
            .await
            .unwrap();
        file.write_all(b"TIDE").await.unwrap();
        file.sync_all().await.unwrap();
    } else {
        assert_eq!(mode, "committed");
    }
    tokio::fs::write(ready, b"ready").await.unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_process_recovers_committed_state_snapshot_and_incomplete_tail_without_drop() {
    let directory = Directory::new().await;
    let cluster = Id::random().unwrap();
    for mode in [
        "committed",
        "snapshot",
        "partial-tail",
        "rotation",
        "replacement",
        "compaction",
    ] {
        let path = directory.path().join(mode);
        let ready = directory.path().join(format!("{mode}.ready"));
        let log_path = directory.path().join(format!("{mode}.child.log"));
        let executable = std::env::current_exe().unwrap();
        let arguments = (
            path.clone(),
            ready.clone(),
            mode.to_owned(),
            cluster.to_string(),
            log_path.clone(),
        );
        let mut child = tokio::task::spawn_blocking(move || {
            let output = std::fs::File::create(&arguments.4).unwrap();
            ProcessCommand::new(executable)
                .args(["--exact", "crash_writer_child", "--nocapture"])
                .env("TIDEMQ_STORAGE_CRASH_PATH", arguments.0)
                .env("TIDEMQ_STORAGE_CRASH_READY", arguments.1)
                .env("TIDEMQ_STORAGE_CRASH_MODE", arguments.2)
                .env("TIDEMQ_STORAGE_CRASH_CLUSTER", arguments.3)
                .stdout(output.try_clone().unwrap())
                .stderr(output)
                .spawn()
                .unwrap()
        })
        .await
        .unwrap();
        let reached_boundary = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if tokio::fs::try_exists(&ready).await.unwrap() {
                    return true;
                }
                if child.try_wait().unwrap().is_some() {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let status = tokio::task::spawn_blocking(move || {
            let _ = child.kill();
            child.wait().unwrap()
        })
        .await
        .unwrap();
        assert!(
            matches!(reached_boundary, Ok(true)),
            "Child did not reach {mode}: {}",
            tokio::fs::read_to_string(log_path).await.unwrap()
        );
        assert!(
            !status.success(),
            "Child is killed before destructors or journal shutdown execute"
        );
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(libc::SIGKILL));
        }
        let mut recovered =
            DiskStore::<support::State>::open(&path, identity(cluster, 1, 1), options(mode), None)
                .await
                .unwrap();
        assert_eq!(
            recovered.log.read_vote().await.unwrap(),
            Some(Vote::new_committed(
                if mode == "replacement" { 14 } else { 8 },
                1
            ))
        );
        assert_eq!(
            recovered
                .state_machine
                .with_application(|state| state.writer_epoch)
                .await,
            12
        );
        assert_eq!(
            recovered
                .state_machine
                .with_application(|state| state.results.get(&25).copied())
                .await,
            Some((0, 33))
        );
        assert_eq!(
            recovered
                .state_machine
                .with_application(|state| state.next_offset)
                .await,
            match mode {
                "compaction" => 129,
                "replacement" => 5,
                _ => 1,
            }
        );
        if mode == "rotation" {
            let tail = recovered.log.try_get_log_entries(2..50).await.unwrap();
            assert_eq!(tail.len(), 48);
            assert!(tail.iter().all(|entry| entry.log_id.leader_id.term == 9));
        } else if mode == "replacement" {
            assert_eq!(
                recovered.log.read_committed().await.unwrap(),
                Some(log_id(14, 5))
            );
            let tail = recovered.log.try_get_log_entries(0..10).await.unwrap();
            assert_eq!(
                tail.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
                vec![log_id(14, 4), log_id(14, 5)]
            );
        }
        assert_eq!(
            recovered
                .log
                .diagnostics()
                .await
                .journal_recovery
                .incomplete_tail_bytes,
            if mode == "partial-tail" { 4 } else { 0 }
        );
        recovered.log.close().await.unwrap();
    }
}
