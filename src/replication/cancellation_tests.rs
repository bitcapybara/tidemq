use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::{CommittedLeaderId, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, oneshot};

use super::{Application, DiskStore, LogEntry, StorageIdentity, StorageOptions};

struct Gate {
    operation: &'static str,
    reached: Option<oneshot::Sender<()>>,
    release: Arc<Notify>,
}

static GATE: Mutex<BTreeMap<PathBuf, Gate>> = Mutex::new(BTreeMap::new());

pub(super) async fn boundary(directory: &Path, operation: &'static str) {
    let release = {
        let mut gate = GATE.lock().unwrap();
        gate.get_mut(directory)
            .filter(|gate| gate.operation == operation)
            .and_then(|gate| {
                gate.reached.take().map(|reached| {
                    reached.send(()).unwrap();
                    gate.release.clone()
                })
            })
    };
    if let Some(release) = release {
        release.notified().await;
    }
}

fn hold(directory: &Path, operation: &'static str) -> (oneshot::Receiver<()>, Arc<Notify>) {
    let (reached, receive) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let mut gate = GATE.lock().unwrap();
    let previous = gate.insert(
        directory.to_owned(),
        Gate {
            operation,
            reached: Some(reached),
            release: release.clone(),
        },
    );
    assert!(previous.is_none());
    (receive, release)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Ledger(u64);

impl Application for Ledger {
    const NAME: &'static str = "cancellation-test-ledger";
    const VERSION: u32 = 1;
    type Command = u64;
    type Response = ();

    fn apply(&mut self, command: u64) {
        self.0 = command;
    }

    fn validate(&self) -> std::io::Result<()> {
        Ok(())
    }
}

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 0), index)
}

fn entry(index: u64) -> LogEntry<Ledger> {
    LogEntry::<Ledger> {
        log_id: log_id(index),
        payload: EntryPayload::Normal(index),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_callers_after_enqueue_and_publication_keeps_memory_and_disk_consistent() {
    let directory = std::env::temp_dir().join(format!(
        "tidemq-storage-cancellation-{}",
        tidemq_protocol::Id::new_v7().unwrap()
    ));
    tokio::fs::create_dir(&directory).await.unwrap();
    for operation in ["truncate_queued", "purge_published", "snapshot_published"] {
        let path = directory.join(operation);
        let identity = StorageIdentity::new::<Ledger>(tidemq_protocol::Id::new_v7().unwrap(), 1, 1);
        let mut storage =
            DiskStore::<Ledger>::open(&path, identity.clone(), StorageOptions::default(), None)
                .await
                .unwrap();
        storage
            .log
            .blocking_append((0..4).map(entry))
            .await
            .unwrap();
        let committed = if operation == "truncate_queued" { 2 } else { 4 };
        storage
            .state_machine
            .apply((0..committed).map(entry))
            .await
            .unwrap();
        if operation != "truncate_queued" {
            storage.state_machine.build_snapshot().await.unwrap();
        }
        let old_snapshot = (operation == "snapshot_published")
            .then(|| storage.log.0.journal.current_snapshot().unwrap().file_name);
        if operation == "snapshot_published" {
            storage.log.blocking_append([entry(4)]).await.unwrap();
            storage.state_machine.apply([entry(4)]).await.unwrap();
        }
        let (reached, release) = hold(&path, operation);
        let mut log = storage.log.clone();
        let mut state_machine = storage.state_machine.clone();
        let caller = tokio::spawn(async move {
            if operation == "truncate_queued" {
                log.truncate(log_id(2)).await
            } else if operation == "snapshot_published" {
                state_machine.build_snapshot().await.map(|_| ())
            } else {
                log.purge(log_id(1)).await
            }
        });
        tokio::time::timeout(Duration::from_secs(30), reached)
            .await
            .unwrap()
            .unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        release.notify_one();
        storage.log.flush().await.unwrap();
        if operation == "snapshot_published" {
            let current = storage
                .state_machine
                .get_current_snapshot()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(current.meta.last_log_id, Some(log_id(4)));
            drop(current);
            let next = storage.state_machine.build_snapshot().await.unwrap();
            assert_eq!(next.meta.last_log_id, Some(log_id(4)));
            drop(next);
            assert!(
                !tokio::fs::try_exists(path.join(old_snapshot.unwrap()))
                    .await
                    .unwrap()
            );
            let authoritative = storage.log.0.journal.current_snapshot().unwrap();
            assert!(
                tokio::fs::try_exists(path.join(authoritative.file_name))
                    .await
                    .unwrap()
            );
        }
        let expected = if operation == "truncate_queued" {
            vec![log_id(0), log_id(1)]
        } else if operation == "snapshot_published" {
            (0..5).map(log_id).collect()
        } else {
            vec![log_id(2), log_id(3)]
        };
        let memory = storage.log.try_get_log_entries(0..5).await.unwrap();
        assert_eq!(
            memory.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
            expected
        );
        storage.log.close().await.unwrap();
        drop(storage);
        GATE.lock().unwrap().remove(&path);
        let mut reopened =
            DiskStore::<Ledger>::open(&path, identity, StorageOptions::default(), None)
                .await
                .unwrap();
        let disk = reopened.log.try_get_log_entries(0..5).await.unwrap();
        assert_eq!(
            disk.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            reopened.state_machine.application().await.0,
            if operation == "snapshot_published" {
                4
            } else {
                committed - 1
            }
        );
        reopened.log.close().await.unwrap();
    }
    tokio::fs::remove_dir_all(directory).await.unwrap();
}

mod tracing_tests;
