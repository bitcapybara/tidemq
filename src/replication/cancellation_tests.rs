use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::{CommittedLeaderId, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, oneshot};

use super::{Application, DiskStore, LogEntry, StorageIdentity, StorageOptions};

struct Gate {
    directory: PathBuf,
    operation: &'static str,
    reached: Option<oneshot::Sender<()>>,
    release: Arc<Notify>,
}

static GATE: Mutex<Option<Gate>> = Mutex::new(None);

pub(super) async fn boundary(directory: &Path, operation: &'static str) {
    let release = {
        let mut gate = GATE.lock().unwrap();
        gate.as_mut()
            .filter(|gate| gate.directory == directory && gate.operation == operation)
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
    assert!(gate.is_none());
    *gate = Some(Gate {
        directory: directory.to_owned(),
        operation,
        reached: Some(reached),
        release: release.clone(),
    });
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
        tidemq_protocol::Id::random().unwrap()
    ));
    tokio::fs::create_dir(&directory).await.unwrap();
    for operation in ["truncate_queued", "purge_published"] {
        let path = directory.join(operation);
        let identity = StorageIdentity::new::<Ledger>(tidemq_protocol::Id::random().unwrap(), 1, 1);
        let mut storage =
            DiskStore::<Ledger>::open(&path, identity.clone(), StorageOptions::default(), None)
                .await
                .unwrap();
        storage
            .log
            .blocking_append((0..4).map(entry))
            .await
            .unwrap();
        let committed = if operation == "purge_published" { 4 } else { 2 };
        storage
            .state_machine
            .apply((0..committed).map(entry))
            .await
            .unwrap();
        if operation == "purge_published" {
            storage.state_machine.build_snapshot().await.unwrap();
        }
        let (reached, release) = hold(&path, operation);
        let mut log = storage.log.clone();
        let caller = tokio::spawn(async move {
            if operation == "truncate_queued" {
                log.truncate(log_id(2)).await
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
        let expected = if operation == "truncate_queued" {
            vec![log_id(0), log_id(1)]
        } else {
            vec![log_id(2), log_id(3)]
        };
        let memory = storage.log.try_get_log_entries(0..4).await.unwrap();
        assert_eq!(
            memory.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
            expected
        );
        storage.log.close().await.unwrap();
        drop(storage);
        *GATE.lock().unwrap() = None;
        let mut reopened =
            DiskStore::<Ledger>::open(&path, identity, StorageOptions::default(), None)
                .await
                .unwrap();
        let disk = reopened.log.try_get_log_entries(0..4).await.unwrap();
        assert_eq!(
            disk.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(reopened.state_machine.application().await.0, committed - 1);
        reopened.log.close().await.unwrap();
    }
    tokio::fs::remove_dir_all(directory).await.unwrap();
}
