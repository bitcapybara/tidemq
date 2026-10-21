#[path = "replication_support/mod.rs"]
mod support;

use std::sync::atomic::{AtomicBool, Ordering};

use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::{Entry, EntryPayload, RaftSnapshotBuilder};
use serde::{Deserialize, Serialize};
use tidemq::replication::{Application, DiskStore, StorageIdentity, StorageOptions};
use tidemq_protocol::Id;

static FAIL_NEXT_APPLY: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct State {
    value: u64,
}

impl Application for State {
    const NAME: &'static str = "local-failure-test";
    const VERSION: u32 = 1;
    type Command = u64;
    type Response = u64;
    fn apply(&mut self, command: u64) -> u64 {
        self.value = command;
        command
    }
    fn try_apply(&mut self, command: u64) -> std::io::Result<u64> {
        if FAIL_NEXT_APPLY.swap(false, Ordering::AcqRel) {
            self.value = 999;
            return Err(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "Injected local allocation failure",
            ));
        }
        Ok(self.apply(command))
    }
    fn validate(&self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn local_apply_failure_stops_authority_and_snapshots_then_replays_the_committed_entry() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("replica");
    let identity = StorageIdentity::new::<State>(Id::new_v7().unwrap(), 2, 1);
    let mut disk =
        DiskStore::<State>::open(&path, identity.clone(), StorageOptions::default(), None)
            .await
            .unwrap();
    let entry = |index, value| Entry {
        log_id: support::log_id(1, index),
        payload: EntryPayload::Normal(value),
    };
    let stable = entry(0, 1);
    disk.log.blocking_append([stable.clone()]).await.unwrap();
    assert_eq!(
        disk.state_machine.apply([stable.clone()]).await.unwrap(),
        vec![1]
    );
    let snapshot = disk.state_machine.build_snapshot().await.unwrap();
    let snapshots = support::matching_files(&path, "snapshot-", ".bin").await;
    assert_eq!(snapshots.len(), 1);
    let stable_path = snapshots.first().unwrap();
    let stable_bytes = tokio::fs::read(stable_path).await.unwrap();
    disk.log.purge(stable.log_id).await.unwrap();
    let failed = entry(1, 7);
    disk.log.blocking_append([failed.clone()]).await.unwrap();
    FAIL_NEXT_APPLY.store(true, Ordering::Release);
    assert!(disk.state_machine.apply([failed.clone()]).await.is_err());
    let diagnostics = disk.log.diagnostics().await;
    assert!(diagnostics.application_failed);
    assert_eq!(diagnostics.committed_log, Some(failed.log_id));
    assert_eq!(diagnostics.applied_log, Some(stable.log_id));
    assert!(
        disk.state_machine
            .try_with_application(|state| state.value)
            .await
            .is_err()
    );
    assert!(
        disk.state_machine
            .try_with_applied(|_, position| position)
            .await
            .is_err()
    );
    assert!(
        disk.state_machine
            .try_with_membership(|_, position, membership| (position, membership.clone()))
            .await
            .is_err()
    );
    assert!(disk.state_machine.applied_state().await.is_err());
    assert!(disk.state_machine.build_snapshot().await.is_err());
    assert!(disk.state_machine.begin_receiving_snapshot().await.is_err());
    assert!(disk.state_machine.apply([entry(2, 8)]).await.is_err());
    assert_eq!(disk.state_machine.application().await.value, 999);
    assert_eq!(tokio::fs::read(stable_path).await.unwrap(), stable_bytes);
    drop(snapshot);
    disk.log.close().await.unwrap();
    drop(disk);
    let reopened = DiskStore::<State>::open(&path, identity, StorageOptions::default(), None)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .state_machine
            .try_with_application(|state| state.value)
            .await
            .unwrap(),
        7
    );
    assert_eq!(
        reopened.log.diagnostics().await.applied_log,
        Some(failed.log_id)
    );
    assert!(!reopened.log.diagnostics().await.application_failed);
    reopened.log.close().await.unwrap();
}
