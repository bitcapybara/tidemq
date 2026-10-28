use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use super::storage::lock;
use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct State {
    value: u64,
    ack: u64,
    operation: u64,
    applications: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Command {
    Set {
        operation: u64,
        value: u64,
        padding: Vec<u8>,
    },
    Ack {
        operation: u64,
        offset: u64,
    },
    Fail,
    Panic,
}

impl Application for State {
    const NAME: &'static str = "local-engine-test";
    const VERSION: u32 = 1;
    type Command = Command;
    type Response = (u64, u64);

    fn apply(&mut self, command: Command) -> Self::Response {
        match command {
            Command::Set {
                operation, value, ..
            } if operation > self.operation => {
                self.value = value;
                self.operation = operation;
                self.applications += 1;
            }
            Command::Ack { operation, offset } if operation > self.operation => {
                self.ack = offset;
                self.operation = operation;
                self.applications += 1;
            }
            _ => {}
        }
        (self.value, self.ack)
    }

    fn try_apply(&mut self, command: Command) -> std::io::Result<Self::Response> {
        match command {
            Command::Fail => {
                self.value = 999;
                Err(std::io::Error::other("Injected apply failure"))
            }
            Command::Panic => panic!("Injected unexpected executor panic"),
            command => Ok(self.apply(command)),
        }
    }

    fn validate(&self) -> std::io::Result<()> {
        if self.value == 999 {
            return Err(std::io::Error::other("Invalid snapshot state"));
        }
        Ok(())
    }
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "tidemq-local-engine-{}",
            tidemq_protocol::Id::new_v7().unwrap()
        )))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn identity() -> StorageIdentity {
    StorageIdentity::new::<State>(tidemq_protocol::Id::new_v7().unwrap(), 7, 1)
}

fn options() -> LocalOptions {
    LocalOptions {
        journal: crate::wal::JournalOptions {
            max_record_bytes: 1024,
            max_file_bytes: 64 * 1024,
            max_pending_bytes: 16 * 1024,
            max_pending_records: 16,
            group_commit_bytes: 4096,
            group_commit_delay: Duration::from_millis(2),
            segment_target_bytes: 4096,
            max_segments: 8,
            max_checkpoint_bytes: 1024,
            max_retained_records: 16,
            control_reserve_bytes: 8 * 1024,
        },
        max_snapshot_bytes: 4096,
        max_snapshot_files: 3,
        max_snapshot_readers: 4,
        metrics: None,
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn command(operation: u64) -> Command {
    Command::Set {
        operation,
        value: operation,
        padding: vec![42; 300],
    }
}

struct Hook {
    stage: &'static str,
    fail: bool,
    fired: AtomicBool,
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    condition: Condvar,
}
static HOOKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Hook>>>> = OnceLock::new();

struct HookGuard {
    directory: PathBuf,
    hook: Arc<Hook>,
}
impl HookGuard {
    fn new(directory: &Path, stage: &'static str, fail: bool) -> Self {
        let hook = Arc::new(Hook {
            stage,
            fail,
            fired: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            condition: Condvar::new(),
        });
        lock(HOOKS.get_or_init(Mutex::default)).insert(directory.to_owned(), hook.clone());
        Self {
            directory: directory.to_owned(),
            hook,
        }
    }
    async fn entered(&self) {
        tokio::time::timeout_at(deadline(), self.hook.entered.notified())
            .await
            .unwrap();
    }
    fn release(&self) {
        *lock(&self.hook.released) = true;
        self.hook.condition.notify_all();
    }
}
impl Drop for HookGuard {
    fn drop(&mut self) {
        self.release();
        lock(HOOKS.get_or_init(Mutex::default)).remove(&self.directory);
    }
}

pub(super) fn boundary(directory: &Path, stage: &'static str) -> Result<()> {
    if std::env::var("TIDEMQ_LOCAL_TEST_PHASE").ok().as_deref() == Some(stage)
        && std::env::var_os("TIDEMQ_LOCAL_TEST_PATH")
            .is_some_and(|path| Path::new(&path) == directory)
        && let Some(parent) = directory.parent()
        && parent.join("armed").is_file()
    {
        std::fs::write(parent.join("ready"), stage).unwrap();
        loop {
            std::thread::park();
        }
    }
    let hook = lock(HOOKS.get_or_init(Mutex::default))
        .get(directory)
        .cloned();
    if let Some(hook) = hook
        && hook.stage == stage
        && !hook.fired.swap(true, Ordering::AcqRel)
    {
        hook.entered.notify_one();
        if hook.fail {
            return Err(Error::Storage {
                source: crate::wal::Error::Io {
                    path: directory.to_owned(),
                    source: std::io::Error::other("Injected local storage failure"),
                },
            });
        }
        let released = lock(&hook.released);
        drop(
            hook.condition
                .wait_while(released, |released| !*released)
                .unwrap(),
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_order_read_after_write_snapshot_and_reopen() {
    let directory = Directory::new();
    let identity = identity();
    let group = LocalGroup::open(&directory.0, identity.clone(), options(), State::default())
        .await
        .unwrap();
    let handle = GroupHandle::Local(group.clone());
    assert!(handle.is_local());
    assert!(handle.raft().is_none());
    assert_eq!(handle.local_node(), 1);
    assert_eq!(handle.state().storage_identity(), &identity);
    for operation in 1..=20 {
        let applied = handle.commit(command(operation), deadline()).await.unwrap();
        assert_eq!(
            applied.position,
            ExecutionPosition::Local {
                sequence: operation
            }
        );
        assert_eq!(applied.response, (operation, 0));
        handle.ensure_read(deadline()).await.unwrap();
        assert_eq!(
            handle
                .state()
                .try_with_application(|a| a.value)
                .await
                .unwrap(),
            operation
        );
    }
    let snapshot = handle.snapshot(deadline()).await.unwrap().unwrap();
    assert_eq!(snapshot.position, ExecutionPosition::Local { sequence: 20 });
    let expected = group.try_with_application(Clone::clone).await.unwrap();
    handle.close(deadline()).await.unwrap();
    let reopened = LocalGroup::open(&directory.0, identity, options(), State::default())
        .await
        .unwrap();
    assert_eq!(
        reopened.try_with_application(Clone::clone).await.unwrap(),
        expected
    );
    assert_eq!(
        reopened
            .submit(command(21), deadline())
            .await
            .unwrap()
            .position,
        ExecutionPosition::Local { sequence: 21 }
    );
    reopened.drain(deadline()).await.unwrap();
}

async fn wait_for(group: &LocalGroup<State>, predicate: impl Fn(&LocalStatistics) -> bool) {
    tokio::time::timeout_at(deadline(), async {
        while !predicate(&group.statistics()) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_cancel_wins_and_releases_command_and_byte_permits() {
    let directory = Directory::new();
    let group = LocalGroup::open(&directory.0, identity(), options(), State::default())
        .await
        .unwrap();
    let hook = HookGuard::new(&directory.0, "before_handoff", false);
    let client = group.clone();
    let submit = tokio::spawn(async move { client.submit(command(1), deadline()).await });
    hook.entered().await;
    submit.abort();
    let _ = submit.await;
    assert_eq!(group.statistics().available_commands, 16);
    assert_eq!(group.statistics().available_bytes, 16 * 1024);
    hook.release();
    group.drain(deadline()).await.unwrap();
    assert_eq!(group.applied(), None);
    assert_eq!(group.statistics().journal.written_records, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_deadline_is_not_submitted_and_handoff_deadline_is_unknown() {
    for (stage, handed_off) in [("before_handoff", false), ("before_durable", true)] {
        let directory = Directory::new();
        let group = LocalGroup::open(&directory.0, identity(), options(), State::default())
            .await
            .unwrap();
        let hook = HookGuard::new(&directory.0, stage, false);
        let client = group.clone();
        let submit = tokio::spawn(async move {
            client
                .submit(command(1), Instant::now() + Duration::from_millis(100))
                .await
        });
        hook.entered().await;
        let result = submit.await.unwrap();
        assert_eq!(matches!(result, Err(Error::Unknown { .. })), handed_off);
        assert_eq!(
            matches!(result, Err(Error::NotSubmitted { .. })),
            !handed_off
        );
        assert_eq!(
            group.statistics().available_commands,
            if handed_off { 15 } else { 16 }
        );
        hook.release();
        if handed_off {
            wait_for(&group, |stats| {
                stats.applied_sequence == 1 && stats.available_commands == 16
            })
            .await;
        }
        group.drain(deadline()).await.unwrap();
        assert_eq!(group.statistics().applied_sequence, u64::from(handed_off));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_handed_off_waiter_keeps_its_budget_until_apply() {
    let directory = Directory::new();
    let group = LocalGroup::open(&directory.0, identity(), options(), State::default())
        .await
        .unwrap();
    let hook = HookGuard::new(&directory.0, "after_durable", false);
    let client = group.clone();
    let submit = tokio::spawn(async move { client.submit(command(1), deadline()).await });
    hook.entered().await;
    submit.abort();
    let _ = submit.await;
    assert_eq!(group.statistics().available_commands, 15);
    assert_eq!(group.statistics().durable_sequence, 1);
    assert_eq!(group.applied(), None);
    hook.release();
    wait_for(&group, |s| {
        s.applied_sequence == 1 && s.available_commands == 16
    })
    .await;
    assert_eq!(group.try_with_application(|a| a.value).await.unwrap(), 1);
    group.drain(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completion_admission_is_reserved_and_batches_share_durability() {
    let directory = Directory::new();
    let mut configured = options();
    configured.journal.max_retained_records = 256;
    let group = LocalGroup::open(&directory.0, identity(), configured, State::default())
        .await
        .unwrap();
    let hook = HookGuard::new(&directory.0, "before_durable", false);
    let client = group.clone();
    let first = tokio::spawn(async move { client.submit(command(1), deadline()).await });
    hook.entered().await;
    let mut tasks = Vec::new();
    for operation in 2..=12 {
        let client = group.clone();
        tasks.push(tokio::spawn(async move {
            client.submit(command(operation), deadline()).await
        }));
    }
    wait_for(&group, |s| s.available_commands == 4).await;
    assert!(matches!(
        group.submit(command(13), deadline()).await,
        Err(Error::NotSubmitted { .. })
    ));
    let client = group.clone();
    let ack = tokio::spawn(async move {
        client
            .submit_completion(
                Command::Ack {
                    operation: 13,
                    offset: 12,
                },
                deadline(),
            )
            .await
    });
    wait_for(&group, |s| s.available_commands == 3).await;
    hook.release();
    first.await.unwrap().unwrap();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(ack.await.unwrap().unwrap().response.1, 12);
    let stats = group.statistics();
    assert!(
        stats.journal.sync_operations < stats.journal.written_records,
        "Concurrent commands must use the existing WAL group commit"
    );
    assert_eq!(stats.journal.written_records, 13);
    group.drain(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_maintenance_crosses_three_capacities_and_restores_ack_and_operation_identity() {
    let directory = Directory::new();
    let identity = identity();
    let configured = options();
    let capacity = configured.journal.max_file_bytes;
    let limit = configured.file_limit_bytes().unwrap();
    let group = LocalGroup::open(
        &directory.0,
        identity.clone(),
        configured.clone(),
        State::default(),
    )
    .await
    .unwrap();
    let mut last = 0;
    for operation in 1..=900 {
        let command = if operation % 2 == 0 {
            Command::Ack {
                operation,
                offset: operation - 1,
            }
        } else {
            command(operation)
        };
        group.submit_completion(command, deadline()).await.unwrap();
        last = operation;
        let stats = group.statistics();
        assert!(stats.journal.current_file_bytes <= capacity);
        if stats.journal.written_bytes > capacity * 3 && stats.maintenance_runs >= 3 {
            break;
        }
    }
    let stats = group.statistics();
    assert!(
        stats.journal.written_bytes > capacity * 3,
        "Cumulative command bytes must exceed three complete WAL capacities"
    );
    assert!(stats.maintenance_runs >= 3);
    assert!(stats.journal.checkpoint_rewrites >= 3);
    assert!(stats.journal.reclaimed_bytes > capacity * 2);
    let expected = group.try_with_application(Clone::clone).await.unwrap();
    assert_eq!(expected.operation, last);
    assert_eq!(expected.applications, last);
    group.drain(deadline()).await.unwrap();
    let mut actual_bytes = 0;
    for entry in std::fs::read_dir(&directory.0).unwrap() {
        actual_bytes += entry.unwrap().metadata().unwrap().len();
    }
    assert!(actual_bytes <= limit);
    let reopened = LocalGroup::open(&directory.0, identity, configured, State::default())
        .await
        .unwrap();
    assert_eq!(
        reopened.try_with_application(Clone::clone).await.unwrap(),
        expected
    );
    let duplicate = Command::Ack {
        operation: last,
        offset: 0,
    };
    reopened
        .submit_completion(duplicate, deadline())
        .await
        .unwrap();
    assert_eq!(
        reopened.try_with_application(Clone::clone).await.unwrap(),
        expected
    );
    assert_eq!(
        reopened.applied(),
        Some(ExecutionPosition::Local { sequence: last + 1 })
    );
    reopened.drain(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pinned_generations_backpressure_then_maintain_without_another_append() {
    let directory = Directory::new();
    let group = LocalGroup::open(&directory.0, identity(), options(), State::default())
        .await
        .unwrap();
    let mut readers = Vec::new();
    let mut operation = 0;
    for generation in 1..=3 {
        for _ in 0..8 {
            operation += 1;
            group.submit(command(operation), deadline()).await.unwrap();
        }
        wait_for(&group, |s| s.snapshot_sequence >= generation * 8).await;
        readers.push(group.snapshot_reader().await.unwrap().unwrap());
    }
    let mut rejected = false;
    for _ in 0..20 {
        operation += 1;
        match group.submit(command(operation), deadline()).await {
            Ok(_) => {}
            Err(Error::NotSubmitted { .. }) => {
                rejected = true;
                break;
            }
            error => panic!("Unexpected submission result: {error:?}"),
        }
    }
    assert!(rejected);
    let applied = group.statistics().applied_sequence;
    let snapshot = group.statistics().snapshot_sequence;
    assert!(snapshot < applied);
    for reader in &readers {
        assert!(directory.0.join(&reader.info.snapshot_id).is_file());
    }
    // A small ACK can still consume the retained-record reserve after production backpressure.
    group
        .submit_completion(
            Command::Ack {
                operation,
                offset: operation - 1,
            },
            deadline(),
        )
        .await
        .unwrap();
    let final_applied = group.statistics().applied_sequence;
    drop(readers);
    wait_for(&group, |s| {
        s.snapshot_sequence == final_applied && !s.maintenance_in_progress
    })
    .await;
    assert!(group.statistics().journal.retained_records < options().journal.max_retained_records);
    assert_eq!(*group.health().borrow(), LocalHealth::Ready);
    group.drain(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_failure_and_unexpected_panic_seal_authoritative_reads_and_later_commands() {
    for fault in [Command::Fail, Command::Panic] {
        let directory = Directory::new();
        let group = LocalGroup::open(&directory.0, identity(), options(), State::default())
            .await
            .unwrap();
        group.submit(command(1), deadline()).await.unwrap();
        assert!(group.submit(fault, deadline()).await.is_err());
        let mut health = group.health();
        tokio::time::timeout_at(deadline(), async {
            while !matches!(*health.borrow_and_update(), LocalHealth::Failed { .. }) {
                health.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            group.try_with_application(|a| a.value).await,
            Err(Error::Fatal { .. })
        ));
        assert!(matches!(
            group.submit(command(3), deadline()).await,
            Err(Error::Fatal { .. })
        ));
        assert!(group.drain(deadline()).await.is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_format_sequence_and_corruption_are_rejected_without_cleaning_evidence() {
    let directory = Directory::new();
    let original = identity();
    let group = LocalGroup::open(&directory.0, original.clone(), options(), State::default())
        .await
        .unwrap();
    group.submit(command(1), deadline()).await.unwrap();
    group.drain(deadline()).await.unwrap();
    let mut wrong = original.clone();
    wrong.node_id = 2;
    assert!(
        LocalGroup::open(&directory.0, wrong, options(), State::default())
            .await
            .is_err()
    );
    let mut wrong = original.clone();
    wrong.group.group_id = 8;
    assert!(
        LocalGroup::open(&directory.0, wrong, options(), State::default())
            .await
            .is_err()
    );
    let mut wrong = original.clone();
    wrong.group.application_version += 1;
    assert!(
        LocalGroup::open(&directory.0, wrong, options(), State::default())
            .await
            .is_err()
    );
    let reopened = LocalGroup::open(&directory.0, original, options(), State::default())
        .await
        .unwrap();
    assert_eq!(
        reopened.applied(),
        Some(ExecutionPosition::Local { sequence: 1 })
    );
    reopened.drain(deadline()).await.unwrap();
    assert!(directory.0.join("identity.bin").is_file());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_reader_keeps_directory_owned_until_it_is_dropped() {
    let directory = Directory::new();
    let identity = identity();
    let group = LocalGroup::open(&directory.0, identity.clone(), options(), State::default())
        .await
        .unwrap();
    group.submit(command(1), deadline()).await.unwrap();
    group.snapshot(deadline()).await.unwrap();
    let reader = group.snapshot_reader().await.unwrap().unwrap();
    group.drain(deadline()).await.unwrap();
    assert!(
        LocalGroup::open(&directory.0, identity.clone(), options(), State::default())
            .await
            .is_err()
    );
    drop(reader);
    let reopened = LocalGroup::open(&directory.0, identity, options(), State::default())
        .await
        .unwrap();
    reopened.drain(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canceled_snapshot_io_keeps_its_reader_lease_and_directory_lock() {
    let directory = Directory::new();
    let identity = identity();
    let group = LocalGroup::open(&directory.0, identity.clone(), options(), State::default())
        .await
        .unwrap();
    group.submit(command(1), deadline()).await.unwrap();
    group.snapshot(deadline()).await.unwrap();
    let reader = Arc::new(group.snapshot_reader().await.unwrap().unwrap());
    let hook = HookGuard::new(&directory.0, "snapshot_read", false);
    let read = reader.clone();
    let operation = tokio::spawn(async move { read.read_chunk(0, 256).await });
    hook.entered().await;
    operation.abort();
    let _ = operation.await;
    assert!(matches!(
        reader.read_chunk(0, 256).await,
        Err(Error::NotSubmitted { .. })
    ));
    drop(reader);
    group.drain(deadline()).await.unwrap();
    assert!(
        LocalGroup::open(&directory.0, identity.clone(), options(), State::default())
            .await
            .is_err()
    );
    hook.release();
    let reopened = tokio::time::timeout_at(deadline(), async {
        loop {
            if let Ok(group) =
                LocalGroup::open(&directory.0, identity.clone(), options(), State::default()).await
            {
                break group;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    reopened.drain(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_deadline_is_bounded_and_handed_off_work_is_recoverable() {
    let directory = Directory::new();
    let identity = identity();
    let group = LocalGroup::open(&directory.0, identity.clone(), options(), State::default())
        .await
        .unwrap();
    let hook = HookGuard::new(&directory.0, "after_durable", false);
    let client = group.clone();
    let submit = tokio::spawn(async move { client.submit(command(1), deadline()).await });
    hook.entered().await;
    assert!(matches!(
        group
            .drain(Instant::now() + Duration::from_millis(10))
            .await,
        Err(Error::Unknown { .. })
    ));
    assert!(matches!(
        *group.health().borrow(),
        LocalHealth::Failed { .. }
    ));
    hook.release();
    let _ = submit.await.unwrap();
    assert!(group.drain(deadline()).await.is_err());
    let reopened = LocalGroup::open(&directory.0, identity, options(), State::default())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .try_with_application(|app| app.value)
            .await
            .unwrap(),
        1
    );
    reopened.drain(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checksum_corruption_is_fatal_while_an_incomplete_active_tail_is_repaired() {
    use std::io::{Read, Seek, Write};
    for incomplete in [true, false] {
        let directory = Directory::new();
        let identity = identity();
        let group = LocalGroup::open(&directory.0, identity.clone(), options(), State::default())
            .await
            .unwrap();
        group.submit(command(1), deadline()).await.unwrap();
        group.drain(deadline()).await.unwrap();
        let segment = std::fs::read_dir(&directory.0)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().is_some_and(|extension| extension == "wal"))
            .unwrap();
        if incomplete {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&segment)
                .unwrap();
            file.write_all(&[1; 40]).unwrap();
            file.sync_all().unwrap();
        } else {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&segment)
                .unwrap();
            file.seek(std::io::SeekFrom::End(-1)).unwrap();
            let mut byte = [0];
            file.read_exact(&mut byte).unwrap();
            file.seek(std::io::SeekFrom::End(-1)).unwrap();
            file.write_all(&[byte[0] ^ 0x80]).unwrap();
            file.sync_all().unwrap();
        }
        let result = LocalGroup::open(&directory.0, identity, options(), State::default()).await;
        if incomplete {
            let reopened = result.unwrap();
            assert_eq!(
                reopened
                    .try_with_application(|app| app.value)
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(
                reopened
                    .submit(command(2), deadline())
                    .await
                    .unwrap()
                    .position,
                ExecutionPosition::Local { sequence: 2 }
            );
            reopened.drain(deadline()).await.unwrap();
        } else {
            assert!(result.is_err());
            assert!(segment.is_file());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_keeps_durable_commands_beyond_the_published_snapshot() {
    use super::storage::{FORMAT, Record};
    let directory = Directory::new();
    let identity = identity();
    let opened = super::storage::open(&directory.0, identity.clone(), options(), State::default())
        .await
        .unwrap();
    let storage = opened.storage;
    let mut app = State::default();
    let mut retained = std::collections::VecDeque::new();
    for sequence in 1..=2 {
        let command = command(sequence);
        let encoded = crate::wal::encode_value(&command, 800).unwrap();
        let record = crate::wal::encode_value(
            &Record {
                format: FORMAT,
                identity: storage.hash,
                sequence,
                command: &encoded,
            },
            1024,
        )
        .unwrap();
        let (id, durable) = storage.journal.append_retained(record).await.unwrap();
        durable.durable().await.unwrap();
        retained.push_back((sequence, id));
        if sequence == 1 {
            app.apply(command);
            storage.publish(&app, 1).await.unwrap().unwrap();
        }
    }
    storage.checkpoint(1, &retained).await.unwrap();
    assert_eq!(storage.journal.statistics().retained_records, 1);
    storage.journal.close().await.unwrap();
    storage.release().await;
    let reopened = LocalGroup::open(&directory.0, identity, options(), State::default())
        .await
        .unwrap();
    assert_eq!(
        reopened.applied(),
        Some(ExecutionPosition::Local { sequence: 2 })
    );
    assert_eq!(
        reopened
            .try_with_application(|app| (app.value, app.applications))
            .await
            .unwrap(),
        (2, 2)
    );
    reopened.drain(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_local_format_identity_and_sequence_never_get_replayed_or_cleaned() {
    use super::storage::{FORMAT, Record};
    for fault in 0..4 {
        let directory = Directory::new();
        let identity = identity();
        let opened =
            super::storage::open(&directory.0, identity.clone(), options(), State::default())
                .await
                .unwrap();
        let storage = opened.storage;
        let command = crate::wal::encode_value(&command(1), 800).unwrap();
        let mut bytes = crate::wal::encode_value(
            &Record {
                format: if fault == 0 { *b"UNKNOWN!" } else { FORMAT },
                identity: if fault == 1 { [0; 32] } else { storage.hash },
                sequence: if fault == 2 { 2 } else { 1 },
                command: &command,
            },
            1024,
        )
        .unwrap();
        if fault == 3 {
            bytes.push(0);
        }
        storage
            .journal
            .append_retained(bytes)
            .await
            .unwrap()
            .1
            .durable()
            .await
            .unwrap();
        storage.journal.close().await.unwrap();
        storage.release().await;
        let orphan = directory.0.join(format!(
            "snapshot-{}.bin",
            tidemq_protocol::Id::new_v7().unwrap()
        ));
        std::fs::write(&orphan, b"preserve-on-failed-recovery").unwrap();
        assert!(
            LocalGroup::open(&directory.0, identity, options(), State::default())
                .await
                .is_err()
        );
        assert!(
            orphan.is_file(),
            "Failed recovery must preserve diagnostic evidence"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_publication_failure_freezes_cleanup_and_reopens_the_durable_chain() {
    for stage in [
        "snapshot_write",
        "snapshot_publish",
        "snapshot_published",
        "checkpoint",
        "checkpoint_published",
        "snapshot_cleanup",
        "snapshot_cleanup_sync",
    ] {
        let directory = Directory::new();
        let identity = identity();
        let group = LocalGroup::open(&directory.0, identity.clone(), options(), State::default())
            .await
            .unwrap();
        group.submit(command(1), deadline()).await.unwrap();
        group.snapshot(deadline()).await.unwrap();
        group.submit(command(2), deadline()).await.unwrap();
        let hook = HookGuard::new(&directory.0, stage, true);
        let result = group.snapshot(deadline()).await;
        let failed = matches!(stage, "snapshot_publish" | "snapshot_published");
        if failed {
            assert!(result.is_err());
            assert!(group.drain(deadline()).await.is_err());
            let snapshots = std::fs::read_dir(&directory.0)
                .unwrap()
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("snapshot-")
                })
                .count();
            assert_eq!(
                snapshots, 2,
                "Uncertain publication must preserve both snapshot generations"
            );
        } else {
            result.unwrap();
            group.drain(deadline()).await.unwrap();
        }
        drop(hook);
        let reopened = LocalGroup::open(&directory.0, identity, options(), State::default())
            .await
            .unwrap();
        assert_eq!(
            reopened
                .try_with_application(|app| app.value)
                .await
                .unwrap(),
            2
        );
        reopened.drain(deadline()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_child() {
    let Some(path) = std::env::var_os("TIDEMQ_LOCAL_TEST_PATH") else {
        return;
    };
    let path = PathBuf::from(path);
    let cluster = std::env::var("TIDEMQ_LOCAL_TEST_CLUSTER")
        .unwrap()
        .parse()
        .unwrap();
    let identity = StorageIdentity::new::<State>(cluster, 7, 1);
    let group = LocalGroup::open(&path, identity, options(), State::default())
        .await
        .unwrap();
    group.submit(command(1), deadline()).await.unwrap();
    group
        .submit_completion(
            Command::Ack {
                operation: 2,
                offset: 1,
            },
            deadline(),
        )
        .await
        .unwrap();
    group.snapshot(deadline()).await.unwrap();
    tokio::fs::write(
        path.parent().unwrap().join("armed"),
        b"send-and-ack-succeeded",
    )
    .await
    .unwrap();
    if std::env::var("TIDEMQ_LOCAL_TEST_PHASE").unwrap() == "acknowledged" {
        tokio::fs::write(path.parent().unwrap().join("ready"), b"acknowledged")
            .await
            .unwrap();
        std::future::pending::<()>().await;
    }
    group.submit(command(3), deadline()).await.unwrap();
    group.snapshot(deadline()).await.unwrap();
    panic!("Requested crash boundary was not reached");
}

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_kill_recovers_send_ack_and_snapshot_publication_boundaries() {
    use std::process::{Command as Process, Stdio};
    for stage in [
        "acknowledged",
        "before_append",
        "before_durable",
        "after_durable",
        "before_apply",
        "after_apply",
        "snapshot_write",
        "snapshot_publish",
        "snapshot_published",
        "checkpoint",
        "checkpoint_published",
        "snapshot_cleanup",
    ] {
        let root = Directory::new();
        std::fs::create_dir(&root.0).unwrap();
        let path = root.0.join("group");
        let identity = identity();
        let mut child = Child(
            Process::new(std::env::current_exe().unwrap())
                .args(["--exact", "execution::tests::crash_child", "--nocapture"])
                .env("TIDEMQ_LOCAL_TEST_PATH", &path)
                .env("TIDEMQ_LOCAL_TEST_PHASE", stage)
                .env(
                    "TIDEMQ_LOCAL_TEST_CLUSTER",
                    identity.group.cluster_id.to_string(),
                )
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        tokio::time::timeout_at(deadline(), async {
            while !root.0.join("ready").is_file() {
                assert!(
                    child.0.try_wait().unwrap().is_none(),
                    "Crash fixture exited before {stage}"
                );
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        let reopened = LocalGroup::open(&path, identity, options(), State::default())
            .await
            .unwrap();
        let restored = reopened.try_with_application(Clone::clone).await.unwrap();
        assert_eq!(
            restored.ack, 1,
            "Acknowledged durable ACK was lost at {stage}"
        );
        if stage == "acknowledged" || stage == "before_append" {
            assert_eq!(restored.value, 1);
        } else if stage == "before_durable" {
            assert!(restored.value == 1 || restored.value == 3);
        } else {
            assert_eq!(restored.value, 3, "Durable command was lost at {stage}");
        }
        let sequence = reopened.statistics().applied_sequence;
        assert_eq!(
            reopened
                .submit(command(4), deadline())
                .await
                .unwrap()
                .position,
            ExecutionPosition::Local {
                sequence: sequence + 1
            }
        );
        reopened.drain(deadline()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_executor_and_wal_task_abort_proactively_fail_health_without_another_request() {
    for writer in [false, true] {
        let directory = Directory::new();
        let identity = identity();
        let group = LocalGroup::open(&directory.0, identity.clone(), options(), State::default())
            .await
            .unwrap();
        let mut health = group.health();
        if writer {
            group.shared.storage.journal.abort_writer();
        } else {
            lock(&group.shared.abort).as_ref().unwrap().abort();
        }
        tokio::time::timeout_at(deadline(), async {
            while !matches!(*health.borrow_and_update(), LocalHealth::Failed { .. }) {
                health.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert!(group.drain(deadline()).await.is_err());
        let reopened = LocalGroup::open(&directory.0, identity, options(), State::default())
            .await
            .unwrap();
        reopened.drain(deadline()).await.unwrap();
    }
}

async fn real_application_capacity_cycles<A: Application>(
    identity: StorageIdentity,
    command: A::Command,
) -> A::Response {
    let directory = Directory::new();
    let mut configured = options();
    configured.journal.max_file_bytes = 16 * 1024;
    configured.journal.control_reserve_bytes = 4096;
    configured.journal.max_pending_bytes = 4096;
    let capacity = configured.journal.max_file_bytes;
    let group = LocalGroup::<A>::open(
        &directory.0,
        identity.clone(),
        configured.clone(),
        A::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        group.file_limit_bytes().unwrap(),
        configured.file_limit_bytes().unwrap()
    );
    let first = group
        .submit(command.clone(), deadline())
        .await
        .unwrap()
        .response;
    let expected_state = group
        .try_with_application(|a| crate::wal::encode_value(a, 4096).unwrap())
        .await
        .unwrap();
    let mut last_response = Vec::new();
    for _ in 0..2000 {
        let result = group
            .submit_completion(command.clone(), deadline())
            .await
            .unwrap();
        last_response = crate::wal::encode_value(&result.response, 4096).unwrap();
        let stats = group.statistics();
        assert!(stats.journal.current_file_bytes <= capacity);
        if stats.journal.written_bytes > capacity * 3 && stats.maintenance_runs >= 3 {
            break;
        }
    }
    let stats = group.statistics();
    assert!(stats.journal.written_bytes > capacity * 3);
    assert!(stats.journal.reclaimed_bytes > capacity * 2);
    assert!(stats.maintenance_runs >= 3);
    assert_eq!(
        group
            .try_with_application(|a| crate::wal::encode_value(a, 4096).unwrap())
            .await
            .unwrap(),
        expected_state
    );
    group.drain(deadline()).await.unwrap();
    let reopened = LocalGroup::<A>::open(&directory.0, identity, configured, A::default())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .try_with_application(|a| crate::wal::encode_value(a, 4096).unwrap())
            .await
            .unwrap(),
        expected_state
    );
    let original = reopened
        .submit_completion(command, deadline())
        .await
        .unwrap();
    assert_eq!(
        crate::wal::encode_value(&original.response, 4096).unwrap(),
        last_response
    );
    reopened.drain(deadline()).await.unwrap();
    first
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_metadata_and_data_groups_automatically_cross_three_wal_capacities() {
    use crate::broker::{DataApplication, DataCommand, DataGroupConfig, DataResponse};
    use crate::metadata::{
        Command as MetadataCommand, DeploymentMode, MetadataLimits, MetadataState,
    };
    let cluster = tidemq_protocol::Id::new_v7().unwrap();
    let metadata = StorageIdentity::new::<MetadataState>(cluster, 7, 1);
    let bootstrap = MetadataCommand::bootstrap(
        tidemq_protocol::Id::new_v7().unwrap(),
        cluster,
        7,
        DeploymentMode::LocalRf1,
        MetadataLimits::default(),
    );
    assert!(
        real_application_capacity_cycles::<MetadataState>(metadata, bootstrap)
            .await
            .is_applied()
    );
    let data = StorageIdentity::new::<DataApplication>(cluster, 8, 1);
    let initialize = DataCommand::Initialize(DataGroupConfig {
        cluster_id: cluster,
        group_id: 8,
        max_partitions: 4,
        max_hot_bytes: 1 << 20,
        max_hot_messages: 1024,
        producer: Default::default(),
        subscription: Default::default(),
        retention: Default::default(),
    });
    assert_eq!(
        real_application_capacity_cycles::<DataApplication>(data, initialize).await,
        DataResponse::Initialized
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bytes_records_and_segments_each_trigger_automatic_maintenance() {
    for trigger in ["bytes", "records", "segments"] {
        let directory = Directory::new();
        let identity = identity();
        let mut configured = options();
        if trigger == "bytes" {
            configured.journal.max_retained_records = 512;
            configured.journal.max_segments = 64;
            configured.journal.segment_target_bytes = 16 * 1024;
        }
        if trigger == "segments" {
            configured.journal.max_retained_records = 512;
            configured.journal.max_segments = 5;
        }
        let group = LocalGroup::open(
            &directory.0,
            identity.clone(),
            configured.clone(),
            State::default(),
        )
        .await
        .unwrap();
        for operation in 1..=400 {
            group.submit(command(operation), deadline()).await.unwrap();
            if group.statistics().maintenance_runs >= 3 {
                break;
            }
        }
        let stats = group.statistics();
        assert!(
            stats.maintenance_runs >= 3,
            "Automatic {trigger} maintenance did not advance"
        );
        assert_eq!(stats.maintenance_trigger, Some(trigger));
        assert!(stats.journal.reclaimed_bytes > 0);
        let expected = group.try_with_application(Clone::clone).await.unwrap();
        group.drain(deadline()).await.unwrap();
        let reopened = LocalGroup::open(&directory.0, identity, configured, State::default())
            .await
            .unwrap();
        assert_eq!(
            reopened.try_with_application(Clone::clone).await.unwrap(),
            expected
        );
        reopened.drain(deadline()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canceled_snapshot_write_keeps_directory_owned_through_the_actual_io() {
    let directory = Directory::new();
    let identity = identity();
    let group = LocalGroup::open(&directory.0, identity.clone(), options(), State::default())
        .await
        .unwrap();
    group.submit(command(1), deadline()).await.unwrap();
    let hook = HookGuard::new(&directory.0, "snapshot_write_io", false);
    let client = group.clone();
    let snapshot = tokio::spawn(async move { client.snapshot(deadline()).await });
    hook.entered().await;
    assert!(
        group
            .drain(Instant::now() + Duration::from_millis(10))
            .await
            .is_err()
    );
    let _ = snapshot.await.unwrap();
    assert!(
        LocalGroup::open(&directory.0, identity.clone(), options(), State::default())
            .await
            .is_err()
    );
    hook.release();
    assert!(group.drain(deadline()).await.is_err());
    let reopened = tokio::time::timeout_at(deadline(), async {
        loop {
            if let Ok(group) =
                LocalGroup::open(&directory.0, identity.clone(), options(), State::default()).await
            {
                break group;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        reopened
            .try_with_application(|app| app.value)
            .await
            .unwrap(),
        1
    );
    reopened.drain(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_wal_writer_close_releases_ownership_even_when_its_handle_is_retained() {
    let directory = Directory::new();
    let (journal, _, recovery) =
        crate::wal::SegmentedJournal::open(&directory.0, options().journal, [8; 32])
            .await
            .unwrap();
    drop(recovery);
    let mut health = journal.health();
    let reference = crate::wal::SnapshotRef {
        file_name: format!("snapshot-{}.bin", tidemq_protocol::Id::new_v7().unwrap()),
        byte_len: 128,
        checksum: [0; 32],
    };
    assert!(journal.publish_snapshot(reference).await.is_err());
    tokio::time::timeout_at(deadline(), async {
        while !matches!(
            *health.borrow_and_update(),
            crate::wal::JournalHealth::Failed { .. }
        ) {
            health.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(journal.close().await.is_err());
    let owner = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.0.join(".writer-lock"))
        .unwrap();
    owner.try_lock().unwrap();
    assert!(matches!(
        *journal.health().borrow(),
        crate::wal::JournalHealth::Failed { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn existing_metrics_cover_local_admission_durability_apply_and_maintenance() {
    use crate::observability::metrics::{Metrics, Operation};
    let directory = Directory::new();
    let metrics = Metrics::default();
    let mut configured = options();
    configured.metrics = metrics.operation(Operation::Control);
    let group = LocalGroup::open(&directory.0, identity(), configured, State::default())
        .await
        .unwrap();
    group.submit(command(1), deadline()).await.unwrap();
    assert!(matches!(
        group.submit(command(2), Instant::now()).await,
        Err(Error::NotSubmitted { .. })
    ));
    group.snapshot(deadline()).await.unwrap();
    let text = metrics.encode().unwrap();
    for stage in [
        "Admission",
        "Queue",
        "WalAdmission",
        "Commit",
        "Apply",
        "WalCheckpoint",
        "WalGc",
    ] {
        assert!(
            text.contains(&format!("stage=\"{stage}\"")),
            "Missing metric stage {stage}"
        );
    }
    assert_eq!(group.statistics().rejected, 1);
    assert!(!text.contains("padding"));
    assert!(!text.contains("operation_id"));
    group.drain(deadline()).await.unwrap();
}

#[test]
fn raft_redirect_preserves_the_actual_leader_identity_and_node() {
    let original =
        openraft::error::ForwardToLeader::new(42, openraft::BasicNode::new("127.0.0.1:9443"));
    let error = super::group::raft_write_error(openraft::error::RaftError::APIError(
        openraft::error::ClientWriteError::ForwardToLeader(original.clone()),
    ));
    match error {
        Error::ForwardToLeader { source } => assert_eq!(source, original),
        error => panic!("Raft leader redirect was erased: {error}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segment_pressure_keeps_a_completion_rotation_while_snapshot_readers_block_production() {
    let directory = Directory::new();
    let mut configured = options();
    configured.journal.max_segments = 5;
    configured.journal.max_retained_records = 512;
    let group = LocalGroup::open(&directory.0, identity(), configured, State::default())
        .await
        .unwrap();
    let mut readers = Vec::new();
    let mut operation = 0;
    for generation in 1..=3 {
        while group.statistics().maintenance_runs < generation {
            operation += 1;
            group.submit(command(operation), deadline()).await.unwrap();
        }
        readers.push(group.snapshot_reader().await.unwrap().unwrap());
    }
    let mut rejected = false;
    for _ in 0..100 {
        operation += 1;
        if matches!(
            group.submit(command(operation), deadline()).await,
            Err(Error::NotSubmitted { .. })
        ) {
            rejected = true;
            break;
        }
    }
    assert!(rejected);
    let rotations = group.statistics().journal.rotations;
    group
        .submit_completion(command(operation), deadline())
        .await
        .unwrap();
    assert!(
        group.statistics().journal.rotations > rotations,
        "Completion work must retain its own segment rotation"
    );
    let applied = group.statistics().applied_sequence;
    drop(readers);
    wait_for(&group, |stats| stats.snapshot_sequence == applied).await;
    group.drain(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_snapshot_staging_uses_the_snapshot_budget_during_recovery() {
    let directory = Directory::new();
    let identity = identity();
    let mut configured = options();
    configured.max_snapshot_bytes = 128 * 1024;
    let group = LocalGroup::open(
        &directory.0,
        identity.clone(),
        configured.clone(),
        State::default(),
    )
    .await
    .unwrap();
    group.submit(command(1), deadline()).await.unwrap();
    group.drain(deadline()).await.unwrap();
    let incoming = directory.0.join(format!(
        ".incoming-{}",
        tidemq_protocol::Id::new_v7().unwrap()
    ));
    let file = std::fs::File::create(&incoming).unwrap();
    file.set_len(configured.journal.max_file_bytes + 4096)
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    let reopened = LocalGroup::open(&directory.0, identity, configured, State::default())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .try_with_application(|application| application.value)
            .await
            .unwrap(),
        1
    );
    assert!(
        !incoming.exists(),
        "Unpublished snapshot staging must be cleaned only after complete recovery"
    );
    reopened.drain(deadline()).await.unwrap();
}
