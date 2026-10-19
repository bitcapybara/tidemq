use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::{BasicNode, LogId, SnapshotMeta, StoredMembership, Vote};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::{Mutex, RwLock};

use crate::observability::metrics::OperationMetrics;
use crate::wal::{
    self, Error, FRAME_HEADER_BYTES, Inspection, Journal, JournalOptions, JournalStatistics, Result,
};

use super::{Application, LogEntry, StorageIdentity};

pub(crate) const FORMAT_VERSION: u32 = 1;
pub(crate) const JOURNAL_FILE: &str = "raft.wal";
pub(crate) const SNAPSHOT_FILE: &str = "snapshot.bin";

#[derive(Clone, Debug)]
pub struct StorageOptions {
    pub journal: JournalOptions,
    pub max_log_entries: usize,
    pub max_log_bytes: usize,
    pub max_snapshot_bytes: usize,
    pub max_replication_batch_bytes: usize,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            journal: JournalOptions::default(),
            max_log_entries: 65_536,
            max_log_bytes: 64 * 1024 * 1024,
            max_snapshot_bytes: 64 * 1024 * 1024,
            max_replication_batch_bytes: 1024 * 1024,
        }
    }
}

impl StorageOptions {
    pub fn validate(&self) -> Result<()> {
        self.journal.validate()?;
        let checkpoint_bound = self
            .max_log_entries
            .checked_mul(FRAME_HEADER_BYTES.saturating_add(16))
            .and_then(|overhead| overhead.checked_add(self.max_log_bytes))
            .and_then(|bytes| bytes.checked_add(4096));
        if self.max_log_entries == 0
            || self.max_log_bytes == 0
            || self.max_log_bytes as u64 >= self.journal.max_file_bytes
            || self.max_snapshot_bytes == 0
            || self.max_snapshot_bytes > u32::MAX as usize
            || self.max_replication_batch_bytes == 0
            || checkpoint_bound.is_none_or(|bytes| bytes as u64 > self.journal.max_file_bytes)
        {
            return Err(Error::Configuration {
                reason: "replicated storage budgets are invalid",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound = "")]
// Variant order is part of the postcard disk format and requires a format-version change.
pub(crate) enum Record<'a, A: Application> {
    Vote(Vote<u64>),
    Committed(Option<LogId<u64>>),
    Entries(Cow<'a, [LogEntry<A>]>),
    Truncate(LogId<u64>),
    Purge(LogId<u64>),
    Identity(StorageIdentity),
}

pub(crate) struct LogState<A: Application> {
    pub vote: Option<Vote<u64>>,
    pub committed: Option<LogId<u64>>,
    pub purged: Option<LogId<u64>>,
    pub entries: BTreeMap<u64, LogEntry<A>>,
    pub entry_bytes: BTreeMap<u64, usize>,
    pub retained_bytes: usize,
}

impl<A: Application> Default for LogState<A> {
    fn default() -> Self {
        Self {
            vote: None,
            committed: None,
            purged: None,
            entries: BTreeMap::new(),
            entry_bytes: BTreeMap::new(),
            retained_bytes: 0,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Applied<A: Application> {
    pub application: A,
    pub last_applied: Option<LogId<u64>>,
    pub membership: StoredMembership<u64, BasicNode>,
    pub failed: bool,
}

impl<A: Application> Default for Applied<A> {
    fn default() -> Self {
        Self {
            application: A::default(),
            last_applied: None,
            membership: StoredMembership::default(),
            failed: false,
        }
    }
}

pub(crate) fn ensure_application_available<A: Application>(state: &Applied<A>) -> Result<()> {
    if state.failed {
        return Err(Error::Writer {
            reason: "application stopped after a fatal local apply failure".into(),
        });
    }
    Ok(())
}

pub(crate) struct Shared<A: Application> {
    pub identity: StorageIdentity,
    pub directory: PathBuf,
    pub options: StorageOptions,
    pub journal: Journal,
    pub log: Mutex<LogState<A>>,
    pub applied: RwLock<Applied<A>>,
    pub snapshot_serial: Mutex<()>,
    pub snapshot_meta: RwLock<Option<SnapshotMeta<u64, BasicNode>>>,
    pub incoming: Mutex<Option<PathBuf>>,
    pub recovered: Inspection,
    pub metrics: Option<Arc<OperationMetrics>>,
    pub _owner: Arc<std::fs::File>,
}

#[derive(Clone)]
pub struct DiskLog<A: Application>(pub(crate) Arc<Shared<A>>);

#[derive(Clone)]
pub struct DiskStateMachine<A: Application>(pub(crate) Arc<Shared<A>>);

pub struct DiskStore<A: Application> {
    pub log: DiskLog<A>,
    pub state_machine: DiskStateMachine<A>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageDiagnostics {
    pub identity: StorageIdentity,
    pub vote: Option<Vote<u64>>,
    pub committed_log: Option<LogId<u64>>,
    pub applied_log: Option<LogId<u64>>,
    pub application_failed: bool,
    pub purged_log: Option<LogId<u64>>,
    pub snapshot_log: Option<LogId<u64>>,
    pub retained_log_entries: usize,
    pub retained_encoded_log_bytes: usize,
    pub remaining_log_entries: usize,
    pub remaining_encoded_log_bytes: usize,
    pub journal_file_limit_bytes: u64,
    pub snapshot_payload_limit_bytes: usize,
    pub journal_recovery: Inspection,
    pub journal_runtime: JournalStatistics,
}

impl<A: Application> DiskStore<A> {
    /// Opens one exclusively owned replica directory. The parent directory must already exist.
    pub async fn open(
        directory: impl AsRef<Path>,
        identity: StorageIdentity,
        options: StorageOptions,
        metrics: Option<Arc<OperationMetrics>>,
    ) -> Result<Self> {
        options.validate()?;
        validate_identity::<A>(&identity)?;
        let directory = directory.as_ref();
        create_directory(directory).await?;
        let owner = lock_directory(directory).await?;
        persist_identity(directory, &identity).await?;
        clean_temporary_files(directory).await?;
        let (journal, recovered, records) = Journal::open_owned(
            &directory.join(JOURNAL_FILE),
            options.journal.clone(),
            metrics.clone(),
            Some(owner.clone()),
        )
        .await?;
        let mut log = LogState::<A>::default();
        let mut records = records.into_iter();
        match records.next() {
            Some(bytes) => {
                let record: Record<A> = wal::decode_value(&bytes)?;
                if !matches!(record, Record::Identity(saved) if saved == identity) {
                    return Err(Error::Corrupt {
                        offset: 0,
                        reason: "journal identity does not match the replica directory",
                    });
                }
            }
            None => {
                journal
                    .append(wal::encode_value(
                        &Record::<A>::Identity(identity.clone()),
                        options.journal.max_record_bytes,
                    )?)
                    .await?
                    .durable()
                    .await?;
            }
        }
        for bytes in records {
            let record = wal::decode_value(&bytes)?;
            super::log::replay(&mut log, record, &options)?;
        }
        let (mut applied, snapshot_meta) =
            super::snapshot::load::<A>(directory, &identity, &options).await?;
        super::state_machine::recover(&mut applied, &log)?;
        let shared = Arc::new(Shared {
            identity,
            directory: directory.to_owned(),
            options,
            journal,
            log: Mutex::new(log),
            applied: RwLock::new(applied),
            snapshot_serial: Mutex::new(()),
            snapshot_meta: RwLock::new(snapshot_meta),
            incoming: Mutex::new(None),
            recovered,
            metrics,
            _owner: owner,
        });
        tracing::info!(event = "raft_storage_recovered", node_id = shared.identity.node_id,
            group_id = %shared.identity.group.group_id, "Recovered durable replicated storage");
        Ok(Self {
            log: DiskLog(shared.clone()),
            state_machine: DiskStateMachine(shared),
        })
    }
}

impl<A: Application> DiskLog<A> {
    pub async fn flush(&self) -> Result<()> {
        self.0.journal.flush().await
    }

    /// Call only after the owning Raft instance has shut down.
    pub async fn close(&self) -> Result<()> {
        self.0.journal.close().await
    }

    /// Local state for diagnosis. This does not establish linearizability or current leadership.
    pub async fn diagnostics(&self) -> StorageDiagnostics {
        let applied = self.0.applied.read().await;
        let log = self.0.log.lock().await;
        let snapshot = self.0.snapshot_meta.read().await;
        StorageDiagnostics {
            identity: self.0.identity.clone(),
            vote: log.vote,
            committed_log: log.committed,
            applied_log: applied.last_applied,
            application_failed: applied.failed,
            purged_log: log.purged,
            snapshot_log: snapshot.as_ref().and_then(|meta| meta.last_log_id),
            retained_log_entries: log.entries.len(),
            retained_encoded_log_bytes: log.retained_bytes,
            remaining_log_entries: self
                .0
                .options
                .max_log_entries
                .saturating_sub(log.entries.len()),
            remaining_encoded_log_bytes: self
                .0
                .options
                .max_log_bytes
                .saturating_sub(log.retained_bytes),
            journal_file_limit_bytes: self.0.options.journal.max_file_bytes,
            snapshot_payload_limit_bytes: self.0.options.max_snapshot_bytes,
            journal_recovery: self.0.recovered.clone(),
            journal_runtime: self.0.journal.statistics(),
        }
    }
}

impl<A: Application> DiskStateMachine<A> {
    /// A fatal local apply failure invalidates every authoritative read until disk recovery.
    /// Obtain a real Raft read barrier first and keep the projection bounded and free of IO.
    pub async fn try_with_application<R>(&self, query: impl FnOnce(&A) -> R) -> Result<R> {
        self.try_with_applied(|application, _| query(application))
            .await
    }

    pub async fn try_with_applied<R>(
        &self,
        query: impl FnOnce(&A, Option<LogId<u64>>) -> R,
    ) -> Result<R> {
        let applied = self.0.applied.read().await;
        ensure_application_available(&applied)?;
        Ok(query(&applied.application, applied.last_applied))
    }

    /// Borrows local state for a bounded synchronous query. The closure must not perform IO,
    /// scan retained history, or copy the entire state. Obtain a Raft read barrier first when
    /// authority or linearizability is required; this diagnostic-only method can show failed
    /// partial state. Authoritative callers must use `try_with_application` instead.
    pub async fn with_application<R>(&self, query: impl FnOnce(&A) -> R) -> R {
        self.with_applied(|application, _| query(application)).await
    }

    /// Reads a bounded projection and its applied position under the same local read lock.
    /// The same no-IO, no-long-scan and Raft-read-barrier requirements as `with_application` apply.
    pub async fn with_applied<R>(&self, query: impl FnOnce(&A, Option<LogId<u64>>) -> R) -> R {
        let applied = self.0.applied.read().await;
        query(&applied.application, applied.last_applied)
    }

    /// Reads an applied local replica view. The caller must obtain a Raft read barrier for
    /// authoritative reads; a recovered local view alone is not proof of ownership or quorum.
    pub async fn application(&self) -> A {
        self.0.applied.read().await.application.clone()
    }
}

fn validate_identity<A: Application>(identity: &StorageIdentity) -> Result<()> {
    if identity.group.group_id == 0
        || identity.group.application != A::NAME
        || identity.group.application_version != A::VERSION
        || A::NAME.is_empty()
        || A::NAME.len() > 128
        || !A::NAME.is_ascii()
    {
        return Err(Error::Configuration {
            reason: "application identity or format version does not match",
        });
    }
    Ok(())
}

async fn create_directory(directory: &Path) -> Result<()> {
    match fs::create_dir(directory).await {
        Ok(()) => {
            let parent = directory.parent().ok_or(Error::Configuration {
                reason: "replica directory parent is missing",
            })?;
            wal::sync_directory(parent).await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(Error::io(directory, error)),
    }
    Ok(())
}

async fn lock_directory(directory: &Path) -> Result<Arc<std::fs::File>> {
    let path = directory.join(".writer-lock");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .await
        .map_err(|e| Error::io(&path, e))?
        .into_std()
        .await;
    let file = tokio::task::spawn_blocking(move || {
        file.try_lock().map_err(|error| Error::Writer {
            reason: format!("replica directory is already locked or locking failed: {error}"),
        })?;
        Ok::<_, Error>(file)
    })
    .await
    .map_err(|error| Error::Writer {
        reason: format!("directory lock task failed: {error}"),
    })??;
    Ok(Arc::new(file))
}

async fn persist_identity(directory: &Path, identity: &StorageIdentity) -> Result<()> {
    let path = directory.join("identity.bin");
    if fs::try_exists(&path)
        .await
        .map_err(|e| Error::io(&path, e))?
    {
        let bytes = super::snapshot::read_single(&path, 16 * 1024).await?;
        let saved: StorageIdentity = wal::decode_value(&bytes)?;
        if saved != *identity {
            return Err(Error::Configuration {
                reason: "replica directory belongs to a different identity",
            });
        }
    } else {
        for data_file in [JOURNAL_FILE, SNAPSHOT_FILE] {
            let data_path = directory.join(data_file);
            if fs::try_exists(&data_path)
                .await
                .map_err(|e| Error::io(&data_path, e))?
            {
                return Err(Error::Configuration {
                    reason: "replica identity is missing from a directory containing durable data",
                });
            }
        }
        let bytes = wal::encode_value(identity, 16 * 1024)?;
        let framed = crate::wal::codec_frame(&bytes, 0)?;
        wal::write_atomic(&path, &framed).await?;
    }
    Ok(())
}

async fn clean_temporary_files(directory: &Path) -> Result<()> {
    let mut entries = fs::read_dir(directory)
        .await
        .map_err(|e| Error::io(directory, e))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| Error::io(directory, e))?
    {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(".incoming-")
            || name.starts_with(".replace-")
            || name == ".journal-checkpoint"
        {
            fs::remove_file(entry.path())
                .await
                .map_err(|e| Error::io(entry.path(), e))?;
        }
    }
    Ok(())
}

pub(crate) fn storage_error(
    subject: openraft::ErrorSubject<u64>,
    verb: openraft::ErrorVerb,
    error: Error,
) -> openraft::StorageError<u64> {
    openraft::StorageError::from_io_error(subject, verb, std::io::Error::other(error))
}
