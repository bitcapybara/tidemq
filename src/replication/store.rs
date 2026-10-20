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
    self, Error, FRAME_HEADER_BYTES, JournalOptions, RecordId, Result, SegmentedInspection,
    SegmentedJournal, SegmentedStatistics,
};

use super::snapshot_registry::{SnapshotDiagnostics, SnapshotRegistry};
use super::{Application, LogEntry, StorageIdentity};

pub(crate) const FORMAT_VERSION: u32 = 2;
const IDENTITY_LIMIT: usize = 16 * 1024;

#[derive(Clone, Debug)]
pub struct StorageOptions {
    pub journal: JournalOptions,
    pub max_log_entries: usize,
    pub max_log_bytes: usize,
    pub max_snapshot_bytes: usize,
    pub max_snapshot_files: usize,
    pub max_snapshot_readers: usize,
    pub max_replication_batch_bytes: usize,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            journal: JournalOptions::default(),
            max_log_entries: 65_536,
            max_log_bytes: 64 * 1024 * 1024,
            max_snapshot_bytes: 64 * 1024 * 1024,
            max_snapshot_files: 4,
            max_snapshot_readers: 8,
            max_replication_batch_bytes: 1024 * 1024,
        }
    }
}

impl StorageOptions {
    pub fn validate(&self) -> Result<()> {
        self.journal.validate_segmented()?;
        let checkpoint_bound = self
            .max_log_entries
            // Includes maximum-width LogIds and journal record locations, never entry payloads.
            .checked_mul(96)
            .and_then(|bytes| bytes.checked_add(1024));
        if self.max_log_entries == 0
            || self.max_log_bytes == 0
            || self.max_log_bytes as u64 >= self.journal.max_file_bytes
            || self.max_snapshot_bytes == 0
            || self.max_snapshot_bytes > u32::MAX as usize
            || self.max_snapshot_files < 2
            || self.max_snapshot_files > 32
            || self.max_snapshot_readers == 0
            || self.max_snapshot_readers > 64
            || self.max_log_entries > self.journal.max_retained_records
            || self.max_replication_batch_bytes == 0
            || checkpoint_bound.is_none_or(|bytes| bytes > self.journal.max_checkpoint_bytes)
        {
            return Err(Error::Configuration {
                reason: "replicated storage budgets are invalid",
            });
        }
        self.replica_file_limit_bytes()?;
        Ok(())
    }

    /// Logical file lengths, including one incoming snapshot and bounded identity metadata.
    pub fn replica_file_limit_bytes(&self) -> Result<u64> {
        (self.max_snapshot_bytes as u64)
            .checked_add(FRAME_HEADER_BYTES as u64)
            .and_then(|bytes| bytes.checked_mul(self.max_snapshot_files.checked_add(1)? as u64))
            .and_then(|bytes| bytes.checked_add(self.journal.max_file_bytes))
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .ok_or(Error::Limit {
                name: "replica file budget",
            })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound = "")]
// Variant order is part of the postcard disk format and requires a format-version change.
pub(crate) enum Record<'a, A: Application> {
    Vote(Vote<u64>),
    Committed(Option<LogId<u64>>),
    Entry(Cow<'a, LogEntry<A>>),
    Truncate(LogId<u64>),
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ControlCheckpoint {
    pub format: u32,
    pub identity: StorageIdentity,
    pub vote: Option<Vote<u64>>,
    pub committed: Option<LogId<u64>>,
    pub purged: Option<LogId<u64>>,
    #[serde(deserialize_with = "bounded_log_ids")]
    pub retained: Vec<LogId<u64>>,
}

fn bounded_log_ids<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<LogId<u64>>, D::Error> {
    struct LogIds;
    impl<'de> serde::de::Visitor<'de> for LogIds {
        type Value = Vec<LogId<u64>>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("at most 65536 retained log identities")
        }
        fn visit_seq<S: serde::de::SeqAccess<'de>>(
            self,
            mut seq: S,
        ) -> std::result::Result<Self::Value, S::Error> {
            use serde::de::Error;
            let count = seq.size_hint().unwrap_or(0);
            if count > 65_536 {
                return Err(S::Error::custom(
                    "Retained log identity count exceeds its limit",
                ));
            }
            let mut result = Vec::new();
            result.try_reserve(count).map_err(S::Error::custom)?;
            while let Some(log_id) = seq.next_element()? {
                if result.len() >= 65_536 {
                    return Err(S::Error::custom(
                        "Retained log identity count exceeds its limit",
                    ));
                }
                result.push(log_id);
            }
            Ok(result)
        }
    }
    deserializer.deserialize_seq(LogIds)
}

#[derive(Serialize, Deserialize)]
struct IdentityEnvelope {
    format: u32,
    identity: StorageIdentity,
}

pub(crate) struct LogState<A: Application> {
    pub vote: Option<Vote<u64>>,
    pub committed: Option<LogId<u64>>,
    pub purged: Option<LogId<u64>>,
    pub entries: BTreeMap<u64, LogEntry<A>>,
    pub entry_bytes: BTreeMap<u64, usize>,
    pub record_refs: BTreeMap<u64, RecordId>,
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
            record_refs: BTreeMap::new(),
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
    pub journal: SegmentedJournal,
    pub log: Arc<Mutex<LogState<A>>>,
    pub applied: RwLock<Applied<A>>,
    pub snapshot_serial: Arc<Mutex<()>>,
    pub snapshot_meta: RwLock<Option<SnapshotMeta<u64, BasicNode>>>,
    pub snapshots: Mutex<SnapshotRegistry>,
    pub incoming: Mutex<Option<PathBuf>>,
    pub recovered: SegmentedInspection,
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
    pub replica_file_limit_bytes: u64,
    pub snapshots: SnapshotDiagnostics,
    pub journal_recovery: SegmentedInspection,
    pub journal_runtime: SegmentedStatistics,
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
        let identity_bytes = wal::encode_value(&identity, IDENTITY_LIMIT)?;
        let (journal, recovered, mut records) = SegmentedJournal::open_owned(
            directory,
            options.journal.clone(),
            *blake3::hash(&identity_bytes).as_bytes(),
            metrics.clone(),
            Some(owner.clone()),
        )
        .await?;
        let log = super::log::recover(&mut records, &identity, &options).await?;
        let current = journal.current_snapshot();
        let (mut applied, snapshot_meta) =
            super::snapshot::load::<A>(directory, &identity, &options, current.as_ref()).await?;
        super::state_machine::recover(&mut applied, &log)?;
        // Domain validation must succeed before any orphan or superseded file is removed.
        journal.cleanup_orphans().await?;
        let mut snapshots = SnapshotRegistry::new(current, &options, owner.clone());
        snapshots.cleanup(directory).await?;
        let shared = Arc::new(Shared {
            identity,
            directory: directory.to_owned(),
            options,
            journal,
            log: Arc::new(Mutex::new(log)),
            applied: RwLock::new(applied),
            snapshot_serial: Arc::new(Mutex::new(())),
            snapshot_meta: RwLock::new(snapshot_meta),
            snapshots: Mutex::new(snapshots),
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
            // Validated at open; retaining this value avoids silently wrapping a budget.
            replica_file_limit_bytes: self
                .0
                .options
                .replica_file_limit_bytes()
                .unwrap_or(u64::MAX),
            snapshots: self.0.snapshots.lock().await.diagnostics(),
            journal_recovery: self.0.recovered.clone(),
            journal_runtime: self.0.journal.statistics(),
        }
    }
}

impl<A: Application> DiskStateMachine<A> {
    /// Immutable identity persisted with the exclusively owned replica directory.
    pub fn storage_identity(&self) -> &StorageIdentity {
        &self.0.identity
    }

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

    /// Projects authority and committed membership under the same applied lock.
    /// Obtain a real quorum read barrier first and keep the projection bounded and free of IO.
    pub async fn try_with_membership<R>(
        &self,
        query: impl FnOnce(&A, Option<LogId<u64>>, &StoredMembership<u64, BasicNode>) -> R,
    ) -> Result<R> {
        let applied = self.0.applied.read().await;
        ensure_application_available(&applied)?;
        Ok(query(
            &applied.application,
            applied.last_applied,
            &applied.membership,
        ))
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
    if !fs::symlink_metadata(directory)
        .await
        .map_err(|error| Error::io(directory, error))?
        .file_type()
        .is_dir()
    {
        return Err(Error::Configuration {
            reason: "replica path must be a real directory",
        });
    }
    Ok(())
}

async fn lock_directory(directory: &Path) -> Result<Arc<std::fs::File>> {
    let path = directory.join(".writer-lock");
    match fs::symlink_metadata(&path).await {
        Ok(meta) if !meta.file_type().is_file() => {
            return Err(Error::Configuration {
                reason: "replica lock must be a regular file",
            });
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(Error::io(&path, error)),
    }
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
    for name in ["raft.wal", "snapshot.bin", ".journal-checkpoint"] {
        let path = directory.join(name);
        if fs::try_exists(&path)
            .await
            .map_err(|error| Error::io(&path, error))?
        {
            return Err(Error::Configuration {
                reason: "legacy replica layout is unsupported; use a fresh replica directory",
            });
        }
    }
    let path = directory.join("identity.bin");
    if fs::try_exists(&path)
        .await
        .map_err(|e| Error::io(&path, e))?
    {
        let metadata = fs::symlink_metadata(&path)
            .await
            .map_err(|error| Error::io(&path, error))?;
        if !metadata.file_type().is_file() {
            return Err(Error::Configuration {
                reason: "replica identity must be a regular file",
            });
        }
        let bytes = super::snapshot::read_single(&path, IDENTITY_LIMIT).await?;
        let saved: IdentityEnvelope = wal::decode_value(&bytes).map_err(|_| Error::Configuration {
            reason: "replica identity format is unsupported or corrupt; use a fresh directory for legacy data",
        })?;
        if saved.format != FORMAT_VERSION || saved.identity != *identity {
            return Err(Error::Configuration {
                reason: "replica directory identity or storage format differs",
            });
        }
    } else {
        let mut entries = fs::read_dir(directory)
            .await
            .map_err(|error| Error::io(directory, error))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| Error::io(directory, error))?
        {
            if entry.file_name() != ".writer-lock" {
                return Err(Error::Configuration {
                    reason: "replica identity is missing from a directory containing durable data",
                });
            }
        }
        let bytes = wal::encode_value(
            &IdentityEnvelope {
                format: FORMAT_VERSION,
                identity: identity.clone(),
            },
            IDENTITY_LIMIT,
        )?;
        let framed = crate::wal::codec_frame(&bytes, 0)?;
        wal::write_atomic(&path, &framed).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_budget_includes_snapshot_generations_and_one_receiver_without_overflow() {
        let options = StorageOptions::default();
        options.validate().unwrap();
        assert_eq!(
            options.replica_file_limit_bytes().unwrap(),
            448 * 1024 * 1024 + 5 * FRAME_HEADER_BYTES as u64 + 64 * 1024
        );
        let overflow = StorageOptions {
            max_snapshot_bytes: usize::MAX,
            ..options.clone()
        };
        assert!(overflow.replica_file_limit_bytes().is_err());
        let insufficient = StorageOptions {
            max_log_entries: 65_537,
            ..options
        };
        assert!(insufficient.validate().is_err());
    }

    #[test]
    fn checkpoint_rejects_forged_collection_size_before_allocating_its_elements() {
        #[derive(Deserialize)]
        struct Bounded(#[serde(deserialize_with = "bounded_log_ids")] Vec<LogId<u64>>);
        let forged = wal::encode_value(&vec![(); 65_537], 16).unwrap();
        assert!(wal::decode_value::<Bounded>(&forged).is_err());
        let empty = wal::encode_value(&Vec::<()>::new(), 16).unwrap();
        assert!(wal::decode_value::<Bounded>(&empty).unwrap().0.is_empty());
    }
}
