use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::fs::{self, File};
use tokio::io::AsyncReadExt;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::wal::{self, RecordId, SegmentedFileKind, SegmentedJournal, SnapshotRef};

use super::{
    Application, Error, ExecutionPosition, LocalOptions, Result, SnapshotInfo, StorageIdentity,
};

pub(super) const FORMAT: [u8; 8] = *b"TIDELOC1";
const IDENTITY_LIMIT: usize = 16 * 1024;

#[derive(Serialize, Deserialize)]
struct Identity {
    format: [u8; 8],
    identity: StorageIdentity,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Record<'a> {
    pub format: [u8; 8],
    pub identity: [u8; 32],
    pub sequence: u64,
    #[serde(borrow)]
    pub command: &'a [u8],
}

#[derive(Serialize, Deserialize)]
pub(super) struct Checkpoint {
    pub format: [u8; 8],
    pub identity: [u8; 32],
    pub sequence: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
struct Snapshot<A: Application> {
    format: [u8; 8],
    identity: StorageIdentity,
    sequence: u64,
    application: A,
}

#[derive(Serialize)]
struct SnapshotView<'a, A: Application> {
    format: [u8; 8],
    identity: &'a StorageIdentity,
    sequence: u64,
    application: &'a A,
}

pub(super) fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match value.lock() {
        Ok(value) => value,
        Err(poisoned) => poisoned.into_inner(),
    }
}

struct Asset {
    reference: SnapshotRef,
    _owner: Arc<std::fs::File>,
}

struct Snapshots {
    assets: Vec<Arc<Asset>>,
    current: Option<SnapshotInfo>,
    reference: Option<SnapshotRef>,
    frozen: bool,
    cleanup_pending: bool,
}

/// A bounded reader pins its snapshot generation and directory ownership through file use.
pub struct LocalSnapshotReader {
    pub info: SnapshotInfo,
    directory: PathBuf,
    asset: Arc<Asset>,
    permit: Arc<OwnedSemaphorePermit>,
    io: Arc<Semaphore>,
    notify: Arc<Notify>,
}

impl LocalSnapshotReader {
    /// Canceled observers leave their single bounded IO operation and reader lease owned.
    pub async fn read_chunk(&self, offset: u64, limit: usize) -> Result<Vec<u8>> {
        if limit == 0 || limit > 65_536 {
            return Err(Error::not_submitted(
                "Snapshot read chunk must contain 1..=65536 bytes",
            ));
        }
        let io = self
            .io
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::not_submitted("Snapshot reader already has an IO operation"))?;
        let asset = self.asset.clone();
        let permit = self.permit.clone();
        let directory = self.directory.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::{Read, Seek};
            let (_io, _permit) = (io, permit);
            boundary(&directory, "snapshot_read")?;
            let path = directory.join(&asset.reference.file_name);
            let mut file =
                std::fs::File::open(&path).map_err(|source| wal::Error::io(&path, source))?;
            file.seek(std::io::SeekFrom::Start(offset))
                .map_err(|source| wal::Error::io(&path, source))?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(limit)
                .map_err(|_| Error::fatal("Snapshot read buffer allocation failed"))?;
            file.take(limit as u64)
                .read_to_end(&mut bytes)
                .map_err(|source| wal::Error::io(&path, source))?;
            drop(asset);
            Ok(bytes)
        })
        .await
        .map_err(|error| Error::fatal(format!("Snapshot read worker failed: {error}")))?
    }
}

impl Drop for LocalSnapshotReader {
    fn drop(&mut self) {
        self.notify.notify_one();
    }
}

pub(super) struct Storage {
    pub directory: PathBuf,
    pub identity: StorageIdentity,
    pub hash: [u8; 32],
    pub options: LocalOptions,
    pub journal: SegmentedJournal,
    owner: Mutex<Option<Arc<std::fs::File>>>,
    snapshots: tokio::sync::Mutex<Snapshots>,
    readers: Arc<Semaphore>,
    io: Arc<Semaphore>,
    pub notify: Arc<Notify>,
}

pub(super) struct Opened<A: Application> {
    pub storage: Storage,
    pub application: A,
    pub sequence: u64,
    pub snapshot: u64,
    pub retained: VecDeque<(u64, RecordId)>,
}

fn corrupt(reason: &'static str) -> Error {
    wal::Error::Corrupt { offset: 0, reason }.into()
}

pub(super) async fn open<A: Application>(
    directory: &Path,
    identity: StorageIdentity,
    options: LocalOptions,
    mut application: A,
) -> Result<Opened<A>> {
    options.validate()?;
    validate_identity::<A>(&identity)?;
    application
        .validate()
        .map_err(|source| Error::Application { source })?;
    create_directory(directory).await?;
    let owner = lock_directory(directory).await?;
    persist_identity(directory, &identity).await?;
    let hash = *blake3::hash(&wal::encode_value(
        &Identity {
            format: FORMAT,
            identity: identity.clone(),
        },
        IDENTITY_LIMIT,
    )?)
    .as_bytes();
    let (journal, _, mut recovery) = SegmentedJournal::open_owned(
        directory,
        options.journal.clone(),
        hash,
        options.metrics.clone(),
        Some(owner.clone()),
    )
    .await?;
    let reference = journal.current_snapshot();
    let restored = recover_snapshot::<A>(directory, reference.as_ref(), &identity, &options).await;
    let (snapshot, info) = match restored {
        Ok(Some(restored)) => {
            application = restored.application;
            let sequence = restored.sequence;
            let info = reference.as_ref().map(|r| SnapshotInfo {
                position: ExecutionPosition::Local { sequence },
                snapshot_id: r.file_name.clone(),
            });
            (sequence, info)
        }
        Ok(None) => (0, None),
        Err(error) => {
            drop(recovery);
            let _ = journal.close().await;
            return Err(error);
        }
    };
    let storage = Storage {
        directory: directory.to_owned(),
        identity,
        hash,
        options: options.clone(),
        journal,
        owner: Mutex::new(Some(owner.clone())),
        snapshots: tokio::sync::Mutex::new(Snapshots {
            assets: reference
                .iter()
                .cloned()
                .map(|reference| {
                    Arc::new(Asset {
                        reference,
                        _owner: owner.clone(),
                    })
                })
                .collect(),
            current: info,
            reference,
            frozen: false,
            cleanup_pending: false,
        }),
        readers: Arc::new(Semaphore::new(options.max_snapshot_readers)),
        io: Arc::new(Semaphore::new(1)),
        notify: Arc::new(Notify::new()),
    };
    let replay = replay(&storage, &mut recovery, &mut application, snapshot).await;
    drop(recovery);
    let (sequence, retained) = match replay {
        Ok(value) => value,
        Err(error) => {
            let _ = storage.journal.close().await;
            return Err(error);
        }
    };
    application
        .validate()
        .map_err(|source| Error::Application { source })?;
    storage.journal.cleanup_orphans().await?;
    storage.cleanup_snapshots().await?;
    tracing::info!(
        event = "local_execution_recovered",
        group_id = storage.identity.group.group_id,
        node_id = storage.identity.node_id,
        sequence,
        snapshot,
        "Recovered local application storage"
    );
    Ok(Opened {
        storage,
        application,
        sequence,
        snapshot,
        retained,
    })
}

async fn replay<A: Application>(
    storage: &Storage,
    recovery: &mut wal::Recovery,
    application: &mut A,
    snapshot: u64,
) -> Result<(u64, VecDeque<(u64, RecordId)>)> {
    let mut sequence = if recovery.checkpoint().is_empty() {
        0
    } else {
        let checkpoint: Checkpoint = wal::decode_value(recovery.checkpoint())?;
        if checkpoint.format != FORMAT
            || checkpoint.identity != storage.hash
            || checkpoint.sequence > snapshot
        {
            return Err(corrupt(
                "Local checkpoint format, identity or coverage differs",
            ));
        }
        checkpoint.sequence
    };
    let mut retained = VecDeque::new();
    while let Some(record) = recovery.next().await? {
        let id = record
            .id
            .ok_or_else(|| corrupt("Local command has no retained record identity"))?;
        let (record, remaining): (Record<'_>, _) = postcard::take_from_bytes(&record.data)
            .map_err(|source| wal::Error::Decode { source })?;
        if !remaining.is_empty() {
            return Err(corrupt("Local command has trailing encoded bytes"));
        }
        if record.format != FORMAT
            || record.identity != storage.hash
            || sequence.checked_add(1) != Some(record.sequence)
        {
            return Err(corrupt(
                "Local command format, identity or sequence differs",
            ));
        }
        let command = wal::decode_value::<A::Command>(record.command)?;
        sequence = record.sequence;
        if sequence > snapshot {
            application
                .try_apply(command)
                .map_err(|source| Error::Application { source })?;
            retained.push_back((sequence, id));
        }
    }
    if sequence < snapshot {
        return Err(corrupt(
            "Local snapshot is ahead of its recoverable command chain",
        ));
    }
    Ok((sequence, retained))
}

impl Storage {
    pub async fn snapshot_info(&self) -> Option<SnapshotInfo> {
        self.snapshots.lock().await.current.clone()
    }

    pub async fn reader(&self) -> Result<Option<LocalSnapshotReader>> {
        let snapshots = self.snapshots.lock().await;
        if snapshots.frozen {
            return Err(Error::fatal("Snapshot publication requires recovery"));
        }
        let (Some(info), Some(reference)) = (&snapshots.current, &snapshots.reference) else {
            return Ok(None);
        };
        let permit = self
            .readers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::not_submitted("Snapshot reader budget is exhausted"))?;
        let asset = snapshots
            .assets
            .iter()
            .find(|a| a.reference == *reference)
            .cloned()
            .ok_or_else(|| corrupt("Published snapshot has no protected asset"))?;
        Ok(Some(LocalSnapshotReader {
            info: info.clone(),
            directory: self.directory.clone(),
            asset,
            permit: Arc::new(permit),
            io: Arc::new(Semaphore::new(1)),
            notify: self.notify.clone(),
        }))
    }

    /// None means bounded snapshot generations remain pinned by active readers.
    pub async fn publish<A: Application>(
        &self,
        application: &A,
        sequence: u64,
    ) -> Result<Option<SnapshotInfo>> {
        self.cleanup_snapshots().await?;
        let mut snapshots = self.snapshots.lock().await;
        if snapshots.frozen {
            return Err(Error::fatal("Snapshot publication requires recovery"));
        }
        if snapshots
            .current
            .as_ref()
            .is_some_and(|info| info.position == ExecutionPosition::Local { sequence })
        {
            return Ok(snapshots.current.clone());
        }
        if snapshots.assets.len() >= self.options.max_snapshot_files {
            return Ok(None);
        }
        application
            .validate()
            .map_err(|source| Error::Application { source })?;
        let bytes = wal::codec_frame(
            &wal::encode_value(
                &SnapshotView {
                    format: FORMAT,
                    identity: &self.identity,
                    sequence,
                    application,
                },
                self.options.max_snapshot_bytes,
            )?,
            0,
        )?;
        let id = tidemq_protocol::Id::new_v7().map_err(|e| Error::fatal(e.to_string()))?;
        let reference = SnapshotRef {
            file_name: format!("snapshot-{id}.bin"),
            byte_len: bytes.len() as u64,
            checksum: *blake3::hash(&bytes).as_bytes(),
        };
        let owner = lock(&self.owner)
            .clone()
            .ok_or_else(|| Error::fatal("Local storage is closed"))?;
        let path = self.directory.join(&reference.file_name);
        boundary(&self.directory, "snapshot_write")?;
        let write_owner = owner.clone();
        let io = self
            .io
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::fatal("Snapshot IO admission stopped"))?;
        let directory = self.directory.clone();
        tokio::spawn(async move {
            let (_owner, _io) = (write_owner, io);
            boundary(&directory, "snapshot_write_io")?;
            wal::write_atomic(&path, &bytes).await?;
            Ok::<(), Error>(())
        })
        .await
        .map_err(|error| Error::fatal(format!("Snapshot write task failed: {error}")))??;
        snapshots.assets.push(Arc::new(Asset {
            reference: reference.clone(),
            _owner: owner,
        }));
        // Any publication error may follow rename; preserve both generations until recovery.
        snapshots.frozen = true;
        boundary(&self.directory, "snapshot_publish")?;
        self.journal.publish_snapshot(reference.clone()).await?;
        boundary(&self.directory, "snapshot_published")?;
        let info = SnapshotInfo {
            position: ExecutionPosition::Local { sequence },
            snapshot_id: reference.file_name.clone(),
        };
        snapshots.current = Some(info.clone());
        snapshots.reference = Some(reference);
        snapshots.frozen = false;
        Ok(Some(info))
    }

    pub async fn checkpoint(
        &self,
        sequence: u64,
        retained: &VecDeque<(u64, RecordId)>,
    ) -> Result<()> {
        let control = wal::encode_value(
            &Checkpoint {
                format: FORMAT,
                identity: self.hash,
                sequence,
            },
            self.options.journal.max_checkpoint_bytes,
        )?;
        let ids = retained
            .iter()
            .filter(|(seq, _)| *seq > sequence)
            .map(|(_, id)| *id)
            .collect();
        boundary(&self.directory, "checkpoint")?;
        self.journal.checkpoint(control, ids).await?;
        boundary(&self.directory, "checkpoint_published")?;
        self.journal.cleanup_orphans().await?;
        self.cleanup_snapshots().await
    }

    pub async fn cleanup_snapshots(&self) -> Result<()> {
        let mut snapshots = self.snapshots.lock().await;
        if snapshots.frozen {
            return Err(Error::fatal("Snapshot publication outcome is uncertain"));
        }
        let mut files = fs::read_dir(&self.directory)
            .await
            .map_err(|source| wal::Error::io(&self.directory, source))?;
        let mut removable = Vec::new();
        let mut entries = 0usize;
        while let Some(file) = files
            .next_entry()
            .await
            .map_err(|source| wal::Error::io(&self.directory, source))?
        {
            entries = entries.saturating_add(1);
            if entries > wal::MAX_SEGMENTED_DIRECTORY_ENTRIES {
                return Err(corrupt("Snapshot directory inventory exceeds its bound"));
            }
            let name = file
                .file_name()
                .into_string()
                .map_err(|_| corrupt("Invalid snapshot file name"))?;
            if !matches!(
                wal::segmented_file_kind(&name),
                Some(SegmentedFileKind::Snapshot | SegmentedFileKind::Incoming)
            ) || snapshots
                .reference
                .as_ref()
                .is_some_and(|r| r.file_name == name)
                || snapshots
                    .assets
                    .iter()
                    .any(|a| a.reference.file_name == name && Arc::strong_count(a) > 1)
            {
                continue;
            }
            removable.push(file.path());
        }
        if !removable.is_empty() || snapshots.cleanup_pending {
            snapshots.cleanup_pending = true;
            let owner = lock(&self.owner)
                .clone()
                .ok_or_else(|| Error::fatal("Local storage is closed"))?;
            let directory = self.directory.clone();
            let io = self
                .io
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Error::fatal("Snapshot IO admission stopped"))?;
            tokio::spawn(async move {
                let (_owner, _io) = (owner, io);
                for path in removable {
                    boundary(&directory, "snapshot_cleanup")?;
                    match fs::remove_file(&path).await {
                        Ok(()) => {}
                        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                        Err(source) => return Err(wal::Error::io(&path, source).into()),
                    }
                }
                boundary(&directory, "snapshot_cleanup_sync")?;
                wal::sync_directory(&directory).await?;
                Ok::<(), Error>(())
            })
            .await
            .map_err(|error| Error::fatal(format!("Snapshot cleanup task failed: {error}")))??;
            snapshots.cleanup_pending = false;
        }
        let current = snapshots.reference.clone();
        snapshots
            .assets
            .retain(|a| current.as_ref() == Some(&a.reference) || Arc::strong_count(a) > 1);
        Ok(())
    }

    pub async fn release(&self) {
        let _io = self.io.acquire().await;
        let mut snapshots = self.snapshots.lock().await;
        snapshots.assets.clear();
        snapshots.current = None;
        snapshots.reference = None;
        lock(&self.owner).take();
    }
}

async fn recover_snapshot<A: Application>(
    directory: &Path,
    reference: Option<&SnapshotRef>,
    identity: &StorageIdentity,
    options: &LocalOptions,
) -> Result<Option<Snapshot<A>>> {
    let Some(reference) = reference else {
        return Ok(None);
    };
    let bytes = read_bounded(
        &directory.join(&reference.file_name),
        options.max_snapshot_bytes + wal::FRAME_HEADER_BYTES,
    )
    .await?;
    let snapshot: Snapshot<A> =
        wal::decode_value(wal::unframe(&bytes, options.max_snapshot_bytes)?)?;
    if snapshot.format != FORMAT || snapshot.identity != *identity || snapshot.sequence == 0 {
        return Err(corrupt(
            "Local snapshot format, identity or sequence differs",
        ));
    }
    snapshot
        .application
        .validate()
        .map_err(|source| Error::Application { source })?;
    Ok(Some(snapshot))
}

async fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let file = File::open(path)
        .await
        .map_err(|source| wal::Error::io(path, source))?;
    let len = file
        .metadata()
        .await
        .map_err(|source| wal::Error::io(path, source))?
        .len();
    if len > limit as u64 {
        return Err(corrupt("Local storage value exceeds its bound"));
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len as usize)
        .map_err(|_| Error::fatal("Storage buffer allocation failed"))?;
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| wal::Error::io(path, source))?;
    if bytes.len() > limit {
        return Err(corrupt("Local storage value exceeds its bound"));
    }
    Ok(bytes)
}

fn validate_identity<A: Application>(identity: &StorageIdentity) -> Result<()> {
    if identity.group.group_id == 0
        || identity.node_id == 0
        || identity.group.application != A::NAME
        || identity.group.application_version != A::VERSION
        || A::NAME.is_empty()
        || A::NAME.len() > 128
        || !A::NAME.is_ascii()
    {
        return Err(Error::Configuration {
            reason: "Local application identity differs",
        });
    }
    Ok(())
}

async fn create_directory(directory: &Path) -> Result<()> {
    match fs::create_dir(directory).await {
        Ok(()) => {
            wal::sync_directory(directory.parent().ok_or(Error::Configuration {
                reason: "Local storage parent is missing",
            })?)
            .await?
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(source) => return Err(wal::Error::io(directory, source).into()),
    }
    let metadata = fs::symlink_metadata(directory)
        .await
        .map_err(|source| wal::Error::io(directory, source))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(corrupt("Local storage must be a real directory"));
    }
    Ok(())
}

async fn lock_directory(directory: &Path) -> Result<Arc<std::fs::File>> {
    let path = directory.join(".writer-lock");
    tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| wal::Error::io(&path, source))?;
        file.try_lock()
            .map_err(|source| wal::Error::io(&path, std::io::Error::other(source)))?;
        Ok(Arc::new(file))
    })
    .await
    .map_err(|e| Error::fatal(format!("Local directory lock worker failed: {e}")))?
}

async fn persist_identity(directory: &Path, identity: &StorageIdentity) -> Result<()> {
    let path = directory.join("identity.bin");
    if fs::try_exists(&path)
        .await
        .map_err(|source| wal::Error::io(&path, source))?
    {
        let metadata = fs::symlink_metadata(&path)
            .await
            .map_err(|source| wal::Error::io(&path, source))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(corrupt("Local identity must be a regular file"));
        }
        let bytes = read_bounded(&path, IDENTITY_LIMIT + wal::FRAME_HEADER_BYTES).await?;
        let stored: Identity = wal::decode_value(wal::unframe(&bytes, IDENTITY_LIMIT)?)?;
        if stored.format != FORMAT || stored.identity != *identity {
            return Err(corrupt("Local storage identity or format differs"));
        }
        return Ok(());
    }
    let mut entries = fs::read_dir(directory)
        .await
        .map_err(|source| wal::Error::io(directory, source))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|source| wal::Error::io(directory, source))?
    {
        if entry.file_name() != ".writer-lock" {
            return Err(corrupt("Nonempty local storage is missing its identity"));
        }
    }
    let bytes = wal::codec_frame(
        &wal::encode_value(
            &Identity {
                format: FORMAT,
                identity: identity.clone(),
            },
            IDENTITY_LIMIT,
        )?,
        0,
    )?;
    wal::write_atomic(&path, &bytes).await?;
    Ok(())
}

pub(super) fn boundary(_directory: &Path, _stage: &'static str) -> Result<()> {
    #[cfg(test)]
    super::tests::boundary(_directory, _stage)?;
    Ok(())
}
