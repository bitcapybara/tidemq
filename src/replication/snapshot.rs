use std::future::Future;
use std::io::{self, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use openraft::{BasicNode, Snapshot, SnapshotMeta};
use serde::{Deserialize, Serialize};
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ReadBuf,
};
use tokio::task::JoinHandle;

use crate::wal::{self, Error, FRAME_HEADER_BYTES, Result, SnapshotRef};

use super::snapshot_registry::{SnapshotAsset, SnapshotLease};
use super::store::{Applied, FORMAT_VERSION};
use super::{Application, GroupIdentity, StorageIdentity, StorageOptions, TypeConfig};

const IO_BYTES: usize = 64 * 1024;
type FileOperation = JoinHandle<io::Result<Option<Vec<u8>>>>;

/// A bounded seekable file; each blocking operation retains its reader/incoming ownership.
#[derive(Debug)]
pub struct SnapshotFile {
    file: Arc<std::fs::File>,
    runtime: tokio::runtime::Handle,
    pending: Option<FileOperation>,
    buffered: Vec<u8>,
    buffered_at: usize,
    position: u64,
    length: u64,
    limit: u64,
    lease: Option<Arc<SnapshotLease>>,
}

impl SnapshotFile {
    pub(crate) async fn open(
        path: &Path,
        limit: u64,
        create: bool,
        lease: Option<Arc<SnapshotLease>>,
    ) -> Result<Self> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| Error::Writer {
            reason: format!("snapshot IO runtime is unavailable: {error}"),
        })?;
        let path = path.to_owned();
        let operation_lease = lease.clone();
        let (file, length) = runtime
            .spawn_blocking(move || {
                let _lease = operation_lease;
                if !create {
                    let metadata = std::fs::symlink_metadata(&path)
                        .map_err(|error| Error::io(&path, error))?;
                    if !metadata.is_file() {
                        return Err(Error::Configuration {
                            reason: "snapshot must be a regular file without a symbolic link",
                        });
                    }
                }
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(create)
                    .create_new(create)
                    .open(&path)
                    .map_err(|error| Error::io(&path, error))?;
                let length = file
                    .metadata()
                    .map_err(|error| Error::io(&path, error))?
                    .len();
                if length > limit {
                    return Err(Error::Limit {
                        name: "snapshot file",
                    });
                }
                Ok::<_, Error>((Arc::new(file), length))
            })
            .await
            .map_err(|error| Error::Writer {
                reason: format!("snapshot open worker failed: {error}"),
            })??;
        Ok(Self {
            file,
            runtime,
            pending: None,
            buffered: Vec::new(),
            buffered_at: 0,
            position: 0,
            length,
            limit,
            lease,
        })
    }

    pub(crate) async fn read_bytes(&mut self, limit: usize) -> Result<Vec<u8>> {
        if self.length > limit as u64 {
            return Err(Error::Limit {
                name: "received snapshot",
            });
        }
        self.flush()
            .await
            .map_err(|source| Error::io("snapshot", source))?;
        self.seek(SeekFrom::Start(0))
            .await
            .map_err(|source| Error::io("snapshot", source))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(usize::try_from(self.length).map_err(|_| Error::Limit {
                name: "snapshot read buffer",
            })?)
            .map_err(|_| Error::Limit {
                name: "snapshot read buffer",
            })?;
        self.take((limit as u64).saturating_add(1))
            .read_to_end(&mut bytes)
            .await
            .map_err(|source| Error::io("snapshot", source))?;
        if bytes.len() > limit {
            return Err(Error::Limit {
                name: "received snapshot",
            });
        }
        Ok(bytes)
    }

    fn queue(
        &mut self,
        operation: impl FnOnce(&std::fs::File) -> io::Result<Option<Vec<u8>>> + Send + 'static,
    ) {
        let file = self.file.clone();
        let lease = self.lease.clone();
        self.pending = Some(self.runtime.spawn_blocking(move || {
            let _lease = lease;
            operation(&file)
        }));
    }

    fn poll_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(pending) = self.pending.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        let result = ready!(Pin::new(pending).poll(cx));
        self.pending = None;
        let buffer = result.map_err(io::Error::other)??;
        if let Some(buffer) = buffer {
            self.buffered = buffer;
            self.buffered_at = 0;
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for SnapshotFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            ready!(this.poll_pending(cx))?;
            let remaining = this
                .buffered
                .get(this.buffered_at..)
                .ok_or_else(|| io::Error::other("Snapshot read buffer position is invalid"))?;
            if !remaining.is_empty() {
                let length = remaining.len().min(buf.remaining());
                let bytes = remaining
                    .get(..length)
                    .ok_or_else(|| io::Error::other("Snapshot read buffer is invalid"))?;
                buf.put_slice(bytes);
                this.buffered_at = this.buffered_at.saturating_add(length);
                this.position = this.position.saturating_add(length as u64);
                return Poll::Ready(Ok(()));
            }
            if this.position >= this.length {
                return Poll::Ready(Ok(()));
            }
            let length = buf.remaining().min(IO_BYTES).min(
                usize::try_from(this.length.saturating_sub(this.position)).unwrap_or(usize::MAX),
            );
            let at = this.position;
            this.queue(move |mut file| {
                file.seek(SeekFrom::Start(at))?;
                let mut bytes = Vec::new();
                bytes.try_reserve_exact(length).map_err(io::Error::other)?;
                bytes.resize(length, 0);
                let length = file.read(&mut bytes)?;
                if length == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Snapshot file ended before its recorded length",
                    ));
                }
                bytes.truncate(length);
                Ok(Some(bytes))
            });
        }
    }
}

impl AsyncWrite for SnapshotFile {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        ready!(this.poll_pending(cx))?;
        if bytes.len() as u64 > this.limit.saturating_sub(this.position) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Snapshot write exceeds its byte limit",
            )));
        }
        let length = bytes.len().min(IO_BYTES);
        if length == 0 {
            return Poll::Ready(Ok(0));
        }
        let mut copy = Vec::new();
        copy.try_reserve_exact(length).map_err(io::Error::other)?;
        copy.extend_from_slice(
            bytes
                .get(..length)
                .ok_or_else(|| io::Error::other("Snapshot write buffer is invalid"))?,
        );
        let at = this.position;
        this.buffered.clear();
        this.buffered_at = 0;
        this.queue(move |mut file| {
            file.seek(SeekFrom::Start(at))?;
            file.write_all(&copy)?;
            Ok(None)
        });
        this.position = this.position.saturating_add(length as u64);
        this.length = this.length.max(this.position);
        Poll::Ready(Ok(length))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_pending(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_pending(cx)
    }
}

impl AsyncSeek for SnapshotFile {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        if this.pending.is_some() {
            return Err(io::Error::other(
                "A snapshot operation is pending; poll completion before seeking",
            ));
        }
        let target = match position {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(delta) => this.position.checked_add_signed(delta),
            SeekFrom::End(delta) => this.length.checked_add_signed(delta),
        }
        .filter(|position| *position <= this.limit)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Snapshot seek exceeds its byte limit",
            )
        })?;
        this.buffered.clear();
        this.buffered_at = 0;
        this.queue(move |mut file| {
            file.seek(SeekFrom::Start(target))?;
            Ok(None)
        });
        this.position = target;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        ready!(this.poll_pending(cx))?;
        Poll::Ready(Ok(this.position))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub(crate) struct Envelope<A: Application> {
    pub format: u32,
    pub group: GroupIdentity,
    pub meta: SnapshotMeta<u64, BasicNode>,
    pub application: A,
}

pub(crate) async fn read_single(path: &Path, max_payload_bytes: usize) -> Result<Vec<u8>> {
    let maximum = max_payload_bytes
        .checked_add(FRAME_HEADER_BYTES)
        .ok_or(Error::Limit {
            name: "snapshot file limit",
        })?;
    let mut file = SnapshotFile::open(path, maximum as u64, false, None).await?;
    let bytes = file.read_bytes(maximum).await?;
    Ok(wal::unframe(&bytes, max_payload_bytes)?.to_vec())
}

pub(crate) async fn load<A: Application>(
    directory: &Path,
    identity: &StorageIdentity,
    options: &StorageOptions,
    reference: Option<&SnapshotRef>,
) -> Result<(Applied<A>, Option<SnapshotMeta<u64, BasicNode>>)> {
    let Some(reference) = reference else {
        return Ok((Applied::default(), None));
    };
    if wal::segmented_file_kind(&reference.file_name) != Some(wal::SegmentedFileKind::Snapshot) {
        return Err(Error::Corrupt {
            offset: 0,
            reason: "manifest snapshot name is invalid",
        });
    }
    let maximum = options
        .max_snapshot_bytes
        .checked_add(FRAME_HEADER_BYTES)
        .ok_or(Error::Limit {
            name: "snapshot file limit",
        })?;
    let mut file = SnapshotFile::open(
        &directory.join(&reference.file_name),
        maximum as u64,
        false,
        None,
    )
    .await?;
    let bytes = file.read_bytes(maximum).await?;
    if bytes.len() as u64 != reference.byte_len
        || *blake3::hash(&bytes).as_bytes() != reference.checksum
    {
        return Err(Error::Corrupt {
            offset: 0,
            reason: "manifest snapshot length or checksum differs",
        });
    }
    let identity = identity.clone();
    let maximum = options.max_snapshot_bytes;
    tokio::task::spawn_blocking(move || {
        let payload = wal::unframe(&bytes, maximum)?;
        let envelope: Envelope<A> = wal::decode_value(payload)?;
        validate(&envelope, &identity)?;
        let meta = envelope.meta;
        Ok((
            Applied {
                failed: false,
                application: envelope.application,
                last_applied: meta.last_log_id,
                membership: meta.last_membership.clone(),
            },
            Some(meta),
        ))
    })
    .await
    .map_err(|error| Error::Writer {
        reason: format!("snapshot recovery worker failed: {error}"),
    })?
}

pub(crate) fn validate<A: Application>(
    envelope: &Envelope<A>,
    identity: &StorageIdentity,
) -> Result<()> {
    if envelope.format != FORMAT_VERSION || envelope.group != identity.group {
        return Err(Error::Configuration {
            reason: "snapshot group, application, or storage format does not match",
        });
    }
    if envelope.meta.snapshot_id.is_empty()
        || envelope.meta.snapshot_id.len() > 256
        || envelope.meta.last_membership.log_id() > &envelope.meta.last_log_id
    {
        return Err(Error::Corrupt {
            offset: 0,
            reason: "snapshot membership or applied metadata is inconsistent",
        });
    }
    envelope
        .application
        .validate()
        .map_err(|error| Error::io("snapshot application", error))
}

pub(crate) async fn persist(
    directory: &Path,
    bytes: Vec<u8>,
    asset: Arc<SnapshotAsset>,
) -> Result<()> {
    let directory = directory.to_owned();
    tokio::task::spawn_blocking(move || {
        let path = directory.join(&asset.reference.file_name);
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|error| Error::io(&path, error))?;
        file.write_all(&bytes)
            .map_err(|error| Error::io(&path, error))?;
        file.sync_all().map_err(|error| Error::io(&path, error))?;
        std::fs::File::open(&directory)
            .and_then(|file| file.sync_all())
            .map_err(|error| Error::io(directory, error))
    })
    .await
    .map_err(|error| Error::Writer {
        reason: format!("snapshot persistence worker failed: {error}"),
    })?
}

pub(crate) async fn open_snapshot<A: Application>(
    directory: &Path,
    reference: &SnapshotRef,
    meta: SnapshotMeta<u64, BasicNode>,
    options: &StorageOptions,
    lease: Arc<SnapshotLease>,
) -> Result<Snapshot<TypeConfig<A>>> {
    let maximum = options
        .max_snapshot_bytes
        .checked_add(FRAME_HEADER_BYTES)
        .ok_or(Error::Limit {
            name: "snapshot file limit",
        })?;
    Ok(Snapshot {
        meta,
        snapshot: Box::new(
            SnapshotFile::open(
                &directory.join(&reference.file_name),
                maximum as u64,
                false,
                Some(lease),
            )
            .await?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::super::snapshot_registry::SnapshotRegistry;
    use super::*;

    async fn fixture() -> (
        std::path::PathBuf,
        Arc<std::fs::File>,
        SnapshotRef,
        SnapshotRegistry,
    ) {
        let directory = std::env::temp_dir().join(format!(
            "tidemq-snapshot-io-{}",
            tidemq_protocol::Id::random().unwrap()
        ));
        tokio::fs::create_dir(&directory).await.unwrap();
        for name in ["identity.bin", "manifest.bin"] {
            tokio::fs::write(directory.join(name), [1; 7])
                .await
                .unwrap();
        }
        let segment = format!("segment-{}.wal", tidemq_protocol::Id::random().unwrap());
        tokio::fs::write(directory.join(segment), [2; 11])
            .await
            .unwrap();
        let owner = Arc::new(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(directory.join(".writer-lock"))
                .unwrap(),
        );
        owner.try_lock().unwrap();
        let reference = reference(&[3; 32]);
        tokio::fs::write(directory.join(&reference.file_name), [3; 32])
            .await
            .unwrap();
        let options = StorageOptions {
            max_snapshot_files: 2,
            max_snapshot_readers: 1,
            ..StorageOptions::default()
        };
        let registry = SnapshotRegistry::new(Some(reference.clone()), &options, owner.clone());
        (directory, owner, reference, registry)
    }

    fn reference(bytes: &[u8]) -> SnapshotRef {
        SnapshotRef {
            file_name: format!("snapshot-{}.bin", tidemq_protocol::Id::random().unwrap()),
            byte_len: bytes.len() as u64,
            checksum: *blake3::hash(bytes).as_bytes(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_reader_io_keeps_retired_file_and_capacity_charge_until_completion() {
        let (directory, owner, first, mut registry) = fixture().await;
        let lease = registry
            .lease(&first, registry.reserve_reader().unwrap())
            .unwrap();
        let mut file =
            SnapshotFile::open(&directory.join(&first.file_name), 1024, false, Some(lease))
                .await
                .unwrap();
        let second = reference(&[4; 64]);
        let candidate = registry.candidate(second).unwrap();
        persist(&directory, vec![4; 64], candidate.clone())
            .await
            .unwrap();
        registry.commit(&candidate);
        drop(candidate);
        let before = crate::runtime::physical::replica_bytes(&directory, true).unwrap();
        let (started, reached) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        file.queue(move |_| {
            started.send(()).unwrap();
            blocked.recv().unwrap();
            Ok(None)
        });
        let pending = file.pending.take().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), reached)
            .await
            .unwrap()
            .unwrap();
        drop(file);
        registry.cleanup(&directory).await.unwrap();
        assert_eq!(registry.diagnostics().snapshot_readers, 1);
        assert!(registry.reserve_reader().is_err());
        assert!(registry.candidate(reference(&[5; 8])).is_err());
        assert!(
            tokio::fs::try_exists(directory.join(&first.file_name))
                .await
                .unwrap()
        );
        assert_eq!(
            crate::runtime::physical::replica_bytes(&directory, true).unwrap(),
            before
        );
        release.send(()).unwrap();
        pending.await.unwrap().unwrap();
        registry.cleanup(&directory).await.unwrap();
        assert_eq!(registry.diagnostics().snapshot_readers, 0);
        assert!(
            !tokio::fs::try_exists(directory.join(&first.file_name))
                .await
                .unwrap()
        );
        assert_eq!(
            crate::runtime::physical::replica_bytes(&directory, true).unwrap(),
            before - first.byte_len
        );
        drop((registry, owner));
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pending_io_owns_the_directory_lock_after_file_and_registry_are_dropped() {
        let (directory, owner, first, registry) = fixture().await;
        let lease = registry
            .lease(&first, registry.reserve_reader().unwrap())
            .unwrap();
        let mut file =
            SnapshotFile::open(&directory.join(first.file_name), 1024, false, Some(lease))
                .await
                .unwrap();
        let (started, reached) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        file.queue(move |_| {
            started.send(()).unwrap();
            blocked.recv().unwrap();
            Ok(None)
        });
        let pending = file.pending.take().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), reached)
            .await
            .unwrap()
            .unwrap();
        drop((file, registry, owner));
        let contender = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.join(".writer-lock"))
            .unwrap();
        assert!(contender.try_lock().is_err());
        release.send(()).unwrap();
        pending.await.unwrap().unwrap();
        contender.try_lock().unwrap();
        drop(contender);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn buffered_snapshot_io_preserves_seek_and_write_boundaries() {
        let directory = std::env::temp_dir().join(format!(
            "tidemq-snapshot-seek-{}",
            tidemq_protocol::Id::random().unwrap()
        ));
        tokio::fs::create_dir(&directory).await.unwrap();
        let mut file = SnapshotFile::open(&directory.join("file.bin"), 4, true, None)
            .await
            .unwrap();
        file.write_all(b"abc").await.unwrap();
        file.seek(SeekFrom::Start(1)).await.unwrap();
        let mut bytes = [0; 2];
        file.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"bc");
        file.seek(SeekFrom::Current(-1)).await.unwrap();
        file.write_all(b"Z").await.unwrap();
        assert_eq!(file.read_bytes(4).await.unwrap(), b"abZ");
        assert!(file.seek(SeekFrom::Start(5)).await.is_err());
        assert!(file.write_all(b"large").await.is_err());
        drop(file);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}
