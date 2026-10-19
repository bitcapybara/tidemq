use std::io::{self, SeekFrom};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use openraft::{BasicNode, Snapshot, SnapshotMeta};
use serde::{Deserialize, Serialize};
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, ReadBuf};

use crate::wal::{self, Error, FRAME_HEADER_BYTES, Result};

use super::store::{Applied, FORMAT_VERSION, SNAPSHOT_FILE};
use super::{Application, GroupIdentity, StorageIdentity, StorageOptions, TypeConfig};

/// A seekable snapshot file whose reads, writes, and seeks are bounded before IO.
#[derive(Debug)]
pub struct SnapshotFile {
    file: File,
    position: u64,
    length: u64,
    limit: u64,
}

impl SnapshotFile {
    pub(crate) async fn open(path: &Path, limit: u64, create: bool) -> Result<Self> {
        let file = if create {
            fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(path)
                .await
        } else {
            fs::OpenOptions::new().read(true).open(path).await
        }
        .map_err(|error| Error::io(path, error))?;
        let length = file
            .metadata()
            .await
            .map_err(|error| Error::io(path, error))?
            .len();
        if length > limit {
            return Err(Error::Limit {
                name: "snapshot file",
            });
        }
        Ok(Self {
            file,
            position: 0,
            length,
            limit,
        })
    }

    pub(crate) async fn read_bytes(&mut self, limit: usize) -> Result<Vec<u8>> {
        if self.length > limit as u64 {
            return Err(Error::Limit {
                name: "received snapshot",
            });
        }
        self.seek(SeekFrom::Start(0))
            .await
            .map_err(|source| Error::io("snapshot", source))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.length as usize)
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
}

impl AsyncRead for SnapshotFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        match Pin::new(&mut this.file).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let read = buf.filled().len().saturating_sub(before);
                this.position = this.position.saturating_add(read as u64);
                Poll::Ready(Ok(()))
            }
            other => other,
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
        if bytes.len() as u64 > this.limit.saturating_sub(this.position) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Snapshot write exceeds its byte limit",
            )));
        }
        match Pin::new(&mut this.file).poll_write(cx, bytes) {
            Poll::Ready(Ok(written)) => {
                this.position = this.position.saturating_add(written as u64);
                this.length = this.length.max(this.position);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_shutdown(cx)
    }
}

impl AsyncSeek for SnapshotFile {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
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
        Pin::new(&mut this.file).start_seek(SeekFrom::Start(target))
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        match Pin::new(&mut this.file).poll_complete(cx) {
            Poll::Ready(Ok(position)) => {
                this.position = position;
                Poll::Ready(Ok(position))
            }
            other => other,
        }
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
    let mut file = SnapshotFile::open(path, maximum as u64, false).await?;
    let bytes = file.read_bytes(maximum).await?;
    Ok(wal::unframe(&bytes, max_payload_bytes)?.to_vec())
}

pub(crate) async fn load<A: Application>(
    directory: &Path,
    identity: &StorageIdentity,
    options: &StorageOptions,
) -> Result<(Applied<A>, Option<SnapshotMeta<u64, BasicNode>>)> {
    let path = directory.join(SNAPSHOT_FILE);
    if !fs::try_exists(&path)
        .await
        .map_err(|e| Error::io(&path, e))?
    {
        return Ok((Applied::default(), None));
    }
    let bytes = read_single(&path, options.max_snapshot_bytes).await?;
    let envelope: Envelope<A> = wal::decode_value(&bytes)?;
    validate(&envelope, identity)?;
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

pub(crate) async fn open_snapshot<A: Application>(
    path: &Path,
    meta: SnapshotMeta<u64, BasicNode>,
    options: &StorageOptions,
) -> Result<Snapshot<TypeConfig<A>>> {
    Ok(Snapshot {
        meta,
        snapshot: Box::new(
            SnapshotFile::open(
                path,
                options
                    .max_snapshot_bytes
                    .saturating_add(FRAME_HEADER_BYTES) as u64,
                false,
            )
            .await?,
        ),
    })
}
