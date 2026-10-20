use super::format::*;
use super::{Error, JournalOptions, Result};
use crate::wal::codec::{frame, parse_header, validate_hash};
use crate::wal::{FRAME_HEADER_BYTES, decode_value, encode_value, sync_directory, unframe};
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub(super) struct DiskFile {
    pub name: String,
    pub bytes: u64,
    pub kind: SegmentedFileKind,
    pub gc_eligible: bool,
}
pub(super) async fn directory_files(path: &Path) -> Result<Vec<DiskFile>> {
    let meta = fs::symlink_metadata(path)
        .await
        .map_err(|e| Error::io(path, e))?;
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Err(corrupt("Storage directory is not a real directory"));
    }
    let mut entries = fs::read_dir(path).await.map_err(|e| Error::io(path, e))?;
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(|e| Error::io(path, e))? {
        if files.len() >= MAX_SEGMENTED_DIRECTORY_ENTRIES {
            return Err(Error::Limit {
                name: "storage directory entries",
            });
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| corrupt("Storage file name is not UTF-8"))?;
        let kind = segmented_file_kind(&name).ok_or(corrupt("Unknown or legacy storage file"))?;
        let metadata = fs::symlink_metadata(entry.path())
            .await
            .map_err(|e| Error::io(entry.path(), e))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(corrupt("Storage entry is not a regular file"));
        }
        files.push(DiskFile {
            name,
            bytes: metadata.len(),
            kind,
            gc_eligible: false,
        });
    }
    Ok(files)
}

pub(super) async fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let file = File::open(path).await.map_err(|e| Error::io(path, e))?;
    let len = file.metadata().await.map_err(|e| Error::io(path, e))?.len();
    if len > limit as u64 {
        return Err(Error::Limit {
            name: "bounded storage file",
        });
    }
    let mut result = Vec::new();
    result
        .try_reserve_exact(len as usize)
        .map_err(|_| Error::Limit {
            name: "storage file allocation",
        })?;
    file.take(limit as u64 + 1)
        .read_to_end(&mut result)
        .await
        .map_err(|e| Error::io(path, e))?;
    if result.len() > limit {
        return Err(Error::Limit {
            name: "bounded storage file",
        });
    }
    Ok(result)
}
pub(super) async fn read_value<T: DeserializeOwned>(path: &Path, limit: usize) -> Result<T> {
    let bytes = read_bounded(path, limit.saturating_add(FRAME_HEADER_BYTES)).await?;
    decode_value(unframe(&bytes, limit)?)
}
pub(super) fn value_frame<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>> {
    frame(&encode_value(value, limit)?, 0)
}
pub(super) async fn create_file(path: &Path, bytes: &[u8]) -> Result<File> {
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
        .await
        .map_err(|e| Error::io(path, e))?;
    file.write_all(bytes)
        .await
        .map_err(|e| Error::io(path, e))?;
    file.sync_all().await.map_err(|e| Error::io(path, e))?;
    Ok(file)
}
pub(super) async fn publish_manifest(directory: &Path, manifest: &Manifest) -> Result<u64> {
    let bytes = value_frame(manifest, MAX_MANIFEST_BYTES)?;
    let temporary = directory.join(format!(".replace-{}", uuid::Uuid::from_bytes(new_id()?)));
    let _file = create_file(&temporary, &bytes).await?;
    #[cfg(test)]
    super::fault_tests::boundary("manifest_temp_sync");
    let path = directory.join(SEGMENTED_MANIFEST_FILE);
    fs::rename(&temporary, &path)
        .await
        .map_err(|e| Error::io(&path, e))?;
    #[cfg(test)]
    super::fault_tests::boundary("manifest_rename");
    sync_directory(directory).await?;
    #[cfg(test)]
    super::fault_tests::boundary("manifest_directory_sync");
    Ok(bytes.len() as u64)
}
pub(super) async fn hash_file(path: &Path, expected: u64) -> Result<[u8; 32]> {
    let mut file = File::open(path).await.map_err(|e| Error::io(path, e))?;
    if file.metadata().await.map_err(|e| Error::io(path, e))?.len() != expected {
        return Err(corrupt("Referenced storage file length differs"));
    }
    let mut hash = blake3::Hasher::new();
    let mut remaining = expected;
    let mut buffer = [0u8; 65_536];
    while remaining != 0 {
        let count =
            usize::try_from(remaining.min(buffer.len() as u64)).map_err(|_| Error::Limit {
                name: "storage read buffer",
            })?;
        let output = buffer
            .get_mut(..count)
            .ok_or(corrupt("Invalid bounded read"))?;
        file.read_exact(output)
            .await
            .map_err(|e| Error::io(path, e))?;
        hash.update(output);
        remaining -= count as u64;
    }
    Ok(*hash.finalize().as_bytes())
}
pub(super) async fn validate_snapshot(
    directory: &Path,
    snapshot: &super::SnapshotRef,
    synchronize: bool,
) -> Result<()> {
    if segmented_file_kind(&snapshot.file_name) != Some(SegmentedFileKind::Snapshot) {
        return Err(corrupt("Invalid snapshot reference name"));
    }
    let path = directory.join(&snapshot.file_name);
    if hash_file(&path, snapshot.byte_len).await? != snapshot.checksum {
        return Err(corrupt("Snapshot reference checksum differs"));
    }
    if !synchronize {
        return Ok(());
    }
    File::open(&path)
        .await
        .map_err(|e| Error::io(&path, e))?
        .sync_all()
        .await
        .map_err(|e| Error::io(&path, e))?;
    sync_directory(directory).await
}
pub(super) fn segment_header(identity: [u8; 32], segment: &Segment) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(SEGMENT_HEADER_PAYLOAD);
    bytes.extend_from_slice(b"TIDESEG2");
    bytes.extend_from_slice(&LAYOUT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&identity);
    bytes.extend_from_slice(&segment.id);
    bytes.extend_from_slice(&segment.first_sequence.to_le_bytes());
    bytes.push(u8::from(segment.reference_only));
    frame(&bytes, 0)
}
pub(super) async fn validate_segment_header(
    path: &Path,
    identity: [u8; 32],
    segment: &Segment,
) -> Result<()> {
    let mut file = File::open(path).await.map_err(|e| Error::io(path, e))?;
    let mut bytes = vec![0; SEGMENT_HEADER_BYTES as usize];
    file.read_exact(&mut bytes)
        .await
        .map_err(|e| Error::io(path, e))?;
    if bytes != segment_header(identity, segment)? {
        return Err(corrupt(
            "Segment header identity, version, or sequence differs",
        ));
    }
    Ok(())
}
pub(super) async fn create_segment(
    directory: &Path,
    identity: [u8; 32],
    sequence: u64,
) -> Result<(Segment, File)> {
    let segment = Segment {
        id: new_id()?,
        first_sequence: sequence,
        reference_only: false,
        records: 0,
        byte_len: SEGMENT_HEADER_BYTES,
        checksum: [0; 32],
    };
    let file = create_file(
        &directory.join(segment.name()),
        &segment_header(identity, &segment)?,
    )
    .await?;
    #[cfg(test)]
    super::fault_tests::boundary("segment_sync");
    sync_directory(directory).await?;
    #[cfg(test)]
    super::fault_tests::boundary("segment_directory_sync");
    Ok((segment, file))
}

pub(super) struct ReadFrame {
    pub header: [u8; FRAME_HEADER_BYTES],
    pub sequence: u64,
    pub payload: Vec<u8>,
    pub bytes: u64,
}
pub(super) async fn read_frame(
    file: &mut File,
    path: &Path,
    offset: u64,
    end: u64,
    options: &JournalOptions,
) -> Result<Option<ReadFrame>> {
    let remaining = end
        .checked_sub(offset)
        .ok_or(corrupt("Invalid segment offset"))?;
    if remaining < FRAME_HEADER_BYTES as u64 {
        return Ok(None);
    }
    file.seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(|e| Error::io(path, e))?;
    let mut header = [0; FRAME_HEADER_BYTES];
    file.read_exact(&mut header)
        .await
        .map_err(|e| Error::io(path, e))?;
    let mut limits = options.clone();
    limits.max_record_bytes =
        limits
            .max_record_bytes
            .checked_add(ENVELOPE_BYTES)
            .ok_or(Error::Limit {
                name: "record envelope",
            })?;
    let (len, sequence) = parse_header(&header, offset, &limits)?;
    let bytes = len
        .checked_add(FRAME_HEADER_BYTES as u64)
        .ok_or(Error::Limit {
            name: "frame length",
        })?;
    if bytes > remaining {
        return Ok(None);
    }
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(len as usize)
        .map_err(|_| Error::Limit {
            name: "record buffer",
        })?;
    payload.resize(len as usize, 0);
    file.read_exact(&mut payload)
        .await
        .map_err(|e| Error::io(path, e))?;
    validate_hash(&header, &payload, offset)?;
    Ok(Some(ReadFrame {
        header,
        sequence,
        payload,
        bytes,
    }))
}
pub(super) fn decode_record(payload: Vec<u8>) -> Result<super::RecoveredRecord> {
    let marker = payload
        .first()
        .copied()
        .ok_or(corrupt("Missing record envelope"))?;
    let id_bytes: [u8; 8] = payload
        .get(1..ENVELOPE_BYTES)
        .and_then(|v| v.try_into().ok())
        .ok_or(corrupt("Incomplete record envelope"))?;
    let id = u64::from_le_bytes(id_bytes);
    let id = match (marker, id) {
        (0, 0) => None,
        (1, id) if id != 0 => Some(RecordId(id)),
        _ => return Err(corrupt("Invalid record identity")),
    };
    let data = payload.into_iter().skip(ENVELOPE_BYTES).collect();
    Ok(super::RecoveredRecord { id, data })
}
pub(super) fn engine_file(kind: SegmentedFileKind) -> bool {
    matches!(
        kind,
        SegmentedFileKind::Manifest
            | SegmentedFileKind::Segment
            | SegmentedFileKind::Checkpoint
            | SegmentedFileKind::Staging
    )
}
pub(super) fn segment_path(directory: &Path, id: [u8; 16]) -> PathBuf {
    directory.join(segment_name(id))
}
