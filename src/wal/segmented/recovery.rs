use super::disk::*;
use super::format::*;
use super::{Error, JournalOptions, RecoveredRecord, Result, SegmentedInspection};
use crate::wal::sync_directory;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::sync::OwnedSemaphorePermit;

pub struct Recovery {
    control: Vec<u8>,
    references: VecDeque<Location>,
    tail: VecDeque<Segment>,
    reader: Option<TailReader>,
    directory: PathBuf,
    options: JournalOptions,
    _permit: Option<OwnedSemaphorePermit>,
    _owner: Option<Arc<std::fs::File>>,
}
struct TailReader {
    file: File,
    path: PathBuf,
    offset: u64,
    end: u64,
}
impl Recovery {
    pub fn checkpoint(&self) -> &[u8] {
        &self.control
    }
    pub async fn next(&mut self) -> Result<Option<RecoveredRecord>> {
        if let Some(location) = self.references.pop_front() {
            return read_location(&self.directory, &location, &self.options)
                .await
                .map(Some);
        }
        loop {
            if let Some(reader) = &mut self.reader {
                if reader.offset < reader.end {
                    let record = read_frame(
                        &mut reader.file,
                        &reader.path,
                        reader.offset,
                        reader.end,
                        &self.options,
                    )
                    .await?
                    .ok_or(corrupt("Validated recovery tail became incomplete"))?;
                    reader.offset =
                        reader
                            .offset
                            .checked_add(record.bytes)
                            .ok_or(Error::Limit {
                                name: "recovery offset",
                            })?;
                    return decode_record(record.payload).map(Some);
                }
                self.reader = None;
            }
            let Some(segment) = self.tail.pop_front() else {
                self._permit.take();
                self._owner.take();
                return Ok(None);
            };
            let path = self.directory.join(segment.name());
            let file = File::open(&path).await.map_err(|e| Error::io(&path, e))?;
            self.reader = Some(TailReader {
                file,
                path,
                offset: SEGMENT_HEADER_BYTES,
                end: segment.byte_len,
            });
        }
    }
}

pub(super) struct Loaded {
    pub manifest: Manifest,
    pub index: BTreeMap<RecordId, Location>,
    pub recovery: Recovery,
    pub inspection: SegmentedInspection,
    pub sequence: u64,
    pub next_record_id: u64,
    pub physical_bytes: u64,
    pub retired: Vec<DiskFile>,
    pub manifest_bytes: u64,
    pub active_hash: blake3::Hasher,
}

pub(super) async fn load(
    directory: &Path,
    options: &JournalOptions,
    identity: [u8; 32],
    repair: bool,
    permit: Option<OwnedSemaphorePermit>,
    owner: Option<Arc<std::fs::File>>,
) -> Result<Loaded> {
    let files = directory_files(directory).await?;
    let manifest_path = directory.join(SEGMENTED_MANIFEST_FILE);
    let mut manifest: Manifest = read_value(&manifest_path, MAX_MANIFEST_BYTES).await?;
    validate_manifest(&manifest, options, identity)?;
    if let Some(snapshot) = &manifest.snapshot {
        validate_snapshot(directory, snapshot).await?;
    }
    let checkpoint = load_checkpoint(directory, &manifest, options).await?;
    let mut index = BTreeMap::new();
    for location in &checkpoint.references {
        if location.offset < SEGMENT_HEADER_BYTES || location.sequence >= manifest.replay_sequence {
            return Err(corrupt(
                "Checkpoint reference crosses the physical replay barrier",
            ));
        }
        if !manifest.sealed.iter().any(|s| s.id == location.segment) {
            return Err(corrupt("Checkpoint references a missing sealed segment"));
        }
        if index.insert(location.id, location.clone()).is_some() {
            return Err(corrupt("Duplicate checkpoint record identity"));
        }
        read_location(directory, location, options).await?;
    }
    let mut inspection = SegmentedInspection {
        active_segment: manifest.active.name(),
        sealed_segments: manifest.sealed.iter().map(Segment::name).collect(),
        ..Default::default()
    };
    let mut tail = VecDeque::new();
    let mut expected = manifest.replay_sequence;
    let mut next_record_id = manifest.next_record_id;
    let mut active_hash = blake3::Hasher::new();
    let segments = manifest
        .sealed
        .iter()
        .chain(std::iter::once(&manifest.active))
        .cloned()
        .collect::<Vec<_>>();
    for segment in &segments {
        let active = segment.id == manifest.active.id;
        let scan = scan_segment(
            directory,
            segment,
            identity,
            options,
            active,
            ScanIndex {
                index: &mut index,
                next_id: &mut next_record_id,
            },
            manifest.replay_sequence,
        )
        .await?;
        inspection.file_bytes =
            inspection
                .file_bytes
                .checked_add(scan.file_bytes)
                .ok_or(Error::Limit {
                    name: "segment total",
                })?;
        inspection.valid_bytes =
            inspection
                .valid_bytes
                .checked_add(scan.valid_bytes)
                .ok_or(Error::Limit {
                    name: "valid segment total",
                })?;
        inspection.records = inspection
            .records
            .checked_add(scan.records)
            .ok_or(Error::Limit {
                name: "record total",
            })?;
        if !segment.reference_only && segment.first_sequence >= manifest.replay_sequence {
            if segment.first_sequence != expected {
                return Err(corrupt("Physical replay sequence is not contiguous"));
            }
            expected = expected.checked_add(scan.records).ok_or(Error::Limit {
                name: "physical sequence",
            })?;
            let mut valid = segment.clone();
            valid.byte_len = scan.valid_bytes;
            valid.records = scan.records;
            tail.push_back(valid);
        } else if !segment.reference_only
            && segment
                .first_sequence
                .checked_add(scan.records)
                .is_none_or(|v| v > manifest.replay_sequence)
        {
            return Err(corrupt("Checkpoint barrier splits a sealed segment"));
        }
        if active {
            inspection.incomplete_tail_bytes = scan.file_bytes.saturating_sub(scan.valid_bytes);
            if repair && inspection.incomplete_tail_bytes != 0 {
                let path = directory.join(segment.name());
                let file = fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .await
                    .map_err(|e| Error::io(&path, e))?;
                file.set_len(scan.valid_bytes)
                    .await
                    .map_err(|e| Error::io(&path, e))?;
                file.sync_all().await.map_err(|e| Error::io(&path, e))?;
                sync_directory(directory).await?;
                tracing::warn!(
                    event = "wal_incomplete_tail_repaired",
                    discarded_bytes = inspection.incomplete_tail_bytes,
                    "Removed incomplete active segment tail"
                );
            }
            active_hash = scan.hash;
            manifest.active.byte_len = scan.valid_bytes;
            manifest.active.records = scan.records;
        }
    }
    let manifest_bytes = files
        .iter()
        .find(|f| f.name == SEGMENTED_MANIFEST_FILE)
        .map_or(0, |f| f.bytes);
    let mut physical_bytes = 0u64;
    let mut retired = Vec::new();
    for file in files {
        if engine_file(file.kind) {
            physical_bytes = physical_bytes.checked_add(file.bytes).ok_or(Error::Limit {
                name: "WAL physical bytes",
            })?;
            if !referenced(&manifest, &file.name) {
                retired.push(file);
            }
        }
    }
    if repair {
        physical_bytes = physical_bytes.saturating_sub(inspection.incomplete_tail_bytes);
    }
    if physical_bytes > options.max_file_bytes {
        return Err(Error::Limit {
            name: "WAL physical bytes including orphans",
        });
    }
    let recovery = Recovery {
        control: checkpoint.control,
        references: checkpoint.references.into(),
        tail,
        reader: None,
        directory: directory.to_owned(),
        options: options.clone(),
        _permit: permit,
        _owner: owner,
    };
    Ok(Loaded {
        manifest,
        index,
        recovery,
        inspection,
        sequence: expected,
        next_record_id,
        physical_bytes,
        retired,
        manifest_bytes,
        active_hash,
    })
}

fn validate_manifest(
    manifest: &Manifest,
    options: &JournalOptions,
    identity: [u8; 32],
) -> Result<()> {
    if manifest.version != LAYOUT_VERSION || manifest.identity != identity {
        return Err(corrupt("Manifest version or identity differs"));
    }
    if manifest.sealed.len().saturating_add(1) > options.max_segments
        || manifest.next_record_id == 0
    {
        return Err(corrupt(
            "Manifest segment count or record allocator is invalid",
        ));
    }
    let mut ids = BTreeSet::new();
    let segments = manifest
        .sealed
        .iter()
        .chain(std::iter::once(&manifest.active))
        .cloned()
        .collect::<Vec<_>>();
    for segment in &segments {
        if !ids.insert(segment.id)
            || segment.byte_len < SEGMENT_HEADER_BYTES
            || segment.byte_len > options.segment_target_bytes
        {
            return Err(corrupt("Manifest has duplicate or invalid segments"));
        }
    }
    if manifest.active.reference_only || manifest.active.first_sequence < manifest.replay_sequence {
        return Err(corrupt("Active segment precedes checkpoint replay barrier"));
    }
    Ok(())
}
async fn load_checkpoint(
    directory: &Path,
    manifest: &Manifest,
    options: &JournalOptions,
) -> Result<Checkpoint> {
    let Some(reference) = &manifest.checkpoint else {
        if manifest.replay_sequence != 0 {
            return Err(corrupt("Missing checkpoint for replay barrier"));
        }
        return Ok(Checkpoint {
            version: LAYOUT_VERSION,
            identity: manifest.identity,
            replay_sequence: 0,
            control: Vec::new(),
            references: Vec::new(),
        });
    };
    let path = directory.join(checkpoint_name(reference.id));
    if hash_file(&path, reference.byte_len).await? != reference.checksum {
        return Err(corrupt("Checkpoint file checksum differs"));
    }
    let checkpoint: Checkpoint = read_value(&path, options.max_checkpoint_bytes).await?;
    if checkpoint.version != LAYOUT_VERSION
        || checkpoint.identity != manifest.identity
        || checkpoint.replay_sequence != manifest.replay_sequence
        || checkpoint.references.len() > options.max_retained_records
    {
        return Err(corrupt(
            "Checkpoint identity, barrier, or index bound differs",
        ));
    }
    Ok(checkpoint)
}

struct SegmentScan {
    file_bytes: u64,
    valid_bytes: u64,
    records: u64,
    hash: blake3::Hasher,
}
struct ScanIndex<'a> {
    index: &'a mut BTreeMap<RecordId, Location>,
    next_id: &'a mut u64,
}
async fn scan_segment(
    directory: &Path,
    segment: &Segment,
    identity: [u8; 32],
    options: &JournalOptions,
    active: bool,
    state: ScanIndex<'_>,
    barrier: u64,
) -> Result<SegmentScan> {
    let ScanIndex { index, next_id } = state;
    let path = directory.join(segment.name());
    validate_segment_header(&path, identity, segment).await?;
    let mut file = File::open(&path).await.map_err(|e| Error::io(&path, e))?;
    let file_bytes = file
        .metadata()
        .await
        .map_err(|e| Error::io(&path, e))?
        .len();
    if file_bytes > options.segment_target_bytes || (!active && file_bytes != segment.byte_len) {
        return Err(corrupt("Segment length differs from manifest or limit"));
    }
    let mut hash = blake3::Hasher::new();
    hash.update(&segment_header(identity, segment)?);
    let mut offset = SEGMENT_HEADER_BYTES;
    let mut records = 0u64;
    while offset < file_bytes {
        let Some(frame) = read_frame(&mut file, &path, offset, file_bytes, options).await? else {
            break;
        };
        let expected = segment
            .first_sequence
            .checked_add(records)
            .ok_or(Error::Limit {
                name: "physical sequence",
            })?;
        if !segment.reference_only && frame.sequence != expected {
            return Err(corrupt("Segment record sequence is not contiguous"));
        }
        hash.update(&frame.header);
        hash.update(&frame.payload);
        let record = decode_record(frame.payload)?;
        if segment.reference_only && record.id.is_none() {
            return Err(corrupt("Reference-only segment contains a control record"));
        }
        if let Some(id) = record.id {
            if segment.reference_only
                && index.get(&id).is_none_or(|location| {
                    location.segment != segment.id
                        || location.offset != offset
                        || location.sequence != frame.sequence
                        || location.frame_bytes != frame.bytes
                })
            {
                return Err(corrupt(
                    "Reference-only segment frame is not named by its checkpoint",
                ));
            }
            *next_id = (*next_id).max(id.0.checked_add(1).ok_or(Error::Limit {
                name: "record identity",
            })?);
            if !segment.reference_only
                && frame.sequence >= barrier
                && (index.len() >= options.max_retained_records
                    || index
                        .insert(
                            id,
                            Location {
                                id,
                                segment: segment.id,
                                offset,
                                frame_bytes: frame.bytes,
                                sequence: frame.sequence,
                            },
                        )
                        .is_some())
            {
                return Err(corrupt(
                    "Retained record index exceeds bound or duplicates identity",
                ));
            }
        }
        offset = offset.checked_add(frame.bytes).ok_or(Error::Limit {
            name: "segment offset",
        })?;
        records += 1;
    }
    if !active
        && (offset != file_bytes
            || records != segment.records
            || hash.finalize().as_bytes() != &segment.checksum)
    {
        return Err(corrupt("Sealed segment length, count, or checksum differs"));
    }
    Ok(SegmentScan {
        file_bytes,
        valid_bytes: offset,
        records,
        hash,
    })
}

pub(super) async fn read_location(
    directory: &Path,
    location: &Location,
    options: &JournalOptions,
) -> Result<RecoveredRecord> {
    let path = segment_path(directory, location.segment);
    let mut file = File::open(&path).await.map_err(|e| Error::io(&path, e))?;
    let end = file
        .metadata()
        .await
        .map_err(|e| Error::io(&path, e))?
        .len();
    let frame = read_frame(&mut file, &path, location.offset, end, options)
        .await?
        .ok_or(corrupt("Checkpoint record is incomplete"))?;
    if frame.sequence != location.sequence || frame.bytes != location.frame_bytes {
        return Err(corrupt("Checkpoint record location differs"));
    }
    let record = decode_record(frame.payload)?;
    if record.id != Some(location.id) {
        return Err(corrupt("Checkpoint record identity differs"));
    }
    Ok(record)
}
pub(super) fn referenced(manifest: &Manifest, name: &str) -> bool {
    name == SEGMENTED_MANIFEST_FILE
        || manifest.active.name() == name
        || manifest.sealed.iter().any(|s| s.name() == name)
        || manifest
            .checkpoint
            .as_ref()
            .is_some_and(|c| checkpoint_name(c.id) == name)
}
