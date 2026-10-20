use super::disk::*;
use super::format::*;
use super::recovery::referenced;
use super::{AppendRecord, Error, JournalOptions, Pending, Request, Result, Shared, lock};
use crate::observability::metrics::{OperationMetrics, Stage};
use crate::wal::sync_directory;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

struct EncodedFrame {
    id: Option<RecordId>,
    sequence: u64,
    bytes: Vec<u8>,
}

pub(super) struct Writer {
    directory: PathBuf,
    file: File,
    manifest: Manifest,
    index: BTreeMap<RecordId, Location>,
    sequence: u64,
    physical_bytes: u64,
    retired: Vec<DiskFile>,
    manifest_bytes: u64,
    hash: blake3::Hasher,
    shared: Arc<Shared>,
    options: JournalOptions,
    metrics: Option<Arc<OperationMetrics>>,
    _owner: Option<Arc<std::fs::File>>,
}
impl Writer {
    #[allow(clippy::too_many_arguments)] // The recovered fields are moved directly into their single owner.
    pub(super) async fn from_loaded(
        directory: &Path,
        options: JournalOptions,
        manifest: Manifest,
        index: BTreeMap<RecordId, Location>,
        sequence: u64,
        physical_bytes: u64,
        retired: Vec<DiskFile>,
        manifest_bytes: u64,
        hash: blake3::Hasher,
        shared: Arc<Shared>,
        metrics: Option<Arc<OperationMetrics>>,
        owner: Option<Arc<std::fs::File>>,
    ) -> Result<Self> {
        let path = directory.join(manifest.active.name());
        let file = fs::OpenOptions::new()
            .append(true)
            .read(true)
            .open(&path)
            .await
            .map_err(|e| Error::io(&path, e))?;
        let writer = Self {
            directory: directory.to_owned(),
            file,
            manifest,
            index,
            sequence,
            physical_bytes,
            retired,
            manifest_bytes,
            hash,
            shared,
            options,
            metrics,
            _owner: owner,
        };
        writer.refresh();
        Ok(writer)
    }
    pub(super) async fn run(mut self, mut receiver: mpsc::Receiver<Request>) {
        let mut carry = None;
        let mut failed = None::<String>;
        loop {
            let request = match carry.take() {
                Some(value) => value,
                None => match receiver.recv().await {
                    Some(value) => value,
                    None => break,
                },
            };
            if let Some(reason) = &failed {
                fail(request, reason);
                continue;
            }
            let result = match request {
                Request::Append(first) => {
                    let (pending, next) = self.collect(first, &mut receiver).await;
                    carry = next;
                    let result = self.append_group(&pending).await;
                    let status = result.as_ref().map(|_| ()).map_err(ToString::to_string);
                    for item in pending {
                        let _ = item.completion.send(status.clone());
                    }
                    result
                }
                Request::Checkpoint {
                    control,
                    ids,
                    completion,
                    ..
                } => {
                    let result = self.checkpoint(control, ids).await;
                    let _ = completion.send(status(&result));
                    result
                }
                Request::Snapshot(snapshot, completion) => {
                    let result = self.publish_snapshot(snapshot).await;
                    let _ = completion.send(status(&result));
                    result
                }
                Request::Cleanup(completion) => {
                    let result = self.cleanup().await;
                    let _ = completion.send(status(&result));
                    result
                }
                Request::Barrier(completion) => {
                    let _ = completion.send(Ok(()));
                    Ok(())
                }
                Request::Shutdown(completion) => {
                    receiver.close();
                    while let Ok(request) = receiver.try_recv() {
                        fail(request, "Segmented writer was closed");
                    }
                    drop(self);
                    let _ = completion.send(Ok(()));
                    return;
                }
            };
            if let Err(error) = result
                && !matches!(error, Error::Limit { .. } | Error::Configuration { .. })
            {
                let error = anyhow::Error::new(error);
                tracing::error!(
                    event = "wal_writer_failed",
                    error = format_args!("{error:#}"),
                    "Segmented writer stopped accepting mutations"
                );
                failed = Some(format!("{error:#}"));
            }
            self.refresh();
        }
    }
    async fn collect(
        &self,
        first: Pending,
        receiver: &mut mpsc::Receiver<Request>,
    ) -> (Vec<Pending>, Option<Request>) {
        let mut bytes = first.physical_bytes;
        let mut batch = vec![first];
        let deadline = tokio::time::Instant::now() + self.options.group_commit_delay;
        let bound = (self.options.group_commit_bytes as u64).min(
            self.options
                .segment_target_bytes
                .saturating_sub(SEGMENT_HEADER_BYTES),
        );
        while bytes < bound && batch.len() < self.options.max_pending_records {
            match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Ok(Some(Request::Append(next))) => {
                    if bytes.saturating_add(next.physical_bytes) > bound {
                        return (batch, Some(Request::Append(next)));
                    }
                    bytes += next.physical_bytes;
                    batch.push(next);
                }
                Ok(Some(other)) => return (batch, Some(other)),
                Ok(None) | Err(_) => break,
            }
        }
        (batch, None)
    }
    #[tracing::instrument(name="wal_segment_group_commit",level="debug",skip_all,fields(requests=pending.len()))]
    async fn append_group(&mut self, pending: &[Pending]) -> Result<()> {
        let bytes = pending
            .iter()
            .try_fold(0u64, |n, p| n.checked_add(p.physical_bytes))
            .ok_or(Error::Limit {
                name: "group bytes",
            })?;
        let retained = pending
            .iter()
            .flat_map(|p| &p.records)
            .filter(|r| r.id.is_some())
            .count();
        if self.index.len().saturating_add(retained) > self.options.max_retained_records {
            return Err(Error::Limit {
                name: "retained record index",
            });
        }
        let rotate =
            self.manifest.active.byte_len.saturating_add(bytes) > self.options.segment_target_bytes;
        let staging = if rotate {
            SEGMENT_HEADER_BYTES + self.options.manifest_reserve()
        } else {
            0
        };
        let reserve = if retained > 0 {
            self.options.control_reserve_bytes
        } else {
            self.options.publication_reserve()?
        };
        if self
            .physical_bytes
            .checked_add(bytes)
            .and_then(|v| v.checked_add(staging))
            .is_none_or(|v| v > self.options.max_file_bytes.saturating_sub(reserve))
        {
            return Err(Error::Limit {
                name: "WAL capacity; checkpoint and purge are required",
            });
        }
        if rotate {
            self.ensure_file_slots(2, true).await?;
        }
        let frames = self.frames(pending)?;
        if rotate {
            self.rotate().await?;
        }
        let start = Instant::now();
        let path = self.directory.join(self.manifest.active.name());
        for EncodedFrame {
            id,
            sequence,
            bytes: frame,
        } in &frames
        {
            let offset = self.manifest.active.byte_len;
            self.file
                .write_all(frame)
                .await
                .map_err(|e| Error::io(&path, e))?;
            self.hash.update(frame);
            self.manifest.active.byte_len += frame.len() as u64;
            self.manifest.active.records += 1;
            self.physical_bytes += frame.len() as u64;
            if let Some(id) = id {
                self.index.insert(
                    *id,
                    Location {
                        id: *id,
                        segment: self.manifest.active.id,
                        offset,
                        frame_bytes: frame.len() as u64,
                        sequence: *sequence,
                    },
                );
            }
        }
        if let Some(metrics) = &self.metrics {
            metrics.observe(Stage::Append, start.elapsed().as_secs_f64());
        }
        self.sync().await?;
        self.sequence = self
            .sequence
            .checked_add(frames.len() as u64)
            .ok_or(Error::Limit {
                name: "physical sequence",
            })?;
        let mut stats = lock(&self.shared.stats);
        stats.written_records = stats.written_records.saturating_add(frames.len() as u64);
        stats.written_bytes = stats.written_bytes.saturating_add(bytes);
        drop(stats);
        for item in pending {
            if let Some(metrics) = &self.metrics {
                metrics.observe(Stage::WalQueue, item.admitted_at.elapsed().as_secs_f64());
            }
            tracing::trace!(parent:&item.span,event="wal_record_durable",records=item.records.len(),"Segment records became durable in one synchronized group");
        }
        Ok(())
    }
    fn frames(&self, pending: &[Pending]) -> Result<Vec<EncodedFrame>> {
        let mut frames = Vec::new();
        let mut sequence = self.sequence;
        for AppendRecord { id, data } in pending.iter().flat_map(|p| &p.records) {
            let mut payload = Vec::new();
            payload
                .try_reserve_exact(data.len().saturating_add(ENVELOPE_BYTES))
                .map_err(|_| Error::Limit {
                    name: "frame envelope allocation",
                })?;
            payload.push(u8::from(id.is_some()));
            payload.extend_from_slice(&id.map_or(0, |id| id.0).to_le_bytes());
            payload.extend_from_slice(data);
            frames.push(EncodedFrame {
                id: *id,
                sequence,
                bytes: crate::wal::codec::frame(&payload, sequence)?,
            });
            sequence = sequence.checked_add(1).ok_or(Error::Limit {
                name: "physical sequence",
            })?;
        }
        Ok(frames)
    }
    async fn sync(&self) -> Result<()> {
        let start = Instant::now();
        let path = self.directory.join(self.manifest.active.name());
        self.file
            .sync_all()
            .await
            .map_err(|e| Error::io(&path, e))?;
        if let Some(metrics) = &self.metrics {
            metrics.observe(Stage::Sync, start.elapsed().as_secs_f64());
        }
        let mut stats = lock(&self.shared.stats);
        stats.sync_operations = stats.sync_operations.saturating_add(1);
        Ok(())
    }
    async fn rotate(&mut self) -> Result<()> {
        let start = Instant::now();
        self.sync().await?;
        let (active, file) =
            create_segment(&self.directory, self.manifest.identity, self.sequence).await?;
        let mut candidate = self.manifest.clone();
        let mut old = self.manifest.active.clone();
        old.checksum = *self.hash.finalize().as_bytes();
        candidate.sealed.push(old);
        candidate.active = active;
        candidate.next_record_id = self.shared.next_record_id.load(Ordering::Relaxed);
        let bytes = publish_manifest(&self.directory, &candidate).await?;
        self.physical_bytes = self
            .physical_bytes
            .saturating_add(SEGMENT_HEADER_BYTES)
            .saturating_add(bytes)
            .saturating_sub(self.manifest_bytes);
        self.manifest_bytes = bytes;
        self.file = file;
        self.manifest = candidate;
        self.hash = blake3::Hasher::new();
        self.hash.update(&segment_header(
            self.manifest.identity,
            &self.manifest.active,
        )?);
        let mut stats = lock(&self.shared.stats);
        stats.rotations = stats.rotations.saturating_add(1);
        tracing::debug!(
            event = "wal_segment_rotated",
            elapsed_seconds = start.elapsed().as_secs_f64(),
            segment_count = self.manifest.sealed.len() + 1,
            "Published a new active segment"
        );
        Ok(())
    }
    async fn ensure_file_slots(&self, additional: usize, reserve_checkpoint: bool) -> Result<()> {
        let files = directory_files(&self.directory).await?;
        if files.len().saturating_add(additional) > MAX_SEGMENTED_DIRECTORY_ENTRIES {
            return Err(Error::Limit {
                name: "storage directory entries",
            });
        }
        let count = files
            .iter()
            .filter(|f| f.kind == SegmentedFileKind::Segment)
            .count();
        let reserve = usize::from(reserve_checkpoint);
        if count.saturating_add(1).saturating_add(reserve) > self.options.max_segments {
            return Err(Error::Limit {
                name: "segment count; checkpoint and purge are required",
            });
        }
        Ok(())
    }
    #[tracing::instrument(name="wal_reference_checkpoint",level="debug",skip_all,fields(retained=ids.len()))]
    async fn checkpoint(&mut self, control: Vec<u8>, ids: Vec<RecordId>) -> Result<()> {
        let start = Instant::now();
        let gate = self
            .shared
            .recovery_gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Writer {
                reason: "Recovery gate closed".to_owned(),
            })?;
        let mut references = Vec::new();
        references
            .try_reserve_exact(ids.len())
            .map_err(|_| Error::Limit {
                name: "checkpoint reference allocation",
            })?;
        let mut seen = BTreeSet::new();
        for id in ids {
            if !seen.insert(id) {
                return Err(Error::Configuration {
                    reason: "Checkpoint contains duplicate record identities",
                });
            }
            references.push(self.index.get(&id).cloned().ok_or(Error::Configuration {
                reason: "Checkpoint references an unknown record identity",
            })?);
        }
        let checkpoint = Checkpoint {
            version: LAYOUT_VERSION,
            identity: self.manifest.identity,
            replay_sequence: self.sequence,
            control,
            references,
        };
        let encoded = value_frame(&checkpoint, self.options.max_checkpoint_bytes)?;
        let staging = encoded.len() as u64 + SEGMENT_HEADER_BYTES + self.options.manifest_reserve();
        if self
            .physical_bytes
            .checked_add(staging)
            .is_none_or(|v| v > self.options.max_file_bytes)
        {
            return Err(Error::Limit {
                name: "checkpoint staging capacity",
            });
        }
        self.ensure_file_slots(3, false).await?;
        self.sync().await?;
        lock(&self.shared.stats).staging_bytes = staging;
        let checkpoint_ref = FileRef {
            id: new_id()?,
            byte_len: encoded.len() as u64,
            checksum: *blake3::hash(&encoded).as_bytes(),
        };
        let path = self.directory.join(checkpoint_name(checkpoint_ref.id));
        let _checkpoint_file = create_file(&path, &encoded).await?;
        #[cfg(test)]
        super::fault_tests::boundary("checkpoint_sync");
        sync_directory(&self.directory).await?;
        #[cfg(test)]
        super::fault_tests::boundary("checkpoint_directory_sync");
        let (active, file) =
            create_segment(&self.directory, self.manifest.identity, self.sequence).await?;
        let needed: BTreeSet<_> = checkpoint.references.iter().map(|r| r.segment).collect();
        let mut sealed = self.manifest.sealed.clone();
        let mut old_active = self.manifest.active.clone();
        old_active.checksum = *self.hash.finalize().as_bytes();
        sealed.push(old_active);
        sealed.retain(|s| needed.contains(&s.id));
        let candidate = Manifest {
            version: LAYOUT_VERSION,
            identity: self.manifest.identity,
            sealed,
            active,
            checkpoint: Some(checkpoint_ref.clone()),
            replay_sequence: self.sequence,
            next_record_id: self.shared.next_record_id.load(Ordering::Relaxed),
            snapshot: self.manifest.snapshot.clone(),
        };
        let manifest_bytes = publish_manifest(&self.directory, &candidate).await?;
        self.retire_replaced(&candidate);
        self.physical_bytes = self
            .physical_bytes
            .saturating_add(encoded.len() as u64)
            .saturating_add(SEGMENT_HEADER_BYTES)
            .saturating_add(manifest_bytes)
            .saturating_sub(self.manifest_bytes);
        self.manifest_bytes = manifest_bytes;
        self.manifest = candidate;
        self.file = file;
        self.index = checkpoint
            .references
            .into_iter()
            .map(|r| (r.id, r))
            .collect();
        self.hash = blake3::Hasher::new();
        self.hash.update(&segment_header(
            self.manifest.identity,
            &self.manifest.active,
        )?);
        {
            let mut stats = lock(&self.shared.stats);
            stats.staging_bytes = 0;
            stats.checkpoint_rewrites = stats.checkpoint_rewrites.saturating_add(1);
            stats.rotations = stats.rotations.saturating_add(1);
        }
        self.gc().await;
        drop(gate);
        tracing::debug!(
            event = "wal_reference_checkpoint_published",
            elapsed_seconds = start.elapsed().as_secs_f64(),
            retained_records = self.index.len(),
            "Published reference checkpoint without rewriting entry payloads"
        );
        Ok(())
    }
    fn retire_replaced(&mut self, candidate: &Manifest) {
        for segment in self
            .manifest
            .sealed
            .iter()
            .chain(std::iter::once(&self.manifest.active))
        {
            let name = segment.name();
            if !referenced(candidate, &name) && !self.retired.iter().any(|f| f.name == name) {
                self.retired.push(DiskFile {
                    name,
                    bytes: segment.byte_len,
                    kind: SegmentedFileKind::Segment,
                });
            }
        }
        if let Some(checkpoint) = &self.manifest.checkpoint {
            let name = checkpoint_name(checkpoint.id);
            if !referenced(candidate, &name) && !self.retired.iter().any(|f| f.name == name) {
                self.retired.push(DiskFile {
                    name,
                    bytes: checkpoint.byte_len,
                    kind: SegmentedFileKind::Checkpoint,
                });
            }
        }
    }
    async fn publish_snapshot(&mut self, snapshot: super::SnapshotRef) -> Result<()> {
        validate_snapshot(&self.directory, &snapshot).await?;
        if self
            .physical_bytes
            .saturating_add(self.options.manifest_reserve())
            > self.options.max_file_bytes
        {
            return Err(Error::Limit {
                name: "snapshot manifest staging capacity",
            });
        }
        let mut manifest = self.manifest.clone();
        manifest.snapshot = Some(snapshot.clone());
        manifest.next_record_id = self.shared.next_record_id.load(Ordering::Relaxed);
        let bytes = publish_manifest(&self.directory, &manifest).await?;
        self.physical_bytes = self
            .physical_bytes
            .saturating_add(bytes)
            .saturating_sub(self.manifest_bytes);
        self.manifest_bytes = bytes;
        self.manifest = manifest;
        *lock(&self.shared.snapshot) = Some(snapshot);
        Ok(())
    }
    async fn cleanup(&mut self) -> Result<()> {
        let _gate = self
            .shared
            .recovery_gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Writer {
                reason: "Recovery gate closed".to_owned(),
            })?;
        for file in directory_files(&self.directory).await? {
            if engine_file(file.kind)
                && !referenced(&self.manifest, &file.name)
                && !self.retired.iter().any(|f| f.name == file.name)
            {
                self.retired.push(file);
            }
        }
        self.gc().await;
        Ok(())
    }
    async fn gc(&mut self) {
        let start = Instant::now();
        let mut removed = Vec::new();
        for (index, file) in self.retired.iter().enumerate() {
            #[cfg(test)]
            super::fault_tests::boundary("gc_before_unlink");
            let path = self.directory.join(&file.name);
            match fs::remove_file(&path).await {
                Ok(()) => removed.push(index),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => removed.push(index),
                Err(error) => self.gc_failure(&path, error),
            }
            #[cfg(test)]
            super::fault_tests::boundary("gc_after_unlink");
        }
        if removed.is_empty() {
            return;
        }
        if let Err(error) = sync_directory(&self.directory).await {
            let error = anyhow::Error::new(error);
            tracing::warn!(
                event = "wal_gc_directory_sync_failed",
                error = format_args!("{error:#}"),
                "Retired bytes remain charged until directory synchronization succeeds"
            );
            let mut stats = lock(&self.shared.stats);
            stats.gc_failures = stats.gc_failures.saturating_add(1);
            return;
        }
        #[cfg(test)]
        super::fault_tests::boundary("gc_directory_sync");
        let mut reclaimed = 0u64;
        let mut position = 0usize;
        self.retired.retain(|file| {
            let keep = !removed.contains(&position);
            position += 1;
            if !keep {
                reclaimed = reclaimed.saturating_add(file.bytes);
            }
            keep
        });
        self.physical_bytes = self.physical_bytes.saturating_sub(reclaimed);
        let mut stats = lock(&self.shared.stats);
        stats.reclaimed_bytes = stats.reclaimed_bytes.saturating_add(reclaimed);
        tracing::debug!(
            event = "wal_segments_collected",
            reclaimed_bytes = reclaimed,
            elapsed_seconds = start.elapsed().as_secs_f64(),
            "Removed unreferenced sealed segments and checkpoints"
        );
    }
    fn gc_failure(&self, path: &Path, error: std::io::Error) {
        let error = anyhow::Error::new(Error::io(path, error));
        tracing::warn!(
            event = "wal_gc_delete_failed",
            error = format_args!("{error:#}"),
            "Retired file remains charged for a later collection retry"
        );
        let mut stats = lock(&self.shared.stats);
        stats.gc_failures = stats.gc_failures.saturating_add(1);
    }
    fn refresh(&self) {
        let mut stats = lock(&self.shared.stats);
        stats.active_bytes = self.manifest.active.byte_len;
        stats.sealed_bytes = self.manifest.sealed.iter().map(|s| s.byte_len).sum();
        stats.checkpoint_bytes = self.manifest.checkpoint.as_ref().map_or(0, |c| c.byte_len);
        stats.retired_bytes = self.retired.iter().map(|f| f.bytes).sum();
        stats.current_file_bytes = self.physical_bytes.saturating_add(stats.staging_bytes);
        stats.segment_count = self.manifest.sealed.len() + 1;
        stats.retained_records = self.index.len();
        stats.index_bytes = (self.index.len() as u64)
            .saturating_mul(std::mem::size_of::<(RecordId, Location)>() as u64);
    }
}
fn status(result: &Result<()>) -> std::result::Result<(), String> {
    result.as_ref().map(|_| ()).map_err(ToString::to_string)
}
fn fail(request: Request, reason: &str) {
    let completion = match request {
        Request::Append(p) => p.completion,
        Request::Checkpoint { completion, .. }
        | Request::Snapshot(_, completion)
        | Request::Cleanup(completion)
        | Request::Barrier(completion)
        | Request::Shutdown(completion) => completion,
    };
    let _ = completion.send(Err(reason.to_owned()));
}
