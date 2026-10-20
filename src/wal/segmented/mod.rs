//! Versioned segmented journal with reference checkpoints and one ordered writer.

mod disk;
#[cfg(test)]
mod fault_tests;
mod format;
mod recovery;
#[cfg(test)]
mod tests;
mod writer;

use crate::observability::metrics::{OperationMetrics, Stage};
use crate::wal::{Error, JournalOptions, JournalStatistics, Result};
use disk::*;
use format::*;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use writer::Writer;

pub use format::{
    MAX_SEGMENTED_DIRECTORY_ENTRIES, RecordId, SEGMENTED_MANIFEST_FILE, SegmentedFileKind,
    SnapshotRef, segmented_file_kind,
};
pub use recovery::Recovery;
pub type SegmentedStatistics = JournalStatistics;

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct SegmentedInspection {
    pub file_bytes: u64,
    pub valid_bytes: u64,
    pub records: u64,
    pub incomplete_tail_bytes: u64,
    pub active_segment: String,
    pub sealed_segments: Vec<String>,
}
#[derive(Debug)]
pub struct RecoveredRecord {
    pub id: Option<RecordId>,
    pub data: Vec<u8>,
}

pub struct SegmentedDurability(oneshot::Receiver<std::result::Result<(), String>>);
impl SegmentedDurability {
    pub async fn durable(self) -> Result<()> {
        self.0
            .await
            .map_err(|_| Error::Writer {
                reason: "Segmented writer stopped before durability".to_owned(),
            })?
            .map_err(|reason| Error::Writer { reason })
    }
}
type Completion = oneshot::Sender<std::result::Result<(), String>>;
struct AppendRecord {
    id: Option<RecordId>,
    data: Vec<u8>,
}
struct Pending {
    records: Vec<AppendRecord>,
    physical_bytes: u64,
    completion: Completion,
    _bytes: OwnedSemaphorePermit,
    _request: OwnedSemaphorePermit,
    admitted_at: Instant,
    span: tracing::Span,
    retained_slots: Option<RetainedSlots>,
}
enum Request {
    Append(Pending),
    Checkpoint {
        control: Vec<u8>,
        ids: Vec<RecordId>,
        completion: Completion,
        _permit: OwnedSemaphorePermit,
    },
    Snapshot(SnapshotRef, Completion),
    Cleanup(Completion),
    Barrier(Completion),
    Shutdown(Completion),
}
struct Shared {
    stats: Mutex<SegmentedStatistics>,
    snapshot: Mutex<Option<SnapshotRef>>,
    next_record_id: AtomicU64,
    retained_count: Arc<AtomicUsize>,
    recovery_gate: Arc<Semaphore>,
}
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
struct RetainedSlots {
    counter: Arc<AtomicUsize>,
    count: usize,
}
impl Drop for RetainedSlots {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.count, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct SegmentedJournal {
    sender: mpsc::Sender<Request>,
    options: JournalOptions,
    shared: Arc<Shared>,
    bytes: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    checkpoint_gate: Arc<Semaphore>,
    append_gate: Arc<tokio::sync::Mutex<()>>,
    metrics: Option<Arc<OperationMetrics>>,
}
impl JournalOptions {
    pub fn validate(&self) -> Result<()> {
        self.validate_segmented()
    }
    pub fn validate_segmented(&self) -> Result<()> {
        self.validate_basic()?;
        let publication = self.publication_reserve()?;
        if self.max_segments < 3
            || self.max_segments > MAX_SEGMENTS
            || self.max_retained_records == 0
            || self.max_retained_records > MAX_REFERENCES
            || self.max_checkpoint_bytes < 128
            || self.max_checkpoint_bytes > MAX_CHECKPOINT_BYTES
            || self.segment_target_bytes > self.max_file_bytes
            || self.segment_target_bytes
                < SEGMENT_HEADER_BYTES
                    .saturating_add(self.max_record_bytes as u64)
                    .saturating_add((crate::wal::FRAME_HEADER_BYTES + ENVELOPE_BYTES) as u64)
            || self.control_reserve_bytes < publication
            || self.control_reserve_bytes >= self.max_file_bytes
        {
            return Err(Error::Configuration {
                reason: "Segment, checkpoint, index, and control reserves must fit bounded WAL limits",
            });
        }
        Ok(())
    }
    fn manifest_reserve(&self) -> u64 {
        512 + (self.max_segments as u64).saturating_mul(80) + crate::wal::FRAME_HEADER_BYTES as u64
    }
    fn publication_reserve(&self) -> Result<u64> {
        (self.max_checkpoint_bytes as u64)
            .checked_add(crate::wal::FRAME_HEADER_BYTES as u64)
            .and_then(|v| v.checked_add(self.manifest_reserve()))
            .and_then(|v| v.checked_add(SEGMENT_HEADER_BYTES))
            .ok_or(Error::Limit {
                name: "checkpoint publication reserve",
            })
    }
}
impl SegmentedJournal {
    /// Acquires the same lock as DiskStore; callers with an owner retain it through pending IO.
    pub async fn open(
        directory: impl AsRef<Path>,
        options: JournalOptions,
        identity: [u8; 32],
    ) -> Result<(Self, SegmentedInspection, Recovery)> {
        let directory = directory.as_ref();
        tokio::fs::create_dir_all(directory)
            .await
            .map_err(|e| Error::io(directory, e))?;
        directory_files(directory).await?;
        let path = directory.join(".writer-lock");
        let owner = tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| Error::io(&path, e))?;
            file.try_lock().map_err(|e| Error::Writer {
                reason: format!("Storage directory is already locked: {e}"),
            })?;
            Ok::<_, Error>(Arc::new(file))
        })
        .await
        .map_err(|e| Error::Writer {
            reason: format!("Storage lock worker failed: {e}"),
        })??;
        Self::open_owned(directory, options, identity, None, Some(owner)).await
    }
    pub(crate) async fn open_owned(
        directory: &Path,
        options: JournalOptions,
        identity: [u8; 32],
        metrics: Option<Arc<OperationMetrics>>,
        owner: Option<Arc<std::fs::File>>,
    ) -> Result<(Self, SegmentedInspection, Recovery)> {
        options.validate_segmented()?;
        let files = directory_files(directory).await?;
        if !files.iter().any(|f| f.name == SEGMENTED_MANIFEST_FILE) {
            if files.iter().any(|f| {
                !matches!(
                    f.kind,
                    SegmentedFileKind::Identity | SegmentedFileKind::Lock
                )
            }) {
                return Err(corrupt("Missing manifest in a nonempty storage layout"));
            }
            let (active, _file) = create_segment(directory, identity, 0).await?;
            let manifest = Manifest {
                version: LAYOUT_VERSION,
                identity,
                sealed: Vec::new(),
                active,
                checkpoint: None,
                replay_sequence: 0,
                next_record_id: 1,
                snapshot: None,
            };
            publish_manifest(directory, &manifest).await?;
        }
        let recovery_gate = Arc::new(Semaphore::new(1));
        let permit = recovery_gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Writer {
                reason: "Recovery admission closed".to_owned(),
            })?;
        let loaded = recovery::load(
            directory,
            &options,
            identity,
            true,
            Some(permit),
            owner.clone(),
        )
        .await?;
        let shared = Arc::new(Shared {
            stats: Mutex::new(SegmentedStatistics::default()),
            snapshot: Mutex::new(loaded.manifest.snapshot.clone()),
            next_record_id: AtomicU64::new(loaded.next_record_id),
            retained_count: Arc::new(AtomicUsize::new(loaded.index.len())),
            recovery_gate,
        });
        let bytes = Arc::new(Semaphore::new(options.max_pending_bytes as usize));
        let requests = Arc::new(Semaphore::new(options.max_pending_records));
        let (sender, receiver) = mpsc::channel(options.max_pending_records);
        let journal = Self {
            sender,
            options: options.clone(),
            shared: shared.clone(),
            bytes,
            requests,
            checkpoint_gate: Arc::new(Semaphore::new(1)),
            append_gate: Arc::new(tokio::sync::Mutex::new(())),
            metrics: metrics.clone(),
        };
        let writer = Writer::from_loaded(
            directory,
            options,
            loaded.manifest,
            loaded.index,
            loaded.sequence,
            loaded.physical_bytes,
            loaded.retired,
            loaded.manifest_bytes,
            loaded.active_hash,
            shared,
            metrics,
            owner,
        )
        .await?;
        tokio::spawn(writer.run(receiver));
        Ok((journal, loaded.inspection, loaded.recovery))
    }
    pub async fn append(&self, data: Vec<u8>) -> Result<SegmentedDurability> {
        let (_, durable) = self.enqueue(vec![data], false).await?;
        Ok(durable)
    }
    pub async fn append_retained(&self, data: Vec<u8>) -> Result<(RecordId, SegmentedDurability)> {
        let (ids, durability) = self.append_retained_batch(vec![data]).await?;
        let id = ids
            .into_iter()
            .next()
            .ok_or(corrupt("Retained append returned no record identity"))?;
        Ok((id, durability))
    }
    pub async fn append_retained_batch(
        &self,
        data: Vec<Vec<u8>>,
    ) -> Result<(Vec<RecordId>, SegmentedDurability)> {
        self.enqueue(data, true).await
    }
    async fn enqueue(
        &self,
        data: Vec<Vec<u8>>,
        retained: bool,
    ) -> Result<(Vec<RecordId>, SegmentedDurability)> {
        let start = Instant::now();
        if data.is_empty() || data.len() > self.options.max_retained_records {
            return Err(Error::Limit {
                name: "append batch record count",
            });
        }
        let payload = data
            .iter()
            .try_fold(0u64, |sum, v| sum.checked_add(v.len() as u64))
            .ok_or(Error::Limit {
                name: "append batch length",
            })?;
        let overhead = (data.len() as u64)
            .checked_mul((crate::wal::FRAME_HEADER_BYTES + ENVELOPE_BYTES) as u64)
            .ok_or(Error::Limit {
                name: "append frame overhead",
            })?;
        let physical = payload.checked_add(overhead).ok_or(Error::Limit {
            name: "append batch length",
        })?;
        if payload > self.options.max_record_bytes as u64
            || physical
                > self
                    .options
                    .segment_target_bytes
                    .saturating_sub(SEGMENT_HEADER_BYTES)
            || physical > u64::from(self.options.max_pending_bytes)
        {
            return Err(Error::Limit {
                name: "append batch bytes",
            });
        }
        let retained_slots = if retained {
            self.shared
                .retained_count
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    n.checked_add(data.len())
                        .filter(|next| *next <= self.options.max_retained_records)
                })
                .map_err(|_| Error::Limit {
                    name: "retained record index admission",
                })?;
            Some(RetainedSlots {
                counter: self.shared.retained_count.clone(),
                count: data.len(),
            })
        } else {
            None
        };
        let bytes = self
            .bytes
            .clone()
            .acquire_many_owned(physical as u32)
            .await
            .map_err(|_| Error::Writer {
                reason: "WAL byte admission closed".to_owned(),
            })?;
        let request = self
            .requests
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Writer {
                reason: "WAL request admission closed".to_owned(),
            })?;
        let _gate = self.append_gate.lock().await;
        let first = if retained {
            self.shared
                .next_record_id
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    v.checked_add(data.len() as u64)
                })
                .map_err(|_| Error::Limit {
                    name: "record identity",
                })?
        } else {
            0
        };
        let mut ids = Vec::new();
        let mut records = Vec::new();
        ids.try_reserve_exact(data.len())
            .map_err(|_| Error::Limit {
                name: "record identity allocation",
            })?;
        records
            .try_reserve_exact(data.len())
            .map_err(|_| Error::Limit {
                name: "pending record allocation",
            })?;
        for (offset, data) in data.into_iter().enumerate() {
            let id = retained.then_some(RecordId(first.checked_add(offset as u64).ok_or(
                Error::Limit {
                    name: "record identity",
                },
            )?));
            if let Some(id) = id {
                ids.push(id);
            }
            records.push(AppendRecord { id, data });
        }
        let (completion, receiver) = oneshot::channel();
        self.send(Request::Append(Pending {
            records,
            physical_bytes: physical,
            completion,
            _bytes: bytes,
            _request: request,
            admitted_at: Instant::now(),
            span: tracing::Span::current(),
            retained_slots,
        }))
        .await?;
        if let Some(metrics) = &self.metrics {
            metrics.observe(Stage::WalAdmission, start.elapsed().as_secs_f64());
        }
        Ok((ids, SegmentedDurability(receiver)))
    }
    pub async fn checkpoint(&self, control: Vec<u8>, retained_ids: Vec<RecordId>) -> Result<()> {
        if control.len() > self.options.max_checkpoint_bytes
            || retained_ids.len() > self.options.max_retained_records
        {
            return Err(Error::Limit {
                name: "checkpoint input",
            });
        }
        let permit = self
            .checkpoint_gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Writer {
                reason: "Checkpoint admission closed".to_owned(),
            })?;
        let (completion, receiver) = oneshot::channel();
        self.send(Request::Checkpoint {
            control,
            ids: retained_ids,
            completion,
            _permit: permit,
        })
        .await?;
        SegmentedDurability(receiver).durable().await
    }
    pub fn current_snapshot(&self) -> Option<SnapshotRef> {
        lock(&self.shared.snapshot).clone()
    }
    pub async fn publish_snapshot(&self, snapshot: SnapshotRef) -> Result<()> {
        let (completion, receiver) = oneshot::channel();
        self.send(Request::Snapshot(snapshot, completion)).await?;
        SegmentedDurability(receiver).durable().await
    }
    pub async fn cleanup_orphans(&self) -> Result<()> {
        self.unit_request(Request::Cleanup).await
    }
    pub async fn flush(&self) -> Result<()> {
        self.unit_request(Request::Barrier).await
    }
    pub async fn close(&self) -> Result<()> {
        self.unit_request(Request::Shutdown).await
    }
    async fn unit_request(&self, make: impl FnOnce(Completion) -> Request) -> Result<()> {
        let (completion, receiver) = oneshot::channel();
        self.send(make(completion)).await?;
        SegmentedDurability(receiver).durable().await
    }
    async fn send(&self, request: Request) -> Result<()> {
        self.sender.send(request).await.map_err(|_| Error::Writer {
            reason: "Segmented writer has stopped".to_owned(),
        })
    }
    pub fn statistics(&self) -> SegmentedStatistics {
        let mut result = lock(&self.shared.stats).clone();
        result.available_pending_bytes = self.bytes.available_permits();
        result.available_pending_records = self.requests.available_permits();
        result
    }
}
/// Read-only validation; only open repairs a torn final active frame.
pub async fn inspect_segmented(
    directory: impl AsRef<Path>,
    options: &JournalOptions,
    identity: [u8; 32],
) -> Result<SegmentedInspection> {
    options.validate_segmented()?;
    recovery::load(directory.as_ref(), options, identity, false, None, None)
        .await
        .map(|v| v.inspection)
}
