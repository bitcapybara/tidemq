use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::observability::metrics::{OperationMetrics, Stage};

use super::codec::{HEADER_BYTES, frame, scan};
use super::{Error, Inspection, Result, sync_directory};

#[derive(Clone, Debug)]
pub struct JournalOptions {
    pub max_record_bytes: usize,
    pub max_file_bytes: u64,
    pub max_pending_bytes: u32,
    pub max_pending_records: usize,
    pub group_commit_bytes: usize,
    pub group_commit_delay: Duration,
}

impl Default for JournalOptions {
    fn default() -> Self {
        Self {
            max_record_bytes: 4 * 1024 * 1024,
            max_file_bytes: 128 * 1024 * 1024,
            max_pending_bytes: 16 * 1024 * 1024,
            max_pending_records: 64,
            group_commit_bytes: 1024 * 1024,
            group_commit_delay: Duration::from_millis(1),
        }
    }
}

impl JournalOptions {
    pub fn validate(&self) -> Result<()> {
        let record_bytes = u64::try_from(self.max_record_bytes).map_err(|_| Error::Limit {
            name: "record length",
        })?;
        if record_bytes == 0
            || record_bytes.saturating_add(HEADER_BYTES) > self.max_file_bytes
            || record_bytes > u64::from(self.max_pending_bytes)
            || self.max_pending_records == 0
            || self.max_pending_records > Semaphore::MAX_PERMITS
            || self.group_commit_bytes == 0
            || self.group_commit_delay > Duration::from_secs(1)
        {
            return Err(Error::Configuration {
                reason: "journal limits must be nonzero, consistent, and bounded",
            });
        }
        Ok(())
    }
}

#[derive(Default)]
struct Counters {
    records: AtomicU64,
    bytes: AtomicU64,
    syncs: AtomicU64,
    rewrites: AtomicU64,
    file_bytes: AtomicU64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct JournalStatistics {
    pub written_records: u64,
    pub written_bytes: u64,
    pub sync_operations: u64,
    pub checkpoint_rewrites: u64,
    pub available_pending_bytes: usize,
    pub available_pending_records: usize,
    pub current_file_bytes: u64,
}

struct Pending {
    data: Vec<u8>,
    completion: oneshot::Sender<std::result::Result<(), String>>,
    _permit: OwnedSemaphorePermit,
    _record_permit: OwnedSemaphorePermit,
    admitted_at: Instant,
    span: tracing::Span,
}

enum Request {
    Append(Pending),
    Replace {
        records: Vec<Vec<u8>>,
        completion: oneshot::Sender<std::result::Result<(), String>>,
        _permit: OwnedSemaphorePermit,
    },
    Barrier(oneshot::Sender<std::result::Result<(), String>>),
    Shutdown(oneshot::Sender<std::result::Result<(), String>>),
}

/// Resolves only after the complete group has been synchronized to disk.
pub struct Durability(oneshot::Receiver<std::result::Result<(), String>>);

impl Durability {
    pub async fn durable(self) -> Result<()> {
        self.0
            .await
            .map_err(|_| Error::Writer {
                reason: "writer stopped before reporting durability".to_owned(),
            })?
            .map_err(|reason| Error::Writer { reason })
    }
}

#[derive(Clone)]
pub struct Journal {
    sender: mpsc::Sender<Request>,
    budget: Arc<Semaphore>,
    counters: Arc<Counters>,
    options: JournalOptions,
    rewrite_gate: Arc<Semaphore>,
    record_budget: Arc<Semaphore>,
    metrics: Option<Arc<OperationMetrics>>,
}

struct Writer {
    path: PathBuf,
    file: File,
    sequence: u64,
    file_bytes: u64,
    options: JournalOptions,
    counters: Arc<Counters>,
    metrics: Option<Arc<OperationMetrics>>,
    _owner: Option<Arc<std::fs::File>>,
}

impl Journal {
    /// The owner guard keeps the caller's exclusive directory lock until all queued IO completes.
    pub(crate) async fn open_owned(
        path: &Path,
        options: JournalOptions,
        metrics: Option<Arc<OperationMetrics>>,
        owner: Option<Arc<std::fs::File>>,
    ) -> Result<(Self, Inspection, Vec<Vec<u8>>)> {
        options.validate()?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .append(true)
            .read(true)
            .open(path)
            .await
            .map_err(|e| Error::io(path, e))?;
        let (inspection, records) = scan(path, &options, true).await?;
        if inspection.incomplete_tail_bytes != 0 {
            file.set_len(inspection.valid_bytes)
                .await
                .map_err(|e| Error::io(path, e))?;
            tracing::warn!(
                event = "wal_incomplete_tail_repaired",
                discarded_bytes = inspection.incomplete_tail_bytes,
                "Removed an incomplete unacknowledged journal tail"
            );
        }
        file.flush().await.map_err(|e| Error::io(path, e))?;
        file.sync_all().await.map_err(|e| Error::io(path, e))?;
        if let Some(parent) = path.parent() {
            sync_directory(parent).await?;
        }
        let counters = Arc::new(Counters {
            file_bytes: AtomicU64::new(inspection.valid_bytes),
            ..Counters::default()
        });
        let (sender, receiver) = mpsc::channel(options.max_pending_records);
        let journal = Self {
            sender,
            budget: Arc::new(Semaphore::new(options.max_pending_bytes as usize)),
            counters: counters.clone(),
            options: options.clone(),
            rewrite_gate: Arc::new(Semaphore::new(1)),
            record_budget: Arc::new(Semaphore::new(options.max_pending_records)),
            metrics: metrics.clone(),
        };
        let writer = Writer {
            path: path.to_owned(),
            file,
            sequence: inspection.records,
            file_bytes: inspection.valid_bytes,
            options,
            counters,
            metrics,
            _owner: owner,
        };
        tokio::spawn(writer.run(receiver));
        Ok((journal, inspection, records))
    }

    /// Opens a journal. The caller must ensure there is only one writer for this path.
    pub async fn open(
        path: impl AsRef<Path>,
        options: JournalOptions,
    ) -> Result<(Self, Inspection, Vec<Vec<u8>>)> {
        Self::open_owned(path.as_ref(), options, None, None).await
    }

    pub async fn append(&self, data: Vec<u8>) -> Result<Durability> {
        let admission_start = Instant::now();
        if data.len() > self.options.max_record_bytes {
            return Err(Error::Limit {
                name: "journal record",
            });
        }
        let size = u32::try_from(data.len()).map_err(|_| Error::Limit {
            name: "pending record bytes",
        })?;
        let permit = self
            .budget
            .clone()
            .acquire_many_owned(size)
            .await
            .map_err(|_| Error::Writer {
                reason: "journal admission is closed".to_owned(),
            })?;
        let (completion, receiver) = oneshot::channel();
        let record_permit = self
            .record_budget
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Writer {
                reason: "journal record admission is closed".to_owned(),
            })?;
        if let Some(metrics) = &self.metrics {
            metrics.observe(Stage::Admission, admission_start.elapsed().as_secs_f64());
        }
        self.send(Request::Append(Pending {
            data,
            completion,
            _permit: permit,
            _record_permit: record_permit,
            admitted_at: Instant::now(),
            span: tracing::Span::current(),
        }))
        .await?;
        Ok(Durability(receiver))
    }

    /// The caller serializes checkpoint construction with appends. The old file stays authoritative
    /// until the replacement file and parent directory have been synchronized.
    pub async fn replace(&self, records: Vec<Vec<u8>>) -> Result<()> {
        let mut total = 0_u64;
        for record in &records {
            if record.len() > self.options.max_record_bytes {
                return Err(Error::Limit {
                    name: "checkpoint record",
                });
            }
            total = total
                .checked_add(record.len() as u64)
                .and_then(|n| n.checked_add(HEADER_BYTES))
                .ok_or(Error::Limit {
                    name: "checkpoint file",
                })?;
        }
        if total > self.options.max_file_bytes {
            return Err(Error::Limit {
                name: "checkpoint file",
            });
        }
        let (completion, receiver) = oneshot::channel();
        let permit = self
            .rewrite_gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Writer {
                reason: "checkpoint admission is closed".to_owned(),
            })?;
        self.send(Request::Replace {
            records,
            completion,
            _permit: permit,
        })
        .await?;
        Durability(receiver).durable().await
    }

    pub async fn flush(&self) -> Result<()> {
        let (completion, receiver) = oneshot::channel();
        self.send(Request::Barrier(completion)).await?;
        Durability(receiver).durable().await
    }

    /// Stops this writer after earlier requests finish. All clones become unusable.
    pub async fn close(&self) -> Result<()> {
        let (completion, receiver) = oneshot::channel();
        self.send(Request::Shutdown(completion)).await?;
        Durability(receiver).durable().await
    }

    async fn send(&self, request: Request) -> Result<()> {
        self.sender.send(request).await.map_err(|_| Error::Writer {
            reason: "journal writer has stopped".to_owned(),
        })
    }

    pub fn statistics(&self) -> JournalStatistics {
        JournalStatistics {
            written_records: self.counters.records.load(Ordering::Relaxed),
            written_bytes: self.counters.bytes.load(Ordering::Relaxed),
            sync_operations: self.counters.syncs.load(Ordering::Relaxed),
            checkpoint_rewrites: self.counters.rewrites.load(Ordering::Relaxed),
            available_pending_bytes: self.budget.available_permits(),
            available_pending_records: self.record_budget.available_permits(),
            current_file_bytes: self.counters.file_bytes.load(Ordering::Relaxed),
        }
    }
}

impl Writer {
    async fn run(mut self, mut receiver: mpsc::Receiver<Request>) {
        let mut carry = None;
        let mut failed: Option<String> = None;
        loop {
            let request = match carry.take() {
                Some(request) => request,
                None => match receiver.recv().await {
                    Some(request) => request,
                    None => break,
                },
            };
            if let Some(reason) = &failed {
                fail_request(request, reason);
                continue;
            }
            let result = match request {
                Request::Append(first) => {
                    let (pending, next) = self.collect(first, &mut receiver).await;
                    carry = next;
                    let result = self.append_group(&pending).await;
                    let status = result.as_ref().map(|_| ()).map_err(ToString::to_string);
                    for item in pending {
                        let Pending {
                            completion,
                            _permit: bytes,
                            _record_permit: record,
                            ..
                        } = item;
                        drop((bytes, record));
                        let _ = completion.send(status.clone());
                    }
                    result
                }
                Request::Replace {
                    records,
                    completion,
                    _permit,
                } => {
                    let result = self.rewrite(records).await;
                    let _ =
                        completion.send(result.as_ref().map(|_| ()).map_err(ToString::to_string));
                    result
                }
                Request::Barrier(completion) => {
                    let _ = completion.send(Ok(()));
                    Ok(())
                }
                Request::Shutdown(completion) => {
                    receiver.close();
                    while let Ok(request) = receiver.try_recv() {
                        fail_request(request, "journal writer was closed");
                    }
                    drop(self);
                    let _ = completion.send(Ok(()));
                    return;
                }
            };
            if let Err(error) = result {
                let error = anyhow::Error::new(error);
                tracing::error!(
                    event = "wal_writer_failed",
                    error = format_args!("{error:#}"),
                    "Journal writer stopped accepting mutations"
                );
                failed = Some(format!("{error:#}"));
            }
        }
    }

    async fn collect(
        &self,
        first: Pending,
        receiver: &mut mpsc::Receiver<Request>,
    ) -> (Vec<Pending>, Option<Request>) {
        let mut bytes = first.data.len();
        let mut batch = vec![first];
        let deadline = tokio::time::Instant::now() + self.options.group_commit_delay;
        while bytes < self.options.group_commit_bytes
            && batch.len() < self.options.max_pending_records
        {
            match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Ok(Some(Request::Append(next))) => {
                    if bytes.saturating_add(next.data.len()) > self.options.group_commit_bytes {
                        return (batch, Some(Request::Append(next)));
                    }
                    bytes = bytes.saturating_add(next.data.len());
                    batch.push(next);
                }
                Ok(Some(other)) => return (batch, Some(other)),
                Ok(None) | Err(_) => break,
            }
        }
        (batch, None)
    }

    #[tracing::instrument(name = "wal_group_commit", level = "debug", skip_all, fields(records = pending.len()))]
    async fn append_group(&mut self, pending: &[Pending]) -> Result<()> {
        let append_start = Instant::now();
        let mut frames = Vec::new();
        let mut end = self.file_bytes;
        let mut sequence = self.sequence;
        for item in pending {
            let encoded = frame(&item.data, sequence)?;
            end = end.checked_add(encoded.len() as u64).ok_or(Error::Limit {
                name: "journal file",
            })?;
            if end > self.options.max_file_bytes {
                return Err(Error::Limit {
                    name: "journal file; snapshot and purge are required",
                });
            }
            sequence = sequence.checked_add(1).ok_or(Error::Limit {
                name: "journal sequence",
            })?;
            frames.push(encoded);
            if let Some(metrics) = &self.metrics {
                metrics.observe(Stage::Queue, item.admitted_at.elapsed().as_secs_f64());
            }
        }
        for frame in frames {
            self.file
                .write_all(&frame)
                .await
                .map_err(|e| Error::io(&self.path, e))?;
        }
        if let Some(metrics) = &self.metrics {
            metrics.observe(Stage::Append, append_start.elapsed().as_secs_f64());
        }
        let sync_start = Instant::now();
        self.file
            .sync_all()
            .await
            .map_err(|e| Error::io(&self.path, e))?;
        if let Some(metrics) = &self.metrics {
            metrics.observe(Stage::Sync, sync_start.elapsed().as_secs_f64());
        }
        self.counters
            .records
            .fetch_add(pending.len() as u64, Ordering::Relaxed);
        self.counters
            .bytes
            .fetch_add(end.saturating_sub(self.file_bytes), Ordering::Relaxed);
        self.counters.syncs.fetch_add(1, Ordering::Relaxed);
        self.file_bytes = end;
        self.counters.file_bytes.store(end, Ordering::Relaxed);
        self.sequence = sequence;
        for item in pending {
            tracing::trace!(parent: &item.span, event = "wal_record_durable", record_bytes = item.data.len(), "Journal record became durable in a synchronized group");
        }
        Ok(())
    }

    async fn rewrite(&mut self, records: Vec<Vec<u8>>) -> Result<()> {
        let parent = self.path.parent().ok_or(Error::Configuration {
            reason: "journal parent is missing",
        })?;
        let temporary = parent.join(".journal-checkpoint");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|e| Error::io(&temporary, e))?;
        let mut bytes = 0_u64;
        let mut sequence = 0_u64;
        for record in records {
            let encoded = frame(&record, sequence)?;
            file.write_all(&encoded)
                .await
                .map_err(|e| Error::io(&temporary, e))?;
            bytes = bytes
                .checked_add(encoded.len() as u64)
                .ok_or(Error::Limit {
                    name: "checkpoint size",
                })?;
            sequence = sequence.checked_add(1).ok_or(Error::Limit {
                name: "checkpoint records",
            })?;
        }
        file.sync_all()
            .await
            .map_err(|e| Error::io(&temporary, e))?;
        fs::rename(&temporary, &self.path)
            .await
            .map_err(|e| Error::io(&self.path, e))?;
        sync_directory(parent).await?;
        self.file = fs::OpenOptions::new()
            .append(true)
            .read(true)
            .open(&self.path)
            .await
            .map_err(|e| Error::io(&self.path, e))?;
        self.sequence = sequence;
        self.file_bytes = bytes;
        self.counters.file_bytes.store(bytes, Ordering::Relaxed);
        self.counters.rewrites.fetch_add(1, Ordering::Relaxed);
        self.counters.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn fail_request(request: Request, reason: &str) {
    let completion = match request {
        Request::Append(pending) => pending.completion,
        Request::Replace { completion, .. }
        | Request::Barrier(completion)
        | Request::Shutdown(completion) => completion,
    };
    let _ = completion.send(Err(reason.to_owned()));
}
