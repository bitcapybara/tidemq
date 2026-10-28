use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::sync::{Notify, OwnedSemaphorePermit, RwLock, Semaphore, mpsc, oneshot, watch};
use tokio::time::Instant;

use crate::observability::metrics::{Outcome, Stage};
use crate::wal::JournalStatistics;

use super::options::RECORD_RESERVE;
use super::storage::{self, Storage, lock};
use super::{
    Application, Applied, Error, ExecutionPosition, GroupState, LocalOptions, LocalSnapshotReader,
    Result, SnapshotInfo, StorageIdentity,
};

const QUEUED: u8 = 0;
const HANDED_OFF: u8 = 1;
const CANCELED: u8 = 2;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum LocalHealth {
    Ready,
    Draining,
    Failed { reason: String },
    Stopped,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LocalStatistics {
    pub durable_sequence: u64,
    pub applied_sequence: u64,
    pub snapshot_sequence: u64,
    pub maintenance_in_progress: bool,
    pub maintenance_trigger: Option<&'static str>,
    pub maintenance_runs: u64,
    pub maintenance_blocked: Option<&'static str>,
    pub maintenance_failures: u64,
    pub queued: u64,
    pub handed_off: u64,
    pub canceled: u64,
    pub rejected: u64,
    pub unknown: u64,
    pub available_commands: usize,
    pub available_bytes: usize,
    pub journal: JournalStatistics,
}

pub(super) struct View<A: Application> {
    pub application: A,
    pub sequence: u64,
    pub failed: bool,
}

pub(super) struct Shared<A: Application> {
    pub storage: Storage,
    pub view: RwLock<View<A>>,
    pub phase: Mutex<LocalHealth>,
    pub health: watch::Sender<LocalHealth>,
    pub stopped: watch::Sender<bool>,
    pub wake: Notify,
    pub stats: Mutex<LocalStatistics>,
    pub sequence: AtomicU64,
    pub commands: Arc<Semaphore>,
    pub bytes: Arc<Semaphore>,
    ordinary_commands: Arc<Semaphore>,
    ordinary_bytes: Arc<Semaphore>,
    ordinary_queue_slots: Arc<Semaphore>,
    pub snapshot_requested: AtomicBool,
    pub snapshot_done: watch::Sender<u64>,
    pub abort: Mutex<Option<tokio::task::AbortHandle>>,
}

impl<A: Application> Shared<A> {
    pub fn ensure_readable(&self) -> Result<()> {
        match &*lock(&self.phase) {
            LocalHealth::Ready | LocalHealth::Draining => Ok(()),
            LocalHealth::Failed { reason } => Err(Error::fatal(reason.clone())),
            LocalHealth::Stopped => Err(Error::fatal("Local execution has stopped")),
        }
    }

    pub fn fail(&self, reason: String) {
        let mut phase = lock(&self.phase);
        if matches!(*phase, LocalHealth::Failed { .. }) {
            return;
        }
        *phase = LocalHealth::Failed { reason };
        self.health.send_replace(phase.clone());
        self.wake.notify_one();
    }
}

pub(super) struct Payload<A: Application> {
    pub command: A::Command,
    pub bytes: Vec<u8>,
    pub reply: oneshot::Sender<Result<Applied<A::Response>>>,
    pub _permits: Vec<OwnedSemaphorePermit>,
    pub enqueued: Instant,
    pub span: tracing::Span,
    pub subscriber: tracing::Dispatch,
}

pub(super) struct QueueEntry<A: Application> {
    pub queued: Arc<Queued<A>>,
    // Cancellation releases payload budgets; the channel owns this slot until dequeue.
    _slot: Option<OwnedSemaphorePermit>,
}

pub(super) struct Queued<A: Application> {
    phase: AtomicU8,
    pub payload: Mutex<Option<Payload<A>>>,
    pub deadline: Instant,
    pub completion: bool,
}

type Enqueued<A> = (
    Arc<Queued<A>>,
    oneshot::Receiver<Result<Applied<<A as Application>::Response>>>,
);

impl<A: Application> Queued<A> {
    pub fn handoff(&self) -> Option<Payload<A>> {
        self.phase
            .compare_exchange(QUEUED, HANDED_OFF, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        lock(&self.payload).take()
    }

    pub fn cancel(&self) -> bool {
        if self
            .phase
            .compare_exchange(QUEUED, CANCELED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return self.phase.load(Ordering::Acquire) == CANCELED;
        }
        lock(&self.payload).take();
        true
    }

    pub fn reject(&self, reason: &str) {
        if self
            .phase
            .compare_exchange(QUEUED, CANCELED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            && let Some(payload) = lock(&self.payload).take()
        {
            let _ = payload.reply.send(Err(Error::not_submitted(reason)));
        }
    }
}

struct Wait<A: Application>(Arc<Queued<A>>);
impl<A: Application> Drop for Wait<A> {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub struct LocalGroup<A: Application> {
    pub(super) shared: Arc<Shared<A>>,
    sender: mpsc::Sender<QueueEntry<A>>,
}

impl<A: Application> Clone for LocalGroup<A> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            sender: self.sender.clone(),
        }
    }
}

impl<A: Application> LocalGroup<A> {
    pub async fn open(
        path: impl AsRef<Path>,
        identity: StorageIdentity,
        options: LocalOptions,
        application: A,
    ) -> Result<Self> {
        let path = path.as_ref().to_owned();
        // Startup cancellation must not release directory ownership while recovery IO continues.
        tokio::spawn(Self::open_inner(path, identity, options, application))
            .await
            .map_err(|error| Error::fatal(format!("Local open task failed: {error}")))?
    }

    async fn open_inner(
        path: PathBuf,
        identity: StorageIdentity,
        options: LocalOptions,
        application: A,
    ) -> Result<Self> {
        let opened = storage::open(path.as_ref(), identity, options, application).await?;
        let options = &opened.storage.options;
        let total_commands = options.journal.max_pending_records;
        let total_bytes = options.journal.max_pending_bytes as usize;
        let ordinary_commands = total_commands - (total_commands / 4).max(1);
        let (sender, receiver) = mpsc::channel(total_commands);
        let (health, _) = watch::channel(LocalHealth::Ready);
        let (stopped, _) = watch::channel(false);
        let (snapshot_done, _) = watch::channel(0);
        let shared = Arc::new(Shared {
            commands: Arc::new(Semaphore::new(total_commands)),
            bytes: Arc::new(Semaphore::new(total_bytes)),
            ordinary_commands: Arc::new(Semaphore::new(ordinary_commands)),
            ordinary_bytes: Arc::new(Semaphore::new(total_bytes - total_bytes / 4)),
            ordinary_queue_slots: Arc::new(Semaphore::new(ordinary_commands)),
            storage: opened.storage,
            view: RwLock::new(View {
                application: opened.application,
                sequence: opened.sequence,
                failed: false,
            }),
            phase: Mutex::new(LocalHealth::Ready),
            health,
            stopped,
            snapshot_done,
            wake: Notify::new(),
            stats: Mutex::new(LocalStatistics {
                durable_sequence: opened.sequence,
                applied_sequence: opened.sequence,
                snapshot_sequence: opened.snapshot,
                ..LocalStatistics::default()
            }),
            sequence: AtomicU64::new(opened.sequence),
            snapshot_requested: AtomicBool::new(false),
            abort: Mutex::new(None),
        });
        let task = tokio::spawn(super::worker::run(
            shared.clone(),
            receiver,
            opened.retained,
        ));
        *lock(&shared.abort) = Some(task.abort_handle());
        let supervised = shared.clone();
        tokio::spawn(async move {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    let error = anyhow::Error::new(error);
                    tracing::error!(
                        event = "local_execution_failed",
                        group_id = supervised.storage.identity.group.group_id,
                        error = format_args!("{error:#}"),
                        "Local execution stopped authoritative service"
                    );
                    supervised.fail(format!("{error:#}"));
                }
                Err(error) => {
                    supervised.fail(format!("Local execution task ended unexpectedly: {error}"))
                }
            }
            if let Err(error) = supervised.storage.journal.close().await {
                let error = anyhow::Error::new(error);
                supervised.fail(format!("Local writer close failed: {error:#}"));
            }
            supervised.storage.release().await;
            {
                let mut phase = lock(&supervised.phase);
                if !matches!(*phase, LocalHealth::Failed { .. }) {
                    *phase = LocalHealth::Stopped;
                    supervised.health.send_replace(phase.clone());
                }
            }
            supervised.stopped.send_replace(true);
        });
        Ok(Self { shared, sender })
    }

    pub fn identity(&self) -> &StorageIdentity {
        &self.shared.storage.identity
    }
    pub fn file_limit_bytes(&self) -> Result<u64> {
        self.shared.storage.options.file_limit_bytes()
    }
    pub fn state(&self) -> GroupState<A> {
        GroupState::local(self.shared.clone())
    }
    pub fn health(&self) -> watch::Receiver<LocalHealth> {
        self.shared.health.subscribe()
    }
    pub fn applied(&self) -> Option<ExecutionPosition> {
        position(self.shared.sequence.load(Ordering::Acquire))
    }
    pub fn statistics(&self) -> LocalStatistics {
        let mut stats = lock(&self.shared.stats).clone();
        stats.available_commands = self.shared.commands.available_permits();
        stats.available_bytes = self.shared.bytes.available_permits();
        stats.journal = self.shared.storage.journal.statistics();
        stats
    }

    pub async fn submit(
        &self,
        command: A::Command,
        deadline: Instant,
    ) -> Result<Applied<A::Response>> {
        self.submit_inner(command, deadline, false).await
    }

    /// Completion work can use the capacity held back from ordinary production admission.
    pub async fn submit_completion(
        &self,
        command: A::Command,
        deadline: Instant,
    ) -> Result<Applied<A::Response>> {
        self.submit_inner(command, deadline, true).await
    }

    async fn submit_inner(
        &self,
        command: A::Command,
        deadline: Instant,
        completion: bool,
    ) -> Result<Applied<A::Response>> {
        let queued = self.enqueue(command, deadline, completion)?;
        let (queued, reply) = queued;
        let guard = Wait(queued);
        let result = match tokio::time::timeout_at(deadline, reply).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                if guard.0.cancel() {
                    Err(Error::not_submitted("Executor ended before handoff"))
                } else {
                    Err(Error::unknown(
                        "Executor ended after handoff; recover the original operation identity",
                    ))
                }
            }
            Err(_) if guard.0.cancel() => {
                lock(&self.shared.stats).canceled += 1;
                Err(Error::not_submitted("Deadline elapsed before handoff"))
            }
            Err(_) => {
                lock(&self.shared.stats).unknown += 1;
                Err(Error::unknown(
                    "Deadline elapsed after handoff; recover the original operation identity",
                ))
            }
        };
        if let Some(metrics) = &self.shared.storage.options.metrics {
            metrics.result(match &result {
                Ok(_) => Outcome::Committed,
                Err(Error::NotSubmitted { .. }) => Outcome::Rejected,
                Err(Error::Unknown { .. }) => Outcome::Unknown,
                Err(_) => Outcome::Failed,
            });
        }
        result
    }

    fn enqueue(
        &self,
        command: A::Command,
        deadline: Instant,
        completion: bool,
    ) -> Result<Enqueued<A>> {
        if Instant::now() >= deadline {
            return Err(self.reject("Deadline elapsed before admission"));
        }
        match &*lock(&self.shared.phase) {
            LocalHealth::Ready => {}
            LocalHealth::Failed { reason } => return Err(Error::fatal(reason.clone())),
            _ => return Err(self.reject("Local execution is draining or stopped")),
        }
        let slot = if completion {
            None
        } else {
            Some(
                self.shared
                    .ordinary_queue_slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| self.reject("Production queue slot budget is exhausted"))?,
            )
        };
        let mut permits = Vec::new();
        if !completion {
            permits.push(
                self.shared
                    .ordinary_commands
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| self.reject("Production command budget is exhausted"))?,
            );
        }
        permits.push(
            self.shared
                .commands
                .clone()
                .try_acquire_owned()
                .map_err(|_| self.reject("Command budget is exhausted"))?,
        );
        let start = Instant::now();
        let bytes = crate::wal::encode_value(
            &command,
            self.shared
                .storage
                .options
                .journal
                .max_record_bytes
                .saturating_sub(RECORD_RESERVE),
        )
        .map_err(|_| self.reject("Command encoding exceeds the local record budget"))?;
        let size = u32::try_from(bytes.len().saturating_add(RECORD_RESERVE))
            .map_err(|_| self.reject("Command size exceeds admission bounds"))?;
        if !completion {
            permits.push(
                self.shared
                    .ordinary_bytes
                    .clone()
                    .try_acquire_many_owned(size)
                    .map_err(|_| self.reject("Production byte budget is exhausted"))?,
            );
        }
        permits.push(
            self.shared
                .bytes
                .clone()
                .try_acquire_many_owned(size)
                .map_err(|_| self.reject("Command byte budget is exhausted"))?,
        );
        let (reply, result) = oneshot::channel();
        let queued = Arc::new(Queued {
            phase: AtomicU8::new(QUEUED),
            deadline,
            completion,
            payload: Mutex::new(Some(Payload {
                command,
                bytes,
                reply,
                _permits: permits,
                enqueued: Instant::now(),
                span: tracing::Span::current(),
                subscriber: tracing::dispatcher::get_default(Clone::clone),
            })),
        });
        let phase = lock(&self.shared.phase);
        match &*phase {
            LocalHealth::Ready => {}
            LocalHealth::Failed { reason } => return Err(Error::fatal(reason.clone())),
            _ => return Err(self.reject("Local execution is draining or stopped")),
        }
        self.sender
            .try_send(QueueEntry {
                queued: queued.clone(),
                _slot: slot,
            })
            .map_err(|_| self.reject("Local submission queue is unavailable"))?;
        drop(phase);
        lock(&self.shared.stats).queued += 1;
        if let Some(metrics) = &self.shared.storage.options.metrics {
            metrics.observe(Stage::Admission, start.elapsed().as_secs_f64());
        }
        Ok((queued, result))
    }

    fn reject(&self, reason: &'static str) -> Error {
        lock(&self.shared.stats).rejected += 1;
        if let Some(metrics) = &self.shared.storage.options.metrics {
            metrics.result(Outcome::Rejected);
        }
        tracing::debug!(
            event = "local_submission_rejected",
            reason,
            group_id = self.identity().group.group_id,
            "Rejected local command before handoff"
        );
        Error::not_submitted(reason)
    }

    pub async fn try_with_application<R>(&self, query: impl FnOnce(&A) -> R) -> Result<R> {
        self.state().try_with_application(query).await
    }

    pub async fn try_with_applied<R>(
        &self,
        query: impl FnOnce(&A, Option<ExecutionPosition>) -> R,
    ) -> Result<R> {
        self.state().try_with_applied(query).await
    }

    pub async fn snapshot_reader(&self) -> Result<Option<LocalSnapshotReader>> {
        self.shared.ensure_readable()?;
        self.shared.storage.reader().await
    }

    pub async fn snapshot(&self, deadline: Instant) -> Result<Option<SnapshotInfo>> {
        self.shared.ensure_readable()?;
        let mut completed = self.shared.snapshot_done.subscribe();
        let mut health = self.health();
        let generation = *completed.borrow();
        self.shared
            .snapshot_requested
            .store(true, Ordering::Release);
        self.shared.wake.notify_one();
        tokio::time::timeout_at(deadline, async {
            loop {
                self.shared.ensure_readable()?;
                if *completed.borrow_and_update() > generation {
                    break;
                }
                tokio::select! {
                    result = completed.changed() => result.map_err(|_| Error::fatal("Snapshot executor stopped"))?,
                    result = health.changed() => result.map_err(|_| Error::fatal("Local supervisor stopped"))?,
                }
            }
            Ok(self.shared.storage.snapshot_info().await)
        })
        .await
        .map_err(|_| Error::unknown("Snapshot deadline elapsed; publication may still complete"))?
    }

    pub async fn drain(&self, deadline: Instant) -> Result<()> {
        let mut stopped = self.shared.stopped.subscribe();
        {
            let mut phase = lock(&self.shared.phase);
            if *phase == LocalHealth::Ready {
                *phase = LocalHealth::Draining;
                self.shared.health.send_replace(phase.clone());
            }
        }
        self.shared.wake.notify_one();
        let result = tokio::time::timeout_at(deadline, async {
            while !*stopped.borrow_and_update() {
                stopped
                    .changed()
                    .await
                    .map_err(|_| Error::fatal("Local supervisor stopped"))?;
            }
            Ok::<(), Error>(())
        })
        .await;
        if result.is_err() {
            self.shared
                .fail("Local drain exceeded its deadline; recovery is required".to_owned());
            if let Some(abort) = lock(&self.shared.abort).as_ref() {
                abort.abort();
            }
            return Err(Error::unknown(
                "Local drain deadline elapsed before writer closure",
            ));
        }
        result.map_err(|_| Error::unknown("Local drain deadline elapsed"))??;
        match &*lock(&self.shared.phase) {
            LocalHealth::Failed { reason } => Err(Error::fatal(reason.clone())),
            _ => Ok(()),
        }
    }
}

pub(super) fn position(sequence: u64) -> Option<ExecutionPosition> {
    (sequence != 0).then_some(ExecutionPosition::Local { sequence })
}
