use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use openraft::error::RaftError;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;

use crate::observability::metrics::{OperationMetrics, Stage};
use crate::replication::Raft;

use super::admission::Admission;
use super::telemetry::SubmissionGuard;
use super::{
    DataApplication, DataCommand, DataResponse, Error, ProducerMetrics, RequestContext, Result,
};

#[derive(Clone, Copy)]
pub(super) struct CommitObservation {
    pub messages: u64,
    pub bytes: u64,
}

const QUEUED: u8 = 0;
const HANDED_OFF: u8 = 1;
const CANCELED: u8 = 2;

#[derive(Clone, Default)]
pub(crate) struct SubmissionStatus(Arc<AtomicU8>);

impl SubmissionStatus {
    pub fn try_handoff(&self) -> bool {
        self.0
            .compare_exchange(QUEUED, HANDED_OFF, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn cancel_before_handoff(&self) -> bool {
        self.0
            .compare_exchange(QUEUED, CANCELED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn was_handed_off(&self) -> bool {
        self.0.load(Ordering::Acquire) == HANDED_OFF
    }

    pub fn waiter(&self) -> QueueWait {
        QueueWait(self.clone())
    }
}

pub(crate) struct QueueWait(SubmissionStatus);

impl Drop for QueueWait {
    fn drop(&mut self) {
        self.0.cancel_before_handoff();
    }
}

pub(super) struct Submission {
    pub context: RequestContext,
    pub command: DataCommand,
    pub observation: Option<CommitObservation>,
    pub admission: Admission,
    pub pending: SubmissionGuard,
    pub stages: Option<Arc<OperationMetrics>>,
    pub reply: oneshot::Sender<Result<DataResponse>>,
    pub enqueued: Instant,
    pub admission_deadline: Instant,
    pub status: SubmissionStatus,
}

pub(super) fn start_queue(
    raft: Raft<DataApplication>,
    capacity: usize,
    metrics: ProducerMetrics,
) -> Result<mpsc::Sender<Submission>> {
    let handle = Handle::try_current().map_err(|source| Error::Runtime { source })?;
    let (sender, receiver) = mpsc::channel(capacity);
    let worker_handle = handle.clone();
    handle.spawn(run_queue(receiver, raft, metrics, worker_handle));
    Ok(sender)
}

// Preserve FIFO handoff while detached waiters keep durability waits from serializing batches.
async fn run_queue(
    mut queue: mpsc::Receiver<Submission>,
    raft: Raft<DataApplication>,
    metrics: ProducerMetrics,
    handle: Handle,
) {
    while let Some(submission) = queue.recv().await {
        let Submission {
            context,
            command,
            observation,
            admission,
            pending,
            stages,
            reply,
            enqueued,
            admission_deadline,
            status,
        } = submission;
        if let Some(stages) = &stages {
            stages.observe(Stage::Queue, enqueued.elapsed().as_secs_f64());
        }
        if Instant::now() >= admission_deadline {
            status.cancel_before_handoff();
            let error = if Instant::now() >= context.deadline {
                Error::Deadline
            } else {
                Error::Lease {
                    source: crate::ownership::LeaseError::Expired,
                }
            };
            let _ = reply.send(Err(error));
            continue;
        }
        if !status.try_handoff() {
            let _ = reply.send(Err(Error::Deadline));
            continue;
        }
        let receiver = match raft.client_write_ff(command).await {
            Ok(receiver) => receiver,
            Err(source) => {
                let _ = reply.send(Err(Error::Commit {
                    operation_id: context.operation_id,
                    request_id: context.request_id,
                    source: Box::new(RaftError::Fatal(source)),
                }));
                continue;
            }
        };
        let metrics = metrics.clone();
        let span = tracing::info_span!("producer_submission", operation_id = %context.operation_id, request_id = %context.request_id, attempt = context.attempt);
        handle.spawn(
            async move {
                let _admission = admission;
                let _pending = pending;
                let started = Instant::now();
                let result = match receiver.await {
                    Ok(Ok(result)) => Ok(result.data),
                    Ok(Err(source)) => Err(Error::Commit {
                        operation_id: context.operation_id,
                        request_id: context.request_id,
                        source: Box::new(RaftError::APIError(source)),
                    }),
                    Err(_) => Err(Error::Unknown {
                        operation_id: context.operation_id,
                        request_id: context.request_id,
                    }),
                };
                if let Some(stages) = stages {
                    stages.observe(Stage::Commit, started.elapsed().as_secs_f64());
                }
                if matches!(&result, Ok(DataResponse::Produced(result)) if !result.duplicate)
                    && let Some(observation) = observation
                {
                    metrics.committed_batch(observation.messages, observation.bytes);
                }
                let _ = reply.send(result);
            }
            .instrument(span),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_and_handoff_have_one_atomic_winner() {
        let queued = SubmissionStatus::default();
        assert!(queued.cancel_before_handoff());
        assert!(!queued.try_handoff());
        assert!(!queued.was_handed_off());
        let submitted = SubmissionStatus::default();
        assert!(submitted.try_handoff());
        assert!(!submitted.cancel_before_handoff());
        assert!(submitted.was_handed_off());
        let dropped = SubmissionStatus::default();
        drop(dropped.waiter());
        assert!(!dropped.try_handoff());
    }
}
