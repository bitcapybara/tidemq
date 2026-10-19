use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tidemq_protocol::{Id, common::TrafficClass};
use tokio::sync::{mpsc, oneshot};

use crate::budget::MemoryBudget;
use crate::metadata::PartitionKey;
use crate::observability::metrics::{OperationMetrics, Stage};
use crate::ownership::{ControlLease, FenceRecord, Work};
use crate::producer::{
    AppendBatch, BatchResult, ProducerLimits, ReadPage, RegisterProducer, SessionInfo,
};
use crate::replication::{DiskStateMachine, Raft};

use super::admission::{Admission, Lane};
use super::submission::{CommitObservation, QueueWait, Submission, SubmissionStatus, start_queue};
use super::telemetry::{AttemptGuard, AttemptOutcome};
use super::{
    BrokerLimits, DataApplication, DataCommand, DataGroupConfig, DataRejection, DataResponse,
    Error, ProducerCommand, ProducerMetrics, Result, WriterFence,
};

#[derive(Clone, Copy, Debug)]
pub struct RequestContext {
    pub operation_id: Id,
    pub request_id: Id,
    pub attempt: u32,
    pub deadline: Instant,
}

impl RequestContext {
    fn check(&self) -> Result<()> {
        let now = Instant::now();
        if self.attempt == 0
            || self.deadline.saturating_duration_since(now) > Duration::from_secs(300)
        {
            return Err(Error::InvalidRequest);
        }
        if now >= self.deadline {
            return Err(Error::Deadline);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ProducerStatus {
    pub next_registration_epoch: u64,
    pub logical_time_ms: u64,
    pub session: Option<SessionInfo>,
}

pub struct HotRead {
    page: ReadPage,
    _admission: Admission,
}

impl HotRead {
    pub fn page(&self) -> &ReadPage {
        &self.page
    }
}

impl fmt::Debug for HotRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HotRead")
            .field("first_offset", &self.page.first_offset)
            .field("message_count", &self.page.messages.len())
            .field("next_offset", &self.page.next_offset)
            .finish()
    }
}

type ResponseReceiver = oneshot::Receiver<Result<DataResponse>>;

struct DataSubmission<'lease> {
    _waiter: QueueWait,
    attempt: AttemptGuard,
    receiver: ResponseReceiver,
    status: SubmissionStatus,
    broker: Broker,
    lease: &'lease ControlLease,
    context: RequestContext,
    partition: PartitionKey,
}

impl DataSubmission<'_> {
    async fn receive(mut self) -> Result<DataResponse> {
        let result = self
            .broker
            .finish_submission(
                self.lease,
                self.context,
                self.partition,
                self.receiver,
                &self.status,
            )
            .await;
        self.attempt.finish(attempt_outcome(&result));
        result
    }
}

/// A queued production operation; dropping it cancels only work not yet handed to Raft.
pub struct ProducerSubmission<'lease>(DataSubmission<'lease>);

impl ProducerSubmission<'_> {
    pub async fn receive(self) -> Result<BatchResult> {
        match self.0.receive().await? {
            DataResponse::Produced(result) => Ok(result),
            _ => Err(Error::UnexpectedResponse),
        }
    }
}

#[derive(Clone)]
pub struct Broker {
    node_id: u64,
    raft: Raft<DataApplication>,
    state: DiskStateMachine<DataApplication>,
    config: DataGroupConfig,
    memory: MemoryBudget,
    produce: Lane,
    control: Lane,
    read: Lane,
    closed: Arc<AtomicBool>,
    metrics: ProducerMetrics,
    stages: Option<Arc<OperationMetrics>>,
    submissions: mpsc::Sender<Submission>,
}

impl Broker {
    pub fn new(
        node_id: u64,
        raft: Raft<DataApplication>,
        state: DiskStateMachine<DataApplication>,
        config: DataGroupConfig,
        memory: MemoryBudget,
        limits: BrokerLimits,
        metrics: ProducerMetrics,
    ) -> Result<Self> {
        if node_id == 0 {
            return Err(Error::InvalidLimits);
        }
        config
            .validate()
            .map_err(|reason| Error::Rejected { reason })?;
        limits.validate()?;
        let capacity = limits
            .produce_operations
            .checked_add(limits.control_operations)
            .ok_or(Error::InvalidLimits)?;
        let submissions = start_queue(raft.clone(), capacity as usize, metrics.clone())?;
        Ok(Self {
            node_id,
            raft,
            state,
            config,
            memory,
            produce: Lane::new(
                limits.produce_operations,
                limits.produce_bytes,
                TrafficClass::Produce,
            ),
            control: Lane::new(
                limits.control_operations,
                limits.control_bytes,
                TrafficClass::Control,
            ),
            read: Lane::new(
                limits.read_operations,
                limits.read_bytes,
                TrafficClass::Delivery,
            ),
            closed: Arc::new(AtomicBool::new(false)),
            metrics,
            stages: None,
            submissions,
        })
    }

    pub fn with_stages(mut self, stages: Arc<OperationMetrics>) -> Self {
        self.stages = Some(stages);
        self
    }

    pub fn metrics(&self) -> &ProducerMetrics {
        &self.metrics
    }

    pub async fn register(
        &self,
        lease: &ControlLease,
        context: RequestContext,
        partition: PartitionKey,
        request: RegisterProducer,
    ) -> Result<SessionInfo> {
        match self
            .submit(
                lease,
                context,
                partition,
                ProducerCommand::Register(request),
            )
            .await?
        {
            DataResponse::Registered(info) => Ok(info),
            _ => Err(Error::UnexpectedResponse),
        }
    }

    pub async fn produce(
        &self,
        lease: &ControlLease,
        context: RequestContext,
        request: AppendBatch,
    ) -> Result<BatchResult> {
        self.submit_produce(lease, context, request)
            .await?
            .receive()
            .await
    }

    /// Serial callers may release their stream-order guard after this method enqueues the batch.
    pub async fn submit_produce<'lease>(
        &self,
        lease: &'lease ControlLease,
        context: RequestContext,
        request: AppendBatch,
    ) -> Result<ProducerSubmission<'lease>> {
        let partition = PartitionKey {
            topic_id: request.topic_id,
            partition: request.partition_id,
        };
        self.submit_data(lease, context, partition, ProducerCommand::Append(request))
            .await
            .map(ProducerSubmission)
    }

    pub async fn retire(
        &self,
        lease: &ControlLease,
        context: RequestContext,
        partition: PartitionKey,
        producer_id: Id,
        producer_epoch: u64,
    ) -> Result<()> {
        let command = ProducerCommand::Retire {
            producer_id,
            producer_epoch,
        };
        match self.submit(lease, context, partition, command).await? {
            DataResponse::Retired => Ok(()),
            _ => Err(Error::UnexpectedResponse),
        }
    }

    pub async fn advance_time(
        &self,
        lease: &ControlLease,
        context: RequestContext,
        partition: PartitionKey,
    ) -> Result<()> {
        match self
            .submit(lease, context, partition, ProducerCommand::AdvanceTime)
            .await?
        {
            DataResponse::TimeAdvanced => Ok(()),
            _ => Err(Error::UnexpectedResponse),
        }
    }

    pub async fn producer_status(
        &self,
        lease: &ControlLease,
        context: RequestContext,
        partition: PartitionKey,
        session: Option<(Id, u64)>,
    ) -> Result<ProducerStatus> {
        context.check()?;
        let _admission = self.control.admit(512, &self.memory)?;
        self.read_barrier(context).await?;
        self.authorize(lease, context, partition, Work::Produce)
            .await?;
        let now = unix_millis()?;
        let status = self
            .with_state(context, |application| {
                let producer = application
                    .producer(partition)
                    .ok_or(Error::PartitionMissing)?;
                let session = session
                    .map(|(id, epoch)| producer.session_at(id, epoch, now))
                    .transpose()
                    .map_err(|reason| Error::Rejected {
                        reason: DataRejection::Producer(reason),
                    })?;
                if session.as_ref().is_some_and(|session| {
                    session.expires_at_ms <= now.max(producer.logical_time_ms())
                }) {
                    return Err(Error::Rejected {
                        reason: DataRejection::Producer(crate::producer::Rejection::SessionExpired),
                    });
                }
                Ok(ProducerStatus {
                    next_registration_epoch: producer.next_registration_epoch(),
                    logical_time_ms: producer.logical_time_ms(),
                    session,
                })
            })
            .await?;
        self.check_reply_lease(lease, partition, context).await?;
        Ok(status)
    }

    pub async fn hot_read(
        &self,
        lease: &ControlLease,
        context: RequestContext,
        partition: PartitionKey,
        first: u64,
        max_messages: usize,
        max_bytes: u32,
    ) -> Result<HotRead> {
        context.check()?;
        let bytes = max_bytes.checked_add(512).ok_or(Error::InvalidRequest)?;
        let admission = self.read.admit(bytes, &self.memory)?;
        self.read_barrier(context).await?;
        self.authorize(lease, context, partition, Work::Deliver)
            .await?;
        let page = self
            .with_state(context, |application| {
                application
                    .producer(partition)
                    .ok_or(Error::PartitionMissing)?
                    .hot_read(first, max_messages, u64::from(max_bytes))
                    .map_err(|reason| Error::Rejected {
                        reason: DataRejection::Producer(reason),
                    })
            })
            .await?;
        self.check_reply_lease(lease, partition, context).await?;
        Ok(HotRead {
            page,
            _admission: admission,
        })
    }

    pub async fn drain(&self, deadline: Instant) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        tokio::time::timeout_at(deadline.into(), async {
            self.produce.drain().await?;
            self.control.drain().await?;
            self.read.drain().await
        })
        .await
        .map_err(|_| Error::Deadline)?
    }

    async fn submit(
        &self,
        lease: &ControlLease,
        context: RequestContext,
        partition: PartitionKey,
        command: ProducerCommand,
    ) -> Result<DataResponse> {
        self.submit_data(lease, context, partition, command)
            .await?
            .receive()
            .await
    }

    async fn submit_data<'lease>(
        &self,
        lease: &'lease ControlLease,
        context: RequestContext,
        partition: PartitionKey,
        command: ProducerCommand,
    ) -> Result<DataSubmission<'lease>> {
        let mut attempt = self.metrics.begin_attempt();
        attempt.bind_stages(self.stages.clone());
        let status = SubmissionStatus::default();
        attempt.bind_submission(status.clone());
        let waiter = status.waiter();
        let receiver = match self
            .enqueue_inner(lease, context, partition, command, status.clone())
            .await
        {
            Ok(receiver) => receiver,
            Err(error) => {
                attempt.finish(error_outcome(&error));
                return Err(error);
            }
        };
        Ok(DataSubmission {
            _waiter: waiter,
            attempt,
            receiver,
            status,
            broker: self.clone(),
            lease,
            context,
            partition,
        })
    }

    async fn enqueue_inner(
        &self,
        lease: &ControlLease,
        context: RequestContext,
        partition: PartitionKey,
        mut command: ProducerCommand,
        status: SubmissionStatus,
    ) -> Result<ResponseReceiver> {
        context.check()?;
        self.check_open()?;
        let started = Instant::now();
        let (bytes, observation) = command_size(&command, &self.config.producer)?;
        let lane = if matches!(command, ProducerCommand::Append(_)) {
            &self.produce
        } else {
            &self.control
        };
        let admission = lane.admit(bytes, &self.memory)?;
        if let ProducerCommand::Append(batch) = &mut command {
            batch.compact_payload().map_err(|reason| Error::Rejected {
                reason: DataRejection::Producer(reason),
            })?;
        }
        if let Some(stages) = &self.stages {
            stages.observe(Stage::Admission, started.elapsed().as_secs_f64());
        }
        self.authorize(lease, context, partition, Work::Produce)
            .await?;
        let now_ms = self
            .with_state(context, |application| {
                application
                    .producer(partition)
                    .map(|state| state.logical_time_ms())
                    .ok_or(Error::PartitionMissing)
            })
            .await?
            .max(unix_millis()?);
        context.check()?;
        self.check_open()?;
        let command = DataCommand::Producer {
            partition,
            writer: WriterFence {
                node_id: self.node_id,
                owner_epoch: lease.grant().epoch,
            },
            now_ms,
            command,
        };
        let pending = self.metrics.begin_submission(u64::from(bytes));
        let (sender, receiver) = oneshot::channel();
        self.submissions
            .try_send(Submission {
                context,
                command,
                observation,
                admission,
                pending,
                stages: self.stages.clone(),
                reply: sender,
                enqueued: Instant::now(),
                admission_deadline: context.deadline.min(lease.expires_at()),
                status: status.clone(),
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::Capacity,
                mpsc::error::TrySendError::Closed(_) => Error::Draining,
            })?;
        Ok(receiver)
    }

    async fn finish_submission(
        &self,
        lease: &ControlLease,
        context: RequestContext,
        partition: PartitionKey,
        receiver: ResponseReceiver,
        status: &SubmissionStatus,
    ) -> Result<DataResponse> {
        let response = match tokio::time::timeout_at(context.deadline.into(), receiver).await {
            Ok(Ok(result)) => result?,
            Ok(Err(_)) if !status.was_handed_off() => return Err(Error::Draining),
            Err(_) if status.cancel_before_handoff() || !status.was_handed_off() => {
                return Err(Error::Deadline);
            }
            _ => {
                return Err(Error::Unknown {
                    operation_id: context.operation_id,
                    request_id: context.request_id,
                });
            }
        };
        if let DataResponse::Rejected(reason) = response {
            return Err(Error::Rejected { reason });
        }
        self.check_reply_lease(lease, partition, context).await?;
        Ok(response)
    }

    fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(Error::Draining)
        } else {
            Ok(())
        }
    }

    async fn with_state<T>(
        &self,
        context: RequestContext,
        query: impl FnOnce(&DataApplication) -> Result<T>,
    ) -> Result<T> {
        tokio::time::timeout_at(
            context.deadline.into(),
            self.state.try_with_application(query),
        )
        .await
        .map_err(|_| Error::Deadline)?
        .map_err(|source| Error::StateRead { source })?
    }

    async fn read_barrier(&self, context: RequestContext) -> Result<()> {
        self.check_open()?;
        tokio::time::timeout_at(context.deadline.into(), self.raft.ensure_linearizable())
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|source| Error::ReadBarrier {
                source: Box::new(source),
            })?;
        Ok(())
    }

    async fn authorize(
        &self,
        lease: &ControlLease,
        context: RequestContext,
        partition: PartitionKey,
        work: Work,
    ) -> Result<FenceRecord> {
        let grant = lease.grant();
        if grant.partition != partition
            || grant.cluster_id != self.config.cluster_id
            || grant.group_id != self.config.group_id
            || grant.owner_node != self.node_id
        {
            return Err(Error::GrantMismatch);
        }
        self.check_open()?;
        let fence = self
            .with_state(context, |application| {
                if application.config() != Some(&self.config) {
                    return Err(Error::Rejected {
                        reason: DataRejection::ConfigurationConflict,
                    });
                }
                let fences = application.ownership().ok_or(Error::PartitionMissing)?;
                fences
                    .check_writer(partition, self.node_id, grant.epoch)
                    .map_err(|reason| Error::Rejected {
                        reason: DataRejection::Fence(reason),
                    })?;
                fences
                    .fence(partition)
                    .cloned()
                    .ok_or(Error::PartitionMissing)
            })
            .await?;
        lease
            .check(&fence, work)
            .map_err(|source| Error::Lease { source })?;
        Ok(fence)
    }

    async fn check_reply_lease(
        &self,
        lease: &ControlLease,
        partition: PartitionKey,
        context: RequestContext,
    ) -> Result<()> {
        let valid = self
            .with_state(context, |application| {
                Ok(application
                    .ownership()
                    .and_then(|state| state.fence(partition))
                    .is_some_and(|fence| lease.check(fence, Work::Produce).is_ok()))
            })
            .await
            .unwrap_or(false);
        if !valid || Instant::now() >= context.deadline {
            return Err(Error::Unknown {
                operation_id: context.operation_id,
                request_id: context.request_id,
            });
        }
        Ok(())
    }
}

fn error_outcome(error: &Error) -> AttemptOutcome {
    if error.result_unknown() {
        return AttemptOutcome::Unknown;
    }
    match error {
        Error::StateRead { .. }
        | Error::UnexpectedResponse
        | Error::Clock { .. }
        | Error::ClockRange
        | Error::Runtime { .. } => AttemptOutcome::Failed,
        _ => AttemptOutcome::Rejected,
    }
}

fn attempt_outcome(result: &Result<DataResponse>) -> AttemptOutcome {
    match result {
        Ok(DataResponse::Produced(result)) if result.duplicate => AttemptOutcome::Duplicate,
        Ok(_) => AttemptOutcome::Committed,
        Err(error) => error_outcome(error),
    }
}

fn command_size(
    command: &ProducerCommand,
    limits: &ProducerLimits,
) -> Result<(u32, Option<CommitObservation>)> {
    let ProducerCommand::Append(batch) = command else {
        return Ok((512, None));
    };
    if batch.messages.is_empty() || batch.messages.len() > limits.max_batch_messages {
        return Err(Error::Rejected {
            reason: DataRejection::Producer(crate::producer::Rejection::InvalidBatch),
        });
    }
    let mut payload = 0_u64;
    for message in &batch.messages {
        let length = message.logical_bytes().ok_or(Error::InvalidRequest)?;
        if length > limits.max_message_bytes {
            return Err(Error::InvalidRequest);
        }
        payload = payload.checked_add(length).ok_or(Error::InvalidRequest)?;
    }
    if payload > limits.max_batch_bytes {
        return Err(Error::InvalidRequest);
    }
    batch.verify_digest().map_err(|reason| Error::Rejected {
        reason: DataRejection::Producer(reason),
    })?;
    let messages = u64::try_from(batch.messages.len()).map_err(|_| Error::InvalidRequest)?;
    let bytes = messages
        .checked_mul(64)
        .and_then(|metadata| metadata.checked_add(payload))
        .and_then(|bytes| bytes.checked_add(512))
        .and_then(|bytes| u32::try_from(bytes).ok())
        .ok_or(Error::InvalidRequest)?;
    Ok((
        bytes,
        Some(CommitObservation {
            messages,
            bytes: payload,
        }),
    ))
}

fn unix_millis() -> Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|source| Error::Clock { source })?
            .as_millis(),
    )
    .map_err(|_| Error::ClockRange)
}
