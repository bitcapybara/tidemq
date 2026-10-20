use std::collections::BTreeMap;
use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use prost::Message;
use tidemq_protocol::{
    Id,
    common::{ErrorCode, FrameKind, TrafficClass},
    producer::canonical_produce_digest,
    public,
};
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout_at},
};

use crate::{Client, Error, Result, admin::bytes};

struct Batch {
    payload: public::ProduceBatch,
    operation: Id,
    uncertain: AtomicBool,
    in_flight: AtomicBool,
    _bytes: OwnedSemaphorePermit,
    _producer_bytes: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
    _domain_slot: OwnedSemaphorePermit,
    _charge: crate::metrics::Charge,
}

/// A bounded, process-local retry identity. Debug never prints message content.
#[derive(Clone)]
pub struct PendingBatch {
    inner: Arc<Batch>,
}

impl fmt::Debug for PendingBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingBatch")
            .field("first_sequence", &self.first_sequence())
            .field("message_count", &self.inner.payload.messages.len())
            .finish_non_exhaustive()
    }
}

impl PendingBatch {
    pub fn first_sequence(&self) -> u64 {
        self.inner.payload.first_sequence
    }
    pub fn operation_id(&self) -> Id {
        self.inner.operation
    }
    pub fn producer_epoch(&self) -> u64 {
        self.inner.payload.producer_epoch
    }
    pub fn digest(&self) -> &[u8] {
        &self.inner.payload.content_digest
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ProduceReceipt {
    pub first_offset: u64,
    pub message_count: u32,
    pub duplicate: bool,
}

struct State {
    next_sequence: u64,
    pending: BTreeMap<u64, PendingBatch>,
    expired: bool,
    rpc: Option<tidemq_transport::rpc::RpcClient>,
}

/// Each instance is one partition/producer/epoch order domain; clones preserve its order.
#[derive(Clone)]
pub struct Producer {
    client: Client,
    topic: Id,
    partition: u32,
    id: Id,
    epoch: u64,
    affinity: u64,
    slots: Arc<Semaphore>,
    state: Arc<Mutex<State>>,
}

impl Producer {
    /// Starts an explicit registration. Keep the supplied identity when an open result is unknown.
    pub async fn open(client: Client, topic: Id, partition: u32, producer: Id) -> Result<Self> {
        client
            .operation(|deadline| {
                Self::open_inner(client.clone(), topic, partition, producer, deadline)
            })
            .await
    }

    async fn open_inner(
        client: Client,
        topic: Id,
        partition: u32,
        producer: Id,
        deadline: Instant,
    ) -> Result<Self> {
        let status: public::ProducerStatusResult = client
            .request(
                FrameKind::ProducerStatus,
                FrameKind::ProducerStatusResult,
                TrafficClass::Control,
                Id::random()?,
                &public::ProducerStatus {
                    topic_id: bytes(topic),
                    partition_id: partition,
                    producer_id: bytes(producer),
                    producer_epoch: 0,
                },
                deadline,
            )
            .await?;
        Self::open_at_inner(
            client,
            topic,
            partition,
            producer,
            status.next_registration_epoch,
            deadline,
        )
        .await
    }

    /// Retries a registration with the same epoch hint; it never silently opens a new session.
    pub async fn open_at(
        client: Client,
        topic: Id,
        partition: u32,
        producer: Id,
        expected_registration_epoch: u64,
    ) -> Result<Self> {
        client
            .operation(|deadline| {
                Self::open_at_inner(
                    client.clone(),
                    topic,
                    partition,
                    producer,
                    expected_registration_epoch,
                    deadline,
                )
            })
            .await
    }

    async fn open_at_inner(
        client: Client,
        topic: Id,
        partition: u32,
        producer: Id,
        expected_registration_epoch: u64,
        deadline: Instant,
    ) -> Result<Self> {
        if expected_registration_epoch == 0 {
            return Err(Error::ResponseMismatch);
        }
        crate::request::mark_registration(producer, expected_registration_epoch);
        let request = public::OpenProducer {
            topic_id: bytes(topic),
            partition_id: partition,
            producer_id: bytes(producer),
            expected_registration_epoch,
            owner_epoch: 0,
        };
        let response: public::ProducerSessionResult = client
            .request(
                FrameKind::OpenProducer,
                FrameKind::ProducerSessionResult,
                TrafficClass::Control,
                producer,
                &request,
                deadline,
            )
            .await
            .map_err(|error| {
                if error.outcome_unknown() || matches!(error, Error::Deadline) {
                    Error::RegistrationUnknown {
                        producer_id: producer,
                        registration_epoch: expected_registration_epoch,
                        source: Box::new(error),
                    }
                } else {
                    error
                }
            })?;
        if response.producer_id.as_ref() != producer.as_bytes()
            || response.producer_epoch != expected_registration_epoch
        {
            return Err(Error::ResponseMismatch);
        }
        Ok(Self {
            slots: Arc::new(Semaphore::new(
                client.inner.config.max_pending_requests.min(32) as usize,
            )),
            client,
            topic,
            partition,
            id: producer,
            epoch: response.producer_epoch,
            affinity: crate::client::affinity(&(
                topic,
                partition,
                producer,
                response.producer_epoch,
            )),
            state: Arc::new(Mutex::new(State {
                next_sequence: response.next_sequence,
                pending: BTreeMap::new(),
                expired: false,
                rpc: None,
            })),
        })
    }

    pub fn id(&self) -> Id {
        self.id
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Enqueues batches in sequence order while responses are read independently.
    #[tracing::instrument(target = "tidemq_trace", name = "tidemq.sdk.produce", skip_all, fields(producer_epoch = self.epoch))]
    pub async fn produce(&self, messages: Vec<public::Message>) -> Result<ProduceReceipt> {
        let result = self
            .client
            .operation(|deadline| self.produce_inner(messages, deadline))
            .await;
        self.record_unknown(&result);
        result
    }

    async fn produce_inner(
        &self,
        messages: Vec<public::Message>,
        deadline: Instant,
    ) -> Result<ProduceReceipt> {
        let _timer = self.client.inner.metrics.timer();
        self.validate_messages(&messages)?;
        let admission = self.client.inner.metrics.admission_timer();
        let domain_slot = timeout_at(deadline, self.slots.clone().acquire_owned())
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|_| Error::Closed)?;
        let request_slot = timeout_at(deadline, self.client.inner.requests.clone().acquire_owned())
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|_| Error::Closed)?;
        let mut state = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| Error::Deadline)?;
        drop(admission);
        if state.expired {
            return Err(Error::SessionExpired);
        }
        if state
            .pending
            .values()
            .any(|batch| batch.inner.uncertain.load(Ordering::Acquire))
        {
            return Err(Error::ProducerBlocked);
        }
        let rpc = self.client.rpc(TrafficClass::Produce, deadline).await?;
        if state
            .rpc
            .as_ref()
            .is_some_and(|previous| !previous.same_instance(&rpc))
            && !state.pending.is_empty()
        {
            for pending in state.pending.values() {
                pending.inner.uncertain.store(true, Ordering::Release);
            }
            return Err(Error::ProducerBlocked);
        }
        state.rpc = Some(rpc.clone());
        let first_sequence = state.next_sequence;
        let count = u64::try_from(messages.len()).map_err(|_| Error::Admission)?;
        let next_sequence = first_sequence
            .checked_add(count)
            .ok_or(Error::Configuration {
                reason: "producer sequence exhausted",
            })?;
        let payload = self.make_payload(first_sequence, messages)?;
        let (batch, frame) = self.prepare(payload, request_slot, domain_slot, deadline)?;
        state.next_sequence = next_sequence;
        state.pending.insert(first_sequence, batch.clone());
        crate::request::mark_produce_pending();
        let guard = Attempt {
            batch: batch.clone(),
            completed: false,
        };
        crate::request::mark_sent();
        let response = rpc
            .submit_affinity(frame, deadline, self.affinity)
            .await
            .map_err(Error::from);
        drop(state);
        let result = self.finish(batch.clone(), guard, response, deadline).await;
        self.retry_rejected(&batch, result, deadline).await
    }

    fn make_payload(
        &self,
        first_sequence: u64,
        messages: Vec<public::Message>,
    ) -> Result<public::ProduceBatch> {
        let digest = canonical_produce_digest(
            self.topic,
            self.partition,
            self.id,
            self.epoch,
            first_sequence,
            &messages,
        )?;
        Ok(public::ProduceBatch {
            topic_id: bytes(self.topic),
            partition_id: self.partition,
            producer_id: bytes(self.id),
            producer_epoch: self.epoch,
            first_sequence,
            messages,
            content_digest: digest.to_vec().into(),
            owner_epoch: 0,
        })
    }

    fn prepare(
        &self,
        mut payload: public::ProduceBatch,
        request_slot: OwnedSemaphorePermit,
        domain_slot: OwnedSemaphorePermit,
        deadline: Instant,
    ) -> Result<(PendingBatch, tidemq_protocol::common::Frame)> {
        let size = payload
            .messages
            .capacity()
            .checked_mul(std::mem::size_of::<public::Message>())
            .and_then(|metadata| metadata.checked_add(payload.encoded_len()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Batch>() + 128))
            .and_then(|bytes| u32::try_from(bytes).ok())
            .ok_or(Error::Admission)?;
        let memory = self
            .client
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(size)
            .map_err(|_| Error::Admission)?;
        let producer_memory = self
            .client
            .inner
            .producer_bytes
            .clone()
            .try_acquire_many_owned(size)
            .map_err(|_| Error::Admission)?;
        // Small caller-owned Bytes views may retain a much larger backing allocation.
        payload.messages = compact_messages(payload.messages)?;
        let operation = Id::random()?;
        let frame = self.client.frame(
            FrameKind::ProduceBatch,
            TrafficClass::Produce,
            Id::random()?,
            operation,
            &payload,
            deadline,
        )?;
        if frame.encoded_len() > self.client.inner.config.protocol.max_frame_bytes as usize {
            return Err(Error::Configuration {
                reason: "encoded producer batch exceeds the frame limit",
            });
        }
        let batch = PendingBatch {
            inner: Arc::new(Batch {
                payload,
                operation,
                uncertain: AtomicBool::new(false),
                in_flight: AtomicBool::new(true),
                _bytes: memory,
                _producer_bytes: producer_memory,
                _slot: request_slot,
                _domain_slot: domain_slot,
                _charge: self.client.inner.metrics.charge(size, false),
            }),
        };
        Ok((batch, frame))
    }

    /// Lists unresolved identities even when the original produce future was dropped.
    pub async fn pending_batches(&self) -> Result<Vec<PendingBatch>> {
        // Local retry identities remain inspectable after closing the network client.
        crate::request::run(
            std::time::Duration::from_millis(self.client.inner.config.request_timeout_ms),
            &tokio_util::sync::CancellationToken::new(),
            |_| self.pending_batches_inner(),
        )
        .await
    }

    async fn pending_batches_inner(&self) -> Result<Vec<PendingBatch>> {
        let state = self.state.lock().await;
        let mut output = Vec::new();
        output
            .try_reserve_exact(state.pending.len())
            .map_err(|_| Error::Admission)?;
        output.extend(state.pending.values().cloned());
        Ok(output)
    }

    pub async fn retry(&self, batch: &PendingBatch) -> Result<ProduceReceipt> {
        let result = self
            .client
            .operation(|deadline| self.retry_inner(batch, deadline))
            .await
            .map_err(|error| {
                if batch.inner.uncertain.load(Ordering::Acquire)
                    && matches!(error, Error::Cancelled | Error::Deadline | Error::Closed)
                {
                    Error::ProduceUnknown {
                        source: Box::new(error),
                    }
                } else {
                    error
                }
            });
        self.record_unknown(&result);
        result
    }

    fn record_unknown(&self, result: &Result<ProduceReceipt>) {
        if result.as_ref().is_err_and(Error::outcome_unknown) {
            self.client.inner.metrics.record_unknown();
        }
    }

    async fn retry_inner(&self, batch: &PendingBatch, deadline: Instant) -> Result<ProduceReceipt> {
        let result = self.retry_once(batch, deadline).await;
        self.retry_rejected(batch, result, deadline).await
    }

    async fn retry_rejected(
        &self,
        batch: &PendingBatch,
        mut result: Result<ProduceReceipt>,
        deadline: Instant,
    ) -> Result<ProduceReceipt> {
        for attempt in 1..self.client.inner.config.max_reconnect_attempts {
            if !result.as_ref().is_err_and(|error| {
                error.retry_rejected()
                    || matches!(error, Error::ProduceUnknown { source } if source.retry_transport())
            }) {
                break;
            }
            if self
                .client
                .backoff(batch.operation_id(), attempt - 1, deadline)
                .await
                .is_err()
            {
                break;
            }
            self.client.inner.metrics.record_retry();
            result = self.retry_once(batch, deadline).await;
        }
        result
    }

    async fn retry_once(&self, batch: &PendingBatch, deadline: Instant) -> Result<ProduceReceipt> {
        let _timer = self.client.inner.metrics.timer();
        let mut state = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| Error::Deadline)?;
        if state.expired {
            return Err(Error::SessionExpired);
        }
        if !state
            .pending
            .get(&batch.first_sequence())
            .is_some_and(|stored| Arc::ptr_eq(&stored.inner, &batch.inner))
        {
            return Err(Error::Configuration {
                reason: "batch does not belong to this live producer",
            });
        }
        if state
            .pending
            .first_key_value()
            .is_some_and(|(sequence, _)| *sequence < batch.first_sequence())
        {
            return Err(Error::ProducerBlocked);
        }
        batch
            .inner
            .in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::Admission)?;
        crate::request::mark_produce_pending();
        let guard = Attempt {
            batch: batch.clone(),
            completed: false,
        };
        let rpc = self.client.rpc(TrafficClass::Produce, deadline).await;
        let response = match rpc {
            Ok(rpc) => {
                if state
                    .rpc
                    .as_ref()
                    .is_some_and(|previous| !previous.same_instance(&rpc))
                {
                    for pending in state.pending.values() {
                        pending.inner.uncertain.store(true, Ordering::Release);
                    }
                }
                state.rpc = Some(rpc.clone());
                crate::request::mark_sent();
                rpc.submit_affinity(
                    self.client.frame(
                        FrameKind::ProduceBatch,
                        TrafficClass::Produce,
                        Id::random()?,
                        batch.inner.operation,
                        &batch.inner.payload,
                        deadline,
                    )?,
                    deadline,
                    self.affinity,
                )
                .await
                .map_err(Error::from)
            }
            Err(error) => Err(error),
        };
        drop(state);
        self.finish(batch.clone(), guard, response, deadline).await
    }

    pub async fn retire(&self) -> Result<()> {
        self.client
            .operation(|deadline| self.retire_inner(deadline))
            .await
    }

    async fn retire_inner(&self, deadline: Instant) -> Result<()> {
        let mut state = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| Error::Deadline)?;
        if !state.pending.is_empty() {
            return Err(Error::ProducerBlocked);
        }
        let response: public::ProducerRetired = self
            .client
            .request(
                FrameKind::RetireProducer,
                FrameKind::ProducerRetired,
                TrafficClass::Control,
                Id::random()?,
                &public::RetireProducer {
                    topic_id: bytes(self.topic),
                    partition_id: self.partition,
                    producer_id: bytes(self.id),
                    producer_epoch: self.epoch,
                    owner_epoch: 0,
                },
                deadline,
            )
            .await?;
        if response.producer_id.as_ref() != self.id.as_bytes()
            || response.producer_epoch != self.epoch
        {
            return Err(Error::ResponseMismatch);
        }
        state.expired = true;
        Ok(())
    }

    async fn finish(
        &self,
        batch: PendingBatch,
        mut guard: Attempt,
        response: Result<tidemq_transport::rpc::RpcResponse>,
        _deadline: Instant,
    ) -> Result<ProduceReceipt> {
        self.client.inner.metrics.record_request();
        let response = match response {
            Ok(response) => response.receive().await.map_err(Error::from),
            Err(error) => Err(error),
        };
        let result = response.and_then(|response| {
            self.client
                .decode::<public::ProduceResult>(response, FrameKind::ProduceResult)
        });
        match result {
            Ok(result)
                if usize::try_from(result.message_count).ok()
                    == Some(batch.inner.payload.messages.len())
                    && result
                        .first_offset
                        .checked_add(u64::from(result.message_count))
                        .is_some() =>
            {
                self.state
                    .lock()
                    .await
                    .pending
                    .remove(&batch.first_sequence());
                guard.completed = true;
                Ok(ProduceReceipt {
                    first_offset: result.first_offset,
                    message_count: result.message_count,
                    duplicate: result.duplicate,
                })
            }
            Err(Error::Remote {
                code: ErrorCode::SessionExpired | ErrorCode::DeduplicationExpired,
                ..
            }) => {
                self.state.lock().await.expired = true;
                Err(Error::SessionExpired)
            }
            Err(
                error @ Error::Remote {
                    outcome: tidemq_protocol::common::Outcome::Rejected,
                    ..
                },
            ) => Err(error),
            other => Err(Error::ProduceUnknown {
                source: Box::new(other.err().unwrap_or(Error::ResponseMismatch)),
            }),
        }
    }

    fn validate_messages(&self, messages: &[public::Message]) -> Result<()> {
        let limits = &self.client.inner.config.protocol;
        if messages.is_empty()
            || messages.len() > limits.max_batch_messages as usize
            || messages.iter().any(|message| {
                message
                    .key
                    .len()
                    .checked_add(message.body.len())
                    .is_none_or(|bytes| bytes > limits.max_message_bytes as usize)
            })
        {
            return Err(Error::Configuration {
                reason: "batch message limits exceeded",
            });
        }
        Ok(())
    }
}

fn compact_messages(messages: Vec<public::Message>) -> Result<Vec<public::Message>> {
    let mut compact = Vec::new();
    compact
        .try_reserve_exact(messages.len())
        .map_err(|_| Error::Admission)?;
    for message in messages {
        compact.push(public::Message {
            key: copy_payload(&message.key)?,
            body: copy_payload(&message.body)?,
        });
    }
    Ok(compact)
}

fn copy_payload(bytes: &[u8]) -> Result<bytes::Bytes> {
    let mut copy = Vec::new();
    copy.try_reserve_exact(bytes.len())
        .map_err(|_| Error::Admission)?;
    copy.extend_from_slice(bytes);
    Ok(copy.into())
}

struct Attempt {
    batch: PendingBatch,
    completed: bool,
}
impl Drop for Attempt {
    fn drop(&mut self) {
        if !self.completed {
            self.batch.inner.uncertain.store(true, Ordering::Release);
        }
        self.batch.inner.in_flight.store(false, Ordering::Release);
    }
}
