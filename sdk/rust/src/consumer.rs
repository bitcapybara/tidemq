//! Durable deliveries and assignment-aware receive barriers.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use tidemq_protocol::{
    Id,
    common::{ErrorCode, FrameKind, TrafficClass},
    public,
};
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout_at},
};

use crate::{Client, Error, Result, admin::bytes, metrics::increment};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryOrdering {
    Parallel,
    KeySerial,
}

#[derive(Clone, Debug)]
pub struct ConsumerConfig {
    pub max_buffered_messages: u32,
    pub max_buffered_bytes: u32,
    pub ordering: DeliveryOrdering,
    /// A takeover is explicit; None never steals another logical session.
    pub replace_session_id: Option<Id>,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            max_buffered_messages: 256,
            max_buffered_bytes: 4 * 1024 * 1024,
            ordering: DeliveryOrdering::Parallel,
            replace_session_id: None,
        }
    }
}

struct DeliveryData {
    offset: u64,
    key: Bytes,
    body: Bytes,
    token: Bytes,
    generation: Generation,
    _bytes: OwnedSemaphorePermit,
    _client_bytes: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
    _charge: crate::metrics::Charge,
}

/// Payload and opaque token stay charged while the host retains any clone.
#[derive(Clone)]
pub struct Delivery {
    inner: Arc<DeliveryData>,
}

impl Delivery {
    pub fn offset(&self) -> u64 {
        self.inner.offset
    }
    pub fn key(&self) -> &[u8] {
        &self.inner.key
    }
    pub fn body(&self) -> &[u8] {
        &self.inner.body
    }
}

impl fmt::Debug for Delivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delivery")
            .field("offset", &self.offset())
            .field("key_bytes", &self.inner.key.len())
            .field("body_bytes", &self.inner.body.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Generation {
    topic: Id,
    partition: u32,
    subscription: Id,
    session: Id,
    cursor: u64,
    assignment: u64,
}

struct State {
    assignment: public::AssignmentResult,
    generation: Generation,
    queue: BTreeMap<u64, Delivery>,
    failed: BTreeMap<u64, Delivery>,
    failed_offsets: BTreeSet<u64>,
    busy_keys: HashMap<Bytes, Bytes>,
    received_before: u64,
    coverage: BTreeMap<u64, u64>,
    barrier_confirmed: bool,
    poll_bulk_next: bool,
    valid: bool,
    recoverable: bool,
}

/// One handle owns a partition assignment and bounded delivery scheduling state.
#[derive(Clone)]
pub struct Consumer {
    client: Client,
    config: ConsumerConfig,
    state: Arc<Mutex<State>>,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    _claim: Arc<ClaimTicket>,
    _failure_budget: Arc<ReadCredit>,
}

impl Consumer {
    pub async fn claim(
        client: Client,
        topic: Id,
        partition: u32,
        subscription: Id,
        config: ConsumerConfig,
        deadline: Instant,
    ) -> Result<Self> {
        if config.max_buffered_messages == 0
            || config.max_buffered_messages > 65_536
            || config.max_buffered_bytes == 0
            || u64::from(config.max_buffered_bytes) > client.inner.config.max_pending_bytes
        {
            return Err(Error::Configuration {
                reason: "consumer queue bounds are invalid",
            });
        }
        let ticket = ClaimTicket::reserve(client.clone(), (topic, partition, subscription))?;
        let slots = Arc::new(Semaphore::new(config.max_buffered_messages as usize));
        let byte_budget = Arc::new(Semaphore::new(config.max_buffered_bytes as usize));
        let failure_budget = reserve_failures(&client, &config, &byte_budget)?;
        let status = client
            .admin()
            .subscription_status(topic, partition, subscription, deadline)
            .await?;
        let previous = status.assignment.ok_or(Error::ResponseMismatch)?;
        if status.assigned
            && previous.session_id.as_ref() != client.session_id().as_bytes()
            && config.replace_session_id.is_none()
        {
            return Err(Error::Remote {
                code: ErrorCode::AssignmentBusy,
                outcome: tidemq_protocol::common::Outcome::Rejected,
                retryable: false,
                expected_sequence: None,
                expected_registration_epoch: None,
                retry_after_ms: None,
            });
        }
        let request = public::ClaimAssignment {
            topic_id: bytes(topic),
            partition_id: partition,
            subscription_id: bytes(subscription),
            expected_cursor_generation: previous.cursor_generation,
            expected_assignment_generation: previous.assignment_generation,
            session_id: bytes(client.session_id()),
            replace_session_id: if status.assigned {
                config.replace_session_id.map(bytes).unwrap_or_default()
            } else {
                Bytes::new()
            },
            cold_before: None,
            owner_epoch: 0,
        };
        let assignment: public::AssignmentResult = client
            .request(
                FrameKind::ClaimAssignment,
                FrameKind::AssignmentResult,
                TrafficClass::Control,
                Id::random()?,
                &request,
                deadline,
            )
            .await?;
        let generation = validate_assignment(
            &assignment,
            topic,
            partition,
            subscription,
            client.session_id(),
        )?;
        if assignment.received_before != assignment.acknowledged_before {
            return Err(Error::ResponseMismatch);
        }
        let received_before = assignment.acknowledged_before;
        Ok(Self {
            _claim: Arc::new(ticket),
            slots,
            bytes: byte_budget,
            _failure_budget: Arc::new(failure_budget),
            client,
            config,
            state: Arc::new(Mutex::new(State {
                barrier_confirmed: received_before >= assignment.cold_before,
                assignment,
                generation,
                queue: BTreeMap::new(),
                failed: BTreeMap::new(),
                failed_offsets: BTreeSet::new(),
                busy_keys: HashMap::new(),
                received_before,
                coverage: BTreeMap::new(),
                valid: true,
                recoverable: false,
                poll_bulk_next: false,
            })),
        })
    }

    /// Fetches into the bounded queue; it never acknowledges business completion.
    pub async fn fill(&self, deadline: Instant) -> Result<usize> {
        let mut last = Error::StaleDelivery;
        for attempt in 0..self.client.inner.config.max_reconnect_attempts {
            match self.fill_once(deadline).await {
                Ok(count) => return Ok(count),
                Err(error) => last = error,
            }
            let mut state = timeout_at(deadline, self.state.lock())
                .await
                .map_err(|_| Error::Deadline)?;
            if !state.recoverable {
                if state.valid && assignment_error(&last) {
                    continue;
                }
                return Err(last);
            }
            match self.reclaim_locked(&mut state, deadline, false).await {
                Ok(()) => {}
                Err(error) if retry_assignment(&error) => last = error,
                Err(error) => return Err(error),
            }
            drop(state);
            if attempt + 1 < self.client.inner.config.max_reconnect_attempts {
                timeout_at(
                    deadline,
                    tokio::time::sleep(self.client.inner.config.retry_backoff),
                )
                .await
                .map_err(|_| Error::Deadline)?;
            }
        }
        // A successful final refresh still permits one read within the same total deadline.
        let valid = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| Error::Deadline)?
            .valid;
        if valid {
            self.fill_once(deadline).await
        } else {
            Err(last)
        }
    }

    async fn fill_once(&self, deadline: Instant) -> Result<usize> {
        let admission = self.client.inner.metrics.admission_timer();
        let mut state = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| Error::Deadline)?;
        drop(admission);
        if !state.valid {
            return Err(Error::StaleDelivery);
        }
        if state.received_before > state.assignment.received_before
            || (state.received_before >= state.assignment.cold_before && !state.barrier_confirmed)
        {
            self.confirm_barrier(&mut state, deadline).await?;
        }
        let available = self
            .slots
            .available_permits()
            .min(self.client.inner.config.protocol.max_batch_messages as usize);
        let available_bytes = self
            .bytes
            .available_permits()
            .min(self.client.inner.bytes.available_permits())
            .min(self.client.inner.config.protocol.max_frame_bytes as usize);
        let (available, payload_bytes) = read_credit(
            available,
            available_bytes,
            self.client.inner.config.protocol.max_message_bytes as usize,
        )?;
        let credit = ReadCredit {
            bytes: self
                .bytes
                .clone()
                .try_acquire_many_owned(
                    u32::try_from(available_bytes).map_err(|_| Error::Admission)?,
                )
                .map_err(|_| Error::Admission)?,
            client: self
                .client
                .inner
                .bytes
                .clone()
                .try_acquire_many_owned(
                    u32::try_from(available_bytes).map_err(|_| Error::Admission)?,
                )
                .map_err(|_| Error::Admission)?,
        };
        let (mut batch, class) = match self
            .read_batch(&state, available, payload_bytes, deadline)
            .await
        {
            Ok(batch) => batch,
            Err(error) => {
                invalidate_on_error(&mut state, &error);
                return Err(error);
            }
        };
        validate_batch(&batch, &state, class)?;
        self.validate_admission(&batch, &state, available, available_bytes)?;
        if batch
            .deliveries
            .iter()
            .try_fold(0_usize, |sum, item| {
                sum.checked_add(item.key.len())?
                    .checked_add(item.body.len())
            })
            .is_none_or(|sum| sum > payload_bytes)
        {
            return Err(Error::ResponseMismatch);
        }
        let incoming = self.admit_deliveries(
            std::mem::take(&mut batch.deliveries),
            state.generation,
            credit,
        )?;
        let count = self.enqueue_batch(&mut state, batch, incoming)?;
        if state.received_before > state.assignment.received_before {
            self.confirm_barrier(&mut state, deadline).await?;
        }
        state.poll_bulk_next = class == TrafficClass::Delivery
            && state.assignment.acknowledged_before < state.assignment.cold_before;
        Ok(count)
    }

    async fn read_batch(
        &self,
        state: &State,
        available: usize,
        available_bytes: usize,
        deadline: Instant,
    ) -> Result<(public::DeliveryBatch, TrafficClass)> {
        let class = if !state.barrier_confirmed
            || (state.poll_bulk_next
                && state.assignment.acknowledged_before < state.assignment.cold_before)
        {
            TrafficClass::Bulk
        } else {
            TrafficClass::Delivery
        };
        let generation = state.generation;
        let request = public::ReadRequest {
            topic_id: bytes(generation.topic),
            partition_id: generation.partition,
            subscription_id: bytes(generation.subscription),
            cursor_generation: generation.cursor,
            assignment_generation: generation.assignment,
            delivery_generation: generation.assignment,
            session_id: bytes(generation.session),
            max_messages: u32::try_from(available).map_err(|_| Error::Admission)?,
            max_bytes: u32::try_from(available_bytes).map_err(|_| Error::Admission)?,
        };
        let batch = self
            .client
            .request_affinity(
                FrameKind::Read,
                FrameKind::DeliveryBatch,
                class,
                Id::random()?,
                &request,
                deadline,
                crate::client::affinity(&(
                    generation.topic,
                    generation.partition,
                    generation.subscription,
                    generation.cursor,
                    generation.assignment,
                )),
            )
            .await?;
        Ok((batch, class))
    }

    fn validate_admission(
        &self,
        batch: &public::DeliveryBatch,
        state: &State,
        available: usize,
        available_bytes: usize,
    ) -> Result<()> {
        if batch.deliveries.len() > available
            || batch.deliveries.iter().any(|delivery| {
                delivery
                    .key
                    .len()
                    .checked_add(delivery.body.len())
                    .is_none_or(|size| {
                        size > self.client.inner.config.protocol.max_message_bytes as usize
                    })
            })
        {
            return Err(Error::ResponseMismatch);
        }
        let has_head = batch
            .deliveries
            .iter()
            .any(|delivery| delivery.offset == state.received_before)
            || batch.acknowledged_ranges.iter().any(|range| {
                range.start <= state.received_before && range.end > state.received_before
            });
        let charged = batch
            .deliveries
            .iter()
            .try_fold(0_usize, |total, delivery| {
                total
                    .checked_add(delivery_charge(delivery)? as usize)
                    .ok_or(Error::Admission)
            })?;
        let reserve = (self.client.inner.config.protocol.max_message_bytes as usize)
            .checked_add(std::mem::size_of::<DeliveryData>() + 229)
            .ok_or(Error::Admission)?;
        if self.config.ordering == DeliveryOrdering::KeySerial
            && !has_head
            && !batch.deliveries.is_empty()
            && (batch.deliveries.len() >= available
                || available_bytes.saturating_sub(charged) < reserve)
        {
            return Err(Error::Admission);
        }
        Ok(())
    }

    fn admit_deliveries(
        &self,
        deliveries: Vec<public::Delivery>,
        generation: Generation,
        mut credit: ReadCredit,
    ) -> Result<Vec<Delivery>> {
        let mut incoming = Vec::new();
        incoming
            .try_reserve_exact(deliveries.len())
            .map_err(|_| Error::Admission)?;
        for delivery in deliveries {
            let length = delivery_charge(&delivery)?;
            let data = DeliveryData {
                offset: delivery.offset,
                key: delivery.key,
                body: delivery.body,
                token: delivery.delivery_token,
                generation,
                _bytes: credit
                    .bytes
                    .split(length as usize)
                    .ok_or(Error::ResponseMismatch)?,
                _client_bytes: credit
                    .client
                    .split(length as usize)
                    .ok_or(Error::ResponseMismatch)?,
                _slot: self
                    .slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::Admission)?,
                _charge: self.client.inner.metrics.charge(length, true),
            };
            incoming.push(Delivery {
                inner: Arc::new(data),
            });
        }
        Ok(incoming)
    }

    fn enqueue_batch(
        &self,
        state: &mut State,
        batch: public::DeliveryBatch,
        incoming: Vec<Delivery>,
    ) -> Result<usize> {
        let count = incoming.len();
        for delivery in incoming {
            if state.failed_offsets.contains(&delivery.offset()) {
                state
                    .busy_keys
                    .insert(delivery.inner.key.clone(), delivery.inner.token.clone());
            }
            cover(
                state,
                delivery.offset(),
                delivery
                    .offset()
                    .checked_add(1)
                    .ok_or(Error::ResponseMismatch)?,
                self.config.max_buffered_messages,
            )?;
            state.queue.insert(delivery.offset(), delivery);
        }
        for range in batch.acknowledged_ranges {
            clear_acknowledged_failures(state, range.start, range.end);
            cover(
                state,
                range.start,
                range.end,
                self.config.max_buffered_messages,
            )?;
        }
        state.assignment.issued_before = batch.issued_before;
        increment(
            &self.client.inner.metrics.deliveries,
            u64::try_from(count).unwrap_or(u64::MAX),
        );
        Ok(count)
    }

    async fn confirm_barrier(&self, state: &mut State, deadline: Instant) -> Result<()> {
        let generation = state.generation;
        let request = public::ReceiveBarrier {
            subscription_id: bytes(generation.subscription),
            partition_id: generation.partition,
            cursor_generation: generation.cursor,
            assignment_generation: generation.assignment,
            received_before: state.received_before,
            topic_id: bytes(generation.topic),
            delivery_generation: generation.assignment,
            session_id: bytes(generation.session),
        };
        let response: Result<public::AssignmentResult> = self
            .client
            .request(
                FrameKind::ReceiveBarrier,
                FrameKind::AssignmentResult,
                TrafficClass::Control,
                Id::random()?,
                &request,
                deadline,
            )
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                invalidate_on_error(state, &error);
                return Err(error);
            }
        };
        if validate_assignment(
            &response,
            generation.topic,
            generation.partition,
            generation.subscription,
            generation.session,
        )? != generation
            || response.received_before < state.received_before
            || response.cold_before != state.assignment.cold_before
        {
            return Err(Error::ResponseMismatch);
        }
        state.barrier_confirmed = state.received_before >= state.assignment.cold_before;
        state.assignment.received_before = state.received_before;
        Ok(())
    }

    /// Returns queued work. KeySerial keeps a key blocked until explicit ACK or retry.
    pub async fn next(&self) -> Result<Option<Delivery>> {
        let mut state = self.state.lock().await;
        if !state.valid {
            return Err(Error::StaleDelivery);
        }
        let offset = state
            .queue
            .iter()
            .find(|(offset, delivery)| {
                self.config.ordering == DeliveryOrdering::Parallel
                    || (**offset < state.received_before
                        && !state.busy_keys.contains_key(&delivery.inner.key))
            })
            .map(|(offset, _)| *offset);
        let Some(delivery) = offset.and_then(|offset| state.queue.remove(&offset)) else {
            return Ok(None);
        };
        if self.config.ordering == DeliveryOrdering::KeySerial {
            state
                .busy_keys
                .insert(delivery.inner.key.clone(), delivery.inner.token.clone());
        }
        Ok(Some(delivery))
    }

    pub async fn recv(&self, deadline: Instant) -> Result<Option<Delivery>> {
        match self.next().await {
            Ok(Some(delivery)) => return Ok(Some(delivery)),
            Ok(None) | Err(Error::StaleDelivery) => {}
            Err(error) => return Err(error),
        }
        self.fill(deadline).await?;
        self.next().await
    }

    pub async fn ack(&self, delivery: &Delivery, deadline: Instant) -> Result<public::AckResult> {
        self.ack_many(std::slice::from_ref(delivery), deadline)
            .await
    }

    /// Splits ACKs using both token count and the encoded control-frame byte limit.
    pub async fn ack_many(
        &self,
        deliveries: &[Delivery],
        deadline: Instant,
    ) -> Result<public::AckResult> {
        if deliveries.is_empty() {
            return Err(Error::Configuration {
                reason: "ACK batch is empty",
            });
        }
        let mut state = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| Error::Deadline)?;
        let generation = state.generation;
        if !state.valid
            || deliveries
                .iter()
                .any(|delivery| delivery.inner.generation != generation)
        {
            return Err(Error::StaleDelivery);
        }
        let mut result = public::AckResult::default();
        let mut batch = ack_batch(generation);
        let mut admitted = Vec::new();
        for delivery in deliveries {
            batch.delivery_tokens.push(delivery.inner.token.clone());
            if batch.delivery_tokens.len()
                > self.client.inner.config.protocol.max_batch_messages as usize
                || !ack_fits(&self.client, &batch)?
            {
                batch.delivery_tokens.pop();
                if batch.delivery_tokens.is_empty() {
                    return Err(Error::Admission);
                }
                self.flush_ack(&mut state, &batch, &admitted, &mut result, deadline)
                    .await?;
                batch = ack_batch(generation);
                admitted.clear();
                batch.delivery_tokens.push(delivery.inner.token.clone());
                if !ack_fits(&self.client, &batch)? {
                    return Err(Error::Admission);
                }
            }
            admitted.push(delivery);
        }
        if !batch.delivery_tokens.is_empty() {
            self.flush_ack(&mut state, &batch, &admitted, &mut result, deadline)
                .await?;
        }
        Ok(result)
    }

    async fn flush_ack(
        &self,
        state: &mut State,
        batch: &public::AckBatch,
        deliveries: &[&Delivery],
        aggregate: &mut public::AckResult,
        deadline: Instant,
    ) -> Result<()> {
        let response: Result<public::AckResult> = self
            .client
            .request(
                FrameKind::AckBatch,
                FrameKind::AckResult,
                TrafficClass::Control,
                Id::random()?,
                batch,
                deadline,
            )
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                invalidate_on_error(state, &error);
                if matches!(
                    error,
                    Error::Remote {
                        code: ErrorCode::StaleDelivery,
                        ..
                    }
                ) {
                    for delivery in deliveries {
                        release_key(state, delivery);
                    }
                }
                return Err(error);
            }
        };
        if u64::from(response.accepted).checked_add(u64::from(response.already_acknowledged))
            != Some(deliveries.len() as u64)
        {
            return Err(Error::ResponseMismatch);
        }
        for delivery in deliveries {
            release_key(state, delivery);
            clear_failure(state, delivery.offset());
        }
        aggregate.accepted = aggregate
            .accepted
            .checked_add(response.accepted)
            .ok_or(Error::ResponseMismatch)?;
        aggregate.already_acknowledged = aggregate
            .already_acknowledged
            .checked_add(response.already_acknowledged)
            .ok_or(Error::ResponseMismatch)?;
        aggregate.acknowledged_before = aggregate
            .acknowledged_before
            .max(response.acknowledged_before);
        state.assignment.acknowledged_before = state
            .assignment
            .acknowledged_before
            .max(response.acknowledged_before);
        state.assignment.issued_before = state
            .assignment
            .issued_before
            .max(response.acknowledged_before);
        cover(
            state,
            state.received_before,
            response.acknowledged_before,
            self.config.max_buffered_messages,
        )?;
        increment(
            &self.client.inner.metrics.acknowledged,
            deliveries.len() as u64,
        );
        Ok(())
    }

    /// Failure never sends an ACK. KeySerial deliberately leaves this key blocked.
    pub async fn fail(&self, delivery: Delivery) -> Result<()> {
        let mut state = self.state.lock().await;
        if delivery.inner.generation != state.generation || !state.valid {
            return Err(Error::StaleDelivery);
        }
        if self.config.ordering == DeliveryOrdering::KeySerial
            && state.busy_keys.get(&delivery.inner.key) != Some(&delivery.inner.token)
        {
            return Err(Error::StaleDelivery);
        }
        if self.config.ordering == DeliveryOrdering::KeySerial {
            if state.failed_offsets.len() >= self.config.max_buffered_messages as usize
                && !state.failed_offsets.contains(&delivery.offset())
            {
                return Err(Error::Admission);
            }
            state.failed_offsets.insert(delivery.offset());
        }
        state.failed.insert(delivery.offset(), delivery);
        Ok(())
    }

    pub async fn retry_failed(&self, offset: u64) -> Result<()> {
        let mut state = self.state.lock().await;
        let delivery = state.failed.remove(&offset);
        let marked = state.failed_offsets.remove(&offset);
        if !marked && delivery.is_none() {
            return Err(Error::KeyBlocked);
        }
        if marked && let Some(queued) = state.queue.get(&offset).cloned() {
            release_key(&mut state, &queued);
        }
        if let Some(delivery) = delivery {
            release_key(&mut state, &delivery);
            if state.valid
                && delivery.inner.generation == state.generation
                && !state.queue.contains_key(&offset)
            {
                state.queue.insert(offset, delivery);
            }
        }
        Ok(())
    }

    /// Seek invalidates all older tokens, queued deliveries and receive-barrier evidence.
    pub async fn seek(&self, offset: u64, operation: Id, deadline: Instant) -> Result<()> {
        let mut state = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| Error::Deadline)?;
        let generation = state.generation;
        let was_valid = state.valid;
        state.valid = false;
        state.recoverable = false;
        let response: Result<public::SeekResult> = self
            .client
            .request(
                FrameKind::SeekSubscription,
                FrameKind::SeekResult,
                TrafficClass::Control,
                operation,
                &public::SeekSubscription {
                    topic_id: bytes(generation.topic),
                    partition_id: generation.partition,
                    subscription_id: bytes(generation.subscription),
                    expected_cursor_generation: generation.cursor,
                    expected_assignment_generation: generation.assignment,
                    target_offset: offset,
                    owner_epoch: 0,
                },
                deadline,
            )
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                invalidate_on_error(&mut state, &error);
                if !state.recoverable
                    && !error.outcome_unknown()
                    && !matches!(error, Error::Deadline)
                {
                    state.valid = was_valid;
                }
                return Err(error);
            }
        };
        state.queue.clear();
        state.failed.clear();
        state.failed_offsets.clear();
        state.busy_keys.clear();
        state.coverage.clear();
        if response.cursor_generation <= generation.cursor
            || response.assignment_generation != response.delivery_generation
        {
            return Err(Error::ResponseMismatch);
        }
        let assignment: public::AssignmentResult = self
            .client
            .request(
                FrameKind::ClaimAssignment,
                FrameKind::AssignmentResult,
                TrafficClass::Control,
                Id::random()?,
                &public::ClaimAssignment {
                    topic_id: bytes(generation.topic),
                    partition_id: generation.partition,
                    subscription_id: bytes(generation.subscription),
                    expected_cursor_generation: response.cursor_generation,
                    expected_assignment_generation: response.assignment_generation,
                    session_id: bytes(generation.session),
                    replace_session_id: Bytes::new(),
                    cold_before: None,
                    owner_epoch: 0,
                },
                deadline,
            )
            .await?;
        state.generation = validate_assignment(
            &assignment,
            generation.topic,
            generation.partition,
            generation.subscription,
            generation.session,
        )?;
        state.received_before = assignment.acknowledged_before;
        state.barrier_confirmed = state.received_before >= assignment.cold_before;
        state.assignment = assignment;
        state.valid = true;
        state.recoverable = false;
        state.poll_bulk_next = false;
        Ok(())
    }

    /// Revalidates ownership after a fence. Other sessions require explicit takeover consent.
    pub async fn reclaim(&self, deadline: Instant) -> Result<()> {
        let mut state = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| Error::Deadline)?;
        self.reclaim_locked(&mut state, deadline, true).await
    }

    // Keep the fresh status and CAS under one consumer lock so clones share one recovery.
    async fn reclaim_locked(
        &self,
        state: &mut State,
        deadline: Instant,
        allow_takeover: bool,
    ) -> Result<()> {
        let generation = state.generation;
        let status = self
            .client
            .admin()
            .subscription_status(
                generation.topic,
                generation.partition,
                generation.subscription,
                deadline,
            )
            .await?;
        let previous = status.assignment.ok_or(Error::ResponseMismatch)?;
        let replacing =
            self.replacement_session(state, &previous, status.assigned, allow_takeover)?;
        let was_valid = state.valid;
        // A cancelled Claim may already have committed a new generation on the server.
        state.valid = false;
        state.recoverable = true;
        let assignment: public::AssignmentResult = self
            .client
            .request(
                FrameKind::ClaimAssignment,
                FrameKind::AssignmentResult,
                TrafficClass::Control,
                Id::random()?,
                &public::ClaimAssignment {
                    topic_id: bytes(generation.topic),
                    partition_id: generation.partition,
                    subscription_id: bytes(generation.subscription),
                    expected_cursor_generation: previous.cursor_generation,
                    expected_assignment_generation: previous.assignment_generation,
                    session_id: bytes(generation.session),
                    replace_session_id: replacing,
                    cold_before: None,
                    owner_epoch: 0,
                },
                deadline,
            )
            .await?;
        install_assignment(state, assignment, was_valid)
    }

    fn replacement_session(
        &self,
        state: &mut State,
        previous: &public::AssignmentResult,
        assigned: bool,
        allow_takeover: bool,
    ) -> Result<Bytes> {
        if !assigned || previous.session_id.as_ref() == state.generation.session.as_bytes() {
            return Ok(Bytes::new());
        }
        invalidate_assignment(state);
        if !allow_takeover {
            return Err(Error::Remote {
                code: ErrorCode::AssignmentBusy,
                outcome: tidemq_protocol::common::Outcome::Rejected,
                retryable: false,
                expected_sequence: None,
                expected_registration_epoch: None,
                retry_after_ms: None,
            });
        }
        let replacement = self.config.replace_session_id.ok_or(Error::StaleDelivery)?;
        if previous.session_id.as_ref() != replacement.as_bytes() {
            return Err(Error::StaleDelivery);
        }
        Ok(bytes(replacement))
    }
}

fn install_assignment(
    state: &mut State,
    assignment: public::AssignmentResult,
    was_valid: bool,
) -> Result<()> {
    let generation = state.generation;
    let new_generation = validate_assignment(
        &assignment,
        generation.topic,
        generation.partition,
        generation.subscription,
        generation.session,
    )?;
    if new_generation.cursor < generation.cursor
        || (new_generation.cursor == generation.cursor
            && new_generation.assignment < generation.assignment)
    {
        return Err(Error::ResponseMismatch);
    }
    if new_generation == generation {
        if assignment.cold_before != state.assignment.cold_before
            || assignment.received_before > state.received_before
        {
            return Err(Error::ResponseMismatch);
        }
    } else if assignment.received_before != assignment.acknowledged_before {
        return Err(Error::ResponseMismatch);
    }
    if new_generation != generation || !was_valid {
        state.queue.clear();
        state.failed.clear();
        state.busy_keys.clear();
        state.coverage.clear();
        state.received_before = assignment.acknowledged_before;
        if new_generation.cursor != generation.cursor {
            state.failed_offsets.clear();
        } else {
            state
                .failed_offsets
                .retain(|offset| *offset >= assignment.acknowledged_before);
        }
    }
    state.generation = new_generation;
    state.barrier_confirmed = state.received_before >= assignment.cold_before;
    state.assignment = assignment;
    state.valid = true;
    state.recoverable = false;
    state.poll_bulk_next = false;
    Ok(())
}

fn invalidate_on_error(state: &mut State, error: &Error) {
    if matches!(
        error,
        Error::Remote {
            code: ErrorCode::Fenced | ErrorCode::StaleGeneration,
            ..
        }
    ) {
        invalidate_assignment(state);
    }
}

fn invalidate_assignment(state: &mut State) {
    state.valid = false;
    state.recoverable = true;
    state.queue.clear();
    state.failed.clear();
    state.busy_keys.clear();
    state.coverage.clear();
}

fn retry_assignment(error: &Error) -> bool {
    error.retry_rejected()
        || error.retry_transport()
        || error.outcome_unknown()
        || matches!(
            error,
            Error::Remote {
                code: ErrorCode::StaleGeneration,
                retryable: true,
                ..
            }
        )
}

fn assignment_error(error: &Error) -> bool {
    matches!(
        error,
        Error::StaleDelivery
            | Error::Remote {
                code: ErrorCode::Fenced | ErrorCode::StaleGeneration,
                ..
            }
    )
}

fn reserve_failures(
    client: &Client,
    config: &ConsumerConfig,
    bytes: &Arc<Semaphore>,
) -> Result<ReadCredit> {
    // Fixed offset markers survive fencing without retaining obsolete delivery bodies or tokens.
    let reserve = if config.ordering == DeliveryOrdering::KeySerial {
        config
            .max_buffered_messages
            .checked_mul(40)
            .ok_or(Error::Admission)?
    } else {
        0
    };
    if reserve >= config.max_buffered_bytes {
        return Err(Error::Configuration {
            reason: "consumer byte bound must leave room beyond failed-offset metadata",
        });
    }
    Ok(ReadCredit {
        bytes: bytes
            .clone()
            .try_acquire_many_owned(reserve)
            .map_err(|_| Error::Admission)?,
        client: client
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(reserve)
            .map_err(|_| Error::Admission)?,
    })
}

fn release_key(state: &mut State, delivery: &Delivery) {
    if state.busy_keys.get(&delivery.inner.key) == Some(&delivery.inner.token) {
        state.busy_keys.remove(&delivery.inner.key);
    }
}

fn clear_failure(state: &mut State, offset: u64) {
    if state.failed_offsets.remove(&offset)
        && let Some(queued) = state.queue.get(&offset).cloned()
    {
        release_key(state, &queued);
    }
    if let Some(delivery) = state.failed.remove(&offset) {
        release_key(state, &delivery);
    }
}

fn clear_acknowledged_failures(state: &mut State, start: u64, end: u64) {
    if start >= end {
        return;
    }
    while let Some(offset) = state.failed_offsets.range(start..end).next().copied() {
        clear_failure(state, offset);
    }
}

struct ClaimTicket {
    client: Client,
    key: (Id, u32, Id),
}
impl ClaimTicket {
    fn reserve(client: Client, key: (Id, u32, Id)) -> Result<Self> {
        {
            let mut claims = client
                .inner
                .consumer_claims
                .lock()
                .map_err(|_| Error::Closed)?;
            if claims.contains(&key) {
                return Err(Error::Configuration {
                    reason: "this client already owns a consumer handle for the subscription partition",
                });
            }
            if claims.len() >= client.inner.config.max_pending_requests as usize {
                return Err(Error::Admission);
            }
            claims.try_reserve(1).map_err(|_| Error::Admission)?;
            claims.insert(key);
        }
        Ok(Self { client, key })
    }
}
impl Drop for ClaimTicket {
    fn drop(&mut self) {
        if let Ok(mut claims) = self.client.inner.consumer_claims.lock() {
            claims.remove(&self.key);
        }
    }
}

fn validate_assignment(
    assignment: &public::AssignmentResult,
    topic: Id,
    partition: u32,
    subscription: Id,
    session: Id,
) -> Result<Generation> {
    if assignment.topic_id.as_ref() != topic.as_bytes()
        || assignment.partition_id != partition
        || assignment.subscription_id.as_ref() != subscription.as_bytes()
        || assignment.session_id.as_ref() != session.as_bytes()
        || assignment.cursor_generation == 0
        || assignment.assignment_generation == 0
        || assignment.assignment_generation != assignment.delivery_generation
        || assignment.acknowledged_before > assignment.received_before
        || assignment.received_before > assignment.issued_before
        || assignment.acknowledged_before > assignment.cold_before
    {
        return Err(Error::ResponseMismatch);
    }
    Ok(Generation {
        topic,
        partition,
        subscription,
        session,
        cursor: assignment.cursor_generation,
        assignment: assignment.assignment_generation,
    })
}

fn validate_batch(batch: &public::DeliveryBatch, state: &State, class: TrafficClass) -> Result<()> {
    let generation = state.generation;
    if batch.topic_id.as_ref() != generation.topic.as_bytes()
        || batch.partition_id != generation.partition
        || batch.subscription_id.as_ref() != generation.subscription.as_bytes()
        || batch.session_id.as_ref() != generation.session.as_bytes()
        || batch.cursor_generation != generation.cursor
        || batch.assignment_generation != generation.assignment
        || batch.delivery_generation != generation.assignment
        || batch.cold_before != state.assignment.cold_before
        || batch.issued_before < state.received_before
        || batch.acknowledged_ranges.len() > 65_536
    {
        return Err(Error::StaleDelivery);
    }
    let mut previous = None;
    for delivery in &batch.deliveries {
        if previous.is_some_and(|offset| offset >= delivery.offset)
            || delivery.offset >= batch.issued_before
            || delivery.delivery_token.len() != 101
            || (class == TrafficClass::Bulk && delivery.offset >= batch.cold_before)
            || (class == TrafficClass::Delivery && delivery.offset < batch.cold_before)
        {
            return Err(Error::ResponseMismatch);
        }
        previous = Some(delivery.offset);
    }
    let mut previous_end = 0;
    for range in &batch.acknowledged_ranges {
        if range.start >= range.end
            || range.start < previous_end
            || range.end > batch.issued_before
            || batch
                .deliveries
                .iter()
                .any(|delivery| delivery.offset >= range.start && delivery.offset < range.end)
        {
            return Err(Error::ResponseMismatch);
        }
        previous_end = range.end;
    }
    Ok(())
}

fn cover(state: &mut State, start: u64, end: u64, limit: u32) -> Result<()> {
    if start >= end || end <= state.received_before {
        return Ok(());
    }
    let mut start = start.max(state.received_before);
    let mut end = end;
    if let Some((&previous_start, &previous_end)) = state.coverage.range(..=start).next_back()
        && previous_end >= start
    {
        start = previous_start;
        end = end.max(previous_end);
        state.coverage.remove(&previous_start);
    }
    while let Some((&next_start, &next_end)) = state.coverage.range(start..=end).next() {
        end = end.max(next_end);
        state.coverage.remove(&next_start);
    }
    if start <= state.received_before {
        state.received_before = state.received_before.max(end);
    } else {
        if state.coverage.len() >= limit as usize {
            return Err(Error::Admission);
        }
        state.coverage.insert(start, end);
    }
    while let Some((&start, &end)) = state.coverage.first_key_value() {
        if start > state.received_before {
            break;
        }
        state.received_before = state.received_before.max(end);
        state.coverage.remove(&start);
    }
    Ok(())
}

fn ack_batch(generation: Generation) -> public::AckBatch {
    public::AckBatch {
        subscription_id: bytes(generation.subscription),
        partition_id: generation.partition,
        cursor_generation: generation.cursor,
        assignment_generation: generation.assignment,
        delivery_tokens: Vec::new(),
        topic_id: bytes(generation.topic),
        delivery_generation: generation.assignment,
        session_id: bytes(generation.session),
    }
}

fn ack_fits(client: &Client, batch: &public::AckBatch) -> Result<bool> {
    let payload = tidemq_protocol::framing::encode_payload(
        batch,
        client.inner.config.protocol.max_control_frame_bytes,
    );
    let Ok(payload) = payload else {
        return Ok(false);
    };
    let frame = tidemq_protocol::common::Frame {
        protocol_version: tidemq_protocol::framing::PROTOCOL_VERSION,
        request_id: bytes(client.session_id()),
        operation_id: bytes(client.session_id()),
        traffic_class: TrafficClass::Control.into(),
        kind: FrameKind::AckBatch.into(),
        traceparent: crate::context::current_traceparent()?,
        payload,
        remaining_timeout_ms: tidemq_protocol::framing::MAX_TIMEOUT_MS,
    };
    Ok(frame.encoded_len() <= client.inner.config.protocol.max_control_frame_bytes as usize)
}

fn delivery_charge(delivery: &public::Delivery) -> Result<u32> {
    delivery
        .key
        .len()
        .checked_add(delivery.body.len())
        .and_then(|bytes| bytes.checked_add(delivery.delivery_token.len()))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<DeliveryData>() + 128))
        .and_then(|bytes| u32::try_from(bytes).ok())
        .ok_or(Error::Admission)
}

struct ReadCredit {
    bytes: OwnedSemaphorePermit,
    client: OwnedSemaphorePermit,
}

pub(crate) fn delivery_overhead() -> usize {
    std::mem::size_of::<DeliveryData>() + 229
}

fn read_credit(slots: usize, bytes: usize, maximum_message: usize) -> Result<(usize, usize)> {
    let overhead = delivery_overhead();
    if slots == 0 || bytes <= overhead {
        return Err(Error::Admission);
    }
    let one_payload = maximum_message.min(bytes - overhead);
    let count = slots.min((bytes - one_payload) / overhead).max(1);
    let payload = bytes
        .checked_sub(count.checked_mul(overhead).ok_or(Error::Admission)?)
        .ok_or(Error::Admission)?;
    Ok((count, payload))
}
