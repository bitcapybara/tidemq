use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use super::token::Claims;
use super::*;

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionState {
    topic_id: Id,
    partition_id: u32,
    limits: Limits,
    identities: BTreeMap<Id, Slot>,
    protection: BTreeMap<u64, u32>,
    total_slots: u32,
    total_bytes: u64,
    observed_committed_end: u64,
    observed_retained_floor: u64,
    owner_fence_generation: u64,
    revision: u64,
}

impl fmt::Debug for SubscriptionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubscriptionState")
            .field("topic_id", &self.topic_id)
            .field("partition_id", &self.partition_id)
            .field("revision", &self.revision)
            .field("owner_fence_generation", &self.owner_fence_generation)
            .field("identities", &self.identities.len())
            .field("window_slots", &self.total_slots)
            .field("in_flight_bytes", &self.total_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Slot {
    Active(Box<Subscription>),
    Retired { cursor_generation: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Subscription {
    creation_id: Id,
    creation_offset: u64,
    signing_key: SigningKey,
    generations: Generations,
    cursor_start: u64,
    acknowledged_before: u64,
    next_offset: u64,
    issued_before: u64,
    session_id: Option<Id>,
    cold_before: u64,
    received_before: u64,
    records: BTreeMap<u64, DeliveryRecord>,
    retries: BTreeMap<(u64, u64), ()>,
    in_flight: u32,
    exhausted: u32,
    bytes: u64,
    last_seek: Option<SeekRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct DeliveryRecord {
    operation_id: Id,
    generations: Generations,
    attempt: u32,
    body_bytes: u32,
    issued_at_ms: u64,
    retry_at_ms: u64,
    lane: DeliveryLane,
    acknowledged: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SeekRecord {
    operation_id: Id,
    expected_cursor_generation: u64,
    position: CursorPosition,
}

impl SubscriptionState {
    pub fn new(topic_id: Id, partition_id: u32, limits: Limits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            topic_id,
            partition_id,
            limits,
            identities: BTreeMap::new(),
            protection: BTreeMap::new(),
            total_slots: 0,
            total_bytes: 0,
            observed_committed_end: 0,
            observed_retained_floor: 0,
            owner_fence_generation: 0,
            revision: 1,
        })
    }

    pub fn topic_id(&self) -> Id {
        self.topic_id
    }

    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// The first offset that any active subscription still protects, without a table scan.
    pub fn protected_before(&self) -> Option<u64> {
        self.protection.first_key_value().map(|(offset, _)| *offset)
    }

    pub fn apply(&mut self, context: DataContext, command: Command) -> Response {
        let result = self.apply_inner(context, command);
        match result {
            Ok(outcome) => Response::Applied(outcome),
            Err(reason) => Response::Rejected(reason),
        }
    }

    // Keep exhaustive command dispatch together so every success advances the same revision.
    fn apply_inner(&mut self, context: DataContext, command: Command) -> Result<Outcome> {
        self.check_context(context)?;
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(Rejection::CounterOverflow)?;
        let outcome = match command {
            Command::Create {
                subscription_id,
                operation_id,
                start_offset,
                signing_key,
            } => self.create(
                context,
                subscription_id,
                operation_id,
                start_offset,
                signing_key,
            ),
            Command::Claim {
                subscription_id,
                expected,
                session_id,
                replace_session_id,
                cold_before,
            } => self.claim(
                context,
                subscription_id,
                expected,
                session_id,
                replace_session_id,
                cold_before,
            ),
            Command::Seek {
                subscription_id,
                operation_id,
                expected_cursor_generation,
                offset,
            } => self.seek(
                context,
                subscription_id,
                operation_id,
                expected_cursor_generation,
                offset,
            ),
            Command::Delete {
                subscription_id,
                expected_cursor_generation,
            } => self.delete(subscription_id, expected_cursor_generation),
            Command::RecordDelivery {
                subscription_id,
                generations,
                session_id,
                operation_id,
                offset,
                expected_attempt,
                body_bytes,
                now_ms,
                lane,
            } => self.record_delivery(
                context,
                subscription_id,
                generations,
                session_id,
                operation_id,
                offset,
                expected_attempt,
                body_bytes,
                now_ms,
                lane,
            ),
            Command::Ack {
                subscription_id,
                generations,
                tokens,
            } => self.acknowledge(subscription_id, generations, tokens),
            Command::ReceiveBarrier {
                subscription_id,
                generations,
                session_id,
                delivery_generation,
                received_before,
            } => self.receive_barrier(
                subscription_id,
                generations,
                session_id,
                delivery_generation,
                received_before,
            ),
        }?;
        self.observed_committed_end = context.committed_end;
        self.observed_retained_floor = context.retained_floor;
        self.revision = revision;
        Ok(outcome)
    }

    fn check_context(&self, context: DataContext) -> Result<()> {
        if context.retained_floor > context.committed_end
            || context.committed_end < self.observed_committed_end
            || context.retained_floor < self.observed_retained_floor
        {
            return Err(Rejection::InvalidContext);
        }
        if self
            .protected_before()
            .is_some_and(|floor| context.retained_floor > floor)
        {
            return Err(Rejection::RetentionConflict);
        }
        Ok(())
    }

    fn active(&self, id: Id) -> Result<&Subscription> {
        match self.identities.get(&id) {
            Some(Slot::Active(state)) => Ok(state),
            Some(Slot::Retired { .. }) => Err(Rejection::Deleted),
            None => Err(Rejection::NotFound),
        }
    }

    fn active_mut(&mut self, id: Id) -> Result<&mut Subscription> {
        match self.identities.get_mut(&id) {
            Some(Slot::Active(state)) => Ok(state),
            Some(Slot::Retired { .. }) => Err(Rejection::Deleted),
            None => Err(Rejection::NotFound),
        }
    }

    fn create(
        &mut self,
        context: DataContext,
        id: Id,
        operation: Id,
        offset: u64,
        signing_key: SigningKey,
    ) -> Result<Outcome> {
        if let Some(slot) = self.identities.get(&id) {
            return match slot {
                Slot::Active(state)
                    if state.creation_id == operation
                        && state.creation_offset == offset
                        && state.signing_key == signing_key =>
                {
                    Ok(Outcome::Created(CursorPosition {
                        generations: Generations {
                            cursor: 1,
                            assignment: 0,
                        },
                        acknowledged_before: offset,
                    }))
                }
                Slot::Active(_) => Err(Rejection::IdentityConflict),
                Slot::Retired { .. } => Err(Rejection::Deleted),
            };
        }
        check_offset(context, offset)?;
        if self.identities.len() >= self.limits.max_subscription_identities as usize {
            return Err(Rejection::IdentityBudget);
        }
        self.move_protection(None, Some(offset))?;
        let state = Subscription::new(operation, offset, signing_key);
        let position = state.position();
        self.identities.insert(id, Slot::Active(Box::new(state)));
        Ok(Outcome::Created(position))
    }

    fn claim(
        &mut self,
        context: DataContext,
        id: Id,
        expected: Generations,
        session: Id,
        replace: Option<Id>,
        cold_before: u64,
    ) -> Result<Outcome> {
        let state = self.active(id)?;
        if expected.cursor != state.generations.cursor {
            return Err(Rejection::StaleGeneration);
        }
        if state.session_id == Some(session) {
            if (expected.assignment != state.generations.assignment
                && expected.assignment.checked_add(1) != Some(state.generations.assignment))
                || cold_before != state.cold_before
            {
                return Err(Rejection::StaleGeneration);
            }
            return Ok(Outcome::Assigned(state.assignment()?));
        }
        if expected.assignment != state.generations.assignment {
            return Err(Rejection::StaleGeneration);
        }
        if state.session_id != replace {
            return Err(Rejection::AssignmentBusy);
        }
        if cold_before < state.acknowledged_before || cold_before > context.committed_end {
            return Err(Rejection::InvalidBarrier);
        }
        let next = state
            .generations
            .assignment
            .checked_add(1)
            .ok_or(Rejection::CounterOverflow)?;
        let max_attempts = self.limits.max_delivery_attempts;
        let state = self.active_mut(id)?;
        state.generations.assignment = next;
        state.session_id = Some(session);
        state.cold_before = cold_before;
        state.received_before = state.acknowledged_before;
        state.issued_before = state.acknowledged_before;
        state.retries.clear();
        for (offset, record) in &state.records {
            if !record.acknowledged && record.attempt < max_attempts {
                state.retries.insert((0, *offset), ());
            }
        }
        Ok(Outcome::Assigned(state.assignment()?))
    }

    fn seek(
        &mut self,
        context: DataContext,
        id: Id,
        operation: Id,
        expected: u64,
        offset: u64,
    ) -> Result<Outcome> {
        let state = self.active(id)?;
        if let Some(previous) = &state.last_seek
            && previous.operation_id == operation
        {
            return if previous.expected_cursor_generation == expected
                && previous.position.acknowledged_before == offset
            {
                Ok(Outcome::Seeked(previous.position))
            } else {
                Err(Rejection::IdentityConflict)
            };
        }
        if expected != state.generations.cursor {
            return Err(Rejection::StaleGeneration);
        }
        check_offset(context, offset)?;
        let generations = Generations {
            cursor: expected.checked_add(1).ok_or(Rejection::CounterOverflow)?,
            assignment: state
                .generations
                .assignment
                .checked_add(1)
                .ok_or(Rejection::CounterOverflow)?,
        };
        let slots = self
            .total_slots
            .checked_sub(state.records.len() as u32)
            .ok_or(Rejection::InvalidSnapshot)?;
        let bytes = self
            .total_bytes
            .checked_sub(state.bytes)
            .ok_or(Rejection::InvalidSnapshot)?;
        self.move_protection(Some(state.acknowledged_before), Some(offset))?;
        let state = self.active_mut(id)?;
        state.reset_cursor(offset, generations);
        let position = state.position();
        state.last_seek = Some(SeekRecord {
            operation_id: operation,
            expected_cursor_generation: expected,
            position,
        });
        self.total_slots = slots;
        self.total_bytes = bytes;
        Ok(Outcome::Seeked(position))
    }

    fn delete(&mut self, id: Id, expected: u64) -> Result<Outcome> {
        if let Some(Slot::Retired { cursor_generation }) = self.identities.get(&id) {
            return if *cursor_generation == expected {
                Ok(Outcome::Deleted)
            } else {
                Err(Rejection::StaleGeneration)
            };
        }
        let state = self.active(id)?;
        if state.generations.cursor != expected {
            return Err(Rejection::StaleGeneration);
        }
        let slots = self
            .total_slots
            .checked_sub(state.records.len() as u32)
            .ok_or(Rejection::InvalidSnapshot)?;
        let bytes = self
            .total_bytes
            .checked_sub(state.bytes)
            .ok_or(Rejection::InvalidSnapshot)?;
        self.move_protection(Some(state.acknowledged_before), None)?;
        self.identities.insert(
            id,
            Slot::Retired {
                cursor_generation: expected,
            },
        );
        self.total_slots = slots;
        self.total_bytes = bytes;
        Ok(Outcome::Deleted)
    }

    // The command mirrors the durable attempt fields; all validation precedes state mutation.
    #[allow(clippy::too_many_arguments)]
    fn record_delivery(
        &mut self,
        context: DataContext,
        id: Id,
        generations: Generations,
        session: Id,
        operation: Id,
        offset: u64,
        expected_attempt: u32,
        body_bytes: u32,
        now_ms: u64,
        lane: DeliveryLane,
    ) -> Result<Outcome> {
        let state = self.active(id)?;
        state.check_session(generations, session)?;
        if offset < state.cursor_start || offset >= context.committed_end {
            return Err(Rejection::NotDelivered);
        }
        if offset < state.acknowledged_before {
            return Ok(Outcome::DeliveryAlreadyAcknowledged { offset });
        }
        let previous = state.records.get(&offset);
        if let Some(record) = previous {
            if record.acknowledged {
                return Ok(Outcome::DeliveryAlreadyAcknowledged { offset });
            }
            if record.operation_id == operation {
                if record.generations != generations
                    || record.body_bytes != body_bytes
                    || record.attempt.checked_sub(1) != Some(expected_attempt)
                    || record.issued_at_ms != now_ms
                    || record.lane != lane
                {
                    return Err(Rejection::IdentityConflict);
                }
                return Ok(Outcome::Delivery(self.receipt(id, offset, record, true)?));
            }
            if record.attempt != expected_attempt {
                return Err(Rejection::StaleAttempt);
            }
            if record.body_bytes != body_bytes {
                return Err(Rejection::IdentityConflict);
            }
            if record.generations == generations && now_ms < record.retry_at_ms {
                return Err(Rejection::RetryNotDue);
            }
        } else {
            self.check_new_delivery(state, offset, expected_attempt, body_bytes)?;
        }
        state.check_lane(offset, lane)?;
        let attempt = expected_attempt
            .checked_add(1)
            .ok_or(Rejection::CounterOverflow)?;
        if attempt > self.limits.max_delivery_attempts {
            return Err(Rejection::AttemptLimit);
        }
        let retry_at_ms = now_ms
            .checked_add(self.limits.ack_timeout_ms)
            .ok_or(Rejection::CounterOverflow)?;
        let record = DeliveryRecord {
            operation_id: operation,
            generations,
            attempt,
            body_bytes,
            issued_at_ms: now_ms,
            retry_at_ms,
            lane,
            acknowledged: false,
        };
        let receipt = self.receipt(id, offset, &record, false)?;
        self.install_delivery(id, offset, record)?;
        Ok(Outcome::Delivery(receipt))
    }

    fn check_new_delivery(
        &self,
        state: &Subscription,
        offset: u64,
        expected_attempt: u32,
        body_bytes: u32,
    ) -> Result<()> {
        if offset != state.next_offset || expected_attempt != 0 {
            return Err(Rejection::OffsetGap);
        }
        offset.checked_add(1).ok_or(Rejection::CounterOverflow)?;
        if state.records.len() >= self.limits.max_window_slots as usize
            || self.total_slots >= self.limits.max_total_window_slots
        {
            return Err(Rejection::WindowFull);
        }
        if state.in_flight >= self.limits.max_in_flight_messages {
            return Err(Rejection::InFlightMessages);
        }
        if state
            .bytes
            .checked_add(u64::from(body_bytes))
            .is_none_or(|bytes| bytes > self.limits.max_in_flight_bytes)
            || self
                .total_bytes
                .checked_add(u64::from(body_bytes))
                .is_none_or(|bytes| bytes > self.limits.max_total_in_flight_bytes)
        {
            return Err(Rejection::InFlightBytes);
        }
        Ok(())
    }

    fn receipt(
        &self,
        id: Id,
        offset: u64,
        record: &DeliveryRecord,
        duplicate: bool,
    ) -> Result<DeliveryReceipt> {
        let state = self.active(id)?;
        Ok(DeliveryReceipt {
            offset,
            attempt: record.attempt,
            retry_at_ms: record.retry_at_ms,
            duplicate,
            token: state.signing_key.issue(Claims {
                topic_id: self.topic_id,
                partition_id: self.partition_id,
                subscription_id: id,
                generations: record.generations,
                offset,
                attempt: record.attempt,
            }),
        })
    }

    fn install_delivery(&mut self, id: Id, offset: u64, record: DeliveryRecord) -> Result<()> {
        let is_new = !self.active(id)?.records.contains_key(&offset);
        let bytes = record.body_bytes;
        let max_attempts = self.limits.max_delivery_attempts;
        let state = self.active_mut(id)?;
        if let Some(previous) = state.records.get(&offset) {
            let due = if previous.generations == state.generations {
                previous.retry_at_ms
            } else {
                0
            };
            state.retries.remove(&(due, offset));
        }
        if record.attempt < max_attempts {
            state.retries.insert((record.retry_at_ms, offset), ());
        } else {
            state.exhausted = state.exhausted.saturating_add(1);
        }
        state.records.insert(offset, record);
        if is_new {
            state.next_offset = offset.saturating_add(1);
            state.in_flight = state.in_flight.saturating_add(1);
            state.bytes = state.bytes.saturating_add(u64::from(bytes));
        }
        state.advance_issued_prefix();
        if is_new {
            self.total_slots = self.total_slots.saturating_add(1);
            self.total_bytes = self.total_bytes.saturating_add(u64::from(bytes));
        }
        Ok(())
    }

    // Validation and the bounded in-place commit stay together to make batch atomicity explicit.
    fn acknowledge(
        &mut self,
        id: Id,
        generations: Generations,
        tokens: Vec<DeliveryToken>,
    ) -> Result<Outcome> {
        let state = self.active(id)?;
        state.check_generations(generations)?;
        if tokens.is_empty() || tokens.len() > self.limits.max_ack_batch as usize {
            return Err(Rejection::AckBatchTooLarge);
        }
        let mut selected = BTreeSet::new();
        let mut already = 0_u32;
        let mut release = 0_u64;
        for token in tokens {
            let claims = state.signing_key.verify(&token)?;
            self.check_claims(id, generations, claims)?;
            if state.is_acknowledged(claims)? || !selected.insert(claims.offset) {
                already = already.checked_add(1).ok_or(Rejection::CounterOverflow)?;
            } else {
                let record = state
                    .records
                    .get(&claims.offset)
                    .ok_or(Rejection::NotDelivered)?;
                release = release
                    .checked_add(u64::from(record.body_bytes))
                    .ok_or(Rejection::CounterOverflow)?;
            }
        }
        let accepted = u32::try_from(selected.len()).map_err(|_| Rejection::CounterOverflow)?;
        let new_frontier = state.frontier_after(&selected);
        let old_frontier = state.acknowledged_before;
        let in_flight = state
            .in_flight
            .checked_sub(accepted)
            .ok_or(Rejection::InvalidSnapshot)?;
        let bytes = state
            .bytes
            .checked_sub(release)
            .ok_or(Rejection::InvalidSnapshot)?;
        let total_bytes = self
            .total_bytes
            .checked_sub(release)
            .ok_or(Rejection::InvalidSnapshot)?;
        let removed = new_frontier
            .checked_sub(old_frontier)
            .ok_or(Rejection::InvalidSnapshot)?;
        let total_slots = self
            .total_slots
            .checked_sub(u32::try_from(removed).map_err(|_| Rejection::InvalidSnapshot)?)
            .ok_or(Rejection::InvalidSnapshot)?;
        self.move_protection(Some(old_frontier), Some(new_frontier))?;
        let max_attempts = self.limits.max_delivery_attempts;
        let state = self.active_mut(id)?;
        state.mark_acknowledged(&selected, max_attempts);
        state.acknowledged_before = new_frontier;
        state.in_flight = in_flight;
        state.bytes = bytes;
        while state
            .records
            .first_key_value()
            .is_some_and(|(offset, _)| *offset < new_frontier)
        {
            state.records.pop_first();
        }
        self.total_slots = total_slots;
        self.total_bytes = total_bytes;
        Ok(Outcome::Acknowledged(AckReceipt {
            acknowledged_before: new_frontier,
            accepted,
            already_acknowledged: already,
        }))
    }

    fn check_claims(&self, id: Id, generations: Generations, claims: Claims) -> Result<()> {
        if claims.topic_id != self.topic_id
            || claims.partition_id != self.partition_id
            || claims.subscription_id != id
        {
            return Err(Rejection::InvalidToken);
        }
        if claims.generations != generations {
            return Err(Rejection::StaleGeneration);
        }
        Ok(())
    }

    fn receive_barrier(
        &mut self,
        id: Id,
        generations: Generations,
        session: Id,
        delivery_generation: u64,
        received_before: u64,
    ) -> Result<Outcome> {
        let state = self.active(id)?;
        state.check_session(generations, session)?;
        if delivery_generation != generations.delivery()
            || received_before < state.received_before
            || received_before > state.issued_before
        {
            return Err(Rejection::InvalidBarrier);
        }
        let state = self.active_mut(id)?;
        state.received_before = received_before;
        Ok(Outcome::ReceiveAdvanced(state.assignment()?))
    }

    fn move_protection(&mut self, previous: Option<u64>, next: Option<u64>) -> Result<()> {
        if previous == next {
            return Ok(());
        }
        let remaining = previous
            .map(|offset| {
                self.protection
                    .get(&offset)
                    .copied()
                    .and_then(|count| count.checked_sub(1))
                    .ok_or(Rejection::InvalidSnapshot)
            })
            .transpose()?;
        let following = next
            .map(|offset| {
                self.protection
                    .get(&offset)
                    .copied()
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or(Rejection::CounterOverflow)
            })
            .transpose()?;
        if let (Some(offset), Some(count)) = (previous, remaining) {
            if count == 0 {
                self.protection.remove(&offset);
            } else {
                self.protection.insert(offset, count);
            }
        }
        if let (Some(offset), Some(count)) = (next, following) {
            self.protection.insert(offset, count);
        }
        Ok(())
    }
}

impl SubscriptionState {
    pub fn owner_fence_generation(&self) -> u64 {
        self.owner_fence_generation
    }

    /// Call atomically with the owning data partition's durable fence transition.
    pub fn invalidate_assignments(&mut self, fence_generation: u64) -> Result<()> {
        if fence_generation < self.owner_fence_generation {
            return Err(Rejection::StaleGeneration);
        }
        if fence_generation == self.owner_fence_generation {
            return Ok(());
        }
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(Rejection::CounterOverflow)?;
        for slot in self.identities.values() {
            if let Slot::Active(state) = slot {
                state
                    .generations
                    .assignment
                    .checked_add(1)
                    .ok_or(Rejection::CounterOverflow)?;
            }
        }
        for slot in self.identities.values_mut() {
            if let Slot::Active(state) = slot {
                state.generations.assignment = state.generations.assignment.saturating_add(1);
                state.session_id = None;
                state.issued_before = state.acknowledged_before;
                state.received_before = state.acknowledged_before;
                state.cold_before = state.acknowledged_before;
                state.retries.clear();
            }
        }
        self.owner_fence_generation = fence_generation;
        self.revision = revision;
        Ok(())
    }

    /// Streaming serialization keeps the checkpoint digest from allocating another state image.
    pub fn recovery_digest(&self) -> Result<[u8; 32]> {
        let mut digest = blake3::Hasher::new();
        digest.update(b"tidemq.subscription-state.v1\0");
        postcard::to_io(self, &mut digest).map_err(|_| Rejection::InvalidSnapshot)?;
        Ok(*digest.finalize().as_bytes())
    }

    pub fn assignment(&self, id: Id) -> Result<Assignment> {
        self.active(id)?.assignment()
    }

    /// Returns bounded, due work from the retry index; exhausted messages never starve this queue.
    pub fn due_retries(&self, id: Id, now_ms: u64, limit: u32) -> Result<Vec<RetryStatus>> {
        let state = self.active(id)?;
        if state.session_id.is_none() {
            return Err(Rejection::SessionMismatch);
        }
        if limit == 0 || limit > self.limits.max_ack_batch {
            return Err(Rejection::InvalidLimits);
        }
        state
            .retries
            .range(..=(now_ms, u64::MAX))
            .take(limit as usize)
            .map(|((due, offset), _)| {
                let record = state
                    .records
                    .get(offset)
                    .ok_or(Rejection::InvalidSnapshot)?;
                Ok(RetryStatus {
                    offset: *offset,
                    attempt: record.attempt,
                    retry_at_ms: *due,
                })
            })
            .collect()
    }

    /// Skip evidence for a bounded read response, bound to its enclosing assignment generations.
    pub fn acknowledged_ranges(
        &self,
        id: Id,
        generations: Generations,
        start: u64,
        end: u64,
        limit: u32,
    ) -> Result<Vec<AcknowledgedRange>> {
        let state = self.active(id)?;
        state.check_generations(generations)?;
        if start < state.cursor_start
            || start > end
            || end > state.next_offset
            || limit == 0
            || limit > self.limits.max_ack_batch
        {
            return Err(Rejection::InvalidBarrier);
        }
        let mut ranges: Vec<AcknowledgedRange> = Vec::new();
        if start < state.acknowledged_before && start < end {
            ranges.push(AcknowledgedRange {
                start,
                end: end.min(state.acknowledged_before),
            });
        }
        for (offset, record) in state.records.range(start..end) {
            if !record.acknowledged {
                continue;
            }
            if let Some(previous) = ranges.last_mut()
                && previous.end == *offset
            {
                previous.end = offset.saturating_add(1);
                continue;
            }
            if ranges.len() >= limit as usize {
                break;
            }
            ranges.push(AcknowledgedRange {
                start: *offset,
                end: offset.saturating_add(1),
            });
        }
        Ok(ranges)
    }

    /// One subscription, fixed fields, and one retry item; no scan of other subscriptions.
    pub fn diagnostics(&self, id: Id) -> Result<Diagnostics> {
        let state = self.active(id)?;
        let window_slots = state.records.len() as u32;
        let phase = if state.session_id.is_none() {
            Phase::Unassigned
        } else if state.received_before >= state.cold_before {
            Phase::Realtime
        } else if state.issued_before >= state.cold_before {
            Phase::AwaitReceiveBarrier
        } else {
            Phase::ColdReplay
        };
        let blocked = self.block_reason(state);
        let next_retry = state
            .retries
            .first_key_value()
            .and_then(|((due, offset), _)| {
                state.records.get(offset).map(|record| RetryStatus {
                    offset: *offset,
                    attempt: record.attempt,
                    retry_at_ms: *due,
                })
            });
        Ok(Diagnostics {
            subscription_id: id,
            phase,
            generations: state.generations,
            acknowledged_before: state.acknowledged_before,
            next_delivery_offset: state.next_offset,
            issued_before: state.issued_before,
            received_before: state.received_before,
            cold_before: state.cold_before,
            window_slots,
            acknowledged_holes: window_slots.saturating_sub(state.in_flight),
            in_flight_messages: state.in_flight,
            in_flight_bytes: state.bytes,
            total_window_slots: self.total_slots,
            total_in_flight_bytes: self.total_bytes,
            next_retry,
            blocked,
        })
    }

    fn block_reason(&self, state: &Subscription) -> Option<BlockReason> {
        if state.exhausted > 0 {
            return Some(BlockReason::AttemptsExhausted);
        }
        if state.records.len() >= self.limits.max_window_slots as usize
            || self.total_slots >= self.limits.max_total_window_slots
        {
            return Some(BlockReason::Window);
        }
        if state.in_flight >= self.limits.max_in_flight_messages {
            return Some(BlockReason::InFlightMessages);
        }
        if state.bytes >= self.limits.max_in_flight_bytes
            || self.total_bytes >= self.limits.max_total_in_flight_bytes
        {
            return Some(BlockReason::InFlightBytes);
        }
        if state.received_before < state.cold_before && state.issued_before >= state.cold_before {
            return Some(BlockReason::AwaitReceiveBarrier);
        }
        None
    }

    /// Validates decoded snapshots before they can become authoritative replicated state.
    pub fn validate(&self) -> Result<()> {
        self.limits
            .validate()
            .map_err(|_| Rejection::InvalidSnapshot)?;
        if self.identities.len() > self.limits.max_subscription_identities as usize
            || self.total_slots > self.limits.max_total_window_slots
            || self.total_bytes > self.limits.max_total_in_flight_bytes
            || self.revision == 0
            || self.observed_retained_floor > self.observed_committed_end
        {
            return Err(Rejection::InvalidSnapshot);
        }
        let mut protection = BTreeMap::<u64, u32>::new();
        let mut total_slots = 0_u32;
        let mut total_bytes = 0_u64;
        for slot in self.identities.values() {
            match slot {
                Slot::Retired { cursor_generation } if *cursor_generation == 0 => {
                    return Err(Rejection::InvalidSnapshot);
                }
                Slot::Retired { .. } => {}
                Slot::Active(state) => {
                    state.validate(
                        self.limits,
                        self.observed_committed_end,
                        self.observed_retained_floor,
                    )?;
                    let count = protection.entry(state.acknowledged_before).or_default();
                    *count = count.checked_add(1).ok_or(Rejection::InvalidSnapshot)?;
                    total_slots = total_slots
                        .checked_add(state.records.len() as u32)
                        .ok_or(Rejection::InvalidSnapshot)?;
                    total_bytes = total_bytes
                        .checked_add(state.bytes)
                        .ok_or(Rejection::InvalidSnapshot)?;
                }
            }
        }
        if protection != self.protection
            || total_slots != self.total_slots
            || total_bytes != self.total_bytes
        {
            return Err(Rejection::InvalidSnapshot);
        }
        Ok(())
    }
}

impl Subscription {
    fn new(operation: Id, offset: u64, signing_key: SigningKey) -> Self {
        Self {
            creation_id: operation,
            creation_offset: offset,
            signing_key,
            generations: Generations {
                cursor: 1,
                assignment: 0,
            },
            cursor_start: offset,
            acknowledged_before: offset,
            next_offset: offset,
            issued_before: offset,
            session_id: None,
            cold_before: offset,
            received_before: offset,
            records: BTreeMap::new(),
            retries: BTreeMap::new(),
            in_flight: 0,
            exhausted: 0,
            bytes: 0,
            last_seek: None,
        }
    }

    fn position(&self) -> CursorPosition {
        CursorPosition {
            generations: self.generations,
            acknowledged_before: self.acknowledged_before,
        }
    }

    fn assignment(&self) -> Result<Assignment> {
        Ok(Assignment {
            session_id: self.session_id.ok_or(Rejection::SessionMismatch)?,
            generations: self.generations,
            acknowledged_before: self.acknowledged_before,
            next_delivery_offset: self.next_offset,
            cold_before: self.cold_before,
            issued_before: self.issued_before,
            received_before: self.received_before,
        })
    }

    fn check_generations(&self, generations: Generations) -> Result<()> {
        if self.generations != generations || self.session_id.is_none() {
            return Err(Rejection::StaleGeneration);
        }
        Ok(())
    }

    fn check_session(&self, generations: Generations, session: Id) -> Result<()> {
        self.check_generations(generations)?;
        if self.session_id != Some(session) {
            return Err(Rejection::SessionMismatch);
        }
        Ok(())
    }

    fn check_lane(&self, offset: u64, lane: DeliveryLane) -> Result<()> {
        let receiving_cold = self.received_before < self.cold_before;
        if receiving_cold && offset >= self.cold_before {
            return Err(Rejection::CutoverPending);
        }
        let expected = if offset < self.cold_before {
            DeliveryLane::Bulk
        } else {
            DeliveryLane::Realtime
        };
        if expected != lane {
            return Err(Rejection::WrongLane);
        }
        Ok(())
    }

    fn reset_cursor(&mut self, offset: u64, generations: Generations) {
        self.generations = generations;
        self.cursor_start = offset;
        self.acknowledged_before = offset;
        self.next_offset = offset;
        self.issued_before = offset;
        self.session_id = None;
        self.cold_before = offset;
        self.received_before = offset;
        self.records.clear();
        self.retries.clear();
        self.in_flight = 0;
        self.exhausted = 0;
        self.bytes = 0;
    }

    fn advance_issued_prefix(&mut self) {
        for (offset, record) in self.records.range(self.issued_before..) {
            if *offset != self.issued_before
                || (!record.acknowledged && record.generations != self.generations)
            {
                break;
            }
            self.issued_before = offset.saturating_add(1);
        }
    }

    fn is_acknowledged(&self, claims: Claims) -> Result<bool> {
        if claims.offset < self.cursor_start || claims.offset >= self.next_offset {
            return Err(Rejection::NotDelivered);
        }
        if claims.offset < self.acknowledged_before {
            return Ok(true);
        }
        let record = self
            .records
            .get(&claims.offset)
            .ok_or(Rejection::NotDelivered)?;
        if record.acknowledged {
            return Ok(true);
        }
        if record.generations != claims.generations || record.attempt != claims.attempt {
            return Err(Rejection::StaleAttempt);
        }
        Ok(false)
    }

    fn frontier_after(&self, selected: &BTreeSet<u64>) -> u64 {
        let mut frontier = self.acknowledged_before;
        for (offset, record) in &self.records {
            if *offset != frontier || (!record.acknowledged && !selected.contains(offset)) {
                break;
            }
            frontier = offset.saturating_add(1);
        }
        frontier
    }

    fn mark_acknowledged(&mut self, selected: &BTreeSet<u64>, max_attempts: u32) {
        for offset in selected {
            if let Some(record) = self.records.get_mut(offset) {
                let due = if record.generations == self.generations {
                    record.retry_at_ms
                } else {
                    0
                };
                self.retries.remove(&(due, *offset));
                record.acknowledged = true;
                if record.attempt >= max_attempts {
                    self.exhausted = self.exhausted.saturating_sub(1);
                }
            }
        }
    }

    fn validate(&self, limits: Limits, committed_end: u64, retained_floor: u64) -> Result<()> {
        if self.generations.cursor == 0
            || self.creation_offset > committed_end
            || (self.session_id.is_some() && self.generations.assignment == 0)
            || (self.last_seek.is_none()
                && (self.generations.cursor != 1 || self.cursor_start != self.creation_offset))
            || self.generations.assignment < self.generations.cursor.saturating_sub(1)
            || self.cursor_start > self.acknowledged_before
            || retained_floor > self.acknowledged_before
            || self.acknowledged_before > self.issued_before
            || self.issued_before > self.next_offset
            || self.next_offset > committed_end
            || self.received_before < self.cursor_start
            || self.received_before > self.issued_before
            || self.cold_before < self.cursor_start
            || self.cold_before > committed_end
            || self.records.len() > limits.max_window_slots as usize
            || self.next_offset.checked_sub(self.acknowledged_before)
                != Some(self.records.len() as u64)
            || self.in_flight > limits.max_in_flight_messages
            || self.bytes > limits.max_in_flight_bytes
            || self
                .records
                .first_key_value()
                .is_some_and(|(_, record)| record.acknowledged)
        {
            return Err(Rejection::InvalidSnapshot);
        }
        if let Some(previous) = &self.last_seek
            && (previous.expected_cursor_generation.checked_add(1) != Some(self.generations.cursor)
                || previous.position.generations.cursor != self.generations.cursor
                || previous.position.generations.assignment > self.generations.assignment
                || previous.position.acknowledged_before != self.cursor_start)
        {
            return Err(Rejection::InvalidSnapshot);
        }
        self.validate_records(limits)
    }

    fn validate_records(&self, limits: Limits) -> Result<()> {
        let mut retries = BTreeMap::new();
        let mut in_flight = 0_u32;
        let mut exhausted = 0_u32;
        let mut bytes = 0_u64;
        let mut next = self.acknowledged_before;
        for (offset, record) in &self.records {
            if *offset != next
                || record.attempt == 0
                || record.attempt > limits.max_delivery_attempts
                || record.generations.cursor != self.generations.cursor
                || record.generations.assignment == 0
                || record.generations.assignment > self.generations.assignment
                || record.issued_at_ms.checked_add(limits.ack_timeout_ms)
                    != Some(record.retry_at_ms)
                || (*offset < self.issued_before
                    && !record.acknowledged
                    && record.generations != self.generations)
            {
                return Err(Rejection::InvalidSnapshot);
            }
            next = next.checked_add(1).ok_or(Rejection::InvalidSnapshot)?;
            if record.acknowledged {
                continue;
            }
            in_flight = in_flight.checked_add(1).ok_or(Rejection::InvalidSnapshot)?;
            bytes = bytes
                .checked_add(u64::from(record.body_bytes))
                .ok_or(Rejection::InvalidSnapshot)?;
            if record.attempt >= limits.max_delivery_attempts {
                exhausted = exhausted.checked_add(1).ok_or(Rejection::InvalidSnapshot)?;
            } else if self.session_id.is_some() {
                let due = if record.generations == self.generations {
                    record.retry_at_ms
                } else {
                    0
                };
                retries.insert((due, *offset), ());
            }
        }
        if retries != self.retries
            || in_flight != self.in_flight
            || bytes != self.bytes
            || exhausted != self.exhausted
        {
            return Err(Rejection::InvalidSnapshot);
        }
        Ok(())
    }
}

fn check_offset(context: DataContext, offset: u64) -> Result<()> {
    if offset < context.retained_floor {
        return Err(Rejection::OutOfRetention);
    }
    if offset > context.committed_end {
        return Err(Rejection::FutureOffset);
    }
    Ok(())
}
