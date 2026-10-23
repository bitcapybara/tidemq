use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use super::{
    AppendBatch, BatchResult, MessageData, ProducerDiagnostic, ProducerLimits, ReadPage,
    RegisterProducer, Rejection, Result, SessionInfo,
};

#[path = "publish.rs"]
mod publish;
pub use publish::{PublishReceipt, PublishRequest, PublisherPermit};

#[cfg(test)]
#[path = "../../tests/scalable_support/publishing.rs"]
mod publishing_tests;

#[cfg(test)]
#[path = "../../tests/scalable_support/producer_memory.rs"]
mod memory_tests;

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SavedResult {
    first_sequence: u64,
    first_offset: u64,
    message_count: u32,
    digest: [u8; 32],
    expires_at_ms: u64,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
struct Session {
    info: SessionInfo,
    results: VecDeque<SavedResult>,
}

enum AppendPlan {
    Duplicate(BatchResult),
    New {
        message_count: u32,
        next_sequence: u64,
        next_offset: u64,
        hot_bytes: u64,
        expires_at_ms: u64,
    },
}

/// Complete authority for one partition, including the finite live exceptions to retirement.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProducerState {
    topic_id: Id,
    partition_id: u32,
    limits: ProducerLimits,
    committed_end: u64,
    retained_floor: u64,
    local_reclaim_end: u64,
    hot_bytes: u64,
    messages: VecDeque<MessageData>,
    next_registration_epoch: u64,
    logical_time_ms: u64,
    revision: u64,
    sessions: BTreeMap<u64, Session>,
    by_producer: BTreeMap<Id, u64>,
    expiry: BTreeMap<(u64, u64), ()>,
    publishers: publish::PublisherLedger,
}

impl fmt::Debug for ProducerState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerState")
            .field("diagnostic", &self.diagnostic())
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl ProducerState {
    pub fn new(topic_id: Id, partition_id: u32, limits: ProducerLimits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            topic_id,
            partition_id,
            limits,
            committed_end: 0,
            retained_floor: 0,
            local_reclaim_end: 0,
            hot_bytes: 0,
            messages: VecDeque::new(),
            next_registration_epoch: 1,
            logical_time_ms: 0,
            revision: 1,
            sessions: BTreeMap::new(),
            by_producer: BTreeMap::new(),
            expiry: BTreeMap::new(),
            publishers: publish::PublisherLedger::default(),
        })
    }

    pub fn topic_id(&self) -> Id {
        self.topic_id
    }

    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    pub fn committed_end(&self) -> u64 {
        self.committed_end
    }

    pub fn limits(&self) -> &ProducerLimits {
        &self.limits
    }

    pub fn hot_bytes(&self) -> u64 {
        self.hot_bytes
    }

    pub fn hot_message_count(&self) -> usize {
        self.messages.len()
    }

    pub fn message_bytes_at(&self, offset: u64) -> Result<u64> {
        let relative =
            offset
                .checked_sub(self.local_reclaim_end)
                .ok_or(Rejection::OffsetExpired {
                    floor: self.retained_floor,
                })?;
        let index = usize::try_from(relative).map_err(|_| Rejection::InvalidBatch)?;
        self.messages
            .get(index)
            .ok_or(Rejection::OffsetNotCommitted {
                end: self.committed_end,
            })?
            .logical_bytes()
            .ok_or(Rejection::InvalidBatch)
    }

    pub fn retained_floor(&self) -> u64 {
        self.retained_floor
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn next_registration_epoch(&self) -> u64 {
        self.next_registration_epoch
    }

    pub fn logical_time_ms(&self) -> u64 {
        self.logical_time_ms
    }

    /// `now_ms` is trusted runtime input carried by a committed command, never client time.
    /// Registering with a retired epoch cannot resurrect an old producer identity.
    pub fn register(&mut self, now_ms: u64, request: RegisterProducer) -> Result<SessionInfo> {
        self.advance_time(now_ms)?;
        let now_ms = self.logical_time_ms;
        let epoch = request.expected_registration_epoch;
        if let Some(session) = self.sessions.get(&epoch) {
            return if session.info.producer_id == request.producer_id {
                Ok(Self::session_info(session, now_ms))
            } else {
                Err(Rejection::RegistrationConflict {
                    expected: self.next_registration_epoch,
                })
            };
        }
        if epoch < self.next_registration_epoch {
            return Err(Rejection::SessionExpired);
        }
        if epoch != self.next_registration_epoch {
            return Err(Rejection::RegistrationConflict {
                expected: self.next_registration_epoch,
            });
        }
        if let Some(epoch) = self.by_producer.get(&request.producer_id) {
            return Err(Rejection::AlreadyOpen { epoch: *epoch });
        }
        if self.sessions.len() >= self.limits.max_sessions {
            return Err(Rejection::SessionCapacity);
        }
        let next = epoch.checked_add(1).ok_or(Rejection::CounterExhausted)?;
        let expires_at_ms = now_ms
            .checked_add(self.limits.session_ttl_ms)
            .ok_or(Rejection::CounterExhausted)?;
        let info = SessionInfo {
            producer_id: request.producer_id,
            producer_epoch: epoch,
            next_sequence: 0,
            expires_at_ms,
            result_floor_sequence: 0,
        };
        self.sessions.insert(
            epoch,
            Session {
                info: info.clone(),
                results: VecDeque::new(),
            },
        );
        self.by_producer.insert(request.producer_id, epoch);
        self.expiry.insert((expires_at_ms, epoch), ());
        self.next_registration_epoch = next;
        Ok(info)
    }

    pub fn session(&self, producer_id: Id, epoch: u64) -> Result<SessionInfo> {
        self.session_at(producer_id, epoch, self.logical_time_ms)
    }

    pub fn session_at(&self, producer_id: Id, epoch: u64, now_ms: u64) -> Result<SessionInfo> {
        let session = self.find_session(producer_id, epoch)?;
        let now_ms = now_ms.max(self.logical_time_ms);
        if session.info.expires_at_ms <= now_ms {
            return Err(Rejection::SessionExpired);
        }
        Ok(Self::session_info(session, now_ms))
    }

    /// Retirement preserves the allocated-epoch high water mark and drops the live exception.
    pub fn retire(&mut self, now_ms: u64, producer_id: Id, epoch: u64) -> Result<()> {
        self.advance_time(now_ms)?;
        match self.find_session(producer_id, epoch) {
            Err(Rejection::SessionExpired) => return Ok(()),
            Err(error) => return Err(error),
            Ok(_) => {}
        }
        if let Some(session) = self.sessions.remove(&epoch) {
            self.by_producer.remove(&session.info.producer_id);
            self.expiry.remove(&(session.info.expires_at_ms, epoch));
        }
        Ok(())
    }

    pub fn append(&mut self, now_ms: u64, request: AppendBatch) -> Result<BatchResult> {
        self.append_with_capacity(now_ms, request, u64::MAX, usize::MAX)
    }

    pub(crate) fn committed_result(
        &self,
        now_ms: u64,
        request: &AppendBatch,
    ) -> Result<Option<BatchResult>> {
        let (count, _) = self.validate_batch(request)?;
        let session = self.find_session(request.producer_id, request.producer_epoch)?;
        Self::previous_result(session, request, count, now_ms.max(self.logical_time_ms))
    }

    pub(crate) fn append_with_capacity(
        &mut self,
        now_ms: u64,
        request: AppendBatch,
        available_bytes: u64,
        available_messages: usize,
    ) -> Result<BatchResult> {
        self.append_admitted(
            now_ms,
            request,
            available_bytes,
            available_messages,
            |_, _| Ok(()),
        )
    }

    pub(crate) fn append_admitted(
        &mut self,
        now_ms: u64,
        request: AppendBatch,
        available_bytes: u64,
        available_messages: usize,
        before_commit: impl FnOnce(u64, u64) -> Result<()>,
    ) -> Result<BatchResult> {
        let now_ms = now_ms.max(self.logical_time_ms);
        self.revision
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let plan = match self.plan_append(now_ms, &request, available_bytes, available_messages) {
            Ok(plan) => plan,
            Err(error) => {
                self.advance_time(now_ms)?;
                return Err(error);
            }
        };
        let (message_count, next_sequence, next_offset, hot_bytes, expires_at_ms) = match plan {
            AppendPlan::Duplicate(result) => {
                self.advance_time(now_ms)?;
                return Ok(result);
            }
            AppendPlan::New {
                message_count,
                next_sequence,
                next_offset,
                hot_bytes,
                expires_at_ms,
            } => (
                message_count,
                next_sequence,
                next_offset,
                hot_bytes,
                expires_at_ms,
            ),
        };
        self.reserve_append(request.producer_epoch, request.messages.len())?;
        before_commit(self.committed_end, next_offset)?;
        self.advance_time(now_ms)?;
        let session = self
            .sessions
            .get_mut(&request.producer_epoch)
            .ok_or(Rejection::InvalidSnapshot)?;
        let result = BatchResult {
            first_offset: self.committed_end,
            message_count,
            duplicate: false,
        };
        session.results.push_back(SavedResult {
            first_sequence: request.first_sequence,
            first_offset: result.first_offset,
            message_count,
            digest: request.content_digest,
            expires_at_ms,
        });
        session.info.next_sequence = next_sequence;
        Self::prune_results(session, now_ms, self.limits.max_results_per_session);
        self.messages.extend(request.messages);
        self.committed_end = next_offset;
        self.hot_bytes = hot_bytes;
        Ok(result)
    }

    // Allocation failure cannot change authoritative state or become a committed business refusal.
    pub(super) fn reserve_append(&mut self, epoch: u64, messages: usize) -> Result<()> {
        self.messages
            .try_reserve(messages)
            .map_err(|_| Rejection::Allocation)?;
        self.sessions
            .get_mut(&epoch)
            .ok_or(Rejection::InvalidSnapshot)?
            .results
            .try_reserve(1)
            .map_err(|_| Rejection::Allocation)?;
        Ok(())
    }

    fn plan_append(
        &self,
        now_ms: u64,
        request: &AppendBatch,
        available_bytes: u64,
        available_messages: usize,
    ) -> Result<AppendPlan> {
        let (message_count, batch_bytes) = self.validate_batch(request)?;
        let session = self.find_session(request.producer_id, request.producer_epoch)?;
        if session.info.expires_at_ms <= now_ms {
            return Err(Rejection::SessionExpired);
        }
        if let Some(result) = Self::previous_result(session, request, message_count, now_ms)? {
            return Ok(AppendPlan::Duplicate(result));
        }
        let next_sequence = request
            .first_sequence
            .checked_add(u64::from(message_count))
            .ok_or(Rejection::CounterExhausted)?;
        let next_offset = self
            .committed_end
            .checked_add(u64::from(message_count))
            .ok_or(Rejection::CounterExhausted)?;
        let hot_bytes = self
            .hot_bytes
            .checked_add(batch_bytes)
            .ok_or(Rejection::CounterExhausted)?;
        let hot_messages = self
            .messages
            .len()
            .checked_add(request.messages.len())
            .ok_or(Rejection::CounterExhausted)?;
        if hot_bytes > self.limits.max_hot_bytes
            || hot_messages > self.limits.max_hot_messages
            || batch_bytes > available_bytes
            || request.messages.len() > available_messages
        {
            return Err(Rejection::DataCapacity);
        }
        let expires_at_ms = now_ms
            .checked_add(self.limits.result_ttl_ms)
            .ok_or(Rejection::CounterExhausted)?;
        Ok(AppendPlan::New {
            message_count,
            next_sequence,
            next_offset,
            hot_bytes,
            expires_at_ms,
        })
    }

    /// Expires live sessions using a finite ordered index rather than scanning every producer.
    pub fn advance_time(&mut self, now_ms: u64) -> Result<()> {
        let now_ms = now_ms.max(self.logical_time_ms);
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        self.logical_time_ms = now_ms;
        while self
            .expiry
            .first_key_value()
            .is_some_and(|((deadline, _), _)| *deadline <= now_ms)
        {
            if let Some(((_, epoch), _)) = self.expiry.pop_first()
                && let Some(session) = self.sessions.remove(&epoch)
            {
                self.by_producer.remove(&session.info.producer_id);
            }
        }
        Ok(())
    }

    /// Reads only a requested bounded subset of committed data; this method is not a Raft barrier.
    pub fn hot_read(&self, first: u64, max_messages: usize, max_bytes: u64) -> Result<ReadPage> {
        if first < self.retained_floor {
            return Err(Rejection::OffsetExpired {
                floor: self.retained_floor,
            });
        }
        self.archive_read(first, max_messages, max_bytes)
    }

    pub(crate) fn archive_read(
        &self,
        first: u64,
        max_messages: usize,
        max_bytes: u64,
    ) -> Result<ReadPage> {
        if max_messages == 0
            || max_messages > self.limits.max_batch_messages
            || max_bytes == 0
            || max_bytes > self.limits.max_batch_bytes
        {
            return Err(Rejection::ReadLimit);
        }
        if first > self.committed_end {
            return Err(Rejection::OffsetNotCommitted {
                end: self.committed_end,
            });
        }
        if first < self.local_reclaim_end {
            return Err(Rejection::OffsetExpired {
                floor: self.local_reclaim_end,
            });
        }
        let start = usize::try_from(first.saturating_sub(self.local_reclaim_end))
            .map_err(|_| Rejection::ReadLimit)?;
        let mut messages = Vec::new();
        messages
            .try_reserve(max_messages.min(self.messages.len().saturating_sub(start)))
            .map_err(|_| Rejection::Allocation)?;
        let mut bytes = 0_u64;
        while messages.len() < max_messages {
            let position = start
                .checked_add(messages.len())
                .ok_or(Rejection::ReadLimit)?;
            let Some(message) = self.messages.get(position) else {
                break;
            };
            let next = bytes
                .checked_add(message.logical_bytes().ok_or(Rejection::CounterExhausted)?)
                .ok_or(Rejection::CounterExhausted)?;
            if next > max_bytes {
                if messages.is_empty() {
                    return Err(Rejection::ReadLimit);
                }
                break;
            }
            messages.push(message.clone());
            bytes = next;
        }
        let next_offset = first
            .checked_add(u64::try_from(messages.len()).map_err(|_| Rejection::CounterExhausted)?)
            .ok_or(Rejection::CounterExhausted)?;
        Ok(ReadPage {
            first_offset: first,
            messages,
            next_offset,
            committed_end: self.committed_end,
        })
    }

    pub fn diagnostic(&self) -> ProducerDiagnostic {
        ProducerDiagnostic {
            topic_id: self.topic_id,
            partition_id: self.partition_id,
            committed_end: self.committed_end,
            retained_floor: self.retained_floor,
            local_reclaim_end: self.local_reclaim_end,
            hot_messages: self.messages.len(),
            hot_message_capacity: self.messages.capacity(),
            hot_message_capacity_bytes: self
                .messages
                .capacity()
                .saturating_mul(std::mem::size_of::<MessageData>()),
            hot_bytes: self.hot_bytes,
            active_sessions: self.sessions.len(),
            retained_results: self
                .sessions
                .values()
                .map(|session| session.results.len())
                .sum(),
            result_capacity: self
                .sessions
                .values()
                .map(|session| session.results.capacity())
                .sum(),
            next_registration_epoch: self.next_registration_epoch,
            logical_time_ms: self.logical_time_ms,
            revision: self.revision,
        }
    }

    /// Full validation is for decoded snapshots and recovery, never per-message admission.
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        self.publishers.validate(&self.limits, self.committed_end)?;
        let message_count = self.committed_end.checked_sub(self.local_reclaim_end);
        if self.revision == 0
            || self.retained_floor > self.committed_end
            || self.local_reclaim_end > self.committed_end
            || self.next_registration_epoch == 0
            || message_count != Some(self.messages.len() as u64)
            || self.messages.len() > self.limits.max_hot_messages
            || self.hot_bytes > self.limits.max_hot_bytes
            || self.sessions.len() > self.limits.max_sessions
            || self.by_producer.len() != self.sessions.len()
            || self.expiry.len() != self.sessions.len()
        {
            return Err(Rejection::InvalidSnapshot);
        }
        let mut bytes = 0_u64;
        for message in &self.messages {
            message
                .metadata(self.topic_id)
                .map_err(|_| Rejection::InvalidSnapshot)?;
            let length = message.logical_bytes().ok_or(Rejection::InvalidSnapshot)?;
            if length > self.limits.max_message_bytes {
                return Err(Rejection::InvalidSnapshot);
            }
            bytes = bytes
                .checked_add(length)
                .ok_or(Rejection::InvalidSnapshot)?;
        }
        if bytes != self.hot_bytes {
            return Err(Rejection::InvalidSnapshot);
        }
        for (epoch, session) in &self.sessions {
            self.validate_session(*epoch, session)?;
        }
        Ok(())
    }

    /// Streaming serialization avoids allocating a second full state image during recovery.
    pub fn recovery_digest(&self) -> Result<[u8; 32]> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"tidemq-producer-state-v1\0");
        postcard::to_io(self, &mut hash).map_err(|_| Rejection::InvalidSnapshot)?;
        Ok(*hash.finalize().as_bytes())
    }

    fn validate_session(&self, epoch: u64, session: &Session) -> Result<()> {
        let info = &session.info;
        if epoch == 0
            || epoch >= self.next_registration_epoch
            || epoch != info.producer_epoch
            || info.expires_at_ms <= self.logical_time_ms
            || info.expires_at_ms.saturating_sub(self.logical_time_ms) > self.limits.session_ttl_ms
            || info.result_floor_sequence > info.next_sequence
            || session.results.len() > self.limits.max_results_per_session
            || self.by_producer.get(&info.producer_id) != Some(&epoch)
            || !self.expiry.contains_key(&(info.expires_at_ms, epoch))
        {
            return Err(Rejection::InvalidSnapshot);
        }
        let mut next_sequence = info.result_floor_sequence;
        let mut previous_end = 0;
        let mut previous_expiry = 0;
        for result in &session.results {
            let end = result
                .first_offset
                .checked_add(u64::from(result.message_count));
            if result.message_count == 0
                || result.message_count as usize > self.limits.max_batch_messages
                || result.first_sequence != next_sequence
                || result.first_offset < previous_end
                || end.is_none_or(|end| end > self.committed_end)
                || result.expires_at_ms < previous_expiry
                || result.expires_at_ms.saturating_sub(self.logical_time_ms)
                    > self.limits.result_ttl_ms
            {
                return Err(Rejection::InvalidSnapshot);
            }
            next_sequence = next_sequence
                .checked_add(u64::from(result.message_count))
                .ok_or(Rejection::InvalidSnapshot)?;
            previous_end = end.ok_or(Rejection::InvalidSnapshot)?;
            previous_expiry = result.expires_at_ms;
        }
        if next_sequence != info.next_sequence {
            return Err(Rejection::InvalidSnapshot);
        }
        Ok(())
    }

    fn find_session(&self, producer_id: Id, epoch: u64) -> Result<&Session> {
        match self.sessions.get(&epoch) {
            Some(session) if session.info.producer_id == producer_id => Ok(session),
            Some(_) => Err(Rejection::SessionConflict),
            None if epoch < self.next_registration_epoch => Err(Rejection::SessionExpired),
            None => Err(Rejection::UnknownSession),
        }
    }

    fn validate_batch(&self, request: &AppendBatch) -> Result<(u32, u64)> {
        if request.topic_id != self.topic_id || request.partition_id != self.partition_id {
            return Err(Rejection::WrongPartition);
        }
        if request.messages.is_empty() || request.messages.len() > self.limits.max_batch_messages {
            return Err(Rejection::InvalidBatch);
        }
        let mut bytes = 0_u64;
        for message in &request.messages {
            let length = message.logical_bytes().ok_or(Rejection::InvalidBatch)?;
            if length > self.limits.max_message_bytes {
                return Err(Rejection::InvalidBatch);
            }
            bytes = bytes.checked_add(length).ok_or(Rejection::InvalidBatch)?;
        }
        if bytes > self.limits.max_batch_bytes {
            return Err(Rejection::InvalidBatch);
        }
        request.verify_digest()?;
        let count = u32::try_from(request.messages.len()).map_err(|_| Rejection::InvalidBatch)?;
        Ok((count, bytes))
    }

    fn previous_result(
        session: &Session,
        request: &AppendBatch,
        message_count: u32,
        now_ms: u64,
    ) -> Result<Option<BatchResult>> {
        if request.first_sequence > session.info.next_sequence {
            return Err(Rejection::SequenceGap {
                expected: session.info.next_sequence,
            });
        }
        if request.first_sequence == session.info.next_sequence {
            return Ok(None);
        }
        let floor = Self::result_floor(session, now_ms);
        if request.first_sequence < floor {
            return Err(Rejection::ResultExpired { before: floor });
        }
        let original = session
            .results
            .iter()
            .find(|result| result.first_sequence == request.first_sequence);
        match original {
            Some(result)
                if result.digest == request.content_digest
                    && result.message_count == message_count =>
            {
                Ok(Some(BatchResult {
                    first_offset: result.first_offset,
                    message_count: result.message_count,
                    duplicate: true,
                }))
            }
            Some(_) => Err(Rejection::SequenceConflict),
            None if request.first_sequence < session.info.result_floor_sequence => {
                Err(Rejection::ResultExpired {
                    before: session.info.result_floor_sequence,
                })
            }
            None => Err(Rejection::SequenceConflict),
        }
    }

    fn result_floor(session: &Session, now_ms: u64) -> u64 {
        session
            .results
            .iter()
            .find(|result| result.expires_at_ms > now_ms)
            .map_or(session.info.next_sequence, |result| result.first_sequence)
    }

    fn session_info(session: &Session, now_ms: u64) -> SessionInfo {
        let mut info = session.info.clone();
        info.result_floor_sequence = Self::result_floor(session, now_ms);
        info
    }

    fn prune_results(session: &mut Session, now_ms: u64, maximum: usize) {
        while session.results.len() > maximum
            || session
                .results
                .front()
                .is_some_and(|result| result.expires_at_ms <= now_ms)
        {
            if let Some(result) = session.results.pop_front() {
                session.info.result_floor_sequence = result
                    .first_sequence
                    .saturating_add(u64::from(result.message_count));
            }
        }
    }
}

impl ProducerState {
    pub fn local_reclaim_end(&self) -> u64 {
        self.local_reclaim_end
    }

    pub fn set_retained_floor(&mut self, floor: u64) -> Result<()> {
        if floor < self.retained_floor || floor > self.committed_end {
            return Err(Rejection::InvalidSnapshot);
        }
        self.retained_floor = floor;
        Ok(())
    }

    pub(crate) fn reclaim_hot(&mut self, end: u64) -> Result<()> {
        if end < self.local_reclaim_end || end > self.committed_end {
            return Err(Rejection::InvalidSnapshot);
        }
        let count = usize::try_from(end - self.local_reclaim_end)
            .map_err(|_| Rejection::InvalidSnapshot)?;
        if count > self.messages.len() {
            return Err(Rejection::InvalidSnapshot);
        }
        let reclaimed = self
            .messages
            .iter()
            .take(count)
            .try_fold(0_u64, |total, message| {
                total
                    .checked_add(message.logical_bytes().ok_or(Rejection::InvalidSnapshot)?)
                    .ok_or(Rejection::InvalidSnapshot)
            })?;
        let bytes = self
            .hot_bytes
            .checked_sub(reclaimed)
            .ok_or(Rejection::InvalidSnapshot)?;
        for _ in 0..count {
            self.messages.pop_front();
        }
        // Reclamation is a lifecycle boundary; retain small rings without keeping peak capacity.
        if self.messages.is_empty()
            && self
                .messages
                .capacity()
                .saturating_mul(size_of::<MessageData>())
                > 64 * 1024
        {
            self.messages = VecDeque::new();
        }
        self.local_reclaim_end = end;
        self.hot_bytes = bytes;
        Ok(())
    }
}
