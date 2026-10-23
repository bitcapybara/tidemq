use super::*;
use tidemq_protocol::topic::{SendIdentity, message_digest, routing_hash};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublisherPermit {
    pub topic: Id,
    pub producer: Id,
    pub registration: u64,
    pub client: Id,
    pub credential: Id,
    pub expires_ms: u64,
    pub live: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishRequest {
    pub identity: SendIdentity,
    pub digest: [u8; 32],
    pub message: MessageData,
    pub retired_before: u64,
}

impl PublishRequest {
    pub fn verify(&self) -> Result<u64> {
        let metadata = self.message.metadata(self.identity.topic)?;
        let key = metadata.key_present.then_some(self.message.key.as_ref());
        if self.identity.registration == 0
            || self.identity.sequence == 0
            || self.retired_before > self.identity.sequence
            || !metadata.key_present && !self.message.key.is_empty()
            || metadata.routing_hash
                != routing_hash(key, self.identity.producer, self.identity.sequence)
                    .map_err(|_| Rejection::InvalidBatch)?
        {
            return Err(Rejection::InvalidBatch);
        }
        if message_digest(self.identity, key, &self.message.body, &metadata.properties)
            .map_err(|_| Rejection::InvalidBatch)?
            != self.digest
        {
            return Err(Rejection::DigestMismatch);
        }
        Ok(metadata.routing_hash)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishReceipt {
    pub offset: u64,
    pub duplicate: bool,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SavedPublish {
    digest: [u8; 32],
    offset: u64,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
struct Publisher {
    client: Id,
    credential: Id,
    floor: u64,
    results: BTreeMap<u64, SavedPublish>,
    retired: bool,
    reap_after_ms: u64,
}

#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct PublisherLedger {
    sessions: BTreeMap<(Id, u64), Publisher>,
}

impl PublisherLedger {
    pub(super) fn validate(&self, limits: &ProducerLimits, end: u64) -> Result<()> {
        if self.sessions.len() > limits.max_sessions {
            return Err(Rejection::InvalidSnapshot);
        }
        for ((_, registration), session) in &self.sessions {
            if *registration == 0
                || session.floor == 0
                || session.results.len() > limits.max_results_per_session
                || session
                    .results
                    .iter()
                    .any(|(sequence, result)| *sequence < session.floor || result.offset >= end)
            {
                return Err(Rejection::InvalidSnapshot);
            }
        }
        Ok(())
    }
}

impl ProducerState {
    pub(crate) fn publisher_cleanup_candidates(
        &self,
        now_ms: u64,
    ) -> impl Iterator<Item = PublisherPermit> + '_ {
        self.publishers
            .sessions
            .iter()
            .filter(move |(_, session)| session.reap_after_ms <= now_ms)
            .map(|((producer, registration), session)| PublisherPermit {
                topic: self.topic_id,
                producer: *producer,
                registration: *registration,
                client: session.client,
                credential: session.credential,
                live: false,
                expires_ms: session
                    .reap_after_ms
                    .saturating_sub(crate::metadata::PUBLISHER_RECOVERY_MS),
            })
    }

    pub(crate) fn cleanup_publisher(
        &mut self,
        permit: &PublisherPermit,
        now_ms: u64,
    ) -> Result<()> {
        self.publisher_binding(
            permit,
            SendIdentity {
                topic: permit.topic,
                producer: permit.producer,
                registration: permit.registration,
                sequence: 1,
            },
        )?;
        if permit.live {
            return Err(Rejection::SessionConflict);
        }
        self.advance_time(now_ms.max(self.logical_time_ms))?;
        let key = (permit.producer, permit.registration);
        if let Some(session) = self.publishers.sessions.get_mut(&key) {
            session.retired = true;
            session.reap_after_ms = session.reap_after_ms.max(
                permit
                    .expires_ms
                    .saturating_add(crate::metadata::PUBLISHER_RECOVERY_MS),
            );
            if session.reap_after_ms <= self.logical_time_ms {
                self.publishers.sessions.remove(&key);
            }
        }
        Ok(())
    }
    pub(crate) fn publisher_binding(
        &self,
        permit: &PublisherPermit,
        identity: SendIdentity,
    ) -> Result<()> {
        if permit.topic != self.topic_id
            || identity.topic != self.topic_id
            || permit.producer != identity.producer
            || permit.registration != identity.registration
            || self
                .publishers
                .sessions
                .get(&(permit.producer, permit.registration))
                .is_some_and(|session| {
                    session.client != permit.client || session.credential != permit.credential
                })
        {
            return Err(Rejection::SessionConflict);
        }
        Ok(())
    }

    pub fn publisher_closed(&self, identity: SendIdentity) -> bool {
        self.publishers
            .sessions
            .get(&(identity.producer, identity.registration))
            .is_some_and(|session| session.retired)
    }
    pub fn publish_state_bytes(&self) -> u64 {
        self.publishers
            .sessions
            .values()
            .fold(0_u64, |bytes, session| {
                bytes
                    .saturating_add(512)
                    .saturating_add((session.results.len() as u64).saturating_mul(128))
            })
    }
    pub fn publish_result(
        &self,
        identity: SendIdentity,
        digest: [u8; 32],
    ) -> Result<Option<PublishReceipt>> {
        let Some(session) = self
            .publishers
            .sessions
            .get(&(identity.producer, identity.registration))
        else {
            return Ok(None);
        };
        if identity.sequence < session.floor {
            return Err(Rejection::ResultExpired {
                before: session.floor,
            });
        }
        match session.results.get(&identity.sequence) {
            Some(result) if result.digest == digest => Ok(Some(PublishReceipt {
                offset: result.offset,
                duplicate: true,
            })),
            Some(_) => Err(Rejection::SequenceConflict),
            None => Ok(None),
        }
    }

    pub fn retire_publishes(&mut self, permit: &PublisherPermit, before: u64) -> Result<()> {
        if permit.topic != self.topic_id || before == 0 {
            return Err(Rejection::SessionConflict);
        }
        let key = (permit.producer, permit.registration);
        if !self.publishers.sessions.contains_key(&key) {
            if permit.live {
                return Ok(());
            }
            if self.publishers.sessions.len() >= self.limits.max_sessions {
                return Err(Rejection::SessionCapacity);
            }
            self.publishers.sessions.insert(
                key,
                Publisher {
                    client: permit.client,
                    credential: permit.credential,
                    floor: before,
                    results: BTreeMap::new(),
                    retired: true,
                    reap_after_ms: permit.expires_ms.saturating_add(620_000),
                },
            );
        }
        let session = self
            .publishers
            .sessions
            .get_mut(&key)
            .ok_or(Rejection::InvalidSnapshot)?;
        if session.client != permit.client
            || session.credential != permit.credential
            || permit.topic != self.topic_id
        {
            return Err(Rejection::SessionConflict);
        }
        session.floor = session.floor.max(before);
        session
            .results
            .retain(|sequence, _| *sequence >= session.floor);
        if !permit.live {
            session.retired = true;
            session.reap_after_ms = session
                .reap_after_ms
                .max(permit.expires_ms.saturating_add(620_000));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // Admission carries the group budget and retention reservation together.
    pub(crate) fn publish_admitted(
        &mut self,
        permit: &PublisherPermit,
        mut request: PublishRequest,
        now_ms: u64,
        available_bytes: u64,
        available_messages: usize,
        before_commit: impl FnOnce(u64, u64, u64) -> Result<()>,
    ) -> Result<PublishReceipt> {
        let now_ms = now_ms.max(self.logical_time_ms);
        self.advance_time(now_ms)?;
        self.publishers
            .sessions
            .retain(|_, session| !session.retired || session.reap_after_ms > now_ms);
        request.verify()?;
        self.publisher_binding(permit, request.identity)?;
        if permit.topic != self.topic_id
            || request.identity.topic != self.topic_id
            || permit.producer != request.identity.producer
            || permit.registration != request.identity.registration
        {
            return Err(Rejection::SessionConflict);
        }
        let key = (permit.producer, permit.registration);
        let session = self.publishers.sessions.get(&key);
        if session.is_some_and(|session| {
            session.client != permit.client || session.credential != permit.credential
        }) {
            return Err(Rejection::SessionConflict);
        }
        if let Some(result) = self.publish_result(request.identity, request.digest)? {
            return Ok(result);
        }
        if !permit.live || permit.expires_ms <= now_ms {
            return Err(Rejection::SessionExpired);
        }
        if let Some(session) = session {
            if session.retired
                || session.client != permit.client
                || session.credential != permit.credential
            {
                return Err(Rejection::SessionConflict);
            }
            if session.results.range(request.retired_before..).count()
                >= self.limits.max_results_per_session
            {
                return Err(Rejection::SessionCapacity);
            }
        } else if self.publishers.sessions.len() >= self.limits.max_sessions {
            return Err(Rejection::SessionCapacity);
        }
        let bytes = request
            .message
            .logical_bytes()
            .ok_or(Rejection::InvalidBatch)?;
        let hot_bytes = self
            .hot_bytes
            .checked_add(bytes)
            .ok_or(Rejection::CounterExhausted)?;
        if bytes > self.limits.max_message_bytes {
            return Err(Rejection::MessageTooLarge);
        }
        if bytes > available_bytes
            || available_messages == 0
            || hot_bytes > self.limits.max_hot_bytes
            || self.messages.len() >= self.limits.max_hot_messages
        {
            return Err(Rejection::DataCapacity);
        }
        let end = self
            .committed_end
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        request.message.key = super::super::types::compact_bytes(&request.message.key)?;
        request.message.body = super::super::types::compact_bytes(&request.message.body)?;
        request.message.context = super::super::types::compact_bytes(&request.message.context)?;
        self.messages
            .try_reserve(1)
            .map_err(|_| Rejection::Allocation)?;
        before_commit(self.committed_end, end, bytes)?;
        let session = self
            .publishers
            .sessions
            .entry(key)
            .or_insert_with(|| Publisher {
                client: permit.client,
                credential: permit.credential,
                floor: 1,
                results: BTreeMap::new(),
                retired: false,
                reap_after_ms: permit.expires_ms.saturating_add(620_000),
            });
        session.floor = session.floor.max(request.retired_before);
        session
            .results
            .retain(|sequence, _| *sequence >= session.floor);
        let offset = self.committed_end;
        session.results.insert(
            request.identity.sequence,
            SavedPublish {
                offset,
                digest: request.digest,
            },
        );
        self.messages.push_back(request.message);
        self.committed_end = end;
        self.hot_bytes = hot_bytes;
        self.revision = revision;
        self.logical_time_ms = self.logical_time_ms.max(now_ms);
        Ok(PublishReceipt {
            offset,
            duplicate: false,
        })
    }
}
