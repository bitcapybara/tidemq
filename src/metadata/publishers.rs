use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use super::{MetadataState, Rejection};
use crate::producer::PublisherPermit;

pub const PUBLISHER_LEASE_MS: u64 = 300_000;
pub const PUBLISHER_RECOVERY_MS: u64 = 620_000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublisherCommand {
    Open {
        topic: Id,
        producer: Id,
        registration: u64,
        client: Id,
        credential: Id,
        now_ms: u64,
        credential_expires_ms: u64,
        digest: [u8; 32],
    },
    Heartbeat {
        producer: Id,
        registration: u64,
        client: Id,
        credential: Id,
        sequence: u64,
        now_ms: u64,
        credential_expires_ms: u64,
        digest: [u8; 32],
    },
    Close {
        producer: Id,
        registration: u64,
        client: Id,
        credential: Id,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublisherHeartbeat {
    sequence: u64,
    operation: Id,
    digest: [u8; 32],
    expires_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublisherRecord {
    pub permit: PublisherPermit,
    open_operation: Id,
    open_digest: [u8; 32],
    heartbeat: Option<PublisherHeartbeat>,
}

impl PublisherRecord {
    pub fn permit_at(&self, now_ms: u64) -> PublisherPermit {
        let mut permit = self.permit.clone();
        permit.live &= permit.expires_ms > now_ms;
        permit
    }
    pub fn next_heartbeat_sequence(&self) -> Option<u64> {
        self.heartbeat
            .as_ref()
            .map_or(Some(1), |heartbeat| heartbeat.sequence.checked_add(1))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishersBook {
    next_registration: u64,
    records: BTreeMap<u64, PublisherRecord>,
}

impl Default for PublishersBook {
    fn default() -> Self {
        Self {
            next_registration: 1,
            records: BTreeMap::new(),
        }
    }
}

impl PublishersBook {
    pub fn records(&self) -> impl Iterator<Item = &PublisherRecord> {
        self.records.values()
    }
    pub fn next_registration(&self) -> u64 {
        self.next_registration
    }
    pub fn record(&self, producer: Id, registration: u64) -> Option<&PublisherRecord> {
        self.records
            .get(&registration)
            .filter(|record| record.permit.producer == producer)
    }
    pub fn apply(
        &mut self,
        operation: Id,
        command: &PublisherCommand,
    ) -> Result<PublisherPermit, Rejection> {
        if let PublisherCommand::Open { now_ms, .. } | PublisherCommand::Heartbeat { now_ms, .. } =
            command
        {
            self.records.retain(|_, record| {
                record
                    .permit
                    .expires_ms
                    .saturating_add(PUBLISHER_RECOVERY_MS)
                    > *now_ms
            });
        }
        match command {
            PublisherCommand::Open { .. } => self.open(operation, command),
            PublisherCommand::Heartbeat {
                producer,
                registration,
                client,
                credential,
                sequence,
                now_ms,
                credential_expires_ms,
                digest,
            } => {
                let record = self.bound_record(*producer, *registration, *client, *credential)?;
                if let Some(heartbeat) = &record.heartbeat {
                    if *sequence < heartbeat.sequence {
                        return Err(Rejection::Superseded);
                    }
                    if *sequence == heartbeat.sequence {
                        return if heartbeat.operation == operation && heartbeat.digest == *digest {
                            Ok(record.permit_at(*now_ms))
                        } else {
                            Err(Rejection::OperationConflict)
                        };
                    }
                }
                if !record.permit_at(*now_ms).live {
                    return Err(Rejection::Superseded);
                }
                if record.next_heartbeat_sequence() != Some(*sequence) {
                    return Err(Rejection::GenerationConflict);
                }
                let expires_ms = lease_end(*now_ms, *credential_expires_ms)?;
                record.permit.expires_ms = record.permit.expires_ms.max(expires_ms);
                record.heartbeat = Some(PublisherHeartbeat {
                    sequence: *sequence,
                    operation,
                    digest: *digest,
                    expires_ms: record.permit.expires_ms,
                });
                Ok(record.permit.clone())
            }
            PublisherCommand::Close {
                producer,
                registration,
                client,
                credential,
            } => {
                let record = self.bound_record(*producer, *registration, *client, *credential)?;
                record.permit.live = false;
                Ok(record.permit.clone())
            }
        }
    }

    fn open(
        &mut self,
        operation: Id,
        command: &PublisherCommand,
    ) -> Result<PublisherPermit, Rejection> {
        let PublisherCommand::Open {
            topic,
            producer,
            registration,
            client,
            credential,
            now_ms,
            credential_expires_ms,
            digest,
        } = command
        else {
            return Err(Rejection::InvalidCommand);
        };
        if let Some(previous) = self.records.get(registration) {
            if previous.permit.producer != *producer {
                return Err(Rejection::MemberEpochConflict {
                    expected: self.next_registration,
                });
            }
            return if previous.open_operation == operation
                && previous.open_digest == *digest
                && previous.permit.client == *client
                && previous.permit.credential == *credential
                && previous.permit.topic == *topic
            {
                Ok(previous.permit_at(*now_ms))
            } else {
                Err(Rejection::OperationConflict)
            };
        }
        if *registration < self.next_registration {
            return Err(Rejection::Superseded);
        }
        if *registration != self.next_registration {
            return Err(Rejection::MemberEpochConflict {
                expected: self.next_registration,
            });
        }
        if self.records.len() >= 4096
            || self
                .records
                .values()
                .filter(|record| record.permit.client == *client && record.permit_at(*now_ms).live)
                .count()
                >= 128
        {
            return Err(Rejection::Capacity);
        }
        let next = self
            .next_registration
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let permit = PublisherPermit {
            topic: *topic,
            producer: *producer,
            registration: *registration,
            client: *client,
            credential: *credential,
            expires_ms: lease_end(*now_ms, *credential_expires_ms)?,
            live: true,
        };
        self.records.insert(
            *registration,
            PublisherRecord {
                permit: permit.clone(),
                open_operation: operation,
                open_digest: *digest,
                heartbeat: None,
            },
        );
        self.next_registration = next;
        Ok(permit)
    }

    fn bound_record(
        &mut self,
        producer: Id,
        registration: u64,
        client: Id,
        credential: Id,
    ) -> Result<&mut PublisherRecord, Rejection> {
        let record = self
            .records
            .get_mut(&registration)
            .ok_or(Rejection::Superseded)?;
        if record.permit.producer != producer
            || record.permit.client != client
            || record.permit.credential != credential
        {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), Rejection> {
        if self.next_registration == 0 || self.records.len() > 4096 {
            return Err(Rejection::InvalidEvidence);
        }
        for (registration, record) in &self.records {
            if *registration == 0
                || *registration != record.permit.registration
                || *registration >= self.next_registration
                || record.permit.expires_ms == 0
                || record.heartbeat.as_ref().is_some_and(|heartbeat| {
                    heartbeat.sequence == 0 || heartbeat.expires_ms > record.permit.expires_ms
                })
            {
                return Err(Rejection::InvalidEvidence);
            }
        }
        Ok(())
    }
}

fn lease_end(now_ms: u64, credential_expires_ms: u64) -> Result<u64, Rejection> {
    if credential_expires_ms <= now_ms {
        return Err(Rejection::InvalidEvidence);
    }
    now_ms
        .checked_add(PUBLISHER_LEASE_MS)
        .map(|end| end.min(credential_expires_ms))
        .ok_or(Rejection::CounterExhausted)
}

impl MetadataState {
    pub fn publishers(&self) -> &PublishersBook {
        &self.publishers
    }

    pub(super) fn publisher_command(
        &mut self,
        operation: Id,
        command: &PublisherCommand,
    ) -> Result<PublisherPermit, Rejection> {
        if let PublisherCommand::Open { topic, .. } = command
            && !self.topic_writable(*topic)
        {
            return Err(Rejection::NotFound);
        }
        self.publishers.apply(operation, command)
    }
}
