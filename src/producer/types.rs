use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use super::{Rejection, Result};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProducerLimits {
    pub max_sessions: usize,
    pub max_results_per_session: usize,
    pub max_hot_messages: usize,
    pub max_hot_bytes: u64,
    pub max_batch_messages: usize,
    pub max_batch_bytes: u64,
    pub max_message_bytes: u64,
    pub session_ttl_ms: u64,
    pub result_ttl_ms: u64,
}

impl Default for ProducerLimits {
    fn default() -> Self {
        Self {
            max_sessions: 128,
            max_results_per_session: 64,
            max_hot_messages: 65_536,
            max_hot_bytes: 32 * 1024 * 1024,
            max_batch_messages: 1_024,
            max_batch_bytes: 1024 * 1024,
            max_message_bytes: 256 * 1024,
            session_ttl_ms: 300_000,
            result_ttl_ms: 60_000,
        }
    }
}

impl ProducerLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_sessions == 0
            || self.max_sessions > 4_096
            || self.max_results_per_session == 0
            || self.max_results_per_session > 1_024
            || self.max_hot_messages == 0
            || self.max_hot_messages > 1_048_576
            || self.max_hot_bytes == 0
            || self.max_hot_bytes > 1024 * 1024 * 1024
            || self.max_batch_messages == 0
            || self.max_batch_messages > 16_384
            || self.max_batch_messages > self.max_hot_messages
            || self.max_message_bytes == 0
            || self.max_message_bytes > self.max_batch_bytes
            || self.max_batch_bytes > self.max_hot_bytes
            || self.max_batch_bytes > 16 * 1024 * 1024
            || self.session_ttl_ms == 0
            || self.session_ttl_ms > 3_600_000
            || self.result_ttl_ms == 0
            || self.result_ttl_ms > self.session_ttl_ms
        {
            return Err(Rejection::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct MessageData {
    pub key: Bytes,
    pub body: Bytes,
}

impl MessageData {
    pub fn logical_bytes(&self) -> Option<u64> {
        let key = u64::try_from(self.key.len()).ok()?;
        key.checked_add(u64::try_from(self.body.len()).ok()?)
    }
}

impl fmt::Debug for MessageData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MessageData")
            .field("key_bytes", &self.key.len())
            .field("body_bytes", &self.body.len())
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RegisterProducer {
    pub producer_id: Id,
    pub expected_registration_epoch: u64,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendBatch {
    pub topic_id: Id,
    pub partition_id: u32,
    pub producer_id: Id,
    pub producer_epoch: u64,
    pub first_sequence: u64,
    pub messages: Vec<MessageData>,
    pub content_digest: [u8; 32],
}

impl AppendBatch {
    // A tiny decoded slice must not pin an entire untrusted frame in the durable hot state.
    pub(crate) fn compact_payload(&mut self) -> Result<()> {
        for message in &mut self.messages {
            message.key = compact_bytes(&message.key)?;
            message.body = compact_bytes(&message.body)?;
        }
        Ok(())
    }

    pub fn verify_digest(&self) -> Result<()> {
        let digest = tidemq_protocol::producer::canonical_produce_digest_parts(
            self.topic_id,
            self.partition_id,
            self.producer_id,
            self.producer_epoch,
            self.first_sequence,
            self.messages
                .iter()
                .map(|message| (message.key.as_ref(), message.body.as_ref())),
        )
        .map_err(|_| Rejection::InvalidBatch)?;
        if digest != self.content_digest {
            return Err(Rejection::DigestMismatch);
        }
        Ok(())
    }
}

fn compact_bytes(bytes: &[u8]) -> Result<Bytes> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| Rejection::Allocation)?;
    owned.extend_from_slice(bytes);
    Ok(Bytes::from(owned))
}

impl fmt::Debug for AppendBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AppendBatch")
            .field("topic_id", &self.topic_id)
            .field("partition_id", &self.partition_id)
            .field("producer_id", &self.producer_id)
            .field("producer_epoch", &self.producer_epoch)
            .field("first_sequence", &self.first_sequence)
            .field("message_count", &self.messages.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionInfo {
    pub producer_id: Id,
    pub producer_epoch: u64,
    pub next_sequence: u64,
    pub expires_at_ms: u64,
    pub result_floor_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BatchResult {
    pub first_offset: u64,
    pub message_count: u32,
    pub duplicate: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadPage {
    pub first_offset: u64,
    pub messages: Vec<MessageData>,
    pub next_offset: u64,
    pub committed_end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProducerDiagnostic {
    pub topic_id: Id,
    pub partition_id: u32,
    pub committed_end: u64,
    pub retained_floor: u64,
    pub hot_messages: usize,
    pub hot_message_capacity: usize,
    pub hot_message_capacity_bytes: usize,
    pub hot_bytes: u64,
    pub active_sessions: usize,
    pub retained_results: usize,
    pub result_capacity: usize,
    pub next_registration_epoch: u64,
    pub logical_time_ms: u64,
    pub revision: u64,
}
