use bytes::Bytes;
use std::{fmt, sync::Arc};
use tidemq_protocol::{Id, public};
use tokio::sync::OwnedSemaphorePermit;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MessageId {
    pub(crate) topic: Id,
    pub(crate) shard: Id,
    pub(crate) offset: u64,
}

impl fmt::Display for MessageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}:{}", self.topic, self.shard, self.offset)
    }
}

pub(crate) struct ReceivedData {
    pub id: MessageId,
    pub key: Bytes,
    pub payload: Bytes,
    pub proof: public::GroupProof,
    pub member: Id,
    pub epoch: u64,
    pub consumer: Id,
    pub partition: u32,
    pub hash: u64,
    pub _memory: OwnedSemaphorePermit,
    pub _client_memory: OwnedSemaphorePermit,
    pub _slot: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub struct ReceivedMessage {
    pub(crate) data: Arc<ReceivedData>,
    pub(crate) delivery_sequence: u64,
}

impl ReceivedMessage {
    pub fn message_id(&self) -> MessageId {
        self.data.id
    }
    pub fn payload(&self) -> &[u8] {
        &self.data.payload
    }
    pub fn key(&self) -> Option<&[u8]> {
        Some(&self.data.key)
    }
}

impl fmt::Debug for ReceivedMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReceivedMessage")
            .field("message_id", &self.data.id)
            .field("payload_bytes", &self.data.payload.len())
            .finish_non_exhaustive()
    }
}
