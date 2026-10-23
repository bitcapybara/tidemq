use bytes::Bytes;
use std::{fmt, sync::Arc};
use tidemq_protocol::{Id, public};
use tokio::sync::OwnedSemaphorePermit;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
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
    pub acknowledgement: std::sync::OnceLock<crate::AckReceipt>,
    pub _memory: OwnedSemaphorePermit,
    pub _client_memory: OwnedSemaphorePermit,
    pub slot: std::sync::Mutex<Option<OwnedSemaphorePermit>>,
}

impl ReceivedData {
    pub(crate) fn release_credit(&self) -> crate::Result<()> {
        self.slot.lock().map_err(|_| crate::Error::Closed)?.take();
        Ok(())
    }
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

#[cfg(test)]
mod credit_tests {
    use super::*;
    #[tokio::test]
    async fn acknowledging_releases_delivery_credit_while_retained_payloads_stay_charged() {
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let memory = Arc::new(tokio::sync::Semaphore::new(512));
        let id = Id::new_v7().unwrap();
        let data = Arc::new(ReceivedData {
            id: MessageId {
                topic: id,
                shard: id,
                offset: 0,
            },
            key: Bytes::new(),
            payload: Bytes::from_static(b"data"),
            proof: public::GroupProof {
                offset: 0,
                attempt: 1,
                mac: Bytes::from(vec![1; 32]),
            },
            member: id,
            epoch: 1,
            consumer: id,
            partition: 0,
            hash: 0,
            acknowledgement: std::sync::OnceLock::new(),
            _memory: memory.clone().acquire_many_owned(256).await.unwrap(),
            _client_memory: memory.clone().acquire_many_owned(256).await.unwrap(),
            slot: std::sync::Mutex::new(Some(slots.clone().acquire_owned().await.unwrap())),
        });
        let retained = data.clone();
        data.release_credit().unwrap();
        assert_eq!(slots.available_permits(), 1);
        assert_eq!(memory.available_permits(), 0);
        drop(data);
        assert_eq!(memory.available_permits(), 0);
        drop(retained);
        assert_eq!(memory.available_permits(), 512);
    }
}
