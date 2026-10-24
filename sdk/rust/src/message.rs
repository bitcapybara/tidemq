use bytes::Bytes;
use std::{fmt, sync::Arc};
use tidemq_protocol::{Id, public};
use tokio::sync::OwnedSemaphorePermit;

#[cfg(test)]
#[path = "../../../tests/scalable_support/send_message.rs"]
mod send_tests;

#[derive(Clone, Default)]
pub struct Message {
    pub(crate) wire: public::PublishMessage,
}

impl Message {
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            wire: public::PublishMessage {
                payload: payload.into(),
                ..Default::default()
            },
        }
    }
    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.wire.key = Some(key.into());
        self
    }
    pub fn property(mut self, name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.wire.properties.insert(name.into(), value.into());
        self
    }
    pub(crate) fn memory_bytes(&self, maximum: u32) -> crate::Result<u32> {
        let bytes = tidemq_protocol::topic::delivery_bytes(
            self.wire.key.as_deref().unwrap_or_default(),
            &self.wire.payload,
            &self.wire.properties,
        )?;
        if bytes > u64::from(maximum).saturating_add(256)
            || self.wire.properties.len() > 128
            || self
                .wire
                .properties
                .keys()
                .any(|name| name.is_empty() || name.len() > 256)
        {
            return Err(crate::Error::Configuration {
                reason: "message exceeds its byte or property limits",
            });
        }
        u32::try_from(bytes.saturating_mul(2).saturating_add(4096)).map_err(|_| {
            crate::Error::Admission {
                resource: "message bytes",
            }
        })
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Message")
            .field("payload_bytes", &self.wire.payload.len())
            .field("has_key", &self.wire.key.is_some())
            .field("property_count", &self.wire.properties.len())
            .finish()
    }
}

impl From<&str> for Message {
    fn from(value: &str) -> Self {
        Self::new(Bytes::copy_from_slice(value.as_bytes()))
    }
}
impl From<String> for Message {
    fn from(value: String) -> Self {
        Self::new(value.into_bytes())
    }
}
impl From<&[u8]> for Message {
    fn from(value: &[u8]) -> Self {
        Self::new(Bytes::copy_from_slice(value))
    }
}
impl From<Vec<u8>> for Message {
    fn from(value: Vec<u8>) -> Self {
        Self::new(value)
    }
}
impl From<Bytes> for Message {
    fn from(value: Bytes) -> Self {
        Self::new(value)
    }
}

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
    pub _charge: crate::metrics::Charge,
    pub id: MessageId,
    pub key: Bytes,
    pub key_present: bool,
    pub properties: std::collections::BTreeMap<String, Bytes>,
    pub payload: Bytes,
    pub proof: public::GroupProof,
    pub member: Id,
    pub epoch: u64,
    pub consumer: Id,
    pub partition: u32,
    pub hash: u64,
    pub assignment_version: u64,
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
        self.data.key_present.then_some(&self.data.key)
    }
    pub fn properties(&self) -> &std::collections::BTreeMap<String, Bytes> {
        &self.data.properties
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
        let metrics = Arc::new(crate::Metrics::default());
        let data = Arc::new(ReceivedData {
            _charge: metrics.charge(512, true),
            id: MessageId {
                topic: id,
                shard: id,
                offset: 0,
            },
            key: Bytes::new(),
            key_present: false,
            properties: Default::default(),
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
            assignment_version: 1,
            acknowledgement: std::sync::OnceLock::new(),
            _memory: memory.clone().acquire_many_owned(256).await.unwrap(),
            _client_memory: memory.clone().acquire_many_owned(256).await.unwrap(),
            slot: std::sync::Mutex::new(Some(slots.clone().acquire_owned().await.unwrap())),
        });
        let retained = data.clone();
        data.release_credit().unwrap();
        assert_eq!(slots.available_permits(), 1);
        assert_eq!(memory.available_permits(), 0);
        assert_eq!(metrics.snapshot().admitted_bytes, 512);
        assert_eq!(metrics.snapshot().active_deliveries, 1);
        drop(data);
        assert_eq!(memory.available_permits(), 0);
        drop(retained);
        assert_eq!(memory.available_permits(), 512);
        assert_eq!(metrics.snapshot().admitted_bytes, 0);
        assert_eq!(metrics.snapshot().active_deliveries, 0);
    }
}
