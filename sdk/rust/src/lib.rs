//! A bounded native QUIC client. The embedding application owns logging and allocation.
//!
//! Producer retries retain their original identity. Receipt differs from durable acknowledgement.
#![doc = include_str!("../README.md")]
mod admin;
mod builder;
mod client;
mod close;
mod consumer;
mod context;
mod error;
mod group_consumer;
mod identity;
mod logical_producer;
mod message;
mod metrics;
mod producer;
mod request;
mod subscriptions;
mod topic;

pub use admin::Admin;
pub use builder::{ClientBuilder, Credentials, TlsTrust};
pub use bytes::Bytes;
pub use client::{Client, ClientConfig};
pub use close::{CloseError, CloseReport};
pub use consumer::{Consumer, ConsumerConfig, Delivery, DeliveryOrdering};
pub use context::scope_traceparent;
pub use error::{Error, Result};
pub use group_consumer::{
    AckProgress, AckReceipt, PendingAck, QueueConsumer, QueueConsumerBuilder, StreamConsumer,
    StreamConsumerBuilder,
};
pub use identity::{
    AccessToken, CredentialId, NamespaceId, Permission, PrincipalBuilder, PrincipalId, TenantId,
};
pub use logical_producer::{
    PendingSend, ProducerBuilder, SendBatchResult, SendReceipt, TopicProducer,
};
pub use message::{Message, MessageId, ReceivedMessage};
pub use metrics::{Metrics, MetricsSnapshot};
pub use producer::{PendingBatch, ProduceReceipt, Producer};
pub use request::RequestContext;
pub use subscriptions::{
    PendingSubscriptionDeletion, SubscriptionDeleted, SubscriptionId, SubscriptionInfo,
};
pub use tidemq_protocol::producer::partition_for_key;
pub use tidemq_protocol::topic::InitialPosition;
pub use tidemq_protocol::{Id, ProtocolLimits, common, public};
pub use tidemq_transport::{ClientTls, PeerTarget, TransportConfig};
pub use tokio_util::sync::CancellationToken;
pub use topic::{TopicId, TopicInfo};
