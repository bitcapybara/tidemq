//! A bounded native QUIC client. The embedding application owns logging and allocation.
//!
//! Producer retries retain their original identity. Receipt differs from durable acknowledgement.
#![doc = include_str!("../README.md")]
mod admin;
mod builder;
mod client;
mod close;
mod cluster;
mod context;
mod error;
mod group_consumer;
mod identity;
mod identity_list;
mod logical_producer;
mod message;
mod metrics;
mod request;
mod resource_limits;
mod subscriptions;
mod topic;

pub use admin::{Admin, AdminReceipt, PendingAdmin};
pub use builder::{ClientBuilder, Credentials, TlsTrust};
use client::ClientConfig;
pub use client::{Client, ClientLimits};
pub use close::{CloseError, CloseReport};
pub use cluster::{
    ClusterNodeInfo, ClusterRole, DataGroupInfo, MetadataMembershipInfo, NodeRegistration,
};
pub use context::scope_traceparent;
pub use error::{Error, ErrorKind, Result};
pub use group_consumer::{
    AckProgress, AckReceipt, PendingAck, QueueConsumer, QueueConsumerBuilder, StreamConsumer,
    StreamConsumerBuilder,
};
pub use identity::{
    AccessToken, CredentialId, NamespaceId, Permission, PrincipalBuilder, PrincipalId, TenantId,
};
pub use identity_list::{
    CredentialInfo, IdentityStatus, NamespaceInfo, PrincipalInfo, ResourceList, TenantInfo,
};
pub use logical_producer::{PendingSend, Producer, ProducerBuilder, SendBatchResult, SendReceipt};
pub use message::{Message, MessageId, ReceivedMessage};
pub use metrics::{Metrics, MetricsSnapshot};
pub use request::RequestContext;
pub use resource_limits::{PolicyUpdate, ResourceLimits, ResourceLimitsBuilder};
pub use subscriptions::{
    PendingSubscriptionDeletion, SubscriptionDeleted, SubscriptionId, SubscriptionInfo,
};
pub use tidemq_protocol::topic::InitialPosition;
pub use tokio_util::sync::CancellationToken;
pub use topic::{TopicId, TopicInfo};

#[cfg(test)]
mod interface_tests {
    fn shared<T: Clone + Send + Sync>() {}

    #[test]
    fn public_logical_handles_support_shared_async_ownership() {
        shared::<super::Client>();
        shared::<super::Producer>();
        shared::<super::StreamConsumer>();
        shared::<super::QueueConsumer>();
        shared::<super::PendingSend>();
        shared::<super::PendingAck>();
        shared::<super::PendingAdmin>();
    }
}
