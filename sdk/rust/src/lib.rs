//! A bounded native QUIC client. The embedding application owns logging and allocation.
//!
//! Producer retries retain their original identity. Receipt differs from durable acknowledgement.
#![doc = include_str!("../README.md")]
mod admin;
mod client;
mod consumer;
mod context;
mod error;
mod metrics;
mod producer;
mod request;

pub use admin::Admin;
pub use bytes::Bytes;
pub use client::{Client, ClientConfig};
pub use consumer::{Consumer, ConsumerConfig, Delivery, DeliveryOrdering};
pub use context::scope_traceparent;
pub use error::{Error, Result};
pub use metrics::{Metrics, MetricsSnapshot};
pub use producer::{PendingBatch, ProduceReceipt, Producer};
pub use request::RequestContext;
pub use tidemq_protocol::producer::partition_for_key;
pub use tidemq_protocol::{Id, ProtocolLimits, common, public};
pub use tidemq_transport::{ClientTls, PeerTarget, TransportConfig};
pub use tokio_util::sync::CancellationToken;
