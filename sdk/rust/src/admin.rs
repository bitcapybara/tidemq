use bytes::Bytes;
use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    public,
};
use tokio::time::Instant;

use crate::{Client, Error, Result};

#[derive(Clone)]
pub struct Admin {
    pub(crate) client: Client,
}

impl Admin {
    pub async fn create_topic(
        &self,
        name: &str,
        partitions: u32,
        deadline: Instant,
    ) -> Result<public::TopicResult> {
        validate_name(name)?;
        if partitions == 0 {
            return Err(Error::Configuration {
                reason: "topic requires partitions",
            });
        }
        self.client
            .request(
                FrameKind::CreateTopic,
                FrameKind::TopicResult,
                TrafficClass::Control,
                Id::random()?,
                &public::CreateTopic {
                    name: name.into(),
                    partitions,
                },
                deadline,
            )
            .await
    }

    pub async fn topic(&self, name: &str, deadline: Instant) -> Result<public::TopicResult> {
        validate_name(name)?;
        self.client
            .request(
                FrameKind::GetTopic,
                FrameKind::TopicResult,
                TrafficClass::Control,
                Id::random()?,
                &public::GetTopic { name: name.into() },
                deadline,
            )
            .await
    }

    pub async fn create_subscription(
        &self,
        topic: Id,
        name: &str,
        start_offset: u64,
        mode: public::DeliveryMode,
        deadline: Instant,
    ) -> Result<public::SubscriptionResult> {
        validate_name(name)?;
        if mode == public::DeliveryMode::Unspecified {
            return Err(Error::Configuration {
                reason: "delivery mode is unspecified",
            });
        }
        self.client
            .request(
                FrameKind::CreateSubscription,
                FrameKind::SubscriptionResult,
                TrafficClass::Control,
                Id::random()?,
                &public::CreateSubscription {
                    topic_id: bytes(topic),
                    name: name.into(),
                    start_offset,
                    delivery_mode: mode.into(),
                },
                deadline,
            )
            .await
    }

    pub async fn subscription(
        &self,
        topic: Id,
        name: &str,
        deadline: Instant,
    ) -> Result<public::SubscriptionResult> {
        validate_name(name)?;
        self.client
            .request(
                FrameKind::GetSubscription,
                FrameKind::SubscriptionResult,
                TrafficClass::Control,
                Id::random()?,
                &public::GetSubscription {
                    topic_id: bytes(topic),
                    name: name.into(),
                },
                deadline,
            )
            .await
    }

    /// Explicit partition deletion preserves protection until the server commits the operation.
    pub async fn delete_subscription(
        &self,
        request: public::DeleteSubscription,
        operation: Id,
        deadline: Instant,
    ) -> Result<public::SubscriptionDeleted> {
        if request.owner_epoch != 0 {
            return Err(Error::Configuration {
                reason: "external clients cannot select an owner epoch",
            });
        }
        self.client
            .request(
                FrameKind::DeleteSubscription,
                FrameKind::SubscriptionDeleted,
                TrafficClass::Control,
                operation,
                &request,
                deadline,
            )
            .await
    }

    pub async fn producer_status(
        &self,
        topic: Id,
        partition: u32,
        producer: Id,
        epoch: u64,
        deadline: Instant,
    ) -> Result<public::ProducerStatusResult> {
        self.client
            .request(
                FrameKind::ProducerStatus,
                FrameKind::ProducerStatusResult,
                TrafficClass::Control,
                Id::random()?,
                &public::ProducerStatus {
                    topic_id: bytes(topic),
                    partition_id: partition,
                    producer_id: bytes(producer),
                    producer_epoch: epoch,
                },
                deadline,
            )
            .await
    }

    /// Query the current session before making an explicit takeover decision.
    pub async fn subscription_status(
        &self,
        topic: Id,
        partition: u32,
        subscription: Id,
        deadline: Instant,
    ) -> Result<public::SubscriptionStatusResult> {
        self.client
            .request(
                FrameKind::SubscriptionStatus,
                FrameKind::SubscriptionStatusResult,
                TrafficClass::Control,
                Id::random()?,
                &public::SubscriptionStatus {
                    topic_id: bytes(topic),
                    partition_id: partition,
                    subscription_id: bytes(subscription),
                },
                deadline,
            )
            .await
    }
}

pub(crate) fn bytes(id: Id) -> Bytes {
    Bytes::copy_from_slice(id.as_bytes())
}
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 255 || name.chars().any(char::is_control) {
        return Err(Error::Configuration {
            reason: "resource name is empty, excessive or contains control characters",
        });
    }
    Ok(())
}
