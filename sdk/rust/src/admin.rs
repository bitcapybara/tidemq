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
    pub async fn create_topic(&self, name: &str, partitions: u32) -> Result<public::TopicResult> {
        self.client
            .operation(|deadline| self.create_topic_inner(name, partitions, deadline))
            .await
    }

    async fn create_topic_inner(
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
                Id::new_v7()?,
                &public::CreateTopic {
                    name: name.into(),
                    partitions,
                },
                deadline,
            )
            .await
    }

    pub async fn topic(&self, name: &str) -> Result<public::TopicResult> {
        self.client
            .operation(|deadline| self.topic_inner(name, deadline))
            .await
    }

    async fn topic_inner(&self, name: &str, deadline: Instant) -> Result<public::TopicResult> {
        validate_name(name)?;
        self.client
            .request(
                FrameKind::GetTopic,
                FrameKind::TopicResult,
                TrafficClass::Control,
                Id::new_v7()?,
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
    ) -> Result<public::SubscriptionResult> {
        self.client
            .operation(|deadline| {
                self.create_subscription_inner(topic, name, start_offset, mode, deadline)
            })
            .await
    }

    async fn create_subscription_inner(
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
                Id::new_v7()?,
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

    pub async fn subscription(&self, topic: Id, name: &str) -> Result<public::SubscriptionResult> {
        self.client
            .operation(|deadline| self.subscription_inner(topic, name, deadline))
            .await
    }

    async fn subscription_inner(
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
                Id::new_v7()?,
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
    ) -> Result<public::SubscriptionDeleted> {
        self.client
            .operation(|deadline| self.delete_subscription_inner(request, operation, deadline))
            .await
    }

    async fn delete_subscription_inner(
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
    ) -> Result<public::ProducerStatusResult> {
        self.client
            .operation(|deadline| {
                self.producer_status_inner(topic, partition, producer, epoch, deadline)
            })
            .await
    }

    async fn producer_status_inner(
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
                Id::new_v7()?,
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
    ) -> Result<public::SubscriptionStatusResult> {
        self.client
            .operation(|deadline| {
                self.subscription_status_inner(topic, partition, subscription, deadline)
            })
            .await
    }

    async fn subscription_status_inner(
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
                Id::new_v7()?,
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
    for (valid, reason) in [
        (!name.is_empty(), "resource name must not be empty"),
        (name.len() <= 255, "resource name must not exceed 255 bytes"),
        (
            !name.chars().any(char::is_control),
            "resource name must not contain control characters",
        ),
    ] {
        if !valid {
            return Err(Error::Configuration { reason });
        }
    }
    Ok(())
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;

    #[test]
    fn invalid_names_distinguish_missing_length_and_control_characters() {
        let empty = validate_name("").unwrap_err().to_string();
        let long = validate_name(&"x".repeat(256)).unwrap_err().to_string();
        let control = validate_name("events\n").unwrap_err().to_string();
        assert!(empty.contains("must not be empty"));
        assert!(long.contains("must not exceed 255 bytes"));
        assert!(control.contains("must not contain control characters"));
        validate_name("events").unwrap();
    }
}
