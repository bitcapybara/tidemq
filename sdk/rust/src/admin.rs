use bytes::Bytes;
use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    public,
};
use tokio::time::Instant;

use crate::{Client, Error, Result};

pub(crate) mod pending;
pub use pending::{AdminReceipt, PendingAdmin};

#[derive(Clone)]
pub struct Admin {
    pub(crate) client: Client,
}

impl Admin {
    pub(crate) async fn topic(&self, name: &str) -> Result<public::TopicResult> {
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
}

pub(crate) fn bytes(id: Id) -> Bytes {
    Bytes::copy_from_slice(id.as_bytes())
}
pub(crate) fn validate_name(name: &str) -> Result<()> {
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
