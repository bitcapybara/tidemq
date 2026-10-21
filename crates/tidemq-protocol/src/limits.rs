use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProtocolLimits {
    pub max_frame_bytes: u32,
    pub max_control_frame_bytes: u32,
    pub max_message_bytes: u32,
    pub max_batch_messages: u32,
    pub max_payload_fields: u32,
    pub max_traceparent_bytes: u32,
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1_048_576,
            max_control_frame_bytes: 16_384,
            max_message_bytes: 262_144,
            max_batch_messages: 1_024,
            max_payload_fields: 1_100,
            max_traceparent_bytes: 55,
        }
    }
}

impl ProtocolLimits {
    pub fn validate(&self) -> Result<()> {
        for (valid, reason) in [
            (
                self.max_control_frame_bytes >= 128,
                "max_control_frame_bytes must be at least 128 bytes",
            ),
            (
                self.max_control_frame_bytes <= self.max_frame_bytes,
                "max_control_frame_bytes must not exceed max_frame_bytes",
            ),
            (
                self.max_frame_bytes <= 16_777_216,
                "max_frame_bytes must not exceed 16777216 bytes",
            ),
            (
                self.max_message_bytes > 0,
                "max_message_bytes must be greater than zero",
            ),
            (
                self.max_message_bytes < self.max_frame_bytes,
                "max_message_bytes must be smaller than max_frame_bytes",
            ),
            (
                (1..=16_384).contains(&self.max_batch_messages),
                "max_batch_messages must be between 1 and 16384",
            ),
            (
                self.max_payload_fields >= self.max_batch_messages.saturating_add(16),
                "max_payload_fields must be at least max_batch_messages + 16",
            ),
            (
                self.max_payload_fields <= 32_768,
                "max_payload_fields must not exceed 32768",
            ),
            (
                self.max_traceparent_bytes == 55,
                "max_traceparent_bytes must be 55 for W3C traceparent version 00",
            ),
        ] {
            if !valid {
                return Err(Error::Invalid { reason });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;

    #[test]
    fn every_protocol_limit_reports_its_own_constraint() {
        type Case = (&'static str, fn(&mut ProtocolLimits));
        let cases: &[Case] = &[
            ("max_control_frame_bytes", |c| {
                c.max_control_frame_bytes = 127
            }),
            ("max_control_frame_bytes", |c| {
                c.max_control_frame_bytes = c.max_frame_bytes + 1
            }),
            ("max_frame_bytes", |c| c.max_frame_bytes = 16_777_217),
            ("max_message_bytes", |c| c.max_message_bytes = 0),
            ("max_message_bytes", |c| {
                c.max_message_bytes = c.max_frame_bytes
            }),
            ("max_batch_messages", |c| c.max_batch_messages = 0),
            ("max_batch_messages", |c| c.max_batch_messages = 16_385),
            ("max_payload_fields", |c| {
                c.max_payload_fields = c.max_batch_messages + 15
            }),
            ("max_payload_fields", |c| c.max_payload_fields = 32_769),
            ("max_traceparent_bytes", |c| c.max_traceparent_bytes = 54),
        ];
        for &(field, mutate) in cases {
            let mut limits = ProtocolLimits::default();
            mutate(&mut limits);
            let Error::Invalid { reason } = limits.validate().unwrap_err() else {
                panic!("Expected a protocol limit validation error for {field}");
            };
            assert!(
                reason.starts_with(field),
                "Expected {field}, received {reason}"
            );
        }
    }
}
