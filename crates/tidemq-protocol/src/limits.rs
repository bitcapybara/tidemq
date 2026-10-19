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
        if self.max_control_frame_bytes < 128
            || self.max_control_frame_bytes > self.max_frame_bytes
            || self.max_frame_bytes > 16_777_216
        {
            return Err(Error::Invalid {
                reason: "frame limits are inconsistent",
            });
        }
        if self.max_message_bytes == 0 || self.max_message_bytes >= self.max_frame_bytes {
            return Err(Error::Invalid {
                reason: "message limit must fit inside a frame",
            });
        }
        if self.max_batch_messages == 0
            || self.max_batch_messages > 16_384
            || self.max_payload_fields < self.max_batch_messages.saturating_add(16)
            || self.max_payload_fields > 32_768
        {
            return Err(Error::Invalid {
                reason: "batch or field limit is inconsistent",
            });
        }
        if self.max_traceparent_bytes != 55 {
            return Err(Error::Invalid {
                reason: "only W3C traceparent version 00 is supported",
            });
        }
        Ok(())
    }
}
