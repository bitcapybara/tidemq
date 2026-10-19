//! TideMQ client facilities. The embedding application owns logging and allocation.

use snafu::Snafu;

pub use tidemq_protocol::{Id, ProtocolLimits, common, public};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid client configuration: {reason}"))]
    Configuration { reason: &'static str },
    #[snafu(display("Invalid protocol configuration"))]
    Protocol { source: tidemq_protocol::Error },
}

/// Client-side bounds used before opening a transport connection.
///
/// ```
/// let config = tidemq_sdk::ClientConfig::new("localhost:7443");
/// config.validate()?;
/// # Ok::<(), tidemq_sdk::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub endpoint: String,
    pub request_timeout_ms: u64,
    pub max_pending_requests: u32,
    pub max_pending_bytes: u64,
    pub protocol: ProtocolLimits,
}

impl ClientConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            request_timeout_ms: 30_000,
            max_pending_requests: 256,
            max_pending_bytes: 16_777_216,
            protocol: ProtocolLimits::default(),
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.endpoint.is_empty()
            || self.endpoint.len() > 253
            || self.endpoint.contains(char::is_whitespace)
        {
            return Err(Error::Configuration {
                reason: "endpoint must be a bounded host and port",
            });
        }
        if self.request_timeout_ms == 0
            || self.request_timeout_ms > tidemq_protocol::framing::MAX_TIMEOUT_MS
        {
            return Err(Error::Configuration {
                reason: "request timeout is outside the supported range",
            });
        }
        if self.max_pending_requests == 0
            || self.max_pending_bytes < u64::from(self.protocol.max_frame_bytes)
        {
            return Err(Error::Configuration {
                reason: "pending request budget is invalid",
            });
        }
        self.protocol
            .validate()
            .map_err(|source| Error::Protocol { source })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_budgets_are_validated_without_global_initialization() {
        let mut config = ClientConfig::new("localhost:7443");
        config.validate().unwrap();
        config.max_pending_bytes = 1;
        assert!(config.validate().is_err());
    }
}
