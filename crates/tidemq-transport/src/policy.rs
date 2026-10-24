use std::net::SocketAddr;
use std::time::Duration;

use s2n_quic::provider::limits::Limits;
use tidemq_protocol::{Id, ProtocolLimits};

use super::{Error, Result};

/// Bounds apply before application handlers run and before peer-supplied allocation.
#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub protocol: ProtocolLimits,
    pub max_connections: usize,
    pub max_handshakes: usize,
    pub max_streams: usize,
    pub core_streams: usize,
    pub max_ack_streams: usize,
    pub connection_window_bytes: u32,
    pub stream_window_bytes: u32,
    pub send_buffer_bytes: u32,
    pub handshake_timeout: Duration,
    pub stream_prelude_timeout: Duration,
    pub idle_timeout: Duration,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            protocol: ProtocolLimits::default(),
            max_connections: 128,
            max_handshakes: 16,
            max_streams: 16,
            core_streams: 2,
            max_ack_streams: 8,
            connection_window_bytes: 65_536,
            stream_window_bytes: 16_384,
            send_buffer_bytes: 16_384,
            handshake_timeout: Duration::from_secs(5),
            stream_prelude_timeout: Duration::from_secs(2),
            idle_timeout: Duration::from_secs(30),
        }
    }
}

impl TransportConfig {
    pub fn validate(&self) -> Result<()> {
        self.protocol
            .validate()
            .map_err(|source| Error::Protocol { source })?;
        self.validate_streams()?;
        for (valid, reason) in [
            (
                self.stream_window_bytes >= 1_024,
                "stream_window_bytes must be at least 1024 bytes",
            ),
            (
                self.connection_window_bytes >= self.stream_window_bytes,
                "connection_window_bytes must be at least stream_window_bytes",
            ),
            (
                self.send_buffer_bytes >= 1_024,
                "send_buffer_bytes must be at least 1024 bytes",
            ),
            (
                !self.handshake_timeout.is_zero()
                    && self.handshake_timeout <= Duration::from_secs(30),
                "handshake_timeout must be greater than zero and at most 30 seconds",
            ),
            (
                !self.stream_prelude_timeout.is_zero(),
                "stream_prelude_timeout must be greater than zero",
            ),
            (
                self.stream_prelude_timeout <= self.handshake_timeout,
                "stream_prelude_timeout must not exceed handshake_timeout",
            ),
            (
                self.idle_timeout >= self.handshake_timeout,
                "idle_timeout must be at least handshake_timeout",
            ),
        ] {
            if !valid {
                return Err(Error::Configuration { reason });
            }
        }
        self.transport_reservation()?;
        Ok(())
    }

    fn validate_streams(&self) -> Result<()> {
        for (valid, reason) in [
            (
                (1..=4_096).contains(&self.max_connections),
                "max_connections must be between 1 and 4096",
            ),
            (
                self.max_handshakes > 0,
                "max_handshakes must be greater than zero",
            ),
            (
                self.max_handshakes <= self.max_connections,
                "max_handshakes must not exceed max_connections",
            ),
            (
                (1..=8).contains(&self.core_streams),
                "core_streams must be between 1 and 8",
            ),
            (
                self.max_streams > self.core_streams,
                "max_streams must be greater than core_streams",
            ),
            (
                self.max_streams <= 4_096,
                "max_streams must not exceed 4096",
            ),
            (
                self.max_ack_streams > 0,
                "max_ack_streams must be greater than zero",
            ),
            (
                self.max_ack_streams < self.max_streams.saturating_sub(self.core_streams),
                "max_ack_streams must leave at least one ordinary stream beyond core_streams",
            ),
        ] {
            if !valid {
                return Err(Error::Configuration { reason });
            }
        }
        Ok(())
    }

    pub(super) fn transport_reservation(&self) -> Result<u32> {
        let streams = u32::try_from(self.max_streams).map_err(|_| Error::Configuration {
            reason: "stream count overflow",
        })?;
        self.send_buffer_bytes
            .checked_mul(streams)
            .and_then(|send| send.checked_add(self.connection_window_bytes))
            .ok_or(Error::Configuration {
                reason: "transport memory envelope overflow",
            })
    }

    pub(super) fn limits(&self) -> Result<Limits> {
        Limits::new()
            .with_data_window(u64::from(self.connection_window_bytes))
            .map_err(|source| crate::error::setup("set connection_window_bytes", source))?
            .with_bidirectional_local_data_window(u64::from(self.stream_window_bytes))
            .map_err(|source| crate::error::setup("set local stream_window_bytes", source))?
            .with_bidirectional_remote_data_window(u64::from(self.stream_window_bytes))
            .map_err(|source| crate::error::setup("set remote stream_window_bytes", source))?
            .with_unidirectional_data_window(0)
            .map_err(|source| crate::error::setup("disable unidirectional data windows", source))?
            .with_max_open_local_bidirectional_streams(self.max_streams as u64)
            .map_err(|source| crate::error::setup("set local max_streams", source))?
            .with_max_open_remote_bidirectional_streams(self.max_streams as u64)
            .map_err(|source| crate::error::setup("set remote max_streams", source))?
            .with_max_open_local_unidirectional_streams(0)
            .map_err(|source| crate::error::setup("disable local unidirectional streams", source))?
            .with_max_open_remote_unidirectional_streams(0)
            .map_err(|source| crate::error::setup("disable remote unidirectional streams", source))?
            .with_max_send_buffer_size(self.send_buffer_bytes)
            .map_err(|source| crate::error::setup("set send_buffer_bytes", source))?
            .with_max_handshake_duration(self.handshake_timeout)
            .map_err(|source| crate::error::setup("set handshake_timeout", source))?
            .with_max_idle_timeout(self.idle_timeout)
            .map_err(|source| crate::error::setup("set idle_timeout", source))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PeerTarget {
    pub address: SocketAddr,
    pub server_name: String,
    pub local_role: String,
    pub remote_role: String,
    pub cluster_id: Option<Id>,
    pub node_id: Option<u64>,
}

impl PeerTarget {
    pub(super) fn validate(&self) -> Result<()> {
        for (valid, reason) in [
            (
                !self.server_name.is_empty(),
                "server_name must not be empty",
            ),
            (
                self.server_name.len() <= 253,
                "server_name must not exceed 253 bytes",
            ),
            (
                self.server_name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-')),
                "server_name must contain only ASCII letters, digits, dots, and hyphens",
            ),
            (
                !self.local_role.is_empty() && self.local_role.len() <= 32,
                "local_role must contain between 1 and 32 bytes",
            ),
            (
                !self.remote_role.is_empty() && self.remote_role.len() <= 32,
                "remote_role must contain between 1 and 32 bytes",
            ),
            (
                self.node_id != Some(0),
                "node_id must be nonzero when present",
            ),
        ] {
            if !valid {
                return Err(Error::Configuration { reason });
            }
        }
        Ok(())
    }
}

pub(super) fn deadline_after(duration: Duration) -> Result<tokio::time::Instant> {
    tokio::time::Instant::now()
        .checked_add(duration)
        .ok_or(Error::Configuration {
            reason: "transport deadline overflow",
        })
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;

    #[test]
    fn transport_limits_report_the_failed_setting() {
        type Case = (&'static str, fn(&mut TransportConfig));
        let cases: &[Case] = &[
            ("max_connections", |c| c.max_connections = 0),
            ("max_connections", |c| c.max_connections = 4_097),
            ("max_handshakes", |c| c.max_handshakes = 0),
            ("max_handshakes", |c| {
                c.max_handshakes = c.max_connections + 1
            }),
            ("core_streams", |c| c.core_streams = 0),
            ("core_streams", |c| c.core_streams = 9),
            ("max_streams", |c| c.max_streams = c.core_streams),
            ("max_streams", |c| c.max_streams = 4_097),
            ("max_ack_streams", |c| c.max_ack_streams = 0),
            ("max_ack_streams", |c| c.max_ack_streams = c.max_streams),
            ("max_ack_streams", |c| {
                c.max_ack_streams = c.max_streams - c.core_streams
            }),
            ("stream_window_bytes", |c| c.stream_window_bytes = 1_023),
            ("connection_window_bytes", |c| {
                c.connection_window_bytes = c.stream_window_bytes - 1
            }),
            ("send_buffer_bytes", |c| c.send_buffer_bytes = 1_023),
            ("handshake_timeout", |c| {
                c.handshake_timeout = Duration::ZERO
            }),
            ("handshake_timeout", |c| {
                c.handshake_timeout = Duration::from_secs(31)
            }),
            ("stream_prelude_timeout", |c| {
                c.stream_prelude_timeout = Duration::ZERO
            }),
            ("stream_prelude_timeout", |c| {
                c.stream_prelude_timeout = c.handshake_timeout + Duration::from_secs(1)
            }),
            ("idle_timeout", |c| c.idle_timeout = Duration::ZERO),
        ];
        for &(field, mutate) in cases {
            let mut config = TransportConfig::default();
            mutate(&mut config);
            let Error::Configuration { reason } = config.validate().unwrap_err() else {
                panic!("Expected a configuration error for {field}");
            };
            assert!(
                reason.starts_with(field),
                "Expected {field}, received {reason}"
            );
        }
        TransportConfig::default().validate().unwrap();
    }
}
