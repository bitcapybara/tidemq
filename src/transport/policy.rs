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
        if self.max_connections == 0
            || self.max_connections > 4_096
            || self.max_handshakes == 0
            || self.max_handshakes > self.max_connections
            || self.core_streams == 0
            || self.core_streams > 8
            || self.max_streams <= self.core_streams
            || self.max_streams > 4_096
            || self.max_ack_streams == 0
            || self.max_ack_streams > self.max_streams.saturating_sub(self.core_streams)
            || self.stream_window_bytes < 1_024
            || self.connection_window_bytes < self.stream_window_bytes
            || self.send_buffer_bytes < 1_024
            || self.handshake_timeout.is_zero()
            || self.handshake_timeout > Duration::from_secs(30)
            || self.stream_prelude_timeout.is_zero()
            || self.stream_prelude_timeout > self.handshake_timeout
            || self.idle_timeout < self.handshake_timeout
        {
            return Err(Error::Configuration {
                reason: "connection, stream, window, or timeout bounds are inconsistent",
            });
        }
        self.transport_reservation()?;
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
        let map = |_| Error::Configuration {
            reason: "QUIC limit rejected",
        };
        Limits::new()
            .with_data_window(u64::from(self.connection_window_bytes))
            .map_err(map)?
            .with_bidirectional_local_data_window(u64::from(self.stream_window_bytes))
            .map_err(map)?
            .with_bidirectional_remote_data_window(u64::from(self.stream_window_bytes))
            .map_err(map)?
            .with_unidirectional_data_window(0)
            .map_err(map)?
            .with_max_open_local_bidirectional_streams(self.max_streams as u64)
            .map_err(map)?
            .with_max_open_remote_bidirectional_streams(self.max_streams as u64)
            .map_err(map)?
            .with_max_open_local_unidirectional_streams(0)
            .map_err(map)?
            .with_max_open_remote_unidirectional_streams(0)
            .map_err(map)?
            .with_max_send_buffer_size(self.send_buffer_bytes)
            .map_err(map)?
            .with_max_handshake_duration(self.handshake_timeout)
            .map_err(map)?
            .with_max_idle_timeout(self.idle_timeout)
            .map_err(map)
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
        if self.server_name.is_empty()
            || self.server_name.len() > 253
            || !self
                .server_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
            || self.local_role.is_empty()
            || self.local_role.len() > 32
            || self.remote_role.is_empty()
            || self.remote_role.len() > 32
            || self.node_id == Some(0)
        {
            return Err(Error::Configuration {
                reason: "peer name or role is invalid",
            });
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
