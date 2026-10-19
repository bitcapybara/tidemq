use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tidemq_protocol::{Id, ProtocolLimits};
use tokio::io::AsyncReadExt;

use crate::error::{IoSnafu, ParseConfigurationSnafu, ProtocolSnafu};
use crate::{Error, Result};

const MAX_CONFIG_BYTES: u64 = 65_536;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    #[default]
    Combined,
    Gateway,
    Broker,
    Storage,
    Metadata,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub node_id: u64,
    pub cluster_id: String,
    pub role: Role,
    pub listen: SocketAddr,
    pub management_listen: SocketAddr,
    pub data_dir: PathBuf,
    pub replication_factor: u8,
    pub memory: MemoryConfig,
    pub protocol: ProtocolLimits,
    pub logging: LoggingConfig,
    pub tls: TlsConfig,
    pub runtime: crate::runtime::RuntimeConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: 1,
            cluster_id: "00000000-0000-4000-8000-000000000001".into(),
            role: Role::Combined,
            listen: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7443),
            management_listen: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9419),
            data_dir: PathBuf::from("data/node-1"),
            replication_factor: 1,
            memory: MemoryConfig::default(),
            protocol: ProtocolLimits::default(),
            logging: LoggingConfig::default(),
            tls: TlsConfig::default(),
            runtime: crate::runtime::RuntimeConfig::default(),
        }
    }
}

impl Config {
    pub async fn load(path: &Path) -> Result<Self> {
        let file = tokio::fs::File::open(path).await.context(IoSnafu {
            operation: "open configuration",
            path,
        })?;
        let mut text = String::new();
        file.take(MAX_CONFIG_BYTES + 1)
            .read_to_string(&mut text)
            .await
            .context(IoSnafu {
                operation: "read configuration",
                path,
            })?;
        if text.len() as u64 > MAX_CONFIG_BYTES {
            return Err(Error::Configuration {
                reason: "configuration exceeds 64 KiB",
            });
        }
        let config: Self = toml::from_str(&text).context(ParseConfigurationSnafu)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.node_id == 0 || self.data_dir.as_os_str().is_empty() {
            return Err(Error::Configuration {
                reason: "node identity and data directory are required",
            });
        }
        self.cluster_id.parse::<Id>().context(ProtocolSnafu)?;
        if !matches!(self.replication_factor, 1 | 3) {
            return Err(Error::Configuration {
                reason: "replication factor must be explicitly 1 or 3",
            });
        }
        if !self.management_listen.ip().is_loopback() {
            return Err(Error::Configuration {
                reason: "remote management requires a protected deployment proxy",
            });
        }
        if self.listen == self.management_listen {
            return Err(Error::Configuration {
                reason: "business and management addresses must differ",
            });
        }
        self.protocol.validate().context(ProtocolSnafu)?;
        self.memory.validate(self.protocol.max_frame_bytes)?;
        self.logging.validate()?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryConfig {
    pub total_bytes: u32,
    pub control_reserved_bytes: u32,
    pub max_connections: u32,
    pub max_streams_per_connection: u32,
    pub max_pending_requests: u32,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            total_bytes: 67_108_864,
            control_reserved_bytes: 4_194_304,
            max_connections: 128,
            max_streams_per_connection: 64,
            max_pending_requests: 256,
        }
    }
}

impl MemoryConfig {
    pub fn validate(&self, frame_bytes: u32) -> Result<()> {
        let minimum = frame_bytes.checked_mul(2).ok_or(Error::Configuration {
            reason: "frame budget overflow",
        })?;
        if self.control_reserved_bytes < 65_536
            || self
                .total_bytes
                .checked_sub(self.control_reserved_bytes)
                .is_none_or(|bytes| bytes < minimum)
        {
            return Err(Error::Configuration {
                reason: "memory budget must preserve control and data capacity",
            });
        }
        if self.max_connections == 0
            || self.max_connections > 4_096
            || self.max_streams_per_connection < 2
            || self.max_streams_per_connection > 4_096
            || self.max_pending_requests == 0
            || self.max_pending_requests > 65_536
        {
            return Err(Error::Configuration {
                reason: "connection, stream, or request bounds are invalid",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    pub filter: String,
    pub json: bool,
    pub queue_events: usize,
    pub max_event_bytes: usize,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            filter: "warn,tidemq=info".into(),
            json: true,
            queue_events: 1_024,
            max_event_bytes: 16_384,
        }
    }
}

impl LoggingConfig {
    pub fn validate(&self) -> Result<()> {
        if self.queue_events == 0
            || self.queue_events > 16_384
            || self.max_event_bytes < 256
            || self.max_event_bytes > 65_536
            || self.filter.len() > 1_024
        {
            return Err(Error::Configuration {
                reason: "logging budget or filter size is invalid",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub trust_roots: PathBuf,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            certificate: "certs/server.pem".into(),
            private_key: "certs/server-key.pem".into(),
            trust_roots: "certs/ca.pem".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_preserve_control_capacity_and_management_isolation() {
        let mut config = Config::default();
        config.validate().unwrap();
        config.memory.control_reserved_bytes = config.memory.total_bytes;
        assert!(config.validate().is_err());
        let config = Config {
            management_listen: "0.0.0.0:9419".parse().unwrap(),
            ..Config::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn unknown_configuration_is_rejected() {
        assert!(toml::from_str::<Config>("unknown_setting = true").is_err());
    }
}
