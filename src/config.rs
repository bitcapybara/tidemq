use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tidemq_protocol::{Id, ProtocolLimits};
use tokio::io::AsyncReadExt;

use crate::error::{ClusterIdentitySnafu, IoSnafu, ParseConfigurationSnafu, ProtocolSnafu};
use crate::{Error, Result};

mod file;
mod process;

pub use process::ProcessConfig;

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
#[serde(try_from = "file::ConfigFile", into = "file::ConfigFile")]
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
    pub tracing: crate::observability::tracing::TracingConfig,
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
            tracing: crate::observability::tracing::TracingConfig::default(),
            tls: TlsConfig::default(),
            runtime: crate::runtime::RuntimeConfig::default(),
        }
    }
}

impl Config {
    pub async fn load(path: &Path) -> Result<Self> {
        let text = read_configuration(path).await?;
        let config: Self = toml::from_str(&text).context(ParseConfigurationSnafu { path })?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.node_id == 0 {
            return Err(Error::Configuration {
                reason: "node_id must be nonzero",
            });
        }
        if self.data_dir.as_os_str().is_empty() {
            return Err(Error::Configuration {
                reason: "data_dir must not be empty",
            });
        }
        self.cluster_id
            .parse::<Id>()
            .context(ClusterIdentitySnafu)?;
        if !matches!(self.replication_factor, 1 | 3) {
            return Err(Error::Configuration {
                reason: "replication_factor must be explicitly 1 or 3",
            });
        }
        if !self.management_listen.ip().is_loopback() {
            return Err(Error::Configuration {
                reason: "management_listen must use a loopback IP; expose remote management through a protected proxy",
            });
        }
        if self.role.serves_gateway() && self.listen == self.management_listen {
            return Err(Error::Configuration {
                reason: "listen must differ from management_listen for a gateway role",
            });
        }
        self.protocol.validate().context(ProtocolSnafu)?;
        self.memory.validate(self.protocol.max_frame_bytes)?;
        self.logging.validate()?;
        self.tracing.validate()?;
        Ok(())
    }
}

async fn read_configuration(path: &Path) -> Result<String> {
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
    Ok(text)
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
        for (valid, reason) in [
            (
                self.control_reserved_bytes >= 65_536,
                "memory.control_reserved_bytes must be at least 65536 bytes",
            ),
            (
                self.control_reserved_bytes <= self.total_bytes,
                "memory.control_reserved_bytes must not exceed memory.total_bytes",
            ),
            (
                self.total_bytes
                    .checked_sub(self.control_reserved_bytes)
                    .is_some_and(|bytes| bytes >= minimum),
                "memory.total_bytes must reserve memory.control_reserved_bytes plus two maximum protocol frames",
            ),
            (
                (1..=4_096).contains(&self.max_connections),
                "memory.max_connections must be between 1 and 4096",
            ),
            (
                (2..=4_096).contains(&self.max_streams_per_connection),
                "memory.max_streams_per_connection must be between 2 and 4096",
            ),
            (
                (1..=65_536).contains(&self.max_pending_requests),
                "memory.max_pending_requests must be between 1 and 65536",
            ),
        ] {
            if !valid {
                return Err(Error::Configuration { reason });
            }
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
        for (valid, reason) in [
            (
                (1..=16_384).contains(&self.queue_events),
                "logging.queue_events must be between 1 and 16384",
            ),
            (
                (256..=65_536).contains(&self.max_event_bytes),
                "logging.max_event_bytes must be between 256 and 65536",
            ),
            (
                self.filter.len() <= 1_024,
                "logging.filter must not exceed 1024 bytes",
            ),
        ] {
            if !valid {
                return Err(Error::Configuration { reason });
            }
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

    #[test]
    fn static_credentials_and_shared_delivery_secrets_are_rejected() {
        for (name, value) in [
            ("access_file", toml::Value::String("access.toml".into())),
            (
                "delivery_key_file",
                toml::Value::String("delivery.key".into()),
            ),
            ("dynamic_auth", toml::Value::Boolean(false)),
        ] {
            let mut document = toml::Value::try_from(Config::default()).unwrap();
            document
                .get_mut("authorization")
                .unwrap()
                .as_table_mut()
                .unwrap()
                .insert(name.into(), value);
            let text = toml::to_string(&document).unwrap();
            assert!(toml::from_str::<Config>(&text).is_err(), "Accepted {name}");
        }
    }

    #[test]
    fn role_sections_round_trip_only_applicable_settings() {
        for role in [
            Role::Combined,
            Role::Gateway,
            Role::Broker,
            Role::Storage,
            Role::Metadata,
        ] {
            let mut original = Config {
                role,
                ..Config::default()
            };
            original.runtime.bootstrap_admin_token_file = Some("private/bootstrap.token".into());
            original.runtime.archive_interval_ms = 2_000;
            if role.stores_data() {
                original.runtime.retention.minimum_age_ms = 1234;
                original.runtime.retention.max_time_boundaries = 512;
                original.runtime.objects = Some(crate::runtime::ObjectConfig {
                    endpoint: "http://127.0.0.1:9000".into(),
                    region: "us-east-1".into(),
                    bucket: "configuration-test".into(),
                    prefix: "round-trip".into(),
                    credentials_file: "private/objects.toml".into(),
                    allow_http_loopback: true,
                });
            }
            let text = toml::to_string(&original).unwrap();
            let document: toml::Table = toml::from_str(&text).unwrap();
            assert!(document.contains_key("authorization"));
            assert_eq!(document.contains_key("gateway"), role.serves_gateway());
            assert_eq!(document.contains_key("storage"), role.stores_data());
            assert!(!document.contains_key("listen"));
            let parsed: Config = toml::from_str(&text).unwrap();
            assert_eq!(parsed.role, role);
            assert_eq!(
                parsed.runtime.bootstrap_admin_token_file,
                original.runtime.bootstrap_admin_token_file
            );
            if role.stores_data() {
                assert_eq!(parsed.runtime.archive_interval_ms, 2_000);
                assert_eq!(parsed.runtime.retention.minimum_age_ms, 1234);
                assert_eq!(parsed.runtime.retention.max_time_boundaries, 512);
                assert_eq!(
                    serde_json::to_value(&parsed.runtime.objects).unwrap(),
                    serde_json::to_value(&original.runtime.objects).unwrap()
                );
            }
            assert_eq!(toml::to_string(&parsed).unwrap(), text);
        }
    }

    #[test]
    fn legacy_configuration_is_rejected() {
        let legacy = r#"
role = 'combined'
listen = '127.0.0.1:7810'
[runtime]
access_file = 'old/access.toml'
delivery_key_file = 'old/delivery.key'
archive_interval_ms = 3000
[runtime.retention]
minimum_age_ms = 1234
max_time_boundaries = 512
"#;
        assert!(toml::from_str::<Config>(legacy).is_err());
        assert!(toml::from_str::<Config>("role = 'broker'").is_err());
        assert!(toml::from_str::<Config>("").is_err());
    }

    #[test]
    fn role_sections_reject_inactive_missing_mixed_and_unknown_settings() {
        let authorization = "[authorization]\nbootstrap_admin_token_file = 'bootstrap.token'\n";
        for text in [
            format!("role = 'broker'\n{authorization}[gateway]\nlisten = '127.0.0.1:7443'"),
            format!("role = 'metadata'\n{authorization}[storage]\n"),
            format!("role = 'gateway'\n{authorization}"),
            format!("role = 'storage'\n{authorization}"),
            format!("role = 'broker'\nlisten = '127.0.0.1:7443'\n{authorization}"),
            format!("role = 'broker'\n{authorization}[runtime]\naccess_file = 'duplicate.toml'"),
            format!("role = 'broker'\n{authorization}[runtime]\narchive_interval_ms = 1000"),
            format!(
                "role = 'gateway'\n{authorization}[gateway]\nlisten = '127.0.0.1:7443'\nunknown = true"
            ),
            format!("role = 'storage'\n{authorization}[storage]\nunknown = true"),
            "role = 'gateway'\n[gateway]\nlisten = '127.0.0.1:7443'".into(),
            format!("{authorization}[gateway]\nlisten = '127.0.0.1:7443'\n[storage]"),
            "role = 'broker'\n[authorization]\naccess_file = ''\ndelivery_key_file = 'key'".into(),
        ] {
            assert!(
                toml::from_str::<Config>(&text).is_err(),
                "Accepted invalid role configuration: {text}"
            );
        }
    }

    #[test]
    fn inactive_gateway_listener_does_not_reserve_an_address() {
        let mut config = Config {
            role: Role::Storage,
            ..Config::default()
        };
        config.runtime.internal_listen = config.listen;
        config.runtime.metadata_voters = vec![2];
        config.runtime.peers = [(1, Role::Storage), (2, Role::Metadata), (3, Role::Broker)]
            .into_iter()
            .map(|(node, role)| crate::runtime::PeerConfig {
                node_id: node,
                role,
                zone: "local".into(),
                internal_address: if node == 1 {
                    config.runtime.internal_listen
                } else {
                    SocketAddr::from(([127, 0, 0, 1], 7500 + node as u16))
                },
                server_name: format!("node-{node}.local"),
                certificate_der: PathBuf::from(format!("certs/node-{node}.der")),
            })
            .collect();
        config.runtime.validate(&config).unwrap();
        config.management_listen = config.listen;
        config.validate().unwrap();
        config.role = Role::Gateway;
        assert!(config.validate().is_err());
    }
}
