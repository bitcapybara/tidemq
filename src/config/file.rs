use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tidemq_protocol::ProtocolLimits;

use super::{Config, LoggingConfig, MemoryConfig, Role, TlsConfig};
use crate::console::EmbeddedConsoleConfig;
use crate::runtime::{GroupConfig, ObjectConfig, PeerConfig, RuntimeConfig};
use crate::{Error, Result};

#[derive(Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct ConfigFile {
    node_id: u64,
    cluster_id: String,
    role: Option<Role>,
    management_listen: SocketAddr,
    data_dir: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    replication_factor: Option<u8>,
    memory: MemoryConfig,
    protocol: ProtocolLimits,
    logging: LoggingConfig,
    tracing: crate::observability::tracing::TracingConfig,
    tls: TlsFile,
    runtime: RuntimeFile,
    #[serde(skip_serializing_if = "Option::is_none")]
    authorization: Option<AuthorizationConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gateway: Option<GatewayConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage: Option<StorageConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    console: Option<EmbeddedConsoleConfig>,
}

impl Default for ConfigFile {
    fn default() -> Self {
        let mut file = Self::from(Config::default());
        file.role = None;
        file.authorization = None;
        file.gateway = None;
        file.storage = None;
        file.console = None;
        file
    }
}

impl ConfigFile {
    fn validate_layout(&self) -> Result<Role> {
        let role = self.role.ok_or(Error::Configuration {
            reason: "role-scoped configuration requires an explicit role",
        })?;
        for (valid, reason) in [
            (
                role.serves_gateway() == self.gateway.is_some(),
                "gateway section is required only for gateway and combined roles",
            ),
            (
                role.stores_data() == self.storage.is_some(),
                "storage section is required only for storage and combined roles",
            ),
            (
                self.authorization.is_some(),
                "role-scoped configuration requires the shared authorization section",
            ),
            (
                (role == Role::Combined) == self.console.is_some(),
                "console section is required only for the combined MQ role",
            ),
        ] {
            if !valid {
                return Err(Error::Configuration { reason });
            }
        }
        if role == Role::Combined {
            if self.replication_factor.is_some() || self.tls.trust_roots.is_some() {
                return Err(Error::Configuration {
                    reason: "combined does not accept replication_factor or tls.trust_roots",
                });
            }
            self.runtime.validate_local()?;
        }
        Ok(role)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct TlsFile {
    certificate: PathBuf,
    private_key: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    trust_roots: Option<PathBuf>,
}

impl Default for TlsFile {
    fn default() -> Self {
        let tls = TlsConfig::default();
        Self {
            certificate: tls.certificate,
            private_key: tls.private_key,
            trust_roots: None,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthorizationConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bootstrap_admin_token_file: Option<PathBuf>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GatewayConfig {
    listen: SocketAddr,
}

#[derive(Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct StorageConfig {
    retention: crate::retention::Policy,
    archive_interval_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    objects: Option<ObjectConfig>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        let runtime = RuntimeConfig::default();
        Self {
            retention: runtime.retention,
            archive_interval_ms: runtime.archive_interval_ms,
            objects: None,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GroupFile {
    group_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    voters: Option<Vec<u64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    standby: Option<Vec<u64>>,
}

#[derive(Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct RuntimeFile {
    #[serde(skip_serializing_if = "Option::is_none")]
    internal_listen: Option<SocketAddr>,
    metadata_group: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_voters: Option<Vec<u64>>,
    groups: Vec<GroupFile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peers: Option<Vec<PeerConfig>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bootstrap: Option<bool>,
    capacity: crate::quota::Limits,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_scaling: Option<crate::runtime::TopicScalingPolicy>,
}

impl Default for RuntimeFile {
    fn default() -> Self {
        Self::from_runtime(RuntimeConfig::default(), true)
    }
}

impl RuntimeFile {
    fn from_runtime(runtime: RuntimeConfig, local: bool) -> Self {
        Self {
            internal_listen: (!local).then_some(runtime.internal_listen),
            metadata_group: runtime.metadata_group,
            metadata_voters: (!local).then_some(runtime.metadata_voters),
            groups: runtime
                .groups
                .into_iter()
                .map(|group| GroupFile {
                    group_id: group.group_id,
                    voters: (!local).then_some(group.voters),
                    standby: (!local).then_some(group.standby),
                })
                .collect(),
            peers: (!local).then_some(runtime.peers),
            bootstrap: (!local).then_some(runtime.bootstrap),
            capacity: runtime.capacity,
            topic_scaling: runtime.topic_scaling,
        }
    }

    fn validate_local(&self) -> Result<()> {
        if self.internal_listen.is_some()
            || self.metadata_voters.is_some()
            || self.peers.is_some()
            || self.bootstrap.is_some()
            || self
                .groups
                .iter()
                .any(|group| group.voters.is_some() || group.standby.is_some())
        {
            return Err(Error::Configuration {
                reason: "combined accepts logical group IDs, capacity and topic scaling only; internal listeners, peers, voters, standby and Raft bootstrap are not supported",
            });
        }
        Ok(())
    }

    fn into_runtime(self, local: bool, node: u64) -> RuntimeConfig {
        let defaults = RuntimeConfig::default();
        RuntimeConfig {
            internal_listen: self.internal_listen.unwrap_or(defaults.internal_listen),
            metadata_group: self.metadata_group,
            // Local ownership is derived; no membership is accepted from the file.
            metadata_voters: self.metadata_voters.unwrap_or_else(|| {
                if local {
                    vec![node]
                } else {
                    defaults.metadata_voters
                }
            }),
            groups: self
                .groups
                .into_iter()
                .map(|group| GroupConfig {
                    group_id: group.group_id,
                    voters: group
                        .voters
                        .unwrap_or_else(|| if local { vec![node] } else { vec![1] }),
                    standby: group.standby.unwrap_or_default(),
                })
                .collect(),
            peers: self.peers.unwrap_or_default(),
            bootstrap: self.bootstrap.unwrap_or(false),
            capacity: self.capacity,
            topic_scaling: self.topic_scaling,
            ..defaults
        }
    }
}

impl TryFrom<ConfigFile> for Config {
    type Error = Error;

    fn try_from(file: ConfigFile) -> Result<Self> {
        let role = file.validate_layout()?;
        let local = role == Role::Combined;
        let mut runtime = file.runtime.into_runtime(local, file.node_id);
        if let Some(authorization) = file.authorization {
            runtime.bootstrap_admin_token_file = authorization.bootstrap_admin_token_file;
        }
        if let Some(storage) = file.storage {
            runtime.retention = storage.retention;
            runtime.archive_interval_ms = storage.archive_interval_ms;
            runtime.objects = storage.objects;
        }
        Ok(Self {
            node_id: file.node_id,
            cluster_id: file.cluster_id,
            role,
            listen: file
                .gateway
                .map(|gateway| gateway.listen)
                .unwrap_or_else(|| Self::default().listen),
            management_listen: file.management_listen,
            data_dir: file.data_dir,
            replication_factor: file.replication_factor.unwrap_or(1),
            memory: file.memory,
            protocol: file.protocol,
            logging: file.logging,
            tracing: file.tracing,
            tls: TlsConfig {
                certificate: file.tls.certificate,
                private_key: file.tls.private_key,
                trust_roots: file.tls.trust_roots.unwrap_or_else(|| {
                    if local {
                        PathBuf::new()
                    } else {
                        TlsConfig::default().trust_roots
                    }
                }),
            },
            console: file.console.unwrap_or_default(),
            runtime,
        })
    }
}

impl From<Config> for ConfigFile {
    fn from(config: Config) -> Self {
        let local = config.role == Role::Combined;
        let gateway = config.role.serves_gateway().then_some(GatewayConfig {
            listen: config.listen,
        });
        let storage = config.role.stores_data().then(|| StorageConfig {
            retention: config.runtime.retention,
            archive_interval_ms: config.runtime.archive_interval_ms,
            objects: config.runtime.objects.clone(),
        });
        Self {
            node_id: config.node_id,
            cluster_id: config.cluster_id,
            role: Some(config.role),
            management_listen: config.management_listen,
            data_dir: config.data_dir,
            replication_factor: (!local).then_some(config.replication_factor),
            memory: config.memory,
            protocol: config.protocol,
            logging: config.logging,
            tracing: config.tracing,
            tls: TlsFile {
                certificate: config.tls.certificate,
                private_key: config.tls.private_key,
                trust_roots: (!local).then_some(config.tls.trust_roots),
            },
            authorization: Some(AuthorizationConfig {
                bootstrap_admin_token_file: config.runtime.bootstrap_admin_token_file.clone(),
            }),
            runtime: RuntimeFile::from_runtime(config.runtime, local),
            gateway,
            storage,
            console: local.then_some(config.console),
        }
    }
}
