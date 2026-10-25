use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tidemq_protocol::ProtocolLimits;

use super::{Config, LoggingConfig, MemoryConfig, Role, TlsConfig};
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
    replication_factor: u8,
    memory: MemoryConfig,
    protocol: ProtocolLimits,
    logging: LoggingConfig,
    tracing: crate::observability::tracing::TracingConfig,
    tls: TlsConfig,
    runtime: RuntimeFile,
    #[serde(skip_serializing_if = "Option::is_none")]
    authorization: Option<AuthorizationConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gateway: Option<GatewayConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage: Option<StorageConfig>,
}

impl Default for ConfigFile {
    fn default() -> Self {
        let mut file = Self::from(Config::default());
        file.role = None;
        file.authorization = None;
        file.gateway = None;
        file.storage = None;
        file
    }
}

impl ConfigFile {
    fn validate_layout(&self) -> Result<Role> {
        let role = self.role.ok_or(Error::Configuration {
            reason: "role-scoped configuration requires an explicit role",
        })?;
        if role.serves_gateway() && self.gateway.is_none() {
            return Err(Error::Configuration {
                reason: "gateway section is required for gateway and combined roles",
            });
        }
        if !role.serves_gateway() && self.gateway.is_some() {
            return Err(Error::Configuration {
                reason: "gateway section is not supported by broker, storage, or metadata roles",
            });
        }
        if role.stores_data() && self.storage.is_none() {
            return Err(Error::Configuration {
                reason: "storage section is required for storage and combined roles",
            });
        }
        if !role.stores_data() && self.storage.is_some() {
            return Err(Error::Configuration {
                reason: "storage section is not supported by gateway, broker, or metadata roles",
            });
        }
        self.authorization.as_ref().ok_or(Error::Configuration {
            reason: "role-scoped configuration requires the shared authorization section",
        })?;
        Ok(role)
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
#[serde(default, deny_unknown_fields)]
struct RuntimeFile {
    internal_listen: SocketAddr,
    metadata_group: u64,
    metadata_voters: Vec<u64>,
    groups: Vec<GroupConfig>,
    peers: Vec<PeerConfig>,
    bootstrap: bool,
    capacity: crate::quota::Limits,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_scaling: Option<crate::runtime::TopicScalingPolicy>,
}

impl Default for RuntimeFile {
    fn default() -> Self {
        Self::from(RuntimeConfig::default())
    }
}

impl From<RuntimeConfig> for RuntimeFile {
    fn from(runtime: RuntimeConfig) -> Self {
        Self {
            internal_listen: runtime.internal_listen,
            metadata_group: runtime.metadata_group,
            metadata_voters: runtime.metadata_voters,
            groups: runtime.groups,
            peers: runtime.peers,
            bootstrap: runtime.bootstrap,
            capacity: runtime.capacity,
            topic_scaling: runtime.topic_scaling,
        }
    }
}

impl RuntimeFile {
    fn into_runtime(self) -> RuntimeConfig {
        RuntimeConfig {
            internal_listen: self.internal_listen,
            metadata_group: self.metadata_group,
            metadata_voters: self.metadata_voters,
            groups: self.groups,
            peers: self.peers,
            bootstrap: self.bootstrap,
            capacity: self.capacity,
            topic_scaling: self.topic_scaling,
            ..RuntimeConfig::default()
        }
    }
}

impl TryFrom<ConfigFile> for Config {
    type Error = Error;

    fn try_from(file: ConfigFile) -> Result<Self> {
        let role = file.validate_layout()?;
        let mut runtime = file.runtime.into_runtime();
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
            replication_factor: file.replication_factor,
            memory: file.memory,
            protocol: file.protocol,
            logging: file.logging,
            tracing: file.tracing,
            tls: file.tls,
            runtime,
        })
    }
}

impl From<Config> for ConfigFile {
    fn from(config: Config) -> Self {
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
            replication_factor: config.replication_factor,
            memory: config.memory,
            protocol: config.protocol,
            logging: config.logging,
            tracing: config.tracing,
            tls: config.tls,
            authorization: Some(AuthorizationConfig {
                bootstrap_admin_token_file: config.runtime.bootstrap_admin_token_file.clone(),
            }),
            runtime: RuntimeFile::from(config.runtime),
            gateway,
            storage,
        }
    }
}
