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
    #[serde(skip_serializing_if = "Option::is_none")]
    listen: Option<SocketAddr>,
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
    fn validate_layout(&self) -> Result<()> {
        if self.authorization.is_none() && self.gateway.is_none() && self.storage.is_none() {
            return Ok(());
        }
        let role = self.role.ok_or(Error::Configuration {
            reason: "role-scoped configuration requires an explicit role",
        })?;
        if self.listen.is_some() || self.runtime.contains_legacy_settings() {
            return Err(Error::Configuration {
                reason: "role sections cannot be mixed with legacy listener, authorization or storage settings",
            });
        }
        if role.serves_gateway() != self.gateway.is_some() {
            return Err(Error::Configuration {
                reason: "gateway section is required only for gateway or combined roles",
            });
        }
        if role.stores_data() != self.storage.is_some() {
            return Err(Error::Configuration {
                reason: "storage section is required only for storage or combined roles",
            });
        }
        let authorization = self.authorization.as_ref().ok_or(Error::Configuration {
            reason: "role-scoped configuration requires the shared authorization section",
        })?;
        if authorization.access_file.as_os_str().is_empty()
            || authorization.delivery_key_file.as_os_str().is_empty()
        {
            return Err(Error::Configuration {
                reason: "shared authorization file paths must be nonempty",
            });
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthorizationConfig {
    access_file: PathBuf,
    delivery_key_file: PathBuf,
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
    access_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delivery_key_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    objects: Option<ObjectConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retention: Option<crate::retention::Policy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    archive_interval_ms: Option<u64>,
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
            access_file: None,
            delivery_key_file: None,
            objects: None,
            retention: None,
            archive_interval_ms: None,
        }
    }
}

impl RuntimeFile {
    fn contains_legacy_settings(&self) -> bool {
        self.access_file.is_some()
            || self.delivery_key_file.is_some()
            || self.objects.is_some()
            || self.retention.is_some()
            || self.archive_interval_ms.is_some()
    }

    fn into_runtime(self) -> RuntimeConfig {
        let defaults = RuntimeConfig::default();
        RuntimeConfig {
            internal_listen: self.internal_listen,
            metadata_group: self.metadata_group,
            metadata_voters: self.metadata_voters,
            groups: self.groups,
            peers: self.peers,
            bootstrap: self.bootstrap,
            capacity: self.capacity,
            access_file: self.access_file.unwrap_or(defaults.access_file),
            delivery_key_file: self.delivery_key_file.unwrap_or(defaults.delivery_key_file),
            objects: self.objects,
            retention: self.retention.unwrap_or(defaults.retention),
            archive_interval_ms: self
                .archive_interval_ms
                .unwrap_or(defaults.archive_interval_ms),
        }
    }
}

impl TryFrom<ConfigFile> for Config {
    type Error = Error;

    fn try_from(file: ConfigFile) -> Result<Self> {
        file.validate_layout()?;
        let mut runtime = file.runtime.into_runtime();
        if let Some(authorization) = file.authorization {
            runtime.access_file = authorization.access_file;
            runtime.delivery_key_file = authorization.delivery_key_file;
        }
        if let Some(storage) = file.storage {
            runtime.retention = storage.retention;
            runtime.archive_interval_ms = storage.archive_interval_ms;
            runtime.objects = storage.objects;
        }
        Ok(Self {
            node_id: file.node_id,
            cluster_id: file.cluster_id,
            role: file.role.unwrap_or_default(),
            listen: file
                .gateway
                .map(|gateway| gateway.listen)
                .or(file.listen)
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
                access_file: config.runtime.access_file.clone(),
                delivery_key_file: config.runtime.delivery_key_file.clone(),
            }),
            runtime: RuntimeFile::from(config.runtime),
            gateway,
            storage,
            listen: None,
        }
    }
}
