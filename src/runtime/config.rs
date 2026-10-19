use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::config::{Config, Role};

use super::{Error, Result};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    pub internal_listen: SocketAddr,
    pub metadata_group: u64,
    pub metadata_voters: Vec<u64>,
    pub groups: Vec<GroupConfig>,
    pub peers: Vec<PeerConfig>,
    pub access_file: PathBuf,
    pub delivery_key_file: PathBuf,
    pub bootstrap: bool,
    pub objects: Option<ObjectConfig>,
    pub capacity: crate::quota::Limits,
    pub retention: crate::retention::Policy,
    pub archive_interval_ms: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            internal_listen: SocketAddr::from(([127, 0, 0, 1], 7444)),
            metadata_group: 1,
            metadata_voters: vec![1],
            groups: vec![GroupConfig {
                group_id: 2,
                voters: vec![1],
            }],
            peers: Vec::new(),
            access_file: "secrets/access.toml".into(),
            delivery_key_file: "secrets/delivery.key".into(),
            bootstrap: false,
            objects: None,
            capacity: crate::quota::Limits::default(),
            retention: crate::retention::Policy::default(),
            archive_interval_ms: 1_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConfig {
    pub group_id: u64,
    pub voters: Vec<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    pub node_id: u64,
    pub role: Role,
    pub zone: String,
    pub internal_address: SocketAddr,
    pub server_name: String,
    pub certificate_der: PathBuf,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectConfig {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub credentials_file: PathBuf,
    pub allow_http_loopback: bool,
}

impl std::fmt::Debug for ObjectConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectConfig")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("credentials_file", &self.credentials_file)
            .finish_non_exhaustive()
    }
}

impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Self::Combined => "combined",
            Self::Gateway => "gateway",
            Self::Broker => "broker",
            Self::Storage => "storage",
            Self::Metadata => "metadata",
        }
    }
    pub fn serves_gateway(self) -> bool {
        matches!(self, Self::Combined | Self::Gateway)
    }
    pub fn serves_broker(self) -> bool {
        matches!(self, Self::Combined | Self::Broker)
    }
    pub fn stores_data(self) -> bool {
        matches!(self, Self::Combined | Self::Storage)
    }
    pub fn stores_metadata(self) -> bool {
        matches!(self, Self::Combined | Self::Metadata)
    }
}

impl RuntimeConfig {
    pub fn validate(&self, config: &Config) -> Result<()> {
        config.validate().map_err(Error::foundation)?;
        self.capacity
            .validate()
            .map_err(|source| Error::operation("validate capacity limits", source))?;
        self.retention
            .validate()
            .map_err(|source| Error::operation("validate retention policy", source))?;
        if !(100..=60_000).contains(&self.archive_interval_ms) {
            return Err(Error::invalid(
                "Archive interval must be between 100 and 60000 milliseconds",
            ));
        }
        if self.peers.is_empty()
            || self.peers.len() > 32
            || self.groups.is_empty()
            || self.groups.len() > 16
            || self.metadata_group == 0
            || self.internal_listen == config.listen
            || self.internal_listen == config.management_listen
        {
            return Err(Error::invalid(
                "Runtime topology or listener bounds are invalid",
            ));
        }
        let mut ids = BTreeSet::new();
        for peer in &self.peers {
            if peer.node_id == 0
                || !ids.insert(peer.node_id)
                || peer.internal_address.port() == 0
                || peer.server_name.is_empty()
                || peer.server_name.len() > 253
                || peer.zone.is_empty()
                || peer.zone.len() > 64
            {
                return Err(Error::invalid(
                    "Runtime peer identity is invalid or duplicated",
                ));
            }
        }
        let local = self.peer(config.node_id)?;
        if local.role != config.role || local.internal_address != self.internal_listen {
            return Err(Error::invalid(
                "Local topology identity differs from process identity",
            ));
        }
        self.validate_voters(&self.metadata_voters, config.replication_factor, true)?;
        let mut groups = BTreeSet::from([self.metadata_group]);
        for group in &self.groups {
            if group.group_id == 0 || !groups.insert(group.group_id) {
                return Err(Error::invalid("Group identity is invalid or duplicated"));
            }
            self.validate_voters(&group.voters, config.replication_factor, false)?;
        }
        if !self.peers.iter().any(|peer| peer.role.serves_broker()) {
            return Err(Error::invalid("At least one configured Broker is required"));
        }
        Ok(())
    }

    fn validate_voters(&self, voters: &[u64], factor: u8, metadata: bool) -> Result<()> {
        let unique: BTreeSet<_> = voters.iter().copied().collect();
        if voters.len() != usize::from(factor) || unique.len() != voters.len() {
            return Err(Error::invalid(
                "Voters must match the explicit replication factor",
            ));
        }
        let mut zones = BTreeSet::new();
        for voter in voters {
            let peer = self.peer(*voter)?;
            if !(if metadata {
                peer.role.stores_metadata()
            } else {
                peer.role.stores_data()
            }) {
                return Err(Error::invalid(
                    "Voter role does not store the selected group",
                ));
            }
            zones.insert(&peer.zone);
        }
        if factor == 3 && zones.len() != 3 {
            return Err(Error::invalid(
                "Three replicas require three distinct configured zones",
            ));
        }
        Ok(())
    }

    pub fn peer(&self, node: u64) -> Result<&PeerConfig> {
        self.peers
            .iter()
            .find(|peer| peer.node_id == node)
            .ok_or_else(|| Error::invalid("Node is absent from trusted topology"))
    }
    pub fn group(&self, group: u64) -> Result<&GroupConfig> {
        self.groups
            .iter()
            .find(|entry| entry.group_id == group)
            .ok_or_else(|| Error::invalid("Data group is absent from trusted topology"))
    }
}
