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
    pub bootstrap_admin_token_file: Option<PathBuf>,
    pub bootstrap: bool,
    pub objects: Option<ObjectConfig>,
    pub capacity: crate::quota::Limits,
    pub retention: crate::retention::Policy,
    pub archive_interval_ms: u64,
    pub topic_scaling: Option<super::TopicScalingPolicy>,
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
                standby: Vec::new(),
            }],
            peers: Vec::new(),
            bootstrap_admin_token_file: None,
            bootstrap: false,
            objects: None,
            capacity: crate::quota::Limits::default(),
            retention: crate::retention::Policy::default(),
            archive_interval_ms: 1_000,
            topic_scaling: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConfig {
    pub group_id: u64,
    pub voters: Vec<u64>,
    #[serde(default)]
    pub standby: Vec<u64>,
}

impl GroupConfig {
    pub fn hosts(&self, node: u64) -> bool {
        self.voters.contains(&node) || self.standby.contains(&node)
    }
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
        if let Some(policy) = &self.topic_scaling {
            if !config.role.stores_metadata() {
                return Err(Error::invalid(
                    "Topic scaling policy belongs on metadata or combined nodes",
                ));
            }
            policy.validate()?;
        }
        if !(100..=60_000).contains(&self.archive_interval_ms) {
            return Err(Error::invalid(
                "Archive interval must be between 100 and 60000 milliseconds",
            ));
        }
        self.validate_objects(config)?;
        if config.role == Role::Combined {
            if !self.peers.is_empty()
                || self.bootstrap
                || !self.metadata_voters.is_empty()
                || !config.tls.trust_roots.as_os_str().is_empty()
            {
                return Err(Error::invalid(
                    "Local execution requires derived ownership without peer, bootstrap or trust root settings",
                ));
            }
            return self.validate_local_groups();
        }
        if config.tls.trust_roots.as_os_str().is_empty() {
            return Err(Error::invalid("Separated roles require tls.trust_roots"));
        }
        self.validate_peers(config)?;
        self.validate_voters(&self.metadata_voters, config.replication_factor, true)?;
        self.validate_groups(config.replication_factor)?;
        if !self.peers.iter().any(|peer| peer.role.serves_broker()) {
            return Err(Error::invalid(
                "runtime.peers must include at least one broker or combined role",
            ));
        }
        Ok(())
    }

    fn validate_local_groups(&self) -> Result<()> {
        if self.metadata_group == 0 || !(1..=16).contains(&self.groups.len()) {
            return Err(Error::invalid(
                "Local execution requires a nonzero metadata_group and 1 to 16 data groups",
            ));
        }
        let mut identifiers = BTreeSet::from([self.metadata_group]);
        for group in &self.groups {
            if group.group_id == 0
                || !identifiers.insert(group.group_id)
                || !group.voters.is_empty()
                || !group.standby.is_empty()
            {
                return Err(Error::invalid(
                    "Local data groups require distinct nonzero IDs without Raft membership",
                ));
            }
        }
        Ok(())
    }

    fn validate_objects(&self, config: &Config) -> Result<()> {
        let Some(objects) = &self.objects else {
            return Ok(());
        };
        for (valid, reason) in [
            (
                config.memory.total_bytes >= 201_326_592,
                "memory.total_bytes must be at least 192 MiB when runtime.objects is configured",
            ),
            (
                config.memory.control_reserved_bytes >= 33_554_432,
                "memory.control_reserved_bytes must be at least 32 MiB when runtime.objects is configured",
            ),
            (
                self.capacity.process_memory.high
                    >= u64::from(config.memory.total_bytes).saturating_add(67_108_864),
                "runtime.capacity.process_memory.high must reserve at least 64 MiB beyond memory.total_bytes when runtime.objects is configured",
            ),
            (
                objects.endpoint.len() <= 2_048,
                "runtime.objects.endpoint must not exceed 2048 bytes",
            ),
            (
                !objects.endpoint.contains('@'),
                "runtime.objects.endpoint must not contain user credentials",
            ),
            (
                !objects.endpoint.chars().any(char::is_whitespace),
                "runtime.objects.endpoint must not contain whitespace",
            ),
        ] {
            if !valid {
                return Err(Error::invalid(reason));
            }
        }
        Ok(())
    }

    fn validate_peers(&self, config: &Config) -> Result<()> {
        for (valid, reason) in [
            (
                (1..=32).contains(&self.peers.len()),
                "runtime.peers must contain between 1 and 32 nodes",
            ),
            (
                (1..=16).contains(&self.groups.len()),
                "runtime.groups must contain between 1 and 16 data groups",
            ),
            (
                self.metadata_group != 0,
                "runtime.metadata_group must be nonzero",
            ),
            (
                self.internal_listen.port() != 0,
                "runtime.internal_listen port must be nonzero",
            ),
            (
                !config.role.serves_gateway() || self.internal_listen != config.listen,
                "runtime.internal_listen must differ from listen for a gateway role",
            ),
            (
                self.internal_listen != config.management_listen,
                "runtime.internal_listen must differ from management_listen",
            ),
        ] {
            if !valid {
                return Err(Error::invalid(reason));
            }
        }
        let mut ids = BTreeSet::new();
        for peer in &self.peers {
            if peer.role == Role::Combined {
                return Err(Error::invalid(
                    "Separated peers cannot use the combined role",
                ));
            }
            for (valid, reason) in [
                (peer.node_id != 0, "node_id must be nonzero"),
                (ids.insert(peer.node_id), "node_id is duplicated"),
                (
                    peer.internal_address.port() != 0,
                    "internal_address port must be nonzero",
                ),
                (
                    !peer.internal_address.ip().is_unspecified(),
                    "internal_address must advertise an explicit reachable address",
                ),
                (
                    !peer.server_name.is_empty() && peer.server_name.len() <= 253,
                    "server_name must contain between 1 and 253 bytes",
                ),
                (
                    !peer.zone.is_empty() && peer.zone.len() <= 64,
                    "zone must contain between 1 and 64 bytes",
                ),
            ] {
                if !valid {
                    return Err(Error::invalid(format!(
                        "runtime.peers node {}: {reason}",
                        peer.node_id
                    )));
                }
            }
        }
        let local = self.peer(config.node_id)?;
        if local.role != config.role {
            return Err(Error::invalid(format!(
                "runtime.peers node {} role must match process role {}",
                config.node_id,
                config.role.name()
            )));
        }
        Ok(())
    }

    fn validate_groups(&self, replication_factor: u8) -> Result<()> {
        let mut groups = BTreeSet::from([self.metadata_group]);
        for group in &self.groups {
            if group.group_id == 0 {
                return Err(Error::invalid("runtime.groups.group_id must be nonzero"));
            }
            if !groups.insert(group.group_id) {
                return Err(Error::invalid(format!(
                    "runtime.groups.group_id {} is duplicated or equals runtime.metadata_group",
                    group.group_id
                )));
            }
            self.validate_voters(&group.voters, replication_factor, false)?;
            let mut hosted: BTreeSet<_> = group.voters.iter().copied().collect();
            if group.standby.len() > 16 {
                return Err(Error::invalid(format!(
                    "runtime.groups group {} standby must not exceed 16 nodes",
                    group.group_id
                )));
            }
            for standby in &group.standby {
                if !hosted.insert(*standby) {
                    return Err(Error::invalid(format!(
                        "runtime.groups group {} standby node {standby} duplicates a voter or standby",
                        group.group_id
                    )));
                }
                if !self.peer(*standby)?.role.stores_data() {
                    return Err(Error::invalid(format!(
                        "runtime.groups group {} standby node {standby} must have a storage or combined role",
                        group.group_id
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_voters(&self, voters: &[u64], factor: u8, metadata: bool) -> Result<()> {
        let unique: BTreeSet<_> = voters.iter().copied().collect();
        let group_kind = if metadata { "metadata" } else { "data" };
        if voters.len() != usize::from(factor) {
            return Err(Error::invalid(format!(
                "{group_kind} voters must contain exactly {factor} nodes, received {}",
                voters.len()
            )));
        }
        if unique.len() != voters.len() {
            return Err(Error::invalid(format!(
                "{group_kind} voters must not contain duplicate node IDs"
            )));
        }
        let mut zones = BTreeSet::new();
        for voter in voters {
            let peer = self.peer(*voter)?;
            if !(if metadata {
                peer.role.stores_metadata()
            } else {
                peer.role.stores_data()
            }) {
                return Err(Error::invalid(format!(
                    "{group_kind} voter node {voter} has role {}, which cannot store this group",
                    peer.role.name()
                )));
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
            .ok_or_else(|| Error::invalid(format!("Node {node} is absent from runtime.peers")))
    }
    pub fn group(&self, group: u64) -> Result<&GroupConfig> {
        self.groups
            .iter()
            .find(|entry| entry.group_id == group)
            .ok_or_else(|| {
                Error::invalid(format!("Data group {group} is absent from runtime.groups"))
            })
    }
}
