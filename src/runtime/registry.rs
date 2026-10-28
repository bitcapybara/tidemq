use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{sync::watch, time::Instant};

use super::{
    Error, Result,
    service::Service,
    wire::{self, Request},
};
use crate::{
    config::Config,
    metadata::{
        DataGroup, NodeIdentity, NodeRecord, NodeRole, NodeSpec, RegisteredNode, RegistryPage,
    },
    transport::{NodeAuthorizations, NodeGrant, PeerTarget},
};

pub(super) const REGISTRY: u32 = 1008;

#[cfg(test)]
#[path = "../../tests/scalable_support/runtime_registry.rs"]
mod tests;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Directory {
    revision: u64,
    membership_index: u64,
    nodes: BTreeMap<u64, RegisteredNode>,
    groups: BTreeMap<u64, DataGroup>,
    metadata_voters: BTreeSet<u64>,
}

pub(crate) struct Registry {
    directory: Mutex<Directory>,
    update: tokio::sync::Mutex<()>,
    pub trust: NodeAuthorizations,
    changes: watch::Sender<()>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedDirectory {
    version: u32,
    cluster: tidemq_protocol::Id,
    node: u64,
    directory: Directory,
    digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
struct PageRequest {
    nodes: Option<u64>,
    groups: Option<u64>,
    revision: u64,
    membership_index: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct Page {
    page: RegistryPage,
    metadata_voters: BTreeSet<u64>,
    membership_index: u64,
}

impl Registry {
    pub fn local(
        config: &Config,
        identity: &NodeGrant,
        address: std::net::SocketAddr,
    ) -> Result<Self> {
        if config.role != crate::config::Role::Combined || identity.node_id != config.node_id {
            return Err(Error::invalid(
                "Local directory requires the process-owned identity",
            ));
        }
        let node = RegisteredNode {
            record: NodeRecord {
                spec: NodeSpec {
                    node_id: config.node_id,
                    zone: "local".into(),
                    endpoint: address,
                    roles: super::node::roles(config.role),
                    schedulable: true,
                },
                generation: 1,
            },
            identity: NodeIdentity {
                certificate_name: identity.certificate_name.clone(),
                certificate_pin: identity.certificate_pin,
                role: config.role.name().into(),
                external_endpoint: None,
                retired: false,
                drain_operation: None,
            },
        };
        let trust = NodeAuthorizations::new(Vec::new()).map_err(|source| {
            Error::operation("initialize empty internal trust registry", source)
        })?;
        Ok(Self {
            directory: Mutex::new(Directory {
                revision: 1,
                membership_index: 0,
                nodes: BTreeMap::from([(config.node_id, node)]),
                groups: config
                    .runtime
                    .groups
                    .iter()
                    .map(|group| {
                        (
                            group.group_id,
                            DataGroup {
                                group_id: group.group_id,
                                generation: 1,
                                placement: crate::metadata::GroupPlacement::Local {
                                    node_id: config.node_id,
                                },
                            },
                        )
                    })
                    .collect(),
                metadata_voters: BTreeSet::new(),
            }),
            update: tokio::sync::Mutex::new(()),
            trust,
            changes: watch::channel(()).0,
        })
    }
    pub async fn load(
        config: &Config,
        cluster: tidemq_protocol::Id,
        grants: Vec<NodeGrant>,
    ) -> Result<Self> {
        let registry = Self::seeds(config, grants)?;
        let path = config.data_dir.join("node-directory.json");
        if tokio::fs::try_exists(&path)
            .await
            .map_err(|source| Error::operation("inspect saved node directory", source))?
        {
            let bytes = super::read_secret(&path, 512 * 1024).await?;
            let saved: SavedDirectory = serde_json::from_slice(&bytes)
                .map_err(|source| Error::operation("decode saved node directory", source))?;
            let encoded = serde_json::to_vec(&saved.directory)
                .map_err(|source| Error::operation("verify saved node directory", source))?;
            if saved.version != 1
                || saved.cluster != cluster
                || saved.node != config.node_id
                || saved.directory.revision == 0
                || saved.digest != *blake3::hash(&encoded).as_bytes()
                || saved.directory.nodes.len() > 128
                || saved.directory.groups.len() > 256
            {
                return Err(Error::invalid(
                    "Saved node directory identity, format or digest is invalid",
                ));
            }
            saved.directory.validate_membership(config)?;
            saved.directory.newer_than(
                &*registry
                    .directory
                    .lock()
                    .map_err(|_| Error::unavailable())?,
            )?;
            let local =
                saved.directory.nodes.get(&config.node_id).ok_or_else(|| {
                    Error::invalid("Saved node directory omitted the local identity")
                })?;
            if local.identity.retired {
                return Err(Error::invalid("Retired node identities cannot restart"));
            }
            let (_, grants) = saved.directory.targets(config, cluster)?;
            registry
                .trust
                .replace(grants)
                .map_err(|source| Error::operation("restore saved node trust", source))?;
            *registry
                .directory
                .lock()
                .map_err(|_| Error::unavailable())? = saved.directory;
        }
        Ok(registry)
    }

    pub fn targets(
        &self,
        config: &Config,
        cluster: tidemq_protocol::Id,
    ) -> Result<Vec<PeerTarget>> {
        self.directory
            .lock()
            .map_err(|_| Error::unavailable())?
            .targets(config, cluster)
            .map(|(targets, _)| targets)
    }

    pub fn seeds(config: &Config, grants: Vec<NodeGrant>) -> Result<Self> {
        let trust = NodeAuthorizations::new(grants.clone())
            .map_err(|source| Error::operation("initialize trusted node registry", source))?;
        let mut nodes = BTreeMap::new();
        for peer in &config.runtime.peers {
            let grant = grants
                .iter()
                .find(|grant| grant.node_id == peer.node_id)
                .ok_or_else(|| Error::invalid("Seed node has no pinned certificate"))?;
            nodes.insert(
                peer.node_id,
                RegisteredNode {
                    record: NodeRecord {
                        spec: NodeSpec {
                            node_id: peer.node_id,
                            zone: peer.zone.clone(),
                            endpoint: peer.internal_address,
                            roles: super::node::roles(peer.role),
                            schedulable: true,
                        },
                        generation: 1,
                    },
                    identity: NodeIdentity {
                        certificate_name: grant.certificate_name.clone(),
                        certificate_pin: grant.certificate_pin,
                        role: peer.role.name().into(),
                        external_endpoint: None,
                        retired: false,
                        drain_operation: None,
                    },
                },
            );
        }
        Ok(Self {
            trust,
            update: tokio::sync::Mutex::new(()),
            directory: Mutex::new(Directory {
                revision: 0,
                membership_index: 0,
                nodes,
                groups: config
                    .runtime
                    .groups
                    .iter()
                    .map(|group| {
                        (
                            group.group_id,
                            DataGroup {
                                group_id: group.group_id,
                                generation: 1,
                                placement: crate::metadata::GroupPlacement::Raft {
                                    voters: group.voters.iter().copied().collect(),
                                },
                            },
                        )
                    })
                    .collect(),
                metadata_voters: config.runtime.metadata_voters.iter().copied().collect(),
            }),
            changes: watch::channel(()).0,
        })
    }

    pub fn nodes(&self) -> Result<Vec<RegisteredNode>> {
        Ok(self
            .directory
            .lock()
            .map_err(|_| Error::unavailable())?
            .nodes
            .values()
            .cloned()
            .collect())
    }

    pub fn contains_group(&self, group: u64) -> bool {
        self.directory
            .lock()
            .is_ok_and(|directory| directory.revision != 0 && directory.groups.contains_key(&group))
    }

    pub fn schedulable(&self, node: u64) -> bool {
        self.directory.lock().is_ok_and(|directory| {
            directory
                .nodes
                .get(&node)
                .is_some_and(|node| !node.identity.retired && node.record.spec.schedulable)
        })
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<()> {
        self.changes.subscribe()
    }

    pub fn diagnostic(&self) -> Result<serde_json::Value> {
        let directory = self.directory.lock().map_err(|_| Error::unavailable())?;
        Ok(
            serde_json::json!({"revision": directory.revision, "membership_index": directory.membership_index,
            "registered_nodes": directory.nodes.len(), "data_groups": directory.groups.len(),
            "metadata_voters": directory.metadata_voters, "scope": "trusted_discovery_only"}),
        )
    }

    pub fn placement_groups(
        &self,
        after: Option<u64>,
        round: usize,
    ) -> Result<BTreeMap<u64, BTreeSet<u64>>> {
        use std::ops::Bound;
        let directory = self.directory.lock().map_err(|_| Error::unavailable())?;
        let storage: Vec<_> = directory
            .nodes
            .iter()
            .filter(|(_, node)| {
                !node.identity.retired
                    && node.record.spec.schedulable
                    && node.record.spec.roles.contains(&NodeRole::Storage)
            })
            .map(|(id, _)| *id)
            .collect();
        let candidates: BTreeSet<_> = storage
            .iter()
            .cycle()
            .skip(round % storage.len().max(1))
            .take(storage.len().min(32))
            .copied()
            .collect();
        Ok(directory
            .groups
            .range((
                after.map_or(Bound::Unbounded, Bound::Excluded),
                Bound::Unbounded,
            ))
            .chain(directory.groups.range((
                Bound::Unbounded,
                after.map_or(Bound::Unbounded, Bound::Included),
            )))
            .take(directory.groups.len().min(16))
            .map(|(id, group)| {
                (
                    *id,
                    candidates
                        .difference(&group.placement.hosts())
                        .copied()
                        .collect(),
                )
            })
            .collect())
    }

    pub fn has_role(&self, node: u64, role: NodeRole) -> bool {
        self.directory
            .lock()
            .ok()
            .and_then(|directory| {
                directory
                    .nodes
                    .get(&node)
                    .map(|node| !node.identity.retired && node.record.spec.roles.contains(&role))
            })
            .unwrap_or(false)
    }

    pub fn candidates(&self, group: u64, metadata: u64) -> Result<Vec<u64>> {
        let directory = self.directory.lock().map_err(|_| Error::unavailable())?;
        if group == metadata {
            let mut nodes: Vec<_> = directory.metadata_voters.iter().copied().collect();
            for (id, node) in &directory.nodes {
                if !node.identity.retired
                    && node.record.spec.roles.contains(&NodeRole::Metadata)
                    && !nodes.contains(id)
                {
                    nodes.push(*id);
                }
            }
            return Ok(nodes);
        }
        let mut nodes: Vec<_> = directory
            .groups
            .get(&group)
            .map(|group| group.placement.hosts().into_iter().collect())
            .ok_or_else(Error::unavailable)?;
        for (id, node) in &directory.nodes {
            if !node.identity.retired
                && node.record.spec.roles.contains(&NodeRole::Storage)
                && !nodes.contains(id)
            {
                nodes.push(*id);
            }
        }
        Ok(nodes)
    }

    pub fn metadata_voters(&self) -> Result<BTreeSet<u64>> {
        Ok(self
            .directory
            .lock()
            .map_err(|_| Error::unavailable())?
            .metadata_voters
            .clone())
    }
}

impl Service {
    pub(super) async fn registry_page(&self, body: &[u8]) -> Result<bytes::Bytes> {
        let request: PageRequest = wire::decode_json(body)?;
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        // Discovery may use a follower; every business capability still requires a fresh quorum proof.
        let page = replica
            .state
            .raft_state()
            .ok_or_else(|| Error::invalid("Local discovery does not expose Raft membership"))?
            .try_with_membership(|state, _, membership| {
                let membership_index = membership.log_id().map_or(0, |log| log.index);
                if membership.membership().get_joint_config().len() != 1
                    || request
                        .membership_index
                        .is_some_and(|expected| expected != membership_index)
                {
                    return Err(Error::unavailable());
                }
                Ok(Page {
                    page: state
                        .registry_page(request.nodes, request.groups, request.revision)
                        .map_err(super::metadata::metadata_rejection)?,
                    membership_index,
                    metadata_voters: membership.membership().voter_ids().collect(),
                })
            })
            .await
            .map_err(|source| Error::operation("read committed node directory", source))??;
        wire::json(&page)
    }

    pub(super) async fn follow_registry(
        self: Arc<Self>,
        mut stopped: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut ticks = tokio::time::interval(Duration::from_secs(1));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = stopped.changed() => return Ok(()), _ = ticks.tick() => {} }
            let deadline = Instant::now() + Duration::from_secs(5);
            tokio::select! {
                _ = stopped.changed() => return Ok(()),
                result = self.refresh_registry(deadline) => {
                    if let Err(error) = result {
                        tracing::debug!(event_code = "registry_refresh_wait", error_code = error.failure().code,
                            "Node discovery will retry its last trusted directory");
                    }
                }
            }
        }
    }

    pub(super) async fn refresh_registry(&self, deadline: Instant) -> Result<()> {
        let mut candidates = self.registry.candidates(
            self.config.runtime.metadata_group,
            self.config.runtime.metadata_group,
        )?;
        if self.metadata.is_some() && !candidates.contains(&self.config.node_id) {
            candidates.push(self.config.node_id);
        }
        for node in candidates {
            if let Ok(directory) = self.fetch_registry(node, deadline).await
                && self.install_registry(directory, deadline).await.is_ok()
            {
                return Ok(());
            }
        }
        Err(Error::unavailable())
    }

    async fn fetch_registry(&self, node: u64, deadline: Instant) -> Result<Directory> {
        let mut request = PageRequest {
            nodes: Some(0),
            groups: Some(0),
            revision: 0,
            membership_index: None,
        };
        let mut directory = Directory {
            revision: 0,
            membership_index: 0,
            nodes: BTreeMap::new(),
            groups: BTreeMap::new(),
            metadata_voters: BTreeSet::new(),
        };
        for _ in 0..32 {
            let request_bytes = wire::json(&request)?;
            let bytes = if node == self.config.node_id {
                self.registry_page(&request_bytes).await?
            } else {
                self.rpc
                    .call(
                        node,
                        self.config.runtime.metadata_group,
                        Request::control(REGISTRY, &request)?,
                        deadline.min(Instant::now() + Duration::from_millis(500)),
                    )
                    .await?
            };
            let reply: Page = wire::decode_json(&bytes)?;
            request = directory.extend(&request, reply, self.cluster)?;
            if request.nodes.is_none() && request.groups.is_none() {
                return Ok(directory);
            }
        }
        Err(Error::invalid("Node directory pagination did not converge"))
    }

    async fn install_registry(&self, directory: Directory, deadline: Instant) -> Result<()> {
        let _update = wire::within(deadline, self.registry.update.lock()).await?;
        let changed = {
            let current = self
                .registry
                .directory
                .lock()
                .map_err(|_| Error::unavailable())?;
            directory.newer_than(&current)?
        };
        if !changed {
            return Ok(());
        }
        directory.validate_membership(&self.config)?;
        let (peers, grants) = directory.targets(&self.config, self.cluster)?;
        self.save_registry(&directory).await?;
        self.rpc
            .pool()?
            .replace_peers(peers, grants.clone(), deadline)
            .await
            .map_err(|source| Error::operation("replace registered internal peers", source))?;
        self.registry
            .trust
            .replace(grants)
            .map_err(|source| Error::operation("replace registered inbound trust", source))?;
        self.authority
            .replace_voters(directory.metadata_voters.clone())?;
        if let Some(service) = &self.scaling.service {
            service
                .replace_metadata_voters(directory.metadata_voters.clone())
                .map_err(|source| Error::operation("replace scaling metadata voters", source))?;
        }
        for coordinator in self.objects.values() {
            coordinator
                .replace_metadata_nodes(directory.metadata_voters.iter().copied().collect())
                .map_err(|source| Error::operation("replace lifecycle metadata voters", source))?;
        }
        *self
            .registry
            .directory
            .lock()
            .map_err(|_| Error::unavailable())? = directory;
        self.registry.changes.send_replace(());
        self.leaders.write().await.clear();
        self.groups.write().await.clear();
        Ok(())
    }

    async fn save_registry(&self, directory: &Directory) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        let encoded = serde_json::to_vec(directory)
            .map_err(|source| Error::operation("encode node directory digest", source))?;
        let saved = SavedDirectory {
            version: 1,
            cluster: self.cluster,
            node: self.config.node_id,
            directory: directory.clone(),
            digest: *blake3::hash(&encoded).as_bytes(),
        };
        let bytes = serde_json::to_vec(&saved)
            .map_err(|source| Error::operation("encode saved node directory", source))?;
        if bytes.len() > 512 * 1024 {
            return Err(Error::invalid(
                "Saved node directory exceeds its byte limit",
            ));
        }
        let temporary = self.config.data_dir.join("node-directory.next");
        let result = async {
            let mut options = tokio::fs::OpenOptions::new();
            options.write(true).create(true).truncate(true);
            #[cfg(unix)]
            options.mode(0o600);
            let mut file = options.open(&temporary).await?;
            file.write_all(&bytes).await?;
            file.sync_all().await?;
            tokio::fs::rename(&temporary, self.config.data_dir.join("node-directory.json")).await?;
            tokio::fs::File::open(&self.config.data_dir)
                .await?
                .sync_all()
                .await
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result.map_err(|source| Error::operation("persist trusted node directory", source))
    }
}

impl Directory {
    fn extend(
        &mut self,
        request: &PageRequest,
        reply: Page,
        cluster: tidemq_protocol::Id,
    ) -> Result<PageRequest> {
        if reply.page.cluster != cluster
            || reply.page.revision == 0
            || reply.page.nodes.len() > 8
            || reply.page.groups.len() > 8
            || request.revision != 0 && reply.page.revision != request.revision
            || request
                .membership_index
                .is_some_and(|expected| expected != reply.membership_index)
            || !matches!(reply.metadata_voters.len(), 1 | 3)
            || reply.metadata_voters.contains(&0)
        {
            return Err(Error::invalid(
                "Node discovery page changed its committed identity",
            ));
        }
        for row in reply.page.nodes {
            let id = row.record.spec.node_id;
            if request.nodes.is_none_or(|after| id <= after)
                || row.record.generation == 0
                || self.nodes.insert(id, row).is_some()
            {
                return Err(Error::invalid("Node discovery page is not ordered"));
            }
        }
        for group in reply.page.groups {
            if request.groups.is_none_or(|after| group.group_id <= after)
                || group.generation == 0
                || self.groups.insert(group.group_id, group).is_some()
            {
                return Err(Error::invalid("Group discovery page is not ordered"));
            }
        }
        self.revision = reply.page.revision;
        self.membership_index = reply.membership_index;
        self.metadata_voters = reply.metadata_voters;
        let next = PageRequest {
            nodes: reply.page.next_nodes,
            groups: reply.page.next_groups,
            revision: self.revision,
            membership_index: Some(self.membership_index),
        };
        if self.nodes.len() > 128 || self.groups.len() > 256 {
            return Err(Error::invalid("Node directory exceeds its finite bound"));
        }
        Ok(next)
    }

    fn newer_than(&self, current: &Self) -> Result<bool> {
        if self.revision < current.revision || self.membership_index < current.membership_index {
            return Err(Error::unavailable());
        }
        if self.revision == current.revision && self.membership_index == current.membership_index {
            return Ok(false);
        }
        for (id, old) in &current.nodes {
            if let Some(new) = self.nodes.get(id)
                && (old.identity.certificate_pin != new.identity.certificate_pin
                    || old.identity.role != new.identity.role
                    || old.identity.certificate_name != new.identity.certificate_name
                    || old.record.spec.endpoint != new.record.spec.endpoint
                    || old.identity.retired && !new.identity.retired
                    || old.identity.drain_operation.is_some()
                        && old.identity.drain_operation != new.identity.drain_operation)
            {
                return Err(Error::invalid(
                    "Committed node directory changed a pinned identity",
                ));
            }
        }
        Ok(true)
    }

    fn validate_membership(&self, config: &Config) -> Result<()> {
        if self.metadata_voters.len() != usize::from(config.replication_factor)
            || self.metadata_voters.contains(&0)
        {
            return Err(Error::invalid(
                "Metadata directory does not match the configured replica count",
            ));
        }
        for node in &self.metadata_voters {
            if self.nodes.get(node).is_none_or(|node| {
                node.identity.retired || !node.record.spec.roles.contains(&NodeRole::Metadata)
            }) {
                return Err(Error::invalid(
                    "Metadata voter is not a registered live identity",
                ));
            }
        }
        for group in self.groups.values() {
            let voters = group.placement.raft_voters().ok_or_else(|| {
                Error::invalid("Separate discovery requires real Raft group membership")
            })?;
            if group.group_id == config.runtime.metadata_group
                || voters.len() != usize::from(config.replication_factor)
                || voters.iter().any(|id| {
                    self.nodes.get(id).is_none_or(|node| {
                        node.identity.retired
                            || !node.record.spec.roles.contains(&NodeRole::Storage)
                    })
                })
            {
                return Err(Error::invalid(
                    "Data group is not backed by registered replicas",
                ));
            }
        }

        Ok(())
    }

    fn targets(
        &self,
        config: &Config,
        cluster: tidemq_protocol::Id,
    ) -> Result<(Vec<PeerTarget>, Vec<NodeGrant>)> {
        let mut grants = Vec::new();
        let mut peers = Vec::new();
        let mut groups = vec![config.runtime.metadata_group];
        groups.extend(self.groups.keys().copied());
        for (id, row) in &self.nodes {
            row.identity
                .validate(&row.record.spec)
                .map_err(super::metadata::metadata_rejection)?;
            if *id != row.record.spec.node_id {
                return Err(Error::invalid("Node directory key is inconsistent"));
            }
            if row.identity.retired {
                continue;
            }
            let role = &row.identity.role;
            grants.push(NodeGrant {
                certificate_pin: row.identity.certificate_pin,
                certificate_name: row.identity.certificate_name.clone(),
                cluster_id: cluster,
                node_id: *id,
                role: role.clone(),
                allowed_groups: groups.clone(),
                allowed_classes: super::node::classes(role),
            });
            peers.push(PeerTarget {
                address: row.record.spec.endpoint,
                server_name: row.identity.certificate_name.clone(),
                local_role: config.role.name().into(),
                remote_role: role.clone(),
                cluster_id: Some(cluster),
                node_id: Some(*id),
            });
        }

        Ok((peers, grants))
    }
}
