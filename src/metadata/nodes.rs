use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};
use tidemq_protocol::Id;

use super::{
    AuthRejection, DataGroup, MetadataState, NodeRecord, NodeRole, NodeSpec, Outcome, Permission,
    Rejection,
};

const PAGE_SIZE: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub certificate_name: String,
    pub certificate_pin: [u8; 32],
    pub role: String,
    pub external_endpoint: Option<SocketAddr>,
    pub retired: bool,
    pub drain_operation: Option<Id>,
}

impl NodeIdentity {
    pub fn roles(&self) -> Result<BTreeSet<NodeRole>, Rejection> {
        Ok(match self.role.as_str() {
            "gateway" => [NodeRole::Gateway].into(),
            "broker" => [NodeRole::Broker].into(),
            "storage" => [NodeRole::Storage].into(),
            "metadata" => [NodeRole::Metadata].into(),
            "combined" => [
                NodeRole::Gateway,
                NodeRole::Broker,
                NodeRole::Storage,
                NodeRole::Metadata,
            ]
            .into(),
            _ => return Err(Rejection::InvalidNode),
        })
    }

    pub(crate) fn validate(
        &self,
        spec: &NodeSpec,
        mode: super::DeploymentMode,
    ) -> Result<(), Rejection> {
        spec.validate(mode)?;
        let local_combined = mode == super::DeploymentMode::Local && self.role == "combined";
        let name = self
            .certificate_name
            .strip_prefix('[')
            .and_then(|name| name.strip_suffix(']'))
            .unwrap_or(&self.certificate_name);
        let valid_name = self
            .certificate_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
            || local_combined && name.parse::<std::net::Ipv6Addr>().is_ok();
        if self.roles()? != spec.roles
            || self.certificate_name.is_empty()
            || self.certificate_name.len() > 253
            || !valid_name
            || self.certificate_pin == [0; 32]
            || (self.retired || self.drain_operation.is_some()) && spec.schedulable
            || self
                .drain_operation
                .is_some_and(|operation| !super::control::is_random_operation_id(operation))
            || self.external_endpoint.is_some_and(|address| {
                !spec.roles.contains(&NodeRole::Gateway)
                    || address.port() == 0
                    || address.ip().is_unspecified() && !local_combined
            })
        {
            return Err(Rejection::InvalidNode);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RegisteredNode {
    pub record: NodeRecord,
    pub identity: NodeIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RegistryPage {
    pub cluster: Id,
    pub revision: u64,
    pub nodes: Vec<RegisteredNode>,
    pub groups: Vec<DataGroup>,
    pub next_nodes: Option<u64>,
    pub next_groups: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClusterAction {
    TransferShard {
        operation: Id,
        shard: Id,
        target_group: u64,
    },
    ReplaceMetadata(super::MetadataChange),
    ReplaceData(crate::scaling::MembershipPlan),
    BeginDrain {
        node: u64,
        operation: Id,
    },
    RegisterNode {
        spec: NodeSpec,
        identity: NodeIdentity,
    },
    SetSchedulable {
        node: u64,
        enabled: bool,
    },
    CreateGroup {
        group: u64,
        voters: BTreeSet<u64>,
    },
    RetireNode {
        node: u64,
        metadata_voters: BTreeSet<u64>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterCommand {
    pub actor: Option<Id>,
    pub now_ms: u64,
    pub request_digest: [u8; 32],
    pub action: ClusterAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClusterOutcome {
    AuthorityTransfer(Id),
    MetadataChange(Id),
    DataChange(crate::scaling::MembershipPlan),
    Node(RegisteredNode),
    Group(DataGroup),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterResult {
    pub request_digest: [u8; 32],
    pub outcome: ClusterOutcome,
}

impl MetadataState {
    pub(crate) fn placement_group(&self, exclude: Option<u64>) -> Option<u64> {
        self.groups
            .values()
            .filter(|group| {
                Some(group.group_id) != exclude
                    && !self
                        .pending_data_replacements()
                        .any(|plan| plan.group_id() == group.group_id)
                    && !self
                        .membership_change(group.group_id)
                        .is_some_and(|change| !change.is_complete())
                    && group.placement.hosts().iter().all(|node| {
                        self.nodes.get(node).is_some_and(|node| {
                            node.spec.schedulable && node.spec.roles.contains(&NodeRole::Storage)
                        })
                    })
            })
            .min_by_key(|group| {
                (
                    self.partitions
                        .values()
                        .filter(|partition| partition.group_id == group.group_id)
                        .count(),
                    group.group_id,
                )
            })
            .map(|group| group.group_id)
    }

    pub fn node_identity(&self, node: u64) -> Option<&NodeIdentity> {
        self.node_identities.get(&node)
    }

    pub fn registry_revision(&self) -> u64 {
        self.registry_revision
    }

    pub fn registry_page(
        &self,
        nodes: Option<u64>,
        groups: Option<u64>,
        expected: u64,
    ) -> Result<RegistryPage, Rejection> {
        if !self.auth.initialized() {
            return Err(Rejection::Uninitialized);
        }
        if expected != 0 && expected != self.registry_revision {
            return Err(Rejection::Superseded);
        }
        let mut page = RegistryPage {
            cluster: self.cluster_id.ok_or(Rejection::Uninitialized)?,
            revision: self.registry_revision,
            nodes: Vec::new(),
            groups: Vec::new(),
            next_nodes: None,
            next_groups: None,
        };
        if let Some(after) = nodes {
            page.nodes = self
                .node_identities
                .keys()
                .filter(|node| **node > after)
                .take(PAGE_SIZE + 1)
                .map(|node| self.registered_node(*node))
                .collect::<Result<_, _>>()?;
            if page.nodes.len() > PAGE_SIZE {
                page.nodes.pop();
                page.next_nodes = page.nodes.last().map(|node| node.record.spec.node_id);
            }
        }
        if let Some(after) = groups {
            page.groups = self
                .groups
                .values()
                .filter(|group| group.group_id > after)
                .take(PAGE_SIZE + 1)
                .cloned()
                .collect();
            if page.groups.len() > PAGE_SIZE {
                page.groups.pop();
                page.next_groups = page.groups.last().map(|group| group.group_id);
            }
        }
        Ok(page)
    }

    pub(super) fn cluster_command(
        &mut self,
        command: &ClusterCommand,
    ) -> Result<ClusterResult, Rejection> {
        if let Some(actor) = command.actor {
            let actor = self
                .auth
                .authorize(actor, command.now_ms, Permission::ManageTenants, None)
                .map_err(Rejection::Authorization)?;
            if actor.tenant.is_some() {
                return Err(Rejection::Authorization(AuthRejection::Forbidden));
            }
        } else if self.auth.initialized()
            || !matches!(command.action, ClusterAction::RegisterNode { .. })
        {
            return Err(Rejection::Authorization(AuthRejection::Forbidden));
        }
        if self.mode == Some(super::DeploymentMode::Local)
            && matches!(
                command.action,
                ClusterAction::TransferShard { .. }
                    | ClusterAction::ReplaceMetadata(_)
                    | ClusterAction::ReplaceData(_)
                    | ClusterAction::BeginDrain { .. }
                    | ClusterAction::CreateGroup { .. }
                    | ClusterAction::RetireNode { .. }
            )
        {
            return Err(Rejection::Unsupported);
        }
        let outcome = match &command.action {
            ClusterAction::TransferShard {
                operation,
                shard,
                target_group,
            } => {
                let birth = self.topology.shard(*shard).ok_or(Rejection::NotFound)?;
                let layout_revision = self
                    .topology
                    .layout(birth.partition.topic_id)
                    .ok_or(Rejection::NotFound)?
                    .revision;
                let target_owner = self
                    .route(birth.partition)
                    .ok_or(Rejection::NotFound)?
                    .owner_node;
                let revision = self
                    .revision
                    .checked_add(1)
                    .ok_or(Rejection::CounterExhausted)?;
                self.transfer_command(
                    *operation,
                    &super::TransferCommand::Prepare {
                        shard: *shard,
                        layout_revision,
                        target_group: *target_group,
                        target_owner,
                    },
                    revision,
                )?;
                ClusterOutcome::AuthorityTransfer(*operation)
            }
            ClusterAction::BeginDrain { node, operation } => {
                if !super::control::is_random_operation_id(*operation) {
                    return Err(Rejection::InvalidEvidence);
                }
                let current = self.registered_node(*node)?;
                if current.identity.retired {
                    return Err(Rejection::IdentityRetired);
                }
                if current.identity.drain_operation.is_none() {
                    let mut spec = current.record.spec;
                    spec.schedulable = false;
                    self.put_node(&spec, current.record.generation)?;
                    self.node_identities
                        .get_mut(node)
                        .ok_or(Rejection::NotFound)?
                        .drain_operation = Some(*operation);
                }
                ClusterOutcome::Node(self.registered_node(*node)?)
            }
            ClusterAction::ReplaceData(plan) => {
                if self.data_replacement_complete(plan) {
                    if !self
                        .membership_change(plan.group_id())
                        .is_some_and(|change| change.plan == *plan && change.is_complete())
                    {
                        return Err(Rejection::Superseded);
                    }
                } else {
                    self.validate_data_replacement(plan)?;
                }
                ClusterOutcome::DataChange(plan.clone())
            }
            ClusterAction::ReplaceMetadata(change) => {
                ClusterOutcome::MetadataChange(self.prepare_metadata_change(change.clone())?)
            }
            ClusterAction::RegisterNode { spec, identity } => self.register_node(spec, identity)?,
            ClusterAction::SetSchedulable { node, enabled } => {
                let current = self.registered_node(*node)?;
                if *enabled && current.identity.drain_operation.is_some() {
                    return Err(Rejection::NodeInUse);
                }
                let mut spec = current.record.spec.clone();
                spec.schedulable = *enabled;
                self.put_node(&spec, current.record.generation)?;
                ClusterOutcome::Node(self.registered_node(*node)?)
            }
            ClusterAction::CreateGroup { group, voters } => {
                if voters.iter().any(|node| {
                    self.node_identities
                        .get(node)
                        .is_none_or(|identity| identity.retired)
                }) {
                    return Err(Rejection::InvalidNode);
                }
                let Outcome::GroupCreated(group) = self.create_group(
                    *group,
                    &super::GroupPlacement::Raft {
                        voters: voters.clone(),
                    },
                )?
                else {
                    return Err(Rejection::InvalidGroup);
                };
                ClusterOutcome::Group(group)
            }
            ClusterAction::RetireNode {
                node,
                metadata_voters,
            } => self.retire_node(*node, metadata_voters)?,
        };
        Ok(ClusterResult {
            request_digest: command.request_digest,
            outcome,
        })
    }

    fn registered_node(&self, node: u64) -> Result<RegisteredNode, Rejection> {
        Ok(RegisteredNode {
            record: self.nodes.get(&node).ok_or(Rejection::NotFound)?.clone(),
            identity: self
                .node_identities
                .get(&node)
                .ok_or(Rejection::NotFound)?
                .clone(),
        })
    }

    fn register_node(
        &mut self,
        spec: &NodeSpec,
        identity: &NodeIdentity,
    ) -> Result<ClusterOutcome, Rejection> {
        identity.validate(spec, self.mode.ok_or(Rejection::Uninitialized)?)?;
        if identity.retired || identity.drain_operation.is_some() {
            return Err(Rejection::IdentityRetired);
        }
        if let Some(previous) = self.node_identities.get(&spec.node_id) {
            if previous.retired {
                return Err(Rejection::IdentityRetired);
            }
            let row = self
                .nodes
                .get(&spec.node_id)
                .ok_or(Rejection::InvalidNode)?;
            if previous != identity
                || row.spec.endpoint != spec.endpoint
                || row.spec.zone != spec.zone
                || row.spec.roles != spec.roles
            {
                return Err(Rejection::OperationConflict);
            }
            return Ok(ClusterOutcome::Node(self.registered_node(spec.node_id)?));
        }
        if self.node_identities.len() >= 128 {
            return Err(Rejection::Capacity);
        }
        if self.node_identities.iter().any(|(node, record)| {
            record.certificate_pin == identity.certificate_pin
                || self
                    .nodes
                    .get(node)
                    .is_some_and(|row| row.spec.endpoint == spec.endpoint)
        }) {
            return Err(Rejection::InvalidNode);
        }
        let previous = self.nodes.get(&spec.node_id);
        if previous.is_some_and(|row| row.spec != *spec) {
            return Err(Rejection::OperationConflict);
        }
        if previous.is_none() {
            self.put_node(spec, 0)?;
        }
        self.node_identities.insert(spec.node_id, identity.clone());
        Ok(ClusterOutcome::Node(self.registered_node(spec.node_id)?))
    }

    fn retire_node(
        &mut self,
        node: u64,
        metadata_voters: &BTreeSet<u64>,
    ) -> Result<ClusterOutcome, Rejection> {
        let record = self.registered_node(node)?;
        if record.identity.retired {
            return Ok(ClusterOutcome::Node(record));
        }
        if metadata_voters.len() > 6
            || metadata_voters.iter().any(|voter| {
                self.nodes
                    .get(voter)
                    .is_none_or(|row| !row.spec.roles.contains(&NodeRole::Metadata))
            })
        {
            return Err(Rejection::InvalidEvidence);
        }
        if self
            .pending_data_replacements()
            .any(|plan| plan.previous().contains(&node) || plan.target().contains(&node))
            || record.record.spec.schedulable
            || metadata_voters.is_empty()
            || metadata_voters.contains(&node)
            || self
                .groups
                .values()
                .any(|group| group.placement.hosts().contains(&node))
            || self.partitions.values().any(|partition| {
                partition
                    .grant
                    .as_ref()
                    .is_some_and(|grant| grant.owner_node == node)
            })
            || self.migrations.values().any(|plan| {
                !plan.is_complete()
                    && (plan.plan.previous.contains(&node) || plan.plan.target.contains(&node))
            })
            || self.metadata_changes.values().any(|change| {
                !change.complete()
                    && (change.previous.contains(&node) || change.target.contains(&node))
            })
        {
            return Err(Rejection::NodeInUse);
        }
        self.node_identities
            .get_mut(&node)
            .ok_or(Rejection::NotFound)?
            .retired = true;
        Ok(ClusterOutcome::Node(self.registered_node(node)?))
    }

    pub(super) fn validate_node_registry(&self) -> Result<(), Rejection> {
        if self.node_identities.len() > 128
            || self.registry_revision == 0
            || self.registry_revision > self.revision
        {
            return Err(Rejection::InvalidNode);
        }
        let mut pins = BTreeMap::new();
        for (node, identity) in &self.node_identities {
            let record = self.nodes.get(node).ok_or(Rejection::InvalidNode)?;
            identity.validate(&record.spec, self.mode.ok_or(Rejection::Uninitialized)?)?;
            if pins.insert(identity.certificate_pin, *node).is_some()
                || identity.retired
                    && self
                        .groups
                        .values()
                        .any(|group| group.placement.hosts().contains(node))
            {
                return Err(Rejection::InvalidNode);
            }
        }
        Ok(())
    }
}
