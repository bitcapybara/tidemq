use std::{collections::BTreeSet, net::SocketAddr};
use tidemq_protocol::{
    common::{FrameKind, TrafficClass},
    public,
};

use crate::{Admin, AdminReceipt, Error, Result, admin::pending::Request};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterRole {
    Gateway,
    Broker,
    Storage,
    Metadata,
    Combined,
}

impl ClusterRole {
    fn name(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::Broker => "broker",
            Self::Storage => "storage",
            Self::Metadata => "metadata",
            Self::Combined => "combined",
        }
    }
    fn parse(value: &str) -> Result<Self> {
        match value {
            "gateway" => Ok(Self::Gateway),
            "broker" => Ok(Self::Broker),
            "storage" => Ok(Self::Storage),
            "metadata" => Ok(Self::Metadata),
            "combined" => Ok(Self::Combined),
            _ => Err(mismatch("node role is not recognized")),
        }
    }
}

/// Operator-supplied identity. The pin is the hash of the exact leaf DER certificate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeRegistration {
    pub node_id: u64,
    pub role: ClusterRole,
    pub zone: String,
    pub internal_endpoint: SocketAddr,
    pub certificate_name: String,
    pub certificate_pin: [u8; 32],
}

impl NodeRegistration {
    pub fn certificate_pin(der: &[u8]) -> [u8; 32] {
        tidemq_transport::NodeGrant::certificate_pin(der)
    }
    fn wire(&self) -> Result<public::ClusterNode> {
        if self.node_id == 0
            || self.zone.is_empty()
            || self.zone.len() > 64
            || self.certificate_name.is_empty()
            || self.certificate_name.len() > 253
            || self.certificate_pin == [0; 32]
            || self.internal_endpoint.port() == 0
            || self.internal_endpoint.ip().is_unspecified()
        {
            return Err(Error::Configuration {
                reason: "node registration requires a bounded explicit identity and endpoint",
            });
        }
        Ok(public::ClusterNode {
            node_id: self.node_id,
            role: self.role.name().into(),
            zone: self.zone.clone(),
            internal_endpoint: self.internal_endpoint.to_string(),
            certificate_name: self.certificate_name.clone(),
            certificate_pin: bytes::Bytes::copy_from_slice(&self.certificate_pin),
            schedulable: true,
            ..Default::default()
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNodeInfo {
    pub registration: NodeRegistration,
    pub schedulable: bool,
    pub retired: bool,
    pub generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataGroupInfo {
    pub group_id: u64,
    pub generation: u64,
    pub voters: Vec<u64>,
}

impl Admin {
    pub async fn register_node(&self, node: NodeRegistration) -> Result<ClusterNodeInfo> {
        self.cluster_node(public::ManageCluster {
            action: public::ClusterAction::RegisterClusterNode.into(),
            registration: Some(node.wire()?),
            ..Default::default()
        })
        .await
    }

    pub async fn set_node_schedulable(
        &self,
        node_id: u64,
        enabled: bool,
    ) -> Result<ClusterNodeInfo> {
        self.cluster_node(public::ManageCluster {
            action: public::ClusterAction::SetNodeSchedulable.into(),
            node_id,
            enabled,
            ..Default::default()
        })
        .await
    }

    async fn cluster_node(&self, request: public::ManageCluster) -> Result<ClusterNodeInfo> {
        match self.execute(Request::Cluster(request)).await? {
            AdminReceipt::NodeConfigured(node) => Ok(node),
            _ => Err(mismatch(
                "cluster node operation returned another receipt kind",
            )),
        }
    }

    pub async fn create_data_group(
        &self,
        group_id: u64,
        voters: impl IntoIterator<Item = u64>,
    ) -> Result<DataGroupInfo> {
        let voters: Vec<_> = voters.into_iter().take(4).collect();
        let unique: BTreeSet<_> = voters.iter().copied().collect();
        if group_id == 0
            || !matches!(voters.len(), 1 | 3)
            || unique.len() != voters.len()
            || unique.contains(&0)
        {
            return Err(Error::Configuration {
                reason: "data group requires one or three distinct nonzero replica identities",
            });
        }
        match self
            .execute(Request::Cluster(public::ManageCluster {
                action: public::ClusterAction::CreateDataGroup.into(),
                group_id,
                voters,
                ..Default::default()
            }))
            .await?
        {
            AdminReceipt::DataGroupCreated(group) => Ok(group),
            _ => Err(mismatch("group creation returned another receipt kind")),
        }
    }

    /// A bounded consistent directory snapshot; it does not grant data-plane authority.
    pub async fn cluster_nodes(&self) -> Result<Vec<ClusterNodeInfo>> {
        let (nodes, _) = self.cluster_directory(true).await?;
        nodes.into_iter().map(node_info).collect()
    }

    pub async fn data_groups(&self) -> Result<Vec<DataGroupInfo>> {
        let (_, groups) = self.cluster_directory(false).await?;
        groups.into_iter().map(group_info).collect()
    }

    async fn cluster_directory(
        &self,
        nodes: bool,
    ) -> Result<(Vec<public::ClusterNode>, Vec<public::ClusterGroup>)> {
        self.client
            .operation(|deadline| async move {
                let mut request = public::GetClusterRegistry {
                    after_node: nodes.then_some(0),
                    after_group: (!nodes).then_some(0),
                    expected_revision: 0,
                };
                let mut result_nodes = Vec::new();
                let mut result_groups = Vec::new();
                for _ in 0..32 {
                    let page: public::ClusterRegistryPage = self
                        .client
                        .request(
                            FrameKind::GetClusterRegistry,
                            FrameKind::ClusterRegistryPage,
                            TrafficClass::Control,
                            tidemq_protocol::Id::new_v7()?,
                            &request,
                            deadline,
                        )
                        .await
                        .map_err(|error| match error {
                            Error::Remote {
                                code: tidemq_protocol::common::ErrorCode::StaleGeneration,
                                ..
                            } => Error::CatalogChanged,
                            error => error,
                        })?;
                    if page.revision == 0
                        || request.expected_revision != 0
                            && page.revision != request.expected_revision
                        || page.nodes.len() > 8
                        || page.groups.len() > 8
                        || page.nodes.iter().any(|node| {
                            request.after_node.is_none_or(|after| node.node_id <= after)
                        })
                        || page.groups.iter().any(|group| {
                            request
                                .after_group
                                .is_none_or(|after| group.group_id <= after)
                        })
                        || page.next_node.is_some_and(|next| {
                            page.nodes.last().is_none_or(|node| node.node_id != next)
                        })
                        || page.next_group.is_some_and(|next| {
                            page.groups
                                .last()
                                .is_none_or(|group| group.group_id != next)
                        })
                    {
                        return Err(mismatch("cluster directory page is inconsistent"));
                    }
                    result_nodes.extend(page.nodes);
                    result_groups.extend(page.groups);
                    if result_nodes.len() > 128 || result_groups.len() > 256 {
                        return Err(mismatch("cluster directory exceeds its finite bound"));
                    }
                    request = public::GetClusterRegistry {
                        after_node: page.next_node,
                        after_group: page.next_group,
                        expected_revision: page.revision,
                    };
                    if request.after_node.is_none() && request.after_group.is_none() {
                        return Ok((result_nodes, result_groups));
                    }
                }
                Err(mismatch("cluster directory pagination did not converge"))
            })
            .await
    }
}

pub(crate) fn receipt(
    request: &public::ManageCluster,
    result: public::ClusterControlResult,
) -> Result<AdminReceipt> {
    if request.action == i32::from(public::ClusterAction::CreateDataGroup) {
        if result.node.is_some() {
            return Err(mismatch("group creation included an unrelated node"));
        }
        let group = group_info(
            result
                .group
                .ok_or_else(|| mismatch("group creation omitted its receipt"))?,
        )?;
        if group.group_id != request.group_id
            || group.voters
                != request
                    .voters
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
        {
            return Err(mismatch(
                "group creation changed its identity or replica set",
            ));
        }
        return Ok(AdminReceipt::DataGroupCreated(group));
    }
    if result.group.is_some() {
        return Err(mismatch("node management included an unrelated group"));
    }
    let node = node_info(
        result
            .node
            .ok_or_else(|| mismatch("node management omitted its receipt"))?,
    )?;
    if let Some(expected) = &request.registration {
        if node.registration
            != node_info(public::ClusterNode {
                generation: node.generation,
                ..expected.clone()
            })?
            .registration
        {
            return Err(mismatch("node registration changed its pinned identity"));
        }
    } else if node.registration.node_id != request.node_id || node.schedulable != request.enabled {
        return Err(mismatch(
            "node scheduling operation changed its target or status",
        ));
    }
    Ok(AdminReceipt::NodeConfigured(node))
}

fn node_info(node: public::ClusterNode) -> Result<ClusterNodeInfo> {
    let registration = NodeRegistration {
        node_id: node.node_id,
        role: ClusterRole::parse(&node.role)?,
        zone: node.zone,
        internal_endpoint: node
            .internal_endpoint
            .parse()
            .map_err(|_| mismatch("node endpoint is invalid"))?,
        certificate_name: node.certificate_name,
        certificate_pin: node
            .certificate_pin
            .as_ref()
            .try_into()
            .map_err(|_| mismatch("node certificate pin is invalid"))?,
    };
    registration
        .wire()
        .map_err(|_| mismatch("node registration is invalid"))?;
    if node.generation == 0 || node.retired && node.schedulable {
        return Err(mismatch("node status is invalid"));
    }
    Ok(ClusterNodeInfo {
        registration,
        schedulable: node.schedulable,
        retired: node.retired,
        generation: node.generation,
    })
}

fn group_info(group: public::ClusterGroup) -> Result<DataGroupInfo> {
    let unique: BTreeSet<_> = group.voters.iter().copied().collect();
    if group.group_id == 0
        || group.generation == 0
        || !matches!(unique.len(), 1 | 3)
        || unique.contains(&0)
        || unique.len() != group.voters.len()
    {
        return Err(mismatch("group directory record is invalid"));
    }
    Ok(DataGroupInfo {
        group_id: group.group_id,
        generation: group.generation,
        voters: unique.into_iter().collect(),
    })
}

fn mismatch(reason: &'static str) -> Error {
    Error::ResponseMismatch {
        reason: reason.into(),
    }
}
