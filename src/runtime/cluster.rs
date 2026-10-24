use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use tidemq_protocol::{
    Id,
    common::{Frame, FrameKind},
    public,
};
use tokio::time::Instant;

use super::{
    Error, Result,
    service::{Service, forbidden},
    wire::{self, Request},
};
use crate::{
    gateway::Access,
    metadata::{
        self, ClusterAction, ClusterCommand, ClusterOutcome, Command, OperationResult, Permission,
        Response,
    },
};

pub(super) const HOSTED_GROUP: u32 = 1009;

#[derive(Serialize, Deserialize)]
struct HostedGroup {
    group: u64,
    voters: BTreeSet<u64>,
    initialized: bool,
    registered: bool,
}

impl Service {
    pub(super) async fn hosted_group(&self, body: &[u8]) -> Result<bytes::Bytes> {
        let group: u64 = wire::decode_json(body)?;
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        let voters = crate::ownership::remote::current_voters(&replica.raft)?;
        let initialized = replica
            .state
            .try_with_application(|state| {
                state.config().is_some_and(|config| {
                    config.cluster_id == self.cluster && config.group_id == group
                })
            })
            .await
            .map_err(|source| Error::operation("inspect prepared data group", source))?;
        wire::json(&HostedGroup {
            group,
            voters,
            initialized,
            registered: self.registry.contains_group(group),
        })
    }

    pub(super) async fn cluster_list_public(
        &self,
        access: &Access,
        frame: &Frame,
    ) -> Result<Frame> {
        let credential = credential(access)?;
        let request: public::GetClusterRegistry =
            tidemq_protocol::framing::decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode cluster registry query", source))?;
        let now = super::data::unix_millis()?;
        let page = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let principal = state
                    .auth()
                    .authorize(credential, now, Permission::ManageTenants, None)
                    .map_err(|_| forbidden())?;
                if principal.tenant.is_some() {
                    return Err(forbidden());
                }
                state
                    .registry_page(
                        request.after_node,
                        request.after_group,
                        request.expected_revision,
                    )
                    .map_err(super::metadata::metadata_rejection)
            })
            .await
            .map_err(|source| Error::operation("read authorized cluster registry", source))??;
        let reply = public::ClusterRegistryPage {
            revision: page.revision,
            nodes: page.nodes.into_iter().map(node_result).collect(),
            groups: page.groups.into_iter().map(group_result).collect(),
            next_node: page.next_nodes,
            next_group: page.next_groups,
        };
        wire::response(
            frame,
            FrameKind::ClusterRegistryPage,
            &reply,
            &self.config.protocol,
        )
    }

    pub(super) async fn cluster_public(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let actor = credential(access)?;
        let request: public::ManageCluster =
            tidemq_protocol::framing::decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode cluster management", source))?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"tidemq/cluster-request/v1\0");
        hash.update(actor.as_bytes());
        hash.update(&frame.payload);
        let digest = *hash.finalize().as_bytes();
        let operation = wire::id(&frame.operation_id)?;
        let outcome = self
            .cluster_operation(actor, operation, digest, request, deadline)
            .await?;
        self.cluster_ready(&outcome, deadline).await?;
        let result = match outcome {
            ClusterOutcome::Node(node) => public::ClusterControlResult {
                node: Some(node_result(node)),
                group: None,
            },
            ClusterOutcome::Group(group) => public::ClusterControlResult {
                node: None,
                group: Some(group_result(group)),
            },
        };
        wire::response(
            frame,
            FrameKind::ClusterControlResult,
            &result,
            &self.config.protocol,
        )
    }

    async fn cluster_operation(
        &self,
        actor: Id,
        operation: Id,
        digest: [u8; 32],
        request: public::ManageCluster,
        deadline: Instant,
    ) -> Result<ClusterOutcome> {
        let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
        let saved = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.management_result(operation, digest))
            .await
            .map_err(|source| Error::operation("read original cluster management result", source))?
            .map_err(super::metadata::metadata_rejection)?;
        let outcome = match saved {
            OperationResult::Known(Response::Rejected { reason, .. }) => {
                return Err(super::metadata::original_metadata_rejection(reason));
            }
            OperationResult::Known(Response::Applied {
                outcome: metadata::Outcome::Cluster(result),
                ..
            }) => result.outcome,
            OperationResult::Known(_) => {
                return Err(Error::invalid(
                    "Cluster operation identity conflicts with another result",
                ));
            }
            OperationResult::UnknownOrRetired => {
                let action = cluster_action(request)?;
                if let ClusterAction::CreateGroup { group, voters } = &action {
                    self.verify_prepared_group(*group, voters, false, deadline)
                        .await?;
                }
                let result = self
                    .meta_write(
                        Command::cluster(
                            operation,
                            self.meta_revision().await?,
                            ClusterCommand {
                                actor: Some(actor),
                                now_ms: super::data::unix_millis()?,
                                request_digest: digest,
                                action,
                            },
                        ),
                        deadline,
                    )
                    .await?;
                let metadata::Outcome::Cluster(result) = result else {
                    return Err(Error::invalid(
                        "Cluster management returned another result kind",
                    ));
                };
                result.outcome
            }
        };

        Ok(outcome)
    }

    async fn cluster_ready(&self, outcome: &ClusterOutcome, deadline: Instant) -> Result<()> {
        self.refresh_registry(deadline)
            .await
            .map_err(Error::admitted)?;
        if let ClusterOutcome::Group(group) = outcome {
            let advanced = self
                .metadata
                .as_ref()
                .ok_or_else(Error::unavailable)?
                .state
                .try_with_application(|state| {
                    state
                        .group(group.group_id)
                        .is_some_and(|current| current.generation > group.generation)
                })
                .await
                .map_err(|source| {
                    Error::operation("read original group creation progress", source)
                })?;
            if advanced {
                return Ok(());
            }
            loop {
                match self
                    .verify_prepared_group(group.group_id, &group.voters, true, deadline)
                    .await
                {
                    Ok(()) => break,
                    Err(error) if Instant::now() >= deadline => return Err(error.admitted()),
                    Err(_) => {
                        tokio::time::sleep_until(
                            deadline.min(Instant::now() + std::time::Duration::from_millis(50)),
                        )
                        .await
                    }
                }
            }
        }

        Ok(())
    }

    async fn verify_prepared_group(
        &self,
        group: u64,
        voters: &BTreeSet<u64>,
        registered: bool,
        deadline: Instant,
    ) -> Result<()> {
        if group == 0
            || group == self.config.runtime.metadata_group
            || voters.len() != usize::from(self.config.replication_factor)
        {
            return Err(Error::invalid(
                "Data group requires an independent identity and the configured replica count",
            ));
        }
        for node in voters {
            if !self.registry.has_role(*node, metadata::NodeRole::Storage) {
                return Err(Error::unavailable());
            }
            let request = Request::control(HOSTED_GROUP, &group)?;
            let bytes = self
                .rpc
                .call(
                    *node,
                    self.config.runtime.metadata_group,
                    request,
                    deadline.min(Instant::now() + std::time::Duration::from_millis(500)),
                )
                .await?;
            let reply: HostedGroup = wire::decode_json(&bytes)?;
            if reply.group != group
                || reply.voters != *voters
                || !reply.initialized
                || registered && !reply.registered
            {
                return Err(Error::unavailable());
            }
        }
        Ok(())
    }
}

fn credential(access: &Access) -> Result<Id> {
    access
        .principal
        .strip_prefix("credential:")
        .ok_or_else(forbidden)?
        .parse()
        .map_err(|_| forbidden())
}

fn cluster_action(request: public::ManageCluster) -> Result<ClusterAction> {
    use public::ClusterAction as Action;
    match Action::try_from(request.action)
        .map_err(|_| Error::invalid("Cluster action is invalid"))?
    {
        Action::RegisterClusterNode => {
            let node = request
                .registration
                .ok_or_else(|| Error::invalid("Node registration is required"))?;
            if node.retired || node.generation != 0 {
                return Err(Error::invalid(
                    "New nodes cannot provide retirement or generation state",
                ));
            }
            let identity = metadata::NodeIdentity {
                certificate_name: node.certificate_name,
                certificate_pin: node
                    .certificate_pin
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::invalid("Certificate pin must contain 32 bytes"))?,
                role: node.role,
                retired: false,
                external_endpoint: None,
            };
            let roles = identity
                .roles()
                .map_err(super::metadata::metadata_rejection)?;
            Ok(ClusterAction::RegisterNode {
                spec: metadata::NodeSpec {
                    node_id: node.node_id,
                    zone: node.zone,
                    endpoint: node.internal_endpoint.parse().map_err(|_| {
                        Error::invalid("Node endpoint must be an explicit socket address")
                    })?,
                    roles,
                    schedulable: node.schedulable,
                },
                identity,
            })
        }
        Action::SetNodeSchedulable => Ok(ClusterAction::SetSchedulable {
            node: request.node_id,
            enabled: request.enabled,
        }),
        Action::CreateDataGroup => {
            let voters: BTreeSet<_> = request.voters.iter().copied().collect();
            if voters.len() != request.voters.len() {
                return Err(Error::invalid("Data group contains repeated replicas"));
            }
            Ok(ClusterAction::CreateGroup {
                group: request.group_id,
                voters,
            })
        }
        Action::Unspecified => Err(Error::invalid("Cluster action is required")),
    }
}

fn node_result(node: metadata::RegisteredNode) -> public::ClusterNode {
    public::ClusterNode {
        node_id: node.record.spec.node_id,
        role: node.identity.role,
        zone: node.record.spec.zone,
        internal_endpoint: node.record.spec.endpoint.to_string(),
        certificate_name: node.identity.certificate_name,
        certificate_pin: bytes::Bytes::copy_from_slice(&node.identity.certificate_pin),
        schedulable: node.record.spec.schedulable,
        retired: node.identity.retired,
        generation: node.record.generation,
    }
}
fn group_result(group: metadata::DataGroup) -> public::ClusterGroup {
    public::ClusterGroup {
        group_id: group.group_id,
        generation: group.generation,
        voters: group.voters.into_iter().collect(),
    }
}
