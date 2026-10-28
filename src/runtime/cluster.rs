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
    pub(super) fn require_cluster(&self) -> Result<()> {
        if self.rpc.is_local() {
            return Err(unsupported());
        }
        Ok(())
    }

    pub(super) async fn hosted_group(&self, body: &[u8]) -> Result<bytes::Bytes> {
        self.require_cluster()?;
        let group: u64 = wire::decode_json(body)?;
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        let placement = replica.current_placement()?;
        let voters = placement.raft_voters().cloned().ok_or_else(unsupported)?;
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
        self.require_cluster()?;
        if frame_kind_action(frame, &self.config.protocol)?
            == public::ClusterAction::InspectAuthorityTransfer
        {
            return self.authority_status_public(access, frame, deadline).await;
        }
        if frame_kind_action(frame, &self.config.protocol)?
            == public::ClusterAction::InspectNodeDrain
        {
            return self.node_drain_public(access, frame, deadline).await;
        }
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
        let result = self.cluster_result(outcome, operation).await?;
        wire::response(
            frame,
            FrameKind::ClusterControlResult,
            &result,
            &self.config.protocol,
        )
    }

    async fn cluster_result(
        &self,
        outcome: ClusterOutcome,
        operation: Id,
    ) -> Result<public::ClusterControlResult> {
        let result = match outcome {
            ClusterOutcome::AuthorityTransfer(id) => self.authority_result(id).await?,
            ClusterOutcome::Node(node) => public::ClusterControlResult {
                node: Some(node_result(node)),
                ..Default::default()
            },
            ClusterOutcome::Group(group) => public::ClusterControlResult {
                group: Some(group_result(group)),
                ..Default::default()
            },
            ClusterOutcome::DataChange(plan) => {
                let complete = self
                    .metadata
                    .as_ref()
                    .ok_or_else(Error::unavailable)?
                    .state
                    .try_with_application(|state| state.data_replacement_complete(&plan))
                    .await
                    .map_err(|source| {
                        Error::operation("read original data replacement progress", source)
                    })?;
                public::ClusterControlResult {
                    data_change: Some(public::DataMembershipChange {
                        operation_id: bytes::Bytes::copy_from_slice(operation.as_bytes()),
                        group_id: plan.group_id(),
                        expected_generation: plan.generation(),
                        previous: plan.previous().iter().copied().collect(),
                        target: plan.target().iter().copied().collect(),
                        complete,
                    }),
                    ..Default::default()
                }
            }
            ClusterOutcome::MetadataChange(id) => {
                let change = self
                    .metadata
                    .as_ref()
                    .ok_or_else(Error::unavailable)?
                    .state
                    .try_with_application(|state| state.metadata_change(id).cloned())
                    .await
                    .map_err(|source| {
                        Error::operation("read metadata replacement progress", source)
                    })?
                    .ok_or_else(Error::unavailable)?;
                public::ClusterControlResult {
                    metadata_change: Some(public::MetadataMembershipChange {
                        operation_id: bytes::Bytes::copy_from_slice(id.as_bytes()),
                        previous: change.previous.into_iter().collect(),
                        target: change.target.into_iter().collect(),
                        expected_membership_index: change.expected_membership_index,
                        completed_membership_index: change.completed_membership_index,
                    }),
                    ..Default::default()
                }
            }
        };
        Ok(result)
    }

    async fn node_drain_public(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let actor = credential(access)?;
        let request: public::ManageCluster =
            tidemq_protocol::framing::decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode node drain query", source))?;
        let now = super::data::unix_millis()?;
        let retired = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let principal = state
                    .auth()
                    .authorize(actor, now, Permission::ManageTenants, None)
                    .map_err(|_| forbidden())?;
                if principal.tenant.is_some() {
                    return Err(forbidden());
                }
                state
                    .node_identity(request.node_id)
                    .map(|identity| identity.retired)
                    .ok_or_else(Error::unavailable)
            })
            .await
            .map_err(|source| Error::operation("authorize node drain query", source))??;
        let progress = self.inspect_node_drain(request.node_id, deadline).await?;
        wire::response(
            frame,
            FrameKind::ClusterControlResult,
            &public::ClusterControlResult {
                drain_progress: Some(public::NodeDrainProgress {
                    node_id: progress.node_id,
                    owned_shards: u32::try_from(progress.owned_partitions)
                        .map_err(|_| Error::unavailable())?,
                    storage_groups: u32::try_from(progress.storage_groups)
                        .map_err(|_| Error::unavailable())?,
                    pending_membership_changes: u32::try_from(progress.pending_membership_changes)
                        .map_err(|_| Error::unavailable())?,
                    metadata_voter: progress.metadata_role,
                    retired,
                }),
                ..Default::default()
            },
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
                let action = if request.action
                    == i32::from(public::ClusterAction::ReplaceMetadataReplica)
                {
                    self.metadata_membership_action(
                        operation,
                        request.removed_node,
                        request.added_node,
                        deadline,
                    )
                    .await?
                } else if request.action == i32::from(public::ClusterAction::ReplaceDataReplica) {
                    let coordinator = crate::scaling::remote::Coordinator::new(
                        self.scaling.rpc.clone(),
                        self.config.runtime.metadata_group,
                    )
                    .map_err(|source| {
                        Error::operation("create operator replica coordinator", source)
                    })?;
                    let plan = coordinator
                        .plan(
                            self.cluster,
                            request.group_id,
                            request.removed_node,
                            request.added_node,
                            deadline,
                        )
                        .await
                        .map_err(|source| {
                            Error::operation("select original data replica replacement", source)
                        })?;
                    self.verify_replacement_target(&plan, request.added_node, deadline)
                        .await?;
                    ClusterAction::ReplaceData(plan)
                } else if request.action == i32::from(public::ClusterAction::RetireClusterNode) {
                    self.retirement_action(request.node_id, deadline).await?
                } else {
                    cluster_action(request, operation)?
                };
                if let ClusterAction::CreateGroup { group, voters } = &action {
                    self.verify_prepared_group(*group, voters, false, deadline)
                        .await?;
                }
                let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
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
        if let ClusterOutcome::AuthorityTransfer(id) = outcome {
            self.resume_authority_transfer(*id, deadline)
                .await
                .map_err(Error::admitted)?;
        }
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
                    .verify_prepared_group(
                        group.group_id,
                        group.placement.raft_voters().ok_or_else(unsupported)?,
                        true,
                        deadline,
                    )
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

    async fn verify_replacement_target(
        &self,
        plan: &crate::scaling::MembershipPlan,
        added: u64,
        deadline: Instant,
    ) -> Result<()> {
        let complete = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.data_replacement_complete(plan))
            .await
            .map_err(|source| Error::operation("read data replacement admission state", source))?;
        if complete {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let reply = crate::scaling::remote::Rpc::call(
            self.scaling.rpc.as_ref(),
            added,
            crate::scaling::remote::Query::Capacity {
                group: plan.group_id(),
                node: added,
            },
            deadline.min(Instant::now() + std::time::Duration::from_secs(1)),
        )
        .await
        .map_err(|source| Error::operation("verify prepared replacement replica", source))?;
        let crate::scaling::remote::Reply::Capacity(capacity) = reply else {
            return Err(Error::unavailable());
        };
        capacity
            .measured(added, started.elapsed())
            .map_err(|source| Error::operation("verify fresh replacement capacity", source))?;
        Ok(())
    }

    pub(super) async fn verify_prepared_group(
        &self,
        group: u64,
        voters: &BTreeSet<u64>,
        registered: bool,
        deadline: Instant,
    ) -> Result<()> {
        self.require_cluster()?;
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

pub(super) fn unsupported() -> Error {
    super::metadata::metadata_rejection(metadata::Rejection::Unsupported)
}

fn cluster_action(request: public::ManageCluster, operation: Id) -> Result<ClusterAction> {
    use public::ClusterAction as Action;
    match Action::try_from(request.action)
        .map_err(|_| Error::invalid("Cluster action is invalid"))?
    {
        Action::TransferShardAuthority => Ok(ClusterAction::TransferShard {
            operation,
            shard: wire::id(&request.shard_id)?,
            target_group: request.group_id,
        }),
        Action::RegisterClusterNode => {
            let node = request
                .registration
                .ok_or_else(|| Error::invalid("Node registration is required"))?;
            if node.retired || node.draining || node.generation != 0 {
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
                drain_operation: None,
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
        Action::BeginNodeDrain => Ok(ClusterAction::BeginDrain {
            node: request.node_id,
            operation,
        }),
        Action::InspectNodeDrain | Action::RetireClusterNode => Err(Error::invalid(
            "Node retirement requires runtime drain evidence",
        )),
        Action::Unspecified => Err(Error::invalid("Cluster action is required")),
        Action::InspectAuthorityTransfer => {
            Err(Error::invalid("Authority transfer inspection is read-only"))
        }
        Action::ReplaceDataReplica => Err(Error::invalid(
            "Data replacement requires a verified original group",
        )),
        Action::ReplaceMetadataReplica => Err(Error::invalid(
            "Metadata replacement requires a verified native membership view",
        )),
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
        draining: node.identity.drain_operation.is_some(),
        generation: node.record.generation,
    }
}
fn group_result(group: metadata::DataGroup) -> public::ClusterGroup {
    public::ClusterGroup {
        group_id: group.group_id,
        generation: group.generation,
        voters: group
            .placement
            .raft_voters()
            .map_or_else(Vec::new, |voters| voters.iter().copied().collect()),
    }
}

fn frame_kind_action(
    frame: &Frame,
    limits: &tidemq_protocol::ProtocolLimits,
) -> Result<public::ClusterAction> {
    let request: public::ManageCluster =
        tidemq_protocol::framing::decode_payload(frame.payload.clone(), limits)
            .map_err(|source| Error::operation("decode cluster action", source))?;
    public::ClusterAction::try_from(request.action)
        .map_err(|_| Error::invalid("Cluster action is invalid"))
}
