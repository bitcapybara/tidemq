use std::time::{Duration, Instant as MonoInstant};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;
use tidemq_protocol::common::{ErrorCode, Frame, FrameKind, Outcome};
use tidemq_protocol::framing::decode_payload;
use tidemq_protocol::public;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::broker::{DataCommand, DataResponse, WriterFence};
use crate::gateway::Access;
use crate::metadata::{
    self, Command, DeploymentMode, MetadataLimits, PartitionKey, RecordStatus, Response,
};
use crate::ownership::remote::DataProof;
use crate::ownership::{ControlPolicy, DataAuthority, Grant, VerifiedGrant};

use super::node::{data_config, random_id};
use super::service::{Service, forbidden};
use super::wire::{self, Request};
use super::{Error, Result};

#[cfg(test)]
#[path = "../../tests/scalable_support/metadata_errors.rs"]
mod error_tests;

#[cfg(test)]
#[path = "../../tests/runtime_support/ownership_cas.rs"]
mod ownership_cas_tests;

#[derive(Serialize, Deserialize)]
pub(crate) struct OwnershipRequest {
    pub grant: Grant,
    pub recover: bool,
}

impl Service {
    pub(super) async fn management_outcome(
        &self,
        operation: Id,
        digest: [u8; 32],
    ) -> Result<Option<metadata::Outcome>> {
        let saved = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.management_result(operation, digest))
            .await
            .map_err(|source| Error::operation("read original management decision", source))?
            .map_err(metadata_rejection)?;
        match saved {
            metadata::OperationResult::Known(Response::Applied { outcome, .. }) => {
                Ok(Some(outcome))
            }
            metadata::OperationResult::Known(Response::Rejected { reason, .. }) => {
                Err(original_metadata_rejection(reason))
            }
            metadata::OperationResult::UnknownOrRetired => Ok(None),
        }
    }
    pub async fn reconcile(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut failover = super::failover::State::new(&self.config);
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = shutdown.changed() => return Ok(()), _ = ticker.tick() => {} }
            let deadline = Instant::now() + Duration::from_secs(2);
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                result = self.reconcile_once(&mut failover, deadline) => result?,
            }
        }
    }

    async fn reconcile_once(
        &self,
        failover: &mut super::failover::State,
        deadline: Instant,
    ) -> Result<()> {
        if let Some(replica) = &self.metadata
            && self.require_leader(replica).is_ok()
        {
            let Ok(_guard) = wire::within(deadline, self.metadata_mutation.lock()).await else {
                return Ok(());
            };
            if let Err(error) = self.reconcile_metadata(deadline).await {
                tracing::debug!(
                    event_code = "runtime_metadata_reconcile_wait",
                    error_code = error.failure().code,
                    "Metadata startup reconciliation will retry"
                );
            }
            if let Err(error) = self.reconcile_consumers(deadline).await {
                tracing::debug!(
                    event_code = "consumer_reconcile_wait",
                    error_code = error.failure().code,
                    "Durable consumer assignment will resume after authority becomes available"
                );
            }
            let plans = replica
                .state
                .try_with_application(|state| {
                    state
                        .topology()
                        .pending()
                        .take(1)
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .await
                .map_err(|source| Error::operation("read unfinished layout plans", source))?;
            for plan in plans {
                if let Err(error) = self.resume_layout(plan, deadline).await {
                    tracing::debug!(
                        event_code = "layout_reconcile_wait",
                        error_code = error.failure().code,
                        "Durable layout plan will resume after authority or capacity becomes available"
                    );
                }
            }
            drop(_guard);
            if let Err(error) = self.reconcile_topic_deletions(deadline).await {
                tracing::debug!(
                    event_code = "topic_delete_reconcile_wait",
                    error_code = error.failure().code,
                    "Durable topic deletion will resume after data authority becomes available"
                );
            }
            if let Err(error) = self.reconcile_consumer_deletions(deadline).await {
                tracing::debug!(
                    event_code = "consumer_delete_reconcile_wait",
                    error_code = error.failure().code,
                    "Durable subscription deletion will resume after its dependencies retire"
                );
            }
        }
        if self
            .metadata
            .as_ref()
            .is_none_or(|replica| !replica.group.is_local())
            && let Err(error) = self.reconcile_owners(failover, deadline).await
        {
            tracing::debug!(
                event_code = "runtime_owner_failover_wait",
                error_code = error.failure().code,
                "Owner failover requires fresh authority and durable fencing"
            );
        }
        for (group, replica) in &self.data {
            if self.require_leader(replica).is_ok() {
                let config = data_config(
                    self.cluster,
                    self.config.runtime.group(*group)?,
                    &self.config,
                );
                let initialized = replica
                    .state
                    .try_with_application(|state| state.config() == Some(&config))
                    .await
                    .map_err(|source| Error::operation("read data initialization", source))?;
                if !initialized {
                    let _ = wire::within(
                        deadline,
                        replica
                            .group
                            .commit(DataCommand::Initialize(config), deadline),
                    )
                    .await;
                }
            }
        }
        Ok(())
    }

    async fn reconcile_metadata(&self, deadline: Instant) -> Result<()> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        wire::within(deadline, replica.group.ensure_read(deadline))
            .await?
            .map_err(wire::execution_error)?;
        let initialized = replica
            .state
            .try_with_application(|state| state.cluster_id().is_some())
            .await
            .map_err(|source| Error::operation("read metadata initialization", source))?;
        if !initialized {
            self.meta_write(
                Command::bootstrap(
                    random_id()?,
                    self.cluster,
                    self.config.runtime.metadata_group,
                    if replica.group.is_local() {
                        DeploymentMode::Local
                    } else if self.config.replication_factor == 1 {
                        DeploymentMode::SeparateRf1
                    } else {
                        DeploymentMode::SeparateRf3
                    },
                    MetadataLimits::default(),
                ),
                deadline,
            )
            .await?;
        }
        let bootstrap = replica
            .state
            .try_with_application(|state| !state.auth().initialized())
            .await
            .map_err(|source| Error::operation("read registry initialization", source))?;
        for node in self.registry.nodes()?.into_iter().filter(|_| bootstrap) {
            let absent = replica
                .state
                .try_with_application(|state| {
                    state.node_identity(node.record.spec.node_id).is_none()
                })
                .await
                .map_err(|source| Error::operation("read node topology", source))?;
            if absent {
                self.meta_write(
                    Command::cluster(
                        random_id()?,
                        self.meta_revision().await?,
                        metadata::ClusterCommand {
                            actor: None,
                            now_ms: super::data::unix_millis()?,
                            request_digest: [0; 32],
                            action: metadata::ClusterAction::RegisterNode {
                                spec: node.record.spec,
                                identity: node.identity,
                            },
                        },
                    ),
                    deadline,
                )
                .await?;
            }
        }
        for group in self.config.runtime.groups.iter().filter(|_| bootstrap) {
            let absent = replica
                .state
                .try_with_application(|state| state.group(group.group_id).is_none())
                .await
                .map_err(|source| Error::operation("read group topology", source))?;
            if absent {
                self.meta_write(
                    Command::create_group(
                        random_id()?,
                        self.meta_revision().await?,
                        group.group_id,
                        if replica.group.is_local() {
                            metadata::GroupPlacement::Local {
                                node_id: self.config.node_id,
                            }
                        } else {
                            metadata::GroupPlacement::Raft {
                                voters: group.voters.iter().copied().collect(),
                            }
                        },
                    ),
                    deadline,
                )
                .await?;
            }
        }
        self.bootstrap_credentials(deadline).await?;
        Ok(())
    }

    pub async fn meta_revision(&self) -> Result<u64> {
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(metadata::MetadataState::revision)
            .await
            .map_err(|source| Error::operation("read metadata revision", source))
    }

    pub(super) async fn metadata_request_revision(&self, operation: Id) -> Result<u64> {
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| match state.lookup_operation(operation) {
                // A successful replay must retain its original runtime-generated CAS revision.
                metadata::OperationResult::Known(Response::Applied { revision, .. }) => {
                    revision.checked_sub(1).ok_or_else(|| {
                        Error::invalid("Durable metadata result has an invalid revision")
                    })
                }
                _ => Ok(state.revision()),
            })
            .await
            .map_err(|source| Error::operation("read original metadata request revision", source))?
    }

    pub(crate) async fn meta_write(
        &self,
        command: Command,
        deadline: Instant,
    ) -> Result<metadata::Outcome> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let binding = command.management_binding.or(match &command.action {
            metadata::Action::Identity(request) => Some(request.request_digest),
            metadata::Action::Cluster(request) => Some(request.request_digest),
            _ => None,
        });
        let operation = command.operation_id;
        let response = self.commit_metadata(command, deadline).await?;
        match response {
            Response::Applied { outcome, .. } => Ok(outcome),
            Response::Rejected { reason, .. } => {
                if let Some(digest) = binding {
                    let recorded = replica
                        .state
                        .try_with_application(|state| state.management_result(operation, digest))
                        .await
                        .map_err(|source| {
                            Error::operation("read original management refusal", source)
                        })?
                        .map_err(metadata_rejection)?;
                    if let metadata::OperationResult::Known(Response::Rejected { reason, .. }) =
                        recorded
                    {
                        return Err(original_metadata_rejection(reason));
                    }
                }
                Err(metadata_rejection(reason))
            }
        }
    }

    async fn commit_metadata(&self, command: Command, deadline: Instant) -> Result<Response> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        #[cfg(test)]
        super::console_operation::tests::before_metadata_write(&command).await;
        #[cfg(test)]
        ownership_cas_tests::before_write(&command).await;
        Ok(replica
            .group
            .commit(command, deadline)
            .await
            .map_err(wire::execution_error)?
            .response)
    }

    pub async fn metadata_public(
        &self,
        access: Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        wire::within(deadline, replica.group.ensure_read(deadline))
            .await?
            .map_err(wire::execution_error)?;
        if super::service::frame_kind(frame)? == FrameKind::ConsoleWrite {
            return self.console_write_public(&access, frame, deadline).await;
        }
        if super::service::frame_kind(frame)? == FrameKind::GetConsoleOperation {
            return self
                .console_operation_public(&access, frame, deadline)
                .await;
        }
        if super::service::frame_kind(frame)? == FrameKind::PublisherControl {
            return self
                .publisher_control_public(&access, frame, deadline)
                .await;
        }
        if super::service::frame_kind(frame)? == FrameKind::ConsumerControl {
            return self.consumer_control_public(&access, frame, deadline).await;
        }
        if super::service::frame_kind(frame)? == FrameKind::SubscriptionAdmin {
            return self
                .subscription_admin_public(&access, frame, deadline)
                .await;
        }
        if super::service::frame_kind(frame)? == FrameKind::RemoveTopic {
            return self.remove_topic_public(&access, frame, deadline).await;
        }
        if super::service::frame_kind(frame)? == FrameKind::ManageIdentity {
            return self.identity_public(&access, frame, deadline).await;
        }
        if super::service::frame_kind(frame)? == FrameKind::ManageCluster {
            return self.cluster_public(&access, frame, deadline).await;
        }
        if super::service::frame_kind(frame)? == FrameKind::GetClusterRegistry {
            return self.cluster_list_public(&access, frame).await;
        }
        if super::service::frame_kind(frame)? == FrameKind::ListIdentities {
            return self.identity_list_public(&access, frame).await;
        }
        if super::service::frame_kind(frame)? == FrameKind::ConsoleRead {
            return self.console_read_public(&access, frame, deadline).await;
        }
        if matches!(
            super::service::frame_kind(frame)?,
            FrameKind::CreateLogicalTopic
                | FrameKind::GetTopicLayout
                | FrameKind::ChangeTopicLayout
        ) {
            return self.topology_public(&access, frame, deadline).await;
        }
        match super::service::frame_kind(frame)? {
            FrameKind::GetTopic => {
                let request: public::GetTopic =
                    decode_payload(frame.payload.clone(), &self.config.protocol)
                        .map_err(|source| Error::operation("decode topic query", source))?;
                access.authorize_topic(&request.name)?;
                let topic = replica
                    .state
                    .try_with_application(|state| {
                        state
                            .topic_by_name(&request.name)
                            .filter(|record| record.status == RecordStatus::Active)
                            .cloned()
                    })
                    .await
                    .map_err(|source| Error::operation("read topic", source))?
                    .ok_or_else(not_found)?;
                wire::response(
                    frame,
                    FrameKind::TopicResult,
                    &public::TopicResult {
                        topic_id: bytes_id(topic.topic_id),
                        name: topic.name,
                    },
                    &self.config.protocol,
                )
            }
            _ => Err(forbidden()),
        }
    }

    pub(super) async fn ensure_route(
        &self,
        partition: PartitionKey,
        deadline: Instant,
    ) -> Result<metadata::Route> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let diagnostic = replica
            .state
            .try_with_application(|state| state.diagnose_ownership(partition))
            .await
            .map_err(|source| Error::operation("read ownership progress", source))?
            .ok_or_else(not_found)?;
        if diagnostic.phase == crate::ownership::OwnershipPhase::Active {
            return replica
                .state
                .try_with_application(|state| state.route(partition).cloned())
                .await
                .map_err(|source| Error::operation("read active route", source))?
                .ok_or_else(Error::unavailable);
        }
        let grant = if diagnostic.phase == crate::ownership::OwnershipPhase::Unassigned {
            let owner = if replica.group.is_local() {
                self.config.node_id
            } else {
                self.select_initial_broker(deadline).await?
            };
            let metadata::Outcome::OwnershipGranted(grant) = self
                .ownership_meta_write(
                    crate::ownership::Transition::Grant,
                    diagnostic.epoch.saturating_add(1),
                    || async {
                        Command::grant_ownership(
                            self.meta_revision().await?,
                            self.cluster,
                            partition,
                            owner,
                            diagnostic.epoch,
                            diagnostic.route_generation,
                        )
                        .map_err(metadata_rejection)
                    },
                    deadline,
                )
                .await?
            else {
                return Err(Error::invalid(
                    "Metadata returned the wrong ownership outcome",
                ));
            };
            grant
        } else {
            replica
                .state
                .try_with_application(|state| state.grant(partition).cloned())
                .await
                .map_err(|source| Error::operation("read ownership grant", source))?
                .ok_or_else(Error::unavailable)?
        };
        self.complete_route(grant, diagnostic.phase, deadline).await
    }

    async fn complete_route(
        &self,
        grant: Grant,
        phase: crate::ownership::OwnershipPhase,
        deadline: Instant,
    ) -> Result<metadata::Route> {
        if self
            .metadata
            .as_ref()
            .is_some_and(|replica| replica.group.is_local())
        {
            return self.complete_local_route(grant, phase, deadline).await;
        }
        let partition = grant.partition;
        let voters = self
            .authoritative_group(grant.group_id, deadline)
            .await?
            .placement
            .raft_voters()
            .cloned()
            .ok_or_else(Error::unavailable)?;
        if phase < crate::ownership::OwnershipPhase::Fenced {
            self.remote_ownership_write(
                &grant,
                &voters,
                crate::ownership::Transition::Fence,
                deadline,
            )
            .await?;
        }
        if phase < crate::ownership::OwnershipPhase::Recovered {
            self.remote_ownership_write(
                &grant,
                &voters,
                crate::ownership::Transition::Recover,
                deadline,
            )
            .await?;
        }
        let metadata::Outcome::RouteActivated(route) = self
            .remote_ownership_write(
                &grant,
                &voters,
                crate::ownership::Transition::Activate,
                deadline,
            )
            .await?
        else {
            return Err(Error::invalid(
                "Metadata returned the wrong activation outcome",
            ));
        };
        self.routes.write().await.remove(&partition);
        Ok(route)
    }

    async fn remote_ownership_write(
        &self,
        grant: &Grant,
        voters: &std::collections::BTreeSet<u64>,
        stage: crate::ownership::Transition,
        deadline: Instant,
    ) -> Result<metadata::Outcome> {
        self.ownership_meta_write(
            stage,
            grant.epoch,
            || async {
                let target = self.leader(grant.group_id, deadline).await?;
                let started = MonoInstant::now();
                let bytes = self
                    .rpc
                    .call(
                        target,
                        grant.group_id,
                        Request::control(
                            wire::OWNERSHIP,
                            &OwnershipRequest {
                                grant: grant.clone(),
                                recover: stage != crate::ownership::Transition::Fence,
                            },
                        )?,
                        deadline,
                    )
                    .await?;
                let proof: DataProof = wire::decode_json(&bytes)?;
                let revision = self.meta_revision().await?;
                match stage {
                    crate::ownership::Transition::Fence => self
                        .authority
                        .verified_fence(proof, grant, voters, started)?
                        .into_observe_command(revision),
                    crate::ownership::Transition::Recover => self
                        .authority
                        .verified_recovery(proof, grant, voters, started)?
                        .into_observe_command(revision),
                    crate::ownership::Transition::Activate => self
                        .authority
                        .verified_recovery(proof, grant, voters, started)?
                        .into_activate_command(revision),
                    crate::ownership::Transition::Grant => {
                        return Err(Error::invalid(
                            "A data proof cannot create an ownership grant",
                        ));
                    }
                }
                .map_err(|source| {
                    Error::operation("validate fresh ownership stage evidence", source)
                })
            },
            deadline,
        )
        .await
    }

    pub(crate) async fn execute_takeover(
        &self,
        partition: PartitionKey,
        target_owner: u64,
        expected_epoch: u64,
        expected_route_generation: u64,
        deadline: Instant,
    ) -> Result<metadata::Route> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
        wire::within(deadline, replica.group.ensure_read(deadline))
            .await?
            .map_err(wire::execution_error)?;
        let diagnostic = replica
            .state
            .try_with_application(|state| state.diagnose_ownership(partition))
            .await
            .map_err(|source| Error::operation("read takeover progress", source))?
            .ok_or_else(not_found)?;
        let target = replica
            .state
            .try_with_application(|state| state.node(target_owner).cloned())
            .await
            .map_err(|source| Error::operation("read takeover target", source))?
            .ok_or_else(not_found)?;
        if !target.spec.schedulable || !target.spec.roles.contains(&metadata::NodeRole::Broker) {
            return Err(forbidden());
        }
        let desired_epoch = expected_epoch
            .checked_add(1)
            .ok_or_else(|| Error::invalid("Ownership epoch is exhausted"))?;
        if diagnostic.epoch == desired_epoch {
            let grant = replica
                .state
                .try_with_application(|state| state.grant(partition).cloned())
                .await
                .map_err(|source| Error::operation("read in-progress takeover", source))?
                .ok_or_else(Error::unavailable)?;
            if grant.owner_node != target_owner
                || grant.expected_route_generation != expected_route_generation
            {
                return Err(Error::reject(ErrorCode::Fenced, Outcome::Rejected, false));
            }
            if diagnostic.phase == crate::ownership::OwnershipPhase::Active {
                return replica
                    .state
                    .try_with_application(|state| state.route(partition).cloned())
                    .await
                    .map_err(|source| Error::operation("read completed takeover", source))?
                    .ok_or_else(Error::unavailable);
            }
            return self.complete_route(grant, diagnostic.phase, deadline).await;
        }
        if diagnostic.epoch != expected_epoch
            || diagnostic.route_generation != expected_route_generation
        {
            return Err(Error::reject(ErrorCode::Fenced, Outcome::Rejected, false));
        }
        let metadata::Outcome::OwnershipGranted(grant) = self
            .ownership_meta_write(
                crate::ownership::Transition::Grant,
                desired_epoch,
                || async {
                    Command::grant_ownership(
                        self.meta_revision().await?,
                        self.cluster,
                        partition,
                        target_owner,
                        expected_epoch,
                        expected_route_generation,
                    )
                    .map_err(metadata_rejection)
                },
                deadline,
            )
            .await?
        else {
            return Err(Error::invalid("Unexpected takeover grant outcome"));
        };
        self.complete_route(grant, crate::ownership::OwnershipPhase::Granted, deadline)
            .await
    }

    pub async fn ownership_command(
        &self,
        group: u64,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let request: OwnershipRequest = wire::decode_json(bytes)?;
        let stage = if request.recover {
            crate::ownership::Transition::Recover
        } else {
            crate::ownership::Transition::Fence
        };
        let mut timer = super::telemetry::OwnershipTimer::new(
            &self.ownership_metrics,
            stage,
            request.grant.operation_id,
            request.grant.epoch,
        );
        let result = self.ownership_stage(group, request, deadline).await;
        timer.finish(&result);
        result
    }

    async fn ownership_stage(
        &self,
        group: u64,
        request: OwnershipRequest,
        deadline: Instant,
    ) -> Result<Bytes> {
        if request.grant.group_id != group || request.grant.cluster_id != self.cluster {
            return Err(forbidden());
        }
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        let verified = self
            .authority
            .verify_grant(&request.grant, deadline)
            .await?;
        self.apply_ownership_stage(group, &request, verified, deadline)
            .await?;
        self.routes.write().await.remove(&request.grant.partition);
        let placement = self.authoritative_group(group, deadline).await?.placement;
        let voters = placement
            .raft_voters()
            .cloned()
            .ok_or_else(Error::unavailable)?;
        let authority = DataAuthority::new(
            replica.group.clone(),
            self.cluster,
            group,
            placement,
            ControlPolicy::default(),
        )
        .map_err(|source| Error::operation("create data authority", source))?;
        let proof = authority
            .remote_proof(&request.grant, request.recover, voters)
            .await
            .map_err(|source| Error::operation("read data quorum proof", source))?;
        wire::json(&proof)
    }

    async fn apply_ownership_stage(
        &self,
        group: u64,
        request: &OwnershipRequest,
        verified: VerifiedGrant,
        deadline: Instant,
    ) -> Result<()> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        if request.recover {
            let fence = replica
                .state
                .try_with_application(|state| {
                    state
                        .ownership()
                        .and_then(|state| state.fence(request.grant.partition))
                        .cloned()
                })
                .await
                .map_err(|source| Error::operation("read recoverable fence", source))?
                .ok_or_else(Error::unavailable)?;
            if fence.grant != request.grant {
                return Err(forbidden());
            }
            let _ = verified;
            self.data_write(
                group,
                DataCommand::Recover {
                    partition: request.grant.partition,
                    writer: WriterFence {
                        node_id: request.grant.owner_node,
                        owner_epoch: request.grant.epoch,
                    },
                },
                deadline,
            )
            .await?;
        } else {
            let command = verified
                .into_fence_command()
                .map_err(|source| Error::operation("consume fresh metadata grant", source))?;
            self.data_write(group, DataCommand::InstallFence(command), deadline)
                .await?;
        }
        Ok(())
    }

    async fn complete_local_route(
        &self,
        grant: Grant,
        phase: crate::ownership::OwnershipPhase,
        deadline: Instant,
    ) -> Result<metadata::Route> {
        let replica = self
            .data
            .get(&grant.group_id)
            .ok_or_else(Error::unavailable)?;
        let placement = self
            .authoritative_group(grant.group_id, deadline)
            .await?
            .placement;
        let data = DataAuthority::new(
            replica.group.clone(),
            self.cluster,
            grant.group_id,
            placement,
            ControlPolicy::default(),
        )
        .map_err(|source| Error::operation("create local data authority", source))?;
        for (recover, stage, required) in [
            (
                false,
                crate::ownership::Transition::Fence,
                crate::ownership::OwnershipPhase::Fenced,
            ),
            (
                true,
                crate::ownership::Transition::Recover,
                crate::ownership::OwnershipPhase::Recovered,
            ),
        ] {
            if phase >= required {
                continue;
            }
            self.ownership_meta_write(
                stage,
                grant.epoch,
                || async {
                    let verified = self.authority.verify_grant(&grant, deadline).await?;
                    let request = OwnershipRequest {
                        grant: grant.clone(),
                        recover,
                    };
                    self.apply_ownership_stage(grant.group_id, &request, verified, deadline)
                        .await?;
                    let revision = self.meta_revision().await?;
                    if recover {
                        data.verify_recovery(&grant)
                            .await
                            .and_then(|proof| proof.into_observe_command(revision))
                    } else {
                        data.verify_fence(&grant)
                            .await
                            .and_then(|proof| proof.into_observe_command(revision))
                    }
                    .map_err(|source| {
                        Error::operation("verify fresh local ownership evidence", source)
                    })
                },
                deadline,
            )
            .await?;
        }
        let metadata::Outcome::RouteActivated(route) = self
            .ownership_meta_write(
                crate::ownership::Transition::Activate,
                grant.epoch,
                || async {
                    let revision = self.meta_revision().await?;
                    data.verify_recovery(&grant)
                        .await
                        .and_then(|proof| proof.into_activate_command(revision))
                        .map_err(|source| {
                            Error::operation("verify fresh local route recovery", source)
                        })
                },
                deadline,
            )
            .await?
        else {
            return Err(Error::invalid(
                "Metadata returned the wrong local activation outcome",
            ));
        };
        self.routes.write().await.remove(&grant.partition);
        Ok(route)
    }

    async fn ownership_meta_write<F, Fut>(
        &self,
        stage: crate::ownership::Transition,
        epoch: u64,
        mut prepare: F,
        deadline: Instant,
    ) -> Result<metadata::Outcome>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<Command>>,
    {
        if Instant::now() >= deadline {
            return Err(Error::unavailable());
        }
        let mut command = wire::within(deadline, prepare()).await??;
        let operation = command.operation_id();
        let mut timer =
            super::telemetry::OwnershipTimer::new(&self.ownership_metrics, stage, operation, epoch);
        let result = async {
            let mut retry = false;
            loop {
                if Instant::now() >= deadline {
                    return Err(metadata_rejection(metadata::Rejection::RevisionConflict));
                }
                if retry {
                    self.ownership_metrics.cas_retry(stage);
                    tracing::debug!(event_code = "ownership_metadata_revision_retry", operation_id = %operation, ?stage, "Retrying a rejected ownership CAS with fresh evidence");
                }
                match self.commit_metadata(command, deadline).await? {
                    Response::Applied { outcome, .. } => return Ok(outcome),
                    Response::Rejected { reason: metadata::Rejection::RevisionConflict, .. } => {
                        tokio::task::yield_now().await;
                        if Instant::now() >= deadline {
                            return Err(metadata_rejection(metadata::Rejection::RevisionConflict));
                        }
                        command = wire::within(deadline, prepare()).await??;
                        if command.operation_id() != operation {
                            return Err(Error::invalid("Ownership retry changed its original operation identity"));
                        }
                        retry = true;
                    }
                    Response::Rejected { reason, .. } => return Err(metadata_rejection(reason)),
                }
            }
        }.await;
        timer.finish(&result);
        result
    }

    pub async fn data_write(
        &self,
        group: u64,
        command: DataCommand,
        deadline: Instant,
    ) -> Result<DataResponse> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        let committed = if command.is_completion() {
            replica.group.commit_completion(command, deadline).await
        } else {
            replica.group.commit(command, deadline).await
        };
        match committed.map_err(wire::execution_error)?.response {
            DataResponse::Rejected(reason) => Err(super::data::data_rejection(reason)),
            response => Ok(response),
        }
    }
}

pub(super) fn metadata_rejection(reason: metadata::Rejection) -> Error {
    use metadata::Rejection as R;
    if let R::MemberEpochConflict { expected } = reason {
        let mut failure =
            Error::reject(ErrorCode::RegistrationConflict, Outcome::Rejected, false).failure();
        failure.expected_registration_epoch = Some(expected);
        return Error::Rejected { failure };
    }
    let (code, retryable) = match &reason {
        R::Authorization(metadata::AuthRejection::Capacity) => (ErrorCode::QuotaExceeded, true),
        R::Authorization(metadata::AuthRejection::Forbidden) => (ErrorCode::Forbidden, false),
        R::Authorization(metadata::AuthRejection::NotFound) => (ErrorCode::NotFound, false),
        R::Authorization(metadata::AuthRejection::Conflict) => (ErrorCode::ContentConflict, false),
        R::Authorization(metadata::AuthRejection::Invalid) => (ErrorCode::InvalidRequest, false),
        R::Unsupported => (ErrorCode::InvalidRequest, false),
        R::SubscriptionInUse => (ErrorCode::AssignmentBusy, false),
        R::Capacity => (ErrorCode::QuotaExceeded, true),
        R::RevisionConflict | R::GenerationConflict | R::RecoveryRequired | R::Uninitialized => {
            (ErrorCode::Unavailable, true)
        }
        R::NotFound | R::IdentityRetired => (ErrorCode::NotFound, false),
        R::Superseded => (ErrorCode::StaleGeneration, false),
        R::OperationConflict | R::NameInUse | R::AlreadyInitialized => {
            (ErrorCode::ContentConflict, false)
        }
        _ => (ErrorCode::InvalidRequest, false),
    };
    Error::reject_with(code, Outcome::Rejected, retryable, reason.to_string())
}

pub(super) fn original_metadata_rejection(reason: metadata::Rejection) -> Error {
    let mut failure = metadata_rejection(reason).failure();
    failure.original_outcome_known = true;
    Error::Rejected { failure }
}

pub(super) fn management_binding(access: &Access, frame: &Frame) -> Result<[u8; 32]> {
    let actor: Id = access
        .principal
        .strip_prefix("credential:")
        .ok_or_else(forbidden)?
        .parse()
        .map_err(|_| forbidden())?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq/management-request/v1\0");
    hash.update(actor.as_bytes());
    hash.update(&frame.kind.to_le_bytes());
    hash.update(&frame.payload);
    Ok(*hash.finalize().as_bytes())
}
pub(crate) fn not_found() -> Error {
    Error::reject(ErrorCode::NotFound, Outcome::Rejected, false)
}
pub(crate) fn bytes_id(id: Id) -> Bytes {
    Bytes::copy_from_slice(id.as_bytes())
}
