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
    self, Command, DeploymentMode, MetadataLimits, NodeSpec, PartitionKey, RecordStatus, Response,
    SubscriptionConfig,
};
use crate::ownership::remote::{DataProof, Query, Value};
use crate::ownership::{ControlPolicy, DataAuthority, Grant};
use crate::subscription::{self, SigningKey};

use super::node::{data_config, random_id, roles};
use super::service::{Service, forbidden};
use super::wire::{self, Request};
use super::{Error, Result};

#[derive(Serialize, Deserialize)]
pub(crate) struct OwnershipRequest {
    pub grant: Grant,
    pub recover: bool,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct CreateSubscriptionRequest {
    pub partition: PartitionKey,
    pub subscription: Id,
    pub operation: Id,
    pub start: u64,
}

impl Service {
    pub async fn reconcile(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = shutdown.changed() => return Ok(()), _ = ticker.tick() => {} }
            let deadline = Instant::now() + Duration::from_secs(2);
            if let Some(replica) = &self.metadata
                && self.require_leader(replica).is_ok()
            {
                let _guard = self.metadata_mutation.lock().await;
                if let Err(error) = self.reconcile_metadata(deadline).await {
                    tracing::debug!(
                        event_code = "runtime_metadata_reconcile_wait",
                        error_code = error.failure().code,
                        "Metadata startup reconciliation will retry"
                    );
                }
            }
            for (group, replica) in &self.data {
                if self.require_leader(replica).is_ok() {
                    let config = data_config(
                        self.cluster,
                        self.config.runtime.group(*group)?,
                        self.config.runtime.retention,
                    );
                    let initialized = replica
                        .state
                        .try_with_application(|state| state.config() == Some(&config))
                        .await
                        .map_err(|source| Error::operation("read data initialization", source))?;
                    if !initialized {
                        let _ = wire::within(
                            deadline,
                            replica.raft.client_write(DataCommand::Initialize(config)),
                        )
                        .await;
                    }
                }
            }
        }
    }

    async fn reconcile_metadata(&self, deadline: Instant) -> Result<()> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
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
                    if self.config.replication_factor == 1 {
                        DeploymentMode::LocalRf1
                    } else {
                        DeploymentMode::ThreeReplica
                    },
                    MetadataLimits::default(),
                ),
                deadline,
            )
            .await?;
        }
        for peer in &self.config.runtime.peers {
            let absent = replica
                .state
                .try_with_application(|state| state.node(peer.node_id).is_none())
                .await
                .map_err(|source| Error::operation("read node topology", source))?;
            if absent {
                self.meta_write(
                    Command::put_node(
                        random_id()?,
                        self.meta_revision().await?,
                        NodeSpec {
                            node_id: peer.node_id,
                            zone: peer.zone.clone(),
                            endpoint: peer.internal_address,
                            roles: roles(peer.role),
                            schedulable: true,
                        },
                        0,
                    ),
                    deadline,
                )
                .await?;
            }
        }
        for group in &self.config.runtime.groups {
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
                        group.voters.iter().copied().collect(),
                    ),
                    deadline,
                )
                .await?;
            }
        }
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

    pub(crate) async fn meta_write(
        &self,
        command: Command,
        deadline: Instant,
    ) -> Result<metadata::Outcome> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        let response = wire::within(deadline, replica.raft.client_write(command))
            .await?
            .map_err(|source| Error::operation("commit metadata command", source))?
            .data;
        match response {
            Response::Applied { outcome, .. } => Ok(outcome),
            Response::Rejected { reason, .. } => Err(metadata_rejection(reason)),
        }
    }

    pub async fn metadata_public(
        &self,
        access: Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        let operation = wire::id(&frame.operation_id)?;
        match super::service::frame_kind(frame)? {
            FrameKind::CreateTopic => {
                let request: public::CreateTopic =
                    decode_payload(frame.payload.clone(), &self.config.protocol)
                        .map_err(|source| Error::operation("decode topic creation", source))?;
                access.authorize_topic(&request.name)?;
                if request.partitions == 0 || request.partitions > 128 {
                    return Err(Error::invalid(
                        "Topic partition count must be between one and 128",
                    ));
                }
                let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
                let groups: Vec<_> = self
                    .config
                    .runtime
                    .groups
                    .iter()
                    .cycle()
                    .take(request.partitions as usize)
                    .map(|group| group.group_id)
                    .collect();
                let outcome = self
                    .meta_write(
                        Command::create_topic(
                            operation,
                            self.meta_revision().await?,
                            operation,
                            request.name,
                            groups,
                        ),
                        deadline,
                    )
                    .await?;
                let metadata::Outcome::TopicConfigured(topic) = outcome else {
                    return Err(Error::invalid(
                        "Metadata returned the wrong topic creation outcome",
                    ));
                };
                for partition in 0..topic.partition_count {
                    self.ensure_route(
                        PartitionKey {
                            topic_id: topic.topic_id,
                            partition,
                        },
                        deadline,
                    )
                    .await?;
                }
                wire::response(
                    frame,
                    FrameKind::TopicResult,
                    &public::TopicResult {
                        topic_id: bytes_id(topic.topic_id),
                        name: topic.name,
                        partitions: topic.partition_count,
                    },
                    &self.config.protocol,
                )
            }
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
                        partitions: topic.partition_count,
                    },
                    &self.config.protocol,
                )
            }
            FrameKind::CreateSubscription => {
                let request: public::CreateSubscription =
                    decode_payload(frame.payload.clone(), &self.config.protocol).map_err(
                        |source| Error::operation("decode subscription creation", source),
                    )?;
                let topic_id = wire::id(&request.topic_id)?;
                let topic = replica
                    .state
                    .try_with_application(|state| {
                        state
                            .topic(topic_id)
                            .filter(|record| record.status == RecordStatus::Active)
                            .cloned()
                    })
                    .await
                    .map_err(|source| Error::operation("read subscription topic", source))?
                    .ok_or_else(not_found)?;
                access.authorize_topic(&topic.name)?;
                let ordering = match public::DeliveryMode::try_from(request.delivery_mode)
                    .map_err(|_| Error::invalid("Unknown subscription delivery mode"))?
                {
                    public::DeliveryMode::Parallel | public::DeliveryMode::Unspecified => {
                        metadata::Ordering::Parallel
                    }
                    public::DeliveryMode::KeySerial => metadata::Ordering::PerKey,
                };
                let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
                let outcome = self
                    .meta_write(
                        Command::put_subscription(
                            operation,
                            self.meta_revision().await?,
                            operation,
                            topic_id,
                            request.name,
                            0,
                            SubscriptionConfig {
                                ordering,
                                ..SubscriptionConfig::default()
                            },
                        ),
                        deadline,
                    )
                    .await?;
                let metadata::Outcome::SubscriptionConfigured(subscription) = outcome else {
                    return Err(Error::invalid(
                        "Metadata returned the wrong subscription creation outcome",
                    ));
                };
                for partition in 0..topic.partition_count {
                    let partition = PartitionKey {
                        topic_id,
                        partition,
                    };
                    let route = self.ensure_route(partition, deadline).await?;
                    let target = self.leader(route.group_id, deadline).await?;
                    let body = CreateSubscriptionRequest {
                        partition,
                        subscription: subscription.subscription_id,
                        operation,
                        start: request.start_offset,
                    };
                    self.rpc
                        .call(
                            target,
                            route.group_id,
                            Request::control(wire::CREATE_SUBSCRIPTION, &body)?,
                            deadline,
                        )
                        .await?;
                }
                wire::response(
                    frame,
                    FrameKind::SubscriptionResult,
                    &public::SubscriptionResult {
                        subscription_id: bytes_id(subscription.subscription_id),
                        topic_id: bytes_id(topic_id),
                        cursor_generation: 1,
                    },
                    &self.config.protocol,
                )
            }
            FrameKind::GetSubscription => {
                let request: public::GetSubscription =
                    decode_payload(frame.payload.clone(), &self.config.protocol)
                        .map_err(|source| Error::operation("decode subscription query", source))?;
                let topic_id = wire::id(&request.topic_id)?;
                let topic = replica
                    .state
                    .try_with_application(|state| state.topic(topic_id).cloned())
                    .await
                    .map_err(|source| Error::operation("read subscription topic", source))?
                    .ok_or_else(not_found)?;
                access.authorize_topic(&topic.name)?;
                let subscription = replica
                    .state
                    .try_with_application(|state| {
                        state.subscription_by_name(topic_id, &request.name).cloned()
                    })
                    .await
                    .map_err(|source| Error::operation("read subscription", source))?
                    .ok_or_else(not_found)?;
                wire::response(
                    frame,
                    FrameKind::SubscriptionResult,
                    &public::SubscriptionResult {
                        subscription_id: bytes_id(subscription.subscription_id),
                        topic_id: bytes_id(topic_id),
                        cursor_generation: 1,
                    },
                    &self.config.protocol,
                )
            }
            _ => Err(forbidden()),
        }
    }

    async fn ensure_route(
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
            let owner = self
                .config
                .runtime
                .peers
                .iter()
                .find(|peer| peer.role.serves_broker())
                .map(|peer| peer.node_id)
                .ok_or_else(Error::unavailable)?;
            let command = Command::grant_ownership(
                self.meta_revision().await?,
                self.cluster,
                partition,
                owner,
                diagnostic.epoch,
                diagnostic.route_generation,
            )
            .map_err(metadata_rejection)?;
            let metadata::Outcome::OwnershipGranted(grant) = self
                .ownership_meta_write(
                    crate::ownership::Transition::Grant,
                    diagnostic.epoch.saturating_add(1),
                    command,
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
        let partition = grant.partition;
        let voters = self
            .authoritative_group(grant.group_id, deadline)
            .await?
            .voters;
        let target = self.leader(grant.group_id, deadline).await?;
        if phase < crate::ownership::OwnershipPhase::Fenced {
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
                            recover: false,
                        },
                    )?,
                    deadline,
                )
                .await?;
            let proof: DataProof = wire::decode_json(&bytes)?;
            let observed = self
                .authority
                .verified_fence(proof, &grant, &voters, started)?
                .into_observe_command(self.meta_revision().await?)
                .map_err(|source| Error::operation("validate fresh fence evidence", source))?;
            self.meta_write(observed, deadline).await?;
        }
        if phase < crate::ownership::OwnershipPhase::Recovered {
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
                            recover: true,
                        },
                    )?,
                    deadline,
                )
                .await?;
            let proof: DataProof = wire::decode_json(&bytes)?;
            let observed = self
                .authority
                .verified_recovery(proof, &grant, &voters, started)?
                .into_observe_command(self.meta_revision().await?)
                .map_err(|source| Error::operation("validate fresh recovery evidence", source))?;
            self.meta_write(observed, deadline).await?;
        }
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
                        recover: true,
                    },
                )?,
                deadline,
            )
            .await?;
        let proof: DataProof = wire::decode_json(&bytes)?;
        let activate = self
            .authority
            .verified_recovery(proof, &grant, &voters, started)?
            .into_activate_command(self.meta_revision().await?)
            .map_err(|source| Error::operation("validate route activation evidence", source))?;
        let metadata::Outcome::RouteActivated(route) = self
            .ownership_meta_write(
                crate::ownership::Transition::Activate,
                grant.epoch,
                activate,
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
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
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
        let command = Command::grant_ownership(
            self.meta_revision().await?,
            self.cluster,
            partition,
            target_owner,
            expected_epoch,
            expected_route_generation,
        )
        .map_err(metadata_rejection)?;
        let metadata::Outcome::OwnershipGranted(grant) = self
            .ownership_meta_write(
                crate::ownership::Transition::Grant,
                desired_epoch,
                command,
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
        let voters = self.authoritative_group(group, deadline).await?.voters;
        let authority = DataAuthority::new(
            replica.raft.clone(),
            replica.state.clone(),
            self.cluster,
            group,
            voters.clone(),
            ControlPolicy::default(),
        )
        .map_err(|source| Error::operation("create data authority", source))?;
        let proof = authority
            .remote_proof(&request.grant, request.recover, voters)
            .await
            .map_err(|source| Error::operation("read data quorum proof", source))?;
        wire::json(&proof)
    }

    async fn ownership_meta_write(
        &self,
        stage: crate::ownership::Transition,
        epoch: u64,
        command: Command,
        deadline: Instant,
    ) -> Result<metadata::Outcome> {
        let mut timer = super::telemetry::OwnershipTimer::new(
            &self.ownership_metrics,
            stage,
            command.operation_id(),
            epoch,
        );
        let result = self.meta_write(command, deadline).await;
        timer.finish(&result);
        result
    }

    pub async fn create_data_subscription(
        &self,
        group: u64,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let request: CreateSubscriptionRequest = wire::decode_json(bytes)?;
        let Value::Subscription(Some(record)) = self
            .authority
            .query(Query::Subscription(request.subscription), deadline)
            .await?
        else {
            return Err(not_found());
        };
        if record.topic_id != request.partition.topic_id {
            return Err(forbidden());
        }
        let lease = self.data_lease(group, request.partition, deadline).await?;
        let mut hasher = blake3::Hasher::new_keyed(&self.delivery_key);
        hasher.update(b"tidemq:delivery-signing:v1");
        hasher.update(request.subscription.as_bytes());
        hasher.update(request.partition.topic_id.as_bytes());
        hasher.update(&request.partition.partition.to_be_bytes());
        let signing_key =
            SigningKey::try_from(*hasher.finalize().as_bytes()).map_err(subscription_rejection)?;
        self.subscription_write(
            group,
            request.partition,
            &lease,
            subscription::Command::Create {
                subscription_id: request.subscription,
                operation_id: request.operation,
                start_offset: request.start,
                signing_key,
            },
            deadline,
            false,
        )
        .await?;
        wire::json(&())
    }

    pub async fn data_write(
        &self,
        group: u64,
        command: DataCommand,
        deadline: Instant,
    ) -> Result<DataResponse> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        match wire::within(deadline, replica.raft.client_write(command))
            .await?
            .map_err(|source| Error::operation("commit data-group command", source))?
            .data
        {
            DataResponse::Rejected(reason) => Err(super::data::data_rejection(reason)),
            response => Ok(response),
        }
    }
}

fn metadata_rejection(reason: metadata::Rejection) -> Error {
    use metadata::Rejection as R;
    let (code, retryable) = match reason {
        R::Capacity => (ErrorCode::QuotaExceeded, true),
        R::RevisionConflict | R::GenerationConflict | R::RecoveryRequired | R::Uninitialized => {
            (ErrorCode::Unavailable, true)
        }
        R::NotFound | R::IdentityRetired => (ErrorCode::NotFound, false),
        R::OperationConflict | R::NameInUse | R::AlreadyInitialized => {
            (ErrorCode::ContentConflict, false)
        }
        _ => (ErrorCode::InvalidRequest, false),
    };
    Error::reject(code, Outcome::Rejected, retryable)
}
pub(crate) fn not_found() -> Error {
    Error::reject(ErrorCode::NotFound, Outcome::Rejected, false)
}
pub(crate) fn bytes_id(id: Id) -> Bytes {
    Bytes::copy_from_slice(id.as_bytes())
}

fn subscription_rejection(reason: subscription::Rejection) -> Error {
    super::data::data_rejection(crate::broker::DataRejection::Subscription(reason))
}
