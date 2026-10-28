use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::{
    Id,
    common::{ErrorCode, Frame, FrameKind},
    framing::decode_payload,
    public,
    topic::{InitialPosition, SubscriptionKind},
};
use tokio::time::Instant;

use super::{
    Error, Result,
    service::{Service, forbidden},
    wire::{self, Request},
};
use crate::{
    broker::{DataCommand, DataResponse, WriterFence},
    gateway::Access,
    metadata::{self, ConsumerSubscription, PartitionKey, Route},
    ownership::{FinalizationKind, Work},
    subscription::{
        self, AckDisposition, DeliveryProof, GroupCommand, GroupIdentity, GroupOutcome, MemberGrant,
    },
};

pub(super) const GROUP_CONTROL: u32 = 1_003;

#[cfg(test)]
#[path = "../../tests/runtime_support/consumer_control_semantic_local.rs"]
mod local_tests;

#[cfg(test)]
mod frame_tests {
    use super::*;
    use tidemq_protocol::topic::{HashRange, STREAM_FENCE_COUNT, STREAM_FENCE_SHIFT};

    #[test]
    fn maximum_member_install_fits_the_internal_control_frame() {
        let id = Id::new_v7().unwrap();
        let grants = (0..STREAM_FENCE_COUNT)
            .map(|index| MemberGrant {
                member: Id::new_v7().unwrap(),
                client: id,
                credential: id,
                epoch: u64::MAX,
                expires_ms: u64::MAX,
                max_messages: 256,
                max_bytes: 8 * 1024 * 1024,
                lanes: vec![HashRange {
                    start: (index as u64) << STREAM_FENCE_SHIFT,
                    end: if index + 1 == STREAM_FENCE_COUNT {
                        None
                    } else {
                        Some(((index + 1) as u64) << STREAM_FENCE_SHIFT)
                    },
                }],
            })
            .collect();
        let request = Request::typed(
            GROUP_CONTROL,
            &Control::Install {
                partition: PartitionKey {
                    topic_id: id,
                    partition: u32::MAX,
                },
                shard: id,
                subscription: id,
                model: SubscriptionKind::Stream,
                initial: InitialPosition::EarliestAvailable,
                version: u64::MAX,
                grants,
                blocked: true,
            },
        )
        .unwrap();
        let limits = tidemq_protocol::ProtocolLimits::default();
        let envelope = tidemq_protocol::internal::InternalCommand {
            group_id: u64::MAX,
            operation_id: Bytes::copy_from_slice(id.as_bytes()),
            encoded_command: tidemq_protocol::framing::encode_payload(
                &request,
                limits.max_control_frame_bytes,
            )
            .unwrap(),
        };
        tidemq_protocol::framing::new_frame(
            FrameKind::InternalCommand,
            tidemq_protocol::common::TrafficClass::Control,
            id,
            id,
            &envelope,
            30_000,
            &limits,
        )
        .unwrap();
    }
}

#[derive(Serialize, Deserialize)]
enum Control {
    Delete {
        partition: PartitionKey,
        domain: Id,
        subscription: Id,
        deletion: Id,
    },
    Ensure {
        partition: PartitionKey,
        shard: Id,
        subscription: Id,
        model: SubscriptionKind,
        initial: InitialPosition,
        blocked: bool,
    },
    Inspect {
        partition: PartitionKey,
        subscription: Id,
    },
    OpenGate {
        partition: PartitionKey,
        subscription: Id,
    },
    Install {
        partition: PartitionKey,
        shard: Id,
        subscription: Id,
        model: SubscriptionKind,
        initial: InitialPosition,
        version: u64,
        grants: Vec<MemberGrant>,
        blocked: bool,
    },
    Renew {
        partition: PartitionKey,
        subscription: Id,
        member: Id,
        epoch: u64,
        version: u64,
        expires_ms: u64,
    },
}

#[derive(Serialize, Deserialize)]
struct Progress {
    frontier: u64,
    gate_open: bool,
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
struct Deleted {
    subscription: Id,
    deletion: Id,
    domain: Id,
}

enum ControlReply {
    Applied,
    Deleted(Deleted),
    Progress(Progress),
}

impl Control {
    fn validate_local_bound(&self) -> Result<()> {
        if let Self::Install { grants, .. } = self {
            let bytes = grants
                .iter()
                .try_fold(std::mem::size_of::<Self>(), |bytes, grant| {
                    grant
                        .lanes
                        .len()
                        .checked_mul(std::mem::size_of::<tidemq_protocol::topic::HashRange>())
                        .and_then(|lanes| lanes.checked_add(std::mem::size_of::<MemberGrant>()))
                        .and_then(|grant| bytes.checked_add(grant))
                });
            if bytes.is_none_or(|bytes| bytes > 65_536) {
                return Err(Error::invalid(
                    "Local consumer control exceeds its memory bound",
                ));
            }
        }
        Ok(())
    }
}

impl Service {
    pub(super) async fn delete_consumer_domain(
        &self,
        plan: &metadata::ConsumerDeletion,
        domain: Id,
        deadline: Instant,
    ) -> Result<()> {
        let permit = self
            .consumer_deletion_permit(plan.subscription, plan.id, domain, deadline)
            .await?;
        let route = self.ensure_route(permit.route.partition, deadline).await?;
        let expected = Deleted {
            subscription: plan.subscription,
            deletion: plan.id,
            domain,
        };
        let reply = self
            .consumer_control(
                route.group_id,
                Control::Delete {
                    partition: route.partition,
                    domain,
                    subscription: plan.subscription,
                    deletion: plan.id,
                },
                deadline,
            )
            .await?;
        if !matches!(reply, ControlReply::Deleted(reply) if reply == expected) {
            return Err(Error::invalid(
                "Data deletion result differs from its durable plan",
            ));
        }
        Ok(())
    }

    pub(super) async fn install_consumer_domain(
        &self,
        row: &ConsumerSubscription,
        domain: Id,
        version: u64,
        grants: Vec<MemberGrant>,
        deadline: Instant,
    ) -> Result<()> {
        let shard = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.topology().shard(domain).cloned())
            .await
            .map_err(|source| Error::operation("read consumer birth domain", source))?
            .ok_or_else(super::metadata::not_found)?;
        let route = self.ensure_route(shard.partition, deadline).await?;
        self.consumer_control(
            route.group_id,
            Control::Install {
                partition: shard.partition,
                shard: domain,
                subscription: row.id,
                model: row.members.model(),
                initial: row.members.initial_position(),
                version,
                grants,
                blocked: row.members.model() == SubscriptionKind::Stream
                    && !shard.predecessors.is_empty(),
            },
            deadline,
        )
        .await
        .map_err(|error| match error {
            Error::Rejected { failure }
                if failure.code == i32::from(ErrorCode::StaleGeneration) =>
            {
                Error::unavailable()
            }
            error => error,
        })?;
        Ok(())
    }

    pub(super) async fn refresh_consumer_grant(
        &self,
        access: &Access,
        route: &Route,
        subscription: Id,
        member: (Id, u64),
        deadline: Instant,
    ) -> Result<()> {
        let (member, epoch) = member;
        let now = super::data::unix_millis()?;
        let version = self
            .data
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state
                    .consumer_group_cursor(route.partition, subscription)
                    .and_then(|cursor| cursor.renewal_version(member, epoch, now))
            })
            .await
            .map_err(|source| Error::operation("read consumer grant expiry", source))?;
        if let Some(version) = version {
            let (client, credential) = super::consumer_control::actor(access)?;
            let expires_ms = self
                .consumer_member_lease(
                    super::auth::MemberLeaseRequest {
                        topic: route.partition.topic_id,
                        subscription,
                        member,
                        epoch,
                        version,
                        client,
                        credential,
                    },
                    deadline,
                )
                .await?;
            if expires_ms > now {
                self.group_commit(
                    route,
                    GroupCommand::Renew {
                        subscription,
                        member,
                        epoch,
                        version,
                        expires_ms,
                        now_ms: super::data::unix_millis()?,
                    },
                    deadline,
                )
                .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn group_control_command(
        &self,
        group: u64,
        body: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let control: Control = wire::decode_binary(body)?;
        match self.execute_group_control(group, control, deadline).await? {
            ControlReply::Applied => wire::binary(&()),
            ControlReply::Deleted(reply) => wire::binary(&reply),
            ControlReply::Progress(reply) => wire::binary(&reply),
        }
    }

    async fn consumer_control(
        &self,
        group: u64,
        control: Control,
        deadline: Instant,
    ) -> Result<ControlReply> {
        let target = self.leader(group, deadline).await?;
        if self.rpc.is_local() {
            use tracing::Instrument;
            let operation = Id::new_v7().map_err(|source| {
                Error::operation("allocate local consumer control identity", source)
            })?;
            return wire::within(
                deadline,
                Box::pin(
                    self.execute_group_control(group, control, deadline)
                        .instrument(
                            tracing::info_span!(target: "tidemq_trace", "consumer_control_local",
                    node_id = self.config.node_id, group_id = group, operation_id = %operation),
                        ),
                ),
            )
            .await?;
        }
        let bytes = self
            .rpc
            .call(
                target,
                group,
                Request::typed(GROUP_CONTROL, &control)?,
                deadline,
            )
            .await?;
        match control {
            Control::Delete { .. } => wire::decode_binary(&bytes).map(ControlReply::Deleted),
            Control::Inspect { .. } => wire::decode_binary(&bytes).map(ControlReply::Progress),
            _ => wire::decode_binary::<()>(&bytes).map(|()| ControlReply::Applied),
        }
    }

    async fn execute_group_control(
        &self,
        group: u64,
        control: Control,
        deadline: Instant,
    ) -> Result<ControlReply> {
        if self.rpc.is_local() {
            control.validate_local_bound()?;
        }
        let _admission = self.rpc.admit_local(
            group,
            crate::transport::StreamPurpose::Request,
            65_536,
            deadline,
        )?;
        wire::remaining(deadline)?;
        let partition = match &control {
            Control::Install { partition, .. }
            | Control::Renew { partition, .. }
            | Control::Ensure { partition, .. }
            | Control::Delete { partition, .. }
            | Control::Inspect { partition, .. }
            | Control::OpenGate { partition, .. } => *partition,
        };
        let lease = self.data_lease(group, partition, deadline).await?;
        let grant = lease.grant();
        let route = Route {
            partition,
            group_id: group,
            owner_node: grant.owner_node,
            epoch: grant.epoch,
            generation: grant.expected_route_generation.saturating_add(1),
            ownership_operation: grant.operation_id,
        };
        match control {
            Control::Delete {
                subscription,
                deletion,
                domain,
                ..
            } => {
                let permit = self
                    .consumer_deletion_permit(subscription, deletion, domain, deadline)
                    .await?;
                if permit.route != route {
                    return Err(forbidden());
                }
                let outcome = self
                    .group_commit(
                        &route,
                        GroupCommand::Delete {
                            subscription,
                            deletion,
                            now_ms: permit.now_ms,
                        },
                        deadline,
                    )
                    .await?;
                if !matches!(outcome, GroupOutcome::Deleted { deletion: recorded } if recorded == deletion)
                {
                    return Err(Error::invalid(
                        "Data deletion did not return its durable tombstone",
                    ));
                }
                return Ok(ControlReply::Deleted(Deleted {
                    subscription,
                    deletion,
                    domain,
                }));
            }
            Control::Inspect { subscription, .. } => {
                let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
                replica
                    .group
                    .ensure_read(deadline)
                    .await
                    .map_err(crate::runtime::wire::execution_error)?;
                let progress = replica
                    .state
                    .try_with_application(|state| {
                        state
                            .consumer_group_cursor(partition, subscription)
                            .map(|cursor| Progress {
                                frontier: cursor.acknowledged_before(),
                                gate_open: cursor.gate_open(),
                            })
                    })
                    .await
                    .map_err(|source| {
                        Error::operation("read durable predecessor progress", source)
                    })?
                    .ok_or_else(super::metadata::not_found)?;
                return Ok(ControlReply::Progress(progress));
            }
            Control::OpenGate { subscription, .. } => {
                self.group_commit(&route, GroupCommand::OpenGate { subscription }, deadline)
                    .await?;
            }
            Control::Ensure {
                shard,
                subscription,
                model,
                initial,
                blocked,
                ..
            } => {
                self.ensure_group_cursor(
                    &route,
                    shard,
                    subscription,
                    model,
                    initial,
                    blocked,
                    deadline,
                )
                .await?;
            }
            Control::Install {
                shard,
                subscription,
                model,
                initial,
                version,
                grants,
                blocked,
                ..
            } => {
                self.ensure_group_cursor(
                    &route,
                    shard,
                    subscription,
                    model,
                    initial,
                    blocked,
                    deadline,
                )
                .await?;
                self.group_commit(
                    &route,
                    GroupCommand::Reap {
                        subscription,
                        now_ms: super::data::unix_millis()?,
                        recovery_ms: metadata::MEMBER_RECOVERY_MS,
                    },
                    deadline,
                )
                .await?;
                self.group_commit(
                    &route,
                    GroupCommand::InstallMembers {
                        subscription,
                        version,
                        grants,
                        now_ms: super::data::unix_millis()?,
                    },
                    deadline,
                )
                .await?;
            }
            Control::Renew {
                subscription,
                member,
                epoch,
                version,
                expires_ms,
                ..
            } => {
                self.group_commit(
                    &route,
                    GroupCommand::Reap {
                        subscription,
                        now_ms: super::data::unix_millis()?,
                        recovery_ms: metadata::MEMBER_RECOVERY_MS,
                    },
                    deadline,
                )
                .await?;
                self.group_commit(
                    &route,
                    GroupCommand::Renew {
                        subscription,
                        member,
                        epoch,
                        version,
                        expires_ms,
                        now_ms: super::data::unix_millis()?,
                    },
                    deadline,
                )
                .await?;
            }
        }
        Ok(ControlReply::Applied)
    }

    // The complete creation context binds one data domain and its immutable start policy.
    #[allow(clippy::too_many_arguments)]
    async fn ensure_group_cursor(
        &self,
        route: &Route,
        shard: Id,
        subscription: Id,
        model: SubscriptionKind,
        initial: InitialPosition,
        blocked: bool,
        deadline: Instant,
    ) -> Result<()> {
        #[cfg(feature = "test-hooks")]
        if super::test_hooks::reject_consumer_creation(route.partition) {
            return Err(Error::unavailable());
        }
        let replica = self
            .data
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?;
        let start = replica
            .state
            .try_with_application(|state| {
                let producer = state
                    .producer(route.partition)
                    .ok_or_else(super::metadata::not_found)?;
                Ok::<_, Error>(match initial {
                    InitialPosition::EarliestAvailable => producer.retained_floor(),
                    InitialPosition::Latest => producer.committed_end(),
                })
            })
            .await
            .map_err(|source| Error::operation("capture consumer start", source))??;
        let mut signing_key = [0; 32];
        getrandom::fill(&mut signing_key)
            .map_err(|source| Error::operation("generate confirmation key", source))?;
        self.group_commit(
            route,
            GroupCommand::Create {
                identity: GroupIdentity {
                    topic: route.partition.topic_id,
                    shard,
                    subscription,
                },
                model,
                start,
                signing_key: signing_key
                    .try_into()
                    .map_err(super::data::subscription_error)?,
                blocked,
            },
            deadline,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn protect_layout_successors(
        &self,
        plan: &metadata::LayoutPlan,
        deadline: Instant,
    ) -> Result<()> {
        let rows = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state
                    .consumer_subscriptions()
                    .filter(|row| row.topic == plan.topic)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(|source| Error::operation("read subscription protections", source))?;
        for shard in &plan.successors {
            let route = self.ensure_route(shard.partition, deadline).await?;
            for row in &rows {
                self.consumer_control(
                    route.group_id,
                    Control::Ensure {
                        partition: shard.partition,
                        shard: shard.route.id,
                        subscription: row.id,
                        model: row.members.model(),
                        initial: InitialPosition::EarliestAvailable,
                        blocked: row.members.model() == SubscriptionKind::Stream,
                    },
                    deadline,
                )
                .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn open_consumer_gates(
        &self,
        row: &ConsumerSubscription,
        deadline: Instant,
    ) -> Result<()> {
        if row.members.model() != SubscriptionKind::Stream {
            return Ok(());
        }
        let shards = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state
                    .topology()
                    .domains(row.topic)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(|source| Error::operation("read predecessor barriers", source))?;
        let pending: Vec<_> = shards
            .iter()
            .filter(|shard| {
                !shard.predecessors.is_empty() && !row.open_gates.contains(&shard.route.id)
            })
            .collect();
        let start = (super::data::unix_millis()? / 100) as usize % pending.len().max(1);
        for shard in pending
            .iter()
            .cycle()
            .skip(start)
            .take(pending.len().min(4))
        {
            let progress = self
                .consumer_domain_progress(shard, row.id, deadline)
                .await?;
            if progress.gate_open {
                self.record_open_consumer_gate(row.id, shard.route.id, deadline)
                    .await?;
                continue;
            }
            let mut complete = true;
            for predecessor in &shard.predecessors {
                let parent = shards
                    .iter()
                    .find(|candidate| candidate.route.id == *predecessor)
                    .ok_or_else(Error::unavailable)?;
                let progress = self
                    .consumer_domain_progress(parent, row.id, deadline)
                    .await?;
                complete &= parent
                    .sealed_end
                    .is_some_and(|end| progress.frontier >= end);
            }
            if complete {
                self.consumer_control(
                    shard.route.group,
                    Control::OpenGate {
                        partition: shard.partition,
                        subscription: row.id,
                    },
                    deadline,
                )
                .await?;
                self.record_open_consumer_gate(row.id, shard.route.id, deadline)
                    .await?;
            }
        }
        Ok(())
    }

    async fn consumer_domain_progress(
        &self,
        shard: &metadata::BirthShard,
        subscription: Id,
        deadline: Instant,
    ) -> Result<Progress> {
        let reply = self
            .consumer_control(
                shard.route.group,
                Control::Inspect {
                    partition: shard.partition,
                    subscription,
                },
                deadline,
            )
            .await?;
        match reply {
            ControlReply::Progress(progress) => Ok(progress),
            _ => Err(Error::invalid(
                "Consumer progress returned an invalid reply",
            )),
        }
    }

    async fn record_open_consumer_gate(
        &self,
        subscription: Id,
        domain: Id,
        deadline: Instant,
    ) -> Result<()> {
        self.consumer_meta_write(
            Id::new_v7().map_err(|source| {
                Error::operation("allocate predecessor completion observation", source)
            })?,
            metadata::ConsumerCommand::OpenedGate {
                subscription,
                domain,
            },
            deadline,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn group_commit(
        &self,
        route: &Route,
        command: GroupCommand,
        deadline: Instant,
    ) -> Result<GroupOutcome> {
        if let GroupCommand::Read {
            subscription,
            request,
            cold,
        } = &command
        {
            let demand = self
                .data
                .get(&route.group_id)
                .ok_or_else(Error::unavailable)?
                .state
                .try_with_application(|state| {
                    state.read_quota_demand(route.partition, *subscription, request, cold)
                })
                .await
                .map_err(|source| Error::operation("reserve tenant delivery credit", source))?
                .map_err(super::data::data_rejection)?;
            if let Some(demand) = demand {
                self.ensure_tenant_quota(route.group_id, route.partition, demand, deadline)
                    .await?;
            }
        }
        if let GroupCommand::Create {
            start, identity, ..
        } = &command
        {
            let replica = self
                .data
                .get(&route.group_id)
                .ok_or_else(Error::unavailable)?;
            let demand = replica
                .state
                .try_with_application(|state| {
                    if state
                        .consumer_group_cursor(route.partition, identity.subscription)
                        .is_some()
                    {
                        return Ok(None);
                    }
                    state
                        .protection_quota_demand(route.partition, *start)
                        .map(Some)
                })
                .await
                .map_err(|source| {
                    Error::operation("reserve tenant subscription protection", source)
                })?
                .map_err(super::data::data_rejection)?;
            if let Some(demand) = demand {
                self.ensure_tenant_quota(route.group_id, route.partition, demand, deadline)
                    .await?;
            }
        }
        let lease = self
            .data_lease(route.group_id, route.partition, deadline)
            .await?;
        let replica = self
            .data
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?;
        let fence = replica
            .state
            .try_with_application(|state| {
                state
                    .ownership()
                    .and_then(|ownership| ownership.fence(route.partition))
                    .cloned()
            })
            .await
            .map_err(|source| Error::operation("read consumer authority fence", source))?
            .ok_or_else(Error::unavailable)?;
        lease
            .check(&fence, Work::ControlMutation)
            .map_err(|_| Error::unavailable())?;
        let finalization = if matches!(
            command,
            GroupCommand::Acknowledge { .. } | GroupCommand::RetireAck { .. }
        ) {
            Some(
                lease
                    .begin_finalization(&fence, FinalizationKind::Ack, deadline.into_std())
                    .map_err(|_| Error::unavailable())?,
            )
        } else {
            None
        };
        let write_deadline = if finalization.is_some() {
            deadline
        } else {
            deadline.min(lease.expires_at().into())
        };
        let original_ack = match &command {
            GroupCommand::Acknowledge {
                subscription,
                request,
            } => Some((*subscription, request.clone())),
            _ => None,
        };
        #[cfg(test)]
        let commit_point = match &command {
            GroupCommand::OpenGate { .. } => Some(super::data::consumer_tests::CommitPoint::Gate),
            GroupCommand::Read { .. } => Some(super::data::consumer_tests::CommitPoint::Read),
            GroupCommand::RetireRead { .. } => {
                Some(super::data::consumer_tests::CommitPoint::RetireRead)
            }
            _ => None,
        };
        #[cfg(test)]
        super::data::control_lease_tests::before_group_write(
            route.partition,
            &command,
            write_deadline,
        )
        .await;
        let result = self
            .data_write(
                route.group_id,
                DataCommand::ConsumerGroup {
                    partition: route.partition,
                    writer: WriterFence {
                        node_id: lease.grant().owner_node,
                        owner_epoch: lease.grant().epoch,
                    },
                    command,
                },
                write_deadline,
            )
            .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                if let Some((subscription, request)) = original_ack {
                    let original = replica
                        .state
                        .try_with_application(|state| {
                            state
                                .consumer_group_cursor(route.partition, subscription)
                                .and_then(|cursor| cursor.original_ack_outcome(&request))
                        })
                        .await
                        .map_err(|source| {
                            Error::operation("read original acknowledgement outcome", source)
                        })?;
                    if let Some(Err(reason)) = original {
                        let mut error = super::data::subscription_error(reason);
                        if let Error::Rejected { failure } = &mut error {
                            failure.original_outcome_known = true;
                        }
                        return Err(error);
                    }
                }
                return Err(error);
            }
        };
        #[cfg(test)]
        if let Some(point) = commit_point {
            super::data::consumer_tests::after_group_commit(
                route.partition,
                lease.expires_at(),
                point,
                match &result {
                    DataResponse::ConsumerGroup(outcome) => Some(outcome.clone()),
                    _ => None,
                },
            )
            .await;
        }
        let current = replica
            .state
            .try_with_application(|state| {
                state
                    .ownership()
                    .and_then(|ownership| ownership.fence(route.partition))
                    .cloned()
            })
            .await
            .map_err(|source| Error::operation("read committed consumer authority", source))?
            .ok_or_else(Error::unknown)?;
        match finalization {
            Some(finalization) => finalization.check(&current),
            None => lease.check(&current, Work::ControlMutation),
        }
        .map_err(|source| Error::CommittedConsumerWriter { source })?;
        match result {
            DataResponse::ConsumerGroup(outcome) => Ok(outcome),
            _ => Err(Error::invalid(
                "Data group returned an invalid consumer outcome",
            )),
        }
    }

    pub(super) async fn group_data_public(
        &self,
        access: &Access,
        frame: &Frame,
        route: Route,
        deadline: Instant,
    ) -> Result<Frame> {
        // Cached routes may still name a reclaimed source; retry routing before looking up members.
        self.data_lease(route.group_id, route.partition, deadline)
            .await?;
        let kind = super::service::frame_kind(frame)?;
        #[cfg(feature = "test-hooks")]
        if kind == FrameKind::GroupAck {
            super::test_hooks::pause_consumer_authorization(route.partition, deadline).await;
        }
        match kind {
            FrameKind::GroupRead => self.group_read_public(access, frame, route, deadline).await,
            FrameKind::GroupAck => self.group_ack_public(access, frame, route, deadline).await,
            FrameKind::GroupRetireRead | FrameKind::GroupRetireAck => {
                let request: public::GroupRetire =
                    decode_payload(frame.payload.clone(), &self.config.protocol).map_err(
                        |source| Error::operation("decode consumer result retirement", source),
                    )?;
                let member = wire::id(&request.member_id)?;
                let subscription = wire::id(&request.subscription_id)?;
                if let Err(error) = self
                    .authorize_group_member(
                        access,
                        &route,
                        subscription,
                        member,
                        request.member_epoch,
                    )
                    .await
                {
                    if kind == FrameKind::GroupRetireRead
                        && matches!(
                            ErrorCode::try_from(error.failure().code),
                            Ok(ErrorCode::NotFound
                                | ErrorCode::StaleGeneration
                                | ErrorCode::Forbidden)
                        )
                        && self
                            .closed_read_is_absent(access, &route, &request, deadline)
                            .await?
                    {
                        return wire::response(
                            frame,
                            FrameKind::GroupRetired,
                            &public::GroupRetired {},
                            &self.config.protocol,
                        );
                    }
                    return Err(error);
                }
                let command = if kind == FrameKind::GroupRetireRead {
                    GroupCommand::RetireRead {
                        subscription,
                        member,
                        epoch: request.member_epoch,
                        sequence: request.sequence,
                    }
                } else {
                    GroupCommand::RetireAck {
                        subscription,
                        member,
                        epoch: request.member_epoch,
                        sequence: request.sequence,
                    }
                };
                self.group_commit(&route, command, deadline).await?;
                wire::response(
                    frame,
                    FrameKind::GroupRetired,
                    &public::GroupRetired {},
                    &self.config.protocol,
                )
            }
            _ => Err(forbidden()),
        }
    }

    async fn authorize_group_member(
        &self,
        access: &Access,
        route: &Route,
        subscription: Id,
        member: Id,
        epoch: u64,
    ) -> Result<SubscriptionKind> {
        let (client, credential) = super::consumer_control::actor(access)?;
        self.data
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                // Source retirement can race the earlier lease read; check the fence with the member.
                if state
                    .ownership()
                    .and_then(|ownership| ownership.fence(route.partition))
                    .is_none_or(|fence| {
                        fence.grant.group_id != route.group_id
                            || fence.grant.operation_id != route.ownership_operation
                    })
                {
                    return Err(Error::unavailable());
                }
                let cursor = state
                    .consumer_group_cursor(route.partition, subscription)
                    .ok_or_else(super::metadata::not_found)?;
                // A fenced member can be reaped before the original SDK handle rejoins.
                if !cursor.contains_member(member, epoch) {
                    return Err(super::data::subscription_error(
                        subscription::Rejection::SessionMismatch,
                    ));
                }
                if !cursor.authorize_member(member, epoch, client, credential) {
                    return Err(forbidden());
                }
                Ok(cursor.model())
            })
            .await
            .map_err(|source| Error::operation("authorize consumer data session", source))?
    }

    async fn closed_read_is_absent(
        &self,
        access: &Access,
        route: &Route,
        request: &public::GroupRetire,
        deadline: Instant,
    ) -> Result<bool> {
        let member = wire::id(&request.member_id)?;
        let subscription = wire::id(&request.subscription_id)?;
        let (client, credential) = super::consumer_control::actor(access)?;
        self.consumer_retirement_permit(
            super::auth::MemberLeaseRequest {
                topic: route.partition.topic_id,
                subscription,
                member,
                epoch: request.member_epoch,
                version: 0,
                client,
                credential,
            },
            deadline,
        )
        .await?;
        let replica = self
            .data
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
        // Metadata has fenced this exact owner; a reaped data record has no recoverable result left.
        replica
            .state
            .try_with_application(|state| {
                state
                    .consumer_group_cursor(route.partition, subscription)
                    .is_none_or(|cursor| !cursor.contains_member(member, request.member_epoch))
            })
            .await
            .map_err(|source| Error::operation("prove fenced consumer read is absent", source))
    }

    async fn group_read_public(
        &self,
        access: &Access,
        frame: &Frame,
        route: Route,
        deadline: Instant,
    ) -> Result<Frame> {
        let request: public::GroupRead =
            decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode group read", source))?;
        let subscription = wire::id(&request.subscription_id)?;
        let member = wire::id(&request.member_id)?;
        self.authorize_group_member(access, &route, subscription, member, request.member_epoch)
            .await?;
        self.refresh_consumer_grant(
            access,
            &route,
            subscription,
            (member, request.member_epoch),
            deadline,
        )
        .await?;
        if request.max_bytes > u64::from(self.config.protocol.max_frame_bytes.saturating_sub(8192))
        {
            return Err(Error::invalid("Group read exceeds its frame budget"));
        }
        if request.max_bytes < u64::from(self.config.protocol.max_message_bytes) + 256 {
            return Err(Error::invalid(
                "Group read bytes must accommodate a maximum-sized message and delivery metadata",
            ));
        }
        let window = (request.max_messages as usize, request.max_bytes);
        let (read, mut page) = self
            .read_consumer_page(
                &route,
                subscription,
                subscription::GroupRead {
                    member,
                    epoch: request.member_epoch,
                    sequence: request.sequence,
                    now_ms: super::data::unix_millis()?,
                    max_messages: request.max_messages,
                    max_bytes: request.max_bytes,
                },
                deadline,
            )
            .await?;
        let sealed_complete = self
            .data
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state.sealed_end(route.partition).is_some_and(|end| {
                    state
                        .consumer_group_cursor(route.partition, subscription)
                        .is_some_and(|cursor| cursor.acknowledged_before() >= end)
                })
            })
            .await
            .map_err(|source| Error::operation("read sealed subscription completion", source))?;
        let deliveries = async {
            let mut deliveries = Vec::new();
            for proof in read.deliveries {
                deliveries.push(
                    self.consumer_delivery(
                        &route,
                        subscription,
                        proof,
                        &mut page,
                        window,
                        deadline,
                    )
                    .await?,
                );
            }
            Ok::<_, Error>(deliveries)
        }
        .await;
        if let Some(page) = page {
            page.release(deadline.into_std())
                .await
                .map_err(|source| Error::operation("release consumer delivery pin", source))?;
        }
        let deliveries = deliveries?;
        #[cfg(feature = "test-hooks")]
        if super::consumer_read_test_hooks::suppress(self.cluster, route.partition, frame, &request)
        {
            return Err(Error::unknown());
        }
        #[cfg(feature = "test-hooks")]
        if super::test_hooks::lose_consumer_reply(route.partition) {
            return Err(Error::unknown());
        }
        wire::response(
            frame,
            FrameKind::GroupDeliveries,
            &public::GroupDeliveries {
                sealed_complete,
                sequence: read.sequence,
                assignment_version: read.assignment_version,
                deliveries,
            },
            &self.config.protocol,
        )
    }

    async fn group_ack_public(
        &self,
        access: &Access,
        frame: &Frame,
        route: Route,
        deadline: Instant,
    ) -> Result<Frame> {
        let request: public::GroupAck =
            decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode group acknowledgement", source))?;
        let subscription = wire::id(&request.subscription_id)?;
        let member = wire::id(&request.member_id)?;
        let model = self
            .authorize_group_member(access, &route, subscription, member, request.member_epoch)
            .await?;
        self.refresh_consumer_grant(
            access,
            &route,
            subscription,
            (member, request.member_epoch),
            deadline,
        )
        .await?;
        let proofs =
            request
                .proofs
                .into_iter()
                .map(|proof| {
                    Ok(DeliveryProof {
                        offset: proof.offset,
                        member,
                        epoch: request.member_epoch,
                        attempt: proof.attempt,
                        mac: proof.mac.as_ref().try_into().map_err(|_| {
                            Error::invalid("Confirmation MAC must contain 32 bytes")
                        })?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
        let result = self
            .group_commit(
                &route,
                GroupCommand::Acknowledge {
                    subscription,
                    request: subscription::GroupAck {
                        member,
                        epoch: request.member_epoch,
                        sequence: request.sequence,
                        now_ms: super::data::unix_millis()?,
                        model,
                        proofs,
                        nack_delay_ms: request.nack_delay_ms,
                    },
                },
                deadline,
            )
            .await?;
        let GroupOutcome::Acknowledged(disposition) = result else {
            return Err(Error::invalid("Acknowledgement returned an invalid result"));
        };
        let (redelivery_scheduled, due_ms) = match disposition {
            AckDisposition::Acknowledged => (false, 0),
            AckDisposition::RedeliveryScheduled { due_ms } => (true, due_ms),
        };
        wire::response(
            frame,
            FrameKind::GroupAckResult,
            &public::GroupAckResult {
                redelivery_scheduled,
                due_ms,
            },
            &self.config.protocol,
        )
    }
}
