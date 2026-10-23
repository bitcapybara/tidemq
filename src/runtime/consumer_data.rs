use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::{
    Id,
    common::{Frame, FrameKind},
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

#[derive(Serialize, Deserialize)]
enum Control {
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
        expires_ms: u64,
    },
}

#[derive(Serialize, Deserialize)]
struct Progress {
    frontier: u64,
    gate_open: bool,
}

struct GrantRenewal {
    subscription: Id,
    member: Id,
    epoch: u64,
    expires_ms: u64,
}

impl Service {
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
        let target = self.leader(route.group_id, deadline).await?;
        self.rpc
            .call(
                target,
                route.group_id,
                Request::typed(
                    GROUP_CONTROL,
                    &Control::Install {
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
                )?,
                deadline,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn renew_consumer_grants(
        &self,
        subscription: Id,
        member: Id,
        epoch: u64,
        deadline: Instant,
    ) -> Result<()> {
        let row = self.consumer_row(subscription).await?;
        let expires_ms = row
            .members
            .member(member, epoch)
            .ok_or_else(super::metadata::not_found)?
            .expires_ms;
        let renewal = GrantRenewal {
            subscription,
            member,
            epoch,
            expires_ms,
        };
        for domains in row.members.plan().domains.chunks(4) {
            let results = tokio::join!(
                self.renew_consumer_domain(domains.first(), &renewal, deadline),
                self.renew_consumer_domain(domains.get(1), &renewal, deadline),
                self.renew_consumer_domain(domains.get(2), &renewal, deadline),
                self.renew_consumer_domain(domains.get(3), &renewal, deadline),
            );
            results.0?;
            results.1?;
            results.2?;
            results.3?;
        }
        Ok(())
    }

    async fn renew_consumer_domain(
        &self,
        domain: Option<&Id>,
        renewal: &GrantRenewal,
        deadline: Instant,
    ) -> Result<()> {
        let Some(domain) = domain else {
            return Ok(());
        };
        let shard = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.topology().shard(*domain).cloned())
            .await
            .map_err(|source| Error::operation("read renewal domain", source))?
            .ok_or_else(super::metadata::not_found)?;
        let target = self.leader(shard.route.group, deadline).await?;
        self.rpc
            .call(
                target,
                shard.route.group,
                Request::typed(
                    GROUP_CONTROL,
                    &Control::Renew {
                        partition: shard.partition,
                        subscription: renewal.subscription,
                        member: renewal.member,
                        epoch: renewal.epoch,
                        expires_ms: renewal.expires_ms,
                    },
                )?,
                deadline,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn group_control_command(
        &self,
        group: u64,
        body: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let control: Control = wire::decode_binary(body)?;
        let partition = match &control {
            Control::Install { partition, .. }
            | Control::Renew { partition, .. }
            | Control::Ensure { partition, .. }
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
            Control::Inspect { subscription, .. } => {
                let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
                wire::within(deadline, replica.raft.ensure_linearizable())
                    .await?
                    .map_err(|_| Error::unavailable())?;
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
                return wire::binary(&progress);
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
                        expires_ms,
                        now_ms: super::data::unix_millis()?,
                    },
                    deadline,
                )
                .await?;
            }
        }
        wire::binary(&())
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
            let target = self.leader(route.group_id, deadline).await?;
            for row in &rows {
                self.rpc
                    .call(
                        target,
                        route.group_id,
                        Request::typed(
                            GROUP_CONTROL,
                            &Control::Ensure {
                                partition: shard.partition,
                                shard: shard.route.id,
                                subscription: row.id,
                                model: row.members.model(),
                                initial: InitialPosition::EarliestAvailable,
                                blocked: row.members.model() == SubscriptionKind::Stream,
                            },
                        )?,
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
                let target = self.leader(shard.route.group, deadline).await?;
                self.rpc
                    .call(
                        target,
                        shard.route.group,
                        Request::typed(
                            GROUP_CONTROL,
                            &Control::OpenGate {
                                partition: shard.partition,
                                subscription: row.id,
                            },
                        )?,
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
        let target = self.leader(shard.route.group, deadline).await?;
        let bytes = self
            .rpc
            .call(
                target,
                shard.route.group,
                Request::typed(
                    GROUP_CONTROL,
                    &Control::Inspect {
                        partition: shard.partition,
                        subscription,
                    },
                )?,
                deadline,
            )
            .await?;
        wire::decode_binary(&bytes)
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

    async fn group_commit(
        &self,
        route: &Route,
        command: GroupCommand,
        deadline: Instant,
    ) -> Result<GroupOutcome> {
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
        .map_err(|_| Error::unknown())?;
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
        let kind = super::service::frame_kind(frame)?;
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
                self.authorize_group_member(
                    access,
                    &route,
                    subscription,
                    member,
                    request.member_epoch,
                )
                .await?;
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
                let cursor = state
                    .consumer_group_cursor(route.partition, subscription)
                    .ok_or_else(super::metadata::not_found)?;
                if !cursor.authorize_member(member, epoch, client, credential) {
                    return Err(forbidden());
                }
                Ok(cursor.model())
            })
            .await
            .map_err(|source| Error::operation("authorize consumer data session", source))?
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
        if request.max_bytes > u64::from(self.config.protocol.max_frame_bytes.saturating_sub(8192))
        {
            return Err(Error::invalid("Group read exceeds its frame budget"));
        }
        let result = self
            .group_commit(
                &route,
                GroupCommand::Read {
                    subscription,
                    request: subscription::GroupRead {
                        member,
                        epoch: request.member_epoch,
                        sequence: request.sequence,
                        now_ms: super::data::unix_millis()?,
                        max_messages: request.max_messages,
                        max_bytes: request.max_bytes,
                    },
                },
                deadline,
            )
            .await?;
        let GroupOutcome::Read(read) = result else {
            return Err(Error::invalid("Read returned an invalid result"));
        };
        let now = super::data::unix_millis()?;
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
        let deliveries = self
            .data
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let producer = state
                    .producer(route.partition)
                    .ok_or_else(super::metadata::not_found)?;
                read.deliveries
                    .into_iter()
                    .map(|proof| {
                        let message = producer
                            .hot_read(proof.offset, 1, request.max_bytes)
                            .map_err(|_| Error::unavailable())?
                            .messages
                            .into_iter()
                            .next()
                            .ok_or_else(Error::unavailable)?;
                        let metadata =
                            message
                                .metadata(route.partition.topic_id)
                                .map_err(|source| {
                                    Error::operation("read stored message context", source)
                                })?;
                        Ok(public::GroupDelivery {
                            key_present: metadata.key_present,
                            properties: metadata.properties,
                            current_attempt: state
                                .consumer_group_cursor(route.partition, subscription)
                                .is_some_and(|cursor| cursor.delivery_is_current(&proof, now)),
                            key_hash: metadata.routing_hash,
                            proof: Some(public::GroupProof {
                                offset: proof.offset,
                                attempt: proof.attempt,
                                mac: Bytes::copy_from_slice(&proof.mac),
                            }),
                            key: message.key,
                            body: message.body,
                        })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .await
            .map_err(|source| Error::operation("read original group delivery bodies", source))??;
        wire::response(
            frame,
            FrameKind::GroupDeliveries,
            &public::GroupDeliveries {
                sealed_complete,
                sequence: read.sequence,
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
