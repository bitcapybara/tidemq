use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use prost::Message;
use tidemq_protocol::common::{ErrorCode, Frame, FrameKind, Outcome, TrafficClass};
use tidemq_protocol::framing::{decode_payload, encode_payload};
use tidemq_protocol::public;
use tidemq_protocol::{Id, ProtocolLimits};
use tokio::time::Instant;

use crate::broker::{self, DataCommand, DataRejection, DataResponse, RequestContext, WriterFence};
use crate::gateway::Access;
use crate::metadata::{PartitionKey, Route};
use crate::ownership::remote::{Query, Value};
use crate::ownership::{ControlLease, FinalizationKind, Work};
use crate::producer::{AppendBatch, MessageData, RegisterProducer, SessionInfo};
use crate::server::SubmissionOrder;
use crate::subscription::{self, Assignment, DeliveryLane, Generations};

use super::metadata::{bytes_id, not_found};
use super::service::{Service, forbidden, frame_kind};
use super::wire;
use super::{Error, Result};

fn payload<T: Message + Default>(frame: &Frame, limits: &ProtocolLimits) -> Result<T> {
    decode_payload(frame.payload.clone(), limits)
        .map_err(|source| Error::operation("decode public business request", source))
}

pub(crate) fn partition(frame: &Frame, limits: &ProtocolLimits) -> Result<PartitionKey> {
    macro_rules! from {
        ($ty:ty) => {{
            let request: $ty = payload(frame, limits)?;
            PartitionKey {
                topic_id: wire::id(&request.topic_id)?,
                partition: request.partition_id,
            }
        }};
    }
    Ok(match frame_kind(frame)? {
        FrameKind::ProduceBatch => from!(public::ProduceBatch),
        FrameKind::OpenProducer => from!(public::OpenProducer),
        FrameKind::ProducerStatus => from!(public::ProducerStatus),
        FrameKind::RetireProducer => from!(public::RetireProducer),
        FrameKind::Read => from!(public::ReadRequest),
        FrameKind::ClaimAssignment => from!(public::ClaimAssignment),
        FrameKind::SubscriptionStatus => from!(public::SubscriptionStatus),
        FrameKind::AckBatch => from!(public::AckBatch),
        FrameKind::ReceiveBarrier => from!(public::ReceiveBarrier),
        FrameKind::SeekSubscription => from!(public::SeekSubscription),
        FrameKind::DeleteSubscription => from!(public::DeleteSubscription),
        _ => return Err(forbidden()),
    })
}

pub(crate) fn set_owner_epoch(
    frame: &mut Frame,
    epoch: u64,
    limits: &ProtocolLimits,
    external: bool,
) -> Result<()> {
    macro_rules! set {
        ($ty:ty) => {{
            let mut request: $ty = payload(frame, limits)?;
            if external && request.owner_epoch != 0 {
                return Err(forbidden());
            }
            if request.owner_epoch != 0 && request.owner_epoch != epoch {
                return Err(Error::reject(ErrorCode::Fenced, Outcome::Rejected, true));
            }
            request.owner_epoch = epoch;
            frame.payload = encode_payload(&request, limits.max_frame_bytes)
                .map_err(|source| Error::operation("bind fresh route epoch", source))?;
        }};
    }
    match frame_kind(frame)? {
        FrameKind::ProduceBatch => set!(public::ProduceBatch),
        FrameKind::OpenProducer => set!(public::OpenProducer),
        FrameKind::RetireProducer => set!(public::RetireProducer),
        FrameKind::ClaimAssignment => set!(public::ClaimAssignment),
        FrameKind::SeekSubscription => set!(public::SeekSubscription),
        FrameKind::DeleteSubscription => set!(public::DeleteSubscription),
        _ => {}
    }
    Ok(())
}

pub(crate) fn affinity(frame: &Frame, limits: &ProtocolLimits) -> Result<u64> {
    let mut hash = blake3::Hasher::new();
    match frame_kind(frame)? {
        FrameKind::ProduceBatch => {
            let request: public::ProduceBatch = payload(frame, limits)?;
            hash.update(&request.topic_id);
            hash.update(&request.partition_id.to_be_bytes());
            hash.update(&request.producer_id);
            hash.update(&request.producer_epoch.to_be_bytes());
        }
        FrameKind::Read => {
            let request: public::ReadRequest = payload(frame, limits)?;
            hash.update(&request.topic_id);
            hash.update(&request.partition_id.to_be_bytes());
            hash.update(&request.subscription_id);
            hash.update(&request.cursor_generation.to_be_bytes());
            hash.update(&request.assignment_generation.to_be_bytes());
        }
        _ => return Ok(0),
    }
    let mut bytes = [0; 8];
    for (destination, source) in bytes.iter_mut().zip(hash.finalize().as_bytes()) {
        *destination = *source;
    }
    Ok(u64::from_be_bytes(bytes))
}

impl Service {
    pub async fn data_lease(
        &self,
        group: u64,
        partition: PartitionKey,
        deadline: Instant,
    ) -> Result<Arc<ControlLease>> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        self.authoritative_group(group, deadline).await?;
        let fence = replica
            .state
            .try_with_application(|state| {
                state
                    .ownership()
                    .and_then(|state| state.fence(partition))
                    .cloned()
            })
            .await
            .map_err(|source| Error::operation("read authoritative data fence", source))?
            .ok_or_else(Error::unavailable)?;
        let slot = {
            let mut leases = self
                .leases
                .lock()
                .map_err(|_| Error::invalid("Lease cache is unavailable"))?;
            if leases.len() >= 2048 && !leases.contains_key(&partition) {
                leases.retain(|_, slot| {
                    Arc::strong_count(slot) > 1
                        || slot.try_lock().ok().is_none_or(|lease| {
                            lease
                                .as_ref()
                                .is_some_and(|lease| lease.expires_at() > std::time::Instant::now())
                        })
                });
            }
            if leases.len() >= 2048 && !leases.contains_key(&partition) {
                return Err(Error::reject(
                    ErrorCode::QuotaExceeded,
                    Outcome::Rejected,
                    true,
                ));
            }
            leases
                .entry(partition)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
                .clone()
        };
        let mut cached = wire::within(deadline, slot.lock()).await?;
        if let Some(lease) = cached
            .as_ref()
            .filter(|lease| lease.check(&fence, Work::ControlMutation).is_ok())
        {
            return Ok(lease.clone());
        }
        let lease = Arc::new(
            self.authority
                .active_lease(&fence, deadline)
                .await
                .map_err(|_| Error::unavailable())?,
        );
        *cached = Some(lease.clone());
        Ok(lease)
    }

    pub async fn data_public(
        &self,
        access: Access,
        frame: &Frame,
        route: Route,
        deadline: Instant,
        order: Option<SubmissionOrder>,
    ) -> Result<Frame> {
        let partition = route.partition;
        let lease = self.data_lease(route.group_id, partition, deadline).await?;
        if lease.grant().epoch != route.epoch || lease.grant().owner_node != route.owner_node {
            return Err(Error::reject(ErrorCode::Fenced, Outcome::Rejected, true));
        }
        let broker = self
            .brokers
            .get(&(route.group_id, route.owner_node))
            .ok_or_else(Error::unavailable)?;
        let context = RequestContext {
            operation_id: wire::id(&frame.operation_id)?,
            request_id: wire::id(&frame.request_id)?,
            attempt: 1,
            deadline: deadline.into_std(),
        };
        match frame_kind(frame)? {
            FrameKind::OpenProducer => {
                drop(order);
                let request: public::OpenProducer = payload(frame, &self.config.protocol)?;
                let info = broker
                    .register(
                        &lease,
                        context,
                        partition,
                        RegisterProducer {
                            producer_id: wire::id(&request.producer_id)?,
                            expected_registration_epoch: request.expected_registration_epoch,
                        },
                    )
                    .await
                    .map_err(broker_error)?;
                let next = broker
                    .producer_status(&lease, context, partition, None)
                    .await
                    .map_err(broker_error)?
                    .next_registration_epoch;
                wire::response(
                    frame,
                    FrameKind::ProducerSessionResult,
                    &session(info, next),
                    &self.config.protocol,
                )
            }
            FrameKind::ProducerStatus => {
                drop(order);
                let request: public::ProducerStatus = payload(frame, &self.config.protocol)?;
                let status = broker
                    .producer_status(
                        &lease,
                        context,
                        partition,
                        if request.producer_epoch == 0 {
                            None
                        } else {
                            Some((wire::id(&request.producer_id)?, request.producer_epoch))
                        },
                    )
                    .await
                    .map_err(broker_error)?;
                wire::response(
                    frame,
                    FrameKind::ProducerStatusResult,
                    &public::ProducerStatusResult {
                        state: if status.session.is_some() {
                            public::ProducerSessionState::Active.into()
                        } else {
                            public::ProducerSessionState::NotRegistered.into()
                        },
                        session: status
                            .session
                            .map(|info| session(info, status.next_registration_epoch)),
                        next_registration_epoch: status.next_registration_epoch,
                    },
                    &self.config.protocol,
                )
            }
            FrameKind::RetireProducer => {
                drop(order);
                let request: public::RetireProducer = payload(frame, &self.config.protocol)?;
                broker
                    .retire(
                        &lease,
                        context,
                        partition,
                        wire::id(&request.producer_id)?,
                        request.producer_epoch,
                    )
                    .await
                    .map_err(broker_error)?;
                wire::response(
                    frame,
                    FrameKind::ProducerRetired,
                    &public::ProducerRetired {
                        producer_id: request.producer_id,
                        producer_epoch: request.producer_epoch,
                        already_retired: false,
                    },
                    &self.config.protocol,
                )
            }
            FrameKind::ProduceBatch => {
                self.admit_produce()?;
                let request: public::ProduceBatch = payload(frame, &self.config.protocol)?;
                let digest = request
                    .content_digest
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::invalid("Producer digest must contain 32 bytes"))?;
                let request = AppendBatch {
                    topic_id: partition.topic_id,
                    partition_id: partition.partition,
                    producer_id: wire::id(&request.producer_id)?,
                    producer_epoch: request.producer_epoch,
                    first_sequence: request.first_sequence,
                    messages: request
                        .messages
                        .into_iter()
                        .map(|message| MessageData {
                            key: message.key,
                            body: message.body,
                        })
                        .collect(),
                    content_digest: digest,
                };
                let submitted = broker
                    .submit_produce(&lease, context, request)
                    .await
                    .map_err(broker_error)?;
                drop(order);
                let result = submitted.receive().await.map_err(broker_error)?;
                wire::response(
                    frame,
                    FrameKind::ProduceResult,
                    &public::ProduceResult {
                        first_offset: result.first_offset,
                        message_count: result.message_count,
                        duplicate: result.duplicate,
                    },
                    &self.config.protocol,
                )
            }
            _ => {
                drop(order);
                self.subscription_public(access, frame, route, &lease, deadline)
                    .await
            }
        }
    }

    pub async fn subscription_write(
        &self,
        group: u64,
        partition: PartitionKey,
        lease: &ControlLease,
        command: subscription::Command,
        deadline: Instant,
        finalizing: bool,
    ) -> Result<subscription::Outcome> {
        self.subscription_commit(group, partition, lease, command, deadline, finalizing, None)
            .await
    }

    // Cold delivery adds one sealed object proof to the same writer, deadline, and finalization checks.
    #[allow(clippy::too_many_arguments)]
    async fn subscription_commit(
        &self,
        group: u64,
        partition: PartitionKey,
        lease: &ControlLease,
        command: subscription::Command,
        deadline: Instant,
        finalizing: bool,
        cold: Option<crate::lifecycle::VerifiedColdDelivery>,
    ) -> Result<subscription::Outcome> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        let fence = replica
            .state
            .try_with_application(|state| {
                state
                    .ownership()
                    .and_then(|state| state.fence(partition))
                    .cloned()
            })
            .await
            .map_err(|source| Error::operation("read subscription writer fence", source))?
            .ok_or_else(Error::unavailable)?;
        lease
            .check(&fence, Work::ControlMutation)
            .map_err(|_| Error::unavailable())?;
        let finalization = if finalizing {
            Some(
                lease
                    .begin_finalization(&fence, FinalizationKind::Ack, deadline.into_std())
                    .map_err(|_| Error::unavailable())?,
            )
        } else {
            None
        };
        let deadline = if finalization.is_some() {
            deadline
        } else {
            deadline.min(Instant::from_std(lease.expires_at()))
        };
        let writer = WriterFence {
            node_id: lease.grant().owner_node,
            owner_epoch: lease.grant().epoch,
        };
        let command = if let Some(proof) = cold {
            DataCommand::ColdSubscription {
                partition,
                writer,
                proof,
                command,
            }
        } else {
            DataCommand::Subscription {
                partition,
                writer,
                command,
            }
        };
        let response = self.data_write(group, command, deadline).await?;
        let current = replica
            .state
            .try_with_application(|state| {
                state
                    .ownership()
                    .and_then(|state| state.fence(partition))
                    .cloned()
            })
            .await
            .map_err(|_| Error::unknown())?
            .ok_or_else(Error::unknown)?;
        let valid = if let Some(finalization) = finalization {
            finalization.check(&current)
        } else {
            lease.check(&current, Work::ControlMutation)
        };
        valid.map_err(|_| Error::unknown())?;
        match response {
            DataResponse::Subscription(outcome) => Ok(outcome),
            _ => Err(Error::invalid(
                "Data group returned an unexpected subscription outcome",
            )),
        }
    }

    async fn assignment(
        &self,
        group: u64,
        partition: PartitionKey,
        subscription: Id,
        deadline: Instant,
    ) -> Result<Assignment> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        replica
            .state
            .try_with_application(|state| {
                state
                    .subscriptions(partition)
                    .ok_or_else(not_found)?
                    .assignment(subscription)
                    .map_err(subscription_error)
            })
            .await
            .map_err(|source| Error::operation("read subscription assignment", source))?
    }

    async fn subscription_diagnostics(
        &self,
        group: u64,
        partition: PartitionKey,
        subscription: Id,
        deadline: Instant,
    ) -> Result<subscription::Diagnostics> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        replica
            .state
            .try_with_application(|state| {
                state
                    .subscriptions(partition)
                    .ok_or_else(not_found)?
                    .diagnostics(subscription)
                    .map_err(subscription_error)
            })
            .await
            .map_err(|source| Error::operation("read subscription cursor generations", source))?
    }

    async fn subscription_public(
        &self,
        _access: Access,
        frame: &Frame,
        route: Route,
        lease: &ControlLease,
        deadline: Instant,
    ) -> Result<Frame> {
        let group = route.group_id;
        let partition = route.partition;
        let outcome = match frame_kind(frame)? {
            FrameKind::ClaimAssignment => {
                let request: public::ClaimAssignment = payload(frame, &self.config.protocol)?;
                let subscription = wire::id(&request.subscription_id)?;
                self.require_subscription(subscription, partition, deadline)
                    .await?;
                let cold_before = if let Some(cold) = request.cold_before {
                    cold
                } else {
                    self.data
                        .get(&group)
                        .ok_or_else(Error::unavailable)?
                        .state
                        .try_with_application(|state| {
                            state
                                .subscriptions(partition)
                                .and_then(|subscriptions| {
                                    subscriptions.assignment(subscription).ok()
                                })
                                .filter(|assignment| {
                                    assignment.session_id.as_bytes() == request.session_id.as_ref()
                                })
                                .map(|assignment| assignment.cold_before)
                                .or_else(|| {
                                    state
                                        .producer(partition)
                                        .map(|producer| producer.committed_end())
                                })
                        })
                        .await
                        .map_err(|source| {
                            Error::operation("freeze delivery cutover boundary", source)
                        })?
                        .ok_or_else(not_found)?
                };
                let outcome = self
                    .subscription_write(
                        group,
                        partition,
                        lease,
                        subscription::Command::Claim {
                            subscription_id: subscription,
                            expected: Generations {
                                cursor: request.expected_cursor_generation,
                                assignment: request.expected_assignment_generation,
                            },
                            session_id: wire::id(&request.session_id)?,
                            replace_session_id: if request.replace_session_id.is_empty() {
                                None
                            } else {
                                Some(wire::id(&request.replace_session_id)?)
                            },
                            cold_before,
                        },
                        deadline,
                        false,
                    )
                    .await?;
                let subscription::Outcome::Assigned(assignment) = outcome else {
                    return Err(Error::invalid("Unexpected claim outcome"));
                };
                return wire::response(
                    frame,
                    FrameKind::AssignmentResult,
                    &assignment_wire(partition, subscription, assignment),
                    &self.config.protocol,
                );
            }
            FrameKind::SubscriptionStatus => {
                let request: public::SubscriptionStatus = payload(frame, &self.config.protocol)?;
                let subscription = wire::id(&request.subscription_id)?;
                self.require_subscription(subscription, partition, deadline)
                    .await?;
                let diagnostic = self
                    .subscription_diagnostics(group, partition, subscription, deadline)
                    .await?;
                let assigned = diagnostic.phase != subscription::Phase::Unassigned;
                let assignment = if assigned {
                    Some(assignment_wire(
                        partition,
                        subscription,
                        self.assignment(group, partition, subscription, deadline)
                            .await?,
                    ))
                } else {
                    Some(public::AssignmentResult {
                        subscription_id: bytes_id(subscription),
                        topic_id: bytes_id(partition.topic_id),
                        partition_id: partition.partition,
                        cursor_generation: diagnostic.generations.cursor,
                        assignment_generation: diagnostic.generations.assignment,
                        delivery_generation: diagnostic.generations.delivery(),
                        acknowledged_before: diagnostic.acknowledged_before,
                        next_delivery_offset: diagnostic.next_delivery_offset,
                        cold_before: diagnostic.cold_before,
                        received_before: diagnostic.received_before,
                        issued_before: diagnostic.issued_before,
                        session_id: Bytes::new(),
                    })
                };
                return wire::response(
                    frame,
                    FrameKind::SubscriptionStatusResult,
                    &public::SubscriptionStatusResult {
                        assigned,
                        assignment,
                    },
                    &self.config.protocol,
                );
            }
            FrameKind::Read => {
                let request: public::ReadRequest = payload(frame, &self.config.protocol)?;
                return self
                    .read_subscription(frame, request, route, lease, deadline)
                    .await;
            }
            FrameKind::AckBatch => {
                let request: public::AckBatch = payload(frame, &self.config.protocol)?;
                let subscription = wire::id(&request.subscription_id)?;
                self.require_subscription(subscription, partition, deadline)
                    .await?;
                let generations = check_generations(
                    request.cursor_generation,
                    request.assignment_generation,
                    request.delivery_generation,
                )?;
                self.check_session(
                    group,
                    partition,
                    subscription,
                    generations,
                    wire::id(&request.session_id)?,
                    deadline,
                )
                .await?;
                let tokens = request
                    .delivery_tokens
                    .into_iter()
                    .map(|token| {
                        subscription::DeliveryToken::try_from(token.to_vec())
                            .map_err(subscription_error)
                    })
                    .collect::<Result<_>>()?;
                self.subscription_write(
                    group,
                    partition,
                    lease,
                    subscription::Command::Ack {
                        subscription_id: subscription,
                        generations,
                        tokens,
                    },
                    deadline,
                    true,
                )
                .await?
            }
            FrameKind::ReceiveBarrier => {
                let request: public::ReceiveBarrier = payload(frame, &self.config.protocol)?;
                let subscription = wire::id(&request.subscription_id)?;
                self.require_subscription(subscription, partition, deadline)
                    .await?;
                let generations = check_generations(
                    request.cursor_generation,
                    request.assignment_generation,
                    request.delivery_generation,
                )?;
                let outcome = self
                    .subscription_write(
                        group,
                        partition,
                        lease,
                        subscription::Command::ReceiveBarrier {
                            subscription_id: subscription,
                            generations,
                            session_id: wire::id(&request.session_id)?,
                            delivery_generation: request.delivery_generation,
                            received_before: request.received_before,
                        },
                        deadline,
                        false,
                    )
                    .await?;
                let subscription::Outcome::ReceiveAdvanced(assignment) = outcome else {
                    return Err(Error::invalid("Unexpected receive barrier outcome"));
                };
                return wire::response(
                    frame,
                    FrameKind::AssignmentResult,
                    &assignment_wire(partition, subscription, assignment),
                    &self.config.protocol,
                );
            }
            FrameKind::SeekSubscription => {
                let request: public::SeekSubscription = payload(frame, &self.config.protocol)?;
                let subscription = wire::id(&request.subscription_id)?;
                self.require_subscription(subscription, partition, deadline)
                    .await?;
                let diagnostic = self
                    .subscription_diagnostics(group, partition, subscription, deadline)
                    .await?;
                if diagnostic.generations.cursor == request.expected_cursor_generation
                    && diagnostic.generations.assignment != request.expected_assignment_generation
                {
                    return Err(subscription_error(subscription::Rejection::StaleGeneration));
                }
                self.subscription_write(
                    group,
                    partition,
                    lease,
                    subscription::Command::Seek {
                        subscription_id: subscription,
                        operation_id: wire::id(&frame.operation_id)?,
                        expected_cursor_generation: request.expected_cursor_generation,
                        offset: request.target_offset,
                    },
                    deadline,
                    false,
                )
                .await?
            }
            FrameKind::DeleteSubscription => {
                let request: public::DeleteSubscription = payload(frame, &self.config.protocol)?;
                let subscription = wire::id(&request.subscription_id)?;
                self.require_subscription(subscription, partition, deadline)
                    .await?;
                match self
                    .subscription_diagnostics(group, partition, subscription, deadline)
                    .await
                {
                    Ok(diagnostic)
                        if diagnostic.generations.assignment
                            != request.expected_assignment_generation =>
                    {
                        return Err(subscription_error(subscription::Rejection::StaleGeneration));
                    }
                    Ok(_) => {}
                    Err(Error::Rejected { failure })
                        if failure.code == i32::from(ErrorCode::NotFound) => {}
                    Err(error) => return Err(error),
                }
                self.subscription_write(
                    group,
                    partition,
                    lease,
                    subscription::Command::Delete {
                        subscription_id: subscription,
                        expected_cursor_generation: request.expected_cursor_generation,
                    },
                    deadline,
                    false,
                )
                .await?
            }
            _ => return Err(forbidden()),
        };
        match outcome {
            subscription::Outcome::Acknowledged(receipt) => wire::response(
                frame,
                FrameKind::AckResult,
                &public::AckResult {
                    acknowledged_before: receipt.acknowledged_before,
                    accepted: receipt.accepted,
                    already_acknowledged: receipt.already_acknowledged,
                },
                &self.config.protocol,
            ),
            subscription::Outcome::Seeked(position) => wire::response(
                frame,
                FrameKind::SeekResult,
                &public::SeekResult {
                    cursor_generation: position.generations.cursor,
                    assignment_generation: position.generations.assignment,
                    delivery_generation: position.generations.delivery(),
                    acknowledged_before: position.acknowledged_before,
                },
                &self.config.protocol,
            ),
            subscription::Outcome::Deleted => {
                let request: public::DeleteSubscription = payload(frame, &self.config.protocol)?;
                wire::response(
                    frame,
                    FrameKind::SubscriptionDeleted,
                    &public::SubscriptionDeleted {
                        subscription_id: request.subscription_id,
                        already_deleted: false,
                    },
                    &self.config.protocol,
                )
            }
            _ => Err(Error::invalid("Unexpected subscription control outcome")),
        }
    }

    async fn require_subscription(
        &self,
        subscription: Id,
        partition: PartitionKey,
        deadline: Instant,
    ) -> Result<()> {
        {
            let cache = self
                .subscriptions
                .lock()
                .map_err(|_| Error::invalid("Subscription configuration cache is unavailable"))?;
            if let Some((topic, _)) = cache
                .get(&subscription)
                .filter(|(_, expires)| *expires > Instant::now())
            {
                if *topic == partition.topic_id {
                    return Ok(());
                }
                return Err(forbidden());
            }
        }
        let started = Instant::now();
        let Value::Subscription(Some(record)) = self
            .authority
            .query(Query::Subscription(subscription), deadline)
            .await?
        else {
            return Err(not_found());
        };
        if record.topic_id != partition.topic_id {
            return Err(forbidden());
        }
        let mut cache = self
            .subscriptions
            .lock()
            .map_err(|_| Error::invalid("Subscription configuration cache is unavailable"))?;
        if cache.len() >= 4096 && !cache.contains_key(&subscription) {
            cache.retain(|_, (_, expires)| *expires > Instant::now());
        }
        if cache.len() >= 4096 && !cache.contains_key(&subscription) {
            return Err(Error::reject(
                ErrorCode::QuotaExceeded,
                Outcome::Rejected,
                true,
            ));
        }
        cache.insert(
            subscription,
            (record.topic_id, started + std::time::Duration::from_secs(1)),
        );
        Ok(())
    }

    async fn check_session(
        &self,
        group: u64,
        partition: PartitionKey,
        subscription: Id,
        generations: Generations,
        session: Id,
        deadline: Instant,
    ) -> Result<Assignment> {
        let assignment = self
            .assignment(group, partition, subscription, deadline)
            .await?;
        if assignment.generations != generations || assignment.session_id != session {
            return Err(subscription_error(subscription::Rejection::StaleGeneration));
        }
        Ok(assignment)
    }

    async fn read_subscription(
        &self,
        frame: &Frame,
        mut request: public::ReadRequest,
        route: Route,
        lease: &ControlLease,
        deadline: Instant,
    ) -> Result<Frame> {
        let group = route.group_id;
        let partition = route.partition;
        let subscription = wire::id(&request.subscription_id)?;
        self.require_subscription(subscription, partition, deadline)
            .await?;
        let generations = check_generations(
            request.cursor_generation,
            request.assignment_generation,
            request.delivery_generation,
        )?;
        let session = wire::id(&request.session_id)?;
        let initial = self
            .check_session(
                group,
                partition,
                subscription,
                generations,
                session,
                deadline,
            )
            .await?;
        if request.max_messages == 0
            || request.max_messages > self.config.protocol.max_batch_messages
            || request.max_bytes < 256
            || request.max_bytes > self.config.protocol.max_frame_bytes
        {
            return Err(Error::invalid(
                "Read request exceeds its message or byte budget",
            ));
        }
        request.max_messages = request.max_messages.min(256);
        request.max_bytes = request
            .max_bytes
            .min(self.config.protocol.max_frame_bytes.saturating_sub(8192));
        let lane = if frame.traffic_class == i32::from(TrafficClass::Bulk) {
            DeliveryLane::Bulk
        } else {
            DeliveryLane::Realtime
        };
        if lane == DeliveryLane::Realtime && initial.received_before < initial.cold_before {
            return Err(subscription_error(subscription::Rejection::CutoverPending));
        }
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        let now = unix_millis()?;
        let retries = replica
            .state
            .try_with_application(|state| {
                state
                    .subscriptions(partition)
                    .ok_or_else(not_found)?
                    .due_retries(subscription, now, request.max_messages)
                    .map_err(subscription_error)
            })
            .await
            .map_err(|source| Error::operation("read bounded retry index", source))??;
        let mut candidates: Vec<(u64, u32)> = retries
            .into_iter()
            .filter(|retry| (retry.offset < initial.cold_before) == (lane == DeliveryLane::Bulk))
            .map(|retry| (retry.offset, retry.attempt))
            .collect();
        let mut next = initial.next_delivery_offset;
        while candidates.len() < request.max_messages as usize
            && (next < initial.cold_before) == (lane == DeliveryLane::Bulk)
        {
            candidates.push((next, 0));
            next = next
                .checked_add(1)
                .ok_or_else(|| Error::invalid("Delivery offset overflow"))?;
        }
        candidates.sort_unstable_by_key(|(offset, _)| *offset);
        let broker = self
            .brokers
            .get(&(group, route.owner_node))
            .ok_or_else(Error::unavailable)?;
        let mut deliveries = Vec::new();
        let mut cold_pages = Vec::new();
        let mut bytes = 0usize;
        for (offset, attempt) in candidates {
            let read = broker
                .hot_read(
                    lease,
                    RequestContext {
                        operation_id: wire::id(&frame.operation_id)?,
                        request_id: wire::id(&frame.request_id)?,
                        attempt: 1,
                        deadline: deadline.into_std(),
                    },
                    partition,
                    offset,
                    1,
                    request.max_bytes,
                )
                .await;
            let (message, proof, cold_page) = match read {
                Ok(read) => {
                    let Some(message) = read.page().messages.first().cloned() else {
                        break;
                    };
                    (message, None, None)
                }
                Err(broker::Error::Rejected {
                    reason:
                        DataRejection::Producer(crate::producer::Rejection::OffsetExpired { .. }),
                }) => {
                    let floor = replica
                        .state
                        .try_with_application(|state| {
                            state
                                .producer(partition)
                                .map(|producer| producer.retained_floor())
                        })
                        .await
                        .map_err(|source| {
                            Error::operation("read logical retention boundary", source)
                        })?
                        .ok_or_else(not_found)?;
                    if offset < floor {
                        return Err(Error::reject(
                            ErrorCode::OffsetExpired,
                            Outcome::Rejected,
                            false,
                        ));
                    }
                    let coordinator = self.objects.get(&group).ok_or_else(Error::unavailable)?;
                    let page = coordinator
                        .read(
                            partition,
                            offset,
                            1,
                            u64::from(request.max_bytes),
                            deadline.into_std(),
                        )
                        .await
                        .map_err(|source| Error::operation("read published cold object", source))?;
                    let message = {
                        let record = page
                            .records()
                            .map_err(|source| Error::operation("validate cold read pin", source))?
                            .first()
                            .ok_or_else(Error::unavailable)?;
                        if record.offset != offset {
                            return Err(Error::invalid(
                                "Cold object returned a different message offset",
                            ));
                        }
                        MessageData {
                            key: Bytes::copy_from_slice(record.key()),
                            body: Bytes::copy_from_slice(record.body()),
                        }
                    };
                    let proof = page
                        .verified_delivery(offset)
                        .map_err(|source| Error::operation("bind cold delivery proof", source))?;
                    (message, Some(proof), Some(page))
                }
                Err(broker::Error::Rejected {
                    reason:
                        DataRejection::Producer(crate::producer::Rejection::OffsetNotCommitted {
                            ..
                        }),
                }) => break,
                Err(error) => return Err(broker_error(error)),
            };
            let logical = message
                .logical_bytes()
                .and_then(|bytes| u32::try_from(bytes).ok())
                .ok_or_else(|| Error::invalid("Delivery message length overflow"))?;
            let next_bytes = bytes
                .checked_add(logical as usize)
                .and_then(|value| value.checked_add(160))
                .ok_or_else(|| Error::invalid("Delivery response length overflow"))?;
            if next_bytes > request.max_bytes as usize {
                break;
            }
            let operation = delivery_operation(wire::id(&frame.operation_id)?, offset)?;
            let outcome = self
                .subscription_commit(
                    group,
                    partition,
                    lease,
                    subscription::Command::RecordDelivery {
                        subscription_id: subscription,
                        generations,
                        session_id: session,
                        operation_id: operation,
                        offset,
                        expected_attempt: attempt,
                        body_bytes: logical,
                        now_ms: now,
                        lane,
                    },
                    deadline,
                    false,
                    proof,
                )
                .await;
            match outcome {
                Ok(subscription::Outcome::Delivery(receipt)) => deliveries.push(public::Delivery {
                    offset,
                    key: message.key.clone(),
                    body: message.body.clone(),
                    delivery_token: Bytes::copy_from_slice(receipt.token.as_bytes()),
                }),
                Ok(subscription::Outcome::DeliveryAlreadyAcknowledged { .. }) => {}
                Err(Error::Rejected { failure })
                    if failure.code == i32::from(ErrorCode::QuotaExceeded) =>
                {
                    break;
                }
                Err(error) => return Err(error),
                _ => return Err(Error::invalid("Unexpected durable delivery outcome")),
            }
            bytes = next_bytes;
            if let Some(page) = cold_page {
                cold_pages.push(page);
            }
        }
        let assignment = self
            .check_session(
                group,
                partition,
                subscription,
                generations,
                session,
                deadline,
            )
            .await?;
        let ranges = replica
            .state
            .try_with_application(|state| {
                state
                    .subscriptions(partition)
                    .ok_or_else(not_found)?
                    .acknowledged_ranges(
                        subscription,
                        generations,
                        assignment.acknowledged_before,
                        assignment.issued_before,
                        256,
                    )
                    .map_err(subscription_error)
            })
            .await
            .map_err(|source| Error::operation("read bounded acknowledged holes", source))??;
        let response = wire::response(
            frame,
            FrameKind::DeliveryBatch,
            &public::DeliveryBatch {
                deliveries,
                next_offset: assignment.next_delivery_offset,
                topic_id: bytes_id(partition.topic_id),
                partition_id: partition.partition,
                subscription_id: bytes_id(subscription),
                cursor_generation: generations.cursor,
                assignment_generation: generations.assignment,
                delivery_generation: generations.delivery(),
                session_id: bytes_id(session),
                cold_before: assignment.cold_before,
                issued_before: assignment.issued_before,
                acknowledged_ranges: ranges
                    .into_iter()
                    .map(|range| public::AcknowledgedRange {
                        start: range.start,
                        end: range.end,
                    })
                    .collect(),
            },
            &self.config.protocol,
        )?;
        for page in cold_pages {
            page.check().map_err(|_| Error::unknown())?;
            page.release(deadline.into_std())
                .await
                .map_err(|_| Error::unknown())?;
        }
        Ok(response)
    }
}

fn session(info: SessionInfo, next_registration_epoch: u64) -> public::ProducerSessionResult {
    public::ProducerSessionResult {
        producer_id: bytes_id(info.producer_id),
        producer_epoch: info.producer_epoch,
        next_sequence: info.next_sequence,
        expires_at_ms: info.expires_at_ms,
        next_registration_epoch,
    }
}
fn assignment_wire(
    partition: PartitionKey,
    subscription: Id,
    assignment: Assignment,
) -> public::AssignmentResult {
    public::AssignmentResult {
        subscription_id: bytes_id(subscription),
        topic_id: bytes_id(partition.topic_id),
        partition_id: partition.partition,
        session_id: bytes_id(assignment.session_id),
        cursor_generation: assignment.generations.cursor,
        assignment_generation: assignment.generations.assignment,
        delivery_generation: assignment.generations.delivery(),
        acknowledged_before: assignment.acknowledged_before,
        next_delivery_offset: assignment.next_delivery_offset,
        cold_before: assignment.cold_before,
        received_before: assignment.received_before,
        issued_before: assignment.issued_before,
    }
}
fn check_generations(cursor: u64, assignment: u64, delivery: u64) -> Result<Generations> {
    if cursor == 0 || assignment == 0 || assignment != delivery {
        Err(subscription_error(subscription::Rejection::StaleGeneration))
    } else {
        Ok(Generations { cursor, assignment })
    }
}
fn delivery_operation(operation: Id, offset: u64) -> Result<Id> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq:read-delivery:v1");
    hash.update(operation.as_bytes());
    hash.update(&offset.to_be_bytes());
    let mut bytes = [0; 16];
    for (destination, source) in bytes.iter_mut().zip(hash.finalize().as_bytes()) {
        *destination = *source;
    }
    wire::id(&bytes)
}
pub(crate) fn unix_millis() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| Error::operation("read wall clock", source))?;
    u64::try_from(duration.as_millis()).map_err(|_| Error::invalid("Wall-clock timestamp overflow"))
}

pub(crate) fn data_rejection(reason: DataRejection) -> Error {
    match reason {
        DataRejection::Producer(reason) => producer_error(reason),
        DataRejection::Subscription(reason) => subscription_error(reason),
        DataRejection::Retention(_) | DataRejection::Lifecycle(_) => Error::unavailable(),
        DataRejection::Fence(_) => Error::reject(ErrorCode::Fenced, Outcome::Rejected, true),
        DataRejection::PartitionMissing | DataRejection::Uninitialized => Error::unavailable(),
        DataRejection::PartitionCapacity => {
            Error::reject(ErrorCode::QuotaExceeded, Outcome::Rejected, true)
        }
        _ => Error::invalid("Data group configuration does not match this service"),
    }
}
fn broker_error(error: broker::Error) -> Error {
    match error {
        broker::Error::Rejected { reason } => data_rejection(reason),
        broker::Error::Capacity | broker::Error::Memory { .. } => {
            Error::reject(ErrorCode::QuotaExceeded, Outcome::Rejected, true)
        }
        broker::Error::Unknown { .. } | broker::Error::Commit { .. } => Error::unknown(),
        broker::Error::Lease { .. } | broker::Error::GrantMismatch => {
            Error::reject(ErrorCode::Fenced, Outcome::Rejected, true)
        }
        broker::Error::Deadline => {
            Error::reject(ErrorCode::DeadlineExceeded, Outcome::Rejected, true)
        }
        broker::Error::Draining
        | broker::Error::ReadBarrier { .. }
        | broker::Error::PartitionMissing => Error::unavailable(),
        error => Error::operation("execute authoritative broker operation", error),
    }
}
fn producer_error(reason: crate::producer::Rejection) -> Error {
    use crate::producer::Rejection as R;
    let code = match &reason {
        R::SequenceGap { .. } => ErrorCode::SequenceGap,
        R::RegistrationConflict { .. } | R::AlreadyOpen { .. } => ErrorCode::RegistrationConflict,
        R::SessionExpired | R::UnknownSession => ErrorCode::SessionExpired,
        R::ResultExpired { .. } => ErrorCode::DeduplicationExpired,
        R::OffsetExpired { .. } => ErrorCode::OffsetExpired,
        R::SessionCapacity | R::DataCapacity => ErrorCode::QuotaExceeded,
        R::SequenceConflict | R::SessionConflict | R::DigestMismatch => ErrorCode::ContentConflict,
        _ => ErrorCode::InvalidRequest,
    };
    let mut error = Error::reject(code, Outcome::Rejected, code == ErrorCode::QuotaExceeded);
    if let Error::Rejected { failure } = &mut error {
        match reason {
            R::SequenceGap { expected } => failure.expected_sequence = Some(expected),
            R::RegistrationConflict { expected } => {
                failure.expected_registration_epoch = Some(expected)
            }
            _ => {}
        }
    }
    error
}
fn subscription_error(reason: subscription::Rejection) -> Error {
    use subscription::Rejection as R;
    let code = match reason {
        R::NotFound | R::Deleted => ErrorCode::NotFound,
        R::AssignmentBusy => ErrorCode::AssignmentBusy,
        R::StaleGeneration | R::SessionMismatch => ErrorCode::StaleGeneration,
        R::StaleAttempt | R::InvalidToken | R::NotDelivered => ErrorCode::StaleDelivery,
        R::OutOfRetention => ErrorCode::OffsetExpired,
        R::WindowFull
        | R::InFlightMessages
        | R::InFlightBytes
        | R::IdentityBudget
        | R::AttemptLimit
        | R::RetryNotDue
        | R::CutoverPending => ErrorCode::QuotaExceeded,
        _ => ErrorCode::InvalidRequest,
    };
    Error::reject(code, Outcome::Rejected, code == ErrorCode::QuotaExceeded)
}
