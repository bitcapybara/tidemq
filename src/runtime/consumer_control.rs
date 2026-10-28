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
    metadata::bytes_id,
    service::{Service, forbidden},
    wire,
};
use crate::{
    gateway::Access,
    metadata::{
        self, Command, ConsumerCommand, ConsumerOutcome, ConsumerSubscription, MemberHeartbeat,
        MemberJoin, MemberStatus,
    },
};

pub(super) fn actor(access: &Access) -> Result<(Id, Id)> {
    let credential = access
        .principal
        .strip_prefix("credential:")
        .ok_or_else(forbidden)?
        .parse()
        .map_err(|_| forbidden())?;
    Ok((access.session.ok_or_else(forbidden)?, credential))
}

fn new_operation() -> Result<Id> {
    Id::new_v7().map_err(|source| Error::operation("allocate consumer control operation", source))
}

pub(super) fn model(value: i32) -> Result<SubscriptionKind> {
    match public::ConsumerModel::try_from(value).ok() {
        Some(public::ConsumerModel::StreamConsumer) => Ok(SubscriptionKind::Stream),
        Some(public::ConsumerModel::QueueConsumer) => Ok(SubscriptionKind::Queue),
        _ => Err(Error::invalid("Consumer model is required")),
    }
}

impl Service {
    pub(super) async fn consumer_control_public(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let request: public::ConsumerControl =
            decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode consumer control", source))?;
        let topic = wire::id(&request.topic_id)?;
        let (client, credential) = actor(access)?;
        let action = public::ConsumerAction::try_from(request.action)
            .map_err(|_| Error::invalid("Consumer action is invalid"))?;
        let operation = wire::id(&frame.operation_id)?;
        #[cfg(feature = "test-hooks")]
        let join_observation = if action == public::ConsumerAction::JoinConsumer {
            super::consumer_join_hook::observe(super::consumer_join_hook::Identity {
                operation,
                topic,
                subscription: wire::id(&request.subscription_id)?,
                member: wire::id(&request.member_id)?,
                epoch: request.member_epoch,
                client,
                credential,
                digest: *blake3::hash(&frame.payload).as_bytes(),
            })
        } else {
            None
        };
        #[cfg(feature = "test-hooks")]
        if let Some(observation) = &join_observation {
            observation
                .pause(super::consumer_join_hook::Point::BeforeCommit, deadline)
                .await;
        }
        let guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
        let subscription = if action == public::ConsumerAction::EnsureConsumerSubscription {
            match self
                .consumer_meta_write(
                    operation,
                    ConsumerCommand::Create {
                        topic,
                        name: request.name.clone(),
                        model: model(request.model)?,
                        initial: if request.start_at_latest {
                            InitialPosition::Latest
                        } else {
                            InitialPosition::EarliestAvailable
                        },
                    },
                    deadline,
                )
                .await?
            {
                ConsumerOutcome::Created { subscription } => subscription,
                _ => {
                    return Err(Error::invalid(
                        "Subscription creation returned an invalid outcome",
                    ));
                }
            }
        } else {
            wire::id(&request.subscription_id)?
        };
        let row = self.consumer_row(subscription).await?;
        if row.topic != topic || row.members.model() != model(request.model)? {
            return Err(Error::invalid(
                "Consumer subscription model or topic differs",
            ));
        }
        let now = super::data::unix_millis()?;
        let expiry = self
            .consumer_credential_expiry(credential, topic, now)
            .await?;
        self.refresh_consumer_if_needed(&row, now, deadline).await?;
        let digest = *blake3::hash(&frame.payload).as_bytes();
        match action {
            public::ConsumerAction::RetireConsumer => {
                self.consumer_meta_write(
                    operation,
                    ConsumerCommand::Retire {
                        subscription,
                        member: wire::id(&request.member_id)?,
                        epoch: request.member_epoch,
                        client,
                        credential,
                    },
                    deadline,
                )
                .await?;
            }
            public::ConsumerAction::JoinConsumer => {
                self.consumer_meta_write(
                    operation,
                    ConsumerCommand::Join {
                        subscription,
                        request: MemberJoin {
                            member: wire::id(&request.member_id)?,
                            expected_epoch: request.member_epoch,
                            client,
                            credential,
                            now_ms: now,
                            credential_expires_ms: expiry,
                            max_messages: request.max_messages,
                            max_bytes: request.max_bytes,
                            digest,
                        },
                    },
                    deadline,
                )
                .await?;
            }
            public::ConsumerAction::HeartbeatConsumer => {
                self.consumer_meta_write(
                    operation,
                    ConsumerCommand::Heartbeat {
                        subscription,
                        request: MemberHeartbeat {
                            member: wire::id(&request.member_id)?,
                            epoch: request.member_epoch,
                            client,
                            credential,
                            sequence: request.heartbeat_sequence,
                            now_ms: now,
                            credential_expires_ms: expiry,
                            digest,
                        },
                    },
                    deadline,
                )
                .await?;
            }
            public::ConsumerAction::LeaveConsumer => {
                self.consumer_meta_write(
                    operation,
                    ConsumerCommand::Leave {
                        subscription,
                        member: wire::id(&request.member_id)?,
                        epoch: request.member_epoch,
                        client,
                        credential,
                        now_ms: now,
                    },
                    deadline,
                )
                .await?;
            }
            public::ConsumerAction::ConsumerStatus
            | public::ConsumerAction::EnsureConsumerSubscription => {}
            public::ConsumerAction::Unspecified => {
                return Err(Error::invalid("Consumer action is required"));
            }
        }
        self.resume_consumer(subscription, deadline).await?;
        let result = self
            .consumer_status(subscription, &request, client, credential)
            .await?;
        drop(guard);
        #[cfg(feature = "test-hooks")]
        if let Some(observation) = &join_observation {
            observation.status(result.member_epoch, result.member_status);
            observation
                .pause(super::consumer_join_hook::Point::Reply, deadline)
                .await;
        }
        wire::response(
            frame,
            FrameKind::ConsumerControlResult,
            &result,
            &self.config.protocol,
        )
    }

    pub(super) async fn consumer_meta_write(
        &self,
        operation: Id,
        command: ConsumerCommand,
        deadline: Instant,
    ) -> Result<ConsumerOutcome> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        for _ in 0..16 {
            self.require_leader(replica)?;
            let revision = self.meta_revision().await?;
            #[cfg(test)]
            super::data::consumer_tests::before_consumer_meta_write(operation, revision, &command)
                .await;
            let request = Command::consumer(operation, revision, command.clone());
            let applied = if matches!(
                command,
                ConsumerCommand::Heartbeat { .. }
                    | ConsumerCommand::Leave { .. }
                    | ConsumerCommand::Retire { .. }
                    | ConsumerCommand::OpenedGate { .. }
                    | ConsumerCommand::Observe { .. }
                    | ConsumerCommand::ObserveDeleted { .. }
                    | ConsumerCommand::FinishDelete { .. }
            ) {
                replica.group.commit_completion(request, deadline).await
            } else {
                replica.group.commit(request, deadline).await
            }
            .map_err(wire::execution_error)?;
            let response = applied.response;
            match response {
                metadata::Response::Applied {
                    outcome: metadata::Outcome::Consumer(outcome),
                    ..
                } => return Ok(outcome),
                metadata::Response::Rejected {
                    reason: metadata::Rejection::RevisionConflict,
                    ..
                } => {
                    // Lease writers can advance the shared revision while a domain is installed.
                    wire::within(
                        deadline,
                        tokio::time::sleep(std::time::Duration::from_millis(2)),
                    )
                    .await?;
                }
                metadata::Response::Rejected { reason, .. } => {
                    return Err(super::metadata::metadata_rejection(reason));
                }
                _ => {
                    return Err(Error::invalid(
                        "Metadata returned an invalid consumer outcome",
                    ));
                }
            }
        }
        Err(Error::unavailable())
    }

    pub(super) async fn consumer_row(&self, subscription: Id) -> Result<ConsumerSubscription> {
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state
                    .consumer_subscription(subscription)
                    .filter(|row| row.deletion.is_none())
                    .cloned()
            })
            .await
            .map_err(|source| Error::operation("read durable consumer subscription", source))?
            .ok_or_else(super::metadata::not_found)
    }

    async fn consumer_credential_expiry(&self, credential: Id, topic: Id, now: u64) -> Result<u64> {
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let topic = state.topic(topic).ok_or_else(super::metadata::not_found)?;
                state
                    .auth()
                    .authorize_topic(credential, now, metadata::Permission::Consume, topic)
                    .map_err(|_| forbidden())?;
                state
                    .auth()
                    .credential(credential)
                    .map(|record| record.expires_ms)
                    .ok_or_else(forbidden)
            })
            .await
            .map_err(|source| Error::operation("authorize consumer membership", source))?
    }

    async fn consumer_status(
        &self,
        subscription: Id,
        request: &public::ConsumerControl,
        client: Id,
        credential: Id,
    ) -> Result<public::ConsumerControlResult> {
        let now = super::data::unix_millis()?;
        let row = self.consumer_row(subscription).await?;
        let member = if request.member_id.is_empty() {
            None
        } else {
            row.members
                .member(wire::id(&request.member_id)?, request.member_epoch)
        };
        if member.is_some_and(|member| member.client != client || member.credential != credential) {
            return Err(forbidden());
        }
        let work = if let Some(member) = member
            .filter(|member| member.status == MemberStatus::Active && row.members.plan().active)
        {
            if row.members.model() == SubscriptionKind::Queue {
                row.members
                    .queue_worksets(super::data::unix_millis()?)
                    .map_err(|_| Error::unavailable())?
                    .remove(&member.member)
                    .unwrap_or_default()
            } else {
                row.members.plan().domains.clone()
            }
        } else {
            Vec::new()
        };
        Ok(public::ConsumerControlResult {
            assignment_fenced: row.members.plan().fenced,
            lane_fences: row.members.lane_fences().to_vec(),
            next_heartbeat_sequence: member
                .and_then(|member| {
                    row.members
                        .next_heartbeat_sequence(member.member, member.epoch)
                })
                .unwrap_or(1),
            remaining_lease_ms: member.map_or(0, |member| member.expires_ms.saturating_sub(now)),
            lanes: row
                .members
                .grants(super::data::unix_millis()?)
                .map_err(|_| Error::unavailable())?
                .into_iter()
                .find(|grant| {
                    member.is_some_and(|member| {
                        member.member == grant.member && member.epoch == grant.epoch
                    })
                })
                .map_or_else(Vec::new, |grant| {
                    grant
                        .lanes
                        .into_iter()
                        .map(|lane| public::ConsumerLane {
                            hash_start: lane.start,
                            hash_end: lane.end,
                        })
                        .collect()
                }),
            assignment_version: row.members.plan().version,
            subscription_id: bytes_id(subscription),
            next_member_epoch: row.members.next_epoch(),
            member_epoch: member.map_or(0, |member| member.epoch),
            expires_ms: member.map_or(0, |member| member.expires_ms),
            member_status: member.map_or(
                if request.member_epoch > 0 && request.member_epoch < row.members.next_epoch() {
                    7
                } else {
                    0
                },
                |member| match member.status {
                    MemberStatus::Joining => 1,
                    MemberStatus::Active => 2,
                    MemberStatus::Leaving => 3,
                    MemberStatus::Left => 4,
                    MemberStatus::Expired => 5,
                    MemberStatus::Revoked => 6,
                },
            ),
            plan_active: row.members.plan().active,
            work_shards: work.into_iter().map(bytes_id).collect(),
        })
    }

    pub(super) async fn reconcile_consumers(&self, deadline: Instant) -> Result<()> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let after = *self
            .consumer_after
            .lock()
            .map_err(|_| Error::unavailable())?;
        let next = replica
            .state
            .try_with_application(|state| {
                state
                    .consumer_subscriptions()
                    .filter(|row| row.deletion.is_none())
                    .find(|row| after.is_none_or(|after| row.id > after))
                    .map(|row| row.id)
                    .or_else(|| {
                        state
                            .consumer_subscriptions()
                            .find(|row| row.deletion.is_none())
                            .map(|row| row.id)
                    })
            })
            .await
            .map_err(|source| Error::operation("page consumer reconciliation", source))?;
        let Some(subscription) = next else {
            return Ok(());
        };
        *self
            .consumer_after
            .lock()
            .map_err(|_| Error::unavailable())? = Some(subscription);
        self.refresh_consumer_if_needed(
            &self.consumer_row(subscription).await?,
            super::data::unix_millis()?,
            deadline,
        )
        .await?;
        self.resume_consumer(subscription, deadline).await
    }

    async fn refresh_consumer_if_needed(
        &self,
        row: &ConsumerSubscription,
        now: u64,
        deadline: Instant,
    ) -> Result<()> {
        let revision = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state
                    .topology()
                    .layout(row.topic)
                    .map(|layout| layout.revision)
            })
            .await
            .map_err(|source| Error::operation("read consumer topology revision", source))?
            .ok_or_else(super::metadata::not_found)?;
        if row.members.needs_refresh(revision, now) {
            self.consumer_meta_write(
                new_operation()?,
                ConsumerCommand::Refresh {
                    subscription: row.id,
                    now_ms: now,
                },
                deadline,
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn resume_consumer(&self, subscription: Id, deadline: Instant) -> Result<()> {
        // Each successful domain observation survives interruption; the next round resumes it.
        let mut remaining = 4;
        for _ in 0..2 {
            let row = self.consumer_row(subscription).await?;
            let plan = row.members.plan().clone();
            if plan.active {
                return self.open_consumer_gates(&row, deadline).await;
            }
            let now = super::data::unix_millis()?;
            if plan.fenced && now < plan.grant_after_ms {
                return Ok(());
            }
            let grants = row.members.grants(now).map_err(|_| Error::unavailable())?;
            if grants.iter().any(|grant| grant.expires_ms <= now) {
                self.refresh_consumer_if_needed(&row, now, deadline).await?;
                return Ok(());
            }
            let version = plan
                .version
                .checked_mul(2)
                .and_then(|version| version.checked_add(u64::from(plan.fenced)))
                .ok_or_else(Error::unavailable)?;
            for domain in plan
                .domains
                .iter()
                .filter(|domain| !plan.installed.contains(domain))
            {
                if remaining == 0 {
                    return Ok(());
                }
                remaining -= 1;
                self.install_consumer_domain(&row, *domain, version, grants.clone(), deadline)
                    .await?;
                self.consumer_meta_write(
                    new_operation()?,
                    ConsumerCommand::Observe {
                        subscription,
                        version: plan.version,
                        fenced: plan.fenced,
                        domain: *domain,
                        now_ms: now,
                    },
                    deadline,
                )
                .await?;
            }
        }
        Ok(())
    }
}
