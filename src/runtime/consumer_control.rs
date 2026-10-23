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

fn model(value: i32) -> Result<SubscriptionKind> {
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
        let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
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
        let digest = *blake3::hash(&frame.payload).as_bytes();
        match action {
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
                self.renew_consumer_grants(
                    subscription,
                    wire::id(&request.member_id)?,
                    request.member_epoch,
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
        match self
            .meta_write(
                Command::consumer(operation, self.meta_revision().await?, command),
                deadline,
            )
            .await?
        {
            metadata::Outcome::Consumer(outcome) => Ok(outcome),
            _ => Err(Error::invalid(
                "Metadata returned an invalid consumer outcome",
            )),
        }
    }

    pub(super) async fn consumer_row(&self, subscription: Id) -> Result<ConsumerSubscription> {
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.consumer_subscription(subscription).cloned())
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
                let name = &state
                    .topic(topic)
                    .ok_or_else(super::metadata::not_found)?
                    .name;
                state
                    .auth()
                    .authorize(credential, now, metadata::Permission::Consume, Some(name))
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
            member_status: member.map_or(0, |member| match member.status {
                MemberStatus::Joining => 1,
                MemberStatus::Active => 2,
                MemberStatus::Leaving => 3,
                MemberStatus::Left => 4,
                MemberStatus::Expired => 5,
                MemberStatus::Revoked => 6,
            }),
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
                    .find(|row| after.is_none_or(|after| row.id > after))
                    .map(|row| row.id)
                    .or_else(|| state.consumer_subscriptions().next().map(|row| row.id))
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
        self.consumer_meta_write(
            new_operation()?,
            ConsumerCommand::Refresh {
                subscription,
                now_ms: super::data::unix_millis()?,
            },
            deadline,
        )
        .await?;
        self.resume_consumer(subscription, deadline).await
    }

    pub(super) async fn resume_consumer(&self, subscription: Id, deadline: Instant) -> Result<()> {
        // Each successful domain observation survives interruption; the next round resumes it.
        for _ in 0..2 {
            let row = self.consumer_row(subscription).await?;
            let plan = row.members.plan().clone();
            if plan.active {
                return self.open_consumer_gates(&row, deadline).await;
            }
            let now = super::data::unix_millis()?;
            let grants = row.members.grants(now).map_err(|_| Error::unavailable())?;
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
