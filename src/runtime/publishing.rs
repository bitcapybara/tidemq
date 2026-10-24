use bytes::Bytes;
use tidemq_protocol::{
    Id,
    common::{Frame, FrameKind},
    framing::decode_payload,
    public,
    topic::SendIdentity,
};
use tokio::time::Instant;

use super::{
    Error, Result,
    service::{Service, forbidden},
    wire,
};
use crate::{
    broker::{DataCommand, DataResponse, WriterFence},
    gateway::Access,
    metadata::{Command, Permission, PublisherCommand, Route},
    ownership::{FinalizationKind, Work},
    producer::{MessageData, PublishRequest},
};

impl Service {
    pub(super) async fn publisher_control_public(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let request: public::PublisherControl =
            decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode publisher control", source))?;
        let topic = wire::id(&request.topic_id)?;
        let producer = wire::id(&request.producer_id)?;
        let (client, credential) = super::consumer_control::actor(access)?;
        let now_ms = super::data::unix_millis()?;
        let expiry = self
            .publisher_credential_expiry(credential, topic, now_ms)
            .await?;
        let action = public::PublisherAction::try_from(request.action)
            .map_err(|_| Error::invalid("Publisher action is invalid"))?;
        let command = publisher_command(
            &request,
            topic,
            producer,
            client,
            credential,
            now_ms,
            expiry,
            *blake3::hash(&frame.payload).as_bytes(),
            action,
        )?;
        let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
        if let Some(command) = command {
            self.meta_write(
                Command::publisher(
                    wire::id(&frame.operation_id)?,
                    self.meta_revision().await?,
                    command,
                ),
                deadline,
            )
            .await?;
        }
        let result = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let book = state.publishers();
                let record = book.record(producer, request.registration);
                if record.is_some_and(|record| {
                    record.permit.topic != topic
                        || record.permit.client != client
                        || record.permit.credential != credential
                }) {
                    return Err(forbidden());
                }
                let permit = record.map(|record| record.permit_at(now_ms));
                Ok(public::PublisherControlResult {
                    next_registration: book.next_registration(),
                    registration: record.map_or(0, |record| record.permit.registration),
                    live: permit.as_ref().is_some_and(|permit| permit.live),
                    remaining_lease_ms: permit
                        .map_or(0, |permit| permit.expires_ms.saturating_sub(now_ms)),
                    next_heartbeat_sequence: record
                        .and_then(|record| record.next_heartbeat_sequence())
                        .unwrap_or(1),
                })
            })
            .await
            .map_err(|source| Error::operation("read publisher control result", source))??;
        wire::response(
            frame,
            FrameKind::PublisherControlResult,
            &result,
            &self.config.protocol,
        )
    }

    async fn publisher_credential_expiry(
        &self,
        credential: Id,
        topic: Id,
        now: u64,
    ) -> Result<u64> {
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let topic = state.topic(topic).ok_or_else(super::metadata::not_found)?;
                state
                    .auth()
                    .authorize_topic(credential, now, Permission::Produce, topic)
                    .map_err(|_| forbidden())?;
                state
                    .auth()
                    .credential(credential)
                    .map(|record| record.expires_ms)
                    .ok_or_else(forbidden)
            })
            .await
            .map_err(|source| Error::operation("authorize publisher permit", source))?
    }

    pub(super) async fn publishing_public(
        &self,
        access: &Access,
        frame: &Frame,
        route: Route,
        deadline: Instant,
    ) -> Result<Frame> {
        if super::service::frame_kind(frame)? == FrameKind::RetirePublishes {
            let request: public::RetirePublishes =
                decode_payload(frame.payload.clone(), &self.config.protocol).map_err(|source| {
                    Error::operation("decode publisher result retirement", source)
                })?;
            self.publisher_birth(wire::id(&request.shard_id)?, route.partition, deadline)
                .await?;
            let mut permit = self
                .publisher_permit(
                    access,
                    route.partition.topic_id,
                    wire::id(&request.producer_id)?,
                    request.registration,
                    deadline,
                )
                .await?;
            permit.live &= !request.close;
            self.publish_commit(&route, deadline, |writer| DataCommand::RetirePublishes {
                partition: route.partition,
                writer,
                permit,
                before: request.before,
            })
            .await?;
            return wire::response(
                frame,
                FrameKind::PublishesRetired,
                &public::PublishesRetired {
                    before: request.before,
                },
                &self.config.protocol,
            );
        }
        let mut attempt = self.producer_metrics.begin_attempt();
        attempt.finish(crate::broker::AttemptOutcome::Unknown);
        let _pending = self
            .producer_metrics
            .begin_submission(frame.payload.len() as u64);
        match self.publish_public(access, frame, route, deadline).await {
            Ok((frame, outcome)) => {
                attempt.finish(outcome);
                Ok(frame)
            }
            Err(error) => {
                let outcome = if error.failure().outcome
                    == i32::from(tidemq_protocol::common::Outcome::Unknown)
                {
                    crate::broker::AttemptOutcome::Unknown
                } else {
                    crate::broker::AttemptOutcome::Rejected
                };
                attempt.finish(outcome);
                Err(error)
            }
        }
    }

    #[tracing::instrument(target = "tidemq_trace", name = "producer_submission", skip_all)]
    async fn publish_public(
        &self,
        access: &Access,
        frame: &Frame,
        route: Route,
        deadline: Instant,
    ) -> Result<(Frame, crate::broker::AttemptOutcome)> {
        self.admit_produce()?;
        let request: public::Publish = decode_payload(frame.payload.clone(), &self.config.protocol)
            .map_err(|source| Error::operation("decode logical publish", source))?;
        #[cfg(feature = "test-hooks")]
        super::test_hooks::pause_publish(route.partition, request.sequence, deadline).await;
        let shard = wire::id(&request.shard_id)?;
        let producer = wire::id(&request.producer_id)?;
        let identity = SendIdentity {
            topic: route.partition.topic_id,
            producer,
            registration: request.registration,
            sequence: request.sequence,
        };
        let permit = self
            .publisher_permit(
                access,
                identity.topic,
                producer,
                identity.registration,
                deadline,
            )
            .await?;
        let digest = request
            .digest
            .as_ref()
            .try_into()
            .map_err(|_| Error::invalid("Logical message digest must contain 32 bytes"))?;
        let message = request
            .message
            .ok_or_else(|| Error::invalid("Logical message is required"))?;
        let bytes = tidemq_protocol::topic::delivery_bytes(
            message.key.as_deref().unwrap_or_default(),
            &message.payload,
            &message.properties,
        )
        .map_err(|source| Error::operation("validate public message size", source))?;
        if bytes > u64::from(self.config.protocol.max_message_bytes).saturating_add(256) {
            return Err(super::data::data_rejection(
                crate::broker::DataRejection::Producer(crate::producer::Rejection::MessageTooLarge),
            ));
        }
        let context = tidemq_protocol::topic::MessageMetadata {
            key_present: message.key.is_some(),
            routing_hash: tidemq_protocol::topic::routing_hash(
                message.key.as_deref(),
                producer,
                request.sequence,
            )
            .map_err(|source| Error::operation("compute logical routing hash", source))?,
            properties: message.properties,
        };
        let birth = self
            .publisher_birth(shard, route.partition, deadline)
            .await?;
        let publish = PublishRequest {
            identity,
            digest,
            retired_before: request.retired_before,
            message: MessageData {
                key: message.key.unwrap_or_default(),
                body: message.payload,
                context: postcard::to_stdvec(&context)
                    .map_err(|source| Error::operation("encode stored message context", source))?
                    .into(),
            },
        };
        let demand = self
            .data
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.publish_quota_demand(route.partition, &publish))
            .await
            .map_err(|source| Error::operation("read tenant publish quota demand", source))?
            .map_err(super::data::data_rejection)?;
        if let Some(demand) = demand {
            self.ensure_tenant_quota(route.group_id, route.partition, demand, deadline)
                .await?;
        }
        let now_ms = super::data::unix_millis()?;
        let result = self
            .publish_commit(&route, deadline, |writer| DataCommand::Publish {
                partition: route.partition,
                writer,
                birth: (shard, birth),
                permit,
                request: publish,
                now_ms,
            })
            .await?;
        use crate::broker::AttemptOutcome;
        let (offset, permanent_seal, sealed_end, permanently_closed, outcome) = match result {
            DataResponse::Published(result) => {
                if !result.duplicate {
                    self.producer_metrics.committed_batch(1, bytes);
                }
                (
                    Some(result.offset),
                    Bytes::new(),
                    0,
                    false,
                    if result.duplicate {
                        AttemptOutcome::Duplicate
                    } else {
                        AttemptOutcome::Committed
                    },
                )
            }
            DataResponse::ShardSealed {
                plan,
                committed_end,
            } => (
                None,
                Bytes::copy_from_slice(plan.as_bytes()),
                committed_end,
                false,
                AttemptOutcome::Rejected,
            ),
            DataResponse::PublishClosed => (None, Bytes::new(), 0, true, AttemptOutcome::Rejected),
            _ => return Err(Error::invalid("Logical publish returned an invalid result")),
        };
        let frame = wire::response(
            frame,
            FrameKind::PublishResult,
            &public::PublishResult {
                topic_id: request.topic_id,
                shard_id: request.shard_id,
                producer_id: request.producer_id,
                registration: request.registration,
                sequence: request.sequence,
                digest: request.digest,
                offset,
                permanent_seal,
                sealed_end,
                permanently_closed,
            },
            &self.config.protocol,
        )?;
        Ok((frame, outcome))
    }

    async fn publish_commit(
        &self,
        route: &Route,
        deadline: Instant,
        command: impl FnOnce(WriterFence) -> DataCommand,
    ) -> Result<DataResponse> {
        let lease = self
            .data_lease(route.group_id, route.partition, deadline)
            .await?;
        let replica = self
            .data
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?;
        let read_fence = |state: &crate::broker::DataApplication| {
            state
                .ownership()
                .and_then(|ownership| ownership.fence(route.partition))
                .cloned()
        };
        let fence = replica
            .state
            .try_with_application(read_fence)
            .await
            .map_err(|source| Error::operation("read publisher authority fence", source))?
            .ok_or_else(Error::unavailable)?;
        lease
            .check(&fence, Work::ControlMutation)
            .map_err(|_| Error::unavailable())?;
        let finalization = lease
            .begin_finalization(&fence, FinalizationKind::Produce, deadline.into_std())
            .map_err(|_| Error::unavailable())?;
        let command = command(WriterFence {
            node_id: lease.grant().owner_node,
            owner_epoch: lease.grant().epoch,
        });
        let result = self.data_write(route.group_id, command, deadline).await?;
        let current = replica
            .state
            .try_with_application(read_fence)
            .await
            .map_err(|source| Error::operation("read committed publisher fence", source))?
            .ok_or_else(Error::unknown)?;
        finalization.check(&current).map_err(|_| Error::unknown())?;
        Ok(result)
    }
}

#[allow(clippy::too_many_arguments)] // Wire fields and trusted admission context form one fixed metadata command.
fn publisher_command(
    request: &public::PublisherControl,
    topic: Id,
    producer: Id,
    client: Id,
    credential: Id,
    now_ms: u64,
    credential_expires_ms: u64,
    digest: [u8; 32],
    action: public::PublisherAction,
) -> Result<Option<PublisherCommand>> {
    Ok(match action {
        public::PublisherAction::OpenPublisher => Some(PublisherCommand::Open {
            topic,
            producer,
            registration: request.registration,
            client,
            credential,
            now_ms,
            credential_expires_ms,
            digest,
        }),
        public::PublisherAction::HeartbeatPublisher => Some(PublisherCommand::Heartbeat {
            producer,
            registration: request.registration,
            client,
            credential,
            sequence: request.heartbeat_sequence,
            now_ms,
            credential_expires_ms,
            digest,
        }),
        public::PublisherAction::ClosePublisher => Some(PublisherCommand::Close {
            producer,
            registration: request.registration,
            client,
            credential,
        }),
        public::PublisherAction::PublisherStatus => None,
        public::PublisherAction::Unspecified => {
            return Err(Error::invalid("Publisher action is required"));
        }
    })
}
