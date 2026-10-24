use tidemq_protocol::{
    Id,
    common::{Frame, FrameKind},
    framing::decode_payload,
    public,
};
use tokio::time::Instant;

use super::{Error, Result, service::Service, wire};
use crate::{
    gateway::Access,
    metadata::{self, Command, TopicDeletion, TopicDeletionCommand},
};

impl Service {
    pub(super) async fn remove_topic_public(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let request: public::RemoveTopic =
            decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode topic removal", source))?;
        let topic = wire::id(&request.topic_id)?;
        self.begin_topic_delete(access, topic, wire::id(&frame.operation_id)?, deadline)
            .await?;
        let plan = match self.resume_topic_delete(topic, deadline).await {
            Ok(plan) => plan,
            Err(error) => {
                tracing::debug!(
                    event_code = "topic_delete_pending",
                    error_code = error.failure().code,
                    "Topic removal will resume from its durable sealing progress"
                );
                self.topic_delete_status(topic).await?
            }
        };
        wire::response(
            frame,
            FrameKind::TopicRemoved,
            &public::TopicRemoved {
                topic_id: super::metadata::bytes_id(topic),
                deletion_operation: super::metadata::bytes_id(plan.id),
                sealed_domains: plan.sealed.len() as u32,
                total_domains: plan.domains.len() as u32,
                complete: plan.complete,
            },
            &self.config.protocol,
        )
    }

    async fn begin_topic_delete(
        &self,
        access: &Access,
        topic: Id,
        operation: Id,
        deadline: Instant,
    ) -> Result<()> {
        let (_, credential) = super::consumer_control::actor(access)?;
        let now = super::data::unix_millis()?;
        let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
        let generation = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let resource = state.topic(topic).ok_or_else(super::metadata::not_found)?;
                state
                    .auth()
                    .authorize_topic(
                        credential,
                        now,
                        metadata::Permission::ManageTopics,
                        resource,
                    )
                    .map_err(|_| super::service::forbidden())?;
                Ok::<_, Error>(
                    state
                        .topic_deletion(topic)
                        .map_or(resource.generation, |plan| plan.generation),
                )
            })
            .await
            .map_err(|source| Error::operation("authorize topic removal", source))??;
        self.meta_write(
            Command::topic_deletion(
                operation,
                self.metadata_request_revision(operation).await?,
                TopicDeletionCommand::Begin { topic, generation },
            ),
            deadline,
        )
        .await?;
        Ok(())
    }

    async fn topic_delete_status(&self, topic: Id) -> Result<TopicDeletion> {
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.topic_deletion(topic).cloned())
            .await
            .map_err(|source| Error::operation("read durable topic removal", source))?
            .ok_or_else(super::metadata::not_found)
    }

    async fn record_topic_delete(
        &self,
        command: TopicDeletionCommand,
        deadline: Instant,
    ) -> Result<()> {
        let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
        let operation = Id::new_v7()
            .map_err(|source| Error::operation("allocate topic removal step", source))?;
        match self
            .meta_write(
                Command::topic_deletion(operation, self.meta_revision().await?, command),
                deadline,
            )
            .await?
        {
            metadata::Outcome::TopicDeletion(_) => Ok(()),
            _ => Err(Error::invalid(
                "Topic removal returned an invalid metadata outcome",
            )),
        }
    }

    async fn resume_topic_delete(&self, topic: Id, deadline: Instant) -> Result<TopicDeletion> {
        let plan = self.topic_delete_status(topic).await?;
        if plan.complete {
            return Ok(plan);
        }
        let domains: Vec<_> = plan
            .domains
            .iter()
            .filter(|domain| !plan.sealed.contains_key(domain))
            .take(4)
            .copied()
            .collect();
        for (index, domain) in domains.iter().enumerate() {
            let child = std::time::Duration::from_millis(wire::remaining(deadline)?)
                .checked_div(domains.len().saturating_sub(index).max(1) as u32)
                .ok_or_else(Error::unavailable)?;
            let child = Instant::now()
                .checked_add(child)
                .ok_or_else(Error::unavailable)?
                .min(deadline);
            match self.seal_topic_delete_domain(&plan, *domain, child).await {
                Ok(end) => {
                    self.record_topic_delete(
                        TopicDeletionCommand::ObserveSeal {
                            topic,
                            deletion: plan.id,
                            domain: *domain,
                            end,
                        },
                        deadline,
                    )
                    .await?
                }
                Err(error) => tracing::debug!(
                    event_code = "topic_delete_domain_pending",
                    error_code = error.failure().code,
                    "Other topic domains may continue sealing"
                ),
            }
        }
        let plan = self.topic_delete_status(topic).await?;
        if plan.sealed.len() == plan.domains.len() {
            self.record_topic_delete(
                TopicDeletionCommand::Finish {
                    topic,
                    deletion: plan.id,
                },
                deadline,
            )
            .await?;
        }
        self.topic_delete_status(topic).await
    }

    async fn seal_topic_delete_domain(
        &self,
        plan: &TopicDeletion,
        domain: Id,
        deadline: Instant,
    ) -> Result<u64> {
        let birth = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.topology().shard(domain).cloned())
            .await
            .map_err(|source| Error::operation("read topic removal domain", source))?
            .ok_or_else(super::metadata::not_found)?;
        self.seal_data_domain(plan.id, birth.partition, birth.route.group, deadline)
            .await
    }

    pub(super) async fn reconcile_topic_deletions(&self, deadline: Instant) -> Result<()> {
        let after = *self
            .topic_delete_after
            .lock()
            .map_err(|_| Error::unavailable())?;
        let next = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state
                    .pending_topic_deletions()
                    .find(|plan| after.is_none_or(|after| plan.topic > after))
                    .or_else(|| state.pending_topic_deletions().next())
                    .map(|plan| plan.topic)
            })
            .await
            .map_err(|source| Error::operation("page unfinished topic removals", source))?;
        if let Some(topic) = next {
            *self
                .topic_delete_after
                .lock()
                .map_err(|_| Error::unavailable())? = Some(topic);
            self.resume_topic_delete(topic, deadline).await?;
        }
        Ok(())
    }
}
