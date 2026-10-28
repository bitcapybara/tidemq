use tidemq_protocol::{
    Id,
    common::{Frame, FrameKind},
    framing::decode_payload,
    public,
    topic::SubscriptionKind,
};
use tokio::time::Instant;

use super::{Error, Result, service::Service, wire};
use crate::metadata::{self, Command, ConsumerCommand, ConsumerDeletion, ConsumerOutcome};

impl Service {
    pub(super) async fn subscription_admin_public(
        &self,
        access: &crate::gateway::Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let request: public::SubscriptionAdmin =
            decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode subscription administration", source))?;
        let topic = wire::id(&request.topic_id)?;
        let model = super::consumer_control::model(request.model)?;
        let action = public::SubscriptionAdminAction::try_from(request.action)
            .map_err(|_| Error::invalid("Subscription administration action is invalid"))?;
        let permission = match action {
            public::SubscriptionAdminAction::InspectSubscription => metadata::Permission::Inspect,
            public::SubscriptionAdminAction::RemoveSubscription
                if !request.subscription_id.is_empty() && request.name.is_empty() =>
            {
                metadata::Permission::ManageTopics
            }
            _ => {
                return Err(Error::invalid(
                    "Subscription administration requires a valid action and identity",
                ));
            }
        };
        let (_, credential) = super::consumer_control::actor(access)?;
        let subscription = if request.subscription_id.is_empty() {
            None
        } else {
            Some(wire::id(&request.subscription_id)?)
        };
        let now = super::data::unix_millis()?;
        let row = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let resource = state.topic(topic).ok_or_else(super::metadata::not_found)?;
                state
                    .auth()
                    .authorize_topic(credential, now, permission, resource)
                    .map_err(|_| super::service::forbidden())?;
                let row = match subscription {
                    Some(id) => state.consumer_subscription(id),
                    None => state.consumer_subscription_by_name(topic, &request.name),
                }
                .filter(|row| row.topic == topic && row.members.model() == model)
                .cloned()
                .ok_or_else(super::metadata::not_found)?;
                Ok::<_, Error>(row)
            })
            .await
            .map_err(|source| {
                Error::operation("authorize subscription administration", source)
            })??;
        let deletion = if action == public::SubscriptionAdminAction::RemoveSubscription {
            Some(
                self.start_consumer_delete(wire::id(&frame.operation_id)?, row.id, model, deadline)
                    .await?,
            )
        } else {
            row.deletion.clone()
        };
        let result = public::SubscriptionAdminResult {
            topic_id: super::metadata::bytes_id(topic),
            subscription_id: super::metadata::bytes_id(row.id),
            name: row.name,
            model: request.model,
            start_at_latest: row.members.initial_position()
                == tidemq_protocol::topic::InitialPosition::Latest,
            deletion_operation: deletion
                .as_ref()
                .map_or_else(bytes::Bytes::new, |plan| super::metadata::bytes_id(plan.id)),
            deleted_domains: deletion
                .as_ref()
                .map_or(0, |plan| plan.deleted.len() as u32),
            total_domains: row.members.plan().domains.len() as u32,
            deletion_complete: deletion.is_some_and(|plan| plan.complete),
        };
        wire::response(
            frame,
            FrameKind::SubscriptionAdminResult,
            &result,
            &self.config.protocol,
        )
    }

    pub(super) async fn start_consumer_delete(
        &self,
        operation: Id,
        subscription: Id,
        model: SubscriptionKind,
        deadline: Instant,
    ) -> Result<ConsumerDeletion> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
        let guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
        let outcome = self
            .meta_write(
                Command::consumer(
                    operation,
                    self.metadata_request_revision(operation).await?,
                    ConsumerCommand::Delete {
                        subscription,
                        model,
                    },
                ),
                deadline,
            )
            .await?;
        if !matches!(
            outcome,
            metadata::Outcome::Consumer(ConsumerOutcome::Deletion(_))
        ) {
            return Err(Error::invalid(
                "Metadata deletion returned an invalid outcome",
            ));
        }
        drop(guard);
        match self.resume_consumer_delete(subscription, deadline).await {
            Ok(plan) => Ok(plan),
            Err(error) => {
                tracing::debug!(
                    event_code = "subscription_delete_pending",
                    error_code = error.failure().code,
                    "Subscription deletion is durable and will continue from its recorded progress"
                );
                self.consumer_delete_status(subscription).await
            }
        }
    }

    pub(super) async fn consumer_delete_status(
        &self,
        subscription: Id,
    ) -> Result<ConsumerDeletion> {
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state
                    .consumer_subscription(subscription)
                    .and_then(|row| row.deletion.clone())
            })
            .await
            .map_err(|source| Error::operation("read durable subscription deletion", source))?
            .ok_or_else(super::metadata::not_found)
    }

    async fn record_consumer_delete(
        &self,
        command: ConsumerCommand,
        deadline: Instant,
    ) -> Result<()> {
        let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
        let operation = Id::new_v7()
            .map_err(|source| Error::operation("allocate subscription deletion step", source))?;
        match self
            .consumer_meta_write(operation, command, deadline)
            .await?
        {
            ConsumerOutcome::Deletion(_) => Ok(()),
            _ => Err(Error::invalid(
                "Metadata deletion step returned an invalid outcome",
            )),
        }
    }

    pub(super) async fn resume_consumer_delete(
        &self,
        subscription: Id,
        deadline: Instant,
    ) -> Result<ConsumerDeletion> {
        let mut plan = self.consumer_delete_status(subscription).await?;
        if plan.complete {
            return Ok(plan);
        }
        // Bound each round; no data RPC holds the metadata mutation lock.
        let domains: Vec<_> = plan
            .domains
            .iter()
            .filter(|domain| !plan.deleted.contains(domain))
            .take(4)
            .copied()
            .collect();
        for (index, domain) in domains.iter().enumerate() {
            let slots = domains.len().saturating_sub(index).max(1) as u32;
            let slice = std::time::Duration::from_millis(wire::remaining(deadline)?)
                .checked_div(slots)
                .ok_or_else(Error::unavailable)?;
            let child_deadline = Instant::now()
                .checked_add(slice)
                .ok_or_else(Error::unavailable)?
                .min(deadline);
            if let Err(error) = self
                .delete_consumer_domain(&plan, *domain, child_deadline)
                .await
            {
                tracing::debug!(
                    event_code = "subscription_delete_domain_pending",
                    error_code = error.failure().code,
                    "Other subscription domains may continue deleting"
                );
                continue;
            }
            self.record_consumer_delete(
                ConsumerCommand::ObserveDeleted {
                    subscription,
                    deletion: plan.id,
                    domain: *domain,
                },
                deadline,
            )
            .await?;
        }
        plan = self.consumer_delete_status(subscription).await?;
        if plan.deleted.len() == plan.domains.len() {
            self.record_consumer_delete(
                ConsumerCommand::FinishDelete {
                    subscription,
                    deletion: plan.id,
                },
                deadline,
            )
            .await?;
        }
        self.consumer_delete_status(subscription).await
    }

    pub(super) async fn reconcile_consumer_deletions(&self, deadline: Instant) -> Result<()> {
        let after = *self
            .consumer_delete_after
            .lock()
            .map_err(|_| Error::unavailable())?;
        let next = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let pending = || {
                    state
                        .consumer_subscriptions()
                        .filter(|row| row.is_deleting())
                };
                pending()
                    .find(|row| after.is_none_or(|after| row.id > after))
                    .or_else(|| pending().next())
                    .map(|row| row.id)
            })
            .await
            .map_err(|source| Error::operation("page unfinished subscription deletions", source))?;
        if let Some(subscription) = next {
            *self
                .consumer_delete_after
                .lock()
                .map_err(|_| Error::unavailable())? = Some(subscription);
            self.resume_consumer_delete(subscription, deadline).await?;
        }
        Ok(())
    }
}
