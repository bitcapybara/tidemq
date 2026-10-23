use serde::{Deserialize, Serialize};
use tidemq_protocol::{
    Id,
    topic::{InitialPosition, SubscriptionKind},
};

use super::{
    MemberHeartbeat, MemberInfo, MemberJoin, MembersBook, MembersPlan, MetadataState, RecordStatus,
    Rejection,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConsumerSubscription {
    pub id: Id,
    pub topic: Id,
    pub name: String,
    pub members: MembersBook,
    pub open_gates: std::collections::BTreeSet<Id>,
    pub deletion: Option<super::ConsumerDeletion>,
}

impl ConsumerSubscription {
    pub fn is_deleted(&self) -> bool {
        self.deletion.as_ref().is_some_and(|plan| plan.complete)
    }
    pub fn is_deleting(&self) -> bool {
        self.deletion.as_ref().is_some_and(|plan| !plan.complete)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ConsumerCommand {
    OpenedGate {
        subscription: Id,
        domain: Id,
    },
    Create {
        topic: Id,
        name: String,
        model: SubscriptionKind,
        initial: InitialPosition,
    },
    Join {
        subscription: Id,
        request: MemberJoin,
    },
    Heartbeat {
        subscription: Id,
        request: MemberHeartbeat,
    },
    Leave {
        subscription: Id,
        member: Id,
        epoch: u64,
        client: Id,
        credential: Id,
        now_ms: u64,
    },
    Retire {
        subscription: Id,
        member: Id,
        epoch: u64,
        client: Id,
        credential: Id,
    },
    Observe {
        subscription: Id,
        version: u64,
        fenced: bool,
        domain: Id,
        now_ms: u64,
    },
    Refresh {
        subscription: Id,
        now_ms: u64,
    },
    Delete {
        subscription: Id,
        model: SubscriptionKind,
    },
    ObserveDeleted {
        subscription: Id,
        deletion: Id,
        domain: Id,
    },
    FinishDelete {
        subscription: Id,
        deletion: Id,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ConsumerOutcome {
    Deletion(super::ConsumerDeletion),
    Retired,
    GateOpened,
    Created { subscription: Id },
    Joined(MemberInfo),
    Renewed(u64),
    Plan(MembersPlan),
}

impl MetadataState {
    pub fn consumer_subscription(&self, subscription: Id) -> Option<&ConsumerSubscription> {
        self.consumer_subscriptions.get(&subscription)
    }

    pub fn consumer_subscription_by_name(
        &self,
        topic: Id,
        name: &str,
    ) -> Option<&ConsumerSubscription> {
        self.consumer_subscriptions.values().find(|subscription| {
            subscription.topic == topic && subscription.name == name && !subscription.is_deleted()
        })
    }

    pub fn consumer_subscriptions(&self) -> impl Iterator<Item = &ConsumerSubscription> {
        self.consumer_subscriptions
            .values()
            .filter(|row| !row.is_deleted())
    }

    pub(super) fn consumer_command(
        &mut self,
        operation: Id,
        command: &ConsumerCommand,
    ) -> Result<ConsumerOutcome, Rejection> {
        match command {
            ConsumerCommand::Delete {
                subscription,
                model,
            } => self.begin_consumer_delete(operation, *subscription, *model),
            ConsumerCommand::ObserveDeleted {
                subscription,
                deletion,
                domain,
            } => self.observe_consumer_delete(*subscription, *deletion, *domain),
            ConsumerCommand::FinishDelete {
                subscription,
                deletion,
            } => self.finish_consumer_delete(*subscription, *deletion),
            ConsumerCommand::Retire {
                subscription,
                member,
                epoch,
                client,
                credential,
            } => {
                self.consumer_subscriptions
                    .get_mut(subscription)
                    .ok_or(Rejection::NotFound)?
                    .members
                    .retire(*member, *epoch, *client, *credential)?;
                Ok(ConsumerOutcome::Retired)
            }
            ConsumerCommand::OpenedGate {
                subscription,
                domain,
            } => {
                let row = self
                    .consumer_subscriptions
                    .get_mut(subscription)
                    .ok_or(Rejection::NotFound)?;
                if !row.members.plan().domains.contains(domain) {
                    return Err(Rejection::InvalidEvidence);
                }
                row.open_gates.insert(*domain);
                Ok(ConsumerOutcome::GateOpened)
            }
            ConsumerCommand::Create {
                topic,
                name,
                model,
                initial,
            } => self.create_consumer_subscription(operation, *topic, name, *model, *initial),
            ConsumerCommand::Join {
                subscription,
                request,
            } => {
                let row = self
                    .consumer_subscriptions
                    .get(subscription)
                    .ok_or(Rejection::NotFound)?;
                if row.deletion.is_some() {
                    return Err(Rejection::IdentityRetired);
                }
                let topic = row.topic;
                if self.topology.pending_transfers().any(|plan| {
                    plan.partition.topic_id == topic && plan.stage < super::TransferStage::Published
                }) {
                    return Err(Rejection::RecoveryRequired);
                }
                let total: usize = self
                    .consumer_subscriptions
                    .values()
                    .map(|record| {
                        record
                            .members
                            .live_for_client(request.client, request.now_ms)
                    })
                    .sum();
                let row = self
                    .consumer_subscriptions
                    .get_mut(subscription)
                    .ok_or(Rejection::NotFound)?;
                if total >= 128
                    && row
                        .members
                        .member(request.member, request.expected_epoch)
                        .is_none()
                {
                    return Err(Rejection::Capacity);
                }
                row.members.reap(request.now_ms);
                row.members
                    .join(operation, request)
                    .map(ConsumerOutcome::Joined)
            }
            ConsumerCommand::Heartbeat {
                subscription,
                request,
            } => self
                .consumer_subscriptions
                .get_mut(subscription)
                .ok_or(Rejection::NotFound)?
                .members
                .heartbeat(operation, request.clone())
                .map(ConsumerOutcome::Renewed),
            ConsumerCommand::Leave {
                subscription,
                member,
                epoch,
                client,
                credential,
                now_ms,
            } => self
                .consumer_subscriptions
                .get_mut(subscription)
                .ok_or(Rejection::NotFound)?
                .members
                .leave(operation, *member, *epoch, *client, *credential, *now_ms)
                .map(ConsumerOutcome::Plan),
            ConsumerCommand::Observe {
                subscription,
                version,
                fenced,
                domain,
                now_ms,
            } => self
                .consumer_subscriptions
                .get_mut(subscription)
                .ok_or(Rejection::NotFound)?
                .members
                .observe_installed(*version, *fenced, *domain, *now_ms)
                .map(ConsumerOutcome::Plan),
            ConsumerCommand::Refresh {
                subscription,
                now_ms,
            } => self.refresh_consumer_subscription(*subscription, *now_ms),
        }
    }

    fn create_consumer_subscription(
        &mut self,
        operation: Id,
        topic: Id,
        name: &str,
        model: SubscriptionKind,
        initial: InitialPosition,
    ) -> Result<ConsumerOutcome, Rejection> {
        if !super::types::valid_name(name, 128) {
            return Err(Rejection::InvalidName);
        }
        let topic_record = self
            .topics
            .get(&topic)
            .filter(|topic| topic.status == RecordStatus::Active)
            .ok_or(Rejection::NotFound)?;
        if let Some(previous) = self.consumer_subscription_by_name(topic, name) {
            if previous.deletion.is_some() {
                return Err(Rejection::IdentityRetired);
            }
            return if previous.members.model() == model {
                Ok(ConsumerOutcome::Created {
                    subscription: previous.id,
                })
            } else {
                Err(Rejection::InvalidSubscription)
            };
        }
        if self.topology.pending_transfers().any(|plan| {
            plan.partition.topic_id == topic && plan.stage < super::TransferStage::Published
        }) {
            return Err(Rejection::RecoveryRequired);
        }
        if self.consumer_subscriptions.contains_key(&operation) {
            return Err(Rejection::OperationConflict);
        }
        if self
            .consumer_subscriptions
            .len()
            .saturating_add(self.subscriptions.len())
            >= self.limits.subscriptions
        {
            return Err(Rejection::Capacity);
        }
        if self.auth.initialized() {
            let (tenant, _, _) = self
                .auth
                .resolve_topic(&topic_record.name)
                .map_err(Rejection::Authorization)?;
            let count = self
                .consumer_subscriptions
                .values()
                .filter(|subscription| {
                    !subscription.is_deleted()
                        && self
                            .topics
                            .get(&subscription.topic)
                            .is_some_and(|topic| topic.tenant == Some(tenant.id))
                })
                .count();
            if count >= tenant.max_subscriptions {
                return Err(Rejection::Capacity);
            }
        }
        let layout = self.topology.layout(topic).ok_or(Rejection::NotFound)?;
        let domains = self
            .topology
            .domains(topic)
            .map(|shard| shard.route.id)
            .collect();
        let members = MembersBook::new(operation, model, initial, layout.revision, domains)?;
        let open_gates = self
            .topology
            .domains(topic)
            .filter(|shard| model == SubscriptionKind::Queue || shard.predecessors.is_empty())
            .map(|shard| shard.route.id)
            .collect();
        self.consumer_subscriptions.insert(
            operation,
            ConsumerSubscription {
                id: operation,
                topic,
                name: name.to_owned(),
                members,
                open_gates,
                deletion: None,
            },
        );
        Ok(ConsumerOutcome::Created {
            subscription: operation,
        })
    }

    fn refresh_consumer_subscription(
        &mut self,
        subscription: Id,
        now: u64,
    ) -> Result<ConsumerOutcome, Rejection> {
        let row = self
            .consumer_subscriptions
            .get(&subscription)
            .ok_or(Rejection::NotFound)?;
        if row.deletion.is_some() {
            return Err(Rejection::IdentityRetired);
        }
        if self.topology.pending_transfers().any(|plan| {
            plan.partition.topic_id == row.topic && plan.stage < super::TransferStage::Published
        }) {
            return Ok(ConsumerOutcome::Plan(row.members.plan().clone()));
        }
        let layout = self.topology.layout(row.topic).ok_or(Rejection::NotFound)?;
        let domains = self
            .topology
            .domains(row.topic)
            .map(|shard| shard.route.id)
            .collect();
        let row = self
            .consumer_subscriptions
            .get_mut(&subscription)
            .ok_or(Rejection::NotFound)?;
        row.members.reap(now);
        row.members
            .refresh(layout.revision, domains, now)
            .map(ConsumerOutcome::Plan)
    }

    pub(super) fn validate_consumer_subscriptions(&self) -> Result<(), Rejection> {
        let mut names = std::collections::BTreeSet::new();
        if self
            .consumer_subscriptions
            .len()
            .saturating_add(self.subscriptions.len())
            > self.limits.subscriptions
        {
            return Err(Rejection::Capacity);
        }
        for (id, row) in &self.consumer_subscriptions {
            if *id != row.id
                || !self.topics.contains_key(&row.topic)
                || !super::types::valid_name(&row.name, 128)
                || !row.is_deleted() && !names.insert((row.topic, &row.name))
            {
                return Err(Rejection::InvalidSubscription);
            }
            row.members.validate()?;
            if let Some(plan) = &row.deletion {
                plan.validate(row)?;
            }
            if row
                .open_gates
                .iter()
                .any(|domain| !row.members.plan().domains.contains(domain))
            {
                return Err(Rejection::InvalidEvidence);
            }
            if !row.is_deleted()
                && row.members.plan().domains.iter().any(|id| {
                    self.topology
                        .shard(*id)
                        .is_none_or(|shard| shard.partition.topic_id != row.topic)
                })
            {
                return Err(Rejection::InvalidEvidence);
            }
        }
        Ok(())
    }
}
