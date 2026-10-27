use super::*;
use tidemq_protocol::Id;
use tidemq_protocol::console as view;
use view::{Decimal, Observation, Page, Query, Reply};

impl MetadataState {
    pub fn console_snapshot(
        &self,
        credential: Id,
        now: u64,
        query: &Query,
    ) -> Result<Reply, Rejection> {
        self.auth
            .current_principal(credential, now)
            .map_err(Rejection::Authorization)?;
        match query {
            Query::Topics {
                tenant,
                namespace,
                after,
                limit,
            } => self
                .console_topics(credential, now, *tenant, *namespace, *after, *limit)
                .map(Reply::Topics),
            Query::Topic { topic } => self
                .console_topic(credential, now, *topic)
                .map(Reply::Topic),
            Query::Shards {
                topic,
                after,
                limit,
            } => self
                .console_shards(credential, now, *topic, *after, *limit)
                .map(Reply::Shards),
            Query::Subscriptions {
                topic,
                after,
                limit,
            } => self
                .console_subscriptions(credential, now, *topic, *after, *limit)
                .map(Reply::Subscriptions),
            Query::Members {
                topic,
                subscription,
                after,
                limit,
            } => self
                .console_members(credential, now, *topic, *subscription, *after, *limit)
                .map(Reply::Members),
            Query::Producers {
                topic,
                after,
                limit,
            } => self
                .console_producers(credential, now, *topic, *after, *limit)
                .map(Reply::Producers),
            Query::Cluster { after, limit } => self
                .console_nodes(credential, now, *after, *limit)
                .map(Reply::Cluster),
            _ => Err(Rejection::InvalidCommand),
        }
    }

    pub(crate) fn console_authorize_topic(
        &self,
        credential: Id,
        now: u64,
        topic: Id,
        permission: Permission,
    ) -> Result<&TopicRecord, Rejection> {
        let actor = self
            .auth
            .current_principal(credential, now)
            .map_err(Rejection::Authorization)?;
        let record = self
            .topic(topic)
            .filter(|row| {
                actor.tenant.is_none_or(|tenant| row.tenant == Some(tenant))
                    && actor
                        .namespace
                        .is_none_or(|namespace| row.namespace == Some(namespace))
            })
            .ok_or(Rejection::Authorization(AuthRejection::Forbidden))?;
        self.auth
            .authorize_topic(credential, now, permission, record)
            .map_err(Rejection::Authorization)?;
        Ok(record)
    }

    fn console_observation(&self, now: u64) -> Observation {
        Observation {
            observed_at_ms: Decimal(now),
            source: "metadataLinearizable".into(),
            revision: Some(Decimal(self.revision())),
            partial: false,
        }
    }

    fn console_topic_row(&self, row: &TopicRecord) -> Result<view::Topic, Rejection> {
        Ok(view::Topic {
            id: row.topic_id,
            name: row.name.clone(),
            tenant: row.tenant.ok_or(Rejection::InvalidCommand)?,
            namespace: row.namespace.ok_or(Rejection::InvalidCommand)?,
            writable: self.topic_writable(row.topic_id),
            shards: Decimal(
                self.topology()
                    .layout(row.topic_id)
                    .map_or(0, |layout| layout.shards.len() as u64),
            ),
        })
    }

    fn console_topics(
        &self,
        credential: Id,
        now: u64,
        tenant: Option<Id>,
        namespace: Option<Id>,
        after: Option<Id>,
        limit: u32,
    ) -> Result<Page<view::Topic>, Rejection> {
        let actor = self
            .auth
            .current_principal(credential, now)
            .map_err(Rejection::Authorization)?;
        if tenant.is_some_and(|tenant| actor.tenant.is_some_and(|own| tenant != own))
            || namespace
                .is_some_and(|namespace| actor.namespace.is_some_and(|own| namespace != own))
        {
            return Err(Rejection::Authorization(AuthRejection::Forbidden));
        }
        let limit = console_limit(limit)?;
        let rows = self.topics.values().filter(|row| {
            after.is_none_or(|after| row.topic_id > after)
                && row.status == RecordStatus::Active
                && tenant.is_none_or(|id| row.tenant == Some(id))
                && namespace.is_none_or(|id| row.namespace == Some(id))
                && self
                    .auth
                    .authorize_topic(credential, now, Permission::Inspect, row)
                    .is_ok()
        });
        let mut items = rows
            .take(limit + 1)
            .map(|row| self.console_topic_row(row))
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = trim_page(&mut items, limit, |row| row.id.to_string());
        Ok(Page {
            items,
            next_cursor,
            observation: self.console_observation(now),
        })
    }

    fn console_topic(
        &self,
        credential: Id,
        now: u64,
        topic: Id,
    ) -> Result<view::TopicDetail, Rejection> {
        let row = self.console_authorize_topic(credential, now, topic, Permission::Inspect)?;
        let topic = self.console_topic_row(row)?;
        let (_, limits) = self
            .auth
            .limits(topic.tenant, topic.namespace)
            .map_err(Rejection::Authorization)?;
        let scope = format!("namespace:{}", topic.namespace);
        let configuration = [
            ("topics", limits.max_topics as u64),
            ("subscriptions", limits.max_subscriptions as u64),
            ("members", limits.max_members as u64),
            ("backlogBytes", limits.capacity.backlog_bytes),
            ("inflightBytes", limits.capacity.inflight_bytes),
            ("inflightMessages", limits.capacity.inflight_messages),
            (
                "publishBytesPerSecond",
                limits.capacity.publish_bytes_per_second,
            ),
            (
                "consumeBytesPerSecond",
                limits.capacity.consume_bytes_per_second,
            ),
            ("coldReads", limits.capacity.cold_reads),
        ]
        .into_iter()
        .map(|(name, value)| view::Configuration {
            name: name.into(),
            value: Some(value.to_string()),
            scope: scope.clone(),
            source: "metadataEffectiveNamespaceLimits".into(),
        })
        .collect();
        let total_shards = Decimal(self.topology().domains(topic.id).count() as u64);
        let shards = self.console_shards(credential, now, topic.id, None, 32)?;
        Ok(view::TopicDetail {
            topic,
            configuration,
            shard_details: shards.items,
            next_shard_cursor: shards.next_cursor,
            total_shards,
            observation: self.console_observation(now),
        })
    }

    fn console_shards(
        &self,
        credential: Id,
        now: u64,
        topic: Id,
        after: Option<Id>,
        limit: u32,
    ) -> Result<Page<view::Shard>, Rejection> {
        self.console_authorize_topic(credential, now, topic, Permission::Inspect)?;
        let limit = console_limit(limit)?;
        let operator = self.console_operator(credential, now);
        let mut items = self
            .topology()
            .domains(topic)
            .filter(|shard| after.is_none_or(|after| shard.route.id > after))
            .take(limit + 1)
            .map(|shard| view::Shard {
                id: shard.route.id,
                sealed: shard.sealed_end.is_some(),
                retained_start: None,
                committed_end: shard.sealed_end.map(Decimal),
                group: operator
                    .then(|| {
                        self.topology()
                            .binding(shard.partition)
                            .map_or(shard.route.group, |binding| binding.authority_group)
                    })
                    .map(Decimal),
                reason: Some("dataObservationPending".into()),
            })
            .collect::<Vec<_>>();
        let next_cursor = trim_page(&mut items, limit, |row| row.id.to_string());
        Ok(Page {
            items,
            next_cursor,
            observation: self.console_observation(now),
        })
    }

    fn console_subscriptions(
        &self,
        credential: Id,
        now: u64,
        topic: Option<Id>,
        after: Option<Id>,
        limit: u32,
    ) -> Result<Page<view::Subscription>, Rejection> {
        if let Some(topic) = topic {
            self.console_authorize_topic(credential, now, topic, Permission::Inspect)?;
        }
        let limit = console_limit(limit)?;
        let rows = self.consumer_subscriptions().filter(|row| {
            after.is_none_or(|after| row.id > after)
                && topic.is_none_or(|topic| row.topic == topic)
                && self
                    .console_authorize_topic(credential, now, row.topic, Permission::Inspect)
                    .is_ok()
        });
        let mut items = rows
            .take(limit + 1)
            .map(|row| view::Subscription {
                id: row.id,
                name: row.name.clone(),
                topic_id: row.topic,
                topic_name: self
                    .topic(row.topic)
                    .map(|topic| topic.name.clone())
                    .unwrap_or_default(),
                model: model_name(row.members.model()).into(),
                members: Decimal(row.members.live_count(now) as u64),
                unconfirmed: None,
                progress_observation: view::SubscriptionProgressObservation::unknown("notSampled"),
                reason: if row.is_deleting() {
                    Some("deleting".into())
                } else if !row.members.plan().active {
                    Some("assignmentPreparing".into())
                } else {
                    None
                },
            })
            .collect::<Vec<_>>();
        let next_cursor = trim_page(&mut items, limit, |row| row.id.to_string());
        Ok(Page {
            items,
            next_cursor,
            observation: self.console_observation(now),
        })
    }

    pub(crate) fn console_subscription(
        &self,
        credential: Id,
        now: u64,
        topic: Id,
        subscription: Id,
    ) -> Result<&ConsumerSubscription, Rejection> {
        self.console_authorize_topic(credential, now, topic, Permission::Inspect)?;
        self.consumer_subscription(subscription)
            .filter(|row| row.topic == topic && !row.is_deleted())
            .ok_or(Rejection::NotFound)
    }

    fn console_members(
        &self,
        credential: Id,
        now: u64,
        topic: Id,
        subscription: Id,
        after: Option<Id>,
        limit: u32,
    ) -> Result<Page<view::Member>, Rejection> {
        let limit = console_limit(limit)?;
        let row = self.console_subscription(credential, now, topic, subscription)?;
        let latest: std::collections::BTreeMap<_, _> = row
            .members
            .observed_members()
            .map(|info| (info.member, info))
            .collect();
        let grants = row.members.grants(now)?;
        let mut items = latest
            .values()
            .filter(|info| after.is_none_or(|after| info.member > after))
            .take(limit + 1)
            .map(|info| {
                let live = info.expires_ms > now
                    && matches!(
                        info.status,
                        MemberStatus::Joining | MemberStatus::Active | MemberStatus::Leaving
                    );
                let lanes = grants
                    .iter()
                    .find(|grant| grant.member == info.member && grant.epoch == info.epoch);
                let shards =
                    if row.members.model() == tidemq_protocol::topic::SubscriptionKind::Queue {
                        row.members.plan().domains.clone()
                    } else {
                        self.topology()
                            .domains(topic)
                            .filter(|shard| {
                                lanes.is_some_and(|grant| {
                                    grant
                                        .lanes
                                        .iter()
                                        .any(|range| ranges_overlap(*range, shard.route.range))
                                })
                            })
                            .map(|shard| shard.route.id)
                            .collect()
                    };
                view::Member {
                    id: info.member,
                    epoch: Decimal(info.epoch),
                    lease_valid: live,
                    expires_at_ms: Decimal(info.expires_ms),
                    last_heartbeat_ms: None,
                    status: format!("{:?}", info.status).to_ascii_lowercase(),
                    shards,
                    reason: if !live {
                        Some("leaseInactive".into())
                    } else if !row.members.plan().active {
                        Some("assignmentPreparing".into())
                    } else {
                        None
                    },
                }
            })
            .collect::<Vec<_>>();
        let next_cursor = trim_page(&mut items, limit, |row| row.id.to_string());
        Ok(Page {
            items,
            next_cursor,
            observation: self.console_observation(now),
        })
    }

    fn console_producers(
        &self,
        credential: Id,
        now: u64,
        topic: Id,
        after: Option<Decimal>,
        limit: u32,
    ) -> Result<Page<view::Producer>, Rejection> {
        self.console_authorize_topic(credential, now, topic, Permission::Inspect)?;
        let limit = console_limit(limit)?;
        let mut items = self
            .publishers()
            .records()
            .filter(|row| {
                row.permit.topic == topic
                    && after.is_none_or(|after| row.permit.registration > after.0)
            })
            .take(limit + 1)
            .map(|row| {
                let permit = row.permit_at(now);
                view::Producer {
                    id: permit.producer,
                    registration: Decimal(permit.registration),
                    lease_valid: permit.live,
                    expires_at_ms: Decimal(permit.expires_ms),
                    last_heartbeat_ms: None,
                    reason: (!permit.live).then(|| "leaseInactive".into()),
                }
            })
            .collect::<Vec<_>>();
        let next_cursor = trim_page(&mut items, limit, |row| row.registration.0.to_string());
        Ok(Page {
            items,
            next_cursor,
            observation: self.console_observation(now),
        })
    }

    fn console_nodes(
        &self,
        credential: Id,
        now: u64,
        after: Option<Decimal>,
        limit: u32,
    ) -> Result<Page<view::Node>, Rejection> {
        if !self.console_operator(credential, now) {
            return Err(Rejection::Authorization(AuthRejection::Forbidden));
        }
        let limit = console_limit(limit)?;
        let mut items = self
            .nodes
            .values()
            .filter(|node| after.is_none_or(|after| node.spec.node_id > after.0))
            .take(limit + 1)
            .map(|node| view::Node {
                id: Decimal(node.spec.node_id),
                endpoint: node.spec.endpoint.to_string(),
                roles: node
                    .spec
                    .roles
                    .iter()
                    .map(|role| format!("{:?}", role).to_ascii_lowercase())
                    .collect(),
                schedulable: node.spec.schedulable,
                retired: self
                    .node_identity(node.spec.node_id)
                    .is_some_and(|identity| identity.retired),
                live: None,
                reason: Some("liveProbeUnavailable".into()),
                generation: Decimal(node.generation),
                zone: node.spec.zone.clone(),
                version: None,
                instance: None,
                ready: None,
                sampled_at_ms: None,
                groups: Vec::new(),
                groups_partial: false,
                log_dropped_events: None,
                log_sink_errors: None,
            })
            .collect::<Vec<_>>();
        let next_cursor = trim_page(&mut items, limit, |row| row.id.0.to_string());
        Ok(Page {
            items,
            next_cursor,
            observation: self.console_observation(now),
        })
    }

    pub(crate) fn console_operator(&self, credential: Id, now: u64) -> bool {
        self.auth
            .current_principal(credential, now)
            .is_ok_and(|actor| actor.tenant.is_none())
            && (self
                .auth
                .authorize(credential, now, Permission::Inspect, None)
                .is_ok()
                || self
                    .auth
                    .authorize(credential, now, Permission::ManageTenants, None)
                    .is_ok())
    }
}

fn console_limit(limit: u32) -> Result<usize, Rejection> {
    if !(1..=32).contains(&limit) {
        return Err(Rejection::InvalidLimit);
    }
    Ok(limit as usize)
}
fn trim_page<T>(
    items: &mut Vec<T>,
    limit: usize,
    cursor: impl FnOnce(&T) -> String,
) -> Option<String> {
    if items.len() <= limit {
        return None;
    }
    items.pop();
    items.last().map(cursor)
}
fn model_name(model: tidemq_protocol::topic::SubscriptionKind) -> &'static str {
    match model {
        tidemq_protocol::topic::SubscriptionKind::Stream => "stream",
        tidemq_protocol::topic::SubscriptionKind::Queue => "queue",
    }
}
fn ranges_overlap(
    left: tidemq_protocol::topic::HashRange,
    right: tidemq_protocol::topic::HashRange,
) -> bool {
    left.end.is_none_or(|end| right.start < end) && right.end.is_none_or(|end| left.start < end)
}
