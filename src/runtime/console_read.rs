use super::{
    Error, Result,
    service::{Service, forbidden},
    wire,
};
use crate::{
    gateway::Access,
    metadata::{PartitionKey, Permission},
};
use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};
use tidemq_protocol::{
    Id,
    common::{ErrorCode, Frame, FrameKind, TrafficClass},
    console::{self as view, Decimal, Query, Reply},
    framing::decode_payload,
    public,
};
use tokio::time::Instant;

#[path = "console_data.rs"]
mod data;
pub(super) use data::{DATA_QUERY, DataRead, DataReply};
#[path = "console_subscriptions.rs"]
mod subscriptions;

impl Service {
    pub(super) async fn console_read_public(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let credential = console_credential(access)?;
        let request: public::ConsoleRead =
            decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode console read envelope", source))?;
        let query: Query = decode_json(&request.json)?;
        if let Query::Messages { topic, shard, .. } = &query {
            tracing::info!(event_code = "console_message_inspection", %credential, %topic, %shard, request_id = ?wire::id(&frame.request_id).ok(), operation_id = ?wire::id(&frame.operation_id).ok(), result = "started", "Console message inspection began");
        }
        let result = self.console_read_query(credential, &query, deadline).await;
        if let Query::Messages { topic, shard, .. } = &query {
            match &result {
                Ok(Reply::Messages(page)) => {
                    tracing::info!(event_code = "console_message_inspection", %credential, %topic, %shard, request_id = ?wire::id(&frame.request_id).ok(), operation_id = ?wire::id(&frame.operation_id).ok(), scanned_start = page.scanned_start.0, scanned_end = page.scanned_end.0, hits = page.items.len(), stop_reason = %page.stop_reason, result = "succeeded", "Console message inspection completed")
                }
                Err(error) => {
                    tracing::info!(event_code = "console_message_inspection", %credential, %topic, %shard, request_id = ?wire::id(&frame.request_id).ok(), operation_id = ?wire::id(&frame.operation_id).ok(), error_code = error.failure().code, result = "rejected", "Console message inspection was rejected")
                }
                _ => {}
            }
        }
        let reply = result?;
        wire::response(
            frame,
            FrameKind::ConsoleReadResult,
            &public::ConsoleReadResult {
                json: encode_json(&reply)?,
            },
            &self.config.protocol,
        )
    }

    pub(super) async fn console_read_query(
        &self,
        credential: Id,
        query: &Query,
        deadline: Instant,
    ) -> Result<Reply> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
        let mut reply = match query {
            Query::Messages { topic, shard, .. } => {
                let binding = self
                    .console_binding(
                        credential,
                        *topic,
                        *shard,
                        Permission::InspectMessageContent,
                    )
                    .await?;
                let reply = self
                    .console_data_query(
                        binding.1,
                        DataRead::Messages {
                            partition: binding.0,
                            query: query.clone(),
                        },
                        deadline,
                    )
                    .await?;
                if self
                    .console_binding(
                        credential,
                        *topic,
                        *shard,
                        Permission::InspectMessageContent,
                    )
                    .await?
                    != binding
                {
                    return Err(Error::unavailable());
                }
                match reply {
                    DataReply::Messages(page) => Reply::Messages(page),
                    _ => return Err(Error::unavailable()),
                }
            }
            Query::Progress {
                topic,
                subscription,
                after,
                limit,
            } => Reply::Progress(
                self.console_progress(credential, *topic, *subscription, *after, *limit, deadline)
                    .await?,
            ),
            _ => replica
                .state
                .try_with_application(|state| {
                    state
                        .console_snapshot(credential, super::data::unix_millis()?, query)
                        .map_err(super::metadata::metadata_rejection)
                })
                .await
                .map_err(|source| Error::operation("read authorized console metadata", source))??,
        };
        match &mut reply {
            Reply::Topic(detail) => {
                self.console_enrich_topic(credential, detail, deadline)
                    .await?
            }
            Reply::Shards(page) => {
                let Query::Shards { topic, .. } = query else {
                    return Err(Error::unavailable());
                };
                for shard in &mut page.items {
                    page.observation.partial |= self
                        .console_enrich_shard(credential, *topic, shard, deadline)
                        .await?
                        .is_none();
                }
            }
            Reply::Cluster(page) => self.console_enrich_nodes(page, deadline).await?,
            Reply::Subscriptions(page) => {
                // Keep subscription sampling from enlarging every query future, including cold reads.
                Box::pin(self.console_enrich_subscriptions(credential, page, deadline)).await?
            }
            _ => {}
        }
        self.console_verify_reply(credential, query, &reply, deadline)
            .await?;
        Ok(reply)
    }

    async fn console_binding(
        &self,
        credential: Id,
        topic: Id,
        shard: Id,
        permission: Permission,
    ) -> Result<(PartitionKey, u64, u64)> {
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state
                    .console_authorize_topic(
                        credential,
                        super::data::unix_millis()?,
                        topic,
                        permission,
                    )
                    .map_err(super::metadata::metadata_rejection)?;
                let birth = state
                    .topology()
                    .shard(shard)
                    .filter(|birth| birth.partition.topic_id == topic)
                    .ok_or_else(super::metadata::not_found)?;
                let binding = state
                    .topology()
                    .binding(birth.partition)
                    .ok_or_else(Error::unavailable)?;
                Ok((birth.partition, binding.authority_group, binding.generation))
            })
            .await
            .map_err(|source| Error::operation("resolve authorized console shard", source))?
    }

    async fn console_enrich_topic(
        &self,
        credential: Id,
        detail: &mut view::TopicDetail,
        deadline: Instant,
    ) -> Result<()> {
        for shard in &mut detail.shard_details {
            if let Some(configuration) = self
                .console_enrich_shard(credential, detail.topic.id, shard, deadline)
                .await?
            {
                detail.configuration.push(configuration);
            } else {
                detail.observation.partial = true;
            }
        }
        Ok(())
    }

    async fn console_enrich_shard(
        &self,
        credential: Id,
        topic: Id,
        shard: &mut view::Shard,
        deadline: Instant,
    ) -> Result<Option<view::Configuration>> {
        let binding = self
            .console_binding(credential, topic, shard.id, Permission::Inspect)
            .await?;
        let Ok(DataReply::Summary(summary)) = self
            .console_data_query(
                binding.1,
                DataRead::Summary {
                    partition: binding.0,
                    subscription: None,
                },
                deadline.min(Instant::now() + std::time::Duration::from_millis(250)),
            )
            .await
        else {
            shard.reason = Some("dataObservationUnavailable".into());
            return Ok(None);
        };
        shard.retained_start = Some(Decimal(summary.retained_start));
        shard.committed_end = Some(Decimal(summary.committed_end));
        shard.reason = None;
        Ok(Some(view::Configuration {
            name: "minimumRetentionAgeMs".into(),
            value: summary.retention_age_ms.map(|value| value.to_string()),
            scope: format!("shard:{}", shard.id),
            source: "dataAppliedRetentionPolicy".into(),
        }))
    }

    async fn console_progress(
        &self,
        credential: Id,
        topic: Id,
        subscription: Id,
        after: Option<Id>,
        limit: u32,
        deadline: Instant,
    ) -> Result<view::Page<view::Progress>> {
        if !(1..=32).contains(&limit) {
            return Err(Error::invalid(
                "Progress page size must be between one and 32",
            ));
        }
        let (mut rows, revision, operator) = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state
                    .console_subscription(
                        credential,
                        super::data::unix_millis()?,
                        topic,
                        subscription,
                    )
                    .map_err(super::metadata::metadata_rejection)?;
                let rows = state
                    .topology()
                    .domains(topic)
                    .filter(|birth| after.is_none_or(|after| birth.route.id > after))
                    .take(limit as usize + 1)
                    .cloned()
                    .collect::<Vec<_>>();
                Ok::<_, Error>((
                    rows,
                    state.revision(),
                    state.console_operator(credential, super::data::unix_millis()?),
                ))
            })
            .await
            .map_err(|source| Error::operation("read authorized subscription shards", source))??;
        let next_cursor = if rows.len() > limit as usize {
            rows.pop();
            rows.last().map(|row| row.route.id.to_string())
        } else {
            None
        };
        let mut observation = view::Observation {
            observed_at_ms: Decimal(super::data::unix_millis()?),
            source: "metadataAndDataLinearizable".into(),
            revision: Some(Decimal(revision)),
            partial: false,
        };
        let mut items = Vec::new();
        for birth in rows {
            let binding = self
                .console_binding(credential, topic, birth.route.id, Permission::Inspect)
                .await?;
            let mut item = view::Progress {
                shard: birth.route.id,
                sealed: birth.sealed_end.is_some(),
                observed_at_ms: None,
                source: "dataObservationUnavailable".into(),
                revision: None,
                retained_start: None,
                committed_end: None,
                frontier: None,
                window_end: None,
                ready: None,
                in_flight: None,
                acked: None,
                paused: None,
                unconfirmed: None,
                tail: None,
                reason: Some("dataObservationUnavailable".into()),
                component_id: operator.then(|| format!("data-group:{}", binding.1)),
            };
            if let Ok(DataReply::Summary(summary)) = self
                .console_data_query(
                    binding.1,
                    DataRead::Summary {
                        partition: binding.0,
                        subscription: Some(subscription),
                    },
                    deadline.min(Instant::now() + std::time::Duration::from_millis(250)),
                )
                .await
            {
                item.observed_at_ms = Some(Decimal(summary.observed_at_ms));
                item.source = "dataLinearizableGroupCursor".into();
                item.revision = summary.revision.map(Decimal);
                item.retained_start = Some(Decimal(summary.retained_start));
                item.committed_end = Some(Decimal(summary.committed_end));
                if let Some(progress) = summary.progress {
                    item.frontier = Some(Decimal(progress.frontier));
                    item.window_end = Some(Decimal(progress.window_end));
                    item.ready = Some(Decimal(progress.ready));
                    item.in_flight = Some(Decimal(progress.in_flight));
                    item.acked = Some(Decimal(progress.acked));
                    item.paused = Some(Decimal(progress.paused));
                    item.unconfirmed = progress.unconfirmed.map(Decimal);
                    item.tail = progress.tail.map(Decimal);
                    item.reason = (!progress.gate_open).then(|| "assignmentGateBlocked".into());
                } else {
                    item.reason = Some("cursorNotMaterialized".into());
                }
            }
            observation.partial |= item.frontier.is_none();
            items.push(item);
        }
        Ok(view::Page {
            items,
            next_cursor,
            observation,
        })
    }

    async fn console_enrich_nodes(
        &self,
        page: &mut view::Page<view::Node>,
        deadline: Instant,
    ) -> Result<()> {
        for node in &mut page.items {
            let sample = if node.id.0 == self.config.node_id {
                self.console_node_probe(deadline).await
            } else {
                let until = deadline.min(Instant::now() + std::time::Duration::from_millis(350));
                match self
                    .rpc
                    .submit(
                        node.id.0,
                        self.config.runtime.metadata_group,
                        TrafficClass::Bulk,
                        super::node::random_id()?,
                        super::wire::Request::control(DATA_QUERY, &DataRead::NodeProbe)?,
                        until,
                        node.id.0,
                    )
                    .await
                {
                    Ok(pending) => match pending.receive().await {
                        Ok(bytes) => decode_json(&bytes),
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                }
            };
            if let Ok(DataReply::NodeProbe {
                node: id,
                instance,
                version,
                ready,
                sampled_at_ms,
                groups,
                groups_partial,
                log_dropped_events,
                log_sink_errors,
            }) = sample
            {
                if id == node.id.0 {
                    node.live = Some(true);
                    node.version = Some(version);
                    node.instance = Some(instance);
                    node.ready = ready;
                    node.sampled_at_ms = sampled_at_ms.map(Decimal);
                    node.groups = groups;
                    node.groups_partial = groups_partial;
                    node.log_dropped_events = log_dropped_events.map(Decimal);
                    node.log_sink_errors = log_sink_errors.map(Decimal);
                    node.reason = ready.is_none().then(|| "readinessSampleUnavailable".into());
                }
            } else {
                page.observation.partial = true;
            }
        }
        page.observation.source = "registryAndAuthenticatedNodeProbe".into();
        Ok(())
    }

    async fn console_verify_reply(
        &self,
        credential: Id,
        query: &Query,
        reply: &Reply,
        deadline: Instant,
    ) -> Result<()> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
        replica
            .state
            .try_with_application(|state| {
                let now = super::data::unix_millis()?;
                state
                    .auth()
                    .current_principal(credential, now)
                    .map_err(|_| forbidden())?;
                let topics: Vec<Id> = match reply {
                    Reply::Topics(page) => page.items.iter().map(|row| row.id).collect(),
                    Reply::Subscriptions(page) => {
                        page.items.iter().map(|row| row.topic_id).collect()
                    }
                    Reply::Topic(detail) => vec![detail.topic.id],
                    Reply::Cluster(_) => {
                        if !state.console_operator(credential, now) {
                            return Err(forbidden());
                        }
                        Vec::new()
                    }
                    _ => match query {
                        Query::Messages { topic, .. }
                        | Query::Shards { topic, .. }
                        | Query::Members { topic, .. }
                        | Query::Progress { topic, .. }
                        | Query::Producers { topic, .. } => vec![*topic],
                        _ => Vec::new(),
                    },
                };
                for topic in topics {
                    state
                        .console_authorize_topic(
                            credential,
                            now,
                            topic,
                            if matches!(query, Query::Messages { .. }) {
                                Permission::InspectMessageContent
                            } else {
                                Permission::Inspect
                            },
                        )
                        .map_err(super::metadata::metadata_rejection)?;
                }
                Ok(())
            })
            .await
            .map_err(|source| {
                Error::operation("recheck current console read authorization", source)
            })?
    }
}

fn console_credential(access: &Access) -> Result<Id> {
    access
        .principal
        .strip_prefix("credential:")
        .ok_or_else(forbidden)?
        .parse()
        .map_err(|_| forbidden())
}
fn encode_json<T: Serialize>(value: &T) -> Result<Bytes> {
    let value = serde_json::to_vec(value)
        .map_err(|source| Error::operation("encode bounded console reply", source))?;
    if value.len() > 256 * 1024 {
        return Err(Error::invalid("Console reply exceeds its finite bound"));
    }
    Ok(value.into())
}
fn decode_json<T: DeserializeOwned>(value: &[u8]) -> Result<T> {
    if value.len() > 256 * 1024 {
        return Err(Error::invalid("Console JSON exceeds its finite bound"));
    }
    serde_json::from_slice(value).map_err(|_| Error::invalid("Console JSON is invalid"))
}
