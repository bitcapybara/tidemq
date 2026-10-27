use super::*;
use crate::producer::{MessageData, PublishReceipt};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use tidemq_protocol::topic::SendIdentity;

pub(crate) const DATA_QUERY: u32 = 1100;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum DataRead {
    Summary {
        partition: PartitionKey,
        subscription: Option<Id>,
    },
    Messages {
        partition: PartitionKey,
        query: Query,
    },
    PublishReceipt {
        partition: PartitionKey,
        identity: SendIdentity,
        digest: [u8; 32],
    },
    NodeProbe,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct DataSummary {
    pub retained_start: u64,
    pub committed_end: u64,
    pub revision: Option<u64>,
    pub observed_at_ms: u64,
    pub progress: Option<crate::subscription::GroupProgress>,
    pub retention_age_ms: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum DataReply {
    Summary(DataSummary),
    Messages(view::MessagePage),
    PublishReceipt(Option<PublishReceipt>),
    NodeProbe {
        node: u64,
        instance: Id,
        version: String,
        ready: Option<bool>,
        sampled_at_ms: Option<u64>,
        groups: Vec<view::NodeGroup>,
        groups_partial: bool,
        log_dropped_events: Option<u64>,
        log_sink_errors: Option<u64>,
    },
}

impl Service {
    pub(in crate::runtime) async fn console_data_query(
        &self,
        group: u64,
        query: DataRead,
        deadline: Instant,
    ) -> Result<DataReply> {
        if self.data.contains_key(&group)
            && self
                .data
                .get(&group)
                .is_some_and(|replica| self.require_leader(replica).is_ok())
        {
            return self.console_data_local(group, query, deadline).await;
        }
        let leader = self.leader(group, deadline).await?;
        let encoded = self
            .rpc
            .submit(
                leader,
                group,
                TrafficClass::Bulk,
                super::super::node::random_id()?,
                super::super::wire::Request::control(DATA_QUERY, &query)?,
                deadline,
                group,
            )
            .await?
            .receive()
            .await?;
        decode_json(&encoded)
    }

    pub(in crate::runtime) async fn console_data_command(
        &self,
        group: u64,
        body: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let query: DataRead = decode_json(body)?;
        let reply = self.console_data_local(group, query, deadline).await?;
        encode_json(&reply)
    }

    pub(in crate::runtime) async fn console_node_probe(
        &self,
        deadline: Instant,
    ) -> Result<DataReply> {
        let now = super::super::data::unix_millis()?;
        let sample = self
            .runtime_metrics
            .0
            .lock()
            .map_err(|_| Error::unavailable())?
            .clone();
        let fresh = sample
            .sampled_at_ms
            .is_some_and(|sampled| now.saturating_sub(sampled) <= 5_000);
        let mut groups = Vec::new();
        if let Some(replica) = &self.metadata {
            groups.push(
                self.console_group_probe(
                    self.config.runtime.metadata_group,
                    "metadata",
                    replica,
                    deadline,
                )
                .await,
            );
        }
        for (group, replica) in self.data.iter().take(32) {
            let mut probe = self
                .console_group_probe(*group, "storage", replica, deadline)
                .await;
            if let Ok((bytes, messages)) = replica
                .state
                .try_with_application(|state| (state.hot_bytes(), state.hot_messages()))
                .await
            {
                probe.hot_bytes = Some(Decimal(bytes));
                probe.hot_messages = Some(Decimal(messages as u64));
            }
            groups.push(probe);
        }
        Ok(DataReply::NodeProbe {
            node: self.config.node_id,
            instance: self.instance,
            version: env!("CARGO_PKG_VERSION").into(),
            ready: fresh.then_some(sample.ready),
            sampled_at_ms: sample.sampled_at_ms,
            groups,
            groups_partial: self.data.len() > 32,
            log_dropped_events: fresh.then_some(sample.log_dropped_events).flatten(),
            log_sink_errors: fresh.then_some(sample.log_sink_errors).flatten(),
        })
    }

    async fn console_group_probe<A: crate::replication::Application>(
        &self,
        group: u64,
        role: &str,
        replica: &super::super::node::Replica<A>,
        deadline: Instant,
    ) -> view::NodeGroup {
        let metrics = replica.raft.metrics();
        let (leader, voters) = {
            let metrics = metrics.borrow();
            (
                metrics.current_leader,
                metrics
                    .membership_config
                    .membership()
                    .voter_ids()
                    .map(Decimal)
                    .collect(),
            )
        };
        let quorum_confirmed = if leader == Some(self.config.node_id) {
            Some(
                tokio::time::timeout_at(
                    deadline.min(Instant::now() + std::time::Duration::from_millis(150)),
                    replica.raft.ensure_linearizable(),
                )
                .await
                .is_ok_and(|result| result.is_ok()),
            )
        } else {
            None
        };
        view::NodeGroup {
            id: Decimal(group),
            role: role.into(),
            leader: leader.map(Decimal),
            voters,
            quorum_confirmed,
            hot_bytes: None,
            hot_messages: None,
        }
    }

    async fn console_data_local(
        &self,
        group: u64,
        query: DataRead,
        deadline: Instant,
    ) -> Result<DataReply> {
        if matches!(query, DataRead::NodeProbe) {
            return self.console_node_probe(deadline).await;
        }
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        match query {
            DataRead::Summary {
                partition,
                subscription,
            } => replica
                .state
                .try_with_applied(|state, applied| {
                    let producer = state
                        .producer(partition)
                        .ok_or_else(super::super::metadata::not_found)?;
                    Ok(DataReply::Summary(DataSummary {
                        retained_start: producer.retained_floor(),
                        committed_end: producer.committed_end(),
                        revision: applied.map(|position| position.index),
                        observed_at_ms: super::super::data::unix_millis()?,
                        progress: subscription
                            .and_then(|subscription| {
                                state.consumer_group_cursor(partition, subscription)
                            })
                            .map(|cursor| cursor.observe_progress(producer.committed_end())),
                        retention_age_ms: state
                            .retention(partition)
                            .map(|retention| retention.policy().minimum_age_ms),
                    }))
                })
                .await
                .map_err(|source| Error::operation("observe console shard", source))?,
            DataRead::PublishReceipt {
                partition,
                identity,
                digest,
            } => replica
                .state
                .try_with_application(|state| {
                    if identity.topic != partition.topic_id {
                        return Err(forbidden());
                    }
                    let producer = state
                        .producer(partition)
                        .ok_or_else(super::super::metadata::not_found)?;
                    producer
                        .publish_result(identity, digest)
                        .map(DataReply::PublishReceipt)
                        .map_err(|source| {
                            Error::operation("read original console publish receipt", source)
                        })
                })
                .await
                .map_err(|source| Error::operation("read console receipt state", source))?,
            DataRead::Messages { partition, query } => self
                .console_messages_local(group, partition, query, deadline)
                .await
                .map(DataReply::Messages),
            DataRead::NodeProbe => self.console_node_probe(deadline).await,
        }
    }

    async fn console_messages_local(
        &self,
        group: u64,
        partition: PartitionKey,
        query: Query,
        deadline: Instant,
    ) -> Result<view::MessagePage> {
        let Query::Messages {
            topic,
            shard,
            start,
            end,
            observed_end,
            limit,
            scan_limit,
            key_base64,
            properties_base64,
        } = query
        else {
            return Err(Error::invalid("Message query is required"));
        };
        if topic != partition.topic_id
            || !(1..=32).contains(&limit)
            || !(1..=1024).contains(&scan_limit)
            || properties_base64.len() > 16
        {
            return Err(Error::invalid("Message query exceeds its finite bounds"));
        }
        let key = key_base64.map(|key| decode_filter(&key)).transpose()?;
        let properties = properties_base64
            .into_iter()
            .map(|(name, value)| {
                if name.is_empty() || name.len() > 256 {
                    return Err(Error::invalid("Property name exceeds its finite bound"));
                }
                Ok((name, decode_filter(&value)?))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
        let summary = match self.console_data_local_summary(group, partition).await? {
            DataReply::Summary(summary) => summary,
            _ => return Err(Error::unavailable()),
        };
        let observed = observed_end.map_or(summary.committed_end, |end| end.0);
        if observed > summary.committed_end {
            return Err(Error::invalid(
                "Observed message end is beyond committed data",
            ));
        }
        let lower = start.map_or_else(
            || observed.saturating_sub(100).max(summary.retained_start),
            |start| start.0,
        );
        let upper = end.map_or(observed, |end| end.0);
        if lower < summary.retained_start {
            return Err(Error::reject(
                ErrorCode::OffsetExpired,
                tidemq_protocol::common::Outcome::Rejected,
                false,
            ));
        }
        if lower > upper || upper > observed {
            return Err(Error::invalid("Message offset window is invalid"));
        }
        let mut page = view::MessagePage {
            items: Vec::new(),
            shard,
            retained_start: Decimal(summary.retained_start),
            observed_end: Decimal(observed),
            scanned_start: Decimal(lower),
            scanned_end: Decimal(lower),
            stop_reason: "rangeEnd".into(),
            next_offset: None,
            observation: data_observation(&summary),
        };
        let mut scanned_bytes = 0_u64;
        while page.scanned_end.0 < upper {
            if page.scanned_end.0.saturating_sub(lower) >= u64::from(scan_limit)
                || scanned_bytes >= 1024 * 1024
                || page.items.len() >= limit as usize
            {
                page.stop_reason = "budget".into();
                page.next_offset = Some(page.scanned_end);
                break;
            }
            wire::remaining(deadline)?;
            let offset = page.scanned_end.0;
            let (message, tier) = self
                .console_message_at(group, partition, offset, deadline)
                .await?;
            scanned_bytes = scanned_bytes.saturating_add(
                message
                    .logical_bytes()
                    .ok_or_else(|| Error::invalid("Message size overflow"))?,
            );
            let metadata = message
                .metadata(topic)
                .map_err(|source| Error::operation("inspect stored message context", source))?;
            page.scanned_end = Decimal(
                offset
                    .checked_add(1)
                    .ok_or_else(|| Error::invalid("Message offset overflow"))?,
            );
            if key.as_ref().is_some_and(|expected| {
                !metadata.key_present || expected.as_slice() != message.key.as_ref()
            }) || properties.iter().any(|(name, expected)| {
                metadata
                    .properties
                    .get(name)
                    .is_none_or(|value| value.as_ref() != expected.as_slice())
            }) {
                continue;
            }
            let preview = message
                .body
                .get(..message.body.len().min(8192))
                .ok_or_else(|| Error::invalid("Message preview range is invalid"))?;
            let candidate = view::Message {
                id: format!("{topic}:{shard}:{offset}"),
                shard,
                offset: Decimal(offset),
                payload_base64: STANDARD.encode(preview),
                key_base64: metadata.key_present.then(|| {
                    STANDARD.encode(
                        message
                            .key
                            .get(..message.key.len().min(8192))
                            .unwrap_or_default(),
                    )
                }),
                properties_base64: metadata
                    .properties
                    .iter()
                    .take(16)
                    .map(|(name, value)| {
                        (
                            name.clone(),
                            STANDARD.encode(value.get(..value.len().min(512)).unwrap_or_default()),
                        )
                    })
                    .collect(),
                tier: tier.into(),
                payload_bytes: Decimal(message.body.len() as u64),
                payload_truncated: preview.len() != message.body.len(),
                metadata_truncated: message.key.len() > 8192
                    || metadata.properties.len() > 16
                    || metadata.properties.values().any(|value| value.len() > 512),
            };
            let candidate_size = encode_json(&candidate)?.len();
            if encode_json(&page)?.len().saturating_add(candidate_size) > 240 * 1024 {
                page.scanned_end = Decimal(offset);
                page.next_offset = Some(Decimal(offset));
                page.stop_reason = "responseBudget".into();
                break;
            }
            page.items.push(candidate);
        }
        Ok(page)
    }

    async fn console_data_local_summary(
        &self,
        group: u64,
        partition: PartitionKey,
    ) -> Result<DataReply> {
        self.data
            .get(&group)
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_applied(|state, applied| {
                let producer = state
                    .producer(partition)
                    .ok_or_else(super::super::metadata::not_found)?;
                Ok(DataReply::Summary(DataSummary {
                    retained_start: producer.retained_floor(),
                    committed_end: producer.committed_end(),
                    revision: applied.map(|position| position.index),
                    observed_at_ms: super::super::data::unix_millis()?,
                    progress: None,
                    retention_age_ms: None,
                }))
            })
            .await
            .map_err(|source| Error::operation("read inspection range boundaries", source))?
    }

    async fn console_message_at(
        &self,
        group: u64,
        partition: PartitionKey,
        offset: u64,
        deadline: Instant,
    ) -> Result<(MessageData, &'static str)> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        let (hot, fence) = replica
            .state
            .try_with_application(|state| {
                let producer = state
                    .producer(partition)
                    .ok_or_else(super::super::metadata::not_found)?;
                let hot = producer.hot_read(offset, 1, producer.limits().max_batch_bytes);
                let fence = state
                    .ownership()
                    .and_then(|ownership| ownership.fence(partition))
                    .cloned();
                Ok::<_, Error>((hot, fence))
            })
            .await
            .map_err(|source| Error::operation("inspect hot message", source))??;
        match hot {
            Ok(page) => {
                return page
                    .messages
                    .into_iter()
                    .next()
                    .map(|message| (message, "hot"))
                    .ok_or_else(Error::unavailable);
            }
            Err(crate::producer::Rejection::OffsetExpired { .. }) => {}
            Err(source) => return Err(Error::operation("inspect bounded hot page", source)),
        }
        let fence = fence.ok_or_else(Error::unavailable)?;
        let route = crate::metadata::Route {
            partition,
            group_id: group,
            owner_node: fence.grant.owner_node,
            epoch: fence.grant.epoch,
            generation: fence.grant.expected_route_generation.saturating_add(1),
            ownership_operation: fence.grant.operation_id,
        };
        let page = self
            .read_tenant_archive(&route, offset, 1, 1024 * 1024, deadline)
            .await?;
        let result = super::super::data::cold_message(&page, offset).and_then(|message| {
            message
                .map(|(message, _)| (message, "cold"))
                .ok_or_else(Error::unavailable)
        });
        let release = page
            .release(deadline.into_std())
            .await
            .map_err(|source| Error::operation("release message inspection archive pin", source));
        let message = result?;
        release?;
        Ok(message)
    }
}

fn decode_filter(value: &str) -> Result<Vec<u8>> {
    if value.len() > 4096 {
        return Err(Error::invalid("Message filter exceeds its finite bound"));
    }
    STANDARD
        .decode(value)
        .map_err(|_| Error::invalid("Message filter is not canonical Base64"))
}
fn data_observation(summary: &DataSummary) -> view::Observation {
    view::Observation {
        observed_at_ms: Decimal(summary.observed_at_ms),
        source: "dataLinearizable".into(),
        revision: summary.revision.map(Decimal),
        partial: false,
    }
}
