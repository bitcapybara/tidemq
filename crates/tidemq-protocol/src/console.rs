//! Bounded console reads; none of these requests registers a consumer or acknowledges data.

use crate::Id;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Decimal(pub u64);
impl Serialize for Decimal {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}
impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.is_empty()
            || value.len() > 1 && value.starts_with('0')
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(serde::de::Error::custom(
                "expected an unsigned decimal string",
            ));
        }
        value.parse().map(Self).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum Query {
    Topics {
        tenant: Option<Id>,
        namespace: Option<Id>,
        after: Option<Id>,
        limit: u32,
    },
    Topic {
        topic: Id,
    },
    Shards {
        topic: Id,
        after: Option<Id>,
        limit: u32,
    },
    Messages {
        topic: Id,
        shard: Id,
        start: Option<Decimal>,
        end: Option<Decimal>,
        observed_end: Option<Decimal>,
        limit: u32,
        scan_limit: u32,
        key_base64: Option<String>,
        properties_base64: BTreeMap<String, String>,
    },
    Subscriptions {
        topic: Option<Id>,
        after: Option<Id>,
        limit: u32,
    },
    Members {
        topic: Id,
        subscription: Id,
        after: Option<Id>,
        limit: u32,
    },
    Progress {
        topic: Id,
        subscription: Id,
        after: Option<Id>,
        limit: u32,
    },
    Producers {
        topic: Id,
        after: Option<Decimal>,
        limit: u32,
    },
    Cluster {
        after: Option<Decimal>,
        limit: u32,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Observation {
    pub observed_at_ms: Decimal,
    pub source: String,
    pub revision: Option<Decimal>,
    pub partial: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    #[serde(flatten)]
    pub observation: Observation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "camelCase")]
pub enum Reply {
    Topics(Page<Topic>),
    Topic(TopicDetail),
    Shards(Page<Shard>),
    Messages(MessagePage),
    Subscriptions(Page<Subscription>),
    Members(Page<Member>),
    Progress(Page<Progress>),
    Producers(Page<Producer>),
    Cluster(Page<Node>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Topic {
    pub id: Id,
    pub name: String,
    pub tenant: Id,
    pub namespace: Id,
    pub writable: bool,
    pub shards: Decimal,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Configuration {
    pub name: String,
    pub value: Option<String>,
    pub scope: String,
    pub source: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Shard {
    pub id: Id,
    pub sealed: bool,
    pub retained_start: Option<Decimal>,
    pub committed_end: Option<Decimal>,
    pub group: Option<Decimal>,
    pub reason: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicDetail {
    #[serde(flatten)]
    pub topic: Topic,
    pub configuration: Vec<Configuration>,
    pub shard_details: Vec<Shard>,
    pub next_shard_cursor: Option<String>,
    pub total_shards: Decimal,
    #[serde(flatten)]
    pub observation: Observation,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: String,
    pub shard: Id,
    pub offset: Decimal,
    pub payload_base64: String,
    pub key_base64: Option<String>,
    pub properties_base64: BTreeMap<String, String>,
    pub tier: String,
    pub payload_bytes: Decimal,
    pub payload_truncated: bool,
    pub metadata_truncated: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagePage {
    pub items: Vec<Message>,
    pub shard: Id,
    pub retained_start: Decimal,
    pub observed_end: Decimal,
    pub scanned_start: Decimal,
    pub scanned_end: Decimal,
    pub stop_reason: String,
    pub next_offset: Option<Decimal>,
    #[serde(flatten)]
    pub observation: Observation,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    pub id: Id,
    pub name: String,
    pub topic_id: Id,
    pub topic_name: String,
    pub model: String,
    pub members: Decimal,
    pub unconfirmed: Option<Decimal>,
    pub reason: Option<String>,
    pub progress_observation: SubscriptionProgressObservation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionProgressObservation {
    pub source: String,
    pub observed_from_ms: Option<Decimal>,
    pub observed_until_ms: Option<Decimal>,
    pub sampled_shards: Option<Decimal>,
    pub partial: bool,
    pub reason: Option<String>,
}

impl SubscriptionProgressObservation {
    pub fn unknown(reason: &str) -> Self {
        Self {
            source: "perShardLinearizableGroupCursorSamples".into(),
            observed_from_ms: None,
            observed_until_ms: None,
            sampled_shards: Some(Decimal(0)),
            partial: true,
            reason: Some(reason.into()),
        }
    }

    /// Preserve actual sample ranges; a merged range never denotes an atomic snapshot.
    pub fn combine(samples: impl IntoIterator<Item = Self>, complete: bool) -> Self {
        let mut result = Self::unknown("subscriptionProgressIncomplete");
        result.partial = !complete;
        result.reason = None;
        for sample in samples {
            if let Some(value) = sample.observed_from_ms {
                result.observed_from_ms = Some(
                    result
                        .observed_from_ms
                        .map_or(value, |previous| Decimal(previous.0.min(value.0))),
                );
            }
            if let Some(value) = sample.observed_until_ms {
                result.observed_until_ms = Some(
                    result
                        .observed_until_ms
                        .map_or(value, |previous| Decimal(previous.0.max(value.0))),
                );
            }
            let combined = result
                .sampled_shards
                .zip(sample.sampled_shards)
                .and_then(|(total, value)| total.0.checked_add(value.0).map(Decimal));
            if result.sampled_shards.is_some()
                && sample.sampled_shards.is_some()
                && combined.is_none()
            {
                result.reason = Some("counterOverflow".into());
            }
            result.sampled_shards = combined;
            result.partial |= sample.partial || combined.is_none();
            if result.reason.is_none() && sample.partial {
                result.reason = sample.reason;
            }
        }
        if result.partial && result.reason.is_none() {
            result.reason = Some("subscriptionProgressIncomplete".into());
        }
        result
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Member {
    pub id: Id,
    pub epoch: Decimal,
    pub lease_valid: bool,
    pub expires_at_ms: Decimal,
    pub last_heartbeat_ms: Option<Decimal>,
    pub status: String,
    pub shards: Vec<Id>,
    pub reason: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Producer {
    pub id: Id,
    pub registration: Decimal,
    pub lease_valid: bool,
    pub expires_at_ms: Decimal,
    pub last_heartbeat_ms: Option<Decimal>,
    pub reason: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Progress {
    pub shard: Id,
    pub sealed: bool,
    pub observed_at_ms: Option<Decimal>,
    pub source: String,
    pub revision: Option<Decimal>,
    pub retained_start: Option<Decimal>,
    pub committed_end: Option<Decimal>,
    pub frontier: Option<Decimal>,
    pub window_end: Option<Decimal>,
    pub ready: Option<Decimal>,
    pub in_flight: Option<Decimal>,
    pub acked: Option<Decimal>,
    pub paused: Option<Decimal>,
    pub unconfirmed: Option<Decimal>,
    pub tail: Option<Decimal>,
    pub reason: Option<String>,
    pub component_id: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    pub id: Decimal,
    pub endpoint: String,
    pub roles: Vec<String>,
    pub schedulable: bool,
    pub retired: bool,
    pub live: Option<bool>,
    pub reason: Option<String>,
    pub generation: Decimal,
    pub zone: String,
    pub version: Option<String>,
    pub instance: Option<Id>,
    pub ready: Option<bool>,
    pub sampled_at_ms: Option<Decimal>,
    pub groups: Vec<NodeGroup>,
    pub groups_partial: bool,
    pub log_dropped_events: Option<Decimal>,
    pub log_sink_errors: Option<Decimal>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeGroup {
    pub id: Decimal,
    pub role: String,
    pub leader: Option<Decimal>,
    pub voters: Vec<Decimal>,
    pub quorum_confirmed: Option<bool>,
    pub hot_bytes: Option<Decimal>,
    pub hot_messages: Option<Decimal>,
}
