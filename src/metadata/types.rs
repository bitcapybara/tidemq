use std::collections::BTreeSet;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use super::Rejection;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataLimits {
    pub topics: usize,
    pub partitions: usize,
    pub partitions_per_topic: usize,
    pub subscriptions: usize,
    pub nodes: usize,
    pub groups: usize,
    pub operations: usize,
}

impl Default for MetadataLimits {
    fn default() -> Self {
        Self {
            topics: 1_024,
            partitions: 8_192,
            partitions_per_topic: 256,
            subscriptions: 4_096,
            nodes: 256,
            groups: 256,
            operations: 16_384,
        }
    }
}

impl MetadataLimits {
    pub fn validate(&self) -> Result<(), Rejection> {
        let maximum = Self::default();
        for (value, cap) in [
            (self.topics, maximum.topics),
            (self.partitions, maximum.partitions),
            (self.partitions_per_topic, maximum.partitions_per_topic),
            (self.subscriptions, maximum.subscriptions),
            (self.nodes, maximum.nodes),
            (self.groups, maximum.groups),
            (self.operations, maximum.operations),
        ] {
            if value == 0 || value > cap {
                return Err(Rejection::InvalidLimit);
            }
        }
        if self.partitions_per_topic > self.partitions {
            return Err(Rejection::InvalidLimit);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentMode {
    LocalRf1,
    ThreeReplica,
}

impl DeploymentMode {
    pub fn replication_factor(self) -> usize {
        match self {
            Self::LocalRf1 => 1,
            Self::ThreeReplica => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Gateway,
    Broker,
    Storage,
    Metadata,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeSpec {
    pub node_id: u64,
    pub zone: String,
    pub endpoint: SocketAddr,
    pub roles: BTreeSet<NodeRole>,
    pub schedulable: bool,
}

impl NodeSpec {
    pub(crate) fn validate(&self) -> Result<(), Rejection> {
        if self.node_id == 0
            || !valid_name(&self.zone, 64)
            || self.endpoint.port() == 0
            || self.endpoint.ip().is_unspecified()
            || self.roles.is_empty()
            || self.roles.len() > 4
        {
            return Err(Rejection::InvalidNode);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub spec: NodeSpec,
    pub generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataGroup {
    pub group_id: u64,
    pub generation: u64,
    pub voters: BTreeSet<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordStatus {
    Active,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicRecord {
    pub topic_id: Id,
    pub name: String,
    pub generation: u64,
    pub partition_count: u32,
    pub status: RecordStatus,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PartitionKey {
    pub topic_id: Id,
    pub partition: u32,
}

impl From<PartitionKey> for String {
    fn from(value: PartitionKey) -> Self {
        format!("{}/{}", value.topic_id, value.partition)
    }
}

impl TryFrom<String> for PartitionKey {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() > 64 {
            return Err("partition key is too long");
        }
        let (topic, partition) = value.split_once('/').ok_or("invalid partition key")?;
        let key = Self {
            topic_id: topic.parse().map_err(|_| "invalid topic identity")?,
            partition: partition.parse().map_err(|_| "invalid partition number")?,
        };
        if String::from(key) != value {
            return Err("partition key is not canonical");
        }
        Ok(key)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ordering {
    Parallel,
    PerKey,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionConfig {
    pub ordering: Ordering,
    pub max_inflight_messages: u32,
    pub max_inflight_bytes: u64,
    pub max_ack_holes: u32,
    pub protects_retention: bool,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            ordering: Ordering::Parallel,
            max_inflight_messages: 1_024,
            max_inflight_bytes: 16 * 1024 * 1024,
            max_ack_holes: 4_096,
            protects_retention: true,
        }
    }
}

impl SubscriptionConfig {
    pub(crate) fn validate(&self) -> Result<(), Rejection> {
        if self.max_inflight_messages == 0
            || self.max_inflight_messages > 65_536
            || self.max_inflight_bytes == 0
            || self.max_inflight_bytes > 64 * 1024 * 1024
            || self.max_ack_holes == 0
            || self.max_ack_holes > 65_536
        {
            return Err(Rejection::InvalidSubscription);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionRecord {
    pub subscription_id: Id,
    pub topic_id: Id,
    pub name: String,
    pub generation: u64,
    pub config: SubscriptionConfig,
    pub status: RecordStatus,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Route {
    pub partition: PartitionKey,
    pub group_id: u64,
    pub owner_node: u64,
    pub epoch: u64,
    pub generation: u64,
    pub ownership_operation: Id,
}

pub(crate) fn valid_name(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}
