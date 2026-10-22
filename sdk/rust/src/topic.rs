use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    public,
    topic::{HashRange, ShardRoute, TopicLayout},
};

use crate::{Admin, Error, Result};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TopicId(Id);

impl std::fmt::Display for TopicId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug)]
pub struct TopicInfo {
    pub(crate) id: Id,
    name: String,
    pub(crate) wire: public::TopicLayoutResult,
}

impl TopicInfo {
    pub fn id(&self) -> TopicId {
        TopicId(self.id)
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn revision(&self) -> u64 {
        self.wire.revision
    }
    pub fn shard_count(&self) -> usize {
        self.wire.active.len()
    }

    pub(crate) fn from_wire(name: String, wire: public::TopicLayoutResult) -> Result<Self> {
        let id = Id::from_slice(&wire.topic_id)?;
        let layout = TopicLayout {
            topic: id,
            revision: wire.revision,
            shards: wire
                .active
                .iter()
                .map(|shard| {
                    Ok(ShardRoute {
                        id: Id::from_slice(&shard.shard_id)?,
                        range: HashRange {
                            start: shard.hash_start,
                            end: shard.hash_end,
                        },
                        group: shard.group_id,
                        authority_epoch: shard.authority_epoch,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        };
        layout.validate(256)?;
        Ok(Self { id, name, wire })
    }
}

impl Admin {
    pub async fn create_logical_topic(&self, name: &str) -> Result<TopicInfo> {
        let name = self.client.topic_name(name)?;
        self.client
            .operation(|deadline| async move {
                let wire = self
                    .client
                    .request(
                        FrameKind::CreateLogicalTopic,
                        FrameKind::TopicLayoutResult,
                        TrafficClass::Control,
                        Id::new_v7()?,
                        &public::LogicalTopic { name: name.clone() },
                        deadline,
                    )
                    .await?;
                TopicInfo::from_wire(name, wire)
            })
            .await
    }

    pub async fn topic_info(&self, name: &str) -> Result<TopicInfo> {
        let name = self.client.topic_name(name)?;
        self.client
            .operation(|deadline| async move {
                let topic = self.topic(&name).await?;
                let wire = self
                    .client
                    .request(
                        FrameKind::GetTopicLayout,
                        FrameKind::TopicLayoutResult,
                        TrafficClass::Control,
                        Id::new_v7()?,
                        &public::GetTopicLayout {
                            topic_id: topic.topic_id,
                        },
                        deadline,
                    )
                    .await?;
                TopicInfo::from_wire(name, wire)
            })
            .await
    }

    pub async fn split_topic_at(&self, name: &str, boundary: u64) -> Result<TopicInfo> {
        let info = self.topic_info(name).await?;
        let source = info
            .wire
            .active
            .iter()
            .find(|shard| {
                boundary > shard.hash_start && shard.hash_end.is_none_or(|end| boundary < end)
            })
            .ok_or(Error::Configuration {
                reason: "split boundary must be inside one active range",
            })?;
        let request = public::ChangeTopicLayout {
            topic_id: info.wire.topic_id.clone(),
            expected_revision: info.wire.revision,
            left_shard: source.shard_id.clone(),
            right_shard: Default::default(),
            split_at: Some(boundary),
        };
        self.change_topic_layout(info.name, request).await
    }

    pub async fn merge_topic_at(&self, name: &str, boundary: u64) -> Result<TopicInfo> {
        let info = self.topic_info(name).await?;
        let left = info
            .wire
            .active
            .iter()
            .find(|shard| shard.hash_end == Some(boundary))
            .ok_or(Error::Configuration {
                reason: "merge boundary must separate two adjacent active ranges",
            })?;
        let right = info
            .wire
            .active
            .iter()
            .find(|shard| shard.hash_start == boundary)
            .ok_or(Error::Configuration {
                reason: "merge boundary must separate two adjacent active ranges",
            })?;
        let request = public::ChangeTopicLayout {
            topic_id: info.wire.topic_id.clone(),
            expected_revision: info.wire.revision,
            left_shard: left.shard_id.clone(),
            right_shard: right.shard_id.clone(),
            split_at: None,
        };
        self.change_topic_layout(info.name, request).await
    }

    async fn change_topic_layout(
        &self,
        name: String,
        request: public::ChangeTopicLayout,
    ) -> Result<TopicInfo> {
        self.client
            .operation(|deadline| async move {
                let wire = self
                    .client
                    .request(
                        FrameKind::ChangeTopicLayout,
                        FrameKind::TopicLayoutResult,
                        TrafficClass::Control,
                        Id::new_v7()?,
                        &request,
                        deadline,
                    )
                    .await?;
                TopicInfo::from_wire(name, wire)
            })
            .await
    }
}
