use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    public,
    topic::{HashRange, ShardRoute, TopicLayout},
};

use crate::{Admin, Error, Result};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TopicId(pub(crate) Id);
impl From<Id> for TopicId {
    fn from(value: Id) -> Self {
        Self(value)
    }
}
impl From<TopicId> for Id {
    fn from(value: TopicId) -> Self {
        value.0
    }
}

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

    pub(crate) fn from_wire(name: String, mut wire: public::TopicLayoutResult) -> Result<Self> {
        if wire.total_shards == 0
            || wire.total_shards > 256
            || wire.total_shards as usize != wire.active.len().saturating_add(wire.history.len())
            || !wire.next_after_shard.is_empty()
        {
            return Err(Error::ResponseMismatch {
                reason: "topic layout is incomplete or exceeds its finite bound".into(),
            });
        }
        let mut identities = std::collections::BTreeSet::new();
        for shard in wire.active.iter().chain(&wire.history) {
            if !identities.insert(Id::from_slice(&shard.shard_id)?)
                || shard.group_id == 0
                || shard.authority_epoch == 0
                || shard.predecessors.len() > 2
            {
                return Err(Error::ResponseMismatch {
                    reason: "topic layout contains a duplicate or invalid birth domain".into(),
                });
            }
        }
        for shard in wire.active.iter().chain(&wire.history) {
            HashRange {
                start: shard.hash_start,
                end: shard.hash_end,
            }
            .validate()?;
            for predecessor in &shard.predecessors {
                if !identities.contains(&Id::from_slice(predecessor)?) {
                    return Err(Error::ResponseMismatch {
                        reason: "topic layout omitted a predecessor".into(),
                    });
                }
            }
        }
        wire.active.sort_by_key(|shard| shard.hash_start);
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
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        let name = self.client.topic_name(name)?;
        self.client
            .operation(|_| async move {
                let topic = Id::from_slice(&self.topic(&name).await?.topic_id)?;
                match self
                    .execute(crate::admin::pending::Request::DeleteTopic(topic))
                    .await?
                {
                    crate::AdminReceipt::TopicDeleted(_) => Ok(()),
                    _ => Err(Error::ResponseMismatch {
                        reason: "administration returned a non-deletion result".into(),
                    }),
                }
            })
            .await
    }

    pub async fn create_topic(&self, name: &str) -> Result<TopicInfo> {
        let name = self.client.topic_name(name)?;
        let receipt = self
            .execute(crate::admin::pending::Request::CreateTopic(name))
            .await?;
        match receipt {
            crate::AdminReceipt::Topic(info) => Ok(info),
            _ => Err(Error::ResponseMismatch {
                reason: "administration returned a non-topic result".into(),
            }),
        }
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
                            expected_revision: 0,
                            after_shard: Default::default(),
                        },
                        deadline,
                    )
                    .await?;
                let wire = complete_layout(&self.client, wire, deadline).await?;
                TopicInfo::from_wire(name, wire)
            })
            .await
    }

    pub async fn split_topic_at(&self, name: &str, boundary: u64) -> Result<TopicInfo> {
        self.client
            .operation(|_| self.split_topic_at_inner(name, boundary))
            .await
    }

    async fn split_topic_at_inner(&self, name: &str, boundary: u64) -> Result<TopicInfo> {
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
        self.client
            .operation(|_| self.merge_topic_at_inner(name, boundary))
            .await
    }

    async fn merge_topic_at_inner(&self, name: &str, boundary: u64) -> Result<TopicInfo> {
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
        let receipt = self
            .execute(crate::admin::pending::Request::Layout { name, request })
            .await?;
        match receipt {
            crate::AdminReceipt::Topic(info) => Ok(info),
            _ => Err(Error::ResponseMismatch {
                reason: "administration returned a non-topic result".into(),
            }),
        }
    }
}

pub(crate) async fn complete_layout(
    client: &crate::Client,
    first: public::TopicLayoutResult,
    deadline: tokio::time::Instant,
) -> Result<public::TopicLayoutResult> {
    let topic_id = first.topic_id.clone();
    let mut first = Some(first);
    for attempt in 0..4 {
        let page = match first.take() {
            Some(page) => page,
            None => {
                client
                    .request(
                        FrameKind::GetTopicLayout,
                        FrameKind::TopicLayoutResult,
                        TrafficClass::Control,
                        Id::new_v7()?,
                        &public::GetTopicLayout {
                            topic_id: topic_id.clone(),
                            expected_revision: 0,
                            after_shard: Default::default(),
                        },
                        deadline,
                    )
                    .await?
            }
        };
        match layout_pages(client, page, deadline).await {
            Err(Error::Remote {
                code: tidemq_protocol::common::ErrorCode::StaleGeneration,
                outcome: tidemq_protocol::common::Outcome::Rejected,
                ..
            }) if attempt < 3 => continue,
            result => return result,
        }
    }
    Err(Error::ResponseMismatch {
        reason: "topic layout snapshot did not finish within its bounded attempts".into(),
    })
}

async fn layout_pages(
    client: &crate::Client,
    mut layout: public::TopicLayoutResult,
    deadline: tokio::time::Instant,
) -> Result<public::TopicLayoutResult> {
    for _ in 0..8 {
        if layout.active.len().saturating_add(layout.history.len()) > 256 {
            return Err(Error::ResponseMismatch {
                reason: "topic layout exceeds its bounded birth-domain count".into(),
            });
        }
        if layout.next_after_shard.is_empty() {
            return Ok(layout);
        }
        let after = Id::from_slice(&layout.next_after_shard)?;
        let page: public::TopicLayoutResult = client
            .request(
                FrameKind::GetTopicLayout,
                FrameKind::TopicLayoutResult,
                TrafficClass::Control,
                Id::new_v7()?,
                &public::GetTopicLayout {
                    topic_id: layout.topic_id.clone(),
                    expected_revision: layout.revision,
                    after_shard: layout.next_after_shard.clone(),
                },
                deadline,
            )
            .await?;
        if page.topic_id != layout.topic_id
            || page.revision != layout.revision
            || page.total_shards != layout.total_shards
            || page.active.len().saturating_add(page.history.len()) > 32
            || !page.next_after_shard.is_empty() && Id::from_slice(&page.next_after_shard)? <= after
        {
            return Err(Error::ResponseMismatch {
                reason: "topic page changed its identity, revision, or cursor".into(),
            });
        }
        layout.active.extend(page.active);
        layout.history.extend(page.history);
        layout.next_after_shard = page.next_after_shard;
    }
    Err(Error::ResponseMismatch {
        reason: "topic layout pagination exceeds its finite bound".into(),
    })
}
