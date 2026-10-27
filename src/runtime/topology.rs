use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::{
    Id,
    common::{Frame, FrameKind},
    framing::decode_payload,
    public,
};
use tokio::time::Instant;

use crate::{
    broker::{DataCommand, DataResponse, WriterFence},
    gateway::Access,
    metadata::{self, BirthShard, Command, LayoutChange, LayoutPlan, LayoutStage, PartitionKey},
};

use super::{
    Error, Result,
    service::Service,
    wire::{self, Request},
};

pub(super) const SEAL_SHARD: u32 = 1_001;

#[derive(Serialize, Deserialize)]
struct SealRequest {
    plan: Id,
    partition: PartitionKey,
}

#[derive(Serialize, Deserialize)]
struct SealReply {
    plan: Id,
    partition: PartitionKey,
    end: u64,
}

impl Service {
    pub(super) async fn topology_public(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let kind = super::service::frame_kind(frame)?;
        if kind == FrameKind::CreateLogicalTopic {
            return self.create_logical_topic(access, frame, deadline).await;
        }
        let topic = if kind == FrameKind::GetTopicLayout {
            let request: public::GetTopicLayout =
                decode_payload(frame.payload.clone(), &self.config.protocol)
                    .map_err(|source| Error::operation("decode layout lookup", source))?;
            wire::id(&request.topic_id)?
        } else {
            let request: public::ChangeTopicLayout =
                decode_payload(frame.payload.clone(), &self.config.protocol)
                    .map_err(|source| Error::operation("decode layout change", source))?;
            wire::id(&request.topic_id)?
        };
        self.authorize_layout(access, topic).await?;
        if kind == FrameKind::ChangeTopicLayout {
            let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
            let request: public::ChangeTopicLayout =
                decode_payload(frame.payload.clone(), &self.config.protocol)
                    .map_err(|source| Error::operation("decode layout change", source))?;
            self.change_layout(
                wire::id(&frame.operation_id)?,
                super::metadata::management_binding(access, frame)?,
                request,
                deadline,
            )
            .await?;
        }
        let result = if kind == FrameKind::GetTopicLayout {
            let request: public::GetTopicLayout =
                decode_payload(frame.payload.clone(), &self.config.protocol)
                    .map_err(|source| Error::operation("decode layout page", source))?;
            self.layout_page(
                topic,
                request.expected_revision,
                if request.after_shard.is_empty() {
                    None
                } else {
                    Some(wire::id(&request.after_shard)?)
                },
            )
            .await?
        } else {
            self.layout_result(topic).await?
        };
        wire::response(
            frame,
            FrameKind::TopicLayoutResult,
            &result,
            &self.config.protocol,
        )
    }

    async fn authorize_layout(&self, access: &Access, topic: Id) -> Result<()> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let name = replica
            .state
            .try_with_application(|state| {
                state
                    .topic(topic)
                    .filter(|topic| topic.status == metadata::RecordStatus::Active)
                    .map(|topic| topic.name.clone())
            })
            .await
            .map_err(|source| Error::operation("read layout resource", source))?
            .ok_or_else(super::metadata::not_found)?;
        access.authorize_topic(&name)
    }

    async fn create_logical_topic(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let request: public::LogicalTopic =
            decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode logical topic creation", source))?;
        access.authorize_topic(&request.name)?;
        let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
        let operation = wire::id(&frame.operation_id)?;
        let digest = super::metadata::management_binding(access, frame)?;
        #[cfg(feature = "test-hooks")]
        let hook_name = request.name.clone();
        let topic = self
            .configure_logical_topic(operation, digest, request.name, deadline)
            .await?;
        #[cfg(feature = "test-hooks")]
        super::test_hooks::pause_topic_creation(&hook_name, deadline).await;
        let active = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state
                    .topic(topic.topic_id)
                    .is_some_and(|topic| topic.status == metadata::RecordStatus::Active)
            })
            .await
            .map_err(|source| Error::operation("read original topic creation status", source))?;
        if active {
            self.ensure_route(
                PartitionKey {
                    topic_id: topic.topic_id,
                    partition: 0,
                },
                deadline,
            )
            .await
            .map_err(Error::admitted)?;
        }
        wire::response(
            frame,
            FrameKind::TopicLayoutResult,
            &self
                .layout_result(topic.topic_id)
                .await
                .map_err(Error::admitted)?,
            &self.config.protocol,
        )
    }

    async fn configure_logical_topic(
        &self,
        operation: Id,
        digest: [u8; 32],
        name: String,
        deadline: Instant,
    ) -> Result<metadata::TopicRecord> {
        if let Some(outcome) = self.management_outcome(operation, digest).await? {
            return match outcome {
                metadata::Outcome::TopicConfigured(topic) => Ok(topic),
                _ => Err(Error::invalid(
                    "Topic creation conflicts with another management operation",
                )),
            };
        }
        let group = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.placement_group(None))
            .await
            .map_err(|source| Error::operation("select registered topic capacity", source))?
            .ok_or_else(Error::unavailable)?;
        let groups = vec![group];
        #[cfg(feature = "test-hooks")]
        let groups = super::watch_test_hooks::initial_topic_groups(self.cluster, &name, groups);
        let outcome = self
            .meta_write(
                Command::create_topic(
                    operation,
                    self.metadata_request_revision(operation).await?,
                    operation,
                    name,
                    groups,
                )
                .bind_management(digest),
                deadline,
            )
            .await?;
        let metadata::Outcome::TopicConfigured(topic) = outcome else {
            return Err(Error::invalid(
                "Logical topic creation returned an invalid outcome",
            ));
        };
        Ok(topic)
    }

    pub(super) async fn layout_result(&self, topic: Id) -> Result<public::TopicLayoutResult> {
        self.layout_page(topic, 0, None).await
    }

    async fn layout_page(
        &self,
        topic: Id,
        revision: u64,
        after: Option<Id>,
    ) -> Result<public::TopicLayoutResult> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        replica
            .state
            .try_with_application(|state| layout_page(state.topology(), topic, revision, after))
            .await
            .map_err(|source| Error::operation("read durable topic layout", source))?
    }

    async fn change_layout(
        &self,
        operation: Id,
        digest: [u8; 32],
        request: public::ChangeTopicLayout,
        deadline: Instant,
    ) -> Result<()> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        if let Some(outcome) = self.management_outcome(operation, digest).await? {
            let metadata::Outcome::LayoutPrepared(original) = outcome else {
                return Err(Error::invalid(
                    "Layout change conflicts with another management operation",
                ));
            };
            let plan = replica
                .state
                .try_with_application(|state| state.topology().plan(original.id).cloned())
                .await
                .map_err(|source| Error::operation("read original layout progress", source))?
                .ok_or_else(Error::unavailable)?;
            return self
                .resume_layout(plan, deadline)
                .await
                .map_err(Error::admitted);
        }
        let topic = wire::id(&request.topic_id)?;
        let left = wire::id(&request.left_shard)?;
        let change = if let Some(at) = request.split_at {
            if !request.right_shard.is_empty() {
                return Err(Error::invalid("Split cannot specify a second source"));
            }
            let group = replica
                .state
                .try_with_application(|state| {
                    state.topology().shard(left).map(|shard| shard.route.group)
                })
                .await
                .map_err(|source| Error::operation("read split source group", source))?
                .ok_or_else(super::metadata::not_found)?;
            let target = replica
                .state
                .try_with_application(|state| state.placement_group(Some(group)))
                .await
                .map_err(|source| Error::operation("select registered split capacity", source))?
                .ok_or_else(|| {
                    Error::invalid("Split requires an independent available data group")
                })?;
            LayoutChange::Split {
                source: left,
                at,
                groups: [group, target],
            }
        } else {
            let group = replica
                .state
                .try_with_application(|state| {
                    state.topology().shard(left).map(|shard| shard.route.group)
                })
                .await
                .map_err(|source| Error::operation("read merge target group", source))?
                .ok_or_else(super::metadata::not_found)?;
            LayoutChange::Merge {
                left,
                right: wire::id(&request.right_shard)?,
                group,
            }
        };
        let outcome = self
            .meta_write(
                Command::prepare_layout(
                    operation,
                    self.metadata_request_revision(operation).await?,
                    topic,
                    request.expected_revision,
                    change,
                )
                .bind_management(digest),
                deadline,
            )
            .await?;
        let metadata::Outcome::LayoutPrepared(plan) = outcome else {
            return Err(Error::invalid(
                "Layout preparation returned an invalid outcome",
            ));
        };
        #[cfg(feature = "test-hooks")]
        if let Some(partition) = replica
            .state
            .try_with_application(|state| state.topology().shard(left).map(|shard| shard.partition))
            .await
            .map_err(|source| Error::operation("read prepared layout test boundary", source))?
        {
            super::test_hooks::pause_layout(partition, deadline).await;
        }
        self.resume_layout(plan, deadline)
            .await
            .map_err(Error::admitted)
    }

    pub(super) async fn resume_layout(&self, plan: LayoutPlan, deadline: Instant) -> Result<()> {
        if plan.stage == LayoutStage::Active {
            return Ok(());
        }
        // Install and recover every destination before permanently closing source writes.
        for successor in &plan.successors {
            self.ensure_route(successor.partition, deadline).await?;
        }
        self.protect_layout_successors(&plan, deadline).await?;
        for source in &plan.sources {
            if plan.sealed.contains_key(source) {
                continue;
            }
            self.seal_layout_source(&plan, *source, deadline).await?;
        }
        let outcome = self
            .meta_write(
                Command::activate_layout(
                    Id::new_v7().map_err(|source| {
                        Error::operation("allocate layout activation identity", source)
                    })?,
                    self.meta_revision().await?,
                    plan.id,
                ),
                deadline,
            )
            .await?;
        if !matches!(outcome, metadata::Outcome::LayoutActivated(_)) {
            return Err(Error::invalid(
                "Layout activation returned an invalid outcome",
            ));
        }
        Ok(())
    }

    async fn seal_layout_source(
        &self,
        plan: &LayoutPlan,
        source: Id,
        deadline: Instant,
    ) -> Result<()> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let shard = replica
            .state
            .try_with_application(|state| state.topology().shard(source).cloned())
            .await
            .map_err(|source| Error::operation("read layout source", source))?
            .ok_or_else(super::metadata::not_found)?;
        let end = self
            .seal_data_domain(plan.id, shard.partition, shard.route.group, deadline)
            .await?;
        self.meta_write(
            Command::observe_layout_seal(
                Id::new_v7().map_err(|source| {
                    Error::operation("allocate seal observation identity", source)
                })?,
                self.meta_revision().await?,
                plan.id,
                source,
                end,
            ),
            deadline,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn seal_data_domain(
        &self,
        plan: Id,
        partition: PartitionKey,
        group: u64,
        deadline: Instant,
    ) -> Result<u64> {
        let target = self.leader(group, deadline).await?;
        let response = self
            .rpc
            .call(
                target,
                group,
                Request::control(SEAL_SHARD, &SealRequest { plan, partition })?,
                deadline,
            )
            .await?;
        let reply: SealReply = wire::decode_json(&response)?;
        if reply.plan != plan || reply.partition != partition {
            return Err(Error::invalid(
                "Data seal reply does not match the original plan and domain",
            ));
        }
        Ok(reply.end)
    }

    pub(super) async fn seal_shard_command(
        &self,
        group: u64,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let request: SealRequest = wire::decode_json(bytes)?;
        let lease = self.data_lease(group, request.partition, deadline).await?;
        let grant = lease.grant();
        let result = self
            .data_write(
                group,
                DataCommand::SealShard {
                    partition: request.partition,
                    writer: WriterFence {
                        node_id: grant.owner_node,
                        owner_epoch: grant.epoch,
                    },
                    plan: request.plan,
                },
                deadline,
            )
            .await?;
        let DataResponse::ShardSealed {
            plan,
            committed_end,
        } = result
        else {
            return Err(Error::invalid(
                "Source data group did not return a durable seal",
            ));
        };
        wire::json(&SealReply {
            plan,
            partition: request.partition,
            end: committed_end,
        })
    }
}

fn layout_page(
    topology: &metadata::Topology,
    topic: Id,
    expected: u64,
    after: Option<Id>,
) -> Result<public::TopicLayoutResult> {
    if after.is_some() && expected == 0 {
        return Err(Error::invalid(
            "A continuation requires its original layout revision",
        ));
    }
    let layout = topology
        .layout(topic)
        .ok_or_else(super::metadata::not_found)?;
    if expected != 0 && expected != layout.revision {
        return Err(Error::reject(
            tidemq_protocol::common::ErrorCode::StaleGeneration,
            tidemq_protocol::common::Outcome::Rejected,
            true,
        ));
    }
    let mut page = topology
        .domains(topic)
        .filter(|shard| after.is_none_or(|after| shard.route.id > after));
    let shards: Vec<_> = page.by_ref().take(32).collect();
    let next_after_shard = if page.next().is_some() {
        shards
            .last()
            .map(|shard| super::metadata::bytes_id(shard.route.id))
            .unwrap_or_default()
    } else {
        Bytes::new()
    };
    let (active, history): (Vec<_>, Vec<_>) = shards
        .into_iter()
        .partition(|shard| shard.sealed_end.is_none());
    Ok(public::TopicLayoutResult {
        total_shards: u32::try_from(topology.domains(topic).count())
            .map_err(|_| Error::unavailable())?,
        topic_id: super::metadata::bytes_id(topic),
        revision: layout.revision,
        active: active.into_iter().map(wire_shard).collect(),
        history: history.into_iter().map(wire_shard).collect(),
        next_after_shard,
    })
}

#[cfg(test)]
#[path = "../../tests/scalable_support/layout_pages.rs"]
mod layout_page_tests;

fn wire_shard(shard: &BirthShard) -> public::ShardRoute {
    public::ShardRoute {
        shard_id: super::metadata::bytes_id(shard.route.id),
        hash_start: shard.route.range.start,
        hash_end: shard.route.range.end,
        group_id: shard.route.group,
        authority_epoch: shard.route.authority_epoch,
        birth_partition: shard.partition.partition,
        predecessors: shard
            .predecessors
            .iter()
            .copied()
            .map(super::metadata::bytes_id)
            .collect(),
        sealed_end: shard.sealed_end,
    }
}
