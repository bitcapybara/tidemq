use std::ops::Bound;

use serde::Serialize;
use tidemq_protocol::Id;

use crate::ownership::OwnershipPhase;

use super::{DeploymentMode, MetadataState, PartitionKey, RecordStatus, Rejection};

#[derive(Clone, Debug, Serialize)]
pub struct MetadataSummary {
    pub source: &'static str,
    pub initialized: bool,
    pub revision: u64,
    pub mode: Option<DeploymentMode>,
    pub topics: usize,
    pub retired_topics: usize,
    pub partitions: usize,
    pub subscriptions: usize,
    pub creating_subscriptions: usize,
    pub pending_retention_plans: usize,
    pub nodes: usize,
    pub groups: usize,
    pub remaining_operations: usize,
    pub ownership_result_slots: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct TopicSummary {
    pub topic_id: Id,
    pub name: String,
    pub generation: u64,
    pub partition_count: u32,
    pub status: RecordStatus,
}

#[derive(Clone, Debug, Serialize)]
pub struct TopicPage {
    pub source: &'static str,
    pub revision: u64,
    pub topics: Vec<TopicSummary>,
    pub next_after: Option<Id>,
}

#[derive(Clone, Debug, Serialize)]
pub struct OwnershipDiagnostic {
    pub source: &'static str,
    pub revision: u64,
    pub partition: PartitionKey,
    pub group_id: u64,
    pub phase: OwnershipPhase,
    pub epoch: u64,
    pub operation_id: Option<Id>,
    pub owner_node: Option<u64>,
    pub route_generation: u64,
    pub grant_metadata_revision: Option<u64>,
    pub fence_generation: Option<u64>,
    pub fence_commit_index: Option<u64>,
    pub recovery_commit_index: Option<u64>,
}

impl MetadataState {
    /// Bounded counts from the applied view. This is not a linearizable read proof.
    pub fn summary(&self) -> MetadataSummary {
        MetadataSummary {
            source: "local_applied",
            initialized: self.cluster_id.is_some(),
            revision: self.revision,
            mode: self.mode,
            topics: self.topics.len(),
            retired_topics: self.topics.len().saturating_sub(self.topic_names.len()),
            partitions: self.partitions.len(),
            subscriptions: self.subscriptions.len(),
            creating_subscriptions: self
                .consumer_subscriptions
                .values()
                .filter(|row| !row.prepared && !row.is_deleted())
                .count(),
            pending_retention_plans: self
                .retention_plans
                .values()
                .filter(|plan| plan.completed_revision.is_none())
                .count(),
            nodes: self.nodes.len(),
            groups: self.groups.len(),
            remaining_operations: self.remaining_operations(),
            ownership_result_slots: self.control_operations.len(),
        }
    }

    /// Direct key lookup; diagnostics do not scan all ownership operations.
    pub fn diagnose_ownership(&self, partition: PartitionKey) -> Option<OwnershipDiagnostic> {
        let record = self.partitions.get(&partition)?;
        Some(OwnershipDiagnostic {
            source: "local_applied",
            revision: self.revision,
            partition,
            group_id: record.group_id,
            phase: record.phase,
            epoch: record.highest_epoch,
            operation_id: record.grant.as_ref().map(|grant| grant.operation_id),
            owner_node: record.grant.as_ref().map(|grant| grant.owner_node),
            route_generation: record.route_generation,
            grant_metadata_revision: record.grant.as_ref().map(|grant| grant.metadata_revision),
            fence_generation: record
                .fence
                .as_ref()
                .map(|evidence| evidence.fence.fence_generation),
            fence_commit_index: record
                .fence
                .as_ref()
                .map(|evidence| evidence.committed.sequence()),
            recovery_commit_index: record
                .recovery
                .as_ref()
                .map(|evidence| evidence.committed.sequence()),
        })
    }

    /// At most 64 records and 32 KiB, including the serialized page envelope.
    pub fn topics_page(
        &self,
        after: Option<Id>,
        limit: usize,
        max_bytes: usize,
    ) -> Result<TopicPage, Rejection> {
        if limit == 0 || limit > 64 || !(512..=32_768).contains(&max_bytes) {
            return Err(Rejection::InvalidLimit);
        }
        let mut page = TopicPage {
            source: "local_applied",
            revision: self.revision,
            topics: Vec::new(),
            next_after: None,
        };
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        for (id, topic) in self.topics.range((lower, Bound::Unbounded)) {
            if page.topics.len() == limit {
                page.next_after = page.topics.last().map(|topic| topic.topic_id);
                break;
            }
            let candidate = TopicSummary {
                topic_id: *id,
                name: topic.name.clone(),
                generation: topic.generation,
                partition_count: topic.partition_count,
                status: topic.status,
            };
            page.topics.push(candidate);
            page.next_after = Some(*id);
            let bytes = serde_json::to_vec(&page).map_err(|_| Rejection::InvalidCommand)?;
            if bytes.len() > max_bytes {
                page.topics.pop();
                page.next_after = page.topics.last().map(|topic| topic.topic_id);
                if page.topics.is_empty() {
                    return Err(Rejection::InvalidLimit);
                }
                break;
            }
            page.next_after = None;
        }
        Ok(page)
    }
}
