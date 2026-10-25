use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    public,
};

use crate::{
    Admin, AdminReceipt, Error, Result, TopicId,
    admin::{bytes, pending::Request},
    identity::resource_id,
};

#[cfg(test)]
#[path = "../../../tests/scalable_support/sdk_authority.rs"]
mod tests;

resource_id!(ShardId);
resource_id!(AdminOperationId);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardInfo {
    pub id: ShardId,
    pub topic: TopicId,
    pub hash_start: u64,
    pub hash_end: Option<u64>,
    pub sealed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorityTransferStage {
    Prepare,
    Frozen,
    Exported,
    Imported,
    Published,
    Checkpointed,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorityTransferInfo {
    pub operation: AdminOperationId,
    pub shard: ShardId,
    pub topic: TopicId,
    pub source_group: u64,
    pub target_group: u64,
    pub stage: AuthorityTransferStage,
}

impl Admin {
    /// Operator inspection only; ordinary producers and consumers keep using the logical topic.
    pub async fn topic_shards(&self, topic: &str) -> Result<Vec<ShardInfo>> {
        let topic = self.topic_info(topic).await?;
        topic
            .wire
            .active
            .iter()
            .chain(&topic.wire.history)
            .map(|shard| {
                Ok(ShardInfo {
                    id: ShardId(Id::from_slice(&shard.shard_id)?),
                    topic: topic.id(),
                    hash_start: shard.hash_start,
                    hash_end: shard.hash_end,
                    sealed: shard.sealed_end.is_some(),
                })
            })
            .collect()
    }

    /// Move the shard's durable authority, retaining its birth identity and immutable objects.
    pub async fn transfer_shard(
        &self,
        shard: ShardId,
        target_group: u64,
    ) -> Result<AuthorityTransferInfo> {
        if target_group == 0 {
            return Err(Error::Configuration {
                reason: "authority transfer requires a nonzero destination group",
            });
        }
        match self
            .execute(Request::Cluster(public::ManageCluster {
                action: public::ClusterAction::TransferShardAuthority.into(),
                shard_id: bytes(shard.0),
                group_id: target_group,
                ..Default::default()
            }))
            .await?
        {
            AdminReceipt::ShardTransferred(info) => Ok(info),
            _ => Err(mismatch("Authority transfer returned another receipt kind")),
        }
    }

    pub async fn authority_transfer_status(
        &self,
        operation: AdminOperationId,
    ) -> Result<Option<AuthorityTransferInfo>> {
        let reply: public::ClusterControlResult = self
            .client
            .operation(|deadline| async move {
                self.client
                    .request(
                        FrameKind::ManageCluster,
                        FrameKind::ClusterControlResult,
                        TrafficClass::Control,
                        Id::new_v7()?,
                        &public::ManageCluster {
                            action: public::ClusterAction::InspectAuthorityTransfer.into(),
                            transfer_operation: bytes(operation.0),
                            ..Default::default()
                        },
                        deadline,
                    )
                    .await
            })
            .await?;
        ensure_only_transfer(&reply)?;
        reply
            .authority_transfer
            .map(|info| {
                let info = from_wire(info)?;
                if info.operation != operation {
                    return Err(mismatch("Authority transfer lookup changed its operation"));
                }
                Ok(info)
            })
            .transpose()
    }
}

pub(crate) fn receipt(
    request: &public::ManageCluster,
    reply: public::ClusterControlResult,
    operation: Id,
) -> Result<AdminReceipt> {
    ensure_only_transfer(&reply)?;
    let wire = reply
        .authority_transfer
        .ok_or_else(|| mismatch("Authority transfer omitted its result"))?;
    let info = from_wire(wire)?;
    if request.action != i32::from(public::ClusterAction::TransferShardAuthority)
        || request.shard_id.as_ref() != info.shard.0.as_bytes()
        || info.target_group != request.group_id
        || info.operation.0 != operation
        || info.stage != AuthorityTransferStage::Retired
    {
        return Err(mismatch(
            "Authority transfer changed its original request or is incomplete",
        ));
    }
    Ok(AdminReceipt::ShardTransferred(info))
}

fn ensure_only_transfer(reply: &public::ClusterControlResult) -> Result<()> {
    if reply.node.is_some()
        || reply.group.is_some()
        || reply.metadata_change.is_some()
        || reply.data_change.is_some()
        || reply.drain_progress.is_some()
    {
        return Err(mismatch("Authority transfer included unrelated results"));
    }
    Ok(())
}

fn from_wire(wire: public::AuthorityTransferInfo) -> Result<AuthorityTransferInfo> {
    if wire.source_group == 0 || wire.target_group == 0 || wire.source_group == wire.target_group {
        return Err(mismatch("Authority transfer has invalid group identities"));
    }
    let stage = match wire.stage {
        0 => AuthorityTransferStage::Prepare,
        1 => AuthorityTransferStage::Frozen,
        2 => AuthorityTransferStage::Exported,
        3 => AuthorityTransferStage::Imported,
        4 => AuthorityTransferStage::Published,
        5 => AuthorityTransferStage::Checkpointed,
        6 => AuthorityTransferStage::Retired,
        _ => return Err(mismatch("Authority transfer has an unknown stage")),
    };
    Ok(AuthorityTransferInfo {
        operation: AdminOperationId(Id::from_slice(&wire.operation_id)?),
        shard: ShardId(Id::from_slice(&wire.shard_id)?),
        topic: TopicId(Id::from_slice(&wire.topic_id)?),
        source_group: wire.source_group,
        target_group: wire.target_group,
        stage,
    })
}

fn mismatch(reason: &str) -> Error {
    Error::ResponseMismatch {
        reason: reason.into(),
    }
}
