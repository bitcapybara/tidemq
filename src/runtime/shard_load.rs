use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::{Id, common::TrafficClass};
use tokio::time::Instant;

use super::{
    Error, Result,
    service::Service,
    wire::{self, Request},
};
use crate::{broker::ShardLoadReport, metadata::PartitionKey};

pub(super) const QUERY: u32 = 1013;
const PAGE: usize = 16;

#[cfg(test)]
#[path = "../../tests/scalable_support/shard_load_wire.rs"]
mod tests;

#[derive(Serialize, Deserialize)]
struct LoadRequest {
    partitions: [Option<PartitionKey>; PAGE],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardLoadPage {
    pub cluster: Id,
    pub node: u64,
    pub instance: Id,
    pub group: u64,
    pub sampled_unix_ms: u64,
    pub reports: [Option<ShardLoadReport>; PAGE],
}

impl Service {
    pub(super) async fn query_shard_loads(
        &self,
        group: u64,
        partitions: &[PartitionKey],
        deadline: Instant,
    ) -> Result<ShardLoadPage> {
        if partitions.len() > PAGE {
            return Err(Error::invalid(
                "Shard load queries accept at most sixteen domains",
            ));
        }
        let mut requested = [None; PAGE];
        for (slot, partition) in requested.iter_mut().zip(partitions) {
            *slot = Some(*partition);
        }
        let request = LoadRequest {
            partitions: requested,
        };
        let started = Instant::now();
        let node = self.leader(group, deadline).await?;
        let bytes = self
            .rpc
            .call(node, group, Request::typed(QUERY, &request)?, deadline)
            .await?;
        let reply: ShardLoadPage = wire::decode_binary(&bytes)?;
        if reply.cluster != self.cluster
            || reply.node != node
            || reply.group != group
            || started.elapsed() > std::time::Duration::from_secs(5)
            || reply
                .reports
                .iter()
                .zip(requested)
                .any(|(report, requested)| {
                    report
                        .as_ref()
                        .is_some_and(|report| Some(report.partition) != requested)
                })
        {
            return Err(Error::unavailable());
        }
        Ok(reply)
    }

    pub(super) async fn shard_load_command(
        &self,
        group: u64,
        body: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let _memory = self
            .budget
            .try_reserve(TrafficClass::Control, 32 * 1024)
            .map_err(|source| Error::operation("reserve bounded shard load observation", source))?;
        let request: LoadRequest = wire::decode_binary(body)?;
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        let reports = replica
            .state
            .try_with_application(|state| {
                let mut reports = std::array::from_fn(|_| None);
                for (report, partition) in reports.iter_mut().zip(request.partitions) {
                    if let Some(partition) = partition {
                        *report = state
                            .partition_load_report(partition)
                            .map_err(super::data::data_rejection)?;
                    }
                }
                Ok::<_, Error>(reports)
            })
            .await
            .map_err(|source| Error::operation("read committed shard load summaries", source))??;
        wire::binary(&ShardLoadPage {
            cluster: self.cluster,
            node: self.config.node_id,
            instance: self.instance,
            group,
            sampled_unix_ms: super::data::unix_millis()?,
            reports,
        })
    }
}
