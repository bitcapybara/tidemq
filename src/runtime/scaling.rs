use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::Bytes;
use prometheus_client::registry::Registry;
use tidemq_protocol::Id;
use tokio::time::Instant;

use crate::{
    broker::DataApplication,
    config::Config,
    metadata::{DeploymentMode, MetadataState},
    scaling::{
        self, Capacity, RepairBudget, ScalingMetrics,
        remote::{self, CapacitySource, Query, Reply},
    },
};

use super::{
    Error, Result,
    node::Replica,
    wire::{self, PeerRpc, Request},
};

pub(crate) const KIND: u32 = 10;
const SAMPLE_AGE: Duration = Duration::from_secs(5);

pub(crate) struct RuntimeScaling {
    pub service: Option<remote::Service<RuntimeRpc, CapacityCache>>,
    pub rpc: Arc<RuntimeRpc>,
    pub capacity: Arc<CapacityCache>,
    pub metrics: Arc<ScalingMetrics>,
}

impl RuntimeScaling {
    // Runtime assembly supplies the same owned facilities to every semantic adapter.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: &Config,
        cluster: Id,
        metadata: Option<&Replica<MetadataState>>,
        data: &BTreeMap<u64, Replica<DataApplication>>,
        rpc: PeerRpc,
        registry_nodes: Arc<super::registry::Registry>,
        repair: RepairBudget,
        registry: &mut Registry,
    ) -> Result<Self> {
        config.runtime.validate(config)?;
        let rpc = Arc::new(RuntimeRpc::new(config, rpc, registry_nodes));
        let capacity = Arc::new(CapacityCache::default());
        let metrics = Arc::new(ScalingMetrics::register(registry));
        let service = if config.role == crate::config::Role::Combined {
            None
        } else {
            let replicas = remote::LocalReplicas::new(
                metadata.map(|replica| replica.group.clone()),
                data.values().map(|replica| replica.group.clone()).collect(),
            )
            .map_err(|source| Error::operation("bind scaling local replicas", source))?;
            let mode = match config.replication_factor {
                1 => DeploymentMode::SeparateRf1,
                3 => DeploymentMode::SeparateRf3,
                _ => return Err(Error::invalid("Scaling requires explicit RF1 or RF3")),
            };
            let service = remote::Service::new(
                remote::ServiceConfig {
                    cluster_id: cluster,
                    node_id: config.node_id,
                    metadata_group: config.runtime.metadata_group,
                    mode,
                    metadata_voters: config.runtime.metadata_voters.iter().copied().collect(),
                },
                replicas,
                rpc.clone(),
                capacity.clone(),
                repair,
                metrics.clone(),
            )
            .map_err(|source| Error::operation("initialize semantic scaling service", source))?;
            Some(service)
        };
        Ok(Self {
            service,
            rpc,
            capacity,
            metrics,
        })
    }
}

#[derive(Default)]
pub(crate) struct CapacityCache {
    sample: std::sync::Mutex<Option<Capacity>>,
}
impl CapacityCache {
    pub fn replace(&self, sample: Option<Capacity>) -> Result<()> {
        *self.sample.lock().map_err(|_| Error::unavailable())? = sample;
        Ok(())
    }
}
impl CapacitySource for CapacityCache {
    fn snapshot(&self) -> Option<Capacity> {
        let sample = *self.sample.lock().ok()?;
        sample.filter(|sample| {
            std::time::Instant::now()
                .checked_duration_since(sample.observed_at)
                .is_some_and(|age| age <= SAMPLE_AGE)
        })
    }
}

pub(crate) struct RuntimeRpc {
    peer: PeerRpc,
    metadata_group: u64,
    registry: Arc<super::registry::Registry>,
}
impl RuntimeRpc {
    fn new(config: &Config, peer: PeerRpc, registry: Arc<super::registry::Registry>) -> Self {
        Self {
            peer,
            metadata_group: config.runtime.metadata_group,
            registry,
        }
    }

    fn validate_target(&self, group: u64, node: u64) -> scaling::Result<()> {
        if !self
            .registry
            .candidates(group, self.metadata_group)
            .map_err(remote_error)?
            .contains(&node)
        {
            return Err(scaling::Error::Authority);
        }
        Ok(())
    }
}

impl remote::Rpc for RuntimeRpc {
    async fn leader(&self, group: u64, deadline: Instant) -> scaling::Result<u64> {
        let nodes = self
            .registry
            .candidates(group, self.metadata_group)
            .map_err(remote_error)?;
        for node in &nodes {
            if Instant::now() >= deadline {
                return Err(scaling::Error::Deadline);
            }
            let request = Request::control(wire::LEADER, &()).map_err(remote_error)?;
            let attempt = deadline.min(Instant::now() + Duration::from_millis(500));
            let Ok(bytes) = self.peer.call(*node, group, request, attempt).await else {
                continue;
            };
            let leader: Option<u64> = wire::decode_json(&bytes).map_err(remote_error)?;
            if let Some(leader) = leader {
                self.validate_target(group, leader)?;
                return Ok(leader);
            }
        }
        Err(scaling::Error::Authority)
    }

    async fn call(&self, node: u64, query: Query, deadline: Instant) -> scaling::Result<Reply> {
        let group = query.group_id(self.metadata_group);
        self.validate_target(group, node)?;
        let class = query.traffic_class();
        let request = Request {
            kind: KIND,
            body: Bytes::from(remote::encode_query(&query)?),
            principal: String::new(),
            scope: String::new(),
            hops: 0,
            client_session: Bytes::new(),
            tenant: Bytes::new(),
        };
        let operation = Id::new_v7().map_err(|source| scaling::Error::Remote {
            source: Box::new(source),
        })?;
        let bytes = self
            .peer
            .submit(node, group, class, operation, request, deadline, group)
            .await
            .map_err(remote_error)?
            .receive()
            .await
            .map_err(remote_error)?;
        remote::decode_reply(&bytes)
    }
}

fn remote_error(source: Error) -> scaling::Error {
    if unreachable(&source) {
        scaling::Error::Unreachable {
            source: Box::new(source),
        }
    } else {
        scaling::Error::Remote {
            source: Box::new(source),
        }
    }
}

fn unreachable(source: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(source);
    for _ in 0..16 {
        let Some(error) = current else {
            return false;
        };
        if let Some(transport) = error.downcast_ref::<crate::transport::Error>() {
            return matches!(
                transport,
                crate::transport::Error::Closed
                    | crate::transport::Error::Connection { .. }
                    | crate::transport::Error::Stream { .. }
                    | crate::transport::Error::Deadline { .. }
            );
        }
        if let Some(rpc) = error.downcast_ref::<crate::transport::rpc::Error>() {
            return match rpc {
                crate::transport::rpc::Error::Closed => true,
                crate::transport::rpc::Error::Deadline { may_have_been_sent } => {
                    *may_have_been_sent
                }
                crate::transport::rpc::Error::Transport { source } => unreachable(source),
                _ => false,
            };
        }
        current = if let Some(io) = error.downcast_ref::<std::io::Error>() {
            io.get_ref()
                .map(|inner| inner as &(dyn std::error::Error + 'static))
        } else {
            error.source()
        };
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_unavailability_is_distinct_from_actual_transport_failure() {
        assert!(!unreachable(&Error::unavailable()));
        assert!(!unreachable(&Error::operation(
            "local admission",
            crate::transport::Error::Admission {
                resource: "test slots"
            }
        )));
        assert!(!unreachable(&Error::operation(
            "local RPC queue",
            crate::transport::rpc::Error::Deadline {
                may_have_been_sent: false
            }
        )));
        assert!(unreachable(&Error::operation(
            "connect peer",
            crate::transport::Error::Closed
        )));
        assert!(unreachable(&Error::operation(
            "peer RPC",
            crate::transport::rpc::Error::Closed
        )));
    }

    #[test]
    fn unavailable_or_stale_capacity_never_becomes_zero_load() {
        let cache = CapacityCache::default();
        assert!(cache.snapshot().is_none());
        let mut sample = Capacity {
            node_id: 4,
            observed_at: std::time::Instant::now(),
            free_bytes: 10 << 30,
            reserved_bytes: 1 << 30,
            bytes_per_second: 10,
            cpu_per_mille: 20,
            connections: 3,
            ack_bytes: 40,
        };
        cache.replace(Some(sample)).unwrap();
        assert_eq!(cache.snapshot().unwrap().node_id, 4);
        sample.observed_at -= SAMPLE_AGE + Duration::from_secs(1);
        cache.replace(Some(sample)).unwrap();
        assert!(cache.snapshot().is_none());
        sample.observed_at = std::time::Instant::now() + Duration::from_secs(1);
        cache.replace(Some(sample)).unwrap();
        assert!(cache.snapshot().is_none());
        cache.replace(None).unwrap();
        assert!(cache.snapshot().is_none());
    }
}
