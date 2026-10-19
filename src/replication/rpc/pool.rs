use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use tidemq_protocol::common::{Frame, FrameKind, TrafficClass};
use tidemq_protocol::framing::{MAX_TIMEOUT_MS, decode_payload, frame_limit, new_frame};
use tidemq_protocol::internal::RaftResult;
use tidemq_protocol::{Id, ProtocolLimits};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant;

use crate::budget::{MemoryBudget, MemoryPermit};
use crate::observability::metrics::{OperationMetrics, Stage};
use crate::transport::rpc::{RpcClient, RpcConfig};
use crate::transport::{Client, ClientCredentials, PeerTarget};

use super::{Error, Result, codec};

#[derive(Clone)]
pub struct NetworkConfig {
    pub protocol: ProtocolLimits,
    pub rpc: RpcConfig,
    pub replication_lanes: usize,
    pub bulk_lanes: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            protocol: ProtocolLimits::default(),
            rpc: RpcConfig::default(),
            replication_lanes: 4,
            bulk_lanes: 2,
        }
    }
}

type Slot = Arc<AsyncMutex<Option<RpcClient>>>;

/// One pool is shared by every local group, with bounded lanes per physical peer and class.
pub struct PeerPool {
    client: Arc<Client>,
    peers: BTreeMap<u64, PeerTarget>,
    credentials: ClientCredentials,
    slots: Mutex<BTreeMap<(u64, i32), Slot>>,
    pub(super) config: NetworkConfig,
    budget: MemoryBudget,
    metrics: Option<Arc<OperationMetrics>>,
    repair: Option<crate::scaling::RepairBudget>,
}

impl PeerPool {
    pub fn new(
        client: Arc<Client>,
        credentials: ClientCredentials,
        peers: Vec<PeerTarget>,
        config: NetworkConfig,
        budget: MemoryBudget,
        metrics: Option<Arc<OperationMetrics>>,
    ) -> Result<Self> {
        config
            .protocol
            .validate()
            .map_err(|source| Error::Protocol { source })?;
        let ClientCredentials::Internal {
            cluster_id,
            node_id,
            role,
            ..
        } = &credentials
        else {
            return Err(Error::Configuration {
                reason: "Raft peers require internal credentials",
            });
        };
        if *node_id == 0
            || peers.len() > 128
            || [config.replication_lanes, config.bulk_lanes]
                .into_iter()
                .any(|count| count == 0 || count > 64)
        {
            return Err(Error::Configuration {
                reason: "Raft local node or peer count is invalid",
            });
        }
        let mut table = BTreeMap::new();
        for peer in peers {
            let id = peer
                .node_id
                .filter(|id| *id != 0)
                .ok_or(Error::Configuration {
                    reason: "Raft peer has no exact node identity",
                })?;
            if peer.cluster_id != Some(*cluster_id)
                || peer.local_role != *role
                || table.insert(id, peer).is_some()
            {
                return Err(Error::Configuration {
                    reason: "Raft peer identity is duplicated or inconsistent",
                });
            }
        }
        Ok(Self {
            client,
            peers: table,
            credentials,
            slots: Mutex::new(BTreeMap::new()),
            config,
            budget,
            metrics,
            repair: None,
        })
    }

    /// Share the node-level budget with its membership coordinator before wrapping in Arc.
    pub fn with_repair_budget(mut self, budget: crate::scaling::RepairBudget) -> Self {
        self.repair = Some(budget);
        self
    }

    pub(crate) async fn connection(
        &self,
        target: u64,
        class: TrafficClass,
        deadline: Instant,
    ) -> Result<RpcClient> {
        let peer = self.peers.get(&target).ok_or(Error::Authorization {
            reason: "Raft target has no configured certificate-bound identity",
        })?;
        let slot = {
            let mut slots = self.slots.lock().map_err(|_| Error::Invalid {
                reason: "Raft connection pool is poisoned",
            })?;
            slots
                .entry((target, class.into()))
                .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
                .clone()
        };
        let mut slot = tokio::time::timeout_at(deadline, slot.lock())
            .await
            .map_err(|_| Error::Invalid {
                reason: "Raft peer connection deadline expired",
            })?;
        if let Some(rpc) = slot.as_ref().filter(|rpc| !rpc.is_closed()) {
            return Ok(rpc.clone());
        }
        let session = self
            .client
            .connect(peer.clone(), &self.credentials, class, deadline)
            .await
            .map_err(|source| Error::Transport { source })?;
        let lanes = if matches!(
            class,
            TrafficClass::Replication | TrafficClass::Produce | TrafficClass::Delivery
        ) {
            self.config.replication_lanes
        } else {
            self.config.bulk_lanes
        };
        let rpc =
            RpcClient::from_session_with_lanes(session, self.config.rpc.clone(), lanes, deadline)
                .await
                .map_err(|source| Error::Rpc { source })?;
        *slot = Some(rpc.clone());
        Ok(rpc)
    }

    pub(super) fn reserve(&self, class: TrafficClass) -> Result<MemoryPermit> {
        let bytes = frame_limit(class, &self.config.protocol)
            .checked_mul(2)
            .ok_or(Error::Configuration {
                reason: "Raft RPC allocation budget overflow",
            })?;
        if class == TrafficClass::Control {
            self.budget.try_reserve_core(bytes)
        } else {
            self.budget.try_reserve(class, bytes)
        }
        .map_err(|source| Error::Memory { source })
    }

    pub(super) fn frame<M: prost::Message>(
        &self,
        kind: FrameKind,
        class: TrafficClass,
        id: Id,
        payload: &M,
        deadline: Instant,
        permit: MemoryPermit,
    ) -> Result<Frame> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let millis = u64::try_from(remaining.as_millis()).map_err(|_| Error::Invalid {
            reason: "Raft RPC deadline overflow",
        })?;
        let mut frame = new_frame(kind, class, id, id, payload, millis, &self.config.protocol)
            .map_err(|source| Error::Protocol { source })?;
        frame.payload = Bytes::from_owner(ChargedPayload {
            bytes: frame.payload,
            _permit: permit,
        });
        Ok(frame)
    }

    pub(super) async fn call<T: DeserializeOwned>(
        &self,
        target: u64,
        frame: Frame,
        deadline: Instant,
        affinity: u64,
    ) -> Result<T> {
        let class = TrafficClass::try_from(frame.traffic_class).map_err(|_| Error::Invalid {
            reason: "invalid outgoing Raft class",
        })?;
        let id = frame.request_id.clone();
        let started = Instant::now();
        let received = async {
            if matches!(class, TrafficClass::Replication | TrafficClass::Bulk)
                && let Some(repair) = &self.repair
                && (if class == TrafficClass::Replication {
                    repair.is_learner(target, affinity)
                } else {
                    repair.is_repair(target, affinity)
                })
                .map_err(|source| Error::Repair { source })?
            {
                let queued = Instant::now();
                let paced = repair.pace(frame.payload.len(), deadline).await;
                if let Some(metrics) = &self.metrics {
                    metrics.observe(Stage::Queue, queued.elapsed().as_secs_f64());
                }
                paced.map_err(|source| Error::Repair { source })?;
            }
            let rpc = self.connection(target, class, deadline).await?;
            let received = if class == TrafficClass::Control {
                rpc.call(frame, deadline).await
            } else {
                rpc.call_affinity(frame, deadline, affinity).await
            }
            .map_err(|source| Error::Rpc { source })?;
            Ok::<_, Error>((rpc, received))
        }
        .await;
        if let Some(metrics) = &self.metrics {
            metrics.observe(Stage::Replicate, started.elapsed().as_secs_f64());
        }
        let (rpc, received) = received?;
        let frame = received.frame();
        if frame.kind != i32::from(FrameKind::RaftResult)
            || frame.traffic_class != i32::from(class)
            || frame.request_id != id
            || frame.operation_id != id
        {
            rpc.close(Instant::now()).await;
            return Err(Error::Invalid {
                reason: "Raft response does not match its request",
            });
        }
        let result: RaftResult = decode_payload(frame.payload.clone(), &self.config.protocol)
            .map_err(|source| Error::Protocol { source })?;
        if result.rpc_id != id {
            rpc.close(Instant::now()).await;
            return Err(Error::Invalid {
                reason: "Raft inner response identity does not match",
            });
        }
        codec::decode(&result.encoded_result)
    }
}

struct ChargedPayload {
    bytes: Bytes,
    _permit: MemoryPermit,
}

impl AsRef<[u8]> for ChargedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

pub(super) fn deadline(ttl: Duration) -> Result<Instant> {
    if ttl.is_zero() || ttl > Duration::from_millis(MAX_TIMEOUT_MS) {
        return Err(Error::Configuration {
            reason: "Raft RPC timeout must be within the wire deadline",
        });
    }
    Instant::now().checked_add(ttl).ok_or(Error::Invalid {
        reason: "Raft RPC deadline overflow",
    })
}
