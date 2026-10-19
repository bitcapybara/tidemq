use std::collections::BTreeSet;

use openraft::raft::{InstallSnapshotRequest, VoteRequest};
use prost::Message;
use serde::Serialize;
use tidemq_protocol::common::{Frame, FrameKind, TrafficClass};
use tidemq_protocol::framing::{
    MAX_TIMEOUT_MS, PROTOCOL_VERSION, decode_payload, frame_limit, new_frame,
};
use tidemq_protocol::internal::{RaftRequest, RaftResult, SnapshotChunk};
use tidemq_protocol::{Id, ProtocolLimits};

use crate::replication::{Application, Raft, TypeConfig};
use crate::transport::{Authorization, NodeGrant};

use super::{Error, MAX_SNAPSHOT_CHUNK_BYTES, Result, codec};

#[derive(Clone, Copy, Debug)]
pub enum GroupRole {
    Metadata,
    Data,
}

impl GroupRole {
    fn permits(self, role: &str) -> bool {
        role == "combined"
            || matches!(
                (self, role),
                (Self::Metadata, "metadata") | (Self::Data, "storage")
            )
    }
}

/// Only member RPCs reach OpenRaft; business commands require a separate authority path.
pub struct RaftService<A: Application> {
    raft: Raft<A>,
    cluster_id: Id,
    group_id: u64,
    role: GroupRole,
    bootstrap_voters: BTreeSet<u64>,
    limits: ProtocolLimits,
}

impl<A: Application> RaftService<A> {
    pub fn new(
        raft: Raft<A>,
        cluster_id: Id,
        group_id: u64,
        role: GroupRole,
        bootstrap_voters: BTreeSet<u64>,
        limits: ProtocolLimits,
    ) -> Result<Self> {
        limits
            .validate()
            .map_err(|source| Error::Protocol { source })?;
        if group_id == 0
            || !matches!(bootstrap_voters.len(), 1 | 3)
            || bootstrap_voters.contains(&0)
        {
            return Err(Error::Configuration {
                reason: "bootstrap topology requires one or three nonzero voters",
            });
        }
        Ok(Self {
            raft,
            cluster_id,
            group_id,
            role,
            bootstrap_voters,
            limits,
        })
    }

    /// The caller runs this in a bounded owned handler, retaining the charged Frame until completion.
    pub async fn handle(&self, authorization: &Authorization, frame: &Frame) -> Result<Frame> {
        let peer = self.authorize(authorization)?;
        let kind = FrameKind::try_from(frame.kind).map_err(|_| Error::Invalid {
            reason: "unknown Raft RPC kind",
        })?;
        let class = TrafficClass::try_from(frame.traffic_class).map_err(|_| Error::Invalid {
            reason: "unknown Raft traffic class",
        })?;
        self.validate_envelope(peer, frame, class)?;
        match kind {
            FrameKind::RaftVote if class == TrafficClass::Control => self.vote(peer, frame).await,
            FrameKind::RaftAppend
                if matches!(class, TrafficClass::Control | TrafficClass::Replication) =>
            {
                self.append(peer, frame, class).await
            }
            FrameKind::RaftSnapshot if class == TrafficClass::Bulk => {
                self.snapshot(peer, frame).await
            }
            _ => Err(Error::Invalid {
                reason: "operation is not a permitted Raft RPC",
            }),
        }
    }

    fn authorize<'a>(&self, authorization: &'a Authorization) -> Result<&'a NodeGrant> {
        let Authorization::Internal(peer) = authorization else {
            return Err(Error::Authorization {
                reason: "Raft RPC requires an authenticated internal node",
            });
        };
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow();
        let membership = metrics.membership_config.membership();
        let member = if membership.nodes().next().is_none() {
            self.bootstrap_voters.contains(&peer.node_id)
        } else {
            membership.voter_ids().any(|node| node == peer.node_id)
        };
        if peer.cluster_id != self.cluster_id
            || !peer.allowed_groups.contains(&self.group_id)
            || !self.role.permits(&peer.role)
            || !member
        {
            return Err(Error::Authorization {
                reason: "node is not an authorized replica of this group",
            });
        }
        Ok(peer)
    }

    fn validate_envelope(
        &self,
        peer: &NodeGrant,
        frame: &Frame,
        class: TrafficClass,
    ) -> Result<()> {
        Id::from_slice(&frame.request_id).map_err(|source| Error::Protocol { source })?;
        Id::from_slice(&frame.operation_id).map_err(|source| Error::Protocol { source })?;
        if frame.protocol_version != PROTOCOL_VERSION
            || frame.remaining_timeout_ms == 0
            || frame.remaining_timeout_ms > MAX_TIMEOUT_MS
            || frame.encoded_len() > frame_limit(class, &self.limits) as usize
            || !peer.allowed_classes.contains(&class)
        {
            return Err(Error::Invalid {
                reason: "Raft frame violates its bound connection envelope",
            });
        }
        Ok(())
    }

    fn request(&self, frame: &Frame) -> Result<RaftRequest> {
        let request: RaftRequest = decode_payload(frame.payload.clone(), &self.limits)
            .map_err(|source| Error::Protocol { source })?;
        if request.group_id != self.group_id || request.rpc_id != frame.request_id {
            return Err(Error::Invalid {
                reason: "Raft RPC identity does not match its envelope",
            });
        }
        Ok(request)
    }

    async fn vote(&self, peer: &NodeGrant, frame: &Frame) -> Result<Frame> {
        let request = self.request(frame)?;
        let rpc: VoteRequest<u64> = codec::decode(&request.encoded_rpc)?;
        validate_sender(peer, rpc.vote.leader_id.node_id)?;
        if rpc
            .last_log_id
            .is_some_and(|log| log.leader_id.term > rpc.vote.leader_id.term)
        {
            return Err(Error::Invalid {
                reason: "candidate log exceeds its election term",
            });
        }
        let response = self.raft.vote(rpc).await;
        self.response(frame, &response)
    }

    async fn append(&self, peer: &NodeGrant, frame: &Frame, class: TrafficClass) -> Result<Frame> {
        let request = self.request(frame)?;
        let rpc: codec::Append<A> = codec::decode(&request.encoded_rpc)?;
        validate_sender(peer, rpc.vote.leader_id.node_id)?;
        codec::validate_append(&rpc)?;
        if (rpc.entries.is_empty()) != (class == TrafficClass::Control) {
            return Err(Error::Invalid {
                reason: "Raft append body does not match its traffic class",
            });
        }
        let response = self.raft.append_entries(rpc.into()).await;
        self.response(frame, &response)
    }

    async fn snapshot(&self, peer: &NodeGrant, frame: &Frame) -> Result<Frame> {
        let chunk: SnapshotChunk = decode_payload(frame.payload.clone(), &self.limits)
            .map_err(|source| Error::Protocol { source })?;
        if chunk.group_id != self.group_id || chunk.snapshot_id != frame.request_id {
            return Err(Error::Invalid {
                reason: "snapshot RPC identity does not match its envelope",
            });
        }
        let rpc: InstallSnapshotRequest<TypeConfig<A>> = codec::decode(&chunk.data)?;
        validate_sender(peer, rpc.vote.leader_id.node_id)?;
        if rpc.data.len() > MAX_SNAPSHOT_CHUNK_BYTES
            || rpc.offset != chunk.offset
            || rpc.done != chunk.finished
            || rpc.offset.checked_add(rpc.data.len() as u64).is_none()
        {
            return Err(Error::Invalid {
                reason: "snapshot chunk exceeds its bounds or disagrees with its envelope",
            });
        }
        let response = self.raft.install_snapshot(rpc).await;
        self.response(frame, &response)
    }

    fn response<T: Serialize>(&self, request: &Frame, result: &T) -> Result<Frame> {
        let class = TrafficClass::try_from(request.traffic_class).map_err(|_| Error::Invalid {
            reason: "invalid Raft response class",
        })?;
        let payload = RaftResult {
            rpc_id: request.request_id.clone(),
            encoded_result: codec::encode(result, frame_limit(class, &self.limits) as usize)?,
        };
        new_frame(
            FrameKind::RaftResult,
            class,
            Id::from_slice(&request.request_id).map_err(|source| Error::Protocol { source })?,
            Id::from_slice(&request.operation_id).map_err(|source| Error::Protocol { source })?,
            &payload,
            request.remaining_timeout_ms,
            &self.limits,
        )
        .map_err(|source| Error::Protocol { source })
    }
}

fn validate_sender(peer: &NodeGrant, declared_node: u64) -> Result<()> {
    if declared_node == 0 || declared_node != peer.node_id {
        return Err(Error::Authorization {
            reason: "Raft sender does not match its authenticated node",
        });
    }
    Ok(())
}

impl<A: Application> crate::server::WireService for RaftService<A> {
    type Error = Error;
    async fn handle(
        &self,
        peer: Authorization,
        frame: &Frame,
        _deadline: tokio::time::Instant,
        order: Option<crate::server::SubmissionOrder>,
    ) -> Result<Option<Frame>> {
        drop(order);
        RaftService::handle(self, &peer, frame).await.map(Some)
    }
}
