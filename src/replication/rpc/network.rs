use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{
    InstallSnapshotError, PayloadTooLarge, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{Backoff, RPCOption};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use tidemq_protocol::Id;
use tidemq_protocol::common::{Frame, FrameKind, TrafficClass};
use tidemq_protocol::framing::frame_limit;
use tidemq_protocol::internal::{RaftRequest, SnapshotChunk};
use tokio::time::Instant;

use crate::replication::{Application, TypeConfig};

use super::{Error, MAX_APPEND_ENTRIES, MAX_SNAPSHOT_CHUNK_BYTES, PeerPool, Result, codec, pool};

pub struct QuicNetworkFactory<A: Application> {
    pool: Arc<PeerPool>,
    group_id: u64,
    _application: PhantomData<fn() -> A>,
}

impl<A: Application> Clone for QuicNetworkFactory<A> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            group_id: self.group_id,
            _application: PhantomData,
        }
    }
}

impl<A: Application> QuicNetworkFactory<A> {
    pub fn new(pool: Arc<PeerPool>, group_id: u64) -> Result<Self> {
        if group_id == 0 {
            return Err(Error::Configuration {
                reason: "Raft network group must be nonzero",
            });
        }
        Ok(Self {
            pool,
            group_id,
            _application: PhantomData,
        })
    }
}

pub struct QuicNetwork<A: Application> {
    pool: Arc<PeerPool>,
    group_id: u64,
    target: u64,
    _application: PhantomData<fn() -> A>,
}

impl<A: Application> RaftNetworkFactory<TypeConfig<A>> for QuicNetworkFactory<A> {
    type Network = QuicNetwork<A>;
    async fn new_client(&mut self, target: u64, _: &BasicNode) -> Self::Network {
        QuicNetwork {
            pool: self.pool.clone(),
            group_id: self.group_id,
            target,
            _application: PhantomData,
        }
    }
}

impl<A: Application> QuicNetwork<A> {
    fn request<T: serde::Serialize>(
        &self,
        kind: FrameKind,
        class: TrafficClass,
        rpc: &T,
        deadline: Instant,
    ) -> Result<Frame> {
        let permit = self.pool.reserve(class)?;
        let id = Id::random().map_err(|source| Error::Protocol { source })?;
        let payload = RaftRequest {
            group_id: self.group_id,
            rpc_id: bytes::Bytes::copy_from_slice(id.as_bytes()),
            encoded_rpc: codec::encode(
                rpc,
                (frame_limit(class, &self.pool.config.protocol) as usize).saturating_sub(1024),
            )?,
        };
        self.pool.frame(kind, class, id, &payload, deadline, permit)
    }

    async fn append(
        &self,
        rpc: AppendEntriesRequest<TypeConfig<A>>,
        option: RPCOption,
    ) -> std::result::Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>
    {
        let count = rpc.entries.len();
        if count > MAX_APPEND_ENTRIES {
            return Err(PayloadTooLarge::new_entries_hint(MAX_APPEND_ENTRIES as u64).into());
        }
        let class = if count == 0 {
            TrafficClass::Control
        } else {
            TrafficClass::Replication
        };
        let deadline = pool::deadline(option.hard_ttl()).map_err(unreachable)?;
        let rpc = codec::Append::<A>::from(rpc);
        codec::validate_append(&rpc).map_err(unreachable)?;
        let frame = self
            .request(FrameKind::RaftAppend, class, &rpc, deadline)
            .map_err(|error| {
                if count > 1
                    && matches!(
                        error,
                        Error::Codec { .. }
                            | Error::Protocol {
                                source: tidemq_protocol::Error::Limit { .. }
                            }
                    )
                {
                    PayloadTooLarge::new_entries_hint((count / 2).max(1) as u64)
                        .with_source_error(&error)
                        .into()
                } else {
                    unreachable(error)
                }
            })?;
        let result: std::result::Result<_, RaftError<u64>> = self
            .pool
            .call(self.target, frame, deadline, self.group_id)
            .await
            .map_err(unreachable)?;
        result.map_err(|source| RemoteError::new(self.target, source).into())
    }
}

impl<A: Application> RaftNetwork<TypeConfig<A>> for QuicNetwork<A> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig<A>>,
        option: RPCOption,
    ) -> std::result::Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>
    {
        self.append(rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> std::result::Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let deadline = pool::deadline(option.hard_ttl()).map_err(unreachable)?;
        let frame = self
            .request(FrameKind::RaftVote, TrafficClass::Control, &rpc, deadline)
            .map_err(unreachable)?;
        let result: std::result::Result<_, RaftError<u64>> = self
            .pool
            .call(self.target, frame, deadline, self.group_id)
            .await
            .map_err(unreachable)?;
        result.map_err(|source| RemoteError::new(self.target, source).into())
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig<A>>,
        option: RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        if rpc.data.len() > MAX_SNAPSHOT_CHUNK_BYTES {
            return Err(unreachable(Error::Invalid {
                reason: "configure OpenRaft snapshot chunks within the wire cap",
            }));
        }
        let deadline = pool::deadline(option.hard_ttl()).map_err(unreachable)?;
        let class = TrafficClass::Bulk;
        let permit = self.pool.reserve(class).map_err(unreachable)?;
        let id = Id::random().map_err(|source| unreachable(Error::Protocol { source }))?;
        let payload = SnapshotChunk {
            group_id: self.group_id,
            snapshot_id: bytes::Bytes::copy_from_slice(id.as_bytes()),
            offset: rpc.offset,
            data: codec::encode(
                &rpc,
                (frame_limit(class, &self.pool.config.protocol) as usize).saturating_sub(1024),
            )
            .map_err(unreachable)?,
            finished: rpc.done,
        };
        let frame = self
            .pool
            .frame(
                FrameKind::RaftSnapshot,
                class,
                id,
                &payload,
                deadline,
                permit,
            )
            .map_err(unreachable)?;
        let result: std::result::Result<_, RaftError<u64, InstallSnapshotError>> = self
            .pool
            .call(self.target, frame, deadline, self.group_id)
            .await
            .map_err(unreachable)?;
        result.map_err(|source| RemoteError::new(self.target, source).into())
    }

    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(Duration::from_millis(50)))
    }
}

fn unreachable<E: std::error::Error>(error: Error) -> RPCError<u64, BasicNode, E> {
    Unreachable::new(&error).into()
}
