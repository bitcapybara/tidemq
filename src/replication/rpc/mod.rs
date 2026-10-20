//! Authenticated wire adapters for real OpenRaft RPCs.

mod codec;
mod network;
mod pool;
mod server;

use snafu::Snafu;

pub use network::QuicNetworkFactory;
pub use pool::{NetworkConfig, PeerPool};
pub use server::{GroupRole, RaftService};

pub const MAX_APPEND_ENTRIES: usize = 128;
pub const MAX_SNAPSHOT_CHUNK_BYTES: usize = 262_144;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid Raft RPC configuration: {reason}"))]
    Configuration { reason: &'static str },
    #[snafu(display("Raft RPC authorization rejected: {reason}"))]
    Authorization { reason: &'static str },
    #[snafu(display("Invalid Raft RPC: {reason}"))]
    Invalid { reason: &'static str },
    #[snafu(display(
        "Raft peer rejected RPC: {code:?}; outcome: {outcome:?}; retryable: {retryable}"
    ))]
    PeerRejected {
        code: tidemq_protocol::common::ErrorCode,
        outcome: tidemq_protocol::common::Outcome,
        retryable: bool,
    },
    #[snafu(display("Raft RPC protocol validation failed"))]
    Protocol { source: tidemq_protocol::Error },
    #[snafu(display("Raft RPC encoding or decoding failed"))]
    Codec { source: crate::wal::Error },
    #[snafu(display("Raft QUIC transport failed"))]
    Transport { source: crate::transport::Error },
    #[snafu(display("Raft multiplexed RPC failed"))]
    Rpc {
        source: crate::transport::rpc::Error,
    },
    #[snafu(display("Raft RPC memory admission failed"))]
    Memory { source: crate::budget::Error },
    #[snafu(display("Raft learner repair admission failed"))]
    Repair { source: crate::scaling::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
