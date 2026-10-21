use openraft::BasicNode;
use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
use snafu::Snafu;
use tidemq_protocol::Id;

use super::DataRejection;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Broker runtime limits are invalid"))]
    InvalidLimits,
    #[snafu(display("Broker admission is closed for draining"))]
    Draining,
    #[snafu(display("The bounded broker admission queue is full"))]
    Capacity,
    #[snafu(display("The request deadline expired before submission"))]
    Deadline,
    #[snafu(display("The request identity or attempt is invalid"))]
    InvalidRequest,
    #[snafu(display("The operation identity does not match the control grant"))]
    GrantMismatch,
    #[snafu(display("The authoritative partition has not been recovered"))]
    PartitionMissing,
    #[snafu(display("The data-group read barrier failed"))]
    ReadBarrier {
        source: Box<RaftError<u64, CheckIsLeaderError<u64, BasicNode>>>,
    },
    #[snafu(display("The data application is unavailable after a replica failure"))]
    StateRead { source: crate::wal::Error },
    #[snafu(display("Control authorization rejected the broker operation"))]
    Lease {
        source: crate::ownership::LeaseError,
    },
    #[snafu(display("The broker memory budget rejected admission"))]
    Memory { source: crate::budget::Error },
    #[snafu(display("The data application rejected the request: {reason}"))]
    Rejected { reason: DataRejection },
    #[snafu(display(
        "The append result is unknown for operation {operation_id}, request {request_id}"
    ))]
    Unknown { operation_id: Id, request_id: Id },
    #[snafu(display(
        "Raft did not return a durable result for operation {operation_id}, request {request_id}"
    ))]
    Commit {
        operation_id: Id,
        request_id: Id,
        source: Box<RaftError<u64, ClientWriteError<u64, BasicNode>>>,
    },
    #[snafu(display("The data application returned an unexpected response"))]
    UnexpectedResponse,
    #[snafu(display("The host clock precedes the Unix epoch"))]
    Clock { source: std::time::SystemTimeError },
    #[snafu(display("The host timestamp exceeds the supported range"))]
    ClockRange,
    #[snafu(display("A Tokio runtime is required for broker submission"))]
    Runtime {
        source: tokio::runtime::TryCurrentError,
    },
}

impl Error {
    pub fn result_unknown(&self) -> bool {
        matches!(self, Self::Unknown { .. } | Self::Commit { .. })
    }
}
