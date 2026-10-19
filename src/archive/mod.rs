//! Immutable pack encoding for committed data; publication remains a separate authority.

pub(crate) mod format;
mod reader;

pub use format::{
    ArchivedRecord, CommittedPartition, Descriptor, Pack, PackIdentity, PackLimits, PartitionIndex,
    Record, validate_pack,
};
pub use reader::ColdReader;

use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("invalid archive input: {reason}"))]
    Invalid { reason: &'static str },
    #[snafu(display("archive integrity validation failed: {reason}"))]
    Corrupt { reason: &'static str },
    #[snafu(display("archive exceeds a configured bound"))]
    Limit,
    #[snafu(display("archive decoding admission is exhausted"))]
    Overloaded,
    #[snafu(display("archive decoding worker failed"))]
    Worker { source: tokio::task::JoinError },
    #[snafu(display("archive allocation failed"))]
    Allocation {
        source: std::collections::TryReserveError,
    },
    #[snafu(display("archive memory admission failed"))]
    Memory {
        source: tidemq_transport::budget::Error,
    },
    #[snafu(display("archive object request failed"))]
    Object { source: crate::object_store::Error },
}

pub type Result<T> = std::result::Result<T, Error>;
