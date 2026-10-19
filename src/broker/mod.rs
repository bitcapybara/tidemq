//! Broker admission and durable production through the authoritative data Raft group.

mod admission;
mod application;
mod error;
mod runtime;
mod submission;
mod telemetry;

#[cfg(test)]
mod tests;

pub use admission::BrokerLimits;
pub use application::{
    ArchiveCandidate, ArchivePage, CapacityUsage, DataApplication, DataCommand, DataGroupConfig,
    DataRejection, DataResponse, ProducerCommand, WriterFence,
};
pub use error::{Error, Result};
pub use runtime::{Broker, HotRead, ProducerStatus, ProducerSubmission, RequestContext};
pub use telemetry::{ProducerMetrics, ProducerMetricsSnapshot};
