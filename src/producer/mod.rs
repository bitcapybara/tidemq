//! Bounded, deterministic producer sessions and committed message offsets.
//!
//! These transitions run only inside the owning data group's committed application.
//! A rejected or disconnected transport attempt must not create a replacement session.

mod error;
mod state;
mod types;

#[cfg(test)]
mod tests;

pub use error::{Rejection, Result};
pub use state::{ProducerState, PublishReceipt, PublishRequest, PublisherPermit};
pub use types::{
    AppendBatch, BatchResult, MessageData, ProducerDiagnostic, ProducerLimits, ReadPage,
    RegisterProducer, SessionInfo,
};
