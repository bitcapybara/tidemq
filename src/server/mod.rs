//! Bounded wire dispatch keeps readers, handlers and response writers independent.

mod dispatch;
pub(crate) mod group_watch;
mod telemetry;
mod tenant_admission;

pub use dispatch::{DeferredReply, DispatchConfig, SubmissionOrder, WireService, run};
pub(crate) use dispatch::{InProcess, LocalResponse};
pub use telemetry::DispatchMetrics;
