//! Bounded wire dispatch keeps readers, handlers and response writers independent.

mod dispatch;
mod telemetry;
mod tenant_admission;

pub use dispatch::{DispatchConfig, SubmissionOrder, WireService, run};
pub use telemetry::DispatchMetrics;
