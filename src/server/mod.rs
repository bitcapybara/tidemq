//! Bounded wire dispatch keeps readers, handlers and response writers independent.

mod dispatch;

pub use dispatch::{DispatchConfig, SubmissionOrder, WireService, run};
