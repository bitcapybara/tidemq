use std::collections::HashMap;
use std::sync::Arc;

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::{
    counter::Counter, family::Family, gauge::Gauge, histogram::Histogram,
};
use prometheus_client::registry::Registry;
use snafu::ResultExt;

use crate::Result;
use crate::error::MetricsSnafu;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, EncodeLabelValue)]
pub enum Operation {
    Produce,
    Read,
    Ack,
    Control,
    Archive,
    Recover,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, EncodeLabelValue)]
pub enum Outcome {
    Committed,
    Rejected,
    Unknown,
    Failed,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, EncodeLabelValue)]
pub enum Stage {
    Admission,
    Queue,
    Decode,
    Append,
    Sync,
    Replicate,
    Commit,
    Apply,
    Response,
    Total,
}

const OPERATIONS: [Operation; 6] = [
    Operation::Produce,
    Operation::Read,
    Operation::Ack,
    Operation::Control,
    Operation::Archive,
    Operation::Recover,
];
const OUTCOMES: [Outcome; 4] = [
    Outcome::Committed,
    Outcome::Rejected,
    Outcome::Unknown,
    Outcome::Failed,
];
const STAGES: [Stage; 10] = [
    Stage::Admission,
    Stage::Queue,
    Stage::Decode,
    Stage::Append,
    Stage::Sync,
    Stage::Replicate,
    Stage::Commit,
    Stage::Apply,
    Stage::Response,
    Stage::Total,
];

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct ResultLabels {
    operation: Operation,
    outcome: Outcome,
}
#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct DurationLabels {
    operation: Operation,
    stage: Stage,
}

pub struct Metrics {
    registry: Registry,
    handles: HashMap<Operation, Arc<OperationMetrics>>,
    pub admitted_bytes: Gauge,
}

#[derive(Clone)]
pub struct OperationMetrics {
    results: HashMap<Outcome, Counter>,
    durations: HashMap<Stage, Histogram>,
}

impl Default for Metrics {
    fn default() -> Self {
        let mut registry = Registry::default();
        let results = Family::<ResultLabels, Counter>::default();
        let durations = Family::<DurationLabels, Histogram>::new_with_constructor(|| {
            Histogram::new([
                0.000_05, 0.000_1, 0.000_5, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0,
            ])
        });
        let admitted_bytes = Gauge::default();
        let mut handles = HashMap::new();
        for operation in OPERATIONS {
            let counters = OUTCOMES
                .into_iter()
                .map(|outcome| {
                    (
                        outcome,
                        results
                            .get_or_create(&ResultLabels { operation, outcome })
                            .clone(),
                    )
                })
                .collect();
            let histograms = STAGES
                .into_iter()
                .map(|stage| {
                    (
                        stage,
                        durations
                            .get_or_create(&DurationLabels { operation, stage })
                            .clone(),
                    )
                })
                .collect();
            handles.insert(
                operation,
                Arc::new(OperationMetrics {
                    results: counters,
                    durations: histograms,
                }),
            );
        }
        registry.register(
            "tidemq_operations",
            "Completed logical operations by outcome.",
            results,
        );
        registry.register(
            "tidemq_stage_duration_seconds",
            "Local monotonic stage duration in seconds.",
            durations,
        );
        registry.register(
            "tidemq_admitted_bytes",
            "Application bytes held by admission permits.",
            admitted_bytes.clone(),
        );
        Self {
            registry,
            handles,
            admitted_bytes,
        }
    }
}

impl Metrics {
    /// Register process collectors once before sharing the metrics with workers.
    pub fn registry_mut(&mut self) -> &mut Registry {
        &mut self.registry
    }

    pub fn operation(&self, operation: Operation) -> Option<Arc<OperationMetrics>> {
        self.handles.get(&operation).cloned()
    }

    pub fn encode(&self) -> Result<String> {
        let mut text = String::new();
        prometheus_client::encoding::text::encode(&mut text, &self.registry)
            .context(MetricsSnafu)?;
        Ok(text)
    }
}

impl OperationMetrics {
    pub fn result(&self, outcome: Outcome) {
        if let Some(counter) = self.results.get(&outcome) {
            counter.inc();
        }
    }
    pub fn observe(&self, stage: Stage, seconds: f64) {
        if seconds.is_finite()
            && seconds >= 0.0
            && let Some(histogram) = self.durations.get(&stage)
        {
            histogram.observe(seconds);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_has_finite_series_and_aggregatable_stage_histograms() {
        let mut metrics = Metrics::default();
        crate::server::DispatchMetrics::default().register(metrics.registry_mut());
        let handles = metrics.operation(Operation::Produce).unwrap();
        handles.result(Outcome::Committed);
        handles.observe(Stage::Commit, 0.002);
        handles.observe(Stage::Commit, f64::NAN);
        let text = metrics.encode().unwrap();
        assert!(text.contains("tidemq_stage_duration_seconds_bucket"));
        assert!(text.contains("outcome=\"Committed\""));
        assert!(!text.contains("request_id"));
        assert!(text.contains("tidemq_dispatch_handler_errors_total 0"));
        assert!(!text.contains("tidemq_tidemq"));
        assert_eq!(text.matches("# EOF").count(), 1);
        assert!(text.lines().count() < 2_000);
    }
}
