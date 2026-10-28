use std::collections::BTreeMap;
use std::time::Duration;

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::{
    counter::Counter, family::Family, gauge::Gauge, histogram::Histogram,
};
use prometheus_client::registry::Registry;
use serde::Serialize;
use tidemq_protocol::Id;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, EncodeLabelValue, Serialize)]
pub enum Transition {
    Grant,
    Fence,
    Recover,
    Activate,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, EncodeLabelValue, Serialize)]
pub enum TransitionOutcome {
    Committed,
    Rejected,
    Unknown,
    Failed,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct Labels {
    stage: Transition,
    outcome: TransitionOutcome,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct StageLabel {
    stage: Transition,
}

/// Fixed 4-stage/4-outcome cardinality. IDs belong in bounded diagnostics, not labels.
#[derive(Clone)]
pub struct OwnershipMetrics {
    counters: BTreeMap<(Transition, TransitionOutcome), Counter>,
    durations: BTreeMap<Transition, Histogram>,
    validity_seconds: Gauge<f64, std::sync::atomic::AtomicU64>,
    cas_retries: Family<StageLabel, Counter>,
}

impl OwnershipMetrics {
    pub fn register(registry: &mut Registry) -> Self {
        let results = Family::<Labels, Counter>::default();
        let durations = Family::<StageLabel, Histogram>::new_with_constructor(|| {
            Histogram::new([0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 3.0, 5.0, 10.0])
        });
        let validity_seconds = Gauge::default();
        let cas_retries = Family::<StageLabel, Counter>::default();
        let mut counters = BTreeMap::new();
        let mut histograms = BTreeMap::new();
        for stage in [
            Transition::Grant,
            Transition::Fence,
            Transition::Recover,
            Transition::Activate,
        ] {
            drop(cas_retries.get_or_create(&StageLabel { stage }));
            histograms.insert(
                stage,
                durations.get_or_create(&StageLabel { stage }).clone(),
            );
            for outcome in [
                TransitionOutcome::Committed,
                TransitionOutcome::Rejected,
                TransitionOutcome::Unknown,
                TransitionOutcome::Failed,
            ] {
                counters.insert(
                    (stage, outcome),
                    results.get_or_create(&Labels { stage, outcome }).clone(),
                );
            }
        }
        registry.register(
            "tidemq_ownership_transitions",
            "Logical ownership transition decisions; response loss is unknown until reconciled.",
            results,
        );
        registry.register(
            "tidemq_ownership_transition_duration_seconds",
            "Monotonic ownership stage duration including its authoritative operation.",
            durations,
        );
        registry.register("tidemq_control_validity_remaining_seconds", "Minimum remaining validity among locally monitored active grants; zero means unverified or expired.", validity_seconds.clone());
        registry.register("tidemq_ownership_metadata_cas_retries", "Ownership metadata revision conflicts retried with fresh evidence and the original operation identity.", cas_retries.clone());
        Self {
            counters,
            durations: histograms,
            validity_seconds,
            cas_retries,
        }
    }

    pub fn record(&self, stage: Transition, outcome: TransitionOutcome, elapsed: Duration) {
        if let Some(counter) = self.counters.get(&(stage, outcome)) {
            counter.inc();
        }
        if let Some(histogram) = self.durations.get(&stage) {
            histogram.observe(elapsed.as_secs_f64());
        }
    }

    pub(crate) fn cas_retry(&self, stage: Transition) {
        self.cas_retries.get_or_create(&StageLabel { stage }).inc();
    }

    /// The runtime supplies the minimum across its bounded active grant set.
    pub fn validity_remaining(&self, remaining: Duration) {
        self.validity_seconds.set(remaining.as_secs_f64());
    }
}

/// Emit once at the committed transition boundary, never while replaying `apply`.
pub fn emit_transition(
    operation_id: Id,
    epoch: u64,
    stage: Transition,
    outcome: TransitionOutcome,
) {
    tracing::info!(event = "ownership_transition", %operation_id, epoch, ?stage, ?outcome, "Ownership transition completed");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ownership_metrics_have_finite_labels_and_stage_histograms() {
        let mut registry = Registry::default();
        let metrics = OwnershipMetrics::register(&mut registry);
        metrics.record(
            Transition::Fence,
            TransitionOutcome::Committed,
            Duration::from_millis(2),
        );
        metrics.validity_remaining(Duration::from_secs(1));
        metrics.cas_retry(Transition::Fence);
        let mut text = String::new();
        prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
        assert!(text.contains(
            "tidemq_ownership_transitions_total{stage=\"Fence\",outcome=\"Committed\"} 1"
        ));
        assert!(!text.contains("operation_id"));
        assert!(!text.contains("group_id"));
        assert!(text.contains("tidemq_ownership_metadata_cas_retries_total{stage=\"Fence\"} 1"));
        assert!(text.lines().count() < 150);
    }
}
