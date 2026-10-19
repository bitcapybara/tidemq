use std::collections::BTreeMap;
use std::time::Duration;

use prometheus_client::{
    encoding::{EncodeLabelSet, EncodeLabelValue},
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::Registry,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, EncodeLabelValue)]
pub enum Stage {
    Queue,
    Learner,
    Membership,
    Metadata,
    Prewarm,
    Drain,
    ServiceRecovery,
    ReplicaRepair,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct Label {
    stage: Stage,
}

/// Eight fixed stages, with no node, group, operation or partition labels.
#[derive(Clone)]
pub struct ScalingMetrics {
    durations: BTreeMap<Stage, Histogram>,
    unknown: Counter,
}

impl ScalingMetrics {
    pub fn register(registry: &mut Registry) -> Self {
        let family = Family::<Label, Histogram>::new_with_constructor(|| {
            Histogram::new([0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 120.0, 600.0])
        });
        let mut durations = BTreeMap::new();
        for stage in [
            Stage::Queue,
            Stage::Learner,
            Stage::Membership,
            Stage::Metadata,
            Stage::Prewarm,
            Stage::Drain,
            Stage::ServiceRecovery,
            Stage::ReplicaRepair,
        ] {
            durations.insert(stage, family.get_or_create(&Label { stage }).clone());
        }
        let unknown = Counter::default();
        registry.register(
            "tidemq_scaling_stage_duration_seconds",
            "Monotonic scaling stage latency; service recovery and replica repair are distinct.",
            family,
        );
        registry.register(
            "tidemq_scaling_unknown_outcomes",
            "Scaling attempts requiring reconciliation of the same persistent plan.",
            unknown.clone(),
        );
        Self { durations, unknown }
    }
    pub fn observe(&self, stage: Stage, elapsed: Duration) {
        if let Some(histogram) = self.durations.get(&stage) {
            histogram.observe(elapsed.as_secs_f64());
        }
    }
    pub fn unknown(&self) {
        self.unknown.inc();
    }
}
