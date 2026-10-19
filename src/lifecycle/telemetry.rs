use std::{sync::Arc, time::Instant};

use prometheus_client::{
    metrics::{counter::Counter, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};

#[derive(Debug)]
pub struct LifecycleMetrics {
    pub(crate) registered: Counter,
    pub(crate) uploaded: Counter,
    pub(crate) published: Counter,
    pub(crate) canceled: Counter,
    pub(crate) deleted: Counter,
    pub(crate) reclaimed: Counter,
    pub(crate) retries: Counter,
    pub(crate) failed: Counter,
    pub(crate) active: Gauge,
    duration: Histogram,
}

impl Default for LifecycleMetrics {
    fn default() -> Self {
        Self {
            registered: Counter::default(),
            uploaded: Counter::default(),
            published: Counter::default(),
            canceled: Counter::default(),
            deleted: Counter::default(),
            reclaimed: Counter::default(),
            retries: Counter::default(),
            failed: Counter::default(),
            active: Gauge::default(),
            duration: Histogram::new([0.001, 0.01, 0.1, 1.0, 5.0, 30.0, 60.0]),
        }
    }
}

impl LifecycleMetrics {
    pub fn register(&self, registry: &mut Registry) {
        for (name, help, metric) in [
            (
                "tidemq_lifecycle_registered",
                "Committed object registrations.",
                &self.registered,
            ),
            (
                "tidemq_lifecycle_uploaded",
                "Verified object uploads.",
                &self.uploaded,
            ),
            (
                "tidemq_lifecycle_published",
                "Committed manifest publications.",
                &self.published,
            ),
            (
                "tidemq_lifecycle_canceled",
                "Durably terminated object tasks.",
                &self.canceled,
            ),
            (
                "tidemq_lifecycle_deleted",
                "Confirmed object delete calls.",
                &self.deleted,
            ),
            (
                "tidemq_lifecycle_reclaimed_bytes",
                "Reclaimed local message bytes.",
                &self.reclaimed,
            ),
            (
                "tidemq_lifecycle_retries",
                "Reconciliation attempts.",
                &self.retries,
            ),
            (
                "tidemq_lifecycle_failures",
                "Failed lifecycle operations.",
                &self.failed,
            ),
        ] {
            registry.register(name, help, metric.clone());
        }
        registry.register(
            "tidemq_lifecycle_active",
            "Admitted lifecycle workers.",
            self.active.clone(),
        );
        registry.register(
            "tidemq_lifecycle_operation_seconds",
            "End-to-end lifecycle operation duration.",
            self.duration.clone(),
        );
    }

    pub(crate) fn timer(self: &Arc<Self>) -> Timer {
        self.active.inc();
        Timer {
            metrics: self.clone(),
            started: Instant::now(),
        }
    }
}

pub(crate) struct Timer {
    metrics: Arc<LifecycleMetrics>,
    started: Instant,
}
impl Drop for Timer {
    fn drop(&mut self) {
        self.metrics.active.dec();
        self.metrics
            .duration
            .observe(self.started.elapsed().as_secs_f64());
    }
}
