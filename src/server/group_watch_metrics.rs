use prometheus_client::{
    metrics::{counter::Counter, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use tidemq_protocol::public::WatchStatus;

pub(crate) struct Metrics {
    pub admitted: Counter,
    pub admission_rejected: Counter,
    pub bytes: Gauge,
    pub targets: Gauge,
    pub rechecks: Counter,
    pub dirty: Counter,
    pub capacity_releases: Counter,
    pub reads: Counter,
    pub retires: Counter,
    pub empty_reads: Counter,
    pub cleanup_unknown: Counter,
    pub admission_queued: Gauge,
    pub rechecks_queued: Gauge,
    pub authority_inbound: Gauge,
    pub authority_outbound: Gauge,
    statuses: [Counter; 7],
    wait: Histogram,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            admitted: Counter::default(),
            admission_rejected: Counter::default(),
            bytes: Gauge::default(),
            targets: Gauge::default(),
            rechecks: Counter::default(),
            dirty: Counter::default(),
            capacity_releases: Counter::default(),
            reads: Counter::default(),
            retires: Counter::default(),
            empty_reads: Counter::default(),
            cleanup_unknown: Counter::default(),
            admission_queued: Gauge::default(),
            rechecks_queued: Gauge::default(),
            authority_inbound: Gauge::default(),
            authority_outbound: Gauge::default(),
            statuses: std::array::from_fn(|_| Counter::default()),
            wait: Histogram::new([0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0]),
        }
    }
}

impl Metrics {
    pub(crate) fn register(&self, registry: &mut Registry) {
        for (name, help, gauge) in [
            (
                "tidemq_watch_admission_queued",
                "Queued tenant/member observation admissions",
                &self.admission_queued,
            ),
            (
                "tidemq_watch_rechecks_queued",
                "Queued tenant/member read-only recheck batches",
                &self.rechecks_queued,
            ),
            (
                "tidemq_watch_authority_inbound",
                "Active bounded observation authorization handlers",
                &self.authority_inbound,
            ),
            (
                "tidemq_watch_authority_outbound",
                "Active bounded observation authorization RPCs",
                &self.authority_outbound,
            ),
        ] {
            registry.register(name, help, gauge.clone());
        }
        for (name, help, counter) in [
            (
                "tidemq_watch_cleanup_unknown",
                "Watch branch cancellations without confirmed remote cleanup",
                &self.cleanup_unknown,
            ),
            (
                "tidemq_watch_admitted",
                "Admitted compact Watch requests",
                &self.admitted,
            ),
            (
                "tidemq_watch_admission_rejected",
                "Watch observation admission refusals",
                &self.admission_rejected,
            ),
            (
                "tidemq_watch_rechecks",
                "Read-only target readiness rechecks",
                &self.rechecks,
            ),
            (
                "tidemq_watch_dirty",
                "Actual target readiness revision changes",
                &self.dirty,
            ),
            (
                "tidemq_watch_result_capacity_releases",
                "Applied data-group read-result capacity increases",
                &self.capacity_releases,
            ),
            (
                "tidemq_watch_group_reads",
                "Applied GroupRead commands after observation activation",
                &self.reads,
            ),
            (
                "tidemq_watch_group_retires",
                "Applied GroupRetireRead commands after observation activation",
                &self.retires,
            ),
            (
                "tidemq_watch_empty_reads",
                "Applied empty GroupRead results after observation activation",
                &self.empty_reads,
            ),
        ] {
            registry.register(name, help, counter.clone());
        }
        registry.register(
            "tidemq_watch_bytes",
            "Charged observation bytes including cancellation watermarks",
            self.bytes.clone(),
        );
        registry.register(
            "tidemq_watch_targets",
            "Charged observation target contexts",
            self.targets.clone(),
        );
        registry.register(
            "tidemq_watch_wait_seconds",
            "Elapsed admitted Watch request time",
            self.wait.clone(),
        );
        for (counter, name) in self.statuses.iter().zip([
            "unspecified",
            "sync",
            "changed",
            "unchanged",
            "reset",
            "retry",
            "rejected",
        ]) {
            registry.register(
                format!("tidemq_watch_{name}"),
                "Watch target results",
                counter.clone(),
            );
        }
    }
    pub(crate) fn result(&self, status: WatchStatus) {
        if let Some(counter) = self.statuses.get(status as usize) {
            counter.inc();
        }
    }
    pub(crate) fn elapsed(&self, seconds: f64) {
        self.wait.observe(seconds);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn encoded_watch_metrics_have_unique_fixed_names_and_no_business_labels() {
        let metrics = Metrics::default();
        let mut registry = Registry::default();
        metrics.register(&mut registry);
        metrics.admitted.inc();
        metrics.admission_rejected.inc();
        metrics.cleanup_unknown.inc();
        metrics.bytes.set(1_536);
        metrics.targets.set(1);
        let statuses = [
            WatchStatus::Unspecified,
            WatchStatus::WatchSync,
            WatchStatus::WatchChanged,
            WatchStatus::WatchUnchanged,
            WatchStatus::WatchReset,
            WatchStatus::WatchRetry,
            WatchStatus::WatchRejected,
        ];
        for (index, status) in statuses.into_iter().enumerate() {
            for _ in 0..=index {
                metrics.result(status);
            }
        }
        metrics.elapsed(0.125);
        let mut encoded = String::new();
        prometheus_client::encoding::text::encode(&mut encoded, &registry).unwrap();
        for (name, value) in [
            ("admitted", 1),
            ("admission_rejected", 1),
            ("cleanup_unknown", 1),
            ("unspecified", 1),
            ("sync", 2),
            ("changed", 3),
            ("unchanged", 4),
            ("reset", 5),
            ("retry", 6),
            ("rejected", 7),
        ] {
            assert!(
                encoded
                    .lines()
                    .any(|line| line == format!("tidemq_watch_{name}_total {value}")),
                "Missing counter {name}: {encoded}"
            );
        }
        for prefix in ["# HELP ", "# TYPE "] {
            let mut descriptors = BTreeSet::new();
            for line in encoded.lines().filter_map(|line| line.strip_prefix(prefix)) {
                let name = line.split_whitespace().next().unwrap();
                assert!(
                    descriptors.insert(name),
                    "Duplicate metric descriptor {prefix}{name}"
                );
            }
        }
        let samples = encoded
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .collect::<Vec<_>>();
        assert_eq!(
            samples.len(),
            33,
            "Unexpected dynamic Watch series: {encoded}"
        );
        let mut identities = BTreeSet::new();
        for sample in samples {
            let identity = sample.split_whitespace().next().unwrap();
            assert!(identities.insert(identity), "Duplicate sample {identity}");
            if sample.contains('{') {
                assert!(
                    sample.starts_with("tidemq_watch_wait_seconds_bucket{le="),
                    "Unexpected Watch label: {sample}"
                );
            }
        }
        for forbidden in [
            "tenant=",
            "topic=",
            "subscription=",
            "member=",
            "operation_id=",
            "observed_token=",
            "credential=",
            "proof=",
            &tidemq_protocol::Id::new_v7().unwrap().to_string(),
        ] {
            assert!(!encoded.contains(forbidden));
        }
    }
}
