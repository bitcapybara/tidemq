use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::{counter::ConstCounter, gauge::ConstGauge};
use serde::Serialize;

use super::submission::SubmissionStatus;
use crate::observability::metrics::{OperationMetrics, Stage};

#[derive(Debug, Default)]
struct Counters {
    attempts: AtomicU64,
    committed: AtomicU64,
    duplicate: AtomicU64,
    rejected: AtomicU64,
    unknown: AtomicU64,
    failed: AtomicU64,
    quota_retries: AtomicU64,
    batches: AtomicU64,
    messages: AtomicU64,
    bytes: AtomicU64,
    pending: AtomicU64,
    pending_bytes: AtomicU64,
}

/// Cloneable fixed handles; register one clone as a collector under the producer prefix.
#[derive(Clone, Debug, Default)]
pub struct ProducerMetrics(Arc<Counters>);

#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptOutcome {
    Committed,
    Duplicate,
    Rejected,
    Unknown,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProducerMetricsSnapshot {
    pub attempts_total: u64,
    pub committed_attempts_total: u64,
    pub duplicate_attempts_total: u64,
    pub rejected_attempts_total: u64,
    pub unknown_attempts_total: u64,
    pub failed_attempts_total: u64,
    pub quota_retries_total: u64,
    pub new_batches_total: u64,
    pub new_messages_total: u64,
    pub new_bytes_total: u64,
    pub pending_operations: u64,
    pub pending_bytes: u64,
}

impl ProducerMetrics {
    pub fn snapshot(&self) -> ProducerMetricsSnapshot {
        let counters = &self.0;
        ProducerMetricsSnapshot {
            attempts_total: counters.attempts.load(Ordering::Relaxed),
            committed_attempts_total: counters.committed.load(Ordering::Relaxed),
            duplicate_attempts_total: counters.duplicate.load(Ordering::Relaxed),
            rejected_attempts_total: counters.rejected.load(Ordering::Relaxed),
            unknown_attempts_total: counters.unknown.load(Ordering::Relaxed),
            failed_attempts_total: counters.failed.load(Ordering::Relaxed),
            quota_retries_total: counters.quota_retries.load(Ordering::Relaxed),
            new_batches_total: counters.batches.load(Ordering::Relaxed),
            new_messages_total: counters.messages.load(Ordering::Relaxed),
            new_bytes_total: counters.bytes.load(Ordering::Relaxed),
            pending_operations: counters.pending.load(Ordering::Relaxed),
            pending_bytes: counters.pending_bytes.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn begin_attempt(&self) -> AttemptGuard {
        add(&self.0.attempts, 1);
        AttemptGuard {
            metrics: self.clone(),
            outcome: None,
            submission: None,
            started: Instant::now(),
            stages: None,
        }
    }

    pub(crate) fn begin_submission(&self, bytes: u64) -> SubmissionGuard {
        add(&self.0.pending, 1);
        add(&self.0.pending_bytes, bytes);
        SubmissionGuard {
            metrics: self.clone(),
            bytes,
        }
    }

    pub(crate) fn committed_batch(&self, messages: u64, bytes: u64) {
        add(&self.0.batches, 1);
        add(&self.0.messages, messages);
        add(&self.0.bytes, bytes);
    }

    pub(crate) fn quota_retry(&self) {
        add(&self.0.quota_retries, 1);
    }
}

pub(crate) struct AttemptGuard {
    metrics: ProducerMetrics,
    outcome: Option<AttemptOutcome>,
    submission: Option<SubmissionStatus>,
    started: Instant,
    stages: Option<Arc<OperationMetrics>>,
}

impl AttemptGuard {
    pub fn bind_stages(&mut self, stages: Option<Arc<OperationMetrics>>) {
        self.stages = stages;
    }

    pub fn bind_submission(&mut self, status: SubmissionStatus) {
        self.submission = Some(status);
    }

    pub fn finish(&mut self, outcome: AttemptOutcome) {
        self.outcome = Some(outcome);
    }
}

impl Drop for AttemptGuard {
    fn drop(&mut self) {
        if let Some(stages) = &self.stages {
            stages.observe(Stage::Total, self.started.elapsed().as_secs_f64());
        }
        let counters = &self.metrics.0;
        let interrupted = if self
            .submission
            .as_ref()
            .is_some_and(SubmissionStatus::was_handed_off)
        {
            AttemptOutcome::Unknown
        } else {
            AttemptOutcome::Rejected
        };
        let counter = match self.outcome.unwrap_or(interrupted) {
            AttemptOutcome::Committed => &counters.committed,
            AttemptOutcome::Duplicate => &counters.duplicate,
            AttemptOutcome::Rejected => &counters.rejected,
            AttemptOutcome::Unknown => &counters.unknown,
            AttemptOutcome::Failed => &counters.failed,
        };
        add(counter, 1);
    }
}

pub(crate) struct SubmissionGuard {
    metrics: ProducerMetrics,
    bytes: u64,
}

impl Drop for SubmissionGuard {
    fn drop(&mut self) {
        self.metrics.0.pending.fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .0
            .pending_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

impl Collector for ProducerMetrics {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let counters = &self.0;
        for (name, help, value) in [
            (
                "attempts",
                "Producer transport attempts",
                &counters.attempts,
            ),
            (
                "committed_attempts",
                "Attempts returning durable results except duplicate produce batches",
                &counters.committed,
            ),
            (
                "duplicate_attempts",
                "Attempts returning an original durable result",
                &counters.duplicate,
            ),
            (
                "rejected_attempts",
                "Attempts rejected without a successful append",
                &counters.rejected,
            ),
            (
                "unknown_attempts",
                "Attempts whose caller did not observe a durable result",
                &counters.unknown,
            ),
            (
                "failed_attempts",
                "Attempts with a local runtime failure",
                &counters.failed,
            ),
            (
                "quota_retries",
                "Additional native commit attempts for one original Publish after quota reconfirmation",
                &counters.quota_retries,
            ),
            (
                "new_batches",
                "New batches observed committed by this runtime",
                &counters.batches,
            ),
            (
                "new_messages",
                "New messages observed committed by this runtime",
                &counters.messages,
            ),
            (
                "new_bytes",
                "New logical key and body bytes observed committed",
                &counters.bytes,
            ),
        ] {
            let metric = ConstCounter::new(value.load(Ordering::Relaxed));
            metric.encode(encoder.encode_descriptor(name, help, None, metric.metric_type())?)?;
        }
        for (name, help, value) in [
            (
                "pending_operations",
                "Submissions still owned until Raft returns",
                &counters.pending,
            ),
            (
                "pending_bytes",
                "Application bytes held by owned submissions",
                &counters.pending_bytes,
            ),
        ] {
            let metric = ConstGauge::new(value.load(Ordering::Relaxed));
            metric.encode(encoder.encode_descriptor(name, help, None, metric.metric_type())?)?;
        }
        Ok(())
    }
}

fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(value))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attempts_cancellation_and_commit_counters_are_independent_and_finite() {
        let metrics = ProducerMetrics::default();
        let mut registry = prometheus_client::registry::Registry::with_prefix("tidemq_producer");
        registry.register_collector(Box::new(metrics.clone()));
        let mut attempt = metrics.begin_attempt();
        let status = SubmissionStatus::default();
        assert!(status.try_handoff());
        attempt.bind_submission(status);
        drop(attempt);
        let mut queued = metrics.begin_attempt();
        queued.bind_submission(SubmissionStatus::default());
        drop(queued);
        let pending = metrics.begin_submission(100);
        metrics.committed_batch(3, 90);
        metrics.quota_retry();
        metrics.quota_retry();
        assert_eq!(metrics.snapshot().unknown_attempts_total, 1);
        assert_eq!(metrics.snapshot().rejected_attempts_total, 1);
        assert_eq!(metrics.snapshot().new_messages_total, 3);
        assert_eq!(metrics.snapshot().pending_bytes, 100);
        assert_eq!(metrics.snapshot().quota_retries_total, 2);
        drop(pending);
        assert_eq!(metrics.snapshot().pending_operations, 0);
        let mut text = String::new();
        prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
        assert!(text.lines().count() < 50);
        assert!(text.contains("tidemq_producer_new_messages_total 3"));
        assert!(text.contains("tidemq_producer_quota_retries_total 2"));
        assert!(!text.contains("producer_id"));
    }
}
