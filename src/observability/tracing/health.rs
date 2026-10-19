use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

#[derive(Clone, Debug, Default)]
pub struct TraceHealth(pub(crate) Arc<Counters>);

#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub admitted: AtomicU64,
    pub sampled_out: AtomicU64,
    pub budget_dropped: AtomicU64,
    pub invalid_dropped: AtomicU64,
    pub pending_spans: AtomicU64,
    pub pending_bytes: AtomicU64,
    pub exports: AtomicU64,
    pub export_failures: AtomicU64,
    pub export_timeouts: AtomicU64,
    pub export_bytes: AtomicU64,
    pub response_rejected: AtomicU64,
    pub shutdown_timeouts: AtomicU64,
    pub discarded_spans: AtomicU64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct TraceSnapshot {
    pub admitted: u64,
    pub sampled_out: u64,
    pub budget_dropped: u64,
    pub invalid_dropped: u64,
    pub pending_spans: u64,
    pub pending_bytes: u64,
    pub exports: u64,
    pub export_failures: u64,
    pub export_timeouts: u64,
    pub export_bytes: u64,
    pub response_rejected: u64,
    pub shutdown_timeouts: u64,
    pub discarded_spans: u64,
}

impl TraceHealth {
    pub fn snapshot(&self) -> TraceSnapshot {
        let c = &self.0;
        TraceSnapshot {
            admitted: c.admitted.load(Ordering::Relaxed),
            sampled_out: c.sampled_out.load(Ordering::Relaxed),
            budget_dropped: c.budget_dropped.load(Ordering::Relaxed),
            invalid_dropped: c.invalid_dropped.load(Ordering::Relaxed),
            pending_spans: c.pending_spans.load(Ordering::Relaxed),
            pending_bytes: c.pending_bytes.load(Ordering::Relaxed),
            exports: c.exports.load(Ordering::Relaxed),
            export_failures: c.export_failures.load(Ordering::Relaxed),
            export_timeouts: c.export_timeouts.load(Ordering::Relaxed),
            export_bytes: c.export_bytes.load(Ordering::Relaxed),
            response_rejected: c.response_rejected.load(Ordering::Relaxed),
            shutdown_timeouts: c.shutdown_timeouts.load(Ordering::Relaxed),
            discarded_spans: c.discarded_spans.load(Ordering::Relaxed),
        }
    }
}

impl TraceSnapshot {
    // The fixed metric dictionary intentionally stays together to keep every series explicit.
    pub(crate) fn encode(
        &self,
        encoder: &mut prometheus_client::encoding::DescriptorEncoder<'_>,
    ) -> std::result::Result<(), std::fmt::Error> {
        use prometheus_client::encoding::EncodeMetric;
        use prometheus_client::metrics::{counter::ConstCounter, gauge::ConstGauge};
        for (name, help, value) in [
            (
                "trace_admitted",
                "Finished spans admitted to the bounded OTLP queue",
                self.admitted,
            ),
            (
                "trace_sampled_out",
                "Spans omitted by the local ratio or rate budget",
                self.sampled_out,
            ),
            (
                "trace_budget_dropped",
                "Finished spans dropped because export capacity is exhausted",
                self.budget_dropped,
            ),
            (
                "trace_invalid_dropped",
                "Finished spans omitted by the export field whitelist",
                self.invalid_dropped,
            ),
            (
                "trace_exports",
                "OTLP export attempts without retries",
                self.exports,
            ),
            (
                "trace_export_failures",
                "OTLP export attempts that failed",
                self.export_failures,
            ),
            (
                "trace_export_timeouts",
                "OTLP exports that reached their local timeout",
                self.export_timeouts,
            ),
            (
                "trace_export_bytes",
                "Encoded OTLP request body bytes attempted",
                self.export_bytes,
            ),
            (
                "trace_response_rejected",
                "OTLP responses rejected at the byte limit",
                self.response_rejected,
            ),
            (
                "trace_shutdown_timeouts",
                "OTLP workers cancelled at the original service shutdown deadline",
                self.shutdown_timeouts,
            ),
            (
                "trace_discarded_spans",
                "Finished sampled spans omitted by validation, capacity, export failure or shutdown",
                self.discarded_spans,
            ),
        ] {
            let counter = ConstCounter::new(value);
            counter.encode(encoder.encode_descriptor(
                name,
                help,
                None,
                counter.metric_type(),
            )?)?;
        }
        for (name, help, value) in [
            (
                "trace_pending_spans",
                "Queued and currently exporting spans sharing the same fixed capacity",
                self.pending_spans,
            ),
            (
                "trace_pending_bytes",
                "Logical trace admission charge at 4096 bytes per pending span, not process RSS",
                self.pending_bytes,
            ),
        ] {
            let gauge = ConstGauge::new(i64::try_from(value).unwrap_or(i64::MAX));
            gauge.encode(encoder.encode_descriptor(name, help, None, gauge.metric_type())?)?;
        }
        Ok(())
    }
}
