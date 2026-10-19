use std::sync::{Arc, Mutex};

use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeMetric},
    metrics::{counter::ConstCounter, gauge::ConstGauge},
};
use serde::Serialize;

#[derive(Clone, Debug, Default)]
pub(crate) struct RuntimeMetrics(pub Arc<Mutex<Snapshot>>);

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct Snapshot {
    pub ready: bool,
    pub process_memory: Option<crate::quota::memory::Snapshot>,
    pub log_dropped_events: Option<u64>,
    pub log_sink_errors: Option<u64>,
    pub log_oversized_events: Option<u64>,
}

impl Collector for RuntimeMetrics {
    fn encode(&self, mut encoder: DescriptorEncoder) -> std::result::Result<(), std::fmt::Error> {
        let snapshot = self.0.lock().map_err(|_| std::fmt::Error)?.clone();
        let ready = ConstGauge::new(i64::from(snapshot.ready));
        ready.encode(encoder.encode_descriptor(
            "ready",
            "Role authority readiness from the latest bounded sample",
            None,
            ready.metric_type(),
        )?)?;
        if let Some(memory) = snapshot.process_memory {
            for (name, help, value) in [
                (
                    "process_rss_bytes",
                    "Resident process memory from the operating system",
                    Some(memory.process_rss_bytes),
                ),
                (
                    "cgroup_current_bytes",
                    "Cgroup current usage, separate from process RSS",
                    memory.cgroup_current_bytes,
                ),
                (
                    "cgroup_limit_bytes",
                    "Finite cgroup memory limit when configured",
                    memory.cgroup_limit_bytes,
                ),
            ] {
                if let Some(value) = value {
                    let gauge = ConstGauge::new(i64::try_from(value).unwrap_or(i64::MAX));
                    gauge.encode(encoder.encode_descriptor(
                        name,
                        help,
                        None,
                        gauge.metric_type(),
                    )?)?;
                }
            }
        }
        for (name, help, value) in [
            (
                "log_dropped_events",
                "Events dropped by the bounded logging queue",
                snapshot.log_dropped_events,
            ),
            (
                "log_sink_errors",
                "Logging writer or flush failures",
                snapshot.log_sink_errors,
            ),
            (
                "log_oversized_events",
                "Events omitted after exceeding their byte budget",
                snapshot.log_oversized_events,
            ),
        ] {
            if let Some(value) = value {
                let counter = ConstCounter::new(value);
                counter.encode(encoder.encode_descriptor(
                    name,
                    help,
                    None,
                    counter.metric_type(),
                )?)?;
            }
        }
        Ok(())
    }
}

pub(crate) struct OperationTimer {
    metrics: Option<Arc<crate::observability::metrics::OperationMetrics>>,
    started: std::time::Instant,
    outcome: crate::observability::metrics::Outcome,
}

impl OperationTimer {
    pub(crate) fn new(
        metrics: Option<Arc<crate::observability::metrics::OperationMetrics>>,
    ) -> Self {
        Self {
            metrics,
            started: std::time::Instant::now(),
            outcome: crate::observability::metrics::Outcome::Unknown,
        }
    }

    pub(crate) fn finish(
        &mut self,
        result: &super::Result<tidemq_protocol::common::Frame>,
        limits: &tidemq_protocol::ProtocolLimits,
    ) {
        use crate::observability::metrics::Outcome;
        self.outcome = match result {
            Ok(frame) if frame.kind == i32::from(tidemq_protocol::common::FrameKind::Error) => {
                tidemq_protocol::framing::decode_payload::<tidemq_protocol::common::ErrorReply>(
                    frame.payload.clone(),
                    limits,
                )
                .map(|error| operation_outcome(error.outcome))
                .unwrap_or(Outcome::Failed)
            }
            Ok(_) => Outcome::Committed,
            Err(error) => operation_outcome(error.failure().outcome),
        };
    }
}

impl Drop for OperationTimer {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.result(self.outcome);
            metrics.observe(
                crate::observability::metrics::Stage::Total,
                self.started.elapsed().as_secs_f64(),
            );
        }
    }
}

fn operation_outcome(outcome: i32) -> crate::observability::metrics::Outcome {
    use crate::observability::metrics::Outcome;
    match tidemq_protocol::common::Outcome::try_from(outcome) {
        Ok(tidemq_protocol::common::Outcome::Committed) => Outcome::Committed,
        Ok(tidemq_protocol::common::Outcome::Rejected) => Outcome::Rejected,
        Ok(tidemq_protocol::common::Outcome::Unknown) => Outcome::Unknown,
        _ => Outcome::Failed,
    }
}

pub(crate) struct OwnershipTimer<'a> {
    metrics: &'a crate::ownership::OwnershipMetrics,
    stage: crate::ownership::Transition,
    operation: tidemq_protocol::Id,
    epoch: u64,
    started: std::time::Instant,
    outcome: crate::ownership::TransitionOutcome,
}

impl<'a> OwnershipTimer<'a> {
    pub(crate) fn new(
        metrics: &'a crate::ownership::OwnershipMetrics,
        stage: crate::ownership::Transition,
        operation: tidemq_protocol::Id,
        epoch: u64,
    ) -> Self {
        Self {
            metrics,
            stage,
            operation,
            epoch,
            started: std::time::Instant::now(),
            outcome: crate::ownership::TransitionOutcome::Unknown,
        }
    }

    pub(crate) fn finish<T>(&mut self, result: &super::Result<T>) {
        use crate::ownership::TransitionOutcome;
        self.outcome = match result {
            Ok(_) => TransitionOutcome::Committed,
            Err(error) => match operation_outcome(error.failure().outcome) {
                crate::observability::metrics::Outcome::Committed => TransitionOutcome::Committed,
                crate::observability::metrics::Outcome::Rejected => TransitionOutcome::Rejected,
                crate::observability::metrics::Outcome::Unknown => TransitionOutcome::Unknown,
                crate::observability::metrics::Outcome::Failed => TransitionOutcome::Failed,
            },
        };
    }
}

impl Drop for OwnershipTimer<'_> {
    fn drop(&mut self) {
        self.metrics
            .record(self.stage, self.outcome, self.started.elapsed());
        crate::ownership::emit_transition(self.operation, self.epoch, self.stage, self.outcome);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_local_operations_record_unknown_and_total_once() {
        let metrics = crate::observability::metrics::Metrics::default();
        let timer =
            OperationTimer::new(metrics.operation(crate::observability::metrics::Operation::Ack));
        drop(timer);
        let encoded = metrics.encode().unwrap();
        assert!(
            encoded.contains("tidemq_operations_total{operation=\"Ack\",outcome=\"Unknown\"} 1")
        );
        assert!(
            encoded.contains(
                "tidemq_stage_duration_seconds_count{operation=\"Ack\",stage=\"Total\"} 1"
            )
        );
    }
}
