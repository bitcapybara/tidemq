use std::sync::{Arc, Mutex};

use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeMetric},
    metrics::{
        counter::{ConstCounter, Counter},
        gauge::ConstGauge,
    },
};
use serde::Serialize;
use tidemq_protocol::common::{Frame, FrameKind, Outcome};

use crate::server::DispatchMetrics;

pub(super) fn log_unknown_error(frame: &Frame, error: super::Error, health: &DispatchMetrics) {
    let Some(diagnostic) = unknown_diagnostic(&error, health, std::time::Instant::now()) else {
        return;
    };
    let command_kind = FrameKind::try_from(frame.kind)
        .map(|kind| kind.as_str_name())
        .unwrap_or("UNSPECIFIED");
    let request_id = tidemq_protocol::Id::from_slice(&frame.request_id)
        .map(|id| id.to_string())
        .unwrap_or_else(|_| "invalid".into());
    let operation_id = tidemq_protocol::Id::from_slice(&frame.operation_id)
        .map(|id| id.to_string())
        .unwrap_or_else(|_| "invalid".into());
    let error = anyhow::Error::new(error);
    tracing::warn!(
        event_code = "runtime_request_unknown",
        stage = "runtime_error",
        command_kind,
        request_id,
        operation_id,
        error_kind = diagnostic.error_kind,
        operation = diagnostic.operation,
        code = diagnostic.code,
        outcome = "Unknown",
        error = %format_args!("{error:#}"),
        "Runtime returned an unknown operation result"
    );
}

struct UnknownDiagnostic {
    error_kind: &'static str,
    operation: &'static str,
    code: i32,
}

fn unknown_diagnostic(
    error: &super::Error,
    health: &DispatchMetrics,
    now: std::time::Instant,
) -> Option<UnknownDiagnostic> {
    let failure = error.failure();
    if failure.outcome != i32::from(Outcome::Unknown) || !health.may_log(now) {
        return None;
    }
    let (error_kind, operation) = match error {
        super::Error::Admitted { .. } => (
            "runtime_admitted_unknown",
            "observe durable operation completion",
        ),
        super::Error::Operation { operation, source } => {
            let error_kind = match source
                .get_ref()
                .and_then(|source| source.downcast_ref::<crate::broker::Error>())
            {
                Some(crate::broker::Error::Unknown { .. }) => "broker_post_handoff_unknown",
                Some(crate::broker::Error::Commit { .. }) => "broker_commit",
                _ => "runtime_operation_unknown",
            };
            (error_kind, *operation)
        }
        super::Error::Rejected { .. } | super::Error::TenantGrantExhausted { .. } => {
            ("runtime_rejected_unknown", "none")
        }
        super::Error::CommittedConsumerWriter { .. } => {
            ("consumer_commit_fence", "verify committed consumer writer")
        }
        super::Error::Invalid { .. } | super::Error::LocalNotSubmitted { .. } => return None,
    };
    Some(UnknownDiagnostic {
        error_kind,
        operation,
        code: failure.code,
    })
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RuntimeMetrics(pub Arc<Mutex<Snapshot>>, Counter);

impl RuntimeMetrics {
    pub(crate) fn hot_delivery_reclaimed(&self) {
        self.1.inc();
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct Snapshot {
    pub ready: bool,
    pub sampled_at_ms: Option<u64>,
    pub process_memory: Option<crate::quota::memory::Snapshot>,
    pub log_dropped_events: Option<u64>,
    pub log_sink_errors: Option<u64>,
    pub log_oversized_events: Option<u64>,
    pub tracing: Option<crate::observability::tracing::TraceSnapshot>,
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
        self.1.encode(encoder.encode_descriptor(
            "read_hot_reclaim_conflicts",
            "Hot delivery lookups that resumed archive replay after local body reclamation",
            None,
            self.1.metric_type(),
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
        if let Some(tracing) = snapshot.tracing {
            tracing.encode(&mut encoder)?;
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
            Ok(frame)
                if frame.kind == i32::from(tidemq_protocol::common::FrameKind::PublishResult) =>
            {
                tidemq_protocol::framing::decode_payload::<tidemq_protocol::public::PublishResult>(
                    frame.payload.clone(),
                    limits,
                )
                .map(|result| {
                    if result.offset.is_some() {
                        Outcome::Committed
                    } else {
                        Outcome::Rejected
                    }
                })
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
    fn unknown_error_log_retains_nested_source_chain_and_wire_failure() {
        use tidemq_protocol::common::{ErrorCode, ErrorReply, TrafficClass};
        use tidemq_protocol::framing::{decode_payload, new_frame};

        let identity = tidemq_protocol::Id::new_v7().unwrap();
        let path = std::env::temp_dir()
            .join(format!("tidemq-runtime-source-{identity}"))
            .join("metadata.wal");
        let source = std::fs::File::open(&path).unwrap_err();
        assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
        let leaf = source.to_string();
        let error = super::super::Error::operation(
            "complete metadata",
            crate::Error::Io {
                operation: "open missing metadata log",
                path: path.clone(),
                source,
            },
        );
        let limits = tidemq_protocol::ProtocolLimits::default();
        let request = new_frame(
            FrameKind::CreateLogicalTopic,
            TrafficClass::Control,
            identity,
            identity,
            &tidemq_protocol::public::LogicalTopic {
                name: "diagnostic".into(),
            },
            1000,
            &limits,
        )
        .unwrap();
        let response = super::super::wire::error_response(&request, &error, &limits).unwrap();
        let failure: ErrorReply = decode_payload(response.payload, &limits).unwrap();
        assert_eq!(failure.code, i32::from(ErrorCode::Unavailable));
        assert_eq!(failure.outcome, i32::from(Outcome::Unknown));
        assert!(failure.retryable);

        let buffer = crate::observability::logging::tests::Buffer(Arc::default());
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            log_unknown_error(&request, error, &DispatchMetrics::default());
        });
        let event: serde_json::Value = serde_json::from_slice(&buffer.0.lock().unwrap()).unwrap();
        let fields = &event["fields"];
        assert_eq!(event["level"], "WARN");
        assert_eq!(fields["event_code"], "runtime_request_unknown");
        assert_eq!(fields["stage"], "runtime_error");
        assert_eq!(fields["command_kind"], "CREATE_LOGICAL_TOPIC");
        assert_eq!(fields["request_id"], identity.to_string());
        assert_eq!(fields["operation_id"], identity.to_string());
        assert_eq!(fields["error_kind"], "runtime_operation_unknown");
        assert_eq!(fields["operation"], "complete metadata");
        assert_eq!(fields["code"], failure.code);
        assert_eq!(fields["outcome"], "Unknown");
        let chain = fields["error"].as_str().unwrap_or_default();
        assert!(
            chain.contains(&leaf),
            "The runtime warning must retain the nested leaf source: {event}"
        );
        assert!(chain.contains("Runtime operation failed: complete metadata"));
        assert!(chain.contains("Failed to open missing metadata log"));
        assert!(chain.contains(path.to_str().unwrap()));
    }

    #[test]
    fn unknown_diagnostics_classify_typed_causes_without_error_text() {
        let identity = tidemq_protocol::Id::from_slice(&[1; 16]).unwrap();
        for (error, expected) in [
            (super::super::Error::unknown(), "runtime_rejected_unknown"),
            (
                super::super::Error::operation(
                    "inspect runtime state",
                    std::io::Error::other("test"),
                ),
                "runtime_operation_unknown",
            ),
            (
                super::super::Error::operation(
                    "observe broker post-handoff result",
                    crate::broker::Error::Unknown {
                        operation_id: identity,
                        request_id: identity,
                    },
                ),
                "broker_post_handoff_unknown",
            ),
            (
                super::super::Error::operation(
                    "commit authoritative broker operation",
                    crate::broker::Error::Commit {
                        operation_id: identity,
                        request_id: identity,
                        source: Box::new(openraft::error::RaftError::Fatal(
                            openraft::error::Fatal::Stopped,
                        )),
                    },
                ),
                "broker_commit",
            ),
        ] {
            let diagnostic = unknown_diagnostic(
                &error,
                &DispatchMetrics::default(),
                std::time::Instant::now(),
            )
            .unwrap();
            assert_eq!(diagnostic.error_kind, expected);
            assert_eq!(
                diagnostic.code,
                i32::from(tidemq_protocol::common::ErrorCode::Unavailable)
            );
        }
    }

    #[test]
    fn only_unknown_errors_use_the_shared_dispatch_log_budget() {
        let health = Arc::new(DispatchMetrics::default());
        let now = std::time::Instant::now();
        for error in [
            super::super::Error::invalid("test invalid request"),
            super::super::Error::unavailable(),
        ] {
            assert!(unknown_diagnostic(&error, &health, now).is_none());
        }
        let error = super::super::Error::unknown();
        let diagnostic = unknown_diagnostic(&error, &health, now).unwrap();
        assert_eq!(diagnostic.operation, "none");
        let dispatcher = health.clone();
        assert!(!dispatcher.may_log(now));
        assert!(unknown_diagnostic(&error, &dispatcher, now).is_none());
        assert!(
            unknown_diagnostic(&error, &dispatcher, now + std::time::Duration::from_secs(1))
                .is_some()
        );
    }

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
