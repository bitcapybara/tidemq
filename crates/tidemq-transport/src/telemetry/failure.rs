use serde::Serialize;

use crate::Error;

/// Bounded endpoint history; identities are diagnostic fields, never metric labels.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct TransportFailures {
    pub total: u64,
    pub logged: u64,
    /// Retained separately so reset fallout cannot evict the last RPC close cause.
    pub last_rpc_failure: Option<TransportFailure>,
    pub recent: [Option<TransportFailure>; 8],
}

/// Only code-defined labels are retained. Error displays and wire contents are excluded.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct TransportFailure {
    pub phase: &'static str,
    pub scope: &'static str,
    pub reason: &'static str,
    pub detail: Option<&'static str>,
    pub io_kind: Option<&'static str>,
    pub connection_id: Option<u64>,
    pub stream_id: Option<u64>,
    pub traffic_class: Option<&'static str>,
    pub purpose: Option<&'static str>,
}

impl TransportFailure {
    pub(crate) fn new(phase: &'static str, scope: &'static str, error: Option<&Error>) -> Self {
        let (reason, detail) = match error {
            None => ("lane_finished", None),
            Some(Error::Configuration { reason }) => ("configuration", Some(*reason)),
            Some(Error::Authentication { reason }) => ("authentication", Some(*reason)),
            Some(Error::Authorization { reason }) => ("authorization", Some(*reason)),
            Some(Error::Unavailable) => ("authority_unavailable", None),
            Some(Error::Admission { resource }) => ("admission", Some(*resource)),
            Some(Error::Deadline { operation }) => ("deadline", Some(*operation)),
            Some(Error::Closed) => ("connection_closed", None),
            Some(Error::Poisoned) => ("poisoned", None),
            Some(Error::Prelude) => ("prelude", None),
            Some(Error::Io { operation, .. }) => ("io", Some(*operation)),
            Some(Error::Connection { operation, .. }) => ("quic_connection", Some(*operation)),
            Some(Error::Stream { operation, .. }) => ("quic_stream", Some(*operation)),
            Some(Error::Protocol { source }) => match source {
                tidemq_protocol::Error::Invalid { reason } => ("protocol_invalid", Some(*reason)),
                tidemq_protocol::Error::Limit { name } => ("protocol_limit", Some(*name)),
                tidemq_protocol::Error::Overflow { name } => ("protocol_overflow", Some(*name)),
                tidemq_protocol::Error::Decode { .. } => ("protocol_decode", None),
                tidemq_protocol::Error::Encode { .. } => ("protocol_encode", None),
                tidemq_protocol::Error::Reserve { .. } => ("protocol_allocation", None),
                _ => ("protocol_identity", None),
            },
            Some(Error::Memory { source }) => match source {
                crate::budget::Error::Admission { traffic_class, .. } => {
                    ("memory_admission", Some(*traffic_class))
                }
                crate::budget::Error::Configuration { reason } => {
                    ("memory_configuration", Some(*reason))
                }
            },
            Some(Error::Allocation { .. }) => ("allocation", None),
            Some(Error::Setup { operation, .. }) => ("setup", Some(*operation)),
        };
        let io_kind = match error {
            Some(Error::Io { source, .. }) => Some(match source.kind() {
                std::io::ErrorKind::UnexpectedEof => "unexpected_eof",
                std::io::ErrorKind::ConnectionReset => "connection_reset",
                std::io::ErrorKind::ConnectionAborted => "connection_aborted",
                std::io::ErrorKind::NotConnected => "not_connected",
                std::io::ErrorKind::BrokenPipe => "broken_pipe",
                std::io::ErrorKind::TimedOut => "timed_out",
                std::io::ErrorKind::InvalidData => "invalid_data",
                std::io::ErrorKind::Other => "other",
                _ => "other_io",
            }),
            _ => None,
        };
        Self {
            phase,
            scope,
            reason,
            detail,
            io_kind,
            connection_id: None,
            stream_id: None,
            traffic_class: None,
            purpose: None,
        }
    }
}

impl super::TransportMetrics {
    pub(crate) fn record_failure(&self, failure: TransportFailure) {
        let Ok(mut history) = self.failures.lock() else {
            return;
        };
        history.total = history.total.saturating_add(1);
        if matches!(failure.scope, "rpc_root" | "rpc_lane") {
            history.last_rpc_failure = Some(failure);
        }
        history.recent.rotate_left(1);
        if let Some(last) = history.recent.last_mut() {
            *last = Some(failure);
        }
        // At most 65 warnings per endpoint lifetime, even under repeated hostile input.
        let log = history.total <= 4 || history.total.is_power_of_two();
        if log {
            history.logged = history.logged.saturating_add(1);
        }
        let sequence = history.total;
        drop(history);
        if log {
            tracing::warn!(
                event = "transport_failure",
                failure_sequence = sequence,
                phase = failure.phase,
                scope = failure.scope,
                reason = failure.reason,
                detail = failure.detail,
                io_kind = failure.io_kind,
                connection_id = failure.connection_id,
                stream_id = failure.stream_id,
                traffic_class = failure.traffic_class,
                purpose = failure.purpose,
                "Transport failure classified before lane cleanup"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_history_and_logs_are_bounded_and_keep_the_rpc_cause() {
        let metrics = super::super::TransportMetrics::default();
        metrics.record_failure(TransportFailure::new(
            "rpc_read",
            "rpc_root",
            Some(&Error::Prelude),
        ));
        for _ in 0..31 {
            metrics.record_failure(TransportFailure::new(
                "frame_read",
                "frame",
                Some(&Error::Io {
                    operation: "read frame body",
                    source: std::io::Error::other("private-payload-and-token"),
                }),
            ));
        }
        let history = metrics.snapshot().failures;
        assert_eq!(history.total, 32);
        assert_eq!(history.logged, 7);
        assert_eq!(history.recent.iter().flatten().count(), 8);
        assert_eq!(history.last_rpc_failure.unwrap().reason, "prelude");
        assert!(
            history
                .recent
                .iter()
                .flatten()
                .all(|failure| failure.reason == "io")
        );
        assert!(!format!("{history:?}").contains("private-payload-and-token"));
    }
}
