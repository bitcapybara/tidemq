use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use prometheus_client::registry::Registry;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::runtime::process::{Phase, Process};

pub(super) struct TaskGuard {
    pub process: Process,
    pub shutdown: watch::Receiver<Option<Instant>>,
    pub dependency: &'static str,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if self.shutdown.borrow().is_none()
            && !matches!(
                self.process.phase(),
                Phase::Draining | Phase::Failed | Phase::Stopped
            )
        {
            self.process.fail(self.dependency);
        }
    }
}

#[derive(Default)]
pub(crate) struct Metrics {
    pub requests: Counter,
    pub asset_errors: Counter,
    pub asset_rechecks: Counter,
    pub asset_recoveries: Counter,
    pub asset_failures: Counter,
    pub connections: Gauge,
    pub handshakes: Gauge,
    pub tls_errors: Counter,
    pub tls_timeouts: Counter,
    pub connection_rejections: Counter,
    pub connection_errors: Counter,
}

impl Metrics {
    pub(crate) fn register(&self, registry: &mut Registry) {
        let registry = registry.sub_registry_with_prefix("tidemq_console");
        for (name, help, metric) in [
            ("http_requests", "Console HTTP requests.", &self.requests),
            (
                "asset_read_errors",
                "Required asset read failures.",
                &self.asset_errors,
            ),
            (
                "asset_rechecks",
                "Singleflight required asset rechecks.",
                &self.asset_rechecks,
            ),
            (
                "asset_recoveries",
                "Required assets recovered within the deadline.",
                &self.asset_recoveries,
            ),
            (
                "asset_failures",
                "Required assets unavailable at the deadline.",
                &self.asset_failures,
            ),
            ("tls_errors", "Rejected TLS handshakes.", &self.tls_errors),
            (
                "tls_timeouts",
                "TLS handshakes exceeding their deadline.",
                &self.tls_timeouts,
            ),
            (
                "connection_rejections",
                "Connections rejected by admission limits.",
                &self.connection_rejections,
            ),
            (
                "connection_errors",
                "Client HTTP connection errors.",
                &self.connection_errors,
            ),
        ] {
            registry.register(name, help, metric.clone());
        }
        registry.register(
            "connections",
            "Owned Console TCP connections.",
            self.connections.clone(),
        );
        registry.register(
            "handshakes",
            "In-flight Console TLS handshakes.",
            self.handshakes.clone(),
        );
    }
}
