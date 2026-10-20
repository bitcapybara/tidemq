use std::sync::Mutex;
use std::time::{Duration, Instant};

use prometheus_client::{
    metrics::{counter::Counter, gauge::Gauge},
    registry::Registry,
};

#[derive(Debug, Default)]
pub struct DispatchMetrics {
    pub(super) admitted: Counter,
    pub(super) rejected: Counter,
    pub(super) handler_errors: Counter,
    pub(super) suppressed_logs: Counter,
    pub(super) active_handlers: Gauge,
    last_error_log: Mutex<Option<Instant>>,
}

impl DispatchMetrics {
    /// Register once in the process root registry, before sharing the runtime.
    pub fn register(&self, registry: &mut Registry) {
        registry.register(
            "tidemq_dispatch_admitted",
            "Wire requests admitted to bounded handlers",
            self.admitted.clone(),
        );
        registry.register(
            "tidemq_dispatch_rejected",
            "Wire requests rejected before handler admission",
            self.rejected.clone(),
        );
        registry.register(
            "tidemq_dispatch_handler_errors",
            "Handler failures regardless of log sampling",
            self.handler_errors.clone(),
        );
        registry.register(
            "tidemq_dispatch_suppressed_logs",
            "Handler warnings suppressed by the fixed rate limit",
            self.suppressed_logs.clone(),
        );
        registry.register(
            "tidemq_dispatch_active_handlers",
            "Currently executing owned wire handlers",
            self.active_handlers.clone(),
        );
    }

    pub(crate) fn may_log(&self, now: Instant) -> bool {
        let Ok(mut last) = self.last_error_log.lock() else {
            self.suppressed_logs.inc();
            return false;
        };
        if last.is_some_and(|last| now.saturating_duration_since(last) < Duration::from_secs(1)) {
            self.suppressed_logs.inc();
            return false;
        }
        *last = Some(now);
        true
    }
}

pub(super) struct HandlerGauge(pub Gauge);
impl Drop for HandlerGauge {
    fn drop(&mut self) {
        self.0.dec();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_logs_are_rate_limited_and_counters_remain_complete() {
        let metrics = DispatchMetrics::default();
        let now = Instant::now();
        assert!(metrics.may_log(now));
        for _ in 0..99 {
            metrics.handler_errors.inc();
            assert!(!metrics.may_log(now));
        }
        assert!(metrics.may_log(now + Duration::from_secs(1)));
        let mut registry = Registry::default();
        metrics.register(&mut registry);
        let mut text = String::new();
        prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
        assert!(text.contains("tidemq_dispatch_suppressed_logs_total 99"));
        assert!(text.contains("tidemq_dispatch_handler_errors_total 99"));
        assert_eq!(text.matches("# EOF").count(), 1);
    }
}
