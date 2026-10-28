use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use serde::Serialize;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub(crate) enum Phase {
    Starting,
    Recovering,
    Ready,
    Draining,
    Failed,
    Stopped,
}

#[derive(Clone)]
pub(crate) struct Process {
    inner: Arc<Inner>,
}

struct Inner {
    phase: AtomicU8,
    transition: Mutex<()>,
    changed: watch::Sender<()>,
    ready: Gauge,
    phase_metric: Gauge,
    failures: Counter,
}

impl Default for Process {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                phase: AtomicU8::new(Phase::Starting as u8),
                transition: Mutex::new(()),
                changed: watch::channel(()).0,
                ready: Gauge::default(),
                phase_metric: Gauge::default(),
                failures: Counter::default(),
            }),
        }
    }
}

impl Process {
    pub fn register(&self, registry: &mut prometheus_client::registry::Registry) {
        registry.register(
            "process_ready",
            "Complete process readiness",
            self.inner.ready.clone(),
        );
        registry.register(
            "process_phase",
            "Process phase: starting=0, recovering=1, ready=2, draining=3, failed=4, stopped=5",
            self.inner.phase_metric.clone(),
        );
        registry.register(
            "process_failures",
            "Required process dependency failures",
            self.inner.failures.clone(),
        );
    }

    pub fn phase(&self) -> Phase {
        match self.inner.phase.load(Ordering::Acquire) {
            0 => Phase::Starting,
            1 => Phase::Recovering,
            2 => Phase::Ready,
            3 => Phase::Draining,
            5 => Phase::Stopped,
            _ => Phase::Failed,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.inner.changed.subscribe()
    }

    pub fn recovering(&self) -> bool {
        self.advance(Phase::Starting, Phase::Recovering)
    }

    pub fn ready(&self) -> bool {
        self.advance(Phase::Recovering, Phase::Ready)
    }

    pub fn drain(&self) {
        loop {
            let previous = self.phase();
            if matches!(previous, Phase::Draining | Phase::Failed | Phase::Stopped) {
                return;
            }
            if self.advance(previous, Phase::Draining) {
                return;
            }
        }
    }

    pub fn fail(&self, dependency: &'static str) {
        loop {
            let previous = self.phase();
            if matches!(previous, Phase::Failed | Phase::Stopped) {
                return;
            }
            if self.advance(previous, Phase::Failed) {
                self.inner.failures.inc();
                tracing::error!(
                    event_code = "process_dependency_failed",
                    dependency,
                    "A required process dependency failed"
                );
                return;
            }
        }
    }

    pub fn stopped(&self) {
        self.advance(Phase::Draining, Phase::Stopped);
    }

    fn advance(&self, previous: Phase, next: Phase) -> bool {
        let Ok(_transition) = self.inner.transition.lock() else {
            self.inner
                .phase
                .store(Phase::Failed as u8, Ordering::Release);
            self.inner.ready.set(0);
            self.inner.phase_metric.set(Phase::Failed as i64);
            self.inner.changed.send_replace(());
            return false;
        };
        if self
            .inner
            .phase
            .compare_exchange(
                previous as u8,
                next as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }
        // Notifications carry no state, so a delayed publisher cannot restore an older phase.
        self.inner.changed.send_replace(());
        let current = self.phase();
        self.inner.ready.set(i64::from(current == Phase::Ready));
        self.inner.phase_metric.set(i64::from(current as u8));
        tracing::info!(event_code = "process_phase_changed", phase = ?current, "Process lifecycle changed");
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_cannot_be_overwritten_by_late_readiness_or_drain() {
        let process = Process::default();
        assert!(process.recovering());
        process.fail("console_assets");
        assert!(!process.ready());
        process.drain();
        process.stopped();
        assert_eq!(process.phase(), Phase::Failed);
        assert_eq!(process.inner.ready.get(), 0);
        assert_eq!(process.inner.failures.get(), 1);
    }

    #[test]
    fn drain_prevents_late_ready_and_stopped_is_terminal() {
        let process = Process::default();
        process.recovering();
        process.drain();
        assert!(!process.ready());
        process.stopped();
        process.fail("late_task");
        assert_eq!(process.phase(), Phase::Stopped);
    }

    #[tokio::test]
    async fn failure_notifies_supervisor_without_requiring_an_http_task_exit() {
        let process = Process::default();
        let mut changed = process.subscribe();
        process.fail("console_assets");
        changed.changed().await.unwrap();
        assert_eq!(process.phase(), Phase::Failed);
    }
}
