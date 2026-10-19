use std::{fmt, time::Instant};

use prometheus_client::{
    encoding::{EncodeLabelSet, EncodeLabelValue},
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

pub mod disk;
pub mod memory;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Capacity watermarks or sampling limits are invalid"))]
    InvalidLimits,
    #[snafu(display("Capacity has no fresh observation"))]
    Unmeasured,
    #[snafu(display("Capacity observation has expired"))]
    Stale,
    #[snafu(display("New work is limited by {kind}"))]
    Pressure { kind: Kind },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Watermark {
    pub low: u64,
    pub high: u64,
}

impl Watermark {
    fn validate(self) -> Result<()> {
        if self.low >= self.high || self.high > i64::MAX as u64 {
            return Err(Error::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    pub logical_backlog: Watermark,
    pub physical_storage: Watermark,
    pub shared_reclaim_lag: Watermark,
    pub process_memory: Watermark,
    pub max_sample_age_ms: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            logical_backlog: Watermark {
                low: 96 << 20,
                high: 128 << 20,
            },
            physical_storage: Watermark {
                low: 192 << 20,
                high: 256 << 20,
            },
            shared_reclaim_lag: Watermark {
                low: 48 << 20,
                high: 64 << 20,
            },
            process_memory: Watermark {
                low: 192 << 20,
                high: 256 << 20,
            },
            max_sample_age_ms: 1_000,
        }
    }
}

impl Limits {
    pub fn validate(self) -> Result<()> {
        for watermark in self.watermarks() {
            watermark.validate()?;
        }
        if !(10..=10_000).contains(&self.max_sample_age_ms) {
            return Err(Error::InvalidLimits);
        }
        Ok(())
    }

    fn watermarks(self) -> [Watermark; 4] {
        [
            self.logical_backlog,
            self.physical_storage,
            self.shared_reclaim_lag,
            self.process_memory,
        ]
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, EncodeLabelValue)]
pub enum Kind {
    LogicalBacklog,
    PhysicalStorage,
    SharedReclaimLag,
    ProcessMemory,
}

const KINDS: [Kind; 4] = [
    Kind::LogicalBacklog,
    Kind::PhysicalStorage,
    Kind::SharedReclaimLag,
    Kind::ProcessMemory,
];

impl fmt::Display for Kind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::LogicalBacklog => "logical backlog",
            Self::PhysicalStorage => "physical storage",
            Self::SharedReclaimLag => "shared object reclamation lag",
            Self::ProcessMemory => "process memory pressure",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Work {
    Produce,
    Prefetch,
    Cache,
    BackgroundSnapshot,
    Read,
    Acknowledge,
    Control,
    GarbageCollect,
}

impl Work {
    fn essential(self) -> bool {
        matches!(
            self,
            Self::Read | Self::Acknowledge | Self::Control | Self::GarbageCollect
        )
    }
}

/// Each value has one accounting meaning; RSS already includes allocator and native pages.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct Usage {
    pub logical_backlog_bytes: u64,
    pub physical_storage_bytes: u64,
    pub shared_reclaim_lag_bytes: u64,
    pub process_memory_bytes: u64,
}

impl Usage {
    fn values(self) -> [u64; 4] {
        [
            self.logical_backlog_bytes,
            self.physical_storage_bytes,
            self.shared_reclaim_lag_bytes,
            self.process_memory_bytes,
        ]
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct Labels {
    kind: Kind,
}

#[derive(Clone, Default)]
pub struct Metrics {
    used: Family<Labels, Gauge>,
    blocked: Family<Labels, Gauge>,
    rejected: Counter,
    unavailable_observations: Counter,
}

impl Metrics {
    pub fn register(&self, registry: &mut Registry) {
        for kind in KINDS {
            drop(self.used.get_or_create(&Labels { kind }));
            drop(self.blocked.get_or_create(&Labels { kind }));
        }
        registry.register(
            "tidemq_capacity_used_bytes",
            "Observed capacity by accounting kind",
            self.used.clone(),
        );
        registry.register(
            "tidemq_capacity_blocked",
            "High watermark hysteresis by accounting kind",
            self.blocked.clone(),
        );
        registry.register(
            "tidemq_capacity_rejected",
            "Optional work rejected before admission",
            self.rejected.clone(),
        );
        registry.register(
            "tidemq_capacity_unavailable_observations",
            "Admission checks with absent or stale capacity observations",
            self.unavailable_observations.clone(),
        );
    }
}

pub struct Quota {
    limits: Limits,
    usage: Usage,
    blocked: [bool; 4],
    observed_at: Option<Instant>,
    metrics: Metrics,
}

impl Quota {
    pub fn new(limits: Limits, metrics: Metrics) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            limits,
            usage: Usage::default(),
            blocked: [false; 4],
            observed_at: None,
            metrics,
        })
    }

    pub fn observe(&mut self, usage: Usage, observed_at: Instant) {
        if self
            .observed_at
            .is_some_and(|previous| observed_at < previous)
        {
            return;
        }
        for (((kind, value), watermark), blocked) in KINDS
            .into_iter()
            .zip(usage.values())
            .zip(self.limits.watermarks())
            .zip(self.blocked.iter_mut())
        {
            if value >= watermark.high {
                *blocked = true;
            } else if value <= watermark.low {
                *blocked = false;
            }
            self.metrics
                .used
                .get_or_create(&Labels { kind })
                .set(i64::try_from(value).unwrap_or(i64::MAX));
            self.metrics
                .blocked
                .get_or_create(&Labels { kind })
                .set(i64::from(*blocked));
        }
        self.usage = usage;
        self.observed_at = Some(observed_at);
    }

    pub fn admit(&self, work: Work, now: Instant) -> Result<()> {
        if work.essential() {
            return Ok(());
        }
        let result = self.check(work, now);
        if result.is_err() {
            self.metrics.rejected.inc();
        }
        result
    }

    fn check(&self, work: Work, now: Instant) -> Result<()> {
        let observed = self.observed_at.ok_or_else(|| {
            self.metrics.unavailable_observations.inc();
            Error::Unmeasured
        })?;
        if now.saturating_duration_since(observed).as_millis()
            > u128::from(self.limits.max_sample_age_ms)
        {
            self.metrics.unavailable_observations.inc();
            return Err(Error::Stale);
        }
        for (kind, blocked) in KINDS.into_iter().zip(self.blocked) {
            if blocked && (work == Work::Produce || kind == Kind::ProcessMemory) {
                return Err(Error::Pressure { kind });
            }
        }
        Ok(())
    }

    pub fn diagnose(&self, now: Instant) -> Diagnostic {
        Diagnostic {
            usage: self.usage,
            blocked: self.blocked,
            sample_age_ms: self.observed_at.map(|observed| {
                u64::try_from(now.saturating_duration_since(observed).as_millis())
                    .unwrap_or(u64::MAX)
            }),
            max_sample_age_ms: self.limits.max_sample_age_ms,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Diagnostic {
    pub usage: Usage,
    /// Order is logical backlog, physical storage, shared reclamation lag, process memory.
    pub blocked: [bool; 4],
    pub sample_age_ms: Option<u64>,
    pub max_sample_age_ms: u64,
}

/// Conservative deployment envelopes are budgets; allocator statistics remain a separate observation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MemoryEnvelope {
    pub process_limit_bytes: u64,
    pub application_bytes: u64,
    pub native_bytes: u64,
    pub thread_stack_bytes: u64,
    pub diagnostic_bytes: u64,
    pub allocator_margin_bytes: u64,
}

impl MemoryEnvelope {
    pub fn validate(self) -> Result<()> {
        let reserved = [
            self.application_bytes,
            self.native_bytes,
            self.thread_stack_bytes,
            self.diagnostic_bytes,
            self.allocator_margin_bytes,
        ]
        .into_iter()
        .try_fold(0_u64, |total, part| total.checked_add(part))
        .ok_or(Error::InvalidLimits)?;
        if self.process_limit_bytes == 0
            || self.application_bytes == 0
            || self.native_bytes == 0
            || self.thread_stack_bytes == 0
            || self.diagnostic_bytes == 0
            || self.allocator_margin_bytes == 0
            || reserved > self.process_limit_bytes
        {
            return Err(Error::InvalidLimits);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn high_low_hysteresis_keeps_ack_read_gc_and_control_available() {
        let metrics = Metrics::default();
        let mut quota = Quota::new(Limits::default(), metrics.clone()).unwrap();
        let now = Instant::now();
        assert!(matches!(
            quota.admit(Work::Produce, now),
            Err(Error::Unmeasured)
        ));
        for work in [
            Work::Read,
            Work::Acknowledge,
            Work::Control,
            Work::GarbageCollect,
        ] {
            quota.admit(work, now).unwrap();
        }
        let usage = Usage {
            logical_backlog_bytes: 128 << 20,
            ..Usage::default()
        };
        quota.observe(usage, now);
        assert!(matches!(
            quota.admit(Work::Produce, now),
            Err(Error::Pressure {
                kind: Kind::LogicalBacklog
            })
        ));
        quota.admit(Work::Prefetch, now).unwrap();
        quota.observe(
            Usage {
                logical_backlog_bytes: 100 << 20,
                ..usage
            },
            now,
        );
        assert!(quota.admit(Work::Produce, now).is_err());
        quota.observe(
            Usage {
                logical_backlog_bytes: 96 << 20,
                ..usage
            },
            now,
        );
        quota.admit(Work::Produce, now).unwrap();
        let later = now + Duration::from_millis(1_001);
        assert!(matches!(
            quota.admit(Work::Produce, later),
            Err(Error::Stale)
        ));
        quota.admit(Work::Acknowledge, later).unwrap();
        let mut registry = Registry::default();
        metrics.register(&mut registry);
        let mut text = String::new();
        prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
        assert_eq!(
            text.lines()
                .filter(|line| line.starts_with("tidemq_capacity_used_bytes{"))
                .count(),
            4
        );
        assert!(text.contains("tidemq_capacity_rejected_total 4"));
        assert_eq!(text.matches("# EOF").count(), 1);
    }

    #[test]
    fn memory_pressure_stops_optional_buffers_and_envelopes_include_non_application_memory() {
        let mut quota = Quota::new(Limits::default(), Metrics::default()).unwrap();
        let now = Instant::now();
        quota.observe(
            Usage {
                process_memory_bytes: 256 << 20,
                ..Usage::default()
            },
            now,
        );
        for work in [
            Work::Produce,
            Work::Prefetch,
            Work::Cache,
            Work::BackgroundSnapshot,
        ] {
            assert!(matches!(
                quota.admit(work, now),
                Err(Error::Pressure {
                    kind: Kind::ProcessMemory
                })
            ));
        }
        let mut envelope = MemoryEnvelope {
            process_limit_bytes: 512 << 20,
            application_bytes: 64 << 20,
            native_bytes: 128 << 20,
            thread_stack_bytes: 32 << 20,
            diagnostic_bytes: 16 << 20,
            allocator_margin_bytes: 64 << 20,
        };
        envelope.validate().unwrap();
        envelope.allocator_margin_bytes = 512 << 20;
        assert!(envelope.validate().is_err());
        envelope.allocator_margin_bytes = u64::MAX;
        assert!(envelope.validate().is_err());
    }
}
