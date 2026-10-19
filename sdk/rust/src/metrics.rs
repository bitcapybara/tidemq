use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct Metrics {
    pub(crate) requests: AtomicU64,
    pub(crate) retries: AtomicU64,
    pub(crate) unknown: AtomicU64,
    pub(crate) deliveries: AtomicU64,
    pub(crate) acknowledged: AtomicU64,
    request_duration_ns: AtomicU64,
    admission_wait_ns: AtomicU64,
    admitted_bytes: AtomicU64,
    pending_batches: AtomicU64,
    active_deliveries: AtomicU64,
    #[cfg(feature = "metrics")]
    latency: prometheus_client::metrics::histogram::Histogram,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            requests: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            unknown: AtomicU64::new(0),
            deliveries: AtomicU64::new(0),
            acknowledged: AtomicU64::new(0),
            request_duration_ns: AtomicU64::new(0),
            admission_wait_ns: AtomicU64::new(0),
            admitted_bytes: AtomicU64::new(0),
            pending_batches: AtomicU64::new(0),
            active_deliveries: AtomicU64::new(0),
            #[cfg(feature = "metrics")]
            latency: prometheus_client::metrics::histogram::Histogram::new([
                0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0, 300.0,
            ]),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MetricsSnapshot {
    pub requests: u64,
    pub retries: u64,
    pub unknown: u64,
    pub deliveries: u64,
    pub acknowledged: u64,
    pub request_duration_ns: u64,
    pub admission_wait_ns: u64,
    pub admitted_bytes: u64,
    pub pending_batches: u64,
    pub active_deliveries: u64,
}

impl Metrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            unknown: self.unknown.load(Ordering::Relaxed),
            deliveries: self.deliveries.load(Ordering::Relaxed),
            acknowledged: self.acknowledged.load(Ordering::Relaxed),
            request_duration_ns: self.request_duration_ns.load(Ordering::Relaxed),
            admission_wait_ns: self.admission_wait_ns.load(Ordering::Relaxed),
            admitted_bytes: self.admitted_bytes.load(Ordering::Relaxed),
            pending_batches: self.pending_batches.load(Ordering::Relaxed),
            active_deliveries: self.active_deliveries.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn timer(self: &std::sync::Arc<Self>) -> Timer {
        Timer {
            metrics: self.clone(),
            started: std::time::Instant::now(),
            admission: false,
        }
    }

    pub(crate) fn admission_timer(self: &std::sync::Arc<Self>) -> Timer {
        Timer {
            metrics: self.clone(),
            started: std::time::Instant::now(),
            admission: true,
        }
    }
    pub(crate) fn charge(self: &std::sync::Arc<Self>, bytes: u32, delivery: bool) -> Charge {
        increment(&self.admitted_bytes, u64::from(bytes));
        increment(
            if delivery {
                &self.active_deliveries
            } else {
                &self.pending_batches
            },
            1,
        );
        Charge {
            metrics: self.clone(),
            bytes,
            delivery,
        }
    }
}

pub(crate) struct Charge {
    metrics: std::sync::Arc<Metrics>,
    bytes: u32,
    delivery: bool,
}
impl Drop for Charge {
    fn drop(&mut self) {
        let _ =
            self.metrics
                .admitted_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                    Some(old.saturating_sub(u64::from(self.bytes)))
                });
        let count = if self.delivery {
            &self.metrics.active_deliveries
        } else {
            &self.metrics.pending_batches
        };
        let _ = count.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
            Some(old.saturating_sub(1))
        });
    }
}

pub(crate) struct Timer {
    metrics: std::sync::Arc<Metrics>,
    started: std::time::Instant,
    admission: bool,
}
impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        if self.admission {
            increment(
                &self.metrics.admission_wait_ns,
                u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            );
            return;
        }
        increment(
            &self.metrics.request_duration_ns,
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
        );
        #[cfg(feature = "metrics")]
        self.metrics.latency.observe(elapsed.as_secs_f64());
    }
}

pub(crate) fn increment(counter: &AtomicU64, count: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(count))
    });
}

#[cfg(feature = "metrics")]
impl Metrics {
    pub fn register(
        self: &std::sync::Arc<Self>,
        registry: &mut prometheus_client::registry::Registry,
    ) {
        registry.register_collector(Box::new(MetricsCollector(self.clone())));
    }
}

#[cfg(feature = "metrics")]
#[derive(Debug)]
struct MetricsCollector(std::sync::Arc<Metrics>);

#[cfg(feature = "metrics")]
impl prometheus_client::collector::Collector for MetricsCollector {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::DescriptorEncoder,
    ) -> std::result::Result<(), std::fmt::Error> {
        use prometheus_client::metrics::{MetricType, counter::ConstCounter};
        let value = self.0.snapshot();
        for (name, help, count) in [
            ("requests", "SDK RPC attempts.", value.requests),
            ("retries", "SDK transport retries.", value.retries),
            (
                "unknown",
                "SDK operations with an unknown result.",
                value.unknown,
            ),
            (
                "deliveries",
                "Deliveries admitted to bounded SDK queues.",
                value.deliveries,
            ),
            (
                "acknowledged",
                "Delivery tokens durably acknowledged.",
                value.acknowledged,
            ),
        ] {
            let metric = encoder.encode_descriptor(name, help, None, MetricType::Counter)?;
            prometheus_client::encoding::EncodeMetric::encode(&ConstCounter::new(count), metric)?;
        }
        use prometheus_client::encoding::EncodeMetric;
        self.0.latency.encode(encoder.encode_descriptor("operation_duration_seconds", "SDK API operation duration including retries and local queueing; excludes user handler time", None, self.0.latency.metric_type())?)?;
        use prometheus_client::metrics::gauge::ConstGauge;
        for (name, help, value) in [
            (
                "admitted_bytes",
                "Charged SDK payload and metadata bytes held by pending batches or delivery clones",
                value.admitted_bytes,
            ),
            (
                "retained_batches",
                "Batch objects retained by the SDK or its host",
                value.pending_batches,
            ),
            (
                "active_deliveries",
                "Delivery objects retained by the SDK or its host",
                value.active_deliveries,
            ),
        ] {
            let gauge = ConstGauge::new(i64::try_from(value).unwrap_or(i64::MAX));
            gauge.encode(encoder.encode_descriptor(name, help, None, gauge.metric_type())?)?;
        }
        let wait = ConstCounter::new(value.admission_wait_ns as f64 / 1_000_000_000.0);
        wait.encode(encoder.encode_descriptor(
            "admission_wait_seconds",
            "Time waiting for SDK producer and consumer admission",
            None,
            wait.metric_type(),
        )?)?;
        Ok(())
    }
}
