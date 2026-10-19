use std::{
    sync::{
        Arc,
        atomic::{AtomicU16, AtomicU64, Ordering},
    },
    time::Instant,
};

use aws_sdk_s3::{
    config::{
        ConfigBag, Intercept, RuntimeComponents,
        interceptors::{
            BeforeDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextRef,
        },
    },
    error::BoxError,
};
use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeMetric},
    metrics::{
        counter::{ConstCounter, Counter},
        histogram::Histogram,
    },
};

#[derive(Debug)]
pub struct S3Metrics {
    attempts: Counter,
    retries: Counter,
    http_errors: Counter,
    pub(super) rejected: Counter,
    pub(crate) cache_hits: Counter,
    pub(crate) cache_misses: Counter,
    pub(super) admission: Histogram,
    pub(super) headers: Histogram,
    pub(super) body: Histogram,
    pub(super) complete: Histogram,
    pub(crate) decode_queue: Histogram,
    pub(crate) decode: Histogram,
}

impl Default for S3Metrics {
    fn default() -> Self {
        fn histogram() -> Histogram {
            Histogram::new([0.001, 0.005, 0.02, 0.1, 0.5, 1.0, 5.0, 30.0])
        }
        Self {
            attempts: Counter::default(),
            retries: Counter::default(),
            http_errors: Counter::default(),
            rejected: Counter::default(),
            cache_hits: Counter::default(),
            cache_misses: Counter::default(),
            admission: histogram(),
            headers: histogram(),
            body: histogram(),
            complete: histogram(),
            decode_queue: histogram(),
            decode: histogram(),
        }
    }
}

impl Collector for S3Metrics {
    fn encode(&self, mut encoder: DescriptorEncoder) -> std::result::Result<(), std::fmt::Error> {
        for (name, help, value) in [
            ("sdk_attempts", "SDK HTTP attempts", &self.attempts),
            (
                "sdk_retries",
                "SDK retry attempts after the initial request",
                &self.retries,
            ),
            (
                "http_errors",
                "HTTP responses with error status",
                &self.http_errors,
            ),
            (
                "admission_rejected",
                "Operations rejected by bounded admission",
                &self.rejected,
            ),
            (
                "cache_hits",
                "Validated cold range cache hits",
                &self.cache_hits,
            ),
            (
                "cache_misses",
                "Cold range cache misses",
                &self.cache_misses,
            ),
        ] {
            let counter = ConstCounter::new(value.get());
            counter.encode(encoder.encode_descriptor(
                name,
                help,
                None,
                counter.metric_type(),
            )?)?;
        }
        for (name, help, histogram) in [
            (
                "admission_seconds",
                "Admission and rate pacing duration",
                &self.admission,
            ),
            (
                "headers_seconds",
                "Range request duration until headers including SDK retries",
                &self.headers,
            ),
            (
                "body_seconds",
                "Range response body read duration",
                &self.body,
            ),
            (
                "operation_seconds",
                "Admitted complete operation duration including body or multipart cleanup",
                &self.complete,
            ),
            (
                "decode_queue_seconds",
                "Cold decode worker queue duration",
                &self.decode_queue,
            ),
            (
                "decode_seconds",
                "Cold range checksum and record decoding duration",
                &self.decode,
            ),
        ] {
            histogram.encode(encoder.encode_descriptor(
                name,
                help,
                None,
                histogram.metric_type(),
            )?)?;
        }
        Ok(())
    }
}

pub(super) struct OperationTimer {
    metrics: Arc<S3Metrics>,
    started: Instant,
}

pub(super) struct StageTimer<'a> {
    histogram: &'a Histogram,
    started: Instant,
}
impl<'a> StageTimer<'a> {
    pub(super) fn new(histogram: &'a Histogram) -> Self {
        Self {
            histogram,
            started: Instant::now(),
        }
    }
}
impl Drop for StageTimer<'_> {
    fn drop(&mut self) {
        self.histogram.observe(self.started.elapsed().as_secs_f64());
    }
}
impl OperationTimer {
    pub(super) fn new(metrics: Arc<S3Metrics>) -> Self {
        Self {
            metrics,
            started: Instant::now(),
        }
    }
}
impl Drop for OperationTimer {
    fn drop(&mut self) {
        self.metrics
            .complete
            .observe(self.started.elapsed().as_secs_f64());
    }
}

#[derive(Debug)]
pub(super) struct Observer {
    metrics: Arc<S3Metrics>,
    attempts: AtomicU64,
    status: Option<Arc<AtomicU16>>,
}

impl Observer {
    pub(super) fn new(metrics: Arc<S3Metrics>) -> Self {
        Self {
            metrics,
            attempts: AtomicU64::new(0),
            status: None,
        }
    }
    pub(super) fn status(metrics: Arc<S3Metrics>, status: Arc<AtomicU16>) -> Self {
        Self {
            metrics,
            attempts: AtomicU64::new(0),
            status: Some(status),
        }
    }
}

impl Intercept for Observer {
    fn name(&self) -> &'static str {
        "TideMqS3Observer"
    }
    fn read_before_attempt(
        &self,
        _: &BeforeTransmitInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> std::result::Result<(), BoxError> {
        self.metrics.attempts.inc();
        if self.attempts.fetch_add(1, Ordering::Relaxed) > 0 {
            self.metrics.retries.inc();
        }
        Ok(())
    }
    fn read_before_deserialization(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> std::result::Result<(), BoxError> {
        let status = context.response().status().as_u16();
        if status >= 400 {
            self.metrics.http_errors.inc();
        }
        if let Some(value) = &self.status {
            value.store(status, Ordering::Release);
        }
        Ok(())
    }
}
