use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use opentelemetry::trace::{Link, SpanContext, SpanKind, Status, TraceId, TraceState};
use opentelemetry::{Context, KeyValue, Value};
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{
    SamplingDecision, SamplingResult, ShouldSample, Span, SpanData, SpanExporter, SpanProcessor,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::{SPAN_CHARGE, TraceHealth, TracingConfig};
use crate::Result;

pub(super) fn allowed_name(name: &str) -> bool {
    matches!(
        name,
        "tidemq.server.request"
            | "console_http_request"
            | "tidemq.sdk.request"
            | "tidemq.sdk.produce"
            | "tidemq.sdk.send"
            | "tidemq.sdk.send_batch"
            | "producer_submission"
    )
}

pub(super) fn allowed_field(name: &str) -> bool {
    matches!(
        name,
        "node_id"
            | "role"
            | "frame_kind"
            | "traffic_class"
            | "kind"
            | "class"
            | "producer_epoch"
            | "attempt"
            | "operation_id"
            | "request_id"
            | "method"
            | "status"
    )
}

#[derive(Clone, Debug)]
pub(super) struct Sampler {
    rate: Arc<AtomicU64>,
    started: Instant,
    ratio: u16,
    limit: u16,
    health: TraceHealth,
}

impl Sampler {
    pub fn new(config: &TracingConfig, health: TraceHealth) -> Self {
        Self {
            rate: Arc::new(AtomicU64::new(0)),
            started: Instant::now(),
            ratio: config.sample_per_mille,
            limit: config.spans_per_second,
            health,
        }
    }

    fn reserve(&self, window: u64) -> bool {
        // Keep the window and count together so contention cannot discard an available sample.
        self.rate
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |packed| {
                let previous = packed >> 16;
                let window = window.max(previous);
                let count = if window == previous {
                    packed & 0xffff
                } else {
                    0
                };
                if count >= u64::from(self.limit) {
                    return None;
                }
                window
                    .checked_mul(1 << 16)
                    .map(|base| base | count.saturating_add(1))
            })
            .is_ok()
    }
}

impl ShouldSample for Sampler {
    fn should_sample(
        &self,
        _parent: Option<&Context>,
        trace: TraceId,
        name: &str,
        _kind: &SpanKind,
        _attributes: &[KeyValue],
        _links: &[Link],
    ) -> SamplingResult {
        let admitted = allowed_name(name)
            && u128::from_be_bytes(trace.to_bytes()) % 1_000 < u128::from(self.ratio)
            && self.reserve(self.started.elapsed().as_secs());
        if !admitted {
            self.health.0.sampled_out.fetch_add(1, Ordering::Relaxed);
        }
        SamplingResult {
            decision: if admitted {
                SamplingDecision::RecordAndSample
            } else {
                SamplingDecision::Drop
            },
            attributes: Vec::new(),
            trace_state: TraceState::default(),
        }
    }
}

#[derive(Debug)]
pub(super) struct Control {
    closed: AtomicBool,
    stop: watch::Sender<bool>,
}

impl Control {
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let _ = self.stop.send(true);
    }
}

#[derive(Debug)]
pub(super) struct Processor {
    queue: mpsc::Sender<Queued>,
    bytes: Arc<Semaphore>,
    control: Arc<Control>,
    health: TraceHealth,
}

#[derive(Debug)]
struct Charge {
    _permit: OwnedSemaphorePermit,
    health: TraceHealth,
    completed: bool,
}
impl Drop for Charge {
    fn drop(&mut self) {
        if !self.completed {
            self.health
                .0
                .discarded_spans
                .fetch_add(1, Ordering::Relaxed);
        }
        self.health.0.pending_spans.fetch_sub(1, Ordering::Relaxed);
        self.health
            .0
            .pending_bytes
            .fetch_sub(u64::from(SPAN_CHARGE), Ordering::Relaxed);
    }
}
#[derive(Debug)]
struct Queued {
    span: SpanData,
    charge: Charge,
}

impl SpanProcessor for Processor {
    fn on_start(&self, _span: &mut Span, _parent: &Context) {}
    // Keep the privacy gate, fixed charge and queue ownership transfer in the same callback.
    fn on_end(&self, mut span: SpanData) {
        if !allowed_name(&span.name)
            || span.attributes.len() > 8
            || !span.attributes.iter().all(valid_attribute)
        {
            self.health
                .0
                .invalid_dropped
                .fetch_add(1, Ordering::Relaxed);
            self.health
                .0
                .discarded_spans
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        // No event, link, arbitrary status text, baggage or tracestate is an exportable field.
        span.events = Default::default();
        span.links = Default::default();
        span.status = Status::Unset;
        span.span_context = SpanContext::new(
            span.span_context.trace_id(),
            span.span_context.span_id(),
            span.span_context.trace_flags(),
            span.span_context.is_remote(),
            TraceState::default(),
        );
        if self.control.closed.load(Ordering::Acquire) {
            self.health
                .0
                .discarded_spans
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Ok(permit) = self.bytes.clone().try_acquire_many_owned(SPAN_CHARGE) else {
            self.health.0.budget_dropped.fetch_add(1, Ordering::Relaxed);
            self.health
                .0
                .discarded_spans
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        self.health.0.pending_spans.fetch_add(1, Ordering::Relaxed);
        self.health
            .0
            .pending_bytes
            .fetch_add(u64::from(SPAN_CHARGE), Ordering::Relaxed);
        let queued = Queued {
            span,
            charge: Charge {
                _permit: permit,
                health: self.health.clone(),
                completed: false,
            },
        };
        if self.queue.try_send(queued).is_ok() {
            self.health.0.admitted.fetch_add(1, Ordering::Relaxed);
        } else {
            self.health.0.budget_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn force_flush(&self) -> OTelSdkResult {
        Err(OTelSdkError::InternalFailure(
            "Use the owned asynchronous trace shutdown deadline".into(),
        ))
    }
    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.control.close();
        Ok(())
    }
}

fn valid_attribute(attribute: &KeyValue) -> bool {
    allowed_field(attribute.key.as_str())
        && match &attribute.value {
            Value::String(value) => value.as_str().len() <= 64,
            Value::I64(_) | Value::Bool(_) => true,
            Value::F64(value) => value.is_finite(),
            _ => false,
        }
}

pub(super) fn start(
    config: &TracingConfig,
    health: TraceHealth,
    exporter: opentelemetry_otlp::SpanExporter,
) -> Result<(Processor, Arc<Control>, JoinHandle<()>)> {
    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|_| super::invalid("Trace export requires an active Tokio runtime"))?;
    let (queue, receiver) = mpsc::channel(usize::from(config.queue_spans));
    let (stop, stopped) = watch::channel(false);
    let control = Arc::new(Control {
        closed: AtomicBool::new(false),
        stop,
    });
    let processor = Processor {
        queue,
        bytes: Arc::new(Semaphore::new(
            usize::from(config.queue_spans) * SPAN_CHARGE as usize,
        )),
        control: control.clone(),
        health: health.clone(),
    };
    let task = runtime.spawn(worker(receiver, stopped, exporter, config.clone(), health));
    Ok((processor, control, task))
}

async fn worker(
    mut receiver: mpsc::Receiver<Queued>,
    mut stop: watch::Receiver<bool>,
    exporter: opentelemetry_otlp::SpanExporter,
    config: TracingConfig,
    health: TraceHealth,
) {
    let interval = Duration::from_millis(u64::from(config.export_interval_ms));
    let timeout = Duration::from_millis(u64::from(config.export_timeout_ms));
    let mut next = Instant::now() + interval;
    let mut batch = Vec::new();
    loop {
        if receiver.is_closed() && receiver.is_empty() {
            break;
        }
        tokio::select! {
            biased;
            _ = stop.changed(), if !receiver.is_closed() => { receiver.close(); }
            _ = tokio::time::sleep_until(next) => {}
        }
        if Instant::now() < next {
            continue;
        }
        tokio::select! {
            biased;
            _ = stop.changed(), if !receiver.is_closed() => { receiver.close(); continue; }
            count = receiver.recv_many(&mut batch, usize::from(config.batch_spans)) => { if count == 0 { break; } }
        }
        let mut spans = Vec::new();
        let mut charges = Vec::new();
        for queued in std::mem::take(&mut batch) {
            spans.push(queued.span);
            charges.push(queued.charge);
        }
        health.0.exports.fetch_add(1, Ordering::Relaxed);
        match tokio::time::timeout(timeout, exporter.export(spans)).await {
            Ok(Ok(())) => {
                for charge in &mut charges {
                    charge.completed = true;
                }
            }
            Ok(Err(_)) => {
                health.0.export_failures.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                health.0.export_timeouts.fetch_add(1, Ordering::Relaxed);
                health.0.export_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        drop(charges);
        next = Instant::now() + interval;
    }
    let _ = exporter.shutdown_with_timeout(Duration::ZERO);
}

#[cfg(test)]
mod sampling_tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn sampling_windows_reset_without_rolling_back_or_exceeding_the_limit() {
        let sampler = Sampler::new(
            &TracingConfig {
                spans_per_second: 2,
                ..TracingConfig::default()
            },
            TraceHealth::default(),
        );
        assert!(sampler.reserve(0));
        assert!(sampler.reserve(0));
        assert!(!sampler.reserve(0));
        assert!(sampler.reserve(1));
        assert!(sampler.reserve(0));
        assert!(!sampler.reserve(1));
        assert!(!sampler.reserve(u64::MAX));
        assert!(sampler.reserve(2));
    }

    #[test]
    fn concurrent_spans_below_the_rate_budget_are_all_sampled() {
        let config = TracingConfig {
            sample_per_mille: 1000,
            spans_per_second: 1024,
            ..TracingConfig::default()
        };
        let health = TraceHealth::default();
        let sampler = Sampler::new(&config, health.clone());
        let barrier = Barrier::new(17);
        let admitted = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..16)
                .map(|_| {
                    let sampler = &sampler;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        (0..32)
                            .filter(|_| {
                                sampler
                                    .should_sample(
                                        None,
                                        TraceId::from_bytes([1; 16]),
                                        "tidemq.sdk.request",
                                        &SpanKind::Internal,
                                        &[],
                                        &[],
                                    )
                                    .decision
                                    == SamplingDecision::RecordAndSample
                            })
                            .count()
                    })
                })
                .collect();
            barrier.wait();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .sum::<usize>()
        });
        assert_eq!(admitted, 512);
        assert_eq!(health.snapshot().sampled_out, 0);
    }
}
