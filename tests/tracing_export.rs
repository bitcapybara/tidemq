use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use opentelemetry::Context;
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use tidemq::observability::tracing::{Tracing, TracingConfig};
use tokio::time::Instant;
use tracing::Dispatch;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;

#[path = "trace_support/native.rs"]
mod native;
#[path = "trace_support/tls.rs"]
mod tls;
mod trace_support;
use trace_support::{CHUNKED_LARGE, Collector, FAIL, HEALTHY, LARGE, SLOW, wait};

fn config(collector: &Collector) -> TracingConfig {
    TracingConfig {
        enabled: true,
        endpoint: collector.endpoint(),
        sample_per_mille: 1000,
        spans_per_second: 1024,
        queue_spans: 16,
        batch_spans: 4,
        export_interval_ms: 100,
        export_timeout_ms: 100,
    }
}

fn parent() -> Context {
    Context::new().with_remote_span_context(SpanContext::new(
        TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
        SpanId::from_hex("00f067aa0ba902b7").unwrap(),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    ))
}

fn emit(dispatch: &Dispatch, count: usize) {
    tracing::dispatcher::with_default(dispatch, || {
        for _ in 0..count {
            let span = tracing::info_span!(target: "tidemq_trace", "tidemq.sdk.request", kind = "GET_TOPIC", class = "CONTROL");
            span.set_parent(parent()).unwrap();
            let _guard = span.enter();
            tracing::error!(
                body = "secret-business-body",
                token = "secret-token",
                "This event must not be exported"
            );
        }
    });
}

struct HostLayer(Arc<AtomicU64>);
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for HostLayer {
    fn on_new_span(
        &self,
        _attributes: &tracing::span::Attributes<'_>,
        _id: &tracing::Id,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn configuration_rejects_unsafe_endpoints_and_unbounded_budgets() {
    assert!(!TracingConfig::default().enabled);
    for endpoint in [
        "http://collector.example/v1/traces",
        "https://collector.example/v1/traces",
        "http://localhost:4318/v1/traces",
        "https://user:secret@collector.example/v1/traces",
        "https://collector.example/v1/traces?token=secret",
        "https://collector.example/other",
        "file:///v1/traces",
    ] {
        let config = TracingConfig {
            endpoint: endpoint.into(),
            ..TracingConfig::default()
        };
        assert!(config.validate().is_err(), "Unsafe endpoint was accepted");
    }
    for endpoint in [
        "http://127.0.0.1:4318/v1/traces",
        "http://[::1]:4318/v1/traces",
        "https://192.0.2.1/v1/traces",
    ] {
        assert!(
            TracingConfig {
                endpoint: endpoint.into(),
                ..TracingConfig::default()
            }
            .validate()
            .is_ok()
        );
    }
    assert!(
        TracingConfig {
            queue_spans: u16::MAX,
            ..TracingConfig::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        TracingConfig {
            batch_spans: 33,
            ..TracingConfig::default()
        }
        .validate()
        .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_otlp_decode_preserves_parent_and_keeps_host_subscriber() {
    let collector = Collector::new().await;
    let mut pipeline = Tracing::start(&config(&collector)).unwrap();
    let host = Arc::new(AtomicU64::new(0));
    let dispatch = Dispatch::new(
        tracing_subscriber::registry()
            .with(pipeline.layer())
            .with(HostLayer(host.clone())),
    );
    emit(&dispatch, 1);
    tracing::dispatcher::with_default(&dispatch, || {
        let _span = tracing::info_span!(target: "tidemq_trace", "tidemq.sdk.request", body = "secret-business-body");
        let _span = tracing::info_span!(target: "tidemq_trace", "tidemq.sdk.request", role = "x".repeat(128));
        let _span = tracing::info_span!(target: "untrusted_dependency", "arbitrary", password = "secret-password");
    });
    wait(|| !collector.spans().is_empty()).await;
    pipeline
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let spans = collector.spans();
    assert_eq!(spans.len(), 1);
    let span = spans.first().unwrap();
    assert_eq!(
        span.trace_id,
        TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736")
            .unwrap()
            .to_bytes()
    );
    assert_eq!(
        span.parent_span_id,
        SpanId::from_hex("00f067aa0ba902b7").unwrap().to_bytes()
    );
    assert!(span.attributes.len() <= 8);
    assert!(span.events.is_empty() && span.links.is_empty() && span.trace_state.is_empty());
    assert!(host.load(Ordering::Relaxed) >= 4);
    assert_eq!(pipeline.health.snapshot().invalid_dropped, 1);
    for request in collector.requests.lock().unwrap().iter() {
        assert!(request.bytes.len() <= 65_536);
        assert!(!request.bytes.windows(6).any(|bytes| bytes == b"secret"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_failed_large_and_restarted_collector_keep_queue_and_rate_bounded() {
    let mut collector = Collector::new().await;
    collector.set(SLOW);
    let mut config = config(&collector);
    config.queue_spans = 4;
    config.batch_spans = 1;
    let mut pipeline = Tracing::start(&config).unwrap();
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(pipeline.layer()));
    emit(&dispatch, 100);
    let health = pipeline.health.snapshot();
    assert!(health.pending_spans <= 4 && health.pending_bytes <= 4 * 4096);
    assert!(health.budget_dropped > 0);
    wait(|| pipeline.health.snapshot().export_timeouts > 0).await;
    wait(|| pipeline.health.snapshot().pending_spans == 0).await;
    collector.set(LARGE);
    emit(&dispatch, 1);
    wait(|| pipeline.health.snapshot().response_rejected > 0).await;
    collector.set(CHUNKED_LARGE);
    let rejected = pipeline.health.snapshot().response_rejected;
    emit(&dispatch, 1);
    wait(|| pipeline.health.snapshot().response_rejected > rejected).await;
    collector.set(FAIL);
    let failures = pipeline.health.snapshot().export_failures;
    emit(&dispatch, 1);
    wait(|| pipeline.health.snapshot().export_failures > failures).await;
    let address = collector.address;
    collector.stop().await;
    let failures = pipeline.health.snapshot().export_failures;
    emit(&dispatch, 1);
    wait(|| pipeline.health.snapshot().export_failures > failures).await;
    let recovered = Collector::bind(address).await;
    recovered.set(HEALTHY);
    emit(&dispatch, 4);
    wait(|| recovered.spans().len() == 4).await;
    pipeline
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    recovered.assert_rate(Duration::from_millis(90));
    assert_eq!(pipeline.health.snapshot().pending_bytes, 0);
    assert!(pipeline.health.snapshot().discarded_spans > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_sampled_flag_cannot_bypass_local_span_rate() {
    let collector = Collector::new().await;
    let mut config = config(&collector);
    config.spans_per_second = 2;
    let mut pipeline = Tracing::start(&config).unwrap();
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(pipeline.layer()));
    emit(&dispatch, 64);
    wait(|| pipeline.health.snapshot().pending_spans == 0 && !collector.spans().is_empty()).await;
    pipeline
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(collector.spans().len(), 2);
    assert_eq!(pipeline.health.snapshot().sampled_out, 62);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_shutdown_deadline_cancels_export_and_releases_all_charges() {
    let collector = Collector::new().await;
    collector.set(SLOW);
    let mut config = config(&collector);
    config.export_timeout_ms = 1000;
    let mut pipeline = Tracing::start(&config).unwrap();
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(pipeline.layer()));
    emit(&dispatch, 4);
    wait(|| !collector.requests.lock().unwrap().is_empty()).await;
    let started = Instant::now();
    assert!(matches!(
        pipeline.shutdown(started + Duration::from_millis(20)).await,
        Err(tidemq::Error::TraceExport { .. })
    ));
    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(pipeline.health.snapshot().shutdown_timeouts, 1);
    assert_eq!(pipeline.health.snapshot().pending_spans, 0);
    assert_eq!(pipeline.health.snapshot().pending_bytes, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_shutdown_does_not_wait_for_the_export_interval() {
    let collector = Collector::new().await;
    let mut config = config(&collector);
    config.export_interval_ms = 5000;
    let mut pipeline = Tracing::start(&config).unwrap();
    let started = Instant::now();
    pipeline
        .shutdown(started + Duration::from_millis(100))
        .await
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(250));
    assert!(collector.requests.lock().unwrap().is_empty());
    assert_eq!(pipeline.health.snapshot().shutdown_timeouts, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_rejects_an_untrusted_ip_certificate_without_sending_spans() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = tls::untrusted_acceptor();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        assert!(
            acceptor.accept(stream).await.is_err(),
            "An untrusted certificate completed its server handshake"
        );
    });
    let config = TracingConfig {
        enabled: true,
        endpoint: format!("https://{address}/v1/traces"),
        sample_per_mille: 1000,
        export_interval_ms: 100,
        ..TracingConfig::default()
    };
    let mut pipeline = Tracing::start(&config).unwrap();
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(pipeline.layer()));
    emit(&dispatch, 1);
    wait(|| pipeline.health.snapshot().export_failures > 0).await;
    pipeline
        .shutdown(Instant::now() + Duration::from_millis(200))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), peer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pipeline.health.snapshot().discarded_spans, 1);
    assert_eq!(pipeline.health.snapshot().pending_bytes, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_tls_handshake_obeys_the_owned_export_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    });
    let config = TracingConfig {
        enabled: true,
        endpoint: format!("https://{address}/v1/traces"),
        sample_per_mille: 1000,
        export_interval_ms: 100,
        export_timeout_ms: 50,
        ..TracingConfig::default()
    };
    let mut pipeline = Tracing::start(&config).unwrap();
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(pipeline.layer()));
    emit(&dispatch, 1);
    wait(|| pipeline.health.snapshot().export_timeouts > 0).await;
    pipeline
        .shutdown(Instant::now() + Duration::from_millis(200))
        .await
        .unwrap();
    peer.abort();
    let _ = peer.await;
    assert_eq!(pipeline.health.snapshot().pending_bytes, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Requires the verified official Collector core 0.162.0 binary in TIDEMQ_OTELCOL_BIN"]
async fn official_collector_accepts_real_otlp_and_releases_ports() {
    let mut collector = native::Native::start().await;
    let config = TracingConfig {
        enabled: true,
        endpoint: format!("http://{}/v1/traces", collector.receiver),
        sample_per_mille: 1000,
        export_interval_ms: 100,
        ..TracingConfig::default()
    };
    let mut pipeline = Tracing::start(&config).unwrap();
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(pipeline.layer()));
    emit(&dispatch, 1);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let metrics = collector.scrape().await.unwrap();
        if metrics.lines().any(|line| {
            line.starts_with("otelcol_receiver_accepted_spans")
                && line
                    .rsplit_once(' ')
                    .is_some_and(|(_, value)| value.parse::<f64>().is_ok_and(|value| value >= 1.0))
        }) {
            for line in metrics.lines().filter(|line| {
                line.starts_with("# TYPE otelcol_receiver_")
                    || line.starts_with("# TYPE otelcol_process_")
            }) {
                println!("{line}");
            }
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Official Collector did not account for the exported span"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    pipeline
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(pipeline.health.snapshot().export_failures, 0);
    assert!(pipeline.health.snapshot().export_bytes > 0);
    collector.close().await;
}
