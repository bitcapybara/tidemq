use super::*;

use opentelemetry::{
    Context,
    trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState},
};
use tidemq::observability::tracing::{Tracing, TracingConfig};
use tidemq_protocol::common::FrameKind;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;

#[path = "../trace_support/mod.rs"]
mod collector;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdk_gateway_broker_trace_and_durable_ack_survive_collector_faults() {
    let mut collector = collector::Collector::new().await;
    let config = TracingConfig {
        enabled: true,
        endpoint: collector.endpoint(),
        sample_per_mille: 1000,
        spans_per_second: 1024,
        queue_spans: 64,
        batch_spans: 32,
        export_interval_ms: 100,
        export_timeout_ms: 100,
    };
    let mut pipeline = Tracing::start(&config).unwrap();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(pipeline.layer()))
        .unwrap();
    let mut fixture = Fixture::new(
        &[
            (1, Role::Metadata),
            (2, Role::Storage),
            (3, Role::Broker),
            (4, Role::Gateway),
        ],
        &[1],
        &[2],
    )
    .await;
    for node in fixture.configs.values_mut() {
        node.tracing = config.clone();
    }
    let mut processes = Vec::new();
    for node in [1, 2, 3, 4] {
        processes.push((node, spawn_process(&fixture, node).await));
    }
    for (node, child) in &mut processes {
        wait_process(&fixture, *node, child).await;
    }
    let client = fixture.client(4).await;
    create_logical_resources(&client).await;
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "traced")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let trace = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
    let parent = Context::new().with_remote_span_context(SpanContext::new(
        trace,
        SpanId::from_hex("00f067aa0ba902b7").unwrap(),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    ));
    let host = tracing::info_span!(target: "tidemq_trace", "tidemq.sdk.request", kind = "HOST_OPERATION", class = "PRODUCE");
    host.set_parent(parent).unwrap();
    roundtrip(&producer, &consumer, 0).instrument(host).await;
    collector::wait_for(
        "SDK, gateway and broker trace linkage",
        || {
            let spans = collector.spans();
            // Independent exporters can deliver a child before its SDK parent.
            spans.iter().any(|gateway| {
                gateway.trace_id == trace.to_bytes()
                    && role(gateway, "gateway")
                    && kind(gateway, tidemq_protocol::common::FrameKind::Publish)
                    && spans.iter().any(|sdk| {
                        sdk.trace_id == trace.to_bytes()
                            && sdk.name == "tidemq.sdk.request"
                            && sdk.span_id == gateway.parent_span_id
                    })
                    && spans.iter().any(|broker| {
                        broker.trace_id == trace.to_bytes()
                            && role(broker, "broker")
                            && broker.parent_span_id == gateway.span_id
                    })
            }) && spans
                .iter()
                .any(|span| span.trace_id == trace.to_bytes() && span.name == "producer_submission")
        },
        || trace_diagnostics(&collector, &pipeline, trace),
    )
    .await;
    let spans = collector.spans();
    let trace_spans: Vec<_> = spans
        .iter()
        .filter(|span| span.trace_id == trace.to_bytes())
        .collect();
    let gateway = trace_spans
        .iter()
        .find(|span| {
            role(span, "gateway") && kind(span, tidemq_protocol::common::FrameKind::Publish)
        })
        .unwrap();
    let sdk = trace_spans
        .iter()
        .find(|span| span.span_id == gateway.parent_span_id)
        .unwrap();
    assert_eq!(sdk.name, "tidemq.sdk.request");
    assert!(
        trace_spans
            .iter()
            .any(|span| span.name == "tidemq.sdk.send")
    );
    assert!(
        trace_spans
            .iter()
            .any(|span| role(span, "broker") && span.parent_span_id == gateway.span_id)
    );
    assert!(
        trace_spans.iter().all(|span| span.events.is_empty()
            && span.links.is_empty()
            && span.attributes.len() <= 8)
    );
    let explicit_trace = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4737").unwrap();
    let explicit_parent = SpanId::from_hex("00f067aa0ba902b8").unwrap();
    tidemq_sdk::scope_traceparent(
        "00-4bf92f3577b34da6a3ce929d0e0e4737-00f067aa0ba902b8-01",
        async {
            roundtrip(&producer, &consumer, 8).await;
            Ok(())
        },
    )
    .await
    .unwrap();
    collector::wait_for(
        "Explicit trace parent propagation",
        || {
            let spans = collector.spans();
            [FrameKind::Publish, FrameKind::GroupAck]
                .iter()
                .all(|expected| {
                    spans.iter().any(|span| {
                        span.trace_id == explicit_trace.to_bytes()
                            && span.parent_span_id == explicit_parent.to_bytes()
                            && role(span, "gateway")
                            && kind(span, *expected)
                    })
                })
        },
        || trace_diagnostics(&collector, &pipeline, explicit_trace),
    )
    .await;
    for mode in [collector::SLOW, collector::FAIL, collector::LARGE] {
        let before = pipeline.health.snapshot();
        collector.set(mode);
        let offset = u64::from(mode);
        roundtrip(&producer, &consumer, offset).await;
        collector::wait(|| {
            let after = pipeline.health.snapshot();
            let recorded = match mode {
                collector::SLOW => after.export_timeouts > before.export_timeouts,
                collector::FAIL => after.export_failures > before.export_failures,
                collector::LARGE => after.response_rejected > before.response_rejected,
                _ => false,
            };
            // Wait for this fault's diagnostic instead of an earlier empty queue observation.
            after.pending_spans == 0 && recorded
        })
        .await;
    }
    assert!(pipeline.health.snapshot().export_timeouts > 0);
    assert!(pipeline.health.snapshot().export_failures > 0);
    assert!(pipeline.health.snapshot().response_rejected > 0);
    let address = collector.address;
    collector.stop().await;
    roundtrip(&producer, &consumer, 4).await;
    let recovered = collector::Collector::bind(address).await;
    for offset in 5..8 {
        roundtrip(&producer, &consumer, offset).await;
    }
    collector::wait(|| !recovered.spans().is_empty()).await;
    recovered.assert_rate(Duration::from_millis(90));
    let health = pipeline.health.snapshot();
    assert!(health.pending_spans <= 64 && health.pending_bytes <= 64 * 4096);
    let management = fixture.configs.get(&4).unwrap().management_listen;
    collector::wait(|| pipeline.health.snapshot().pending_spans == 0).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let metrics = http(management, "/metrics").await;
    assert!(metrics.contains("tidemq_runtime_trace_export_failures_total"));
    assert!(metrics.contains("tidemq_runtime_trace_pending_bytes"));
    assert_eq!(metrics.matches("# EOF").count(), 1);
    for request in collector
        .requests
        .lock()
        .unwrap()
        .iter()
        .chain(recovered.requests.lock().unwrap().iter())
    {
        assert!(request.bytes.len() <= 65_536);
        assert!(
            !request
                .bytes
                .windows(12)
                .any(|value| value == b"trace-secret")
        );
    }
    assert_eq!(client.metrics().snapshot().acknowledged, 9);
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    for (node, child) in processes.iter_mut().rev() {
        let pid = i32::try_from(child.id().unwrap()).unwrap();
        // The owned child PID is live and SIGINT uses the CLI's normal bounded drain path.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGINT) }, 0);
        let status = tokio::time::timeout(Duration::from_secs(6), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(
            status.success(),
            "Traced runtime node {node} failed to drain: {}",
            tokio::fs::read_to_string(fixture.directory.join(format!("process-{node}.log")))
                .await
                .unwrap()
        );
    }
    let shutdown_deadline = Instant::now() + Duration::from_secs(5);
    pipeline.shutdown(shutdown_deadline).await.unwrap();
    assert_eq!(pipeline.health.snapshot().pending_bytes, 0);
}

async fn roundtrip(
    producer: &tidemq_sdk::Producer,
    consumer: &tidemq_sdk::QueueConsumer,
    sequence: u64,
) {
    let started = Instant::now();
    let payload = format!("trace-secret-body-{sequence}");
    let receipt = producer
        .send(tidemq_sdk::Message::new(payload.clone()).key("trace-secret-key"))
        .await
        .unwrap();
    let delivery = consumer.receive().await.unwrap();
    assert_eq!(delivery.message_id(), receipt.message_id());
    assert_eq!(delivery.payload(), payload.as_bytes());
    assert_eq!(delivery.key(), Some(b"trace-secret-key".as_slice()));
    assert_eq!(
        consumer.ack(&delivery).await.unwrap(),
        tidemq_sdk::AckReceipt::Acknowledged
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "Collector fault delayed a durable business roundtrip beyond its local test budget"
    );
}

fn trace_diagnostics(
    collector: &collector::Collector,
    pipeline: &Tracing,
    trace: TraceId,
) -> String {
    let spans: Vec<_> = collector
        .spans()
        .into_iter()
        .filter(|span| {
            span.trace_id == trace.to_bytes()
                || role(span, "gateway") && kind(span, FrameKind::GroupAck)
        })
        .map(|span| {
            (
                span.name,
                span.trace_id,
                span.span_id,
                span.parent_span_id,
                span.attributes,
            )
        })
        .collect();
    format!(
        "SDK exporter health: {:?}; observed trace spans: {spans:?}",
        pipeline.health.snapshot()
    )
}

fn role(span: &opentelemetry_proto::tonic::trace::v1::Span, expected: &str) -> bool {
    span.attributes.iter().any(|attribute| attribute.key == "role" && matches!(attribute.value.as_ref().and_then(|value| value.value.as_ref()), Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(value)) if value == expected))
}

fn kind(
    span: &opentelemetry_proto::tonic::trace::v1::Span,
    expected: tidemq_protocol::common::FrameKind,
) -> bool {
    span.attributes.iter().any(|attribute| attribute.key == "frame_kind" && matches!(attribute.value.as_ref().and_then(|value| value.value.as_ref()), Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(value)) if *value == i64::from(i32::from(expected))))
}
