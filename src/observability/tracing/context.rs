use opentelemetry::Context;
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Returns only the bounded W3C parent; baggage and tracestate never cross a TideMQ hop.
pub fn current_traceparent() -> String {
    let context = tracing::Span::current().context();
    let span = context.span();
    let context = span.span_context();
    if context.is_valid() {
        format!(
            "00-{}-{}-{:02x}",
            context.trace_id(),
            context.span_id(),
            context.trace_flags().to_u8()
        )
    } else {
        String::new()
    }
}

pub fn request_span(
    frame: &tidemq_protocol::common::Frame,
    node_id: u64,
    role: &'static str,
) -> tracing::Span {
    let span = tracing::info_span!(target: "tidemq_trace", "tidemq.server.request", node_id, role, frame_kind = frame.kind, traffic_class = frame.traffic_class);
    set_parent(&span, &frame.traceparent, true);
    span
}

pub(crate) fn set_parent(span: &tracing::Span, traceparent: &str, is_remote: bool) {
    let _ = span.set_parent(remote_parent(traceparent, is_remote).unwrap_or_default());
}

/// Ending an observation never cancels an owned business future after its handoff.
pub(crate) async fn observe_until<T>(
    future: impl std::future::Future<Output = T>,
    span: tracing::Span,
    deadline: std::time::Instant,
) -> T {
    if span.is_disabled() {
        return future.await;
    }
    tokio::pin!(future);
    let result = tokio::time::timeout_at(deadline.into(), future.as_mut().instrument(span)).await;
    match result {
        Ok(result) => result,
        Err(_) => future.await,
    }
}

fn remote_parent(value: &str, is_remote: bool) -> Option<Context> {
    if !tidemq_protocol::framing::valid_traceparent(value) {
        return None;
    }
    let trace = TraceId::from_hex(value.get(3..35)?).ok()?;
    let span = SpanId::from_hex(value.get(36..52)?).ok()?;
    let flags = u8::from_str_radix(value.get(53..55)?, 16).ok()?;
    Some(Context::new().with_remote_span_context(SpanContext::new(
        trace,
        span,
        TraceFlags::new(flags),
        is_remote,
        TraceState::default(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing_subscriber::layer::SubscriberExt;

    struct Close(Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Close {
        fn on_close(&self, _id: tracing::Id, _context: tracing_subscriber::layer::Context<'_, S>) {
            if let Some(sender) = self.0.lock().unwrap().take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn observation_expiry_keeps_the_owned_business_future_alive() {
        let (closed, close) = tokio::sync::oneshot::channel();
        let subscriber =
            tracing_subscriber::registry().with(Close(Arc::new(Mutex::new(Some(closed)))));
        let span = tracing::subscriber::with_default(subscriber, || {
            tracing::info_span!("bounded_observation")
        });
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(observe_until(
            receiver,
            span,
            std::time::Instant::now() + Duration::from_millis(10),
        ));
        tokio::time::timeout(Duration::from_secs(1), close)
            .await
            .unwrap()
            .unwrap();
        assert!(!task.is_finished());
        sender.send(42).unwrap();
        assert_eq!(task.await.unwrap().unwrap(), 42);
    }
}
