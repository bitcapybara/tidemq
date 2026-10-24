use std::future::Future;
use tracing::Instrument;

use crate::{Error, Result};

tokio::task_local! { static TRACEPARENT: String; }

pub(crate) fn inherit_trace<T>(operation: impl Future<Output = T>) -> impl Future<Output = T> {
    let explicit = TRACEPARENT.try_with(Clone::clone).unwrap_or_default();
    TRACEPARENT.scope(explicit, operation.instrument(tracing::Span::current()))
}

/// Propagates one validated host trace context through a finite SDK operation.
/// Spawned host tasks must scope their own context; no connection retains it.
pub async fn scope_traceparent<T>(
    traceparent: &str,
    operation: impl Future<Output = Result<T>>,
) -> Result<T> {
    if !tidemq_protocol::framing::valid_traceparent(traceparent) {
        return Err(Error::Configuration {
            reason: "traceparent must be a valid bounded W3C context",
        });
    }
    let mut context = String::new();
    context
        .try_reserve_exact(traceparent.len())
        .map_err(|_| Error::Admission {
            resource: "traceparent buffer allocation",
        })?;
    context.push_str(traceparent);
    TRACEPARENT.scope(context, operation).await
}

pub(crate) fn current_traceparent() -> Result<String> {
    let explicit = TRACEPARENT
        .try_with(|context| -> Result<String> {
            let mut output = String::new();
            output
                .try_reserve_exact(context.len())
                .map_err(|_| Error::Admission {
                    resource: "traceparent propagation buffer allocation",
                })?;
            output.push_str(context);
            Ok(output)
        })
        .unwrap_or_else(|_| Ok(String::new()))?;
    if !explicit.is_empty() {
        return Ok(explicit);
    }
    #[cfg(feature = "opentelemetry")]
    {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let context = tracing::Span::current().context();
        let span = context.span();
        let context = span.span_context();
        if context.is_valid() {
            return Ok(format!(
                "00-{}-{}-{:02x}",
                context.trace_id(),
                context.span_id(),
                context.trace_flags().to_u8()
            ));
        }
    }
    Ok(String::new())
}
