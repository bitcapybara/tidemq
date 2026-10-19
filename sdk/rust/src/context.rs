use std::future::Future;

use crate::{Error, Result};

tokio::task_local! { static TRACEPARENT: String; }

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
        .map_err(|_| Error::Admission)?;
    context.push_str(traceparent);
    TRACEPARENT.scope(context, operation).await
}

pub(crate) fn current_traceparent() -> Result<String> {
    TRACEPARENT
        .try_with(|context| {
            let mut output = String::new();
            output
                .try_reserve_exact(context.len())
                .map_err(|_| Error::Admission)?;
            output.push_str(context);
            Ok(output)
        })
        .unwrap_or_else(|_| Ok(String::new()))
}
