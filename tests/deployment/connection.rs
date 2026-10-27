use std::{future::Future, time::Duration};

use tidemq_sdk::{Error, ErrorKind, Result};
use tokio::time::Instant;

pub(super) async fn connect_until<T, F, Attempt>(deadline: Instant, mut attempt: F) -> Result<T>
where
    F: FnMut(Instant) -> Attempt,
    Attempt: Future<Output = Result<T>>,
{
    loop {
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        let error = match tokio::time::timeout_at(deadline, attempt(deadline))
            .await
            .map_err(|_| Error::Deadline)?
        {
            Ok(client) => return Ok(client),
            Err(error) => error,
        };
        if !retryable_setup(&error) || Instant::now() >= deadline {
            return Err(error);
        }
        // Client construction precedes every business operation, so retries cannot duplicate a write.
        eprintln!(
            "Retrying SDK connection setup before business admission: kind={:?}",
            error.kind()
        );
        let retry_at = Instant::now()
            .checked_add(Duration::from_millis(200))
            .ok_or(Error::Deadline)?;
        tokio::time::sleep_until(retry_at.min(deadline)).await;
        if retry_at >= deadline {
            return Err(error);
        }
    }
}

fn retryable_setup(error: &Error) -> bool {
    if !matches!(
        error.kind(),
        ErrorKind::ResourceExhausted | ErrorKind::Unavailable
    ) {
        return false;
    }
    let mut source = error;
    for _ in 0..16 {
        match source {
            Error::Remote { retryable, .. } => return *retryable,
            Error::Background { source: inner } => source = inner.as_ref(),
            Error::Query { source: inner } | Error::RequestUnknown { source: inner } => {
                source = inner.as_ref();
            }
            _ => return true,
        }
    }
    false
}
