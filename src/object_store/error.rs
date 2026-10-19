use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("invalid object storage configuration: {reason}"))]
    Configuration { reason: &'static str },
    #[snafu(display("object storage admission is exhausted"))]
    Overloaded,
    #[snafu(display("object storage {operation} timed out; a write may have completed"))]
    Timeout { operation: &'static str },
    #[snafu(display("object storage write outcome is unknown; reconcile the immutable object"))]
    Unknown,
    #[snafu(display("immutable object content conflicts with the registered descriptor"))]
    Conflict,
    #[snafu(display("object does not exist"))]
    Missing,
    #[snafu(display("object range is outside the descriptor"))]
    Range,
    #[snafu(display("object integrity validation failed: {reason}"))]
    Corrupt { reason: &'static str },
    #[snafu(display("object storage {operation} failed"))]
    Sdk {
        operation: &'static str,
        source: aws_sdk_s3::error::BoxError,
    },
    #[snafu(display("object storage worker failed"))]
    Worker { source: tokio::task::JoinError },
    #[snafu(display("object storage upload was cancelled"))]
    Cancelled,
    #[snafu(display("object storage allocation failed"))]
    Allocation {
        source: std::collections::TryReserveError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

pub(super) fn sdk<E>(operation: &'static str, error: aws_sdk_s3::error::SdkError<E>) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    if matches!(&error, aws_sdk_s3::error::SdkError::TimeoutError(_)) {
        return Error::Timeout { operation };
    }
    match error
        .raw_response()
        .map(|response| response.status().as_u16())
    {
        Some(404) => Error::Missing,
        Some(412 | 409) => Error::Conflict,
        Some(416) => Error::Range,
        _ => Error::Sdk {
            operation,
            source: Box::new(error),
        },
    }
}
