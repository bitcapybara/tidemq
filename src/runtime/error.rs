use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tidemq_protocol::common::{ErrorCode, Outcome};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid runtime input: {reason}"))]
    Invalid { reason: &'static str },
    #[snafu(display("Runtime operation failed: {operation}"))]
    Operation { operation: &'static str, source: std::io::Error },
    #[snafu(display("Runtime operation was rejected: {failure:?}"))]
    Rejected { failure: Failure },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Failure {
    pub code: i32,
    pub outcome: i32,
    pub retryable: bool,
    pub expected_sequence: Option<u64>,
    pub expected_registration_epoch: Option<u64>,
}

impl Error {
    pub fn invalid(reason: &'static str) -> Self { Self::Invalid { reason } }
    pub fn operation(operation: &'static str, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Operation { operation, source: std::io::Error::other(source) }
    }
    pub fn foundation(source: crate::Error) -> Self { Self::operation("foundation service", source) }
    pub fn reject(code: ErrorCode, outcome: Outcome, retryable: bool) -> Self {
        Self::Rejected { failure: Failure::new(code, outcome, retryable) }
    }
    pub fn unavailable() -> Self { Self::reject(ErrorCode::Unavailable, Outcome::Rejected, true) }
    pub fn unknown() -> Self { Self::reject(ErrorCode::Unavailable, Outcome::Unknown, true) }
    pub fn failure(&self) -> Failure {
        match self {
            Self::Rejected { failure } => failure.clone(),
            Self::Invalid { .. } => Failure::new(ErrorCode::InvalidRequest, Outcome::Rejected, false),
            Self::Operation { .. } => Failure::new(ErrorCode::Unavailable, Outcome::Unknown, true),
        }
    }
}

impl Failure {
    fn new(code: ErrorCode, outcome: Outcome, retryable: bool) -> Self {
        Self { code: code.into(), outcome: outcome.into(), retryable, expected_sequence: None, expected_registration_epoch: None }
    }
}
