use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tidemq_protocol::common::{ErrorCode, Outcome};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("An admitted operation is not yet fully observed"))]
    Admitted { source: Box<Error> },
    #[snafu(display("Invalid runtime input: {reason}"))]
    Invalid { reason: String },
    #[snafu(display("Runtime operation failed: {operation}"))]
    Operation {
        operation: &'static str,
        source: std::io::Error,
    },
    #[snafu(display("Local request was rejected before handler admission"))]
    LocalNotSubmitted { source: crate::Error },
    #[snafu(display("Failed to verify committed consumer writer"))]
    CommittedConsumerWriter {
        source: crate::ownership::LeaseError,
    },
    #[snafu(display("Runtime operation was rejected: {}", failure.message))]
    Rejected { failure: Failure },
    #[snafu(display("Runtime operation was rejected: {}", failure.message))]
    TenantGrantExhausted { failure: Failure },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Failure {
    pub message: String,
    pub code: i32,
    pub outcome: i32,
    pub retryable: bool,
    pub expected_sequence: Option<u64>,
    pub expected_registration_epoch: Option<u64>,
    pub retry_after_ms: Option<u64>,
    pub original_outcome_known: bool,
}

impl Error {
    pub(crate) fn admitted(self) -> Self {
        Self::Admitted {
            source: Box::new(self),
        }
    }
    pub fn invalid(reason: impl Into<String>) -> Self {
        Self::Invalid {
            reason: reason.into(),
        }
    }
    pub fn operation(
        operation: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Operation {
            operation,
            source: std::io::Error::other(source),
        }
    }
    pub fn foundation(source: crate::Error) -> Self {
        Self::operation("foundation service", source)
    }
    pub fn reject(code: ErrorCode, outcome: Outcome, retryable: bool) -> Self {
        Self::Rejected {
            failure: Failure::new(code, outcome, retryable),
        }
    }
    pub(crate) fn reject_with(
        code: ErrorCode,
        outcome: Outcome,
        retryable: bool,
        message: impl Into<String>,
    ) -> Self {
        let mut failure = Failure::new(code, outcome, retryable);
        failure.message = message.into();
        Self::Rejected { failure }
    }
    pub fn unavailable() -> Self {
        Self::reject(ErrorCode::Unavailable, Outcome::Rejected, true)
    }
    pub fn unknown() -> Self {
        Self::reject(ErrorCode::Unavailable, Outcome::Unknown, true)
    }
    pub fn failure(&self) -> Failure {
        match self {
            Self::Admitted { source } => {
                let mut failure = source.failure();
                failure.outcome = Outcome::Unknown.into();
                failure.original_outcome_known = false;
                failure
            }
            Self::Rejected { failure } | Self::TenantGrantExhausted { failure } => failure.clone(),
            Self::LocalNotSubmitted { .. } => {
                Failure::new(ErrorCode::Unavailable, Outcome::Rejected, true)
            }
            Self::Invalid { reason } => {
                let mut failure = Failure::new(ErrorCode::InvalidRequest, Outcome::Rejected, false);
                failure.message = reason.clone();
                failure
            }
            Self::Operation { operation, .. } => {
                let mut failure = Failure::new(ErrorCode::Unavailable, Outcome::Unknown, true);
                // Local error chains can contain private paths and credentials; logs retain them.
                failure.message = format!(
                    "Failed to {operation}; outcome is unknown; query or retry with the original operation identity"
                );
                failure
            }
            Self::CommittedConsumerWriter { .. } => {
                let mut failure = Failure::new(ErrorCode::Unavailable, Outcome::Unknown, true);
                failure.message = "Committed consumer writer verification is unknown; retry the original operation".into();
                failure
            }
        }
    }
}

impl Failure {
    fn new(code: ErrorCode, outcome: Outcome, retryable: bool) -> Self {
        Self {
            message: rejection_message(code).into(),
            code: code.into(),
            outcome: outcome.into(),
            retryable,
            expected_sequence: None,
            expected_registration_epoch: None,
            retry_after_ms: None,
            original_outcome_known: false,
        }
    }
}

fn rejection_message(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::InvalidRequest => "Request violates the operation's validation rules",
        ErrorCode::Unauthenticated => "Authentication failed; verify the configured credentials",
        ErrorCode::Forbidden => "The authenticated principal is not authorized for this operation",
        ErrorCode::Fenced => {
            "Ownership epoch or lease is stale; refresh the route and retry the original operation"
        }
        ErrorCode::QuotaExceeded => {
            "Operation exceeds the service's admission capacity; retry after capacity becomes available"
        }
        ErrorCode::Unavailable => "The required leader or authoritative service is unavailable",
        ErrorCode::DeadlineExceeded => {
            "The request deadline expired before the operation could complete"
        }
        ErrorCode::ResultUnknown => {
            "Operation outcome is unknown; query or retry with the original identity"
        }
        ErrorCode::SequenceGap => "Producer sequence does not match the next expected sequence",
        ErrorCode::ContentConflict => "Operation identity was reused with different content",
        ErrorCode::DeduplicationExpired => {
            "Original producer result has expired; do not recreate an unresolved batch with a new identity"
        }
        ErrorCode::StaleDelivery => {
            "Delivery token or attempt is stale; receive the current delivery before acknowledging"
        }
        ErrorCode::OffsetExpired => "Requested offset is below the retained floor",
        ErrorCode::NotFound => "The requested resource identity does not exist",
        ErrorCode::InternalError => {
            "Service failed internally; use request_id and operation_id to locate its diagnostic log"
        }
        ErrorCode::SessionExpired => {
            "Producer session expired or was retired; reconcile unresolved batches before opening a new session"
        }
        ErrorCode::RegistrationConflict => {
            "Producer registration epoch differs from the expected epoch"
        }
        ErrorCode::AssignmentBusy => {
            "Another consumer session owns the subscription partition; an explicit takeover is required"
        }
        ErrorCode::StaleGeneration => {
            "Cursor or assignment generation is stale; refresh the assignment"
        }
        ErrorCode::Unspecified => "Service returned an unspecified error code",
    }
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;

    #[test]
    fn error_codes_keep_distinct_diagnostics_and_outcomes() {
        let mut messages = std::collections::BTreeSet::new();
        for value in 0..=19 {
            let code = ErrorCode::try_from(value).unwrap();
            let failure = Error::reject(code, Outcome::Rejected, false).failure();
            assert!(messages.insert(failure.message));
            assert_eq!(failure.code, value);
            assert_eq!(failure.outcome, i32::from(Outcome::Rejected));
        }
        let unknown = Error::unknown().failure();
        assert_eq!(unknown.outcome, i32::from(Outcome::Unknown));
        assert!(unknown.retryable);
    }

    #[test]
    fn admitted_failure_keeps_its_cause_and_cannot_be_reported_as_a_current_refusal() {
        let error = Error::unavailable().admitted();
        assert!(std::error::Error::source(&error).is_some());
        let failure = error.failure();
        assert_eq!(failure.code, i32::from(ErrorCode::Unavailable));
        assert_eq!(failure.outcome, i32::from(Outcome::Unknown));
        assert!(!failure.original_outcome_known);
    }
}
