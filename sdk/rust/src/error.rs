use snafu::Snafu;
use tidemq_protocol::common::{ErrorCode, Outcome};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    InvalidArgument,
    Unauthenticated,
    PermissionDenied,
    ResourceExhausted,
    Unavailable,
    DeadlineExceeded,
    Cancelled,
    NotFound,
    Conflict,
    StaleDelivery,
    RecoveryExpired,
    OutcomeUnknown,
    Closed,
    Protocol,
    Internal,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Administration outcome is unresolved; retry the original pending operation"))]
    Administration {
        pending: crate::PendingAdmin,
        source: std::sync::Arc<Error>,
    },
    #[snafu(display("Subscription deletion is incomplete; retry the original pending deletion"))]
    SubscriptionDeletion {
        pending: crate::PendingSubscriptionDeletion,
        source: std::sync::Arc<Error>,
    },
    #[snafu(display("Send did not complete; inspect the original pending send"))]
    SendUnresolved {
        pending: crate::PendingSend,
        source: std::sync::Arc<Error>,
    },
    #[snafu(display("Batch send was interrupted; inspect its original pending sends"))]
    BatchSendInterrupted {
        pending: Vec<crate::PendingSend>,
        source: std::sync::Arc<Error>,
    },
    #[snafu(display("The message was not submitted"))]
    SendNotSubmitted { source: std::sync::Arc<Error> },
    #[snafu(display("Send recovery exceeded its finite window; its outcome remains unknown"))]
    SendRecoveryExpired,
    #[snafu(display("Acknowledgement is incomplete; inspect the original pending operation"))]
    Acknowledgement {
        pending: crate::PendingAck,
        source: std::sync::Arc<Error>,
    },
    #[snafu(display(
        "This delivery already has an unresolved ACK or NACK with different semantics"
    ))]
    AcknowledgementInProgress,
    #[snafu(display("An owned SDK operation failed"))]
    Background { source: std::sync::Arc<Error> },
    #[snafu(display("An SDK task did not finish normally"))]
    Task { source: tokio::task::JoinError },
    #[snafu(display(
        "Credential {id} was committed but its one-time secret is unavailable; revoke it and issue a new credential"
    ))]
    CredentialSecretUnavailable { id: crate::CredentialId },
    #[snafu(display("Could not read TLS or credential material"))]
    Material { source: std::io::Error },
    #[snafu(display("Invalid client configuration: {reason}"))]
    Configuration { reason: &'static str },
    #[snafu(display("Invalid protocol data"))]
    Protocol { source: tidemq_protocol::Error },
    #[snafu(display("QUIC transport failed"))]
    Transport { source: tidemq_transport::Error },
    #[snafu(display("RPC failed; a submitted operation may have an unknown outcome"))]
    Rpc {
        source: tidemq_transport::rpc::Error,
    },
    #[snafu(display("Endpoint resolution failed for {endpoint}"))]
    Resolve {
        endpoint: String,
        source: std::io::Error,
    },
    #[snafu(display("Client deadline exceeded"))]
    Deadline,
    #[snafu(display("Client request cancelled"))]
    Cancelled,
    #[snafu(display("Request interrupted after submission; its outcome is unknown"))]
    RequestUnknown { source: Box<Error> },
    #[snafu(display("Client admission failed for {resource}"))]
    Admission { resource: &'static str },
    #[snafu(display("Invalid client transport memory budget"))]
    Memory {
        source: tidemq_transport::budget::Error,
    },
    #[snafu(display(
        "Server returned {}: {message}; outcome: {}; retryable: {retryable}",
        code.as_str_name(), outcome.as_str_name()
    ))]
    Remote {
        code: ErrorCode,
        message: String,
        outcome: Outcome,
        retryable: bool,
        expected_sequence: Option<u64>,
        expected_registration_epoch: Option<u64>,
        retry_after_ms: Option<u64>,
        original_outcome_known: bool,
    },
    #[snafu(display("Invalid server response: {reason}"))]
    ResponseMismatch { reason: String },
    #[snafu(display("Send result is still being resolved"))]
    SendInProgress,
    #[snafu(display("Producer session has expired; unresolved outcomes cannot be recreated"))]
    SessionExpired,
    #[snafu(display("Delivery belongs to an obsolete assignment"))]
    StaleDelivery,
    #[snafu(display(
        "The original topic was deleted and its name now belongs to a different topic"
    ))]
    TopicReplaced,
    #[snafu(display("The resource catalog changed during pagination; start a new listing"))]
    CatalogChanged,
    #[snafu(display("The SDK client is closed"))]
    Closed,
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Background { source }
            | Self::Administration { source, .. }
            | Self::SubscriptionDeletion { source, .. }
            | Self::Acknowledgement { source, .. }
            | Self::SendUnresolved { source, .. }
            | Self::BatchSendInterrupted { source, .. }
            | Self::SendNotSubmitted { source } => source.kind(),
            Self::RequestUnknown { source } => source.kind(),
            Self::Configuration { .. } | Self::Material { .. } | Self::Memory { .. } => {
                ErrorKind::InvalidArgument
            }
            Self::Admission { .. } => ErrorKind::ResourceExhausted,
            Self::Closed => ErrorKind::Closed,
            Self::Cancelled => ErrorKind::Cancelled,
            Self::Deadline => ErrorKind::DeadlineExceeded,
            Self::StaleDelivery => ErrorKind::StaleDelivery,
            Self::TopicReplaced => ErrorKind::NotFound,
            Self::AcknowledgementInProgress | Self::SendInProgress | Self::CatalogChanged => {
                ErrorKind::Conflict
            }
            Self::SessionExpired
            | Self::SendRecoveryExpired
            | Self::CredentialSecretUnavailable { .. } => ErrorKind::RecoveryExpired,
            Self::Remote { code, .. } => remote_kind(*code),
            Self::Transport { source } => transport_kind(source),
            Self::Rpc {
                source: tidemq_transport::rpc::Error::Transport { source },
            } => transport_kind(source),
            Self::Rpc {
                source: tidemq_transport::rpc::Error::Deadline { .. },
            } => ErrorKind::DeadlineExceeded,
            Self::Rpc { .. } | Self::Resolve { .. } => ErrorKind::Unavailable,
            Self::Protocol { .. } | Self::ResponseMismatch { .. } => ErrorKind::Protocol,
            Self::Task { .. } => ErrorKind::Internal,
        }
    }

    pub(crate) fn original_outcome_known(&self) -> bool {
        match self {
            Self::Background { source }
            | Self::Administration { source, .. }
            | Self::SubscriptionDeletion { source, .. }
            | Self::Acknowledgement { source, .. }
            | Self::SendUnresolved { source, .. }
            | Self::BatchSendInterrupted { source, .. } => source.original_outcome_known(),
            Self::Remote {
                original_outcome_known,
                ..
            } => *original_outcome_known,
            _ => false,
        }
    }

    pub(crate) fn preserve_uncertainty(self, uncertain: bool) -> Self {
        if uncertain && !self.outcome_unknown() && !self.original_outcome_known() {
            Self::RequestUnknown {
                source: Box::new(self),
            }
        } else {
            self
        }
    }

    pub(crate) fn permission_denied(&self) -> bool {
        match self {
            Self::Background { source }
            | Self::Administration { source, .. }
            | Self::SubscriptionDeletion { source, .. }
            | Self::Acknowledgement { source, .. }
            | Self::SendUnresolved { source, .. }
            | Self::BatchSendInterrupted { source, .. }
            | Self::SendNotSubmitted { source } => source.permission_denied(),
            Self::Remote {
                code: ErrorCode::Forbidden | ErrorCode::Unauthenticated,
                ..
            } => true,
            Self::Transport { source } => matches!(
                source,
                tidemq_transport::Error::Authentication { .. }
                    | tidemq_transport::Error::Authorization { .. }
            ),
            Self::Rpc {
                source: tidemq_transport::rpc::Error::Transport { source },
            } => matches!(
                source,
                tidemq_transport::Error::Authentication { .. }
                    | tidemq_transport::Error::Authorization { .. }
            ),
            _ => false,
        }
    }
    pub(crate) fn retry_rejected(&self) -> bool {
        matches!(
            self,
            Self::Remote {
                code: ErrorCode::Unavailable | ErrorCode::Fenced,
                outcome: Outcome::Rejected,
                retryable: true,
                ..
            }
        )
    }

    pub(crate) fn retry_admission(&self) -> bool {
        matches!(
            self,
            Self::Remote {
                code: ErrorCode::QuotaExceeded,
                outcome: Outcome::Rejected,
                retryable: true,
                ..
            }
        )
    }

    pub fn outcome_unknown(&self) -> bool {
        match self {
            Self::Background { source }
            | Self::Administration { source, .. }
            | Self::SubscriptionDeletion { source, .. }
            | Self::Acknowledgement { source, .. }
            | Self::SendUnresolved { source, .. }
            | Self::BatchSendInterrupted { source, .. } => source.outcome_unknown(),
            Self::SendRecoveryExpired | Self::RequestUnknown { .. } => true,
            Self::Transport { source } => transient_transport(source),
            Self::Rpc { source } => match source {
                tidemq_transport::rpc::Error::Closed
                | tidemq_transport::rpc::Error::Deadline {
                    may_have_been_sent: true,
                } => true,
                tidemq_transport::rpc::Error::Transport { source } => transient_transport(source),
                _ => false,
            },
            Self::Remote { outcome, .. } => *outcome == Outcome::Unknown,
            _ => false,
        }
    }

    pub(crate) fn retry_transport(&self) -> bool {
        match self {
            Self::Transport { source } => transient_transport(source),
            Self::Rpc { source } => match source {
                tidemq_transport::rpc::Error::Closed
                | tidemq_transport::rpc::Error::Deadline { .. } => true,
                tidemq_transport::rpc::Error::Transport { source } => transient_transport(source),
                _ => false,
            },
            _ => false,
        }
    }
}

fn remote_kind(code: ErrorCode) -> ErrorKind {
    match code {
        ErrorCode::InvalidRequest => ErrorKind::InvalidArgument,
        ErrorCode::Unauthenticated => ErrorKind::Unauthenticated,
        ErrorCode::Forbidden => ErrorKind::PermissionDenied,
        ErrorCode::QuotaExceeded => ErrorKind::ResourceExhausted,
        ErrorCode::Unavailable | ErrorCode::Fenced => ErrorKind::Unavailable,
        ErrorCode::DeadlineExceeded => ErrorKind::DeadlineExceeded,
        ErrorCode::NotFound => ErrorKind::NotFound,
        ErrorCode::AssignmentBusy
        | ErrorCode::RegistrationConflict
        | ErrorCode::ContentConflict
        | ErrorCode::SequenceGap => ErrorKind::Conflict,
        ErrorCode::StaleDelivery | ErrorCode::StaleGeneration => ErrorKind::StaleDelivery,
        ErrorCode::DeduplicationExpired | ErrorCode::OffsetExpired | ErrorCode::SessionExpired => {
            ErrorKind::RecoveryExpired
        }
        ErrorCode::ResultUnknown => ErrorKind::OutcomeUnknown,
        ErrorCode::InternalError | ErrorCode::Unspecified => ErrorKind::Internal,
    }
}

fn transport_kind(error: &tidemq_transport::Error) -> ErrorKind {
    match error {
        tidemq_transport::Error::Authentication { .. } => ErrorKind::Unauthenticated,
        tidemq_transport::Error::Authorization { .. } => ErrorKind::PermissionDenied,
        tidemq_transport::Error::Configuration { .. } => ErrorKind::InvalidArgument,
        tidemq_transport::Error::Admission { .. } | tidemq_transport::Error::Memory { .. } => {
            ErrorKind::ResourceExhausted
        }
        tidemq_transport::Error::Deadline { .. } => ErrorKind::DeadlineExceeded,
        tidemq_transport::Error::Protocol { .. } | tidemq_transport::Error::Prelude => {
            ErrorKind::Protocol
        }
        _ => ErrorKind::Unavailable,
    }
}

fn transient_transport(error: &tidemq_transport::Error) -> bool {
    matches!(
        error,
        tidemq_transport::Error::Deadline { .. }
            | tidemq_transport::Error::Closed
            | tidemq_transport::Error::Poisoned
            | tidemq_transport::Error::Io { .. }
            | tidemq_transport::Error::Connection { .. }
            | tidemq_transport::Error::Stream { .. }
    )
}

impl From<tidemq_protocol::Error> for Error {
    fn from(source: tidemq_protocol::Error) -> Self {
        Self::Protocol { source }
    }
}
impl From<tidemq_transport::Error> for Error {
    fn from(source: tidemq_transport::Error) -> Self {
        Self::Transport { source }
    }
}
impl From<tidemq_transport::rpc::Error> for Error {
    fn from(source: tidemq_transport::rpc::Error) -> Self {
        Self::Rpc { source }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_current_refusal_does_not_erase_an_earlier_unknown_outcome() {
        let refusal = |original_outcome_known| Error::Remote {
            code: ErrorCode::StaleDelivery,
            outcome: Outcome::Rejected,
            message: "The attempt was replaced".into(),
            retryable: false,
            expected_sequence: None,
            expected_registration_epoch: None,
            retry_after_ms: None,
            original_outcome_known,
        };
        assert!(refusal(false).preserve_uncertainty(true).outcome_unknown());
        assert!(!refusal(true).preserve_uncertainty(true).outcome_unknown());
        assert!(!refusal(false).preserve_uncertainty(false).outcome_unknown());
        assert!(Error::Deadline.preserve_uncertainty(true).outcome_unknown());
    }

    #[test]
    fn authorization_is_terminal_while_connection_loss_can_have_an_unknown_result() {
        let denied = Error::Transport {
            source: tidemq_transport::Error::Authorization {
                reason: "scope rejected",
            },
        };
        assert!(!denied.retry_transport());
        assert!(!denied.outcome_unknown());
        let lost = Error::Rpc {
            source: tidemq_transport::rpc::Error::Closed,
        };
        assert!(lost.retry_transport());
        assert!(lost.outcome_unknown());
        let not_sent = Error::Rpc {
            source: tidemq_transport::rpc::Error::Deadline {
                may_have_been_sent: false,
            },
        };
        assert!(!not_sent.outcome_unknown());
    }
}
