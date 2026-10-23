use snafu::Snafu;
use tidemq_protocol::common::{ErrorCode, Outcome};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
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
    #[snafu(display("Producer has unresolved batches; retry their original identities first"))]
    ProducerBlocked,
    #[snafu(display("Producer session has expired; unresolved outcomes cannot be recreated"))]
    SessionExpired,
    #[snafu(display(
        "Producer registration result is unknown for {producer_id} at epoch {registration_epoch}"
    ))]
    RegistrationUnknown {
        producer_id: tidemq_protocol::Id,
        registration_epoch: u64,
        source: Box<Error>,
    },
    #[snafu(display(
        "Producer result is unknown; inspect pending_batches and retry the original batch"
    ))]
    ProduceUnknown { source: Box<Error> },
    #[snafu(display("Delivery belongs to an obsolete assignment"))]
    StaleDelivery,
    #[snafu(display("A failed delivery blocks this key until the application retries it"))]
    KeyBlocked,
    #[snafu(display("The SDK client is closed"))]
    Closed,
}

impl Error {
    pub(crate) fn original_outcome_known(&self) -> bool {
        match self {
            Self::Background { source } | Self::Acknowledgement { source, .. } => {
                source.original_outcome_known()
            }
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
            Self::Background { source } | Self::Acknowledgement { source, .. } => {
                source.permission_denied()
            }
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

    pub fn outcome_unknown(&self) -> bool {
        match self {
            Self::Background { source } | Self::Acknowledgement { source, .. } => {
                source.outcome_unknown()
            }
            Self::ProduceUnknown { .. }
            | Self::RegistrationUnknown { .. }
            | Self::RequestUnknown { .. } => true,
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
