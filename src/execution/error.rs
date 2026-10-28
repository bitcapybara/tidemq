use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Command was not submitted: {reason}"))]
    NotSubmitted { reason: String },
    #[snafu(display("Raft request must be forwarded to its current leader"))]
    ForwardToLeader {
        source: openraft::error::ForwardToLeader<u64, openraft::BasicNode>,
    },
    #[snafu(display("Command outcome is unknown: {reason}"))]
    Unknown { reason: String },
    #[snafu(display("Execution group failed: {reason}"))]
    Fatal { reason: String },
    #[snafu(display("Execution storage failed"))]
    Storage { source: crate::wal::Error },
    #[snafu(display("Application validation or execution failed"))]
    Application { source: std::io::Error },
    #[snafu(display("Invalid execution configuration: {reason}"))]
    Configuration { reason: &'static str },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<crate::wal::Error> for Error {
    fn from(source: crate::wal::Error) -> Self {
        Self::Storage { source }
    }
}

impl Error {
    pub(crate) fn not_submitted(reason: impl Into<String>) -> Self {
        Self::NotSubmitted {
            reason: reason.into(),
        }
    }
    pub(crate) fn unknown(reason: impl Into<String>) -> Self {
        Self::Unknown {
            reason: reason.into(),
        }
    }
    pub(crate) fn fatal(reason: impl Into<String>) -> Self {
        Self::Fatal {
            reason: reason.into(),
        }
    }
}
