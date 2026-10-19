use serde::{Deserialize, Serialize};
use snafu::Snafu;

pub type Result<T> = std::result::Result<T, Rejection>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Snafu)]
pub enum Rejection {
    #[snafu(display("Producer limits are invalid"))]
    InvalidLimits,
    #[snafu(display("Producer identity or batch is invalid"))]
    InvalidBatch,
    #[snafu(display("Producer content digest does not match the canonical batch"))]
    DigestMismatch,
    #[snafu(display("Partition identity does not match the producer state"))]
    WrongPartition,
    #[snafu(display("Registration requires epoch {expected}"))]
    RegistrationConflict { expected: u64 },
    #[snafu(display("Producer already has an active session at epoch {epoch}"))]
    AlreadyOpen { epoch: u64 },
    #[snafu(display("Producer session is retired or expired"))]
    SessionExpired,
    #[snafu(display("Producer session has not been registered"))]
    UnknownSession,
    #[snafu(display("Producer identity does not match its registered epoch"))]
    SessionConflict,
    #[snafu(display("Producer requires sequence {expected}"))]
    SequenceGap { expected: u64 },
    #[snafu(display("Producer batch overlaps a different committed batch"))]
    SequenceConflict,
    #[snafu(display("Original result expired; retained results begin at sequence {before}"))]
    ResultExpired { before: u64 },
    #[snafu(display("Producer session capacity is exhausted"))]
    SessionCapacity,
    #[snafu(display("Partition hot data capacity is exhausted"))]
    DataCapacity,
    #[snafu(display("Producer counter capacity is exhausted"))]
    CounterExhausted,
    #[snafu(display("Requested offset is below retained floor {floor}"))]
    OffsetExpired { floor: u64 },
    #[snafu(display("Requested offset is above committed end {end}"))]
    OffsetNotCommitted { end: u64 },
    #[snafu(display("Read limits are invalid or cannot hold the next message"))]
    ReadLimit,
    #[snafu(display("Bounded producer buffer allocation failed"))]
    Allocation,
    #[snafu(display("Producer snapshot violates its state invariants"))]
    InvalidSnapshot,
}
