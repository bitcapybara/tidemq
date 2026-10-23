use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tidemq_protocol::Id;

use super::{DeliveryToken, SigningKey};

pub type Result<T> = std::result::Result<T, Rejection>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Snafu)]
pub enum Rejection {
    #[snafu(display("Subscription model does not match the requested consumer operation"))]
    ModelMismatch,
    #[snafu(display("Original operation is outside the retained result coverage"))]
    OutcomeExpired,
    #[snafu(display("Subscription limits are invalid"))]
    InvalidLimits,
    #[snafu(display("Authoritative data boundaries are inconsistent"))]
    InvalidContext,
    #[snafu(display("The subscription identity does not exist"))]
    NotFound,
    #[snafu(display("The subscription identity has been retired"))]
    Deleted,
    #[snafu(display("The immutable subscription identity conflicts with its original creation"))]
    IdentityConflict,
    #[snafu(display("The subscription identity budget is exhausted"))]
    IdentityBudget,
    #[snafu(display("The requested offset is below the retained floor"))]
    OutOfRetention,
    #[snafu(display("The requested offset is beyond committed messages"))]
    FutureOffset,
    #[snafu(display("The requested generations do not match the current cursor and assignment"))]
    StaleGeneration,
    #[snafu(display("A different consumer session owns the assignment"))]
    AssignmentBusy,
    #[snafu(display("The consumer session is not assigned"))]
    SessionMismatch,
    #[snafu(display("The delivery token is malformed or its authentication failed"))]
    InvalidToken,
    #[snafu(display("The delivery attempt has been superseded"))]
    StaleAttempt,
    #[snafu(display("The offset has not been delivered in this cursor generation"))]
    NotDelivered,
    #[snafu(display("The bounded delivery window is full"))]
    WindowFull,
    #[snafu(display("The in-flight message budget is exhausted"))]
    InFlightMessages,
    #[snafu(display("The in-flight byte budget is exhausted"))]
    InFlightBytes,
    #[snafu(display("The message reached the delivery attempt limit and remains unacknowledged"))]
    AttemptLimit,
    #[snafu(display("The current delivery attempt has not reached its retry deadline"))]
    RetryNotDue,
    #[snafu(display("The receive barrier is not a monotonic prefix of issued deliveries"))]
    InvalidBarrier,
    #[snafu(display("The SDK receive barrier has not reached the cold-to-hot boundary"))]
    CutoverPending,
    #[snafu(display("The delivery lane does not match the current cold-to-hot phase"))]
    WrongLane,
    #[snafu(display("New delivery must begin at the next contiguous offset"))]
    OffsetGap,
    #[snafu(display("The ACK batch exceeds its bounded admission limit"))]
    AckBatchTooLarge,
    #[snafu(display("The retained floor would cross an active subscription's protected offset"))]
    RetentionConflict,
    #[snafu(display("A subscription counter cannot advance without overflow"))]
    CounterOverflow,
    #[snafu(display("The decoded subscription snapshot violates its invariants"))]
    InvalidSnapshot,
    #[snafu(display("Unretired read or acknowledgement results still protect this subscription"))]
    RecoveryPending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    pub max_subscription_identities: u32,
    pub max_total_window_slots: u32,
    pub max_total_in_flight_bytes: u64,
    pub max_window_slots: u32,
    pub max_in_flight_messages: u32,
    pub max_in_flight_bytes: u64,
    pub max_delivery_attempts: u32,
    pub ack_timeout_ms: u64,
    pub max_ack_batch: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_subscription_identities: 128,
            max_total_window_slots: 4_096,
            max_total_in_flight_bytes: 64 * 1_024 * 1_024,
            max_window_slots: 1_024,
            max_in_flight_messages: 256,
            max_in_flight_bytes: 8 * 1_024 * 1_024,
            max_delivery_attempts: 32,
            ack_timeout_ms: 30_000,
            max_ack_batch: 256,
        }
    }
}

impl Limits {
    pub fn validate(&self) -> Result<()> {
        if !(1..=4_096).contains(&self.max_subscription_identities)
            || !(1..=262_144).contains(&self.max_total_window_slots)
            || !(1..=65_536).contains(&self.max_window_slots)
            || self.max_window_slots > self.max_total_window_slots
            || self.max_in_flight_messages == 0
            || self.max_in_flight_messages > self.max_window_slots
            || self.max_in_flight_bytes == 0
            || self.max_in_flight_bytes > self.max_total_in_flight_bytes
            || self.max_total_in_flight_bytes > 1_u64 << 40
            || !(1..=1_000_000).contains(&self.max_delivery_attempts)
            || !(1..=300_000).contains(&self.ack_timeout_ms)
            || !(1..=1_024).contains(&self.max_ack_batch)
        {
            return Err(Rejection::InvalidLimits);
        }
        Ok(())
    }
}

/// These boundaries must come from the producer state in the same data-group apply operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataContext {
    pub committed_end: u64,
    pub retained_floor: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Generations {
    pub cursor: u64,
    pub assignment: u64,
}

impl Generations {
    /// The first wire version deliberately uses assignment generation for the delivery generation.
    pub fn delivery(self) -> u64 {
        self.assignment
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DeliveryLane {
    Bulk,
    Realtime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Create {
        subscription_id: Id,
        operation_id: Id,
        start_offset: u64,
        signing_key: SigningKey,
    },
    Claim {
        subscription_id: Id,
        expected: Generations,
        session_id: Id,
        replace_session_id: Option<Id>,
        cold_before: u64,
    },
    Seek {
        subscription_id: Id,
        operation_id: Id,
        expected_cursor_generation: u64,
        offset: u64,
    },
    Delete {
        subscription_id: Id,
        expected_cursor_generation: u64,
    },
    RecordDelivery {
        subscription_id: Id,
        generations: Generations,
        session_id: Id,
        operation_id: Id,
        offset: u64,
        expected_attempt: u32,
        body_bytes: u32,
        now_ms: u64,
        lane: DeliveryLane,
    },
    Ack {
        subscription_id: Id,
        generations: Generations,
        tokens: Vec<DeliveryToken>,
    },
    ReceiveBarrier {
        subscription_id: Id,
        generations: Generations,
        session_id: Id,
        delivery_generation: u64,
        received_before: u64,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum Response {
    #[default]
    Noop,
    Applied(Outcome),
    Rejected(Rejection),
}

impl Response {
    pub fn into_result(self) -> Result<Outcome> {
        match self {
            Self::Applied(outcome) => Ok(outcome),
            Self::Rejected(reason) => Err(reason),
            Self::Noop => Err(Rejection::InvalidContext),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Outcome {
    Created(CursorPosition),
    Assigned(Assignment),
    Seeked(CursorPosition),
    Deleted,
    Delivery(DeliveryReceipt),
    DeliveryAlreadyAcknowledged { offset: u64 },
    Acknowledged(AckReceipt),
    ReceiveAdvanced(Assignment),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CursorPosition {
    pub generations: Generations,
    pub acknowledged_before: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub session_id: Id,
    pub generations: Generations,
    pub acknowledged_before: u64,
    pub next_delivery_offset: u64,
    pub issued_before: u64,
    pub cold_before: u64,
    pub received_before: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeliveryReceipt {
    pub offset: u64,
    pub attempt: u32,
    pub retry_at_ms: u64,
    pub token: DeliveryToken,
    pub duplicate: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AckReceipt {
    pub acknowledged_before: u64,
    pub accepted: u32,
    pub already_acknowledged: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Phase {
    Unassigned,
    ColdReplay,
    AwaitReceiveBarrier,
    Realtime,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BlockReason {
    Window,
    InFlightMessages,
    InFlightBytes,
    AwaitReceiveBarrier,
    AttemptsExhausted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Diagnostics {
    pub subscription_id: Id,
    pub phase: Phase,
    pub generations: Generations,
    pub acknowledged_before: u64,
    pub next_delivery_offset: u64,
    pub issued_before: u64,
    pub received_before: u64,
    pub cold_before: u64,
    pub window_slots: u32,
    pub acknowledged_holes: u32,
    pub in_flight_messages: u32,
    pub in_flight_bytes: u64,
    pub total_window_slots: u32,
    pub total_in_flight_bytes: u64,
    pub next_retry: Option<RetryStatus>,
    pub blocked: Option<BlockReason>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetryStatus {
    pub offset: u64,
    pub attempt: u32,
    pub retry_at_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AcknowledgedRange {
    pub start: u64,
    pub end: u64,
}
