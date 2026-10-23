//! Deterministic subscription state embedded in the message partition's data Raft group.
//!
//! A successful `apply` is an application result, not evidence of quorum durability. The caller
//! must submit through the data group's authorized Raft handler before dispatching a delivery or
//! reporting a durable ACK. Runtime ownership leases and request deadlines are checked outside
//! this deterministic state machine.

mod group;
mod state;
mod token;
mod types;

pub use group::{
    AckDisposition, Candidate, DeliveryProof, GroupAck, GroupCommand, GroupCursor, GroupIdentity,
    GroupOutcome, GroupRead, MemberGrant, ReadResult,
};
pub use state::SubscriptionState;
pub use token::{DELIVERY_TOKEN_BYTES, DeliveryToken, SigningKey};
pub use types::*;
