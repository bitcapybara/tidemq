//! Finite reference models, independent of the service implementation.
//!
//! These models explore durable transitions and lost replies. They do not
//! simulate disks, Raft elections, networking, or the implemented service.

pub mod publication;
pub mod subscription;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Fault {
    Healthy,
    ReplyLost,
    NoQuorum,
}

impl Fault {
    pub const ALL: [Self; 3] = [Self::Healthy, Self::ReplyLost, Self::NoQuorum];
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    Rejected,
    Unknown,
    Durable,
}

pub fn commit<S: Clone + Eq>(
    state: &mut S,
    fault: Fault,
    update: impl FnOnce(&mut S) -> bool,
) -> Outcome {
    if fault == Fault::NoQuorum {
        return Outcome::Unknown;
    }
    let mut next = state.clone();
    if !update(&mut next) {
        return Outcome::Rejected;
    }
    *state = next;
    if fault == Fault::ReplyLost {
        Outcome::Unknown
    } else {
        Outcome::Durable
    }
}
