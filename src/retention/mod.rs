use std::collections::VecDeque;

use serde::{Deserialize, Serialize};
use snafu::Snafu;

const TIME_BUCKET_MS: u64 = 1_000;

pub type Result<T> = std::result::Result<T, Rejection>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Snafu)]
pub enum Rejection {
    #[snafu(display("Retention policy exceeds its bounded limits"))]
    InvalidPolicy,
    #[snafu(display("Retention context does not match the committed state"))]
    InvalidContext,
    #[snafu(display("The bounded retention timestamp window is full"))]
    TimestampBudget,
    #[snafu(display("Retention timestamp allocation failed"))]
    Allocation,
    #[snafu(display("Retention revision changed; expected {expected}"))]
    StaleRevision { expected: u64 },
    #[snafu(display("Retention is protected above the maximum reclaim boundary {maximum}"))]
    Protected { maximum: u64 },
    #[snafu(display("Retention arithmetic overflow"))]
    CounterOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Policy {
    pub minimum_age_ms: u64,
    pub max_time_boundaries: u32,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            minimum_age_ms: 3_600_000,
            max_time_boundaries: 8_192,
        }
    }
}

impl Policy {
    pub fn validate(self) -> Result<()> {
        let required = self
            .minimum_age_ms
            .div_ceil(TIME_BUCKET_MS)
            .saturating_add(1);
        if !(2..=1_048_576).contains(&self.max_time_boundaries)
            || required > u64::from(self.max_time_boundaries)
        {
            return Err(Rejection::InvalidPolicy);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Boundary {
    end: u64,
    eligible_at_ms: u64,
}

/// This state is applied in the same data group as producer offsets and subscription cursors.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetentionState {
    policy: Policy,
    floor: u64,
    committed_end: u64,
    logical_time_ms: u64,
    revision: u64,
    boundaries: VecDeque<Boundary>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub expected_revision: u64,
    pub before: u64,
    pub now_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub floor: u64,
    pub committed_end: u64,
    pub revision: u64,
    pub age_eligible_before: u64,
    pub protected_before: u64,
    pub maximum_floor: u64,
    pub time_boundaries: usize,
    pub time_boundary_capacity: u32,
}

impl RetentionState {
    pub fn new(policy: Policy) -> Result<Self> {
        policy.validate()?;
        Ok(Self {
            policy,
            floor: 0,
            committed_end: 0,
            logical_time_ms: 0,
            revision: 1,
            boundaries: VecDeque::new(),
        })
    }

    pub fn floor(&self) -> u64 {
        self.floor
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Call after producer validation/reservation and before its infallible mutation.
    /// Duplicate producer results must bypass this method so retries never refresh message age.
    pub fn record_append(&mut self, previous_end: u64, end: u64, now_ms: u64) -> Result<()> {
        if previous_end != self.committed_end || end <= previous_end {
            return Err(Rejection::InvalidContext);
        }
        let now_ms = now_ms.max(self.logical_time_ms);
        let bucket_end = now_ms
            .checked_add(TIME_BUCKET_MS - 1)
            .map(|value| value / TIME_BUCKET_MS)
            .and_then(|value| value.checked_mul(TIME_BUCKET_MS))
            .ok_or(Rejection::CounterOverflow)?;
        let eligible_at_ms = bucket_end
            .checked_add(self.policy.minimum_age_ms)
            .ok_or(Rejection::CounterOverflow)?;
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(Rejection::CounterOverflow)?;
        let same_bucket = self
            .boundaries
            .back()
            .is_some_and(|last| last.eligible_at_ms == eligible_at_ms);
        if !same_bucket {
            if self.boundaries.len() >= self.policy.max_time_boundaries as usize {
                return Err(Rejection::TimestampBudget);
            }
            self.boundaries
                .try_reserve(1)
                .map_err(|_| Rejection::Allocation)?;
        }
        if same_bucket {
            let last = self
                .boundaries
                .back_mut()
                .ok_or(Rejection::InvalidContext)?;
            last.end = end;
        } else {
            self.boundaries.push_back(Boundary {
                end,
                eligible_at_ms,
            });
        }
        self.committed_end = end;
        self.logical_time_ms = now_ms;
        self.revision = revision;
        Ok(())
    }

    pub fn status(&self, now_ms: u64, protected_before: Option<u64>) -> Status {
        let age_eligible_before = self
            .boundaries
            .iter()
            .take_while(|boundary| boundary.eligible_at_ms <= now_ms)
            .last()
            .map_or(self.floor, |boundary| boundary.end);
        let protected_before = protected_before.unwrap_or(self.committed_end);
        Status {
            floor: self.floor,
            committed_end: self.committed_end,
            revision: self.revision,
            age_eligible_before,
            protected_before,
            maximum_floor: age_eligible_before
                .min(protected_before)
                .min(self.committed_end),
            time_boundaries: self.boundaries.len(),
            time_boundary_capacity: self.policy.max_time_boundaries,
        }
    }

    /// The application supplies the live producer end and minimum active ACK frontier under its apply lock.
    pub fn advance(
        &mut self,
        command: Command,
        committed_end: u64,
        protected_before: Option<u64>,
    ) -> Result<Status> {
        if committed_end != self.committed_end
            || command.before < self.floor
            || protected_before.is_some_and(|offset| offset < self.floor || offset > committed_end)
        {
            return Err(Rejection::InvalidContext);
        }
        if command.before == self.floor {
            return Ok(self.status(command.now_ms, protected_before));
        }
        if command.expected_revision != self.revision {
            return Err(Rejection::StaleRevision {
                expected: self.revision,
            });
        }
        let status = self.status(command.now_ms, protected_before);
        if command.before > status.maximum_floor {
            return Err(Rejection::Protected {
                maximum: status.maximum_floor,
            });
        }
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(Rejection::CounterOverflow)?;
        while self
            .boundaries
            .front()
            .is_some_and(|boundary| boundary.end <= command.before)
        {
            self.boundaries.pop_front();
        }
        self.floor = command.before;
        self.revision = revision;
        Ok(self.status(command.now_ms, protected_before))
    }

    pub fn validate(&self) -> Result<()> {
        self.policy.validate()?;
        if self.floor > self.committed_end
            || self.revision == 0
            || self.boundaries.len() > self.policy.max_time_boundaries as usize
        {
            return Err(Rejection::InvalidContext);
        }
        let mut previous_end = self.floor;
        let mut previous_time = None;
        for boundary in &self.boundaries {
            if boundary.end <= previous_end
                || boundary.end > self.committed_end
                || previous_time.is_some_and(|time| time >= boundary.eligible_at_ms)
            {
                return Err(Rejection::InvalidContext);
            }
            previous_end = boundary.end;
            previous_time = Some(boundary.eligible_at_ms);
        }
        if previous_end != self.committed_end {
            return Err(Rejection::InvalidContext);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimum_age_ack_protection_and_cas_jointly_bound_the_floor() {
        let mut state = RetentionState::new(Policy {
            minimum_age_ms: 1_000,
            max_time_boundaries: 4,
        })
        .unwrap();
        state.record_append(0, 2, 1).unwrap();
        state.record_append(2, 4, 1_001).unwrap();
        let command = Command {
            expected_revision: state.revision(),
            before: 2,
            now_ms: 1_999,
        };
        assert_eq!(
            state.advance(command, 4, Some(2)),
            Err(Rejection::Protected { maximum: 0 })
        );
        assert_eq!(
            state.advance(
                Command {
                    now_ms: 2_000,
                    ..command
                },
                4,
                Some(1)
            ),
            Err(Rejection::Protected { maximum: 1 })
        );
        assert_eq!(
            state
                .advance(
                    Command {
                        now_ms: 2_000,
                        ..command
                    },
                    4,
                    Some(2)
                )
                .unwrap()
                .floor,
            2
        );
        assert_eq!(
            state
                .advance(
                    Command {
                        now_ms: 2_000,
                        ..command
                    },
                    4,
                    Some(2)
                )
                .unwrap()
                .floor,
            2
        );
        let stale = Command {
            expected_revision: command.expected_revision,
            before: 4,
            now_ms: 3_000,
        };
        assert!(matches!(
            state.advance(stale, 4, None),
            Err(Rejection::StaleRevision { .. })
        ));
        state
            .advance(
                Command {
                    expected_revision: state.revision(),
                    ..stale
                },
                4,
                None,
            )
            .unwrap();
        state.validate().unwrap();
        let image = postcard::to_stdvec(&state).unwrap();
        let recovered: RetentionState = postcard::from_bytes(&image).unwrap();
        assert_eq!(recovered, state);
        recovered.validate().unwrap();
    }

    #[test]
    fn timestamps_are_bounded_and_a_full_window_reopens_after_safe_floor_advance() {
        let mut state = RetentionState::new(Policy {
            minimum_age_ms: 0,
            max_time_boundaries: 2,
        })
        .unwrap();
        state.record_append(0, 1, 1).unwrap();
        state.record_append(1, 2, 2).unwrap();
        assert_eq!(state.status(1_000, None).time_boundaries, 1);
        state.record_append(2, 3, 1_001).unwrap();
        let before = state.clone();
        assert_eq!(
            state.record_append(3, 4, 2_001),
            Err(Rejection::TimestampBudget)
        );
        assert_eq!(state, before);
        state
            .advance(
                Command {
                    expected_revision: state.revision(),
                    before: 2,
                    now_ms: 2_001,
                },
                3,
                None,
            )
            .unwrap();
        state.record_append(3, 4, 2_001).unwrap();
        state.validate().unwrap();
        assert_eq!(state.status(2_001, None).maximum_floor, 3);
    }
}
