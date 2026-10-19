use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use snafu::Snafu;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{FenceRecord, Grant};

#[derive(Clone, Copy, Debug)]
pub struct ControlPolicy {
    pub ttl: Duration,
    pub finalization_grace: Duration,
    pub max_finalizations: usize,
}

impl Default for ControlPolicy {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(3),
            finalization_grace: Duration::from_secs(1),
            max_finalizations: 256,
        }
    }
}

impl ControlPolicy {
    pub fn validate(self) -> Result<(), Error> {
        if self.ttl.is_zero()
            || self.ttl > Duration::from_secs(10)
            || self.finalization_grace > Duration::from_secs(1)
            || self.max_finalizations == 0
            || self.max_finalizations > 4_096
        {
            return Err(Error::InvalidPolicy);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Work {
    Produce,
    Deliver,
    ControlMutation,
    Publish,
    GarbageCollect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalizationKind {
    Ack,
    Read,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Control validity policy is outside its bounded limits"))]
    InvalidPolicy,
    #[snafu(display("Control authorization has expired"))]
    Expired,
    #[snafu(display("Control authorization does not match the committed fence"))]
    GrantChanged,
    #[snafu(display("The operation deadline has expired"))]
    Deadline,
    #[snafu(display("The bounded finalization allowance is exhausted"))]
    FinalizationCapacity,
}

/// Process-local authority obtained from an active route in a fresh linearizable read.
/// No serde/clone constructor exists; disk state cannot renew this validity window.
pub struct ControlLease {
    grant: Grant,
    route_generation: u64,
    read_started: Instant,
    expires_at: Instant,
    finalization_until: Instant,
    finalizations: Arc<Semaphore>,
}

#[derive(Clone, Debug, Serialize)]
pub struct LeaseDiagnostic {
    pub operation_id: tidemq_protocol::Id,
    pub epoch: u64,
    pub route_generation: u64,
    pub valid: bool,
    pub reason: ValidityReason,
    pub read_age_millis: u64,
    pub remaining_millis: u64,
    pub available_finalizations: usize,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidityReason {
    FreshLinearizableRead,
    Expired,
}

impl ControlLease {
    pub(super) fn from_verified(
        grant: Grant,
        route_generation: u64,
        read_started: Instant,
        policy: ControlPolicy,
        finalizations: Arc<Semaphore>,
    ) -> Result<Self, Error> {
        policy.validate()?;
        let expires_at = read_started
            .checked_add(policy.ttl)
            .ok_or(Error::InvalidPolicy)?;
        let finalization_until = expires_at
            .checked_add(policy.finalization_grace)
            .ok_or(Error::InvalidPolicy)?;
        if Instant::now() >= expires_at {
            return Err(Error::Expired);
        }
        Ok(Self {
            grant,
            route_generation,
            read_started,
            expires_at,
            finalization_until,
            finalizations,
        })
    }

    pub fn grant(&self) -> &Grant {
        &self.grant
    }

    pub fn expires_at(&self) -> Instant {
        self.expires_at
    }

    pub fn check(&self, fence: &FenceRecord, _work: Work) -> Result<(), Error> {
        self.check_at(fence, Instant::now())
    }

    fn check_at(&self, fence: &FenceRecord, now: Instant) -> Result<(), Error> {
        if fence.grant != self.grant {
            return Err(Error::GrantChanged);
        }
        if now >= self.expires_at {
            return Err(Error::Expired);
        }
        Ok(())
    }

    /// Permission and data quorum must still be checked by the ACK/read operation.
    pub fn begin_finalization(
        &self,
        fence: &FenceRecord,
        kind: FinalizationKind,
        deadline: Instant,
    ) -> Result<Finalization, Error> {
        self.check_at(fence, Instant::now())?;
        let deadline = deadline.min(self.finalization_until);
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        let permit = self
            .finalizations
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::FinalizationCapacity)?;
        Ok(Finalization {
            grant: self.grant.clone(),
            kind,
            deadline,
            _permit: permit,
        })
    }

    pub fn diagnose(&self) -> LeaseDiagnostic {
        let now = Instant::now();
        let valid = now < self.expires_at;
        LeaseDiagnostic {
            operation_id: self.grant.operation_id,
            epoch: self.grant.epoch,
            route_generation: self.route_generation,
            valid,
            reason: if valid {
                ValidityReason::FreshLinearizableRead
            } else {
                ValidityReason::Expired
            },
            read_age_millis: millis(now.saturating_duration_since(self.read_started)),
            remaining_millis: millis(self.expires_at.saturating_duration_since(now)),
            available_finalizations: self.finalizations.available_permits(),
        }
    }
}

pub struct Finalization {
    grant: Grant,
    kind: FinalizationKind,
    deadline: Instant,
    _permit: OwnedSemaphorePermit,
}

impl Finalization {
    pub fn kind(&self) -> FinalizationKind {
        self.kind
    }

    pub fn check(&self, fence: &FenceRecord) -> Result<(), Error> {
        self.check_at(fence, Instant::now())
    }

    fn check_at(&self, fence: &FenceRecord, now: Instant) -> Result<(), Error> {
        if fence.grant != self.grant {
            return Err(Error::GrantChanged);
        }
        if now >= self.deadline {
            return Err(Error::Deadline);
        }
        Ok(())
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
pub(crate) fn test_lease(grant: Grant, policy: ControlPolicy) -> Result<ControlLease, Error> {
    ControlLease::from_verified(
        grant,
        1,
        Instant::now(),
        policy,
        Arc::new(Semaphore::new(policy.max_finalizations)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ownership::tests::grant;

    #[test]
    fn validity_starts_at_read_start_and_expiry_rejects_every_new_work_class() {
        let grant = grant(1);
        let fence = FenceRecord {
            grant: grant.clone(),
            fence_generation: 1,
        };
        let started = Instant::now();
        let policy = ControlPolicy::default();
        let lease = ControlLease::from_verified(
            grant,
            1,
            started,
            policy,
            Arc::new(Semaphore::new(policy.max_finalizations)),
        )
        .unwrap();
        assert_eq!(lease.expires_at, started + policy.ttl);
        assert!(
            lease
                .check_at(&fence, lease.expires_at - Duration::from_nanos(1))
                .is_ok()
        );
        assert!(matches!(
            lease.check_at(&fence, lease.expires_at),
            Err(Error::Expired)
        ));
        let mut future = fence.clone();
        future.grant.epoch = 2;
        assert!(matches!(
            lease.check_at(&future, started),
            Err(Error::GrantChanged)
        ));
        assert!(matches!(
            ControlLease::from_verified(
                fence.grant.clone(),
                1,
                started - Duration::from_secs(20),
                policy,
                Arc::new(Semaphore::new(policy.max_finalizations))
            ),
            Err(Error::Expired)
        ));
        for work in [
            Work::Produce,
            Work::Deliver,
            Work::ControlMutation,
            Work::Publish,
            Work::GarbageCollect,
        ] {
            assert!(lease.check(&fence, work).is_ok());
        }
        assert!(
            !serde_json::to_string(&lease.diagnose())
                .unwrap()
                .contains("token")
        );
    }

    #[test]
    fn in_flight_finalization_is_capped_and_never_survives_a_new_fence() {
        let grant = grant(1);
        let fence = FenceRecord {
            grant: grant.clone(),
            fence_generation: 1,
        };
        let policy = ControlPolicy {
            max_finalizations: 1,
            ..ControlPolicy::default()
        };
        let lease = ControlLease::from_verified(
            grant,
            1,
            Instant::now(),
            policy,
            Arc::new(Semaphore::new(policy.max_finalizations)),
        )
        .unwrap();
        let finalization = lease
            .begin_finalization(
                &fence,
                FinalizationKind::Ack,
                Instant::now() + Duration::from_secs(60),
            )
            .unwrap();
        assert_eq!(finalization.deadline, lease.finalization_until);
        assert!(matches!(
            lease.begin_finalization(
                &fence,
                FinalizationKind::Read,
                Instant::now() + Duration::from_secs(60)
            ),
            Err(Error::FinalizationCapacity)
        ));
        assert!(finalization.check_at(&fence, lease.expires_at).is_ok());
        assert!(matches!(
            finalization.check_at(&fence, lease.finalization_until),
            Err(Error::Deadline)
        ));
        let mut replaced = fence.clone();
        replaced.grant.epoch = 2;
        assert!(matches!(
            finalization.check(&replaced),
            Err(Error::GrantChanged)
        ));
        drop(finalization);
        assert_eq!(lease.diagnose().available_finalizations, 1);
    }
}
