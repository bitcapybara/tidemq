use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use super::{Error, Result};

#[derive(Clone, Copy, Debug)]
pub struct RepairLimits {
    pub concurrent: usize,
    pub bytes_per_second: u64,
    pub burst_bytes: u32,
    pub queued_bytes: u32,
}
impl Default for RepairLimits {
    fn default() -> Self {
        Self {
            concurrent: 2,
            bytes_per_second: 32 * 1024 * 1024,
            burst_bytes: 1024 * 1024,
            queued_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Default)]
struct Registration {
    leases: usize,
    learners: usize,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct RepairDiagnostic {
    pub active_routes: usize,
    pub learner_routes: usize,
    pub available_repairs: usize,
    pub queued_bytes: usize,
}

struct Inner {
    limits: RepairLimits,
    active: Mutex<BTreeMap<(u64, u64), Registration>>,
    repairs: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    next: tokio::sync::Mutex<Instant>,
}

/// Shared by the node's groups; normal voter replication and all control RPCs bypass it.
#[derive(Clone)]
pub struct RepairBudget(Arc<Inner>);

impl RepairBudget {
    pub fn new(limits: RepairLimits) -> Result<Self> {
        if limits.concurrent == 0
            || limits.concurrent > 32
            || limits.bytes_per_second == 0
            || limits.burst_bytes == 0
            || limits.queued_bytes < limits.burst_bytes
            || limits.queued_bytes > 64 * 1024 * 1024
        {
            return Err(Error::Configuration);
        }
        Ok(Self(Arc::new(Inner {
            limits,
            active: Mutex::new(BTreeMap::new()),
            repairs: Arc::new(Semaphore::new(limits.concurrent)),
            bytes: Arc::new(Semaphore::new(limits.queued_bytes as usize)),
            next: tokio::sync::Mutex::new(Instant::now()),
        })))
    }

    pub async fn register(
        &self,
        target: u64,
        group: u64,
        deadline: Instant,
    ) -> Result<RepairLease> {
        if target == 0 || group == 0 {
            return Err(Error::Configuration);
        }
        let permit = self.acquire(deadline).await?;
        self.register_permit(target, group, permit)
    }

    /// Admission for application-level transfer work without changing Raft peer routing.
    pub(crate) async fn acquire(&self, deadline: Instant) -> Result<OwnedSemaphorePermit> {
        tokio::time::timeout_at(deadline, self.0.repairs.clone().acquire_owned())
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|_| Error::Overloaded)
    }

    pub fn try_register(&self, target: u64, group: u64) -> Result<RepairLease> {
        if target == 0 || group == 0 {
            return Err(Error::Configuration);
        }
        let permit = self.try_acquire()?;
        self.register_permit(target, group, permit)
    }

    pub(crate) fn try_acquire(&self) -> Result<OwnedSemaphorePermit> {
        self.0
            .repairs
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)
    }

    fn register_permit(
        &self,
        target: u64,
        group: u64,
        permit: OwnedSemaphorePermit,
    ) -> Result<RepairLease> {
        let mut active = self.0.active.lock().map_err(|_| Error::Poisoned)?;
        if active.len() >= 32 {
            return Err(Error::Overloaded);
        }
        let count = active.entry((target, group)).or_default();
        count.leases = count.leases.checked_add(1).ok_or(Error::Overloaded)?;
        count.learners = count.learners.checked_add(1).ok_or(Error::Overloaded)?;
        Ok(RepairLease {
            budget: self.clone(),
            target,
            group,
            learner: true,
            _permit: permit,
        })
    }

    pub fn is_repair(&self, target: u64, group: u64) -> Result<bool> {
        Ok(self
            .0
            .active
            .lock()
            .map_err(|_| Error::Poisoned)?
            .contains_key(&(target, group)))
    }

    pub fn is_learner(&self, target: u64, group: u64) -> Result<bool> {
        Ok(self
            .0
            .active
            .lock()
            .map_err(|_| Error::Poisoned)?
            .get(&(target, group))
            .is_some_and(|registration| registration.learners > 0))
    }

    pub fn diagnose(&self) -> Result<RepairDiagnostic> {
        let active = self.0.active.lock().map_err(|_| Error::Poisoned)?;
        Ok(RepairDiagnostic {
            active_routes: active.len(),
            learner_routes: active.values().filter(|entry| entry.learners > 0).count(),
            available_repairs: self.0.repairs.available_permits(),
            queued_bytes: (self.0.limits.queued_bytes as usize)
                .saturating_sub(self.0.bytes.available_permits()),
        })
    }

    /// FIFO byte reservations are retained while waiting, bounded by the original deadline.
    pub async fn pace(&self, bytes: usize, deadline: Instant) -> Result<()> {
        let bytes = u32::try_from(bytes).map_err(|_| Error::Overloaded)?;
        if bytes > self.0.limits.burst_bytes {
            return Err(Error::Overloaded);
        }
        let _queued = self
            .0
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| Error::Overloaded)?;
        let mut next = tokio::time::timeout_at(deadline, self.0.next.lock())
            .await
            .map_err(|_| Error::Deadline)?;
        let now = Instant::now();
        let burst = Duration::from_secs_f64(
            f64::from(self.0.limits.burst_bytes) / self.0.limits.bytes_per_second as f64,
        );
        let earliest = now.checked_sub(burst).unwrap_or(now);
        let scheduled = (*next)
            .max(earliest)
            .checked_add(Duration::from_secs_f64(
                f64::from(bytes) / self.0.limits.bytes_per_second as f64,
            ))
            .ok_or(Error::Configuration)?;
        let wake = scheduled.max(now);
        if wake >= deadline {
            return Err(Error::Deadline);
        }
        *next = scheduled;
        // Holding the FIFO lock prevents cancelled or concurrent requests overtaking a reservation.
        tokio::time::sleep_until(wake).await;
        Ok(())
    }
}

pub struct RepairLease {
    budget: RepairBudget,
    target: u64,
    group: u64,
    learner: bool,
    _permit: OwnedSemaphorePermit,
}
impl RepairLease {
    pub fn set_learner(&mut self, learner: bool) -> Result<()> {
        if self.learner == learner {
            return Ok(());
        }
        let mut active = self.budget.0.active.lock().map_err(|_| Error::Poisoned)?;
        let entry = active
            .get_mut(&(self.target, self.group))
            .ok_or(Error::Configuration)?;
        entry.learners = if learner {
            entry.learners.checked_add(1).ok_or(Error::Overloaded)?
        } else {
            entry.learners.saturating_sub(1)
        };
        self.learner = learner;
        Ok(())
    }
}
impl Drop for RepairLease {
    fn drop(&mut self) {
        if let Ok(mut active) = self.budget.0.active.lock()
            && let Some(count) = active.get_mut(&(self.target, self.group))
        {
            count.leases = count.leases.saturating_sub(1);
            if self.learner {
                count.learners = count.learners.saturating_sub(1);
            }
            if count.leases == 0 {
                active.remove(&(self.target, self.group));
            }
        }
    }
}
