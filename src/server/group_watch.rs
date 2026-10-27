//! Independent, charged observation identities and cancellation watermarks.

use crate::budget::{MemoryBudget, MemoryPermit};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tidemq_protocol::Id;
use tidemq_protocol::common::TrafficClass;
use tokio::sync::watch;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
#[path = "group_watch_admission.rs"]
mod admission;
pub(crate) use admission::Turn as Admission;
#[path = "group_watch_metrics.rs"]
mod telemetry;
pub(crate) use telemetry::Metrics;
use tokio::time::Instant;

#[derive(Clone)]
pub(crate) struct Cancellation(watch::Sender<bool>);
impl Cancellation {
    fn new() -> Self {
        Self(watch::channel(false).0)
    }
    fn cancel(&self) {
        self.0.send_replace(true);
    }
    pub(crate) fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    pub(crate) async fn cancelled(&self) {
        let mut receiver = self.0.subscribe();
        while !*receiver.borrow_and_update() {
            if receiver.changed().await.is_err() {
                break;
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct Budget(Arc<BudgetInner>);
struct BudgetInner {
    memory: MemoryBudget,
    maximum: usize,
    usage: Mutex<Usage>,
    forwards: Arc<Semaphore>,
    admission: admission::Queue,
    rechecks: admission::Queue,
    authority_inbound: Arc<Semaphore>,
    authority_outbound: Arc<Semaphore>,
    metrics: Metrics,
}
#[derive(Default)]
struct Usage {
    bytes: usize,
    targets: usize,
    tenants: BTreeMap<Option<Id>, usize>,
    members: BTreeMap<(Option<Id>, Id), (usize, usize)>,
}
pub(crate) struct Charge {
    budget: Budget,
    bytes: usize,
    targets: usize,
    member_targets: usize,
    tenant: Option<Id>,
    member: Id,
    waiting: bool,
    _memory: MemoryPermit,
    _forward: Option<OwnedSemaphorePermit>,
}

pub(crate) struct AuthorityCharge {
    _charge: Charge,
    _slot: OwnedSemaphorePermit,
    gauge: prometheus_client::metrics::gauge::Gauge,
}

impl Drop for AuthorityCharge {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

impl Budget {
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn diagnostic(&self) -> (usize, usize, usize) {
        let (bytes, targets) = self
            .0
            .usage
            .lock()
            .map(|usage| (usage.bytes, usage.targets))
            .unwrap_or((usize::MAX, usize::MAX));
        (
            bytes,
            targets,
            16_usize.saturating_sub(self.0.forwards.available_permits()),
        )
    }
    pub(crate) fn metrics(&self) -> &Metrics {
        &self.0.metrics
    }
    pub(crate) fn new(memory: MemoryBudget) -> Self {
        let metrics = Metrics::default();
        Self(Arc::new(BudgetInner {
            maximum: (memory.data_capacity() / 16).min(8 * 1024 * 1024),
            memory,
            usage: Mutex::new(Usage::default()),
            // Runtime internal RPCs have 128 ordinary slots; observations leave 112 available.
            forwards: Arc::new(Semaphore::new(16)),
            admission: admission::Queue::new(8, metrics.admission_queued.clone()),
            rechecks: admission::Queue::new(1, metrics.rechecks_queued.clone()),
            authority_inbound: Arc::new(Semaphore::new(8)),
            authority_outbound: Arc::new(Semaphore::new(8)),
            metrics,
        }))
    }

    pub(crate) async fn admit(
        &self,
        tenant: Option<Id>,
        member: Id,
        deadline: Instant,
    ) -> Option<admission::Turn> {
        let charge = self.reserve(tenant, member, 0, 1_024, false)?;
        self.0
            .admission
            .enter(tenant, member, charge, deadline)
            .await
    }

    pub(crate) async fn recheck(
        &self,
        tenant: Option<Id>,
        member: Id,
        deadline: Instant,
    ) -> Option<admission::Turn> {
        let charge = self.reserve(tenant, member, 0, 1_024, false)?;
        self.0
            .rechecks
            .enter(tenant, member, charge, deadline)
            .await
    }

    pub(crate) fn reserve_authority(
        &self,
        tenant: Option<Id>,
        member: Id,
        inbound: bool,
    ) -> Option<AuthorityCharge> {
        let (slots, gauge) = if inbound {
            (&self.0.authority_inbound, &self.0.metrics.authority_inbound)
        } else {
            (
                &self.0.authority_outbound,
                &self.0.metrics.authority_outbound,
            )
        };
        let slot = slots.clone().try_acquire_owned().ok()?;
        let charge = self.reserve(tenant, member, 0, 512, false)?;
        gauge.inc();
        Some(AuthorityCharge {
            _charge: charge,
            _slot: slot,
            gauge: gauge.clone(),
        })
    }

    pub(crate) fn close(&self) {
        self.0.admission.close();
        self.0.rechecks.close();
        self.0.authority_inbound.close();
        self.0.authority_outbound.close();
    }

    pub(crate) fn reserve(
        &self,
        tenant: Option<Id>,
        member: Id,
        targets: usize,
        bytes: usize,
        waiting: bool,
    ) -> Option<Charge> {
        self.reserve_counted(tenant, member, (targets, targets), bytes, waiting)
    }

    pub(crate) fn reserve_cached(
        &self,
        tenant: Option<Id>,
        member: Id,
        targets: usize,
        bytes: usize,
    ) -> Option<Charge> {
        self.reserve_counted(tenant, member, (targets, 0), bytes, false)
    }

    pub(crate) fn reserve_forward(
        &self,
        tenant: Option<Id>,
        member: Id,
        bytes: usize,
    ) -> Option<Charge> {
        let forward = self.0.forwards.clone().try_acquire_owned().ok()?;
        let mut charge = self.reserve(tenant, member, 0, bytes, false)?;
        charge._forward = Some(forward);
        Some(charge)
    }

    fn reserve_counted(
        &self,
        tenant: Option<Id>,
        member: Id,
        counts: (usize, usize),
        bytes: usize,
        waiting: bool,
    ) -> Option<Charge> {
        let (targets, active_targets) = counts;
        let size = u32::try_from(bytes).ok()?;
        let mut usage = self.0.usage.lock().ok()?;
        let tenant_bytes = usage.tenants.get(&tenant).copied().unwrap_or(0);
        let (member_targets, member_waiting) = usage
            .members
            .get(&(tenant, member))
            .copied()
            .unwrap_or_default();
        if bytes == 0
            || targets > 32
            || usage.bytes.checked_add(bytes)? > self.0.maximum
            || usage.targets.checked_add(targets)? > 4_096
            || tenant_bytes.checked_add(bytes)? > self.0.maximum / 4
            || member_targets.checked_add(active_targets)? > 256
            || waiting && member_waiting.checked_add(active_targets)? > 224
        {
            return None;
        }
        let memory = self
            .0
            .memory
            .try_reserve(TrafficClass::Control, size)
            .ok()?;
        usage.bytes += bytes;
        usage.targets += targets;
        self.0.metrics.bytes.inc_by(bytes as i64);
        self.0.metrics.targets.inc_by(targets as i64);
        usage.tenants.insert(tenant, tenant_bytes + bytes);
        usage.members.insert(
            (tenant, member),
            (
                member_targets + active_targets,
                member_waiting + if waiting { active_targets } else { 0 },
            ),
        );
        Some(Charge {
            budget: self.clone(),
            bytes,
            targets,
            member_targets: active_targets,
            tenant,
            member,
            waiting,
            _memory: memory,
            _forward: None,
        })
    }
}

impl Charge {
    pub(crate) fn tenant(&self) -> Option<Id> {
        self.tenant
    }
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.0.metrics.bytes.dec_by(self.bytes as i64);
        self.budget.0.metrics.targets.dec_by(self.targets as i64);
        if let Ok(mut usage) = self.budget.0.usage.lock() {
            usage.bytes = usage.bytes.saturating_sub(self.bytes);
            usage.targets = usage.targets.saturating_sub(self.targets);
            if let Some(bytes) = usage.tenants.get_mut(&self.tenant) {
                *bytes = bytes.saturating_sub(self.bytes);
                if *bytes == 0 {
                    usage.tenants.remove(&self.tenant);
                }
            }
            if let Some((targets, waiting)) = usage.members.get_mut(&(self.tenant, self.member)) {
                *targets = targets.saturating_sub(self.member_targets);
                *waiting =
                    waiting.saturating_sub(if self.waiting { self.member_targets } else { 0 });
                if *targets == 0 {
                    usage.members.remove(&(self.tenant, self.member));
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Key {
    pub principal: Arc<str>,
    pub session: Id,
    pub member: Id,
    pub epoch: u64,
    pub root: Id,
    pub operation: Id,
}

struct Entry {
    digest: [u8; 32],
    attempt: u64,
    deadline: Instant,
    active: bool,
    cancelled: Cancellation,
    _charge: Arc<Charge>,
}

pub(crate) struct Registry {
    entries: Mutex<BTreeMap<Key, Entry>>,
    stop: Cancellation,
    reaper: Mutex<Option<tokio::task::JoinHandle<()>>>,
    cleanup: Mutex<tokio::task::JoinSet<Charge>>,
}
impl Default for Registry {
    fn default() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            stop: Cancellation::new(),
            reaper: Mutex::new(None),
            cleanup: Mutex::new(tokio::task::JoinSet::new()),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Refusal {
    Conflict,
    Duplicate,
    Stale,
    Expired,
    Unavailable,
}

pub(crate) struct Registration {
    registry: Arc<Registry>,
    key: Key,
    attempt: u64,
    pub cancelled: Cancellation,
    pub deadline: Instant,
    _charge: Arc<Charge>,
}

impl Registry {
    pub(crate) fn cleanup(
        &self,
        future: impl std::future::Future<Output = ()> + Send + 'static,
        charge: Charge,
    ) -> Result<(), Refusal> {
        if self.stop.is_cancelled() {
            return Err(Refusal::Unavailable);
        }
        let mut tasks = self.cleanup.lock().map_err(|_| Refusal::Unavailable)?;
        tasks.spawn(async move {
            future.await;
            charge
        });
        Ok(())
    }
    #[cfg(feature = "test-hooks")]
    pub(crate) fn cleanup_count(&self) -> usize {
        self.cleanup
            .lock()
            .map(|tasks| tasks.len())
            .unwrap_or(usize::MAX)
    }
    #[cfg(feature = "test-hooks")]
    pub(crate) fn diagnostic(&self) -> (usize, usize, usize) {
        let (active, watermarks) = self
            .entries
            .lock()
            .map(|entries| {
                (
                    entries.values().filter(|entry| entry.active).count(),
                    entries.values().filter(|entry| !entry.active).count(),
                )
            })
            .unwrap_or((usize::MAX, usize::MAX));
        let timers = self
            .reaper
            .lock()
            .map(|task| usize::from(task.is_some()))
            .unwrap_or(usize::MAX);
        (active, watermarks, timers)
    }
    pub(crate) fn start(self: &Arc<Self>, charge: Charge) -> Result<(), Refusal> {
        let mut stored = self.reaper.lock().map_err(|_| Refusal::Unavailable)?;
        if stored.is_some() {
            return Err(Refusal::Duplicate);
        }
        let weak = Arc::downgrade(self);
        let stop = self.stop.clone();
        *stored = Some(tokio::spawn(async move {
            let _charge = charge;
            let mut timer = tokio::time::interval(std::time::Duration::from_millis(50));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {_=stop.cancelled()=>break,_=timer.tick()=>{}}
                let Some(registry) = weak.upgrade() else {
                    break;
                };
                if let Ok(mut entries) = registry.entries.lock() {
                    Self::expire(&mut entries, Instant::now());
                }
                if let Ok(mut cleanup) = registry.cleanup.lock() {
                    while cleanup.try_join_next().is_some() {}
                }
            }
        }));
        Ok(())
    }

    pub(crate) async fn close(&self, deadline: Instant) -> bool {
        self.shutdown();
        let task = self.reaper.lock().ok().and_then(|mut task| task.take());
        let mut clean = true;
        if let Some(mut task) = task
            && !matches!(
                tokio::time::timeout_at(deadline, &mut task).await,
                Ok(Ok(()))
            )
        {
            task.abort();
            let _ = task.await;
            clean = false;
        }
        let tasks = self
            .cleanup
            .lock()
            .ok()
            .map(|mut tasks| std::mem::take(&mut *tasks));
        let Some(mut tasks) = tasks else {
            return false;
        };
        while !tasks.is_empty() {
            if tokio::time::timeout_at(deadline, tasks.join_next())
                .await
                .is_err()
            {
                tasks.abort_all();
                clean = false;
                break;
            }
        }
        while tasks.join_next().await.is_some() {}
        clean
    }
    pub(crate) fn register(
        self: &Arc<Self>,
        key: Key,
        digest: [u8; 32],
        attempt: u64,
        deadline: Instant,
        charge: Charge,
    ) -> Result<Registration, Refusal> {
        let mut entries = self.entries.lock().map_err(|_| Refusal::Unavailable)?;
        let now = Instant::now();
        Self::expire(&mut entries, now);
        if entries.iter().any(|(watermark, entry)| {
            watermark.epoch == 0
                && watermark.principal == key.principal
                && watermark.session == key.session
                && watermark.root == key.root
                && watermark.operation == key.operation
                && attempt <= entry.attempt
        }) {
            return Err(Refusal::Stale);
        }
        if deadline <= now || attempt == 0 {
            return Err(Refusal::Expired);
        }
        let original_deadline = if let Some(previous) = entries.get(&key) {
            if previous.digest != digest {
                return Err(Refusal::Conflict);
            }
            if attempt < previous.attempt || attempt == previous.attempt && !previous.active {
                return Err(Refusal::Stale);
            }
            if attempt == previous.attempt {
                return Err(Refusal::Duplicate);
            }
            previous.cancelled.cancel();
            previous.deadline
        } else {
            deadline
        };
        let cancelled = Cancellation::new();
        let charge = Arc::new(charge);
        entries.insert(
            key.clone(),
            Entry {
                digest,
                attempt,
                deadline: original_deadline,
                active: true,
                cancelled: cancelled.clone(),
                _charge: charge.clone(),
            },
        );
        Ok(Registration {
            registry: self.clone(),
            key,
            attempt,
            cancelled,
            deadline: original_deadline.min(deadline),
            _charge: charge,
        })
    }

    pub(crate) fn cancel(
        &self,
        principal: &str,
        session: Id,
        root: Id,
        operation: Id,
        attempt: u64,
    ) -> Result<bool, Refusal> {
        let mut entries = self.entries.lock().map_err(|_| Refusal::Unavailable)?;
        Self::expire(&mut entries, Instant::now());
        let mut matched = false;
        for (key, entry) in entries.iter_mut() {
            if key.principal.as_ref() == principal
                && key.session == session
                && key.root == root
                && key.operation == operation
                && entry.attempt == attempt
            {
                entry.active = false;
                entry.cancelled.cancel();
                matched = true;
            }
        }
        Ok(matched)
    }

    pub(crate) fn shutdown(&self) {
        self.stop.cancel();
        if let Ok(mut entries) = self.entries.lock() {
            for entry in entries.values_mut() {
                entry.active = false;
                entry.cancelled.cancel();
            }
            entries.clear();
        }
    }

    pub(crate) fn remember_cancel(
        &self,
        mut key: Key,
        attempt: u64,
        deadline: Instant,
        charge: Charge,
    ) -> Result<(), Refusal> {
        let mut entries = self.entries.lock().map_err(|_| Refusal::Unavailable)?;
        Self::expire(&mut entries, Instant::now());
        key.epoch = 0;
        key.member = key.operation;
        if let Some(entry) = entries.get_mut(&key) {
            entry.attempt = entry.attempt.max(attempt);
            entry.deadline = entry.deadline.max(deadline);
        } else {
            let cancelled = Cancellation::new();
            cancelled.cancel();
            entries.insert(
                key,
                Entry {
                    digest: [0; 32],
                    attempt,
                    deadline,
                    active: false,
                    cancelled,
                    _charge: Arc::new(charge),
                },
            );
        }
        Ok(())
    }

    fn expire(entries: &mut BTreeMap<Key, Entry>, now: Instant) {
        entries.retain(|_, entry| {
            if entry.deadline <= now {
                entry.cancelled.cancel();
                false
            } else {
                true
            }
        });
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Ok(mut entries) = self.registry.entries.lock()
            && let Some(entry) = entries.get_mut(&self.key)
            && entry.attempt == self.attempt
        {
            entry.active = false;
            entry.cancelled.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    fn setup() -> (Budget, Arc<Registry>, Key) {
        let id = Id::new_v7().unwrap();
        let memory = MemoryBudget::new(crate::budget::MemoryLimits {
            total_bytes: 128 * 1024 * 1024,
            control_reserved_bytes: 16 * 1024 * 1024,
        })
        .unwrap();
        (
            Budget::new(memory),
            Arc::new(Registry::default()),
            Key {
                principal: "credential:test".into(),
                session: id,
                member: id,
                epoch: 1,
                root: id,
                operation: id,
            },
        )
    }
    fn charge(budget: &Budget, key: &Key) -> Charge {
        budget.reserve(None, key.member, 1, 4_096, true).unwrap()
    }

    #[tokio::test]
    async fn old_attempt_cancel_reply_and_cleanup_cannot_remove_a_replacement() {
        let (budget, registry, key) = setup();
        let deadline = Instant::now() + Duration::from_secs(1);
        let first = registry
            .register(key.clone(), [1; 32], 1, deadline, charge(&budget, &key))
            .unwrap();
        assert!(matches!(
            registry.register(key.clone(), [1; 32], 1, deadline, charge(&budget, &key)),
            Err(Refusal::Duplicate)
        ));
        let next = registry
            .register(
                key.clone(),
                [1; 32],
                2,
                deadline + Duration::from_secs(1),
                charge(&budget, &key),
            )
            .unwrap();
        assert!(first.cancelled.is_cancelled());
        assert_eq!(next.deadline, deadline);
        drop(first);
        assert!(
            !registry
                .cancel(&key.principal, key.session, key.root, key.operation, 1)
                .unwrap()
        );
        assert!(!next.cancelled.is_cancelled());
        assert!(
            registry
                .cancel(&key.principal, key.session, key.root, key.operation, 2)
                .unwrap()
        );
        assert!(next.cancelled.is_cancelled());
        assert!(matches!(
            registry.register(key.clone(), [1; 32], 2, deadline, charge(&budget, &key)),
            Err(Refusal::Stale)
        ));
        assert!(matches!(
            registry.register(key.clone(), [2; 32], 3, deadline, charge(&budget, &key)),
            Err(Refusal::Conflict)
        ));
    }

    #[test]
    fn tombstones_stay_charged_and_expire_at_the_original_deadline() {
        let (budget, registry, key) = setup();
        let initial = budget.0.memory.available_bytes();
        let registration = registry
            .register(
                key.clone(),
                [1; 32],
                1,
                Instant::now() + Duration::from_secs(1),
                charge(&budget, &key),
            )
            .unwrap();
        drop(registration);
        assert_eq!(budget.0.usage.lock().unwrap().targets, 1);
        let mut entries = registry.entries.lock().unwrap();
        Registry::expire(&mut entries, Instant::now() + Duration::from_secs(2));
        assert!(entries.is_empty());
        assert_eq!(budget.0.memory.available_bytes(), initial);
        assert_eq!(budget.0.usage.lock().unwrap().targets, 0);
    }

    #[test]
    fn member_waits_leave_probe_capacity_and_never_borrow_control_reserves() {
        let (budget, _, key) = setup();
        let core = budget.0.memory.available_core_bytes();
        let ack = budget.0.memory.available_ack_bytes();
        let mut charges = Vec::new();
        for _ in 0..7 {
            charges.push(budget.reserve(None, key.member, 32, 1_024, true).unwrap());
        }
        assert!(budget.reserve(None, key.member, 1, 1_024, true).is_none());
        charges.push(budget.reserve(None, key.member, 32, 1_024, false).unwrap());
        assert!(budget.reserve(None, key.member, 1, 1_024, false).is_none());
        assert_eq!(budget.0.memory.available_core_bytes(), core);
        assert_eq!(budget.0.memory.available_ack_bytes(), ack);
        drop(charges);
        assert_eq!(budget.0.usage.lock().unwrap().targets, 0);
    }
}
