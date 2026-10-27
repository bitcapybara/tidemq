//! Volatile bounded target revisions, driven by committed apply and read-only rechecks.

use super::*;
use crate::server::group_watch::{Budget, Charge};
use bytes::Bytes;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tidemq_protocol::public::WatchStatus;
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct WatchKey {
    pub partition: PartitionKey,
    pub shard: Id,
    pub subscription: Id,
    pub member: Id,
    pub epoch: u64,
    pub plan: u64,
    pub installation: u64,
    pub route_epoch: u64,
    pub route_generation: u64,
    pub leader_term: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Dependency {
    Shard(PartitionKey),
    Cursor(PartitionKey, Id),
    Quota(PartitionKey),
    Results,
    Windows,
    Bytes,
}

struct Projection {
    local: [u8; 32],
    source: [u8; 32],
    ready: bool,
    due_ms: Option<u64>,
    dependencies: BTreeSet<Dependency>,
}

pub(crate) struct WatchSample {
    pub status: WatchStatus,
    pub token: Bytes,
    pub due_ms: Option<u64>,
    pub signal: Arc<Notify>,
}

struct Target {
    incarnation: Id,
    revision: u64,
    local: [u8; 32],
    source: [u8; 32],
    ready: bool,
    dependencies: Vec<Dependency>,
    signal: Arc<Notify>,
    _charge: Arc<Charge>,
}

struct CachePage {
    remaining: usize,
    charge: Arc<Charge>,
}

#[derive(Default)]
struct Observations {
    targets: BTreeMap<Arc<WatchKey>, Target>,
    dependencies: BTreeMap<Dependency, BTreeSet<Arc<WatchKey>>>,
    budget: Option<Budget>,
    invalidated: bool,
    pending: BTreeSet<Arc<WatchKey>>,
    pages: BTreeMap<Option<Id>, CachePage>,
    #[cfg(feature = "test-hooks")]
    partition_commands: BTreeMap<PartitionKey, u64>,
    #[cfg(feature = "test-hooks")]
    unscoped_commands: u64,
}

#[derive(Default)]
pub(crate) struct WatchHub(Mutex<Observations>);

pub(crate) struct WatchSignal {
    dependencies: BTreeSet<Dependency>,
    before: (usize, usize, u64),
    capacity: bool,
    reset: bool,
    read: Option<(PartitionKey, Id, crate::subscription::GroupRead)>,
    retire: bool,
    #[cfg(feature = "test-hooks")]
    pub(crate) partition: Option<PartitionKey>,
}

impl WatchHub {
    #[cfg(feature = "test-hooks")]
    pub(crate) fn diagnostic_commands(&self) -> (Vec<(PartitionKey, u64)>, u64) {
        self.0
            .lock()
            .map(|observations| {
                (
                    observations
                        .partition_commands
                        .iter()
                        .map(|(partition, count)| (*partition, *count))
                        .collect(),
                    observations.unscoped_commands,
                )
            })
            .unwrap_or_else(|_| (Vec::new(), u64::MAX))
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn committed_command(&self, partition: Option<PartitionKey>) {
        if let Ok(mut observations) = self.0.lock() {
            if let Some(partition) = partition
                && (observations.partition_commands.len() < 8_192
                    || observations.partition_commands.contains_key(&partition))
            {
                let count = observations
                    .partition_commands
                    .entry(partition)
                    .or_default();
                *count = count.saturating_add(1);
                return;
            }
            observations.unscoped_commands = observations.unscoped_commands.saturating_add(1);
        }
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn diagnostic(&self) -> (usize, usize, usize) {
        self.0
            .lock()
            .map(|observations| {
                (
                    observations.targets.len(),
                    observations.dependencies.values().map(BTreeSet::len).sum(),
                    observations.pending.len(),
                )
            })
            .unwrap_or((usize::MAX, usize::MAX, usize::MAX))
    }
    pub(crate) fn before(application: &DataApplication, command: &DataCommand) -> WatchSignal {
        let mut dependencies = BTreeSet::new();
        let mut capacity = false;
        let mut read = None;
        let mut retire = false;
        let reset = match command {
            DataCommand::ConsumerGroup {
                partition, command, ..
            } => {
                match command {
                    crate::subscription::GroupCommand::Read {
                        subscription,
                        request,
                        ..
                    } => read = Some((*partition, *subscription, *request)),
                    crate::subscription::GroupCommand::RetireRead { .. } => retire = true,
                    _ => {}
                }
                dependencies.insert(Dependency::Cursor(*partition, command.subscription()));
                dependencies.insert(Dependency::Quota(*partition));
                capacity = true;
                false
            }
            DataCommand::InstallQuota { partition, .. } => {
                dependencies.insert(Dependency::Quota(*partition));
                false
            }
            DataCommand::Publish { partition, .. }
            | DataCommand::Producer { partition, .. }
            | DataCommand::SealShard { partition, .. } => {
                dependencies.insert(Dependency::Shard(*partition));
                false
            }
            DataCommand::InstallFence(_)
            | DataCommand::AuthorityTransfer(_)
            | DataCommand::Recover { .. } => true,
            _ => false,
        };
        WatchSignal {
            dependencies,
            before: if capacity {
                application.watch_capacities().unwrap_or_default()
            } else {
                (0, 0, 0)
            },
            capacity,
            reset,
            read,
            retire,
            #[cfg(feature = "test-hooks")]
            partition: match command {
                DataCommand::InstallQuota { partition, .. }
                | DataCommand::Publish { partition, .. }
                | DataCommand::RetirePublishes { partition, .. }
                | DataCommand::CleanupPublisher { partition, .. }
                | DataCommand::SealShard { partition, .. }
                | DataCommand::ConsumerGroup { partition, .. }
                | DataCommand::Retention { partition, .. }
                | DataCommand::Recover { partition, .. }
                | DataCommand::Producer { partition, .. }
                | DataCommand::ColdSubscription { partition, .. }
                | DataCommand::Subscription { partition, .. } => Some(*partition),
                DataCommand::InstallFence(command) => Some(command.grant.partition),
                DataCommand::Initialize(_)
                | DataCommand::AuthorityTransfer(_)
                | DataCommand::Lifecycle(_) => None,
            },
        }
    }

    /// Called while the application write lock still protects this exact applied order.
    pub(crate) fn applied(&self, application: &DataApplication, mut change: WatchSignal) {
        if change.reset {
            if let Ok(mut observations) = self.0.lock() {
                observations.invalidated = true;
                let keys = observations.targets.keys().cloned().collect::<Vec<_>>();
                observations.pending.extend(keys);
            }
            return;
        }
        if change.capacity
            && let Ok(after) = application.watch_capacities()
        {
            if after.0 > change.before.0 {
                change.dependencies.insert(Dependency::Results);
            }
            if after.1 > change.before.1 {
                change.dependencies.insert(Dependency::Windows);
            }
            if after.2 > change.before.2 {
                change.dependencies.insert(Dependency::Bytes);
            }
        }
        let empty_read = change
            .read
            .is_some_and(|(partition, subscription, request)| {
                application
                    .consumer_group_cursor(partition, subscription)
                    .and_then(|cursor| cursor.original_read(&request))
                    .is_some_and(|result| result.is_ok_and(|read| read.deliveries.is_empty()))
            });
        if let Ok(mut observations) = self.0.lock() {
            if let Some(budget) = &observations.budget {
                if change.read.is_some() {
                    budget.metrics().reads.inc();
                    if empty_read {
                        budget.metrics().empty_reads.inc();
                    }
                }
                if change.retire {
                    budget.metrics().retires.inc();
                }
                if change.dependencies.contains(&Dependency::Results) {
                    budget.metrics().capacity_releases.inc();
                }
            }
            let mut affected = BTreeSet::new();
            for dependency in change.dependencies {
                if let Some(keys) = observations.dependencies.get(&dependency) {
                    affected.extend(keys.iter().cloned());
                }
            }
            for key in affected {
                if let Some(target) = observations.targets.get_mut(&key) {
                    if empty_read
                        && change
                            .read
                            .is_some_and(|(partition, subscription, request)| {
                                key.partition == partition
                                    && key.subscription == subscription
                                    && key.member == request.member
                                    && key.epoch == request.epoch
                            })
                    {
                        // An authoritative empty result preserves a blocked-to-ready transition even
                        // when capacity returns before the next observation registers its dependencies.
                        target.ready = false;
                    }
                    observations.pending.insert(key);
                }
            }
        }
    }

    /// Notification runs after dropping the application lock; no handler executes under apply.
    pub(crate) fn notify(&self) {
        if let Ok(mut observations) = self.0.lock() {
            for key in std::mem::take(&mut observations.pending) {
                if let Some(target) = observations.targets.get(&key) {
                    target.signal.notify_waiters();
                }
            }
            if observations.invalidated {
                observations.targets.clear();
                observations.dependencies.clear();
                observations.pages.clear();
                observations.invalidated = false;
            }
        }
    }

    pub(crate) fn reset(&self) {
        if let Ok(mut observations) = self.0.lock() {
            for target in observations.targets.values() {
                target.signal.notify_waiters();
            }
            observations.targets.clear();
            observations.dependencies.clear();
            observations.pending.clear();
            observations.pages.clear();
            observations.invalidated = false;
        }
    }

    fn observe(
        &self,
        key: WatchKey,
        projection: Projection,
        token: &[u8],
        budget: &Budget,
        tenant: Option<Id>,
    ) -> Option<WatchSample> {
        let mut observations = self.0.lock().ok()?;
        if observations.invalidated {
            return None;
        }
        observations.budget = Some(budget.clone());
        budget.metrics().rechecks.inc();
        if !observations.targets.contains_key(&key) {
            let charge = Self::cache_charge(&mut observations, budget, tenant, key.member)?;
            observations.targets.insert(
                Arc::new(key),
                Target {
                    incarnation: Id::new_v7().ok()?,
                    revision: 1,
                    local: projection.local,
                    source: projection.source,
                    ready: projection.ready,
                    dependencies: Vec::new(),
                    signal: Arc::new(Notify::new()),
                    _charge: charge,
                },
            );
        }
        let target = observations.targets.get_mut(&key)?;
        if target.source != projection.source
            || !target.ready && projection.ready
            || target.local != projection.local && projection.ready
        {
            budget.metrics().dirty.inc();
            if let Some(revision) = target.revision.checked_add(1) {
                target.revision = revision;
            } else {
                target.incarnation = Id::new_v7().ok()?;
                target.revision = 1;
            }
        }
        target.local = projection.local;
        target.source = projection.source;
        target.ready = projection.ready;
        let signal = target.signal.clone();
        let previous = std::mem::replace(
            &mut target.dependencies,
            projection.dependencies.iter().copied().collect(),
        );
        let mut current = Vec::with_capacity(24);
        current.extend_from_slice(target.incarnation.as_bytes());
        current.extend_from_slice(&target.revision.to_be_bytes());
        let status = if token.is_empty() {
            WatchStatus::WatchSync
        } else if token == current {
            WatchStatus::WatchUnchanged
        } else if token.get(..16) == current.get(..16) {
            WatchStatus::WatchChanged
        } else {
            WatchStatus::WatchReset
        };
        // The applied lock prevents capacity changing between projection and dependency registration.
        let key = observations.targets.get_key_value(&key)?.0.clone();
        for dependency in &projection.dependencies {
            observations
                .dependencies
                .entry(*dependency)
                .or_default()
                .insert(key.clone());
        }
        for dependency in previous
            .iter()
            .filter(|dependency| !projection.dependencies.contains(dependency))
        {
            if let Some(keys) = observations.dependencies.get_mut(dependency) {
                keys.remove(&key);
                if keys.is_empty() {
                    observations.dependencies.remove(dependency);
                }
            }
        }
        Some(WatchSample {
            status,
            token: Bytes::from(current),
            due_ms: projection.due_ms,
            signal,
        })
    }

    fn cache_charge(
        observations: &mut Observations,
        budget: &Budget,
        tenant: Option<Id>,
        member: Id,
    ) -> Option<Arc<Charge>> {
        const PAGE: usize = 16;
        // Shared keys and page permits avoid one full identity and permit per dependency edge.
        const TARGET_BYTES: usize = 1_536;
        if let Some(page) = observations.pages.get_mut(&tenant) {
            let charge = page.charge.clone();
            page.remaining = page.remaining.checked_sub(1)?;
            if page.remaining == 0 {
                observations.pages.remove(&tenant);
            }
            return Some(charge);
        }
        let mut charge = budget.reserve_cached(tenant, member, PAGE, PAGE * TARGET_BYTES);
        if charge.is_none() {
            let expired = observations
                .targets
                .values()
                .filter(|target| target._charge.tenant() == tenant)
                .find(|target| {
                    observations
                        .targets
                        .values()
                        .filter(|other| Arc::ptr_eq(&other._charge, &target._charge))
                        .all(|other| Arc::strong_count(&other.signal) == 1)
                })
                .map(|target| target._charge.clone())?;
            let keys = observations
                .targets
                .iter()
                .filter(|(_, target)| Arc::ptr_eq(&target._charge, &expired))
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            for key in keys {
                Self::remove(observations, key);
            }
            observations
                .pages
                .retain(|_, page| !Arc::ptr_eq(&page.charge, &expired));
            drop(expired);
            charge = budget.reserve_cached(tenant, member, PAGE, PAGE * TARGET_BYTES);
        }
        let charge = Arc::new(charge?);
        observations.pages.insert(
            tenant,
            CachePage {
                remaining: PAGE - 1,
                charge: charge.clone(),
            },
        );
        Some(charge)
    }

    fn remove(observations: &mut Observations, key: Arc<WatchKey>) {
        if let Some(target) = observations.targets.remove(&key) {
            for dependency in target.dependencies {
                if let Some(keys) = observations.dependencies.get_mut(&dependency) {
                    keys.remove(&key);
                    if keys.is_empty() {
                        observations.dependencies.remove(&dependency);
                    }
                }
            }
        }
    }
}

impl DataApplication {
    pub(crate) fn watch_hub(&self) -> Arc<WatchHub> {
        self.watch.clone()
    }

    pub(crate) fn watch_sample(
        &self,
        key: WatchKey,
        actor: (Id, Id),
        token: &[u8],
        now_ms: u64,
        budget: &Budget,
        tenant: Option<Id>,
    ) -> Result<WatchSample, DataRejection> {
        let (client, credential) = actor;
        let state = self
            .partitions
            .get(&key.partition)
            .ok_or(DataRejection::PartitionMissing)?;
        let cursor = state
            .groups
            .get(&key.subscription)
            .ok_or(DataRejection::Subscription(
                subscription::Rejection::NotFound,
            ))?;
        if cursor.identity().shard != key.shard {
            return Err(DataRejection::Subscription(
                subscription::Rejection::InvalidContext,
            ));
        }
        let view = cursor
            .watch_state(
                key.member,
                key.epoch,
                client,
                credential,
                state.producer.committed_end(),
                now_ms,
            )
            .map_err(DataRejection::Subscription)?;
        let installed = key
            .plan
            .checked_mul(2)
            .and_then(|value| value.checked_add(u64::from(view.version % 2 == 1)));
        if installed != Some(view.version) || view.version != key.installation {
            return Err(DataRejection::Subscription(
                subscription::Rejection::StaleGeneration,
            ));
        }
        let mut dependencies = BTreeSet::from([
            Dependency::Shard(key.partition),
            Dependency::Cursor(key.partition, key.subscription),
        ]);
        let capacities = self.watch_capacities()?;
        let mut ready = view.eligible && view.candidate_bytes.is_some();
        let mut due_ms = view.due_ms;
        if view.candidate_bytes.is_some() {
            if capacities.0 == 0 {
                dependencies.insert(Dependency::Results);
                ready = false;
            }
            if view.unseen && capacities.1 == 0 {
                dependencies.insert(Dependency::Windows);
                ready = false;
            }
            let bytes = view.candidate_bytes.unwrap_or(1);
            if capacities.2 < bytes {
                dependencies.insert(Dependency::Bytes);
                ready = false;
            }
            let quota = state
                .quota
                .as_ref()
                .ok_or(DataRejection::Quota(QuotaRejection::Missing))?;
            let usage = state.quota_usage()?;
            let available = quota.read_budget(usage, now_ms);
            if !available
                .as_ref()
                .is_ok_and(|budget| budget.bytes >= bytes && budget.messages > 0)
            {
                dependencies.insert(Dependency::Quota(key.partition));
                ready = false;
                if let Some(refill_due) = quota.watch_refill_due(usage, bytes, now_ms) {
                    due_ms = Some(due_ms.map_or(refill_due, |due| due.min(refill_due)));
                }
            }
        }
        self.watch
            .observe(
                key,
                Projection {
                    local: view.fingerprint,
                    source: {
                        let mut hash = blake3::Hasher::new();
                        hash.update(&state.producer.committed_end().to_be_bytes());
                        hash.update(&view.version.to_be_bytes());
                        if let Some((shard, end)) = state.seal {
                            hash.update(shard.as_bytes());
                            hash.update(&end.to_be_bytes());
                        }
                        *hash.finalize().as_bytes()
                    },
                    ready,
                    due_ms,
                    dependencies,
                },
                token,
                budget,
                tenant,
            )
            .ok_or_else(|| {
                budget.metrics().admission_rejected.inc();
                DataRejection::Subscription(subscription::Rejection::WindowFull)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn budget() -> Budget {
        Budget::new(
            crate::budget::MemoryBudget::new(crate::budget::MemoryLimits {
                total_bytes: 128 * 1024 * 1024,
                control_reserved_bytes: 16 * 1024 * 1024,
            })
            .unwrap(),
        )
    }
    fn key() -> WatchKey {
        let id = Id::new_v7().unwrap();
        WatchKey {
            partition: PartitionKey {
                topic_id: id,
                partition: 0,
            },
            shard: id,
            subscription: id,
            member: id,
            epoch: 1,
            plan: 1,
            installation: 2,
            route_epoch: 1,
            route_generation: 1,
            leader_term: 1,
        }
    }
    fn projection(dependencies: BTreeSet<Dependency>, ready: bool) -> Projection {
        Projection {
            local: [1; 32],
            source: [2; 32],
            ready,
            due_ms: None,
            dependencies,
        }
    }

    #[test]
    fn resource_rechecks_do_not_dirty_idle_targets_or_partially_unblocked_targets() {
        let hub = WatchHub::default();
        let budget = budget();
        let idle = key();
        let mut blocked = idle;
        blocked.subscription = Id::new_v7().unwrap();
        let first = hub
            .observe(
                idle,
                projection(BTreeSet::from([Dependency::Shard(idle.partition)]), false),
                &[],
                &budget,
                None,
            )
            .unwrap();
        let waiting = hub
            .observe(
                blocked,
                projection(
                    BTreeSet::from([Dependency::Results, Dependency::Bytes]),
                    false,
                ),
                &[],
                &budget,
                None,
            )
            .unwrap();
        for _ in 0..1_000 {
            assert_eq!(
                hub.observe(
                    idle,
                    projection(BTreeSet::from([Dependency::Shard(idle.partition)]), false),
                    &first.token,
                    &budget,
                    None
                )
                .unwrap()
                .status,
                WatchStatus::WatchUnchanged
            );
            assert_eq!(
                hub.observe(
                    blocked,
                    projection(BTreeSet::from([Dependency::Bytes]), false),
                    &waiting.token,
                    &budget,
                    None
                )
                .unwrap()
                .status,
                WatchStatus::WatchUnchanged
            );
        }
        assert_eq!(
            hub.observe(
                blocked,
                projection(BTreeSet::new(), true),
                &waiting.token,
                &budget,
                None
            )
            .unwrap()
            .status,
            WatchStatus::WatchChanged
        );
    }

    #[tokio::test]
    async fn snapshot_reset_notifies_waiters_and_replaces_old_incarnations() {
        let hub = WatchHub::default();
        let budget = budget();
        let key = key();
        let dependencies = BTreeSet::from([Dependency::Shard(key.partition)]);
        let first = hub
            .observe(
                key,
                projection(dependencies.clone(), false),
                &[],
                &budget,
                None,
            )
            .unwrap();
        let notified = first.signal.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        assert!(budget.metrics().bytes.get() > 0);

        hub.reset();
        tokio::time::timeout(std::time::Duration::from_secs(1), notified)
            .await
            .expect("Reset did not wake the registered observer");
        assert_eq!(budget.metrics().bytes.get(), 0);
        assert_eq!(budget.metrics().targets.get(), 0);
        let reset = hub
            .observe(
                key,
                projection(dependencies, false),
                &first.token,
                &budget,
                None,
            )
            .unwrap();
        assert_eq!(reset.status, WatchStatus::WatchReset);
        assert_ne!(reset.token.get(..16), first.token.get(..16));
        hub.reset();
        assert_eq!(budget.metrics().bytes.get(), 0);
        assert_eq!(budget.metrics().targets.get(), 0);
    }

    #[test]
    fn revision_overflow_replaces_the_incarnation_instead_of_wrapping() {
        let hub = WatchHub::default();
        let budget = budget();
        let key = key();
        let first = hub
            .observe(key, projection(BTreeSet::new(), false), &[], &budget, None)
            .unwrap();
        let mut previous = first.token.get(..16).unwrap().to_vec();
        previous.extend_from_slice(&u64::MAX.to_be_bytes());
        hub.0
            .lock()
            .unwrap()
            .targets
            .get_mut(&key)
            .unwrap()
            .revision = u64::MAX;

        let reset = hub
            .observe(
                key,
                projection(BTreeSet::new(), true),
                &previous,
                &budget,
                None,
            )
            .unwrap();
        assert_eq!(reset.status, WatchStatus::WatchReset);
        assert_ne!(reset.token.get(..16), previous.get(..16));
        assert_eq!(reset.token.get(16..), Some(1_u64.to_be_bytes().as_slice()));
        assert_eq!(budget.metrics().dirty.get(), 1);
        hub.reset();
        assert_eq!(budget.metrics().bytes.get(), 0);
    }

    #[test]
    fn cache_pressure_preserves_active_waiters_and_eviction_resets_old_tokens() {
        let hub = WatchHub::default();
        let budget = budget();
        let key = key();
        let first = hub
            .observe(
                key,
                projection(BTreeSet::from([Dependency::Results]), false),
                &[],
                &budget,
                None,
            )
            .unwrap();
        let previous = first.token.clone();
        for _ in 1..16 {
            let mut other = key;
            other.shard = Id::new_v7().unwrap();
            drop(
                hub.observe(
                    other,
                    projection(BTreeSet::new(), false),
                    &[],
                    &budget,
                    None,
                )
                .unwrap(),
            );
        }
        let cache_bytes = usize::try_from(budget.metrics().bytes.get()).unwrap();
        let tenant_maximum = (128 - 16) * 1024 * 1024 / 16 / 4;
        let pressure = budget
            .reserve(None, key.member, 0, tenant_maximum - cache_bytes, false)
            .unwrap();
        let mut next = key;
        next.shard = Id::new_v7().unwrap();
        assert!(
            hub.observe(next, projection(BTreeSet::new(), false), &[], &budget, None)
                .is_none()
        );
        assert_eq!(budget.metrics().targets.get(), 16);

        drop(first);
        let admitted = hub
            .observe(next, projection(BTreeSet::new(), false), &[], &budget, None)
            .unwrap();
        assert_eq!(admitted.status, WatchStatus::WatchSync);
        assert_eq!(budget.metrics().targets.get(), 16);
        let reset = hub
            .observe(
                key,
                projection(BTreeSet::new(), false),
                &previous,
                &budget,
                None,
            )
            .unwrap();
        assert_eq!(reset.status, WatchStatus::WatchReset);
        drop(pressure);
        hub.reset();
        assert_eq!(budget.metrics().bytes.get(), 0);
        assert_eq!(budget.metrics().targets.get(), 0);
        let observations = hub.0.lock().unwrap();
        assert!(observations.dependencies.is_empty());
        assert!(observations.pages.is_empty());
    }
}
