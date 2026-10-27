//! Deterministic Watch registration races; not present in production builds.

#[path = "watch_authority_test_hooks.rs"]
mod authority;
pub use super::read_trace_test_hooks::{ReadTrace, ReadTraceEvent, ReadTraceMember};
pub use authority::{AuthorizeQuorumPause, CoreHandlerLimit};
pub(super) use authority::{before_authority_quorum, core_handlers};

use crate::metadata::PartitionKey;
use crate::{
    broker::DataApplication,
    replication::DiskStateMachine,
    server::group_watch::{Budget, Registry},
};
use std::collections::{BTreeMap, VecDeque};
use tidemq_protocol::Id;

#[derive(Clone)]
pub struct Diagnostics {
    budget: Budget,
    registry: Arc<Registry>,
    groups: BTreeMap<u64, DiskStateMachine<DataApplication>>,
    cluster: Id,
    node: u64,
}
#[derive(Debug, serde::Serialize)]
pub struct Snapshot {
    pub charged_bytes: usize,
    pub target_contexts: usize,
    pub forwards: usize,
    pub cleanup_tasks: usize,
    pub active_requests: usize,
    pub cancellation_watermarks: usize,
    pub timers: usize,
    pub group_reads: u64,
    pub group_retires: u64,
    pub empty_reads: u64,
    pub dirty: u64,
    pub capacity_releases: u64,
    pub groups: Vec<GroupSnapshot>,
}
#[derive(Debug, serde::Serialize)]
pub struct GroupSnapshot {
    pub group: u64,
    pub applied_index: Option<u64>,
    pub cached_targets: usize,
    pub dependency_edges: usize,
    pub queued_rechecks: usize,
    pub available_read_results: Option<usize>,
    pub partition_commands: Vec<(PartitionKey, u64)>,
    pub unscoped_commands: u64,
}
impl super::RunningNode {
    pub async fn evict_watch_leader_hint(&self, group: u64) -> Option<u64> {
        self.service
            .leaders
            .write()
            .await
            .remove(&group)
            .map(|(leader, _)| leader)
    }

    pub fn watch_diagnostics(&self) -> Diagnostics {
        Diagnostics {
            budget: self.service.watch_budget.clone(),
            registry: self.service.watch_registry.clone(),
            groups: self
                .service
                .data
                .iter()
                .map(|(group, replica)| (*group, replica.state.clone()))
                .collect(),
            cluster: self.service.cluster,
            node: self.service.config.node_id,
        }
    }
}
impl Diagnostics {
    /// Immediate local applied state; this diagnostic projection does not establish a quorum read.
    pub async fn snapshot(&self) -> Snapshot {
        let (charged_bytes, target_contexts, forwards) = self.budget.diagnostic();
        let (active_requests, cancellation_watermarks, timers) = self.registry.diagnostic();
        let cleanup_tasks = self.registry.cleanup_count();
        let mut groups = Vec::new();
        for (group, state) in &self.groups {
            groups.push(
                state
                    .with_applied(|application, applied| {
                        let (cached_targets, dependency_edges, queued_rechecks) =
                            application.watch_hub().diagnostic();
                        let (partition_commands, unscoped_commands) =
                            application.watch_hub().diagnostic_commands();
                        GroupSnapshot {
                            group: *group,
                            applied_index: applied.map(|log| log.index),
                            cached_targets,
                            dependency_edges,
                            queued_rechecks,
                            available_read_results: application
                                .watch_capacities()
                                .ok()
                                .map(|capacity| capacity.0),
                            partition_commands,
                            unscoped_commands,
                        }
                    })
                    .await,
            );
        }
        let metrics = self.budget.metrics();
        Snapshot {
            charged_bytes,
            target_contexts,
            forwards,
            cleanup_tasks,
            active_requests,
            cancellation_watermarks,
            timers,
            groups,
            group_reads: metrics.reads.get(),
            group_retires: metrics.retires.get(),
            empty_reads: metrics.empty_reads.get(),
            dirty: metrics.dirty.get(),
            capacity_releases: metrics.capacity_releases.get(),
        }
    }
    pub fn events(&self) -> Vec<WatchEvent> {
        EVENTS
            .lock()
            .map(|events| {
                events
                    .iter()
                    .filter(|event| event.cluster == self.cluster && event.node == self.node)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub enum WatchPhase {
    Registered,
    Forward,
    Complete,
    Cancelled,
    CancelUnknown,
    Refused,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct WatchEvent {
    pub cluster: Id,
    pub node: u64,
    pub group: u64,
    pub root: Id,
    pub parent: Option<Id>,
    pub operation: Id,
    pub attempt: u64,
    pub targets: usize,
    pub phase: WatchPhase,
}
static EVENTS: Mutex<VecDeque<WatchEvent>> = Mutex::new(VecDeque::new());
pub(super) fn record(
    cluster: Id,
    node: u64,
    group: u64,
    operation: Id,
    request: &tidemq_protocol::public::GroupWatch,
    phase: WatchPhase,
) {
    let Ok(root) = Id::from_slice(&request.root_operation_id) else {
        return;
    };
    event(WatchEvent {
        cluster,
        node,
        group,
        operation,
        root,
        parent: Id::from_slice(&request.parent_operation_id).ok(),
        attempt: request.attempt,
        targets: request.targets.len(),
        phase,
    });
}
pub(super) fn event(event: WatchEvent) {
    if let Ok(mut events) = EVENTS.lock() {
        if events.len() >= 4_096 {
            events.pop_front();
        }
        events.push_back(event);
    }
}

static LIMITS: Mutex<BTreeMap<(Id, u64), u32>> = Mutex::new(BTreeMap::new());
pub struct ReadResultLimit {
    cluster: Id,
    group: u64,
}
impl ReadResultLimit {
    pub fn install(cluster: Id, group: u64, slots: u32) -> io::Result<Self> {
        if group == 0 || !(2..=4_096).contains(&slots) {
            return Err(io::Error::other(
                "Watch fixture result slots must be between 2 and 4096",
            ));
        }
        let mut limits = LIMITS
            .lock()
            .map_err(|_| io::Error::other("Watch fixture limits are poisoned"))?;
        if limits.len() >= 16 || limits.contains_key(&(cluster, group)) {
            return Err(io::Error::other(
                "Watch fixture result limit is already installed or full",
            ));
        }
        limits.insert((cluster, group), slots);
        Ok(Self { cluster, group })
    }
}
impl Drop for ReadResultLimit {
    fn drop(&mut self) {
        if let Ok(mut limits) = LIMITS.lock() {
            limits.remove(&(self.cluster, self.group));
        }
    }
}
pub fn subscription_limits(
    cluster: Id,
    group: u64,
    mut limits: crate::subscription::Limits,
) -> crate::subscription::Limits {
    if let Some(slots) = LIMITS
        .lock()
        .ok()
        .and_then(|limits| limits.get(&(cluster, group)).copied())
    {
        limits.max_total_window_slots = slots;
        limits.max_window_slots = limits.max_window_slots.min(slots);
        limits.max_in_flight_messages = limits.max_in_flight_messages.min(slots);
    }
    limits
}

type TopicPlacement = BTreeMap<(Id, String), Vec<u64>>;
static TOPIC_GROUPS: Mutex<TopicPlacement> = Mutex::new(BTreeMap::new());

/// Overrides only the initial placement passed through the ordinary durable topic creation path.
pub struct InitialTopicGroups {
    cluster: Id,
    name: String,
}
impl InitialTopicGroups {
    pub fn install(cluster: Id, name: String, groups: Vec<u64>) -> io::Result<Self> {
        if name.is_empty()
            || name.len() > 256
            || groups.is_empty()
            || groups.len() > 8_192
            || groups.contains(&0)
        {
            return Err(io::Error::other(
                "Initial topic placement violates fixture bounds",
            ));
        }
        let mut placements = TOPIC_GROUPS
            .lock()
            .map_err(|_| io::Error::other("Initial topic placements are poisoned"))?;
        let key = (cluster, name.clone());
        let total = placements.values().map(Vec::len).sum::<usize>();
        if placements.len() >= 16
            || total.saturating_add(groups.len()) > 8_192
            || placements.contains_key(&key)
        {
            return Err(io::Error::other(
                "Initial topic placement is already installed or full",
            ));
        }
        placements.insert(key, groups);
        Ok(Self { cluster, name })
    }
}
impl Drop for InitialTopicGroups {
    fn drop(&mut self) {
        if let Ok(mut placements) = TOPIC_GROUPS.lock() {
            placements.remove(&(self.cluster, self.name.clone()));
        }
    }
}
pub(super) fn initial_topic_groups(cluster: Id, name: &str, defaults: Vec<u64>) -> Vec<u64> {
    TOPIC_GROUPS
        .lock()
        .ok()
        .and_then(|placements| placements.get(&(cluster, name.to_owned())).cloned())
        .unwrap_or(defaults)
}
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{sync::Notify, time::Instant};

enum PauseTarget {
    Partition(PartitionKey),
    Group(Id, u64),
}
struct Hook {
    target: PauseTarget,
    entered: Notify,
    resume: Notify,
    released: AtomicBool,
}
static PAUSE: Mutex<Option<Arc<Hook>>> = Mutex::new(None);
pub struct BeforeWait(Arc<Hook>);

impl BeforeWait {
    pub fn install(partition: PartitionKey) -> io::Result<Self> {
        Self::install_target(PauseTarget::Partition(partition))
    }
    pub fn install_group(cluster: Id, group: u64) -> io::Result<Self> {
        Self::install_target(PauseTarget::Group(cluster, group))
    }
    fn install_target(target: PauseTarget) -> io::Result<Self> {
        let mut slot = PAUSE
            .lock()
            .map_err(|_| io::Error::other("Watch test hook is poisoned"))?;
        if slot.is_some() {
            return Err(io::Error::other("A Watch test pause is already installed"));
        }
        let hook = Arc::new(Hook {
            target,
            entered: Notify::new(),
            resume: Notify::new(),
            released: AtomicBool::new(false),
        });
        *slot = Some(hook.clone());
        Ok(Self(hook))
    }
    pub async fn wait(&self, deadline: Instant) -> io::Result<()> {
        tokio::time::timeout_at(deadline, self.0.entered.notified())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Watch did not reach its registration pause",
                )
            })
    }
    pub fn resume(&self) {
        self.0.released.store(true, Ordering::Release);
        self.0.resume.notify_one();
    }
}
impl Drop for BeforeWait {
    fn drop(&mut self) {
        if let Ok(mut slot) = PAUSE.lock()
            && slot.as_ref().is_some_and(|hook| Arc::ptr_eq(hook, &self.0))
        {
            slot.take();
        }
        self.resume();
    }
}
pub(super) async fn before_wait(
    cluster: Id,
    group: u64,
    partition: PartitionKey,
    deadline: Instant,
) {
    let hook = PAUSE.lock().ok().and_then(|slot| {
        slot.as_ref()
            .filter(|hook| match hook.target {
                PauseTarget::Partition(expected) => expected == partition,
                PauseTarget::Group(expected_cluster, expected_group) => {
                    expected_cluster == cluster && expected_group == group
                }
            })
            .cloned()
    });
    if let Some(hook) = hook {
        if hook.released.load(Ordering::Acquire) {
            return;
        }
        hook.entered.notify_one();
        let _ = tokio::time::timeout_at(deadline, hook.resume.notified()).await;
    }
}
