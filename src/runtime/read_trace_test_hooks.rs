//! Bounded, payload-free snapshots of actual committed Read admission in test builds.

use crate::{metadata::PartitionKey, subscription::GroupIdentity};
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    sync::{Arc, Mutex},
};
use tidemq_protocol::Id;

#[derive(Clone, Debug, serde::Serialize)]
pub struct ReadTraceMember {
    pub active: bool,
    pub expires_ms: u64,
    pub in_flight_messages: u32,
    pub in_flight_bytes: u64,
    pub max_messages: u32,
    pub max_bytes: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct ReadTraceEvent {
    pub subscription: Id,
    pub member: Id,
    pub epoch: u64,
    pub sequence: u64,
    pub now_ms: u64,
    pub budget_messages: u64,
    pub budget_bytes: u64,
    pub budget_results: u32,
    pub cursor_next: u64,
    pub producer_next: u64,
    pub cursor_windows: usize,
    pub ready: usize,
    pub eligible_ready: usize,
    pub candidates: usize,
    pub candidate_min_bytes: Option<u64>,
    pub group_windows: usize,
    pub group_reads: usize,
    pub group_bytes: u64,
    pub member_state: Option<ReadTraceMember>,
    pub deliveries: Option<usize>,
}

#[derive(Default)]
struct Events {
    rows: VecDeque<ReadTraceEvent>,
    overflowed: bool,
}
struct Trace {
    cluster: Id,
    partition: PartitionKey,
    events: Mutex<Events>,
}
type Key = (Id, Id);
static TRACES: Mutex<BTreeMap<Key, Arc<Trace>>> = Mutex::new(BTreeMap::new());

pub struct ReadTrace {
    key: Key,
    trace: Arc<Trace>,
}
impl ReadTrace {
    pub fn install(cluster: Id, partition: PartitionKey, shard: Id) -> io::Result<Self> {
        let key = (partition.topic_id, shard);
        let mut traces = TRACES
            .lock()
            .map_err(|_| io::Error::other("Read trace registry is poisoned"))?;
        if traces.len() >= 16 || traces.contains_key(&key) {
            return Err(io::Error::other(
                "Read trace target is already installed or full",
            ));
        }
        let trace = Arc::new(Trace {
            cluster,
            partition,
            events: Mutex::new(Events::default()),
        });
        traces.insert(key, trace.clone());
        Ok(Self { key, trace })
    }

    pub fn events(&self) -> Vec<ReadTraceEvent> {
        self.trace
            .events
            .lock()
            .map(|events| events.rows.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn overflowed(&self) -> bool {
        self.trace
            .events
            .lock()
            .map_or(true, |events| events.overflowed)
    }

    pub fn scope(&self) -> (Id, PartitionKey) {
        (self.trace.cluster, self.trace.partition)
    }
}
impl Drop for ReadTrace {
    fn drop(&mut self) {
        if let Ok(mut traces) = TRACES.lock()
            && traces
                .get(&self.key)
                .is_some_and(|trace| Arc::ptr_eq(trace, &self.trace))
        {
            traces.remove(&self.key);
        }
    }
}

pub(crate) fn enabled(identity: GroupIdentity) -> bool {
    TRACES
        .lock()
        .is_ok_and(|traces| traces.contains_key(&(identity.topic, identity.shard)))
}

pub(crate) fn record(identity: GroupIdentity, event: ReadTraceEvent) {
    let trace = TRACES
        .lock()
        .ok()
        .and_then(|traces| traces.get(&(identity.topic, identity.shard)).cloned());
    if let Some(trace) = trace
        && let Ok(mut events) = trace.events.lock()
    {
        if events.rows.len() >= 256 {
            events.overflowed = true;
        } else {
            events.rows.push_back(event);
        }
    }
}
