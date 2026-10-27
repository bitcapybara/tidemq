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
type Key = (Id, PartitionKey, Id);
static TRACES: Mutex<BTreeMap<Key, Arc<Trace>>> = Mutex::new(BTreeMap::new());

pub struct ReadTrace {
    key: Key,
    trace: Arc<Trace>,
}
impl ReadTrace {
    pub fn install(cluster: Id, partition: PartitionKey, shard: Id) -> io::Result<Self> {
        let key = (cluster, partition, shard);
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

pub(crate) fn enabled(scope: (Id, PartitionKey), identity: GroupIdentity) -> bool {
    identity.topic == scope.1.topic_id
        && TRACES
            .lock()
            .is_ok_and(|traces| traces.contains_key(&(scope.0, scope.1, identity.shard)))
}

pub(crate) fn record(scope: (Id, PartitionKey), identity: GroupIdentity, event: ReadTraceEvent) {
    if identity.topic != scope.1.topic_id {
        return;
    }
    let trace = TRACES
        .lock()
        .ok()
        .and_then(|traces| traces.get(&(scope.0, scope.1, identity.shard)).cloned());
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

#[cfg(test)]
mod tests {
    use super::*;

    fn event(identity: GroupIdentity, sequence: u64) -> ReadTraceEvent {
        ReadTraceEvent {
            subscription: identity.subscription,
            member: identity.subscription,
            epoch: 1,
            sequence,
            now_ms: 1,
            budget_messages: 1,
            budget_bytes: 1,
            budget_results: 1,
            cursor_next: 0,
            producer_next: 0,
            cursor_windows: 0,
            ready: 0,
            eligible_ready: 0,
            candidates: 0,
            candidate_min_bytes: None,
            group_windows: 0,
            group_reads: 0,
            group_bytes: 0,
            member_state: None,
            deliveries: Some(0),
        }
    }

    #[test]
    fn scopes_isolate_reused_ids_and_bound_registration_and_event_retention() {
        let cluster = Id::new_v7().unwrap();
        let other_cluster = Id::new_v7().unwrap();
        let partition = PartitionKey {
            topic_id: Id::new_v7().unwrap(),
            partition: 0,
        };
        let other_partition = PartitionKey {
            partition: 1,
            ..partition
        };
        let identity = GroupIdentity {
            topic: partition.topic_id,
            shard: Id::new_v7().unwrap(),
            subscription: Id::new_v7().unwrap(),
        };
        let scope = (cluster, partition);
        let trace = ReadTrace::install(cluster, partition, identity.shard).unwrap();
        assert!(enabled(scope, identity));
        assert!(!enabled((other_cluster, partition), identity));
        assert!(!enabled((cluster, other_partition), identity));
        record((other_cluster, partition), identity, event(identity, 99));
        record((cluster, other_partition), identity, event(identity, 98));
        assert!(trace.events().is_empty());

        let second = ReadTrace::install(other_cluster, partition, identity.shard).unwrap();
        let third = ReadTrace::install(cluster, other_partition, identity.shard).unwrap();
        record(scope, identity, event(identity, 1));
        record((other_cluster, partition), identity, event(identity, 2));
        record((cluster, other_partition), identity, event(identity, 3));
        assert_eq!(trace.events().first().unwrap().sequence, 1);
        assert_eq!(second.events().first().unwrap().sequence, 2);
        assert_eq!(third.events().first().unwrap().sequence, 3);
        assert_eq!(trace.events().len(), 1);
        assert_eq!(second.events().len(), 1);
        assert_eq!(third.events().len(), 1);

        let wrong_topic = GroupIdentity {
            topic: Id::new_v7().unwrap(),
            ..identity
        };
        assert!(!enabled(scope, wrong_topic));
        record(scope, wrong_topic, event(wrong_topic, 97));
        assert_eq!(trace.events().len(), 1);
        for sequence in 2..=256 {
            record(scope, identity, event(identity, sequence));
        }
        assert_eq!(trace.events().len(), 256);
        assert!(!trace.overflowed());
        record(scope, identity, event(identity, 257));
        assert!(trace.overflowed());
        assert_eq!(trace.events().last().unwrap().sequence, 256);

        drop(trace);
        assert!(!enabled(scope, identity));
        assert!(enabled((other_cluster, partition), identity));
        let replacement = ReadTrace::install(cluster, partition, identity.shard).unwrap();
        assert!(replacement.events().is_empty());
        assert!(!replacement.overflowed());
        assert!(ReadTrace::install(cluster, partition, identity.shard).is_err());
        let additional: Vec<_> = (0..13)
            .map(|_| ReadTrace::install(Id::new_v7().unwrap(), partition, identity.shard).unwrap())
            .collect();
        assert!(ReadTrace::install(Id::new_v7().unwrap(), partition, identity.shard).is_err());
        drop(additional);
        let released =
            ReadTrace::install(Id::new_v7().unwrap(), partition, identity.shard).unwrap();
        drop((replacement, second, third, released));
        assert!(!enabled(scope, identity));
    }
}
