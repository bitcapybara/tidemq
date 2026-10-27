//! Charged, bounded tenant/member round robin for observation work only.

use super::Charge;
use prometheus_client::metrics::gauge::Gauge;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
};
use tidemq_protocol::Id;
use tokio::{
    sync::oneshot,
    time::{Instant, timeout_at},
};

type Tenant = Option<Id>;

#[derive(Clone)]
pub(crate) struct Queue(Arc<Inner>);

struct Inner {
    state: Mutex<State>,
    queued: Gauge,
}

struct State {
    limit: usize,
    next: u64,
    closed: bool,
    entries: BTreeMap<u64, Entry>,
    active: BTreeSet<u64>,
    tenants: VecDeque<Tenant>,
    queues: BTreeMap<Tenant, Members>,
}

#[derive(Default)]
struct Members {
    order: VecDeque<Id>,
    requests: BTreeMap<Id, VecDeque<u64>>,
}

struct Entry {
    tenant: Tenant,
    member: Id,
    ready: Option<oneshot::Sender<()>>,
}

pub(crate) struct Turn {
    queue: Queue,
    id: u64,
    _charge: Charge,
}

impl Queue {
    pub(crate) fn new(limit: usize, queued: Gauge) -> Self {
        Self(Arc::new(Inner {
            state: Mutex::new(State {
                limit,
                next: 0,
                closed: false,
                entries: BTreeMap::new(),
                active: BTreeSet::new(),
                tenants: VecDeque::new(),
                queues: BTreeMap::new(),
            }),
            queued,
        }))
    }

    pub(crate) async fn enter(
        &self,
        tenant: Tenant,
        member: Id,
        charge: Charge,
        deadline: Instant,
    ) -> Option<Turn> {
        let (id, ready) = self.enqueue(tenant, member)?;
        let turn = Turn {
            queue: self.clone(),
            id,
            _charge: charge,
        };
        match timeout_at(deadline, ready).await {
            Ok(Ok(())) => Some(turn),
            _ => None,
        }
    }

    fn enqueue(&self, tenant: Tenant, member: Id) -> Option<(u64, oneshot::Receiver<()>)> {
        let mut state = self.0.state.lock().ok()?;
        if state.closed
            || state.entries.len() >= 128
            || state
                .entries
                .values()
                .filter(|entry| entry.tenant == tenant)
                .count()
                >= 32
            || state
                .entries
                .values()
                .filter(|entry| entry.tenant == tenant && entry.member == member)
                .count()
                >= 8
        {
            return None;
        }
        let id = state.next.checked_add(1)?;
        state.next = id;
        let (send, receive) = oneshot::channel();
        state.entries.insert(
            id,
            Entry {
                tenant,
                member,
                ready: Some(send),
            },
        );
        if !state.queues.contains_key(&tenant) {
            state.tenants.push_back(tenant);
        }
        let members = state.queues.entry(tenant).or_default();
        if !members.requests.contains_key(&member) {
            members.order.push_back(member);
        }
        members.requests.entry(member).or_default().push_back(id);
        state.dispatch();
        self.0.queued.set(state.queued() as i64);
        Some((id, receive))
    }

    pub(crate) fn close(&self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.closed = true;
            state.queues.clear();
            state.tenants.clear();
            state.entries.retain(|_, entry| entry.ready.is_none());
            state.release_empty_storage();
            self.0.queued.set(0);
        }
    }
}

impl State {
    fn release_empty_storage(&mut self) {
        if self.entries.is_empty() {
            self.entries = BTreeMap::new();
            self.active = BTreeSet::new();
            self.queues = BTreeMap::new();
            self.tenants = VecDeque::new();
        }
    }

    fn queued(&self) -> usize {
        self.entries.len().saturating_sub(self.active.len())
    }

    fn next(&mut self) -> Option<u64> {
        let tenant = self.tenants.pop_front()?;
        let members = self.queues.get_mut(&tenant)?;
        let member = members.order.pop_front()?;
        let requests = members.requests.get_mut(&member)?;
        let id = requests.pop_front()?;
        if requests.is_empty() {
            members.requests.remove(&member);
        } else {
            members.order.push_back(member);
        }
        if members.order.is_empty() {
            self.queues.remove(&tenant);
        } else {
            self.tenants.push_back(tenant);
        }
        Some(id)
    }

    fn dispatch(&mut self) {
        while !self.closed && self.active.len() < self.limit {
            let Some(id) = self.next() else { break };
            let Some(ready) = self
                .entries
                .get_mut(&id)
                .and_then(|entry| entry.ready.take())
            else {
                continue;
            };
            self.active.insert(id);
            if ready.send(()).is_err() {
                self.active.remove(&id);
                self.entries.remove(&id);
            }
        }
    }

    fn remove_waiting(&mut self, id: u64, entry: &Entry) {
        if let Some(members) = self.queues.get_mut(&entry.tenant) {
            if let Some(requests) = members.requests.get_mut(&entry.member) {
                requests.retain(|request| *request != id);
                if requests.is_empty() {
                    members.requests.remove(&entry.member);
                    members.order.retain(|member| *member != entry.member);
                }
            }
            if members.order.is_empty() {
                self.queues.remove(&entry.tenant);
                self.tenants.retain(|tenant| *tenant != entry.tenant);
            }
        }
    }
}

impl Drop for Turn {
    fn drop(&mut self) {
        if let Ok(mut state) = self.queue.0.state.lock() {
            if let Some(entry) = state.entries.remove(&self.id) {
                state.remove_waiting(self.id, &entry);
            }
            state.active.remove(&self.id);
            state.dispatch();
            state.release_empty_storage();
            self.queue.0.queued.set(state.queued() as i64);
        }
    }
}

#[cfg(test)]
#[path = "group_watch_admission_tests.rs"]
mod tests;
