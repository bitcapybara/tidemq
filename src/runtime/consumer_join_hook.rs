//! Bounded consumer Join barriers for integration tests; absent from default builds.
use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
};

use tidemq_protocol::Id;
use tokio::{
    sync::{Notify, watch},
    time::{Instant, timeout_at},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Point {
    BeforeCommit,
    Reply,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Identity {
    pub operation: Id,
    pub topic: Id,
    pub subscription: Id,
    pub member: Id,
    pub epoch: u64,
    pub client: Id,
    pub credential: Id,
    pub digest: [u8; 32],
}

pub struct Snapshot {
    pub attempts: usize,
    pub changed_identity: bool,
    pub original_status: Option<(u64, u32)>,
}

#[derive(Default)]
struct Observed {
    original: Option<Identity>,
    attempts: usize,
    changed_identity: bool,
    original_status: Option<(u64, u32)>,
    original_finished: bool,
    paused: bool,
}

struct Barrier {
    point: Point,
    observed: Mutex<Observed>,
    changed: Notify,
    release: watch::Sender<bool>,
}

type Key = (Id, Id);
static BARRIERS: Mutex<BTreeMap<Key, Arc<Barrier>>> = Mutex::new(BTreeMap::new());

pub struct InstalledJoinPause {
    key: Key,
    barrier: Arc<Barrier>,
}

impl InstalledJoinPause {
    pub fn install(topic: Id, member: Id, point: Point) -> io::Result<Self> {
        let mut slots = BARRIERS
            .lock()
            .map_err(|_| io::Error::other("Join barrier lock poisoned"))?;
        if slots.len() >= 16 || slots.contains_key(&(topic, member)) {
            return Err(io::Error::other(
                "Join barrier capacity or identity conflict",
            ));
        }
        let (release, _) = watch::channel(false);
        let barrier = Arc::new(Barrier {
            point,
            observed: Mutex::new(Observed::default()),
            changed: Notify::new(),
            release,
        });
        slots.insert((topic, member), barrier.clone());
        Ok(Self {
            key: (topic, member),
            barrier,
        })
    }

    pub async fn reached(&self, deadline: Instant) -> io::Result<Identity> {
        self.wait(deadline, |row| row.paused).await?;
        self.barrier
            .observed
            .lock()
            .map_err(|_| io::Error::other("Join observation lock poisoned"))?
            .original
            .clone()
            .ok_or_else(|| io::Error::other("Join identity missing"))
    }

    pub async fn replayed(&self, deadline: Instant) -> io::Result<()> {
        self.wait(deadline, |row| row.attempts >= 2).await
    }

    pub async fn finished(&self, deadline: Instant) -> io::Result<()> {
        self.wait(deadline, |row| row.original_finished).await
    }

    async fn wait(&self, deadline: Instant, ready: impl Fn(&Observed) -> bool) -> io::Result<()> {
        loop {
            let changed = self.barrier.changed.notified();
            let observed = {
                let row = self
                    .barrier
                    .observed
                    .lock()
                    .map_err(|_| io::Error::other("Join observation lock poisoned"))?;
                ready(&row)
            };
            if observed {
                return Ok(());
            }
            timeout_at(deadline, changed)
                .await
                .map_err(|_| io::Error::other("Join observation deadline elapsed"))?;
        }
    }

    pub fn snapshot(&self) -> io::Result<Snapshot> {
        let row = self
            .barrier
            .observed
            .lock()
            .map_err(|_| io::Error::other("Join observation lock poisoned"))?;
        Ok(Snapshot {
            attempts: row.attempts,
            changed_identity: row.changed_identity,
            original_status: row.original_status,
        })
    }

    pub fn resume(&self) {
        self.barrier.release.send_replace(true);
    }
}

impl Drop for InstalledJoinPause {
    fn drop(&mut self) {
        self.resume();
        if let Ok(mut slots) = BARRIERS.lock() {
            slots.remove(&self.key);
        }
    }
}

pub(super) struct Observation {
    barrier: Arc<Barrier>,
    first: bool,
}

pub(super) fn observe(identity: Identity) -> Option<Observation> {
    let barrier = BARRIERS
        .lock()
        .ok()?
        .get(&(identity.topic, identity.member))?
        .clone();
    let first = {
        let mut row = barrier.observed.lock().ok()?;
        let first = row.original.is_none();
        row.changed_identity |= row
            .original
            .as_ref()
            .is_some_and(|original| original != &identity);
        row.original.get_or_insert(identity);
        row.attempts = row.attempts.saturating_add(1);
        first
    };
    barrier.changed.notify_one();
    Some(Observation { barrier, first })
}

impl Observation {
    pub(super) async fn pause(&self, point: Point, deadline: Instant) {
        if !self.first || self.barrier.point != point {
            return;
        }
        if let Ok(mut row) = self.barrier.observed.lock() {
            row.paused = true;
        }
        self.barrier.changed.notify_one();
        let mut release = self.barrier.release.subscribe();
        let _ = timeout_at(deadline, release.wait_for(|released| *released)).await;
    }

    pub(super) fn status(&self, epoch: u64, status: u32) {
        if self.first
            && let Ok(mut row) = self.barrier.observed.lock()
        {
            row.original_status = Some((epoch, status));
        }
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        if self.first
            && let Ok(mut row) = self.barrier.observed.lock()
        {
            row.original_finished = true;
        }
        self.barrier.changed.notify_one();
    }
}
