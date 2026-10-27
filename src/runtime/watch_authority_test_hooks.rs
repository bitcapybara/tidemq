//! Bounded test-only controls for the actual Watch authorization execution path.

use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tidemq_protocol::Id;
use tokio::{sync::Notify, time::Instant};

type Node = (Id, u64);
struct Pause {
    node: Node,
    parked: AtomicUsize,
    released: AtomicBool,
    changed: Notify,
    resume: Notify,
}
static PAUSE: Mutex<Option<Arc<Pause>>> = Mutex::new(None);

pub struct AuthorizeQuorumPause(Arc<Pause>);
impl AuthorizeQuorumPause {
    pub fn install(cluster: Id, node: u64) -> io::Result<Self> {
        let mut slot = PAUSE
            .lock()
            .map_err(|_| io::Error::other("Watch authority pause is poisoned"))?;
        if node == 0 || slot.is_some() {
            return Err(io::Error::other(
                "Watch authority pause is invalid or already installed",
            ));
        }
        let pause = Arc::new(Pause {
            node: (cluster, node),
            parked: AtomicUsize::new(0),
            released: AtomicBool::new(false),
            changed: Notify::new(),
            resume: Notify::new(),
        });
        *slot = Some(pause.clone());
        Ok(Self(pause))
    }

    pub fn parked(&self) -> usize {
        self.0.parked.load(Ordering::Acquire)
    }

    pub async fn wait_parked(&self, count: usize, deadline: Instant) -> io::Result<()> {
        if !(1..=8).contains(&count) {
            return Err(io::Error::other(
                "Watch authority pause count must be between 1 and 8",
            ));
        }
        tokio::time::timeout_at(deadline, async {
            loop {
                let changed = self.0.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.parked() >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Watch authority did not reach its pause",
            )
        })
    }

    pub fn release(&self) {
        self.0.released.store(true, Ordering::Release);
        self.0.resume.notify_waiters();
    }
}

impl Drop for AuthorizeQuorumPause {
    fn drop(&mut self) {
        if let Ok(mut slot) = PAUSE.lock()
            && slot
                .as_ref()
                .is_some_and(|pause| Arc::ptr_eq(pause, &self.0))
        {
            slot.take();
        }
        self.release();
    }
}

struct Parked(Arc<Pause>);
impl Drop for Parked {
    fn drop(&mut self) {
        self.0.parked.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

pub(crate) async fn before_authority_quorum(cluster: Id, node: u64, deadline: Instant) {
    let pause = PAUSE.lock().ok().and_then(|slot| {
        slot.as_ref()
            .filter(|pause| pause.node == (cluster, node))
            .cloned()
    });
    let Some(pause) = pause else { return };
    let resumed = pause.resume.notified();
    tokio::pin!(resumed);
    resumed.as_mut().enable();
    if pause.released.load(Ordering::Acquire)
        || pause
            .parked
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .is_err()
    {
        return;
    }
    let _parked = Parked(pause.clone());
    pause.changed.notify_waiters();
    let _ = tokio::time::timeout_at(deadline, resumed).await;
}

struct CoreLimit {
    limit: usize,
    applied: AtomicBool,
}
static CORE_LIMITS: Mutex<BTreeMap<Node, Arc<CoreLimit>>> = Mutex::new(BTreeMap::new());

pub struct CoreHandlerLimit {
    node: Node,
    limit: Arc<CoreLimit>,
}
impl CoreHandlerLimit {
    pub fn install(cluster: Id, node: u64, limit: usize) -> io::Result<Self> {
        let mut limits = CORE_LIMITS
            .lock()
            .map_err(|_| io::Error::other("Watch fixture handler limits are poisoned"))?;
        let node = (cluster, node);
        if node.1 == 0
            || !(1..=4_096).contains(&limit)
            || limits.len() >= 16
            || limits.contains_key(&node)
        {
            return Err(io::Error::other(
                "Watch fixture handler limit is invalid or already installed",
            ));
        }
        let limit = Arc::new(CoreLimit {
            limit,
            applied: AtomicBool::new(false),
        });
        limits.insert(node, limit.clone());
        Ok(Self { node, limit })
    }

    pub fn applied(&self) -> bool {
        self.limit.applied.load(Ordering::Acquire)
    }
}
impl Drop for CoreHandlerLimit {
    fn drop(&mut self) {
        if let Ok(mut limits) = CORE_LIMITS.lock()
            && limits
                .get(&self.node)
                .is_some_and(|limit| Arc::ptr_eq(limit, &self.limit))
        {
            limits.remove(&self.node);
        }
    }
}

pub(crate) fn core_handlers(cluster: Id, node: u64, default: usize) -> usize {
    CORE_LIMITS
        .lock()
        .ok()
        .and_then(|limits| {
            limits.get(&(cluster, node)).map(|limit| {
                limit.applied.store(true, Ordering::Release);
                limit.limit
            })
        })
        .unwrap_or(default)
}
