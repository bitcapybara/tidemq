//! Fault injection for integration tests; absent from default builds.

use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Mutex},
};

use tokio::{sync::Notify, time::Instant};

use crate::metadata::PartitionKey;

#[derive(Clone, Copy, Eq, PartialEq)]
enum PausePoint {
    HotDelivery(u64),
    Publish(u64),
}

static PAUSE: Mutex<Option<Arc<RuntimePause>>> = Mutex::new(None);

struct RuntimePause {
    partition: PartitionKey,
    point: PausePoint,
    ready: Notify,
    resume: Notify,
}

pub struct InstalledPause(Arc<RuntimePause>);

impl InstalledPause {
    pub fn hot_delivery(partition: PartitionKey, offset: u64) -> io::Result<Self> {
        Self::install(partition, PausePoint::HotDelivery(offset))
    }
    pub fn publish(partition: PartitionKey, sequence: u64) -> io::Result<Self> {
        Self::install(partition, PausePoint::Publish(sequence))
    }
    fn install(partition: PartitionKey, point: PausePoint) -> io::Result<Self> {
        let mut slot = PAUSE
            .lock()
            .map_err(|_| io::Error::other("Test hook is poisoned"))?;
        if slot.is_some() {
            return Err(io::Error::other("A runtime pause is already installed"));
        }
        let hook = Arc::new(RuntimePause {
            partition,
            point,
            ready: Notify::new(),
            resume: Notify::new(),
        });
        *slot = Some(hook.clone());
        Ok(Self(hook))
    }

    pub async fn wait(&self, deadline: Instant) -> io::Result<()> {
        tokio::time::timeout_at(deadline, self.0.ready.notified())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Runtime operation did not reach its pause",
                )
            })
    }

    pub fn resume(&self) {
        self.0.resume.notify_one();
    }
}

impl Drop for InstalledPause {
    fn drop(&mut self) {
        if let Ok(mut slot) = PAUSE.lock()
            && slot.as_ref().is_some_and(|hook| Arc::ptr_eq(hook, &self.0))
        {
            slot.take();
        }
        self.resume();
    }
}

pub(super) async fn pause_hot_delivery(partition: PartitionKey, offset: u64, deadline: Instant) {
    pause(partition, PausePoint::HotDelivery(offset), deadline).await;
}
pub(super) async fn pause_publish(partition: PartitionKey, sequence: u64, deadline: Instant) {
    pause(partition, PausePoint::Publish(sequence), deadline).await;
}
async fn pause(partition: PartitionKey, point: PausePoint, deadline: Instant) {
    let hook = PAUSE.lock().ok().and_then(|mut slot| {
        if slot
            .as_ref()
            .is_some_and(|hook| hook.partition == partition && hook.point == point)
        {
            slot.take()
        } else {
            None
        }
    });
    if let Some(hook) = hook {
        hook.ready.notify_one();
        let _ = tokio::time::timeout_at(deadline, hook.resume.notified()).await;
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ReadPoint {
    Reply,
    Body(u64),
}

struct ReadFault {
    partition: PartitionKey,
    point: ReadPoint,
    triggered: AtomicBool,
}

static READ_FAULT: Mutex<Option<Arc<ReadFault>>> = Mutex::new(None);

pub struct InstalledReadFault(Arc<ReadFault>);

impl InstalledReadFault {
    pub fn lose_reply(partition: PartitionKey) -> io::Result<Self> {
        Self::install(partition, ReadPoint::Reply)
    }

    pub fn body_failure(partition: PartitionKey, offset: u64) -> io::Result<Self> {
        Self::install(partition, ReadPoint::Body(offset))
    }

    fn install(partition: PartitionKey, point: ReadPoint) -> io::Result<Self> {
        let mut slot = READ_FAULT
            .lock()
            .map_err(|_| io::Error::other("Test hook is poisoned"))?;
        if slot.is_some() {
            return Err(io::Error::other(
                "A consumer Read fault is already installed",
            ));
        }
        let fault = Arc::new(ReadFault {
            partition,
            point,
            triggered: AtomicBool::new(false),
        });
        *slot = Some(fault.clone());
        Ok(Self(fault))
    }

    pub fn was_triggered(&self) -> bool {
        self.0.triggered.load(Ordering::Acquire)
    }
}

impl Drop for InstalledReadFault {
    fn drop(&mut self) {
        if let Ok(mut slot) = READ_FAULT.lock()
            && slot
                .as_ref()
                .is_some_and(|fault| Arc::ptr_eq(fault, &self.0))
        {
            slot.take();
        }
    }
}

fn trigger_read_fault(partition: PartitionKey, point: ReadPoint) -> bool {
    let fault = READ_FAULT.lock().ok().and_then(|mut slot| {
        if slot
            .as_ref()
            .is_some_and(|fault| fault.partition == partition && fault.point == point)
        {
            slot.take()
        } else {
            None
        }
    });
    if let Some(fault) = fault {
        fault.triggered.store(true, Ordering::Release);
        true
    } else {
        false
    }
}

pub(super) fn lose_consumer_reply(partition: PartitionKey) -> bool {
    trigger_read_fault(partition, ReadPoint::Reply)
}

pub(super) fn fail_consumer_body(partition: PartitionKey, offset: u64) -> bool {
    trigger_read_fault(partition, ReadPoint::Body(offset))
}
