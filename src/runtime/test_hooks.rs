//! Fault injection for integration tests; absent from default builds.

use std::{
    io,
    sync::{Arc, Mutex},
};

use tokio::{sync::Notify, time::Instant};

use crate::metadata::PartitionKey;

static HOT_DELIVERY: Mutex<Option<Arc<HotDeliveryPause>>> = Mutex::new(None);

pub struct HotDeliveryPause {
    partition: PartitionKey,
    offset: u64,
    ready: Notify,
    resume: Notify,
}

pub struct InstalledPause(Arc<HotDeliveryPause>);

impl InstalledPause {
    pub fn hot_delivery(partition: PartitionKey, offset: u64) -> io::Result<Self> {
        let mut slot = HOT_DELIVERY
            .lock()
            .map_err(|_| io::Error::other("Test hook is poisoned"))?;
        if slot.is_some() {
            return Err(io::Error::other(
                "A hot delivery pause is already installed",
            ));
        }
        let hook = Arc::new(HotDeliveryPause {
            partition,
            offset,
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
                    "Hot delivery did not reach its pause",
                )
            })
    }

    pub fn resume(&self) {
        self.0.resume.notify_one();
    }
}

impl Drop for InstalledPause {
    fn drop(&mut self) {
        if let Ok(mut slot) = HOT_DELIVERY.lock()
            && slot.as_ref().is_some_and(|hook| Arc::ptr_eq(hook, &self.0))
        {
            slot.take();
        }
        self.resume();
    }
}

pub(super) async fn pause_hot_delivery(partition: PartitionKey, offset: u64, deadline: Instant) {
    let hook = HOT_DELIVERY.lock().ok().and_then(|mut slot| {
        if slot
            .as_ref()
            .is_some_and(|hook| hook.partition == partition && hook.offset == offset)
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
