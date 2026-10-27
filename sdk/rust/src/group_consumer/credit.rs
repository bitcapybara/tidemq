use super::*;
use crate::client::admission::{Attempt, Protected, frame_bytes};
use tokio::sync::OwnedSemaphorePermit;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerMode {
    Isolated,
    Serial,
}

pub(super) struct Pools {
    pub native: Arc<crate::client::protected::NativeProtection>,
    pub ordinary_slots: Arc<Semaphore>,
    pub ordinary_bytes: Arc<Semaphore>,
    pub protected_slots: Arc<Semaphore>,
    pub protected_bytes: Arc<Semaphore>,
    pub read_rpc: Arc<Semaphore>,
    pub protected_rpc: Arc<Semaphore>,
    pub recovery_rpc: Arc<Semaphore>,
    pub shared: Mutex<Option<Arc<Protected>>>,
    pub mode: ConsumerMode,
    pub minimum: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Pool {
    Ordinary,
    Protected,
}

pub(super) struct Credit {
    pub slots: OwnedSemaphorePermit,
    pub memory: OwnedSemaphorePermit,
    pub client: OwnedSemaphorePermit,
    pub protected: Option<Arc<Protected>>,
}

impl Pools {
    pub fn new(client: &Client, slots: u32, bytes: u32) -> Result<Self> {
        let config = &client.inner.config;
        let minimum = u32::try_from(minimum_prefetch_bytes(config.protocol.max_message_bytes))
            .map_err(|_| Error::Configuration {
                reason: "minimum consumer credit exceeds u32",
            })?;
        if slots == 0
            || bytes < minimum
            || config.protocol.max_frame_bytes.saturating_sub(8192) < minimum
            || client.inner.bytes.available_permits() < minimum as usize
            || client.inner.admission.frames.available_permits()
                < frame_bytes(config, TrafficClass::Delivery) as usize
        {
            return Err(Error::Configuration {
                reason: "consumer cannot admit one legal maximum-sized read and its frame",
            });
        }
        let isolated = slots >= 2
            && bytes / 2 >= minimum
            && config
                .max_pending_requests
                .saturating_sub((config.max_pending_requests / 4).max(1))
                >= 4;
        let shared = if isolated {
            Some(client.inner.admission.protect(
                &client.inner.bytes,
                &client.inner.requests,
                minimum,
                frame_bytes(config, TrafficClass::Delivery),
            )?)
        } else {
            None
        };
        let protected = if isolated { minimum } else { 0 };
        Ok(Self {
            native: Arc::new(crate::client::protected::NativeProtection::default()),
            ordinary_slots: Arc::new(Semaphore::new(
                slots.saturating_sub(u32::from(isolated)) as usize
            )),
            ordinary_bytes: Arc::new(Semaphore::new(bytes.saturating_sub(protected) as usize)),
            protected_slots: Arc::new(Semaphore::new(usize::from(isolated))),
            protected_bytes: Arc::new(Semaphore::new(protected as usize)),
            read_rpc: Arc::new(Semaphore::new(if isolated { 7 } else { 1 })),
            protected_rpc: Arc::new(Semaphore::new(usize::from(isolated))),
            recovery_rpc: Arc::new(Semaphore::new(if isolated { 2 } else { 1 })),
            shared: Mutex::new(shared),
            mode: if isolated {
                ConsumerMode::Isolated
            } else {
                ConsumerMode::Serial
            },
            minimum,
        })
    }

    pub fn reserve(
        &self,
        client: &Client,
        pool: Pool,
        contenders: usize,
    ) -> Result<Option<Credit>> {
        let shared = self.shared.lock().map_err(|_| Error::Closed)?.clone();
        let (slots, memory, client_memory) = match pool {
            Pool::Ordinary => (
                &self.ordinary_slots,
                &self.ordinary_bytes,
                &client.inner.bytes,
            ),
            Pool::Protected => (
                &self.protected_slots,
                &self.protected_bytes,
                &shared.as_ref().ok_or(Error::Closed)?.bytes,
            ),
        };
        let available = memory
            .available_permits()
            .min(client_memory.available_permits());
        let fair_bytes = (available / contenders.max(1))
            .max(self.minimum as usize)
            .min(available);
        let budget = fair_bytes.min(
            client
                .inner
                .config
                .protocol
                .max_frame_bytes
                .saturating_sub(8192) as usize,
        );
        let messages = slots
            .available_permits()
            .min(if pool == Pool::Protected { 1 } else { 32 })
            .min(budget.saturating_sub(self.minimum.saturating_sub(256) as usize) / 256);
        if budget < self.minimum as usize || messages == 0 {
            return Ok(None);
        }
        let bytes = if pool == Pool::Protected {
            self.minimum
        } else {
            budget as u32
        };
        let Some(slots) = slots.clone().try_acquire_many_owned(messages as u32).ok() else {
            return Ok(None);
        };
        let Some(memory) = memory.clone().try_acquire_many_owned(bytes).ok() else {
            return Ok(None);
        };
        let Some(client) = client_memory.clone().try_acquire_many_owned(bytes).ok() else {
            return Ok(None);
        };
        Ok(Some(Credit {
            slots,
            memory,
            client,
            protected: (pool == Pool::Protected).then_some(shared).flatten(),
        }))
    }

    pub fn attempt(
        &self,
        client: &Client,
        pool: Pool,
        recovery: bool,
        class: TrafficClass,
    ) -> Result<Option<Attempt>> {
        let shared = self.shared.lock().map_err(|_| Error::Closed)?.clone();
        let protected = (pool == Pool::Protected && !recovery)
            .then_some(shared.as_ref())
            .flatten();
        let Some(mut attempt) = client
            .inner
            .admission
            .try_attempt(frame_bytes(&client.inner.config, class), protected)
        else {
            return Ok(None);
        };
        if protected.is_none() {
            let Ok(request) = client.inner.requests.clone().try_acquire_owned() else {
                return Ok(None);
            };
            attempt.request = Some(request);
        }
        if class == TrafficClass::Delivery {
            if protected.is_some() {
                attempt.native = Some(self.native.clone());
            }
            // Epoch fencing may remove domain records before their old RPC futures finish.
            let rpc = if protected.is_some() {
                &self.protected_rpc
            } else {
                &self.read_rpc
            };
            let Ok(read) = rpc.clone().try_acquire_owned() else {
                return Ok(None);
            };
            attempt.read = Some(read);
            if recovery {
                let Ok(recovery) = self.recovery_rpc.clone().try_acquire_owned() else {
                    return Ok(None);
                };
                attempt.recovery = Some(recovery);
            }
        }
        Ok(Some(attempt))
    }
}
