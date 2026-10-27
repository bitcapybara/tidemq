//! Execution permits end with an RPC attempt; protected backing outlives retained deliveries.
use super::*;
use std::collections::VecDeque;
use tokio::sync::OwnedSemaphorePermit;

pub(crate) struct Admission {
    pub execution: Arc<Semaphore>,
    pub frames: Arc<Semaphore>,
    turns: std::sync::Mutex<[VecDeque<Id>; 3]>,
}

#[derive(Clone, Copy)]
pub(crate) enum Lane {
    Read = 0,
    Recovery = 1,
    Watch = 2,
}

pub(crate) struct Attempt {
    _execution: OwnedSemaphorePermit,
    _frames: OwnedSemaphorePermit,
    _protected: Option<Arc<Protected>>,
    pub request: Option<OwnedSemaphorePermit>,
    pub read: Option<OwnedSemaphorePermit>,
    pub recovery: Option<OwnedSemaphorePermit>,
    pub native: Option<Arc<super::protected::NativeProtection>>,
}

pub(crate) struct Protected {
    pub bytes: Arc<Semaphore>,
    execution: Arc<Semaphore>,
    frames: Arc<Semaphore>,
    _bytes: OwnedSemaphorePermit,
    _execution: OwnedSemaphorePermit,
    _frames: OwnedSemaphorePermit,
    _requests: OwnedSemaphorePermit,
}

pub(crate) fn frame_bytes(config: &ClientConfig, class: TrafficClass) -> u32 {
    // Encoded and decoded buffers can coexist; payload credit is charged separately.
    tidemq_protocol::framing::frame_limit(class, &config.protocol)
        .saturating_mul(2)
        .saturating_add(8192)
}

impl Admission {
    pub fn new(config: &ClientConfig) -> Self {
        let controls = (config.max_pending_requests / 4).max(1);
        Self {
            execution: Arc::new(Semaphore::new(
                config.max_pending_requests.saturating_sub(controls) as usize,
            )),
            // This extra SDK envelope is separate from the native private lane's reserved frame region.
            frames: Arc::new(Semaphore::new(
                ((config
                    .transport_memory_bytes
                    .saturating_sub(config.transport_control_reserved_bytes))
                    / 2)
                .min(u64::from(u32::MAX)) as usize,
            )),
            turns: std::sync::Mutex::new(std::array::from_fn(|_| VecDeque::new())),
        }
    }

    pub fn protect(
        &self,
        bytes: &Arc<Semaphore>,
        requests: &Arc<Semaphore>,
        minimum: u32,
        frame: u32,
    ) -> Result<Arc<Protected>> {
        let exhausted = || Error::Admission {
            resource: "consumer protected client share",
        };
        let memory = bytes
            .clone()
            .try_acquire_many_owned(minimum)
            .map_err(|_| exhausted())?;
        let execution = self
            .execution
            .clone()
            .try_acquire_owned()
            .map_err(|_| exhausted())?;
        let frames = self
            .frames
            .clone()
            .try_acquire_many_owned(frame)
            .map_err(|_| exhausted())?;
        let request = requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| exhausted())?;
        if bytes.available_permits() < minimum as usize
            || self.execution.available_permits() == 0
            || self.frames.available_permits() < frame as usize
            || requests.available_permits() == 0
        {
            return Err(exhausted());
        }
        Ok(Arc::new(Protected {
            bytes: Arc::new(Semaphore::new(minimum as usize)),
            execution: Arc::new(Semaphore::new(1)),
            frames: Arc::new(Semaphore::new(frame as usize)),
            _bytes: memory,
            _execution: execution,
            _frames: frames,
            _requests: request,
        }))
    }

    /// One bounded waiting entry per consumer and lane, including unsuccessful turns.
    pub fn fair<T>(
        &self,
        member: Id,
        lane: Lane,
        reserve: impl FnOnce() -> Result<Option<T>>,
    ) -> Result<Option<T>> {
        let mut turns = self.turns.lock().map_err(|_| Error::Closed)?;
        let queue = turns.get_mut(lane as usize).ok_or(Error::Closed)?;
        if !queue.contains(&member) {
            if queue.len() >= 128 {
                return Err(Error::Admission {
                    resource: "consumer admission queue",
                });
            }
            queue.push_back(member);
        }
        if queue.front() != Some(&member) {
            return Ok(None);
        }
        let result = reserve();
        queue.pop_front();
        if !matches!(&result, Ok(Some(_))) {
            queue.push_back(member);
        }
        result
    }

    pub fn forget(&self, member: Id) {
        if let Ok(mut turns) = self.turns.lock() {
            for queue in turns.iter_mut() {
                queue.retain(|id| *id != member);
            }
        }
    }

    pub fn withdraw(&self, member: Id, lane: Lane) {
        if let Ok(mut turns) = self.turns.lock()
            && let Some(queue) = turns.get_mut(lane as usize)
        {
            queue.retain(|id| *id != member);
        }
    }

    pub fn try_attempt(&self, frame: u32, protected: Option<&Arc<Protected>>) -> Option<Attempt> {
        let (execution, frames) = protected.map_or((&self.execution, &self.frames), |share| {
            (&share.execution, &share.frames)
        });
        Some(Attempt {
            _execution: execution.clone().try_acquire_owned().ok()?,
            _frames: frames.clone().try_acquire_many_owned(frame).ok()?,
            _protected: protected.cloned(),
            request: None,
            read: None,
            recovery: None,
            native: None,
        })
    }

    pub async fn attempt(&self, frame: u32, deadline: Instant) -> Result<Attempt> {
        let execution = timeout_at(deadline, self.execution.clone().acquire_owned())
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|_| Error::Closed)?;
        let frames = timeout_at(deadline, self.frames.clone().acquire_many_owned(frame))
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|_| Error::Closed)?;
        Ok(Attempt {
            _execution: execution,
            _frames: frames,
            _protected: None,
            request: None,
            read: None,
            recovery: None,
            native: None,
        })
    }
}
