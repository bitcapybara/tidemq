use super::*;
use crate::budget::{FrameReservation, MemoryBudget};

/// Client-wide Delivery admission, retained across physical reconnects.
#[derive(Clone)]
pub struct RpcBudget {
    inner: Arc<SharedBudget>,
}

struct SharedBudget {
    normal: Capacity,
    config: RpcConfig,
    normal_lanes: usize,
    stream_capacity: usize,
    registrations: Arc<Semaphore>,
    registration: Mutex<()>,
    memory: MemoryBudget,
    frame_bytes: u32,
    request_bytes: u32,
}

/// A consumer's bounded native pending, frame, and lane reservation.
#[derive(Clone)]
pub struct ProtectedCapacity {
    inner: Arc<Reservation>,
}

struct Reservation {
    budget: RpcBudget,
    capacity: Capacity,
    frames: FrameReservation,
    active: Arc<Semaphore>,
    _slot: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    _registration: OwnedSemaphorePermit,
}

pub(super) struct ProtectedLane {
    reservation: ProtectedCapacity,
    parent: Weak<State>,
    _active: OwnedSemaphorePermit,
}

impl ProtectedLane {
    pub(super) fn capacity(&self) -> Capacity {
        self.reservation.inner.capacity.clone()
    }

    pub(super) fn parent_admitting(&self) -> bool {
        self.parent.upgrade().is_some_and(|state| {
            !state.closed.load(Ordering::Acquire) && !state.draining.load(Ordering::Acquire)
        })
    }
}

impl RpcBudget {
    pub fn for_connection(
        config: &RpcConfig,
        connection: &Connection,
        normal_lanes: usize,
    ) -> Result<Self> {
        config.validate()?;
        if connection.is_closed() {
            return Err(Error::Closed);
        }
        if connection.class() != TrafficClass::Delivery
            || normal_lanes == 0
            || normal_lanes > 64
            || normal_lanes > connection.request_capacity()
        {
            return Err(Error::Configuration {
                reason: "shared RPC budget requires Delivery and valid normal request lanes",
            });
        }
        let protocol = &connection.state.config.protocol;
        let frame_bytes = protocol
            .max_frame_bytes
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(8))
            .ok_or(Error::Configuration {
                reason: "protected frame capacity exceeds u32",
            })?;
        let request_bytes =
            protocol
                .max_control_frame_bytes
                .checked_add(4)
                .ok_or(Error::Configuration {
                    reason: "protected request capacity exceeds u32",
                })?;
        let registrations = connection
            .request_capacity()
            .saturating_sub(normal_lanes)
            .min(config.max_pending_requests.saturating_sub(1));
        Ok(Self {
            inner: Arc::new(SharedBudget {
                normal: Capacity {
                    slots: Arc::new(Semaphore::new(config.max_pending_requests)),
                    bytes: Arc::new(Semaphore::new(config.max_pending_bytes as usize)),
                    max_bytes: config.max_pending_bytes,
                },
                config: config.clone(),
                normal_lanes,
                stream_capacity: connection.request_capacity(),
                registrations: Arc::new(Semaphore::new(registrations)),
                registration: Mutex::new(()),
                memory: connection.state.budget.clone(),
                frame_bytes,
                request_bytes,
            }),
        })
    }

    pub(super) fn capacity(&self) -> Capacity {
        self.inner.normal.clone()
    }

    pub(super) fn validate(
        &self,
        config: &RpcConfig,
        connection: &Connection,
        normal_lanes: usize,
    ) -> Result<()> {
        let protocol = &connection.state.config.protocol;
        let same_frames = protocol
            .max_frame_bytes
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(8))
            == Some(self.inner.frame_bytes)
            && protocol.max_control_frame_bytes.checked_add(4) == Some(self.inner.request_bytes);
        if connection.class() != TrafficClass::Delivery
            || normal_lanes != self.inner.normal_lanes
            || connection.request_capacity() != self.inner.stream_capacity
            || config.max_pending_requests != self.inner.config.max_pending_requests
            || config.max_pending_bytes != self.inner.config.max_pending_bytes
            || !same_frames
            || !self.inner.memory.same_instance(&connection.state.budget)
        {
            return Err(Error::Configuration {
                reason: "shared RPC budget does not match this connection",
            });
        }
        Ok(())
    }

    fn reserve(&self, connection: &Connection) -> Result<ProtectedCapacity> {
        // Three UUIDs, maximal varints, the frame envelope and a full traceparent fit in 256 bytes.
        if self.inner.request_bytes < 256 {
            return Err(Error::Configuration {
                reason: "protected pending bytes must fit a complete traced GroupRead",
            });
        }
        let _registration = self.inner.registration.lock().map_err(|_| Error::Closed)?;
        let registration = take_slot(&self.inner.registrations, "protected request lanes")?;
        // Registration must leave a complete ordinary read possible in every native pool.
        let _ordinary_slot = take_slot(&self.inner.normal.slots, "ordinary request floor")?;
        let _ordinary_bytes = take_bytes(&self.inner.normal.bytes, self.inner.request_bytes)?;
        let _ordinary_frames = connection
            .reserve_frames(self.inner.frame_bytes)
            .map_err(|source| Error::Transport { source })?;
        let slot = take_slot(&self.inner.normal.slots, "protected pending requests")?;
        let bytes = take_bytes(&self.inner.normal.bytes, self.inner.request_bytes)?;
        let frames = connection
            .reserve_frames(self.inner.frame_bytes)
            .map_err(|source| Error::Transport { source })?;
        Ok(ProtectedCapacity {
            inner: Arc::new(Reservation {
                budget: self.clone(),
                capacity: Capacity {
                    slots: Arc::new(Semaphore::new(1)),
                    bytes: Arc::new(Semaphore::new(self.inner.request_bytes as usize)),
                    max_bytes: self.inner.request_bytes,
                },
                frames,
                active: Arc::new(Semaphore::new(1)),
                _slot: slot,
                _bytes: bytes,
                _registration: registration,
            }),
        })
    }
}

impl RpcClient {
    pub fn reserve_protected(&self) -> Result<ProtectedCapacity> {
        let (budget, connection) = self.protected_source()?;
        budget.reserve(connection)
    }

    pub async fn open_protected(
        &self,
        protected: &ProtectedCapacity,
        deadline: Instant,
    ) -> Result<Self> {
        let (budget, connection) = self.protected_source()?;
        if !Arc::ptr_eq(&budget.inner, &protected.inner.budget.inner) {
            return Err(Error::Configuration {
                reason: "protected capacity belongs to another RPC budget",
            });
        }
        let active = timeout_at(deadline, protected.inner.active.clone().acquire_owned())
            .await
            .map_err(|_| Error::Deadline {
                may_have_been_sent: false,
            })?
            .map_err(|_| Error::Closed)?;
        self.protected_source()?;
        let stream = connection
            .open(crate::StreamPurpose::Request, deadline)
            .await
            .map_err(|source| Error::Transport { source })?
            .with_reserved_frames(&protected.inner.frames)
            .map_err(|source| Error::Transport { source })?;
        self.protected_source()?;
        let owner = Arc::new(ProtectedLane {
            reservation: protected.clone(),
            parent: Arc::downgrade(&self.inner.state),
            _active: active,
        });
        let config = RpcConfig {
            max_pending_requests: 1,
            max_pending_bytes: protected.inner.capacity.max_bytes,
            read_idle_timeout: self.inner.read_idle_timeout,
            liveness_timeout: budget.inner.config.liveness_timeout,
        };
        // A private lane never owns or closes the shared authenticated session.
        Self::start_with_capacity(vec![stream], config, None, None, Some(owner))
    }

    fn protected_source(&self) -> Result<(&RpcBudget, &Connection)> {
        if self.is_closed() || self.inner.state.draining.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let state = &self.inner.state;
        match (&state.shared, &state.connection) {
            (Some(budget), Some(connection)) => {
                if connection.is_closed() {
                    return Err(Error::Closed);
                }
                Ok((budget, connection))
            }
            _ => Err(Error::Configuration {
                reason: "protected reads require a shared Delivery RPC budget",
            }),
        }
    }
}

fn take_slot(pool: &Arc<Semaphore>, resource: &'static str) -> Result<OwnedSemaphorePermit> {
    pool.clone()
        .try_acquire_owned()
        .map_err(|_| Error::Admission { resource })
}

fn take_bytes(pool: &Arc<Semaphore>, bytes: u32) -> Result<OwnedSemaphorePermit> {
    pool.clone()
        .try_acquire_many_owned(bytes)
        .map_err(|_| Error::Admission {
            resource: "protected pending bytes",
        })
}

#[cfg(test)]
mod tests;
