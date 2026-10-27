use std::sync::Arc;

use tidemq_protocol::common::TrafficClass;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid memory budget: {reason}"))]
    Configuration { reason: &'static str },
    #[snafu(display("Memory admission exhausted for {traffic_class}"))]
    Admission {
        traffic_class: &'static str,
        source: tokio::sync::TryAcquireError,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct MemoryLimits {
    pub total_bytes: u64,
    pub control_reserved_bytes: u64,
}

/// Shared byte admission with capacity reserved for authenticated control work.
#[derive(Clone)]
pub struct MemoryBudget {
    data_capacity: usize,
    total: Arc<Semaphore>,
    data: Arc<Semaphore>,
    normal: Arc<Semaphore>,
    core: Arc<Semaphore>,
    ack: Arc<Semaphore>,
    handshake: Arc<Semaphore>,
    cache: Arc<Semaphore>,
}

pub struct MemoryPermit {
    _total: OwnedSemaphorePermit,
    _data: Option<OwnedSemaphorePermit>,
    _class: Option<OwnedSemaphorePermit>,
    // A charged child buffer keeps its entire carved parent region alive.
    _parent: Option<Arc<MemoryPermit>>,
}

/// A private Delivery frame pool carved from the shared normal/data budget.
#[derive(Clone)]
pub struct FrameReservation {
    source: Arc<Semaphore>,
    pool: Arc<Semaphore>,
    parent: Arc<MemoryPermit>,
}

impl FrameReservation {
    pub fn available_bytes(&self) -> usize {
        self.pool.available_permits()
    }

    pub(crate) fn validate(&self, budget: &MemoryBudget, class: TrafficClass) -> Result<()> {
        if class != TrafficClass::Delivery {
            return Err(Error::Configuration {
                reason: "reserved frames require the Delivery traffic class",
            });
        }
        if !Arc::ptr_eq(&self.source, &budget.total) {
            return Err(Error::Configuration {
                reason: "frame reservation belongs to a different memory budget",
            });
        }
        Ok(())
    }

    pub(crate) fn try_reserve(&self, budget: &MemoryBudget, bytes: u32) -> Result<MemoryPermit> {
        self.validate(budget, TrafficClass::Delivery)?;
        if bytes == 0 {
            return Err(Error::Configuration {
                reason: "reserved frame admission requires nonzero bytes",
            });
        }
        let permit = self
            .pool
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|source| Error::Admission {
                traffic_class: "reserved delivery frames",
                source,
            })?;
        Ok(MemoryPermit {
            _total: permit,
            _data: None,
            _class: None,
            _parent: Some(self.parent.clone()),
        })
    }
}

impl MemoryBudget {
    pub fn new(config: impl Into<MemoryLimits>) -> Result<Self> {
        let config = config.into();
        let data = config
            .total_bytes
            .checked_sub(config.control_reserved_bytes)
            .ok_or(Error::Configuration {
                reason: "control_reserved_bytes must not exceed total_bytes",
            })?;
        let total = usize::try_from(config.total_bytes).map_err(|_| Error::Configuration {
            reason: "total memory does not fit the platform",
        })?;
        let data = usize::try_from(data).map_err(|_| Error::Configuration {
            reason: "data memory does not fit the platform",
        })?;
        let reserved =
            usize::try_from(config.control_reserved_bytes).map_err(|_| Error::Configuration {
                reason: "control memory does not fit the platform",
            })?;
        let handshake = reserved / 4;
        let ack = reserved / 8;
        let core = (reserved / 2).saturating_sub(ack);
        let normal = total
            .checked_sub(core)
            .and_then(|value| value.checked_sub(handshake))
            .and_then(|value| value.checked_sub(ack))
            .ok_or(Error::Configuration {
                reason: "critical memory reservation exceeds total",
            })?;
        if core == 0 || handshake == 0 || ack == 0 {
            return Err(Error::Configuration {
                reason: "control_reserved_bytes must be at least 8 bytes to reserve core, ACK, and handshake capacity",
            });
        }
        if total > Semaphore::MAX_PERMITS {
            return Err(Error::Configuration {
                reason: "total_bytes exceeds the platform's semaphore capacity",
            });
        }
        Ok(Self {
            data_capacity: data,
            total: Arc::new(Semaphore::new(total)),
            data: Arc::new(Semaphore::new(data)),
            normal: Arc::new(Semaphore::new(normal)),
            core: Arc::new(Semaphore::new(core)),
            ack: Arc::new(Semaphore::new(ack)),
            handshake: Arc::new(Semaphore::new(handshake)),
            // Cached ownership must leave headroom for new requests, uploads and decoding.
            cache: Arc::new(Semaphore::new(data / 8)),
        })
    }

    /// Additional shared retention cap for cached buffers that already hold memory admission.
    pub fn try_retain_cached(&self, bytes: u32) -> Result<OwnedSemaphorePermit> {
        if bytes == 0 {
            return Err(Error::Configuration {
                reason: "cached bytes must be greater than zero",
            });
        }
        self.cache
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|source| Error::Admission {
                traffic_class: "read cache",
                source,
            })
    }

    /// Keep frame capacity private to one consumer across connections sharing this budget.
    pub fn reserve_frames(&self, class: TrafficClass, bytes: u32) -> Result<FrameReservation> {
        if class != TrafficClass::Delivery {
            return Err(Error::Configuration {
                reason: "frame reservations require the Delivery traffic class",
            });
        }
        let capacity = usize::try_from(bytes).map_err(|_| Error::Configuration {
            reason: "frame reservation does not fit the platform",
        })?;
        if capacity == 0 || capacity > Semaphore::MAX_PERMITS {
            return Err(Error::Configuration {
                reason: "frame reservation must be nonzero and fit the semaphore capacity",
            });
        }
        let parent = self.try_reserve(class, bytes)?;
        Ok(FrameReservation {
            source: self.total.clone(),
            pool: Arc::new(Semaphore::new(capacity)),
            parent: Arc::new(parent),
        })
    }

    pub fn try_reserve(&self, class: TrafficClass, bytes: u32) -> Result<MemoryPermit> {
        if class == TrafficClass::Unspecified {
            return Err(Error::Configuration {
                reason: "admission traffic_class must not be unspecified",
            });
        }
        if bytes == 0 {
            return Err(Error::Configuration {
                reason: "admission bytes must be greater than zero",
            });
        }
        let data = if class == TrafficClass::Control {
            None
        } else {
            Some(
                self.data
                    .clone()
                    .try_acquire_many_owned(bytes)
                    .map_err(|source| Error::Admission {
                        traffic_class: class_name(class),
                        source,
                    })?,
            )
        };
        let normal = self
            .normal
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|source| Error::Admission {
                traffic_class: class_name(class),
                source,
            })?;
        let total = self
            .total
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|source| Error::Admission {
                traffic_class: class_name(class),
                source,
            })?;
        Ok(MemoryPermit {
            _total: total,
            _data: data,
            _class: Some(normal),
            _parent: None,
        })
    }

    /// Only authenticated core lanes may consume this quota; ordinary ACKs cannot.
    pub fn try_reserve_core(&self, bytes: u32) -> Result<MemoryPermit> {
        self.try_reserve_critical(&self.core, "core", bytes)
    }

    pub fn try_reserve_ack(&self, bytes: u32) -> Result<MemoryPermit> {
        self.try_reserve_critical(&self.ack, "ack", bytes)
    }

    /// Unauthenticated work has a separate cap and cannot consume the core reserve.
    pub fn try_reserve_handshake(&self, bytes: u32) -> Result<MemoryPermit> {
        self.try_reserve_critical(&self.handshake, "handshake", bytes)
    }

    fn try_reserve_critical(
        &self,
        quota: &Arc<Semaphore>,
        class: &'static str,
        bytes: u32,
    ) -> Result<MemoryPermit> {
        if bytes == 0 {
            return Err(Error::Configuration {
                reason: "critical admission requires nonzero bytes",
            });
        }
        let permit = quota
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|source| Error::Admission {
                traffic_class: class,
                source,
            })?;
        let total = self
            .total
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|source| Error::Admission {
                traffic_class: class,
                source,
            })?;
        Ok(MemoryPermit {
            _total: total,
            _data: None,
            _class: Some(permit),
            _parent: None,
        })
    }

    /// Configured data capacity, independent of temporary reservations.
    pub fn data_capacity(&self) -> usize {
        self.data_capacity
    }

    pub fn available_bytes(&self) -> usize {
        self.total.available_permits()
    }
    pub fn available_data_bytes(&self) -> usize {
        self.data.available_permits()
    }
    pub fn available_normal_bytes(&self) -> usize {
        self.normal.available_permits()
    }
    pub fn available_core_bytes(&self) -> usize {
        self.core.available_permits()
    }
    pub fn available_ack_bytes(&self) -> usize {
        self.ack.available_permits()
    }
    pub fn available_handshake_bytes(&self) -> usize {
        self.handshake.available_permits()
    }
}

pub fn class_name(class: TrafficClass) -> &'static str {
    match class {
        TrafficClass::Control => "control",
        TrafficClass::Produce => "produce",
        TrafficClass::Delivery => "delivery",
        TrafficClass::Bulk => "bulk",
        TrafficClass::Replication => "replication",
        TrafficClass::Unspecified => "unspecified",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(budget: &MemoryBudget) -> [usize; 6] {
        [
            budget.available_bytes(),
            budget.available_data_bytes(),
            budget.available_normal_bytes(),
            budget.available_core_bytes(),
            budget.available_ack_bytes(),
            budget.available_handshake_bytes(),
        ]
    }

    fn small_budget() -> MemoryBudget {
        MemoryBudget::new(MemoryLimits {
            total_bytes: 100,
            control_reserved_bytes: 20,
        })
        .unwrap()
    }

    #[test]
    fn frame_regions_carve_normal_data_and_cannot_borrow_from_each_other() {
        let budget = small_budget();
        let before = snapshot(&budget);
        let first = budget.reserve_frames(TrafficClass::Delivery, 30).unwrap();
        let second = budget.reserve_frames(TrafficClass::Delivery, 30).unwrap();
        assert_eq!(snapshot(&budget), [40, 20, 25, 8, 2, 5]);
        assert!(first.try_reserve(&budget, 31).is_err());
        let first_frame = first.try_reserve(&budget, 30).unwrap();
        assert!(first.try_reserve(&budget, 1).is_err());
        assert_eq!(second.available_bytes(), 30);
        let second_frame = second.try_reserve(&budget, 30).unwrap();
        assert_eq!(snapshot(&budget), [40, 20, 25, 8, 2, 5]);
        let ordinary = budget.try_reserve(TrafficClass::Delivery, 20).unwrap();
        let control = budget.try_reserve(TrafficClass::Control, 5).unwrap();
        assert!(budget.try_reserve(TrafficClass::Delivery, 1).is_err());
        let core = budget.try_reserve_core(8).unwrap();
        let ack = budget.try_reserve_ack(2).unwrap();
        let handshake = budget.try_reserve_handshake(5).unwrap();
        assert_eq!(budget.available_bytes(), 0);
        drop((first_frame, second_frame));
        assert_eq!(budget.available_bytes(), 0);
        assert_eq!(first.available_bytes(), 30);
        assert_eq!(second.available_bytes(), 30);
        drop((first, second, ordinary, control, core, ack, handshake));
        assert_eq!(snapshot(&budget), before);
    }

    #[test]
    fn child_permits_keep_the_parent_region_after_every_handle_is_dropped() {
        let budget = small_budget();
        let before = snapshot(&budget);
        let region = budget.reserve_frames(TrafficClass::Delivery, 40).unwrap();
        let handle_clone = region.clone();
        let first = region.try_reserve(&budget, 25).unwrap();
        let last = Arc::new(region.try_reserve(&budget, 15).unwrap());
        let last_clone = last.clone();
        drop((region, handle_clone, first, last));
        assert_eq!(snapshot(&budget), [60, 40, 45, 8, 2, 5]);
        drop(last_clone);
        assert_eq!(snapshot(&budget), before);
    }

    #[test]
    fn frame_reservations_require_delivery_and_the_original_shared_budget() {
        let budget = small_budget();
        let before = snapshot(&budget);
        for class in [
            TrafficClass::Unspecified,
            TrafficClass::Control,
            TrafficClass::Produce,
            TrafficClass::Bulk,
            TrafficClass::Replication,
        ] {
            assert!(budget.reserve_frames(class, 1).is_err());
        }
        for bytes in [0, 81, u32::MAX] {
            assert!(
                budget
                    .reserve_frames(TrafficClass::Delivery, bytes)
                    .is_err()
            );
            assert_eq!(snapshot(&budget), before);
        }
        let region = budget.reserve_frames(TrafficClass::Delivery, 40).unwrap();
        let unrelated = small_budget();
        assert!(region.validate(&unrelated, TrafficClass::Delivery).is_err());
        assert!(region.try_reserve(&unrelated, 1).is_err());
        assert!(region.validate(&budget, TrafficClass::Control).is_err());
        assert!(region.try_reserve(&budget, 0).is_err());
        let reconnected = budget.clone();
        region
            .validate(&reconnected, TrafficClass::Delivery)
            .unwrap();
        let memory = region.try_reserve(&reconnected, 40).unwrap();
        drop((region, memory));
        assert_eq!(snapshot(&budget), before);
        assert_eq!(snapshot(&unrelated), before);
    }

    #[test]
    fn failed_frame_region_creation_rolls_back_its_parent_admission() {
        let budget = small_budget();
        let before = snapshot(&budget);
        let ordinary = budget.try_reserve(TrafficClass::Control, 85).unwrap();
        let saturated = snapshot(&budget);
        assert!(budget.reserve_frames(TrafficClass::Delivery, 20).is_err());
        assert_eq!(snapshot(&budget), saturated);
        drop(ordinary);
        assert_eq!(snapshot(&budget), before);
        let region = budget.reserve_frames(TrafficClass::Delivery, 80).unwrap();
        assert!(region.try_reserve(&budget, 81).is_err());
        assert_eq!(region.available_bytes(), 80);
        drop(region);
        assert_eq!(snapshot(&budget), before);
    }

    #[test]
    fn cache_retention_shares_a_node_cap_without_double_charging_active_buffers() {
        let budget = MemoryBudget::new(MemoryLimits {
            total_bytes: 100,
            control_reserved_bytes: 20,
        })
        .unwrap();
        let buffer = budget.try_reserve(TrafficClass::Bulk, 6).unwrap();
        let available = budget.available_data_bytes();
        let first = budget.try_retain_cached(6).unwrap();
        let other = budget.clone();
        assert!(other.try_retain_cached(5).is_err());
        let remaining = other.try_retain_cached(4).unwrap();
        assert!(budget.try_retain_cached(1).is_err());
        assert_eq!(budget.available_data_bytes(), available);
        drop((first, remaining));
        let replacement = budget.try_retain_cached(10).unwrap();
        assert_eq!(budget.available_data_bytes(), available);
        drop((buffer, replacement));
        assert_eq!(budget.available_data_bytes(), 80);
        assert!(budget.try_retain_cached(0).is_err());
    }

    #[test]
    fn saturated_data_preserves_control_and_drop_restores_capacity() {
        let config = MemoryLimits {
            total_bytes: 100,
            control_reserved_bytes: 20,
        };
        let budget = MemoryBudget::new(config).unwrap();
        let data = budget.try_reserve(TrafficClass::Produce, 80).unwrap();
        assert!(budget.try_reserve(TrafficClass::Bulk, 1).is_err());
        let control = budget.try_reserve(TrafficClass::Control, 5).unwrap();
        let ack = budget.try_reserve_ack(2).unwrap();
        let core = budget.try_reserve_core(8).unwrap();
        let handshake = budget.try_reserve_handshake(5).unwrap();
        assert_eq!(budget.available_bytes(), 0);
        drop(data);
        drop(control);
        drop(core);
        drop(handshake);
        drop(ack);
        assert_eq!(budget.available_bytes(), 100);
    }

    #[test]
    fn failed_two_phase_reservation_does_not_leak_data_capacity() {
        let config = MemoryLimits {
            total_bytes: 100,
            control_reserved_bytes: 20,
        };
        let budget = MemoryBudget::new(config).unwrap();
        let control = budget.try_reserve(TrafficClass::Control, 85).unwrap();
        assert!(budget.try_reserve(TrafficClass::Produce, 20).is_err());
        drop(control);
        assert!(budget.try_reserve(TrafficClass::Produce, 80).is_ok());
    }

    #[test]
    fn saturated_normal_control_cannot_consume_ack_core_or_handshake() {
        let config = MemoryLimits {
            total_bytes: 100,
            control_reserved_bytes: 20,
        };
        let budget = MemoryBudget::new(config).unwrap();
        let normal = budget.try_reserve(TrafficClass::Control, 85).unwrap();
        assert!(budget.try_reserve(TrafficClass::Produce, 1).is_err());
        assert!(budget.try_reserve(TrafficClass::Control, 1).is_err());
        let core = budget.try_reserve_core(8).unwrap();
        let handshake = budget.try_reserve_handshake(5).unwrap();
        let ack = budget.try_reserve_ack(2).unwrap();
        assert!(budget.try_reserve_ack(1).is_err());
        assert!(budget.try_reserve_handshake(1).is_err());
        assert!(budget.try_reserve_core(1).is_err());
        drop((normal, core, handshake, ack));
        assert_eq!(budget.available_bytes(), 100);
    }
}
