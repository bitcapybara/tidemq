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
    total: Arc<Semaphore>,
    data: Arc<Semaphore>,
    normal: Arc<Semaphore>,
    core: Arc<Semaphore>,
    handshake: Arc<Semaphore>,
}

pub struct MemoryPermit {
    _total: OwnedSemaphorePermit,
    _data: Option<OwnedSemaphorePermit>,
    _class: OwnedSemaphorePermit,
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
        let core = reserved / 2;
        let handshake = reserved / 4;
        let normal = total
            .checked_sub(core)
            .and_then(|value| value.checked_sub(handshake))
            .ok_or(Error::Configuration {
                reason: "critical memory reservation exceeds total",
            })?;
        if core == 0 || handshake == 0 {
            return Err(Error::Configuration {
                reason: "control_reserved_bytes must be at least 4 bytes to reserve core and handshake capacity",
            });
        }
        if total > Semaphore::MAX_PERMITS {
            return Err(Error::Configuration {
                reason: "total_bytes exceeds the platform's semaphore capacity",
            });
        }
        Ok(Self {
            total: Arc::new(Semaphore::new(total)),
            data: Arc::new(Semaphore::new(data)),
            normal: Arc::new(Semaphore::new(normal)),
            core: Arc::new(Semaphore::new(core)),
            handshake: Arc::new(Semaphore::new(handshake)),
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
            _class: normal,
        })
    }

    /// Only authenticated core lanes may consume this quota; ordinary ACKs cannot.
    pub fn try_reserve_core(&self, bytes: u32) -> Result<MemoryPermit> {
        self.try_reserve_critical(&self.core, "core", bytes)
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
            _class: permit,
        })
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
        let core = budget.try_reserve_core(10).unwrap();
        let handshake = budget.try_reserve_handshake(5).unwrap();
        assert_eq!(budget.available_bytes(), 0);
        drop(data);
        drop(control);
        drop(core);
        drop(handshake);
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
    fn saturated_normal_control_cannot_consume_core_or_handshake() {
        let config = MemoryLimits {
            total_bytes: 100,
            control_reserved_bytes: 20,
        };
        let budget = MemoryBudget::new(config).unwrap();
        let normal = budget.try_reserve(TrafficClass::Control, 85).unwrap();
        assert!(budget.try_reserve(TrafficClass::Produce, 1).is_err());
        assert!(budget.try_reserve(TrafficClass::Control, 1).is_err());
        let core = budget.try_reserve_core(10).unwrap();
        let handshake = budget.try_reserve_handshake(5).unwrap();
        assert!(budget.try_reserve_handshake(1).is_err());
        assert!(budget.try_reserve_core(1).is_err());
        drop((normal, core, handshake));
        assert_eq!(budget.available_bytes(), 100);
    }
}
