use std::sync::Arc;

use tidemq_protocol::common::TrafficClass;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::MemoryConfig;
use crate::{Error, Result};

/// Shared byte admission with capacity reserved for authenticated control work.
#[derive(Clone)]
pub struct MemoryBudget {
    total: Arc<Semaphore>,
    data: Arc<Semaphore>,
}

pub struct MemoryPermit {
    _total: OwnedSemaphorePermit,
    _data: Option<OwnedSemaphorePermit>,
}

impl MemoryBudget {
    pub fn new(config: &MemoryConfig) -> Result<Self> {
        let data = config
            .total_bytes
            .checked_sub(config.control_reserved_bytes)
            .ok_or(Error::Configuration {
                reason: "control reservation exceeds total memory",
            })?;
        let total = usize::try_from(config.total_bytes).map_err(|_| Error::Configuration {
            reason: "total memory does not fit the platform",
        })?;
        let data = usize::try_from(data).map_err(|_| Error::Configuration {
            reason: "data memory does not fit the platform",
        })?;
        if total > Semaphore::MAX_PERMITS || data > Semaphore::MAX_PERMITS {
            return Err(Error::Configuration {
                reason: "memory budget exceeds semaphore capacity",
            });
        }
        Ok(Self {
            total: Arc::new(Semaphore::new(total)),
            data: Arc::new(Semaphore::new(data)),
        })
    }

    pub fn try_reserve(&self, class: TrafficClass, bytes: u32) -> Result<MemoryPermit> {
        if class == TrafficClass::Unspecified || bytes == 0 {
            return Err(Error::Configuration {
                reason: "admission requires a traffic class and nonzero bytes",
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
        })
    }

    pub fn available_bytes(&self) -> usize {
        self.total.available_permits()
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
        let config = MemoryConfig {
            total_bytes: 100,
            control_reserved_bytes: 20,
            ..MemoryConfig::default()
        };
        let budget = MemoryBudget::new(&config).unwrap();
        let data = budget.try_reserve(TrafficClass::Produce, 80).unwrap();
        assert!(budget.try_reserve(TrafficClass::Bulk, 1).is_err());
        let control = budget.try_reserve(TrafficClass::Control, 20).unwrap();
        assert_eq!(budget.available_bytes(), 0);
        drop(data);
        drop(control);
        assert_eq!(budget.available_bytes(), 100);
    }

    #[test]
    fn failed_two_phase_reservation_does_not_leak_data_capacity() {
        let config = MemoryConfig {
            total_bytes: 100,
            control_reserved_bytes: 20,
            ..MemoryConfig::default()
        };
        let budget = MemoryBudget::new(&config).unwrap();
        let control = budget.try_reserve(TrafficClass::Control, 90).unwrap();
        assert!(budget.try_reserve(TrafficClass::Produce, 20).is_err());
        drop(control);
        assert!(budget.try_reserve(TrafficClass::Produce, 80).is_ok());
    }
}
