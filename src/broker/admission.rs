use std::sync::Arc;

use tidemq_protocol::common::TrafficClass;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::budget::{MemoryBudget, MemoryPermit};

use super::{Error, Result};

#[derive(Clone, Debug)]
pub struct BrokerLimits {
    pub produce_operations: u32,
    pub produce_bytes: u32,
    pub control_operations: u32,
    pub control_bytes: u32,
    pub read_operations: u32,
    pub read_bytes: u32,
}

impl Default for BrokerLimits {
    fn default() -> Self {
        Self {
            produce_operations: 128,
            produce_bytes: 16 * 1024 * 1024,
            control_operations: 32,
            control_bytes: 1024 * 1024,
            read_operations: 64,
            read_bytes: 4 * 1024 * 1024,
        }
    }
}

impl BrokerLimits {
    pub fn validate(&self) -> Result<()> {
        for count in [
            self.produce_operations,
            self.control_operations,
            self.read_operations,
        ] {
            if count == 0 || count > 4_096 {
                return Err(Error::InvalidLimits);
            }
        }
        for bytes in [self.produce_bytes, self.control_bytes, self.read_bytes] {
            if !(512..=256 * 1024 * 1024).contains(&bytes)
                || bytes as usize > Semaphore::MAX_PERMITS
            {
                return Err(Error::InvalidLimits);
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct Lane {
    operations: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    maximum_operations: u32,
    class: TrafficClass,
}

pub(crate) struct Admission {
    _operation: OwnedSemaphorePermit,
    _lane_bytes: OwnedSemaphorePermit,
    _memory: MemoryPermit,
}

impl Lane {
    pub fn new(operations: u32, bytes: u32, class: TrafficClass) -> Self {
        Self {
            operations: Arc::new(Semaphore::new(operations as usize)),
            bytes: Arc::new(Semaphore::new(bytes as usize)),
            maximum_operations: operations,
            class,
        }
    }

    pub fn admit(&self, bytes: u32, memory: &MemoryBudget) -> Result<Admission> {
        let operation = self
            .operations
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let lane_bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| Error::Capacity)?;
        let memory = memory
            .try_reserve(self.class, bytes)
            .map_err(|source| Error::Memory { source })?;
        Ok(Admission {
            _operation: operation,
            _lane_bytes: lane_bytes,
            _memory: memory,
        })
    }

    pub async fn drain(&self) -> Result<()> {
        let _all = self
            .operations
            .clone()
            .acquire_many_owned(self.maximum_operations)
            .await
            .map_err(|_| Error::Draining)?;
        Ok(())
    }
}
