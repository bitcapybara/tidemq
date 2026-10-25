use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use tidemq_protocol::common::TrafficClass;

use crate::budget::{MemoryBudget, MemoryPermit};

struct Usage {
    requests: usize,
    bytes: usize,
    _memory: MemoryPermit,
}

pub(super) struct TenantAdmission {
    requests: usize,
    bytes: usize,
    budget: MemoryBudget,
    usage: Mutex<BTreeMap<[u8; 16], Usage>>,
}

pub(super) struct TenantPermit {
    admission: Arc<TenantAdmission>,
    tenant: [u8; 16],
    bytes: usize,
}

impl TenantAdmission {
    pub fn new(budget: MemoryBudget, handlers: usize, maximum_frame: usize) -> Arc<Self> {
        Arc::new(Self {
            requests: (handlers / 4).max(1),
            bytes: (budget.data_capacity() / 4).max(maximum_frame),
            budget,
            usage: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn try_reserve(self: &Arc<Self>, tenant: [u8; 16], bytes: usize) -> Option<TenantPermit> {
        if bytes == 0 || bytes > self.bytes {
            return None;
        }
        let mut usage = self.usage.lock().ok()?;
        if !usage.contains_key(&tenant) {
            if usage.len() >= 128 {
                return None;
            }
            let memory = self.budget.try_reserve(TrafficClass::Control, 256).ok()?;
            usage.insert(
                tenant,
                Usage {
                    requests: 0,
                    bytes: 0,
                    _memory: memory,
                },
            );
        }
        let current = usage.get_mut(&tenant)?;
        let retained = current.bytes.checked_add(bytes)?;
        if current.requests >= self.requests || retained > self.bytes {
            return None;
        }
        current.requests = current.requests.checked_add(1)?;
        current.bytes = retained;
        Some(TenantPermit {
            admission: self.clone(),
            tenant,
            bytes,
        })
    }
}

impl Drop for TenantPermit {
    fn drop(&mut self) {
        if let Ok(mut usage) = self.admission.usage.lock()
            && let Some(current) = usage.get_mut(&self.tenant)
        {
            current.requests = current.requests.saturating_sub(1);
            current.bytes = current.bytes.saturating_sub(self.bytes);
            if current.requests == 0 {
                usage.remove(&self.tenant);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::MemoryLimits;

    #[test]
    fn tenant_windows_share_one_key_bound_bytes_and_release_all_accounting() {
        let budget = MemoryBudget::new(MemoryLimits {
            total_bytes: 65_536,
            control_reserved_bytes: 8192,
        })
        .unwrap();
        let normal = budget.available_normal_bytes();
        let admission = TenantAdmission::new(budget.clone(), 8, 100);
        assert!(
            admission
                .try_reserve([3; 16], budget.data_capacity())
                .is_none()
        );
        assert!(admission.usage.lock().unwrap().is_empty());
        let a = admission.try_reserve([1; 16], 100).unwrap();
        let a_second_connection = admission.try_reserve([1; 16], 100).unwrap();
        assert!(admission.try_reserve([1; 16], 1).is_none());
        let b = admission.try_reserve([2; 16], 100).unwrap();
        assert!(
            admission
                .try_reserve([2; 16], budget.data_capacity())
                .is_none()
        );
        drop(a);
        let retry = admission.try_reserve([1; 16], 100).unwrap();
        drop((retry, a_second_connection, b));
        assert!(admission.usage.lock().unwrap().is_empty());
        assert_eq!(budget.available_normal_bytes(), normal);
    }

    #[test]
    fn admission_metadata_stays_bounded_and_expired_tenant_entries_do_not_accumulate() {
        let budget = MemoryBudget::new(MemoryLimits {
            total_bytes: 262_144,
            control_reserved_bytes: 65_536,
        })
        .unwrap();
        let normal = budget.available_normal_bytes();
        let admission = TenantAdmission::new(budget.clone(), 1024, 100);
        let mut held = Vec::new();
        for tenant in 0..128 {
            held.push(admission.try_reserve([tenant; 16], 1).unwrap());
        }
        assert!(admission.try_reserve([128; 16], 1).is_none());
        drop(held.pop());
        held.push(admission.try_reserve([128; 16], 1).unwrap());
        drop(held);
        assert!(admission.usage.lock().unwrap().is_empty());
        assert_eq!(budget.available_normal_bytes(), normal);
    }
}
