use super::*;
use tidemq_transport::budget::{MemoryBudget, MemoryLimits, MemoryPermit};

struct Buffer {
    bytes: Vec<u8>,
    _memory: MemoryPermit,
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[test]
fn idle_cache_eviction_releases_retention_but_preserves_active_view_admission() {
    let budget = MemoryBudget::new(MemoryLimits {
        total_bytes: 4 * 1024 * 1024,
        control_reserved_bytes: 1024 * 1024,
    })
    .unwrap();
    let before = budget.available_data_bytes();
    let charge = S3Store::buffer_charge(128).unwrap();
    let bytes = Bytes::from_owner(Buffer {
        bytes: vec![0x5a; 128],
        _memory: budget
            .try_reserve(tidemq_protocol::common::TrafficClass::Bulk, charge)
            .unwrap(),
    });
    let id = Id::new_v7().unwrap();
    let key = (
        PackIdentity {
            cluster_id: id,
            group_id: 1,
            object_id: id,
        },
        [1; 32],
        80,
        128,
    );
    let now = Instant::now();
    let mut cache = Cache {
        entries: VecDeque::new(),
        bytes: bytes.len(),
    };
    cache.entries.push_back(CacheEntry {
        key,
        bytes,
        last_used: now,
        _retained: budget.try_retain_cached(charge).unwrap(),
    });
    let active = cache.get(key, now + CACHE_IDLE / 2).unwrap();
    cache.expire_idle(now + CACHE_IDLE);
    assert_eq!(cache.entries.len(), 1);
    cache.expire_idle(now + CACHE_IDLE + CACHE_IDLE / 2);
    assert_eq!(cache.bytes, 0);
    assert!(cache.entries.is_empty());
    assert!(cache.get(key, now + CACHE_IDLE * 2).is_none());
    let reuse = budget.try_retain_cached((before / 8) as u32).unwrap();
    drop(reuse);
    assert!(budget.available_data_bytes() < before);
    assert_eq!(active.as_ref(), vec![0x5a; 128]);
    drop(active);
    assert_eq!(budget.available_data_bytes(), before);
}
