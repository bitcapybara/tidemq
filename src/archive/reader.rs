use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use tidemq_protocol::Id;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use super::{
    ArchivedRecord, Descriptor, Error, PackIdentity, PackLimits, Result, format::decode_records,
};
use crate::object_store::S3Store;

type CacheKey = (PackIdentity, [u8; 32], u64, u64);
const CACHE_IDLE: Duration = Duration::from_secs(60);

#[cfg(test)]
#[path = "../../tests/scalable_support/cold_cache_memory.rs"]
mod memory_tests;

struct CacheEntry {
    key: CacheKey,
    bytes: Bytes,
    last_used: Instant,
    _retained: OwnedSemaphorePermit,
}

struct Cache {
    entries: VecDeque<CacheEntry>,
    bytes: usize,
}

impl Cache {
    fn get(&mut self, key: CacheKey, now: Instant) -> Option<Bytes> {
        self.expire_idle(now);
        let entry = self.entries.iter_mut().find(|entry| entry.key == key)?;
        entry.last_used = now;
        Some(entry.bytes.clone())
    }

    fn evict_oldest(&mut self) -> bool {
        let Some(entry) = self.entries.pop_front() else {
            return false;
        };
        self.bytes = self.bytes.saturating_sub(entry.bytes.len());
        true
    }

    fn expire_idle(&mut self, now: Instant) {
        let mut released = 0_usize;
        self.entries.retain(|entry| {
            let keep = now.saturating_duration_since(entry.last_used) < CACHE_IDLE;
            if !keep {
                released = released.saturating_add(entry.bytes.len());
            }
            keep
        });
        self.bytes = self.bytes.saturating_sub(released);
    }
}

/// A bounded range cache; every read still requires a descriptor from an authorized manifest view.
pub struct ColdReader {
    store: S3Store,
    limits: PackLimits,
    max_cache_bytes: usize,
    max_cache_entries: usize,
    cache: Mutex<Cache>,
    decode_slots: Arc<Semaphore>,
}

impl ColdReader {
    pub fn new(
        store: S3Store,
        limits: PackLimits,
        max_cache_bytes: usize,
        max_cache_entries: usize,
    ) -> Result<Self> {
        limits.validate()?;
        if max_cache_bytes > limits.max_bytes || max_cache_entries > 4096 {
            return Err(Error::Invalid {
                reason: "cold cache bounds exceed the object or entry limit",
            });
        }
        Ok(Self {
            decode_slots: store.decoder_slots(),
            store,
            limits,
            max_cache_bytes,
            max_cache_entries,
            cache: Mutex::new(Cache {
                entries: VecDeque::new(),
                bytes: 0,
            }),
        })
    }

    /// Coalesce a partition's contiguous record range into one request without speculative prefetch.
    pub async fn read_partition(
        &self,
        descriptor: &Descriptor,
        topic_id: Id,
        partition_id: u32,
    ) -> Result<Vec<ArchivedRecord>> {
        let permit = self
            .decode_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        descriptor.validate(self.limits)?;
        self.store
            .object_key(descriptor)
            .map_err(|source| Error::Object { source })?;
        let index = descriptor
            .partitions
            .iter()
            .find(|index| index.topic_id == topic_id && index.partition_id == partition_id)
            .ok_or(Error::Invalid {
                reason: "partition is absent from the authorized descriptor",
            })?;
        let length = index
            .byte_end
            .checked_sub(index.byte_start)
            .ok_or(Error::Limit)?;
        let key = (
            descriptor.identity,
            descriptor.checksum,
            index.byte_start,
            length,
        );
        let hit = self.cache.lock().await.get(key, Instant::now());
        let bytes = match hit {
            Some(bytes) => {
                self.store.metrics().cache_hits.inc();
                bytes
            }
            None => {
                self.store.metrics().cache_misses.inc();
                self.store
                    .read_range(descriptor, index.byte_start, length)
                    .await
                    .map_err(|source| Error::Object { source })?
            }
        };
        let cached = bytes.clone();
        let index = index.clone();
        let limits = self.limits;
        let budget = self.store.budget().clone();
        let metrics = self.store.metrics();
        let queued = Instant::now();
        let records = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            metrics.decode_queue.observe(queued.elapsed().as_secs_f64());
            let started = Instant::now();
            let result = decode_records(&index, bytes, limits, &budget);
            metrics.decode.observe(started.elapsed().as_secs_f64());
            result
        })
        .await
        .map_err(|source| Error::Worker { source })??;
        // Cache only after one complete validation; cancellation retains the worker's admission.
        self.insert(key, cached).await?;
        Ok(records)
    }

    async fn insert(&self, key: CacheKey, bytes: Bytes) -> Result<()> {
        if bytes.len() > self.max_cache_bytes || self.max_cache_entries == 0 {
            return Ok(());
        }
        let mut cache = self.cache.lock().await;
        let now = Instant::now();
        cache.expire_idle(now);
        if cache.entries.iter().any(|entry| entry.key == key) {
            return Ok(());
        }
        while cache.entries.len() >= self.max_cache_entries
            || cache
                .bytes
                .checked_add(bytes.len())
                .is_none_or(|size| size > self.max_cache_bytes)
        {
            if !cache.evict_oldest() {
                break;
            }
        }
        // Match S3's retained body admission, including its bounded transport allowance.
        let retained =
            S3Store::buffer_charge(bytes.len()).map_err(|source| Error::Object { source })?;
        let retained = loop {
            match self.store.budget().try_retain_cached(retained) {
                Ok(permit) => break permit,
                Err(_) if cache.evict_oldest() => {}
                Err(_) => return Ok(()),
            }
        };
        cache
            .entries
            .try_reserve(1)
            .map_err(|source| Error::Allocation { source })?;
        cache.bytes = cache.bytes.checked_add(bytes.len()).ok_or(Error::Limit)?;
        cache.entries.push_back(CacheEntry {
            key,
            bytes,
            last_used: now,
            _retained: retained,
        });
        Ok(())
    }

    pub(crate) async fn expire_idle(&self) {
        self.cache.lock().await.expire_idle(Instant::now());
    }

    pub async fn cached_bytes(&self) -> usize {
        self.cache.lock().await.bytes
    }

    pub async fn clear(&self) {
        let mut cache = self.cache.lock().await;
        cache.entries.clear();
        cache.bytes = 0;
    }
}
