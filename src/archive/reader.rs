use std::{collections::VecDeque, sync::Arc, time::Instant};

use bytes::Bytes;
use tidemq_protocol::Id;
use tokio::sync::{Mutex, Semaphore};

use super::{
    ArchivedRecord, Descriptor, Error, PackIdentity, PackLimits, Result, format::decode_records,
};
use crate::object_store::S3Store;

type CacheKey = (PackIdentity, [u8; 32], u64, u64);

struct Cache {
    entries: VecDeque<(CacheKey, Bytes)>,
    bytes: usize,
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
        let hit = self
            .cache
            .lock()
            .await
            .entries
            .iter()
            .find(|(candidate, _)| *candidate == key)
            .map(|(_, bytes)| bytes.clone());
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
        if cache.entries.iter().any(|(candidate, _)| *candidate == key) {
            return Ok(());
        }
        while cache.entries.len() >= self.max_cache_entries
            || cache
                .bytes
                .checked_add(bytes.len())
                .is_none_or(|size| size > self.max_cache_bytes)
        {
            let Some((_, evicted)) = cache.entries.pop_front() else {
                break;
            };
            cache.bytes = cache.bytes.saturating_sub(evicted.len());
        }
        cache
            .entries
            .try_reserve(1)
            .map_err(|source| Error::Allocation { source })?;
        cache.bytes = cache.bytes.checked_add(bytes.len()).ok_or(Error::Limit)?;
        cache.entries.push_back((key, bytes));
        Ok(())
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
