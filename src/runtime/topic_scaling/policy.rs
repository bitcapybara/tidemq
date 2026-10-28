use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::broker::ShardLoad;
use crate::runtime::{Error, Result};

/// An operator-supplied capacity profile for one execution group at the configured RF.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkCapacity {
    pub publish_bytes_per_second: u64,
    pub publish_messages_per_second: u64,
    pub delivery_bytes_per_second: u64,
    pub deliveries_per_second: u64,
    pub read_requests_per_second: u64,
    pub ack_bytes_per_second: u64,
    pub acknowledgements_per_second: u64,
    pub ack_requests_per_second: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TopicScalingPolicy {
    pub capacity: WorkCapacity,
    pub sample_interval_ms: u64,
    pub split_window_ms: u64,
    pub merge_window_ms: u64,
    pub cooldown_ms: u64,
    pub high_per_mille: u16,
    pub low_per_mille: u16,
    pub min_shards: usize,
    pub max_shards: usize,
}

impl Default for TopicScalingPolicy {
    fn default() -> Self {
        Self {
            capacity: WorkCapacity::default(),
            sample_interval_ms: 10_000,
            split_window_ms: 60_000,
            merge_window_ms: 600_000,
            cooldown_ms: 300_000,
            high_per_mille: 700,
            low_per_mille: 300,
            min_shards: 1,
            max_shards: 128,
        }
    }
}

impl WorkCapacity {
    pub(super) fn values(&self) -> [u64; 8] {
        [
            self.publish_bytes_per_second,
            self.publish_messages_per_second,
            self.delivery_bytes_per_second,
            self.deliveries_per_second,
            self.read_requests_per_second,
            self.ack_bytes_per_second,
            self.acknowledgements_per_second,
            self.ack_requests_per_second,
        ]
    }
}

impl TopicScalingPolicy {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.capacity.values().contains(&0)
            || !(100..=60_000).contains(&self.sample_interval_ms)
            || self.split_window_ms < self.sample_interval_ms.saturating_mul(2)
            || self.merge_window_ms < self.split_window_ms.saturating_mul(2)
            || self.cooldown_ms < self.split_window_ms
            || self.merge_window_ms > 86_400_000
            || self.cooldown_ms > 86_400_000
            || !(1..self.high_per_mille).contains(&self.low_per_mille)
            || self.high_per_mille >= 1000
            || self.min_shards == 0
            || self.min_shards > self.max_shards
            || self.max_shards > 128
        {
            return Err(Error::invalid(
                "Topic scaling requires a positive RF-specific capacity profile, bounded windows, low < high < 1000, and 1 <= min_shards <= max_shards <= 128",
            ));
        }
        Ok(())
    }

    pub(super) fn interval(&self) -> Duration {
        Duration::from_millis(self.sample_interval_ms)
    }

    pub(super) fn high(&self, rates: &[u64; 8]) -> bool {
        rates
            .iter()
            .zip(self.capacity.values())
            .any(|(rate, capacity)| {
                u128::from(*rate) * 1000 >= u128::from(capacity) * u128::from(self.high_per_mille)
            })
    }

    pub(super) fn writes_high(&self, rates: &[u64; 8]) -> bool {
        rates
            .iter()
            .zip(self.capacity.values())
            .take(2)
            .any(|(rate, capacity)| {
                u128::from(*rate) * 1000 >= u128::from(capacity) * u128::from(self.high_per_mille)
            })
    }

    pub(super) fn low(&self, rates: &[u64; 8]) -> bool {
        rates
            .iter()
            .zip(self.capacity.values())
            .all(|(rate, capacity)| {
                u128::from(*rate) * 1000 <= u128::from(capacity) * u128::from(self.low_per_mille)
            })
    }
}

pub(super) fn rates(delta: &ShardLoad, elapsed: Duration) -> Option<[u64; 8]> {
    let nanos = elapsed.as_nanos();
    if nanos == 0 {
        return None;
    }
    let values = [
        delta.publish_bytes,
        delta.publish_messages,
        delta.delivery_bytes,
        delta.deliveries,
        delta.read_requests,
        delta.ack_bytes,
        delta.acknowledgements,
        delta.ack_requests,
    ];
    let mut rates = [0; 8];
    for (rate, value) in rates.iter_mut().zip(values) {
        *rate = u64::try_from((u128::from(value) * 1_000_000_000).div_ceil(nanos)).ok()?;
    }
    Some(rates)
}
