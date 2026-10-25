use crate::metadata::PartitionKey;
use serde::{Deserialize, Serialize};
use tidemq_protocol::topic::HashRange;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardLoadReport {
    pub partition: PartitionKey,
    pub birth: Option<(tidemq_protocol::Id, HashRange)>,
    pub owner_epoch: u64,
    pub committed_end: u64,
    pub published_end: u64,
    pub protected_backlog_bytes: u64,
    pub frozen: bool,
    pub sealed: bool,
    pub counters: ShardLoad,
}

#[cfg(test)]
#[path = "../../tests/scalable_support/shard_load.rs"]
mod tests;

/// Committed work only; fixed summaries never retain keys or message bodies.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardLoad {
    pub publish_bytes: u64,
    pub publish_messages: u64,
    pub delivery_bytes: u64,
    pub deliveries: u64,
    pub read_requests: u64,
    pub ack_bytes: u64,
    pub acknowledgements: u64,
    pub ack_requests: u64,
    pub hash_bins: [u64; 16],
    pub invalid: bool,
}

fn add(counter: &mut u64, bytes: u64) -> bool {
    match counter.checked_add(bytes) {
        Some(next) => {
            *counter = next;
            false
        }
        None => {
            *counter = u64::MAX;
            true
        }
    }
}

impl ShardLoad {
    pub(super) const LOGICAL_BYTES: u64 = 256;

    pub fn valid(&self) -> bool {
        !self.invalid
            && self
                .hash_bins
                .iter()
                .try_fold(0_u64, |sum, bytes| sum.checked_add(*bytes))
                == Some(self.publish_bytes)
    }
    pub fn publish(&mut self, range: HashRange, hash: u64, bytes: u64) {
        let end = range.end.map_or(1_u128 << 64, u128::from);
        let Some(width) = end
            .checked_sub(u128::from(range.start))
            .filter(|width| *width > 0)
        else {
            self.invalid = true;
            return;
        };
        let Some(distance) = hash
            .checked_sub(range.start)
            .filter(|_| range.contains(hash))
        else {
            self.invalid = true;
            return;
        };
        let Ok(bin) = usize::try_from(u128::from(distance) * 16 / width) else {
            self.invalid = true;
            return;
        };
        let Some(bin) = self.hash_bins.get_mut(bin) else {
            self.invalid = true;
            return;
        };
        self.invalid |= add(bin, bytes);
        self.invalid |= add(&mut self.publish_bytes, bytes);
        self.invalid |= add(&mut self.publish_messages, 1);
    }

    pub fn deliver(&mut self, bytes: u64, messages: u64) {
        self.invalid |= add(&mut self.delivery_bytes, bytes);
        self.invalid |= add(&mut self.deliveries, messages);
        self.invalid |= add(&mut self.read_requests, 1);
    }

    pub fn acknowledge(&mut self, bytes: Option<u64>, messages: u64) {
        let Some(bytes) = bytes else {
            self.invalid = true;
            return;
        };
        self.invalid |= add(&mut self.ack_bytes, bytes);
        self.invalid |= add(&mut self.acknowledgements, messages);
        self.invalid |= add(&mut self.ack_requests, 1);
    }

    pub fn difference(&self, before: &Self) -> Option<Self> {
        if !self.valid() || !before.valid() {
            return None;
        }
        let mut bins = [0; 16];
        for ((out, current), previous) in bins.iter_mut().zip(self.hash_bins).zip(before.hash_bins)
        {
            *out = current.checked_sub(previous)?;
        }
        let delta = Self {
            publish_bytes: self.publish_bytes.checked_sub(before.publish_bytes)?,
            publish_messages: self.publish_messages.checked_sub(before.publish_messages)?,
            delivery_bytes: self.delivery_bytes.checked_sub(before.delivery_bytes)?,
            deliveries: self.deliveries.checked_sub(before.deliveries)?,
            read_requests: self.read_requests.checked_sub(before.read_requests)?,
            ack_bytes: self.ack_bytes.checked_sub(before.ack_bytes)?,
            acknowledgements: self.acknowledgements.checked_sub(before.acknowledgements)?,
            ack_requests: self.ack_requests.checked_sub(before.ack_requests)?,
            hash_bins: bins,
            invalid: false,
        };
        let sum = delta
            .hash_bins
            .iter()
            .try_fold(0_u64, |sum, bytes| sum.checked_add(*bytes))?;
        (sum == delta.publish_bytes).then_some(delta)
    }

    /// A cut requires observed writes on both sides; a narrow hotspot supplies no such proof.
    pub fn split_at(&self, range: HashRange) -> Option<u64> {
        if !self.valid() || self.publish_bytes == 0 {
            return None;
        }
        let end = range.end.map_or(1_u128 << 64, u128::from);
        let width = end.checked_sub(u128::from(range.start))?;
        let mut left = 0_u64;
        let mut best: Option<(u64, u64)> = None;
        for (index, bytes) in self.hash_bins.iter().take(15).enumerate() {
            left = left.checked_add(*bytes)?;
            let right = self.publish_bytes.checked_sub(left)?;
            if left.min(right) < self.publish_bytes.div_ceil(10) {
                continue;
            }
            let cut =
                u128::from(range.start).checked_add((width * (index as u128 + 1)).div_ceil(16))?;
            let cut = u64::try_from(cut).ok()?;
            if cut <= range.start || u128::from(cut) >= end {
                continue;
            }
            let imbalance = left.abs_diff(right);
            if best.is_none_or(|(previous, _)| imbalance < previous) {
                best = Some((imbalance, cut));
            }
        }
        best.map(|(_, cut)| cut)
    }
}
