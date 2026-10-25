use std::time::Duration;

use tidemq_protocol::Id;
use tokio::time::Instant;

use crate::broker::{ShardLoad, ShardLoadReport};

use super::{
    Domain, Status,
    policy::{self, TopicScalingPolicy},
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Identity {
    node: u64,
    instance: Id,
    generation: u64,
    layout: u64,
    owner: u64,
    birth: Option<(Id, tidemq_protocol::topic::HashRange)>,
}

pub(super) struct Sample {
    pub domain: Domain,
    pub node: u64,
    pub instance: Id,
    pub report: ShardLoadReport,
}

impl Sample {
    fn identity(&self) -> Identity {
        Identity {
            node: self.node,
            instance: self.instance,
            generation: self.domain.generation,
            layout: self.domain.layout,
            owner: self.report.owner_epoch,
            birth: self.report.birth,
        }
    }
    pub fn valid(&self) -> bool {
        // A fresh fenced partition can precede its first publish and therefore its birth binding.
        let empty = self.report.birth.is_none()
            && self.report.committed_end == 0
            && self.report.counters.publish_messages == 0
            && self.report.counters.publish_bytes == 0
            && self.report.counters.deliveries == 0
            && self.report.counters.acknowledgements == 0;
        self.report.partition == self.domain.shard.partition
            && self.report.owner_epoch == self.domain.owner_epoch
            && (self.report.birth
                == Some((self.domain.shard.route.id, self.domain.shard.route.range))
                || empty)
            && !self.report.frozen
            && self.report.counters.valid()
            && self.report.sealed != self.domain.active
    }
}

pub(super) struct Window {
    identity: Identity,
    last: ShardLoadReport,
    at: Instant,
    eligible: Instant,
    high: Option<(Instant, ShardLoad, u64)>,
    low: Option<(Instant, [u64; 8])>,
}

#[derive(Clone)]
pub(super) struct Evaluation {
    pub rates: [u64; 8],
    pub low_peak: Option<[u64; 8]>,
    pub split: Option<u64>,
    pub split_share: u16,
    pub status: Status,
}

impl Window {
    pub fn new(sample: &Sample, policy: &TopicScalingPolicy, now: Instant) -> Self {
        Self {
            identity: sample.identity(),
            last: sample.report.clone(),
            at: now,
            eligible: now + Duration::from_millis(policy.cooldown_ms),
            high: None,
            low: None,
        }
    }

    pub fn observe(
        &mut self,
        sample: &Sample,
        policy: &TopicScalingPolicy,
        now: Instant,
        archived: bool,
    ) -> Option<Evaluation> {
        let elapsed = now.checked_duration_since(self.at)?;
        if self.identity != sample.identity()
            || elapsed > policy.interval().saturating_mul(3)
            || sample.report.committed_end < self.last.committed_end
            || sample.report.published_end < self.last.published_end
        {
            *self = Self::new(sample, policy, now);
            return None;
        }
        if elapsed < policy.interval() / 2 {
            return None;
        }
        let Some(delta) = sample.report.counters.difference(&self.last.counters) else {
            *self = Self::new(sample, policy, now);
            return None;
        };
        let rates = policy::rates(&delta, elapsed)?;
        let mut result = Evaluation {
            rates,
            low_peak: None,
            split: None,
            split_share: 1000,
            status: Status::Collecting,
        };
        if policy.low(&rates) {
            let (since, peak) = self.low.get_or_insert((self.at, [0; 8]));
            for (peak, rate) in peak.iter_mut().zip(rates) {
                *peak = (*peak).max(rate);
            }
            if now.duration_since(*since) >= Duration::from_millis(policy.merge_window_ms)
                && now >= self.eligible
            {
                result.low_peak = Some(*peak);
            }
        } else {
            self.low = None;
        }
        if policy.writes_high(&rates) {
            let (since, baseline, published) = self.high.get_or_insert_with(|| {
                (self.at, self.last.counters.clone(), self.last.published_end)
            });
            if now.duration_since(*since) >= Duration::from_millis(policy.split_window_ms) {
                result.status = if now < self.eligible {
                    Status::Cooling
                } else if archived
                    && sample.report.published_end == *published
                    && delta.publish_messages > 0
                {
                    Status::ArchiveBehind
                } else {
                    let load = sample.report.counters.difference(baseline)?;
                    result.split = load.split_at(sample.domain.shard.route.range);
                    if let Some(cut) = result.split {
                        result.split_share =
                            split_share(&load, sample.domain.shard.route.range, cut)?;
                    }
                    if result.split.is_some() {
                        Status::SplitReady
                    } else {
                        Status::HotKeyOrNarrowRange
                    }
                };
            }
        } else {
            self.high = None;
            if policy.high(&rates) {
                result.status = Status::ConsumerCapacity;
            }
        }
        self.last = sample.report.clone();
        self.at = now;
        Some(result)
    }
}

fn split_share(
    load: &ShardLoad,
    range: tidemq_protocol::topic::HashRange,
    cut: u64,
) -> Option<u16> {
    let width = range
        .end
        .map_or(1_u128 << 64, u128::from)
        .checked_sub(u128::from(range.start))?;
    let left = load
        .hash_bins
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            u128::from(range.start) + (width * (*index as u128 + 1)).div_ceil(16) <= u128::from(cut)
        })
        .try_fold(0_u64, |sum, (_, bytes)| sum.checked_add(*bytes))?;
    let largest = left.max(load.publish_bytes.checked_sub(left)?);
    u16::try_from((u128::from(largest) * 1000).div_ceil(u128::from(load.publish_bytes))).ok()
}
