use anyhow::{Context, Result, ensure};
use serde::Serialize;

const SUB_BUCKETS: usize = 32;
const BUCKETS: usize = 64 * SUB_BUCKETS;

/// Fixed-size logarithmic histogram; quantiles are bucket upper bounds.
pub struct Histogram {
    buckets: Vec<u64>,
    count: u64,
    sum_ns: u128,
    maximum_ns: u64,
}

#[derive(Debug, Serialize)]
pub struct Distribution {
    pub samples: u64,
    pub mean_ns: Option<f64>,
    pub p50_upper_ns: Option<u64>,
    pub p99_upper_ns: Option<u64>,
    pub p999_upper_ns: Option<u64>,
    pub maximum_ns: u64,
    pub quantile_method: &'static str,
    pub p99_minimum_samples: u64,
    pub p999_minimum_samples: u64,
}

impl Histogram {
    pub fn new() -> Result<Self> {
        let mut buckets = Vec::new();
        buckets.try_reserve_exact(BUCKETS)?;
        buckets.resize(BUCKETS, 0);
        Ok(Self {
            buckets,
            count: 0,
            sum_ns: 0,
            maximum_ns: 0,
        })
    }

    pub fn record(&mut self, value: u64) -> Result<()> {
        let bucket = self
            .buckets
            .get_mut(bucket_index(value))
            .context("Histogram bucket is invalid")?;
        *bucket = bucket.checked_add(1).context("Histogram count overflow")?;
        self.count = self.count.checked_add(1).context("Sample count overflow")?;
        self.sum_ns = self
            .sum_ns
            .checked_add(u128::from(value))
            .context("Histogram sum overflow")?;
        self.maximum_ns = self.maximum_ns.max(value);
        Ok(())
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        ensure!(
            self.buckets.len() == other.buckets.len(),
            "Histogram mismatch"
        );
        for (target, source) in self.buckets.iter_mut().zip(&other.buckets) {
            *target = target
                .checked_add(*source)
                .context("Bucket count overflow")?;
        }
        self.count = self
            .count
            .checked_add(other.count)
            .context("Count overflow")?;
        self.sum_ns = self
            .sum_ns
            .checked_add(other.sum_ns)
            .context("Sum overflow")?;
        self.maximum_ns = self.maximum_ns.max(other.maximum_ns);
        Ok(())
    }

    pub fn report(&self) -> Distribution {
        Distribution {
            samples: self.count,
            mean_ns: (self.count > 0).then(|| self.sum_ns as f64 / self.count as f64),
            p50_upper_ns: self.quantile(50, 100),
            p99_upper_ns: (self.count >= 1_000)
                .then(|| self.quantile(99, 100))
                .flatten(),
            p999_upper_ns: (self.count >= 10_000)
                .then(|| self.quantile(999, 1_000))
                .flatten(),
            maximum_ns: self.maximum_ns,
            quantile_method: "all_samples_log2_32_subbuckets_upper_bound_max_relative_width_3.125_percent",
            p99_minimum_samples: 1_000,
            p999_minimum_samples: 10_000,
        }
    }

    fn quantile(&self, numerator: u64, denominator: u64) -> Option<u64> {
        if self.count == 0 || denominator == 0 {
            return None;
        }
        let rank = u128::from(self.count)
            .checked_mul(u128::from(numerator))?
            .div_ceil(u128::from(denominator));
        let mut observed = 0_u128;
        for (index, count) in self.buckets.iter().enumerate() {
            observed = observed.checked_add(u128::from(*count))?;
            if observed >= rank {
                return Some(bucket_upper(index).min(self.maximum_ns));
            }
        }
        None
    }
}

fn bucket_index(value: u64) -> usize {
    let value = value.max(1);
    let exponent = (63 - value.leading_zeros()) as usize;
    let base = 1_u128 << exponent;
    let sub = ((u128::from(value) - base) * SUB_BUCKETS as u128 / base) as usize;
    exponent * SUB_BUCKETS + sub
}

fn bucket_upper(index: usize) -> u64 {
    let exponent = index / SUB_BUCKETS;
    let sub = index % SUB_BUCKETS;
    let base = 1_u128 << exponent;
    let exclusive = base + ((sub + 1) as u128 * base).div_ceil(SUB_BUCKETS as u128);
    u64::try_from(exclusive.saturating_sub(1)).unwrap_or(u64::MAX)
}

pub struct Seen {
    bits: Vec<u8>,
    limit: u64,
}

impl Seen {
    pub fn new(limit: u64) -> Result<Self> {
        let bytes = usize::try_from(limit.div_ceil(8))?;
        let mut bits = Vec::new();
        bits.try_reserve_exact(bytes)?;
        bits.resize(bytes, 0);
        Ok(Self { bits, limit })
    }

    pub fn insert(&mut self, value: u64) -> Result<()> {
        ensure!(
            value < self.limit,
            "Message identity exceeded the planned range"
        );
        let byte = self
            .bits
            .get_mut(usize::try_from(value / 8)?)
            .context("Missing identity bit")?;
        let mask = 1_u8 << (value % 8);
        ensure!(*byte & mask == 0, "Duplicate message identity or offset");
        *byte |= mask;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_cover_values_and_have_bounded_relative_error() {
        let mut previous = 0;
        for exponent in 0..64 {
            for sub in 0..32 {
                let value = u64::try_from((1_u128 << exponent) * (32 + sub) / 32).unwrap();
                let upper = bucket_upper(bucket_index(value));
                assert!(upper >= value);
                assert!(upper >= previous);
                assert!(u128::from(upper) <= u128::from(value) * 33 / 32 + 1);
                previous = upper;
            }
        }
        assert_eq!(bucket_upper(bucket_index(u64::MAX)), u64::MAX);
    }

    #[test]
    fn merge_counts_every_sample_and_suppresses_unsupported_tail_claims() {
        let mut first = Histogram::new().unwrap();
        let mut second = Histogram::new().unwrap();
        for value in 1..=10_000 {
            if value <= 5_000 {
                first.record(value).unwrap();
            } else {
                second.record(value).unwrap();
            }
        }
        assert_eq!(second.report().samples, 5_000);
        assert!(second.report().p999_upper_ns.is_none());
        first.merge(&second).unwrap();
        let report = first.report();
        assert_eq!(report.samples, 10_000);
        assert!((5_000..=5_156).contains(&report.p50_upper_ns.unwrap()));
        assert!((9_900..=10_000).contains(&report.p99_upper_ns.unwrap()));
        assert!((9_990..=10_000).contains(&report.p999_upper_ns.unwrap()));
        assert_eq!(report.mean_ns, Some(5_000.5));
    }

    #[test]
    fn seen_rejects_duplicates_and_out_of_range_without_growing() {
        let mut seen = Seen::new(9).unwrap();
        seen.insert(8).unwrap();
        assert!(seen.insert(8).is_err());
        assert!(seen.insert(9).is_err());
        assert_eq!(seen.bits.len(), 2);
    }
}
