use std::{sync::Arc, time::Duration};

use tokio::{sync::Semaphore, time::Instant};

use super::{Error, Result};
use crate::transport::TransportMetrics;

mod native;

const MIN_INTERVAL: Duration = Duration::from_millis(250);
const MAX_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
pub struct PressureSample {
    pub observed_at: std::time::Instant,
    pub cpu_per_mille: u16,
    pub bytes_per_second: u64,
    pub cpu_capacity_millicores: u64,
    pub active_connections: u32,
    pub source: &'static str,
}

struct Observation {
    at: std::time::Instant,
    cpu_ns: u128,
    capacity: u64,
    bytes: u64,
    connections: u32,
    source: &'static str,
}

/// Measures deltas; missing native samples, resets and stale intervals never become zero load.
pub struct PressureSampler {
    metrics: Vec<Arc<TransportMetrics>>,
    slot: Arc<Semaphore>,
    previous: std::sync::Mutex<Option<Observation>>,
}

impl PressureSampler {
    pub fn new(metrics: Arc<TransportMetrics>) -> Self {
        Self {
            metrics: vec![metrics],
            slot: Arc::new(Semaphore::new(1)),
            previous: std::sync::Mutex::new(None),
        }
    }

    /// Aggregates the node's client, internal server and public server without duplicate counters.
    pub fn with_metrics(metrics: Vec<Arc<TransportMetrics>>) -> Result<Self> {
        if metrics.is_empty() || metrics.len() > 4 {
            return Err(Error::Configuration);
        }
        for (index, metric) in metrics.iter().enumerate() {
            if metrics
                .iter()
                .take(index)
                .any(|old| Arc::ptr_eq(old, metric))
            {
                return Err(Error::Configuration);
            }
        }
        Ok(Self {
            metrics,
            slot: Arc::new(Semaphore::new(1)),
            previous: std::sync::Mutex::new(None),
        })
    }

    /// The native worker retains admission after a deadline until its actual read completes.
    pub async fn sample(&self, deadline: Instant) -> Result<Option<PressureSample>> {
        let permit = self
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let metrics = self.metrics.clone();
        let task = tokio::task::spawn_blocking(move || {
            let cpu = native::read_cpu().map_err(|source| Error::Observation { source })?;
            let mut bytes = 0_u64;
            let mut connections = 0_u64;
            for metric in metrics {
                let transport = metric.snapshot();
                bytes = bytes
                    .checked_add(transport.bytes_sent_total)
                    .and_then(|bytes| bytes.checked_add(transport.bytes_received_total))
                    .ok_or(Error::Placement)?;
                connections = connections
                    .checked_add(transport.active_connections)
                    .ok_or(Error::Placement)?;
            }
            let connections = u32::try_from(connections).map_err(|_| Error::Placement)?;
            Ok((
                Observation {
                    at: std::time::Instant::now(),
                    cpu_ns: cpu.nanoseconds,
                    capacity: cpu.millicores,
                    source: cpu.source,
                    bytes,
                    connections,
                },
                permit,
            ))
        });
        let (current, _permit) = tokio::time::timeout_at(deadline, task)
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|source| Error::Worker { source })??;
        let mut previous = self.previous.lock().map_err(|_| Error::Poisoned)?;
        let sample = previous
            .as_ref()
            .and_then(|previous| delta(previous, &current));
        if previous
            .as_ref()
            .is_none_or(|old| current.at.saturating_duration_since(old.at) >= MIN_INTERVAL)
        {
            *previous = Some(current);
        }
        Ok(sample)
    }
}

fn delta(previous: &Observation, current: &Observation) -> Option<PressureSample> {
    let elapsed = current.at.checked_duration_since(previous.at)?;
    if !(MIN_INTERVAL..=MAX_INTERVAL).contains(&elapsed)
        || previous.capacity != current.capacity
        || current.capacity == 0
    {
        return None;
    }
    let cpu = current.cpu_ns.checked_sub(previous.cpu_ns)?;
    let bytes = current.bytes.checked_sub(previous.bytes)?;
    let cpu_load = cpu
        .checked_mul(1_000_000)?
        .checked_div(elapsed.as_nanos())?
        .checked_div(u128::from(current.capacity))?;
    let rate = u128::from(bytes)
        .checked_mul(1_000_000_000)?
        .checked_div(elapsed.as_nanos())?;
    Some(PressureSample {
        observed_at: current.at,
        cpu_per_mille: u16::try_from(cpu_load).unwrap_or(u16::MAX),
        bytes_per_second: u64::try_from(rate).ok()?,
        cpu_capacity_millicores: current.capacity,
        active_connections: current.connections,
        source: current.source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(at: std::time::Instant, cpu_ns: u128, bytes: u64) -> Observation {
        Observation {
            at,
            cpu_ns,
            capacity: 500,
            bytes,
            connections: 4,
            source: "measured_test_fixture",
        }
    }

    #[test]
    fn fractional_cpu_quotas_and_reset_or_stale_intervals_are_conservative() {
        let now = std::time::Instant::now();
        let first = observation(now, 1_000_000_000, 1_000);
        let mut second = observation(now + Duration::from_secs(1), 1_250_000_000, 5_000);
        let sample = delta(&first, &second).unwrap();
        assert_eq!(sample.cpu_per_mille, 500);
        assert_eq!(sample.bytes_per_second, 4_000);
        assert_eq!(sample.active_connections, 4);
        second.capacity = 1000;
        assert!(delta(&first, &second).is_none());
        second.capacity = 500;
        second.bytes = 0;
        assert!(delta(&first, &second).is_none());
        second.bytes = 5_000;
        second.cpu_ns = 0;
        assert!(delta(&first, &second).is_none());
        second.cpu_ns = 1_250_000_000;
        second.at = now + Duration::from_secs(6);
        assert!(delta(&first, &second).is_none());
        second.at = now + Duration::from_millis(100);
        assert!(delta(&first, &second).is_none());
    }

    #[test]
    fn endpoint_collection_rejects_unbounded_or_duplicate_observations() {
        let metrics = Arc::new(TransportMetrics::default());
        assert!(PressureSampler::with_metrics(vec![]).is_err());
        assert!(PressureSampler::with_metrics(vec![metrics.clone(), metrics]).is_err());
        assert!(
            PressureSampler::with_metrics(
                (0..5)
                    .map(|_| Arc::new(TransportMetrics::default()))
                    .collect()
            )
            .is_err()
        );
        assert!(
            PressureSampler::with_metrics(
                (0..3)
                    .map(|_| Arc::new(TransportMetrics::default()))
                    .collect()
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn native_cpu_requires_two_actual_samples_before_reporting_pressure() {
        let sampler = PressureSampler::new(Arc::new(TransportMetrics::default()));
        assert!(
            sampler
                .sample(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
                .is_none()
        );
        tokio::time::sleep(MIN_INTERVAL + Duration::from_millis(50)).await;
        let measured = sampler
            .sample(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert!(measured.cpu_capacity_millicores > 0);
        assert_eq!(measured.bytes_per_second, 0);
        assert!(measured.observed_at <= std::time::Instant::now());
    }
}
