use std::sync::{Arc, Mutex};

use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeMetric},
    metrics::{counter::ConstCounter, gauge::ConstGauge},
};
use serde::Serialize;

#[derive(Clone, Debug, Default)]
pub(crate) struct RuntimeMetrics(pub Arc<Mutex<Snapshot>>);

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct Snapshot {
    pub ready: bool,
    pub process_memory: Option<crate::quota::memory::Snapshot>,
    pub log_dropped_events: Option<u64>,
    pub log_sink_errors: Option<u64>,
    pub log_oversized_events: Option<u64>,
}

impl Collector for RuntimeMetrics {
    fn encode(&self, mut encoder: DescriptorEncoder) -> std::result::Result<(), std::fmt::Error> {
        let snapshot = self.0.lock().map_err(|_| std::fmt::Error)?.clone();
        let ready = ConstGauge::new(i64::from(snapshot.ready));
        ready.encode(encoder.encode_descriptor(
            "ready",
            "Role authority readiness from the latest bounded sample",
            None,
            ready.metric_type(),
        )?)?;
        if let Some(memory) = snapshot.process_memory {
            for (name, help, value) in [
                (
                    "process_rss_bytes",
                    "Resident process memory from the operating system",
                    Some(memory.process_rss_bytes),
                ),
                (
                    "cgroup_current_bytes",
                    "Cgroup current usage, separate from process RSS",
                    memory.cgroup_current_bytes,
                ),
                (
                    "cgroup_limit_bytes",
                    "Finite cgroup memory limit when configured",
                    memory.cgroup_limit_bytes,
                ),
            ] {
                if let Some(value) = value {
                    let gauge = ConstGauge::new(i64::try_from(value).unwrap_or(i64::MAX));
                    gauge.encode(encoder.encode_descriptor(
                        name,
                        help,
                        None,
                        gauge.metric_type(),
                    )?)?;
                }
            }
        }
        for (name, help, value) in [
            (
                "log_dropped_events",
                "Events dropped by the bounded logging queue",
                snapshot.log_dropped_events,
            ),
            (
                "log_sink_errors",
                "Logging writer or flush failures",
                snapshot.log_sink_errors,
            ),
            (
                "log_oversized_events",
                "Events omitted after exceeding their byte budget",
                snapshot.log_oversized_events,
            ),
        ] {
            if let Some(value) = value {
                let counter = ConstCounter::new(value);
                counter.encode(encoder.encode_descriptor(
                    name,
                    help,
                    None,
                    counter.metric_type(),
                )?)?;
            }
        }
        Ok(())
    }
}
