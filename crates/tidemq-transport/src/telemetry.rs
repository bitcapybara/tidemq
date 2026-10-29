use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

mod failure;
pub use failure::{TransportFailure, TransportFailures};

use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeMetric},
    metrics::{counter::ConstCounter, gauge::ConstGauge},
};
use serde::Serialize;

#[derive(Debug, Default)]
pub struct TransportMetrics {
    failures: Mutex<TransportFailures>,
    pub(super) connections: AtomicU64,
    pub(super) active_connections: AtomicU64,
    pub(super) liveness_probes: AtomicU64,
    pub(super) liveness_failures: AtomicU64,
    pub(super) rejected: AtomicU64,
    pub(super) streams: AtomicU64,
    pub(super) frames_sent: AtomicU64,
    pub(super) frames_received: AtomicU64,
    pub(super) bytes_sent: AtomicU64,
    pub(super) bytes_received: AtomicU64,
    pub(super) deadlines: AtomicU64,
    pub(super) malformed: AtomicU64,
    pub(super) send_wait_ns: AtomicU64,
    pub(super) tls_failed: AtomicU64,
    pub(super) packets_lost: AtomicU64,
    pub(super) data_blocked: AtomicU64,
    pub(super) stream_data_blocked: AtomicU64,
    pub(super) streams_blocked: AtomicU64,
}

#[derive(Clone, Debug, Serialize)]
pub struct TransportSnapshot {
    /// Eight most recent classified failures, including RPC close causes before cleanup.
    pub failures: TransportFailures,
    pub connections_total: u64,
    /// Physical connection resources still retained, including draining handles.
    pub active_connections: u64,
    pub liveness_probes_total: u64,
    pub liveness_failures_total: u64,
    pub rejected_total: u64,
    pub streams_total: u64,
    pub frames_sent_total: u64,
    pub frames_received_total: u64,
    pub bytes_sent_total: u64,
    pub bytes_received_total: u64,
    pub deadlines_total: u64,
    pub malformed_total: u64,
    /// Time spent awaiting send admission, including QUIC flow/congestion effects.
    pub send_wait_ns_total: u64,
    pub tls_failed_total: u64,
    pub packets_lost_total: u64,
    pub data_blocked_frames_total: u64,
    pub stream_data_blocked_frames_total: u64,
    pub streams_blocked_frames_total: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecoverySnapshot {
    pub min_rtt_us: u64,
    pub smoothed_rtt_us: u64,
    pub latest_rtt_us: u64,
    pub pto_count: u32,
    pub congestion_window_bytes: u32,
    pub bytes_in_flight: u32,
    pub congestion_limited: bool,
}

impl TransportMetrics {
    pub fn snapshot(&self) -> TransportSnapshot {
        TransportSnapshot {
            failures: self
                .failures
                .lock()
                .map(|history| *history)
                .unwrap_or_default(),
            connections_total: self.connections.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            liveness_probes_total: self.liveness_probes.load(Ordering::Relaxed),
            liveness_failures_total: self.liveness_failures.load(Ordering::Relaxed),
            rejected_total: self.rejected.load(Ordering::Relaxed),
            streams_total: self.streams.load(Ordering::Relaxed),
            frames_sent_total: self.frames_sent.load(Ordering::Relaxed),
            frames_received_total: self.frames_received.load(Ordering::Relaxed),
            bytes_sent_total: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received_total: self.bytes_received.load(Ordering::Relaxed),
            deadlines_total: self.deadlines.load(Ordering::Relaxed),
            malformed_total: self.malformed.load(Ordering::Relaxed),
            send_wait_ns_total: self.send_wait_ns.load(Ordering::Relaxed),
            tls_failed_total: self.tls_failed.load(Ordering::Relaxed),
            packets_lost_total: self.packets_lost.load(Ordering::Relaxed),
            data_blocked_frames_total: self.data_blocked.load(Ordering::Relaxed),
            stream_data_blocked_frames_total: self.stream_data_blocked.load(Ordering::Relaxed),
            streams_blocked_frames_total: self.streams_blocked.load(Ordering::Relaxed),
        }
    }
}

impl Collector for TransportMetrics {
    // Inline fixed descriptors make the scrape's series and allocation bounds explicit.
    fn encode(&self, mut encoder: DescriptorEncoder) -> std::result::Result<(), std::fmt::Error> {
        let active = ConstGauge::new(self.active_connections.load(Ordering::Relaxed));
        active.encode(encoder.encode_descriptor(
            "active_connections",
            "Physical connection resources including retained draining handles",
            None,
            active.metric_type(),
        )?)?;
        let counters = [
            (
                "liveness_probes",
                "Native new-packet acknowledgement probes",
                &self.liveness_probes,
            ),
            (
                "liveness_failures",
                "Native liveness probes without timely authenticated progress",
                &self.liveness_failures,
            ),
            (
                "connections",
                "Authenticated physical connections",
                &self.connections,
            ),
            (
                "rejected",
                "Rejected connections or streams",
                &self.rejected,
            ),
            ("streams", "Admitted streams", &self.streams),
            (
                "frames_sent",
                "Application frames enqueued",
                &self.frames_sent,
            ),
            (
                "frames_received",
                "Application frames decoded",
                &self.frames_received,
            ),
            (
                "bytes_sent",
                "Application bytes enqueued including prefixes",
                &self.bytes_sent,
            ),
            (
                "bytes_received",
                "Decoded application bytes excluding prefixes",
                &self.bytes_received,
            ),
            ("deadlines", "Expired transport operations", &self.deadlines),
            (
                "malformed",
                "Failed frame reads or decoding",
                &self.malformed,
            ),
            (
                "tls_failed",
                "Failed native TLS handshakes",
                &self.tls_failed,
            ),
            (
                "packets_lost",
                "Native packet loss notifications including MTU probes",
                &self.packets_lost,
            ),
            (
                "data_blocked_frames",
                "Sent DATA_BLOCKED notifications",
                &self.data_blocked,
            ),
            (
                "stream_data_blocked_frames",
                "Sent STREAM_DATA_BLOCKED notifications",
                &self.stream_data_blocked,
            ),
            (
                "streams_blocked_frames",
                "Sent STREAMS_BLOCKED notifications",
                &self.streams_blocked,
            ),
        ];
        for (name, help, value) in counters {
            let counter = ConstCounter::new(value.load(Ordering::Relaxed));
            counter.encode(encoder.encode_descriptor(
                name,
                help,
                None,
                counter.metric_type(),
            )?)?;
        }
        let seconds =
            ConstCounter::new(self.send_wait_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0);
        seconds.encode(encoder.encode_descriptor(
            "send_wait_seconds",
            "Time awaiting send readiness and enqueueing",
            None,
            seconds.metric_type(),
        )?)?;
        Ok(())
    }
}

pub(super) fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(value))
    });
}
