use std::path::PathBuf;

use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Invalid configuration: {reason}"))]
    Configuration { reason: &'static str },
    #[snafu(display("Failed to {operation}: {}", path.display()))]
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Failed to parse configuration: {}", path.display()))]
    ParseConfiguration {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[snafu(display("Invalid protocol configuration or message"))]
    Protocol { source: tidemq_protocol::Error },
    #[snafu(display("Invalid cluster_id configuration"))]
    ClusterIdentity { source: tidemq_protocol::Error },
    #[snafu(display("Transport operation failed"))]
    Transport { source: tidemq_transport::Error },
    #[snafu(display("Admission budget is exhausted for {traffic_class}"))]
    Admission {
        traffic_class: &'static str,
        source: tokio::sync::TryAcquireError,
    },
    #[snafu(display("Local request memory admission failed"))]
    LocalMemory { source: crate::budget::Error },
    #[snafu(display("Failed to initialize logging"))]
    Logging {
        source: tracing::subscriber::SetGlobalDefaultError,
    },
    #[snafu(display("Trace export failed: {reason}"))]
    TraceExport { reason: &'static str },
    #[snafu(display("Failed to initialize bounded OTLP exporter"))]
    TraceExporter {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Invalid log filter"))]
    LogFilter {
        source: tracing_subscriber::filter::ParseError,
    },
    #[snafu(display("Failed to reload log filter"))]
    LogReload {
        source: tracing_subscriber::reload::Error,
    },
    #[snafu(display("Failed to encode metrics"))]
    Metrics { source: std::fmt::Error },
    #[snafu(display("Allocator statistics are unavailable: {reason}"))]
    Allocator { reason: &'static str },
    #[snafu(display("Failed to decode allocator statistics"))]
    AllocatorJson { source: serde_json::Error },
    #[snafu(display("Failed to execute a blocking diagnostic task"))]
    Task { source: tokio::task::JoinError },
    #[snafu(display("Failed to serialize diagnostic output"))]
    Json { source: serde_json::Error },
    #[snafu(display("Failed to reserve application buffer"))]
    Reserve {
        source: std::collections::TryReserveError,
    },
    #[cfg(feature = "allocator-jemalloc")]
    #[snafu(display("Failed to read jemalloc statistics or configuration"))]
    Jemalloc { source: tikv_jemalloc_ctl::Error },
}
