//! Bounded load generator for a separately running, authenticated TideMQ service.

#[path = "perf/sample.rs"]
mod sample;
#[path = "perf/stats.rs"]
mod stats;
#[path = "perf/workload.rs"]
mod workload;

use std::{net::SocketAddr, path::PathBuf, process::ExitCode, time::Duration};

use anyhow::{Context, Result, ensure};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use tidemq_sdk::{Bytes, Client, ClientConfig, ClientTls, Id, PeerTarget};
use tokio::{io::AsyncWriteExt, time::Instant};

// Examples are independent binaries and do not inherit src/main.rs allocator selection.
#[global_allocator]
static ALLOCATOR: std::alloc::System = std::alloc::System;

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Live,
    Replay,
}

#[derive(Parser, Clone, Serialize)]
#[command(about = "Bounded real-SDK load against an independent TideMQ OS process")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7443")]
    endpoint: SocketAddr,
    #[arg(long, default_value = "localhost")]
    server_name: String,
    #[arg(long)]
    ca: PathBuf,
    /// File containing one token; credentials never appear in arguments or reports.
    #[arg(long)]
    token_file: PathBuf,
    #[arg(long)]
    management: SocketAddr,
    #[arg(long)]
    service_pid: u32,
    #[arg(long)]
    service_binary: PathBuf,
    #[arg(long)]
    service_config: PathBuf,
    /// Existing build identity JSON produced by scripts/perf-build.sh.
    #[arg(long)]
    build_identity: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 4_096)]
    messages: u64,
    #[arg(long, default_value_t = 256)]
    warmup_messages: u64,
    #[arg(long, default_value_t = 128)]
    message_bytes: usize,
    #[arg(long, default_value_t = 32)]
    batch_messages: u32,
    #[arg(long, default_value_t = 4)]
    workers: usize,
    /// Independent one-partition topics measured concurrently after serial setup.
    #[arg(long, default_value_t = 1)]
    concurrent_partitions: usize,
    /// Fixed per-partition offer interval; zero selects the finite burst workload.
    #[arg(long, default_value_t = 0)]
    produce_interval_ms: u64,
    /// Reused Request streams per active data class; Core and ACK limits stay fixed.
    #[arg(long, default_value_t = 4)]
    request_lanes: usize,
    #[arg(long, default_value_t = 32)]
    ack_messages: usize,
    #[arg(long, default_value_t = 0)]
    consumer_delay_ms: u64,
    #[arg(long, default_value_t = 120)]
    phase_timeout_secs: u64,
    #[arg(long, default_value_t = 10_000)]
    request_timeout_ms: u64,
    #[arg(long, default_value_t = 3)]
    cooldown_secs: u64,
    #[arg(long, value_enum, default_value_t = Mode::Live)]
    mode: Mode,
    #[arg(long, default_value_t = 1_000)]
    replay_settle_ms: u64,
    /// Fail replay validation unless the server's cold-cache-miss counter increases.
    #[arg(long, default_value_t = false)]
    require_cold: bool,
    /// Must describe a real server mode; this flag does not alter server observability.
    #[arg(long, default_value = "recommended")]
    observability: String,
}

impl Args {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.management.ip().is_loopback(),
            "Management sampling requires loopback"
        );
        ensure!(
            self.service_pid > 1 && self.service_pid != std::process::id(),
            "A separate service PID is required"
        );
        ensure!(
            (1..=1_000_000).contains(&self.messages),
            "Measured messages must be in 1..=1000000"
        );
        ensure!(
            self.warmup_messages <= 100_000,
            "Warmup messages exceeded their bound"
        );
        ensure!(
            (32..=262_136).contains(&self.message_bytes),
            "Body bytes must be in 32..=262136, reserving eight key bytes within the message limit"
        );
        ensure!(
            (1..=1_024).contains(&self.batch_messages),
            "Batch count exceeded the protocol bound"
        );
        ensure!(
            (1..=64).contains(&self.workers),
            "Workers must be in 1..=64"
        );
        ensure!(
            (1..=4).contains(&self.concurrent_partitions),
            "Concurrent partitions must be in 1..=4"
        );
        ensure!(
            matches!(self.request_lanes, 2 | 4),
            "Request lanes must be either 2 or 4 for the fixed-budget comparison"
        );
        ensure!(
            (1..=128).contains(&self.ack_messages),
            "ACK count must be in 1..=128"
        );
        ensure!(
            (1..=600).contains(&self.phase_timeout_secs),
            "Phase deadline must be in 1..=600 seconds"
        );
        ensure!(
            (100..=60_000).contains(&self.request_timeout_ms),
            "Request deadline must be in 100..=60000 milliseconds"
        );
        ensure!(
            self.cooldown_secs <= 60
                && self.consumer_delay_ms <= 1_000
                && self.replay_settle_ms <= 30_000,
            "Timing option exceeded its bound"
        );
        ensure!(
            self.observability.len() <= 64 && !self.observability.chars().any(char::is_control),
            "Observability identity is invalid"
        );
        ensure!(
            !self.require_cold || matches!(self.mode, Mode::Replay),
            "Cold validation requires replay mode"
        );
        ensure!(
            self.produce_interval_ms <= 1_000,
            "Offer interval exceeded one second"
        );
        let offered_ms = self
            .messages
            .max(self.warmup_messages)
            .div_ceil(u64::from(self.batch_messages))
            .checked_mul(self.produce_interval_ms)
            .context("Offer schedule overflow")?;
        ensure!(
            self.produce_interval_ms == 0
                || offered_ms
                    .checked_add(self.request_timeout_ms)
                    .is_some_and(|duration| duration < self.phase_timeout_secs * 1_000),
            "Offer schedule must leave a full request budget inside the phase deadline"
        );
        let batch_bytes = (self.message_bytes + 96)
            .checked_mul(self.batch_messages as usize)
            .context("Batch size overflow")?;
        ensure!(
            batch_bytes <= 900_000,
            "Encoded batch must leave protocol framing headroom"
        );
        ensure!(
            batch_bytes
                .checked_mul(self.workers)
                .and_then(|bytes| bytes.checked_mul(self.concurrent_partitions))
                .is_some_and(|bytes| bytes <= 8 * 1024 * 1024),
            "Concurrent production exceeded the fixed 8 MiB generator bound"
        );
        let total_bytes = self
            .messages
            .max(self.warmup_messages)
            .checked_mul(u64::try_from(self.message_bytes + 8)?)
            .context("Phase byte count overflow")?;
        ensure!(
            total_bytes
                .checked_mul(u64::try_from(self.concurrent_partitions)?)
                .is_some_and(|bytes| bytes <= 256 * 1024 * 1024),
            "One phase exceeded the 256 MiB logical byte limit"
        );
        Ok(())
    }
}

#[derive(Serialize)]
struct Report {
    schema_version: u32,
    status: &'static str,
    generator_allocator: &'static str,
    configuration: Args,
    sdk_configuration: serde_json::Value,
    identity: sample::Identity,
    warmup: Option<workload::GroupReport>,
    measured: workload::GroupReport,
    cooldown: Vec<sample::ResourceSample>,
    limitations: [&'static str; 5],
}

#[tokio::main(worker_threads = 4)]
async fn main() -> ExitCode {
    let args = Args::parse();
    let result = run(&args).await;
    let (report, passed) = match result {
        Ok(report) => {
            let passed = report.status == "passed";
            (serde_json::to_value(report), passed)
        }
        Err(error) => (
            Ok(
                serde_json::json!({"schema_version":1,"status":"failed","error":format!("{error:#}")}),
            ),
            false,
        ),
    };
    let saved = match report {
        Ok(report) => save(&args.output, &report).await,
        Err(error) => Err(error.into()),
    };
    match saved {
        Ok(()) if passed => ExitCode::SUCCESS,
        Ok(()) => {
            eprintln!("Load validation failed; inspect the bounded report");
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("Cannot save load report: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &Args) -> Result<Report> {
    args.validate()?;
    let identity = sample::Identity::capture(args).await?;
    let roots = sample::read_bounded(&args.ca, 65_536).await?;
    let credential = sample::read_bounded(&args.token_file, 8_192).await?;
    let credential = String::from_utf8(credential).context("Token file must be UTF-8")?;
    let config = client_config(args);
    let sdk_configuration = sdk_configuration(&config);
    let client = Client::with_target(
        config,
        ClientTls {
            trust_roots: Bytes::from(roots),
            identity: None,
            expected_nodes: Vec::new(),
        },
        PeerTarget {
            address: args.endpoint,
            server_name: args.server_name.clone(),
            local_role: "sdk".into(),
            remote_role: "gateway".into(),
            cluster_id: None,
            node_id: None,
        },
        credential.trim_end_matches(['\r', '\n']).into(),
    )
    .await?;
    let nonce = Id::new_v7()?;
    let warmup = if args.warmup_messages > 0 {
        Some(workload::group(&client, args, "warmup", nonce, args.warmup_messages).await?)
    } else {
        None
    };
    let measured = workload::group(&client, args, "measured", nonce, args.messages).await?;
    let mut cooldown = Vec::new();
    cooldown.try_reserve_exact(args.cooldown_secs as usize + 1)?;
    let started = Instant::now();
    for second in 0..=args.cooldown_secs {
        cooldown.push(sample::resource(args, started).await);
        if second < args.cooldown_secs {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    client.close().await;
    let passed = measured
        .partitions
        .iter()
        .all(|phase| phase.terminal_error.is_none())
        && warmup.as_ref().is_none_or(|group| {
            group
                .partitions
                .iter()
                .all(|phase| phase.terminal_error.is_none())
        });
    Ok(Report {
        schema_version: 3,
        status: if passed { "passed" } else { "failed" },
        generator_allocator: "std::alloc::System; four Tokio worker threads",
        configuration: args.clone(),
        sdk_configuration,
        identity,
        warmup,
        measured,
        cooldown,
        limitations: [
            "Bounded finite load; paced offers retain planned timestamps and scheduling lateness without a saturation claim",
            "Loopback nodes do not prove three physical availability zones or production SLOs",
            "Replay assignment alone is not physical cold I/O; require_cold checks a real server counter delta",
            "P99 requires 1000 samples and P99.9 requires 10000; missing tails are null",
            "Configured observability label must be checked against the captured service config and runtime capabilities",
        ],
    })
}

fn client_config(args: &Args) -> ClientConfig {
    let mut config = ClientConfig::new(args.endpoint.to_string());
    config.request_timeout_ms = args.request_timeout_ms;
    config.request_lanes = args.request_lanes;
    config
}

fn sdk_configuration(config: &ClientConfig) -> serde_json::Value {
    serde_json::json!({
        "request_lanes": config.request_lanes,
        "max_pending_requests": config.max_pending_requests,
        "max_pending_bytes": config.max_pending_bytes,
        "transport_memory_bytes": config.transport_memory_bytes,
        "transport_control_reserved_bytes": config.transport_control_reserved_bytes,
        "max_connections": config.transport.max_connections,
        "max_handshakes": config.transport.max_handshakes,
        "max_streams": config.transport.max_streams,
        "core_streams": config.transport.core_streams,
        "max_ack_streams": config.transport.max_ack_streams,
        "connection_window_bytes": config.transport.connection_window_bytes,
        "stream_window_bytes": config.transport.stream_window_bytes,
        "send_buffer_bytes": config.transport.send_buffer_bytes,
        "liveness_timeout_ms": config.liveness_timeout.as_millis(),
    })
}

async fn save(path: &std::path::Path, report: &serde_json::Value) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(report)?;
    ensure!(
        bytes.len() <= 8 * 1024 * 1024,
        "Report exceeded the 8 MiB output budget"
    );
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(path)
        .await
        .context("Create a new private report file")?;
    file.write_all(&bytes).await?;
    file.write_all(b"\n").await?;
    file.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Args {
        Args::parse_from([
            "perf_load",
            "--ca",
            "ca.pem",
            "--token-file",
            "token",
            "--management",
            "127.0.0.1:9419",
            "--service-pid",
            "1234",
            "--service-binary",
            "tidemq",
            "--service-config",
            "node.toml",
            "--build-identity",
            "identity.json",
            "--output",
            "report.json",
        ])
    }

    #[test]
    fn bounds_reject_excessive_inflight_bytes_and_invalid_cold_claims() {
        let mut args = args();
        args.validate().unwrap();
        args.message_bytes = 262_144;
        assert!(args.validate().is_err());
        args.batch_messages = 1;
        args.messages = 64;
        assert!(args.validate().is_err());
        args.message_bytes = 262_136;
        args.validate().unwrap();
        args.require_cold = true;
        assert!(args.validate().is_err());
        args.mode = Mode::Replay;
        args.validate().unwrap();
    }

    #[test]
    fn lane_comparison_accepts_only_two_or_four_and_preserves_all_fixed_budgets() {
        let mut args = args();
        for lanes in [0, 1, 3, 8] {
            args.request_lanes = lanes;
            assert!(args.validate().is_err());
        }
        let snapshots = [2, 4].map(|lanes| {
            args.request_lanes = lanes;
            args.validate().unwrap();
            let config = client_config(&args);
            config.validate().unwrap();
            let mut snapshot = sdk_configuration(&config);
            assert_eq!(snapshot.get("request_lanes").unwrap(), lanes);
            snapshot.as_object_mut().unwrap().remove("request_lanes");
            snapshot
        });
        assert_eq!(snapshots.first(), snapshots.last());
    }

    #[test]
    fn concurrent_partition_count_shares_the_existing_finite_byte_budget() {
        let mut args = args();
        args.concurrent_partitions = 4;
        args.validate().unwrap();
        args.concurrent_partitions = 5;
        assert!(args.validate().is_err());
        args.concurrent_partitions = 4;
        args.message_bytes = 262_136;
        args.batch_messages = 1;
        args.messages = 256;
        args.warmup_messages = 0;
        args.validate().unwrap();
        args.messages = 257;
        assert!(args.validate().is_err());
    }

    #[test]
    fn scheduled_latency_phase_leaves_its_original_request_budget() {
        let mut args = args();
        args.messages = 256;
        args.warmup_messages = 64;
        args.batch_messages = 1;
        args.produce_interval_ms = 160;
        args.phase_timeout_secs = 60;
        args.validate().unwrap();
        args.phase_timeout_secs = 50;
        assert!(args.validate().is_err());
        args.phase_timeout_secs = 60;
        args.produce_interval_ms = 1_001;
        assert!(args.validate().is_err());
    }
}
