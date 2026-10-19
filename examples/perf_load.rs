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
                .is_some_and(|bytes| bytes <= 8 * 1024 * 1024),
            "Concurrent production exceeded the fixed 8 MiB generator bound"
        );
        let total_bytes = self
            .messages
            .max(self.warmup_messages)
            .checked_mul(u64::try_from(self.message_bytes + 8)?)
            .context("Phase byte count overflow")?;
        ensure!(
            total_bytes <= 256 * 1024 * 1024,
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
    identity: sample::Identity,
    warmup: Option<workload::PhaseReport>,
    measured: workload::PhaseReport,
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
    let mut config = ClientConfig::new(args.endpoint.to_string());
    config.request_timeout_ms = args.request_timeout_ms;
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
        Instant::now()
            .checked_add(Duration::from_millis(args.request_timeout_ms))
            .context("Deadline overflow")?,
    )
    .await?;
    let nonce = Id::random()?;
    let warmup = if args.warmup_messages > 0 {
        Some(workload::phase(&client, args, "warmup", nonce, args.warmup_messages).await?)
    } else {
        None
    };
    let measured = workload::phase(&client, args, "measured", nonce, args.messages).await?;
    let mut cooldown = Vec::new();
    cooldown.try_reserve_exact(args.cooldown_secs as usize + 1)?;
    let started = Instant::now();
    for _ in 0..=args.cooldown_secs {
        cooldown.push(sample::resource(args, started).await);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    client.close(client.deadline()?).await;
    let passed = measured.terminal_error.is_none()
        && warmup
            .as_ref()
            .is_none_or(|phase| phase.terminal_error.is_none());
    Ok(Report {
        schema_version: 1,
        status: if passed { "passed" } else { "failed" },
        generator_allocator: "std::alloc::System; four Tokio worker threads",
        configuration: args.clone(),
        identity,
        warmup,
        measured,
        cooldown,
        limitations: [
            "Closed-loop finite load; no open-loop saturation or coordinated-omission correction claim",
            "RF1 local pilot does not prove RF3, three physical availability zones or production SLOs",
            "Replay assignment alone is not physical cold I/O; require_cold checks a real server counter delta",
            "P99 requires 1000 samples and P99.9 requires 10000; missing tails are null",
            "Configured observability label must be checked against the captured service config and runtime capabilities",
        ],
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
}
