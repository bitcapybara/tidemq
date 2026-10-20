//! Bounded real-SDK validation coordinated with host-owned container fault injection.

#[path = "deploy/ledger.rs"]
mod ledger;
#[path = "deploy/output.rs"]
mod output;
#[path = "deploy/recovery.rs"]
mod recovery;
#[path = "deploy/scenario.rs"]
mod scenario;

use std::{net::SocketAddr, path::PathBuf, process::ExitCode, time::Duration};

use anyhow::{Context, Result, ensure};
use clap::Parser;
use serde::Serialize;
use tidemq_sdk::{Bytes, Client, ClientConfig, ClientTls, Id, PeerTarget};
use tokio::time::{Instant, timeout_at};

use output::{Output, read_bounded, request_deadline, unix_ms};

#[global_allocator]
static ALLOCATOR: std::alloc::System = std::alloc::System;

#[derive(Parser)]
#[command(about = "Bounded real-SDK smoke and original-identity container fault validation")]
struct Args {
    #[arg(long)]
    endpoint: SocketAddr,
    #[arg(long)]
    server_name: String,
    #[arg(long)]
    ca: PathBuf,
    #[arg(long)]
    token_file: PathBuf,
    /// Must not exist; its writable parent must already exist.
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long, default_value_t = 64)]
    messages: u64,
    #[arg(long, default_value_t = 1_024)]
    message_bytes: usize,
    /// Produce first, then settle and claim the original consumer at the resulting H.
    #[arg(long, default_value_t = 0)]
    replay_settle_ms: u64,
    #[arg(long)]
    stage_fault: bool,
    #[arg(long)]
    require_unavailable: bool,
}

impl Args {
    fn validate(&self) -> Result<()> {
        ensure!(self.endpoint.port() > 0, "Endpoint port must be nonzero");
        ensure!(
            !self.server_name.is_empty()
                && self.server_name.len() <= 253
                && !self.server_name.chars().any(char::is_control),
            "Invalid TLS server name"
        );
        ensure!(
            (1..=256).contains(&self.messages),
            "Messages must be in 1..=256"
        );
        ensure!(
            (32..=65_536).contains(&self.message_bytes),
            "Message bytes must be in 32..=65536"
        );
        ensure!(
            self.replay_settle_ms <= 15_000,
            "Replay settle must be at most 15000 milliseconds"
        );
        ensure!(
            !self.require_unavailable || self.stage_fault,
            "Unavailable validation requires staged fault injection"
        );
        Ok(())
    }
}

#[derive(Serialize)]
struct PhaseTime {
    phase: &'static str,
    unix_ms: u64,
    elapsed_ms: u64,
}

#[derive(Serialize)]
struct Report {
    schema_version: u32,
    status: &'static str,
    configuration: serde_json::Value,
    ledger: ledger::Ledger,
    topic_id: Option<String>,
    subscription_id: Option<String>,
    initial_cold_before: Option<u64>,
    same_consumer_session_verified: bool,
    authoritative_frontiers: Vec<u64>,
    batches: Vec<recovery::BatchEvidence>,
    phases: Vec<PhaseTime>,
    last_phase: &'static str,
    failure_phase: Option<&'static str>,
    failure_sdk_error: Option<recovery::ErrorObservation>,
    ack_unknown_retries: u32,
    sdk_before: Option<serde_json::Value>,
    sdk_after: Option<serde_json::Value>,
    transport_after: Option<serde_json::Value>,
    client_close_completed: bool,
    #[serde(skip)]
    started: Instant,
}

impl Report {
    fn new(args: &Args, run: Id, total_seconds: u64, started: Instant) -> Result<Self> {
        let expected = args
            .messages
            .checked_add(if args.stage_fault { 16 } else { 0 })
            .context("Expected message count overflow")?;
        Ok(Self {
            schema_version: 1,
            status: "failed",
            configuration: serde_json::json!({"endpoint":args.endpoint,"server_name":args.server_name,
                "messages":args.messages,"message_bytes":args.message_bytes,"replay_settle_ms":args.replay_settle_ms,
                "stage_fault":args.stage_fault,"require_unavailable":args.require_unavailable,
                "request_timeout_ms":10000,"total_timeout_seconds":total_seconds,"host_wait_timeout_seconds":120,
                "producer_workers":1,"consumer_instances":1,"consumer_credit_messages":256,
                "consumer_credit_bytes":4194304,"batch_messages":8,"allocator":"system"}),
            ledger: ledger::Ledger::new(expected, args.message_bytes, run)?,
            topic_id: None,
            subscription_id: None,
            initial_cold_before: None,
            same_consumer_session_verified: false,
            authoritative_frontiers: Vec::new(),
            batches: Vec::new(),
            phases: Vec::new(),
            last_phase: "created",
            failure_phase: None,
            failure_sdk_error: None,
            ack_unknown_retries: 0,
            sdk_before: None,
            sdk_after: None,
            transport_after: None,
            client_close_completed: false,
            started,
        })
    }

    fn mark(&mut self, phase: &'static str) -> Result<()> {
        ensure!(self.phases.len() < 32, "Phase timestamp bound exceeded");
        self.phases.push(PhaseTime {
            phase,
            unix_ms: unix_ms()?,
            elapsed_ms: u64::try_from(self.started.elapsed().as_millis())?,
        });
        self.last_phase = phase;
        Ok(())
    }
}

#[tokio::main(worker_threads = 4)]
async fn main() -> ExitCode {
    match run(&Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Deployment validation failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &Args) -> Result<()> {
    args.validate()?;
    let started = Instant::now();
    let seconds = if args.stage_fault { 360 } else { 180 };
    let total = started
        .checked_add(Duration::from_secs(seconds))
        .context("Task deadline overflow")?;
    // Ten seconds remain for owned SDK shutdown and private report publication.
    let work = total
        .checked_sub(Duration::from_secs(10))
        .context("Work deadline underflow")?;
    let output = timeout_at(work, Output::create(&args.output_dir))
        .await
        .context("Output setup exceeded the task deadline")??;
    let run = Id::random()?;
    let mut report = Report::new(args, run, seconds, started)?;
    report.mark("connecting")?;
    let mut result = match timeout_at(work, connect(args, work)).await {
        Ok(Ok(client)) => {
            validate_connected(args, &output, &client, run, &mut report, work, total).await
        }
        Ok(Err(error)) => Err(error),
        Err(error) => Err(error).context("Connection setup exceeded the work deadline"),
    };
    if result.is_ok() {
        report.status = "passed";
    } else {
        report.failure_phase = Some(report.last_phase);
        report.failure_sdk_error = result.as_ref().err().and_then(|error| {
            error
                .chain()
                .find_map(|source| source.downcast_ref::<tidemq_sdk::Error>())
                .map(recovery::ErrorObservation::read)
        });
    }
    if let Err(error) = report.mark("validation_finished") {
        report.status = "failed";
        if result.is_ok() {
            result = Err(error);
        }
    }
    timeout_at(total, output.write("report.json", &report))
        .await
        .context("Report publication exceeded the original task deadline")??;
    result
}

// The one client remains available for bounded shutdown after any scenario error.
async fn validate_connected(
    args: &Args,
    output: &Output,
    client: &Client,
    run: Id,
    report: &mut Report,
    work: Instant,
    total: Instant,
) -> Result<()> {
    report.sdk_before = Some(client_metrics(client));
    let mut result =
        match timeout_at(work, scenario::run(args, output, client, run, report, work)).await {
            Ok(result) => result,
            Err(error) => Err(error).context("Validation exceeded the original work deadline"),
        };
    report.sdk_after = Some(client_metrics(client));
    match serde_json::to_value(client.transport_metrics().snapshot()) {
        Ok(snapshot) => report.transport_after = Some(snapshot),
        Err(error) if result.is_ok() => {
            result = Err(error).context("Serialize bounded transport counters")
        }
        Err(_) => {}
    }
    let close = total
        .checked_sub(Duration::from_secs(2))
        .context("Close deadline underflow")?
        .min(
            Instant::now()
                .checked_add(Duration::from_secs(5))
                .context("Close deadline overflow")?,
        );
    match timeout_at(close, client.close()).await {
        Ok(()) => report.client_close_completed = true,
        Err(error) if result.is_ok() => {
            result = Err(error).context("Owned SDK close exceeded its budget")
        }
        Err(_) => {}
    }
    result
}

async fn connect(args: &Args, deadline: Instant) -> Result<Client> {
    let roots = read_bounded(&args.ca, 65_536).await?;
    let token = String::from_utf8(read_bounded(&args.token_file, 8_192).await?)
        .context("Token file must be UTF-8")?;
    let mut configuration = ClientConfig::new(args.endpoint.to_string());
    configuration.request_timeout_ms = 10_000;
    Ok(Client::with_target(
        configuration,
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
        token.trim_end_matches(['\r', '\n']).into(),
    )
    .await?)
}

fn client_metrics(client: &Client) -> serde_json::Value {
    let value = client.metrics().snapshot();
    serde_json::json!({"requests":value.requests,"retries":value.retries,"unknown":value.unknown,
        "deliveries":value.deliveries,"acknowledged":value.acknowledged,"admitted_bytes":value.admitted_bytes,
        "pending_batches":value.pending_batches,"active_deliveries":value.active_deliveries,
        "request_duration_ns":value.request_duration_ns,"admission_wait_ns":value.admission_wait_ns})
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Args {
        Args::try_parse_from([
            "deploy_validation",
            "--endpoint",
            "127.0.0.1:7410",
            "--server-name",
            "localhost",
            "--ca",
            "ca.pem",
            "--token-file",
            "access.token",
            "--output-dir",
            "new-case",
        ])
        .unwrap()
    }

    #[test]
    fn cli_defaults_and_resource_boundaries_are_fixed() {
        let mut options = args();
        options.validate().unwrap();
        assert_eq!(
            (
                options.messages,
                options.message_bytes,
                options.replay_settle_ms
            ),
            (64, 1024, 0)
        );
        options.messages = 257;
        assert!(options.validate().is_err());
        options.messages = 256;
        options.message_bytes = 65_536;
        options.replay_settle_ms = 15_000;
        options.stage_fault = true;
        options.require_unavailable = true;
        options.validate().unwrap();
        options.stage_fault = false;
        assert!(options.validate().is_err());
        options.require_unavailable = false;
        options.replay_settle_ms = 15_001;
        assert!(options.validate().is_err());
        options.replay_settle_ms = 0;
        options.message_bytes = 31;
        assert!(options.validate().is_err());
    }

    #[test]
    fn report_contains_counts_without_message_content_or_private_ledger() {
        let options = args();
        let mut report = Report::new(&options, Id::random().unwrap(), 180, Instant::now()).unwrap();
        for _ in 0..32 {
            report.mark("test_phase").unwrap();
        }
        assert!(report.mark("excess_phase").is_err());
        let json = serde_json::to_value(report).unwrap();
        let ledger = json.get("ledger").unwrap().as_object().unwrap();
        assert_eq!(ledger.len(), 5);
        assert_eq!(ledger.get("expected").unwrap(), 64);
        assert!(ledger.get("run").is_none());
        assert!(ledger.get("seen").is_none());
        assert!(ledger.get("acked").is_none());
        let text = serde_json::to_string(&json).unwrap();
        assert!(text.len() < 65_536);
        assert!(!text.contains("access.token"));
        assert!(!text.contains("ca.pem"));
    }
}
