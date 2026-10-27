//! Bounded real-SDK measurements against a separate service process.

#[path = "single_node_bench/ledger.rs"]
mod ledger;
#[path = "single_node_bench/workload.rs"]
mod workload;

use std::{
    net::SocketAddr,
    path::PathBuf,
    process::ExitCode,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use tidemq_protocol::Id;
use tidemq_sdk::{Client, Credentials, MetricsSnapshot, TlsTrust};
use tokio::{io::AsyncWriteExt, time::timeout};

#[derive(Clone, Copy, Debug, PartialEq, Serialize, ValueEnum)]
enum Case {
    Traffic,
    Idle,
}

#[derive(Parser, Serialize)]
#[command(about = "Bounded standalone real-SDK throughput, latency and idle measurement")]
struct Args {
    #[arg(long)]
    endpoint: SocketAddr,
    #[arg(long)]
    #[serde(skip)]
    ca: PathBuf,
    #[arg(long)]
    #[serde(skip)]
    token_file: PathBuf,
    /// Must not exist. Private reports include the complete identity and sequence ledger.
    #[arg(long)]
    #[serde(skip)]
    output_dir: PathBuf,
    #[arg(long, value_enum, default_value_t = Case::Traffic)]
    case: Case,
    #[arg(long, default_value_t = 2048)]
    messages: usize,
    #[arg(long, default_value_t = 1024)]
    message_bytes: usize,
    /// Shared original producer and Queue consumer, with this many send and ACK workers.
    #[arg(long, default_value_t = 4)]
    concurrency: usize,
    #[arg(long, default_value_t = 10)]
    idle_seconds: u64,
    #[arg(long, default_value_t = 120)]
    timeout_seconds: u64,
    /// Publish ready.json, then wait for the harness to create output-dir/start.
    #[arg(long)]
    wait_for_start: bool,
    /// Retain integrity evidence, but omit timing summaries from profiled runs.
    #[arg(long)]
    profiled: bool,
}

impl Args {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.endpoint.ip().is_loopback() && self.endpoint.port() > 0,
            "A loopback endpoint with a nonzero port is required"
        );
        ensure!(
            (1..=200_000).contains(&self.messages),
            "Messages must be in 1..=200000"
        );
        ensure!(
            (32..=65_536).contains(&self.message_bytes),
            "Message bytes must be in 32..=65536"
        );
        ensure!(
            (1..=32).contains(&self.concurrency) && self.concurrency <= self.messages,
            "Concurrency must be in 1..=32 and not exceed messages"
        );
        ensure!(
            self.messages
                .checked_mul(self.message_bytes)
                .context("Payload budget overflow")?
                <= 512 * 1024 * 1024,
            "Total payload exceeds 512 MiB"
        );
        ensure!(
            (1..=60).contains(&self.idle_seconds),
            "Idle seconds must be in 1..=60"
        );
        ensure!(
            (15..=600).contains(&self.timeout_seconds) && self.idle_seconds < self.timeout_seconds,
            "Timeout must be in 15..=600 and exceed idle seconds"
        );
        Ok(())
    }
}

#[derive(Serialize)]
struct Report<'a> {
    schema_version: u32,
    status: &'static str,
    configuration: &'a Args,
    elapsed_ns: Option<u64>,
    acknowledged_messages_per_second: Option<f64>,
    send_commit_latency: Option<ledger::Latency>,
    produce_to_receive_latency: Option<ledger::Latency>,
    sdk_before: Option<serde_json::Value>,
    sdk_after: Option<serde_json::Value>,
    sdk_after_tail: Option<serde_json::Value>,
    sdk_after_close: Option<serde_json::Value>,
    unresolved_sends: Vec<String>,
    unresolved_acks: Option<usize>,
    close_complete: bool,
    failure: Option<String>,
    failure_sdk_kind: Option<String>,
    failure_outcome_unknown: Option<bool>,
}

#[tokio::main(worker_threads = 4)]
async fn main() -> ExitCode {
    match run(&Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Measurement failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &Args) -> Result<()> {
    args.validate()?;
    let mut builder = tokio::fs::DirBuilder::new();
    #[cfg(unix)]
    builder.mode(0o700);
    builder
        .create(&args.output_dir)
        .await
        .context("Create a new output directory")?;
    let count = if args.case == Case::Traffic {
        args.messages
    } else {
        0
    };
    let ledger = Arc::new(Mutex::new(ledger::Ledger::new(count, Id::new_v7()?)?));
    let mut report = Report {
        schema_version: 1,
        status: "failed",
        configuration: args,
        elapsed_ns: None,
        acknowledged_messages_per_second: None,
        send_commit_latency: None,
        produce_to_receive_latency: None,
        sdk_before: None,
        sdk_after: None,
        sdk_after_tail: None,
        sdk_after_close: None,
        unresolved_sends: Vec::new(),
        unresolved_acks: None,
        close_complete: false,
        failure: None,
        failure_sdk_kind: None,
        failure_outcome_unknown: None,
    };
    let mut client = None;
    let result = timeout(Duration::from_secs(args.timeout_seconds), async {
        client = Some(
            Client::builder(
                args.endpoint.to_string(),
                TlsTrust::ca_file(&args.ca),
                Credentials::token_file(&args.token_file),
            )
            .request_timeout(Duration::from_secs(10))
            .build()
            .await?,
        );
        measure(
            args,
            client.as_ref().context("Client is unavailable")?,
            &ledger,
            &mut report,
        )
        .await
    })
    .await
    .context("Original measurement deadline exceeded")
    .and_then(|result| result);
    if let Err(error) = &result {
        failure(&mut report, error);
    }
    if let Some(client) = client {
        close_client(&client, &mut report).await;
    }
    if result.is_ok() && report.failure.is_none() {
        match summarize(&ledger, &mut report) {
            Ok(()) => report.status = "passed",
            Err(error) => failure(&mut report, &error),
        }
    }
    let ledger_bytes = serde_json::to_vec(&*workload::lock(&ledger)?)?;
    write(args, "ledger.json", ledger_bytes).await?;
    write(args, "report.json", serde_json::to_vec_pretty(&report)?).await?;
    ensure!(
        report.status == "passed",
        "Measurement invalid; see report.json and ledger.json"
    );
    Ok(())
}

async fn close_client(client: &Client, report: &mut Report<'_>) {
    match timeout(Duration::from_secs(5), client.close()).await {
        Ok(Ok(close))
            if close.local_cleanup_complete()
                && close.unresolved_sends() == 0
                && close.unresolved_acks() == 0
                && close.unresolved_reads() == 0 =>
        {
            report.close_complete = true
        }
        Ok(Ok(close)) => {
            report
                .failure
                .get_or_insert_with(|| format!("SDK close incomplete: {close:?}"));
        }
        Ok(Err(error)) => {
            report.failure.get_or_insert_with(|| {
                format!("SDK close failed: {:#}", anyhow::Error::new(error))
            });
        }
        Err(error) => {
            report.failure.get_or_insert_with(|| {
                format!("SDK close timed out: {:#}", anyhow::Error::new(error))
            });
        }
    }
    let after = client.metrics().snapshot();
    report.sdk_after_close = Some(metrics(after));
    if after.unknown != 0 {
        report
            .failure
            .get_or_insert_with(|| "SDK observed an Unknown outcome during this run".into());
    }
}

fn failure(report: &mut Report<'_>, error: &anyhow::Error) {
    report.failure = Some(format!("{error:#}"));
    if let Some(error) = error
        .chain()
        .find_map(|source| source.downcast_ref::<tidemq_sdk::Error>())
    {
        report.failure_sdk_kind = Some(format!("{:?}", error.kind()));
        report.failure_outcome_unknown = Some(error.outcome_unknown());
    }
}

async fn measure(
    args: &Args,
    client: &Client,
    ledger: &workload::SharedLedger,
    report: &mut Report<'_>,
) -> Result<()> {
    let run = workload::lock(ledger)?.run_id;
    let tenant_name = format!("perf-{run}");
    let admin = client.admin();
    let tenant = admin
        .create_tenant(&tenant_name)
        .await
        .context("Create benchmark tenant")?;
    admin
        .create_namespace(tenant, "default")
        .await
        .context("Create benchmark namespace")?;
    let topic = format!("{tenant_name}/default/messages");
    admin
        .create_topic(&topic)
        .await
        .context("Create benchmark topic")?;
    let producer = client.new_producer(&topic).build().await?;
    let consumer = client
        .new_queue_consumer(&topic, "perf")
        .prefetch_messages(256)
        .prefetch_bytes(4 * 1024 * 1024)
        .build()
        .await?;
    write(
        args,
        "ready.json",
        serde_json::to_vec(&serde_json::json!({"topic":topic,"run_id":run}))?,
    )
    .await?;
    if args.wait_for_start {
        wait_marker(args, "start").await?;
    }
    let before = client.metrics().snapshot();
    report.sdk_before = Some(metrics(before));
    ensure!(
        before.unknown == 0,
        "SDK observed an Unknown outcome during setup"
    );
    let result = match args.case {
        Case::Traffic => workload::traffic(args, &producer, &consumer, ledger).await,
        Case::Idle => workload::idle(&consumer, args.idle_seconds).await,
    };
    report.sdk_after = Some(metrics(client.metrics().snapshot()));
    report.unresolved_sends = producer
        .pending_sends()
        .await?
        .iter()
        .map(|pending| format!("{pending:?}"))
        .collect();
    report.unresolved_acks = Some(consumer.pending_acks().await?.len());
    report.elapsed_ns = Some(result?);
    ensure!(
        client.metrics().snapshot().unknown == before.unknown,
        "SDK observed an Unknown outcome; measurement is invalid"
    );
    ensure!(
        report.unresolved_sends.is_empty() && report.unresolved_acks == Some(0),
        "Original Pending operations remain unresolved"
    );
    workload::check_tail(&consumer).await?;
    report.sdk_after_tail = Some(metrics(client.metrics().snapshot()));
    write(args, "finished.json", b"{}\n".to_vec()).await?;
    if args.wait_for_start {
        wait_marker(args, "collected").await?;
    }
    consumer.close().await?;
    producer.close().await?;
    Ok(())
}

fn summarize(ledger: &workload::SharedLedger, report: &mut Report<'_>) -> Result<()> {
    let ledger = workload::lock(ledger)?;
    ledger.complete()?;
    if report.configuration.case == Case::Traffic && !report.configuration.profiled {
        let elapsed = report.elapsed_ns.context("Missing measurement duration")?;
        ensure!(elapsed > 0, "Measurement duration must be positive");
        report.acknowledged_messages_per_second =
            Some(ledger.entries.len() as f64 * 1_000_000_000.0 / elapsed as f64);
        let durations = |committed: bool| -> Result<Vec<u64>> {
            ledger
                .entries
                .iter()
                .map(|entry| {
                    let end = if committed {
                        entry.committed_ns
                    } else {
                        entry.received_ns
                    }
                    .context("Missing latency endpoint")?;
                    end.checked_sub(entry.started_ns.context("Missing latency start")?)
                        .context("Latency timestamp underflow")
                })
                .collect()
        };
        report.send_commit_latency = ledger::latency(durations(true)?)?;
        report.produce_to_receive_latency = ledger::latency(durations(false)?)?;
    }
    Ok(())
}

fn metrics(value: MetricsSnapshot) -> serde_json::Value {
    serde_json::json!({"requests":value.requests,"retries":value.retries,"unknown":value.unknown,
        "deliveries":value.deliveries,"acknowledged":value.acknowledged,"pending_sends":value.pending_sends,
        "active_deliveries":value.active_deliveries,"admitted_bytes":value.admitted_bytes,
        "request_duration_ns":value.request_duration_ns,"admission_wait_ns":value.admission_wait_ns})
}

async fn wait_marker(args: &Args, name: &str) -> Result<()> {
    loop {
        match tokio::fs::symlink_metadata(args.output_dir.join(name)).await {
            Ok(metadata) => {
                ensure!(
                    metadata.is_file() && metadata.len() == 0,
                    "Harness marker must be an empty regular file"
                );
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tokio::time::sleep(Duration::from_millis(20)).await
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn write(args: &Args, name: &str, bytes: Vec<u8>) -> Result<()> {
    ensure!(bytes.len() <= 128 * 1024 * 1024, "Artifact exceeds 128 MiB");
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(args.output_dir.join(name)).await?;
    file.write_all(&bytes).await?;
    file.sync_all().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Args {
        Args::try_parse_from([
            "bench",
            "--endpoint",
            "127.0.0.1:7410",
            "--ca",
            "ca.pem",
            "--token-file",
            "token",
            "--output-dir",
            "new-output",
        ])
        .unwrap()
    }

    #[test]
    fn cli_enforces_bounded_memory_concurrency_and_time() {
        let mut value = args();
        value.validate().unwrap();
        value.messages = 0;
        assert!(value.validate().is_err());
        value.messages = 200_001;
        assert!(value.validate().is_err());
        value.messages = 200_000;
        value.message_bytes = 65_536;
        assert!(value.validate().is_err());
        value.messages = 4;
        value.message_bytes = 1024;
        value.concurrency = 5;
        assert!(value.validate().is_err());
        value.concurrency = 4;
        value.timeout_seconds = 601;
        assert!(value.validate().is_err());
        value.timeout_seconds = 120;
        value.endpoint = "192.0.2.1:7410".parse().unwrap();
        assert!(value.validate().is_err());
    }

    #[test]
    fn configuration_never_serializes_credentials_or_private_paths() {
        let text = serde_json::to_string(&args()).unwrap();
        for hidden in ["token_file", "ca.pem", "new-output"] {
            assert!(!text.contains(hidden));
        }
    }

    #[tokio::test]
    async fn failed_setup_retains_a_failed_report_and_unsubmitted_identity_ledger() {
        let mut value = args();
        value.output_dir =
            std::env::temp_dir().join(format!("tidemq-perf-{}", Id::new_v7().unwrap()));
        value.ca = value.output_dir.join("missing-ca.pem");
        value.token_file = value.output_dir.join("missing.token");
        value.messages = 4;
        assert!(run(&value).await.is_err());
        let report: serde_json::Value = serde_json::from_slice(
            &tokio::fs::read(value.output_dir.join("report.json"))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(report.get("status").unwrap(), "failed");
        assert!(
            report
                .get("acknowledged_messages_per_second")
                .unwrap()
                .is_null()
        );
        let ledger: serde_json::Value = serde_json::from_slice(
            &tokio::fs::read(value.output_dir.join("ledger.json"))
                .await
                .unwrap(),
        )
        .unwrap();
        let entries = ledger.get("entries").unwrap().as_array().unwrap();
        assert_eq!(entries.len(), 4);
        assert!(
            entries
                .iter()
                .all(|entry| entry.get("started_ns").unwrap().is_null())
        );
        assert!(run(&value).await.is_err());
        tokio::fs::remove_dir_all(value.output_dir).await.unwrap();
    }
}
