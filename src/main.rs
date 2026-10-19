use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use serde::Serialize;
use tidemq::config::Config;
use tidemq::{Error, Result, allocator};

#[cfg(any(
    all(feature = "allocator-system", feature = "allocator-mimalloc"),
    all(feature = "allocator-system", feature = "allocator-jemalloc"),
    all(feature = "allocator-mimalloc", feature = "allocator-jemalloc")
))]
compile_error!(
    "Allocator features are mutually exclusive; use --no-default-features when selecting an alternative."
);

#[cfg(feature = "allocator-mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[cfg(all(feature = "allocator-jemalloc", not(feature = "allocator-mimalloc")))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
#[cfg(not(any(feature = "allocator-mimalloc", feature = "allocator-jemalloc")))]
#[global_allocator]
static ALLOCATOR: std::alloc::System = std::alloc::System;

#[derive(Parser)]
#[command(version, about = "TideMQ service and bounded diagnostics")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start authenticated listeners and durable queue authorities.
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    /// Validate configuration without opening listeners or writing data.
    ValidateConfig {
        #[arg(long)]
        config: PathBuf,
    },
    /// Report this process's configuration and allocator, not a remote running node.
    Diagnose {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Export the foundation metric schema with fresh-process counters.
    Metrics,
    /// Compare allocator snapshots around a bounded live-allocation sample.
    AllocatorProbe {
        #[arg(long, default_value_t = 1_048_576)]
        block_bytes: u32,
        #[arg(long, default_value_t = 8)]
        blocks: u32,
    },
    /// Capture known live and released allocation stacks using startup-enabled jemalloc profiling.
    #[cfg(all(feature = "heap-profiling", unix))]
    AllocatorProfileProbe {
        /// Existing private directory with mode 0700; artifacts contain addresses, never message bodies.
        #[arg(long)]
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let error = anyhow::Error::new(error);
            let _ = writeln!(io::stderr().lock(), "TideMQ failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Some(Command::Serve { config }) => {
            let config = Config::load(&config).await?;
            let logging = tidemq::observability::logging::initialize(&config.logging)?;
            let node_id = config.node_id;
            let role = config.role.name();
            let node = tidemq::runtime::RunningNode::start(config)
                .await
                .map_err(runtime_error)?;
            tracing::info!(event_code = "service_started", node_id, role, internal_address = %node.internal_address, management_address = %node.management_address, external_address = ?node.external_address, "Authenticated TideMQ runtime is accepting connections");
            shutdown_signal().await?;
            node.shutdown().await.map_err(runtime_error)?;
            tracing::info!(
                event_code = "service_stopped",
                node_id,
                role,
                "TideMQ runtime drained and closed its durable storage"
            );
            drop(logging);
            Ok(())
        }
        Some(Command::ValidateConfig { config }) => {
            Config::load(&config).await?;
            print_json(&serde_json::json!({"valid": true}))
        }
        Some(Command::Diagnose { config }) => {
            let config = if let Some(path) = config {
                Config::load(&path).await?
            } else {
                Config::default()
            };
            config.validate()?;
            print_json(&serde_json::json!({"version": env!("CARGO_PKG_VERSION"),
                "scope": "current_process", "configuration": config,
                "allocator": allocator::snapshot().await?, "messaging_runtime_available": true}))
        }
        Some(Command::Metrics) => {
            let text = tidemq::observability::metrics::Metrics::default().encode()?;
            io::stdout()
                .lock()
                .write_all(text.as_bytes())
                .map_err(|source| Error::Io {
                    operation: "write metrics",
                    path: PathBuf::from("stdout"),
                    source,
                })
        }
        Some(Command::AllocatorProbe {
            block_bytes,
            blocks,
        }) => print_json(&allocator::probe(block_bytes, blocks).await?),
        #[cfg(all(feature = "heap-profiling", unix))]
        Some(Command::AllocatorProfileProbe { output }) => {
            print_json(&allocator::profile::probe(output).await?)
        }
        None => {
            let logging = tidemq::observability::logging::initialize(&Config::default().logging)?;
            tracing::info!(
                event_code = "service_status",
                version = env!("CARGO_PKG_VERSION"),
                allocator = allocator::backend(),
                messaging_runtime_available = false,
                "TideMQ foundation is available; the messaging runtime is not implemented yet"
            );
            drop(logging);
            Ok(())
        }
    }
}

fn runtime_error(source: tidemq::runtime::Error) -> Error {
    Error::Io {
        operation: "run queue service",
        path: PathBuf::from("runtime"),
        source: io::Error::other(source),
    }
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        )
        .map_err(|source| Error::Io {
            operation: "install termination signal handler",
            path: PathBuf::from("signals"),
            source,
        })?;
        tokio::select! { result = tokio::signal::ctrl_c() => result.map_err(|source| Error::Io { operation: "wait for interrupt signal", path: PathBuf::from("signals"), source }), _ = terminate.recv() => Ok(()) }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.map_err(|source| Error::Io {
        operation: "wait for interrupt signal",
        path: PathBuf::from("signals"),
        source,
    })
}

fn print_json(value: &impl Serialize) -> Result<()> {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer_pretty(&mut output, value).map_err(|source| Error::Json { source })?;
    writeln!(output).map_err(|source| Error::Io {
        operation: "write diagnostics",
        path: PathBuf::from("stdout"),
        source,
    })
}
