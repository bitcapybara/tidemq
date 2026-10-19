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
                "allocator": allocator::snapshot().await?, "messaging_runtime_available": false}))
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
