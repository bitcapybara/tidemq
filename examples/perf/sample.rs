use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, timeout},
};

use super::Args;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BuildIdentity {
    pub source_commit: String,
    pub cargo_lock_sha256: String,
    pub service_binary_sha256: String,
    pub allocator_feature: String,
    pub rustc: String,
    pub host: String,
    pub profile: String,
    pub tracked_source_clean: bool,
}

#[derive(Serialize)]
pub struct Identity {
    pub build: BuildIdentity,
    pub service_pid: u32,
    pub verified_process_executable: PathBuf,
    pub service_binary_blake3: String,
    pub service_config_blake3: String,
    pub generator_binary_blake3: String,
    pub initial_diagnostics: Value,
}

#[derive(Serialize)]
pub struct ProcessSample {
    pub cumulative_cpu_ns: u64,
    pub rss_bytes: u64,
    pub source: &'static str,
}

#[derive(Serialize)]
pub struct ResourceSample {
    pub elapsed_ms: u64,
    pub process: Option<ProcessSample>,
    pub runtime_process: Value,
    pub allocator: Value,
    pub capacity: Value,
    pub ready: Option<bool>,
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct ResourceSummary {
    pub samples: usize,
    pub successful_os_samples: usize,
    pub sampled_rss_max_bytes: Option<u64>,
    pub cumulative_cpu_delta_ns: Option<u64>,
    pub observed_cpu_interval_ms: Option<u64>,
    pub sampling_errors: usize,
}

pub fn summarize(samples: &[ResourceSample]) -> ResourceSummary {
    let first = samples.iter().find(|sample| sample.process.is_some());
    let last = samples.iter().rfind(|sample| sample.process.is_some());
    ResourceSummary {
        samples: samples.len(),
        successful_os_samples: samples
            .iter()
            .filter(|sample| sample.process.is_some())
            .count(),
        sampled_rss_max_bytes: samples
            .iter()
            .filter_map(|sample| sample.process.as_ref().map(|process| process.rss_bytes))
            .max(),
        cumulative_cpu_delta_ns: first.zip(last).and_then(|(first, last)| {
            last.process
                .as_ref()?
                .cumulative_cpu_ns
                .checked_sub(first.process.as_ref()?.cumulative_cpu_ns)
        }),
        observed_cpu_interval_ms: first
            .zip(last)
            .and_then(|(first, last)| last.elapsed_ms.checked_sub(first.elapsed_ms)),
        sampling_errors: samples
            .iter()
            .filter(|sample| sample.error.is_some())
            .count(),
    }
}

impl Identity {
    pub async fn capture(args: &Args) -> Result<Self> {
        let build: BuildIdentity =
            serde_json::from_slice(&read_bounded(&args.build_identity, 16_384).await?)?;
        ensure!(
            build.profile == "release" && build.tracked_source_clean,
            "Performance evidence requires a clean release build identity"
        );
        ensure!(
            matches!(
                build.allocator_feature.as_str(),
                "allocator-system" | "allocator-mimalloc" | "allocator-jemalloc"
            ),
            "Unknown service allocator feature"
        );
        ensure!(
            build.source_commit.len() == 40
                && build.cargo_lock_sha256.len() == 64
                && build.service_binary_sha256.len() == 64,
            "Build identity hashes are invalid"
        );
        let binary = tokio::fs::canonicalize(&args.service_binary).await?;
        let executable = executable(args.service_pid).await?;
        ensure!(
            binary == executable,
            "Service PID does not execute the selected binary"
        );
        let diagnostics = diagnostics(args.management).await?;
        ensure!(
            diagnostics.get("ready").and_then(Value::as_bool) == Some(true),
            "Service must be ready before load setup"
        );
        ensure!(
            diagnostics
                .get("allocator")
                .is_some_and(|value| !value.is_null()),
            "Actual service allocator diagnostics are required"
        );
        ensure!(
            diagnostics
                .pointer("/allocator/backend")
                .and_then(Value::as_str)
                == build.allocator_feature.strip_prefix("allocator-"),
            "Actual service allocator differs from the selected build identity"
        );
        Ok(Self {
            build,
            service_pid: args.service_pid,
            verified_process_executable: executable,
            service_binary_blake3: hash_file(&binary).await?,
            service_config_blake3: blake3::hash(
                &read_bounded(&args.service_config, 1_048_576).await?,
            )
            .to_hex()
            .to_string(),
            generator_binary_blake3: hash_file(&std::env::current_exe()?).await?,
            initial_diagnostics: diagnostics,
        })
    }
}

pub async fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("Open bounded input {}", path.display()))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(limit + 1)?;
    file.take(u64::try_from(limit + 1)?)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(
        bytes.len() <= limit,
        "Input file exceeded its finite byte limit"
    );
    Ok(bytes)
}

async fn hash_file(path: &Path) -> Result<String> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut digest = blake3::Hasher::new();
    let mut block = [0_u8; 65_536];
    let mut total = 0_u64;
    loop {
        let read = file.read(&mut block).await?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read)?)
            .context("Binary byte count overflow")?;
        ensure!(
            total <= 256 * 1024 * 1024,
            "Binary exceeded the 256 MiB identity bound"
        );
        digest.update(block.get(..read).context("Invalid binary read size")?);
    }
    Ok(digest.finalize().to_hex().to_string())
}

pub async fn diagnostics(address: SocketAddr) -> Result<Value> {
    serde_json::from_str(&http(address, "/diagnostics").await?)
        .context("Decode runtime diagnostics")
}

pub async fn cold_misses(address: SocketAddr) -> Result<Option<u64>> {
    let metrics = http(address, "/metrics").await?;
    ensure!(
        metrics.lines().filter(|line| *line == "# EOF").count() == 1,
        "Metrics require exactly one OpenMetrics EOF"
    );
    metrics
        .lines()
        .find_map(|line| line.strip_prefix("tidemq_s3_cache_misses_total "))
        .map(|value| value.parse().context("Decode cold cache miss counter"))
        .transpose()
}

async fn http(address: SocketAddr, path: &str) -> Result<String> {
    ensure!(
        address.ip().is_loopback(),
        "Management sampling requires loopback"
    );
    timeout(Duration::from_secs(2), async {
        let mut stream = TcpStream::connect(address).await?;
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(132_097)?;
        stream.take(132_097).read_to_end(&mut bytes).await?;
        ensure!(
            bytes.len() <= 132_096,
            "Management response exceeded its finite bound"
        );
        let text = String::from_utf8(bytes)?;
        let (header, body) = text
            .split_once("\r\n\r\n")
            .context("Incomplete management response")?;
        ensure!(
            header.starts_with("HTTP/1.1 200 "),
            "Management response was not successful"
        );
        Ok::<_, anyhow::Error>(body.to_owned())
    })
    .await
    .context("Management sampling deadline exceeded")?
}

pub async fn resource(args: &Args, started: Instant) -> ResourceSample {
    let (process, diagnostic) =
        tokio::join!(process(args.service_pid), diagnostics(args.management));
    let mut errors = Vec::new();
    let process = match process {
        Ok(value) => Some(value),
        Err(error) => {
            errors.push(format!("Process sample unavailable: {error:#}"));
            None
        }
    };
    let diagnostic = match diagnostic {
        Ok(value) => value,
        Err(error) => {
            errors.push(format!("Runtime sample unavailable: {error:#}"));
            Value::Null
        }
    };
    ResourceSample {
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        process,
        runtime_process: diagnostic.get("process").cloned().unwrap_or(Value::Null),
        allocator: bounded_allocator(diagnostic.get("allocator")),
        capacity: diagnostic.get("capacity").cloned().unwrap_or(Value::Null),
        ready: diagnostic.get("ready").and_then(Value::as_bool),
        error: (!errors.is_empty()).then(|| errors.join("; ")),
    }
}

fn bounded_allocator(value: Option<&Value>) -> Value {
    let Some(value) = value else {
        return Value::Null;
    };
    if serde_json::to_vec(value).is_ok_and(|bytes| bytes.len() <= 8_192) {
        return value.clone();
    }
    serde_json::json!({"backend":value.get("backend"),"native_version":value.get("native_version"),
        "stats":null,"stats_omitted":"Native statistics exceeded the 8192-byte per-sample budget; the full initial diagnostic remains in identity"})
}

async fn ps(pid: u32, fields: &[&str]) -> Result<String> {
    let path = if tokio::fs::metadata("/bin/ps").await.is_ok() {
        "/bin/ps"
    } else {
        "/usr/bin/ps"
    };
    let mut command = tokio::process::Command::new(path);
    command
        .arg("-p")
        .arg(pid.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    for field in fields {
        command.arg("-o").arg(field);
    }
    timeout(Duration::from_secs(1), async {
        let mut child = command.spawn().context("Start bounded process sampler")?;
        let stdout = child
            .stdout
            .take()
            .context("Process sampler stdout is unavailable")?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(4_097)?;
        stdout.take(4_097).read_to_end(&mut bytes).await?;
        ensure!(
            bytes.len() <= 4_096,
            "Process sampler exceeded its output bound"
        );
        ensure!(
            child.wait().await?.success(),
            "Service process is unavailable"
        );
        String::from_utf8(bytes).context("Process sampler output is not UTF-8")
    })
    .await
    .context("Process sampler deadline exceeded")?
}

async fn executable(pid: u32) -> Result<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        Ok(tokio::fs::canonicalize(format!("/proc/{pid}/exe")).await?)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(tokio::fs::canonicalize(ps(pid, &["comm="]).await?.trim()).await?)
    }
}

async fn process(pid: u32) -> Result<ProcessSample> {
    let text = ps(pid, &["time=", "rss="]).await?;
    let mut fields = text.split_whitespace();
    let time = fields
        .next()
        .context("Missing cumulative process CPU time")?;
    let rss = fields
        .next()
        .context("Missing process RSS")?
        .parse::<u64>()?;
    ensure!(fields.next().is_none(), "Unexpected process sampler fields");
    Ok(ProcessSample {
        cumulative_cpu_ns: cpu_time(time)?,
        rss_bytes: rss.checked_mul(1_024).context("RSS byte count overflow")?,
        source: "OS ps: process cumulative CPU, RSS in KiB converted to bytes; CPU resolution is platform dependent",
    })
}

fn cpu_time(value: &str) -> Result<u64> {
    let (days, clock) = match value.split_once('-') {
        Some((days, clock)) => (days.parse::<u64>()?, clock),
        None => (0, value),
    };
    let mut parts = clock.rsplit(':');
    let seconds = parts
        .next()
        .context("Missing CPU seconds")?
        .parse::<f64>()?;
    let minutes = parts
        .next()
        .context("Missing CPU minutes")?
        .parse::<u64>()?;
    let hours = parts
        .next()
        .map(str::parse::<u64>)
        .transpose()?
        .unwrap_or(0);
    ensure!(
        parts.next().is_none() && seconds.is_finite() && (0.0..60.0).contains(&seconds),
        "Invalid CPU time fields"
    );
    let whole = days
        .checked_mul(86_400)
        .and_then(|value| {
            hours
                .checked_mul(3_600)
                .and_then(|hours| value.checked_add(hours))
        })
        .and_then(|value| {
            minutes
                .checked_mul(60)
                .and_then(|minutes| value.checked_add(minutes))
        })
        .context("CPU time overflow")?;
    let duration = Duration::try_from_secs_f64(whole as f64 + seconds)?;
    Ok(u64::try_from(duration.as_nanos())?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cpu_time_parses_linux_and_macos_formats_without_invented_zeroes() {
        assert_eq!(cpu_time("01:02.50").unwrap(), 62_500_000_000);
        assert_eq!(cpu_time("2-03:04:05").unwrap(), 183_845_000_000_000);
        assert!(cpu_time("1:NaN").is_err());
        assert!(cpu_time("1:99").is_err());
        assert!(cpu_time("unknown").is_err());
    }
}
