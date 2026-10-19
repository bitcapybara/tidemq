use std::{io, sync::Arc};

use serde::Serialize;
use snafu::Snafu;
use tokio::{sync::Semaphore, time::Instant};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("A process memory sample is already running"))]
    Busy,
    #[snafu(display("Process memory sampling exceeded its deadline"))]
    Deadline { source: tokio::time::error::Elapsed },
    #[snafu(display("The process memory sampler stopped"))]
    Task { source: tokio::task::JoinError },
    #[snafu(display("Process memory is unavailable"))]
    Unavailable { source: io::Error },
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Snapshot {
    pub process_rss_bytes: u64,
    pub cgroup_current_bytes: Option<u64>,
    pub cgroup_limit_bytes: Option<u64>,
    pub source: &'static str,
}

pub struct MemorySampler {
    slot: Arc<Semaphore>,
}

impl Default for MemorySampler {
    fn default() -> Self {
        Self {
            slot: Arc::new(Semaphore::new(1)),
        }
    }
}

impl MemorySampler {
    /// A timed-out native read retains its single worker slot until the read actually ends.
    pub async fn sample(&self, deadline: Instant) -> Result<Snapshot, Error> {
        let permit = self
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            read_snapshot().map_err(|source| Error::Unavailable { source })
        });
        tokio::time::timeout_at(deadline, task)
            .await
            .map_err(|source| Error::Deadline { source })?
            .map_err(|source| Error::Task { source })?
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_rss(status: &str) -> io::Result<u64> {
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .ok_or_else(|| io::Error::other("process status has no RSS field"))?;
    let mut fields = value.split_ascii_whitespace();
    let kib = fields
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| io::Error::other("process RSS is not an unsigned integer"))?;
    if fields.next() != Some("kB") || fields.next().is_some() {
        return Err(io::Error::other("process RSS has an unknown unit"));
    }
    kib.checked_mul(1_024)
        .ok_or_else(|| io::Error::other("process RSS overflows bytes"))
}

#[cfg(any(target_os = "linux", test))]
fn cgroup_path(text: &str) -> io::Result<std::path::PathBuf> {
    use std::path::{Component, Path};
    let path = text
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or_else(|| io::Error::other("cgroup v2 membership is absent"))?;
    if path.len() > 4_096 || !path.starts_with('/') {
        return Err(io::Error::other("cgroup v2 path is invalid"));
    }
    let mut result = std::path::PathBuf::from("/sys/fs/cgroup");
    for component in Path::new(path).components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => result.push(name),
            _ => return Err(io::Error::other("cgroup v2 path traverses its mount")),
        }
    }
    Ok(result)
}

#[cfg(target_os = "linux")]
fn read_limited(path: &std::path::Path, limit: u64) -> io::Result<String> {
    use std::io::Read;
    let mut text = String::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_string(&mut text)?;
    if text.len() as u64 > limit {
        return Err(io::Error::other(
            "memory observation exceeds its byte limit",
        ));
    }
    Ok(text)
}

#[cfg(target_os = "linux")]
fn read_snapshot() -> io::Result<Snapshot> {
    use std::path::Path;
    let process_rss_bytes = parse_rss(&read_limited(Path::new("/proc/self/status"), 32_768)?)?;
    let cgroup = read_limited(Path::new("/proc/self/cgroup"), 16_384)
        .and_then(|text| cgroup_path(&text))
        .ok();
    let (cgroup_current_bytes, cgroup_limit_bytes) = cgroup.map_or((None, None), |path| {
        let current = read_limited(&path.join("memory.current"), 64)
            .ok()
            .and_then(|value| value.trim().parse().ok());
        let limit = read_limited(&path.join("memory.max"), 64)
            .ok()
            .and_then(|value| value.trim().parse().ok());
        (current, limit)
    });
    Ok(Snapshot {
        process_rss_bytes,
        cgroup_current_bytes,
        cgroup_limit_bytes,
        source: "linux_proc_status_cgroup_v2",
    })
}

#[cfg(target_os = "macos")]
fn read_snapshot() -> io::Result<Snapshot> {
    let mut info = libc::mach_task_basic_info {
        virtual_size: 0,
        resident_size: 0,
        resident_size_max: 0,
        user_time: libc::time_value_t {
            seconds: 0,
            microseconds: 0,
        },
        system_time: libc::time_value_t {
            seconds: 0,
            microseconds: 0,
        },
        policy: 0,
        suspend_count: 0,
    };
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
    // The OS initializes this task port; the pinned libc binding avoids another Mach wrapper crate.
    #[allow(deprecated)]
    let task = unsafe { libc::mach_task_self() };
    // The flavor, initialized structure and word count use the same libc ABI definition.
    let result = unsafe {
        libc::task_info(
            task,
            libc::MACH_TASK_BASIC_INFO,
            std::ptr::from_mut(&mut info).cast(),
            &mut count,
        )
    };
    if result != libc::KERN_SUCCESS || count != libc::MACH_TASK_BASIC_INFO_COUNT {
        return Err(io::Error::other(format!(
            "Mach memory observation failed with status {result}"
        )));
    }
    Ok(Snapshot {
        process_rss_bytes: info.resident_size,
        cgroup_current_bytes: None,
        cgroup_limit_bytes: None,
        source: "macos_mach_task_basic_info",
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_snapshot() -> io::Result<Snapshot> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process memory sampling is unsupported on this target",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn proc_fields_require_exact_units_and_cgroup_paths_stay_in_the_mount() {
        assert_eq!(
            parse_rss("Name:\ttidemq\nVmRSS:\t1234 kB\n").unwrap(),
            1_263_616
        );
        for invalid in [
            "VmRSS: -1 kB",
            "VmRSS: 3 MB",
            "VmRSS: 18446744073709551615 kB",
            "VmRSS: 2 kB extra",
            "VmSize: 4 kB",
        ] {
            assert!(parse_rss(invalid).is_err());
        }
        assert_eq!(
            cgroup_path("0::/service/tidemq\n").unwrap().to_str(),
            Some("/sys/fs/cgroup/service/tidemq")
        );
        assert!(cgroup_path("0::/../../outside").is_err());
        assert!(cgroup_path("0::relative").is_err());
        assert!(cgroup_path("1:memory:/legacy").is_err());
    }

    #[tokio::test]
    async fn the_real_sampler_reports_rss_and_preserves_one_owned_worker() {
        let sampler = MemorySampler::default();
        let permit = sampler.slot.clone().acquire_owned().await.unwrap();
        assert!(matches!(
            sampler.sample(Instant::now()).await,
            Err(Error::Busy)
        ));
        drop(permit);
        let snapshot = sampler
            .sample(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(snapshot.process_rss_bytes > 0);
        assert!(!snapshot.source.is_empty());
    }
}
