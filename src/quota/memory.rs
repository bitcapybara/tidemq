use std::{io, sync::Arc};

use serde::Serialize;
use snafu::Snafu;
use tokio::{sync::Semaphore, time::Instant};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("A resource observation is already running"))]
    Busy,
    #[snafu(display("Resource observation exceeded its deadline"))]
    Deadline { source: tokio::time::error::Elapsed },
    #[snafu(display("The resource observation worker stopped"))]
    Task { source: tokio::task::JoinError },
    #[snafu(display("Resource observation is unavailable"))]
    Unavailable { source: io::Error },
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Snapshot {
    pub process_rss_bytes: u64,
    pub cgroup_current_bytes: Option<u64>,
    pub cgroup_limit_bytes: Option<u64>,
    pub cgroup_limit: CgroupLimit,
    pub source: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum CgroupLimit {
    Unsupported,
    Unavailable,
    Unbounded,
    Bounded(u64),
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
        observe(&self.slot, deadline, read_snapshot).await
    }
}

pub(super) async fn observe<T: Send + 'static>(
    slot: &Arc<Semaphore>,
    deadline: Instant,
    read: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> Result<T, Error> {
    let permit = slot.clone().try_acquire_owned().map_err(|_| Error::Busy)?;
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        read().map_err(|source| Error::Unavailable { source })
    });
    tokio::time::timeout_at(deadline, task)
        .await
        .map_err(|source| Error::Deadline { source })?
        .map_err(|source| Error::Task { source })?
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
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "cgroup v2 membership is absent")
        })?;
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

#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_limit(text: &str) -> io::Result<CgroupLimit> {
    let value = text.trim();
    if value == "max" {
        return Ok(CgroupLimit::Unbounded);
    }
    value
        .parse::<u64>()
        .map(CgroupLimit::Bounded)
        .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))
}

#[cfg(any(target_os = "linux", test))]
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

#[cfg(any(target_os = "linux", test))]
fn read_cgroup_level(path: &std::path::Path, root: bool) -> io::Result<(Option<u64>, CgroupLimit)> {
    let limit = match read_limited(&path.join("memory.max"), 64) {
        Ok(text) => parse_cgroup_limit(&text)?,
        Err(error) if root && error.kind() == io::ErrorKind::NotFound => {
            return Ok((None, CgroupLimit::Unbounded));
        }
        Err(error) => return Err(error),
    };
    let current = read_limited(&path.join("memory.current"), 64)?
        .trim()
        .parse::<u64>()
        .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
    Ok((Some(current), limit))
}

#[cfg(any(target_os = "linux", test))]
fn read_cgroup_hierarchy(
    path: &std::path::Path,
    root: &std::path::Path,
) -> io::Result<(Option<u64>, CgroupLimit)> {
    let mut path = path;
    let mut leaf_current = None;
    let mut pressured: Option<(u64, u64)> = None;
    for level in 0..64 {
        if !path.starts_with(root) {
            return Err(io::Error::other("cgroup observation escaped its mount"));
        }
        let (current, limit) = read_cgroup_level(path, path == root)?;
        if level == 0 {
            leaf_current = current;
        }
        if let (Some(current), CgroupLimit::Bounded(limit)) = (current, limit)
            && pressured.is_none_or(|(previous, maximum)| {
                limit == 0
                    || (maximum != 0
                        && u128::from(current) * u128::from(maximum)
                            > u128::from(previous) * u128::from(limit))
            })
        {
            pressured = Some((current, limit));
        }
        if path == root {
            return Ok(pressured.map_or(
                (leaf_current, CgroupLimit::Unbounded),
                |(current, limit)| (Some(current), CgroupLimit::Bounded(limit)),
            ));
        }
        path = path
            .parent()
            .ok_or_else(|| io::Error::other("cgroup mount root is unreachable"))?;
    }
    Err(io::Error::other(
        "cgroup observation exceeds 64 visible levels",
    ))
}

#[cfg(target_os = "linux")]
fn read_snapshot() -> io::Result<Snapshot> {
    use std::path::Path;
    let process_rss_bytes = parse_rss(&read_limited(Path::new("/proc/self/status"), 32_768)?)?;
    let cgroup =
        read_limited(Path::new("/proc/self/cgroup"), 16_384).and_then(|text| cgroup_path(&text));
    let (cgroup_current_bytes, cgroup_limit) = match cgroup {
        Ok(path) => read_cgroup_hierarchy(&path, Path::new("/sys/fs/cgroup"))
            .unwrap_or((None, CgroupLimit::Unavailable)),
        Err(error) if error.kind() == io::ErrorKind::Unsupported => {
            (None, CgroupLimit::Unsupported)
        }
        Err(_) => (None, CgroupLimit::Unavailable),
    };
    let cgroup_limit_bytes = match cgroup_limit {
        CgroupLimit::Bounded(bytes) => Some(bytes),
        _ => None,
    };
    Ok(Snapshot {
        process_rss_bytes,
        cgroup_current_bytes,
        cgroup_limit_bytes,
        cgroup_limit,
        source: "linux_proc_status_cgroup_v2_most_pressured_visible_ancestor",
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
        cgroup_limit: CgroupLimit::Unsupported,
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
        assert_eq!(parse_cgroup_limit("max\n").unwrap(), CgroupLimit::Unbounded);
        assert_eq!(
            parse_cgroup_limit("1024\n").unwrap(),
            CgroupLimit::Bounded(1024)
        );
        for invalid in ["", "-1", "max extra", "18446744073709551616"] {
            assert!(parse_cgroup_limit(invalid).is_err());
        }
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

    #[test]
    fn visible_ancestor_pressure_includes_siblings_and_never_becomes_a_leaf_only_limit() {
        let root = std::env::temp_dir().join(format!(
            "tidemq-cgroup-{}",
            tidemq_protocol::Id::new_v7().unwrap()
        ));
        let parent = root.join("parent");
        let leaf = parent.join("service");
        std::fs::create_dir_all(&leaf).unwrap();
        for (path, current, limit) in [(&leaf, "10", "100"), (&parent, "95", "100")] {
            std::fs::write(path.join("memory.current"), current).unwrap();
            std::fs::write(path.join("memory.max"), limit).unwrap();
        }
        assert_eq!(
            read_cgroup_hierarchy(&leaf, &root).unwrap(),
            (Some(95), CgroupLimit::Bounded(100))
        );
        std::fs::write(leaf.join("memory.max"), "max").unwrap();
        assert_eq!(
            read_cgroup_hierarchy(&leaf, &root).unwrap(),
            (Some(95), CgroupLimit::Bounded(100))
        );
        std::fs::write(parent.join("memory.max"), "max").unwrap();
        assert_eq!(
            read_cgroup_hierarchy(&leaf, &root).unwrap(),
            (Some(10), CgroupLimit::Unbounded)
        );
        std::fs::write(parent.join("memory.max"), "invalid").unwrap();
        assert!(read_cgroup_hierarchy(&leaf, &root).is_err());
        assert!(read_cgroup_hierarchy(&leaf, &root.join("unrelated")).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
