use std::io;

pub(super) struct Cpu {
    pub nanoseconds: u128,
    pub millicores: u64,
    pub source: &'static str,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn read_cpu() -> io::Result<Cpu> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // The initialized structure and RUSAGE_SELF selector use the pinned libc ABI.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // getrusage succeeded before fields are read from the initialized structure.
    let usage = unsafe { usage.assume_init() };
    let user = nanos(usage.ru_utime.tv_sec, usage.ru_utime.tv_usec)?;
    let system = nanos(usage.ru_stime.tv_sec, usage.ru_stime.tv_usec)?;
    let nanoseconds = user
        .checked_add(system)
        .ok_or_else(|| io::Error::other("process CPU time overflows nanoseconds"))?;
    let available = std::thread::available_parallelism()?.get();
    let millicores = u64::try_from(available)
        .ok()
        .and_then(|count| count.checked_mul(1000))
        .ok_or_else(|| io::Error::other("available CPU capacity overflows millicores"))?;
    #[cfg(target_os = "linux")]
    let millicores = quota_capacity(millicores)?;
    Ok(Cpu {
        nanoseconds,
        millicores,
        #[cfg(target_os = "linux")]
        source: "getrusage_cgroup_v2_cpu_and_transport_bytes",
        #[cfg(target_os = "macos")]
        source: "getrusage_host_cpu_and_transport_bytes",
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(super) fn read_cpu() -> io::Result<Cpu> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process CPU sampling is unsupported on this target",
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos", test))]
fn nanos(seconds: libc::time_t, micros: libc::suseconds_t) -> io::Result<u128> {
    if !(0..1_000_000).contains(&micros) {
        return Err(io::Error::other("process CPU microseconds are invalid"));
    }
    let seconds = u128::try_from(seconds)
        .map_err(|_| io::Error::other("process CPU seconds are negative"))?;
    let micros = u128::try_from(micros)
        .map_err(|_| io::Error::other("process CPU microseconds are negative"))?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|seconds| {
            micros
                .checked_mul(1000)
                .and_then(|micros| seconds.checked_add(micros))
        })
        .ok_or_else(|| io::Error::other("process CPU time overflows nanoseconds"))
}

#[cfg(any(target_os = "linux", test))]
fn cgroup_path(text: &str) -> io::Result<std::path::PathBuf> {
    use std::path::{Component, Path, PathBuf};
    let path = text
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .filter(|path| path.len() <= 4096 && path.starts_with('/'))
        .ok_or_else(|| io::Error::other("cgroup v2 CPU membership is absent or invalid"))?;
    let mut result = PathBuf::from("/sys/fs/cgroup");
    let mut depth = 0_u8;
    for component in Path::new(path).components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) if depth < 32 => {
                result.push(name);
                depth += 1;
            }
            _ => {
                return Err(io::Error::other(
                    "cgroup CPU path traverses its bounded mount",
                ));
            }
        }
    }
    Ok(result)
}

#[cfg(any(target_os = "linux", test))]
fn parse_quota(text: &str) -> io::Result<Option<u64>> {
    let mut fields = text.split_ascii_whitespace();
    let quota = fields.next();
    let period = fields
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|period| *period > 0)
        .ok_or_else(|| io::Error::other("cgroup CPU period is invalid"))?;
    if fields.next().is_some() {
        return Err(io::Error::other("cgroup CPU quota has extra fields"));
    }
    if quota == Some("max") {
        return Ok(None);
    }
    let quota = quota
        .and_then(|quota| quota.parse::<u64>().ok())
        .and_then(|quota| quota.checked_mul(1000))
        .and_then(|quota| quota.checked_div(period))
        .filter(|quota| *quota > 0)
        .ok_or_else(|| io::Error::other("cgroup CPU quota is invalid or too small"))?;
    Ok(Some(quota))
}

#[cfg(any(target_os = "linux", test))]
fn read_limited(path: &std::path::Path, limit: u64) -> io::Result<String> {
    use std::io::Read;
    let mut text = String::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_string(&mut text)?;
    if text.len() as u64 > limit {
        return Err(io::Error::other("CPU observation exceeds its byte limit"));
    }
    Ok(text)
}

#[cfg(any(target_os = "linux", test))]
fn read_quota(path: &std::path::Path, root: &std::path::Path) -> io::Result<Option<u64>> {
    match read_limited(&path.join("cpu.max"), 256) {
        Ok(text) => parse_quota(&text),
        // The host root has no bandwidth controller; a namespaced root may still have a quota.
        Err(error) if path == root && error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "linux")]
fn quota_capacity(mut available: u64) -> io::Result<u64> {
    use std::path::Path;
    let root = Path::new("/sys/fs/cgroup");
    let membership = read_limited(Path::new("/proc/self/cgroup"), 16_384)?;
    let mut path = cgroup_path(&membership)?;
    loop {
        if let Some(quota) = read_quota(&path, root)? {
            available = available.min(quota);
        }
        if path == root {
            return Ok(available);
        }
        path = path
            .parent()
            .filter(|parent| parent.starts_with(root))
            .ok_or_else(|| io::Error::other("cgroup CPU hierarchy escaped its mount"))?
            .to_path_buf();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_cpu_quota_is_optional_but_present_limits_and_child_errors_are_preserved() {
        let root = std::env::temp_dir().join(format!(
            "tidemq-cpu-quota-{}",
            tidemq_protocol::Id::new_v7().unwrap()
        ));
        std::fs::create_dir(&root).unwrap();
        let child = root.join("child");
        std::fs::create_dir(&child).unwrap();
        assert_eq!(read_quota(&root, &root).unwrap(), None);
        assert_eq!(
            read_quota(&child, &root).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        std::fs::write(root.join("cpu.max"), "50000 100000\n").unwrap();
        assert_eq!(read_quota(&root, &root).unwrap(), Some(500));
        std::fs::write(root.join("cpu.max"), "invalid\n").unwrap();
        assert!(read_quota(&root, &root).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_cpu_inputs_reject_invalid_time_and_container_quotas() {
        assert_eq!(nanos(1, 500_000).unwrap(), 1_500_000_000);
        assert!(nanos(-1, 0).is_err());
        assert!(nanos(0, 1_000_000).is_err());
        assert_eq!(parse_quota("50000 100000\n").unwrap(), Some(500));
        assert_eq!(parse_quota("max 100000").unwrap(), None);
        for invalid in ["", "0 100000", "1 0", "100 100 extra", "max 0", "-1 1"] {
            assert!(parse_quota(invalid).is_err());
        }
        assert_eq!(
            cgroup_path("0::/tenant/workload").unwrap(),
            std::path::PathBuf::from("/sys/fs/cgroup/tenant/workload")
        );
        assert!(cgroup_path("0::/tenant/../other").is_err());
        assert!(cgroup_path("0::relative").is_err());
        assert!(cgroup_path("1:cpu:/legacy").is_err());
    }
}
