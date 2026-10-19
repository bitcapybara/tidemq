use std::{io, path::PathBuf, sync::Arc};

use serde::Serialize;
use tokio::{sync::Semaphore, time::Instant};

use super::memory::{Error, observe};

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Snapshot {
    pub total_bytes: u64,
    /// Bytes available to the running user; filesystem-reserved blocks are excluded.
    pub available_bytes: u64,
    pub source: &'static str,
}

pub struct DiskSampler {
    directory: PathBuf,
    slot: Arc<Semaphore>,
}

impl DiskSampler {
    pub fn new(directory: PathBuf) -> Result<Self, Error> {
        if directory.as_os_str().is_empty()
            || directory.as_os_str().as_encoded_bytes().len() > 4_096
            || directory.as_os_str().as_encoded_bytes().contains(&0)
        {
            return Err(Error::Unavailable {
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "configured data directory is invalid",
                ),
            });
        }
        Ok(Self {
            directory,
            slot: Arc::new(Semaphore::new(1)),
        })
    }

    pub async fn sample(&self, deadline: Instant) -> Result<Snapshot, Error> {
        let path = self.directory.clone();
        observe(&self.slot, deadline, move || read_snapshot(&path)).await
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn read_snapshot(path: &std::path::Path) -> io::Result<Snapshot> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|source| io::Error::new(io::ErrorKind::InvalidInput, source))?;
    let mut result = std::mem::MaybeUninit::<libc::statvfs>::zeroed();
    // The NUL-terminated path and correctly sized writable structure follow the pinned libc ABI.
    if unsafe { libc::statvfs(path.as_ptr(), result.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // All fields started initialized and statvfs succeeded before any field is read.
    let result = unsafe { result.assume_init() };
    if result.f_frsize == 0 {
        return Err(io::Error::other("filesystem fragment size is zero"));
    }
    let bytes = |blocks| {
        u128::from(blocks)
            .checked_mul(u128::from(result.f_frsize))
            .and_then(|size| u64::try_from(size).ok())
            .ok_or_else(|| io::Error::other("filesystem capacity overflows bytes"))
    };
    let total_bytes = bytes(result.f_blocks)?;
    let available_bytes = bytes(result.f_bavail)?;
    if available_bytes > total_bytes {
        return Err(io::Error::other(
            "filesystem available capacity exceeds total capacity",
        ));
    }
    Ok(Snapshot {
        total_bytes,
        available_bytes,
        source: "configured_data_filesystem_statvfs",
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_snapshot(_path: &std::path::Path) -> io::Result<Snapshot> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "filesystem capacity sampling is unsupported on this target",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn real_filesystem_capacity_is_observed_without_inventing_missing_values() {
        let sampler = DiskSampler::new(std::env::temp_dir()).unwrap();
        let snapshot = sampler
            .sample(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(snapshot.total_bytes > 0);
        assert!(snapshot.available_bytes <= snapshot.total_bytes);
        assert!(DiskSampler::new(PathBuf::from("invalid\0path")).is_err());
        let missing = std::env::temp_dir().join(format!(
            "missing-{}",
            tidemq_protocol::Id::random().unwrap()
        ));
        assert!(
            DiskSampler::new(missing)
                .unwrap()
                .sample(Instant::now() + Duration::from_secs(1))
                .await
                .is_err()
        );
    }
}
