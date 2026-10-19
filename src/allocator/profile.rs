//! Bounded native jemalloc sampling; the binary must own the matching allocator.

use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::Serialize;
use tokio::sync::Semaphore;

use crate::{Error, Result};

static OPERATION: Semaphore = Semaphore::const_new(1);

#[derive(Debug, Serialize)]
pub struct Settings {
    pub compiled: bool,
    pub enabled_at_startup: bool,
    pub active: bool,
    pub log2_sample_bytes: usize,
    pub unwinder: &'static str,
}

pub fn settings() -> Result<Settings> {
    // SAFETY: These fixed keys have bool/bool/bool/size_t values in the pinned jemalloc.
    unsafe {
        Ok(Settings {
            compiled: tikv_jemalloc_ctl::raw::read(b"config.prof\0").map_err(native_error)?,
            enabled_at_startup: tikv_jemalloc_ctl::raw::read(b"opt.prof\0")
                .map_err(native_error)?,
            active: tikv_jemalloc_ctl::raw::read(b"prof.active\0").map_err(native_error)?,
            log2_sample_bytes: tikv_jemalloc_ctl::raw::read(b"opt.lg_prof_sample\0")
                .map_err(native_error)?,
            unwinder: "libunwind",
        })
    }
}

#[derive(Debug, Serialize)]
pub struct Artifact {
    pub path: PathBuf,
    pub bytes: u64,
    pub allocator: &'static str,
    pub sampling_window_ms: u64,
}

/// A timed-out caller leaves the native operation running under its exclusive permit.
pub async fn sample(
    directory: PathBuf,
    max_bytes: u64,
    window: Duration,
    deadline: Duration,
) -> Result<Artifact> {
    if !(1_024..=8_388_608).contains(&max_bytes)
        || window.is_zero()
        || window > Duration::from_secs(30)
        || deadline <= window
        || deadline > Duration::from_secs(60)
    {
        return Err(failure("invalid heap sampling budget"));
    }
    let permit = OPERATION
        .try_acquire()
        .map_err(|_| failure("another heap sampling or native dump operation is still running"))?;
    let task = tokio::spawn(async move {
        let _permit = permit;
        let checked_directory = directory.clone();
        let guard = tokio::task::spawn_blocking(move || {
            validate_directory(&checked_directory, max_bytes)?;
            ActiveGuard::start()
        })
        .await
        .map_err(|source| Error::Task { source })??;
        tokio::time::sleep(window).await;
        tokio::task::spawn_blocking(move || {
            let mut guard = guard;
            guard.stop()?;
            dump(&directory, max_bytes, window)
        })
        .await
        .map_err(|source| Error::Task { source })?
    });
    tokio::time::timeout(deadline, task)
        .await
        .map_err(|_| failure("heap sampling deadline expired; native work is not cancelled and remains exclusive"))?
        .map_err(|source| Error::Task { source })?
}

struct ActiveGuard(bool);

impl ActiveGuard {
    fn start() -> Result<Self> {
        let status = settings()?;
        if !status.compiled || !status.enabled_at_startup || status.active {
            return Err(failure(
                "heap sampling requires startup prof:true,prof_active:false",
            ));
        }
        set_active(true)?;
        Ok(Self(true))
    }

    fn stop(&mut self) -> Result<()> {
        set_active(false)?;
        self.0 = false;
        Ok(())
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        if self.0 {
            let _ = set_active(false);
        }
    }
}

fn set_active(value: bool) -> Result<()> {
    // SAFETY: prof.active is a writable bool; no caller-provided mallctl name is accepted.
    unsafe { tikv_jemalloc_ctl::raw::write(b"prof.active\0", value) }.map_err(native_error)
}

fn dump(directory: &Path, limit: u64, window: Duration) -> Result<Artifact> {
    validate_directory(directory, limit)?;
    let id = tidemq_protocol::Id::random().map_err(|source| Error::Protocol { source })?;
    let target = directory.join(format!("heap-{id}"));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&target)
        .map_err(|source| io_error("create private profile directory", &target, source))?;
    let result = dump_in_directory(&target, limit, window);
    let _ = std::fs::remove_file(target.join("native.fifo"));
    if result.is_err() {
        let _ = std::fs::remove_file(target.join("profile.heap"));
        let _ = std::fs::remove_dir(&target);
    }
    result
}

fn validate_directory(directory: &Path, limit: u64) -> Result<()> {
    let metadata = std::fs::symlink_metadata(directory)
        .map_err(|source| io_error("inspect profile directory", directory, source))?;
    if !metadata.is_dir() || metadata.mode() & 0o077 != 0 {
        return Err(failure(
            "profile directory must be a private real directory with mode 0700",
        ));
    }
    let entries = std::fs::read_dir(directory)
        .map_err(|source| io_error("list profile directory", directory, source))?;
    for (index, entry) in entries.take(8).enumerate() {
        entry.map_err(|source| io_error("inspect profile artifact", directory, source))?;
        if index >= 7 {
            return Err(failure(
                "profile retention budget reached; remove an old artifact explicitly",
            ));
        }
    }
    let path = c_path(directory)?;
    let mut info = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: path is NUL-terminated and info has the exact platform statvfs layout.
    if unsafe { libc::statvfs(path.as_ptr(), info.as_mut_ptr()) } != 0 {
        return Err(io_error(
            "read profile disk availability",
            directory,
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: A successful statvfs initialized the entire result.
    let info = unsafe { info.assume_init() };
    let available = u128::from(info.f_bavail) * u128::from(info.f_frsize);
    if available < u128::from(limit) * 2 + 67_108_864 {
        return Err(failure("profile disk reserve is unavailable"));
    }
    Ok(())
}

fn dump_in_directory(directory: &Path, limit: u64, window: Duration) -> Result<Artifact> {
    let fifo_path = directory.join("native.fifo");
    let fifo_name = c_path(&fifo_path)?;
    // SAFETY: The fresh private directory contains no existing entry at this NUL-terminated path.
    if unsafe { libc::mkfifo(fifo_name.as_ptr(), 0o600) } != 0 {
        return Err(io_error(
            "create native profile pipe",
            &fifo_path,
            io::Error::last_os_error(),
        ));
    }
    let pipe = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&fifo_path)
        .map_err(|source| io_error("open native profile pipe", &fifo_path, source))?;
    let path = directory.join("profile.heap");
    let output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .map_err(|source| io_error("create heap profile", &path, source))?;
    let finished = Arc::new(AtomicBool::new(false));
    let reader_finished = finished.clone();
    let reader = std::thread::Builder::new()
        .name("heap-profile-drain".to_owned())
        .spawn(move || drain(pipe, output, limit, &reader_finished))
        .map_err(|source| io_error("start bounded profile drain", &path, source))?;
    // SAFETY: prof.dump expects const char* by reference; fifo_name outlives the synchronous call.
    let native = unsafe { tikv_jemalloc_ctl::raw::write(b"prof.dump\0", fifo_name.as_ptr()) }
        .map_err(native_error);
    finished.store(true, Ordering::Release);
    let bytes = reader
        .join()
        .map_err(|_| failure("heap profile drain thread failed"))?
        .map_err(|source| io_error("drain bounded heap profile", &path, source))?;
    native?;
    Ok(Artifact {
        path,
        bytes,
        allocator: "jemalloc",
        sampling_window_ms: window.as_millis() as u64,
    })
}

fn drain(mut pipe: File, output: File, limit: u64, finished: &AtomicBool) -> io::Result<u64> {
    let mut sink = BoundedSink {
        output,
        bytes: 0,
        limit,
        error: None,
    };
    let mut buffer = [0_u8; 8_192];
    loop {
        let native_finished = finished.load(Ordering::Acquire);
        match pipe.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                if let Some(bytes) = buffer.get(..read) {
                    sink.consume(bytes);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if native_finished {
                    break;
                }
                let mut descriptor = libc::pollfd {
                    fd: pipe.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: poll receives one valid descriptor and a short timeout on a dedicated blocking thread.
                unsafe {
                    libc::poll(&mut descriptor, 1, 20);
                }
            }
            Err(error) => return Err(error),
        }
    }
    sink.finish()
}

struct BoundedSink<W> {
    output: W,
    bytes: u64,
    limit: u64,
    error: Option<io::Error>,
}

impl<W: Write> BoundedSink<W> {
    fn consume(&mut self, bytes: &[u8]) {
        if self.error.is_some() {
            return;
        }
        if (bytes.len() as u64) > self.limit.saturating_sub(self.bytes) {
            self.error = Some(io::Error::other("heap profile exceeds its output budget"));
        } else if let Err(error) = self.output.write_all(bytes) {
            self.error = Some(error);
        } else {
            self.bytes += bytes.len() as u64;
        }
    }

    fn finish(mut self) -> io::Result<u64> {
        if let Some(error) = self.error {
            return Err(error);
        }
        self.output.flush()?;
        Ok(self.bytes)
    }
}

fn c_path(path: &Path) -> Result<CString> {
    if path.as_os_str().len() > 1_024 {
        return Err(failure("heap profile path is too long"));
    }
    CString::new(path.as_os_str().as_bytes()).map_err(|_| failure("heap profile path contains NUL"))
}

fn native_error(source: tikv_jemalloc_ctl::Error) -> Error {
    Error::Jemalloc { source }
}
fn failure(reason: &'static str) -> Error {
    Error::Allocator { reason }
}
fn io_error(operation: &'static str, path: &Path, source: io::Error) -> Error {
    Error::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

#[derive(Debug, Serialize)]
pub struct ProbeArtifacts {
    pub held: Artifact,
    pub released: Artifact,
    pub known_live_bytes: u64,
}

/// Produce a known allocation stack with live and cross-thread-release snapshots.
pub async fn probe(directory: PathBuf) -> Result<ProbeArtifacts> {
    let capture = sample(
        directory.clone(),
        8_388_608,
        Duration::from_millis(750),
        Duration::from_secs(5),
    );
    let workload = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        tokio::task::spawn_blocking(known_live_profile_allocation)
            .await
            .map_err(|source| Error::Task { source })?
    };
    let (held, buffer) = tokio::join!(capture, workload);
    let buffer = buffer?;
    let held = held?;
    tokio::task::spawn_blocking(move || drop(buffer))
        .await
        .map_err(|source| Error::Task { source })?;
    let released = sample(
        directory,
        8_388_608,
        Duration::from_millis(100),
        Duration::from_secs(5),
    )
    .await?;
    Ok(ProbeArtifacts {
        held,
        released,
        known_live_bytes: 8_388_608,
    })
}

#[inline(never)]
fn known_live_profile_allocation() -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(8_388_608)
        .map_err(|source| Error::Reserve { source })?;
    buffer.resize(8_388_608, 0xa5);
    std::hint::black_box(&buffer);
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_budget_keeps_draining_without_extending_the_artifact() {
        let mut sink = BoundedSink {
            output: Vec::new(),
            bytes: 0,
            limit: 4,
            error: None,
        };
        sink.consume(b"1234");
        sink.consume(b"5");
        sink.consume(b"ignored");
        assert_eq!(sink.output, b"1234");
        assert!(sink.finish().is_err());
    }
}
