use serde::Serialize;
use snafu::ResultExt;

use crate::error::{ReserveSnafu, TaskSnafu};
use crate::{Error, Result};

#[derive(Debug, Serialize)]
pub struct AllocatorSnapshot {
    pub backend: &'static str,
    pub selection_scope: &'static str,
    pub native_version: Option<String>,
    pub stats: Option<serde_json::Value>,
    pub note: &'static str,
}

pub fn backend() -> &'static str {
    if cfg!(feature = "allocator-mimalloc") {
        "mimalloc"
    } else if cfg!(feature = "allocator-jemalloc") {
        "jemalloc"
    } else {
        "system"
    }
}

pub async fn snapshot() -> Result<AllocatorSnapshot> {
    tokio::task::spawn_blocking(read_snapshot)
        .await
        .context(TaskSnafu)?
}

#[cfg(feature = "allocator-mimalloc")]
fn read_snapshot() -> Result<AllocatorSnapshot> {
    let raw = mimalloc::MiMalloc::stats_json().map_err(|reason| Error::Allocator { reason })?;
    if raw.count_bytes() > 1_048_576 {
        return Err(Error::Allocator {
            reason: "native statistics exceed the diagnostic budget",
        });
    }
    let text = raw.to_str().map_err(|_| Error::Allocator {
        reason: "native statistics are not UTF-8",
    })?;
    let stats = serde_json::from_str(text).map_err(|source| Error::AllocatorJson { source })?;
    Ok(AllocatorSnapshot {
        backend: "mimalloc",
        selection_scope: "service_binary",
        native_version: Some(mimalloc::MiMalloc.version().to_string()),
        stats: Some(stats),
        note: "Native aggregate statistics; detailed counters depend on MI_STAT. This is not a heap stack profile or measured Linux RSS.",
    })
}

#[cfg(all(feature = "allocator-jemalloc", not(feature = "allocator-mimalloc")))]
fn read_snapshot() -> Result<AllocatorSnapshot> {
    use tikv_jemalloc_ctl::{epoch, stats, version};
    let failure = |source| Error::Jemalloc { source };
    epoch::advance().map_err(failure)?;
    let allocated = stats::allocated::read().map_err(failure)?;
    let active = stats::active::read().map_err(failure)?;
    let resident = stats::resident::read().map_err(failure)?;
    let retained = stats::retained::read().map_err(failure)?;
    Ok(AllocatorSnapshot {
        backend: "jemalloc",
        selection_scope: "service_binary",
        native_version: Some(version::read().map_err(failure)?.to_owned()),
        stats: Some(
            serde_json::json!({"allocated_bytes": allocated, "active_bytes": active,
            "resident_bytes": resident, "retained_virtual_bytes": retained}),
        ),
        note: "Epoch-refreshed allocator statistics; fields are not atomic, process RSS, or a heap stack profile.",
    })
}

#[cfg(not(any(feature = "allocator-mimalloc", feature = "allocator-jemalloc")))]
fn read_snapshot() -> Result<AllocatorSnapshot> {
    Ok(AllocatorSnapshot {
        backend: "system",
        selection_scope: "service_binary",
        native_version: None,
        stats: None,
        note: "Detailed System allocator statistics are platform-specific and unavailable here; use operating-system tools.",
    })
}

#[derive(Serialize)]
pub struct AllocationProbe {
    pub requested_live_bytes: u64,
    pub before: AllocatorSnapshot,
    pub held: AllocatorSnapshot,
    pub after_free: AllocatorSnapshot,
}

pub async fn probe(block_bytes: u32, blocks: u32) -> Result<AllocationProbe> {
    let total = u64::from(block_bytes)
        .checked_mul(u64::from(blocks))
        .ok_or(Error::Configuration {
            reason: "allocation probe size overflow",
        })?;
    if block_bytes == 0 || blocks == 0 || blocks > 64 || total > 33_554_432 {
        return Err(Error::Configuration {
            reason: "allocation probe is limited to 64 blocks and 32 MiB",
        });
    }
    let before = snapshot().await?;
    let mut buffers = Vec::new();
    buffers
        .try_reserve_exact(blocks as usize)
        .context(ReserveSnafu)?;
    for _ in 0..blocks {
        buffers.push(allocate_probe_block(block_bytes as usize)?);
    }
    std::hint::black_box(&buffers);
    let held = snapshot().await?;
    drop(buffers);
    let after_free = snapshot().await?;
    Ok(AllocationProbe {
        requested_live_bytes: total,
        before,
        held,
        after_free,
    })
}

#[inline(never)]
fn allocate_probe_block(bytes: usize) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(bytes).context(ReserveSnafu)?;
    buffer.resize(bytes, 0x5a);
    Ok(buffer)
}
