use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::wal::{MAX_SEGMENTED_DIRECTORY_ENTRIES, SegmentedFileKind, segmented_file_kind};

use super::{Error, Result};

/// One owned worker counts every named replica file, including retired and staging files.
/// File lengths are independent of logical retained entries and filesystem available space.
pub(crate) struct PhysicalSampler {
    directory: PathBuf,
    slot: Arc<Semaphore>,
}

impl PhysicalSampler {
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            slot: Arc::new(Semaphore::new(1)),
        }
    }

    pub async fn sample(&self, groups: Vec<(u64, bool)>, deadline: Instant) -> Result<u64> {
        if groups.len() > 17 {
            return Err(Error::invalid(
                "Physical sample exceeds the configured group bound",
            ));
        }
        let permit = self
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::unavailable())?;
        let directory = self.directory.clone();
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            groups
                .into_iter()
                .try_fold(0_u64, |total, (group, snapshot)| {
                    total
                        .checked_add(replica_bytes(
                            &directory.join(format!("group-{group}")),
                            snapshot,
                        )?)
                        .ok_or_else(|| Error::invalid("Physical storage byte count overflowed"))
                })
        });
        tokio::time::timeout_at(deadline, task)
            .await
            .map_err(|_| Error::unavailable())?
            .map_err(|source| Error::operation("join physical storage observation", source))?
    }
}

pub(crate) fn replica_bytes(directory: &Path, snapshot_expected: bool) -> Result<u64> {
    let metadata = std::fs::symlink_metadata(directory)
        .map_err(|source| Error::operation("inspect known replica directory", source))?;
    if !metadata.is_dir() {
        return Err(Error::invalid("Replica path is not a regular directory"));
    }
    let mut total = 0_u64;
    let mut identity = false;
    let mut manifest = false;
    let mut segment = false;
    let mut snapshot = false;
    for (position, entry) in std::fs::read_dir(directory)
        .map_err(|source| Error::operation("open known replica directory", source))?
        .enumerate()
    {
        if position >= MAX_SEGMENTED_DIRECTORY_ENTRIES {
            return Err(Error::invalid(
                "Replica directory exceeded its finite file bound",
            ));
        }
        let entry =
            entry.map_err(|source| Error::operation("inspect replica file entry", source))?;
        let name = entry.file_name();
        let kind = name
            .to_str()
            .and_then(segmented_file_kind)
            .ok_or_else(|| Error::invalid("Replica directory contains an invalid layout entry"))?;
        let metadata = entry
            .path()
            .symlink_metadata()
            .map_err(|source| Error::operation("measure replica file", source))?;
        if !metadata.is_file() {
            return Err(Error::invalid(
                "Replica directory contains a non-regular entry",
            ));
        }
        identity |= kind == SegmentedFileKind::Identity;
        manifest |= kind == SegmentedFileKind::Manifest;
        segment |= kind == SegmentedFileKind::Segment;
        snapshot |= kind == SegmentedFileKind::Snapshot;
        total = add_file_bytes(total, metadata.len())?;
    }
    if !identity || !manifest || !segment || (snapshot_expected && !snapshot) {
        return Err(Error::unavailable());
    }
    Ok(total)
}

fn add_file_bytes(total: u64, bytes: u64) -> Result<u64> {
    total
        .checked_add(bytes)
        .ok_or_else(|| Error::invalid("Replica byte count overflowed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn real_file_lengths_include_all_segments_staging_and_unreferenced_snapshots() {
        let directory = std::env::temp_dir().join(format!(
            "tidemq-physical-{}",
            tidemq_protocol::Id::new_v7().unwrap()
        ));
        let group = directory.join("group-2");
        tokio::fs::create_dir_all(&group).await.unwrap();
        for name in ["identity.bin", "manifest.bin"] {
            tokio::fs::write(group.join(name), [0; 5]).await.unwrap();
        }
        for _ in 0..20 {
            let id = tidemq_protocol::Id::new_v7().unwrap();
            tokio::fs::write(group.join(format!("segment-{id}.wal")), [0; 31])
                .await
                .unwrap();
        }
        let staging = format!(".replace-{}", tidemq_protocol::Id::new_v7().unwrap());
        tokio::fs::write(group.join(staging), [0; 7]).await.unwrap();
        let sampler = PhysicalSampler::new(directory.clone());
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        assert_eq!(
            sampler.sample(vec![(2, false)], deadline).await.unwrap(),
            637
        );
        assert!(sampler.sample(vec![(2, true)], deadline).await.is_err());
        for _ in 0..2 {
            let id = tidemq_protocol::Id::new_v7().unwrap();
            tokio::fs::write(group.join(format!("snapshot-{id}.bin")), [0; 11])
                .await
                .unwrap();
        }
        let checkpoint = format!("checkpoint-{}.bin", tidemq_protocol::Id::new_v7().unwrap());
        tokio::fs::write(group.join(checkpoint), [0; 13])
            .await
            .unwrap();
        assert_eq!(
            sampler.sample(vec![(2, true)], deadline).await.unwrap(),
            672
        );
        assert!(sampler.sample(vec![(3, false)], deadline).await.is_err());
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[test]
    fn malformed_and_legacy_layouts_are_not_capacity_evidence() {
        let directory = replica_directory();
        assert!(replica_bytes(&directory, false).is_ok());
        for name in [
            "raft.wal",
            "snapshot.bin",
            "segment-invalid.wal",
            "unknown.bin",
        ] {
            let path = directory.join(name);
            std::fs::write(&path, [0; 3]).unwrap();
            assert!(replica_bytes(&directory, false).is_err(), "{name}");
            std::fs::remove_file(path).unwrap();
        }
        std::fs::remove_file(directory.join("manifest.bin")).unwrap();
        assert!(replica_bytes(&directory, false).is_err());
        std::fs::remove_dir_all(directory).unwrap();
        assert!(add_file_bytes(u64::MAX, 1).is_err());
    }

    #[test]
    fn directory_entry_limit_includes_orphans_and_staging_files() {
        let directory = replica_directory();
        for _ in 3..MAX_SEGMENTED_DIRECTORY_ENTRIES {
            let id = tidemq_protocol::Id::new_v7().unwrap();
            std::fs::write(directory.join(format!(".replace-{id}")), [0; 1]).unwrap();
        }
        assert!(replica_bytes(&directory, false).is_ok());
        let id = tidemq_protocol::Id::new_v7().unwrap();
        std::fs::write(directory.join(format!("checkpoint-{id}.bin")), [0; 1]).unwrap();
        assert!(replica_bytes(&directory, false).is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn non_regular_entries_and_symbolic_links_fail_closed() {
        let directory = replica_directory();
        let nested = directory.join("nested");
        std::fs::create_dir(&nested).unwrap();
        assert!(replica_bytes(&directory, false).is_err());
        std::fs::remove_dir(nested).unwrap();
        let id = tidemq_protocol::Id::new_v7().unwrap();
        let link = directory.join(format!("snapshot-{id}.bin"));
        std::os::unix::fs::symlink(directory.join("identity.bin"), &link).unwrap();
        assert!(replica_bytes(&directory, false).is_err());
        std::fs::remove_file(link).unwrap();
        let alias = directory.with_extension("alias");
        std::os::unix::fs::symlink(&directory, &alias).unwrap();
        assert!(replica_bytes(&alias, false).is_err());
        std::fs::remove_file(alias).unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn replica_directory() -> PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "tidemq-physical-{}",
            tidemq_protocol::Id::new_v7().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("identity.bin"), [0; 5]).unwrap();
        std::fs::write(directory.join("manifest.bin"), [0; 7]).unwrap();
        let id = tidemq_protocol::Id::new_v7().unwrap();
        std::fs::write(directory.join(format!("segment-{id}.wal")), [0; 11]).unwrap();
        directory
    }
}
