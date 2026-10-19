use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::time::Instant;

use super::{Error, Result};

/// One owned worker counts fixed replica directories, including bounded atomic-replace files.
/// Journal bytes are actual file lengths, independent of retained encoded Raft entries.
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

fn replica_bytes(directory: &Path, snapshot_expected: bool) -> Result<u64> {
    let mut total = 0_u64;
    let mut journal = false;
    let mut snapshot = false;
    for (position, entry) in std::fs::read_dir(directory)
        .map_err(|source| Error::operation("open known replica directory", source))?
        .enumerate()
    {
        if position >= 16 {
            return Err(Error::invalid(
                "Replica directory exceeded its finite file bound",
            ));
        }
        let entry =
            entry.map_err(|source| Error::operation("inspect replica file entry", source))?;
        let metadata = entry
            .path()
            .symlink_metadata()
            .map_err(|source| Error::operation("measure replica file", source))?;
        if !metadata.is_file() {
            return Err(Error::invalid(
                "Replica directory contains a non-regular entry",
            ));
        }
        journal |= entry.file_name() == "raft.wal";
        snapshot |= entry.file_name() == "snapshot.bin";
        total = total
            .checked_add(metadata.len())
            .ok_or_else(|| Error::invalid("Replica byte count overflowed"))?;
    }
    if !journal || (snapshot_expected && !snapshot) {
        return Err(Error::unavailable());
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn real_file_lengths_include_pending_replacements_and_missing_evidence_is_rejected() {
        let directory = std::env::temp_dir().join(format!(
            "tidemq-physical-{}",
            tidemq_protocol::Id::random().unwrap()
        ));
        let group = directory.join("group-2");
        tokio::fs::create_dir_all(&group).await.unwrap();
        tokio::fs::write(group.join("raft.wal"), [0; 31])
            .await
            .unwrap();
        tokio::fs::write(group.join(".replace-test"), [0; 7])
            .await
            .unwrap();
        let sampler = PhysicalSampler::new(directory.clone());
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        assert_eq!(
            sampler.sample(vec![(2, false)], deadline).await.unwrap(),
            38
        );
        assert!(sampler.sample(vec![(2, true)], deadline).await.is_err());
        tokio::fs::write(group.join("snapshot.bin"), [0; 11])
            .await
            .unwrap();
        assert_eq!(sampler.sample(vec![(2, true)], deadline).await.unwrap(), 49);
        assert!(sampler.sample(vec![(3, false)], deadline).await.is_err());
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}
