use std::path::Path;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::wal::{
    Error, MAX_SEGMENTED_DIRECTORY_ENTRIES, Result, SegmentedFileKind, SnapshotRef,
    segmented_file_kind,
};

use super::StorageOptions;

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct SnapshotDiagnostics {
    pub snapshot_files: usize,
    pub snapshot_file_bytes: u64,
    pub snapshot_retired_bytes: u64,
    pub snapshot_readers: usize,
    pub snapshot_incoming_files: usize,
    pub snapshot_gc_failures: u64,
    pub snapshot_file_limit: usize,
    pub snapshot_reader_limit: usize,
    pub snapshot_publication_uncertain: bool,
    pub snapshot_publication_pending: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn uncertain_manifest_publication_freezes_cleanup_and_keeps_both_generations() {
        let directory = std::env::temp_dir().join(format!(
            "tidemq-snapshot-freeze-{}",
            tidemq_protocol::Id::new_v7().unwrap()
        ));
        tokio::fs::create_dir(&directory).await.unwrap();
        let owner = Arc::new(std::fs::File::create(directory.join(".writer-lock")).unwrap());
        let old = SnapshotRef {
            file_name: format!("snapshot-{}.bin", tidemq_protocol::Id::new_v7().unwrap()),
            byte_len: 7,
            checksum: [0; 32],
        };
        let new = SnapshotRef {
            file_name: format!("snapshot-{}.bin", tidemq_protocol::Id::new_v7().unwrap()),
            byte_len: 11,
            checksum: [1; 32],
        };
        for reference in [&old, &new] {
            tokio::fs::write(
                directory.join(&reference.file_name),
                vec![0; reference.byte_len as usize],
            )
            .await
            .unwrap();
        }
        let options = StorageOptions::default();
        let mut registry = SnapshotRegistry::new(Some(old.clone()), &options, owner);
        let candidate = registry.candidate(new.clone()).unwrap();
        registry.begin_publication(&candidate);
        drop(candidate);
        registry.cleanup(&directory).await.unwrap();
        assert!(registry.diagnostics().snapshot_publication_pending);
        assert!(registry.candidate(new.clone()).is_err());
        registry.freeze_publication();
        assert!(registry.cleanup(&directory).await.is_err());
        assert!(registry.candidate(new.clone()).is_err());
        assert!(registry.reserve_reader().is_err());
        assert!(registry.incoming().is_err());
        let diagnostic = registry.diagnostics();
        assert!(diagnostic.snapshot_publication_uncertain);
        assert_eq!(diagnostic.snapshot_file_bytes, 18);
        assert_eq!(diagnostic.snapshot_file_limit, options.max_snapshot_files);
        assert_eq!(
            diagnostic.snapshot_reader_limit,
            options.max_snapshot_readers
        );
        for reference in [&old, &new] {
            assert!(
                tokio::fs::try_exists(directory.join(&reference.file_name))
                    .await
                    .unwrap()
            );
        }
        drop(registry);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}

#[derive(Debug)]
pub(crate) struct SnapshotAsset {
    pub reference: SnapshotRef,
    _owner: Arc<std::fs::File>,
}

#[derive(Debug)]
pub(crate) struct SnapshotLease {
    _asset: Option<Arc<SnapshotAsset>>,
    _owner: Arc<std::fs::File>,
    _permit: OwnedSemaphorePermit,
}

pub(crate) struct SnapshotRegistry {
    current: Option<SnapshotRef>,
    assets: Vec<Arc<SnapshotAsset>>,
    incoming_files: Vec<String>,
    owner: Arc<std::fs::File>,
    readers: Arc<Semaphore>,
    incoming: Arc<Semaphore>,
    max_files: usize,
    max_readers: usize,
    gc_failures: u64,
    publication_uncertain: bool,
    publishing: Option<SnapshotRef>,
}

impl SnapshotRegistry {
    pub fn new(
        current: Option<SnapshotRef>,
        options: &StorageOptions,
        owner: Arc<std::fs::File>,
    ) -> Self {
        let assets = current
            .iter()
            .cloned()
            .map(|reference| {
                Arc::new(SnapshotAsset {
                    reference,
                    _owner: owner.clone(),
                })
            })
            .collect();
        Self {
            current,
            assets,
            incoming_files: Vec::new(),
            owner,
            readers: Arc::new(Semaphore::new(options.max_snapshot_readers)),
            incoming: Arc::new(Semaphore::new(1)),
            max_files: options.max_snapshot_files,
            max_readers: options.max_snapshot_readers,
            gc_failures: 0,
            publication_uncertain: false,
            publishing: None,
        }
    }

    pub fn diagnostics(&self) -> SnapshotDiagnostics {
        let mut value = SnapshotDiagnostics {
            snapshot_files: self.assets.len(),
            snapshot_readers: self
                .max_readers
                .saturating_sub(self.readers.available_permits()),
            snapshot_incoming_files: self.incoming_files.len(),
            snapshot_gc_failures: self.gc_failures,
            snapshot_file_limit: self.max_files,
            snapshot_reader_limit: self.max_readers,
            snapshot_publication_uncertain: self.publication_uncertain,
            snapshot_publication_pending: self.publishing.is_some(),
            ..SnapshotDiagnostics::default()
        };
        for asset in &self.assets {
            value.snapshot_file_bytes = value
                .snapshot_file_bytes
                .saturating_add(asset.reference.byte_len);
            if self.current.as_ref() != Some(&asset.reference) {
                value.snapshot_retired_bytes = value
                    .snapshot_retired_bytes
                    .saturating_add(asset.reference.byte_len);
            }
        }
        value
    }

    pub fn reserve_reader(&self) -> Result<OwnedSemaphorePermit> {
        self.ensure_available()?;
        self.readers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Limit {
                name: "snapshot reader handles",
            })
    }

    pub fn lease(
        &self,
        reference: &SnapshotRef,
        permit: OwnedSemaphorePermit,
    ) -> Result<Arc<SnapshotLease>> {
        self.ensure_available()?;
        let asset = self
            .assets
            .iter()
            .find(|asset| asset.reference == *reference)
            .cloned()
            .ok_or(Error::Corrupt {
                offset: 0,
                reason: "snapshot is not registered for reader ownership",
            })?;
        Ok(Arc::new(SnapshotLease {
            _asset: Some(asset),
            _owner: self.owner.clone(),
            _permit: permit,
        }))
    }

    pub fn candidate(&mut self, reference: SnapshotRef) -> Result<Arc<SnapshotAsset>> {
        self.ensure_available()?;
        if self.publishing.is_some() {
            return Err(Error::Writer {
                reason:
                    "An unfinished snapshot publication requires recovery before another generation"
                        .into(),
            });
        }
        if self.assets.len() >= self.max_files {
            return Err(Error::Limit {
                name: "named snapshot generations",
            });
        }
        let asset = Arc::new(SnapshotAsset {
            reference,
            _owner: self.owner.clone(),
        });
        self.assets.push(asset.clone());
        Ok(asset)
    }

    pub fn commit(&mut self, asset: &SnapshotAsset) {
        self.current = Some(asset.reference.clone());
        self.publishing = None;
    }

    pub fn begin_publication(&mut self, asset: &SnapshotAsset) {
        self.publishing = Some(asset.reference.clone());
    }

    pub fn freeze_publication(&mut self) {
        self.publication_uncertain = true;
    }

    fn ensure_available(&self) -> Result<()> {
        if self.publication_uncertain {
            return Err(Error::Writer {
                reason: "Snapshot publication outcome is uncertain; restart is required".into(),
            });
        }
        Ok(())
    }

    pub fn incoming(&mut self) -> Result<(String, Arc<SnapshotLease>)> {
        self.ensure_available()?;
        if !self.incoming_files.is_empty() {
            return Err(Error::Limit {
                name: "incoming snapshot files",
            });
        }
        let permit = self
            .incoming
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Limit {
                name: "incoming snapshot handles",
            })?;
        let id = tidemq_protocol::Id::new_v7().map_err(|error| Error::Writer {
            reason: error.to_string(),
        })?;
        let name = format!(".incoming-{id}");
        self.incoming_files.push(name.clone());
        Ok((
            name,
            Arc::new(SnapshotLease {
                _asset: None,
                _owner: self.owner.clone(),
                _permit: permit,
            }),
        ))
    }

    /// Run only after the complete manifest, domain state, and recovery chain are validated.
    pub async fn cleanup(&mut self, directory: &Path) -> Result<()> {
        self.ensure_available()?;
        self.discover(directory).await?;
        let mut removable = Vec::new();
        removable
            .try_reserve_exact(self.assets.len().saturating_add(self.incoming_files.len()))
            .map_err(|_| Error::Limit {
                name: "snapshot cleanup inventory",
            })?;
        for asset in &self.assets {
            if self.current.as_ref() != Some(&asset.reference)
                && self.publishing.as_ref() != Some(&asset.reference)
                && Arc::strong_count(asset) == 1
            {
                removable.push(asset.reference.file_name.clone());
            }
        }
        if self.incoming.available_permits() == 1 {
            removable.extend(self.incoming_files.iter().cloned());
        }
        for name in removable {
            if self.remove(directory, &name).await {
                self.assets
                    .retain(|asset| asset.reference.file_name != name);
                self.incoming_files.retain(|file| *file != name);
            }
        }
        if self.assets.len() > self.max_files || self.incoming_files.len() > 1 {
            return Err(Error::Limit {
                name: "snapshot files retained after cleanup",
            });
        }
        Ok(())
    }

    async fn discover(&mut self, directory: &Path) -> Result<()> {
        let mut entries = tokio::fs::read_dir(directory)
            .await
            .map_err(|error| Error::io(directory, error))?;
        let mut count = 0_usize;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| Error::io(directory, error))?
        {
            count = count.checked_add(1).ok_or(Error::Limit {
                name: "snapshot directory inventory",
            })?;
            if count > MAX_SEGMENTED_DIRECTORY_ENTRIES {
                return Err(Error::Limit {
                    name: "snapshot directory entries",
                });
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::Configuration {
                    reason: "snapshot directory contains a noncanonical name",
                })?;
            let kind = segmented_file_kind(&name).ok_or(Error::Configuration {
                reason: "snapshot directory contains an unknown layout entry",
            })?;
            // WAL rotation owns these files and can retire them during snapshot discovery.
            if !matches!(
                kind,
                SegmentedFileKind::Snapshot | SegmentedFileKind::Incoming
            ) {
                continue;
            }
            let metadata = tokio::fs::symlink_metadata(entry.path())
                .await
                .map_err(|error| Error::io(entry.path(), error))?;
            if !metadata.is_file() {
                return Err(Error::Configuration {
                    reason: "snapshot directory contains a nonregular entry",
                });
            }
            match kind {
                SegmentedFileKind::Snapshot
                    if !self
                        .assets
                        .iter()
                        .any(|asset| asset.reference.file_name == name) =>
                {
                    self.assets.push(Arc::new(SnapshotAsset {
                        reference: SnapshotRef {
                            file_name: name,
                            byte_len: metadata.len(),
                            checksum: [0; 32],
                        },
                        _owner: self.owner.clone(),
                    }));
                }
                SegmentedFileKind::Incoming if !self.incoming_files.contains(&name) => {
                    self.incoming_files.push(name);
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn remove(&mut self, directory: &Path, name: &str) -> bool {
        let path = directory.join(name);
        let parent = directory.to_owned();
        let owner = self.owner.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _owner = owner;
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::io(path, error)),
            }
            std::fs::File::open(&parent)
                .and_then(|file| file.sync_all())
                .map_err(|error| Error::io(parent, error))
        })
        .await
        .map_err(|error| Error::Writer {
            reason: format!("snapshot cleanup worker failed: {error}"),
        })
        .and_then(std::convert::identity);
        if let Err(error) = result {
            self.gc_failures = self.gc_failures.saturating_add(1);
            let error = anyhow::Error::new(error);
            tracing::warn!(
                event = "raft_snapshot_gc_failed",
                file_name = name,
                error = format_args!("{error:#}"),
                "Retained a snapshot file after cleanup failed"
            );
            return false;
        }
        true
    }
}
