use std::time::Instant;

use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftSnapshotBuilder,
    Snapshot, SnapshotMeta, StorageError, StoredMembership,
};
use tokio::io::AsyncWriteExt;
use tracing::Instrument;
use tracing::instrument::WithSubscriber;

use crate::observability::metrics::Stage;
use crate::wal::{self, Error, FRAME_HEADER_BYTES, Result, SnapshotRef};

use super::snapshot::{self, Envelope};
use super::store::{
    Applied, FORMAT_VERSION, LogState, ensure_application_available, storage_error,
};
use super::{Application, DiskLog, DiskStateMachine, LogEntry, SnapshotFile, TypeConfig};

pub(crate) fn recover<A: Application>(applied: &mut Applied<A>, log: &LogState<A>) -> Result<()> {
    if log.purged > applied.last_applied {
        return Err(Error::Corrupt {
            offset: 0,
            reason: "purged logs are not covered by the durable snapshot",
        });
    }
    if log.committed <= applied.last_applied {
        return applied
            .application
            .validate()
            .map_err(|e| Error::io("application recovery", e));
    }
    let Some(committed) = log.committed else {
        return Ok(());
    };
    let start = match applied.last_applied {
        Some(last) => last.index.checked_add(1).ok_or(Error::Limit {
            name: "applied log index",
        })?,
        None => log
            .entries
            .first_key_value()
            .map(|(index, _)| *index)
            .ok_or(Error::Corrupt {
                offset: 0,
                reason: "committed log is missing from the journal",
            })?,
    };
    let mut expected = start;
    for entry in log
        .entries
        .values()
        .filter(|entry| entry.log_id.index >= start && entry.log_id.index <= committed.index)
    {
        if entry.log_id.index != expected {
            return Err(Error::Corrupt {
                offset: 0,
                reason: "committed recovery range contains a gap",
            });
        }
        apply_entry(applied, entry.clone())?;
        expected = expected.checked_add(1).ok_or(Error::Limit {
            name: "recovery index",
        })?;
    }
    if applied.last_applied != Some(committed) {
        return Err(Error::Corrupt {
            offset: 0,
            reason: "committed recovery log identity is missing or different",
        });
    }
    applied
        .application
        .validate()
        .map_err(|error| Error::io("application recovery", error))
}

fn apply_entry<A: Application>(state: &mut Applied<A>, entry: LogEntry<A>) -> Result<A::Response> {
    ensure_application_available(state)?;
    let response = match entry.payload {
        EntryPayload::Blank => A::Response::default(),
        EntryPayload::Membership(membership) => {
            state.membership = StoredMembership::new(Some(entry.log_id), membership);
            A::Response::default()
        }
        EntryPayload::Normal(command) => match state.application.try_apply(command) {
            Ok(response) => response,
            Err(source) => {
                state.failed = true;
                return Err(Error::io("application apply", source));
            }
        },
    };
    state.last_applied = Some(entry.log_id);
    Ok(response)
}

impl<A: Application> RaftStateMachine<TypeConfig<A>> for DiskStateMachine<A> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (Option<LogId<u64>>, StoredMembership<u64, BasicNode>),
        StorageError<u64>,
    > {
        let applied = self.0.applied.read().await;
        ensure_application_available(&applied)
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Read, error))?;
        Ok((applied.last_applied, applied.membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<Vec<A::Response>, StorageError<u64>>
    where
        I: IntoIterator<Item = LogEntry<A>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.apply_committed(entries)
            .await
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, error))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<SnapshotFile>, StorageError<u64>> {
        let result = async {
            ensure_application_available(&*self.0.applied.read().await)?;
            let (name, lease) = {
                let mut registry = self.0.snapshots.lock().await;
                registry.cleanup(&self.0.directory).await?;
                registry.incoming()?
            };
            let this = self.clone();
            tokio::spawn(
                async move {
                    let path = this.0.directory.join(name);
                    let limit = this
                        .0
                        .options
                        .max_snapshot_bytes
                        .checked_add(FRAME_HEADER_BYTES)
                        .ok_or(Error::Limit {
                            name: "incoming snapshot bytes",
                        })?;
                    let file = SnapshotFile::open(&path, limit as u64, true, Some(lease)).await?;
                    *this.0.incoming.lock().await = Some(path);
                    Ok(Box::new(file))
                }
                .in_current_span()
                .with_current_subscriber(),
            )
            .await
            .map_err(|error| Error::Writer {
                reason: format!("incoming snapshot task failed: {error}"),
            })?
        }
        .await;
        result.map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        file: Box<SnapshotFile>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let serial = self.0.snapshot_serial.clone().lock_owned().await;
        let this = self.clone();
        let saved_meta = meta.clone();
        let result = tokio::spawn(
            async move {
                let _serial = serial;
                this.install_durable_snapshot(saved_meta, file).await
            }
            .in_current_span()
            .with_current_subscriber(),
        )
        .await
        .map_err(|error| Error::Writer {
            reason: format!("snapshot installation task failed: {error}"),
        })
        .and_then(std::convert::identity);
        result.map_err(|error| {
            storage_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Write,
                error,
            )
        })
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<TypeConfig<A>>>, StorageError<u64>> {
        let _serial = self.0.snapshot_serial.lock().await;
        ensure_application_available(&*self.0.applied.read().await)
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Read, error))?;
        let meta = self.0.snapshot_meta.read().await.clone();
        match meta {
            Some(meta) => {
                let result = async {
                    let reference = self.0.journal.current_snapshot().ok_or(Error::Corrupt {
                        offset: 0,
                        reason: "snapshot metadata has no manifest reference",
                    })?;
                    let lease = {
                        let registry = self.0.snapshots.lock().await;
                        registry.lease(&reference, registry.reserve_reader()?)?
                    };
                    snapshot::open_snapshot(
                        &self.0.directory,
                        &reference,
                        meta,
                        &self.0.options,
                        lease,
                    )
                    .await
                }
                .await;
                result.map(Some).map_err(|error| {
                    storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error)
                })
            }
            None => Ok(None),
        }
    }
}

impl<A: Application> DiskStateMachine<A> {
    #[tracing::instrument(name = "raft_apply", level = "trace", skip_all, fields(node_id = self.0.identity.node_id, group_id = self.0.identity.group.group_id))]
    async fn apply_committed<I: IntoIterator<Item = LogEntry<A>>>(
        &self,
        entries: I,
    ) -> Result<Vec<A::Response>> {
        let mut batch = Vec::new();
        for entry in entries {
            if batch.len() >= self.0.options.max_log_entries {
                return Err(Error::Limit {
                    name: "apply batch entries",
                });
            }
            batch.push(entry);
        }
        let Some(last) = batch.last().map(|entry| entry.log_id) else {
            return Ok(Vec::new());
        };
        let mut applied = self.0.applied.write().await;
        ensure_application_available(&applied)?;
        let mut previous = applied.last_applied;
        for entry in &batch {
            if previous.is_some_and(|previous| entry.log_id.index <= previous.index) {
                return Err(Error::Configuration {
                    reason: "state machine entries must advance applied state",
                });
            }
            previous = Some(entry.log_id);
        }
        // Persist the commit boundary before exposing apply results, including on RF1.
        let log = DiskLog(self.0.clone());
        if self.0.log.lock().await.committed < Some(last) {
            log.persist_committed(Some(last)).await?;
        }
        let start = Instant::now();
        let responses = batch
            .into_iter()
            .map(|entry| apply_entry(&mut applied, entry))
            .collect::<Result<Vec<_>>>()?;
        if let Some(metrics) = &self.0.metrics {
            metrics.observe(Stage::Apply, start.elapsed().as_secs_f64());
        }
        Ok(responses)
    }
}

impl<A: Application> RaftSnapshotBuilder<TypeConfig<A>> for DiskStateMachine<A> {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig<A>>, StorageError<u64>> {
        self.build_durable_snapshot()
            .await
            .map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))
    }
}

impl<A: Application> DiskStateMachine<A> {
    #[tracing::instrument(name = "raft_snapshot_build", level = "debug", skip_all, fields(node_id = self.0.identity.node_id, group_id = self.0.identity.group.group_id))]
    async fn build_durable_snapshot(&self) -> Result<Snapshot<TypeConfig<A>>> {
        let serial = self.0.snapshot_serial.clone().lock_owned().await;
        let this = self.clone();
        // Once publication starts, cancellation must not leave the manifest and memory disagreeing.
        tokio::spawn(
            async move {
                let _serial = serial;
                this.build_snapshot_owned().await
            }
            .in_current_span()
            .with_current_subscriber(),
        )
        .await
        .map_err(|error| Error::Writer {
            reason: format!("snapshot build task failed: {error}"),
        })?
    }

    async fn build_snapshot_owned(&self) -> Result<Snapshot<TypeConfig<A>>> {
        let reader = self.0.snapshots.lock().await.reserve_reader()?;
        let applied = {
            let applied = self.0.applied.read().await;
            ensure_application_available(&applied)?;
            applied.clone()
        };
        let identity = tidemq_protocol::Id::new_v7().map_err(|error| Error::Writer {
            reason: error.to_string(),
        })?;
        let meta = SnapshotMeta {
            last_log_id: applied.last_applied,
            last_membership: applied.membership,
            snapshot_id: identity.to_string(),
        };
        let envelope = Envelope {
            format: FORMAT_VERSION,
            group: self.0.identity.group.clone(),
            meta: meta.clone(),
            application: applied.application,
        };
        snapshot::validate(&envelope, &self.0.identity)?;
        let maximum = self.0.options.max_snapshot_bytes;
        let encoded = tokio::task::spawn_blocking(move || {
            let bytes = wal::encode_value(&envelope, maximum)?;
            let bytes = wal::codec_frame(&bytes, 0)?;
            let checksum = *blake3::hash(&bytes).as_bytes();
            Ok::<_, Error>((bytes, checksum))
        })
        .await
        .map_err(|error| Error::Writer {
            reason: format!("snapshot encoding task failed: {error}"),
        })??;
        let (encoded, checksum) = encoded;
        let asset = self.publish_snapshot_bytes(encoded, checksum).await?;
        *self.0.snapshot_meta.write().await = Some(meta.clone());
        tracing::info!(event = "raft_snapshot_persisted", node_id = self.0.identity.node_id,
            group_id = %self.0.identity.group.group_id, snapshot_id = %meta.snapshot_id,
            "Persisted a snapshot before consensus log compaction");
        let lease = self
            .0
            .snapshots
            .lock()
            .await
            .lease(&asset.reference, reader)?;
        snapshot::open_snapshot(
            &self.0.directory,
            &asset.reference,
            meta,
            &self.0.options,
            lease,
        )
        .await
    }

    async fn publish_snapshot_bytes(
        &self,
        bytes: Vec<u8>,
        checksum: [u8; 32],
    ) -> Result<std::sync::Arc<super::snapshot_registry::SnapshotAsset>> {
        let id = tidemq_protocol::Id::new_v7().map_err(|error| Error::Writer {
            reason: error.to_string(),
        })?;
        let reference = SnapshotRef {
            file_name: format!("snapshot-{id}.bin"),
            byte_len: bytes.len() as u64,
            checksum,
        };
        let asset = {
            let mut registry = self.0.snapshots.lock().await;
            registry.cleanup(&self.0.directory).await?;
            registry.candidate(reference)?
        };
        snapshot::persist(&self.0.directory, bytes, asset.clone()).await?;
        // Keep both possible authorities protected if the runtime interrupts the owned task.
        self.0.snapshots.lock().await.begin_publication(&asset);
        if let Err(error) = self
            .0
            .journal
            .publish_snapshot(asset.reference.clone())
            .await
        {
            // A failed directory sync can leave the manifest naming this candidate.
            self.0.snapshots.lock().await.freeze_publication();
            return Err(error);
        }
        #[cfg(test)]
        super::cancellation_tests::boundary(&self.0.directory, "snapshot_published").await;
        self.0.snapshots.lock().await.commit(&asset);
        Ok(asset)
    }

    async fn install_durable_snapshot(
        &self,
        meta: SnapshotMeta<u64, BasicNode>,
        mut file: Box<SnapshotFile>,
    ) -> Result<()> {
        file.flush()
            .await
            .map_err(|error| Error::io("received snapshot", error))?;
        let bytes = file
            .read_bytes(
                self.0
                    .options
                    .max_snapshot_bytes
                    .checked_add(FRAME_HEADER_BYTES)
                    .ok_or(Error::Limit {
                        name: "incoming snapshot bytes",
                    })?,
            )
            .await?;
        let identity = self.0.identity.clone();
        let maximum = self.0.options.max_snapshot_bytes;
        let (envelope, bytes, checksum) = tokio::task::spawn_blocking(move || {
            let payload = wal::unframe(&bytes, maximum)?;
            let envelope: Envelope<A> = wal::decode_value(payload)?;
            snapshot::validate(&envelope, &identity)?;
            let checksum = *blake3::hash(&bytes).as_bytes();
            Ok::<_, Error>((envelope, bytes, checksum))
        })
        .await
        .map_err(|error| Error::Writer {
            reason: format!("snapshot decode task failed: {error}"),
        })??;
        if envelope.meta != meta {
            return Err(Error::Corrupt {
                offset: 0,
                reason: "snapshot metadata does not match the checksummed state",
            });
        }
        let mut applied = self.0.applied.write().await;
        ensure_application_available(&applied)?;
        if meta.last_log_id < applied.last_applied {
            return Err(Error::Configuration {
                reason: "snapshot would revert applied state",
            });
        }
        self.publish_snapshot_bytes(bytes, checksum).await?;
        *applied = Applied {
            application: envelope.application,
            last_applied: meta.last_log_id,
            membership: meta.last_membership.clone(),
            failed: false,
        };
        *self.0.snapshot_meta.write().await = Some(meta.clone());
        tracing::info!(event = "raft_snapshot_installed", node_id = self.0.identity.node_id,
            group_id = %self.0.identity.group.group_id, snapshot_id = %meta.snapshot_id,
            "Installed a durable validated snapshot");
        Ok(())
    }
}
