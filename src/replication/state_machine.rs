use std::time::Instant;

use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftSnapshotBuilder,
    Snapshot, SnapshotMeta, StorageError, StoredMembership,
};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::observability::metrics::Stage;
use crate::wal::{self, Error, FRAME_HEADER_BYTES, Result};

use super::snapshot::{self, Envelope};
use super::store::{
    Applied, FORMAT_VERSION, LogState, SNAPSHOT_FILE, ensure_application_available, storage_error,
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
            let mut incoming = self.0.incoming.lock().await;
            if let Some(previous) = incoming.take() {
                match fs::remove_file(&previous).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(Error::io(previous, error)),
                }
            }
            let identity = tidemq_protocol::Id::random().map_err(|error| Error::Writer {
                reason: error.to_string(),
            })?;
            let path = self.0.directory.join(format!(".incoming-{identity}"));
            let file = SnapshotFile::open(
                &path,
                self.0
                    .options
                    .max_snapshot_bytes
                    .saturating_add(FRAME_HEADER_BYTES) as u64,
                true,
            )
            .await?;
            *incoming = Some(path);
            Ok(Box::new(file))
        }
        .await;
        result.map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        mut file: Box<SnapshotFile>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let result = async {
            file.flush()
                .await
                .map_err(|error| Error::io("received snapshot", error))?;
            let bytes = file
                .read_bytes(
                    self.0
                        .options
                        .max_snapshot_bytes
                        .saturating_add(FRAME_HEADER_BYTES),
                )
                .await?;
            let identity = self.0.identity.clone();
            let maximum = self.0.options.max_snapshot_bytes;
            let envelope = tokio::task::spawn_blocking(move || {
                let payload = wal::unframe(&bytes, maximum)?;
                let envelope: Envelope<A> = wal::decode_value(payload)?;
                snapshot::validate(&envelope, &identity)?;
                Ok::<_, Error>((envelope, bytes))
            })
            .await
            .map_err(|error| Error::Writer {
                reason: format!("snapshot decode task failed: {error}"),
            })??;
            let (envelope, bytes) = envelope;
            if envelope.meta != *meta {
                return Err(Error::Corrupt {
                    offset: 0,
                    reason: "snapshot metadata does not match the checksummed state",
                });
            }
            let _serial = self.0.snapshot_serial.lock().await;
            let mut applied = self.0.applied.write().await;
            ensure_application_available(&applied)?;
            if meta.last_log_id < applied.last_applied {
                return Err(Error::Configuration {
                    reason: "snapshot would revert applied state",
                });
            }
            wal::write_atomic(&self.0.directory.join(SNAPSHOT_FILE), &bytes).await?;
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
        .await;
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
            Some(meta) => snapshot::open_snapshot(
                &self.0.directory.join(SNAPSHOT_FILE),
                meta,
                &self.0.options,
            )
            .await
            .map(Some)
            .map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error)),
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
        let _serial = self.0.snapshot_serial.lock().await;
        let applied = {
            let applied = self.0.applied.read().await;
            ensure_application_available(&applied)?;
            applied.clone()
        };
        let identity = tidemq_protocol::Id::random().map_err(|error| Error::Writer {
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
            wal::codec_frame(&bytes, 0)
        })
        .await
        .map_err(|error| Error::Writer {
            reason: format!("snapshot encoding task failed: {error}"),
        })??;
        wal::write_atomic(&self.0.directory.join(SNAPSHOT_FILE), &encoded).await?;
        *self.0.snapshot_meta.write().await = Some(meta.clone());
        tracing::info!(event = "raft_snapshot_persisted", node_id = self.0.identity.node_id,
            group_id = %self.0.identity.group.group_id, snapshot_id = %meta.snapshot_id,
            "Persisted a snapshot before consensus log compaction");
        snapshot::open_snapshot(&self.0.directory.join(SNAPSHOT_FILE), meta, &self.0.options).await
    }
}
