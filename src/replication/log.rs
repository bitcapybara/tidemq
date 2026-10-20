use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::time::Instant;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftLogReader, StorageError, Vote};
use std::borrow::Cow;

use crate::observability::metrics::Stage;
use crate::wal::{self, Error, Result};

use super::store::{ControlCheckpoint, FORMAT_VERSION, LogState, Record, storage_error};
use super::{Application, DiskLog, LogEntry, StorageOptions, TypeConfig};

fn invalid(reason: &'static str) -> Error {
    Error::Configuration { reason }
}

fn replay<A: Application>(
    state: &mut LogState<A>,
    record: Record<'_, A>,
    id: Option<wal::RecordId>,
    encoded: &[u8],
    options: &StorageOptions,
) -> Result<()> {
    validate_record(state, &record)?;
    match record {
        Record::Vote(vote) => {
            state.vote = Some(vote);
        }
        Record::Committed(committed) => {
            state.committed = committed;
        }
        Record::Entry(entry) => {
            let id = id.ok_or(invalid(
                "retained entry is missing its physical record identity",
            ))?;
            if state
                .purged
                .is_some_and(|purged| entry.log_id.index <= purged.index)
            {
                return Ok(());
            }
            let (_, bytes) = prepare_entries(
                state,
                std::slice::from_ref(entry.as_ref()),
                std::slice::from_ref(&encoded),
                options,
            )?;
            install_entries(
                state,
                vec![entry.into_owned()],
                vec![encoded.len()],
                vec![id],
                bytes,
            );
        }
        Record::Truncate(log_id) => {
            state.entries.retain(|index, _| *index < log_id.index);
            state.entry_bytes.retain(|index, _| *index < log_id.index);
            state.record_refs.retain(|index, _| *index < log_id.index);
            state.retained_bytes = retained_bytes(state)?;
        }
    }
    Ok(())
}

fn validate_record<A: Application>(state: &LogState<A>, record: &Record<'_, A>) -> Result<()> {
    let reason = match record {
        Record::Vote(vote)
            if state.vote.is_some_and(|current| {
                vote.partial_cmp(&current).is_none_or(|order| order.is_lt())
            }) =>
        {
            Some("durable vote must not regress or become incomparable")
        }
        Record::Committed(committed) if *committed < state.committed => {
            Some("durable committed log must not regress")
        }
        Record::Truncate(log_id)
            if state
                .committed
                .is_some_and(|committed| log_id.index <= committed.index) =>
        {
            Some("cannot truncate committed logs")
        }
        _ => None,
    };
    match reason {
        Some(reason) => Err(invalid(reason)),
        None => Ok(()),
    }
}

fn retained_bytes<A: Application>(state: &LogState<A>) -> Result<usize> {
    state.entry_bytes.values().try_fold(0_usize, |sum, size| {
        sum.checked_add(*size).ok_or(Error::Limit {
            name: "retained log bytes",
        })
    })
}

fn prepare_entries<A: Application, E: AsRef<[u8]>>(
    state: &LogState<A>,
    entries: &[LogEntry<A>],
    encoded: &[E],
    options: &StorageOptions,
) -> Result<(Vec<usize>, usize)> {
    let mut previous = None;
    let mut sizes = Vec::new();
    let mut bytes = state.retained_bytes;
    let mut count = state.entries.len();
    if entries.len() != encoded.len() {
        return Err(invalid("entry payload count differs from the append batch"));
    }
    for (entry, encoded) in entries.iter().zip(encoded) {
        let encoded = encoded.as_ref();
        let index = entry.log_id.index;
        if state.purged.is_some_and(|purged| index <= purged.index)
            || previous.is_some_and(|previous: u64| previous.checked_add(1) != Some(index))
        {
            return Err(invalid(
                "appended logs must be consecutive and above the purged boundary",
            ));
        }
        if previous.is_none() {
            let last = state
                .entries
                .last_key_value()
                .map(|(_, entry)| entry.log_id)
                .or(state.purged);
            if last.is_some_and(|last| index > last.index.saturating_add(1)) {
                return Err(invalid("appended logs must not leave a gap"));
            }
        }
        if let Some(existing) = state.entries.get(&index) {
            if wal::encode_value(
                &Record::Entry(Cow::Borrowed(existing)),
                options.journal.max_record_bytes,
            )?
            .as_slice()
                != encoded
            {
                return Err(invalid(
                    "conflicting logs must be truncated before replacement",
                ));
            }
        } else {
            count = count.checked_add(1).ok_or(Error::Limit {
                name: "log entries",
            })?;
        }
        bytes = bytes
            .checked_sub(state.entry_bytes.get(&index).copied().unwrap_or(0))
            .and_then(|size| size.checked_add(encoded.len()))
            .ok_or(Error::Limit {
                name: "retained log bytes",
            })?;
        if bytes > options.max_log_bytes || count > options.max_log_entries {
            return Err(Error::Limit {
                name: "retained logs; admission must preserve snapshot capacity",
            });
        }
        sizes.push(encoded.len());
        previous = Some(index);
    }
    Ok((sizes, bytes))
}

fn install_entries<A: Application>(
    state: &mut LogState<A>,
    entries: Vec<LogEntry<A>>,
    sizes: Vec<usize>,
    ids: Vec<wal::RecordId>,
    bytes: usize,
) {
    for ((entry, size), id) in entries.into_iter().zip(sizes).zip(ids) {
        state.entry_bytes.insert(entry.log_id.index, size);
        state.record_refs.insert(entry.log_id.index, id);
        state.entries.insert(entry.log_id.index, entry);
    }
    state.retained_bytes = bytes;
}

impl<A: Application> RaftLogReader<TypeConfig<A>> for DiskLog<A> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<LogEntry<A>>, StorageError<u64>> {
        let Some((start, end)) = normalize(&range) else {
            return Ok(Vec::new());
        };
        let state = self.0.log.lock().await;
        Ok(state
            .entries
            .range((Bound::Included(start), end))
            .map(|(_, entry)| entry.clone())
            .collect())
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> std::result::Result<Vec<LogEntry<A>>, StorageError<u64>> {
        if start >= end {
            return Ok(Vec::new());
        }
        let state = self.0.log.lock().await;
        let mut result = Vec::new();
        let mut bytes = 0_usize;
        for (index, entry) in state.entries.range(start..end) {
            let size = state.entry_bytes.get(index).copied().unwrap_or(0);
            if !result.is_empty()
                && bytes.saturating_add(size) > self.0.options.max_replication_batch_bytes
            {
                break;
            }
            bytes = bytes.saturating_add(size);
            result.push(entry.clone());
        }
        Ok(result)
    }
}

fn normalize<R: RangeBounds<u64>>(range: &R) -> Option<(u64, Bound<u64>)> {
    let start = match range.start_bound() {
        Bound::Unbounded => 0,
        Bound::Included(start) => *start,
        Bound::Excluded(start) => start.checked_add(1)?,
    };
    let end = match range.end_bound() {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Excluded(end) if *end <= start => return None,
        Bound::Excluded(end) => Bound::Excluded(*end),
        Bound::Included(end) if *end < start => return None,
        Bound::Included(end) => Bound::Included(*end),
    };
    Some((start, end))
}

impl<A: Application> RaftLogStorage<TypeConfig<A>> for DiskLog<A> {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<openraft::LogState<TypeConfig<A>>, StorageError<u64>> {
        let state = self.0.log.lock().await;
        Ok(openraft::LogState {
            last_purged_log_id: state.purged,
            last_log_id: state
                .entries
                .last_key_value()
                .map(|(_, entry)| entry.log_id)
                .or(state.purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> std::result::Result<(), StorageError<u64>> {
        self.persist(Record::Vote(*vote))
            .await
            .map_err(|error| storage_error(ErrorSubject::Vote, ErrorVerb::Write, error))
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.0.log.lock().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        self.persist_committed(committed)
            .await
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.0.log.lock().await.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig<A>>,
    ) -> std::result::Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = LogEntry<A>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let result = self.enqueue_entries(entries).await;
        match result {
            Ok(durability) => {
                tokio::spawn(async move {
                    let result = match durability {
                        Some(durability) => {
                            durability.durable().await.map_err(std::io::Error::other)
                        }
                        None => Ok(()),
                    };
                    callback.log_io_completed(result);
                });
                Ok(())
            }
            Err(error) => {
                callback.log_io_completed(Err(std::io::Error::other(error.to_string())));
                Err(storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let mut state = self.0.log.clone().lock_owned().await;
        let store = self.clone();
        let result = finish_mutation(tokio::spawn(async move {
            store
                .persist_locked(&mut state, Record::Truncate(log_id))
                .await?;
            store.checkpoint_locked(&state, state.purged).await
        }))
        .await;
        result.map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Delete, error))
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let mut state = self.0.log.clone().lock_owned().await;
        let store = self.clone();
        let result = finish_mutation(tokio::spawn(async move {
            if state.purged.is_some_and(|purged| log_id < purged) {
                return Err(invalid("purged log boundary must not regress"));
            }
            // OpenRaft authorizes purge after snapshot coverage; recovery rechecks that contract.
            store.checkpoint_locked(&state, Some(log_id)).await?;
            #[cfg(test)]
            super::cancellation_tests::boundary(&store.0.directory, "purge_published").await;
            state.entries.retain(|index, _| *index > log_id.index);
            state.entry_bytes.retain(|index, _| *index > log_id.index);
            state.record_refs.retain(|index, _| *index > log_id.index);
            state.retained_bytes = retained_bytes(&state)?;
            state.purged = Some(log_id);
            Ok(())
        }))
        .await;
        result.map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Delete, error))
    }
}

impl<A: Application> DiskLog<A> {
    #[tracing::instrument(name = "raft_log_append", level = "trace", skip_all, fields(node_id = self.0.identity.node_id, group_id = self.0.identity.group.group_id))]
    async fn enqueue_entries<I: IntoIterator<Item = LogEntry<A>>>(
        &self,
        entries: I,
    ) -> Result<Option<wal::SegmentedDurability>> {
        let mut batch = Vec::new();
        for entry in entries {
            if batch.len() >= self.0.options.max_log_entries {
                return Err(Error::Limit {
                    name: "append batch entries",
                });
            }
            batch.push(entry);
        }
        let mut state = self.0.log.lock().await;
        // A replayed prefix already covered by the compacted boundary cannot be resurrected.
        batch.retain(|entry| {
            state
                .purged
                .is_none_or(|purged| entry.log_id.index > purged.index)
        });
        if batch.is_empty() {
            return Ok(None);
        }
        let encoded = batch
            .iter()
            .map(|entry| {
                wal::encode_value(
                    &Record::Entry(Cow::Borrowed(entry)),
                    self.0.options.journal.max_record_bytes,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let batch_bytes = encoded
            .iter()
            .try_fold(0_usize, |total, bytes| total.checked_add(bytes.len()))
            .ok_or(Error::Limit {
                name: "append batch bytes",
            })?;
        if batch_bytes > self.0.options.journal.max_record_bytes {
            return Err(Error::Limit {
                name: "append batch bytes",
            });
        }
        // Validate first, then enqueue and expose the entries before the IO callback can fire.
        let (sizes, bytes) = prepare_entries(&state, &batch, &encoded, &self.0.options)?;
        self.maybe_checkpoint(&state, batch.len()).await?;
        let (ids, durability) = self.0.journal.append_retained_batch(encoded).await?;
        install_entries(&mut state, batch, sizes, ids, bytes);
        Ok(Some(durability))
    }

    #[tracing::instrument(name = "raft_commit_marker", level = "trace", skip_all, fields(node_id = self.0.identity.node_id, group_id = self.0.identity.group.group_id))]
    pub(crate) async fn persist_committed(&self, committed: Option<LogId<u64>>) -> Result<()> {
        let start = Instant::now();
        self.persist(Record::Committed(committed)).await?;
        if let Some(metrics) = &self.0.metrics {
            metrics.observe(Stage::CommitPersist, start.elapsed().as_secs_f64());
        }
        Ok(())
    }

    async fn persist(&self, record: Record<'static, A>) -> Result<()> {
        let mut state = self.0.log.clone().lock_owned().await;
        let store = self.clone();
        finish_mutation(tokio::spawn(async move {
            store.persist_locked(&mut state, record).await
        }))
        .await
    }

    async fn persist_locked(&self, state: &mut LogState<A>, record: Record<'_, A>) -> Result<()> {
        if matches!(&record, Record::Vote(vote) if Some(*vote) == state.vote)
            || matches!(&record, Record::Committed(committed) if *committed == state.committed)
        {
            return Ok(());
        }
        validate_record(&state, &record)?;
        self.maybe_checkpoint(state, 0).await?;
        let encoded = wal::encode_value(&record, self.0.options.journal.max_record_bytes)?;
        let durability = self.0.journal.append(encoded).await?;
        #[cfg(test)]
        if matches!(record, Record::Truncate(_)) {
            super::cancellation_tests::boundary(&self.0.directory, "truncate_queued").await;
        }
        durability.durable().await?;
        replay(state, record, None, &[], &self.0.options)
    }

    async fn maybe_checkpoint(&self, state: &LogState<A>, additional: usize) -> Result<()> {
        let options = &self.0.options.journal;
        let stats = self.0.journal.statistics();
        let trigger = options
            .max_file_bytes
            .saturating_sub(options.control_reserve_bytes)
            .saturating_sub(options.segment_target_bytes);
        if stats.current_file_bytes >= trigger
            || stats.retained_records.saturating_add(additional) > options.max_retained_records
        {
            self.checkpoint_locked(state, state.purged).await?;
        }
        Ok(())
    }

    async fn checkpoint_locked(
        &self,
        state: &LogState<A>,
        purged: Option<LogId<u64>>,
    ) -> Result<()> {
        let mut retained = Vec::new();
        let mut references = Vec::new();
        for (index, entry) in &state.entries {
            if purged.is_some_and(|purged| *index <= purged.index) {
                continue;
            }
            retained.push(entry.log_id);
            references.push(
                *state
                    .record_refs
                    .get(index)
                    .ok_or(invalid("retained log has no record location"))?,
            );
        }
        let checkpoint = ControlCheckpoint {
            format: FORMAT_VERSION,
            identity: self.0.identity.clone(),
            vote: state.vote,
            committed: state.committed,
            purged,
            retained,
        };
        let encoded = wal::encode_value(&checkpoint, self.0.options.journal.max_checkpoint_bytes)?;
        self.0.journal.checkpoint(encoded, references).await
    }
}

async fn finish_mutation(task: tokio::task::JoinHandle<Result<()>>) -> Result<()> {
    task.await.map_err(|error| Error::Writer {
        reason: format!("replica mutation task failed: {error}"),
    })?
}

pub(crate) async fn recover<A: Application>(
    records: &mut wal::Recovery,
    identity: &super::StorageIdentity,
    options: &StorageOptions,
) -> Result<LogState<A>> {
    let checkpoint: Option<ControlCheckpoint> = if records.checkpoint().is_empty() {
        None
    } else {
        Some(wal::decode_value(records.checkpoint())?)
    };
    let mut state = LogState::default();
    let mut retained = std::collections::VecDeque::new();
    let mut committed = None;
    if let Some(checkpoint) = checkpoint {
        if checkpoint.format != FORMAT_VERSION
            || checkpoint.identity != *identity
            || checkpoint.retained.len() > options.max_log_entries
        {
            return Err(invalid(
                "checkpoint identity, format, or retained count differs",
            ));
        }
        state.vote = checkpoint.vote;
        state.purged = checkpoint.purged;
        committed = checkpoint.committed;
        retained = checkpoint.retained.into();
    }
    if retained.is_empty() {
        state.committed = committed;
    }
    while let Some(recovered) = records.next().await? {
        let record: Record<A> = wal::decode_value(&recovered.data)?;
        if let Some(expected) = retained.pop_front() {
            if !matches!(&record, Record::Entry(entry) if entry.log_id == expected)
                || recovered.id.is_none()
            {
                return Err(invalid(
                    "checkpoint retained log identity differs from its location",
                ));
            }
            replay(&mut state, record, recovered.id, &recovered.data, options)?;
            if retained.is_empty() {
                state.committed = committed;
            }
        } else {
            if matches!(record, Record::Entry(_)) != recovered.id.is_some() {
                return Err(invalid("record retention class differs from its payload"));
            }
            replay(&mut state, record, recovered.id, &recovered.data, options)?;
        }
    }
    if !retained.is_empty() {
        return Err(invalid("checkpoint retained log records are missing"));
    }
    Ok(state)
}
