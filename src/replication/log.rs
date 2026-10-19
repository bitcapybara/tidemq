use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::time::Instant;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftLogReader, StorageError, Vote};
use std::borrow::Cow;

use crate::observability::metrics::Stage;
use crate::wal::{self, Error, Result};

use super::store::{LogState, Record, storage_error};
use super::{Application, DiskLog, LogEntry, StorageOptions, TypeConfig};

fn invalid(reason: &'static str) -> Error {
    Error::Configuration { reason }
}

pub(crate) fn replay<A: Application>(
    state: &mut LogState<A>,
    record: Record<'_, A>,
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
        Record::Entries(entries) => {
            let mut entries = entries.into_owned();
            entries.retain(|entry| {
                state
                    .purged
                    .is_none_or(|purged| entry.log_id.index > purged.index)
            });
            let (sizes, bytes) = prepare_entries(state, &entries, options)?;
            install_entries(state, entries, sizes, bytes);
        }
        Record::Truncate(log_id) => {
            state.entries.retain(|index, _| *index < log_id.index);
            state.entry_bytes.retain(|index, _| *index < log_id.index);
            state.retained_bytes = retained_bytes(state)?;
        }
        Record::Purge(log_id) => {
            state.entries.retain(|index, _| *index > log_id.index);
            state.entry_bytes.retain(|index, _| *index > log_id.index);
            state.retained_bytes = retained_bytes(state)?;
            state.purged = Some(log_id);
        }
        Record::Identity(_) => {
            return Err(invalid(
                "journal identity must appear only at the beginning",
            ));
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
        Record::Purge(log_id) if state.purged.is_some_and(|purged| *log_id < purged) => {
            Some("purged log boundary must not regress")
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

fn prepare_entries<A: Application>(
    state: &LogState<A>,
    entries: &[LogEntry<A>],
    options: &StorageOptions,
) -> Result<(Vec<usize>, usize)> {
    let mut previous = None;
    let mut sizes = Vec::new();
    let mut bytes = state.retained_bytes;
    let mut count = state.entries.len();
    for entry in entries {
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
        let encoded = wal::encode_value(entry, options.journal.max_record_bytes)?;
        if let Some(existing) = state.entries.get(&index) {
            if wal::encode_value(existing, options.journal.max_record_bytes)? != encoded {
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
    bytes: usize,
) {
    for (entry, size) in entries.into_iter().zip(sizes) {
        state.entry_bytes.insert(entry.log_id.index, size);
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
                    callback.log_io_completed(
                        durability.durable().await.map_err(std::io::Error::other),
                    );
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
        self.persist(Record::Truncate(log_id))
            .await
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Delete, error))
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let result = async {
            let mut state = self.0.log.lock().await;
            let mut trial = clone_log(&state);
            replay(&mut trial, Record::Purge(log_id), &self.0.options)?;
            let records = checkpoint(&trial, &self.0.options, &self.0.identity)?;
            self.0.journal.replace(records).await?;
            *state = trial;
            Ok(())
        }
        .await;
        result.map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Delete, error))
    }
}

impl<A: Application> DiskLog<A> {
    #[tracing::instrument(name = "raft_log_append", level = "trace", skip_all, fields(node_id = self.0.identity.node_id, group_id = self.0.identity.group.group_id))]
    async fn enqueue_entries<I: IntoIterator<Item = LogEntry<A>>>(
        &self,
        entries: I,
    ) -> Result<wal::Durability> {
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
        let encoded = wal::encode_value(
            &Record::<A>::Entries(Cow::Borrowed(&batch)),
            self.0.options.journal.max_record_bytes,
        )?;
        // Validate first, then enqueue and expose the entries before the IO callback can fire.
        let (sizes, bytes) = prepare_entries(&state, &batch, &self.0.options)?;
        let durability = self.0.journal.append(encoded).await?;
        install_entries(&mut state, batch, sizes, bytes);
        Ok(durability)
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

    async fn persist(&self, record: Record<'_, A>) -> Result<()> {
        let encoded = wal::encode_value(&record, self.0.options.journal.max_record_bytes)?;
        let mut state = self.0.log.lock().await;
        if matches!(&record, Record::Vote(vote) if Some(*vote) == state.vote)
            || matches!(&record, Record::Committed(committed) if *committed == state.committed)
        {
            return Ok(());
        }
        validate_record(&state, &record)?;
        self.0.journal.append(encoded).await?.durable().await?;
        replay(&mut state, record, &self.0.options)
    }
}

fn clone_log<A: Application>(state: &LogState<A>) -> LogState<A> {
    LogState {
        vote: state.vote,
        committed: state.committed,
        purged: state.purged,
        entries: state.entries.clone(),
        entry_bytes: state.entry_bytes.clone(),
        retained_bytes: state.retained_bytes,
    }
}

fn checkpoint<A: Application>(
    state: &LogState<A>,
    options: &StorageOptions,
    identity: &super::StorageIdentity,
) -> Result<Vec<Vec<u8>>> {
    let mut records = vec![wal::encode_value(
        &Record::<A>::Identity(identity.clone()),
        options.journal.max_record_bytes,
    )?];
    if let Some(vote) = state.vote {
        records.push(wal::encode_value(
            &Record::<A>::Vote(vote),
            options.journal.max_record_bytes,
        )?);
    }
    if let Some(purged) = state.purged {
        records.push(wal::encode_value(
            &Record::<A>::Purge(purged),
            options.journal.max_record_bytes,
        )?);
    }
    for entry in state.entries.values() {
        records.push(wal::encode_value(
            &Record::Entries(vec![entry.clone()].into()),
            options.journal.max_record_bytes,
        )?);
    }
    if let Some(committed) = state.committed {
        records.push(wal::encode_value(
            &Record::<A>::Committed(Some(committed)),
            options.journal.max_record_bytes,
        )?);
    }
    Ok(records)
}
