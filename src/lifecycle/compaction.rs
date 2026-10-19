use super::*;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactionStep {
    pub next_after: Option<Id>,
    pub replacement: Option<Id>,
}

impl Coordinator {
    /// Inspect one bounded page and replace at most one object with checkpointed dead ranges.
    pub async fn compact_next(
        &self,
        after: Option<Id>,
        deadline: Instant,
    ) -> Result<CompactionStep> {
        let page = self.pending_objects(after, 8, deadline).await?;
        for (source_id, phase) in page.objects {
            if phase != ObjectPhase::Published {
                continue;
            }
            let ranges = match self
                .meta_query(Query::Compaction(source_id), deadline)
                .await?
            {
                View::Compaction(Some(ranges)) => ranges,
                View::Compaction(None) => continue,
                _ => return Err(Error::Configuration),
            };
            let source = self.object(source_id, deadline).await?;
            self.verify_compaction_floors(&source, &ranges, deadline)
                .await?;
            let prepared = self.prepare(ranges, vec![source_id], deadline).await?;
            let replacement = prepared.object_id();
            self.register(&prepared, deadline).await?;
            self.authorize(replacement, deadline).await?;
            self.upload(&prepared, deadline).await?;
            self.publish_or_cancel_stale(replacement, deadline).await?;
            if self.object(replacement, deadline).await?.phase == ObjectPhase::Published {
                self.checkpoint(replacement, deadline).await?;
                self.0.metrics.compacted.inc();
                tracing::debug!(event = "object_compaction_checkpointed", source_object_id = %source_id, replacement_object_id = %replacement, "Object compaction reached every current voter checkpoint");
            }
            return Ok(CompactionStep {
                next_after: Some(source_id),
                replacement: Some(replacement),
            });
        }
        Ok(CompactionStep {
            next_after: page.next_after,
            replacement: None,
        })
    }

    async fn verify_compaction_floors(
        &self,
        source: &ObjectRecord,
        ranges: &[RangeRequest],
        deadline: Instant,
    ) -> Result<()> {
        self.data_read(deadline, |state, _| {
            for range in ranges {
                let producer = state
                    .producer(range.partition)
                    .ok_or(Error::Configuration)?;
                let original = source
                    .descriptor()
                    .partitions
                    .iter()
                    .find(|index| key(index) == range.partition)
                    .ok_or(Error::Configuration)?;
                if (range.offsets.start > original.offsets.start
                    && (range.offsets.start > producer.retained_floor()
                        || range.offsets.start
                            > state.lifecycle().checkpointed_floor(range.partition)))
                    || range.offsets.end > producer.committed_end()
                {
                    return Err(Error::Rejected {
                        reason: Rejection::CheckpointRequired,
                    });
                }
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn publish_or_cancel_stale(
        &self,
        object_id: Id,
        deadline: Instant,
    ) -> Result<()> {
        match self.publish(object_id, deadline).await {
            Err(
                error @ Error::Rejected {
                    reason: Rejection::StaleGeneration | Rejection::InvalidRange,
                },
            ) => {
                self.cancel_stale_replacement(object_id, error, deadline)
                    .await
            }
            result => result,
        }
    }

    pub(super) async fn cancel_stale_replacement(
        &self,
        object_id: Id,
        error: Error,
        deadline: Instant,
    ) -> Result<()> {
        let object = self.object(object_id, deadline).await?;
        if object.plan.replaces.is_empty() || object.phase == ObjectPhase::Published {
            return Err(error);
        }
        self.cancel(object_id, deadline).await
    }

    pub(super) async fn copy_replacement_range(
        &self,
        sources: &[ObjectRecord],
        range: &RangeRequest,
        deadline: Instant,
    ) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        let mut next = range.offsets.start;
        while next < range.offsets.end {
            let source = sources
                .iter()
                .find(|source| {
                    source.descriptor().partitions.iter().any(|index| {
                        key(index) == range.partition
                            && index.offsets.start <= next
                            && next < index.offsets.end
                    })
                })
                .ok_or(Error::Rejected {
                    reason: Rejection::InvalidRange,
                })?;
            let copied = self
                .copy_source_partition(source, range.partition, next, range.offsets.end, deadline)
                .await?;
            if copied.is_empty() {
                return Err(Error::Rejected {
                    reason: Rejection::InvalidRange,
                });
            }
            next = copied
                .last()
                .ok_or(Error::Configuration)?
                .offset
                .checked_add(1)
                .ok_or(Error::Configuration)?;
            records.extend(copied);
        }
        Ok(records)
    }

    async fn copy_source_partition(
        &self,
        source: &ObjectRecord,
        partition: PartitionKey,
        first: u64,
        end: u64,
        deadline: Instant,
    ) -> Result<Vec<Record>> {
        let leases = self.leases(&source.plan, Work::Publish, deadline).await?;
        let until = deadline.min(earliest_expiry(&leases)?);
        let pin = self
            .acquire_pin(
                vec![source.object_id()],
                MAX_PIN_TTL.min(until.saturating_duration_since(Instant::now())),
                deadline,
            )
            .await?;
        let result = async {
            let archived = tokio::time::timeout_at(
                pin.expires_at.min(until).into(),
                self.0.reader.read_partition(
                    source.descriptor(),
                    partition.topic_id,
                    partition.partition,
                ),
            )
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|source| Error::Archive { source })?;
            let records = copy_records(&archived, first, end)?;
            pin.check()?;
            check_deadline(until)?;
            Ok(records)
        }
        .await;
        let released = self.release_pin(pin, deadline).await;
        let records = result?;
        released?;
        Ok(records)
    }
}

fn copy_records(archived: &[ArchivedRecord], first: u64, end: u64) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    records
        .try_reserve(archived.len())
        .map_err(|source| Error::Allocation { source })?;
    let mut next = first;
    for record in archived
        .iter()
        .filter(|record| first <= record.offset && record.offset < end)
    {
        if record.offset != next {
            return Err(Error::Rejected {
                reason: Rejection::InvalidRange,
            });
        }
        records.push(Record {
            offset: record.offset,
            key: copied(record.key())?,
            body: copied(record.body())?,
        });
        next = next.checked_add(1).ok_or(Error::Configuration)?;
    }
    Ok(records)
}
