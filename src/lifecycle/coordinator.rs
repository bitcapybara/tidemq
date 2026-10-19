use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant},
};

use openraft::LogId;
use tidemq_protocol::Id;
use tidemq_transport::budget::MemoryBudget;
use tokio::sync::Semaphore;

use crate::{
    archive::{
        ArchivedRecord, ColdReader, CommittedPartition, Pack, PackIdentity, PackLimits, Record,
    },
    broker::{DataApplication, DataCommand as BrokerCommand, DataRejection, DataResponse},
    metadata::PartitionKey,
    object_store::S3Store,
    ownership::{ControlLease, ControlPolicy, Grant, Work},
    replication::{Application, DiskStateMachine, Raft},
};

use super::{
    Error, LifecycleMetrics, ObjectRecord, RangeRequest, ReadPin, Result, VerifiedColdDelivery,
    backend::{MetadataReplica, Query, View},
    catalog::key,
    types::{
        CatalogCommand, CatalogOutcome, DataCommand, DataOperation, DataOutcome, IntentPhase,
        MAX_PARTITIONS, MAX_PIN_TTL, ObjectPhase, PinRecord, Plan, Rejection,
    },
};

#[derive(Clone)]
pub struct DataReplica {
    pub(crate) raft: Raft<DataApplication>,
    pub(crate) state: DiskStateMachine<DataApplication>,
}

impl DataReplica {
    pub fn local(raft: Raft<DataApplication>, state: DiskStateMachine<DataApplication>) -> Self {
        Self { raft, state }
    }
    pub(crate) fn node_id(&self) -> u64 {
        self.state.0.identity.node_id
    }
}

#[derive(Clone)]
pub struct Coordinator(Arc<Inner>);
struct Inner {
    metadata: MetadataReplica,
    data: DataReplica,
    checkpoints: Vec<super::ReplicaCheckpoint>,
    store: S3Store,
    reader: ColdReader,
    budget: MemoryBudget,
    uploads: Arc<Semaphore>,
    controls: Arc<Semaphore>,
    metrics: Arc<LifecycleMetrics>,
    cluster_id: Id,
    data_group: u64,
}

/// Object identity and contents are derived from authoritative committed data.
pub struct PreparedObject {
    plan: Plan,
    pack: Pack,
}
impl PreparedObject {
    pub fn object_id(&self) -> Id {
        self.plan.descriptor.identity.object_id
    }
}

pub struct GuardedPage {
    coordinator: Coordinator,
    pin: Option<ReadPin>,
    records: Vec<ArchivedRecord>,
    partition: PartitionKey,
    manifest_generation: u64,
    object_id: Id,
    leases: Vec<ControlLease>,
}

impl GuardedPage {
    pub fn check(&self) -> Result<()> {
        self.pin.as_ref().ok_or(Error::Deadline)?.check()?;
        if self
            .leases
            .iter()
            .any(|lease| Instant::now() >= lease.expires_at())
        {
            return Err(Error::Deadline);
        }
        Ok(())
    }
    pub fn records(&self) -> Result<&[ArchivedRecord]> {
        self.check()?;
        Ok(&self.records)
    }
    pub fn verified_delivery(&self, offset: u64) -> Result<VerifiedColdDelivery> {
        self.check()?;
        let record = self
            .records
            .iter()
            .find(|record| record.offset == offset)
            .ok_or(Error::Rejected {
                reason: Rejection::InvalidRange,
            })?;
        let bytes = record
            .key()
            .len()
            .checked_add(record.body().len())
            .ok_or(Error::Configuration)?;
        Ok(VerifiedColdDelivery {
            partition: self.partition,
            object_id: self.object_id,
            manifest_generation: self.manifest_generation,
            offset,
            body_bytes: u32::try_from(bytes).map_err(|_| Error::Configuration)?,
        })
    }
    pub async fn release(mut self, deadline: Instant) -> Result<()> {
        if let Some(pin) = self.pin.take() {
            self.coordinator
                .meta_write(CatalogCommand::ReleasePin { pin: pin.record }, deadline)
                .await?;
        }
        Ok(())
    }
}

impl Coordinator {
    pub fn new(
        metadata: MetadataReplica,
        data: DataReplica,
        replicas: Vec<DataReplica>,
        store: S3Store,
        budget: MemoryBudget,
    ) -> Result<Self> {
        let cluster_id = data.state.0.identity.group.cluster_id;
        let data_group = data.state.0.identity.group.group_id;
        let metadata_group = metadata.group_id;
        let unique: BTreeSet<_> = replicas.iter().map(DataReplica::node_id).collect();
        if metadata.cluster_id != cluster_id
            || data_group == metadata_group
            || !matches!(replicas.len(), 1 | 3)
            || unique.len() != replicas.len()
            || replicas
                .iter()
                .any(|replica| replica.state.0.identity.group != data.state.0.identity.group)
        {
            return Err(Error::Configuration);
        }
        let reader = ColdReader::new(store.clone(), PackLimits::default(), 32 * 1024 * 1024, 64)
            .map_err(|source| Error::Archive { source })?;
        Ok(Self(Arc::new(Inner {
            metadata,
            data,
            checkpoints: replicas
                .into_iter()
                .map(super::ReplicaCheckpoint::local)
                .collect(),
            store,
            reader,
            budget,
            uploads: Arc::new(Semaphore::new(1)),
            controls: Arc::new(Semaphore::new(2)),
            metrics: Arc::new(LifecycleMetrics::default()),
            cluster_id,
            data_group,
        })))
    }

    pub fn with_checkpoints(mut self, checkpoints: Vec<super::ReplicaCheckpoint>) -> Result<Self> {
        let unique: BTreeSet<_> = checkpoints
            .iter()
            .map(super::ReplicaCheckpoint::node_id)
            .collect();
        if !matches!(checkpoints.len(), 1 | 3)
            || unique.len() != checkpoints.len()
            || checkpoints.iter().any(|checkpoint| {
                checkpoint.cluster_id != self.0.cluster_id
                    || checkpoint.group_id != self.0.data_group
            })
        {
            return Err(Error::Configuration);
        }
        Arc::get_mut(&mut self.0)
            .ok_or(Error::Configuration)?
            .checkpoints = checkpoints;
        Ok(self)
    }

    pub fn metrics(&self) -> Arc<LifecycleMetrics> {
        self.0.metrics.clone()
    }

    pub async fn diagnose(&self, object_id: Id, deadline: Instant) -> Result<super::Diagnostic> {
        match self
            .meta_query(Query::Diagnose(object_id), deadline)
            .await?
        {
            View::Diagnostic(diagnostic) => Ok(diagnostic),
            _ => Err(Error::Configuration),
        }
    }

    pub async fn prepare(
        &self,
        ranges: Vec<RangeRequest>,
        replaces: Vec<Id>,
        deadline: Instant,
    ) -> Result<PreparedObject> {
        if ranges.is_empty() || ranges.len() > MAX_PARTITIONS || replaces.len() > 64 {
            return Err(Error::Configuration);
        }
        let object_id = Id::random().map_err(|_| Error::Configuration)?;
        let (grants, expected_generations) = match self
            .meta_query(Query::Prepare(ranges.clone()), deadline)
            .await?
        {
            View::Prepared {
                grants,
                generations,
            } => (grants, generations),
            _ => return Err(Error::Configuration),
        };
        let pack = self.build_pack(object_id, &ranges, deadline).await?;
        let plan = Plan {
            descriptor: pack.descriptor().clone(),
            grants,
            expected_generations,
            replaces,
        };
        super::catalog::validate_plan(&plan).map_err(|reason| Error::Rejected { reason })?;
        Ok(PreparedObject { plan, pack })
    }

    /// Registration is committed before a data authorization can exist.
    pub async fn register(
        &self,
        prepared: &PreparedObject,
        deadline: Instant,
    ) -> Result<ObjectRecord> {
        let outcome = self
            .meta_write(CatalogCommand::Register(prepared.plan.clone()), deadline)
            .await?;
        self.0.metrics.registered.inc();
        object_result(outcome)
    }

    pub async fn authorize(&self, object_id: Id, deadline: Instant) -> Result<()> {
        let object = self.object(object_id, deadline).await?;
        if !matches!(
            object.phase,
            ObjectPhase::Registered | ObjectPhase::Uploaded
        ) {
            return Err(Error::Rejected {
                reason: Rejection::CancelRequested,
            });
        }
        self.data_write(DataCommand::Authorize(object.plan), Work::Publish, deadline)
            .await?;
        Ok(())
    }

    pub async fn upload(&self, prepared: &PreparedObject, deadline: Instant) -> Result<()> {
        let _slot = self
            .0
            .uploads
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let _timer = self.0.metrics.timer();
        let object_id = prepared.object_id();
        let object = self.object(object_id, deadline).await?;
        if object.plan != prepared.plan
            || !matches!(
                object.phase,
                ObjectPhase::Registered | ObjectPhase::Uploaded
            )
        {
            return Err(Error::Rejected {
                reason: Rejection::Conflict,
            });
        }
        self.authorized_intent(object_id, &object.plan, deadline)
            .await?;
        let leases = self.leases(&object.plan, Work::Publish, deadline).await?;
        let upload_deadline = deadline.min(earliest_expiry(&leases)?);
        tokio::time::timeout_at(
            upload_deadline.into(),
            self.0.store.put_create_only(&prepared.pack),
        )
        .await
        .map_err(|_| Error::Unknown)?
        .map_err(|source| Error::Object { source })?;
        self.verified_uploaded(object_id, deadline).await
    }

    async fn verified_uploaded(&self, object_id: Id, deadline: Instant) -> Result<()> {
        let object = self.object(object_id, deadline).await?;
        let leases = self.leases(&object.plan, Work::Publish, deadline).await?;
        tokio::time::timeout_at(
            deadline.min(earliest_expiry(&leases)?).into(),
            self.0.store.verify_uploaded(object.descriptor()),
        )
        .await
        .map_err(|_| Error::Unknown)?
        .map_err(|source| Error::Object { source })?;
        self.meta_write(CatalogCommand::Uploaded { object_id }, deadline)
            .await?;
        self.0.metrics.uploaded.inc();
        Ok(())
    }

    /// The metadata CAS is the readable publication boundary, distinct from upload and reclaim.
    pub async fn publish(&self, object_id: Id, deadline: Instant) -> Result<()> {
        self.publish_manifest(object_id, deadline).await?;
        self.complete(object_id, deadline).await
    }

    /// Kept separate so a recovering worker can finish a committed manifest after a crash.
    pub async fn publish_manifest(&self, object_id: Id, deadline: Instant) -> Result<()> {
        let object = self.object(object_id, deadline).await?;
        if object.phase == ObjectPhase::Published {
            return Ok(());
        }
        self.authorized_intent(object_id, &object.plan, deadline)
            .await?;
        let _leases = self.leases(&object.plan, Work::Publish, deadline).await?;
        self.meta_write(CatalogCommand::Publish { object_id }, deadline)
            .await?;
        self.0.metrics.published.inc();
        Ok(())
    }

    pub async fn complete(&self, object_id: Id, deadline: Instant) -> Result<()> {
        let object = self.object(object_id, deadline).await?;
        if object.phase != ObjectPhase::Published {
            return Err(Error::Rejected {
                reason: Rejection::NotPublished,
            });
        }
        let generations = object.published_generations.clone();
        self.data_write(
            DataCommand::Published {
                plan: object.plan,
                generations,
            },
            Work::Publish,
            deadline,
        )
        .await?;
        Ok(())
    }

    pub async fn cancel(&self, object_id: Id, deadline: Instant) -> Result<()> {
        match self
            .meta_write(CatalogCommand::RequestCancel { object_id }, deadline)
            .await
        {
            Err(Error::Rejected {
                reason: Rejection::AlreadyPublished,
            }) => return self.complete(object_id, deadline).await,
            result => {
                result?;
            }
        }
        let object = self.object(object_id, deadline).await?;
        if matches!(
            object.phase,
            ObjectPhase::Deleting | ObjectPhase::Deleted | ObjectPhase::Aborted
        ) {
            return Ok(());
        }
        let (outcome, _) = self
            .data_write(
                DataCommand::Abort(object.plan),
                Work::ControlMutation,
                deadline,
            )
            .await?;
        let DataOutcome::Aborted { generation } = outcome else {
            return Err(Error::Configuration);
        };
        self.meta_write(
            CatalogCommand::FinishCancel {
                object_id,
                data_generation: generation,
            },
            deadline,
        )
        .await?;
        self.0.metrics.canceled.inc();
        Ok(())
    }

    /// Reconcile every intermediate state using the original immutable object identity.
    pub async fn reconcile(&self, object_id: Id, deadline: Instant) -> Result<()> {
        self.0.metrics.retries.inc();
        let object = self.object(object_id, deadline).await?;
        match object.phase {
            ObjectPhase::Registered => {
                self.authorize(object_id, deadline).await?;
                match self.verified_uploaded(object_id, deadline).await {
                    Ok(()) => {}
                    Err(Error::Object {
                        source: crate::object_store::Error::Missing,
                    }) => {
                        let ranges = object
                            .plan
                            .descriptor
                            .partitions
                            .iter()
                            .map(|i| RangeRequest {
                                partition: key(i),
                                offsets: i.offsets,
                            })
                            .collect::<Vec<_>>();
                        let pack = self.build_pack(object_id, &ranges, deadline).await?;
                        if pack.descriptor() != object.descriptor() {
                            return Err(Error::Rejected {
                                reason: Rejection::Conflict,
                            });
                        }
                        self.upload(
                            &PreparedObject {
                                plan: object.plan,
                                pack,
                            },
                            deadline,
                        )
                        .await?;
                    }
                    Err(error) => return Err(error),
                }
                self.publish(object_id, deadline).await
            }
            ObjectPhase::Uploaded => self.publish(object_id, deadline).await,
            ObjectPhase::Published => self.complete(object_id, deadline).await,
            ObjectPhase::CancelRequested => self.cancel(object_id, deadline).await,
            ObjectPhase::Aborted | ObjectPhase::Deleting | ObjectPhase::Deleted => {
                self.collect(object_id, deadline).await
            }
        }
    }

    /// A terminal ledger remains after DELETE, so late orphan writes can only be collected again.
    pub async fn collect(&self, object_id: Id, deadline: Instant) -> Result<()> {
        let _slot = self
            .0
            .controls
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let _timer = self.0.metrics.timer();
        let object = object_result(
            self.meta_write(CatalogCommand::BeginDelete { object_id }, deadline)
                .await?,
        )?;
        let started = Instant::now();
        self.meta_query(Query::Health, deadline).await?;
        tokio::time::timeout_at(
            deadline.min(started + ControlPolicy::default().ttl).into(),
            self.0.store.delete(object.descriptor()),
        )
        .await
        .map_err(|_| Error::Unknown)?
        .map_err(|source| Error::Object { source })?;
        self.meta_write(CatalogCommand::Deleted { object_id }, deadline)
            .await?;
        self.0.metrics.deleted.inc();
        Ok(())
    }

    pub async fn checkpoint(&self, object_id: Id, deadline: Instant) -> Result<()> {
        let _slot = self
            .0
            .controls
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        self.complete(object_id, deadline).await?;
        let (outcome, position) = self
            .data_write(
                DataCommand::Checkpoint { object_id },
                Work::ControlMutation,
                deadline,
            )
            .await?;
        let DataOutcome::Checkpoint { generation } = outcome else {
            return Err(Error::Configuration);
        };
        let voters = self.expected_data_voters(deadline).await?;
        let configured: BTreeSet<_> = self
            .0
            .checkpoints
            .iter()
            .map(super::ReplicaCheckpoint::node_id)
            .collect();
        if voters != configured {
            return Err(Error::Rejected {
                reason: Rejection::Membership,
            });
        }
        let mut snapshots = Vec::new();
        for checkpoint in &self.0.checkpoints {
            let proof = checkpoint
                .build(object_id, generation, position, &voters, deadline)
                .await?;
            snapshots.push((proof.node_id, proof.snapshot_id, proof.position.index));
        }
        self.data_read(deadline, |_, _| Ok(())).await?;
        if self.expected_data_voters(deadline).await? != voters {
            return Err(Error::Rejected {
                reason: Rejection::Membership,
            });
        }
        self.meta_write(
            CatalogCommand::Checkpoint {
                object_id,
                generation,
                voters: voters.into_iter().collect(),
                snapshots,
            },
            deadline,
        )
        .await?;
        Ok(())
    }

    pub async fn reclaim(
        &self,
        partition: PartitionKey,
        end: u64,
        deadline: Instant,
    ) -> Result<()> {
        let generation = match self
            .meta_query(Query::Reclaim { partition, end }, deadline)
            .await?
        {
            View::Reclaim { generation, .. } => generation,
            _ => return Err(Error::Configuration),
        };
        let before = self
            .data_read(deadline, |state, _| Ok(state.hot_bytes()))
            .await?;
        self.data_write(
            DataCommand::Reclaim {
                partition,
                end,
                checkpoint_generation: generation,
            },
            Work::GarbageCollect,
            deadline,
        )
        .await?;
        let after = self
            .data_read(deadline, |state, _| Ok(state.hot_bytes()))
            .await?;
        self.0
            .metrics
            .reclaimed
            .inc_by(before.saturating_sub(after));
        Ok(())
    }

    /// One manifest segment per bounded page; no speculative cross-object reorder buffer.
    pub async fn read(
        &self,
        partition: PartitionKey,
        first: u64,
        max_messages: usize,
        max_bytes: u64,
        deadline: Instant,
    ) -> Result<GuardedPage> {
        if max_messages == 0 || max_messages > 1_024 || max_bytes == 0 || max_bytes > 1024 * 1024 {
            return Err(Error::Configuration);
        }
        let (object, generation) = match self
            .meta_query(Query::Read { partition, first }, deadline)
            .await?
        {
            View::Read { object, generation } => (*object, generation),
            _ => return Err(Error::Configuration),
        };
        let leases = self.leases(&object.plan, Work::Deliver, deadline).await?;
        let guard_deadline = deadline.min(earliest_expiry(&leases)?);
        let pin = self
            .acquire_pin(
                vec![object.object_id()],
                MAX_PIN_TTL.min(guard_deadline.saturating_duration_since(Instant::now())),
                deadline,
            )
            .await?;
        let records = tokio::time::timeout_at(
            pin.expires_at.min(guard_deadline).into(),
            self.0.reader.read_partition(
                object.descriptor(),
                partition.topic_id,
                partition.partition,
            ),
        )
        .await
        .map_err(|_| Error::Deadline)?
        .map_err(|source| Error::Archive { source })?;
        let mut selected = Vec::new();
        selected
            .try_reserve(max_messages.min(records.len()))
            .map_err(|source| Error::Allocation { source })?;
        let mut bytes = 0_u64;
        for record in records
            .into_iter()
            .filter(|record| record.offset >= first)
            .take(max_messages)
        {
            let size = (record.key().len() as u64)
                .checked_add(record.body().len() as u64)
                .ok_or(Error::Configuration)?;
            if bytes.checked_add(size).is_none_or(|next| next > max_bytes) {
                break;
            }
            bytes += size;
            selected.push(record);
        }
        if selected.is_empty() {
            return Err(Error::Rejected {
                reason: Rejection::Capacity,
            });
        }
        let page = GuardedPage {
            coordinator: self.clone(),
            pin: Some(pin),
            records: selected,
            partition,
            manifest_generation: generation,
            object_id: object.object_id(),
            leases,
        };
        page.check()?;
        Ok(page)
    }

    pub async fn acquire_pin(
        &self,
        objects: Vec<Id>,
        ttl: Duration,
        deadline: Instant,
    ) -> Result<ReadPin> {
        if ttl.is_zero() || ttl > MAX_PIN_TTL {
            return Err(Error::Configuration);
        }
        let started = Instant::now();
        let record = PinRecord {
            id: Id::random().map_err(|_| Error::Configuration)?,
            objects,
            ttl_millis: u64::try_from(ttl.as_millis()).map_err(|_| Error::Configuration)?,
        };
        self.meta_write(CatalogCommand::Pin(record.clone()), deadline)
            .await?;
        let pin = ReadPin {
            record,
            expires_at: started.checked_add(ttl).ok_or(Error::Configuration)?,
        };
        pin.check()?;
        Ok(pin)
    }

    pub async fn release_pin(&self, pin: ReadPin, deadline: Instant) -> Result<()> {
        self.meta_write(CatalogCommand::ReleasePin { pin: pin.record }, deadline)
            .await?;
        Ok(())
    }

    /// Observe a committed immutable pin, then wait its complete TTL on this process's monotonic clock.
    pub async fn expire_pin(&self, pin_id: Id, deadline: Instant) -> Result<()> {
        let pin = match self.meta_query(Query::Pin(pin_id), deadline).await? {
            View::Pin(pin) => pin,
            _ => return Err(Error::Configuration),
        };
        let Some(pin) = pin else {
            return Ok(());
        };
        let wait_until = Instant::now()
            .checked_add(Duration::from_millis(pin.ttl_millis))
            .ok_or(Error::Configuration)?;
        if wait_until >= deadline {
            return Err(Error::Deadline);
        }
        tokio::time::sleep_until(wait_until.into()).await;
        self.meta_write(CatalogCommand::ReleasePin { pin }, deadline)
            .await?;
        Ok(())
    }

    async fn object(&self, object_id: Id, deadline: Instant) -> Result<ObjectRecord> {
        match self.meta_query(Query::Object(object_id), deadline).await? {
            View::Object(object) => Ok(*object),
            _ => Err(Error::Configuration),
        }
    }

    async fn build_pack(
        &self,
        object_id: Id,
        ranges: &[RangeRequest],
        deadline: Instant,
    ) -> Result<Pack> {
        let mut owned = Vec::new();
        let mut total_records = 0_usize;
        let mut total_bytes = 0_u64;
        for range in ranges {
            let (committed_end, local) = self
                .data_read(deadline, |state, _| {
                    let producer = state.producer(range.partition).ok_or(Error::Rejected {
                        reason: Rejection::NotFound,
                    })?;
                    if range.offsets.start >= range.offsets.end
                        || range.offsets.end > producer.committed_end()
                    {
                        return Err(Error::Rejected {
                            reason: Rejection::InvalidRange,
                        });
                    }
                    Ok((producer.committed_end(), producer.local_reclaim_end()))
                })
                .await?;
            let mut records = Vec::new();
            let mut next = range.offsets.start;
            while next < range.offsets.end {
                check_deadline(deadline)?;
                if next < local {
                    let page = self
                        .read(range.partition, next, 1_024, 1024 * 1024, deadline)
                        .await?;
                    for record in page
                        .records()?
                        .iter()
                        .take_while(|record| record.offset < range.offsets.end)
                    {
                        records.push(Record {
                            offset: record.offset,
                            key: bytes::Bytes::copy_from_slice(record.key()),
                            body: bytes::Bytes::copy_from_slice(record.body()),
                        });
                        next = record.offset.checked_add(1).ok_or(Error::Configuration)?;
                    }
                    page.release(deadline).await?;
                } else {
                    let page = self
                        .data_read(deadline, |state, _| {
                            state
                                .producer(range.partition)
                                .ok_or(Error::Configuration)?
                                .hot_read(next, 1_024, 1024 * 1024)
                                .map_err(|_| Error::Rejected {
                                    reason: Rejection::InvalidRange,
                                })
                        })
                        .await?;
                    if page.messages.is_empty() {
                        return Err(Error::Rejected {
                            reason: Rejection::InvalidRange,
                        });
                    }
                    for message in page.messages {
                        if next >= range.offsets.end {
                            break;
                        }
                        records.push(Record {
                            offset: next,
                            key: message.key,
                            body: message.body,
                        });
                        next = next.checked_add(1).ok_or(Error::Configuration)?;
                    }
                }
                if records.len() > 100_000 {
                    return Err(Error::Rejected {
                        reason: Rejection::Capacity,
                    });
                }
            }
            total_records = total_records
                .checked_add(records.len())
                .ok_or(Error::Configuration)?;
            for record in &records {
                total_bytes = total_bytes
                    .checked_add(record.key.len() as u64)
                    .and_then(|n| n.checked_add(record.body.len() as u64))
                    .ok_or(Error::Configuration)?;
            }
            if total_records > 100_000 || total_bytes > 64 * 1024 * 1024 {
                return Err(Error::Rejected {
                    reason: Rejection::Capacity,
                });
            }
            owned.push((range.clone(), committed_end, records));
        }
        let identity = PackIdentity {
            cluster_id: self.0.cluster_id,
            group_id: self.0.data_group,
            object_id,
        };
        let budget = self.0.budget.clone();
        tokio::task::spawn_blocking(move || {
            let parts = owned
                .iter()
                .map(|(range, committed_end, records)| CommittedPartition {
                    group_id: identity.group_id,
                    topic_id: range.partition.topic_id,
                    partition_id: range.partition.partition,
                    offsets: range.offsets,
                    committed_end: *committed_end,
                    records,
                })
                .collect::<Vec<_>>();
            Pack::build(identity, &parts, PackLimits::default(), &budget)
        })
        .await
        .map_err(|source| Error::Worker { source })?
        .map_err(|source| Error::Archive { source })
    }

    async fn authorized_intent(&self, object_id: Id, plan: &Plan, deadline: Instant) -> Result<()> {
        self.data_read(deadline, |state, _| {
            let intent = state.lifecycle().intent(object_id).ok_or(Error::Rejected {
                reason: Rejection::NotFound,
            })?;
            if intent.plan != *plan || intent.phase != IntentPhase::Authorized {
                return Err(Error::Rejected {
                    reason: Rejection::Aborted,
                });
            }
            Ok(())
        })
        .await
    }

    async fn leases(
        &self,
        plan: &Plan,
        work: Work,
        deadline: Instant,
    ) -> Result<Vec<ControlLease>> {
        let fences = self
            .data_read(deadline, |state, _| {
                plan.descriptor
                    .partitions
                    .iter()
                    .map(|index| {
                        state
                            .ownership()
                            .and_then(|ownership| ownership.fence(key(index)))
                            .cloned()
                            .ok_or(Error::Rejected {
                                reason: Rejection::Identity,
                            })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .await?;
        let mut leases = Vec::new();
        for fence in fences {
            let lease = self
                .0
                .metadata
                .backend
                .lease(fence.clone(), deadline)
                .await?;
            lease
                .check(&fence, work)
                .map_err(|source| Error::Lease { source })?;
            leases.push(lease);
        }
        Ok(leases)
    }

    async fn meta_query(&self, query: Query, deadline: Instant) -> Result<View> {
        let view = self
            .0
            .metadata
            .backend
            .query(self.0.data_group, query, deadline)
            .await?;
        if view.cluster_id != self.0.cluster_id || view.group_id != self.0.metadata.group_id {
            return Err(Error::Configuration);
        }
        Ok(view.view)
    }

    async fn expected_data_voters(&self, deadline: Instant) -> Result<BTreeSet<u64>> {
        match self.meta_query(Query::DataVoters, deadline).await? {
            View::DataVoters(group) if group.group_id == self.0.data_group => Ok(group.voters),
            _ => Err(Error::Configuration),
        }
    }

    async fn data_read<R: Send>(
        &self,
        deadline: Instant,
        query: impl FnOnce(&DataApplication, Option<LogId<u64>>) -> Result<R> + Send,
    ) -> Result<R> {
        let expected = self.expected_data_voters(deadline).await?;
        tokio::time::timeout_at(deadline.into(), async {
            self.0
                .data
                .raft
                .ensure_linearizable()
                .await
                .map_err(consensus)?;
            if stable_voters(&self.0.data.raft)? != expected {
                return Err(Error::Rejected {
                    reason: Rejection::Membership,
                });
            }
            self.0
                .data
                .state
                .try_with_applied(|state, position| {
                    if state.config().is_none_or(|config| {
                        config.cluster_id != self.0.cluster_id
                            || config.group_id != self.0.data_group
                    }) {
                        return Err(Error::Configuration);
                    }
                    query(state, position)
                })
                .await
                .map_err(|source| Error::Storage { source })?
        })
        .await
        .map_err(|_| Error::Deadline)?
    }

    async fn meta_write(
        &self,
        command: CatalogCommand,
        deadline: Instant,
    ) -> Result<CatalogOutcome> {
        self.0.metadata.backend.execute(command, deadline).await
    }

    async fn data_write(
        &self,
        command: DataCommand,
        work: Work,
        deadline: Instant,
    ) -> Result<(DataOutcome, LogId<u64>)> {
        let plan = match &command {
            DataCommand::Authorize(plan)
            | DataCommand::Abort(plan)
            | DataCommand::Published { plan, .. } => plan.clone(),
            DataCommand::Checkpoint { object_id } => self.object(*object_id, deadline).await?.plan,
            DataCommand::Reclaim { partition, end, .. } => {
                match self
                    .meta_query(
                        Query::Reclaim {
                            partition: *partition,
                            end: *end,
                        },
                        deadline,
                    )
                    .await?
                {
                    View::Reclaim { plan, .. } => *plan,
                    _ => return Err(Error::Configuration),
                }
            }
        };
        let leases = self.leases(&plan, work, deadline).await?;
        let until = deadline.min(earliest_expiry(&leases)?);
        let grants: Vec<Grant> = leases.iter().map(|lease| lease.grant().clone()).collect();
        let result = tokio::time::timeout_at(
            until.into(),
            self.0
                .data
                .raft
                .client_write(BrokerCommand::Lifecycle(DataOperation { command, grants })),
        )
        .await
        .map_err(|_| Error::Unknown)?
        .map_err(consensus)?;
        match result.data {
            DataResponse::Lifecycle(result_data) => Ok((result_data.outcome, result.log_id)),
            DataResponse::Rejected(DataRejection::Lifecycle(reason)) => {
                Err(Error::Rejected { reason })
            }
            _ => Err(Error::Rejected {
                reason: Rejection::Conflict,
            }),
        }
    }
}

fn object_result(outcome: CatalogOutcome) -> Result<ObjectRecord> {
    if let CatalogOutcome::Object(object) = outcome {
        return Ok(*object);
    }
    Err(Error::Configuration)
}
fn consensus(error: impl std::fmt::Display) -> Error {
    Error::Consensus {
        reason: error.to_string(),
    }
}
fn earliest_expiry(leases: &[ControlLease]) -> Result<Instant> {
    leases
        .iter()
        .map(ControlLease::expires_at)
        .min()
        .ok_or(Error::Configuration)
}
fn check_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(Error::Deadline)
    } else {
        Ok(())
    }
}
pub(crate) fn stable_voters<A: Application>(raft: &Raft<A>) -> Result<BTreeSet<u64>> {
    let receiver = raft.metrics();
    let metrics = receiver.borrow();
    let membership = metrics.membership_config.membership();
    let voters: BTreeSet<_> = membership.voter_ids().take(4).collect();
    if membership.get_joint_config().len() != 1
        || !matches!(voters.len(), 1 | 3)
        || voters.contains(&0)
    {
        return Err(Error::Rejected {
            reason: Rejection::Membership,
        });
    }
    Ok(voters)
}
