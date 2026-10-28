use super::*;
use crate::metadata::{
    AuthorityTransfer, MAX_AUTHORITY_TRANSFERS, MAX_TRANSFER_BYTES, TransferManifest, TransferStage,
};
use crate::ownership::CommitPosition;
use snafu::Snafu;
use std::io::Write;

const MAX_FROZEN_METADATA_BYTES: u64 = 16 * 1024 * 1024;
const TRANSFER_FORMAT: u32 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferCapacity {
    pub max_bytes: u64,
    pub max_hot_bytes: u64,
    pub max_hot_messages: usize,
    pub max_window_slots: usize,
    pub max_read_results: usize,
    // ACK head receipts are charged to members; this bounds the remaining result slots.
    pub max_ack_slots: usize,
    pub max_members: usize,
    pub max_in_flight_bytes: u64,
}

impl TransferCapacity {
    fn covers(self, other: Self) -> bool {
        self.max_bytes >= other.max_bytes
            && self.max_hot_bytes >= other.max_hot_bytes
            && self.max_hot_messages >= other.max_hot_messages
            && self.max_window_slots >= other.max_window_slots
            && self.max_read_results >= other.max_read_results
            && self.max_ack_slots >= other.max_ack_slots
            && self.max_members >= other.max_members
            && self.max_in_flight_bytes >= other.max_in_flight_bytes
    }

    fn validate(self) -> Result<(), TransferDataRejection> {
        if self.max_bytes <= 32
            || self.max_bytes > MAX_TRANSFER_BYTES
            || self.max_hot_bytes > MAX_TRANSFER_BYTES
            || self.max_hot_messages > 1_048_576
            || [
                self.max_window_slots,
                self.max_read_results,
                self.max_ack_slots,
                self.max_members,
            ]
            .into_iter()
            .any(|count| count > 1_048_576)
            || self.max_in_flight_bytes > MAX_TRANSFER_BYTES
        {
            return Err(TransferDataRejection::Capacity);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Snafu)]
pub enum TransferDataRejection {
    #[snafu(display("The shard is frozen for authority transfer"))]
    Frozen,
    #[snafu(display("The authority transfer identity or evidence is invalid"))]
    InvalidPlan,
    #[snafu(display("The authority transfer conflicts with its original operation"))]
    Conflict,
    #[snafu(display("The authority transfer has not reached the required durable stage"))]
    NotReady,
    #[snafu(display("The authority transfer exceeds its reserved capacity"))]
    Capacity,
    #[snafu(display("The authority transfer payload could not be encoded"))]
    Encoding,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AuthorityDataCommand {
    ReclaimSource {
        plan: Box<AuthorityTransfer>,
    },
    RecordRetiredCut {
        plan: Id,
        cut: CommitPosition,
    },
    Reserve {
        plan: Box<AuthorityTransfer>,
        capacity: TransferCapacity,
    },
    BeginImport {
        plan: Box<AuthorityTransfer>,
    },
    ImportPage(TransferPage),
    FinishImport {
        plan: Id,
    },
    Freeze {
        plan: Box<AuthorityTransfer>,
        capacity: TransferCapacity,
    },
    RecordCut {
        plan: Id,
        cut: CommitPosition,
    },
    ExpandReservation {
        plan: Id,
        capacity: TransferCapacity,
    },
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferPage {
    pub plan: Id,
    pub offset: u64,
    pub bytes: bytes::Bytes,
    pub digest: [u8; 32],
}

impl std::fmt::Debug for TransferPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransferPage")
            .field("plan", &self.plan)
            .field("offset", &self.offset)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransferOutcome {
    SourceReclaimed {
        plan: Id,
        partition: PartitionKey,
    },
    Reserved {
        plan: Id,
    },
    ImportProgress {
        plan: Id,
        received: u64,
    },
    Imported(RecoveredOwnership),
    Frozen {
        plan: Id,
        partition: PartitionKey,
        committed_end: u64,
    },
    Cut(CommitPosition),
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct AuthorityDataState {
    sources: BTreeMap<Id, FrozenSource>,
    targets: BTreeMap<Id, TargetImport>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct TargetImport {
    plan: AuthorityTransfer,
    initial_capacity: TransferCapacity,
    capacity: TransferCapacity,
    pages: BTreeMap<u64, ImportedPage>,
    received: u64,
    recovered: Option<RecoveredOwnership>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ImportedPage {
    bytes: Option<bytes::Bytes>,
    length: usize,
    digest: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
struct FrozenSource {
    plan: AuthorityTransfer,
    capacity: TransferCapacity,
    cut: Option<CommitPosition>,
    lifecycle: Option<crate::lifecycle::AuthoritySlice>,
    metadata_bytes: u64,
    committed_end: u64,
    phase: SourcePhase,
    manifest: Option<TransferManifest>,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
enum SourcePhase {
    Frozen,
    Reclaimed,
    Retired(CommitPosition),
}

impl FrozenSource {
    fn outcome(&self) -> TransferOutcome {
        TransferOutcome::Frozen {
            plan: self.plan.id,
            partition: self.plan.partition,
            committed_end: self.committed_end,
        }
    }
}

#[derive(Serialize)]
struct ExportView<'a> {
    format: u32,
    plan: &'a AuthorityTransfer,
    cut: CommitPosition,
    state: &'a PartitionState,
    lifecycle: &'a crate::lifecycle::AuthoritySlice,
}

// Clone one frozen scope so disk I/O never holds the group's application lock.
#[derive(Serialize)]
pub(crate) struct AuthorityExport {
    format: u32,
    plan: AuthorityTransfer,
    cut: CommitPosition,
    state: PartitionState,
    lifecycle: crate::lifecycle::AuthoritySlice,
}

impl AuthorityExport {
    pub(crate) fn write_to(&self, writer: impl Write) -> Result<(), TransferDataRejection> {
        postcard::to_io(self, writer)
            .map(|_| ())
            .map_err(|_| TransferDataRejection::Encoding)
    }

    pub(crate) fn manifest(&self, bytes: u64, digest: [u8; 32]) -> TransferManifest {
        TransferManifest {
            bytes,
            digest,
            source_cut: self.cut,
            committed_end: self.state.producer.committed_end(),
        }
    }
}

impl AuthorityDataState {
    pub(super) fn is_frozen(&self, partition: PartitionKey) -> bool {
        self.sources
            .values()
            .any(|source| source.plan.partition == partition && source.phase == SourcePhase::Frozen)
    }

    pub(super) fn has_source(&self, partition: PartitionKey) -> bool {
        self.sources
            .values()
            .any(|source| source.plan.partition == partition)
    }

    fn accepts_import(&self, plan: &AuthorityTransfer) -> bool {
        self.sources
            .values()
            .filter(|source| source.plan.partition == plan.partition)
            .all(|source| {
                matches!(source.phase, SourcePhase::Retired(_))
                    && source.plan.shard == plan.shard
                    && source.plan.target.epoch < plan.target.epoch
                    && source.plan.binding_generation < plan.binding_generation
            })
    }

    pub(super) fn reservation(&self) -> Option<&TransferCapacity> {
        self.targets
            .values()
            .find(|target| target.recovered.is_none())
            .map(|target| &target.capacity)
    }

    pub(super) fn is_reserved(&self, partition: PartitionKey) -> bool {
        self.targets
            .values()
            .any(|target| target.plan.partition == partition && target.recovered.is_none())
    }
}

#[path = "authority_import.rs"]
mod target;

#[path = "authority_retire.rs"]
mod retirement;

impl DataApplication {
    pub(super) fn authority_transfer(
        &mut self,
        command: AuthorityDataCommand,
    ) -> Result<DataResponse, DataRejection> {
        let outcome = match command {
            AuthorityDataCommand::ReclaimSource { plan } => self.reclaim_source(*plan),
            AuthorityDataCommand::RecordRetiredCut { plan, cut } => {
                self.record_retired_cut(plan, cut)
            }
            AuthorityDataCommand::Reserve { plan, capacity } => {
                self.reserve_target(*plan, capacity)
            }
            AuthorityDataCommand::BeginImport { plan } => self.begin_import(*plan),
            AuthorityDataCommand::ImportPage(page) => self.import_page(page),
            AuthorityDataCommand::FinishImport { plan } => self.finish_import(plan),
            AuthorityDataCommand::Freeze { plan, capacity } => self.freeze_source(*plan, capacity),
            AuthorityDataCommand::RecordCut { plan, cut } => self.record_source_cut(plan, cut),
            AuthorityDataCommand::ExpandReservation { plan, capacity } => {
                self.expand_reservation(plan, capacity)
            }
        }
        .map_err(DataRejection::Transfer)?;
        Ok(DataResponse::AuthorityTransfer(outcome))
    }

    fn freeze_source(
        &mut self,
        plan: AuthorityTransfer,
        capacity: TransferCapacity,
    ) -> Result<TransferOutcome, TransferDataRejection> {
        capacity.validate()?;
        plan.validate()
            .map_err(|_| TransferDataRejection::InvalidPlan)?;
        if let Some(original) = self.authority.sources.get(&plan.id) {
            if !same_plan(&original.plan, &plan) || original.capacity != capacity {
                return Err(TransferDataRejection::Conflict);
            }
            return Ok(original.outcome());
        }
        if self.authority.is_frozen(plan.partition) {
            return Err(TransferDataRejection::Conflict);
        }
        if self.authority.sources.len() >= MAX_AUTHORITY_TRANSFERS {
            return Err(TransferDataRejection::Capacity);
        }
        self.validate_freeze_source(&plan, capacity)?;
        let state = self
            .partitions
            .get(&plan.partition)
            .ok_or(TransferDataRejection::InvalidPlan)?;
        let (lifecycle, metadata_bytes) = self.freeze_metadata(plan.partition)?;
        let committed_end = state.producer.committed_end();
        let source = FrozenSource {
            plan,
            capacity,
            cut: None,
            lifecycle: Some(lifecycle),
            metadata_bytes,
            committed_end,
            phase: SourcePhase::Frozen,
            manifest: None,
        };
        // Leave room for the final source log position's maximum encoded width.
        encoded_size(
            &view(
                &source,
                state,
                CommitPosition::Raft {
                    term: 1,
                    leader_node: 1,
                    index: 1,
                },
            )?,
            capacity.max_bytes.saturating_sub(32),
        )?;
        let outcome = source.outcome();
        self.authority.sources.insert(source.plan.id, source);
        Ok(outcome)
    }

    fn validate_freeze_source(
        &self,
        plan: &AuthorityTransfer,
        capacity: TransferCapacity,
    ) -> Result<(), TransferDataRejection> {
        if plan.stage != TransferStage::Prepare
            || self.config.as_ref().is_none_or(|config| {
                config.cluster_id != plan.source.cluster_id
                    || config.group_id != plan.source.group_id
            })
            || self
                .ownership
                .as_ref()
                .and_then(|fences| fences.fence(plan.partition))
                .is_none_or(|fence| fence.grant != plan.source)
            || self.ownership.as_ref().is_none_or(|fences| {
                fences
                    .check_writer(plan.partition, plan.source.owner_node, plan.source.epoch)
                    .is_err()
            })
        {
            return Err(TransferDataRejection::InvalidPlan);
        }
        let state = self
            .partitions
            .get(&plan.partition)
            .ok_or(TransferDataRejection::InvalidPlan)?;
        if state.birth.is_some_and(|(shard, _)| shard != plan.shard)
            || state.birth.is_none() && state.producer.committed_end() != 0
            || state
                .groups
                .values()
                .any(|cursor| cursor.identity().shard != plan.shard)
        {
            return Err(TransferDataRejection::InvalidPlan);
        }
        let hot_state = state
            .producer
            .hot_bytes()
            .checked_add(state.producer.publish_state_bytes())
            .ok_or(TransferDataRejection::Capacity)?;
        if hot_state > capacity.max_hot_bytes
            || state.producer.diagnostic().hot_messages > capacity.max_hot_messages
            || !super::groups::transfer_usage(state)
                .map_err(|_| TransferDataRejection::InvalidPlan)?
                .fits(capacity)
        {
            return Err(TransferDataRejection::Capacity);
        }
        Ok(())
    }

    fn freeze_metadata(
        &self,
        partition: PartitionKey,
    ) -> Result<(crate::lifecycle::AuthoritySlice, u64), TransferDataRejection> {
        let used = self
            .authority
            .sources
            .values()
            .try_fold(0_u64, |bytes, source| {
                bytes
                    .checked_add(source.metadata_bytes)
                    .ok_or(TransferDataRejection::Capacity)
            })?;
        let available = MAX_FROZEN_METADATA_BYTES
            .checked_sub(used)
            .ok_or(TransferDataRejection::Capacity)?;
        let lifecycle = self
            .lifecycle
            .authority_slice(partition, available)
            .map_err(|reason| match reason {
                crate::lifecycle::Rejection::Capacity => TransferDataRejection::Capacity,
                crate::lifecycle::Rejection::RecoveryDependency => TransferDataRejection::NotReady,
                _ => TransferDataRejection::InvalidPlan,
            })?;
        let metadata_bytes = encoded_size(&lifecycle, available)?;
        Ok((lifecycle, metadata_bytes))
    }

    fn record_source_cut(
        &mut self,
        plan: Id,
        cut: CommitPosition,
    ) -> Result<TransferOutcome, TransferDataRejection> {
        let source = self
            .authority
            .sources
            .get_mut(&plan)
            .ok_or(TransferDataRejection::NotReady)?;
        if let Some(original) = source.cut {
            return Ok(TransferOutcome::Cut(original));
        }
        if cut.sequence() == 0
            || !cut.validate()
            || !cut
                .raft_leader()
                .is_some_and(|leader| source.plan.source_voters.contains(&leader))
        {
            return Err(TransferDataRejection::InvalidPlan);
        }
        source.cut = Some(cut);
        Ok(TransferOutcome::Cut(cut))
    }

    pub fn write_transfer_payload(
        &self,
        plan: Id,
        mut writer: impl Write,
    ) -> Result<(), TransferDataRejection> {
        let source = self
            .authority
            .sources
            .get(&plan)
            .ok_or(TransferDataRejection::NotReady)?;
        if source.phase != SourcePhase::Frozen {
            return Err(TransferDataRejection::NotReady);
        }
        let state = self
            .partitions
            .get(&source.plan.partition)
            .ok_or(TransferDataRejection::InvalidPlan)?;
        let cut = source.cut.ok_or(TransferDataRejection::NotReady)?;
        postcard::to_io(&view(source, state, cut)?, &mut writer)
            .map(|_| ())
            .map_err(|_| TransferDataRejection::Encoding)
    }

    pub(crate) fn transfer_export_size(&self, plan: Id) -> Result<u64, TransferDataRejection> {
        let (source, state, cut) = self.export_source(plan)?;
        encoded_size(&view(source, state, cut)?, source.capacity.max_bytes)
    }

    pub(crate) fn transfer_export(
        &self,
        plan: Id,
    ) -> Result<AuthorityExport, TransferDataRejection> {
        let (source, state, cut) = self.export_source(plan)?;
        Ok(AuthorityExport {
            format: TRANSFER_FORMAT,
            plan: source.plan.clone(),
            cut,
            state: state.clone(),
            lifecycle: source
                .lifecycle
                .clone()
                .ok_or(TransferDataRejection::NotReady)?,
        })
    }

    fn export_source(
        &self,
        plan: Id,
    ) -> Result<(&FrozenSource, &PartitionState, CommitPosition), TransferDataRejection> {
        let source = self
            .authority
            .sources
            .get(&plan)
            .ok_or(TransferDataRejection::NotReady)?;
        if source.phase != SourcePhase::Frozen {
            return Err(TransferDataRejection::NotReady);
        }
        let state = self
            .partitions
            .get(&source.plan.partition)
            .ok_or(TransferDataRejection::InvalidPlan)?;
        Ok((
            source,
            state,
            source.cut.ok_or(TransferDataRejection::NotReady)?,
        ))
    }

    pub(crate) fn transfer_source_capacity(&self, plan: Id) -> Option<TransferCapacity> {
        self.authority
            .sources
            .get(&plan)
            .map(|source| source.capacity)
    }

    pub(crate) fn transfer_import_progress(
        &self,
        plan: Id,
    ) -> Option<(u64, Option<RecoveredOwnership>)> {
        self.authority
            .targets
            .get(&plan)
            .map(|target| (target.received, target.recovered.clone()))
    }

    pub(super) fn validate_transfers(&self) -> Result<(), DataRejection> {
        self.validate_targets().map_err(DataRejection::Transfer)?;
        let mut metadata_bytes = 0_u64;
        let mut active = std::collections::BTreeSet::new();
        if self.authority.sources.len() > MAX_AUTHORITY_TRANSFERS {
            return Err(DataRejection::Transfer(TransferDataRejection::Capacity));
        }
        for (id, source) in &self.authority.sources {
            if *id != source.plan.id
                || source.phase == SourcePhase::Frozen && !active.insert(source.plan.partition)
            {
                return Err(DataRejection::Transfer(TransferDataRejection::Conflict));
            }
            self.validate_source_record(source)?;
            metadata_bytes = metadata_bytes
                .checked_add(source.metadata_bytes)
                .ok_or(DataRejection::Transfer(TransferDataRejection::Capacity))?;
        }
        if metadata_bytes > MAX_FROZEN_METADATA_BYTES {
            return Err(DataRejection::Transfer(TransferDataRejection::Capacity));
        }
        Ok(())
    }

    fn validate_frozen_source(
        &self,
        partition: PartitionKey,
        source: &FrozenSource,
    ) -> Result<(), DataRejection> {
        let state = self
            .partitions
            .get(&partition)
            .ok_or(DataRejection::Transfer(TransferDataRejection::InvalidPlan))?;
        let lifecycle = source
            .lifecycle
            .as_ref()
            .ok_or(DataRejection::Transfer(TransferDataRejection::InvalidPlan))?;
        self.validate_freeze_source(&source.plan, source.capacity)
            .map_err(DataRejection::Transfer)?;
        if partition != source.plan.partition
            || state.producer.committed_end() != source.committed_end
            || lifecycle.partition != partition
            || source.cut.is_some_and(|cut| {
                cut.sequence() == 0
                    || !cut.validate()
                    || !cut
                        .raft_leader()
                        .is_some_and(|leader| source.plan.source_voters.contains(&leader))
            })
        {
            return Err(DataRejection::Transfer(TransferDataRejection::InvalidPlan));
        }
        lifecycle.validate().map_err(DataRejection::Lifecycle)?;
        let actual =
            encoded_size(lifecycle, MAX_FROZEN_METADATA_BYTES).map_err(DataRejection::Transfer)?;
        if actual != source.metadata_bytes {
            return Err(DataRejection::Transfer(TransferDataRejection::InvalidPlan));
        }
        encoded_size(
            &view(
                source,
                state,
                source.cut.unwrap_or(CommitPosition::Raft {
                    term: 1,
                    leader_node: 1,
                    index: 1,
                }),
            )
            .map_err(DataRejection::Transfer)?,
            source.capacity.max_bytes,
        )
        .map_err(DataRejection::Transfer)?;
        Ok(())
    }
}

fn same_plan(left: &AuthorityTransfer, right: &AuthorityTransfer) -> bool {
    left.id == right.id
        && left.shard == right.shard
        && left.partition == right.partition
        && left.source == right.source
        && left.target == right.target
        && left.layout_revision == right.layout_revision
        && left.binding_generation == right.binding_generation
        && left.source_voters == right.source_voters
        && left.target_voters == right.target_voters
}

fn view<'a>(
    source: &'a FrozenSource,
    state: &'a PartitionState,
    cut: CommitPosition,
) -> Result<ExportView<'a>, TransferDataRejection> {
    Ok(ExportView {
        format: TRANSFER_FORMAT,
        plan: &source.plan,
        cut,
        state,
        lifecycle: source
            .lifecycle
            .as_ref()
            .ok_or(TransferDataRejection::NotReady)?,
    })
}

fn encoded_size(value: &impl Serialize, limit: u64) -> Result<u64, TransferDataRejection> {
    crate::budget::encoded_size(value, limit).map_err(|_| TransferDataRejection::Capacity)
}
