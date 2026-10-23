use super::*;
use crate::metadata::{AuthorityTransfer, MAX_TRANSFER_BYTES, TransferStage};
use crate::ownership::CommitPosition;
use snafu::Snafu;
use std::io::Write;

const MAX_FROZEN_METADATA_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferCapacity {
    pub max_bytes: u64,
    pub max_hot_bytes: u64,
    pub max_hot_messages: usize,
}

impl TransferCapacity {
    fn validate(self) -> Result<(), TransferDataRejection> {
        if self.max_bytes <= 32
            || self.max_bytes > MAX_TRANSFER_BYTES
            || self.max_hot_bytes > MAX_TRANSFER_BYTES
            || self.max_hot_messages > 1_048_576
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
    Freeze {
        plan: Box<AuthorityTransfer>,
        capacity: TransferCapacity,
    },
    RecordCut {
        plan: Id,
        cut: CommitPosition,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransferOutcome {
    Frozen {
        plan: Id,
        partition: PartitionKey,
        committed_end: u64,
    },
    Cut(CommitPosition),
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct AuthorityDataState {
    sources: BTreeMap<PartitionKey, FrozenSource>,
}

#[derive(Clone, Serialize, Deserialize)]
struct FrozenSource {
    plan: AuthorityTransfer,
    capacity: TransferCapacity,
    cut: Option<CommitPosition>,
    lifecycle: crate::lifecycle::AuthoritySlice,
    metadata_bytes: u64,
    committed_end: u64,
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

impl AuthorityDataState {
    pub(super) fn is_frozen(&self, partition: PartitionKey) -> bool {
        self.sources.contains_key(&partition)
    }
}

impl DataApplication {
    pub(super) fn authority_transfer(
        &mut self,
        command: AuthorityDataCommand,
    ) -> Result<DataResponse, DataRejection> {
        let outcome = match command {
            AuthorityDataCommand::Freeze { plan, capacity } => self.freeze_source(*plan, capacity),
            AuthorityDataCommand::RecordCut { plan, cut } => self.record_source_cut(plan, cut),
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
        if let Some(original) = self.authority.sources.get(&plan.partition) {
            if !same_plan(&original.plan, &plan) {
                return Err(TransferDataRejection::Conflict);
            }
            return Ok(original.outcome());
        }
        if self
            .authority
            .sources
            .values()
            .any(|source| source.plan.id == plan.id)
        {
            return Err(TransferDataRejection::Conflict);
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
            lifecycle,
            metadata_bytes,
            committed_end,
        };
        // Leave room for the final source log position's maximum encoded width.
        encoded_size(
            &view(
                &source,
                state,
                CommitPosition {
                    term: 1,
                    leader_node: 1,
                    index: 1,
                },
            ),
            capacity.max_bytes.saturating_sub(32),
        )?;
        let outcome = source.outcome();
        self.authority.sources.insert(source.plan.partition, source);
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
        if state.producer.hot_bytes() > capacity.max_hot_bytes
            || state.producer.diagnostic().hot_messages > capacity.max_hot_messages
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
            .values_mut()
            .find(|source| source.plan.id == plan)
            .ok_or(TransferDataRejection::NotReady)?;
        if let Some(original) = source.cut {
            return Ok(TransferOutcome::Cut(original));
        }
        if cut.index == 0 || cut.term == 0 || !source.plan.source_voters.contains(&cut.leader_node)
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
            .values()
            .find(|source| source.plan.id == plan)
            .ok_or(TransferDataRejection::NotReady)?;
        let state = self
            .partitions
            .get(&source.plan.partition)
            .ok_or(TransferDataRejection::InvalidPlan)?;
        let cut = source.cut.ok_or(TransferDataRejection::NotReady)?;
        postcard::to_io(&view(source, state, cut), &mut writer)
            .map(|_| ())
            .map_err(|_| TransferDataRejection::Encoding)
    }

    pub(super) fn validate_transfers(&self) -> Result<(), DataRejection> {
        let mut metadata_bytes = 0_u64;
        let mut identities = std::collections::BTreeSet::new();
        for (partition, source) in &self.authority.sources {
            if !identities.insert(source.plan.id) {
                return Err(DataRejection::Transfer(TransferDataRejection::Conflict));
            }
            self.validate_frozen_source(*partition, source)?;
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
        source
            .plan
            .validate()
            .map_err(|_| DataRejection::Transfer(TransferDataRejection::InvalidPlan))?;
        source
            .capacity
            .validate()
            .map_err(DataRejection::Transfer)?;
        self.validate_freeze_source(&source.plan, source.capacity)
            .map_err(DataRejection::Transfer)?;
        if partition != source.plan.partition
            || state.producer.committed_end() != source.committed_end
            || source.lifecycle.partition != partition
            || source.cut.is_some_and(|cut| {
                cut.index == 0
                    || cut.term == 0
                    || !source.plan.source_voters.contains(&cut.leader_node)
            })
        {
            return Err(DataRejection::Transfer(TransferDataRejection::InvalidPlan));
        }
        source
            .lifecycle
            .validate()
            .map_err(DataRejection::Lifecycle)?;
        let actual = encoded_size(&source.lifecycle, MAX_FROZEN_METADATA_BYTES)
            .map_err(DataRejection::Transfer)?;
        if actual != source.metadata_bytes {
            return Err(DataRejection::Transfer(TransferDataRejection::InvalidPlan));
        }
        encoded_size(
            &view(
                source,
                state,
                source.cut.unwrap_or(CommitPosition {
                    term: 1,
                    leader_node: 1,
                    index: 1,
                }),
            ),
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
) -> ExportView<'a> {
    ExportView {
        format: 1,
        plan: &source.plan,
        cut,
        state,
        lifecycle: &source.lifecycle,
    }
}

fn encoded_size(value: &impl Serialize, limit: u64) -> Result<u64, TransferDataRejection> {
    crate::budget::encoded_size(value, limit).map_err(|_| TransferDataRejection::Capacity)
}
