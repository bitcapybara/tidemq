use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use crate::metadata::PartitionKey;
use crate::ownership::{
    FenceCommand, FenceRecord, FenceRejection, FenceResponse, FenceState, OwnershipApplication,
    RecoveredOwnership, RecoveryCheckpoint,
};
use crate::producer::{
    self, AppendBatch, BatchResult, ProducerLimits, ProducerState, RegisterProducer, SessionInfo,
};
use crate::replication::Application;
use crate::subscription::{self, DataContext, SubscriptionState};

#[path = "authority_data.rs"]
mod authority_data;
#[path = "groups.rs"]
mod groups;
#[path = "publish.rs"]
mod publish;
pub use authority_data::{
    AuthorityDataCommand, TransferCapacity, TransferDataRejection, TransferOutcome,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataGroupConfig {
    pub cluster_id: Id,
    pub group_id: u64,
    pub max_partitions: usize,
    pub max_hot_bytes: u64,
    pub max_hot_messages: usize,
    pub producer: ProducerLimits,
    pub subscription: subscription::Limits,
    pub retention: crate::retention::Policy,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct CapacityUsage {
    pub logical_backlog_upper_bytes: u64,
    pub ack_state_logical_bytes: u64,
    pub hot_body_bytes: u64,
    pub unpublished_upper_bytes: u64,
    pub hot_messages: usize,
    pub time_boundaries: usize,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ArchiveCandidate {
    pub partition: PartitionKey,
    pub published_end: u64,
    pub committed_end: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArchivePage {
    pub partitions: Vec<ArchiveCandidate>,
    pub next: Option<PartitionKey>,
}

impl DataGroupConfig {
    pub fn validate(&self) -> Result<(), DataRejection> {
        self.producer.validate().map_err(DataRejection::Producer)?;
        self.subscription
            .validate()
            .map_err(DataRejection::Subscription)?;
        self.retention
            .validate()
            .map_err(DataRejection::Retention)?;
        if self.group_id == 0
            || self.max_partitions == 0
            || self.max_partitions > 8_192
            || self.max_hot_bytes == 0
            || self.max_hot_bytes > 1024 * 1024 * 1024
            || self.max_hot_messages == 0
            || self.max_hot_messages > 1_048_576
        {
            return Err(DataRejection::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WriterFence {
    pub node_id: u64,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ProducerCommand {
    Register(RegisterProducer),
    Append(AppendBatch),
    Retire {
        producer_id: Id,
        producer_epoch: u64,
    },
    AdvanceTime,
}

/// Private service handlers admit these commands; never deserialize this enum from a wire RPC.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataCommand {
    AuthorityTransfer(AuthorityDataCommand),
    Publish {
        partition: PartitionKey,
        writer: WriterFence,
        birth: (Id, tidemq_protocol::topic::HashRange),
        permit: producer::PublisherPermit,
        request: producer::PublishRequest,
        now_ms: u64,
    },
    RetirePublishes {
        partition: PartitionKey,
        writer: WriterFence,
        permit: producer::PublisherPermit,
        before: u64,
    },
    CleanupPublisher {
        partition: PartitionKey,
        writer: WriterFence,
        permit: producer::PublisherPermit,
        now_ms: u64,
    },
    Initialize(DataGroupConfig),
    SealShard {
        partition: PartitionKey,
        writer: WriterFence,
        plan: Id,
    },
    ConsumerGroup {
        partition: PartitionKey,
        writer: WriterFence,
        command: subscription::GroupCommand,
    },
    Retention {
        partition: PartitionKey,
        writer: WriterFence,
        command: crate::retention::Command,
    },
    InstallFence(FenceCommand),
    Recover {
        partition: PartitionKey,
        writer: WriterFence,
    },
    Producer {
        partition: PartitionKey,
        writer: WriterFence,
        now_ms: u64,
        command: ProducerCommand,
    },
    Lifecycle(crate::lifecycle::DataOperation),
    ColdSubscription {
        partition: PartitionKey,
        writer: WriterFence,
        proof: crate::lifecycle::VerifiedColdDelivery,
        command: subscription::Command,
    },
    Subscription {
        partition: PartitionKey,
        writer: WriterFence,
        command: subscription::Command,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataRejection {
    Transfer(TransferDataRejection),
    Uninitialized,
    InvalidConfiguration,
    ConfigurationConflict,
    PartitionCapacity,
    PartitionMissing,
    ShardSealed,
    Fence(FenceRejection),
    Producer(producer::Rejection),
    Subscription(subscription::Rejection),
    Retention(crate::retention::Rejection),
    Lifecycle(crate::lifecycle::Rejection),
}

impl fmt::Display for DataRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transfer(reason) => write!(formatter, "{reason}"),
            Self::Uninitialized => formatter.write_str("Data group has not been initialized"),
            Self::InvalidConfiguration => formatter.write_str(
                "Data group configuration violates its identity or capacity constraints",
            ),
            Self::ConfigurationConflict => {
                formatter.write_str("Data group was initialized with a different configuration")
            }
            Self::PartitionCapacity => {
                formatter.write_str("Data group partition capacity is exhausted")
            }
            Self::PartitionMissing => {
                formatter.write_str("Requested partition is not hosted by this data group")
            }
            Self::ShardSealed => {
                formatter.write_str("Birth shard has been permanently sealed for new writes")
            }
            Self::Fence(reason) => write!(formatter, "{reason}"),
            Self::Producer(reason) => write!(formatter, "{reason}"),
            Self::Subscription(reason) => write!(formatter, "{reason}"),
            Self::Retention(reason) => write!(formatter, "{reason}"),
            Self::Lifecycle(reason) => write!(formatter, "{reason}"),
        }
    }
}

#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataResponse {
    AuthorityTransfer(TransferOutcome),
    #[default]
    Noop,
    Initialized,
    ShardSealed {
        plan: Id,
        committed_end: u64,
    },
    Fenced(FenceRecord),
    Recovered(RecoveredOwnership),
    Registered(SessionInfo),
    Produced(BatchResult),
    Published(producer::PublishReceipt),
    PublishClosed,
    Retired,
    TimeAdvanced,
    Retention(crate::retention::Status),
    Subscription(subscription::Outcome),
    ConsumerGroup(subscription::GroupOutcome),
    Lifecycle(crate::lifecycle::DataResult),
    Rejected(DataRejection),
}

impl fmt::Debug for DataResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuthorityTransfer(outcome) => formatter
                .debug_tuple("AuthorityTransfer")
                .field(outcome)
                .finish(),
            Self::Published(receipt) => formatter.debug_tuple("Published").field(receipt).finish(),
            Self::PublishClosed => formatter.write_str("PublishClosed"),
            Self::Noop => formatter.write_str("Noop"),
            Self::Initialized => formatter.write_str("Initialized"),
            Self::ShardSealed {
                plan,
                committed_end,
            } => formatter
                .debug_struct("ShardSealed")
                .field("plan", plan)
                .field("committed_end", committed_end)
                .finish(),
            Self::Fenced(fence) => formatter
                .debug_tuple("Fenced")
                .field(&fence.grant.partition)
                .field(&fence.grant.epoch)
                .finish(),
            Self::Recovered(recovered) => formatter
                .debug_tuple("Recovered")
                .field(&recovered.fence.grant.partition)
                .field(&recovered.fence.fence_generation)
                .finish(),
            Self::Registered(session) => {
                formatter.debug_tuple("Registered").field(session).finish()
            }
            Self::Produced(result) => formatter.debug_tuple("Produced").field(result).finish(),
            Self::Retired => formatter.write_str("Retired"),
            Self::TimeAdvanced => formatter.write_str("TimeAdvanced"),
            Self::Retention(status) => formatter.debug_tuple("Retention").field(status).finish(),
            Self::Subscription(_) => formatter.write_str("Subscription(<bounded outcome>)"),
            Self::ConsumerGroup(result) => formatter
                .debug_tuple("ConsumerGroup")
                .field(result)
                .finish(),
            Self::Lifecycle(result) => formatter.debug_tuple("Lifecycle").field(result).finish(),
            Self::Rejected(reason) => formatter.debug_tuple("Rejected").field(reason).finish(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PartitionState {
    producer: ProducerState,
    subscriptions: SubscriptionState,
    retention: crate::retention::RetentionState,
    seal: Option<(Id, u64)>,
    birth: Option<(Id, tidemq_protocol::topic::HashRange)>,
    groups: BTreeMap<Id, subscription::GroupCursor>,
}

impl PartitionState {
    fn protected_before(&self) -> Option<u64> {
        self.subscriptions
            .protected_before()
            .into_iter()
            .chain(self.groups.values().map(|cursor| cursor.protected_before()))
            .min()
    }
}

/// The complete data-group authority: offsets, sessions, writer fences, delivery attempts and ACKs.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DataApplication {
    config: Option<DataGroupConfig>,
    ownership: Option<FenceState>,
    partitions: BTreeMap<PartitionKey, PartitionState>,
    hot_bytes: u64,
    hot_messages: usize,
    lifecycle: crate::lifecycle::DataState,
    authority: authority_data::AuthorityDataState,
}

impl fmt::Debug for DataApplication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DataApplication")
            .field("initialized", &self.config.is_some())
            .field("partitions", &self.partitions.len())
            .field("hot_bytes", &self.hot_bytes)
            .field("hot_messages", &self.hot_messages)
            .finish_non_exhaustive()
    }
}

impl DataApplication {
    pub fn shard_seal(&self, partition: PartitionKey) -> Option<(Id, u64)> {
        self.partitions.get(&partition).and_then(|state| state.seal)
    }

    fn seal_shard(
        &mut self,
        partition: PartitionKey,
        writer: WriterFence,
        plan: Id,
    ) -> Result<DataResponse, DataRejection> {
        self.check_writer(partition, writer)?;
        let state = self
            .partitions
            .get_mut(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        if let Some((previous, committed_end)) = state.seal {
            return if previous == plan {
                Ok(DataResponse::ShardSealed {
                    plan,
                    committed_end,
                })
            } else {
                Err(DataRejection::ShardSealed)
            };
        }
        let committed_end = state.producer.committed_end();
        state.seal = Some((plan, committed_end));
        Ok(DataResponse::ShardSealed {
            plan,
            committed_end,
        })
    }

    pub fn retention(&self, partition: PartitionKey) -> Option<&crate::retention::RetentionState> {
        self.partitions
            .get(&partition)
            .map(|state| &state.retention)
    }

    /// At most the configured partition count, with logarithmic time-index lookups and no body scan.
    pub fn capacity_usage(&self) -> Result<CapacityUsage, DataRejection> {
        self.config.as_ref().ok_or(DataRejection::Uninitialized)?;
        let mut usage = CapacityUsage {
            hot_body_bytes: self.hot_bytes,
            hot_messages: self.hot_messages,
            ..CapacityUsage::default()
        };
        for (partition, state) in &self.partitions {
            usage.ack_state_logical_bytes = usage
                .ack_state_logical_bytes
                .checked_add(
                    state
                        .subscriptions
                        .logical_state_bytes()
                        .map_err(DataRejection::Subscription)?,
                )
                .ok_or(DataRejection::InvalidConfiguration)?;
            for cursor in state.groups.values() {
                usage.ack_state_logical_bytes = usage
                    .ack_state_logical_bytes
                    .checked_add(cursor.logical_state_bytes())
                    .ok_or(DataRejection::InvalidConfiguration)?;
            }
            let backlog = state
                .retention
                .logical_backlog_upper_bytes(state.protected_before())
                .map_err(DataRejection::Retention)?;
            let published = self.lifecycle.published_end(*partition);
            let unpublished = if published < state.retention.floor() {
                state.producer.hot_bytes()
            } else {
                state
                    .retention
                    .logical_backlog_upper_bytes(Some(published))
                    .map_err(DataRejection::Retention)?
            };
            usage.logical_backlog_upper_bytes = usage
                .logical_backlog_upper_bytes
                .checked_add(backlog)
                .ok_or(DataRejection::InvalidConfiguration)?;
            usage.unpublished_upper_bytes = usage
                .unpublished_upper_bytes
                .checked_add(unpublished)
                .ok_or(DataRejection::InvalidConfiguration)?;
            usage.time_boundaries = usage
                .time_boundaries
                .checked_add(state.retention.status(0, None).time_boundaries)
                .ok_or(DataRejection::InvalidConfiguration)?;
        }
        Ok(usage)
    }

    pub fn archive_candidates(
        &self,
        after: Option<PartitionKey>,
        limit: usize,
    ) -> Result<ArchivePage, DataRejection> {
        use std::ops::Bound;
        if limit == 0 || limit > 64 {
            return Err(DataRejection::InvalidConfiguration);
        }
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut entries = self
            .partitions
            .range((start, Bound::Unbounded))
            .filter(|(partition, _)| !self.authority.is_frozen(**partition));
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(limit)
            .map_err(|_| DataRejection::Producer(producer::Rejection::Allocation))?;
        for (partition, state) in entries.by_ref().take(limit) {
            partitions.push(ArchiveCandidate {
                partition: *partition,
                published_end: self.lifecycle.published_end(*partition),
                committed_end: state.producer.committed_end(),
            });
        }
        let next = if entries.next().is_some() {
            partitions.last().map(|candidate| candidate.partition)
        } else {
            None
        };
        Ok(ArchivePage { partitions, next })
    }

    fn retention_command(
        &mut self,
        partition: PartitionKey,
        writer: WriterFence,
        command: crate::retention::Command,
    ) -> Result<DataResponse, DataRejection> {
        self.check_writer(partition, writer)?;
        let state = self
            .partitions
            .get_mut(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        let status = state
            .retention
            .advance(
                command,
                state.producer.committed_end(),
                state.protected_before(),
            )
            .map_err(DataRejection::Retention)?;
        state
            .producer
            .set_retained_floor(status.floor)
            .map_err(DataRejection::Producer)?;
        Ok(DataResponse::Retention(status))
    }

    pub fn lifecycle(&self) -> &crate::lifecycle::DataState {
        &self.lifecycle
    }
    pub fn hot_bytes(&self) -> u64 {
        self.hot_bytes
    }
    pub fn hot_messages(&self) -> usize {
        self.hot_messages
    }

    pub fn config(&self) -> Option<&DataGroupConfig> {
        self.config.as_ref()
    }

    pub fn ownership(&self) -> Option<&FenceState> {
        self.ownership.as_ref()
    }

    pub fn producer(&self, partition: PartitionKey) -> Option<&ProducerState> {
        self.partitions.get(&partition).map(|state| &state.producer)
    }
    pub fn sealed_end(&self, partition: PartitionKey) -> Option<u64> {
        self.partitions.get(&partition)?.seal.map(|(_, end)| end)
    }

    pub fn subscriptions(&self, partition: PartitionKey) -> Option<&SubscriptionState> {
        self.partitions
            .get(&partition)
            .map(|state| &state.subscriptions)
    }

    fn initialize(&mut self, config: DataGroupConfig) -> Result<DataResponse, DataRejection> {
        config.validate()?;
        if let Some(current) = &self.config {
            return if *current == config {
                Ok(DataResponse::Initialized)
            } else {
                Err(DataRejection::ConfigurationConflict)
            };
        }
        let fences = FenceState::new(config.cluster_id, config.group_id, config.max_partitions)
            .map_err(DataRejection::Fence)?;
        self.ownership = Some(fences);
        self.config = Some(config);
        Ok(DataResponse::Initialized)
    }

    fn install_fence(&mut self, command: FenceCommand) -> Result<DataResponse, DataRejection> {
        if self.authority.is_frozen(command.grant.partition) {
            return Err(DataRejection::Transfer(TransferDataRejection::Frozen));
        }
        let config = self.config.as_ref().ok_or(DataRejection::Uninitialized)?;
        let partition = command.grant.partition;
        let mut fences = self
            .ownership
            .as_ref()
            .ok_or(DataRejection::Uninitialized)?
            .clone();
        let fence = match fences.apply(command) {
            FenceResponse::Installed(fence) => fence,
            FenceResponse::Rejected(reason) => return Err(DataRejection::Fence(reason)),
        };
        if let Some(state) = self.partitions.get_mut(&partition) {
            state
                .subscriptions
                .invalidate_assignments(fence.fence_generation)
                .map_err(DataRejection::Subscription)?;
        } else {
            if self.partitions.len() >= config.max_partitions {
                return Err(DataRejection::PartitionCapacity);
            }
            let producer = ProducerState::new(
                partition.topic_id,
                partition.partition,
                config.producer.clone(),
            )
            .map_err(DataRejection::Producer)?;
            let mut subscriptions = SubscriptionState::new(
                partition.topic_id,
                partition.partition,
                config.subscription,
            )
            .map_err(DataRejection::Subscription)?;
            subscriptions
                .invalidate_assignments(fence.fence_generation)
                .map_err(DataRejection::Subscription)?;
            self.partitions.insert(
                partition,
                PartitionState {
                    producer,
                    subscriptions,
                    retention: crate::retention::RetentionState::new(config.retention)
                        .map_err(DataRejection::Retention)?,
                    seal: None,
                    birth: None,
                    groups: BTreeMap::new(),
                },
            );
        }
        self.ownership = Some(fences);
        Ok(DataResponse::Fenced(fence))
    }

    fn recover(
        &mut self,
        partition: PartitionKey,
        writer: WriterFence,
    ) -> Result<DataResponse, DataRejection> {
        if self.authority.is_frozen(partition) {
            return Err(DataRejection::Transfer(TransferDataRejection::Frozen));
        }
        let fences = self
            .ownership
            .as_ref()
            .ok_or(DataRejection::Uninitialized)?;
        let fence = fences
            .fence(partition)
            .ok_or(DataRejection::PartitionMissing)?;
        if fence.grant.owner_node != writer.node_id || fence.grant.epoch != writer.owner_epoch {
            return Err(DataRejection::Fence(FenceRejection::EpochConflict));
        }
        if let Some(recovered) = fences.recovered(partition) {
            return Ok(DataResponse::Recovered(recovered));
        }
        let state = self
            .partitions
            .get(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        state.producer.validate().map_err(DataRejection::Producer)?;
        state
            .subscriptions
            .validate()
            .map_err(DataRejection::Subscription)?;
        if state.subscriptions.owner_fence_generation() != fence.fence_generation {
            return Err(DataRejection::Fence(FenceRejection::RecoveryRequired));
        }
        let mut digest = blake3::Hasher::new();
        digest.update(b"tidemq-data-recovery-v1\0");
        postcard::to_io(&state.groups, &mut digest)
            .map_err(|_| DataRejection::InvalidConfiguration)?;
        postcard::to_io(&state.seal, &mut digest)
            .map_err(|_| DataRejection::InvalidConfiguration)?;
        digest.update(
            &self
                .lifecycle
                .recovery_digest()
                .map_err(DataRejection::Lifecycle)?,
        );
        state
            .retention
            .validate()
            .map_err(DataRejection::Retention)?;
        digest.update(&state.retention.recovery_digest());
        digest.update(
            &state
                .producer
                .recovery_digest()
                .map_err(DataRejection::Producer)?,
        );
        digest.update(
            &state
                .subscriptions
                .recovery_digest()
                .map_err(DataRejection::Subscription)?,
        );
        let checkpoint = RecoveryCheckpoint {
            fence_generation: fence.fence_generation,
            producer_state_revision: state.producer.revision(),
            ack_state_revision: state.subscriptions.revision(),
            committed_end: state.producer.committed_end(),
            state_digest: *digest.finalize().as_bytes(),
        };
        let recovered = self
            .ownership
            .as_mut()
            .ok_or(DataRejection::Uninitialized)?
            .complete_recovery(partition, checkpoint)
            .map_err(DataRejection::Fence)?;
        Ok(DataResponse::Recovered(recovered))
    }

    // Every producer transition passes the same committed writer fence before dispatch.
    fn producer_command(
        &mut self,
        partition: PartitionKey,
        writer: WriterFence,
        now_ms: u64,
        command: ProducerCommand,
    ) -> Result<DataResponse, DataRejection> {
        self.check_writer(partition, writer)?;
        let config = self.config.as_ref().ok_or(DataRejection::Uninitialized)?;
        let available_bytes = config.max_hot_bytes.saturating_sub(self.hot_bytes);
        let available_messages = config.max_hot_messages.saturating_sub(self.hot_messages);
        let state = self
            .partitions
            .get_mut(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        match command {
            ProducerCommand::Append(ref request) if state.seal.is_some() => state
                .producer
                .committed_result(now_ms, request)
                .map_err(DataRejection::Producer)?
                .map(DataResponse::Produced)
                .ok_or(DataRejection::ShardSealed),
            ProducerCommand::Register(_) if state.seal.is_some() => Err(DataRejection::ShardSealed),
            ProducerCommand::Register(request) => state
                .producer
                .register(now_ms, request)
                .map(DataResponse::Registered)
                .map_err(DataRejection::Producer),
            ProducerCommand::Retire {
                producer_id,
                producer_epoch,
            } => state
                .producer
                .retire(now_ms, producer_id, producer_epoch)
                .map(|()| DataResponse::Retired)
                .map_err(DataRejection::Producer),
            ProducerCommand::AdvanceTime => state
                .producer
                .advance_time(now_ms)
                .map(|()| DataResponse::TimeAdvanced)
                .map_err(DataRejection::Producer),
            ProducerCommand::Append(request) => {
                let logical_bytes = request
                    .messages
                    .iter()
                    .try_fold(0_u64, |sum, message| {
                        sum.checked_add(message.logical_bytes()?)
                    })
                    .ok_or(DataRejection::Producer(producer::Rejection::InvalidBatch))?;
                let before_bytes = state.producer.hot_bytes();
                let before_messages = state.producer.hot_message_count();
                let result = state
                    .producer
                    .append_admitted(
                        now_ms,
                        request,
                        available_bytes,
                        available_messages,
                        |previous, end| {
                            state
                                .retention
                                .record_append(previous, end, now_ms, logical_bytes)
                                .map_err(|error| match error {
                                    crate::retention::Rejection::Allocation => {
                                        producer::Rejection::Allocation
                                    }
                                    crate::retention::Rejection::TimestampBudget => {
                                        producer::Rejection::DataCapacity
                                    }
                                    crate::retention::Rejection::CounterOverflow => {
                                        producer::Rejection::CounterExhausted
                                    }
                                    _ => producer::Rejection::InvalidSnapshot,
                                })
                        },
                    )
                    .map_err(DataRejection::Producer)?;
                if !result.duplicate {
                    self.hot_bytes = self
                        .hot_bytes
                        .saturating_add(state.producer.hot_bytes().saturating_sub(before_bytes));
                    self.hot_messages = self.hot_messages.saturating_add(
                        state
                            .producer
                            .hot_message_count()
                            .saturating_sub(before_messages),
                    );
                }
                Ok(DataResponse::Produced(result))
            }
        }
    }

    fn subscription_command(
        &mut self,
        partition: PartitionKey,
        writer: WriterFence,
        command: subscription::Command,
    ) -> Result<DataResponse, DataRejection> {
        self.subscription_checked(partition, writer, command, None)
    }

    fn subscription_checked(
        &mut self,
        partition: PartitionKey,
        writer: WriterFence,
        command: subscription::Command,
        cold: Option<crate::lifecycle::VerifiedColdDelivery>,
    ) -> Result<DataResponse, DataRejection> {
        self.check_writer(partition, writer)?;
        if let Some(proof) = &cold {
            if proof.partition() != partition {
                return Err(DataRejection::Lifecycle(
                    crate::lifecycle::Rejection::Identity,
                ));
            }
            self.lifecycle
                .verify_delivery(proof)
                .map_err(DataRejection::Lifecycle)?;
        }
        let fences = self
            .ownership
            .as_ref()
            .ok_or(DataRejection::Uninitialized)?;
        let fence = fences
            .fence(partition)
            .ok_or(DataRejection::PartitionMissing)?;
        let state = self
            .partitions
            .get_mut(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        if state.subscriptions.owner_fence_generation() != fence.fence_generation {
            return Err(DataRejection::Fence(FenceRejection::RecoveryRequired));
        }
        let context = DataContext {
            committed_end: state.producer.committed_end(),
            retained_floor: state.producer.retained_floor(),
        };
        if let subscription::Command::RecordDelivery {
            offset, body_bytes, ..
        } = &command
        {
            let actual = if let Some(proof) = &cold {
                if proof.offset() != *offset {
                    return Err(DataRejection::Lifecycle(
                        crate::lifecycle::Rejection::InvalidRange,
                    ));
                }
                u64::from(proof.body_bytes())
            } else {
                state
                    .producer
                    .message_bytes_at(*offset)
                    .map_err(DataRejection::Producer)?
            };
            if actual != u64::from(*body_bytes) {
                return Err(DataRejection::Subscription(
                    subscription::Rejection::InvalidContext,
                ));
            }
        }
        match state.subscriptions.apply(context, command) {
            subscription::Response::Applied(outcome) => Ok(DataResponse::Subscription(outcome)),
            subscription::Response::Rejected(reason) => Err(DataRejection::Subscription(reason)),
            subscription::Response::Noop => Err(DataRejection::Subscription(
                subscription::Rejection::InvalidContext,
            )),
        }
    }

    fn check_writer(
        &self,
        partition: PartitionKey,
        writer: WriterFence,
    ) -> Result<(), DataRejection> {
        if self.authority.is_frozen(partition) {
            return Err(DataRejection::Transfer(TransferDataRejection::Frozen));
        }
        self.ownership
            .as_ref()
            .ok_or(DataRejection::Uninitialized)?
            .check_writer(partition, writer.node_id, writer.owner_epoch)
            .map_err(DataRejection::Fence)
    }

    // Full group validation belongs to recovery and snapshot input, outside normal admission.
    fn validate_complete(&self) -> Result<(), DataRejection> {
        self.validate_transfers()?;
        let Some(config) = &self.config else {
            return if self.ownership.is_none()
                && self.partitions.is_empty()
                && self.hot_bytes == 0
                && self.hot_messages == 0
            {
                Ok(())
            } else {
                Err(DataRejection::InvalidConfiguration)
            };
        };
        config.validate()?;
        self.lifecycle
            .validate()
            .map_err(DataRejection::Lifecycle)?;
        self.validate_groups()?;
        let fences = self
            .ownership
            .as_ref()
            .ok_or(DataRejection::Uninitialized)?;
        fences.validate_snapshot().map_err(DataRejection::Fence)?;
        if fences.cluster_id() != config.cluster_id
            || fences.group_id() != config.group_id
            || fences.partition_count() != self.partitions.len()
            || self.partitions.len() > config.max_partitions
        {
            return Err(DataRejection::InvalidConfiguration);
        }
        let mut bytes = 0_u64;
        let mut messages = 0_usize;
        for (partition, state) in &self.partitions {
            if let Some((birth, range)) = state.birth {
                range
                    .validate()
                    .map_err(|_| DataRejection::InvalidConfiguration)?;
                if state
                    .groups
                    .values()
                    .any(|cursor| cursor.identity().shard != birth)
                {
                    return Err(DataRejection::InvalidConfiguration);
                }
            }
            if state
                .seal
                .is_some_and(|(_, end)| end != state.producer.committed_end())
            {
                return Err(DataRejection::InvalidConfiguration);
            }
            let fence = fences
                .fence(*partition)
                .ok_or(DataRejection::PartitionMissing)?;
            state.producer.validate().map_err(DataRejection::Producer)?;
            state
                .retention
                .validate()
                .map_err(DataRejection::Retention)?;
            state
                .subscriptions
                .validate()
                .map_err(DataRejection::Subscription)?;
            if state.producer.topic_id() != partition.topic_id
                || state.producer.partition_id() != partition.partition
                || state.subscriptions.topic_id() != partition.topic_id
                || state.subscriptions.partition_id() != partition.partition
                || state.subscriptions.owner_fence_generation() != fence.fence_generation
                || state.producer.limits() != &config.producer
                || state.subscriptions.limits() != config.subscription
                || state.retention.policy() != config.retention
                || state.retention.committed_end() != state.producer.committed_end()
                || state.retention.floor() != state.producer.retained_floor()
            {
                return Err(DataRejection::InvalidConfiguration);
            }
            if self.lifecycle.published_end(*partition) > state.producer.committed_end()
                || self.lifecycle.local_reclaim_end(*partition)
                    != state.producer.local_reclaim_end()
                || self.lifecycle.checkpointed_floor(*partition) > state.producer.retained_floor()
            {
                return Err(DataRejection::Lifecycle(
                    crate::lifecycle::Rejection::InvalidSnapshot,
                ));
            }
            let diagnostic = state.producer.diagnostic();
            bytes = bytes
                .checked_add(diagnostic.hot_bytes)
                .ok_or(DataRejection::InvalidConfiguration)?;
            messages = messages
                .checked_add(diagnostic.hot_messages)
                .ok_or(DataRejection::InvalidConfiguration)?;
        }
        if bytes != self.hot_bytes
            || messages != self.hot_messages
            || bytes > config.max_hot_bytes
            || messages > config.max_hot_messages
        {
            return Err(DataRejection::InvalidConfiguration);
        }
        Ok(())
    }
}

impl Application for DataApplication {
    const NAME: &'static str = "tidemq-data";
    const VERSION: u32 = 8;
    type Command = DataCommand;
    type Response = DataResponse;

    fn apply(&mut self, command: DataCommand) -> DataResponse {
        let result = match command {
            DataCommand::AuthorityTransfer(command) => self.authority_transfer(command),
            DataCommand::Publish {
                partition,
                writer,
                birth,
                permit,
                request,
                now_ms,
            } => self.publish_message(partition, writer, birth, permit, request, now_ms),
            DataCommand::RetirePublishes {
                partition,
                writer,
                permit,
                before,
            } => self.retire_publishes(partition, writer, permit, before),
            DataCommand::CleanupPublisher {
                partition,
                writer,
                permit,
                now_ms,
            } => self.check_writer(partition, writer).and_then(|()| {
                self.partitions
                    .get_mut(&partition)
                    .ok_or(DataRejection::PartitionMissing)?
                    .producer
                    .cleanup_publisher(&permit, now_ms)
                    .map_err(DataRejection::Producer)?;
                Ok(DataResponse::Retired)
            }),
            DataCommand::Initialize(config) => self.initialize(config),
            DataCommand::ConsumerGroup {
                partition,
                writer,
                command,
            } => self.consumer_group(partition, writer, command),
            DataCommand::SealShard {
                partition,
                writer,
                plan,
            } => self.seal_shard(partition, writer, plan),
            DataCommand::Retention {
                partition,
                writer,
                command,
            } => self.retention_command(partition, writer, command),
            DataCommand::Lifecycle(operation) => self.lifecycle_command(operation),
            DataCommand::ColdSubscription {
                partition,
                writer,
                proof,
                command,
            } => self.cold_subscription(partition, writer, proof, command),
            DataCommand::InstallFence(command) => self.install_fence(command),
            DataCommand::Recover { partition, writer } => self.recover(partition, writer),
            DataCommand::Producer {
                partition,
                writer,
                now_ms,
                command,
            } => self.producer_command(partition, writer, now_ms, command),
            DataCommand::Subscription {
                partition,
                writer,
                command,
            } => self.subscription_command(partition, writer, command),
        };
        result.unwrap_or_else(DataResponse::Rejected)
    }

    fn try_apply(&mut self, command: DataCommand) -> std::io::Result<DataResponse> {
        let response = self.apply(command);
        match &response {
            DataResponse::Rejected(DataRejection::Producer(producer::Rejection::Allocation)) => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "Committed data application allocation failed",
                ))
            }
            DataResponse::Rejected(DataRejection::Producer(
                producer::Rejection::InvalidSnapshot,
            ))
            | DataResponse::Rejected(DataRejection::Subscription(
                subscription::Rejection::InvalidSnapshot,
            )) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Committed data application invariants failed",
            )),
            DataResponse::Rejected(DataRejection::Lifecycle(
                crate::lifecycle::Rejection::InvalidSnapshot,
            )) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Committed lifecycle application invariants failed",
            )),
            _ => Ok(response),
        }
    }

    fn validate(&self) -> std::io::Result<()> {
        self.validate_complete().map_err(|reason| {
            std::io::Error::other(format!("Invalid data application snapshot: {reason}"))
        })
    }
}

impl OwnershipApplication for DataApplication {
    fn ownership_state(&self) -> Option<&FenceState> {
        self.ownership.as_ref()
    }
}

impl DataApplication {
    fn lifecycle_command(
        &mut self,
        operation: crate::lifecycle::DataOperation,
    ) -> Result<DataResponse, DataRejection> {
        use crate::lifecycle::{DataCommand as LifecycleCommand, Rejection};
        let config = self.config.as_ref().ok_or(DataRejection::Uninitialized)?;
        if operation.grants.is_empty() || operation.grants.len() > 64 {
            return Err(DataRejection::Lifecycle(Rejection::Identity));
        }
        for grant in &operation.grants {
            if grant.cluster_id != config.cluster_id || grant.group_id != config.group_id {
                return Err(DataRejection::Lifecycle(Rejection::Identity));
            }
            self.check_writer(
                grant.partition,
                WriterFence {
                    node_id: grant.owner_node,
                    owner_epoch: grant.epoch,
                },
            )?;
        }
        if let LifecycleCommand::Authorize(plan) = &operation.command
            && self
                .lifecycle
                .intent(plan.descriptor.identity.object_id)
                .is_none()
        {
            for index in &plan.descriptor.partitions {
                let partition = PartitionKey {
                    topic_id: index.topic_id,
                    partition: index.partition_id,
                };
                let producer = self
                    .producer(partition)
                    .ok_or(DataRejection::PartitionMissing)?;
                if index.offsets.end > producer.committed_end()
                    || !plan.grants.iter().any(|grant| {
                        grant.partition == partition && operation.grants.contains(grant)
                    })
                {
                    return Err(DataRejection::Lifecycle(Rejection::InvalidRange));
                }
            }
        }
        let reclaim = match &operation.command {
            LifecycleCommand::Reclaim { partition, end, .. } => Some((*partition, *end)),
            _ => None,
        };
        let outcome = self
            .lifecycle
            .apply(operation.command)
            .map_err(DataRejection::Lifecycle)?;
        if let Some((partition, end)) = reclaim {
            let state = self
                .partitions
                .get_mut(&partition)
                .ok_or(DataRejection::PartitionMissing)?;
            let before_bytes = state.producer.hot_bytes();
            let before_messages = state.producer.hot_message_count();
            state
                .producer
                .reclaim_hot(end)
                .map_err(DataRejection::Producer)?;
            self.hot_bytes = self
                .hot_bytes
                .checked_sub(before_bytes.saturating_sub(state.producer.hot_bytes()))
                .ok_or(DataRejection::Lifecycle(Rejection::InvalidSnapshot))?;
            self.hot_messages = self
                .hot_messages
                .checked_sub(before_messages.saturating_sub(state.producer.hot_message_count()))
                .ok_or(DataRejection::Lifecycle(Rejection::InvalidSnapshot))?;
        }
        Ok(DataResponse::Lifecycle(crate::lifecycle::DataResult {
            outcome,
        }))
    }
}

impl DataApplication {
    fn cold_subscription(
        &mut self,
        partition: PartitionKey,
        writer: WriterFence,
        proof: crate::lifecycle::VerifiedColdDelivery,
        command: subscription::Command,
    ) -> Result<DataResponse, DataRejection> {
        if !matches!(command, subscription::Command::RecordDelivery { .. }) {
            return Err(DataRejection::Lifecycle(
                crate::lifecycle::Rejection::Conflict,
            ));
        }
        self.subscription_checked(partition, writer, command, Some(proof))
    }
}
