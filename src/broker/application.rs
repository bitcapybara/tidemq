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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataGroupConfig {
    pub cluster_id: Id,
    pub group_id: u64,
    pub max_partitions: usize,
    pub max_hot_bytes: u64,
    pub max_hot_messages: usize,
    pub producer: ProducerLimits,
    pub subscription: subscription::Limits,
}

impl DataGroupConfig {
    pub fn validate(&self) -> Result<(), DataRejection> {
        self.producer.validate().map_err(DataRejection::Producer)?;
        self.subscription
            .validate()
            .map_err(DataRejection::Subscription)?;
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
    Initialize(DataGroupConfig),
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
    Subscription {
        partition: PartitionKey,
        writer: WriterFence,
        command: subscription::Command,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataRejection {
    Uninitialized,
    InvalidConfiguration,
    ConfigurationConflict,
    PartitionCapacity,
    PartitionMissing,
    Fence(FenceRejection),
    Producer(producer::Rejection),
    Subscription(subscription::Rejection),
}

#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataResponse {
    #[default]
    Noop,
    Initialized,
    Fenced(FenceRecord),
    Recovered(RecoveredOwnership),
    Registered(SessionInfo),
    Produced(BatchResult),
    Retired,
    TimeAdvanced,
    Subscription(subscription::Outcome),
    Rejected(DataRejection),
}

impl fmt::Debug for DataResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Noop => formatter.write_str("Noop"),
            Self::Initialized => formatter.write_str("Initialized"),
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
            Self::Subscription(_) => formatter.write_str("Subscription(<bounded outcome>)"),
            Self::Rejected(reason) => formatter.debug_tuple("Rejected").field(reason).finish(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PartitionState {
    producer: ProducerState,
    subscriptions: SubscriptionState,
}

/// The complete data-group authority: offsets, sessions, writer fences, delivery attempts and ACKs.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DataApplication {
    config: Option<DataGroupConfig>,
    ownership: Option<FenceState>,
    partitions: BTreeMap<PartitionKey, PartitionState>,
    hot_bytes: u64,
    hot_messages: usize,
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
    pub fn config(&self) -> Option<&DataGroupConfig> {
        self.config.as_ref()
    }

    pub fn ownership(&self) -> Option<&FenceState> {
        self.ownership.as_ref()
    }

    pub fn producer(&self, partition: PartitionKey) -> Option<&ProducerState> {
        self.partitions.get(&partition).map(|state| &state.producer)
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
                let before_bytes = state.producer.hot_bytes();
                let before_messages = state.producer.hot_message_count();
                let result = state
                    .producer
                    .append_with_capacity(now_ms, request, available_bytes, available_messages)
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
        self.check_writer(partition, writer)?;
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
            let actual = state
                .producer
                .message_bytes_at(*offset)
                .map_err(DataRejection::Producer)?;
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
        self.ownership
            .as_ref()
            .ok_or(DataRejection::Uninitialized)?
            .check_writer(partition, writer.node_id, writer.owner_epoch)
            .map_err(DataRejection::Fence)
    }

    // Full group validation belongs to recovery and snapshot input, outside normal admission.
    fn validate_complete(&self) -> Result<(), DataRejection> {
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
            let fence = fences
                .fence(*partition)
                .ok_or(DataRejection::PartitionMissing)?;
            state.producer.validate().map_err(DataRejection::Producer)?;
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
            {
                return Err(DataRejection::InvalidConfiguration);
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
    const VERSION: u32 = 1;
    type Command = DataCommand;
    type Response = DataResponse;

    fn apply(&mut self, command: DataCommand) -> DataResponse {
        let result = match command {
            DataCommand::Initialize(config) => self.initialize(config),
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
            _ => Ok(response),
        }
    }

    fn validate(&self) -> std::io::Result<()> {
        self.validate_complete().map_err(|reason| {
            std::io::Error::other(format!("Invalid data application snapshot: {reason:?}"))
        })
    }
}

impl OwnershipApplication for DataApplication {
    fn ownership_state(&self) -> Option<&FenceState> {
        self.ownership.as_ref()
    }
}
