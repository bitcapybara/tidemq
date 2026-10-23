use super::*;
use crate::subscription::{Candidate, GroupCommand, GroupCursor, GroupOutcome, GroupRead};

#[derive(Default)]
struct Usage {
    windows: usize,
    reads: usize,
    acks: usize,
    members: usize,
    bytes: u64,
}

impl DataApplication {
    pub(super) fn validate_groups(&self) -> Result<(), DataRejection> {
        let limits = self
            .config
            .as_ref()
            .ok_or(DataRejection::Uninitialized)?
            .subscription;
        let usage = self.group_usage()?;
        if usage.windows > limits.max_total_window_slots as usize
            || usage.reads > limits.max_total_window_slots as usize
            || usage.acks > (limits.max_total_window_slots as usize).saturating_add(usage.members)
            || usage.members > limits.max_total_window_slots as usize
            || usage.bytes > limits.max_total_in_flight_bytes
        {
            return Err(DataRejection::InvalidConfiguration);
        }
        for (partition, state) in &self.partitions {
            if state.groups.len() > limits.max_subscription_identities as usize {
                return Err(DataRejection::InvalidConfiguration);
            }
            for (subscription, cursor) in &state.groups {
                let identity = cursor.identity();
                if identity.topic != partition.topic_id
                    || identity.subscription != *subscription
                    || cursor.limits() != limits
                    || cursor.next_offset() > state.producer.committed_end()
                    || cursor.protected_before() < state.producer.retained_floor()
                {
                    return Err(DataRejection::InvalidConfiguration);
                }
                cursor.validate().map_err(DataRejection::Subscription)?;
            }
        }
        Ok(())
    }

    pub fn consumer_group_cursor(
        &self,
        partition: PartitionKey,
        subscription: Id,
    ) -> Option<&GroupCursor> {
        self.partitions.get(&partition)?.groups.get(&subscription)
    }

    pub(super) fn consumer_group(
        &mut self,
        partition: PartitionKey,
        writer: WriterFence,
        command: GroupCommand,
    ) -> Result<DataResponse, DataRejection> {
        self.check_writer(partition, writer)?;
        let limits = self
            .config
            .as_ref()
            .ok_or(DataRejection::Uninitialized)?
            .subscription;
        let usage = self.group_usage()?;
        let subscription = command.subscription();
        let state = self
            .partitions
            .get_mut(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        if let GroupCommand::Create {
            identity,
            model,
            start,
            signing_key,
            blocked,
        } = command
        {
            if identity.topic != partition.topic_id {
                return Err(DataRejection::Subscription(
                    subscription::Rejection::InvalidContext,
                ));
            }
            if let Some(cursor) = state.groups.get(&subscription) {
                return if cursor.model() == model {
                    Ok(DataResponse::ConsumerGroup(GroupOutcome::Created))
                } else {
                    Err(DataRejection::Subscription(
                        subscription::Rejection::ModelMismatch,
                    ))
                };
            }
            if state.groups.len() >= limits.max_subscription_identities as usize
                || start < state.producer.retained_floor()
                || start > state.producer.committed_end()
            {
                return Err(DataRejection::Subscription(
                    subscription::Rejection::InvalidContext,
                ));
            }
            let mut cursor = GroupCursor::new(identity, model, start, signing_key.into(), limits)
                .map_err(DataRejection::Subscription)?;
            if blocked {
                cursor.block_gate();
            }
            state.groups.insert(subscription, cursor);
            return Ok(DataResponse::ConsumerGroup(GroupOutcome::Created));
        }
        let cursor = state
            .groups
            .get_mut(&subscription)
            .ok_or(DataRejection::Subscription(
                subscription::Rejection::NotFound,
            ))?;
        let outcome = execute_group(cursor, &state.producer, limits, usage, command)
            .map_err(DataRejection::Subscription)?;
        Ok(DataResponse::ConsumerGroup(outcome))
    }

    fn group_usage(&self) -> Result<Usage, DataRejection> {
        self.partitions
            .values()
            .flat_map(|state| state.groups.values())
            .try_fold(Usage::default(), |mut usage, cursor| {
                usage.windows = usage
                    .windows
                    .checked_add(cursor.window_slots())
                    .ok_or(DataRejection::InvalidConfiguration)?;
                usage.reads = usage
                    .reads
                    .checked_add(cursor.read_result_slots())
                    .ok_or(DataRejection::InvalidConfiguration)?;
                usage.acks = usage
                    .acks
                    .checked_add(cursor.ack_result_slots())
                    .ok_or(DataRejection::InvalidConfiguration)?;
                usage.members = usage
                    .members
                    .checked_add(cursor.member_count())
                    .ok_or(DataRejection::InvalidConfiguration)?;
                usage.bytes = usage
                    .bytes
                    .checked_add(cursor.in_flight_bytes())
                    .ok_or(DataRejection::InvalidConfiguration)?;
                Ok(usage)
            })
    }
}

fn execute_group(
    cursor: &mut GroupCursor,
    producer: &ProducerState,
    limits: subscription::Limits,
    usage: Usage,
    command: GroupCommand,
) -> subscription::Result<GroupOutcome> {
    use subscription::Rejection;
    match command {
        GroupCommand::InstallMembers {
            version,
            grants,
            now_ms,
            ..
        } => {
            let additional = grants
                .iter()
                .filter(|grant| !cursor.contains_member(grant.member, grant.epoch))
                .count();
            if usage
                .members
                .checked_add(additional)
                .is_none_or(|count| count > limits.max_total_window_slots as usize)
            {
                return Err(Rejection::IdentityBudget);
            }
            cursor.install_members(version, &grants, now_ms)?;
            Ok(GroupOutcome::MembersInstalled)
        }
        GroupCommand::Read { request, .. } => {
            read_group(cursor, producer, limits, usage, request).map(GroupOutcome::Read)
        }
        GroupCommand::RetireRead {
            member,
            epoch,
            sequence,
            ..
        } => {
            cursor.retire_read(member, epoch, sequence)?;
            Ok(GroupOutcome::ReadRetired)
        }
        GroupCommand::Acknowledge { request, .. } => {
            if usage.acks >= limits.max_total_window_slots as usize
                && !cursor.ack_known(&request)
                && !cursor.ack_is_head(&request)
            {
                return Err(Rejection::WindowFull);
            }
            cursor.acknowledge(request).map(GroupOutcome::Acknowledged)
        }
        GroupCommand::RetireAck {
            member,
            epoch,
            sequence,
            ..
        } => {
            if usage.acks >= limits.max_total_window_slots as usize
                && cursor.ack_retirement_requires_slot(member, epoch, sequence)
            {
                return Err(Rejection::WindowFull);
            }
            cursor.retire_ack(member, epoch, sequence)?;
            Ok(GroupOutcome::AckRetired)
        }
        GroupCommand::Renew {
            member,
            epoch,
            expires_ms,
            now_ms,
            ..
        } => cursor
            .renew(member, epoch, expires_ms, now_ms)
            .map(GroupOutcome::Renewed),
        GroupCommand::Reap {
            now_ms,
            recovery_ms,
            ..
        } => cursor
            .reap_members(now_ms, recovery_ms)
            .map(GroupOutcome::Reaped),
        GroupCommand::Retry { offset, now_ms, .. } => {
            cursor.retry_message(offset, now_ms)?;
            Ok(GroupOutcome::RetryScheduled)
        }
        GroupCommand::Create { .. } => Err(Rejection::IdentityConflict),
        GroupCommand::OpenGate { .. } => {
            cursor.open_gate();
            Ok(GroupOutcome::GateOpened)
        }
    }
}

fn read_group(
    cursor: &mut GroupCursor,
    producer: &ProducerState,
    limits: subscription::Limits,
    usage: Usage,
    request: GroupRead,
) -> subscription::Result<subscription::ReadResult> {
    let available_windows = (limits.max_total_window_slots as usize).saturating_sub(usage.windows);
    let count = (limits.max_window_slots as usize)
        .saturating_sub(cursor.window_slots())
        .min(available_windows)
        .min(producer.limits().max_batch_messages);
    let mut candidates = Vec::new();
    if count > 0 {
        let page = producer
            .hot_read(
                cursor.next_offset(),
                count,
                producer.limits().max_batch_bytes,
            )
            .map_err(|_| subscription::Rejection::InvalidContext)?;
        for (index, message) in page.messages.iter().enumerate() {
            let offset = page
                .first_offset
                .checked_add(
                    u64::try_from(index).map_err(|_| subscription::Rejection::CounterOverflow)?,
                )
                .ok_or(subscription::Rejection::CounterOverflow)?;
            let metadata = message
                .metadata(producer.topic_id())
                .map_err(|_| subscription::Rejection::InvalidContext)?;
            let bytes = tidemq_protocol::topic::delivery_bytes(
                &message.key,
                &message.body,
                &metadata.properties,
            )
            .map_err(|_| subscription::Rejection::CounterOverflow)?;
            candidates.push(Candidate {
                offset,
                hash: metadata.routing_hash,
                bytes,
            });
        }
    }
    cursor.read_admitted(
        request,
        &candidates,
        limits.max_total_in_flight_bytes.saturating_sub(usage.bytes),
        u32::try_from((limits.max_total_window_slots as usize).saturating_sub(usage.reads))
            .map_err(|_| subscription::Rejection::WindowFull)?,
    )
}
