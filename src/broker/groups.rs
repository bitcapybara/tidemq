use super::*;
use crate::subscription::{
    Candidate, GroupCommand, GroupCursor, GroupOutcome, GroupRead, ReadBudget,
};

#[derive(Default)]
pub(super) struct Usage {
    windows: usize,
    reads: usize,
    acks: usize,
    members: usize,
    bytes: u64,
}

impl Usage {
    fn add(&mut self, other: Self) -> Result<(), DataRejection> {
        self.windows = self
            .windows
            .checked_add(other.windows)
            .ok_or(DataRejection::InvalidConfiguration)?;
        self.reads = self
            .reads
            .checked_add(other.reads)
            .ok_or(DataRejection::InvalidConfiguration)?;
        self.acks = self
            .acks
            .checked_add(other.acks)
            .ok_or(DataRejection::InvalidConfiguration)?;
        self.members = self
            .members
            .checked_add(other.members)
            .ok_or(DataRejection::InvalidConfiguration)?;
        self.bytes = self
            .bytes
            .checked_add(other.bytes)
            .ok_or(DataRejection::InvalidConfiguration)?;
        Ok(())
    }

    fn remove_reservation(&mut self, capacity: TransferCapacity) -> Result<(), DataRejection> {
        self.windows = self
            .windows
            .checked_sub(capacity.max_window_slots)
            .ok_or(DataRejection::InvalidConfiguration)?;
        self.reads = self
            .reads
            .checked_sub(capacity.max_read_results)
            .ok_or(DataRejection::InvalidConfiguration)?;
        self.acks = self
            .acks
            .checked_sub(capacity.max_ack_slots.saturating_add(capacity.max_members))
            .ok_or(DataRejection::InvalidConfiguration)?;
        self.members = self
            .members
            .checked_sub(capacity.max_members)
            .ok_or(DataRejection::InvalidConfiguration)?;
        self.bytes = self
            .bytes
            .checked_sub(capacity.max_in_flight_bytes)
            .ok_or(DataRejection::InvalidConfiguration)?;
        Ok(())
    }

    fn validate(&self, limits: subscription::Limits) -> Result<(), DataRejection> {
        if self.windows > limits.max_total_window_slots as usize
            || self.reads > limits.max_total_window_slots as usize
            || self.acks > (limits.max_total_window_slots as usize).saturating_add(self.members)
            || self.members > limits.max_total_window_slots as usize
            || self.bytes > limits.max_total_in_flight_bytes
        {
            return Err(DataRejection::InvalidConfiguration);
        }
        Ok(())
    }

    pub(super) fn fits(&self, capacity: TransferCapacity) -> bool {
        self.windows <= capacity.max_window_slots
            && self.reads <= capacity.max_read_results
            && self.acks.saturating_sub(self.members) <= capacity.max_ack_slots
            && self.members <= capacity.max_members
            && self.bytes <= capacity.max_in_flight_bytes
    }

    fn reserved(capacity: TransferCapacity) -> Self {
        Self {
            windows: capacity.max_window_slots,
            reads: capacity.max_read_results,
            acks: capacity.max_ack_slots.saturating_add(capacity.max_members),
            members: capacity.max_members,
            bytes: capacity.max_in_flight_bytes,
        }
    }
}

pub(super) fn transfer_usage(state: &PartitionState) -> Result<Usage, DataRejection> {
    usage(state.groups.values())
}

fn usage<'a>(mut cursors: impl Iterator<Item = &'a GroupCursor>) -> Result<Usage, DataRejection> {
    cursors.try_fold(Usage::default(), |mut total, cursor| {
        total.add(Usage {
            windows: cursor.window_slots(),
            reads: cursor.read_result_slots(),
            acks: cursor.ack_result_slots(),
            members: cursor.member_count(),
            bytes: cursor.in_flight_bytes(),
        })?;
        Ok(total)
    })
}

impl DataApplication {
    pub(crate) fn watch_capacities(&self) -> Result<(usize, usize, u64), DataRejection> {
        let limits = self
            .config
            .as_ref()
            .ok_or(DataRejection::Uninitialized)?
            .subscription;
        let usage = self.group_usage()?;
        let maximum = limits.max_total_window_slots as usize;
        Ok((
            maximum.saturating_sub(usage.reads),
            maximum.saturating_sub(usage.windows),
            limits.max_total_in_flight_bytes.saturating_sub(usage.bytes),
        ))
    }
    pub(super) fn available_transfer_windows(
        &self,
        mut capacity: TransferCapacity,
    ) -> Result<TransferCapacity, DataRejection> {
        let limits = self
            .config
            .as_ref()
            .ok_or(DataRejection::Uninitialized)?
            .subscription;
        let mut used = self.group_usage()?;
        if let Some(previous) = self.authority.reservation() {
            used.remove_reservation(*previous)?;
        }
        let maximum = limits.max_total_window_slots as usize;
        capacity.max_window_slots = maximum
            .checked_sub(used.windows)
            .ok_or(DataRejection::InvalidConfiguration)?;
        capacity.max_read_results = maximum
            .checked_sub(used.reads)
            .ok_or(DataRejection::InvalidConfiguration)?;
        capacity.max_ack_slots = maximum
            .checked_sub(used.acks.saturating_sub(used.members))
            .ok_or(DataRejection::InvalidConfiguration)?;
        capacity.max_members = maximum
            .checked_sub(used.members)
            .ok_or(DataRejection::InvalidConfiguration)?;
        capacity.max_in_flight_bytes = limits
            .max_total_in_flight_bytes
            .checked_sub(used.bytes)
            .ok_or(DataRejection::InvalidConfiguration)?
            .min(crate::metadata::MAX_TRANSFER_BYTES);
        Ok(capacity)
    }

    pub(super) fn validate_groups(&self) -> Result<(), DataRejection> {
        let limits = self
            .config
            .as_ref()
            .ok_or(DataRejection::Uninitialized)?
            .subscription;
        let usage = self.group_usage()?;
        usage.validate(limits)?;
        for (partition, state) in &self.partitions {
            if state
                .groups
                .len()
                .saturating_add(state.deleted_groups.len())
                > limits.max_subscription_identities as usize
                || state
                    .groups
                    .keys()
                    .any(|id| state.deleted_groups.contains_key(id))
            {
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
        if let GroupCommand::Delete {
            subscription,
            deletion,
            now_ms,
        } = &command
        {
            return self.delete_consumer_group(partition, *subscription, *deletion, *now_ms);
        }
        if self
            .partitions
            .get(&partition)
            .is_some_and(|state| state.deleted_groups.contains_key(&command.subscription()))
        {
            return Err(DataRejection::Subscription(
                subscription::Rejection::Deleted,
            ));
        }
        let config = self.config.as_ref().ok_or(DataRejection::Uninitialized)?;
        let limits = config.subscription;
        #[cfg(feature = "test-hooks")]
        let trace_scope = (config.cluster_id, partition);
        let usage = self.group_usage()?;
        let subscription = command.subscription();
        if let GroupCommand::Read { request, cold, .. } = &command {
            if let Some(original) = self
                .consumer_group_cursor(partition, subscription)
                .and_then(|cursor| cursor.original_read(request))
            {
                return original
                    .map(|read| DataResponse::ConsumerGroup(GroupOutcome::Read(read)))
                    .map_err(DataRejection::Subscription);
            }
            if cold.len() > limits.max_window_slots as usize {
                return Err(DataRejection::Subscription(
                    subscription::Rejection::WindowFull,
                ));
            }
            for proof in cold {
                if proof.partition() != partition
                    || proof.delivery_bytes < u64::from(proof.body_bytes())
                {
                    return Err(DataRejection::Subscription(
                        subscription::Rejection::InvalidContext,
                    ));
                }
                self.lifecycle
                    .verify_delivery(proof)
                    .map_err(DataRejection::Lifecycle)?;
            }
        }
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
            if state
                .groups
                .len()
                .saturating_add(state.deleted_groups.len())
                >= limits.max_subscription_identities as usize
                || start < state.producer.retained_floor()
                || start > state.producer.committed_end()
            {
                return Err(DataRejection::Subscription(
                    subscription::Rejection::InvalidContext,
                ));
            }
            let quota = state
                .quota
                .as_ref()
                .ok_or(DataRejection::Quota(QuotaRejection::Missing))?;
            let before = state
                .protected_before()
                .map_or(start, |previous| previous.min(start));
            let backlog = state
                .retention
                .logical_backlog_upper_bytes(Some(before))
                .map_err(DataRejection::Retention)?;
            if quota.grant.frozen || backlog > quota.grant.capacity.backlog_bytes {
                return Err(DataRejection::Quota(QuotaRejection::Exhausted));
            }
            let mut cursor = GroupCursor::new(identity, model, start, signing_key.into(), limits)
                .map_err(DataRejection::Subscription)?;
            if blocked {
                cursor.block_gate();
            }
            state.groups.insert(subscription, cursor);
            return Ok(DataResponse::ConsumerGroup(GroupOutcome::Created));
        }
        let read_now = match &command {
            GroupCommand::Read { request, .. } => Some(request.now_ms),
            _ => None,
        };
        let budget = match read_now {
            Some(now_ms) => state
                .quota
                .as_ref()
                .ok_or(DataRejection::Quota(QuotaRejection::Missing))?
                .read_budget(state.quota_usage()?, now_ms)
                .map_err(DataRejection::Quota)?,
            None => ReadBudget {
                bytes: 0,
                messages: 0,
                results: 0,
            },
        };
        let cursor = state
            .groups
            .get_mut(&subscription)
            .ok_or(DataRejection::Subscription(
                subscription::Rejection::NotFound,
            ))?;
        let acknowledgement = match &command {
            GroupCommand::Acknowledge { request, .. } if !cursor.ack_known(request) => Some((
                crate::budget::encoded_size(request, crate::metadata::MAX_TRANSFER_BYTES).ok(),
                request.proofs.len() as u64,
            )),
            _ => None,
        };
        let outcome = execute_group(
            cursor,
            &state.producer,
            limits,
            usage,
            budget,
            command,
            #[cfg(feature = "test-hooks")]
            trace_scope,
        )
        .map_err(DataRejection::Subscription)?;
        if let (Some(now_ms), GroupOutcome::Read(read)) = (read_now, &outcome) {
            if let Some(quota) = &mut state.quota {
                quota.commit_consume(read.delivery_bytes, now_ms);
            }
            state
                .load
                .deliver(read.delivery_bytes, read.deliveries.len() as u64);
        }
        if matches!(outcome, GroupOutcome::Acknowledged(_))
            && let Some((bytes, messages)) = acknowledgement
        {
            state.load.acknowledge(bytes, messages);
        }
        Ok(DataResponse::ConsumerGroup(outcome))
    }

    fn delete_consumer_group(
        &mut self,
        partition: PartitionKey,
        subscription: Id,
        deletion: Id,
        now: u64,
    ) -> Result<DataResponse, DataRejection> {
        use subscription::Rejection;
        let state = self
            .partitions
            .get_mut(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        if let Some(original) = state.deleted_groups.get(&subscription) {
            return if *original == deletion {
                Ok(DataResponse::ConsumerGroup(GroupOutcome::Deleted {
                    deletion,
                }))
            } else {
                Err(DataRejection::Subscription(Rejection::IdentityConflict))
            };
        }
        let cursor = state
            .groups
            .get_mut(&subscription)
            .ok_or(DataRejection::Subscription(Rejection::NotFound))?;
        cursor
            .reap_members(now, crate::metadata::MEMBER_RECOVERY_MS)
            .map_err(DataRejection::Subscription)?;
        cursor
            .ready_to_delete()
            .map_err(DataRejection::Subscription)?;
        state
            .groups
            .remove(&subscription)
            .ok_or(DataRejection::Subscription(Rejection::NotFound))?;
        state.deleted_groups.insert(subscription, deletion);
        Ok(DataResponse::ConsumerGroup(GroupOutcome::Deleted {
            deletion,
        }))
    }

    fn group_usage(&self) -> Result<Usage, DataRejection> {
        let mut total = usage(
            self.partitions
                .values()
                .flat_map(|state| state.groups.values()),
        )?;
        if let Some(capacity) = self.authority.reservation() {
            total.add(Usage::reserved(*capacity))?;
        }
        Ok(total)
    }

    pub(super) fn validate_target_group_capacity(
        &self,
        state: Option<&PartitionState>,
        capacity: TransferCapacity,
    ) -> Result<(), DataRejection> {
        let mut total = self.group_usage()?;
        if let Some(previous) = self.authority.reservation() {
            total.remove_reservation(*previous)?;
        }
        if let Some(state) = state {
            total.add(transfer_usage(state)?)?;
        } else {
            total.add(Usage::reserved(capacity))?;
        }
        total.validate(
            self.config
                .as_ref()
                .ok_or(DataRejection::Uninitialized)?
                .subscription,
        )
    }
}

fn execute_group(
    cursor: &mut GroupCursor,
    producer: &ProducerState,
    limits: subscription::Limits,
    usage: Usage,
    budget: ReadBudget,
    command: GroupCommand,
    #[cfg(feature = "test-hooks")] trace_scope: (Id, PartitionKey),
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
        GroupCommand::Read { request, cold, .. } => read_group(
            cursor,
            producer,
            limits,
            (usage, budget),
            request,
            &cold,
            #[cfg(feature = "test-hooks")]
            trace_scope,
        )
        .map(GroupOutcome::Read),
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
            version,
            expires_ms,
            now_ms,
            ..
        } => cursor
            .renew(member, epoch, version, expires_ms, now_ms)
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
        GroupCommand::Create { .. } | GroupCommand::Delete { .. } => {
            Err(Rejection::IdentityConflict)
        }
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
    admission: (Usage, ReadBudget),
    request: GroupRead,
    cold: &[crate::lifecycle::VerifiedColdDelivery],
    #[cfg(feature = "test-hooks")] trace_scope: (Id, PartitionKey),
) -> subscription::Result<subscription::ReadResult> {
    let (usage, mut budget) = admission;
    if let Some(original) = cursor.original_read(&request) {
        return original;
    }
    let candidates = read_candidates(cursor, producer, limits, &usage, cold)?;
    budget.bytes = budget
        .bytes
        .min(limits.max_total_in_flight_bytes.saturating_sub(usage.bytes));
    budget.results = budget.results.min(
        u32::try_from((limits.max_total_window_slots as usize).saturating_sub(usage.reads))
            .map_err(|_| subscription::Rejection::WindowFull)?,
    );
    #[cfg(feature = "test-hooks")]
    let trace = crate::runtime::read_trace_test_hooks::enabled(trace_scope, cursor.identity())
        .then(|| {
            let mut event = cursor.trace_read_state(request, producer.committed_end());
            event.budget_messages = budget.messages;
            event.budget_bytes = budget.bytes;
            event.budget_results = budget.results;
            event.candidates = candidates.len();
            event.candidate_min_bytes = candidates.iter().map(|candidate| candidate.bytes).min();
            event.group_windows = usage.windows;
            event.group_reads = usage.reads;
            event.group_bytes = usage.bytes;
            event
        });
    let result = cursor.read_admitted(request, &candidates, budget);
    #[cfg(feature = "test-hooks")]
    if let Some(mut event) = trace {
        event.deliveries = result.as_ref().ok().map(|read| read.deliveries.len());
        crate::runtime::read_trace_test_hooks::record(trace_scope, cursor.identity(), event);
    }
    result
}

fn read_candidates(
    cursor: &GroupCursor,
    producer: &ProducerState,
    limits: subscription::Limits,
    usage: &Usage,
    cold: &[crate::lifecycle::VerifiedColdDelivery],
) -> subscription::Result<Vec<Candidate>> {
    let available_windows = (limits.max_total_window_slots as usize).saturating_sub(usage.windows);
    let count = (limits.max_window_slots as usize)
        .saturating_sub(cursor.window_slots())
        .min(available_windows)
        .min(producer.limits().max_batch_messages);
    let mut candidates: Vec<_> = cold
        .iter()
        .filter(|proof| proof.offset() >= cursor.next_offset())
        .take(count)
        .map(|proof| Candidate {
            offset: proof.offset(),
            hash: proof.routing_hash,
            bytes: proof.delivery_bytes,
        })
        .collect();
    if candidates
        .first()
        .is_some_and(|candidate| candidate.offset != cursor.next_offset())
    {
        return Err(subscription::Rejection::AssignmentBusy);
    }
    if count > 0 && candidates.is_empty() {
        let page = producer
            .hot_read(
                cursor.next_offset(),
                count,
                producer.limits().max_batch_bytes,
            )
            .map_err(|_| subscription::Rejection::AssignmentBusy)?;
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
    Ok(candidates)
}

impl DataApplication {
    pub fn read_quota_demand(
        &self,
        partition: PartitionKey,
        subscription: Id,
        request: &GroupRead,
        cold: &[crate::lifecycle::VerifiedColdDelivery],
    ) -> Result<Option<crate::metadata::QuotaCapacity>, DataRejection> {
        let state = self
            .partitions
            .get(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        let cursor = state
            .groups
            .get(&subscription)
            .ok_or(DataRejection::Subscription(
                subscription::Rejection::NotFound,
            ))?;
        if cursor.original_read(request).is_some() {
            return Ok(None);
        }
        let limits = self
            .config
            .as_ref()
            .ok_or(DataRejection::Uninitialized)?
            .subscription;
        let candidates =
            read_candidates(cursor, &state.producer, limits, &self.group_usage()?, cold)
                .map_err(DataRejection::Subscription)?;
        let Some(bytes) = cursor.first_delivery_bytes(request, &candidates) else {
            return Ok(None);
        };
        let usage = state.quota_usage()?;
        Ok(Some(crate::metadata::QuotaCapacity {
            inflight_bytes: usage
                .inflight_bytes
                .checked_add(bytes)
                .ok_or(DataRejection::Quota(QuotaRejection::Arithmetic))?,
            inflight_messages: usage
                .inflight_messages
                .checked_add(1)
                .ok_or(DataRejection::Quota(QuotaRejection::Arithmetic))?,
            consume_bytes_per_second: state.quota.as_ref().map_or(bytes, |quota| {
                quota.consume_rate_required(bytes, request.now_ms)
            }),
            ..Default::default()
        }))
    }
}
