use super::{Limits, Rejection, Result};

#[cfg(test)]
#[path = "../../tests/scalable_support/consumer_memory.rs"]
mod memory_tests;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GroupCommand {
    Create {
        identity: GroupIdentity,
        model: SubscriptionKind,
        start: u64,
        signing_key: super::SigningKey,
        blocked: bool,
    },
    InstallMembers {
        subscription: Id,
        version: u64,
        grants: Vec<MemberGrant>,
        now_ms: u64,
    },
    Read {
        subscription: Id,
        request: GroupRead,
        cold: Vec<crate::lifecycle::VerifiedColdDelivery>,
    },
    RetireRead {
        subscription: Id,
        member: Id,
        epoch: u64,
        sequence: u64,
    },
    Acknowledge {
        subscription: Id,
        request: GroupAck,
    },
    RetireAck {
        subscription: Id,
        member: Id,
        epoch: u64,
        sequence: u64,
    },
    Renew {
        subscription: Id,
        member: Id,
        epoch: u64,
        version: u64,
        expires_ms: u64,
        now_ms: u64,
    },
    Reap {
        subscription: Id,
        now_ms: u64,
        recovery_ms: u64,
    },
    Retry {
        subscription: Id,
        offset: u64,
        now_ms: u64,
    },
    OpenGate {
        subscription: Id,
    },
    Delete {
        subscription: Id,
        deletion: Id,
        now_ms: u64,
    },
}

impl GroupCommand {
    pub fn subscription(&self) -> Id {
        match self {
            Self::Create { identity, .. } => identity.subscription,
            Self::InstallMembers { subscription, .. }
            | Self::Read { subscription, .. }
            | Self::RetireRead { subscription, .. }
            | Self::Acknowledge { subscription, .. }
            | Self::RetireAck { subscription, .. }
            | Self::Renew { subscription, .. }
            | Self::Reap { subscription, .. }
            | Self::Retry { subscription, .. } => *subscription,
            Self::OpenGate { subscription } | Self::Delete { subscription, .. } => *subscription,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GroupOutcome {
    Created,
    MembersInstalled,
    Read(ReadResult),
    ReadRetired,
    Acknowledged(AckDisposition),
    AckRetired,
    Renewed(u64),
    Reaped(usize),
    RetryScheduled,
    GateOpened,
    Deleted { deletion: Id },
}
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tidemq_protocol::{
    Id,
    topic::{HashRange, SubscriptionKind},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemberGrant {
    pub member: Id,
    pub client: Id,
    pub credential: Id,
    pub epoch: u64,
    pub expires_ms: u64,
    pub lanes: Vec<HashRange>,
    pub max_messages: u32,
    pub max_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    pub offset: u64,
    pub hash: u64,
    pub bytes: u64,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeliveryProof {
    pub offset: u64,
    pub member: Id,
    pub epoch: u64,
    pub attempt: u32,
    pub mac: [u8; 32],
}

impl std::fmt::Debug for DeliveryProof {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeliveryProof")
            .field("offset", &self.offset)
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum SlotState {
    Ready(u64),
    InFlight(Id, u64),
    Acked,
    Paused,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Slot {
    candidate: Candidate,
    attempt: u32,
    attempt_limit: u32,
    state: SlotState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Member {
    grant: MemberGrant,
    active: bool,
    in_flight: u32,
    bytes: u64,
    read_sequence: u64,
    read: Option<ReadResult>,
    read_digest: [u8; 32],
    ack_floor: u64,
    acks: BTreeMap<u64, SavedAck>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadResult {
    pub sequence: u64,
    pub assignment_version: u64,
    pub deliveries: Vec<DeliveryProof>,
    pub delivery_bytes: u64,
    pub retired: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadBudget {
    pub bytes: u64,
    pub messages: u64,
    pub results: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AckDisposition {
    Acknowledged,
    RedeliveryScheduled { due_ms: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SavedAck {
    digest: [u8; 32],
    result: Result<AckDisposition>,
    retired: bool,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupCursor {
    topic: Id,
    shard: Id,
    subscription: Id,
    model: SubscriptionKind,
    signing_key: [u8; 32],
    limits: Limits,
    frontier: u64,
    next: u64,
    version: u64,
    gate_open: bool,
    members: BTreeMap<(Id, u64), Member>,
    slots: BTreeMap<u64, Slot>,
    ready: BTreeSet<(u64, u64)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupIdentity {
    pub topic: Id,
    pub shard: Id,
    pub subscription: Id,
}

impl std::fmt::Debug for GroupCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupCursor")
            .field("subscription", &self.subscription)
            .field("model", &self.model)
            .field("frontier", &self.frontier)
            .field("slots", &self.slots.len())
            .finish_non_exhaustive()
    }
}

impl GroupCursor {
    pub(crate) fn ready_to_delete(&self) -> Result<()> {
        if self.members.values().any(|member| member.active) {
            return Err(Rejection::AssignmentBusy);
        }
        if self.members.values().any(|member| {
            member.read.as_ref().is_some_and(|read| !read.retired)
                || member.acks.values().any(|ack| !ack.retired)
        }) {
            return Err(Rejection::RecoveryPending);
        }
        Ok(())
    }

    pub fn gate_open(&self) -> bool {
        self.gate_open
    }
    pub(crate) fn block_gate(&mut self) {
        self.gate_open = false;
    }
    pub(crate) fn open_gate(&mut self) {
        self.gate_open = true;
    }
    pub fn authorize_member(&self, member: Id, epoch: u64, client: Id, credential: Id) -> bool {
        self.members
            .get(&(member, epoch))
            .is_some_and(|row| row.grant.client == client && row.grant.credential == credential)
    }
    pub(crate) fn renewal_version(&self, member: Id, epoch: u64, now: u64) -> Option<u64> {
        self.members
            .get(&(member, epoch))
            .filter(|member| member.active && member.grant.expires_ms <= now.saturating_add(5_000))
            .map(|_| self.version)
    }
    pub fn limits(&self) -> Limits {
        self.limits
    }
    pub fn window_slots(&self) -> usize {
        self.slots.len()
    }
    pub fn result_slots(&self) -> usize {
        self.members
            .values()
            .map(|member| {
                member.acks.len() + member.read.as_ref().map_or(0, |read| read.deliveries.len())
            })
            .sum()
    }
    pub fn ack_result_slots(&self) -> usize {
        self.members.values().map(|member| member.acks.len()).sum()
    }
    pub fn read_result_slots(&self) -> usize {
        self.members
            .values()
            .filter_map(|member| member.read.as_ref())
            .map(|read| read.deliveries.len())
            .sum()
    }
    pub fn contains_member(&self, member: Id, epoch: u64) -> bool {
        self.members.contains_key(&(member, epoch))
    }
    pub fn identity(&self) -> GroupIdentity {
        GroupIdentity {
            topic: self.topic,
            shard: self.shard,
            subscription: self.subscription,
        }
    }
    pub fn member_count(&self) -> usize {
        self.members.len()
    }
    pub fn logical_state_bytes(&self) -> u64 {
        (self
            .window_slots()
            .saturating_add(self.result_slots())
            .saturating_add(self.member_count()) as u64)
            .saturating_mul(512)
    }
    pub fn ack_known(&self, request: &GroupAck) -> bool {
        self.members
            .get(&(request.member, request.epoch))
            .is_some_and(|member| {
                request.sequence <= member.ack_floor || member.acks.contains_key(&request.sequence)
            })
    }
    pub fn original_ack_outcome(&self, request: &GroupAck) -> Option<Result<AckDisposition>> {
        let member = self.members.get(&(request.member, request.epoch))?;
        let saved = member.acks.get(&request.sequence)?;
        (!saved.retired && ack_digest(request).ok() == Some(saved.digest)).then_some(saved.result)
    }
    pub fn ack_retirement_requires_slot(&self, id: Id, epoch: u64, sequence: u64) -> bool {
        self.members.get(&(id, epoch)).is_some_and(|member| {
            sequence > member.ack_floor.saturating_add(1) && !member.acks.contains_key(&sequence)
        })
    }
    pub fn ack_is_head(&self, request: &GroupAck) -> bool {
        self.members
            .get(&(request.member, request.epoch))
            .is_some_and(|member| member.ack_floor.checked_add(1) == Some(request.sequence))
    }
    pub fn in_flight_bytes(&self) -> u64 {
        self.members
            .values()
            .fold(0_u64, |sum, member| sum.saturating_add(member.bytes))
    }
    pub fn in_flight_messages(&self) -> u64 {
        self.members.values().fold(0_u64, |sum, member| {
            sum.saturating_add(u64::from(member.in_flight))
        })
    }
    pub fn new(
        identity: GroupIdentity,
        model: SubscriptionKind,
        start: u64,
        signing_key: [u8; 32],
        limits: Limits,
    ) -> Result<Self> {
        limits.validate()?;
        if signing_key == [0; 32] {
            return Err(Rejection::InvalidToken);
        }
        Ok(Self {
            topic: identity.topic,
            shard: identity.shard,
            subscription: identity.subscription,
            model,
            signing_key,
            limits,
            frontier: start,
            next: start,
            version: 0,
            gate_open: true,
            members: BTreeMap::new(),
            slots: BTreeMap::new(),
            ready: BTreeSet::new(),
        })
    }

    pub fn model(&self) -> SubscriptionKind {
        self.model
    }
    pub fn next_offset(&self) -> u64 {
        self.next
    }
    pub fn acknowledged_before(&self) -> u64 {
        self.frontier
    }
    pub fn protected_before(&self) -> u64 {
        self.members
            .values()
            .filter_map(|member| member.read.as_ref())
            .filter(|read| !read.retired)
            .flat_map(|read| &read.deliveries)
            .map(|delivery| delivery.offset)
            .next()
            .map_or(self.frontier, |offset| offset.min(self.frontier))
    }

    pub fn install_members(
        &mut self,
        version: u64,
        grants: &[MemberGrant],
        now: u64,
    ) -> Result<()> {
        if version < self.version {
            return Err(Rejection::StaleGeneration);
        }
        self.validate_grants(grants, now)?;
        if version == self.version {
            let current: Vec<_> = self
                .members
                .values()
                .filter(|member| member.active)
                .map(|member| &member.grant)
                .collect();
            let matches = current.len() == grants.len()
                && grants.iter().all(|grant| {
                    current.iter().any(|installed| {
                        let mut original = (*installed).clone();
                        original.expires_ms = grant.expires_ms;
                        original == *grant
                    })
                });
            if !matches {
                return Err(Rejection::IdentityConflict);
            }
            self.fence_invalid_deliveries(now);
            for grant in grants {
                let member = self
                    .members
                    .get_mut(&(grant.member, grant.epoch))
                    .ok_or(Rejection::InvalidSnapshot)?;
                member.grant.expires_ms = member.grant.expires_ms.max(grant.expires_ms);
            }
            return Ok(());
        }
        let new = grants
            .iter()
            .filter(|grant| !self.members.contains_key(&(grant.member, grant.epoch)))
            .count();
        if self
            .members
            .len()
            .checked_add(new)
            .is_none_or(|count| count > 256)
        {
            return Err(Rejection::IdentityBudget);
        }
        // Expired attempts must be fenced before a later lease can be installed.
        self.fence_invalid_deliveries(now);
        for member in self.members.values_mut() {
            member.active = false;
        }
        for grant in grants {
            let member = self
                .members
                .entry((grant.member, grant.epoch))
                .or_insert_with(|| Member {
                    grant: grant.clone(),
                    active: true,
                    in_flight: 0,
                    bytes: 0,
                    read_sequence: 0,
                    read: None,
                    read_digest: [0; 32],
                    ack_floor: 0,
                    acks: BTreeMap::new(),
                });
            member.grant = grant.clone();
            member.active = true;
        }
        self.version = version;
        self.fence_invalid_deliveries(now);
        Ok(())
    }

    fn validate_grants(&self, grants: &[MemberGrant], now: u64) -> Result<()> {
        if grants.len() > 128 {
            return Err(Rejection::IdentityBudget);
        }
        let mut identities = BTreeSet::new();
        let mut lanes = Vec::new();
        for grant in grants {
            if grant.expires_ms <= now {
                return Err(Rejection::StaleGeneration);
            }
            if grant.epoch == 0
                || grant.max_messages == 0
                || grant.max_bytes == 0
                || grant.max_messages > self.limits.max_in_flight_messages
                || grant.max_bytes > self.limits.max_in_flight_bytes
                || grant.lanes.len() > 256
                || !identities.insert(grant.member)
            {
                return Err(Rejection::InvalidContext);
            }
            for lane in &grant.lanes {
                lane.validate().map_err(|_| Rejection::InvalidContext)?;
                lanes.push(*lane);
            }
        }
        if self.model == SubscriptionKind::Queue && !lanes.is_empty() {
            return Err(Rejection::InvalidContext);
        }
        lanes.sort_by_key(|lane| lane.start);
        if self.model == SubscriptionKind::Stream
            && lanes
                .windows(2)
                .any(|pair| match (pair.first(), pair.get(1)) {
                    (Some(left), Some(right)) => left.end.is_none_or(|end| end > right.start),
                    _ => true,
                })
        {
            return Err(Rejection::AssignmentBusy);
        }
        Ok(())
    }

    fn eligible(&self, member: &Member, candidate: Candidate, now: u64) -> bool {
        member.grant.expires_ms > now && self.owns(member, candidate)
    }

    fn owns(&self, member: &Member, candidate: Candidate) -> bool {
        member.active
            && (self.model == SubscriptionKind::Queue
                || member
                    .grant
                    .lanes
                    .iter()
                    .any(|lane| lane.contains(candidate.hash)))
    }

    fn fence_invalid_deliveries(&mut self, now: u64) {
        let invalid: Vec<_> = self
            .slots
            .iter()
            .filter_map(|(offset, slot)| {
                let SlotState::InFlight(id, epoch) = slot.state else {
                    return None;
                };
                (!self
                    .members
                    .get(&(id, epoch))
                    .is_some_and(|member| self.owns(member, slot.candidate)))
                .then_some(*offset)
            })
            .collect();
        for offset in invalid {
            self.release_attempt(offset, now);
        }
    }

    fn release_attempt(&mut self, offset: u64, due: u64) {
        let due = if self.model == SubscriptionKind::Stream {
            0
        } else {
            due
        };
        if let Some(slot) = self.slots.get_mut(&offset) {
            if let SlotState::InFlight(id, epoch) = slot.state
                && let Some(member) = self.members.get_mut(&(id, epoch))
            {
                member.in_flight = member.in_flight.saturating_sub(1);
                member.bytes = member.bytes.saturating_sub(slot.candidate.bytes);
            }
            slot.state = SlotState::Ready(due);
            self.ready.insert((due, offset));
        }
    }

    pub fn read(&mut self, request: GroupRead, candidates: &[Candidate]) -> Result<ReadResult> {
        self.read_admitted(
            request,
            candidates,
            ReadBudget {
                bytes: self.limits.max_total_in_flight_bytes,
                messages: u64::from(self.limits.max_total_window_slots),
                results: self.limits.max_total_window_slots,
            },
        )
    }

    pub(crate) fn original_read(&self, request: &GroupRead) -> Option<Result<ReadResult>> {
        let Some(member) = self.members.get(&(request.member, request.epoch)) else {
            return Some(Err(Rejection::SessionMismatch));
        };
        if request.sequence == member.read_sequence && request.sequence > 0 {
            return Some(if member.read_digest != read_digest(request) {
                Err(Rejection::IdentityConflict)
            } else {
                member
                    .read
                    .as_ref()
                    .filter(|read| !read.retired)
                    .cloned()
                    .ok_or(Rejection::OutcomeExpired)
            });
        }
        (request.sequence < member.read_sequence).then_some(Err(Rejection::OutcomeExpired))
    }

    pub(crate) fn minimum_delivery_bytes(
        &self,
        request: &GroupRead,
        candidates: &[Candidate],
    ) -> Option<u64> {
        let member = self.members.get(&(request.member, request.epoch))?;
        self.ready
            .iter()
            .filter(|(due, _)| *due <= request.now_ms)
            .filter_map(|(_, offset)| self.slots.get(offset).map(|slot| slot.candidate))
            .chain(candidates.iter().copied())
            .filter(|candidate| self.eligible(member, *candidate, request.now_ms))
            .map(|candidate| candidate.bytes)
            .filter(|bytes| *bytes <= request.max_bytes)
            .min()
    }

    pub(crate) fn read_admitted(
        &mut self,
        request: GroupRead,
        candidates: &[Candidate],
        budget: ReadBudget,
    ) -> Result<ReadResult> {
        if let Some(original) = self.original_read(&request) {
            return original;
        }
        let member = self
            .members
            .get(&(request.member, request.epoch))
            .ok_or(Rejection::SessionMismatch)?;
        let digest = read_digest(&request);
        if !self.gate_open {
            return Err(Rejection::AssignmentBusy);
        }
        if request.sequence
            != member
                .read_sequence
                .checked_add(1)
                .ok_or(Rejection::CounterOverflow)?
            || member.read.as_ref().is_some_and(|read| !read.retired)
        {
            return Err(Rejection::InvalidBarrier);
        }
        if !member.active || member.grant.expires_ms <= request.now_ms {
            return Err(Rejection::SessionMismatch);
        }
        if request.max_messages == 0
            || request.max_messages > member.grant.max_messages
            || request.max_bytes == 0
            || request.max_bytes > member.grant.max_bytes
        {
            return Err(Rejection::InvalidContext);
        }
        self.validate_candidates(candidates)?;
        self.fence_invalid_deliveries(request.now_ms);
        for candidate in candidates {
            if self.slots.len() >= self.limits.max_window_slots as usize {
                break;
            }
            self.next = candidate
                .offset
                .checked_add(1)
                .ok_or(Rejection::CounterOverflow)?;
            self.slots.insert(
                candidate.offset,
                Slot {
                    candidate: *candidate,
                    attempt: 0,
                    attempt_limit: self.limits.max_delivery_attempts,
                    state: SlotState::Ready(0),
                },
            );
            self.ready.insert((0, candidate.offset));
        }
        self.deliver(request, digest, budget)
    }

    fn validate_candidates(&self, candidates: &[Candidate]) -> Result<()> {
        if candidates.len() > self.limits.max_window_slots as usize {
            return Err(Rejection::WindowFull);
        }
        let mut next = self.next;
        for candidate in candidates {
            if candidate.offset != next
                || candidate.bytes == 0
                || candidate.bytes > self.limits.max_in_flight_bytes
            {
                return Err(Rejection::InvalidContext);
            }
            next = next.checked_add(1).ok_or(Rejection::CounterOverflow)?;
        }
        Ok(())
    }

    fn deliver(
        &mut self,
        request: GroupRead,
        digest: [u8; 32],
        budget: ReadBudget,
    ) -> Result<ReadResult> {
        let member = self
            .members
            .get(&(request.member, request.epoch))
            .ok_or(Rejection::SessionMismatch)?;
        let retained: usize = self
            .members
            .values()
            .filter_map(|member| member.read.as_ref())
            .filter(|read| !read.retired)
            .map(|read| read.deliveries.len())
            .sum();
        let result_capacity = self
            .limits
            .max_total_window_slots
            .saturating_sub(u32::try_from(retained).map_err(|_| Rejection::WindowFull)?);
        let maximum = request
            .max_messages
            .min(member.grant.max_messages.saturating_sub(member.in_flight))
            .min(result_capacity)
            .min(budget.results)
            .min(u32::try_from(budget.messages).unwrap_or(u32::MAX));
        let mut bytes = 0_u64;
        let available = self
            .limits
            .max_total_in_flight_bytes
            .saturating_sub(self.in_flight_bytes())
            .min(budget.bytes);
        let mut selected = Vec::new();
        let mut paused = Vec::new();
        let mut blocked = if self.model == SubscriptionKind::Stream {
            self.slots
                .values()
                .filter(|slot| slot.state == SlotState::Paused)
                .map(|slot| slot.candidate.hash)
                .collect::<BTreeSet<_>>()
        } else {
            BTreeSet::new()
        };
        for (due, offset) in &self.ready {
            if *due > request.now_ms || selected.len() >= maximum as usize {
                break;
            }
            let slot = self.slots.get(offset).ok_or(Rejection::InvalidSnapshot)?;
            if !self.eligible(member, slot.candidate, request.now_ms)
                || blocked.contains(&slot.candidate.hash)
            {
                continue;
            }
            if slot.attempt >= slot.attempt_limit {
                paused.push((*due, *offset));
                if self.model == SubscriptionKind::Stream {
                    blocked.insert(slot.candidate.hash);
                }
                continue;
            }
            let next = bytes
                .checked_add(slot.candidate.bytes)
                .ok_or(Rejection::CounterOverflow)?;
            if next > request.max_bytes
                || next > available
                || next > member.grant.max_bytes.saturating_sub(member.bytes)
            {
                // Keep this key behind its unavailable predecessor while other keys progress.
                if self.model == SubscriptionKind::Stream {
                    blocked.insert(slot.candidate.hash);
                }
                continue;
            }
            let attempt = slot
                .attempt
                .checked_add(1)
                .ok_or(Rejection::CounterOverflow)?;
            selected.push((*due, *offset, attempt));
            bytes = next;
        }
        let deliveries: Vec<_> = selected
            .iter()
            .map(|(_, offset, attempt)| {
                self.proof(*offset, request.member, request.epoch, *attempt)
            })
            .collect();
        for (due, offset) in paused {
            self.ready.remove(&(due, offset));
            self.slots
                .get_mut(&offset)
                .ok_or(Rejection::InvalidSnapshot)?
                .state = SlotState::Paused;
        }
        for (due, offset, attempt) in selected {
            self.ready.remove(&(due, offset));
            let slot = self
                .slots
                .get_mut(&offset)
                .ok_or(Rejection::InvalidSnapshot)?;
            slot.attempt = attempt;
            slot.state = SlotState::InFlight(request.member, request.epoch);
        }
        let result = ReadResult {
            sequence: request.sequence,
            assignment_version: self.version,
            deliveries,
            delivery_bytes: bytes,
            retired: false,
        };
        let member = self
            .members
            .get_mut(&(request.member, request.epoch))
            .ok_or(Rejection::SessionMismatch)?;
        member.in_flight = member
            .in_flight
            .checked_add(
                u32::try_from(result.deliveries.len()).map_err(|_| Rejection::CounterOverflow)?,
            )
            .ok_or(Rejection::CounterOverflow)?;
        member.bytes = member
            .bytes
            .checked_add(bytes)
            .ok_or(Rejection::CounterOverflow)?;
        member.read_sequence = request.sequence;
        member.read_digest = digest;
        member.read = Some(result.clone());
        Ok(result)
    }

    pub fn retire_read(&mut self, member: Id, epoch: u64, sequence: u64) -> Result<()> {
        let member = self
            .members
            .get_mut(&(member, epoch))
            .ok_or(Rejection::SessionMismatch)?;
        if sequence > 0 && sequence < member.read_sequence {
            return Ok(());
        }
        if member.read_sequence != sequence {
            return Err(Rejection::InvalidBarrier);
        }
        let read = member.read.as_mut().ok_or(Rejection::InvalidBarrier)?;
        // The next Read replaces this vector; keep only the retired identity during recovery.
        read.deliveries = Vec::new();
        read.retired = true;
        Ok(())
    }

    pub fn acknowledge(&mut self, request: GroupAck) -> Result<AckDisposition> {
        let member_key = (request.member, request.epoch);
        let member = self
            .members
            .get(&member_key)
            .ok_or(Rejection::SessionMismatch)?;
        let digest = ack_digest(&request)?;
        if let Some(saved) = member.acks.get(&request.sequence) {
            if saved.retired {
                return Err(Rejection::OutcomeExpired);
            }
            if saved.digest != digest {
                return Err(Rejection::IdentityConflict);
            }
            return saved.result;
        }
        if request.sequence <= member.ack_floor {
            return Err(Rejection::OutcomeExpired);
        }
        if member.acks.len() >= self.limits.max_window_slots as usize
            && member.ack_floor.checked_add(1) != Some(request.sequence)
        {
            return Err(Rejection::WindowFull);
        }
        let result = self
            .validate_ack(&request, member)
            .and_then(|_| self.ack_disposition(&request));
        let result = match result {
            Ok(result) => result,
            Err(reason) => {
                self.members
                    .get_mut(&member_key)
                    .ok_or(Rejection::SessionMismatch)?
                    .acks
                    .insert(
                        request.sequence,
                        SavedAck {
                            digest,
                            result: Err(reason),
                            retired: false,
                        },
                    );
                return Err(reason);
            }
        };
        for proof in &request.proofs {
            match result {
                AckDisposition::RedeliveryScheduled { due_ms } => {
                    self.release_attempt(proof.offset, due_ms)
                }
                AckDisposition::Acknowledged => {
                    self.release_attempt(proof.offset, 0);
                    self.ready.remove(&(0, proof.offset));
                    self.slots
                        .get_mut(&proof.offset)
                        .ok_or(Rejection::InvalidSnapshot)?
                        .state = SlotState::Acked;
                }
            }
        }
        while self
            .slots
            .get(&self.frontier)
            .is_some_and(|slot| slot.state == SlotState::Acked)
        {
            self.slots.remove(&self.frontier);
            self.frontier = self
                .frontier
                .checked_add(1)
                .ok_or(Rejection::CounterOverflow)?;
        }
        self.members
            .get_mut(&member_key)
            .ok_or(Rejection::SessionMismatch)?
            .acks
            .insert(
                request.sequence,
                SavedAck {
                    digest,
                    result: Ok(result),
                    retired: false,
                },
            );
        Ok(result)
    }

    fn ack_disposition(&self, request: &GroupAck) -> Result<AckDisposition> {
        if let Some(delay) = request.nack_delay_ms {
            if self.model != SubscriptionKind::Queue || delay > 300_000 || request.proofs.len() != 1
            {
                return Err(Rejection::InvalidContext);
            }
            Ok(AckDisposition::RedeliveryScheduled {
                due_ms: request
                    .now_ms
                    .checked_add(delay)
                    .ok_or(Rejection::CounterOverflow)?,
            })
        } else {
            Ok(AckDisposition::Acknowledged)
        }
    }

    fn validate_ack(&self, request: &GroupAck, member: &Member) -> Result<()> {
        if request.model != self.model {
            return Err(Rejection::ModelMismatch);
        }
        if request.sequence == 0 || !member.active || member.grant.expires_ms <= request.now_ms {
            return Err(Rejection::SessionMismatch);
        }
        if request.proofs.is_empty()
            || request.proofs.len() > self.limits.max_ack_batch as usize
            || self.model == SubscriptionKind::Queue && request.proofs.len() != 1
        {
            return Err(Rejection::AckBatchTooLarge);
        }
        if member.acks.len() >= self.limits.max_window_slots as usize
            && member.ack_floor.checked_add(1) != Some(request.sequence)
        {
            return Err(Rejection::WindowFull);
        }
        let mut offsets = BTreeSet::new();
        for proof in &request.proofs {
            let slot = self
                .slots
                .get(&proof.offset)
                .ok_or(Rejection::NotDelivered)?;
            if proof.member != request.member
                || proof.epoch != request.epoch
                || !offsets.insert(proof.offset)
                || slot.state != SlotState::InFlight(request.member, request.epoch)
                || slot.attempt != proof.attempt
                || !self.valid_mac(proof)?
            {
                return Err(Rejection::StaleAttempt);
            }
        }
        Ok(())
    }

    pub fn delivery_is_current(&self, proof: &DeliveryProof, _now: u64) -> bool {
        self.slots.get(&proof.offset).is_some_and(|slot| {
            slot.state == SlotState::InFlight(proof.member, proof.epoch)
                && slot.attempt == proof.attempt
                && self
                    .members
                    .get(&(proof.member, proof.epoch))
                    .is_some_and(|member| self.owns(member, slot.candidate))
                && self.valid_mac(proof).unwrap_or(false)
        })
    }

    pub fn retire_ack(&mut self, member: Id, epoch: u64, sequence: u64) -> Result<()> {
        let member = self
            .members
            .get_mut(&(member, epoch))
            .ok_or(Rejection::SessionMismatch)?;
        if sequence <= member.ack_floor {
            return Ok(());
        }
        if sequence
            == member
                .ack_floor
                .checked_add(1)
                .ok_or(Rejection::CounterOverflow)?
        {
            member.acks.remove(&sequence);
            member.ack_floor = sequence;
        } else if let Some(ack) = member.acks.get_mut(&sequence) {
            ack.retired = true;
        } else {
            if member.acks.len() >= self.limits.max_window_slots as usize {
                return Err(Rejection::WindowFull);
            }
            // Retirement also fences an operation that was rejected before result allocation.
            member.acks.insert(
                sequence,
                SavedAck {
                    digest: [0; 32],
                    result: Err(Rejection::OutcomeExpired),
                    retired: true,
                },
            );
        }
        while member
            .ack_floor
            .checked_add(1)
            .is_some_and(|next| member.acks.get(&next).is_some_and(|ack| ack.retired))
        {
            let next = member
                .ack_floor
                .checked_add(1)
                .ok_or(Rejection::CounterOverflow)?;
            member.acks.remove(&next);
            member.ack_floor = next;
        }
        Ok(())
    }

    fn proof(&self, offset: u64, member: Id, epoch: u64, attempt: u32) -> DeliveryProof {
        let mut hash = blake3::Hasher::new_keyed(&self.signing_key);
        hash.update(b"tidemq/group-delivery/v1\0");
        for id in [self.topic, self.shard, self.subscription, member] {
            hash.update(id.as_bytes());
        }
        hash.update(&offset.to_be_bytes());
        hash.update(&epoch.to_be_bytes());
        hash.update(&attempt.to_be_bytes());
        DeliveryProof {
            offset,
            member,
            epoch,
            attempt,
            mac: *hash.finalize().as_bytes(),
        }
    }

    fn valid_mac(&self, proof: &DeliveryProof) -> Result<bool> {
        let expected = self.proof(proof.offset, proof.member, proof.epoch, proof.attempt);
        Ok(
            blake3::Hash::from_slice(&proof.mac).map_err(|_| Rejection::InvalidToken)?
                == blake3::Hash::from_slice(&expected.mac).map_err(|_| Rejection::InvalidToken)?,
        )
    }

    pub fn retry_message(&mut self, offset: u64, now: u64) -> Result<()> {
        let slot = self.slots.get(&offset).ok_or(Rejection::NotDelivered)?;
        if slot.state == SlotState::Acked {
            return Err(Rejection::NotDelivered);
        }
        let limit = slot
            .attempt
            .checked_add(self.limits.max_delivery_attempts)
            .ok_or(Rejection::CounterOverflow)?;
        if let SlotState::Ready(due) = slot.state {
            self.ready.remove(&(due, offset));
        }
        self.release_attempt(offset, now);
        self.slots
            .get_mut(&offset)
            .ok_or(Rejection::NotDelivered)?
            .attempt_limit = limit;
        Ok(())
    }

    pub fn reap_members(&mut self, now: u64, recovery_ms: u64) -> Result<usize> {
        if recovery_ms == 0 || recovery_ms > 3_600_000 {
            return Err(Rejection::InvalidLimits);
        }
        self.fence_invalid_deliveries(now);
        // Cached data leases restrict admission; only a metadata assignment revokes ownership.
        let before = self.members.len();
        self.members.retain(|_, member| {
            member.active
                || (!member.acks.is_empty()
                    || member.read.as_ref().is_some_and(|read| !read.retired))
                    && member.grant.expires_ms.saturating_add(recovery_ms) > now
        });
        Ok(before.saturating_sub(self.members.len()))
    }

    pub fn renew(
        &mut self,
        member: Id,
        epoch: u64,
        version: u64,
        expires_ms: u64,
        now: u64,
    ) -> Result<u64> {
        if self.version != version {
            return Err(Rejection::StaleGeneration);
        }
        let installed = self
            .members
            .get(&(member, epoch))
            .ok_or(Rejection::SessionMismatch)?;
        if !installed.active || expires_ms <= now || expires_ms.saturating_sub(now) > 15_000 {
            return Err(Rejection::SessionMismatch);
        }
        // Metadata checked the live member epoch and this exact installed assignment version.
        self.fence_invalid_deliveries(now);
        let member = self
            .members
            .get_mut(&(member, epoch))
            .ok_or(Rejection::SessionMismatch)?;
        member.grant.expires_ms = member.grant.expires_ms.max(expires_ms);
        Ok(member.grant.expires_ms)
    }

    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        if self.in_flight_bytes() > self.limits.max_total_in_flight_bytes
            || self.signing_key == [0; 32]
            || self.frontier > self.next
            || self.members.len() > 256
            || self.slots.len() > self.limits.max_window_slots as usize
            || self.next.checked_sub(self.frontier) != u64::try_from(self.slots.len()).ok()
        {
            return Err(Rejection::InvalidSnapshot);
        }
        let active: Vec<_> = self
            .members
            .values()
            .filter(|member| member.active)
            .map(|member| member.grant.clone())
            .collect();
        self.validate_grants(&active, 0)?;
        let mut ready = BTreeSet::new();
        let mut counters: BTreeMap<(Id, u64), (u32, u64)> = BTreeMap::new();
        let mut next = self.frontier;
        for (offset, slot) in &self.slots {
            if *offset != next
                || slot.candidate.offset != *offset
                || slot.attempt > slot.attempt_limit
                || slot.attempt_limit == 0
            {
                return Err(Rejection::InvalidSnapshot);
            }
            next = next.checked_add(1).ok_or(Rejection::InvalidSnapshot)?;
            match slot.state {
                SlotState::Ready(due) => {
                    ready.insert((due, *offset));
                }
                SlotState::InFlight(id, epoch) => {
                    if slot.attempt == 0 || !self.members.contains_key(&(id, epoch)) {
                        return Err(Rejection::InvalidSnapshot);
                    }
                    let counter = counters.entry((id, epoch)).or_default();
                    counter.0 = counter.0.checked_add(1).ok_or(Rejection::InvalidSnapshot)?;
                    counter.1 = counter
                        .1
                        .checked_add(slot.candidate.bytes)
                        .ok_or(Rejection::InvalidSnapshot)?;
                }
                _ => {}
            }
        }
        if ready != self.ready {
            return Err(Rejection::InvalidSnapshot);
        }
        for (key, member) in &self.members {
            let (count, bytes) = counters.get(key).copied().unwrap_or_default();
            if *key != (member.grant.member, member.grant.epoch)
                || member.in_flight != count
                || member.bytes != bytes
                || count > member.grant.max_messages
                || bytes > member.grant.max_bytes
                || member.acks.len() > (self.limits.max_window_slots as usize).saturating_add(1)
                || member
                    .acks
                    .keys()
                    .any(|sequence| *sequence <= member.ack_floor)
            {
                return Err(Rejection::InvalidSnapshot);
            }
            if let Some(read) = &member.read {
                if read.sequence != member.read_sequence
                    || read.assignment_version == 0
                    || read.assignment_version > self.version
                    || read.retired && !read.deliveries.is_empty()
                {
                    return Err(Rejection::InvalidSnapshot);
                }
                for proof in &read.deliveries {
                    if (proof.member, proof.epoch) != *key || !self.valid_mac(proof)? {
                        return Err(Rejection::InvalidSnapshot);
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupRead {
    pub member: Id,
    pub epoch: u64,
    pub sequence: u64,
    pub now_ms: u64,
    pub max_messages: u32,
    pub max_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupAck {
    pub member: Id,
    pub epoch: u64,
    pub sequence: u64,
    pub now_ms: u64,
    pub model: SubscriptionKind,
    pub proofs: Vec<DeliveryProof>,
    pub nack_delay_ms: Option<u64>,
}

fn read_digest(request: &GroupRead) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq/group-read/v1\0");
    hash.update(request.member.as_bytes());
    hash.update(&request.epoch.to_be_bytes());
    hash.update(&request.sequence.to_be_bytes());
    hash.update(&request.max_messages.to_be_bytes());
    hash.update(&request.max_bytes.to_be_bytes());
    *hash.finalize().as_bytes()
}

fn ack_digest(request: &GroupAck) -> Result<[u8; 32]> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq/group-ack/v1\0");
    hash.update(request.member.as_bytes());
    hash.update(&request.epoch.to_be_bytes());
    hash.update(&request.sequence.to_be_bytes());
    hash.update(&[match request.model {
        SubscriptionKind::Stream => 1,
        SubscriptionKind::Queue => 2,
    }]);
    postcard::to_io(&request.proofs, &mut hash).map_err(|_| Rejection::InvalidContext)?;
    postcard::to_io(&request.nack_delay_ms, &mut hash).map_err(|_| Rejection::InvalidContext)?;
    Ok(*hash.finalize().as_bytes())
}
