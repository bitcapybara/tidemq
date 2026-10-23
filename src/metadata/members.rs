use super::Rejection;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tidemq_protocol::{
    Id,
    topic::{HashRange, InitialPosition, SubscriptionKind},
};

pub const MEMBER_LEASE_MS: u64 = 15_000;
pub const MEMBER_RECOVERY_MS: u64 = 620_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MemberStatus {
    Joining,
    Active,
    Leaving,
    Left,
    Expired,
    Revoked,
}

impl MemberStatus {
    fn live(self) -> bool {
        matches!(self, Self::Joining | Self::Active | Self::Leaving)
    }
    fn assigned(self) -> bool {
        matches!(self, Self::Joining | Self::Active)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemberInfo {
    pub member: Id,
    pub epoch: u64,
    pub client: Id,
    pub credential: Id,
    pub expires_ms: u64,
    pub status: MemberStatus,
    pub max_messages: u32,
    pub max_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemberJoin {
    pub member: Id,
    pub expected_epoch: u64,
    pub client: Id,
    pub credential: Id,
    pub now_ms: u64,
    pub credential_expires_ms: u64,
    pub max_messages: u32,
    pub max_bytes: u64,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Heartbeat {
    sequence: u64,
    operation: Id,
    digest: [u8; 32],
    expires_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct MemberRecord {
    info: MemberInfo,
    join_operation: Id,
    join_digest: [u8; 32],
    join_result: MemberInfo,
    heartbeat: Option<Heartbeat>,
    leave_operation: Option<Id>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MembersPlan {
    pub version: u64,
    pub layout_revision: u64,
    pub domains: Vec<Id>,
    pub installed: BTreeSet<Id>,
    pub active: bool,
    pub fenced: bool,
    pub grant_after_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MembersBook {
    subscription: Id,
    model: SubscriptionKind,
    initial: InitialPosition,
    next_epoch: u64,
    members: BTreeMap<(Id, u64), MemberRecord>,
    plan: MembersPlan,
    committed: BTreeMap<Id, crate::subscription::MemberGrant>,
    plan_grants: Vec<crate::subscription::MemberGrant>,
    lane_fences: Vec<u64>,
}

impl MembersBook {
    pub fn new(
        subscription: Id,
        model: SubscriptionKind,
        initial: InitialPosition,
        layout_revision: u64,
        domains: Vec<Id>,
    ) -> Result<Self, Rejection> {
        validate_domains(&domains)?;
        if layout_revision == 0 {
            return Err(Rejection::InvalidCommand);
        }
        Ok(Self {
            subscription,
            model,
            initial,
            next_epoch: 1,
            members: BTreeMap::new(),
            plan: MembersPlan {
                version: 1,
                layout_revision,
                domains,
                installed: BTreeSet::new(),
                active: false,
                fenced: false,
                grant_after_ms: 0,
            },
            committed: BTreeMap::new(),
            plan_grants: Vec::new(),
            lane_fences: if model == SubscriptionKind::Stream {
                vec![0; 64]
            } else {
                Vec::new()
            },
        })
    }

    pub fn model(&self) -> SubscriptionKind {
        self.model
    }
    pub fn initial_position(&self) -> InitialPosition {
        self.initial
    }
    pub fn next_epoch(&self) -> u64 {
        self.next_epoch
    }
    pub fn needs_refresh(&self, revision: u64, now: u64) -> bool {
        revision != self.plan.layout_revision
            || self
                .members
                .values()
                .any(|record| record.info.status.assigned() && record.info.expires_ms <= now)
    }
    pub fn plan(&self) -> &MembersPlan {
        &self.plan
    }
    pub fn lane_fences(&self) -> &[u64] {
        &self.lane_fences
    }
    pub fn member(&self, id: Id, epoch: u64) -> Option<&MemberInfo> {
        self.members.get(&(id, epoch)).map(|record| &record.info)
    }
    pub fn next_heartbeat_sequence(&self, id: Id, epoch: u64) -> Option<u64> {
        self.members.get(&(id, epoch)).and_then(|record| {
            record
                .heartbeat
                .as_ref()
                .map_or(Some(1), |heartbeat| heartbeat.sequence.checked_add(1))
        })
    }
    pub fn assigned(&self, now: u64) -> impl Iterator<Item = &MemberInfo> {
        self.members
            .values()
            .map(|record| &record.info)
            .filter(move |info| info.status.assigned() && info.expires_ms > now)
    }
    pub fn live_for_client(&self, client: Id, now: u64) -> usize {
        self.members
            .values()
            .filter(|record| {
                record.info.client == client
                    && record.info.status.live()
                    && record.info.expires_ms > now
            })
            .count()
    }

    pub(crate) fn can_delete(&self) -> bool {
        self.plan.active
            && self.committed.is_empty()
            && self.plan_grants.is_empty()
            && self
                .members
                .values()
                .all(|record| !record.info.status.live())
    }

    pub fn join(&mut self, operation: Id, request: &MemberJoin) -> Result<MemberInfo, Rejection> {
        if let Some(record) = self.members.get(&(request.member, request.expected_epoch)) {
            return if record.join_operation == operation
                && record.join_digest == request.digest
                && record.info.client == request.client
                && record.info.credential == request.credential
            {
                Ok(record.join_result.clone())
            } else {
                Err(Rejection::OperationConflict)
            };
        }
        if request.expected_epoch < self.next_epoch {
            if self.members.values().any(|record| {
                record.info.epoch == request.expected_epoch && record.info.member != request.member
            }) {
                return Err(Rejection::MemberEpochConflict {
                    expected: self.next_epoch,
                });
            }
            return Err(Rejection::Superseded);
        }
        if request.expected_epoch != self.next_epoch {
            return Err(Rejection::GenerationConflict);
        }
        if !self.plan.active {
            return Err(Rejection::RecoveryRequired);
        }
        let active = self
            .members
            .values()
            .filter(|record| record.info.status.live() && record.info.expires_ms > request.now_ms);
        if active
            .clone()
            .any(|record| record.info.member == request.member)
        {
            return Err(Rejection::NameInUse);
        }
        if active.count() >= 128 || self.members.len() >= 256 {
            return Err(Rejection::Capacity);
        }
        if request.max_messages == 0
            || request.max_messages > 256
            || request.max_bytes == 0
            || request.max_bytes > 8 * 1024 * 1024
            || request.credential_expires_ms <= request.now_ms
        {
            return Err(Rejection::InvalidSubscription);
        }
        let expires_ms = request
            .now_ms
            .checked_add(MEMBER_LEASE_MS)
            .ok_or(Rejection::CounterExhausted)?
            .min(request.credential_expires_ms);
        let next = self
            .next_epoch
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let version = self
            .plan
            .version
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let info = MemberInfo {
            member: request.member,
            epoch: self.next_epoch,
            client: request.client,
            credential: request.credential,
            expires_ms,
            status: MemberStatus::Joining,
            max_messages: request.max_messages,
            max_bytes: request.max_bytes,
        };
        self.members.insert(
            (info.member, info.epoch),
            MemberRecord {
                info: info.clone(),
                join_operation: operation,
                join_digest: request.digest,
                join_result: info.clone(),
                heartbeat: None,
                leave_operation: None,
            },
        );
        self.next_epoch = next;
        self.begin_plan(version, request.now_ms)?;
        Ok(info)
    }

    pub fn heartbeat(&mut self, operation: Id, request: MemberHeartbeat) -> Result<u64, Rejection> {
        let record = self
            .members
            .get_mut(&(request.member, request.epoch))
            .ok_or(Rejection::Superseded)?;
        if record.info.client != request.client || record.info.credential != request.credential {
            return Err(Rejection::InvalidEvidence);
        }
        if let Some(previous) = &record.heartbeat {
            if request.sequence < previous.sequence {
                return Err(Rejection::Superseded);
            }
            if request.sequence == previous.sequence {
                return if previous.operation == operation && previous.digest == request.digest {
                    Ok(previous.expires_ms)
                } else {
                    Err(Rejection::OperationConflict)
                };
            }
        }
        if !record.info.status.assigned() || record.info.expires_ms <= request.now_ms {
            return Err(Rejection::Superseded);
        }
        let expected = record
            .heartbeat
            .as_ref()
            .map_or(Some(1), |previous| previous.sequence.checked_add(1))
            .ok_or(Rejection::CounterExhausted)?;
        if request.sequence != expected || request.credential_expires_ms <= request.now_ms {
            return Err(Rejection::GenerationConflict);
        }
        let expires_ms = request
            .now_ms
            .checked_add(MEMBER_LEASE_MS)
            .ok_or(Rejection::CounterExhausted)?
            .min(request.credential_expires_ms);
        record.info.expires_ms = record.info.expires_ms.max(expires_ms);
        record.heartbeat = Some(Heartbeat {
            sequence: request.sequence,
            operation,
            digest: request.digest,
            expires_ms: record.info.expires_ms,
        });
        Ok(record.info.expires_ms)
    }

    pub fn leave(
        &mut self,
        operation: Id,
        member: Id,
        epoch: u64,
        client: Id,
        credential: Id,
        now: u64,
    ) -> Result<MembersPlan, Rejection> {
        let record = self
            .members
            .get(&(member, epoch))
            .ok_or(Rejection::Superseded)?;
        if record.info.client != client || record.info.credential != credential {
            return Err(Rejection::InvalidEvidence);
        }
        if record.leave_operation == Some(operation) {
            return Ok(self.plan.clone());
        }
        if !record.info.status.assigned() {
            return Err(Rejection::Superseded);
        }
        let version = self
            .plan
            .version
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let record = self
            .members
            .get_mut(&(member, epoch))
            .ok_or(Rejection::NotFound)?;
        record.info.status = MemberStatus::Leaving;
        record.leave_operation = Some(operation);
        self.begin_plan(version, now)?;
        Ok(self.plan.clone())
    }

    pub fn observe_installed(
        &mut self,
        version: u64,
        fenced: bool,
        domain: Id,
        now: u64,
    ) -> Result<MembersPlan, Rejection> {
        if version != self.plan.version || fenced != self.plan.fenced {
            return Err(Rejection::Superseded);
        }
        if !self.plan.domains.contains(&domain) {
            return Err(Rejection::InvalidEvidence);
        }
        if fenced && now < self.plan.grant_after_ms {
            return Err(Rejection::RecoveryRequired);
        }
        self.plan.installed.insert(domain);
        if self.plan.installed.len() == self.plan.domains.len() {
            if !self.plan.fenced {
                self.plan.fenced = true;
                self.plan.installed.clear();
                return Ok(self.plan.clone());
            }
            self.plan.active = true;
            for record in self.members.values_mut() {
                record.info.status = match record.info.status {
                    MemberStatus::Joining if record.info.expires_ms > now => MemberStatus::Active,
                    MemberStatus::Joining => MemberStatus::Expired,
                    MemberStatus::Leaving => MemberStatus::Left,
                    status => status,
                };
            }
            self.committed = self
                .plan_grants
                .clone()
                .into_iter()
                .map(|grant| (grant.member, grant))
                .collect();
        }
        Ok(self.plan.clone())
    }

    pub fn refresh(
        &mut self,
        layout_revision: u64,
        domains: Vec<Id>,
        now: u64,
    ) -> Result<MembersPlan, Rejection> {
        validate_domains(&domains)?;
        if layout_revision < self.plan.layout_revision {
            return Err(Rejection::Superseded);
        }
        let expired = self
            .members
            .values()
            .any(|record| record.info.status.assigned() && record.info.expires_ms <= now);
        if !expired && layout_revision == self.plan.layout_revision && domains == self.plan.domains
        {
            return Ok(self.plan.clone());
        }
        let version = self
            .plan
            .version
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        for record in self.members.values_mut() {
            if record.info.status.live() && record.info.expires_ms <= now {
                record.info.status = MemberStatus::Expired;
            }
        }
        self.plan.domains = domains;
        self.plan.layout_revision = layout_revision;
        self.begin_plan(version, now)?;
        Ok(self.plan.clone())
    }

    pub fn reap(&mut self, now: u64) -> usize {
        let before = self.members.len();
        self.members.retain(|_, record| {
            record.info.status.live()
                || record.info.expires_ms.saturating_add(MEMBER_RECOVERY_MS) > now
        });
        before.saturating_sub(self.members.len())
    }

    pub fn retire(
        &mut self,
        member: Id,
        epoch: u64,
        client: Id,
        credential: Id,
    ) -> Result<(), Rejection> {
        let Some(record) = self.members.get(&(member, epoch)) else {
            return if epoch > 0 && epoch < self.next_epoch {
                Ok(())
            } else {
                Err(Rejection::NotFound)
            };
        };
        if record.info.client != client || record.info.credential != credential {
            return Err(Rejection::InvalidEvidence);
        }
        if !self.plan.active
            || record.info.status.live()
            || self
                .plan_grants
                .iter()
                .any(|grant| grant.member == member && grant.epoch == epoch)
            || self
                .committed
                .get(&member)
                .is_some_and(|grant| grant.epoch == epoch)
        {
            return Err(Rejection::RecoveryRequired);
        }
        self.members.remove(&(member, epoch));
        Ok(())
    }

    fn begin_plan(&mut self, version: u64, now: u64) -> Result<(), Rejection> {
        // A partial full phase can issue deliveries before its last metadata observation.
        let issued: Vec<_> = if self.plan.fenced && now >= self.plan.grant_after_ms {
            self.plan_grants.clone()
        } else {
            self.committed.values().cloned().collect()
        };
        let desired = self.desired_grants(now)?;
        self.plan.grant_after_ms = self.record_revocations(&issued, &desired, version, now)?;
        self.committed = issued
            .into_iter()
            .filter_map(|mut grant| {
                let retained = desired
                    .iter()
                    .find(|new| new.member == grant.member && new.epoch == grant.epoch)?;
                grant.lanes.retain(|lane| retained.lanes.contains(lane));
                Some((grant.member, grant))
            })
            .collect();
        self.plan_grants = desired;
        self.plan.version = version;
        self.plan.active = false;
        self.plan.fenced = false;
        self.plan.installed.clear();
        Ok(())
    }

    fn record_revocations(
        &mut self,
        issued: &[crate::subscription::MemberGrant],
        desired: &[crate::subscription::MemberGrant],
        version: u64,
        now: u64,
    ) -> Result<u64, Rejection> {
        let fence_version = version.checked_mul(2).ok_or(Rejection::CounterExhausted)?;
        let mut fences = self.lane_fences.clone();
        let mut grant_after = if self.plan.active {
            now
        } else {
            self.plan.grant_after_ms.max(now)
        };
        if self.model == SubscriptionKind::Stream {
            for old in issued {
                let retained = desired
                    .iter()
                    .find(|grant| grant.member == old.member && grant.epoch == old.epoch);
                for lane in &old.lanes {
                    if retained.is_some_and(|grant| grant.lanes.contains(lane)) {
                        continue;
                    }
                    let index = usize::try_from(lane.start >> 58)
                        .map_err(|_| Rejection::InvalidEvidence)?;
                    let fence = fences.get_mut(index).ok_or(Rejection::InvalidEvidence)?;
                    *fence = (*fence).max(fence_version);
                    if let Some(member) = self.member(old.member, old.epoch)
                        && member.status.assigned()
                    {
                        // A disconnected SDK can deliver prefetch until its previous lease ends.
                        grant_after = grant_after.max(member.expires_ms);
                    }
                }
            }
        }
        self.lane_fences = fences;
        Ok(grant_after)
    }

    pub fn grants(&self, now: u64) -> Result<Vec<crate::subscription::MemberGrant>, Rejection> {
        let mut desired = self.plan_grants.clone();
        for grant in &mut desired {
            if let Some(member) = self.member(grant.member, grant.epoch) {
                grant.expires_ms = grant.expires_ms.max(member.expires_ms);
            }
        }
        if self.plan.fenced && now >= self.plan.grant_after_ms {
            return Ok(desired);
        }
        Ok(desired
            .into_iter()
            .filter_map(|mut grant| {
                let old = self
                    .committed
                    .get(&grant.member)
                    .filter(|old| old.epoch == grant.epoch)?;
                grant.lanes.retain(|lane| old.lanes.contains(lane));
                Some(grant)
            })
            .collect())
    }

    fn desired_grants(&self, now: u64) -> Result<Vec<crate::subscription::MemberGrant>, Rejection> {
        let lanes = self.lane_assignments(now)?;
        Ok(self
            .assigned(now)
            .map(|member| crate::subscription::MemberGrant {
                member: member.member,
                client: member.client,
                credential: member.credential,
                epoch: member.epoch,
                expires_ms: member.expires_ms,
                lanes: lanes.get(&member.member).cloned().unwrap_or_default(),
                max_messages: member.max_messages,
                max_bytes: member.max_bytes,
            })
            .collect())
    }

    pub fn lane_assignments(&self, now: u64) -> Result<BTreeMap<Id, Vec<HashRange>>, Rejection> {
        let members: Vec<_> = self.assigned(now).collect();
        let mut result = BTreeMap::new();
        if members.is_empty() || self.model == SubscriptionKind::Queue {
            return Ok(result);
        }
        for index in 0..64_u64 {
            let end = if index == 63 {
                None
            } else {
                Some((index + 1) << 58)
            };
            let lane = HashRange {
                start: index << 58,
                end,
            };
            let owner = members
                .iter()
                .max_by_key(|member| {
                    let mut hash = blake3::Hasher::new();
                    hash.update(b"tidemq/stream-lane/v1\0");
                    hash.update(&index.to_be_bytes());
                    hash.update(member.member.as_bytes());
                    *hash.finalize().as_bytes()
                })
                .ok_or(Rejection::InvalidEvidence)?;
            result
                .entry(owner.member)
                .or_insert_with(Vec::new)
                .push(lane);
        }
        Ok(result)
    }

    pub fn queue_worksets(&self, now: u64) -> Result<BTreeMap<Id, Vec<Id>>, Rejection> {
        let members: Vec<_> = self.assigned(now).collect();
        let mut result = BTreeMap::new();
        if members.is_empty() || self.model != SubscriptionKind::Queue {
            return Ok(result);
        }
        let count = self.plan.domains.len();
        for (index, member) in members.iter().enumerate() {
            let start = index
                .checked_mul(count)
                .ok_or(Rejection::CounterExhausted)?
                / members.len();
            let end = (index + 1)
                .checked_mul(count)
                .ok_or(Rejection::CounterExhausted)?
                / members.len();
            let mut domains = self
                .plan
                .domains
                .get(start..end)
                .ok_or(Rejection::InvalidEvidence)?
                .to_vec();
            let extra = self
                .plan
                .domains
                .get(end % count)
                .copied()
                .ok_or(Rejection::InvalidEvidence)?;
            if !domains.contains(&extra) {
                domains.push(extra);
            }
            result.insert(member.member, domains);
        }
        Ok(result)
    }

    pub fn validate(&self) -> Result<(), Rejection> {
        validate_domains(&self.plan.domains)?;
        if self.next_epoch == 0
            || self.plan.version == 0
            || self.lane_fences.len()
                != if self.model == SubscriptionKind::Stream {
                    64
                } else {
                    0
                }
            || self
                .plan
                .version
                .checked_mul(2)
                .is_none_or(|version| self.lane_fences.iter().any(|fence| *fence > version))
            || self.members.len() > 256
            || self.plan.layout_revision == 0
            || self.plan.active
                && self.plan.version > 0
                && self.plan.installed.len() != self.plan.domains.len()
            || self
                .plan
                .installed
                .iter()
                .any(|domain| !self.plan.domains.contains(domain))
        {
            return Err(Rejection::InvalidEvidence);
        }
        let mut live = BTreeSet::new();
        for ((id, epoch), record) in &self.members {
            if *id != record.info.member
                || *epoch != record.info.epoch
                || *epoch == 0
                || *epoch >= self.next_epoch
                || record.info.max_messages == 0
                || record.info.max_bytes == 0
                || record.info.expires_ms == 0
                || record.info.status.assigned() && !live.insert(*id)
            {
                return Err(Rejection::InvalidEvidence);
            }
        }
        if live.len() > 128 {
            return Err(Rejection::Capacity);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemberHeartbeat {
    pub member: Id,
    pub epoch: u64,
    pub client: Id,
    pub credential: Id,
    pub sequence: u64,
    pub now_ms: u64,
    pub credential_expires_ms: u64,
    pub digest: [u8; 32],
}

fn validate_domains(domains: &[Id]) -> Result<(), Rejection> {
    if domains.is_empty()
        || domains.len() > 256
        || domains.iter().collect::<BTreeSet<_>>().len() != domains.len()
    {
        return Err(Rejection::InvalidSubscription);
    }
    Ok(())
}
