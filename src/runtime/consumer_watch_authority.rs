//! Quorum member/plan authorization for observations; never renews a data delivery grant.

use super::{
    Error, Result,
    service::{Service, forbidden},
    wire::{self, Request},
};
use crate::{
    gateway::Access,
    metadata::{MemberStatus, PartitionKey, Route},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::{Id, public};
use tokio::time::Instant;

tokio::task_local! {
    static DEFERRED_AUTHORITY: bool;
}

pub(super) const AUTHORIZE: u32 = 1_014;

#[derive(Serialize, Deserialize)]
struct Membership {
    topic: Id,
    subscription: Id,
    member: Id,
    epoch: u64,
    plan: u64,
    client: Id,
    credential: Id,
    #[serde(deserialize_with = "bounded_targets")]
    targets: Vec<(Id, u32)>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Permit {
    #[serde(deserialize_with = "bounded_targets")]
    pub targets: Vec<(Id, Route)>,
    pub installed_version: u64,
    pub remaining_ms: u64,
    pub tenant: Option<Id>,
}

fn bounded_targets<'de, D, T>(deserializer: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    struct Targets<T>(std::marker::PhantomData<T>);
    impl<'de, T: serde::Deserialize<'de>> serde::de::Visitor<'de> for Targets<T> {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("at most 32 watch authority targets")
        }
        fn visit_seq<S: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: S,
        ) -> std::result::Result<Vec<T>, S::Error> {
            use serde::de::Error;
            let count = sequence.size_hint().unwrap_or(0);
            if count > 32 {
                return Err(S::Error::custom("Watch authority target count exceeds 32"));
            }
            let mut targets = Vec::new();
            targets.try_reserve_exact(count).map_err(S::Error::custom)?;
            while let Some(target) = sequence.next_element()? {
                if targets.len() >= 32 {
                    return Err(S::Error::custom("Watch authority target count exceeds 32"));
                }
                targets.push(target);
            }
            Ok(targets)
        }
    }
    deserializer.deserialize_seq(Targets(std::marker::PhantomData))
}

impl Service {
    pub(super) fn defer_watch_authority(
        self: &std::sync::Arc<Self>,
        peer: crate::transport::Authorization,
        frame: &tidemq_protocol::common::Frame,
        deadline: Instant,
    ) -> Option<crate::server::DeferredReply<Error>> {
        use tidemq_protocol::{
            common::FrameKind, framing::decode_payload, internal::InternalCommand,
        };
        if !matches!(peer, crate::transport::Authorization::Internal(_))
            || frame.kind != i32::from(FrameKind::InternalCommand)
        {
            return None;
        }
        let envelope: InternalCommand =
            decode_payload(frame.payload.clone(), &self.config.protocol).ok()?;
        let request: Request =
            decode_payload(envelope.encoded_command, &self.config.protocol).ok()?;
        if request.kind != AUTHORIZE {
            return None;
        }
        let membership: Membership = wire::decode_binary(&request.body).ok()?;
        let slot = self
            .watch_budget
            .reserve_authority(None, membership.member, true)?;
        let charge = self.watch_budget.reserve(
            None,
            membership.member,
            0,
            16_384_usize.saturating_add(frame.payload.len().saturating_mul(2)),
            false,
        )?;
        let compact = tidemq_protocol::common::Frame {
            request_id: Bytes::copy_from_slice(&frame.request_id),
            operation_id: Bytes::copy_from_slice(&frame.operation_id),
            payload: Bytes::copy_from_slice(&frame.payload),
            ..frame.clone()
        };
        let service = self.clone();
        Some(Box::pin(async move {
            let _slot = slot;
            let response = DEFERRED_AUTHORITY
                .scope(
                    true,
                    crate::server::WireService::handle(
                        service.as_ref(),
                        peer,
                        &compact,
                        deadline,
                        None,
                    ),
                )
                .await?;
            Ok(response.map(|response| super::consumer_watch::charge_reply(response, charge)))
        }))
    }

    pub(super) async fn watch_authorize(
        &self,
        access: &Access,
        watch: &public::GroupWatch,
        deadline: Instant,
    ) -> Result<(Permit, Instant)> {
        let (client, credential) = super::consumer_control::actor(access)?;
        let _authorization = self
            .watch_budget
            .reserve_authority(access.tenant, wire::id(&watch.member_id)?, false)
            .ok_or_else(|| self.watch_overloaded())?;
        let request = Membership {
            topic: wire::id(&watch.topic_id)?,
            subscription: wire::id(&watch.subscription_id)?,
            member: wire::id(&watch.member_id)?,
            epoch: watch.member_epoch,
            plan: watch.assignment_version,
            client,
            credential,
            targets: watch
                .targets
                .iter()
                .map(|target| Ok((wire::id(&target.shard_id)?, target.birth_partition)))
                .collect::<Result<_>>()?,
        };
        let started = Instant::now();
        let leader = self
            .leader(self.config.runtime.metadata_group, deadline)
            .await?;
        let bytes = self
            .rpc
            .call(
                leader,
                self.config.runtime.metadata_group,
                Request::typed(AUTHORIZE, &request)?,
                deadline,
            )
            .await?;
        let permit: Permit = wire::decode_binary(&bytes)?;
        if permit.remaining_ms == 0
            || permit.remaining_ms > 1_000
            || permit.targets.len() != request.targets.len()
            || permit.tenant != access.tenant
            || permit.targets.iter().zip(&request.targets).any(
                |((shard, route), (expected, partition))| {
                    shard != expected
                        || route.partition
                            != (PartitionKey {
                                topic_id: request.topic,
                                partition: *partition,
                            })
                },
            )
        {
            return Err(forbidden());
        }
        let expires = started
            .checked_add(std::time::Duration::from_millis(permit.remaining_ms))
            .ok_or_else(Error::unavailable)?;
        if expires <= Instant::now() {
            return Err(Error::unavailable());
        }
        Ok((permit, expires))
    }

    pub(super) async fn watch_authority_command(
        &self,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let request: Membership = wire::decode_binary(bytes)?;
        if !(1..=32).contains(&request.targets.len()) {
            return Err(Error::invalid(
                "Watch authorization target count is invalid",
            ));
        }
        if !DEFERRED_AUTHORITY
            .try_with(|deferred| *deferred)
            .unwrap_or(false)
        {
            return Err(self.watch_overloaded());
        }
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        #[cfg(feature = "test-hooks")]
        super::watch_test_hooks::before_authority_quorum(
            self.cluster,
            self.config.node_id,
            deadline,
        )
        .await;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        let now = super::data::unix_millis()?;
        let permit = replica
            .state
            .try_with_application(|state| {
                state
                    .auth()
                    .authorize_topic(
                        request.credential,
                        now,
                        crate::metadata::Permission::Consume,
                        state.topic(request.topic).ok_or_else(forbidden)?,
                    )
                    .map_err(|_| forbidden())?;
                let credential_expires = state
                    .auth()
                    .credential(request.credential)
                    .ok_or_else(forbidden)?
                    .expires_ms;
                let row = state
                    .consumer_subscription(request.subscription)
                    .filter(|row| row.topic == request.topic && row.deletion.is_none())
                    .ok_or_else(forbidden)?;
                let plan = row.members.plan();
                if !plan.active || plan.version != request.plan {
                    return Err(Error::unavailable());
                }
                let member = row
                    .members
                    .member(request.member, request.epoch)
                    .filter(|member| {
                        member.client == request.client
                            && member.credential == request.credential
                            && member.status == MemberStatus::Active
                            && member.expires_ms > now
                    })
                    .ok_or_else(forbidden)?;
                let work = if row.members.model() == tidemq_protocol::topic::SubscriptionKind::Queue
                {
                    row.members
                        .queue_worksets(now)
                        .map_err(|_| Error::unavailable())?
                        .remove(&request.member)
                        .unwrap_or_default()
                } else {
                    plan.domains.clone()
                };
                let mut targets = Vec::with_capacity(request.targets.len());
                for (shard, partition) in &request.targets {
                    let birth = state
                        .topology()
                        .shard(*shard)
                        .filter(|birth| {
                            birth.partition
                                == (PartitionKey {
                                    topic_id: request.topic,
                                    partition: *partition,
                                })
                                && work.contains(shard)
                        })
                        .ok_or_else(forbidden)?;
                    let route = state
                        .route(birth.partition)
                        .cloned()
                        .ok_or_else(Error::unavailable)?;
                    targets.push((*shard, route));
                }
                let installed_version = plan
                    .version
                    .checked_mul(2)
                    .and_then(|version| version.checked_add(u64::from(plan.fenced)))
                    .ok_or_else(Error::unavailable)?;
                Ok(Permit {
                    targets,
                    installed_version,
                    tenant: state
                        .auth()
                        .credential_scope(request.credential, now)
                        .map_err(|_| forbidden())?
                        .0,
                    remaining_ms: member
                        .expires_ms
                        .min(credential_expires)
                        .saturating_sub(now)
                        .min(1_000),
                })
            })
            .await
            .map_err(|source| Error::operation("authorize read-only watch member", source))??;
        wire::binary(&permit)
    }
}
