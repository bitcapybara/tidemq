use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::{
    Id,
    common::{ErrorCode, Frame, FrameKind, Outcome},
    framing::decode_payload,
    public,
};
use tokio::time::Instant;

use super::{
    Error, Result,
    service::Service,
    wire::{self, Request},
};
use crate::{
    gateway::Access,
    metadata::{AuthRejection, Permission, TokenDigest},
    transport::{CredentialGrant, TokenAuthenticator},
};

pub(super) const AUTH_QUERY: u32 = 1_002;
const LEASE: Duration = Duration::from_secs(1);

pub(super) fn admit_deadline(deadline: Instant, permission_expires: Instant) -> Result<Instant> {
    if permission_expires <= Instant::now() {
        return Err(Error::unavailable());
    }
    wire::remaining(deadline)?;
    // An admitted operation keeps its original finite completion deadline.
    Ok(deadline)
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    #[test]
    fn a_valid_permission_lease_does_not_shorten_an_admitted_operation() {
        let now = Instant::now();
        let original = now + Duration::from_secs(30);
        assert_eq!(
            admit_deadline(original, now + Duration::from_secs(1)).unwrap(),
            original
        );
        assert!(admit_deadline(original, now).is_err());
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(super) struct MemberLeaseRequest {
    pub topic: Id,
    pub subscription: Id,
    pub member: Id,
    pub epoch: u64,
    pub version: u64,
    pub client: Id,
    pub credential: Id,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct DeletionPermit {
    pub subscription: Id,
    pub deletion: Id,
    pub domain: Id,
    pub route: crate::metadata::Route,
    pub now_ms: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
enum AuthQuery {
    PublisherRecovery {
        topic: Id,
        producer: Id,
        registration: u64,
        client: Id,
        credential: Id,
    },
    Consumer(MemberLeaseRequest),
    ConsumerDeletion {
        subscription: Id,
        deletion: Id,
        domain: Id,
    },
    BirthShard(Id),
    TopicResource(Id),
    Publisher {
        topic: Id,
        producer: Id,
        registration: u64,
        client: Id,
        credential: Id,
    },
    Ready,
    Authenticate(TokenDigest),
    Authorize {
        credential: Id,
        permission: Permission,
        name: Option<String>,
    },
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct AuthReply {
    deletion: Option<DeletionPermit>,
    consumer_expires_ms: u64,
    birth: Option<crate::metadata::BirthShard>,
    resource: Option<String>,
    publisher: Option<crate::producer::PublisherPermit>,
    credential: Option<Id>,
    expires_ms: u64,
    now_ms: u64,
}

#[derive(Default)]
pub(super) struct AuthCache(Mutex<BTreeMap<AuthQuery, (AuthReply, Instant)>>);

pub(super) struct Authenticator(pub Arc<OnceLock<Weak<Service>>>);

impl TokenAuthenticator for Authenticator {
    fn authenticate<'a>(
        &'a self,
        token: &'a str,
        deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = crate::transport::Result<CredentialGrant>> + Send + 'a>> {
        Box::pin(async move {
            let service = self.0.get().and_then(Weak::upgrade).ok_or(
                crate::transport::Error::Authentication {
                    reason: "authentication service is not ready",
                },
            )?;
            let digest =
                TokenDigest::of(token).map_err(|_| crate::transport::Error::Authentication {
                    reason: "access token rejected",
                })?;
            let (reply, _) = service
                .auth_query(AuthQuery::Authenticate(digest), deadline)
                .await
                .map_err(|_| crate::transport::Error::Authentication {
                    reason: "access token rejected or authentication authority is unavailable",
                })?;
            let kinds = (1..=128)
                .filter_map(|value| FrameKind::try_from(value).ok())
                .filter(|kind| crate::gateway::external_kind(*kind))
                .collect();
            let credential = reply
                .credential
                .ok_or(crate::transport::Error::Authentication {
                    reason: "credential authority omitted its identity",
                })?;
            Ok(CredentialGrant {
                credential,
                allowed_kinds: kinds,
            })
        })
    }
}

impl Service {
    pub(super) async fn consumer_deletion_permit(
        &self,
        subscription: Id,
        deletion: Id,
        domain: Id,
        deadline: Instant,
    ) -> Result<DeletionPermit> {
        let (reply, expires) = self
            .auth_query(
                AuthQuery::ConsumerDeletion {
                    subscription,
                    deletion,
                    domain,
                },
                deadline,
            )
            .await?;
        admit_deadline(deadline, expires)?;
        reply
            .deletion
            .filter(|permit| {
                permit.subscription == subscription
                    && permit.deletion == deletion
                    && permit.domain == domain
            })
            .ok_or_else(super::service::forbidden)
    }

    pub(super) async fn publisher_recovery_permit(
        &self,
        permit: &crate::producer::PublisherPermit,
        deadline: Instant,
    ) -> Result<Option<crate::producer::PublisherPermit>> {
        let (reply, _) = self
            .auth_query(
                AuthQuery::PublisherRecovery {
                    topic: permit.topic,
                    producer: permit.producer,
                    registration: permit.registration,
                    client: permit.client,
                    credential: permit.credential,
                },
                deadline,
            )
            .await?;
        Ok(reply.publisher)
    }
    pub(super) async fn consumer_member_lease(
        &self,
        request: MemberLeaseRequest,
        deadline: Instant,
    ) -> Result<u64> {
        let (reply, _) = self
            .auth_query(AuthQuery::Consumer(request), deadline)
            .await?;
        Ok(reply.consumer_expires_ms)
    }
    pub(super) async fn publisher_birth(
        &self,
        shard: Id,
        partition: crate::metadata::PartitionKey,
        deadline: Instant,
    ) -> Result<tidemq_protocol::topic::HashRange> {
        let (reply, _) = self
            .auth_query(AuthQuery::BirthShard(shard), deadline)
            .await?;
        let birth = reply
            .birth
            .filter(|birth| birth.partition == partition)
            .ok_or_else(super::service::forbidden)?;
        Ok(birth.route.range)
    }
    pub(super) async fn publisher_permit(
        &self,
        access: &Access,
        topic: Id,
        producer: Id,
        registration: u64,
        deadline: Instant,
    ) -> Result<crate::producer::PublisherPermit> {
        let (client, credential) = super::consumer_control::actor(access)?;
        let (reply, _) = self
            .auth_query(
                AuthQuery::Publisher {
                    topic,
                    producer,
                    registration,
                    client,
                    credential,
                },
                deadline,
            )
            .await?;
        reply.publisher.ok_or_else(super::service::forbidden)
    }
    pub(super) async fn auth_ready(&self, deadline: Instant) -> bool {
        self.auth_query(AuthQuery::Ready, deadline).await.is_ok()
    }

    pub(super) async fn bootstrap_credentials(&self, deadline: Instant) -> Result<()> {
        use crate::metadata::{AuthAction, AuthCommand, Command};
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        if replica
            .state
            .try_with_application(|state| state.auth().initialized())
            .await
            .map_err(|source| Error::operation("read authentication bootstrap state", source))?
        {
            return Ok(());
        }
        let file = self.config.runtime.bootstrap_admin_token_file.as_ref().ok_or_else(||
            Error::invalid("Uninitialized authentication requires a controlled bootstrap token file on metadata nodes"))?;
        let bytes = super::read_secret(file, 258).await?;
        let token = std::str::from_utf8(&bytes)
            .map_err(|_| Error::invalid("Bootstrap token must be UTF-8"))?
            .trim_end_matches(['\r', '\n']);
        let digest = TokenDigest::of(token).map_err(auth_rejection)?;
        let now_ms = super::data::unix_millis()?;
        let expires_ms = now_ms
            .checked_add(30 * 24 * 60 * 60 * 1000)
            .ok_or_else(Error::unavailable)?;
        self.meta_write(
            Command::identity(
                Id::new_v7()
                    .map_err(|source| Error::operation("allocate bootstrap operation", source))?,
                self.meta_revision().await?,
                AuthCommand {
                    actor: None,
                    now_ms,
                    request_digest: *blake3::hash(b"tidemq-bootstrap").as_bytes(),
                    action: AuthAction::Bootstrap {
                        principal: Id::new_v7().map_err(|source| {
                            Error::operation("allocate bootstrap principal", source)
                        })?,
                        credential: Id::new_v7().map_err(|source| {
                            Error::operation("allocate bootstrap credential", source)
                        })?,
                        digest,
                        expires_ms,
                    },
                },
            ),
            deadline,
        )
        .await?;
        Ok(())
    }
    async fn auth_query(
        &self,
        query: AuthQuery,
        deadline: Instant,
    ) -> Result<(AuthReply, Instant)> {
        let cacheable = !matches!(query, AuthQuery::ConsumerDeletion { .. });
        if cacheable
            && let Some((reply, expires)) = self
                .auth_cache
                .0
                .lock()
                .map_err(|_| Error::unavailable())?
                .get(&query)
                .filter(|(_, expires)| *expires > Instant::now())
                .cloned()
        {
            return Ok((reply, expires));
        }
        let started = Instant::now();
        let leader = self
            .leader(self.config.runtime.metadata_group, deadline)
            .await?;
        let bytes = self
            .rpc
            .call(
                leader,
                self.config.runtime.metadata_group,
                Request::control(AUTH_QUERY, &query)?,
                deadline,
            )
            .await?;
        let reply: AuthReply = wire::decode_json(&bytes)?;
        let remaining =
            Duration::from_millis(reply.expires_ms.saturating_sub(reply.now_ms)).min(LEASE);
        let expires = started
            .checked_add(remaining)
            .ok_or_else(Error::unavailable)?;
        if expires <= Instant::now() {
            return Err(Error::unavailable());
        }
        if !cacheable {
            return Ok((reply, expires));
        }
        let mut cache = self.auth_cache.0.lock().map_err(|_| Error::unavailable())?;
        if cache.len() >= 2048 {
            cache.retain(|_, (_, expires)| *expires > Instant::now());
        }
        if cache.len() >= 2048 {
            return Err(Error::reject(
                ErrorCode::QuotaExceeded,
                Outcome::Rejected,
                true,
            ));
        }
        cache.insert(query, (reply.clone(), expires));
        Ok((reply, expires))
    }

    pub(super) async fn auth_command(&self, bytes: &[u8], deadline: Instant) -> Result<Bytes> {
        let query: AuthQuery = wire::decode_json(bytes)?;
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        let now = super::data::unix_millis()?;
        let reply = replica
            .state
            .try_with_application(|state| match query {
                AuthQuery::ConsumerDeletion {
                    subscription,
                    deletion,
                    domain,
                } => {
                    let plan = state
                        .consumer_subscription(subscription)
                        .and_then(|row| row.deletion.as_ref())
                        .filter(|plan| plan.id == deletion && plan.domains.contains(&domain))
                        .ok_or_else(super::service::forbidden)?;
                    let birth = state
                        .topology()
                        .shard(domain)
                        .filter(|birth| birth.partition.topic_id == plan.topic)
                        .ok_or_else(super::service::forbidden)?;
                    let route = state
                        .route(birth.partition)
                        .cloned()
                        .ok_or_else(Error::unavailable)?;
                    Ok(AuthReply {
                        deletion: Some(DeletionPermit {
                            subscription,
                            deletion,
                            domain,
                            route,
                            now_ms: now,
                        }),
                        expires_ms: now.saturating_add(1000),
                        now_ms: now,
                        ..Default::default()
                    })
                }
                AuthQuery::BirthShard(shard) => {
                    let birth = state
                        .topology()
                        .shard(shard)
                        .cloned()
                        .ok_or_else(super::service::forbidden)?;
                    Ok(AuthReply {
                        birth: Some(birth),
                        expires_ms: now.saturating_add(1000),
                        now_ms: now,
                        ..Default::default()
                    })
                }
                AuthQuery::TopicResource(topic) => {
                    let topic = state
                        .topic(topic)
                        .filter(|topic| topic.status == crate::metadata::RecordStatus::Active)
                        .ok_or_else(super::service::forbidden)?;
                    Ok(AuthReply {
                        resource: Some(topic.name.clone()),
                        expires_ms: now.saturating_add(1000),
                        now_ms: now,
                        ..Default::default()
                    })
                }
                AuthQuery::PublisherRecovery {
                    topic,
                    producer,
                    registration,
                    client,
                    credential,
                } => {
                    if registration == 0 || registration >= state.publishers().next_registration() {
                        return Err(super::service::forbidden());
                    }
                    let record = state.publishers().record(producer, registration);
                    if record.is_some_and(|record| {
                        record.permit.topic != topic
                            || record.permit.client != client
                            || record.permit.credential != credential
                    }) {
                        return Err(super::service::forbidden());
                    }
                    Ok(AuthReply {
                        publisher: record.map(|record| record.permit_at(now)),
                        expires_ms: now.saturating_add(1000),
                        now_ms: now,
                        ..Default::default()
                    })
                }
                AuthQuery::Consumer(request) => {
                    // Data keeps its exact installed version while a membership plan is pending.
                    let consumer_expires_ms = state
                        .consumer_subscription(request.subscription)
                        .filter(|row| {
                            row.deletion.is_none()
                                && row.topic == request.topic
                                && request.version > 0
                                && row
                                    .members
                                    .plan()
                                    .version
                                    .checked_mul(2)
                                    .and_then(|version| version.checked_add(1))
                                    .is_some_and(|current| request.version <= current)
                        })
                        .and_then(|row| row.members.member(request.member, request.epoch))
                        .filter(|member| {
                            member.client == request.client
                                && member.credential == request.credential
                                && member.status == crate::metadata::MemberStatus::Active
                                && member.expires_ms > now
                        })
                        .map_or(0, |member| member.expires_ms);
                    Ok(AuthReply {
                        consumer_expires_ms,
                        expires_ms: now.saturating_add(1000),
                        now_ms: now,
                        ..Default::default()
                    })
                }
                AuthQuery::Publisher {
                    topic,
                    producer,
                    registration,
                    client,
                    credential,
                } => {
                    let topic_record = state
                        .topic(topic)
                        .filter(|topic| topic.status == crate::metadata::RecordStatus::Active)
                        .ok_or_else(super::service::forbidden)?;
                    state
                        .auth()
                        .authorize(
                            credential,
                            now,
                            Permission::Produce,
                            Some(&topic_record.name),
                        )
                        .map_err(auth_rejection)?;
                    let record = state
                        .publishers()
                        .record(producer, registration)
                        .ok_or_else(|| {
                            Error::reject(ErrorCode::SessionExpired, Outcome::Rejected, false)
                        })?;
                    if record.permit.topic != topic
                        || record.permit.client != client
                        || record.permit.credential != credential
                    {
                        return Err(super::service::forbidden());
                    }
                    let permit = record.permit_at(now);
                    let expires_ms = if permit.live {
                        permit.expires_ms.min(now.saturating_add(1000))
                    } else {
                        now.saturating_add(1000)
                    };
                    Ok(AuthReply {
                        publisher: Some(permit),
                        credential: Some(credential),
                        expires_ms,
                        now_ms: now,
                        ..Default::default()
                    })
                }
                AuthQuery::Ready => {
                    if !state.auth().initialized() {
                        return Err(Error::unavailable());
                    }
                    Ok(AuthReply {
                        credential: None,
                        expires_ms: now.saturating_add(1000),
                        now_ms: now,
                        ..Default::default()
                    })
                }
                AuthQuery::Authenticate(digest) => {
                    let credential = state
                        .auth()
                        .authenticate(digest, now)
                        .map_err(auth_rejection)?;
                    Ok(AuthReply {
                        credential: Some(credential.id),
                        expires_ms: credential.expires_ms,
                        now_ms: now,
                        ..Default::default()
                    })
                }
                AuthQuery::Authorize {
                    credential,
                    permission,
                    name,
                } => {
                    state
                        .auth()
                        .authorize(credential, now, permission, name.as_deref())
                        .map_err(auth_rejection)?;
                    let record = state
                        .auth()
                        .credential(credential)
                        .ok_or_else(super::service::forbidden)?;
                    Ok(AuthReply {
                        credential: Some(credential),
                        expires_ms: record.expires_ms,
                        now_ms: now,
                        ..Default::default()
                    })
                }
            })
            .await
            .map_err(|source| Error::operation("read credential authority", source))??;
        wire::json(&reply)
    }

    pub(super) async fn dynamic_access(
        &self,
        credential: Id,
        session: Id,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<(Access, Instant)> {
        let kind = super::service::frame_kind(frame)?;
        if !crate::gateway::external_kind(kind) {
            return Err(super::service::forbidden());
        }
        let permission = match kind {
            FrameKind::SubscriptionAdmin => match decode::<public::SubscriptionAdmin>(self, frame)?
                .action
            {
                value
                    if value == i32::from(public::SubscriptionAdminAction::InspectSubscription) =>
                {
                    Permission::Inspect
                }
                value
                    if value == i32::from(public::SubscriptionAdminAction::RemoveSubscription) =>
                {
                    Permission::ManageTopics
                }
                _ => {
                    return Err(Error::invalid(
                        "Subscription administration action is required",
                    ));
                }
            },
            FrameKind::ManageIdentity => Permission::ManageCredentials,
            FrameKind::GetTopic | FrameKind::GetTopicLayout | FrameKind::Cancel => {
                Permission::Inspect
            }
            FrameKind::CreateLogicalTopic | FrameKind::ChangeTopicLayout => {
                Permission::ManageTopics
            }
            FrameKind::PublisherControl | FrameKind::Publish | FrameKind::RetirePublishes => {
                Permission::Produce
            }
            FrameKind::Diagnose => Permission::ManageTenants,
            _ => Permission::Consume,
        };
        let name = self.auth_resource_name(frame, deadline).await?;
        let (_, expires) = self
            .auth_query(
                AuthQuery::Authorize {
                    credential,
                    permission,
                    name,
                },
                deadline,
            )
            .await?;
        Ok((
            Access {
                principal: Arc::from(format!("credential:{credential}")),
                scope: Arc::from("*"),
                session: Some(session),
            },
            expires,
        ))
    }

    async fn auth_resource_name(&self, frame: &Frame, deadline: Instant) -> Result<Option<String>> {
        let kind = super::service::frame_kind(frame)?;
        let topic = match kind {
            FrameKind::CreateLogicalTopic => {
                return Ok(Some(decode::<public::LogicalTopic>(self, frame)?.name));
            }
            FrameKind::GetTopic => return Ok(Some(decode::<public::GetTopic>(self, frame)?.name)),
            FrameKind::GetTopicLayout => {
                wire::id(&decode::<public::GetTopicLayout>(self, frame)?.topic_id)?
            }
            FrameKind::ChangeTopicLayout => {
                wire::id(&decode::<public::ChangeTopicLayout>(self, frame)?.topic_id)?
            }
            FrameKind::ConsumerControl => {
                wire::id(&decode::<public::ConsumerControl>(self, frame)?.topic_id)?
            }
            FrameKind::SubscriptionAdmin => {
                wire::id(&decode::<public::SubscriptionAdmin>(self, frame)?.topic_id)?
            }
            FrameKind::PublisherControl => {
                wire::id(&decode::<public::PublisherControl>(self, frame)?.topic_id)?
            }
            FrameKind::Cancel | FrameKind::Diagnose | FrameKind::ManageIdentity => return Ok(None),
            _ => super::data::partition(frame, &self.config.protocol)?.topic_id,
        };
        let (reply, _) = self
            .auth_query(AuthQuery::TopicResource(topic), deadline)
            .await?;
        Ok(Some(reply.resource.ok_or_else(super::service::forbidden)?))
    }
}

fn decode<M: prost::Message + Default>(service: &Service, frame: &Frame) -> Result<M> {
    decode_payload(frame.payload.clone(), &service.config.protocol)
        .map_err(|source| Error::operation("decode authorization resource", source))
}

fn auth_rejection(_: AuthRejection) -> Error {
    super::service::forbidden()
}
