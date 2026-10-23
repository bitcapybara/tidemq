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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
enum AuthQuery {
    Ready,
    Authenticate(TokenDigest),
    Authorize {
        credential: Id,
        permission: Permission,
        name: Option<String>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
struct AuthReply {
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
    pub(super) async fn auth_ready(&self, deadline: Instant) -> bool {
        self.auth_query(AuthQuery::Ready, deadline).await.is_ok()
    }

    pub(super) async fn bootstrap_credentials(&self, deadline: Instant) -> Result<()> {
        use crate::metadata::{AuthAction, AuthCommand, Command};
        if !self.config.runtime.dynamic_auth {
            return Ok(());
        }
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
        if let Some((reply, expires)) = self
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
                AuthQuery::Ready => {
                    if !state.auth().initialized() {
                        return Err(Error::unavailable());
                    }
                    Ok(AuthReply {
                        credential: None,
                        expires_ms: now.saturating_add(1000),
                        now_ms: now,
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
        let permission = match kind {
            FrameKind::ManageIdentity => Permission::ManageCredentials,
            FrameKind::GetTopic
            | FrameKind::GetTopicLayout
            | FrameKind::GetSubscription
            | FrameKind::Cancel => Permission::Inspect,
            FrameKind::CreateTopic
            | FrameKind::CreateLogicalTopic
            | FrameKind::CreateSubscription
            | FrameKind::DeleteSubscription
            | FrameKind::SeekSubscription
            | FrameKind::ChangeTopicLayout => Permission::ManageTopics,
            FrameKind::ProduceBatch
            | FrameKind::OpenProducer
            | FrameKind::ProducerStatus
            | FrameKind::RetireProducer => Permission::Produce,
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
        use crate::ownership::remote::{Query, Value};
        let kind = super::service::frame_kind(frame)?;
        let topic = match kind {
            FrameKind::CreateLogicalTopic => {
                return Ok(Some(decode::<public::LogicalTopic>(self, frame)?.name));
            }
            FrameKind::CreateTopic => {
                return Ok(Some(decode::<public::CreateTopic>(self, frame)?.name));
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
            FrameKind::CreateSubscription => {
                wire::id(&decode::<public::CreateSubscription>(self, frame)?.topic_id)?
            }
            FrameKind::GetSubscription => {
                wire::id(&decode::<public::GetSubscription>(self, frame)?.topic_id)?
            }
            FrameKind::Cancel | FrameKind::Diagnose | FrameKind::ManageIdentity => return Ok(None),
            _ => super::data::partition(frame, &self.config.protocol)?.topic_id,
        };
        let Value::Topic(Some(record)) =
            self.authority.query(Query::Topic(topic), deadline).await?
        else {
            return Err(super::service::forbidden());
        };
        Ok(Some(record.name))
    }
}

fn decode<M: prost::Message + Default>(service: &Service, frame: &Frame) -> Result<M> {
    decode_payload(frame.payload.clone(), &service.config.protocol)
        .map_err(|source| Error::operation("decode authorization resource", source))
}

fn auth_rejection(_: AuthRejection) -> Error {
    super::service::forbidden()
}
