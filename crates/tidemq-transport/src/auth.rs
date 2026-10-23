use bytes::Bytes;
use std::{future::Future, pin::Pin, sync::Arc};
use tidemq_protocol::{
    Id, ProtocolLimits,
    common::{Frame, FrameKind, TrafficClass},
    framing::{decode_payload, new_frame},
    internal::{InternalCommand, PeerHello, RaftRequest, SnapshotChunk},
    public::Hello,
};
use tokio::time::Instant;

use super::{Error, Result};

/// A token is never retained in Debug output or exposed by a getter.
#[derive(Clone)]
pub struct TokenGrant {
    digest: blake3::Hash,
    principal: String,
    scope: String,
    allowed_kinds: Vec<FrameKind>,
}

impl TokenGrant {
    pub fn new(
        token: &str,
        principal: String,
        scope: String,
        mut allowed_kinds: Vec<FrameKind>,
    ) -> Result<Self> {
        validate_token(token)?;
        for (valid, reason) in [
            (
                !principal.is_empty() && principal.len() <= 128,
                "token principal must contain between 1 and 128 bytes",
            ),
            (
                !scope.is_empty() && scope.len() <= 128,
                "token scope must contain between 1 and 128 bytes",
            ),
            (
                !allowed_kinds.is_empty() && allowed_kinds.len() <= 32,
                "token allowed_kinds must contain between 1 and 32 operations",
            ),
        ] {
            if !valid {
                return Err(Error::Configuration { reason });
            }
        }
        if allowed_kinds.iter().any(|kind| {
            matches!(
                kind,
                FrameKind::RaftVote
                    | FrameKind::RaftAppend
                    | FrameKind::RaftSnapshot
                    | FrameKind::RaftResult
                    | FrameKind::InternalCommand
                    | FrameKind::InternalResult
                    | FrameKind::Unspecified
                    | FrameKind::Hello
                    | FrameKind::HelloResult
            )
        }) {
            return Err(Error::Configuration {
                reason: "token allowed_kinds must contain only public operations; internal and HELLO operations cannot be granted",
            });
        }
        for (existing, added) in [
            (FrameKind::GetTopic, FrameKind::GetTopicLayout),
            (FrameKind::CreateTopic, FrameKind::CreateLogicalTopic),
            (FrameKind::CreateTopic, FrameKind::ChangeTopicLayout),
        ] {
            if allowed_kinds.contains(&existing) && !allowed_kinds.contains(&added) {
                allowed_kinds.push(added);
            }
        }
        Ok(Self {
            digest: blake3::hash(token.as_bytes()),
            principal,
            scope,
            allowed_kinds,
        })
    }
}

/// Pins are leaf DER hashes, checked only after native TLS chain verification succeeds.
#[derive(Clone, Debug)]
pub struct NodeGrant {
    pub certificate_pin: [u8; 32],
    pub certificate_name: String,
    pub cluster_id: Id,
    pub node_id: u64,
    pub role: String,
    pub allowed_groups: Vec<u64>,
    pub allowed_classes: Vec<TrafficClass>,
}

impl NodeGrant {
    pub fn certificate_pin(leaf_der: &[u8]) -> [u8; 32] {
        *blake3::hash(leaf_der).as_bytes()
    }

    pub(super) fn validate(&self) -> Result<()> {
        for (valid, reason) in [
            (self.node_id != 0, "node grant node_id must be nonzero"),
            (
                !self.certificate_name.is_empty() && self.certificate_name.len() <= 253,
                "node grant certificate_name must contain between 1 and 253 bytes",
            ),
            (
                self.certificate_name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-')),
                "node grant certificate_name must contain only ASCII letters, digits, dots, and hyphens",
            ),
            (
                !self.role.is_empty() && self.role.len() <= 32,
                "node grant role must contain between 1 and 32 bytes",
            ),
            (
                self.allowed_groups.len() <= 4_096,
                "node grant allowed_groups must not exceed 4096 entries",
            ),
            (
                !self.allowed_groups.contains(&0),
                "node grant allowed_groups must not contain group zero",
            ),
            (
                !self.allowed_classes.is_empty() && self.allowed_classes.len() <= 5,
                "node grant allowed_classes must contain between 1 and 5 traffic classes",
            ),
            (
                !self.allowed_classes.contains(&TrafficClass::Unspecified),
                "node grant allowed_classes must not include unspecified",
            ),
        ] {
            if !valid {
                return Err(Error::Configuration { reason });
            }
        }
        Ok(())
    }
}

pub(super) fn validate_node_grants(grants: &[NodeGrant]) -> Result<()> {
    for (index, grant) in grants.iter().enumerate() {
        grant.validate()?;
        if grants.iter().take(index).any(|previous| {
            previous.certificate_pin == grant.certificate_pin
                && (previous.cluster_id != grant.cluster_id
                    || previous.node_id != grant.node_id
                    || previous.role != grant.role)
        }) {
            return Err(Error::Configuration {
                reason: "one certificate pin cannot authorize multiple node identities",
            });
        }
    }
    Ok(())
}

#[derive(Clone)]
pub enum ServerAuthorization {
    External(Vec<TokenGrant>),
    Dynamic(Arc<dyn TokenAuthenticator>),
    Internal(Vec<NodeGrant>),
}

/// Called through `Arc<dyn TokenAuthenticator>`; native async methods are not dyn-compatible.
pub trait TokenAuthenticator: Send + Sync {
    fn authenticate<'a>(
        &'a self,
        token: &'a str,
        deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<CredentialGrant>> + Send + 'a>>;
}

pub struct CredentialGrant {
    pub credential: Id,
    pub allowed_kinds: Vec<FrameKind>,
}

impl ServerAuthorization {
    pub(super) fn validate(&self) -> Result<()> {
        match self {
            Self::Dynamic(_) => Ok(()),
            Self::External(grants) if !grants.is_empty() && grants.len() <= 1_024 => Ok(()),
            Self::Internal(grants) if !grants.is_empty() && grants.len() <= 4_096 => {
                validate_node_grants(grants)
            }
            Self::External(_) => Err(Error::Configuration {
                reason: "external authorization must contain between 1 and 1024 token grants",
            }),
            Self::Internal(_) => Err(Error::Configuration {
                reason: "internal authorization must contain between 1 and 4096 node grants",
            }),
        }
    }

    pub(super) fn internal(&self) -> bool {
        matches!(self, Self::Internal(_))
    }

    // Keep the two disjoint binding policies atomic; neither may fall back to the other.
    pub(super) async fn authorize(
        &self,
        frame: &Frame,
        peer_pin: Option<[u8; 32]>,
        limits: &ProtocolLimits,
        deadline: Instant,
    ) -> Result<(Id, TrafficClass, Authorization)> {
        if FrameKind::try_from(frame.kind).ok() != Some(FrameKind::Hello) {
            return Err(Error::Authentication {
                reason: "first frame must be HELLO",
            });
        }
        let class =
            TrafficClass::try_from(frame.traffic_class).map_err(|_| Error::Authentication {
                reason: "HELLO traffic class is invalid",
            })?;
        match self {
            Self::Dynamic(authenticator) => {
                let hello: Hello = decode_payload(frame.payload.clone(), limits)
                    .map_err(|source| Error::Protocol { source })?;
                validate_token(&hello.access_token)?;
                let grant = authenticator
                    .authenticate(&hello.access_token, deadline)
                    .await?;
                let token = TokenGrant::new(
                    &hello.access_token,
                    grant.credential.to_string(),
                    "*".into(),
                    grant.allowed_kinds.clone(),
                )?;
                let (session_id, class, _) = bind_external(frame, class, &hello, &token)?;
                Ok((
                    session_id,
                    class,
                    Authorization::Credential {
                        credential: grant.credential,
                        session_id,
                        allowed_kinds: grant.allowed_kinds,
                    },
                ))
            }
            Self::External(grants) => {
                let hello: Hello = decode_payload(frame.payload.clone(), limits)
                    .map_err(|source| Error::Protocol { source })?;
                validate_token(&hello.access_token)?;
                let digest = blake3::hash(hello.access_token.as_bytes());
                let grant = grants.iter().find(|grant| grant.digest == digest).ok_or(
                    Error::Authentication {
                        reason: "access token rejected",
                    },
                )?;
                bind_external(frame, class, &hello, grant)
            }
            Self::Internal(grants) => {
                let pin = peer_pin.ok_or(Error::Authentication {
                    reason: "verified peer certificate evidence is missing",
                })?;
                let hello: PeerHello = decode_payload(frame.payload.clone(), limits)
                    .map_err(|source| Error::Protocol { source })?;
                let grant = grants
                    .iter()
                    .find(|grant| {
                        grant.certificate_pin == pin
                            && hello.cluster_id.as_ref() == grant.cluster_id.as_bytes()
                            && hello.node_id == grant.node_id
                            && hello.role == grant.role
                    })
                    .ok_or(Error::Authorization {
                        reason: "certificate does not authorize the claimed node",
                    })?;
                if hello.traffic_class != frame.traffic_class
                    || !grant.allowed_classes.contains(&class)
                {
                    return Err(Error::Authorization {
                        reason: "node cannot bind this traffic class",
                    });
                }
                let session = Id::from_slice(&frame.operation_id)
                    .map_err(|source| Error::Protocol { source })?;
                Ok((session, class, Authorization::Internal(grant.clone())))
            }
        }
    }
}

fn bind_external(
    frame: &Frame,
    class: TrafficClass,
    hello: &Hello,
    grant: &TokenGrant,
) -> Result<(Id, TrafficClass, Authorization)> {
    if hello.traffic_class != frame.traffic_class || class == TrafficClass::Replication {
        return Err(Error::Authorization {
            reason: "token cannot bind this traffic class",
        });
    }
    let session = Id::from_slice(&hello.session_id).map_err(|source| Error::Protocol { source })?;
    if frame.operation_id.as_ref() != session.as_bytes() {
        return Err(Error::Authentication {
            reason: "HELLO session binding differs",
        });
    }
    Ok((
        session,
        class,
        Authorization::External {
            principal: grant.principal.clone(),
            scope: grant.scope.clone(),
            allowed_kinds: grant.allowed_kinds.clone(),
        },
    ))
}

/// Transport checks operation classes. Business code must also resolve scope/resource ownership.
#[derive(Clone, Debug)]
pub enum Authorization {
    Credential {
        credential: Id,
        session_id: Id,
        allowed_kinds: Vec<FrameKind>,
    },
    External {
        principal: String,
        scope: String,
        allowed_kinds: Vec<FrameKind>,
    },
    Internal(NodeGrant),
    Server,
}

impl Authorization {
    pub fn allows_group(&self, group: u64) -> bool {
        matches!(self, Self::Internal(grant) if grant.allowed_groups.contains(&group))
    }

    pub(super) fn validate_frame(&self, frame: &Frame, limits: &ProtocolLimits) -> Result<()> {
        let kind = FrameKind::try_from(frame.kind).map_err(|_| Error::Authorization {
            reason: "unknown frame kind",
        })?;
        if matches!(kind, FrameKind::Hello | FrameKind::HelloResult) {
            return Err(Error::Authentication {
                reason: "connection is already bound",
            });
        }
        match self {
            Self::External { allowed_kinds, .. } | Self::Credential { allowed_kinds, .. }
                if !allowed_kinds.contains(&kind) =>
            {
                Err(Error::Authorization {
                    reason: "token does not permit this operation",
                })
            }
            Self::Internal(_) => {
                let group = match kind {
                    FrameKind::RaftVote | FrameKind::RaftAppend => Some(
                        decode_payload::<RaftRequest>(frame.payload.clone(), limits)
                            .map_err(|source| Error::Protocol { source })?
                            .group_id,
                    ),
                    FrameKind::RaftSnapshot => Some(
                        decode_payload::<SnapshotChunk>(frame.payload.clone(), limits)
                            .map_err(|source| Error::Protocol { source })?
                            .group_id,
                    ),
                    FrameKind::InternalCommand => Some(
                        decode_payload::<InternalCommand>(frame.payload.clone(), limits)
                            .map_err(|source| Error::Protocol { source })?
                            .group_id,
                    ),
                    _ => None,
                };
                if group.is_some_and(|group| !self.allows_group(group)) {
                    return Err(Error::Authorization {
                        reason: "node is not authorized for this group",
                    });
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone)]
pub enum ClientCredentials {
    External {
        session_id: Id,
        access_token: String,
    },
    Internal {
        session_id: Id,
        cluster_id: Id,
        node_id: u64,
        role: String,
    },
}

impl ClientCredentials {
    pub(super) fn internal(&self) -> bool {
        matches!(self, Self::Internal { .. })
    }

    pub(super) fn session(&self) -> Id {
        match self {
            Self::External { session_id, .. } | Self::Internal { session_id, .. } => *session_id,
        }
    }

    pub(super) fn digest(&self) -> Result<blake3::Hash> {
        match self {
            Self::External { access_token, .. } => {
                validate_token(access_token)?;
                Ok(blake3::hash(access_token.as_bytes()))
            }
            Self::Internal {
                cluster_id,
                node_id,
                role,
                ..
            } => {
                if *node_id == 0 || role.is_empty() || role.len() > 32 {
                    return Err(Error::Configuration {
                        reason: "client node identity is invalid",
                    });
                }
                let mut hasher = blake3::Hasher::new();
                hasher.update(cluster_id.as_bytes());
                hasher.update(&node_id.to_be_bytes());
                hasher.update(role.as_bytes());
                Ok(hasher.finalize())
            }
        }
    }

    pub(super) fn hello(
        &self,
        class: TrafficClass,
        timeout_ms: u64,
        limits: &ProtocolLimits,
    ) -> Result<Frame> {
        let request = Id::new_v7().map_err(|source| Error::Protocol { source })?;
        let session = self.session();
        let frame = match self {
            Self::External { access_token, .. } => {
                validate_token(access_token)?;
                new_frame(
                    FrameKind::Hello,
                    class,
                    request,
                    session,
                    &Hello {
                        session_id: Bytes::copy_from_slice(session.as_bytes()),
                        traffic_class: class.into(),
                        access_token: access_token.clone(),
                    },
                    timeout_ms,
                    limits,
                )
            }
            Self::Internal {
                cluster_id,
                node_id,
                role,
                ..
            } => new_frame(
                FrameKind::Hello,
                class,
                request,
                session,
                &PeerHello {
                    cluster_id: Bytes::copy_from_slice(cluster_id.as_bytes()),
                    node_id: *node_id,
                    role: role.clone(),
                    traffic_class: class.into(),
                },
                timeout_ms,
                limits,
            ),
        };
        frame.map_err(|source| Error::Protocol { source })
    }
}

fn validate_token(token: &str) -> Result<()> {
    if !(32..=256).contains(&token.len()) {
        return Err(Error::Authentication {
            reason: "access token must contain between 32 and 256 bytes",
        });
    }
    if !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::Authentication {
            reason: "access token must contain only printable ASCII characters without whitespace",
        });
    }
    Ok(())
}
