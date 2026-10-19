//! Authenticated principal delegation and finite QUIC forwarding.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;
use tidemq_protocol::common::{ErrorCode, FrameKind, Outcome};

use crate::runtime::{Error, Result};
use crate::transport::{Authorization, TokenGrant};

#[derive(Clone)]
pub(crate) struct Access {
    pub principal: String,
    pub scope: String,
}

#[derive(Clone)]
pub(crate) struct AccessTable {
    principals: BTreeMap<(String, String), Vec<FrameKind>>,
    tokens: Vec<TokenGrant>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AccessFile {
    tokens: Vec<Token>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Token {
    value: String,
    principal: String,
    scope: String,
    operations: Vec<String>,
}

impl AccessTable {
    pub async fn load(path: &Path) -> Result<Self> {
        let bytes = crate::runtime::read_secret(path, 65_536).await?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|source| Error::operation("decode access configuration", source))?;
        let file: AccessFile = toml::from_str(text)
            .map_err(|_| Error::invalid("Access configuration is malformed"))?;
        if file.tokens.is_empty() || file.tokens.len() > 128 {
            return Err(Error::invalid(
                "Access configuration must contain a bounded nonempty token table",
            ));
        }
        let mut principals = BTreeMap::new();
        let mut tokens = Vec::new();
        for token in file.tokens {
            if !valid_scope(&token.scope)
                || token.operations.is_empty()
                || token.operations.len() > 24
            {
                return Err(Error::invalid(
                    "Access scope or operation bounds are invalid",
                ));
            }
            let kinds: Vec<_> = token
                .operations
                .iter()
                .map(|name| {
                    FrameKind::from_str_name(name)
                        .filter(|kind| external_kind(*kind))
                        .ok_or_else(|| {
                            Error::invalid("Access configuration contains an unsupported operation")
                        })
                })
                .collect::<Result<_>>()?;
            if let Some(previous) = principals.insert(
                (token.principal.clone(), token.scope.clone()),
                kinds.clone(),
            ) && previous != kinds
            {
                return Err(Error::invalid(
                    "A principal scope has conflicting operation grants",
                ));
            }
            tokens.push(
                TokenGrant::new(&token.value, token.principal, token.scope, kinds)
                    .map_err(|source| Error::operation("validate access grant", source))?,
            );
        }
        Ok(Self { principals, tokens })
    }

    pub fn tokens(&self) -> Vec<TokenGrant> {
        self.tokens.clone()
    }
    pub fn external(&self, peer: &Authorization, kind: FrameKind) -> Result<Access> {
        let Authorization::External {
            principal,
            scope,
            allowed_kinds,
        } = peer
        else {
            return Err(forbidden());
        };
        let access = self.delegated(principal, scope, kind)?;
        if !allowed_kinds.contains(&kind) {
            return Err(forbidden());
        }
        Ok(access)
    }
    pub fn delegated(&self, principal: &str, scope: &str, kind: FrameKind) -> Result<Access> {
        self.principals
            .get(&(principal.to_owned(), scope.to_owned()))
            .filter(|kinds| kinds.contains(&kind))
            .ok_or_else(forbidden)?;
        Ok(Access {
            principal: principal.to_owned(),
            scope: scope.to_owned(),
        })
    }
}

impl Access {
    pub fn authorize_topic(&self, name: &str) -> Result<()> {
        if self.scope == "*" || self.scope.strip_prefix("topic:") == Some(name) {
            Ok(())
        } else {
            Err(forbidden())
        }
    }
}

fn valid_scope(scope: &str) -> bool {
    scope == "*"
        || scope.strip_prefix("topic:").is_some_and(|name| {
            !name.is_empty()
                && name.len() <= 122
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
}
fn forbidden() -> Error {
    Error::reject(ErrorCode::Forbidden, Outcome::Rejected, false)
}

pub(crate) fn external_kind(kind: FrameKind) -> bool {
    matches!(
        kind,
        FrameKind::CreateTopic
            | FrameKind::GetTopic
            | FrameKind::CreateSubscription
            | FrameKind::GetSubscription
            | FrameKind::OpenProducer
            | FrameKind::ProducerStatus
            | FrameKind::RetireProducer
            | FrameKind::ProduceBatch
            | FrameKind::Read
            | FrameKind::AckBatch
            | FrameKind::ReceiveBarrier
            | FrameKind::ClaimAssignment
            | FrameKind::SeekSubscription
            | FrameKind::DeleteSubscription
            | FrameKind::SubscriptionStatus
            | FrameKind::Cancel
            | FrameKind::Diagnose
    )
}
