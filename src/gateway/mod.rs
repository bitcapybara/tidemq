//! Authenticated principal delegation and finite QUIC forwarding.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use tidemq_protocol::common::{ErrorCode, FrameKind, Outcome};

use crate::runtime::{Error, Result};
use crate::transport::{Authorization, TokenGrant};

#[derive(Clone)]
pub(crate) struct Access {
    pub principal: Arc<str>,
    pub scope: Arc<str>,
}

#[derive(Clone)]
pub(crate) struct AccessTable {
    principals: BTreeMap<Arc<str>, BTreeMap<Arc<str>, Vec<FrameKind>>>,
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
    pub(crate) fn empty() -> Self {
        Self {
            principals: BTreeMap::new(),
            tokens: Vec::new(),
        }
    }
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
        let mut principals: BTreeMap<Arc<str>, BTreeMap<Arc<str>, Vec<FrameKind>>> =
            BTreeMap::new();
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
            if let Some(previous) = principals
                .entry(Arc::from(token.principal.as_str()))
                .or_default()
                .insert(Arc::from(token.scope.as_str()), kinds.clone())
                && previous != kinds
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
        let kind = match kind {
            FrameKind::GetTopicLayout => FrameKind::GetTopic,
            FrameKind::ChangeTopicLayout | FrameKind::CreateLogicalTopic => FrameKind::CreateTopic,
            kind => kind,
        };
        let (principal, scopes) = self
            .principals
            .get_key_value(principal)
            .ok_or_else(forbidden)?;
        let (scope, _) = scopes
            .get_key_value(scope)
            .filter(|(_, kinds)| kinds.contains(&kind))
            .ok_or_else(forbidden)?;
        Ok(Access {
            principal: Arc::clone(principal),
            scope: Arc::clone(scope),
        })
    }
}

impl Access {
    pub fn authorize_topic(&self, name: &str) -> Result<()> {
        if self.scope.as_ref() == "*" || self.scope.strip_prefix("topic:") == Some(name) {
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
            | FrameKind::CreateLogicalTopic
            | FrameKind::GetTopicLayout
            | FrameKind::ChangeTopicLayout
            | FrameKind::ManageIdentity
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

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> AccessTable {
        AccessTable {
            principals: BTreeMap::from([
                (
                    Arc::from("reader"),
                    BTreeMap::from([
                        (Arc::from("topic:orders"), vec![FrameKind::Read]),
                        (Arc::from("topic:invoices"), vec![FrameKind::Read]),
                    ]),
                ),
                (
                    Arc::from("writer"),
                    BTreeMap::from([(Arc::from("*"), vec![FrameKind::ProduceBatch])]),
                ),
            ]),
            tokens: Vec::new(),
        }
    }

    #[test]
    fn delegated_access_reuses_validated_identity_storage() {
        let table = table();
        let first = table
            .delegated("reader", "topic:orders", FrameKind::Read)
            .unwrap();
        let second = table
            .delegated("reader", "topic:orders", FrameKind::Read)
            .unwrap();
        assert!(Arc::ptr_eq(&first.principal, &second.principal));
        assert!(Arc::ptr_eq(&first.scope, &second.scope));
        let cloned = first.clone();
        assert!(Arc::ptr_eq(&first.principal, &cloned.principal));
        assert!(Arc::ptr_eq(&first.scope, &cloned.scope));
        assert!(first.authorize_topic("orders").is_ok());
        assert!(first.authorize_topic("invoices").is_err());
        assert!(
            table
                .delegated("writer", "*", FrameKind::ProduceBatch)
                .unwrap()
                .authorize_topic("orders")
                .is_ok()
        );
    }

    #[test]
    fn authorization_keeps_principal_scope_and_connection_grants_separate() {
        let table = table();
        for (principal, scope, kind) in [
            ("unknown", "topic:orders", FrameKind::Read),
            ("reader", "*", FrameKind::Read),
            ("writer", "topic:orders", FrameKind::ProduceBatch),
            ("reader", "topic:orders", FrameKind::ProduceBatch),
        ] {
            assert!(table.delegated(principal, scope, kind).is_err());
        }
        let peer = Authorization::External {
            principal: "reader".to_owned(),
            scope: "topic:orders".to_owned(),
            allowed_kinds: vec![FrameKind::Read],
        };
        assert!(table.external(&peer, FrameKind::Read).is_ok());
        assert!(table.external(&peer, FrameKind::ProduceBatch).is_err());
        let limited_peer = Authorization::External {
            principal: "reader".to_owned(),
            scope: "topic:orders".to_owned(),
            allowed_kinds: vec![FrameKind::Cancel],
        };
        assert!(table.external(&limited_peer, FrameKind::Read).is_err());
        assert!(
            table
                .external(&Authorization::Server, FrameKind::Read)
                .is_err()
        );
    }
}
