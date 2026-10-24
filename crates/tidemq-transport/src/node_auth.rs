use std::{ops::Deref, sync::Arc};
use tokio::sync::watch;

use super::{Error, NodeGrant, Result};

#[cfg(test)]
#[path = "../../../tests/transport_support/node_registry.rs"]
mod tests;

#[derive(Clone, Debug)]
pub struct NodeAuthorizations(watch::Sender<Arc<[NodeGrant]>>);

impl NodeAuthorizations {
    pub fn new(grants: Vec<NodeGrant>) -> Result<Self> {
        validate(&grants)?;
        Ok(Self(watch::channel(grants.into()).0))
    }

    pub fn snapshot(&self) -> Arc<[NodeGrant]> {
        self.0.borrow().clone()
    }

    /// Updates trusted identities after their authoritative registration has been verified.
    pub fn replace(&self, grants: Vec<NodeGrant>) -> Result<()> {
        validate(&grants)?;
        self.0.send_if_modified(|current| {
            if current.as_ref() == grants.as_slice() {
                return false;
            }
            *current = grants.into();
            true
        });
        Ok(())
    }

    pub(crate) fn bind(&self, grant: NodeGrant) -> AuthorizedNode {
        AuthorizedNode {
            grant,
            registry: Some(self.clone()),
        }
    }
}

fn validate(grants: &[NodeGrant]) -> Result<()> {
    if grants.len() > 128 {
        return Err(Error::Configuration {
            reason: "node authorization list must not exceed 128 entries",
        });
    }
    super::auth::validate_node_grants(grants)
}

#[derive(Clone, Debug)]
pub struct AuthorizedNode {
    grant: NodeGrant,
    registry: Option<NodeAuthorizations>,
}

impl From<NodeGrant> for AuthorizedNode {
    fn from(grant: NodeGrant) -> Self {
        Self {
            grant,
            registry: None,
        }
    }
}

impl Deref for AuthorizedNode {
    type Target = NodeGrant;
    fn deref(&self) -> &Self::Target {
        &self.grant
    }
}

impl AuthorizedNode {
    pub(crate) fn valid(&self) -> bool {
        self.registry
            .as_ref()
            .is_none_or(|registry| registry.snapshot().contains(&self.grant))
    }

    pub(crate) async fn revoked(&self) {
        let Some(registry) = &self.registry else {
            return std::future::pending().await;
        };
        let mut changed = registry.0.subscribe();
        loop {
            if !changed.borrow_and_update().contains(&self.grant) {
                return;
            }
            if changed.changed().await.is_err() {
                return;
            }
        }
    }
}
