use crate::{Admin, AdminReceipt, Error, Result, admin::bytes};
use std::time::Duration;
use tidemq_protocol::{Id, public};

macro_rules! resource_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
        pub struct $name(Id);
        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }
        impl std::str::FromStr for $name {
            type Err = Error;
            fn from_str(value: &str) -> Result<Self> {
                Ok(Self(value.parse()?))
            }
        }
    };
}

resource_id!(TenantId);
resource_id!(NamespaceId);
resource_id!(PrincipalId);
resource_id!(CredentialId);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Permission {
    ManageTenants,
    ManageCredentials,
    ManageTopics,
    Produce,
    Consume,
    Inspect,
}

impl Permission {
    fn as_str(self) -> &'static str {
        match self {
            Self::ManageTenants => "manage_tenants",
            Self::ManageCredentials => "manage_credentials",
            Self::ManageTopics => "manage_topics",
            Self::Produce => "produce",
            Self::Consume => "consume",
            Self::Inspect => "inspect",
        }
    }
}

#[derive(Clone)]
pub struct AccessToken {
    id: CredentialId,
    token: std::sync::Arc<str>,
    expires_ms: u64,
}

impl std::fmt::Debug for AccessToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AccessToken")
            .field("id", &self.id)
            .field("expires_ms", &self.expires_ms)
            .finish_non_exhaustive()
    }
}

impl AccessToken {
    pub fn id(&self) -> CredentialId {
        self.id
    }
    pub fn expires_at_unix_ms(&self) -> u64 {
        self.expires_ms
    }
    pub fn expose_secret(&self) -> &str {
        &self.token
    }
}

pub struct PrincipalBuilder {
    admin: Admin,
    request: public::ManageIdentity,
}

impl PrincipalBuilder {
    pub fn namespace(mut self, namespace: NamespaceId) -> Self {
        self.request.namespace_id = bytes(namespace.0);
        self
    }
    pub async fn build(self) -> Result<PrincipalId> {
        if self.request.permissions.is_empty() || self.request.permissions.len() > 6 {
            return Err(Error::Configuration {
                reason: "principal must have between one and six permissions",
            });
        }
        let result = self.admin.manage_identity(&self.request).await?;
        match result {
            AdminReceipt::PrincipalConfigured(id) => Ok(id),
            _ => Err(result_mismatch()),
        }
    }
}

impl Admin {
    pub async fn set_tenant_limits(
        &self,
        tenant: TenantId,
        limits: crate::ResourceLimits,
    ) -> Result<crate::PolicyUpdate> {
        let receipt = self
            .manage_identity(&public::ManageIdentity {
                action: public::IdentityAction::SetTenantLimits.into(),
                target_id: bytes(tenant.0),
                limits: Some(limits.0),
                ..Default::default()
            })
            .await?;
        match receipt {
            AdminReceipt::TenantLimitsChanged(id) if id == tenant => {
                Ok(crate::PolicyUpdate::Applied)
            }
            AdminReceipt::PolicySuperseded => Ok(crate::PolicyUpdate::Superseded),
            _ => Err(result_mismatch()),
        }
    }

    pub async fn set_namespace_limits(
        &self,
        namespace: NamespaceId,
        limits: crate::ResourceLimits,
    ) -> Result<crate::PolicyUpdate> {
        let receipt = self
            .manage_identity(&public::ManageIdentity {
                action: public::IdentityAction::SetNamespaceLimits.into(),
                target_id: bytes(namespace.0),
                limits: Some(limits.0),
                ..Default::default()
            })
            .await?;
        match receipt {
            AdminReceipt::NamespaceLimitsChanged(id) if id == namespace => {
                Ok(crate::PolicyUpdate::Applied)
            }
            AdminReceipt::PolicySuperseded => Ok(crate::PolicyUpdate::Superseded),
            _ => Err(result_mismatch()),
        }
    }

    pub async fn set_principal_enabled(&self, principal: PrincipalId, enabled: bool) -> Result<()> {
        let receipt = self
            .manage_identity(&public::ManageIdentity {
                action: public::IdentityAction::SetPrincipalEnabled.into(),
                target_id: bytes(principal.0),
                enabled,
                ..Default::default()
            })
            .await?;
        match receipt {
            AdminReceipt::PrincipalStatusChanged(id) if id == principal => Ok(()),
            _ => Err(result_mismatch()),
        }
    }

    pub async fn create_tenant(&self, name: &str) -> Result<TenantId> {
        let result = self
            .manage_identity(&public::ManageIdentity {
                action: public::IdentityAction::CreateTenantIdentity.into(),
                name: name.into(),
                ..Default::default()
            })
            .await?;
        match result {
            AdminReceipt::TenantCreated(id) => Ok(id),
            _ => Err(result_mismatch()),
        }
    }

    pub async fn create_namespace(&self, tenant: TenantId, name: &str) -> Result<NamespaceId> {
        let result = self
            .manage_identity(&public::ManageIdentity {
                action: public::IdentityAction::CreateNamespaceIdentity.into(),
                parent_id: bytes(tenant.0),
                name: name.into(),
                ..Default::default()
            })
            .await?;
        match result {
            AdminReceipt::NamespaceCreated(id) => Ok(id),
            _ => Err(result_mismatch()),
        }
    }

    pub fn new_principal(
        &self,
        tenant: TenantId,
        name: impl Into<String>,
        permissions: impl IntoIterator<Item = Permission>,
    ) -> PrincipalBuilder {
        PrincipalBuilder {
            admin: self.clone(),
            request: public::ManageIdentity {
                action: public::IdentityAction::PutPrincipalIdentity.into(),
                parent_id: bytes(tenant.0),
                name: name.into(),
                permissions: permissions
                    .into_iter()
                    .take(7)
                    .map(|permission| permission.as_str().to_owned())
                    .collect(),
                enabled: true,
                ..Default::default()
            },
        }
    }

    pub async fn issue_token(
        &self,
        principal: PrincipalId,
        lifetime: Duration,
    ) -> Result<AccessToken> {
        let lifetime_ms =
            u64::try_from(lifetime.as_millis()).map_err(|_| Error::Configuration {
                reason: "credential lifetime exceeds its range",
            })?;
        let result = self
            .manage_identity(&public::ManageIdentity {
                action: public::IdentityAction::IssueCredential.into(),
                parent_id: bytes(principal.0),
                lifetime_ms,
                ..Default::default()
            })
            .await?;
        match result {
            AdminReceipt::CredentialIssued(token) => Ok(token),
            AdminReceipt::CredentialSecretUnavailable { id, .. } => {
                Err(Error::CredentialSecretUnavailable { id })
            }
            _ => Err(result_mismatch()),
        }
    }

    pub async fn revoke_token(&self, credential: CredentialId) -> Result<()> {
        self.manage_identity(&public::ManageIdentity {
            action: public::IdentityAction::RevokeCredential.into(),
            target_id: bytes(credential.0),
            ..Default::default()
        })
        .await?;
        Ok(())
    }

    pub async fn set_tenant_enabled(&self, tenant: TenantId, enabled: bool) -> Result<()> {
        self.manage_identity(&public::ManageIdentity {
            action: public::IdentityAction::SetTenantEnabled.into(),
            target_id: bytes(tenant.0),
            enabled,
            ..Default::default()
        })
        .await?;
        Ok(())
    }

    async fn manage_identity(&self, request: &public::ManageIdentity) -> Result<AdminReceipt> {
        self.execute(crate::admin::pending::Request::Identity(request.clone()))
            .await
    }
}

fn result_mismatch() -> Error {
    Error::ResponseMismatch {
        reason: "identity operation returned a different result kind".into(),
    }
}

pub(crate) fn receipt(
    request: &public::ManageIdentity,
    result: public::IdentityResult,
    operation: Id,
) -> Result<AdminReceipt> {
    use public::IdentityAction as Action;
    let action = Action::try_from(request.action).map_err(|_| result_mismatch())?;
    let id = Id::from_slice(&result.id)?;
    let expected = match action {
        Action::RevokeCredential
        | Action::SetTenantEnabled
        | Action::SetPrincipalEnabled
        | Action::SetTenantLimits
        | Action::SetNamespaceLimits => Id::from_slice(&request.target_id)?,
        Action::PutPrincipalIdentity if !request.target_id.is_empty() => {
            Id::from_slice(&request.target_id)?
        }
        _ => operation,
    };
    if id != expected {
        return Err(result_mismatch());
    }
    let policy = matches!(action, Action::SetTenantLimits | Action::SetNamespaceLimits);
    if result.policy_pending || result.policy_superseded && !policy {
        return Err(result_mismatch());
    }
    if result.policy_superseded {
        return Ok(AdminReceipt::PolicySuperseded);
    }
    Ok(match action {
        Action::CreateTenantIdentity => AdminReceipt::TenantCreated(TenantId(id)),
        Action::CreateNamespaceIdentity => AdminReceipt::NamespaceCreated(NamespaceId(id)),
        Action::PutPrincipalIdentity => AdminReceipt::PrincipalConfigured(PrincipalId(id)),
        Action::RevokeCredential => AdminReceipt::CredentialRevoked(CredentialId(id)),
        Action::SetTenantEnabled => AdminReceipt::TenantStatusChanged(TenantId(id)),
        Action::SetTenantLimits => AdminReceipt::TenantLimitsChanged(TenantId(id)),
        Action::SetNamespaceLimits => AdminReceipt::NamespaceLimitsChanged(NamespaceId(id)),
        Action::SetPrincipalEnabled => AdminReceipt::PrincipalStatusChanged(PrincipalId(id)),
        Action::IssueCredential => {
            if result.parent_id != request.parent_id || result.expires_at_ms == 0 {
                return Err(result_mismatch());
            }
            match (result.access_token, result.secret_unavailable) {
                (Some(token), false)
                    if (32..=256).contains(&token.len())
                        && token.bytes().all(|byte| byte.is_ascii_graphic()) =>
                {
                    AdminReceipt::CredentialIssued(AccessToken {
                        id: CredentialId(id),
                        token: token.into(),
                        expires_ms: result.expires_at_ms,
                    })
                }
                (None, true) => AdminReceipt::CredentialSecretUnavailable {
                    id: CredentialId(id),
                    expires_at_unix_ms: result.expires_at_ms,
                },
                _ => return Err(result_mismatch()),
            }
        }
        Action::Unspecified => return Err(result_mismatch()),
    })
}

#[cfg(test)]
mod receipt_tests {
    use super::*;

    #[test]
    fn identity_results_cannot_replace_the_original_resource_or_reconstruct_a_lost_secret() {
        let operation = Id::new_v7().unwrap();
        let parent = Id::new_v7().unwrap();
        let request = public::ManageIdentity {
            action: public::IdentityAction::IssueCredential.into(),
            parent_id: bytes(parent),
            ..Default::default()
        };
        let reply = public::IdentityResult {
            id: bytes(operation),
            parent_id: bytes(parent),
            expires_at_ms: 1,
            secret_unavailable: true,
            ..Default::default()
        };
        assert!(
            matches!(receipt(&request, reply.clone(), operation).unwrap(),
            AdminReceipt::CredentialSecretUnavailable { id, .. } if id == CredentialId(operation))
        );
        let mut foreign = reply.clone();
        foreign.id = bytes(Id::new_v7().unwrap());
        assert!(receipt(&request, foreign, operation).is_err());
        let mut contradictory = reply;
        contradictory.access_token = Some("s".repeat(64));
        assert!(receipt(&request, contradictory, operation).is_err());
    }

    #[test]
    fn issued_secret_is_bounded_and_shared_without_entering_debug_output() {
        let operation = Id::new_v7().unwrap();
        let parent = Id::new_v7().unwrap();
        let request = public::ManageIdentity {
            action: public::IdentityAction::IssueCredential.into(),
            parent_id: bytes(parent),
            ..Default::default()
        };
        let token = "private-credential-secret-that-must-not-be-logged";
        let reply = public::IdentityResult {
            id: bytes(operation),
            parent_id: bytes(parent),
            expires_at_ms: 1,
            access_token: Some(token.into()),
            ..Default::default()
        };
        let issued = receipt(&request, reply.clone(), operation).unwrap();
        assert!(!format!("{issued:?}").contains(token));
        let AdminReceipt::CredentialIssued(issued) = issued else {
            panic!("Expected a credential");
        };
        let cloned = issued.clone();
        assert!(std::sync::Arc::ptr_eq(&issued.token, &cloned.token));
        assert_eq!(cloned.expose_secret(), token);
        let mut invalid = reply;
        invalid.access_token = Some(format!("{token}\n"));
        assert!(receipt(&request, invalid, operation).is_err());
    }
}
