use crate::{Admin, Error, Result, admin::bytes};
use std::time::Duration;
use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    public,
};

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

pub struct AccessToken {
    id: CredentialId,
    token: String,
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
        Ok(PrincipalId(Id::from_slice(&result.id)?))
    }
}

impl Admin {
    pub async fn create_tenant(&self, name: &str) -> Result<TenantId> {
        let result = self
            .manage_identity(&public::ManageIdentity {
                action: public::IdentityAction::CreateTenantIdentity.into(),
                name: name.into(),
                ..Default::default()
            })
            .await?;
        Ok(TenantId(Id::from_slice(&result.id)?))
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
        Ok(NamespaceId(Id::from_slice(&result.id)?))
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
        let id = CredentialId(Id::from_slice(&result.id)?);
        let token = result
            .access_token
            .ok_or_else(|| Error::CredentialSecretUnavailable { id })?;
        Ok(AccessToken {
            id,
            token,
            expires_ms: result.expires_at_ms,
        })
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

    async fn manage_identity(
        &self,
        request: &public::ManageIdentity,
    ) -> Result<public::IdentityResult> {
        let operation = Id::new_v7()?;
        self.client
            .operation(|deadline| {
                self.client.request(
                    FrameKind::ManageIdentity,
                    FrameKind::IdentityResult,
                    TrafficClass::Control,
                    operation,
                    request,
                    deadline,
                )
            })
            .await
    }
}
