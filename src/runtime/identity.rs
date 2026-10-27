use super::{
    Error, Result,
    service::{Service, forbidden},
    wire,
};
use crate::{
    gateway::Access,
    metadata::{
        AuthAction, AuthCommand, AuthOutcome, Command, Namespace, OperationResult, Permission,
        Principal, Response, Tenant, TokenDigest,
    },
};
use tidemq_protocol::{
    Id,
    common::{ErrorCode, Frame, FrameKind, Outcome},
    public,
};
use tokio::time::Instant;

impl Service {
    pub(super) async fn identity_list_public(
        &self,
        access: &Access,
        frame: &Frame,
    ) -> Result<Frame> {
        let credential = access
            .principal
            .strip_prefix("credential:")
            .ok_or_else(forbidden)?
            .parse()
            .map_err(|_| forbidden())?;
        let request: public::ListIdentities =
            tidemq_protocol::framing::decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode identity listing", source))?;
        let now_ms = super::data::unix_millis()?;
        let page = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let mut page = state.auth().identity_page(credential, now_ms, &request)?;
                page.cluster_id = state
                    .cluster_id()
                    .map(super::metadata::bytes_id)
                    .unwrap_or_default();
                if let Some(scope) = request.resource_scope.as_ref() {
                    let scope = crate::metadata::ResourceScope::from_wire(scope)
                        .map_err(crate::metadata::Rejection::Authorization)?;
                    state.validate_authorization_scope(scope, false)?;
                }
                Ok(page)
            })
            .await
            .map_err(|source| Error::operation("read authorized identity page", source))?
            .map_err(super::metadata::metadata_rejection)?;
        wire::response(frame, FrameKind::IdentityPage, &page, &self.config.protocol)
    }

    pub(super) async fn identity_public(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let credential: Id = access
            .principal
            .strip_prefix("credential:")
            .ok_or_else(forbidden)?
            .parse()
            .map_err(|_| forbidden())?;
        let request: public::ManageIdentity =
            tidemq_protocol::framing::decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode identity management", source))?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"tidemq/identity-request/v1\0");
        hash.update(credential.as_bytes());
        hash.update(&frame.payload);
        let digest = *hash.finalize().as_bytes();
        let operation = wire::id(&frame.operation_id)?;
        let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let saved = replica
            .state
            .try_with_application(|state| state.management_result(operation, digest))
            .await
            .map_err(|source| Error::operation("read original identity management result", source))?
            .map_err(super::metadata::metadata_rejection)?;
        if let OperationResult::Known(saved) = saved {
            if let Response::Rejected { reason, .. } = saved {
                return Err(super::metadata::original_metadata_rejection(reason));
            }
            let Response::Applied {
                outcome: crate::metadata::Outcome::Identity(saved),
                ..
            } = saved
            else {
                return Err(Error::reject(
                    ErrorCode::ContentConflict,
                    Outcome::Rejected,
                    false,
                ));
            };
            if saved.request_digest != digest {
                return Err(Error::reject(
                    ErrorCode::ContentConflict,
                    Outcome::Rejected,
                    false,
                ));
            }
            let result = self.identity_result(saved.outcome, None, deadline).await?;
            return wire::response(
                frame,
                FrameKind::IdentityResult,
                &result,
                &self.config.protocol,
            );
        }
        let now_ms = super::data::unix_millis()?;
        let previous = self.identity_base_limits(&request).await?;
        let (action, token) = identity_action(&request, operation, now_ms, previous)?;
        let outcome = self
            .meta_write(
                Command::identity(
                    operation,
                    self.meta_revision().await?,
                    AuthCommand {
                        actor: Some(credential),
                        now_ms,
                        request_digest: digest,
                        action,
                    },
                ),
                deadline,
            )
            .await?;
        let crate::metadata::Outcome::Identity(result) = outcome else {
            return Err(Error::invalid(
                "Identity management returned an invalid result",
            ));
        };
        let result = self
            .identity_result(result.outcome, token, deadline)
            .await?;
        wire::response(
            frame,
            FrameKind::IdentityResult,
            &result,
            &self.config.protocol,
        )
    }

    async fn identity_base_limits(
        &self,
        request: &public::ManageIdentity,
    ) -> Result<Option<crate::metadata::TenantLimits>> {
        let action = public::IdentityAction::try_from(request.action)
            .map_err(|_| Error::invalid("Identity action is invalid"))?;
        if !matches!(
            action,
            public::IdentityAction::SetTenantLimits | public::IdentityAction::SetNamespaceLimits
        ) {
            return Ok(None);
        }
        let target = wire::id(&request.target_id)?;
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                if action == public::IdentityAction::SetTenantLimits {
                    return state
                        .auth()
                        .tenant(target)
                        .map(|tenant| Some(tenant.limits))
                        .ok_or_else(super::metadata::not_found);
                }
                let namespace = state
                    .auth()
                    .namespace(target)
                    .ok_or_else(super::metadata::not_found)?;
                state
                    .auth()
                    .limits(namespace.tenant, namespace.id)
                    .map(|(_, limits)| Some(limits))
                    .map_err(|_| forbidden())
            })
            .await
            .map_err(|source| Error::operation("read current resource limits", source))?
    }

    async fn identity_result(
        &self,
        outcome: AuthOutcome,
        token: Option<String>,
        deadline: Instant,
    ) -> Result<public::IdentityResult> {
        if let AuthOutcome::Policy(policy) = outcome {
            return self.quota_policy_result(policy, deadline).await;
        }
        let (id, credential) = match outcome {
            AuthOutcome::Tenant(id) | AuthOutcome::Namespace(id) | AuthOutcome::Principal(id) => {
                (id, false)
            }
            AuthOutcome::Credential(id) => (id, true),
            AuthOutcome::Policy(_) => return Err(Error::invalid("Policy result was not resolved")),
        };
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let info = replica
            .state
            .try_with_application(|state| state.auth().credential(id).cloned())
            .await
            .map_err(|source| Error::operation("read credential result metadata", source))?;
        Ok(public::IdentityResult {
            id: super::metadata::bytes_id(id),
            parent_id: info
                .as_ref()
                .map(|credential| super::metadata::bytes_id(credential.principal))
                .unwrap_or_default(),
            expires_at_ms: info.as_ref().map_or(0, |credential| credential.expires_ms),
            secret_unavailable: credential && token.is_none(),
            access_token: token,
            policy_pending: false,
            policy_superseded: false,
        })
    }

    async fn quota_policy_result(
        &self,
        policy: Id,
        deadline: Instant,
    ) -> Result<public::IdentityResult> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let now_ms = super::data::unix_millis()?;
        let (mut record, ready) = replica
            .state
            .try_with_application(|state| {
                let record = state
                    .quota_policy(policy)
                    .cloned()
                    .ok_or_else(super::metadata::not_found)?;
                Ok::<_, Error>((
                    record,
                    state
                        .quota_policy_ready(policy, now_ms)
                        .map_err(super::metadata::metadata_rejection)?,
                ))
            })
            .await
            .map_err(|source| Error::operation("read durable quota policy progress", source))??;
        if !record.complete && !record.superseded && ready {
            let outcome = self
                .meta_write(
                    Command::observe_quota_policy(
                        Id::new_v7().map_err(|source| {
                            Error::operation("allocate quota policy observation", source)
                        })?,
                        self.meta_revision().await?,
                        policy,
                        now_ms,
                    ),
                    deadline,
                )
                .await?;
            let crate::metadata::Outcome::QuotaPolicy(observed) = outcome else {
                return Err(Error::invalid(
                    "Policy observation returned an invalid result",
                ));
            };
            record = observed;
        }
        Ok(public::IdentityResult {
            id: super::metadata::bytes_id(record.namespace.unwrap_or(record.tenant)),
            policy_pending: !record.complete && !record.superseded,
            policy_superseded: record.superseded,
            ..Default::default()
        })
    }
}

fn identity_action(
    request: &public::ManageIdentity,
    operation: Id,
    now: u64,
    previous: Option<crate::metadata::TenantLimits>,
) -> Result<(AuthAction, Option<String>)> {
    use public::IdentityAction as Action;
    let action = Action::try_from(request.action)
        .map_err(|_| Error::invalid("Identity action is invalid"))?;
    let target = || wire::id(&request.target_id);
    let parent = || wire::id(&request.parent_id);
    let value = match action {
        Action::CreateTenantIdentity => AuthAction::CreateTenant(Tenant {
            id: operation,
            name: request.name.clone(),
            enabled: true,
            limits: crate::metadata::TenantLimits::default(),
        }),
        Action::CreateNamespaceIdentity => AuthAction::CreateNamespace(Namespace {
            id: operation,
            tenant: parent()?,
            name: request.name.clone(),
            enabled: true,
            limits: None,
        }),
        Action::PutPrincipalIdentity => principal_action(request, operation)?,
        Action::IssueCredential => {
            let expires_ms = now
                .checked_add(request.lifetime_ms)
                .filter(|_| request.lifetime_ms != 0)
                .ok_or_else(|| Error::invalid("Credential lifetime must be finite and nonzero"))?;
            let token = generate_token()?;
            let digest = TokenDigest::of(&token).map_err(|_| forbidden())?;
            return Ok((
                AuthAction::CreateCredential {
                    id: operation,
                    principal: parent()?,
                    digest,
                    expires_ms,
                },
                Some(token),
            ));
        }
        Action::RevokeCredential => AuthAction::DisableCredential { id: target()? },
        Action::SetTenantEnabled => AuthAction::SetTenantEnabled {
            id: target()?,
            enabled: request.enabled,
        },
        Action::SetTenantLimits | Action::SetNamespaceLimits => {
            let limits = request
                .limits
                .as_ref()
                .ok_or_else(|| Error::invalid("Resource limits are required"))?;
            let limits = crate::metadata::TenantLimits::from_wire(
                limits,
                previous
                    .ok_or_else(|| Error::invalid("Current resource limits are unavailable"))?,
            )
            .map_err(|_| Error::invalid("Resource limits exceed their finite bounds"))?;
            if action == Action::SetTenantLimits {
                AuthAction::SetTenantLimits {
                    id: target()?,
                    limits,
                }
            } else {
                AuthAction::SetNamespaceLimits {
                    id: target()?,
                    limits,
                }
            }
        }
        Action::SetPrincipalEnabled => AuthAction::SetPrincipalEnabled {
            id: target()?,
            enabled: request.enabled,
        },
        Action::DeleteTenantIdentity => AuthAction::DeleteTenant { id: target()? },
        Action::DeleteNamespaceIdentity => AuthAction::DeleteNamespace { id: target()? },
        Action::SetNamespaceEnabled => AuthAction::SetNamespaceEnabled {
            id: target()?,
            enabled: request.enabled,
        },
        Action::GrantResourcePermission | Action::RevokeResourcePermission => {
            if request.permissions.len() != 1 {
                return Err(Error::invalid(
                    "Exactly one resource permission is required",
                ));
            }
            let permission = request
                .permissions
                .first()
                .ok_or_else(|| Error::invalid("Resource permission is required"))?;
            AuthAction::SetResourcePermission {
                principal: target()?,
                scope: identity_scope(request)?,
                permission: Permission::from_name(permission)
                    .map_err(|_| Error::invalid("Resource permission is invalid"))?,
                allow: action == Action::GrantResourcePermission,
            }
        }
        Action::ReplaceResourceGrants => {
            if request.resource_grants.len() > 32 {
                return Err(Error::invalid("Resource grant count exceeds its bound"));
            }
            AuthAction::ReplaceResourceGrants {
                principal: target()?,
                expected_revision: request.expected_auth_revision,
                grants: request
                    .resource_grants
                    .iter()
                    .map(crate::metadata::ResourceGrant::from_wire)
                    .collect::<std::result::Result<_, _>>()
                    .map_err(|_| Error::invalid("Resource grants are invalid"))?,
            }
        }
        Action::Unspecified => return Err(Error::invalid("Identity action is unspecified")),
    };
    Ok((value, None))
}

fn principal_action(request: &public::ManageIdentity, operation: Id) -> Result<AuthAction> {
    if request.permissions.len() > 7 {
        return Err(Error::invalid("Permission count exceeds its bound"));
    }
    let permissions = request
        .permissions
        .iter()
        .map(|permission| {
            Permission::from_name(permission)
                .map_err(|_| Error::invalid("Permission name is invalid"))
        })
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    let principal = Principal {
        id: if request.target_id.is_empty() {
            operation
        } else {
            wire::id(&request.target_id)?
        },
        tenant: Some(wire::id(&request.parent_id)?),
        namespace: if request.namespace_id.is_empty() {
            None
        } else {
            Some(wire::id(&request.namespace_id)?)
        },
        name: request.name.clone(),
        enabled: request.enabled,
        permissions,
    };
    if request.target_id.is_empty() {
        Ok(AuthAction::PutPrincipal(principal))
    } else {
        Ok(AuthAction::ReplacePrincipal {
            principal,
            expected_revision: request.expected_auth_revision,
        })
    }
}

fn identity_scope(request: &public::ManageIdentity) -> Result<crate::metadata::ResourceScope> {
    let scope = request
        .resource_scope
        .as_ref()
        .ok_or_else(|| Error::invalid("Resource scope is required"))?;
    crate::metadata::ResourceScope::from_wire(scope)
        .map_err(|_| Error::invalid("Resource scope is invalid"))
}

fn generate_token() -> Result<String> {
    let mut entropy = [0_u8; 32];
    getrandom::fill(&mut entropy)
        .map_err(|source| Error::operation("generate credential entropy", source))?;
    let mut token = String::with_capacity(64);
    use std::fmt::Write;
    for byte in entropy {
        write!(&mut token, "{byte:02x}")
            .map_err(|_| Error::invalid("Credential encoding failed"))?;
    }
    Ok(token)
}
