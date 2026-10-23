use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tidemq_protocol::Id;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Snafu)]
pub enum AuthRejection {
    #[snafu(display("Authentication or authorization was rejected"))]
    Forbidden,
    #[snafu(display("Authentication resource is absent or disabled"))]
    NotFound,
    #[snafu(display("Authentication resource already exists"))]
    Conflict,
    #[snafu(display("Authentication request is invalid"))]
    Invalid,
    #[snafu(display("Authentication resource capacity is exhausted"))]
    Capacity,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TokenDigest([u8; 32]);

impl std::fmt::Debug for TokenDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TokenDigest(<redacted>)")
    }
}

impl TokenDigest {
    pub fn of(token: &str) -> Result<Self, AuthRejection> {
        if !(32..=256).contains(&token.len()) || !token.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(AuthRejection::Invalid);
        }
        Ok(Self(*blake3::hash(token.as_bytes()).as_bytes()))
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Permission {
    ManageTenants,
    ManageCredentials,
    ManageTopics,
    Produce,
    Consume,
    Inspect,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Tenant {
    pub id: Id,
    pub name: String,
    pub enabled: bool,
    pub max_topics: usize,
    pub max_subscriptions: usize,
    pub max_principals: usize,
    pub max_credentials: usize,
    pub max_backlog_bytes: u64,
}

impl Tenant {
    fn validate(&self) -> Result<(), AuthRejection> {
        valid_component(&self.name)?;
        if !(1..=1024).contains(&self.max_topics)
            || !(1..=4096).contains(&self.max_subscriptions)
            || !(1..=1024).contains(&self.max_principals)
            || !(1..=4096).contains(&self.max_credentials)
            || self.max_backlog_bytes == 0
        {
            return Err(AuthRejection::Invalid);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Namespace {
    pub id: Id,
    pub tenant: Id,
    pub name: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Principal {
    pub id: Id,
    pub tenant: Option<Id>,
    pub name: String,
    pub enabled: bool,
    pub permissions: BTreeSet<Permission>,
    pub namespace: Option<Id>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Credential {
    pub id: Id,
    pub principal: Id,
    pub enabled: bool,
    pub expires_ms: u64,
    digest: TokenDigest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AuthAction {
    Bootstrap {
        principal: Id,
        credential: Id,
        digest: TokenDigest,
        expires_ms: u64,
    },
    CreateTenant(Tenant),
    CreateNamespace(Namespace),
    PutPrincipal(Principal),
    CreateCredential {
        id: Id,
        principal: Id,
        digest: TokenDigest,
        expires_ms: u64,
    },
    DisableCredential {
        id: Id,
    },
    SetTenantEnabled {
        id: Id,
        enabled: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthCommand {
    pub actor: Option<Id>,
    pub now_ms: u64,
    pub request_digest: [u8; 32],
    pub action: AuthAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AuthOutcome {
    Tenant(Id),
    Namespace(Id),
    Principal(Id),
    Credential(Id),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthResult {
    pub request_digest: [u8; 32],
    pub outcome: AuthOutcome,
}

#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthState {
    revision: u64,
    tenants: BTreeMap<Id, Tenant>,
    namespaces: BTreeMap<Id, Namespace>,
    principals: BTreeMap<Id, Principal>,
    credentials: BTreeMap<Id, Credential>,
    digests: BTreeMap<[u8; 32], Id>,
}

impl std::fmt::Debug for AuthState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthState")
            .field("revision", &self.revision)
            .field("tenants", &self.tenants.len())
            .field("namespaces", &self.namespaces.len())
            .field("principals", &self.principals.len())
            .field("credentials", &self.credentials.len())
            .finish()
    }
}

impl AuthState {
    pub fn initialized(&self) -> bool {
        self.revision > 0
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn credential(&self, id: Id) -> Option<&Credential> {
        self.credentials.get(&id)
    }
    pub fn tenant(&self, id: Id) -> Option<&Tenant> {
        self.tenants.get(&id)
    }
    pub(crate) fn validate_topic_parent(
        &self,
        topic: &super::TopicRecord,
    ) -> Result<(), AuthRejection> {
        if !self.initialized() {
            return if topic.tenant.is_none() && topic.namespace.is_none() {
                Ok(())
            } else {
                Err(AuthRejection::Invalid)
            };
        }
        let tenant = topic
            .tenant
            .and_then(|id| self.tenants.get(&id))
            .ok_or(AuthRejection::Invalid)?;
        let namespace = topic
            .namespace
            .and_then(|id| self.namespaces.get(&id))
            .ok_or(AuthRejection::Invalid)?;
        let mut parts = topic.name.split('/');
        if namespace.tenant != tenant.id
            || parts.next() != Some(&tenant.name)
            || parts.next() != Some(&namespace.name)
            || parts.next().is_none()
            || parts.next().is_some()
        {
            return Err(AuthRejection::Invalid);
        }
        Ok(())
    }

    pub fn authenticate(
        &self,
        digest: TokenDigest,
        now_ms: u64,
    ) -> Result<&Credential, AuthRejection> {
        let id = self
            .digests
            .get(&digest.0)
            .ok_or(AuthRejection::Forbidden)?;
        let credential = self
            .credentials
            .get(id)
            .filter(|credential| credential.enabled && credential.expires_ms > now_ms)
            .ok_or(AuthRejection::Forbidden)?;
        self.active_principal(credential.principal)?;
        Ok(credential)
    }

    pub fn authorize(
        &self,
        credential: Id,
        now_ms: u64,
        permission: Permission,
        name: Option<&str>,
    ) -> Result<&Principal, AuthRejection> {
        let credential = self
            .credentials
            .get(&credential)
            .filter(|credential| credential.enabled && credential.expires_ms > now_ms)
            .ok_or(AuthRejection::Forbidden)?;
        let principal = self.active_principal(credential.principal)?;
        if !principal.permissions.contains(&permission)
            && !(permission == Permission::Inspect
                && principal.permissions.iter().any(|permission| {
                    matches!(
                        permission,
                        Permission::Produce | Permission::Consume | Permission::ManageTopics
                    )
                }))
        {
            return Err(AuthRejection::Forbidden);
        }
        if let Some(name) = name {
            let (tenant, namespace, _) = self.resolve_topic(name)?;
            if principal.tenant.is_some_and(|id| id != tenant.id)
                || principal.namespace.is_some_and(|id| id != namespace.id)
            {
                return Err(AuthRejection::Forbidden);
            }
        }
        Ok(principal)
    }

    pub fn resolve_topic<'a>(
        &self,
        name: &'a str,
    ) -> Result<(&Tenant, &Namespace, &'a str), AuthRejection> {
        let mut components = name.split('/');
        let tenant = components.next().ok_or(AuthRejection::Invalid)?;
        let namespace = components.next().ok_or(AuthRejection::Invalid)?;
        let topic = components.next().ok_or(AuthRejection::Invalid)?;
        if components.next().is_some() {
            return Err(AuthRejection::Invalid);
        }
        for component in [tenant, namespace, topic] {
            valid_component(component)?;
        }
        let tenant = self
            .tenants
            .values()
            .find(|record| record.name == tenant && record.enabled)
            .ok_or(AuthRejection::NotFound)?;
        let namespace = self
            .namespaces
            .values()
            .find(|record| record.tenant == tenant.id && record.name == namespace && record.enabled)
            .ok_or(AuthRejection::NotFound)?;
        Ok((tenant, namespace, topic))
    }

    fn active_principal(&self, id: Id) -> Result<&Principal, AuthRejection> {
        let principal = self
            .principals
            .get(&id)
            .filter(|principal| principal.enabled)
            .ok_or(AuthRejection::Forbidden)?;
        if principal
            .tenant
            .is_some_and(|id| self.tenants.get(&id).is_none_or(|tenant| !tenant.enabled))
        {
            return Err(AuthRejection::Forbidden);
        }
        if principal.namespace.is_some_and(|id| {
            self.namespaces
                .get(&id)
                .is_none_or(|namespace| !namespace.enabled)
        }) {
            return Err(AuthRejection::Forbidden);
        }
        Ok(principal)
    }

    pub(crate) fn apply(&mut self, command: &AuthCommand) -> Result<AuthOutcome, AuthRejection> {
        if let AuthAction::Bootstrap {
            principal,
            credential,
            digest,
            expires_ms,
        } = command.action
        {
            if self.initialized() {
                return Err(AuthRejection::Conflict);
            }
            if command.actor.is_some() {
                return Err(AuthRejection::Forbidden);
            }
            validate_expiry(command.now_ms, expires_ms)?;
            self.principals.insert(
                principal,
                Principal {
                    id: principal,
                    tenant: None,
                    namespace: None,
                    name: "platform-admin".into(),
                    enabled: true,
                    permissions: all_permissions(),
                },
            );
            self.credentials.insert(
                credential,
                Credential {
                    id: credential,
                    principal,
                    enabled: true,
                    expires_ms,
                    digest,
                },
            );
            self.digests.insert(digest.0, credential);
            self.revision = 1;
            return Ok(AuthOutcome::Credential(credential));
        }
        let actor = command.actor.ok_or(AuthRejection::Forbidden)?;
        let permission = if matches!(
            command.action,
            AuthAction::CreateTenant(_) | AuthAction::SetTenantEnabled { .. }
        ) {
            Permission::ManageTenants
        } else {
            Permission::ManageCredentials
        };
        let actor = self
            .authorize(actor, command.now_ms, permission, None)?
            .clone();
        let next = self
            .revision
            .checked_add(1)
            .ok_or(AuthRejection::Capacity)?;
        let result = self.execute(&actor, command)?;
        self.revision = next;
        Ok(result)
    }

    fn execute(
        &mut self,
        actor: &Principal,
        command: &AuthCommand,
    ) -> Result<AuthOutcome, AuthRejection> {
        match &command.action {
            AuthAction::CreateTenant(tenant) => {
                if actor.tenant.is_some() {
                    return Err(AuthRejection::Forbidden);
                }
                tenant.validate()?;
                if self.tenants.len() >= 256 {
                    return Err(AuthRejection::Capacity);
                }
                if self.tenants.contains_key(&tenant.id)
                    || self
                        .tenants
                        .values()
                        .any(|record| record.name == tenant.name)
                {
                    return Err(AuthRejection::Conflict);
                }
                self.tenants.insert(tenant.id, tenant.clone());
                Ok(AuthOutcome::Tenant(tenant.id))
            }
            AuthAction::CreateNamespace(namespace) => {
                if actor.namespace.is_some() {
                    return Err(AuthRejection::Forbidden);
                }
                valid_component(&namespace.name)?;
                self.manage_tenant(actor, namespace.tenant)?;
                if self.namespaces.len() >= 4096 {
                    return Err(AuthRejection::Capacity);
                }
                if self.namespaces.contains_key(&namespace.id)
                    || self.namespaces.values().any(|record| {
                        record.tenant == namespace.tenant && record.name == namespace.name
                    })
                {
                    return Err(AuthRejection::Conflict);
                }
                self.namespaces.insert(namespace.id, namespace.clone());
                Ok(AuthOutcome::Namespace(namespace.id))
            }
            AuthAction::PutPrincipal(principal) => self.put_principal(actor, principal),
            AuthAction::CreateCredential {
                id,
                principal,
                digest,
                expires_ms,
            } => {
                validate_expiry(command.now_ms, *expires_ms)?;
                let target = self
                    .principals
                    .get(principal)
                    .ok_or(AuthRejection::NotFound)?;
                self.manage_principal(actor, target)?;
                if !target.permissions.is_subset(&actor.permissions) {
                    return Err(AuthRejection::Forbidden);
                }
                if self.credentials.contains_key(id) || self.digests.contains_key(&digest.0) {
                    return Err(AuthRejection::Conflict);
                }
                if self.credentials.len() >= 8192 {
                    return Err(AuthRejection::Capacity);
                }
                if let Some(tenant) = target.tenant {
                    let limit = self
                        .tenants
                        .get(&tenant)
                        .ok_or(AuthRejection::NotFound)?
                        .max_credentials;
                    if self
                        .credentials
                        .values()
                        .filter(|credential| {
                            self.principals
                                .get(&credential.principal)
                                .is_some_and(|principal| principal.tenant == Some(tenant))
                        })
                        .count()
                        >= limit
                    {
                        return Err(AuthRejection::Capacity);
                    }
                }
                self.credentials.insert(
                    *id,
                    Credential {
                        id: *id,
                        principal: *principal,
                        enabled: true,
                        expires_ms: *expires_ms,
                        digest: *digest,
                    },
                );
                self.digests.insert(digest.0, *id);
                Ok(AuthOutcome::Credential(*id))
            }
            AuthAction::DisableCredential { id } => {
                let credential = self.credentials.get(id).ok_or(AuthRejection::NotFound)?;
                self.manage_principal(
                    actor,
                    self.principals
                        .get(&credential.principal)
                        .ok_or(AuthRejection::NotFound)?,
                )?;
                self.credentials
                    .get_mut(id)
                    .ok_or(AuthRejection::NotFound)?
                    .enabled = false;
                Ok(AuthOutcome::Credential(*id))
            }
            AuthAction::SetTenantEnabled { id, enabled } => {
                if actor.tenant.is_some() {
                    return Err(AuthRejection::Forbidden);
                }
                self.tenants
                    .get_mut(id)
                    .ok_or(AuthRejection::NotFound)?
                    .enabled = *enabled;
                Ok(AuthOutcome::Tenant(*id))
            }
            AuthAction::Bootstrap { .. } => Err(AuthRejection::Conflict),
        }
    }

    fn put_principal(
        &mut self,
        actor: &Principal,
        principal: &Principal,
    ) -> Result<AuthOutcome, AuthRejection> {
        valid_component(&principal.name)?;
        self.manage_principal(actor, principal)?;
        if self.principals.values().any(|record| {
            record.id != principal.id
                && record.name == principal.name
                && record.tenant == principal.tenant
        }) {
            return Err(AuthRejection::Conflict);
        }
        if principal.permissions.is_empty() || !principal.permissions.is_subset(&actor.permissions)
        {
            return Err(AuthRejection::Forbidden);
        }
        if principal.tenant.is_some() && principal.permissions.contains(&Permission::ManageTenants)
        {
            return Err(AuthRejection::Forbidden);
        }
        if principal.namespace.is_some_and(|id| {
            self.namespaces
                .get(&id)
                .is_none_or(|namespace| Some(namespace.tenant) != principal.tenant)
        }) {
            return Err(AuthRejection::Invalid);
        }
        if let Some(previous) = self.principals.get(&principal.id) {
            self.manage_principal(actor, previous)?;
            if previous.tenant != principal.tenant || previous.namespace != principal.namespace {
                return Err(AuthRejection::Conflict);
            }
        } else {
            if self.principals.len() >= 4096 {
                return Err(AuthRejection::Capacity);
            }
            if let Some(tenant) = principal.tenant {
                let limit = self
                    .tenants
                    .get(&tenant)
                    .ok_or(AuthRejection::NotFound)?
                    .max_principals;
                if self
                    .principals
                    .values()
                    .filter(|principal| principal.tenant == Some(tenant))
                    .count()
                    >= limit
                {
                    return Err(AuthRejection::Capacity);
                }
            }
        }
        self.principals.insert(principal.id, principal.clone());
        Ok(AuthOutcome::Principal(principal.id))
    }

    fn manage_tenant(&self, actor: &Principal, tenant: Id) -> Result<(), AuthRejection> {
        if !self.tenants.contains_key(&tenant) {
            return Err(AuthRejection::NotFound);
        }
        if actor.tenant.is_some_and(|id| id != tenant) {
            return Err(AuthRejection::Forbidden);
        }
        Ok(())
    }

    fn manage_principal(
        &self,
        actor: &Principal,
        principal: &Principal,
    ) -> Result<(), AuthRejection> {
        if actor.tenant.is_some() && actor.tenant != principal.tenant
            || actor.namespace.is_some() && actor.namespace != principal.namespace
        {
            return Err(AuthRejection::Forbidden);
        }
        if let Some(tenant) = principal.tenant {
            self.manage_tenant(actor, tenant)?;
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), AuthRejection> {
        if self.tenants.len() > 256
            || self.namespaces.len() > 4096
            || self.principals.len() > 4096
            || self.credentials.len() > 8192
            || self.digests.len() != self.credentials.len()
        {
            return Err(AuthRejection::Capacity);
        }
        for (id, tenant) in &self.tenants {
            tenant.validate()?;
            if *id != tenant.id {
                return Err(AuthRejection::Invalid);
            }
        }
        for (id, namespace) in &self.namespaces {
            valid_component(&namespace.name)?;
            if *id != namespace.id || !self.tenants.contains_key(&namespace.tenant) {
                return Err(AuthRejection::Invalid);
            }
        }
        for (id, principal) in &self.principals {
            valid_component(&principal.name)?;
            if *id != principal.id
                || principal.permissions.is_empty()
                || principal
                    .tenant
                    .is_some_and(|id| !self.tenants.contains_key(&id))
                || principal.namespace.is_some_and(|id| {
                    self.namespaces
                        .get(&id)
                        .is_none_or(|namespace| Some(namespace.tenant) != principal.tenant)
                })
            {
                return Err(AuthRejection::Invalid);
            }
        }
        for (id, credential) in &self.credentials {
            if *id != credential.id
                || !self.principals.contains_key(&credential.principal)
                || self.digests.get(&credential.digest.0) != Some(id)
            {
                return Err(AuthRejection::Invalid);
            }
        }
        if self.revision == 0
            && (!self.principals.is_empty()
                || !self.credentials.is_empty()
                || !self.tenants.is_empty()
                || !self.namespaces.is_empty())
        {
            return Err(AuthRejection::Invalid);
        }
        Ok(())
    }
}

fn valid_component(name: &str) -> Result<(), AuthRejection> {
    if name.is_empty()
        || name.len() > 64
        || matches!(name, "." | "..")
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(AuthRejection::Invalid);
    }
    Ok(())
}

fn validate_expiry(now: u64, expires: u64) -> Result<(), AuthRejection> {
    if expires <= now || expires.saturating_sub(now) > 366 * 24 * 60 * 60 * 1000 {
        return Err(AuthRejection::Invalid);
    }
    Ok(())
}

fn all_permissions() -> BTreeSet<Permission> {
    [
        Permission::ManageTenants,
        Permission::ManageCredentials,
        Permission::ManageTopics,
        Permission::Produce,
        Permission::Consume,
        Permission::Inspect,
    ]
    .into()
}
