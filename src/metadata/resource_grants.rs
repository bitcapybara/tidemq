use super::*;

const MAX_RESOURCE_GRANTS: usize = 4096;
const MAX_PRINCIPAL_GRANTS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ResourceScope {
    /// Existing platform authority may appear as a source, but cannot be granted as a resource item.
    Cluster,
    Tenant(Id),
    Namespace {
        tenant: Id,
        namespace: Id,
    },
    Topic {
        tenant: Id,
        namespace: Id,
        topic: Id,
    },
}

impl ResourceScope {
    pub fn from_wire(
        scope: &tidemq_protocol::public::ResourceScope,
    ) -> Result<Self, AuthRejection> {
        let decode = |value: &[u8]| Id::from_slice(value).map_err(|_| AuthRejection::Invalid);
        match (
            scope.tenant_id.is_empty(),
            scope.namespace_id.is_empty(),
            scope.topic_id.is_empty(),
        ) {
            (true, true, true) => Ok(Self::Cluster),
            (false, true, true) => Ok(Self::Tenant(decode(&scope.tenant_id)?)),
            (false, false, true) => Ok(Self::Namespace {
                tenant: decode(&scope.tenant_id)?,
                namespace: decode(&scope.namespace_id)?,
            }),
            (false, false, false) => Ok(Self::Topic {
                tenant: decode(&scope.tenant_id)?,
                namespace: decode(&scope.namespace_id)?,
                topic: decode(&scope.topic_id)?,
            }),
            _ => Err(AuthRejection::Invalid),
        }
    }

    pub fn to_wire(self) -> tidemq_protocol::public::ResourceScope {
        let bytes = |id: Option<Id>| {
            id.map(|id| bytes::Bytes::copy_from_slice(id.as_bytes()))
                .unwrap_or_default()
        };
        tidemq_protocol::public::ResourceScope {
            tenant_id: bytes(self.tenant()),
            namespace_id: bytes(self.namespace()),
            topic_id: bytes(self.topic()),
        }
    }

    pub fn tenant(self) -> Option<Id> {
        match self {
            Self::Cluster => None,
            Self::Tenant(tenant) | Self::Namespace { tenant, .. } | Self::Topic { tenant, .. } => {
                Some(tenant)
            }
        }
    }

    pub fn namespace(self) -> Option<Id> {
        match self {
            Self::Namespace { namespace, .. } | Self::Topic { namespace, .. } => Some(namespace),
            _ => None,
        }
    }

    pub fn topic(self) -> Option<Id> {
        match self {
            Self::Topic { topic, .. } => Some(topic),
            _ => None,
        }
    }

    pub fn covers(self, other: Self) -> bool {
        match self {
            Self::Cluster => true,
            Self::Tenant(tenant) => other.tenant() == Some(tenant),
            Self::Namespace { tenant, namespace } => {
                other.tenant() == Some(tenant) && other.namespace() == Some(namespace)
            }
            Self::Topic { .. } => self == other,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceGrant {
    pub scope: ResourceScope,
    pub permissions: BTreeSet<Permission>,
}

impl ResourceGrant {
    pub fn from_wire(
        grant: &tidemq_protocol::public::ResourceGrant,
    ) -> Result<Self, AuthRejection> {
        if grant.permissions.len() > 7 {
            return Err(AuthRejection::Invalid);
        }
        let permissions = grant
            .permissions
            .iter()
            .map(|value| Permission::from_name(value))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if permissions.len() != grant.permissions.len() {
            return Err(AuthRejection::Invalid);
        }
        Ok(Self {
            scope: ResourceScope::from_wire(grant.scope.as_ref().ok_or(AuthRejection::Invalid)?)?,
            permissions,
        })
    }

    pub fn to_wire(&self) -> tidemq_protocol::public::ResourceGrant {
        tidemq_protocol::public::ResourceGrant {
            scope: Some(self.scope.to_wire()),
            permissions: self
                .permissions
                .iter()
                .map(|permission| permission.name().to_owned())
                .collect(),
        }
    }
}

impl Permission {
    pub fn name(self) -> &'static str {
        match self {
            Self::ManageTenants => "manage_tenants",
            Self::ManageCredentials => "manage_credentials",
            Self::ManageTopics => "manage_topics",
            Self::Produce => "produce",
            Self::Consume => "consume",
            Self::Inspect => "inspect",
            Self::InspectMessageContent => "inspect_message_content",
        }
    }

    pub fn from_name(value: &str) -> Result<Self, AuthRejection> {
        match value {
            "manage_tenants" => Ok(Self::ManageTenants),
            "manage_credentials" => Ok(Self::ManageCredentials),
            "manage_topics" => Ok(Self::ManageTopics),
            "produce" => Ok(Self::Produce),
            "consume" => Ok(Self::Consume),
            "inspect" => Ok(Self::Inspect),
            "inspect_message_content" => Ok(Self::InspectMessageContent),
            _ => Err(AuthRejection::Invalid),
        }
    }

    pub fn is_resource_permission(self) -> bool {
        matches!(
            self,
            Self::ManageTopics
                | Self::Produce
                | Self::Consume
                | Self::Inspect
                | Self::InspectMessageContent
        )
    }
}

impl Principal {
    pub fn resource_scope(&self) -> ResourceScope {
        match (self.tenant, self.namespace) {
            (Some(tenant), Some(namespace)) => ResourceScope::Namespace { tenant, namespace },
            (Some(tenant), None) => ResourceScope::Tenant(tenant),
            _ => ResourceScope::Cluster,
        }
    }
}

impl AuthState {
    pub(crate) fn explicit_resource_scopes(&self) -> impl Iterator<Item = ResourceScope> + '_ {
        self.resource_grants.keys().map(|(_, scope)| *scope)
    }

    pub fn direct_resource_grants(&self, principal: Id) -> Vec<ResourceGrant> {
        let mut grants = BTreeMap::<ResourceScope, BTreeSet<Permission>>::new();
        if let Some(row) = self.principals.get(&principal) {
            let permissions: BTreeSet<_> = row.permissions.iter().copied().collect();
            if !permissions.is_empty() {
                grants.insert(row.resource_scope(), permissions);
            }
        }
        for ((subject, scope), permissions) in &self.resource_grants {
            if *subject == principal {
                grants.entry(*scope).or_default().extend(permissions);
            }
        }
        grants
            .into_iter()
            .map(|(scope, permissions)| ResourceGrant { scope, permissions })
            .collect()
    }

    pub fn effective_resource_grants(
        &self,
        principal: Id,
        scope: ResourceScope,
    ) -> Vec<ResourceGrant> {
        self.direct_resource_grants(principal)
            .into_iter()
            .filter(|grant| grant.scope.covers(scope))
            .collect()
    }

    pub(super) fn permission_implied(
        permissions: &BTreeSet<Permission>,
        permission: Permission,
    ) -> bool {
        permissions.contains(&permission)
            || permission == Permission::Inspect
                && permissions.iter().any(|permission| {
                    matches!(
                        permission,
                        Permission::Produce | Permission::Consume | Permission::ManageTopics
                    )
                })
    }

    pub(super) fn allows(
        &self,
        principal: &Principal,
        permission: Permission,
        scope: ResourceScope,
    ) -> bool {
        Self::permission_implied(&principal.permissions, permission)
            || self
                .resource_grants
                .iter()
                .any(|((subject, granted), permissions)| {
                    *subject == principal.id
                        && granted.covers(scope)
                        && Self::permission_implied(permissions, permission)
                })
    }

    pub(super) fn require_revision(&self, expected: u64) -> Result<(), AuthRejection> {
        if expected == 0 || expected != self.revision {
            return Err(AuthRejection::Conflict);
        }
        Ok(())
    }

    pub(super) fn validate_scope(&self, scope: ResourceScope) -> Result<(), AuthRejection> {
        if scope == ResourceScope::Cluster {
            return Ok(());
        }
        let tenant = scope.tenant().ok_or(AuthRejection::Invalid)?;
        if !self.tenants.contains_key(&tenant) {
            return Err(AuthRejection::NotFound);
        }
        if scope.namespace().is_some_and(|namespace| {
            self.namespaces
                .get(&namespace)
                .is_none_or(|row| row.tenant != tenant)
        }) {
            return Err(AuthRejection::Invalid);
        }
        Ok(())
    }

    fn validate_grant_scope(
        &self,
        principal: &Principal,
        scope: ResourceScope,
    ) -> Result<(), AuthRejection> {
        if scope == ResourceScope::Cluster {
            return Err(AuthRejection::Invalid);
        }
        if !principal.resource_scope().covers(scope) {
            return Err(AuthRejection::Forbidden);
        }
        self.validate_scope(scope)?;
        Ok(())
    }

    fn validate_delegation(
        &self,
        actor: &Principal,
        scope: ResourceScope,
        permissions: &BTreeSet<Permission>,
    ) -> Result<(), AuthRejection> {
        if !actor.resource_scope().covers(scope)
            || permissions
                .iter()
                .any(|permission| !self.allows(actor, *permission, scope))
        {
            return Err(AuthRejection::Forbidden);
        }
        Ok(())
    }

    pub(super) fn validate_principal_delegation(
        &self,
        actor: &Principal,
        target: &Principal,
    ) -> Result<(), AuthRejection> {
        self.validate_delegation(actor, target.resource_scope(), &target.permissions)?;
        for ((principal, scope), permissions) in &self.resource_grants {
            if *principal == target.id {
                self.validate_delegation(actor, *scope, permissions)?;
            }
        }
        Ok(())
    }

    pub(super) fn set_resource_permission(
        &mut self,
        actor: &Principal,
        principal: Id,
        scope: ResourceScope,
        permission: Permission,
        allow: bool,
    ) -> Result<AuthOutcome, AuthRejection> {
        let target = self.managed_resource_principal(actor, principal)?;
        self.validate_live_parents(target)?;
        self.validate_grant_scope(target, scope)?;
        if !permission.is_resource_permission() || !actor.resource_scope().covers(scope) {
            return Err(AuthRejection::Forbidden);
        }
        if allow {
            self.validate_delegation(actor, scope, &[permission].into())?;
        }
        if scope == target.resource_scope() {
            let permissions = &mut self
                .principals
                .get_mut(&principal)
                .ok_or(AuthRejection::NotFound)?
                .permissions;
            if allow {
                permissions.insert(permission);
            } else {
                permissions.remove(&permission);
            }
        } else {
            self.set_explicit_permission(principal, scope, permission, allow)?;
        }
        Ok(AuthOutcome::Principal(principal))
    }

    fn set_explicit_permission(
        &mut self,
        principal: Id,
        scope: ResourceScope,
        permission: Permission,
        allow: bool,
    ) -> Result<(), AuthRejection> {
        let key = (principal, scope);
        if allow {
            if !self.resource_grants.contains_key(&key)
                && (self.resource_grants.len() >= MAX_RESOURCE_GRANTS
                    || self
                        .resource_grants
                        .keys()
                        .filter(|(subject, _)| *subject == principal)
                        .count()
                        >= MAX_PRINCIPAL_GRANTS)
            {
                return Err(AuthRejection::Capacity);
            }
            self.resource_grants
                .entry(key)
                .or_default()
                .insert(permission);
        } else if let Some(permissions) = self.resource_grants.get_mut(&key) {
            permissions.remove(&permission);
            if permissions.is_empty() {
                self.resource_grants.remove(&key);
            }
        }
        Ok(())
    }

    pub(super) fn replace_resource_grants(
        &mut self,
        actor: &Principal,
        principal: Id,
        expected: u64,
        grants: &[ResourceGrant],
    ) -> Result<AuthOutcome, AuthRejection> {
        self.require_revision(expected)?;
        let target = self.managed_resource_principal(actor, principal)?;
        self.validate_live_parents(target)?;
        let mut replacements = self.checked_resource_grants(actor, target, grants)?;
        let mut base = target.permissions.clone();
        base.retain(|permission| !permission.is_resource_permission());
        base.extend(
            replacements
                .remove(&target.resource_scope())
                .unwrap_or_default(),
        );
        let previous = self
            .resource_grants
            .keys()
            .filter(|(subject, _)| *subject == principal)
            .count();
        if self
            .resource_grants
            .len()
            .saturating_sub(previous)
            .saturating_add(replacements.len())
            > MAX_RESOURCE_GRANTS
        {
            return Err(AuthRejection::Capacity);
        }
        self.resource_grants
            .retain(|(subject, _), _| *subject != principal);
        self.resource_grants.extend(
            replacements
                .into_iter()
                .map(|(scope, permissions)| ((principal, scope), permissions)),
        );
        self.principals
            .get_mut(&principal)
            .ok_or(AuthRejection::NotFound)?
            .permissions = base;
        Ok(AuthOutcome::Principal(principal))
    }

    fn managed_resource_principal(
        &self,
        actor: &Principal,
        principal: Id,
    ) -> Result<&Principal, AuthRejection> {
        let missing = if actor.tenant.is_some() {
            AuthRejection::Forbidden
        } else {
            AuthRejection::NotFound
        };
        let target = self.principals.get(&principal).ok_or(missing)?;
        self.manage_principal(actor, target)?;
        Ok(target)
    }

    fn checked_resource_grants(
        &self,
        actor: &Principal,
        target: &Principal,
        grants: &[ResourceGrant],
    ) -> Result<BTreeMap<ResourceScope, BTreeSet<Permission>>, AuthRejection> {
        if grants.len() > MAX_PRINCIPAL_GRANTS {
            return Err(AuthRejection::Capacity);
        }
        let mut replacements = BTreeMap::new();
        for grant in grants {
            self.validate_grant_scope(target, grant.scope)?;
            if grant.permissions.is_empty()
                || grant
                    .permissions
                    .iter()
                    .any(|permission| !permission.is_resource_permission())
            {
                return Err(AuthRejection::Invalid);
            }
            self.validate_delegation(actor, grant.scope, &grant.permissions)?;
            if replacements
                .insert(grant.scope, grant.permissions.clone())
                .is_some()
            {
                return Err(AuthRejection::Invalid);
            }
        }
        Ok(replacements)
    }

    pub(super) fn validate_resource_grants(&self) -> Result<(), AuthRejection> {
        if self.resource_grants.len() > MAX_RESOURCE_GRANTS {
            return Err(AuthRejection::Capacity);
        }
        let mut counts = BTreeMap::<Id, usize>::new();
        for ((principal, scope), permissions) in &self.resource_grants {
            let row = self
                .principals
                .get(principal)
                .ok_or(AuthRejection::Invalid)?;
            self.validate_grant_scope(row, *scope)?;
            if *scope == row.resource_scope()
                || permissions.is_empty()
                || permissions
                    .iter()
                    .any(|permission| !permission.is_resource_permission())
            {
                return Err(AuthRejection::Invalid);
            }
            let count = counts.entry(*principal).or_default();
            *count += 1;
            if *count > MAX_PRINCIPAL_GRANTS {
                return Err(AuthRejection::Capacity);
            }
        }
        Ok(())
    }
}
