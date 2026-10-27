use super::*;

pub(super) fn authorize_identity(
    state: &metadata::MetadataState,
    credential: Id,
    now: u64,
    operation: Id,
    payload: &[u8],
    scope: metadata::ResourceScope,
) -> Result<()> {
    let auth = state.auth();
    let actor = auth
        .authorize(credential, now, Permission::ManageCredentials, None)
        .map_err(|_| forbidden())?;
    if actor.tenant.is_some_and(|id| scope.tenant() != Some(id))
        || actor
            .namespace
            .is_some_and(|id| scope.namespace() != Some(id))
    {
        return Err(forbidden());
    }
    let request = public::ManageIdentity::decode(payload)
        .map_err(|_| Error::invalid("Bound identity command is invalid"))?;
    let (tenant, namespace) = identity_target_scope(state, operation, &request)?;
    if actor.tenant.is_some_and(|id| Some(id) != tenant)
        || actor.namespace.is_some_and(|id| Some(id) != namespace)
    {
        return Err(forbidden());
    }
    Ok(())
}

fn identity_target_scope(
    state: &metadata::MetadataState,
    operation: Id,
    request: &public::ManageIdentity,
) -> Result<(Option<Id>, Option<Id>)> {
    let auth = state.auth();
    let action = public::IdentityAction::try_from(request.action)
        .map_err(|_| Error::invalid("Bound identity action is invalid"))?;
    let principal = match action {
        public::IdentityAction::PutPrincipalIdentity => auth.principal(operation),
        public::IdentityAction::IssueCredential => Some(
            auth.principal(wire::id(&request.parent_id)?)
                .ok_or_else(forbidden)?,
        ),
        public::IdentityAction::RevokeCredential => {
            let target = auth
                .credential(wire::id(&request.target_id)?)
                .ok_or_else(forbidden)?;
            Some(auth.principal(target.principal).ok_or_else(forbidden)?)
        }
        _ => Some(
            auth.principal(wire::id(&request.target_id)?)
                .ok_or_else(forbidden)?,
        ),
    };
    if let Some(principal) = principal {
        Ok((principal.tenant, principal.namespace))
    } else {
        Ok((
            Some(wire::id(&request.parent_id)?),
            (!request.namespace_id.is_empty())
                .then(|| wire::id(&request.namespace_id))
                .transpose()?,
        ))
    }
}

pub(super) fn scoped_binding(
    state: &metadata::MetadataState,
    credential: Id,
    request: &WriteRequest,
    now: u64,
) -> Result<ConsoleBinding> {
    match &request.command {
        WriteCommand::Send {
            topic,
            key_base64,
            payload_base64,
            properties_base64,
        } => {
            let message = decode_message(payload_base64, key_base64.as_deref(), properties_base64)?;
            send_binding(
                state,
                credential,
                request.operation_id,
                *topic,
                message,
                now,
            )
        }
        WriteCommand::CreateTopic {
            tenant,
            namespace,
            name,
        } => {
            let tenant_record = state.auth().tenant(*tenant).ok_or_else(forbidden)?;
            let namespace_record = state
                .auth()
                .namespace(*namespace)
                .filter(|row| row.tenant == *tenant)
                .ok_or_else(forbidden)?;
            Ok(ConsoleBinding::CreateTopic(format!(
                "{}/{}/{name}",
                tenant_record.name, namespace_record.name
            )))
        }
        WriteCommand::ReplaceScope {
            principal,
            expected_revision,
            scope,
            permissions,
        } => grant_binding(state, *principal, expected_revision.0, scope, permissions),
        command => prepare_binding(state, command)?
            .ok_or_else(|| Error::invalid("Console command is missing")),
    }
}

pub(super) fn identity_scope(
    state: &metadata::MetadataState,
    command: &WriteCommand,
) -> Result<metadata::ResourceScope> {
    let principal = match command {
        WriteCommand::CreatePrincipal {
            tenant, namespace, ..
        } => {
            return principal_scope(Some(*tenant), *namespace);
        }
        WriteCommand::SetPermission { scope, .. } | WriteCommand::ReplaceScope { scope, .. } => {
            return metadata::ResourceScope::from_wire(&scope_wire(scope))
                .map_err(|_| Error::invalid("Console resource scope is invalid"));
        }
        WriteCommand::SetPrincipalEnabled { principal, .. }
        | WriteCommand::IssueCredential { principal, .. }
        | WriteCommand::ReplaceGrants { principal, .. } => *principal,
        WriteCommand::RevokeCredential { credential } => {
            state
                .auth()
                .credential(*credential)
                .ok_or_else(forbidden)?
                .principal
        }
        _ => return Err(Error::invalid("Console identity scope is missing")),
    };
    let principal = state.auth().principal(principal).ok_or_else(forbidden)?;
    principal_scope(principal.tenant, principal.namespace)
}

fn principal_scope(tenant: Option<Id>, namespace: Option<Id>) -> Result<metadata::ResourceScope> {
    match (tenant, namespace) {
        (None, None) => Ok(metadata::ResourceScope::Cluster),
        (Some(tenant), None) => Ok(metadata::ResourceScope::Tenant(tenant)),
        (Some(tenant), Some(namespace)) => {
            Ok(metadata::ResourceScope::Namespace { tenant, namespace })
        }
        (None, Some(_)) => Err(Error::invalid("A Console namespace requires its tenant")),
    }
}

fn send_binding(
    state: &metadata::MetadataState,
    credential: Id,
    id: Id,
    topic: Id,
    message: public::PublishMessage,
    now: u64,
) -> Result<ConsoleBinding> {
    let registration = state.publishers().next_registration();
    let identity = tidemq_protocol::topic::SendIdentity {
        topic,
        producer: id,
        registration,
        sequence: 1,
    };
    let hash = tidemq_protocol::topic::routing_hash(message.key.as_deref(), id, 1)
        .map_err(|source| Error::operation("route console send", source))?;
    let shard = state
        .topology()
        .layout(topic)
        .and_then(|layout| {
            layout
                .shards
                .iter()
                .find(|shard| shard.range.contains(hash))
        })
        .and_then(|route| state.topology().shard(route.id))
        .ok_or_else(super::super::metadata::not_found)?;
    let expires_ms = now.saturating_add(metadata::PUBLISHER_LEASE_MS).min(
        state
            .auth()
            .credential(credential)
            .ok_or_else(forbidden)?
            .expires_ms,
    );
    Ok(ConsoleBinding::Send {
        topic,
        shard: shard.route.id,
        partition: shard.partition,
        group: shard.route.group,
        permit: crate::producer::PublisherPermit {
            topic,
            producer: id,
            registration,
            client: id,
            credential,
            expires_ms,
            live: true,
        },
        digest: tidemq_protocol::topic::message_digest(
            identity,
            message.key.as_deref(),
            &message.payload,
            &message.properties,
        )
        .map_err(|source| Error::operation("bind console message", source))?,
    })
}

fn grant_binding(
    state: &metadata::MetadataState,
    principal: Id,
    revision: u64,
    scope: &api::Scope,
    permissions: &[String],
) -> Result<ConsoleBinding> {
    if permissions.len() > 5 || revision == 0 {
        return Err(Error::invalid(
            "Resource grant revision or permission bounds are invalid",
        ));
    }
    let target = metadata::ResourceScope::from_wire(&scope_wire(scope))
        .map_err(|_| Error::invalid("Resource scope is invalid"))?;
    let mut grants = state
        .auth()
        .direct_resource_grants(principal)
        .into_iter()
        .filter(|grant| grant.scope != target && grant.scope != metadata::ResourceScope::Cluster)
        .filter_map(|mut grant| {
            grant.permissions.retain(|permission| {
                !matches!(
                    permission,
                    Permission::ManageCredentials | Permission::ManageTenants
                )
            });
            (!grant.permissions.is_empty()).then_some(grant)
        })
        .map(|grant| grant.to_wire())
        .collect::<Vec<_>>();
    if !permissions.is_empty() {
        grants.push(public::ResourceGrant {
            scope: Some(scope_wire(scope)),
            permissions: permissions.to_vec(),
        });
    }
    let request = public::ManageIdentity {
        action: public::IdentityAction::ReplaceResourceGrants.into(),
        target_id: id_bytes(principal),
        expected_auth_revision: revision,
        resource_grants: grants,
        ..Default::default()
    };
    if request.encoded_len() > 4096 {
        return Err(Error::invalid(
            "Identity request exceeds its finite byte limit",
        ));
    }
    Ok(ConsoleBinding::Identity {
        payload: request.encode_to_vec(),
        scope: target,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clearing_a_scope_keeps_the_selected_scope_when_the_native_grant_list_is_empty() {
        let state = metadata::MetadataState::default();
        let principal = Id::new_v7().unwrap();
        let tenant = Id::new_v7().unwrap();
        let namespace = Id::new_v7().unwrap();
        let topic = Id::new_v7().unwrap();
        for (scope, expected) in [
            (
                api::Scope {
                    tenant,
                    namespace: None,
                    topic: None,
                },
                format!("tenant:{tenant}"),
            ),
            (
                api::Scope {
                    tenant,
                    namespace: Some(namespace),
                    topic: None,
                },
                format!("namespace:{tenant}/{namespace}"),
            ),
            (
                api::Scope {
                    tenant,
                    namespace: Some(namespace),
                    topic: Some(topic),
                },
                format!("topic:{tenant}/{namespace}/{topic}"),
            ),
        ] {
            let binding = grant_binding(&state, principal, 1, &scope, &[]).unwrap();
            let (target, actual) = binding.labels().unwrap();
            assert_eq!(target, principal.to_string());
            assert_eq!(actual, expected);
            let ConsoleBinding::Identity { payload, .. } = binding else {
                panic!("A scope change must bind an identity request");
            };
            let native = public::ManageIdentity::decode(payload.as_slice()).unwrap();
            assert!(native.resource_grants.is_empty());
        }
    }

    #[test]
    fn principal_creation_captures_its_explicit_scope_and_missing_targets_are_not_guessed() {
        let state = metadata::MetadataState::default();
        let tenant = Id::new_v7().unwrap();
        let namespace = Id::new_v7().unwrap();
        let command = WriteCommand::CreatePrincipal {
            name: "new-identity".into(),
            tenant,
            namespace: Some(namespace),
            permissions: vec!["produce".into()],
        };
        let binding = prepare_binding(&state, &command).unwrap().unwrap();
        assert_eq!(
            binding.labels().unwrap(),
            (
                "new-identity".into(),
                format!("namespace:{tenant}/{namespace}")
            )
        );
        let command = WriteCommand::IssueCredential {
            principal: Id::new_v7().unwrap(),
            lifetime_ms: Decimal(300_000),
        };
        assert!(identity_scope(&state, &command).is_err());
    }
}
