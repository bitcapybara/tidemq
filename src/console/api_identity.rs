use serde_json::{Value, json};
use tidemq_protocol::Id;
use tidemq_sdk::{
    IdentityStatus, PrincipalInfo, ResourceGrant, ResourceList, ResourceScope, TenantId, TopicId,
};

use super::api::{ApiError, ApiResult, BTreeParams, Context, page};
use super::api_query::{cursor, listing_fingerprint, optional_id, parse_id, seal_cursor};

pub(super) async fn query(
    context: &Context<'_>,
    path: &str,
    params: &BTreeParams,
) -> ApiResult<Value> {
    let tenant = optional_id(params.get("tenantId"))?.or_else(|| {
        context
            .identity
            .principal
            .tenant
            .and_then(|id| id.to_string().parse().ok())
    });
    let limit = params
        .get("limit")
        .map(|value| value.parse::<u32>().map_err(|_| ApiError::invalid()))
        .transpose()?
        .unwrap_or(32);
    if !(1..=32).contains(&limit) {
        return Err(ApiError::invalid());
    }
    match path {
        "tenants" => {
            let mut listing = context.client.admin().tenants().page_size(limit);
            resume(context, path, params, &mut listing)?;
            let items=listing.next_page().await.map_err(ApiError::read)?.unwrap_or_default().into_iter().map(|item|json!({"id":item.id.to_string(),"name":item.name,"enabled":item.status==IdentityStatus::Active})).collect();
            finish(context, path, params, listing, items)
        }
        "namespaces" => {
            let tenant = tenant.ok_or_else(ApiError::invalid)?;
            let mut listing = context
                .client
                .admin()
                .namespaces(TenantId::from(tenant))
                .page_size(limit);
            resume(context, path, params, &mut listing)?;
            let items=listing.next_page().await.map_err(ApiError::read)?.unwrap_or_default().into_iter().map(|item|json!({"id":item.id.to_string(),"tenantId":item.tenant.to_string(),"name":item.name,"enabled":item.status==IdentityStatus::Active})).collect();
            finish(context, path, params, listing, items)
        }
        "principals" => principals(context, path, params, tenant, limit).await,
        "credentials" => {
            let id = parse_id(params.get("principalId").ok_or_else(ApiError::invalid)?)?;
            let mut listing = context
                .client
                .admin()
                .credentials(id.into())
                .page_size(limit);
            resume(context, path, params, &mut listing)?;
            let items=listing.next_page().await.map_err(ApiError::read)?.unwrap_or_default().into_iter().map(|item|json!({"id":item.id.to_string(),"principalId":item.principal.to_string(),"name":item.id.to_string(),"enabled":item.enabled,"expiresAtMs":item.expires_at_unix_ms.to_string(),"createdAt":null})).collect();
            finish(context, path, params, listing, items)
        }
        "grants" => grants(context, params).await,
        _ => Err(ApiError::not_found()),
    }
}

async fn principals(
    context: &Context<'_>,
    path: &str,
    params: &BTreeParams,
    tenant: Option<Id>,
    limit: u32,
) -> ApiResult<Value> {
    if !context
        .identity
        .principal
        .permissions
        .contains(&tidemq_sdk::Permission::ManageCredentials)
    {
        return page(
            vec![principal_json(&context.identity.principal)],
            None,
            Some(context.identity.revision),
        );
    }
    let admin = context.client.admin();
    let mut listing = if let Some(tenant) = tenant {
        admin.principals(tenant.into())
    } else {
        admin.platform_principals()
    }
    .page_size(limit);
    resume(context, path, params, &mut listing)?;
    let items = listing
        .next_page()
        .await
        .map_err(ApiError::read)?
        .unwrap_or_default()
        .iter()
        .map(principal_json)
        .collect();
    finish(context, path, params, listing, items)
}
fn principal_json(principal: &PrincipalInfo) -> Value {
    json!({"id":principal.id.to_string(),"name":principal.name,"kind":null,"enabled":principal.enabled,"tenant":principal.tenant.map(|id|id.to_string()),"namespace":principal.namespace.map(|id|id.to_string()),"permissions":principal.permissions.iter().map(|permission|permission.as_str()).collect::<Vec<_>>(),"resourceGrants":principal.resource_grants.iter().map(grant_json).collect::<Vec<_>>()})
}

fn resume<T>(
    context: &Context<'_>,
    path: &str,
    params: &BTreeParams,
    listing: &mut ResourceList<T>,
) -> ApiResult<()> {
    if let Some(cursor) = cursor(context, path, params.get("cursor"))? {
        if cursor.fingerprint != listing_fingerprint(params)? {
            return Err(ApiError::invalid());
        }
        let id = parse_id(&cursor.after)?;
        listing.set_continuation(id, cursor.revision);
    }
    Ok(())
}
fn finish<T>(
    context: &Context<'_>,
    path: &str,
    params: &BTreeParams,
    listing: ResourceList<T>,
    items: Vec<Value>,
) -> ApiResult<Value> {
    let cursor = listing
        .continuation()
        .map_err(ApiError::read)?
        .map(|(after, revision)| {
            seal_cursor(
                context,
                path,
                after.to_string(),
                revision,
                listing_fingerprint(params)?,
                None,
            )
        })
        .transpose()?;
    page(items, cursor, listing.revision())
}

pub(super) fn grant_json(grant: &ResourceGrant) -> Value {
    let (kind, id, tenant, namespace, topic) = scope_parts(grant.scope);
    json!({"resourceKind":kind,"resourceId":id,"tenantId":tenant,"namespaceId":namespace,"topicId":topic,"permissions":grant.permissions.iter().map(|permission|permission.as_str()).collect::<Vec<_>>()})
}
fn scope_parts(
    scope: ResourceScope,
) -> (
    &'static str,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    match scope {
        ResourceScope::Cluster => ("cluster", "cluster".into(), None, None, None),
        ResourceScope::Tenant(tenant) => (
            "tenant",
            tenant.to_string(),
            Some(tenant.to_string()),
            None,
            None,
        ),
        ResourceScope::Namespace { tenant, namespace } => (
            "namespace",
            namespace.to_string(),
            Some(tenant.to_string()),
            Some(namespace.to_string()),
            None,
        ),
        ResourceScope::Topic {
            tenant,
            namespace,
            topic,
        } => (
            "topic",
            topic.to_string(),
            Some(tenant.to_string()),
            Some(namespace.to_string()),
            Some(topic.to_string()),
        ),
    }
}

pub(super) async fn resolve_scope(
    context: &Context<'_>,
    tenant: Option<Id>,
    namespace: Option<Id>,
    topic: Option<Id>,
) -> ApiResult<ResourceScope> {
    let tenant = tenant.or_else(|| context.identity.principal.tenant.map(Id::from));
    match (tenant, namespace, topic) {
        (Some(tenant), namespace, None) => Ok(match namespace {
            Some(namespace) => ResourceScope::Namespace {
                tenant: tenant.into(),
                namespace: namespace.into(),
            },
            None => ResourceScope::Tenant(tenant.into()),
        }),
        (tenant, namespace, Some(id)) => {
            if let (Some(tenant), Some(namespace)) = (tenant, namespace) {
                return Ok(ResourceScope::Topic {
                    tenant: tenant.into(),
                    namespace: namespace.into(),
                    topic: TopicId::from(id),
                });
            }
            let tidemq_protocol::console::Reply::Topic(topic) = context
                .client
                .console_read(tidemq_protocol::console::Query::Topic { topic: id })
                .await
                .map_err(ApiError::read)?
            else {
                return Err(ApiError::unavailable());
            };
            if tenant.is_some_and(|tenant| tenant != topic.topic.tenant)
                || namespace.is_some_and(|namespace| namespace != topic.topic.namespace)
            {
                return Err(ApiError::invalid());
            }
            Ok(ResourceScope::Topic {
                tenant: topic.topic.tenant.into(),
                namespace: topic.topic.namespace.into(),
                topic: id.into(),
            })
        }
        _ => Err(ApiError::invalid()),
    }
}

async fn grants(context: &Context<'_>, params: &BTreeParams) -> ApiResult<Value> {
    if params.keys().any(|key| {
        !matches!(
            key.as_str(),
            "principalId" | "tenantId" | "namespaceId" | "topicId" | "cursor" | "limit"
        )
    }) {
        return Err(ApiError::invalid());
    }
    let principal = optional_id(params.get("principalId"))?;
    let tenant = optional_id(params.get("tenantId"))?;
    let namespace = optional_id(params.get("namespaceId"))?;
    let topic = optional_id(params.get("topicId"))?;
    let scope = if tenant.is_some() || namespace.is_some() || topic.is_some() {
        Some(resolve_scope(context, tenant, namespace, topic).await?)
    } else {
        None
    };
    let Some(scope) = scope else {
        let target = if let Some(id) = principal {
            context
                .client
                .principal_identity(id.into(), None)
                .await
                .map_err(ApiError::read)?
        } else {
            context
                .client
                .current_identity()
                .await
                .map_err(ApiError::read)?
        };
        let mut items = Vec::new();
        for grant in &target.principal.resource_grants {
            if grant.scope == ResourceScope::Cluster {
                continue;
            }
            let view = context
                .client
                .principal_identity(target.principal.id, Some(grant.scope))
                .await
                .map_err(ApiError::read)?;
            items.push(authorization_json(
                &view.principal,
                grant.scope,
                view.revision,
            ));
        }
        return page(items, None, Some(target.revision));
    };
    if let Some(principal) = principal {
        let value = context
            .client
            .principal_identity(principal.into(), Some(scope))
            .await
            .map_err(ApiError::read)?;
        return page(
            vec![authorization_json(&value.principal, scope, value.revision)],
            None,
            Some(value.revision),
        );
    }
    if !context
        .identity
        .principal
        .permissions
        .contains(&tidemq_sdk::Permission::ManageCredentials)
    {
        let value = context
            .client
            .effective_permissions(scope)
            .await
            .map_err(ApiError::read)?;
        return page(
            vec![authorization_json(&value.principal, scope, value.revision)],
            None,
            Some(value.revision),
        );
    }
    let mut listing = context
        .client
        .admin()
        .resource_authorizations(scope)
        .page_size(32);
    resume(context, "grants", params, &mut listing)?;
    let rows = listing
        .next_page()
        .await
        .map_err(ApiError::read)?
        .unwrap_or_default();
    let revision = listing.revision().ok_or_else(ApiError::unavailable)?;
    let items = rows
        .iter()
        .map(|row| authorization_json(row, scope, revision))
        .collect();
    finish(context, "grants", params, listing, items)
}

fn authorization_json(principal: &PrincipalInfo, scope: ResourceScope, revision: u64) -> Value {
    let (kind, id, tenant, namespace, topic) = scope_parts(scope);
    let direct = principal
        .effective_grants
        .iter()
        .filter(|grant| grant.scope == scope)
        .flat_map(|grant| {
            grant
                .permissions
                .iter()
                .map(|permission| permission.as_str())
        })
        .collect::<Vec<_>>();
    let inherited=principal.effective_grants.iter().filter(|grant|grant.scope!=scope).flat_map(|grant|grant.permissions.iter().map(|permission|json!({"permission":permission.as_str(),"source":scope_parts(grant.scope).1}))).collect::<Vec<_>>();
    json!({"principalId":principal.id.to_string(),"principalName":principal.name,"resourceKind":kind,"resourceId":id,"tenantId":tenant,"namespaceId":namespace,"topicId":topic,"version":revision.to_string(),"direct":direct,"inherited":inherited,"effective":principal.effective_permissions.iter().map(|permission|permission.as_str()).collect::<Vec<_>>()})
}
