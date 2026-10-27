use std::collections::BTreeMap;

use axum::http::Method;
use serde::Deserialize;
use serde_json::Value;
use tidemq_protocol::{
    Id,
    console::Decimal,
    console_operation::{Scope, WriteCommand, WriteRequest},
};
use tidemq_sdk::ResourceScope;

use super::api::{ApiError, ApiResult, Context, decode, encode};
use super::api_query::parse_id;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateTopic {
    operation_id: Id,
    tenant_id: Id,
    namespace_id: Id,
    name: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OperationId {
    operation_id: Id,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreatePrincipal {
    operation_id: Id,
    name: String,
    tenant_id: Id,
    namespace_id: Option<Id>,
    #[serde(default)]
    permissions: Vec<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Enabled {
    operation_id: Id,
    enabled: bool,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Credential {
    operation_id: Id,
    principal_id: Id,
    lifetime_ms: Decimal,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GrantInput {
    operation_id: Id,
    principal_id: Id,
    tenant_id: Option<Id>,
    namespace_id: Option<Id>,
    topic_id: Option<Id>,
    permissions: Vec<String>,
    expected_version: Decimal,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Send {
    operation_id: Id,
    payload_base64: String,
    key_base64: Option<String>,
    #[serde(default)]
    properties_base64: BTreeMap<String, String>,
}

pub(super) async fn write(
    context: &Context<'_>,
    method: &Method,
    path: &str,
    body: &[u8],
) -> ApiResult<Value> {
    let parts: Vec<_> = path.split('/').collect();
    let request = match (method, parts.as_slice()) {
        (&Method::POST, ["topics"]) => {
            let input: CreateTopic = decode(body)?;
            WriteRequest {
                operation_id: input.operation_id,
                command: WriteCommand::CreateTopic {
                    tenant: input.tenant_id,
                    namespace: input.namespace_id,
                    name: input.name,
                },
            }
        }
        (&Method::DELETE, ["topics", id]) => {
            let input: OperationId = decode(body)?;
            WriteRequest {
                operation_id: input.operation_id,
                command: WriteCommand::DeleteTopic {
                    topic: parse_id(id)?,
                },
            }
        }
        (&Method::POST, ["principals"]) => {
            let input: CreatePrincipal = decode(body)?;
            WriteRequest {
                operation_id: input.operation_id,
                command: WriteCommand::CreatePrincipal {
                    name: input.name,
                    tenant: input.tenant_id,
                    namespace: input.namespace_id,
                    permissions: input.permissions,
                },
            }
        }
        (&Method::PATCH, ["principals", id]) => {
            let input: Enabled = decode(body)?;
            WriteRequest {
                operation_id: input.operation_id,
                command: WriteCommand::SetPrincipalEnabled {
                    principal: parse_id(id)?,
                    enabled: input.enabled,
                },
            }
        }
        (&Method::POST, ["credentials"]) => {
            let input: Credential = decode(body)?;
            WriteRequest {
                operation_id: input.operation_id,
                command: WriteCommand::IssueCredential {
                    principal: input.principal_id,
                    lifetime_ms: input.lifetime_ms,
                },
            }
        }
        (&Method::DELETE, ["credentials", id]) => {
            let input: OperationId = decode(body)?;
            WriteRequest {
                operation_id: input.operation_id,
                command: WriteCommand::RevokeCredential {
                    credential: parse_id(id)?,
                },
            }
        }
        (&Method::PUT, ["grants"]) => grant_request(context, decode(body)?).await?,
        (&Method::POST, ["topics", id, "send"]) => {
            let input: Send = decode(body)?;
            WriteRequest {
                operation_id: input.operation_id,
                command: WriteCommand::Send {
                    topic: parse_id(id)?,
                    payload_base64: input.payload_base64,
                    key_base64: input.key_base64,
                    properties_base64: input.properties_base64,
                },
            }
        }
        _ => return Err(ApiError::not_found()),
    };
    encode(
        context
            .client
            .console_write(&request)
            .await
            .map_err(ApiError::sdk)?,
    )
}

async fn grant_request(context: &Context<'_>, input: GrantInput) -> ApiResult<WriteRequest> {
    if input.permissions.len() > 5 || input.expected_version.0 == 0 {
        return Err(ApiError::invalid());
    }
    let scope = super::api_identity::resolve_scope(
        context,
        input.tenant_id,
        input.namespace_id,
        input.topic_id,
    )
    .await?;
    Ok(WriteRequest {
        operation_id: input.operation_id,
        command: WriteCommand::ReplaceScope {
            principal: input.principal_id,
            expected_revision: input.expected_version,
            scope: convert_scope(scope)?,
            permissions: input.permissions,
        },
    })
}
fn convert_scope(scope: ResourceScope) -> ApiResult<Scope> {
    match scope {
        ResourceScope::Tenant(tenant) => Ok(Scope {
            tenant: tenant.into(),
            namespace: None,
            topic: None,
        }),
        ResourceScope::Namespace { tenant, namespace } => Ok(Scope {
            tenant: tenant.into(),
            namespace: Some(namespace.into()),
            topic: None,
        }),
        ResourceScope::Topic {
            tenant,
            namespace,
            topic,
        } => Ok(Scope {
            tenant: tenant.into(),
            namespace: Some(namespace.into()),
            topic: Some(topic.into()),
        }),
        ResourceScope::Cluster => Err(ApiError::invalid()),
    }
}
