use std::collections::BTreeMap;

use axum::http::Uri;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tidemq_protocol::{
    Id,
    console::{Decimal, Query, Reply, SubscriptionProgressObservation},
};
use tidemq_sdk::{NamespaceId, ResourceScope, TenantId, TopicId};

use super::api::{ApiError, ApiResult, BTreeParams, Context, decode, encode, now_ms, parameters};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Cursor {
    pub actor: String,
    pub path: String,
    pub after: String,
    pub revision: u64,
    pub expires_ms: u64,
    pub fingerprint: String,
    pub end: Option<u64>,
}

pub(super) fn cursor(
    context: &Context<'_>,
    path: &str,
    value: Option<&String>,
) -> ApiResult<Option<Cursor>> {
    let Some(value) = value else { return Ok(None) };
    let decoded = context
        .state
        .sessions
        .open_cursor(value)
        .map_err(|_| ApiError::invalid())?;
    let cursor: Cursor = decode(&decoded)?;
    if cursor.actor != context.identity.principal.id.to_string()
        || cursor.path != path
        || cursor.expires_ms <= now_ms()?
    {
        return Err(ApiError::invalid());
    }
    Ok(Some(cursor))
}
pub(super) fn seal_cursor(
    context: &Context<'_>,
    path: &str,
    after: String,
    revision: u64,
    fingerprint: String,
    end: Option<u64>,
) -> ApiResult<String> {
    let value = Cursor {
        actor: context.identity.principal.id.to_string(),
        path: path.into(),
        after,
        revision,
        expires_ms: now_ms()?.saturating_add(300_000),
        fingerprint,
        end,
    };
    context
        .state
        .sessions
        .seal_cursor(&serde_json::to_vec(&value).map_err(|_| ApiError::unavailable())?)
        .map_err(Into::into)
}
pub(super) fn parse_id(value: &str) -> ApiResult<Id> {
    value.parse().map_err(|_| ApiError::invalid())
}
pub(super) fn optional_id(value: Option<&String>) -> ApiResult<Option<Id>> {
    value.map(|value| parse_id(value)).transpose()
}

pub(super) fn listing_fingerprint(params: &BTreeParams) -> ApiResult<String> {
    let mut filters = params.clone();
    filters.remove("cursor");
    filters.remove("limit");
    Ok(
        blake3::hash(&serde_json::to_vec(&filters).map_err(|_| ApiError::invalid())?)
            .to_hex()
            .to_string(),
    )
}

pub(super) async fn query(
    context: &Context<'_>,
    path: &str,
    uri: &Uri,
    body: &[u8],
) -> ApiResult<Value> {
    let params = parameters(uri)?;
    if path == "capabilities" {
        if !params.is_empty() {
            return Err(ApiError::invalid());
        }
        return Ok(context.client.capabilities());
    }
    if path == "components" || path == "alerts" || path.starts_with("components/") {
        return monitoring(context, path, &params).await;
    }
    if matches!(
        path,
        "tenants" | "namespaces" | "principals" | "credentials" | "grants"
    ) {
        return super::api_identity::query(context, path, &params).await;
    }
    let limit = params
        .get("limit")
        .map(|value| value.parse::<u32>().map_err(|_| ApiError::invalid()))
        .transpose()?
        .unwrap_or(32);
    if !(1..=32).contains(&limit) {
        return Err(ApiError::invalid());
    }
    let continuation = cursor(context, path, params.get("cursor"))?;
    let fingerprint = listing_fingerprint(&params)?;
    if continuation
        .as_ref()
        .is_some_and(|cursor| cursor.fingerprint != fingerprint)
    {
        return Err(ApiError::invalid());
    }
    let after = if path == "cluster" || path.ends_with("/producers") {
        None
    } else {
        continuation
            .as_ref()
            .map(|cursor| parse_id(&cursor.after))
            .transpose()?
    };
    let parts: Vec<_> = path.split('/').collect();
    let query = match parts.as_slice() {
        ["topics"] => Query::Topics {
            tenant: optional_id(params.get("tenantId"))?,
            namespace: optional_id(params.get("namespaceId"))?,
            after,
            limit,
        },
        ["topics", topic] => Query::Topic {
            topic: parse_id(topic)?,
        },
        ["topics", topic, "shards"] => Query::Shards {
            topic: parse_id(topic)?,
            after,
            limit,
        },
        ["topics", topic, "messages"] => {
            return messages(context, path, parse_id(topic)?, body).await;
        }
        ["topics", topic, "producers"] => Query::Producers {
            topic: parse_id(topic)?,
            after: continuation
                .as_ref()
                .map(|cursor| {
                    cursor
                        .after
                        .parse::<u64>()
                        .map(Decimal)
                        .map_err(|_| ApiError::invalid())
                })
                .transpose()?,
            limit,
        },
        ["subscriptions"] => Query::Subscriptions {
            topic: optional_id(params.get("topicId"))?,
            after,
            limit,
        },
        ["subscriptions", subscription, "members"] => Query::Members {
            topic: parse_id(params.get("topicId").ok_or_else(ApiError::invalid)?)?,
            subscription: parse_id(subscription)?,
            after,
            limit,
        },
        ["subscriptions", subscription, "progress"] => Query::Progress {
            topic: parse_id(params.get("topicId").ok_or_else(ApiError::invalid)?)?,
            subscription: parse_id(subscription)?,
            after,
            limit,
        },
        ["cluster"] => {
            context.require_operator()?;
            if context.client.is_local() {
                if continuation.is_some() {
                    return Err(ApiError::invalid());
                }
                return context
                    .client
                    .local_observation()
                    .await
                    .map_err(ApiError::read)?
                    .ok_or_else(ApiError::unavailable);
            }
            Query::Cluster {
                after: continuation
                    .as_ref()
                    .map(|cursor| {
                        cursor
                            .after
                            .parse::<u64>()
                            .map(Decimal)
                            .map_err(|_| ApiError::invalid())
                    })
                    .transpose()?,
                limit,
            }
        }
        ["operations", id] => {
            return encode(
                context
                    .client
                    .console_operation(parse_id(id)?)
                    .await
                    .map_err(ApiError::read)?,
            );
        }
        ["overview"] => return overview(context).await,
        _ => return Err(ApiError::not_found()),
    };
    let reply = context
        .client
        .console_read(query)
        .await
        .map_err(ApiError::read)?;
    let mut value = match reply {
        Reply::Topics(value) => encode(value)?,
        Reply::Shards(value) => encode(value)?,
        Reply::Messages(value) => encode(value)?,
        Reply::Subscriptions(value) => encode(value)?,
        Reply::Members(value) => encode(value)?,
        Reply::Progress(value) => encode(value)?,
        Reply::Producers(value) => encode(value)?,
        Reply::Cluster(value) => encode(value)?,
        Reply::Topic(value) => {
            let scope = ResourceScope::Topic {
                tenant: TenantId::from(value.topic.tenant),
                namespace: NamespaceId::from(value.topic.namespace),
                topic: TopicId::from(value.topic.id),
            };
            let permissions = context
                .client
                .effective_permissions(scope)
                .await
                .map_err(ApiError::read)?;
            let mut json = encode(value)?;
            if let Some(object) = json.as_object_mut() {
                if let Some(Value::String(after)) = object.get("nextShardCursor") {
                    let token = seal_cursor(
                        context,
                        &format!("{path}/shards"),
                        after.clone(),
                        0,
                        listing_fingerprint(&BTreeParams::new())?,
                        None,
                    )?;
                    object.insert("nextShardCursor".into(), json!(token));
                }
                object.insert(
                    "effectivePermissions".into(),
                    json!(
                        permissions
                            .principal
                            .effective_permissions
                            .iter()
                            .map(|permission| permission.as_str())
                            .collect::<Vec<_>>()
                    ),
                );
            }
            json
        }
    };
    if let Some(object) = value.as_object_mut()
        && let Some(Value::String(after)) = object.get("nextCursor")
    {
        let after = after.clone();
        let revision = object
            .get("revision")
            .and_then(Value::as_str)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        object.insert(
            "nextCursor".into(),
            json!(seal_cursor(
                context,
                path,
                after,
                revision,
                fingerprint,
                None
            )?),
        );
    }
    Ok(value)
}

async fn monitoring(context: &Context<'_>, path: &str, params: &BTreeParams) -> ApiResult<Value> {
    context.require_operator()?;
    if path == "alerts" {
        return Ok(context.state.observability.alerts().await);
    }
    let continuation = cursor(context, path, params.get("cursor"))?;
    let fingerprint = listing_fingerprint(params)?;
    if continuation
        .as_ref()
        .is_some_and(|cursor| cursor.fingerprint != fingerprint)
    {
        return Err(ApiError::invalid());
    }
    let after = continuation
        .map(|cursor| {
            cursor
                .after
                .parse::<u64>()
                .map(Decimal)
                .map_err(|_| ApiError::invalid())
        })
        .transpose()?;
    let limit = params
        .get("limit")
        .map(|value| value.parse::<u32>().map_err(|_| ApiError::invalid()))
        .transpose()?
        .unwrap_or(32);
    if !(1..=32).contains(&limit) {
        return Err(ApiError::invalid());
    }
    if let Some(snapshot) = context
        .client
        .local_observation()
        .await
        .map_err(ApiError::read)?
    {
        if after.is_some() {
            return Err(ApiError::invalid());
        }
        return local_monitoring(context, path, &snapshot).await;
    }
    let cluster = match context
        .client
        .console_read(Query::Cluster { after, limit })
        .await
    {
        Ok(Reply::Cluster(snapshot)) => encode(snapshot)?,
        Ok(_) => return Err(ApiError::unavailable()),
        Err(error)
            if matches!(
                error.kind(),
                tidemq_sdk::ErrorKind::Unauthenticated | tidemq_sdk::ErrorKind::PermissionDenied
            ) =>
        {
            return Err(ApiError::sdk(error));
        }
        Err(_) => {
            json!({"items":[],"nextCursor":null,"observedAtMs":now_ms()?.to_string(),"source":"tidemq_cluster_unavailable","partial":true})
        }
    };
    if path == "components" {
        return Ok(context.state.observability.components(&cluster).await);
    }
    let parts: Vec<_> = path.split('/').collect();
    let ["components", id, "instances"] = parts.as_slice() else {
        return Err(ApiError::not_found());
    };
    if !context.state.observability.contains_component(id) {
        return Err(ApiError::not_found());
    }
    let mut value = context.state.observability.instances(&cluster, id).await;
    if matches!(*id, "metadata" | "storage" | "broker" | "gateway")
        && let Some(after) = cluster.get("nextCursor").and_then(Value::as_str)
        && let Some(object) = value.as_object_mut()
    {
        object.insert(
            "nextCursor".into(),
            json!(seal_cursor(
                context,
                path,
                after.into(),
                0,
                fingerprint,
                None
            )?),
        );
    }
    Ok(value)
}

async fn local_monitoring(context: &Context<'_>, path: &str, snapshot: &Value) -> ApiResult<Value> {
    let node = snapshot
        .get("items")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .ok_or_else(ApiError::unavailable)?;
    if path == "components" {
        let mut value = context.state.observability.components(snapshot).await;
        let items = value
            .get_mut("items")
            .and_then(Value::as_array_mut)
            .ok_or_else(ApiError::unavailable)?;
        for item in items.iter_mut() {
            let Some(id) = item.get("id").and_then(Value::as_str) else {
                continue;
            };
            if matches!(
                id,
                "console" | "metadata" | "storage" | "broker" | "gateway"
            ) {
                *item = local_component(node, id, snapshot);
            }
        }
        if let Some(object) = value.as_object_mut() {
            object.insert("source".into(), json!("local_process_and_fixed_probes"));
            object.insert(
                "partial".into(),
                json!(items_partial(
                    snapshot,
                    &object.get("items").cloned().unwrap_or(Value::Null)
                )),
            );
        }
        return Ok(value);
    }
    let parts: Vec<_> = path.split('/').collect();
    let ["components", id, "instances"] = parts.as_slice() else {
        return Err(ApiError::not_found());
    };
    if !context.state.observability.contains_component(id) {
        return Err(ApiError::not_found());
    }
    if !matches!(
        *id,
        "console" | "metadata" | "storage" | "broker" | "gateway"
    ) {
        return Ok(context.state.observability.instances(snapshot, id).await);
    }
    let mut item = node.clone();
    let object = item.as_object_mut().ok_or_else(ApiError::unavailable)?;
    object.insert(
        "id".into(),
        json!(format!(
            "{id}:{}",
            node.get("id")
                .and_then(Value::as_str)
                .ok_or_else(ApiError::unavailable)?
        )),
    );
    object.insert(
        "nodeId".into(),
        node.get("id").cloned().unwrap_or(Value::Null),
    );
    object.insert("role".into(), json!(id));
    object.insert("zone".into(), Value::Null);
    object.insert("health".into(), json!(local_health(node)));
    object.insert(
        "detail".into(),
        json!("Shared process readiness and local durable execution"),
    );
    object.insert("source".into(), json!("local_process_authority"));
    object.insert(
        "observedAtMs".into(),
        snapshot.get("observedAtMs").cloned().unwrap_or(Value::Null),
    );
    object.insert("partial".into(), json!(false));
    Ok(
        json!({"items":[item],"nextCursor":null,"observedAtMs":snapshot.get("observedAtMs"),"source":"local_process_authority","partial":false}),
    )
}

fn local_component(node: &Value, id: &str, snapshot: &Value) -> Value {
    json!({"id":id,"name":id,"health":local_health(node),"detail":"Shared process readiness and local durable execution",
        "instances":"1","registered":"1","ready": if node.get("ready").and_then(Value::as_bool)==Some(true) {"1"}else{"0"},
        "links":[],"execution":"local","observedAtMs":snapshot.get("observedAtMs"),"source":"local_process_authority","partial":false})
}

fn local_health(node: &Value) -> &'static str {
    if node.get("ready").and_then(Value::as_bool) == Some(true) {
        "healthy"
    } else {
        "unavailable"
    }
}

fn items_partial(snapshot: &Value, items: &Value) -> bool {
    snapshot.get("partial").and_then(Value::as_bool) != Some(false)
        || items.as_array().is_none_or(|items| {
            items
                .iter()
                .any(|item| item.get("partial").and_then(Value::as_bool) != Some(false))
        })
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MessageQuery {
    shard: Id,
    start_offset: Option<Decimal>,
    end_offset: Option<Decimal>,
    observed_end: Option<Decimal>,
    message_id: Option<String>,
    cursor: Option<String>,
    key_base64: Option<String>,
    #[serde(default)]
    properties_base64: BTreeMap<String, String>,
    #[serde(default = "message_limit")]
    limit: u32,
}
fn message_limit() -> u32 {
    32
}
async fn messages(context: &Context<'_>, path: &str, topic: Id, body: &[u8]) -> ApiResult<Value> {
    let mut input: MessageQuery = decode(body)?;
    if !(1..=32).contains(&input.limit) {
        return Err(ApiError::invalid());
    }
    if let Some(id) = input.message_id.as_ref() {
        let fields: Vec<_> = id.split(':').collect();
        let [topic_id, shard_id, offset] = fields.as_slice() else {
            return Err(ApiError::invalid());
        };
        if parse_id(topic_id)? != topic || parse_id(shard_id)? != input.shard {
            return Err(ApiError::invalid());
        }
        let offset = offset.parse::<u64>().map_err(|_| ApiError::invalid())?;
        input.start_offset = Some(Decimal(offset.saturating_sub(16)));
        input.end_offset = Some(Decimal(offset.saturating_add(17)));
    }
    let fingerprint = blake3::hash(
        &serde_json::to_vec(&(
            topic,
            input.shard,
            &input.key_base64,
            &input.properties_base64,
            input.end_offset,
        ))
        .map_err(|_| ApiError::invalid())?,
    )
    .to_hex()
    .to_string();
    if let Some(cursor) = cursor(context, path, input.cursor.as_ref())? {
        if cursor.fingerprint != fingerprint {
            return Err(ApiError::invalid());
        }
        input.start_offset = Some(Decimal(
            cursor.after.parse().map_err(|_| ApiError::invalid())?,
        ));
        input.observed_end = cursor.end.map(Decimal);
    }
    let query = Query::Messages {
        topic,
        shard: input.shard,
        start: input.start_offset,
        end: input.end_offset,
        observed_end: input.observed_end,
        limit: input.limit,
        scan_limit: 512,
        key_base64: input.key_base64,
        properties_base64: input.properties_base64,
    };
    let Reply::Messages(reply) = context
        .client
        .console_read(query)
        .await
        .map_err(ApiError::read)?
    else {
        return Err(ApiError::unavailable());
    };
    let cursor = reply
        .next_offset
        .map(|offset| {
            seal_cursor(
                context,
                path,
                offset.0.to_string(),
                0,
                fingerprint,
                Some(reply.observed_end.0),
            )
        })
        .transpose()?;
    let mut value = encode(reply)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("nextCursor".into(), json!(cursor));
    }
    Ok(value)
}

async fn overview(context: &Context<'_>) -> ApiResult<Value> {
    let Reply::Topics(topics) = context
        .client
        .console_read(Query::Topics {
            tenant: None,
            namespace: None,
            after: None,
            limit: 32,
        })
        .await
        .map_err(ApiError::read)?
    else {
        return Err(ApiError::unavailable());
    };
    let Reply::Subscriptions(subscriptions) = context
        .client
        .console_read(Query::Subscriptions {
            topic: None,
            after: None,
            limit: 32,
        })
        .await
        .map_err(ApiError::read)?
    else {
        return Err(ApiError::unavailable());
    };
    let mut progress_observation = SubscriptionProgressObservation::combine(
        subscriptions
            .items
            .iter()
            .map(|subscription| subscription.progress_observation.clone()),
        subscriptions.next_cursor.is_none() && !subscriptions.observation.partial,
    );
    let unconfirmed = subscriptions
        .items
        .iter()
        .try_fold(0_u64, |count, subscription| {
            count.checked_add(subscription.unconfirmed?.0)
        });
    if unconfirmed.is_none() && !progress_observation.partial {
        progress_observation.partial = true;
        progress_observation.reason = Some("counterOverflow".into());
    }
    let incomplete = topics.next_cursor.is_some()
        || subscriptions.next_cursor.is_some()
        || topics.observation.partial
        || subscriptions.observation.partial
        || progress_observation.partial;
    let anomalies=subscriptions.items.iter().filter_map(|subscription|subscription.reason.as_ref().map(|reason|json!({"id":subscription.id,"summary":reason,"topicId":subscription.topic_id,"subscriptionId":subscription.id}))).collect::<Vec<_>>();
    Ok(
        json!({"topics":if topics.next_cursor.is_none(){Some(topics.items.len().to_string())}else{None},"consumers":if subscriptions.next_cursor.is_none(){Some(subscriptions.items.len().to_string())}else{None},"unconfirmed":if incomplete{None}else{unconfirmed.map(|value|value.to_string())},"progressObservation":progress_observation,"alerts":null,"anomalies":anomalies,"observedAtMs":now_ms()?.to_string(),"source":"authorized_topic_and_subscription_pages","partial":incomplete}),
    )
}
