use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json,
    body::Bytes,
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tidemq_sdk::{ErrorKind, Permission, PrincipalAuthorization};
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

use super::{
    ConsoleConfig, EmbeddedConsoleConfig,
    backend::{Backend, Connection},
    http::StateData,
    session::{Session, SessionError, SessionManager},
};

pub(crate) struct ApiState {
    cluster_name: String,
    pub sessions: SessionManager,
    backend: Backend,
    admission: Semaphore,
    pub observability: super::observability::Observability,
}

impl ApiState {
    pub async fn load(config: &ConsoleConfig) -> crate::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            cluster_name: config.cluster.server_name.clone(),
            sessions: SessionManager::load(&config.session).await?,
            backend: Backend::load(config).await?,
            admission: Semaphore::new(4),
            observability: super::observability::Observability::load(config)?,
        }))
    }

    pub(crate) async fn load_local(
        config: &EmbeddedConsoleConfig,
        backend: crate::runtime::LocalConsole,
    ) -> crate::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            cluster_name: "Combined".into(),
            sessions: SessionManager::load(&config.session).await?,
            backend: Backend::Local(backend),
            admission: Semaphore::new(4),
            observability: super::observability::Observability::load_config(&config.observability)?,
        }))
    }

    async fn client(&self, token: &str) -> ApiResult<Connection> {
        self.backend
            .connect(token)
            .await
            .map_err(ApiError::identity)
    }
}

pub(super) struct Context<'a> {
    pub state: &'a ApiState,
    pub client: &'a Connection,
    pub identity: PrincipalAuthorization,
    pub session: Session,
}
impl Context<'_> {
    pub fn operator(&self) -> bool {
        self.identity.principal.tenant.is_none()
            && (self
                .identity
                .principal
                .permissions
                .contains(&Permission::Inspect)
                || self
                    .identity
                    .principal
                    .permissions
                    .contains(&Permission::ManageTenants))
    }
    pub fn require_operator(&self) -> ApiResult<()> {
        if self.operator() {
            Ok(())
        } else {
            Err(ApiError::forbidden())
        }
    }
}

pub(super) type ApiResult<T> = Result<T, ApiError>;
#[derive(Debug)]
pub(super) struct ApiError {
    status: StatusCode,
    pub code: &'static str,
    message: &'static str,
}

#[cfg(test)]
#[path = "api_local_tests.rs"]
mod local_tests;
impl ApiError {
    pub fn invalid() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message: "Request fields or bounds are invalid",
        }
    }
    pub fn forbidden() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "permission_denied",
            message: "This resource is outside the current permissions",
        }
    }
    pub fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: "Endpoint or resource was not found",
        }
    }
    pub fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "upstream_unavailable",
            message: "The authoritative service is unavailable",
        }
    }
    pub fn unsupported() -> Self {
        Self {
            status: StatusCode::NOT_IMPLEMENTED,
            code: "unsupported_operation",
            message: "Cluster administration is unavailable for local execution",
        }
    }
    fn identity(error: tidemq_sdk::Error) -> Self {
        if matches!(
            error.kind(),
            ErrorKind::PermissionDenied | ErrorKind::Unauthenticated
        ) {
            Self {
                status: StatusCode::UNAUTHORIZED,
                code: "authentication_required",
                message: "The current identity or credential is no longer active",
            }
        } else {
            Self::read(error)
        }
    }
    pub fn read(error: tidemq_sdk::Error) -> Self {
        if remote_error_code(&error) == Some(tidemq_protocol::common::ErrorCode::OffsetExpired) {
            return Self {
                status: StatusCode::GONE,
                code: "offset_expired",
                message: "The requested message offset is older than the retained range; refresh the shard boundaries",
            };
        }
        if matches!(
            error.kind(),
            ErrorKind::DeadlineExceeded
                | ErrorKind::OutcomeUnknown
                | ErrorKind::Unavailable
                | ErrorKind::Closed
                | ErrorKind::Cancelled
        ) {
            Self::unavailable()
        } else {
            Self::sdk(error)
        }
    }
    pub fn sdk(error: tidemq_sdk::Error) -> Self {
        let (status, code, message) = match error.kind() {
            ErrorKind::Unauthenticated => (
                StatusCode::UNAUTHORIZED,
                "authentication_required",
                "The core rejected this credential",
            ),
            ErrorKind::PermissionDenied => return Self::forbidden(),
            ErrorKind::InvalidArgument => return Self::invalid(),
            ErrorKind::NotFound => return Self::not_found(),
            ErrorKind::Conflict => (
                StatusCode::CONFLICT,
                "revision_or_content_conflict",
                "The operation content or resource revision conflicts; reload before editing",
            ),
            ErrorKind::RecoveryExpired => (
                StatusCode::GONE,
                "recovery_expired",
                "The retained result or resource window has expired",
            ),
            ErrorKind::ResourceExhausted => (
                StatusCode::TOO_MANY_REQUESTS,
                "resource_exhausted",
                "The bounded request capacity is exhausted",
            ),
            ErrorKind::DeadlineExceeded | ErrorKind::OutcomeUnknown => (
                StatusCode::GATEWAY_TIMEOUT,
                "outcome_unknown",
                "The request did not yield an authoritative result",
            ),
            _ => return Self::unavailable(),
        };
        Self {
            status,
            code,
            message,
        }
    }
}

fn remote_error_code(error: &tidemq_sdk::Error) -> Option<tidemq_protocol::common::ErrorCode> {
    use tidemq_sdk::Error;
    let mut current = error;
    for _ in 0..16 {
        current = match current {
            Error::Remote { code, .. } => return Some(*code),
            Error::Query { source } | Error::RequestUnknown { source } => source.as_ref(),
            Error::Background { source }
            | Error::Administration { source, .. }
            | Error::SubscriptionDeletion { source, .. }
            | Error::SendUnresolved { source, .. }
            | Error::BatchSendInterrupted { source, .. }
            | Error::SendNotSubmitted { source }
            | Error::Acknowledgement { source, .. } => source.as_ref(),
            _ => return None,
        };
    }
    None
}
impl From<SessionError> for ApiError {
    fn from(error: SessionError) -> Self {
        Self {
            status: error.status(),
            code: error.code(),
            message: match error {
                SessionError::Origin => "The request origin is not allowed",
                SessionError::Csrf => "The CSRF token is invalid",
                _ => "The secure session is missing, invalid, or expired",
            },
        }
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(json!({ "code": self.code, "message": self.message })),
        )
            .into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(header::SET_COOKIE, SessionManager::clear_cookie());
        }
        response
    }
}

pub(super) async fn dispatch(State(state): State<Arc<StateData>>, request: Request) -> Response {
    let Some(api) = state.api.as_ref() else {
        return ApiError::unavailable().into_response();
    };
    let Ok(_permit) = api.admission.try_acquire() else {
        return (StatusCode::TOO_MANY_REQUESTS, Json(json!({"code":"resource_exhausted","message":"Console request capacity is exhausted"}))).into_response();
    };
    let (parts, body) = request.into_parts();
    let business_write = !matches!(parts.method, Method::GET | Method::HEAD)
        && !parts.uri.path().ends_with("/messages")
        && !parts.uri.path().ends_with("/session");
    let mut routed = false;
    let result = tokio::time::timeout(Duration::from_secs(12), async {
        let bytes = axum::body::to_bytes(body, 1_500_000)
            .await
            .map_err(|_| ApiError::invalid())?;
        routed = true;
        route(api, parts.method, parts.uri, parts.headers, bytes).await
    })
    .await;
    match result { Ok(Ok(response)) => response, Ok(Err(error)) => error.into_response(), Err(_) if !routed => (StatusCode::REQUEST_TIMEOUT, Json(json!({"code":"request_body_timeout","message":"The request body was not received within its deadline; no business operation was submitted"}))).into_response(), Err(_) if business_write => (StatusCode::GATEWAY_TIMEOUT, Json(json!({"code":"outcome_unknown","message":"Query the original operation before submitting another write"}))).into_response(),Err(_)=>ApiError::unavailable().into_response() }
}

async fn route(
    api: &ApiState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let path = uri
        .path()
        .strip_prefix("/api/v1/")
        .ok_or_else(ApiError::not_found)?;
    if path == "session" && method == Method::POST {
        return login(api, &headers, &body).await;
    }
    let session = api.sessions.authenticate(&headers, now_ms()?)?;
    if !matches!(method, Method::GET | Method::HEAD) {
        api.sessions.check_csrf(&headers, &session)?;
    }
    if path == "session" && method == Method::DELETE {
        return Ok((
            StatusCode::NO_CONTENT,
            [(header::SET_COOKIE, SessionManager::clear_cookie())],
        )
            .into_response());
    }
    let client = api.client(session.token()).await?;
    let result = async {
        let identity = client
            .current_identity()
            .await
            .map_err(ApiError::identity)?;
        let context = Context {
            state: api,
            client: &client,
            identity,
            session,
        };
        if method == Method::GET && path == "me" {
            return Ok(Json(me(
                &context.identity,
                context.session.csrf_token(),
                context.session.expires_at_unix_ms(),
                &api.cluster_name,
                context.client.capabilities(),
            ))
            .into_response());
        }
        if method == Method::GET || method == Method::POST && path.ends_with("/messages") {
            return super::api_query::query(&context, path, &uri, &body)
                .await
                .map(|value| Json(value).into_response());
        }
        super::api_write::write(&context, &method, path, &body)
            .await
            .map(|value| Json(value).into_response())
    }
    .await;
    client.close().await;
    result
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Login {
    token: Zeroizing<String>,
}
async fn login(api: &ApiState, headers: &HeaderMap, body: &[u8]) -> ApiResult<Response> {
    api.sessions.check_origin(headers)?;
    let input: Login = decode(body)?;
    let client = api.client(&input.token).await?;
    let identity = client.current_identity().await.map_err(ApiError::identity);
    client.close().await;
    let identity = identity?;
    let issued = api.sessions.establish(
        &input.token,
        identity.credential_expires_at_unix_ms,
        now_ms()?,
    )?;
    let value = me(
        &identity,
        &issued.csrf_token,
        issued.expires_at_unix_ms,
        &api.cluster_name,
        client.capabilities(),
    );
    Ok(([(header::SET_COOKIE, issued.set_cookie)], Json(value)).into_response())
}

fn me(
    identity: &PrincipalAuthorization,
    csrf: &str,
    expires: u64,
    cluster_name: &str,
    capabilities: Value,
) -> Value {
    let principal = &identity.principal;
    json!({ "principal": {"id":principal.id.to_string(),"name":principal.name}, "cluster":{"id":identity.cluster_id.to_string(),"name":cluster_name}, "capabilities": capabilities,
        "tenant":principal.tenant.map(|id|id.to_string()),"namespace":principal.namespace.map(|id|id.to_string()),
        "operator":principal.tenant.is_none() && (principal.permissions.contains(&Permission::Inspect)||principal.permissions.contains(&Permission::ManageTenants)),
        "permissions":principal.permissions.iter().map(|permission|permission.as_str()).collect::<Vec<_>>(), "csrfToken":csrf,"expiresAtMs":expires.to_string(),
        "credentialId":identity.credential_id.to_string(),"credentialExpiresAtMs":identity.credential_expires_at_unix_ms.to_string(),"revision":identity.revision.to_string(),"resourceGrants":principal.resource_grants.iter().map(super::api_identity::grant_json).collect::<Vec<_>>() })
}

pub(super) fn now_ms() -> ApiResult<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or_else(ApiError::unavailable)
}
pub(super) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> ApiResult<T> {
    serde_json::from_slice(bytes).map_err(|_| ApiError::invalid())
}
pub(super) fn encode<T: Serialize>(value: T) -> ApiResult<Value> {
    serde_json::to_value(value).map_err(|_| ApiError::unavailable())
}
pub(super) fn observation(source: &str) -> ApiResult<Value> {
    Ok(json!({"observedAtMs":now_ms()?.to_string(),"source":source,"partial":false}))
}
pub(super) fn page(
    items: Vec<Value>,
    cursor: Option<String>,
    revision: Option<u64>,
) -> ApiResult<Value> {
    let mut value = observation("tidemq_metadata")?;
    let object = value.as_object_mut().ok_or_else(ApiError::unavailable)?;
    object.insert("items".into(), json!(items));
    object.insert("nextCursor".into(), json!(cursor));
    object.insert(
        "revision".into(),
        json!(revision.map(|value| value.to_string())),
    );
    Ok(value)
}

pub(super) fn parameters(uri: &Uri) -> ApiResult<BTreeParams> {
    let mut result = BTreeParams::new();
    if uri.query().is_some_and(|query| query.len() > 4096) {
        return Err(ApiError::invalid());
    }
    for (key, value) in uri
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
    {
        if !key.bytes().all(|byte| byte.is_ascii_alphanumeric())
            || value.contains(['%', '+'])
            || result.insert(key.to_owned(), value.to_owned()).is_some()
        {
            return Err(ApiError::invalid());
        }
    }
    Ok(result)
}
pub(super) type BTreeParams = std::collections::BTreeMap<String, String>;

#[cfg(test)]
mod tests {
    use super::*;
    fn refusal(code: tidemq_protocol::common::ErrorCode) -> tidemq_sdk::Error {
        tidemq_sdk::Error::Query {
            source: Box::new(tidemq_sdk::Error::Remote {
                code,
                message: "Core read refusal".into(),
                outcome: tidemq_protocol::common::Outcome::Rejected,
                retryable: false,
                expected_sequence: None,
                expected_registration_epoch: None,
                retry_after_ms: None,
                original_outcome_known: false,
            }),
        }
    }
    #[test]
    fn console_message_expiry_is_distinct_from_failed_cold_reads_and_expired_operation_recovery() {
        use tidemq_protocol::common::ErrorCode;
        let expired = ApiError::read(refusal(ErrorCode::OffsetExpired));
        assert_eq!(expired.status, StatusCode::GONE);
        assert_eq!(expired.code, "offset_expired");
        let unavailable = ApiError::read(refusal(ErrorCode::Unavailable));
        assert_eq!(unavailable.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(unavailable.code, "upstream_unavailable");
        let operation = ApiError::read(refusal(ErrorCode::DeduplicationExpired));
        assert_eq!(operation.code, "recovery_expired");
    }

    #[test]
    fn console_expiry_classification_follows_boxed_and_shared_sdk_wrappers() {
        use tidemq_protocol::common::ErrorCode;
        for (code, expected) in [
            (ErrorCode::OffsetExpired, "offset_expired"),
            (ErrorCode::DeduplicationExpired, "recovery_expired"),
            (ErrorCode::Unavailable, "upstream_unavailable"),
        ] {
            for wrapper in 0..3 {
                let query = refusal(code);
                let error = match wrapper {
                    0 => query,
                    1 => tidemq_sdk::Error::RequestUnknown {
                        source: Box::new(query),
                    },
                    _ => tidemq_sdk::Error::Background {
                        source: Arc::new(tidemq_sdk::Error::RequestUnknown {
                            source: Box::new(query),
                        }),
                    },
                };
                assert_eq!(ApiError::read(error).code, expected);
            }
        }
    }

    #[test]
    fn console_remote_error_inspection_has_a_fixed_wrapper_budget() {
        use tidemq_protocol::common::ErrorCode;
        let mut error = refusal(ErrorCode::OffsetExpired);
        for _ in 0..14 {
            error = tidemq_sdk::Error::Query {
                source: Box::new(error),
            };
        }
        assert_eq!(remote_error_code(&error), Some(ErrorCode::OffsetExpired));
        let too_deep = tidemq_sdk::Error::Query {
            source: Box::new(error),
        };
        assert_eq!(remote_error_code(&too_deep), None);
    }
}
