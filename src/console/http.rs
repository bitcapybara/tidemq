use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use serde::Serialize;
use tower::ServiceExt;
use tower_http::services::ServeFile;
use tracing::Instrument;

use super::files::StaticFiles;

pub(super) struct StateData {
    pub(super) files: StaticFiles,
    pub(super) ready: AtomicBool,
    requests: AtomicU64,
    pub(super) api: Option<Arc<super::api::ApiState>>,
}

impl StateData {
    #[cfg(test)]
    pub(super) fn new(files: StaticFiles) -> Arc<Self> {
        Arc::new(Self {
            files,
            ready: AtomicBool::new(true),
            requests: AtomicU64::new(0),
            api: None,
        })
    }

    pub(super) fn with_api(files: StaticFiles, api: Arc<super::api::ApiState>) -> Arc<Self> {
        Arc::new(Self {
            files,
            ready: AtomicBool::new(true),
            requests: AtomicU64::new(0),
            api: Some(api),
        })
    }
}

pub(super) fn public_router(state: Arc<StateData>) -> Router {
    Router::new()
        .route("/api/v1", any(super::api::dispatch))
        .route("/api/v1/{*path}", any(super::api::dispatch))
        .layer(axum::extract::DefaultBodyLimit::max(1_500_000))
        .fallback(static_response)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            secure_response,
        ))
        .with_state(state)
}

pub(super) fn management_router(state: Arc<StateData>) -> Router {
    Router::new()
        .route(
            "/health/live",
            get(|| async { Json(serde_json::json!({"live": true})) }),
        )
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .fallback(|| async { api_error(StatusCode::NOT_FOUND, "not_found", "Endpoint not found") })
        .layer(middleware::from_fn_with_state(
            state.clone(),
            secure_response,
        ))
        .with_state(state)
}

#[derive(Serialize)]
struct ApiError {
    code: &'static str,
    message: &'static str,
}

fn api_error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (status, Json(ApiError { code, message })).into_response()
}

async fn static_response(State(state): State<Arc<StateData>>, request: Request) -> Response {
    if request.method() != Method::GET && request.method() != Method::HEAD {
        return api_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "Only GET and HEAD are allowed for page assets",
        );
    }
    let name = request.uri().path().trim_start_matches('/');
    let name = if state.files.contains(name) {
        name
    } else if is_page_route(name) {
        "index.html"
    } else {
        return api_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "Page or asset not found",
        );
    };
    let path = match state.files.resolve(name).await {
        Ok(path) => path,
        Err(_) => {
            return api_error(
                StatusCode::NOT_FOUND,
                "asset_unavailable",
                "Page asset is unavailable; refresh after deployment completes",
            );
        }
    };
    match ServeFile::new(path).oneshot(request).await {
        Ok(response)
            if response.status().is_success() || response.status() == StatusCode::NOT_MODIFIED =>
        {
            response.map(Body::new)
        }
        Ok(response) if response.status() == StatusCode::RANGE_NOT_SATISFIABLE => api_error(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "invalid_range",
            "Requested asset byte range is unavailable",
        ),
        Ok(_) | Err(_) => api_error(
            StatusCode::NOT_FOUND,
            "asset_unavailable",
            "Page asset is unavailable; refresh after deployment completes",
        ),
    }
}

fn is_page_route(name: &str) -> bool {
    if name.is_empty() {
        return true;
    }
    if name.contains(['.', '\\', '%']) {
        return false;
    }
    let mut segments = name.split('/');
    matches!(
        segments.next(),
        Some("login" | "topics" | "consumers" | "identities" | "cluster" | "operations")
    ) && segments.all(|part| {
        !part.is_empty()
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    })
}

async fn secure_response(
    State(state): State<Arc<StateData>>,
    request: Request,
    next: Next,
) -> Response {
    state.requests.fetch_add(1, Ordering::Relaxed);
    let request_id = tidemq_protocol::Id::new_v7().ok().map(|id| id.to_string());
    // Do not record URI, query, headers, or body: they may contain user secrets.
    let span = tracing::info_span!(target: "tidemq_trace", "console_http_request", request_id, method = %request.method(), status = tracing::field::Empty);
    let parent = request
        .headers()
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    crate::observability::tracing::set_parent(&span, parent, true);
    let (mut response, traceparent) = async {
        let traceparent = crate::observability::tracing::current_traceparent();
        (next.run(request).await, traceparent)
    }
    .instrument(span.clone())
    .await;
    span.record("status", response.status().as_u16());
    let headers = response.headers_mut();
    if let Some(value) = request_id.and_then(|value| HeaderValue::from_str(&value).ok()) {
        headers.insert("x-request-id", value);
    }
    if !traceparent.is_empty()
        && let Ok(value) = HeaderValue::from_str(&traceparent)
    {
        headers.insert("traceparent", value);
    }
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static("default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"));
    response
}

async fn ready(State(state): State<Arc<StateData>>) -> Response {
    let ready = state.ready.load(Ordering::Acquire);
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({"ready": ready, "scope": "console_local"})),
    )
        .into_response()
}

async fn metrics(State(state): State<Arc<StateData>>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        format!(
            "# HELP tidemq_console_ready Whether the local Console is ready.\n# TYPE tidemq_console_ready gauge\ntidemq_console_ready {}\n# HELP tidemq_console_http_requests_total Requests served by this Console process.\n# TYPE tidemq_console_http_requests_total counter\ntidemq_console_http_requests_total {}\n",
            u8::from(state.ready.load(Ordering::Acquire)),
            state.requests.load(Ordering::Relaxed),
        ),
    )
}
