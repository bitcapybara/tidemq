use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Extension, Json, Router};
use serde::Serialize;
use tower_http::services::ServeFile;
use tracing::Instrument;

use super::assets::{AssetHealth, Failure};
use super::files::StaticFiles;
use super::lifecycle::Metrics;
use crate::runtime::process::{Phase, Process};

pub(super) struct StateData {
    pub(super) files: StaticFiles,
    pub(super) process: Process,
    pub(super) assets: AssetHealth,
    pub(super) metrics: Arc<Metrics>,
    pub(super) api: Option<Arc<super::api::ApiState>>,
}

impl StateData {
    #[cfg(test)]
    pub(super) fn new(files: StaticFiles) -> Arc<Self> {
        let process = Process::default();
        process.recovering();
        process.ready();
        Self::build(files, None, process, Arc::default()).0
    }

    pub(super) fn build(
        files: StaticFiles,
        api: Option<Arc<super::api::ApiState>>,
        process: Process,
        metrics: Arc<Metrics>,
    ) -> (Arc<Self>, tokio::sync::mpsc::Receiver<Failure>) {
        let (assets, receive) = AssetHealth::new(files.clone(), process.clone(), metrics.clone());
        (
            Arc::new(Self {
                files,
                process,
                assets,
                metrics,
                api,
            }),
            receive,
        )
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

pub(super) fn management_router(
    state: Arc<StateData>,
    registry: Arc<prometheus_client::registry::Registry>,
) -> Router {
    Router::new()
        .route(
            "/health/live",
            get(|| async { Json(serde_json::json!({"live": true})) }),
        )
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .layer(Extension(registry))
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
    let name = name.to_owned();
    if let Some(asset) = state.files.required(&name)
        && !valid_range(request.headers().get(header::RANGE), asset.length)
    {
        return invalid_range();
    }
    let path = match state.files.resolve(&name).await {
        Ok(path) => path,
        Err(_) => return asset_unavailable(&state, &name, "resolve"),
    };
    serve_asset(&state, &name, path, request).await
}

async fn serve_asset(
    state: &StateData,
    name: &str,
    path: std::path::PathBuf,
    request: Request,
) -> Response {
    let head = request.method() == Method::HEAD;
    match ServeFile::new(path).try_call(request).await {
        Ok(response)
            if response.status().is_success() || response.status() == StatusCode::NOT_MODIFIED =>
        {
            let remaining = if head {
                Some(0)
            } else {
                response
                    .headers()
                    .get(header::CONTENT_LENGTH)
                    .and_then(|length| length.to_str().ok())
                    .and_then(|length| length.parse().ok())
            };
            response.map(|body| state.assets.observe(Body::new(body), name, remaining))
        }
        Ok(response) if response.status() == StatusCode::RANGE_NOT_SATISFIABLE => invalid_range(),
        Ok(response)
            if response.status().is_client_error()
                && response.status() != StatusCode::NOT_FOUND =>
        {
            response.map(Body::new)
        }
        Ok(_) | Err(_) => asset_unavailable(state, name, "open_read"),
    }
}

fn asset_unavailable(state: &StateData, name: &str, class: &'static str) -> Response {
    state.assets.report(name, class);
    api_error(
        StatusCode::NOT_FOUND,
        "asset_unavailable",
        "Page asset is unavailable; refresh after deployment completes",
    )
}

fn invalid_range() -> Response {
    api_error(
        StatusCode::RANGE_NOT_SATISFIABLE,
        "invalid_range",
        "Requested asset byte range is unavailable",
    )
}

fn valid_range(value: Option<&HeaderValue>, length: u64) -> bool {
    let Some(value) = value else {
        return true;
    };
    let Some(value) = value
        .to_str()
        .ok()
        .filter(|value| value.len() <= 512)
        .and_then(|value| value.strip_prefix("bytes="))
    else {
        return false;
    };
    let Some((start, end)) = value.split_once('-') else {
        return false;
    };
    if start.is_empty() {
        return end.parse::<u64>().is_ok_and(|suffix| suffix > 0);
    }
    start.parse::<u64>().is_ok_and(|start| {
        start < length && (end.is_empty() || end.parse::<u64>().is_ok_and(|end| end >= start))
    })
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
    state.metrics.requests.inc();
    let reject_write = request.uri().path().starts_with("/api/v1")
        && !matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::OPTIONS
        )
        && state.process.phase() != Phase::Ready;
    let request_id = tidemq_protocol::Id::new_v7().ok().map(|id| id.to_string());
    let method = match request.method().as_str() {
        "GET" | "HEAD" | "POST" | "PUT" | "DELETE" | "PATCH" | "OPTIONS" | "CONNECT" | "TRACE" => {
            request.method().as_str()
        }
        _ => "OTHER",
    };
    // Do not record URI, query, headers, or body: they may contain user secrets.
    let span = tracing::info_span!(target: "tidemq_trace", "console_http_request", request_id, method, status = tracing::field::Empty);
    let parent = request
        .headers()
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    crate::observability::tracing::set_parent(&span, parent, true);
    let (mut response, traceparent) = async {
        let traceparent = crate::observability::tracing::current_traceparent();
        let response = if reject_write {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "process_not_ready",
                "The process is not ready; no business operation was submitted",
            )
        } else {
            next.run(request).await
        };
        (response, traceparent)
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
    let ready = state.process.phase() == Phase::Ready;
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

async fn metrics(
    State(state): State<Arc<StateData>>,
    Extension(registry): Extension<Arc<prometheus_client::registry::Registry>>,
) -> impl IntoResponse {
    let mut body = String::new();
    let _ = prometheus_client::encoding::text::encode(&mut body, &registry);
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        format!(
            "# HELP tidemq_console_ready Whether the local Console is ready.\n# TYPE tidemq_console_ready gauge\ntidemq_console_ready {}\n{body}",
            u8::from(state.process.phase() == Phase::Ready)
        ),
    )
}
