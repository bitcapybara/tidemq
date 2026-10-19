use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use opentelemetry_http::{HttpClient, HttpError, Request, Response};

use super::{MAX_REQUEST_BYTES, TraceHealth};
use crate::Result;

#[derive(Debug)]
struct NoDns;
impl reqwest::dns::Resolve for NoDns {
    fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async { Err(failed()) })
    }
}

const MAX_RESPONSE_BYTES: usize = 4_096;

#[derive(Debug)]
pub(super) struct Client {
    inner: reqwest::Client,
    endpoint: reqwest::Url,
    health: TraceHealth,
}

impl Client {
    pub fn new(endpoint: reqwest::Url, timeout: Duration, health: TraceHealth) -> Result<Self> {
        let roots = webpki_root_certs::TLS_SERVER_ROOT_CERTS
            .iter()
            .map(|certificate| reqwest::Certificate::from_der(certificate.as_ref()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| super::invalid("Failed to load the compiled OTLP trust anchors"))?;
        let inner = reqwest::Client::builder()
            .no_proxy()
            // No uncancellable getaddrinfo or platform trust network fetch enters the owned worker.
            .dns_resolver(std::sync::Arc::new(NoDns))
            .tls_certs_only(roots)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .http1_only()
            .http1_max_headers(16)
            .pool_max_idle_per_host(0)
            .connect_timeout(timeout)
            .read_timeout(timeout)
            .timeout(timeout)
            .build()
            .map_err(|_| super::invalid("Failed to initialize bounded OTLP HTTP client"))?;
        Ok(Self {
            inner,
            endpoint,
            health,
        })
    }

    fn error(&self, error: reqwest::Error) -> HttpError {
        if error.is_timeout() {
            self.health
                .0
                .export_timeouts
                .fetch_add(1, Ordering::Relaxed);
        }
        failed()
    }

    async fn response_bytes(
        &self,
        mut response: reqwest::Response,
    ) -> std::result::Result<Response<Bytes>, HttpError> {
        let status = response.status();
        let mut bytes = BytesMut::new();
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
        {
            self.health
                .0
                .response_rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err(failed());
        }
        while let Some(chunk) = response.chunk().await.map_err(|error| self.error(error))? {
            if bytes
                .len()
                .checked_add(chunk.len())
                .is_none_or(|length| length > MAX_RESPONSE_BYTES)
            {
                self.health
                    .0
                    .response_rejected
                    .fetch_add(1, Ordering::Relaxed);
                return Err(failed());
            }
            bytes.extend_from_slice(&chunk);
        }
        Response::builder()
            .status(status)
            .body(bytes.freeze())
            .map_err(|_| failed())
    }
}

fn failed() -> HttpError {
    Box::new(std::io::Error::other("OTLP HTTP request failed"))
}

#[async_trait::async_trait]
impl HttpClient for Client {
    async fn send_bytes(
        &self,
        request: Request<Bytes>,
    ) -> std::result::Result<Response<Bytes>, HttpError> {
        if request.uri() != self.endpoint.as_str() || request.body().len() > MAX_REQUEST_BYTES {
            return Err(failed());
        }
        let body = request.into_body();
        self.health
            .0
            .export_bytes
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        // Construct headers here: upstream OTEL_* environment headers are never forwarded.
        let response = self
            .inner
            .post(self.endpoint.clone())
            .header("content-type", "application/x-protobuf")
            .body(body)
            .send()
            .await
            .map_err(|error| self.error(error))?;
        self.response_bytes(response).await
    }
}
