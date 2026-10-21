//! Optional, bounded trace export owned by the service entry point.

mod context;
mod health;
mod http;
mod processor;

use std::sync::Arc;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanLimits};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;

use crate::{Error, Result};

pub use context::{current_traceparent, request_span};
pub(crate) use context::{observe_until, set_parent};
pub use health::{TraceHealth, TraceSnapshot};

pub(crate) const SPAN_CHARGE: u32 = 4_096;
const MAX_REQUEST_BYTES: usize = 65_536;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TracingConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub sample_per_mille: u16,
    pub spans_per_second: u16,
    pub queue_spans: u16,
    pub batch_spans: u16,
    pub export_interval_ms: u16,
    pub export_timeout_ms: u16,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://127.0.0.1:4318/v1/traces".into(),
            sample_per_mille: 10,
            spans_per_second: 128,
            queue_spans: 256,
            batch_spans: 32,
            export_interval_ms: 500,
            export_timeout_ms: 500,
        }
    }
}

impl TracingConfig {
    pub fn validate(&self) -> Result<()> {
        self.url()?;
        for (valid, reason) in [
            (
                self.sample_per_mille <= 1_000,
                "tracing.sample_per_mille must not exceed 1000",
            ),
            (
                (1..=1_024).contains(&self.spans_per_second),
                "tracing.spans_per_second must be between 1 and 1024",
            ),
            (
                (1..=1_024).contains(&self.queue_spans),
                "tracing.queue_spans must be between 1 and 1024",
            ),
            (
                (1..=32).contains(&self.batch_spans),
                "tracing.batch_spans must be between 1 and 32",
            ),
            (
                self.batch_spans <= self.queue_spans,
                "tracing.batch_spans must not exceed tracing.queue_spans",
            ),
            (
                (100..=5_000).contains(&self.export_interval_ms),
                "tracing.export_interval_ms must be between 100 and 5000",
            ),
            (
                (50..=1_000).contains(&self.export_timeout_ms),
                "tracing.export_timeout_ms must be between 50 and 1000",
            ),
        ] {
            if !valid {
                return Err(invalid(reason));
            }
        }
        Ok(())
    }

    fn url(&self) -> Result<reqwest::Url> {
        if self.endpoint.len() > 512 {
            return Err(invalid("tracing.endpoint must not exceed 512 bytes"));
        }
        let url = reqwest::Url::parse(&self.endpoint)
            .map_err(|_| invalid("tracing.endpoint must be an absolute HTTP or HTTPS URL"))?;
        let address = url.host_str().and_then(|host| {
            host.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .ok()
        });
        let loopback = address.is_some_and(|address| address.is_loopback());
        for (valid, reason) in [
            (
                url.username().is_empty() && url.password().is_none(),
                "tracing.endpoint must not contain user credentials",
            ),
            (
                url.query().is_none(),
                "tracing.endpoint must not contain query parameters",
            ),
            (
                url.fragment().is_none(),
                "tracing.endpoint must not contain a fragment",
            ),
            (
                url.host_str().is_some(),
                "tracing.endpoint must contain a host",
            ),
            (
                address.is_some(),
                "tracing.endpoint host must be a literal IP address",
            ),
            (
                url.path() == "/v1/traces",
                "tracing.endpoint path must be /v1/traces",
            ),
            (
                url.scheme() == "https" || (url.scheme() == "http" && loopback),
                "tracing.endpoint must use HTTPS; HTTP is allowed only for loopback IPs",
            ),
        ] {
            if !valid {
                return Err(invalid(reason));
            }
        }
        Ok(url)
    }
}

pub struct Tracing {
    provider: SdkTracerProvider,
    task: Option<JoinHandle<()>>,
    control: Arc<processor::Control>,
    pub health: TraceHealth,
    pub instance_id: String,
}

impl Tracing {
    pub fn start(config: &TracingConfig) -> Result<Self> {
        config.validate()?;
        if !config.enabled {
            return Err(invalid("Trace export is disabled"));
        }
        reject_environment()?;
        let health = TraceHealth::default();
        let mut exporter = exporter(config, health.clone())?;
        let instance_id = tidemq_protocol::Id::new_v7()
            .map_err(|source| Error::Protocol { source })?
            .to_string();
        let resource = Resource::builder_empty()
            .with_attributes([
                KeyValue::new("service.name", "tidemq"),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                KeyValue::new("service.instance.id", instance_id.clone()),
            ])
            .build();
        opentelemetry_sdk::trace::SpanExporter::set_resource(&mut exporter, &resource);
        let (processor, control, task) = processor::start(config, health.clone(), exporter)?;
        let provider = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_sampler(processor::Sampler::new(config, health.clone()))
            .with_span_limits(SpanLimits {
                max_attributes_per_span: 8,
                max_events_per_span: 0,
                max_links_per_span: 0,
                max_attributes_per_event: 0,
                max_attributes_per_link: 0,
            })
            .with_span_processor(processor)
            .build();
        Ok(Self {
            provider,
            task: Some(task),
            control,
            health,
            instance_id,
        })
    }

    /// The host chooses the subscriber; this method never installs a global provider.
    pub fn layer<S>(&self) -> impl Layer<S> + use<S>
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    {
        tracing_opentelemetry::layer()
            .with_tracer(self.provider.tracer("tidemq"))
            .with_location(false)
            .with_target(false)
            .with_threads(false)
            .with_tracked_inactivity(false)
            .with_error_fields_to_exceptions(false)
            .with_error_records_to_exceptions(false)
            .with_error_events_to_exceptions(false)
            .with_error_events_to_status(false)
            .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                metadata.is_span()
                    && metadata.target() == "tidemq_trace"
                    && processor::allowed_name(metadata.name())
                    && metadata
                        .fields()
                        .iter()
                        .all(|field| processor::allowed_field(field.name()))
            }))
    }

    /// Drain only within the caller's existing total service shutdown deadline.
    pub async fn shutdown(&mut self, deadline: Instant) -> Result<()> {
        self.control.close();
        let _ = self.provider.shutdown_with_timeout(Duration::ZERO);
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(Error::TraceExport {
                reason: "worker failed during shutdown",
            }),
            Err(_) => {
                task.abort();
                let _ = task.await;
                self.health
                    .0
                    .shutdown_timeouts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err(Error::TraceExport {
                    reason: "worker exceeded the total shutdown deadline",
                })
            }
        }
    }
}

impl Drop for Tracing {
    fn drop(&mut self) {
        self.control.close();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn invalid(reason: &'static str) -> Error {
    Error::Configuration { reason }
}

fn exporter(
    config: &TracingConfig,
    health: TraceHealth,
) -> Result<opentelemetry_otlp::SpanExporter> {
    let timeout = Duration::from_millis(u64::from(config.export_timeout_ms));
    let client = http::Client::new(config.url()?, timeout, health)?;
    opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
        .with_endpoint(config.endpoint.clone())
        .with_timeout(timeout)
        .with_http_client(client)
        .with_retry_policy(opentelemetry_otlp::RetryPolicy::disabled())
        .with_max_request_body_size(MAX_REQUEST_BYTES)
        .build()
        .map_err(|source| Error::TraceExporter {
            source: Box::new(source),
        })
}

fn reject_environment() -> Result<()> {
    for variable in [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_COMPRESSION",
        "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
        "OTEL_EXPORTER_OTLP_TIMEOUT",
        "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
    ] {
        if std::env::var_os(variable).is_some() {
            return Err(invalid(
                "Configure OTLP only through TideMQ configuration; exporter environment overrides are unsupported",
            ));
        }
    }
    Ok(())
}
