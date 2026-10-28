//! One fixed process subscriber covers the actual HTTP, SDK, core, and OTLP boundaries.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry_proto::tonic::trace::v1::Span;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use reqwest::{Response, StatusCode, header};
use tidemq::config::{Config, Role, TlsConfig};
use tidemq::console::{ConsoleConfig, RunningConsole};
use tidemq::observability::tracing::{Tracing, TracingConfig};
use tidemq::runtime::{GroupConfig, PeerConfig, RunningNode, RuntimeConfig};
use tidemq_protocol::Id;
use tidemq_sdk::Client;
use tokio::time::Instant;
use tracing_subscriber::layer::SubscriberExt;

// Shared helpers also define optional fault and object-store cases for other integration targets.
#[allow(dead_code)]
#[path = "trace_support/mod.rs"]
mod collector;
#[allow(dead_code)]
#[path = "runtime_support/fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "lifecycle_support/rustfs.rs"]
mod rustfs;
#[path = "s3_support/mod.rs"]
mod s3;
use fixture::*;

const TOKEN: &str = "console-trace-private-bootstrap-token-with-at-least-32-characters";
const ORIGIN: &str = "https://localhost:8443";
const PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

async fn start_console(fixture: &Fixture) -> RunningConsole {
    let directory = fixture.directory.join("console-trace");
    tokio::fs::create_dir_all(directory.join("dist/.vite"))
        .await
        .unwrap();
    tokio::fs::create_dir(directory.join("dist/assets"))
        .await
        .unwrap();
    for (path, body) in [
        ("dist/index.html", "<html>Console trace test</html>"),
        ("dist/assets/trace.js", "export const ready = true;"),
        (
            "dist/.vite/manifest.json",
            r#"{"index.html":{"file":"assets/trace.js","isEntry":true}}"#,
        ),
    ] {
        tokio::fs::write(directory.join(path), body).await.unwrap();
    }
    let key = directory.join("session.key");
    tokio::fs::write(&key, [39_u8; 32]).await.unwrap();
    private_file(key.clone()).await;
    let node = fixture.configs.get(&1).unwrap();
    let text = format!(
        r#"
role = "console"
listen = "127.0.0.1:8443"
management_listen = "127.0.0.1:0"
static_dir = {}
[cluster]
address = "{}"
server_name = "node-1.local"
trust_roots = {}
[session]
key_file = {}
public_origin = "{ORIGIN}"
"#,
        serde_json::to_string(&directory.join("dist")).unwrap(),
        node.listen,
        serde_json::to_string(&node.tls.trust_roots).unwrap(),
        serde_json::to_string(&key).unwrap(),
    );
    let text = format!(
        "{text}\n[tls]\ncertificate = {}\nprivate_key = {}\n",
        serde_json::to_string(&node.tls.certificate).unwrap(),
        serde_json::to_string(&node.tls.private_key).unwrap()
    );
    let mut config: ConsoleConfig = toml::from_str(&text).unwrap();
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.listen = reservation.local_addr().unwrap();
    drop(reservation);
    RunningConsole::start(config).await.unwrap()
}

fn correlation(response: &Response, input: &str, ids: &mut BTreeSet<String>) -> String {
    let id = response
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap();
    id.parse::<Id>().unwrap();
    assert_ne!(id, "untrusted-request-id");
    assert!(ids.insert(id.to_owned()));
    let traced = response
        .headers()
        .get("traceparent")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(tidemq_protocol::framing::valid_traceparent(traced));
    assert_ne!(traced, input);
    if input == PARENT {
        assert_eq!(traced.get(3..35), PARENT.get(3..35));
    } else {
        assert_ne!(traced.get(3..35), PARENT.get(3..35));
    }
    traced.to_owned()
}

fn has_sdk_core_chain(spans: &[Span], parent: &str) -> bool {
    let trace = TraceId::from_hex(parent.get(3..35).unwrap())
        .unwrap()
        .to_bytes();
    let http = SpanId::from_hex(parent.get(36..52).unwrap())
        .unwrap()
        .to_bytes();
    spans.iter().any(|span| {
        span.name == "console_http_request" && span.trace_id == trace && span.span_id == http
    }) && spans.iter().any(|sdk| {
        sdk.name == "tidemq.sdk.request"
            && sdk.trace_id == trace
            && sdk.parent_span_id == http
            && spans.iter().any(|core| {
                core.name == "tidemq.server.request"
                    && core.trace_id == trace
                    && core.parent_span_id == sdk.span_id
            })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_http_correlation_and_sdk_core_trace_preserve_parents_without_exporting_secrets() {
    let collector = collector::Collector::new().await;
    let mut pipeline = Tracing::start(&TracingConfig {
        enabled: true,
        endpoint: collector.endpoint(),
        sample_per_mille: 1000,
        spans_per_second: 1024,
        queue_spans: 128,
        batch_spans: 16,
        export_interval_ms: 100,
        export_timeout_ms: 100,
    })
    .unwrap();
    // This integration target owns its process, matching the executable's subscriber lifetime.
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(pipeline.layer()))
        .unwrap();
    let mut fixture = Fixture::new(
        &[
            (1, Role::Gateway),
            (2, Role::Metadata),
            (3, Role::Broker),
            (4, Role::Storage),
        ],
        &[2],
        &[4],
    )
    .await;
    fixture.start_all().await;
    let console = start_console(&fixture).await;
    let base = format!("https://127.0.0.1:{}", console.address.port());
    let roots = tokio::fs::read(&fixture.configs.get(&1).unwrap().tls.trust_roots)
        .await
        .unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .tls_certs_only(reqwest::Certificate::from_pem_bundle(&roots).unwrap())
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let mut ids = BTreeSet::new();
    let mut http_spans = Vec::new();
    for input in [PARENT, "invalid-parent"] {
        let response = client
            .get(format!("{base}/login?token=trace-secret-query"))
            .header("traceparent", input)
            .header("x-request-id", "untrusted-request-id")
            .header("baggage", "private=trace-secret-baggage")
            .header("tracestate", "private=trace-secret-state")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        http_spans.push(correlation(&response, input, &mut ids));
        response.bytes().await.unwrap();
    }
    let response = client
        .post(format!("{base}/api/v1/session"))
        .header(header::ORIGIN, ORIGIN)
        .header(header::CONTENT_TYPE, "application/json")
        .header("traceparent", PARENT)
        .body(serde_json::to_vec(&serde_json::json!({"token":TOKEN})).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let login_trace = correlation(&response, PARENT, &mut ids);
    let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert!(body.get("credentialId").is_some());
    assert!(!body.to_string().contains(TOKEN));
    collector::wait(|| has_sdk_core_chain(&collector.spans(), &login_trace)).await;
    console
        .shutdown_until(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    fixture.close().await;
    pipeline
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_exported_requests(&collector, &http_spans, &login_trace);
}

fn assert_exported_requests(collector: &collector::Collector, pages: &[String], login: &str) {
    let spans = collector.spans();
    assert!(has_sdk_core_chain(&spans, login));
    let parent = SpanId::from_hex(PARENT.get(36..52).unwrap())
        .unwrap()
        .to_bytes();
    for (index, trace) in pages.iter().enumerate() {
        let id = SpanId::from_hex(trace.get(36..52).unwrap())
            .unwrap()
            .to_bytes();
        let span = spans.iter().find(|span| span.span_id == id).unwrap();
        assert_eq!(span.name, "console_http_request");
        if index == 0 {
            assert_eq!(span.parent_span_id, parent);
        } else {
            assert!(span.parent_span_id.is_empty());
        }
    }
    assert!(spans.iter().all(|span| span.attributes.len() <= 8
        && span.events.is_empty()
        && span.links.is_empty()
        && span.trace_state.is_empty()));
    for request in collector.requests.lock().unwrap().iter() {
        assert!(
            !request
                .bytes
                .windows(TOKEN.len())
                .any(|bytes| bytes == TOKEN.as_bytes())
        );
        assert!(
            !request
                .bytes
                .windows(12)
                .any(|bytes| bytes == b"trace-secret")
        );
        assert!(
            !request
                .bytes
                .windows(20)
                .any(|bytes| bytes == b"untrusted-request-id")
        );
    }
}
