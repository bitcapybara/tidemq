use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::{ConsoleConfig, RunningConsole, files, http};
use crate::config::{Config, ProcessConfig, Role};

const CONFIG: &str = r#"
role = "console"
listen = "127.0.0.1:8443"
management_listen = "127.0.0.1:0"
static_dir = "dist"
[tls]
certificate = "ca.pem"
private_key = "server.key"
[cluster]
address = "gateway.invalid:7443"
server_name = "gateway.tidemq.local"
trust_roots = "ca.pem"
[session]
key_file = "session.key"
public_origin = "https://console.example.test"
"#;

#[test]
fn process_configuration_validates_combined_for_startup_and_diagnosis() {
    let config = ProcessConfig::Node(Box::default());
    config.validate_for_diagnosis().unwrap();
    config.validate_for_startup().unwrap();
    let mut node = Config::default();
    node.console.as_mut().unwrap().static_dir = PathBuf::new();
    let config = ProcessConfig::Node(Box::new(node));
    assert!(config.validate_for_diagnosis().is_err());
    assert!(config.validate_for_startup().is_err());
}

pub(super) struct Fixture {
    pub(super) root: PathBuf,
    pub(super) config: ConsoleConfig,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "tidemq-console-{}",
            tidemq_protocol::Id::new_v7().unwrap()
        ));
        let directory = root.join("dist");
        tokio::fs::create_dir_all(directory.join(".vite"))
            .await
            .unwrap();
        tokio::fs::create_dir(directory.join("assets"))
            .await
            .unwrap();
        tokio::fs::write(directory.join("index.html"), "<html><script type=\"module\" src=\"/assets/app-test.js\"></script><link rel=\"stylesheet\" href=\"/assets/app-test.css\"></html>")
            .await
            .unwrap();
        tokio::fs::write(
            directory.join("assets/app-test.js"),
            "export const ready = true;",
        )
        .await
        .unwrap();
        tokio::fs::write(
            directory.join("assets/app-test.css"),
            "body { color: black; }",
        )
        .await
        .unwrap();
        tokio::fs::write(
            directory.join(".vite/manifest.json"),
            r#"{"index.html":{"file":"assets/app-test.js","isEntry":true,"css":["assets/app-test.css"]}}"#,
        )
        .await
        .unwrap();
        let key = root.join("session.key");
        tokio::fs::write(&key, [42_u8; 32]).await.unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600))
                .await
                .unwrap();
        }
        let roots = root.join("ca.pem");
        let certificate = rcgen::generate_simple_self_signed(vec![
            "gateway.tidemq.local".into(),
            "console.example.test".into(),
            "localhost".into(),
            "127.0.0.1".into(),
        ])
        .unwrap();
        tokio::fs::write(&roots, certificate.cert.pem())
            .await
            .unwrap();
        let server_key = root.join("server.key");
        tokio::fs::write(&server_key, certificate.signing_key.serialize_pem())
            .await
            .unwrap();
        let mut config: ConsoleConfig = toml::from_str(CONFIG).unwrap();
        config.static_dir = directory;
        config.cluster.trust_roots = roots;
        config.tls.certificate = config.cluster.trust_roots.clone();
        config.tls.private_key = server_key;
        config.session.key_file = key;
        // Keep both addresses leased across startup, shutdown, and rebind assertions.
        config.listen = crate::runtime::test_tcp_address();
        config.management_listen = crate::runtime::test_tcp_address();
        Self { root, config }
    }

    pub(super) async fn client(&self) -> reqwest::Client {
        let certificate = tokio::fs::read(&self.config.tls.certificate).await.unwrap();
        reqwest::Client::builder()
            .no_proxy()
            .tls_certs_only(vec![reqwest::Certificate::from_pem(&certificate).unwrap()])
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap()
    }

    pub(super) async fn connect(
        &self,
        address: SocketAddr,
    ) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
        use tokio_rustls::rustls::{
            self,
            pki_types::{CertificateDer, ServerName, pem::PemObject},
        };
        let mut roots = rustls::RootCertStore::empty();
        let certificates = tokio::fs::read(&self.config.tls.certificate).await.unwrap();
        roots
            .add(CertificateDer::from_pem_slice(&certificates).unwrap())
            .unwrap();
        let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
            .connect(
                ServerName::try_from("localhost").unwrap(),
                tokio::net::TcpStream::connect(address).await.unwrap(),
            )
            .await
            .unwrap()
    }

    pub(super) async fn clean(self) {
        tokio::fs::remove_dir_all(self.root).await.unwrap();
    }
}

#[test]
fn console_configuration_is_separate_from_mq_roles_and_strict() {
    let process: ProcessConfig = toml::from_str(CONFIG).unwrap();
    process.validate().unwrap();
    assert!(matches!(process, ProcessConfig::Console(_)));
    assert!(toml::from_str::<Config>(CONFIG).is_err());
    assert!(serde_json::from_str::<Role>("\"console\"").is_err());
    for extra in [
        "node_id = 1\n",
        "data_dir = 'node-data'\n",
        "replication_factor = 3\n",
        "unknown = true\n",
        "[runtime]\nbootstrap = true\n",
        "[gateway]\nlisten = '127.0.0.1:7443'\n",
        "[storage]\n",
        "[authorization]\n",
    ] {
        let text = if extra.starts_with('[') {
            format!("{CONFIG}\n{extra}")
        } else {
            format!("{extra}{CONFIG}")
        };
        assert!(
            toml::from_str::<ProcessConfig>(&text).is_err(),
            "Accepted inactive configuration: {extra}"
        );
    }
    for text in [
        CONFIG.replace("gateway.invalid:7443", "https://gateway.invalid:7443"),
        CONFIG.replace("gateway.invalid:7443", "user@gateway.invalid:7443"),
        CONFIG.replace("gateway.invalid:7443", "gateway.invalid:0"),
        CONFIG.replace("gateway.invalid:7443", "gateway.invalid:7443/path"),
        CONFIG.replace(
            "https://console.example.test",
            "http://console.example.test",
        ),
        CONFIG.replace(
            "https://console.example.test",
            "https://console.example.test/path",
        ),
        CONFIG.replace(
            "management_listen = \"127.0.0.1:0\"",
            "management_listen = \"0.0.0.0:9420\"",
        ),
    ] {
        let config: ProcessConfig = toml::from_str(&text).unwrap();
        assert!(
            config.validate().is_err(),
            "Accepted invalid Console configuration"
        );
    }
    for address in [
        "gateway.tidemq.svc.cluster.local:7443",
        "gateway:7443",
        "127.0.0.1:7443",
        "[::1]:7443",
    ] {
        let config: ProcessConfig =
            toml::from_str(&CONFIG.replace("gateway.invalid:7443", address)).unwrap();
        config.validate().unwrap();
    }
}

#[tokio::test]
async fn console_serves_spa_and_assets_without_exposing_private_files_or_api_fallback() {
    let fixture = Fixture::new().await;
    let files = files::validate_resources(&fixture.config).await.unwrap();
    let router = http::public_router(http::StateData::new(files));
    for path in [
        "/",
        "/login",
        "/topics/topic-id",
        "/consumers/subscription-id",
        "/identities",
        "/cluster",
        "/operations/operation-id",
        "/assets/app-test.js",
    ] {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "Failed route: {path}");
        assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
        assert_eq!(
            response.headers().get("x-content-type-options").unwrap(),
            "nosniff"
        );
    }
    for path in [
        "/api",
        "/api/v2/topics",
        "/assets/missing.js",
        "/missing.js",
        "/.vite/manifest.json",
        "/session.key",
        "/../session.key",
        "/assets/%2e%2e/session.key",
        "/health/ready",
        "/metrics",
    ] {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "Exposed path: {path}"
        );
        let body = to_bytes(response.into_body(), 4_096).await.unwrap();
        let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error.get("code").unwrap(), "not_found");
    }
    for method in ["GET", "POST", "DELETE"] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/session")
                    .method(method)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().get("set-cookie").is_none());
        let body = to_bytes(response.into_body(), 4_096).await.unwrap();
        let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error.get("code").unwrap(), "upstream_unavailable");
    }
    fixture.clean().await;
}

#[tokio::test]
async fn console_readiness_is_local_and_shutdown_releases_both_listeners() {
    let fixture = Fixture::new().await;
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let public = SocketAddr::from(([127, 0, 0, 1], console.address.port()));
    let management = console.management_address.unwrap();
    let client = fixture.client().await;
    let page = client
        .get(format!("https://{public}/topics/topic-id"))
        .send()
        .await
        .unwrap();
    assert_eq!(page.status(), 200);
    let ready = client
        .get(format!("http://{management}/health/ready"))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), 200);
    assert!(ready.text().await.unwrap().contains("console_local"));
    let metrics = client
        .get(format!("http://{management}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("tidemq_console_ready 1"));
    console
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(tokio::net::TcpStream::connect(public).await.is_err());
    assert!(tokio::net::TcpStream::connect(management).await.is_err());
    let mut directory = tokio::fs::read_dir(&fixture.root).await.unwrap();
    let mut entries = Vec::new();
    while let Some(entry) = directory.next_entry().await.unwrap() {
        entries.push(entry.file_name().to_string_lossy().into_owned());
    }
    entries.sort();
    assert_eq!(entries, ["ca.pem", "dist", "server.key", "session.key"]);
    fixture.clean().await;
}

#[tokio::test]
async fn console_rejects_missing_assets_invalid_trust_and_unrestricted_session_keys() {
    let fixture = Fixture::new().await;
    tokio::fs::remove_file(fixture.config.static_dir.join("assets/app-test.js"))
        .await
        .unwrap();
    assert!(RunningConsole::start(fixture.config.clone()).await.is_err());
    tokio::fs::write(
        fixture.config.static_dir.join("assets/app-test.js"),
        "export {};",
    )
    .await
    .unwrap();
    tokio::fs::write(&fixture.config.cluster.trust_roots, "not a certificate")
        .await
        .unwrap();
    assert!(RunningConsole::start(fixture.config.clone()).await.is_err());
    fixture.clean().await;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let fixture = Fixture::new().await;
        tokio::fs::set_permissions(
            &fixture.config.session.key_file,
            std::fs::Permissions::from_mode(0o644),
        )
        .await
        .unwrap();
        assert!(RunningConsole::start(fixture.config.clone()).await.is_err());
        tokio::fs::set_permissions(
            &fixture.config.session.key_file,
            std::fs::Permissions::from_mode(0o440),
        )
        .await
        .unwrap();
        assert!(files::validate_resources(&fixture.config).await.is_ok());
        fixture.clean().await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn console_rejects_assets_symlinked_outside_the_build_directory() {
    let fixture = Fixture::new().await;
    let asset = fixture.config.static_dir.join("assets/app-test.js");
    tokio::fs::remove_file(&asset).await.unwrap();
    std::os::unix::fs::symlink(&fixture.config.session.key_file, &asset).unwrap();
    assert!(files::validate_resources(&fixture.config).await.is_err());
    fixture.clean().await;
}

#[tokio::test]
async fn console_slow_request_bodies_expire_before_submission_and_release_admission() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let fixture = Fixture::new().await;
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let address = SocketAddr::from(([127, 0, 0, 1], console.address.port()));
    let mut stalled = Vec::new();
    for _ in 0..4 {
        let mut stream = fixture.connect(address).await;
        stream
            .write_all(format!("POST /api/v1/topics HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{{").as_bytes())
            .await
            .unwrap();
        stalled.push(stream);
    }
    let client = fixture.client().await;
    let url = format!("https://{address}/api/v1/me");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let response = client.get(&url).send().await.unwrap();
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                break;
            }
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("The incomplete bodies did not occupy the bounded API slots");
    let mut responses = tokio::task::JoinSet::new();
    for stream in stalled {
        responses.spawn(async move {
            let mut response = Vec::new();
            tokio::time::timeout(
                Duration::from_secs(15),
                stream.take(8_192).read_to_end(&mut response),
            )
            .await
            .expect("An incomplete request body exceeded its response deadline")
            .unwrap();
            String::from_utf8(response).unwrap()
        });
    }
    while let Some(response) = responses.join_next().await {
        let response = response.unwrap();
        assert!(response.starts_with("HTTP/1.1 408 "));
        assert!(response.contains("request_body_timeout"));
        assert!(response.contains("no business operation was submitted"));
        assert!(!response.contains("outcome_unknown"));
    }
    let response = client.get(url).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let error: serde_json::Value =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(error.get("code").unwrap(), "authentication_required");
    console
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    fixture.clean().await;
}
