use std::net::SocketAddr;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;
use tower::ServiceExt;

use super::tests::Fixture;
use super::{RunningConsole, files, http};
use crate::runtime::process::{Phase, Process};

fn address(console: &RunningConsole) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], console.address.port()))
}

fn client_ephemeral_ports() -> std::ops::RangeInclusive<u16> {
    #[cfg(target_os = "macos")]
    let contents = {
        let output = std::process::Command::new("sysctl")
            .args([
                "-n",
                "net.inet.ip.portrange.first",
                "net.inet.ip.portrange.last",
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap()
    };
    #[cfg(target_os = "linux")]
    let contents = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").unwrap();
    let mut values = contents
        .split_ascii_whitespace()
        .map(|value| value.parse::<u16>().unwrap());
    let first = values.next().unwrap();
    let last = values.next().unwrap();
    assert!(first <= last);
    first..=last
}

#[tokio::test]
async fn fixture_leases_exclude_automatic_ports_and_survive_shutdown_rebind_checks() {
    let ephemeral = tokio::task::spawn_blocking(client_ephemeral_ports)
        .await
        .unwrap();
    let first = Fixture::new().await;
    let second = Fixture::new().await;
    let addresses = [
        first.config.listen,
        first.config.management_listen,
        second.config.listen,
        second.config.management_listen,
    ];
    let isolated = std::collections::BTreeSet::from(addresses).len() == 4
        && addresses
            .iter()
            .all(|address| address.port() != 0 && !ephemeral.contains(&address.port()));
    if !isolated {
        first.clean().await;
        second.clean().await;
        panic!("Fixture leases overlap the kernel's automatic port allocation: {addresses:?}");
    }
    let console = RunningConsole::start(first.config.clone()).await.unwrap();
    let public = address(&console);
    let management = console.management_address.unwrap();
    let metrics = console.state.metrics.clone();
    let tasks = console.connections.clone();
    let client = first.client().await;
    client
        .get(format!("https://{public}/"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    console
        .shutdown_until(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(
        tasks.is_empty(),
        "Accepted connection tasks survived shutdown"
    );
    assert_eq!(metrics.connections.get(), 0);
    assert_eq!(metrics.handshakes.get(), 0);
    let competing = RunningConsole::start(second.config.clone()).await.unwrap();
    let rebound_public = tokio::net::TcpListener::bind(public).await.unwrap();
    let rebound_management = tokio::net::TcpListener::bind(management).await.unwrap();
    drop((rebound_public, rebound_management));
    shutdown(competing).await;
    first.clean().await;
    second.clean().await;
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(4), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("The bounded Console condition did not complete");
}

async fn shutdown(console: RunningConsole) {
    let public = address(&console);
    let management = console.management_address;
    let metrics = console.state.metrics.clone();
    console
        .shutdown_until(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(metrics.connections.get(), 0, "A Console connection leaked");
    assert_eq!(metrics.handshakes.get(), 0, "A Console handshake leaked");
    let rebound = tokio::net::TcpListener::bind(public)
        .await
        .unwrap_or_else(|error| panic!("Console port {public} could not be rebound: {error}"));
    drop(rebound);
    if let Some(address) = management {
        drop(tokio::net::TcpListener::bind(address).await.unwrap());
    }
}

#[tokio::test]
async fn native_https_checks_trust_and_rejects_plaintext_without_losing_readiness() {
    let fixture = Fixture::new().await;
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let url = format!("https://{}/", address(&console));
    let response = fixture.client().await.get(&url).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("content-security-policy").is_some());
    response.bytes().await.unwrap();
    assert!(
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(&url)
            .send()
            .await
            .is_err()
    );
    let mut stream = tokio::net::TcpStream::connect(address(&console))
        .await
        .unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response)).await;
    assert!(!response.starts_with(b"HTTP/1.1 200"));
    assert_eq!(console.state.process.phase(), Phase::Ready);
    assert_eq!(console.state.metrics.asset_rechecks.get(), 0);
    shutdown(console).await;
    fixture.clean().await;
}

#[tokio::test]
async fn invalid_tls_material_and_bind_failure_release_previously_bound_ports() {
    let mut fixture = Fixture::new().await;
    for contents in ["not a key", ""] {
        tokio::fs::write(&fixture.config.tls.private_key, contents)
            .await
            .unwrap();
        assert!(RunningConsole::start(fixture.config.clone()).await.is_err());
    }
    let other =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "console.example.test".into()])
            .unwrap();
    tokio::fs::write(
        &fixture.config.tls.private_key,
        other.signing_key.serialize_pem(),
    )
    .await
    .unwrap();
    assert!(RunningConsole::start(fixture.config.clone()).await.is_err());
    tokio::fs::write(&fixture.config.tls.certificate, other.cert.pem())
        .await
        .unwrap();
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    fixture.config.management_listen = occupied.local_addr().unwrap();
    let reserved = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    fixture.config.listen = reserved.local_addr().unwrap();
    drop(reserved);
    assert!(RunningConsole::start(fixture.config.clone()).await.is_err());
    drop(
        tokio::net::TcpListener::bind(fixture.config.listen)
            .await
            .unwrap(),
    );
    drop(occupied);
    fixture.clean().await;
}

#[tokio::test]
async fn slow_handshakes_are_concurrent_bounded_and_expire_without_a_process_failure() {
    let fixture = Fixture::new().await;
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let first = tokio::net::TcpStream::connect(address(&console))
        .await
        .unwrap();
    wait_for(|| console.state.metrics.handshakes.get() == 1).await;
    let response = fixture
        .client()
        .await
        .get(format!("https://{}/", address(&console)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.bytes().await.unwrap();
    let mut stalled = vec![first];
    for _ in 0..40 {
        stalled.push(
            tokio::net::TcpStream::connect(address(&console))
                .await
                .unwrap(),
        );
    }
    wait_for(|| console.state.metrics.connection_rejections.get() > 0).await;
    assert!(console.state.metrics.handshakes.get() <= 32);
    assert!(console.state.metrics.connections.get() <= 256);
    wait_for(|| console.state.metrics.tls_timeouts.get() > 0).await;
    assert_eq!(console.state.process.phase(), Phase::Ready);
    assert_eq!(console.state.metrics.asset_rechecks.get(), 0);
    drop(stalled);
    shutdown(console).await;
    fixture.clean().await;
}

#[tokio::test]
async fn required_index_script_and_css_fail_shared_process_before_the_http_task_exits() {
    for name in ["index.html", "assets/app-test.js", "assets/app-test.css"] {
        let fixture = Fixture::new().await;
        let mut console = RunningConsole::start(fixture.config.clone()).await.unwrap();
        let client = fixture.client().await;
        let process = console.state.process.clone();
        let mut changed = process.subscribe();
        let path = fixture.config.static_dir.join(name);
        let contents = tokio::fs::read(&path).await.unwrap();
        tokio::fs::remove_file(&path).await.unwrap();
        let response = client
            .get(format!("https://{}/{name}", address(&console)))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        tokio::time::timeout(Duration::from_secs(3), changed.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(process.phase(), Phase::Failed);
        assert_eq!(console.state.metrics.asset_failures.get(), 1);
        assert_eq!(
            console.tasks.len(),
            3,
            "Resource failure incorrectly required a listener exit"
        );
        let response = client
            .get(format!(
                "http://{}/health/ready",
                console.management_address.unwrap()
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = client
            .post(format!("https://{}/api/v1/topics", address(&console)))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        tokio::fs::write(&path, contents).await.unwrap();
        assert_eq!(
            client
                .get(format!("https://{}/{name}", address(&console)))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert!(!process.ready(), "A late asset recovery restored readiness");
        assert_eq!(process.phase(), Phase::Failed);
        assert!(console.wait_for_exit().await.is_err());
        shutdown(console).await;
        fixture.clean().await;
    }
}

#[tokio::test]
async fn same_length_required_asset_corruption_fails_readiness_over_https() {
    for name in ["index.html", "assets/app-test.js", "assets/app-test.css"] {
        let fixture = Fixture::new().await;
        let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
        let client = fixture.client().await;
        let process = console.state.process.clone();
        let path = fixture.config.static_dir.join(name);
        let contents = tokio::fs::read(&path).await.unwrap();
        tokio::fs::write(&path, vec![b'!'; contents.len()])
            .await
            .unwrap();
        let response = client
            .get(format!("https://{}/{name}", address(&console)))
            .send()
            .await
            .unwrap();
        let _ = response.bytes().await;
        let failed = tokio::time::timeout(Duration::from_secs(2), async {
            while process.phase() != Phase::Failed {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok();
        let readiness = client
            .get(format!(
                "http://{}/health/ready",
                console.management_address.unwrap()
            ))
            .send()
            .await
            .unwrap()
            .status();
        let failures = console.state.metrics.asset_failures.get();
        shutdown(console).await;
        fixture.clean().await;
        assert!(failed, "Same-length corruption was not supervised: {name}");
        assert_eq!(readiness, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(failures, 1);
    }
}

#[tokio::test]
async fn same_length_asset_damage_recovers_inside_the_existing_recheck_deadline() {
    let fixture = Fixture::new().await;
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let client = fixture.client().await;
    let path = fixture.config.static_dir.join("assets/app-test.js");
    let original = tokio::fs::read(&path).await.unwrap();
    tokio::fs::write(&path, vec![b'!'; original.len()])
        .await
        .unwrap();
    let url = format!("https://{}/assets/app-test.js", address(&console));
    let response = client.get(&url).send().await.unwrap();
    let _ = response.bytes().await;
    tokio::fs::write(&path, &original).await.unwrap();
    wait_for(|| console.state.metrics.asset_recoveries.get() == 1).await;
    assert_eq!(console.state.metrics.asset_rechecks.get(), 1);
    assert_eq!(console.state.metrics.asset_failures.get(), 0);
    assert_eq!(console.state.process.phase(), Phase::Ready);
    assert_eq!(
        client.get(url).send().await.unwrap().bytes().await.unwrap(),
        original
    );
    shutdown(console).await;
    fixture.clean().await;
}

#[tokio::test]
async fn https_partial_conditional_and_cancelled_assets_do_not_report_corruption() {
    let fixture = Fixture::new().await;
    tokio::fs::write(
        fixture.config.static_dir.join("assets/app-test.js"),
        vec![b'x'; 8_388_608],
    )
    .await
    .unwrap();
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let client = fixture.client().await;
    let url = format!("https://{}/assets/app-test.js", address(&console));
    drop(client.get(&url).send().await.unwrap());
    assert_eq!(
        client.head(&url).send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .get(&url)
            .header("if-modified-since", "Wed, 01 Jan 2098 00:00:00 GMT")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_MODIFIED
    );
    let response = client
        .get(&url)
        .header("range", "bytes=0-3")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.bytes().await.unwrap().len(), 4);
    assert_eq!(
        client
            .get(&url)
            .header("range", "not-a-range")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::RANGE_NOT_SATISFIABLE
    );
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(console.state.process.phase(), Phase::Ready);
    assert_eq!(console.state.metrics.asset_errors.get(), 0);
    assert_eq!(console.state.metrics.asset_rechecks.get(), 0);
    shutdown(console).await;
    fixture.clean().await;
}

#[tokio::test]
async fn transient_asset_errors_are_singleflight_and_recover_within_the_deadline() {
    let fixture = Fixture::new().await;
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let path = fixture.config.static_dir.join("assets/app-test.js");
    let contents = tokio::fs::read(&path).await.unwrap();
    tokio::fs::remove_file(&path).await.unwrap();
    let router = http::public_router(console.state.clone());
    let mut requests = tokio::task::JoinSet::new();
    for _ in 0..128 {
        let router = router.clone();
        requests.spawn(async move {
            router
                .oneshot(
                    Request::builder()
                        .uri("/assets/app-test.js")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
        });
    }
    while let Some(status) = requests.join_next().await {
        assert_eq!(status.unwrap(), StatusCode::NOT_FOUND);
    }
    tokio::fs::write(&path, contents).await.unwrap();
    wait_for(|| console.state.metrics.asset_recoveries.get() == 1).await;
    assert_eq!(console.state.metrics.asset_rechecks.get(), 1);
    assert_eq!(console.state.metrics.asset_failures.get(), 0);
    assert_eq!(console.state.process.phase(), Phase::Ready);
    shutdown(console).await;
    fixture.clean().await;
}

#[tokio::test]
async fn request_errors_and_dropped_asset_bodies_never_schedule_rechecks() {
    let fixture = Fixture::new().await;
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let client = fixture.client().await;
    let url = format!("https://{}", address(&console));
    for _ in 0..8 {
        assert_eq!(
            client
                .get(format!("{url}/assets/missing.js?secret=never-log-this"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            client
                .post(format!("{url}/assets/app-test.js"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        for range in ["not-a-range", "bytes=99999999-", "bytes=4-1", "bytes=-0"] {
            assert_eq!(
                client
                    .get(format!("{url}/assets/app-test.js"))
                    .header("range", range)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::RANGE_NOT_SATISFIABLE
            );
        }
        let mut stream = fixture.connect(address(&console)).await;
        stream
            .write_all(b"GET /assets/app-test.js HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        drop(stream);
        let response = http::public_router(console.state.clone())
            .oneshot(
                Request::builder()
                    .uri("/assets/app-test.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        drop(response);
    }
    let response = client
        .get(format!("{url}/assets/app-test.js"))
        .header("range", "bytes=0-3")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.bytes().await.unwrap().len(), 4);
    assert_eq!(console.state.metrics.asset_errors.get(), 0);
    assert_eq!(console.state.metrics.asset_rechecks.get(), 0);
    assert_eq!(console.state.process.phase(), Phase::Ready);
    shutdown(console).await;
    fixture.clean().await;
}

#[tokio::test]
async fn server_side_file_truncation_after_response_headers_is_supervised() {
    let fixture = Fixture::new().await;
    let path = fixture.config.static_dir.join("assets/app-test.js");
    tokio::fs::write(&path, vec![b'x'; 1_048_576])
        .await
        .unwrap();
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let response = http::public_router(console.state.clone())
        .oneshot(
            Request::builder()
                .uri("/assets/app-test.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    tokio::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .await
        .unwrap()
        .set_len(0)
        .await
        .unwrap();
    assert!(
        to_bytes(response.into_body(), 2_097_152).await.is_err(),
        "Truncated file stream was accepted as complete"
    );
    wait_for(|| console.state.process.phase() == Phase::Failed).await;
    assert_eq!(console.state.metrics.asset_rechecks.get(), 1);
    assert_eq!(console.state.metrics.asset_failures.get(), 1);
    shutdown(console).await;
    fixture.clean().await;
}

#[tokio::test]
async fn draining_cancels_pending_rechecks_and_cannot_be_undone_by_late_reads() {
    let fixture = Fixture::new().await;
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let path = fixture.config.static_dir.join("index.html");
    tokio::fs::remove_file(&path).await.unwrap();
    let response = http::public_router(console.state.clone())
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let process = console.state.process.clone();
    process.drain();
    tokio::fs::write(&path, "<html>late success</html>")
        .await
        .unwrap();
    assert!(!process.ready());
    assert_eq!(process.phase(), Phase::Draining);
    shutdown(console).await;
    assert_eq!(process.phase(), Phase::Stopped);
    fixture.clean().await;
}

#[tokio::test]
async fn startup_manifest_tracks_imports_css_and_actual_index_references() {
    let fixture = Fixture::new().await;
    let directory = &fixture.config.static_dir;
    tokio::fs::write(directory.join("assets/import.js"), "export {};")
        .await
        .unwrap();
    tokio::fs::write(directory.join("assets/optional.js"), "export {};")
        .await
        .unwrap();
    tokio::fs::write(directory.join(".vite/manifest.json"), r#"{"index.html":{"file":"assets/app-test.js","isEntry":true,"imports":["shared"],"dynamicImports":["optional"]},"shared":{"file":"assets/import.js","css":["assets/app-test.css"]},"optional":{"file":"assets/optional.js"}}"#).await.unwrap();
    let files = files::validate_resources(&fixture.config).await.unwrap();
    for name in [
        "index.html",
        "assets/app-test.js",
        "assets/import.js",
        "assets/app-test.css",
    ] {
        assert!(files.required(name).is_some());
    }
    assert!(files.required("assets/optional.js").is_some());
    tokio::fs::write(
        directory.join("index.html"),
        "<script type='module' src='/assets/not-in-manifest.js'></script>",
    )
    .await
    .unwrap();
    assert!(files::validate_resources(&fixture.config).await.is_err());
    fixture.clean().await;
}

#[tokio::test]
async fn shutdown_uses_one_deadline_and_reaps_stalled_tls_and_http_connections() {
    let fixture = Fixture::new().await;
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let stalled_tls = tokio::net::TcpStream::connect(address(&console))
        .await
        .unwrap();
    let mut http = fixture.connect(address(&console)).await;
    http.write_all(b"POST /api/v1/topics HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{").await.unwrap();
    wait_for(|| console.state.metrics.requests.get() > 0).await;
    let metrics = console.state.metrics.clone();
    let public = address(&console);
    let start = Instant::now();
    assert!(
        console
            .shutdown_until(start + Duration::from_millis(50))
            .await
            .is_err()
    );
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(metrics.connections.get(), 0);
    assert_eq!(metrics.handshakes.get(), 0);
    drop(tokio::net::TcpListener::bind(public).await.unwrap());
    drop((stalled_tls, http));
    fixture.clean().await;
}

#[tokio::test]
async fn outer_shutdown_deadline_aborts_a_required_task_that_ignores_shutdown() {
    let fixture = Fixture::new().await;
    let mut console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    console
        .tasks
        .spawn(std::future::pending::<crate::Result<()>>());
    let stream = fixture.connect(address(&console)).await;
    let metrics = console.state.metrics.clone();
    let public = address(&console);
    let started = Instant::now();
    assert!(
        console
            .shutdown_until(started + Duration::from_millis(50))
            .await
            .is_err()
    );
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(metrics.connections.get(), 0);
    assert_eq!(metrics.handshakes.get(), 0);
    drop(tokio::net::TcpListener::bind(public).await.unwrap());
    drop(stream);
    fixture.clean().await;
}

#[tokio::test]
async fn embedded_start_uses_the_pre_registered_metrics_and_parent_readiness_without_management() {
    let fixture = Fixture::new().await;
    let process = Process::default();
    process.recovering();
    let metrics = std::sync::Arc::new(super::ConsoleMetrics::default());
    let mut registry = prometheus_client::registry::Registry::default();
    metrics.register(&mut registry);
    process.register(&mut registry);
    let api = super::api::ApiState::load(&fixture.config).await.unwrap();
    let config = super::EmbeddedConsoleConfig {
        listen: fixture.config.listen,
        static_dir: fixture.config.static_dir.clone(),
        session: fixture.config.session.clone(),
        observability: Default::default(),
    };
    let mut tls = fixture.config.tls.clone();
    tls.trust_roots = fixture.root.join("intentionally-absent-peer-roots.pem");
    let console =
        RunningConsole::start_embedded(&config, &tls, api, process.clone(), metrics.clone())
            .await
            .unwrap();
    assert!(console.management_address.is_none());
    assert_eq!(console.tasks.len(), 2);
    assert_eq!(
        process.phase(),
        Phase::Recovering,
        "The Console bypassed the parent's readiness gate"
    );
    let client = fixture.client().await;
    let url = format!("https://{}", address(&console));
    assert_eq!(
        client
            .post(format!("{url}/api/v1/topics"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        client
            .get(format!("{url}/metrics"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert!(process.ready());
    let response = client
        .get(format!("{url}/?secret=never-export-this"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.bytes().await.unwrap();
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    assert!(text.contains("tidemq_console_http_requests_total 3"));
    assert!(text.contains("process_ready 1"));
    assert!(!text.contains("never-export-this"));
    shutdown(console).await;
    assert_eq!(
        process.phase(),
        Phase::Draining,
        "The Console prematurely marked the parent process stopped"
    );
    fixture.clean().await;
}

#[tokio::test]
async fn an_unexpected_required_task_exit_notifies_the_process_without_polling_wait_for_exit() {
    let fixture = Fixture::new().await;
    let mut console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let process = console.state.process.clone();
    let mut changed = process.subscribe();
    super::spawn_owned(
        &mut console.tasks,
        process.clone(),
        console.shutdown.subscribe(),
        "console_test_task",
        async { Ok(()) },
    );
    tokio::time::timeout(Duration::from_secs(1), changed.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(process.phase(), Phase::Failed);
    assert!(console.wait_for_exit().await.is_err());
    shutdown(console).await;
    fixture.clean().await;
}

#[tokio::test]
async fn certificate_chain_preserves_leaf_der_and_rejects_empty_or_invalid_pem() {
    let fixture = Fixture::new().await;
    let first = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let second = rcgen::generate_simple_self_signed(vec!["issuer.local".into()]).unwrap();
    let path = &fixture.config.tls.certificate;
    tokio::fs::write(path, format!("{}{}", first.cert.pem(), second.cert.pem()))
        .await
        .unwrap();
    let chain = super::certificate_chain(path).await.unwrap();
    assert_eq!(chain.len(), 2);
    assert_eq!(chain.first().unwrap().as_ref(), first.cert.der().as_ref());
    for contents in [
        "",
        "not a PEM certificate",
        "-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----",
    ] {
        tokio::fs::write(path, contents).await.unwrap();
        assert!(super::certificate_chain(path).await.is_err());
    }
    fixture.clean().await;
}

#[tokio::test]
async fn tls_startup_requires_dns_and_ip_san_coverage_without_peer_trust_roots() {
    let fixture = Fixture::new().await;
    let mut config = fixture.config.tls.clone();
    config.trust_roots = fixture.root.join("absent-peer-trust-roots.pem");
    for origin in [
        "https://console.example.test",
        "https://localhost:7445",
        "https://127.0.0.1:7445",
    ] {
        assert!(super::load_https_identity(&config, origin).await.is_ok());
    }
    for origin in [
        "https://wrong.example.test",
        "https://127.0.0.2",
        "http://localhost",
        "https://[::1]",
    ] {
        assert!(super::load_https_identity(&config, origin).await.is_err());
    }
    let ipv6 = rcgen::generate_simple_self_signed(vec!["::1".into()]).unwrap();
    tokio::fs::write(&config.certificate, ipv6.cert.pem())
        .await
        .unwrap();
    tokio::fs::write(&config.private_key, ipv6.signing_key.serialize_pem())
        .await
        .unwrap();
    assert!(
        super::load_https_identity(&config, "https://[::1]:7445")
            .await
            .is_ok()
    );
    assert!(
        super::load_https_identity(&config, "https://[::2]:7445")
            .await
            .is_err()
    );
    fixture.clean().await;
}

#[tokio::test]
async fn startup_rejects_expired_future_and_client_only_server_certificates() {
    for case in ["expired", "future", "client_only"] {
        let fixture = Fixture::new().await;
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec![
            "console.example.test".into(),
            "localhost".into(),
            "127.0.0.1".into(),
        ])
        .unwrap();
        match case {
            "expired" => params.not_after = rcgen::date_time_ymd(2001, 1, 1),
            "future" => params.not_before = rcgen::date_time_ymd(2099, 1, 1),
            _ => params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth],
        }
        let certificate = params.self_signed(&key).unwrap();
        tokio::fs::write(&fixture.config.tls.certificate, certificate.pem())
            .await
            .unwrap();
        tokio::fs::write(&fixture.config.tls.private_key, key.serialize_pem())
            .await
            .unwrap();
        let standalone = super::validate_config(&fixture.config).await;
        let embedded = super::EmbeddedConsoleConfig {
            listen: fixture.config.listen,
            static_dir: fixture.config.static_dir.clone(),
            session: fixture.config.session.clone(),
            observability: Default::default(),
        };
        let embedded = super::validate_embedded(&embedded, &fixture.config.tls).await;
        let rejected = match RunningConsole::start(fixture.config.clone()).await {
            Ok(console) => {
                shutdown(console).await;
                false
            }
            Err(_) => true,
        };
        drop(
            tokio::net::TcpListener::bind(fixture.config.listen)
                .await
                .unwrap(),
        );
        fixture.clean().await;
        assert!(
            standalone.is_err(),
            "Invalid standalone TLS material accepted: {case}"
        );
        assert!(
            embedded.is_err(),
            "Invalid embedded TLS material accepted: {case}"
        );
        assert!(rejected, "Invalid server TLS material reached bind: {case}");
    }
}

#[tokio::test]
async fn private_ca_server_leaf_is_valid_without_shipping_its_root_certificate() {
    let mut fixture = Fixture::new().await;
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = rcgen::Issuer::new(ca_params, ca_key);
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec![
        "console.example.test".into(),
        "localhost".into(),
        "127.0.0.1".into(),
    ])
    .unwrap();
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let leaf = params.signed_by(&key, &issuer).unwrap();
    tokio::fs::write(&fixture.config.tls.certificate, leaf.pem())
        .await
        .unwrap();
    tokio::fs::write(&fixture.config.tls.private_key, key.serialize_pem())
        .await
        .unwrap();
    fixture.config.cluster.trust_roots = fixture.root.join("issuer.pem");
    tokio::fs::write(&fixture.config.cluster.trust_roots, ca.pem())
        .await
        .unwrap();
    fixture.config.tls.trust_roots = fixture.root.join("absent-peer-trust-roots.pem");
    super::validate_config(&fixture.config).await.unwrap();
    let embedded = super::EmbeddedConsoleConfig {
        listen: fixture.config.listen,
        static_dir: fixture.config.static_dir.clone(),
        session: fixture.config.session.clone(),
        observability: Default::default(),
    };
    super::validate_embedded(&embedded, &fixture.config.tls)
        .await
        .unwrap();
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .tls_certs_only(vec![
            reqwest::Certificate::from_pem(ca.pem().as_bytes()).unwrap(),
        ])
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let response = client
        .get(format!("https://{}/", address(&console)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.bytes().await.unwrap();
    shutdown(console).await;
    fixture.clean().await;
}

#[tokio::test]
async fn startup_material_validation_never_binds_or_creates_keys_and_checks_rotation() {
    let mut fixture = Fixture::new().await;
    let public = tokio::net::TcpListener::bind(fixture.config.listen)
        .await
        .unwrap();
    let management = tokio::net::TcpListener::bind(fixture.config.management_listen)
        .await
        .unwrap();
    fixture.config.management_listen = management.local_addr().unwrap();
    super::validate_config(&fixture.config).await.unwrap();
    let mut embedded = super::EmbeddedConsoleConfig {
        listen: fixture.config.listen,
        static_dir: fixture.config.static_dir.clone(),
        session: fixture.config.session.clone(),
        observability: Default::default(),
    };
    let mut tls = fixture.config.tls.clone();
    tls.trust_roots = fixture.root.join("absent-peer-material.pem");
    super::validate_embedded(&embedded, &tls).await.unwrap();
    let missing = fixture.root.join("must-not-create.key");
    embedded.session.previous_key_files.push(missing.clone());
    assert!(super::validate_embedded(&embedded, &tls).await.is_err());
    assert!(!missing.exists());
    fixture
        .config
        .session
        .previous_key_files
        .push(missing.clone());
    assert!(super::validate_config(&fixture.config).await.is_err());
    embedded.session.previous_key_files.clear();
    embedded.session.key_file = missing.clone();
    assert!(super::validate_embedded(&embedded, &tls).await.is_err());
    assert!(!missing.exists());
    assert_eq!(
        tokio::fs::read(&fixture.config.session.key_file)
            .await
            .unwrap(),
        vec![42_u8; 32]
    );
    drop((public, management));
    fixture.clean().await;
}

#[cfg(unix)]
#[tokio::test]
async fn actual_local_stream_read_errors_trigger_recheck_but_client_body_drop_does_not() {
    use bytes::Bytes;
    use http_body::{Body as HttpBody, Frame};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};

    struct DirectoryRead(tokio::fs::File);
    impl HttpBody for DirectoryRead {
        type Data = Bytes;
        type Error = std::io::Error;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            let mut bytes = [0_u8; 1];
            let mut buffer = ReadBuf::new(&mut bytes);
            match Pin::new(&mut self.0).poll_read(context, &mut buffer) {
                Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),
                Poll::Ready(Ok(())) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }
    let fixture = Fixture::new().await;
    let console = RunningConsole::start(fixture.config.clone()).await.unwrap();
    let directory = tokio::fs::File::open(&fixture.config.static_dir)
        .await
        .unwrap();
    let body = console.state.assets.observe(
        Body::new(DirectoryRead(directory)),
        "assets/app-test.js",
        Some(1),
        false,
    );
    assert!(to_bytes(body, 1024).await.is_err());
    wait_for(|| console.state.metrics.asset_recoveries.get() == 1).await;
    assert_eq!(console.state.metrics.asset_errors.get(), 1);
    assert_eq!(console.state.metrics.asset_rechecks.get(), 1);
    assert_eq!(console.state.process.phase(), Phase::Ready);
    shutdown(console).await;
    fixture.clean().await;
}
