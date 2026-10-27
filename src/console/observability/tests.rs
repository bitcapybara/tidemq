use axum::Router;
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::get;
use tokio::net::TcpListener;

use super::*;

fn component(url: &str) -> ComponentProbeConfig {
    ComponentProbeConfig {
        id: "prometheus".into(),
        name: "Prometheus".into(),
        probe_url: url.into(),
        detail_url: Some("https://metrics.example.test/graph?g0.expr=up".into()),
    }
}

fn cluster() -> Value {
    json!({"items": [
        {"id": "11", "roles": ["metadata", "storage"], "live": true, "ready": true, "sampledAtMs": "1700000000100", "retired": false, "zone": "zone-a", "version": "0.1.0", "instance": "instance-a", "groupsPartial": false,
            "logDroppedEvents": "7", "logSinkErrors": "3", "groups": [
                {"id": "1", "role": "metadata", "leader": "11", "voters": ["11"], "quorumConfirmed": true, "hotBytes": null, "hotMessages": null},
                {"id": "2", "role": "storage", "leader": "11", "voters": ["11", "12"], "quorumConfirmed": true, "hotBytes": "128", "hotMessages": "2"}
            ]},
        {"id": "12", "roles": ["storage"], "live": null, "reason": "No lease evidence", "retired": false},
        {"id": "13", "roles": ["gateway"], "live": false, "retired": true},
    ], "nextCursor": null, "observedAtMs": "1700000000123", "source": "metadata_snapshot", "partial": false})
}

#[test]
fn observability_configuration_is_fixed_bounded_and_rejects_credentials() {
    let mut config = ObservabilityConfig::default();
    config
        .components
        .push(component("http://prometheus.tidemq.svc:9090/-/ready"));
    config.alerts_url = Some("http://prometheus.tidemq.svc:9090/api/v1/alerts".into());
    config.validate().unwrap();
    for url in [
        "file:///private/session.key",
        "http://user:secret@prometheus/-/ready",
        "http://prometheus/-/ready?token=secret",
        "http://prometheus/-/ready#fragment",
        "http://prometheus:0/-/ready",
    ] {
        let mut invalid = config.clone();
        invalid.components[0].probe_url = url.into();
        assert!(
            invalid.validate().is_err(),
            "Accepted an unsafe fixed probe URL"
        );
    }
    for id in [
        "console",
        "metadata",
        "storage",
        "broker",
        "gateway",
        "",
        "UpperCase",
        "../prometheus",
    ] {
        let mut invalid = config.clone();
        invalid.components[0].id = id.into();
        assert!(
            invalid.validate().is_err(),
            "Accepted an unsafe component identifier"
        );
    }
    let mut duplicate = config.clone();
    duplicate.components.push(config.components[0].clone());
    assert!(duplicate.validate().is_err());
    let mut excessive = config.clone();
    excessive.components = (0..33)
        .map(|index| {
            let mut item = component("http://prometheus/-/ready");
            item.id = format!("service-{index}");
            item
        })
        .collect();
    assert!(excessive.validate().is_err());
    config.alerts_url = Some("http://prometheus/api/v1/admin/tsdb/delete_series".into());
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn component_quorum_requires_a_leader_proof_and_never_treats_follower_unknown_as_failure() {
    let observations = Observability::from_config(ObservabilityConfig::default()).unwrap();
    let mut snapshot = cluster();
    let nodes = snapshot.get_mut("items").unwrap().as_array_mut().unwrap();
    let follower = nodes.get_mut(1).unwrap();
    follower.as_object_mut().unwrap().extend([
        ("ready".into(), json!(true)), ("sampledAtMs".into(), json!("1700000000101")),
        ("groupsPartial".into(), json!(false)),
        ("groups".into(), json!([{"id": "2", "role": "storage", "leader": "11", "voters": ["11", "12"], "quorumConfirmed": null, "hotBytes": "120", "hotMessages": "2"}])),
    ]);
    let component = nodes::component(&snapshot, "storage");
    assert_eq!(component.get("health").unwrap(), "healthy");
    assert_eq!(
        component
            .get("groups")
            .unwrap()
            .get(0)
            .unwrap()
            .get("quorumConfirmed")
            .unwrap(),
        true
    );
    assert_eq!(
        component
            .get("groups")
            .unwrap()
            .get(0)
            .unwrap()
            .get("hotBytes")
            .unwrap(),
        "128"
    );
    let follower = snapshot.get_mut("items").unwrap().get_mut(1).unwrap();
    follower
        .get_mut("groups")
        .unwrap()
        .get_mut(0)
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("quorumConfirmed".into(), json!(false));
    assert_eq!(
        nodes::component(&snapshot, "storage")
            .get("health")
            .unwrap(),
        "healthy"
    );
    let leader = snapshot.get_mut("items").unwrap().get_mut(0).unwrap();
    leader
        .get_mut("groups")
        .unwrap()
        .get_mut(1)
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("quorumConfirmed".into(), json!(false));
    assert_eq!(
        nodes::component(&snapshot, "storage")
            .get("health")
            .unwrap(),
        "degraded"
    );
    let leader = snapshot.get_mut("items").unwrap().get_mut(0).unwrap();
    leader
        .as_object_mut()
        .unwrap()
        .insert("ready".into(), Value::Null);
    assert_eq!(
        nodes::component(&snapshot, "storage")
            .get("health")
            .unwrap(),
        "unknown"
    );
    assert_eq!(
        nodes::instance(
            &snapshot,
            snapshot.get("items").unwrap().get(0).unwrap(),
            "storage"
        )
        .get("health")
        .unwrap(),
        "unknown"
    );
    let listing = observations.instances(&snapshot, "storage").await;
    assert_eq!(
        listing
            .get("items")
            .unwrap()
            .get(1)
            .unwrap()
            .get("health")
            .unwrap(),
        "healthy"
    );
}

#[tokio::test]
async fn registry_components_preserve_unknown_health_and_never_infer_quorum_or_replica_counts() {
    let observations = Observability::from_config(ObservabilityConfig::default()).unwrap();
    let snapshot = cluster();
    let components = observations.components(&snapshot).await;
    let items = components.get("items").unwrap().as_array().unwrap();
    let metadata = items
        .iter()
        .find(|item| item.get("id").unwrap() == "metadata")
        .unwrap();
    assert_eq!(metadata.get("health").unwrap(), "healthy");
    assert!(
        metadata
            .get("detail")
            .unwrap()
            .as_str()
            .unwrap()
            .contains("linearizable")
    );
    let storage = items
        .iter()
        .find(|item| item.get("id").unwrap() == "storage")
        .unwrap();
    assert_eq!(storage.get("health").unwrap(), "unknown");
    assert!(storage.get("ready").unwrap().is_null());
    assert_eq!(storage.get("registered").unwrap(), "2");
    let console = items
        .iter()
        .find(|item| item.get("id").unwrap() == "console")
        .unwrap();
    assert!(console.get("instances").unwrap().is_null());
    let instances = observations.instances(&snapshot, "storage").await;
    assert_eq!(instances.get("observedAtMs").unwrap(), "1700000000123");
    assert_eq!(instances.get("source").unwrap(), "metadata_snapshot");
    let first = instances.get("items").unwrap().get(0).unwrap();
    assert_eq!(first.get("zone").unwrap(), "zone-a");
    assert_eq!(first.get("version").unwrap(), "0.1.0");
    assert_eq!(first.get("instance").unwrap(), "instance-a");
    assert_eq!(first.get("logDroppedEvents").unwrap(), "7");
    assert_eq!(first.get("logSinkErrors").unwrap(), "3");
    assert_eq!(
        first
            .get("groups")
            .unwrap()
            .get(0)
            .unwrap()
            .get("quorumConfirmed")
            .unwrap(),
        true
    );
    let second = instances.get("items").unwrap().get(1).unwrap();
    assert_eq!(second.get("health").unwrap(), "unknown");
    assert!(second.get("zone").unwrap().is_null());
    let dependency = observations.instances(&snapshot, "prometheus").await;
    assert!(
        dependency
            .get("items")
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(dependency.get("partial").unwrap(), true);
    let unavailable = observations
        .components(&json!({"items": [], "partial": true}))
        .await;
    assert!(
        unavailable
            .get("items")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item.get("id").unwrap() != "console")
            .all(|item| item.get("health").unwrap() == "unknown")
    );
}

#[tokio::test]
async fn fixed_probes_bound_bodies_and_never_follow_redirects_or_forward_credentials() {
    let app = Router::new()
        .route(
            "/ready",
            get(|headers: HeaderMap| async move {
                assert!(headers.get(header::COOKIE).is_none());
                assert!(headers.get(header::AUTHORIZATION).is_none());
                "private upstream response that must not appear in the page"
            }),
        )
        .route(
            "/redirect",
            get(|| async { (StatusCode::FOUND, [(header::LOCATION, "/ready")], "") }),
        )
        .route("/failed", get(|| async { StatusCode::SERVICE_UNAVAILABLE }))
        .route("/large", get(|| async { "x".repeat(MAX_BODY_BYTES + 1) }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let observations = Observability::from_config(ObservabilityConfig::default()).unwrap();
    for (path, health, detail) in [
        ("ready", "healthy", "HTTP success"),
        ("redirect", "unknown", "redirect"),
        ("failed", "unavailable", "unsuccessful"),
        ("large", "unknown", "body budget"),
    ] {
        let config = component(&format!("http://{address}/{path}"));
        let result = probes::component(&observations.client, &observations.permits, &config).await;
        assert_eq!(result.get("health").unwrap(), health);
        assert!(
            result
                .get("detail")
                .unwrap()
                .as_str()
                .unwrap()
                .contains(detail)
        );
        assert!(!result.to_string().contains("private upstream response"));
        assert_eq!(
            result
                .get("links")
                .unwrap()
                .get(0)
                .unwrap()
                .get("url")
                .unwrap(),
            "https://metrics.example.test/graph?g0.expr=up"
        );
    }
    task.abort();
}

#[tokio::test]
async fn alert_source_failure_and_missing_configuration_are_not_reported_as_zero_alerts() {
    let observations = Observability::from_config(ObservabilityConfig::default()).unwrap();
    let unconfigured = observations.alerts().await;
    assert_eq!(unconfigured.get("partial").unwrap(), true);
    assert!(
        unconfigured
            .get("reason")
            .unwrap()
            .as_str()
            .unwrap()
            .contains("not configured")
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().route("/api/v1/alerts", get(|| async { axum::Json(json!({"status": "success", "data": {"alerts": [
        {"state": "firing", "labels": {"alertname": "StorageUnavailable", "secret_label": "not-rendered"}, "annotations": {"summary": "One storage endpoint is unavailable", "runbook_url": "javascript:alert(1)"}},
        {"state": "pending", "labels": {"alertname": "AckLag"}, "annotations": {}},
        {"state": "unknown", "labels": {"alertname": "Unsupported"}},
    ]}})) }));
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = ObservabilityConfig {
        alerts_url: Some(format!("http://{address}/api/v1/alerts")),
        alerts_detail_url: Some("https://alerts.example.test/alerts".into()),
        ..Default::default()
    };
    let observations = Observability::from_config(config).unwrap();
    let result = observations.alerts().await;
    assert_eq!(result.get("items").unwrap().as_array().unwrap().len(), 2);
    assert_eq!(result.get("partial").unwrap(), true);
    assert!(!result.to_string().contains("not-rendered"));
    assert!(!result.to_string().contains("javascript:"));
    let first = result.get("items").unwrap().get(0).unwrap();
    assert_eq!(
        first.get("url").unwrap(),
        "https://alerts.example.test/alerts"
    );
    assert!(first.get("runbookUrl").unwrap().is_null());
    task.abort();
    let failed = observations.alerts().await;
    assert_eq!(failed.get("partial").unwrap(), true);
    assert!(!failed.get("reason").unwrap().is_null());
}
