use serde_json::{Value, json};

use super::{Observability, page, probes};

const MAX_ALERTS: usize = 128;

pub(super) async fn query(observability: &Observability) -> Value {
    let Some(url) = &observability.config.alerts_url else {
        return page(
            Vec::new(),
            "unconfigured_alerts",
            true,
            Some("A read-only alerts source is not configured"),
        );
    };
    let body = match probes::request(&observability.client, &observability.permits, url).await {
        Ok(body) => body,
        Err(error) => {
            return page(
                Vec::new(),
                "configured_prometheus_alerts",
                true,
                Some(&error.to_string()),
            );
        }
    };
    match parse(&body, observability.config.alerts_detail_url.as_deref()) {
        Some((items, partial)) => page(
            items,
            "configured_prometheus_alerts",
            partial,
            partial.then_some("Some alerts exceeded the supported response shape or row budget"),
        ),
        None => page(
            Vec::new(),
            "configured_prometheus_alerts",
            true,
            Some("The alerts source did not return the supported Prometheus response shape"),
        ),
    }
}

fn parse(body: &[u8], detail_url: Option<&str>) -> Option<(Vec<Value>, bool)> {
    let value: Value = serde_json::from_slice(body).ok()?;
    if value.get("status").and_then(Value::as_str) != Some("success") {
        return None;
    }
    let alerts = value.get("data")?.get("alerts")?.as_array()?;
    let mut partial = alerts.len() > MAX_ALERTS;
    let mut items = Vec::new();
    for alert in alerts.iter().take(MAX_ALERTS) {
        match row(alert, detail_url) {
            Some(row) => items.push(row),
            None => partial = true,
        }
    }
    Some((items, partial))
}

fn row(alert: &Value, detail_url: Option<&str>) -> Option<Value> {
    let state = alert.get("state")?.as_str()?;
    if !matches!(state, "firing" | "pending") {
        return None;
    }
    let labels = alert.get("labels")?.as_object()?;
    let name = labels.get("alertname")?.as_str()?;
    if name.is_empty() {
        return None;
    }
    let annotations = alert.get("annotations").and_then(Value::as_object);
    let detail = annotations
        .and_then(|annotations| {
            annotations
                .get("description")
                .or_else(|| annotations.get("summary"))
        })
        .and_then(Value::as_str)
        .map(|value| bounded_text(value, 2_048));
    let labels: std::collections::BTreeMap<_, _> = labels
        .iter()
        .map(|(key, value)| Some((key.as_str(), value.as_str()?)))
        .collect::<Option<_>>()?;
    let encoded_labels = serde_json::to_vec(&labels).ok()?;
    let id = blake3::hash(&encoded_labels).to_hex().to_string();
    Some(
        json!({"id": id, "name": bounded_text(name, 256), "state": state,
        "detail": detail, "url": detail_url, "runbookUrl": null}),
    )
}

fn bounded_text(value: &str, characters: usize) -> String {
    value
        .chars()
        .filter(|character| !character.is_control() || *character == '\n' || *character == '\t')
        .take(characters)
        .collect()
}
