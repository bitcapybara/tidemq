//! Operator-only observations from the registry and fixed, read-only dependencies.

mod alerts;
mod nodes;
mod probes;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::ConsoleConfig;
use super::config::{ComponentProbeConfig, ObservabilityConfig};

const CORE_ROLES: [&str; 4] = ["metadata", "storage", "broker", "gateway"];
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_BODY_BYTES: usize = 65_536;

pub struct Observability {
    config: ObservabilityConfig,
    client: reqwest::Client,
    permits: Arc<Semaphore>,
}

impl Observability {
    /// Building the client does not resolve or contact any configured dependency.
    pub fn load(config: &ConsoleConfig) -> crate::Result<Self> {
        Self::from_config(config.observability.clone())
    }

    fn from_config(config: ObservabilityConfig) -> crate::Result<Self> {
        config.validate()?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(PROBE_TIMEOUT)
            .connect_timeout(Duration::from_secs(1))
            .pool_max_idle_per_host(1)
            .build()
            .map_err(|_| crate::Error::Configuration {
                reason: "console observability HTTP client could not be initialized",
            })?;
        Ok(Self {
            config,
            client,
            permits: Arc::new(Semaphore::new(4)),
        })
    }

    pub fn contains_component(&self, id: &str) -> bool {
        id == "console"
            || CORE_ROLES.contains(&id)
            || self
                .config
                .components
                .iter()
                .any(|component| component.id == id)
    }

    pub async fn components(&self, cluster: &Value) -> Value {
        let mut items = vec![console_component()];
        items.extend(
            CORE_ROLES
                .into_iter()
                .map(|role| nodes::component(cluster, role)),
        );
        let mut tasks = JoinSet::new();
        for config in &self.config.components {
            let config = config.clone();
            let client = self.client.clone();
            let permits = self.permits.clone();
            tasks.spawn(async move { probes::component(&client, &permits, &config).await });
        }
        let mut partial = snapshot_partial(cluster);
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(item) => items.push(item),
                Err(_) => partial = true,
            }
        }
        items.sort_by(|left, right| {
            left.get("id")
                .and_then(Value::as_str)
                .cmp(&right.get("id").and_then(Value::as_str))
        });
        partial |= items
            .iter()
            .any(|item| item.get("partial").and_then(Value::as_bool) != Some(false));
        page(items, "console_registry_and_fixed_probes", partial, None)
    }

    pub async fn instances(&self, cluster: &Value, component_id: &str) -> Value {
        if component_id == "console" {
            return page(
                vec![json!({
                    "id": "current-console-process", "nodeId": null, "role": "console", "zone": null,
                    "version": env!("CARGO_PKG_VERSION"), "health": "healthy",
                    "detail": "This responding Console process is ready; other Console replicas are not enumerated.",
                    "observedAtMs": observed_at_ms(), "source": "console_current_process", "partial": false,
                })],
                "console_current_process",
                true,
                Some("Other Console replicas are not enumerated"),
            );
        }
        if !CORE_ROLES.contains(&component_id) {
            return page(
                Vec::new(),
                "configured_dependency",
                true,
                Some("Dependency instances are not enumerated by a health endpoint"),
            );
        }
        let items: Vec<_> = nodes::role_nodes(cluster, component_id)
            .into_iter()
            .map(|node| nodes::instance(cluster, node, component_id))
            .collect();
        json!({"items": items, "nextCursor": null, "observedAtMs": cluster.get("observedAtMs"),
            "source": snapshot_source(cluster), "partial": snapshot_partial(cluster), "reason": null})
    }

    pub async fn alerts(&self) -> Value {
        alerts::query(self).await
    }
}

fn console_component() -> Value {
    json!({
        "id": "console", "name": "Console", "health": "healthy",
        "detail": "This responding Console process is ready; total replica count is unknown.",
        "instances": null, "registered": null, "ready": null, "links": [],
        "observedAtMs": observed_at_ms(), "source": "console_current_process", "partial": false,
    })
}

fn snapshot_partial(cluster: &Value) -> bool {
    cluster.get("partial").and_then(Value::as_bool) != Some(false)
        || cluster.get("items").and_then(Value::as_array).is_none()
        || cluster
            .get("observedAtMs")
            .and_then(Value::as_str)
            .is_none()
        || cluster
            .get("nextCursor")
            .is_some_and(|cursor| !cursor.is_null())
}

fn snapshot_source(cluster: &Value) -> &str {
    cluster
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or("metadata_registry")
}

fn observed_at_ms() -> Option<String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis().to_string())
}

fn page(items: Vec<Value>, source: &str, partial: bool, reason: Option<&str>) -> Value {
    let observed_at = observed_at_ms();
    json!({"items": items, "nextCursor": null, "observedAtMs": observed_at, "source": source, "partial": partial || observed_at.is_none(), "reason": reason})
}

fn links(config: &ComponentProbeConfig) -> Value {
    config.detail_url.as_ref().map_or_else(
        || json!([]),
        |url| json!([{"label": "Details", "url": url}]),
    )
}

#[cfg(test)]
mod tests;
