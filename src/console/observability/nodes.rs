use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use super::{snapshot_partial, snapshot_source};

pub(super) fn component(cluster: &Value, role: &str) -> Value {
    let nodes = role_nodes(cluster, role);
    let active: Vec<_> = nodes
        .iter()
        .copied()
        .filter(|node| !retired(node))
        .collect();
    let ready = active
        .iter()
        .filter(|node| readiness(node) == Some(true))
        .count();
    let missing = active.iter().any(|node| readiness(node).is_none());
    let groups = groups(&active, role);
    let needs_quorum = matches!(role, "metadata" | "storage");
    let groups_partial = active
        .iter()
        .any(|node| node.get("groupsPartial").and_then(Value::as_bool) != Some(false));
    let quorum_missing = needs_quorum
        && (groups.is_empty()
            || groups_partial
            || groups.iter().any(|group| {
                group
                    .get("quorumConfirmed")
                    .and_then(Value::as_bool)
                    .is_none()
            }));
    let quorum_failed = groups
        .iter()
        .any(|group| group.get("quorumConfirmed").and_then(Value::as_bool) == Some(false));
    let health = if active.is_empty() || missing || truncated(cluster) {
        "unknown"
    } else if ready == 0 {
        "unavailable"
    } else if ready != active.len() || quorum_failed {
        "degraded"
    } else if quorum_missing {
        "unknown"
    } else {
        "healthy"
    };
    let detail = if needs_quorum {
        "Authenticated readiness samples and leader linearizable probes; follower quorum is unknown, and desired replica counts are not reported."
    } else {
        "Authenticated process readiness samples; registry liveness and deployment desired counts are separate facts."
    };
    json!({
        "id": role, "name": role, "health": health, "detail": detail,
        "instances": (!truncated(cluster)).then(|| nodes.len().to_string()),
        "registered": (!truncated(cluster)).then(|| active.len().to_string()),
        "ready": (!missing && !truncated(cluster)).then(|| ready.to_string()), "groups": groups, "links": [],
        "observedAtMs": cluster.get("observedAtMs"), "source": snapshot_source(cluster),
        "partial": snapshot_partial(cluster) || missing || quorum_missing,
    })
}

pub(super) fn instance(cluster: &Value, node: &Value, role: &str) -> Value {
    let ready = readiness(node);
    let groups: Vec<_> = node_groups(node, role).cloned().collect();
    let own_quorum_failed = groups
        .iter()
        .any(|group| leader_proof(node, group) == Some(false));
    let health = if retired(node) {
        "unknown"
    } else {
        match ready {
            Some(true) if own_quorum_failed => "degraded",
            Some(true) => "healthy",
            Some(false) => "unavailable",
            None => "unknown",
        }
    };
    let id = node.get("id").and_then(Value::as_str);
    let detail = if retired(node) {
        "The registry marks this node as retired"
    } else if own_quorum_failed {
        "The process answered its readiness probe, but a local leader could not confirm a linearizable quorum"
    } else if ready.is_none() {
        node.get("reason")
            .and_then(Value::as_str)
            .unwrap_or("A fresh authenticated readiness sample is unavailable")
    } else {
        "Authenticated process readiness; follower groups do not assert quorum"
    };
    json!({
        "id": format!("{role}:{}", id.unwrap_or("unknown")), "nodeId": id, "role": role,
        "zone": node.get("zone"), "version": node.get("version"), "instance": node.get("instance"),
        "endpoint": node.get("endpoint"), "live": node.get("live"), "ready": ready,
        "sampledAtMs": node.get("sampledAtMs"), "groups": groups, "groupsPartial": node.get("groupsPartial"),
        "logDroppedEvents": node.get("logDroppedEvents"), "logSinkErrors": node.get("logSinkErrors"),
        "health": health, "detail": detail, "observedAtMs": cluster.get("observedAtMs"),
        "source": snapshot_source(cluster), "partial": snapshot_partial(cluster) || ready.is_none() || id.is_none(),
    })
}

pub(super) fn role_nodes<'a>(cluster: &'a Value, role: &str) -> Vec<&'a Value> {
    cluster
        .get("items")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|node| {
            node.get("roles")
                .and_then(Value::as_array)
                .is_some_and(|roles| roles.iter().any(|value| value.as_str() == Some(role)))
        })
        .collect()
}

fn readiness(node: &Value) -> Option<bool> {
    node.get("sampledAtMs").and_then(Value::as_str)?;
    node.get("ready").and_then(Value::as_bool)
}

fn retired(node: &Value) -> bool {
    node.get("retired").and_then(Value::as_bool) == Some(true)
}

fn truncated(cluster: &Value) -> bool {
    cluster.get("items").and_then(Value::as_array).is_none()
        || cluster
            .get("nextCursor")
            .is_some_and(|cursor| !cursor.is_null())
}

fn node_groups<'a>(node: &'a Value, role: &'a str) -> impl Iterator<Item = &'a Value> {
    node.get("groups")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(move |group| group.get("role").and_then(Value::as_str) == Some(role))
}

fn leader_proof(node: &Value, group: &Value) -> Option<bool> {
    let id = node.get("id").and_then(Value::as_str)?;
    let leader = group.get("leader").and_then(Value::as_str)?;
    if id != leader || readiness(node).is_none() {
        return None;
    }
    group.get("quorumConfirmed").and_then(Value::as_bool)
}

fn groups(nodes: &[&Value], role: &str) -> Vec<Value> {
    let mut observations: BTreeMap<&str, Vec<(&Value, &Value)>> = BTreeMap::new();
    for node in nodes {
        for group in node_groups(node, role) {
            if let Some(id) = group.get("id").and_then(Value::as_str) {
                observations.entry(id).or_default().push((node, group));
            }
        }
    }
    observations
        .into_iter()
        .filter_map(|(id, rows)| group(id, &rows))
        .collect()
}

fn group(id: &str, rows: &[(&Value, &Value)]) -> Option<Value> {
    let (_, first) = rows.first()?;
    let leaders: BTreeSet<_> = rows
        .iter()
        .filter_map(|(_, group)| group.get("leader").and_then(Value::as_str))
        .collect();
    let leader = (leaders.len() == 1)
        .then(|| leaders.first().copied())
        .flatten();
    let proof = rows
        .iter()
        .find(|(node, group)| leader_proof(node, group).is_some());
    let quorum = if leaders.len() == 1 {
        proof.and_then(|(node, group)| leader_proof(node, group))
    } else {
        None
    };
    let leader_view = proof.map(|(_, group)| *group);
    let canonical = leader_view.unwrap_or(first);
    Some(
        json!({"id": id, "role": first.get("role"), "leader": leader, "voters": canonical.get("voters"),
            "quorumConfirmed": quorum, "hotBytes": leader_view.and_then(|group| group.get("hotBytes")),
            "hotMessages": leader_view.and_then(|group| group.get("hotMessages")),
            "reason": if leaders.len() > 1 { Some("Observed replicas disagree on the leader") } else if quorum.is_none() { Some("A fresh leader linearizable proof is unavailable") } else { None },
        }),
    )
}
