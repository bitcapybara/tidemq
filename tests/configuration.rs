use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use tidemq::config::{Config, ProcessConfig, Role};
use tidemq::console::{ClusterConfig, ConsoleConfig, ConsoleRole, SessionConfig};
use tidemq::runtime::{ObjectConfig, PeerConfig, RuntimeConfig, TopicScalingPolicy, WorkCapacity};
use tidemq_protocol::Id;
use tokio::process::Command;

async fn generate(mode: &str, output: &Path) {
    let result = Command::new("bash")
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("deploy/local/generate.sh"))
        .arg(mode)
        .arg(output)
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "Generator failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
}

async fn validate_nodes(directory: &Path, nodes: &[u64]) {
    for node in nodes {
        let path = directory.join(format!("node-{node}.toml"));
        let config = Config::load(&path).await.unwrap();
        config.runtime.validate(&config).unwrap();
        assert_eq!(config.node_id, *node);
        let text = tokio::fs::read_to_string(&path).await.unwrap();
        let document: toml::Table = toml::from_str(&text).unwrap();
        assert!(document.contains_key("authorization"));
        assert_eq!(
            document.contains_key("gateway"),
            config.role.serves_gateway()
        );
        assert_eq!(document.contains_key("storage"), config.role.stores_data());
        assert!(!document.contains_key("listen"));
    }
}

#[tokio::test]
async fn generator_validates_only_canonical_role_configuration() {
    let root = std::env::temp_dir().join(format!("tidemq-config-layout-{}", Id::new_v7().unwrap()));
    tokio::fs::create_dir(&root).await.unwrap();
    for (mode, nodes) in [
        ("rf1", vec![1]),
        ("rf3-combined", vec![1, 2, 3]),
        ("rf3", vec![11, 12, 13, 21, 22, 23, 31, 32, 41, 42]),
    ] {
        let directory = root.join(mode);
        generate(mode, &directory).await;
        validate_nodes(&directory, &nodes).await;
    }
    let rejected = root.join("rejected");
    let result = Command::new("bash")
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("deploy/local/generate.sh"))
        .arg("rf1")
        .arg(&rejected)
        .arg("legacy")
        .output()
        .await
        .unwrap();
    assert!(!result.status.success());
    assert!(!rejected.exists());
    tokio::fs::remove_dir_all(root).await.unwrap();
}

const NODE_ROLES: [Role; 5] = [
    Role::Combined,
    Role::Gateway,
    Role::Broker,
    Role::Storage,
    Role::Metadata,
];

fn node_template(role: Role) -> &'static str {
    // Exhaustive matching makes a new role require its own template.
    match role {
        Role::Combined => include_str!("../config/combined.toml"),
        Role::Gateway => include_str!("../config/gateway.toml"),
        Role::Broker => include_str!("../config/broker.toml"),
        Role::Storage => include_str!("../config/storage.toml"),
        Role::Metadata => include_str!("../config/metadata.toml"),
    }
}

fn expand_optional_examples(text: &str) -> String {
    text.lines()
        .map(|line| line.trim_start().strip_prefix("#? ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
}

fn field_paths(value: &serde_json::Value) -> BTreeSet<String> {
    fn collect(value: &serde_json::Value, path: &str, paths: &mut BTreeSet<String>) {
        match value {
            serde_json::Value::Object(fields) => {
                for (key, value) in fields {
                    let child = format!("{path}.{key}");
                    paths.insert(child.clone());
                    collect(value, &child, paths);
                }
            }
            serde_json::Value::Array(entries) => {
                for entry in entries {
                    collect(entry, &format!("{path}[]"), paths);
                }
            }
            _ => {}
        }
    }
    let mut paths = BTreeSet::new();
    collect(value, "", &mut paths);
    paths
}

fn node_schema(role: Role) -> Config {
    let defaults = RuntimeConfig::default();
    // No struct update: new optional runtime fields must be considered explicitly.
    let runtime = RuntimeConfig {
        internal_listen: defaults.internal_listen,
        metadata_group: defaults.metadata_group,
        metadata_voters: defaults.metadata_voters,
        groups: defaults.groups,
        peers: vec![PeerConfig {
            node_id: 1,
            role,
            zone: "local".into(),
            internal_address: defaults.internal_listen,
            server_name: "node-1.local".into(),
            certificate_der: "certs/node-1.der".into(),
        }],
        bootstrap_admin_token_file: Some("secrets/bootstrap.token".into()),
        bootstrap: defaults.bootstrap,
        objects: role.stores_data().then(|| ObjectConfig {
            endpoint: "https://objects.example.test".into(),
            region: "replace-me".into(),
            bucket: "replace-me".into(),
            prefix: "replace-me".into(),
            credentials_file: "secrets/objects.toml".into(),
            allow_http_loopback: false,
        }),
        capacity: defaults.capacity,
        retention: defaults.retention,
        archive_interval_ms: defaults.archive_interval_ms,
        topic_scaling: role.stores_metadata().then(TopicScalingPolicy::default),
    };
    let defaults = Config::default();
    Config {
        node_id: defaults.node_id,
        cluster_id: defaults.cluster_id,
        role,
        listen: defaults.listen,
        management_listen: defaults.management_listen,
        data_dir: defaults.data_dir,
        replication_factor: defaults.replication_factor,
        memory: defaults.memory,
        protocol: defaults.protocol,
        logging: defaults.logging,
        tracing: defaults.tracing,
        tls: defaults.tls,
        runtime,
    }
}

fn console_schema() -> ConsoleConfig {
    let mut config = ConsoleConfig {
        role: ConsoleRole::Console,
        listen: "127.0.0.1:8080".parse().unwrap(),
        management_listen: "127.0.0.1:9420".parse().unwrap(),
        static_dir: "console/web/dist".into(),
        cluster: ClusterConfig {
            address: "gateway.tidemq.local:7443".into(),
            server_name: "gateway.tidemq.local".into(),
            trust_roots: "private/ca.pem".into(),
        },
        session: SessionConfig {
            key_file: "private/console-session.key".into(),
            previous_key_files: Vec::new(),
            public_origin: "https://console.example.test".into(),
            max_age_seconds: 1800,
        },
        observability: Default::default(),
        logging: Default::default(),
        tracing: Default::default(),
    };
    config.observability = serde_json::from_value(serde_json::json!({
        "components": [{
            "id": "prometheus-1",
            "name": "Prometheus",
            "probe_url": "http://127.0.0.1:9090/-/ready",
            "detail_url": "https://metrics.example.test"
        }],
        "alerts_url": "http://127.0.0.1:9090/api/v1/alerts",
        "alerts_detail_url": "https://metrics.example.test/alerts"
    }))
    .unwrap();
    config
}

#[test]
fn templates_cover_all_serialized_fields_including_disabled_options() {
    for role in NODE_ROLES {
        let text = expand_optional_examples(node_template(role));
        let document: toml::Table = toml::from_str(&text).unwrap();
        let expected = serde_json::to_value(node_schema(role)).unwrap();
        let actual = serde_json::to_value(document).unwrap();
        assert_eq!(
            field_paths(&actual),
            field_paths(&expected),
            "{}",
            role.name()
        );
        let config: ProcessConfig = toml::from_str(&text).unwrap();
        config.validate().unwrap();
    }
    let text = expand_optional_examples(include_str!("../config/console.toml"));
    let document: toml::Table = toml::from_str(&text).unwrap();
    assert_eq!(
        field_paths(&serde_json::to_value(document).unwrap()),
        field_paths(&serde_json::to_value(console_schema()).unwrap())
    );
    let config: ProcessConfig = toml::from_str(&text).unwrap();
    config.validate_for_startup().unwrap();
}

#[test]
fn node_templates_preserve_defaults_and_role_boundaries() {
    for role in NODE_ROLES {
        let document: toml::Table = toml::from_str(node_template(role)).unwrap();
        let config: Config = toml::from_str(node_template(role)).unwrap();
        let defaults = Config::default();
        let serialized_defaults = toml::Value::try_from(&defaults).unwrap();
        for section in ["memory", "protocol", "logging", "tracing"] {
            assert_eq!(
                document.get(section),
                serialized_defaults.get(section),
                "{}: {section}",
                role.name()
            );
        }
        assert_eq!(config.role, role);
        assert_eq!(config.runtime.capacity, defaults.runtime.capacity);
        assert_eq!(config.runtime.retention, defaults.runtime.retention);
        assert_eq!(
            config.runtime.archive_interval_ms,
            defaults.runtime.archive_interval_ms
        );
        assert!(config.runtime.objects.is_none());
        assert!(config.runtime.topic_scaling.is_none());
        assert_eq!(document.contains_key("gateway"), role.serves_gateway());
        assert_eq!(document.contains_key("storage"), role.stores_data());
        assert!(document.contains_key("authorization"));
        assert!(!document.contains_key("listen"));
        assert_eq!(
            config.runtime.bootstrap_admin_token_file.is_some(),
            role.stores_metadata()
        );
        config.runtime.validate(&config).unwrap();
    }
}

#[test]
fn separated_templates_share_one_valid_topology() {
    let reference: Config = toml::from_str(node_template(Role::Metadata)).unwrap();
    let mut listeners = BTreeSet::new();
    let mut directories = BTreeSet::new();
    let mut nodes = BTreeSet::new();
    for role in NODE_ROLES
        .into_iter()
        .filter(|role| *role != Role::Combined)
    {
        let config: Config = toml::from_str(node_template(role)).unwrap();
        assert_eq!(config.cluster_id, reference.cluster_id);
        assert_eq!(config.replication_factor, 1);
        assert_eq!(
            config.runtime.metadata_voters,
            reference.runtime.metadata_voters
        );
        assert_eq!(
            serde_json::to_value(&config.runtime.peers).unwrap(),
            serde_json::to_value(&reference.runtime.peers).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&config.runtime.groups).unwrap(),
            serde_json::to_value(&reference.runtime.groups).unwrap()
        );
        let local = config.runtime.peer(config.node_id).unwrap();
        assert_eq!(local.role, role);
        assert_eq!(local.internal_address, config.runtime.internal_listen);
        assert!(nodes.insert(config.node_id));
        assert!(directories.insert(config.data_dir));
        assert!(listeners.insert(config.management_listen));
        assert!(listeners.insert(config.runtime.internal_listen));
        if role.serves_gateway() {
            assert!(listeners.insert(config.listen));
        }
    }
    assert_eq!(nodes, BTreeSet::from([11, 21, 31, 41]));
}

#[test]
fn console_template_preserves_optional_defaults() {
    let text = include_str!("../config/console.toml");
    let config: ConsoleConfig = toml::from_str(text).unwrap();
    let mut minimal: toml::Table = toml::from_str(text).unwrap();
    for section in ["logging", "tracing", "observability"] {
        minimal.remove(section);
    }
    let session = minimal.get_mut("session").unwrap().as_table_mut().unwrap();
    session.remove("previous_key_files");
    session.remove("max_age_seconds");
    let defaults: ConsoleConfig = toml::Value::Table(minimal).try_into().unwrap();
    assert_eq!(
        serde_json::to_value(config).unwrap(),
        serde_json::to_value(defaults).unwrap()
    );
}

#[test]
fn optional_examples_require_operator_supplied_capacity() {
    for role in [Role::Combined, Role::Storage, Role::Metadata] {
        let mut config: Config =
            toml::from_str(&expand_optional_examples(node_template(role))).unwrap();
        if config.runtime.objects.is_some() {
            assert!(config.runtime.validate(&config).is_err());
            config.memory.total_bytes = 201_326_592;
            config.memory.control_reserved_bytes = 33_554_432;
        }
        if config.runtime.topic_scaling.is_some() {
            assert!(config.runtime.validate(&config).is_err());
        }
        if let Some(policy) = &mut config.runtime.topic_scaling {
            assert_eq!(
                serde_json::to_value(&*policy).unwrap(),
                serde_json::to_value(TopicScalingPolicy::default()).unwrap()
            );
            // Synthetic positive capacities test validation, not deployment sizing.
            policy.capacity = WorkCapacity {
                publish_bytes_per_second: 1,
                publish_messages_per_second: 1,
                delivery_bytes_per_second: 1,
                deliveries_per_second: 1,
                read_requests_per_second: 1,
                ack_bytes_per_second: 1,
                acknowledgements_per_second: 1,
                ack_requests_per_second: 1,
            };
        }
        config.runtime.validate(&config).unwrap();
    }
}

#[tokio::test]
async fn process_and_cli_load_every_template_consistently() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut names: Vec<_> = NODE_ROLES.iter().map(|role| role.name()).collect();
    names.push("console");
    for name in names {
        let path = root.join(format!("config/{name}.toml"));
        let process = ProcessConfig::load(&path).await.unwrap();
        process.validate_for_startup().unwrap();
        match &process {
            ProcessConfig::Node(config) => assert_eq!(
                serde_json::to_value(&**config).unwrap(),
                serde_json::to_value(Config::load(&path).await.unwrap()).unwrap()
            ),
            ProcessConfig::Console(_) => assert!(Config::load(&path).await.is_err()),
        }
        for command in ["validate-config", "diagnose"] {
            let result = Command::new(env!("CARGO_BIN_EXE_tidemq"))
                .args([command, "--config"])
                .arg(&path)
                .output()
                .await
                .unwrap();
            assert!(
                result.status.success(),
                "{command} rejected {name}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            let output: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
            if command == "validate-config" {
                assert_eq!(output, serde_json::json!({"valid": true}));
            } else {
                assert_eq!(
                    output.pointer("/configuration/role"),
                    serde_json::to_value(&process).unwrap().get("role")
                );
            }
        }
    }
}

#[tokio::test]
async fn startup_commands_reject_invalid_runtime_and_unknown_fields() {
    let root =
        std::env::temp_dir().join(format!("tidemq-invalid-config-{}", Id::new_v7().unwrap()));
    tokio::fs::create_dir(&root).await.unwrap();
    let path = root.join("invalid.toml");
    for text in [
        node_template(Role::Combined).replace("metadata_voters = [1]", "metadata_voters = [9]"),
        format!("unknown_setting = true\n{}", node_template(Role::Combined)),
    ] {
        tokio::fs::write(&path, text).await.unwrap();
        for command in ["validate-config", "diagnose", "serve"] {
            let result = Command::new(env!("CARGO_BIN_EXE_tidemq"))
                .args([command, "--config"])
                .arg(&path)
                .current_dir(&root)
                .output()
                .await
                .unwrap();
            assert!(
                !result.status.success(),
                "{command} accepted invalid configuration"
            );
        }
    }
    assert!(
        !root.join("data").exists(),
        "Validation must precede storage creation"
    );
    tokio::fs::remove_dir_all(root).await.unwrap();
}
