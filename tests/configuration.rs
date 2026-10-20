use std::path::{Path, PathBuf};

use tidemq::config::Config;
use tidemq_protocol::Id;
use tokio::process::Command;

async fn generate(mode: &str, layout: &str, output: &Path) {
    let result = Command::new("bash")
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("deploy/local/generate.sh"))
        .arg(mode)
        .arg(output)
        .arg(layout)
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "Generator failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
}

async fn validate_nodes(directory: &Path, nodes: &[u64], layout: &str) {
    for node in nodes {
        let path = directory.join(format!("node-{node}.toml"));
        let config = Config::load(&path).await.unwrap();
        config.runtime.validate(&config).unwrap();
        assert_eq!(config.node_id, *node);
        let text = tokio::fs::read_to_string(&path).await.unwrap();
        let document: toml::Table = toml::from_str(&text).unwrap();
        assert_eq!(document.contains_key("authorization"), layout == "roles");
        assert_eq!(
            document.contains_key("gateway"),
            layout == "roles" && config.role.serves_gateway()
        );
        assert_eq!(
            document.contains_key("storage"),
            layout == "roles" && config.role.stores_data()
        );
        assert_eq!(document.contains_key("listen"), layout == "legacy");
    }
}

#[tokio::test]
async fn generator_validates_each_role_in_both_supported_layouts() {
    let root = std::env::temp_dir().join(format!("tidemq-config-layout-{}", Id::random().unwrap()));
    tokio::fs::create_dir(&root).await.unwrap();
    for layout in ["roles", "legacy"] {
        for (mode, nodes) in [
            ("rf1", vec![1]),
            ("rf3-combined", vec![1, 2, 3]),
            ("rf3", vec![11, 12, 13, 21, 22, 23, 31, 32, 41, 42]),
        ] {
            let directory = root.join(format!("{mode}-{layout}"));
            generate(mode, layout, &directory).await;
            validate_nodes(&directory, &nodes, layout).await;
        }
    }
    let rejected = root.join("rejected");
    let result = Command::new("bash")
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("deploy/local/generate.sh"))
        .arg("rf1")
        .arg(&rejected)
        .arg("unknown")
        .output()
        .await
        .unwrap();
    assert!(!result.status.success());
    assert!(!rejected.exists());
    tokio::fs::remove_dir_all(root).await.unwrap();
}
