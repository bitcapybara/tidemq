use super::*;

async fn dynamic_fixture() -> Fixture {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    let bootstrap = fixture.directory.join("bootstrap.token");
    tokio::fs::write(&bootstrap, TOKEN).await.unwrap();
    private_file(bootstrap.clone()).await;
    let config = fixture.configs.get_mut(&1).unwrap();
    config.runtime.dynamic_auth = true;
    config.runtime.bootstrap_admin_token_file = Some(bootstrap);
    config.runtime.groups.push(GroupConfig {
        group_id: 3,
        voters: vec![1],
        standby: Vec::new(),
    });
    fixture.start_all().await;
    fixture
}

async fn token_client(fixture: &Fixture, token: &str) -> Client {
    let config = fixture.configs.get(&1).unwrap();
    Client::with_target(
        ClientConfig::new("node-1.local:7443"),
        ClientTls {
            trust_roots: fixture.roots.clone(),
            identity: None,
            expected_nodes: Vec::new(),
        },
        PeerTarget {
            address: config.listen,
            server_name: "node-1.local".into(),
            local_role: "sdk".into(),
            remote_role: "combined".into(),
            cluster_id: None,
            node_id: None,
        },
        token.into(),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_topic_split_merge_and_credentials_survive_a_real_runtime_restart() {
    let mut fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    let namespace = client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let principal = client
        .admin()
        .new_principal(tenant, "writer", [tidemq_sdk::Permission::Produce])
        .namespace(namespace)
        .build()
        .await
        .unwrap();
    let token = client
        .admin()
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    let topic = client
        .admin()
        .create_logical_topic("acme/app/orders")
        .await
        .unwrap();
    assert_eq!(topic.shard_count(), 1);
    let split = client
        .admin()
        .split_topic_at("acme/app/orders", 1 << 63)
        .await
        .unwrap();
    assert_eq!(split.shard_count(), 2);
    assert_eq!(split.id(), topic.id());
    let merged = client
        .admin()
        .merge_topic_at("acme/app/orders", 1 << 63)
        .await
        .unwrap();
    assert_eq!(merged.shard_count(), 1);
    assert!(merged.revision() > split.revision());
    let app = token_client(&fixture, token.expose_secret()).await;
    assert_eq!(
        app.admin()
            .topic_info("acme/app/orders")
            .await
            .unwrap()
            .id(),
        topic.id()
    );
    assert!(app.admin().create_tenant("forbidden").await.is_err());
    client.close().await;
    app.close().await;
    fixture.restart(1).await;
    let app = token_client(&fixture, token.expose_secret()).await;
    assert_eq!(
        app.admin()
            .topic_info("acme/app/orders")
            .await
            .unwrap()
            .id(),
        topic.id()
    );
    let root = fixture.client(1).await;
    root.admin().revoke_token(token.id()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        app.admin().topic_info("acme/app/orders").await.is_err(),
        "Revocation must affect an already authenticated connection"
    );
    app.close().await;
    root.close().await;
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_namespace_authorization_is_checked_on_each_resource() {
    let fixture = dynamic_fixture().await;
    let root = fixture.client(1).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let other = root.admin().create_tenant("other").await.unwrap();
    root.admin().create_namespace(other, "app").await.unwrap();
    root.admin()
        .create_logical_topic("acme/app/orders")
        .await
        .unwrap();
    root.admin()
        .create_logical_topic("other/app/orders")
        .await
        .unwrap();
    let principal = root
        .admin()
        .new_principal(tenant, "reader", [tidemq_sdk::Permission::Consume])
        .namespace(namespace)
        .build()
        .await
        .unwrap();
    let token = root
        .admin()
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    let app = token_client(&fixture, token.expose_secret()).await;
    assert!(app.admin().topic_info("acme/app/orders").await.is_ok());
    assert!(app.admin().topic_info("other/app/orders").await.is_err());
    root.admin()
        .set_tenant_enabled(tenant, false)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(app.admin().topic_info("acme/app/orders").await.is_err());
    app.close().await;
    root.close().await;
    fixture.close().await;
}
