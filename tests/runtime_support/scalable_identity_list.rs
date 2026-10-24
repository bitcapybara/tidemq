use super::*;
use std::collections::BTreeSet;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_listing_hides_secrets_enforces_real_parent_scope_and_detects_catalog_changes() {
    let fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let admin = client.admin();
    let tenant = admin.create_tenant("acme").await.unwrap();
    let other = admin.create_tenant("other").await.unwrap();
    let namespace = admin.create_namespace(tenant, "app").await.unwrap();
    let other_namespace = admin.create_namespace(tenant, "private").await.unwrap();
    admin.create_namespace(other, "app").await.unwrap();
    let topic = "acme/app/listing";
    admin.create_topic(topic).await.unwrap();
    let principal = admin
        .new_principal(
            tenant,
            "directory-admin",
            [tidemq_sdk::Permission::ManageCredentials],
        )
        .namespace(namespace)
        .build()
        .await
        .unwrap();
    let foreign = admin
        .new_principal(
            other,
            "directory-admin",
            [tidemq_sdk::Permission::ManageCredentials],
        )
        .build()
        .await
        .unwrap();
    let token = admin
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    let mut tenants = admin.tenants().page_size(1);
    let first = tenants.next_page().await.unwrap().unwrap();
    assert_eq!(first.len(), 1);
    let producer = client.new_producer(topic).build().await.unwrap();
    let second = tenants.next_page().await.unwrap().unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(
        BTreeSet::from([first[0].id, second[0].id]),
        BTreeSet::from([tenant, other])
    );
    assert!(tenants.next_page().await.unwrap().is_none());
    let mut changed = admin.tenants().page_size(1);
    changed.next_page().await.unwrap().unwrap();
    admin
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    assert!(matches!(
        changed.next_page().await,
        Err(tidemq_sdk::Error::CatalogChanged)
    ));
    let platform = admin
        .platform_principals()
        .next_page()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(platform.len(), 1);
    assert_eq!(platform[0].name, "platform-admin");
    let roots = admin
        .credentials(platform[0].id)
        .next_page()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(roots.len(), 1);
    let credentials = admin
        .credentials(principal)
        .next_page()
        .await
        .unwrap()
        .unwrap();
    let original = credentials
        .iter()
        .find(|credential| credential.id == token.id())
        .unwrap();
    assert_eq!(original.expires_at_unix_ms, token.expires_at_unix_ms());
    let diagnostic = format!("{roots:?} {credentials:?}");
    assert!(!diagnostic.contains(TOKEN));
    assert!(!diagnostic.contains(token.expose_secret()));
    let application = token_client(&fixture, token.expose_secret()).await;
    let visible = application
        .admin()
        .namespaces(tenant)
        .next_page()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, namespace);
    assert_ne!(visible[0].id, other_namespace);
    assert_eq!(visible[0].limits.backlog_bytes(), Some(1024 * 1024 * 1024));
    let visible = application
        .admin()
        .principals(tenant)
        .next_page()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, principal);
    assert!(
        application
            .admin()
            .namespaces(other)
            .next_page()
            .await
            .is_err()
    );
    assert!(
        application
            .admin()
            .principals(other)
            .next_page()
            .await
            .is_err()
    );
    assert!(
        application
            .admin()
            .credentials(foreign)
            .next_page()
            .await
            .is_err()
    );
    assert!(
        application
            .admin()
            .platform_principals()
            .next_page()
            .await
            .is_err()
    );
    assert!(
        application
            .admin()
            .tenants()
            .page_size(33)
            .next_page()
            .await
            .is_err()
    );
    application.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
