use super::*;
use tidemq_sdk::{IdentityStatus, Permission};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_parent_deletion_keeps_tombstones_and_isolates_recreated_names_after_restart() {
    let mut fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let admin = client.admin();
    let tenant = admin.create_tenant("acme").await.unwrap();
    let namespace = admin.create_namespace(tenant, "app").await.unwrap();
    let original = admin.create_topic("acme/app/events").await.unwrap();
    let principal = admin
        .new_principal(tenant, "writer", [Permission::Produce])
        .namespace(namespace)
        .build()
        .await
        .unwrap();
    let token = admin
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    let application = token_client(&fixture, token.expose_secret()).await;
    assert!(
        application
            .admin()
            .topic_info("acme/app/events")
            .await
            .is_ok()
    );
    assert!(admin.delete_namespace(namespace).await.is_err());
    assert!(admin.delete_tenant(tenant).await.is_err());
    admin.set_namespace_enabled(namespace, false).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(
        application
            .admin()
            .topic_info("acme/app/events")
            .await
            .is_err()
    );
    admin.set_namespace_enabled(namespace, true).await.unwrap();
    admin.delete_topic("acme/app/events").await.unwrap();
    assert!(admin.delete_namespace(namespace).await.is_err());
    admin.set_principal_enabled(principal, false).await.unwrap();
    assert!(admin.delete_namespace(namespace).await.is_err());
    admin.revoke_token(token.id()).await.unwrap();
    admin.delete_namespace(namespace).await.unwrap();
    assert!(admin.set_namespace_enabled(namespace, true).await.is_err());
    assert!(admin.set_principal_enabled(principal, true).await.is_err());
    assert!(
        admin
            .issue_token(principal, Duration::from_secs(300))
            .await
            .is_err()
    );
    admin.delete_tenant(tenant).await.unwrap();
    assert!(admin.set_tenant_enabled(tenant, true).await.is_err());
    let replacement_tenant = admin.create_tenant("acme").await.unwrap();
    let replacement_namespace = admin
        .create_namespace(replacement_tenant, "app")
        .await
        .unwrap();
    let replacement = admin.create_topic("acme/app/events").await.unwrap();
    assert_ne!(replacement_tenant, tenant);
    assert_ne!(replacement_namespace, namespace);
    assert_ne!(replacement.id(), original.id());
    admin.delete_namespace(namespace).await.unwrap();
    admin.delete_tenant(tenant).await.unwrap();
    assert_eq!(
        admin.topic_info("acme/app/events").await.unwrap().id(),
        replacement.id()
    );
    application.close().await.unwrap();
    client.close().await.unwrap();
    fixture.stop(1).await;
    fixture.restart(1).await;
    let client = fixture.client(1).await;
    let admin = client.admin();
    let tenants = admin.tenants().next_page().await.unwrap().unwrap();
    assert_eq!(
        tenants.iter().find(|row| row.id == tenant).unwrap().status,
        IdentityStatus::Deleted
    );
    assert_eq!(
        tenants
            .iter()
            .find(|row| row.id == replacement_tenant)
            .unwrap()
            .status,
        IdentityStatus::Active
    );
    let namespaces = admin.namespaces(tenant).next_page().await.unwrap().unwrap();
    assert_eq!(namespaces[0].id, namespace);
    assert_eq!(namespaces[0].status, IdentityStatus::Deleted);
    let consumer = client
        .new_queue_consumer("acme/app/events", "jobs")
        .build()
        .await
        .unwrap();
    let producer = client
        .new_producer("acme/app/events")
        .build()
        .await
        .unwrap();
    let sent = producer.send("replacement-only").await.unwrap();
    let delivered = consumer.receive().await.unwrap();
    assert_eq!(sent.message_id(), delivered.message_id());
    assert_eq!(delivered.payload(), b"replacement-only");
    consumer.ack(&delivered).await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
