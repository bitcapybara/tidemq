use super::*;
use std::collections::BTreeSet;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_sdk_authorization_protects_real_topic_operations_and_preserves_normal_consumption()
{
    use tidemq_sdk::{Permission, ResourceScope};
    let fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let admin = client.admin();
    let tenant = admin.create_tenant("acme").await.unwrap();
    let namespace = admin.create_namespace(tenant, "app").await.unwrap();
    let topic = "acme/app/scoped";
    admin.create_topic(topic).await.unwrap();
    admin.create_topic("acme/app/private").await.unwrap();
    assert!(matches!(
        admin.topic_info("acme/app/missing").await,
        Err(tidemq_sdk::Error::Remote {
            code: tidemq_protocol::common::ErrorCode::NotFound,
            ..
        })
    ));
    let topic_id = admin.topic_info(topic).await.unwrap().id();
    let scope = ResourceScope::Topic {
        tenant,
        namespace,
        topic: topic_id,
    };
    let principal = admin
        .new_principal(tenant, "topic-user", [])
        .build()
        .await
        .unwrap();
    let token = admin
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    let application = token_client(&fixture, token.expose_secret()).await;
    let own = application.current_identity().await.unwrap();
    assert_eq!(own.principal.id, principal);
    assert!(own.principal.permissions.is_empty());
    let other = admin.create_tenant("other").await.unwrap();
    let absent = Id::new_v7()
        .unwrap()
        .to_string()
        .parse::<tidemq_sdk::TenantId>()
        .unwrap();
    for tenant in [other, absent] {
        assert!(matches!(
            application
                .effective_permissions(ResourceScope::Tenant(tenant))
                .await,
            Err(tidemq_sdk::Error::Remote {
                code: tidemq_protocol::common::ErrorCode::Forbidden,
                ..
            })
        ));
    }
    assert!(matches!(
        application.admin().topic_info("acme/app/missing").await,
        Err(tidemq_sdk::Error::Remote {
            code: tidemq_protocol::common::ErrorCode::Forbidden,
            ..
        })
    ));
    assert!(application.new_producer(topic).build().await.is_err());
    admin
        .grant_permission(principal, scope, Permission::Produce)
        .await
        .unwrap();
    admin
        .grant_permission(principal, scope, Permission::Consume)
        .await
        .unwrap();
    let own = application.effective_permissions(scope).await.unwrap();
    assert!(
        own.principal
            .effective_permissions
            .contains(&Permission::Inspect)
    );
    assert!(
        !own.principal
            .effective_permissions
            .contains(&Permission::InspectMessageContent)
    );
    assert_eq!(own.principal.effective_grants.len(), 1);
    assert!(
        application
            .admin()
            .principals(tenant)
            .next_page()
            .await
            .is_err()
    );
    assert!(
        application
            .admin()
            .resource_authorizations(scope)
            .next_page()
            .await
            .is_err()
    );
    assert!(
        application
            .new_producer("acme/app/private")
            .build()
            .await
            .is_err()
    );
    let consumer = application
        .new_stream_consumer(topic, "events")
        .build()
        .await
        .unwrap();
    let producer = application.new_producer(topic).build().await.unwrap();
    producer
        .send(
            tidemq_sdk::Message::new("normal-consumer-payload")
                .key("message-key")
                .property("source", "sdk"),
        )
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(20), consumer.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.payload(), b"normal-consumer-payload");
    assert_eq!(message.key(), Some(b"message-key".as_slice()));
    assert_eq!(message.properties().get("source").unwrap().as_ref(), b"sdk");
    let mut listing = admin.resource_authorizations(scope);
    let rows = listing.next_page().await.unwrap().unwrap();
    assert!(
        rows.iter()
            .any(|row| row.id == principal
                && row.effective_permissions.contains(&Permission::Produce))
    );
    assert!(listing.revision().is_some());
    admin
        .grant_permission(principal, scope, Permission::InspectMessageContent)
        .await
        .unwrap();
    assert!(
        admin
            .replace_resource_grants(principal, own.revision, &[])
            .await
            .is_err()
    );
    let current = application.effective_permissions(scope).await.unwrap();
    assert!(
        current
            .principal
            .effective_permissions
            .contains(&Permission::InspectMessageContent)
    );
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    application.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

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
