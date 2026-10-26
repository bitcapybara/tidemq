use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use tidemq_protocol::Id;
use tidemq_sdk::{
    Client, Credentials, ErrorKind, IdentityStatus, InitialPosition, Message, Permission,
    ResourceLimits, ResourceList, TlsTrust,
};

use super::{Args, Report};

pub async fn run(args: &Args, client: &Client, run: Id, report: &mut Report) -> Result<()> {
    report.mark("api_contract")?;
    let admin = client.admin();
    let tenant_name = format!("api-{run}");
    let tenant = admin.create_tenant(&tenant_name).await?;
    let namespace = admin.create_namespace(tenant, "contracts").await?;
    let topic = format!("{tenant_name}/contracts/messages");
    let info = admin.create_topic(&topic).await?;
    ensure!(
        admin.topic_info(&topic).await?.id() == info.id(),
        "Topic identity changed"
    );
    ensure!(
        contains(admin.tenants(), |entry| entry.id == tenant
            && entry.status == IdentityStatus::Active)
        .await?,
        "Tenant listing lost the created identity"
    );
    ensure!(
        contains(admin.namespaces(tenant), |entry| entry.id == namespace
            && entry.name == "contracts")
        .await?,
        "Namespace listing lost the created identity"
    );
    report.api_checks.push("identity_catalog_pagination");

    admin
        .set_namespace_limits(namespace, ResourceLimits::builder().topics(1).build()?)
        .await?;
    reject(
        admin
            .create_topic(&format!("{tenant_name}/contracts/over-quota"))
            .await,
        ErrorKind::ResourceExhausted,
    )?;
    ensure!(
        contains(admin.namespaces(tenant), |entry| entry.id == namespace
            && entry.limits.topics() == Some(1))
        .await?,
        "Namespace quota was not observable"
    );
    report.api_checks.push("namespace_topic_quota");

    authorization(args, client, tenant, namespace, &topic, report).await?;
    messaging(client, &topic, report).await?;
    let nodes = admin.cluster_nodes().await?;
    ensure!(!nodes.is_empty(), "Cluster node catalog is empty");
    ensure!(
        !admin.data_groups().await?.is_empty(),
        "Data group catalog is empty"
    );
    report.api_checks.push("cluster_catalog");

    admin.delete_topic(&topic).await?;
    reject(admin.topic_info(&topic).await, ErrorKind::NotFound)?;
    admin.delete_namespace(namespace).await?;
    admin.delete_tenant(tenant).await?;
    report.api_checks.push("resource_deletion");
    report.mark("api_contract_complete")
}

async fn contains<T>(listing: ResourceList<T>, predicate: impl Fn(&T) -> bool) -> Result<bool> {
    let mut listing = listing.page_size(1);
    let mut found = false;
    for _ in 0..64 {
        match listing.next_page().await? {
            Some(page) => {
                ensure!(page.len() == 1, "Catalog page exceeded its requested size");
                found |= page.iter().any(&predicate);
            }
            None => return Ok(found),
        }
    }
    bail!("Catalog pagination exceeded the bounded test size")
}

fn reject<T>(result: tidemq_sdk::Result<T>, expected: ErrorKind) -> Result<()> {
    match result {
        Err(error) => ensure!(
            error.kind() == expected,
            "Unexpected rejection kind: {:?}, expected {expected:?}",
            error.kind()
        ),
        Ok(_) => bail!("Operation unexpectedly succeeded; expected {expected:?}"),
    }
    Ok(())
}

async fn authorization(
    args: &Args,
    client: &Client,
    tenant: tidemq_sdk::TenantId,
    namespace: tidemq_sdk::NamespaceId,
    topic: &str,
    report: &mut Report,
) -> Result<()> {
    let admin = client.admin();
    let principal = admin
        .new_principal(tenant, "reader", [Permission::Inspect])
        .namespace(namespace)
        .build()
        .await?;
    let token = admin
        .issue_token(principal, Duration::from_secs(300))
        .await?;
    ensure!(
        contains(admin.principals(tenant), |entry| entry.id == principal
            && entry.namespace == Some(namespace))
        .await?,
        "Principal listing lost its scope"
    );
    ensure!(
        contains(admin.credentials(principal), |entry| entry.id == token.id()
            && entry.enabled)
        .await?,
        "Credential listing lost its active token"
    );
    let reader = Client::builder(
        args.endpoint.to_string(),
        TlsTrust::ca_file(&args.ca),
        Credentials::token(token.expose_secret()),
    )
    .request_timeout(Duration::from_secs(5))
    .build()
    .await?;
    let result = async {
        reader.admin().topic_info(topic).await?;
        reject(
            reader
                .admin()
                .create_topic(&format!("{topic}-forbidden"))
                .await,
            ErrorKind::PermissionDenied,
        )?;
        report.api_checks.push("scoped_authorization");
        admin.revoke_token(token.id()).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match reader.admin().topic_info(topic).await {
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::Unauthenticated | ErrorKind::PermissionDenied
                    ) =>
                {
                    break;
                }
                Err(error) => {
                    return Err(error).context("Revocation returned an unrelated failure");
                }
                Ok(_) => ensure!(
                    tokio::time::Instant::now() < deadline,
                    "Revoked token retained access"
                ),
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        report.api_checks.push("token_revocation");
        Ok(())
    }
    .await;
    let closed = reader.close().await?;
    ensure!(
        closed.local_cleanup_complete(),
        "Scoped client close did not complete"
    );
    result?;
    admin.set_principal_enabled(principal, false).await?;
    Ok(())
}

async fn messaging(client: &Client, topic: &str, report: &mut Report) -> Result<()> {
    let producer = client.new_producer(topic).build().await?;
    let queue = client
        .new_queue_consumer(topic, "queue-contract")
        .initial_position(InitialPosition::EarliestAvailable)
        .build()
        .await?;
    let stream = client
        .new_stream_consumer(topic, "stream-contract")
        .initial_position(InitialPosition::EarliestAvailable)
        .build()
        .await?;
    let sent = producer
        .send(Message::new("black-box-api-contract").key("ordered-key"))
        .await?;
    let delivery = queue.receive().await?;
    ensure!(
        delivery.message_id() == sent.message_id()
            && delivery.payload() == b"black-box-api-contract"
            && delivery.key() == Some(b"ordered-key".as_slice()),
        "Queue delivery differs from its committed message"
    );
    queue.nack(&delivery, Duration::from_millis(10)).await?;
    let redelivered = queue.receive().await?;
    ensure!(
        redelivered.message_id() == sent.message_id(),
        "NACK did not preserve the message identity"
    );
    queue.ack(&redelivered).await?;
    report.api_checks.push("queue_nack_redelivery_ack");

    let replayed = stream.receive().await?;
    ensure!(
        replayed.message_id() == sent.message_id() && replayed.payload() == delivery.payload(),
        "Stream and Queue did not independently receive the message"
    );
    stream.ack_cumulative(&replayed).await?;
    stream.close().await?;
    let resumed = client
        .new_stream_consumer(topic, "stream-contract")
        .build()
        .await?;
    let next = producer
        .send(Message::new("after-cumulative-ack").key("ordered-key"))
        .await?;
    ensure!(
        resumed.receive().await?.message_id() == next.message_id(),
        "Stream replayed an acknowledged message after reconnect"
    );
    resumed.close().await?;
    report.api_checks.push("stream_cumulative_ack_resume");
    queue.close().await?;
    ensure!(
        producer.pending_sends().await?.is_empty(),
        "API producer retained pending sends"
    );
    producer.close().await?;
    let admin = client.admin();
    let queue_id = admin
        .queue_subscription(topic, "queue-contract")
        .await?
        .id();
    let stream_id = admin
        .stream_subscription(topic, "stream-contract")
        .await?
        .id();
    ensure!(
        admin
            .delete_queue_subscription(topic, "queue-contract")
            .await?
            .subscription_id()
            == queue_id,
        "Queue deletion returned another identity"
    );
    ensure!(
        admin
            .delete_stream_subscription(topic, "stream-contract")
            .await?
            .subscription_id()
            == stream_id,
        "Stream deletion returned another identity"
    );
    report.api_checks.push("subscription_inspection_deletion");
    Ok(())
}
