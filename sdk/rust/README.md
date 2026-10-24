# TideMQ Rust SDK

Use logical Topic names, optional message keys, and durable Stream or Queue subscriptions. The SDK follows shard routing and manages producer identities, membership, and acknowledgements. It depends on the protocol and transport crates, without linking server storage or installing an allocator, logger, or exporter.

```toml
[dependencies]
tidemq-sdk = { path = "sdk/rust" }
```

The Topic must already exist. Create subscriptions before publishing messages that they must protect.

```rust,no_run
use std::time::Duration;
use tidemq_sdk::{Client, Credentials, Message, TlsTrust};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::builder(
    "mq.example.com:7443",
    TlsTrust::ca_file("certs/ca.pem"),
    Credentials::token_file("secrets/app.token"),
)
.namespace("acme/app")
.connect_timeout(Duration::from_secs(5))
.request_timeout(Duration::from_secs(30))
.close_timeout(Duration::from_secs(5))
.build()
.await?;

let stream = client.new_stream_consumer("orders", "order-state").build().await?;
let queue = client.new_queue_consumer("orders", "billing-jobs").build().await?;
let producer = client.new_producer("orders").build().await?;
let receipt = producer.send(Message::new("created").key("order-42")).await?;
println!("Committed message {}", receipt.message_id());

let message = stream.receive().await?;
process(message.payload()).await?;
stream.ack_cumulative(&message).await?;

let message = queue.receive().await?;
process(message.payload()).await?;
queue.ack(&message).await?;

stream.close().await?;
queue.close().await?;
producer.close().await?;
let report = client.close().await?;
println!("Unresolved sends: {}", report.unresolved_sends());
# Ok(()) }
# async fn process(_: &[u8]) -> std::io::Result<()> { Ok(()) }
```

Admin creates a logical Topic with `admin.create_topic(name).await?`, without selecting a fixed partition count. `Producer`, `StreamConsumer`, and `QueueConsumer` are distinct business types.

Required inputs belong in the builder constructor; optional settings use chained setters. Constructors and setters perform no I/O. Successful `build().await` returns a connected client or a usable logical producer/consumer. Without a namespace, supply the full `tenant/namespace/topic` name.

TLS trust contains the server CA certificate; a CA private key is never a client input. Credentials support token files, environment variables, and explicit tokens. The cluster stores credential verification records in Metadata Raft. Applications receive their tokens through private files or deployment Secrets. Debug output omits secrets and message content.

Administrators configure tenant or namespace budgets with `ResourceLimits::builder()` and `admin.set_tenant_limits(id, limits)` or `set_namespace_limits(id, limits)`. A builder changes only configured fields; namespace fields initially inherit the tenant's current limits. `ResourceLimits::default()` explicitly restores the standard limits. Namespaces share their tenant's total budget. `PolicyUpdate::Applied` means the persistent policy and required data admission fences are installed; `Superseded` means a newer update replaced it before completion. Interrupted updates retain the original `PendingAdmin`. Lowering a budget preserves protected data and allows ACK and recovery to release existing occupancy.

`admin.tenants()`, `namespaces(tenant)`, `principals(tenant)`, `platform_principals()` and `credentials(principal)` return typed listings. Use `page_size(1..=32)` and `next_page().await?`; None ends that enumeration. Pages contain identities and effective limits, without token values or verification digests. A concurrent identity change returns `CatalogChanged`; start a new listing. Routine message, membership and quota activity does not invalidate identity pagination.

`delete_namespace(id)` requires explicitly deleted topics, disabled child principals, and revoked or expired child credentials. `delete_tenant(id)` additionally requires explicitly deleted namespaces. These operations retain identity tombstones and original results; recreating a name allocates a new identity. Tenant and namespace listings expose `IdentityStatus::{Active, Disabled, Deleted}`. Use `set_tenant_enabled` or `set_namespace_enabled` for reversible suspension; deleted identities cannot be enabled again.

Connection limits count physical connections across gateways and traffic classes. Reusing a Client shares its connection pool; distinct Client instances consume separate capacity. Connection admission and cold-read batches use finite Metadata Raft leases, so adding a node or changing a data leader does not duplicate their allowance. `set_principal_enabled(id, false)` disables a principal's credentials for new admission.

## Sending and receiving

`send` accepts `Into<Message>`: use `producer.send("hello").await?` for a plain message, or `Message` for keys and properties. Success means durable data Raft commitment. One logical producer concurrently sends to multiple shards; use a bounded task pool or FuturesOrdered/FuturesUnordered to drive concurrent calls. Awaiting each call before creating the next call deliberately serializes the application.

Stream preserves effective delivery order for the same key. Only Stream provides cumulative ACK, which covers the application's shared delivery prefix, including messages received through other clones. Coordinate concurrent business work before advancing that prefix. Queue distributes each message to one member and provides individual `ack` and `nack(message, delay)`. NACK durably schedules redelivery; it does not acknowledge successful processing. Delivery is at least once, so business effects should be idempotent.

`receive().await` waits for a message. `try_receive()` only checks the local queue; None does not prove that the server has no backlog. Each subscription has independent progress. Zero members preserve the subscription and its backlog.

Client, producer, StreamConsumer and QueueConsumer clones share their logical instance, budgets and closing state. Consumer clones compete for one receive queue and may acknowledge each other's valid deliveries. Use another `new_*().build().await` for an independent instance. Closing any clone closes that entire logical instance.

## Deadlines and recovery

Network operations use the configured finite request timeout. Each new physical connection resolves the endpoint again and uses `connect_timeout` for DNS, candidate attempts, authentication and stream setup, within the remaining request deadline. Healthy connections remain reusable. Candidate addresses share that connect budget; reconnects retain the logical session and resource bounds. Use `RequestContext::with_timeout(Duration)` and `context.scope(operation)` for a shared total deadline or cancellation token. Queueing, retries and subrequests use the same remaining budget. A receive without an explicit context keeps waiting while its individual network requests remain finite.

Cancellation does not undo a committed operation. An admitted send or ACK is retained by its owned driver. Inspect `producer.pending_sends().await?` or `consumer.pending_acks().await?`, then call `pending.retry().await?` to query or continue the original operation. Snapshots include unresolved work and completed results that the application has not claimed. They do not drain results or acknowledge unprocessed messages. Recover before closing the original Client; there is no SDK persistent outbox.

Admin provides `stream_subscription`/`queue_subscription` and `delete_stream_subscription`/`delete_queue_subscription`. Deletion rejects online members, freezes the fixed subscription UUID, and removes protection in every data domain before succeeding. Interrupted deletions remain in `admin.pending_subscription_deletions()?`; retry those handles instead of deleting by name again. A later subscription with the same name has a different identity.

Platform administrators register pinned internal identities with `register_node(NodeRegistration { ... })`, list `cluster_nodes()`/`data_groups()`, and create a prepared replica group with `create_data_group(group_id, voters)`. Registration updates running peers without restarting them. The deployment must start actual group replicas first; group creation verifies their initialization and membership and waits for directory convergence. `set_node_schedulable(id, false)` stops new placement; it does not complete node drain. These operator actions share `pending_operations()` recovery. Ordinary producers and consumers use logical Topics.

Resource and credential creation, credential changes, and Topic split/merge retain their original operation in an owned driver. An unknown result carries `Error::Administration { pending, .. }`. Use `pending.retry().await?` or inspect `admin.pending_operations()?`; each retry returns an `AdminReceipt` for that original operation. The shared recovery reserve bounds retained administration work to 16 entries and 128 KiB per entry, and may reject earlier when other logical resources occupy that reserve. A lost one-time credential secret resolves to `CredentialSecretUnavailable` with the committed credential ID; revoke it before issuing another credential.

All close methods return a CloseReport or a CloseError containing a report. Inspect local cleanup, member departures, unresolved reads/sends/ACKs/deletions/administration and unclaimed results. Consumer close fences its member and retires accepted or uncertain Read results; failed retirement is reported through `unresolved_reads()`. Close never performs business ACKs or deletes a durable subscription. Host-held messages and recovery handles retain their shared memory charges until their final release.

## Memory and observability

Client and logical instance limits bound retained messages and bytes, requests, results, connections and tasks. A consumer byte window must hold one maximum-sized message plus 512 bytes of delivery metadata (262656 bytes with the current SDK message limit). Read reservations keep enough room for that message; when retained payloads consume the remaining credit, reads wait for their release. Consumer prefetch limits cover reserved reads, the local queue and application-held payloads. ACK releases logical delivery credit; retained payload clones keep their memory charge. Transport, control and recovery reserves have separate limits. These logical budgets are not a process RSS limit.

Pending sends retain a bounded share of request slots and bytes, leaving receiving and control capacity available. `pending_requests` requires at least three entries. A message or batch that cannot fit its share is rejected before admission. Claiming a completed send releases its request slots; retained recovery handles continue to account for their metadata memory.

The host owns Tokio, logging, metrics, TLS materials and allocator policy. `metrics` registers fixed series through `client.metrics().register(...)`. `opentelemetry` uses the host's provider and tracing bridge. `scope_traceparent` propagates a validated explicit trace context. See the [SDK guide](../../docs/guide/rust-sdk.zh-CN.md) and [trace export contract](../../docs/operations/tracing.zh-CN.md).

The `produce` and `consume` examples use TIDEMQ_ENDPOINT, TIDEMQ_CA_PEM, TIDEMQ_TOKEN and TIDEMQ_TOPIC. Consumption also requires TIDEMQ_SUBSCRIPTION; TIDEMQ_MESSAGE_COUNT defaults to ten.

```bash
cargo clippy -p tidemq-sdk --all-features --all-targets --no-deps --locked -- -D warnings
cargo test -p tidemq-sdk --all-features --doc --locked
cargo run -p tidemq-sdk --example host_observability --features metrics
```

Functional and fault integration cases live under the repository's tests directory. Functional validation does not establish throughput or calibrated capacity.

The SDK IPv4-to-IPv6 reconnection test requires a hostname resolving to both loopback families.
It uses `localhost` by default. Containers can set `TIDEMQ_TEST_DUAL_STACK_HOST=sdk-dualstack.test`
and add `--add-host sdk-dualstack.test:127.0.0.1 --add-host sdk-dualstack.test:::1` to their Docker invocation.
The test checks this prerequisite before starting network operations.

The consumer types enforce their acknowledgement contracts at compile time:

```compile_fail
use tidemq_sdk::{QueueConsumer, ReceivedMessage};
async fn invalid(queue: &QueueConsumer, message: &ReceivedMessage) {
    queue.ack_cumulative(message).await;
}
```

```compile_fail
use tidemq_sdk::{StreamConsumer, ReceivedMessage};
async fn invalid(stream: &StreamConsumer, message: &ReceivedMessage) {
    stream.ack(message).await;
}
```
