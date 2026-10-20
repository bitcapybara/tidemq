# TideMQ Rust SDK

Native authenticated QUIC client facilities in the TideMQ repository. The library depends on the shared protocol and transport crates, without server, OpenRaft, WAL, S3, allocator or subscriber initialization.

```toml
[dependencies]
tidemq-sdk = { path = "sdk/rust", features = ["metrics"] }
```

Create a `Client` with `ClientConfig`, a trusted CA PEM and an access token. Use `client.admin()` for resource creation and lookup. `Producer::open` explicitly registers a caller-chosen producer ID; concurrent `produce` calls enqueue in sequence order and wait for responses independently. `Consumer::claim` provides bounded delivery queues, manual durable ACKs and explicit assignment takeover.

```rust,no_run
use tidemq_sdk::{Client, ClientConfig, Id, Producer, Bytes, public::Message};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let ca = tokio::fs::read("certs/ca.pem").await?;
let client = Client::connect(ClientConfig::new("localhost:7443"), ca.into(),
    std::env::var("TIDEMQ_TOKEN")?).await?;
let topic = client.admin().create_topic("events", 1).await?;
let producer = Producer::open(client.clone(), Id::from_slice(&topic.topic_id)?, 0,
    Id::random()?).await?;
let receipt = producer.produce(vec![Message {
    key: Bytes::from_static(b"key"), body: Bytes::from_static(b"message"),
}]).await?;
println!("Committed offset {}", receipt.first_offset);
client.close().await;
# Ok(()) }
```

Every network operation uses `ClientConfig::request_timeout_ms` by default, including admission queues, connection establishment, retries and backoff. Callers no longer create an `Instant` for every request. Set an explicit shared total budget and a Tokio cancellation parent with `RequestContext`:

```rust,no_run
use std::time::Duration;
use tidemq_sdk::{Client, RequestContext};

# async fn example(client: &Client) -> tidemq_sdk::Result<()> {
let context = RequestContext::with_timeout(Duration::from_secs(5))?;
let cancellation = context.cancellation_token();
// Pass a clone of cancellation to the task that owns shutdown or user cancellation.
context.scope(async {
    client.admin().topic("events").await?;
    client.admin().subscription(tidemq_sdk::Id::random()?, "workers").await?;
    Ok(())
}).await?;
cancellation.cancel();
# Ok(()) }
```

`RequestContext::new(absolute_deadline, cancellation_token)` accepts an existing `tokio_util::sync::CancellationToken`; the SDK also re-exports this type. A token signals cancellation and is not a timer: an inline Tokio timeout enforces the absolute deadline without spawning timer tasks. Each operation uses the earlier of the configured client timeout and the context deadline. Reusing a context across calls preserves its original total budget; nested SDK calls and retries never refresh an operation deadline. Nested scopes use the earliest deadline and preserve every parent cancellation; nesting is bounded to 16 scopes. `context.child()` isolates cancellation from its parent and siblings. Scope carries this policy for SDK calls and does not cancel unrelated host computation between calls. It follows the future being polled and does not implicitly propagate to spawned Tokio tasks; explicitly clone and scope the context inside each spawned task.

Cancellation drops the current operation future and releases transient admission permits. It does not roll back a server operation or reset a shared QUIC lane. `Cancelled` before submission has no submitted request; `RequestUnknown` means interruption after potential submission. Registration interruption preserves its epoch through `RegistrationUnknown`, and an admitted producer batch remains available as `ProduceUnknown` with the original identity in `pending_batches()`. Those unresolved batches intentionally retain their bounded permits until resolved or dropped. `client.close().await` cancels in-flight operations, rejects new ones, and performs cleanup within the configured timeout. It does not use an already-cancelled request context to skip connection cleanup. Local pending-batch inspection remains available after close.

`ProduceUnknown` and a dropped produce future retain the original batch in `pending_batches()`. Retry the earliest batch with `retry`, preserving producer, epoch, sequence and digest. New sequences remain blocked until unresolved batches recover. Expired sessions never silently acquire a new identity. `RegistrationUnknown` exposes the original producer ID and registration epoch for `open_at`. There is no persistent SDK outbox or process-restart recovery promise.

Explicitly rejected, retryable `Unavailable` and `Fenced` responses retry within the original deadline and attempt budget. Producer transport loss also retries the original batch within that budget. Each attempt has a new request ID and preserves its operation and business identity. An unresolved outcome after exhaustion remains available through `pending_batches()`; permanent errors never trigger a new registration.

A `Consumer` clone shares one assignment. Default ordering is parallel; `KeySerial` releases a key after durable ACK, while `fail` blocks it until explicit `retry_failed`. A receive barrier proves continuous bounded SDK enqueue, independently of business ACK. Bulk retries below the cold boundary remain possible after hot delivery starts. Seek and ownership generation changes invalidate older deliveries. A stale token cannot ACK a newer attempt.

Each traffic class uses one independently authenticated connection under the same logical session. Four reusable data lanes are the initial default, with stable order-domain affinity. Producers share finite FIFO admission, at most 32 pending batches per producer and 256 across the client by default. Request deadlines include bounded reconnect attempts and backoff. Cancellation never resets a lane shared with unrelated requests.

While work is in flight, a native PING probe requires acknowledgement of a newly sent 1-RTT packet. Application response latency alone never triggers a reconnect. The initial probe interval and timeout are one second each, configurable through `ClientConfig::liveness_timeout`; total recovery also includes reconnect and authority recovery. PING/packet ACK proves network progress, never durable message completion. `client.transport_metrics()` exposes the fixed native collector, including liveness probes and failures, for the host registry.

The host owns Tokio, certificates, credentials, logging, exporter and allocator policy. The optional `metrics` feature registers fixed-series metrics into the host registry through `client.metrics().register(...)`. Logical byte accounting includes retained message objects and metadata; it does not claim to cap host-owned shared allocations, allocator overhead or RSS. Debug and spans omit payloads and secrets.

Use `scope_traceparent(valid_w3c_context, sdk_operation).await` to propagate a host trace through one finite operation. Invalid contexts are rejected before polling the operation. Context is task-scoped, never stored on a shared connection, and newly spawned host tasks scope their own context. No OpenTelemetry backend is installed.

The optional `opentelemetry` feature reads the current host tracing span through OpenTelemetry 0.33 and tracing-opentelemetry 0.34. The host supplies its subscriber and bridge layer, including the `tidemq_trace` target. SDK spans skip all arguments; an explicit `scope_traceparent` takes precedence. The feature installs no provider, exporter, subscriber, global propagator or HTTP listener. See [bounded service trace export](../../docs/operations/tracing.zh-CN.md) for the independent server-side policy.

Run from the repository root:

```bash
cargo test -p tidemq-sdk --all-features
cargo clippy -p tidemq-sdk --all-features --all-targets --no-deps
cargo run -p tidemq-sdk --example host_observability --features metrics
```

`produce` and `consume` examples use `TIDEMQ_ENDPOINT`, `TIDEMQ_CA_PEM`, `TIDEMQ_TOKEN`, and `TIDEMQ_TOPIC_ID`. Consumption also requires `TIDEMQ_SUBSCRIPTION_ID`; `TIDEMQ_ORDERING=key` selects key-serial scheduling. See [the SDK guide](../../docs/guide/rust-sdk.zh-CN.md) for lifecycle, errors, memory and takeover contracts.

The SDK tests run against a real UDP/TLS protocol peer. They do not replace the runtime RF1/RF3, failover, load-balancer, Linux or three-zone durability and performance suites.

The shared pending-byte budget reserves at least one quarter for consumption, including one maximum delivery and its metadata. Read credit is reserved before issuing the request. Retained producer payloads are compact copies, so small `Bytes` views do not pin arbitrarily large caller allocations. The final consumer clone releases its local claim without acknowledging or deleting the durable assignment.
