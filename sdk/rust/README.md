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
let topic = client.admin().create_topic("events", 1, client.deadline()?).await?;
let producer = Producer::open(client.clone(), Id::from_slice(&topic.topic_id)?, 0,
    Id::random()?, client.deadline()?).await?;
let receipt = producer.produce(vec![Message {
    key: Bytes::from_static(b"key"), body: Bytes::from_static(b"message"),
}], client.deadline()?).await?;
println!("Committed offset {}", receipt.first_offset);
client.close(client.deadline()?).await;
# Ok(()) }
```

`ProduceUnknown` and a dropped produce future retain the original batch in `pending_batches()`. Retry the earliest batch with `retry`, preserving producer, epoch, sequence and digest. New sequences remain blocked until unresolved batches recover. Expired sessions never silently acquire a new identity. `RegistrationUnknown` exposes the original producer ID and registration epoch for `open_at`. There is no persistent SDK outbox or process-restart recovery promise.

A `Consumer` clone shares one assignment. Default ordering is parallel; `KeySerial` releases a key after durable ACK, while `fail` blocks it until explicit `retry_failed`. A receive barrier proves continuous bounded SDK enqueue, independently of business ACK. Bulk retries below the cold boundary remain possible after hot delivery starts. Seek and ownership generation changes invalidate older deliveries. A stale token cannot ACK a newer attempt.

Each traffic class uses one independently authenticated connection under the same logical session. Four reusable data lanes are the initial default, with stable order-domain affinity. Producers share finite FIFO admission, at most 32 pending batches per producer and 256 across the client by default. Request deadlines include bounded reconnect attempts and backoff. Cancellation never resets a lane shared with unrelated requests.

The host owns Tokio, certificates, credentials, logging, exporter and allocator policy. The optional `metrics` feature registers fixed-series metrics into the host registry through `client.metrics().register(...)`. Logical byte accounting includes retained message objects and metadata; it does not claim to cap host-owned shared allocations, allocator overhead or RSS. Debug and spans omit payloads and secrets.

Use `scope_traceparent(valid_w3c_context, sdk_operation).await` to propagate a host trace through one finite operation. Invalid contexts are rejected before polling the operation. Context is task-scoped, never stored on a shared connection, and newly spawned host tasks scope their own context. No OpenTelemetry backend is installed.

Run from the repository root:

```bash
cargo test -p tidemq-sdk --all-features
cargo clippy -p tidemq-sdk --all-features --all-targets --no-deps
cargo run -p tidemq-sdk --example host_observability --features metrics
```

`produce` and `consume` examples use `TIDEMQ_ENDPOINT`, `TIDEMQ_CA_PEM`, `TIDEMQ_TOKEN`, and `TIDEMQ_TOPIC_ID`. Consumption also requires `TIDEMQ_SUBSCRIPTION_ID`; `TIDEMQ_ORDERING=key` selects key-serial scheduling. See [the SDK guide](../../docs/guide/rust-sdk.zh-CN.md) for lifecycle, errors, memory and takeover contracts.

The SDK tests run against a real UDP/TLS protocol peer. They do not replace the runtime RF1/RF3, failover, load-balancer, Linux or three-zone durability and performance suites.
