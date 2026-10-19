# TideMQ

English | [简体中文](README.zh-CN.md)

A cloud-native message queue built on object storage, designed for high throughput, low latency, and elastic scaling.

TideMQ targets data ingestion and stream processing. Its architecture separates message serving, replicated write-ahead logs (WALs), object storage, and control metadata so that compute and storage can scale independently.

> **Status:** Foundation implementation: a four-package workspace, authenticated bounded QUIC transport, durable WAL/OpenRaft storage, generated bounded protocol, configuration validation, structured logging, finite metrics, and allocator diagnostics. The messaging runtime, subscriptions, and object integration are under development. Performance and recovery targets remain unvalidated. See the [implementation status](docs/implementation-status.zh-CN.md).

## Design goals

- **High throughput and low latency:** Batch appends, group commit, pipelined replication, and bounded queues. Object storage uploads stay outside the producer acknowledgement path.
- **Object storage for long-term data:** Archive committed messages into immutable objects, with range reads, manifests, compaction, and safe reclamation.
- **Independent compute and storage scaling:** Separate Brokers from the WAL storage pool; deploy Gateways independently or together with Brokers.
- **Durability across three availability zones:** Use three WAL replicas per data group in one cloud region, with quorum commit before producer acknowledgement. Keep Raft control metadata separate from the data path.
- **Durable subscriptions:** Stable offsets within each partition, per-message acknowledgements, and at-least-once delivery. Support parallel completion or SDK-managed serial processing per key; external side effects require their own idempotency or fencing.
- **A stable client endpoint:** A fixed domain and layer-4 load balancer in front of protocol Gateways, with SDK reconnection, session recovery, bounded retries, and backpressure.
- **Backlog protection:** Combine minimum retention with protection for unacknowledged messages. At capacity limits, throttle or reject new writes while allowing consumption, acknowledgements, and reclamation to continue.

## Architecture

| Component | Responsibility |
| --- | --- |
| Client SDK | Batching, request identity, reconnects, retry deadlines, and consumer concurrency |
| Gateway | Protocol connections, authentication, cached routing, forwarding, and draining |
| Broker | Write coordination, subscription delivery, caching, and flow control |
| WAL storage pool | Independent replicated data groups that commit messages, offsets, deduplication results, writer fences, and publication intents |
| Metadata Raft | Topics, partition placement, ownership epochs, subscription configuration, object manifests, and lifecycle state |
| Subscription state | Sharded replicated acknowledgement progress, delivery generations, and recovery checkpoints, outside the global metadata group |
| Object storage | Immutable historical data objects and range reads |

### Write path

1. The SDK sends a batch through the stable endpoint; the Gateway forwards it to the owning Broker.
2. The Broker submits the batch with its ownership epoch to the partition's data group.
3. The data group validates fencing, producer identity, and sequence numbers, then assigns offsets and durably replicates the batch under Raft commit rules.
4. Only committed messages become visible to consumers, and the producer receives the committed offsets. A lost response is retried with the same batch identity to recover the original result.

### Background archival

1. Build immutable objects from committed messages, combining partitions only within the same data group in the initial design.
2. Register and upload each object, reconcile durable publication intent with conditional manifest updates, and record publication completion in the data group.
3. Reclaim local message data only after publication, durable recovery checkpoints, and replica catch-up conditions permit it. Object deletion also waits for references, pending publication tasks, and valid pins to clear.

The design keeps four completion points distinct: producer acknowledgement, durable consumer acknowledgement, object publication, and WAL reclamation. A timeout alone does not establish whether an operation committed.

## Deployment and recovery

The planned deployment modes are a combined single node for development, multiple nodes on a private network with a shared object backend, and separate compute, storage, and metadata pools across three cloud availability zones. Single-node mode has no node-level high availability and is not an automatic production fallback.

Candidate recovery targets are typically within 1 second for planned migration, P99 at most 5 seconds for a Broker or Gateway failure, and P99 at most 10 seconds for a single-zone failure. These are design and benchmark targets, not an SLA. Measurements include failure detection, leader election, fencing, state recovery, routing, and SDK reconnection. Gateway failure can break existing connections; the SDK must reconnect and restore sessions.

## Getting started

The repository pins Rust 1.96.0 with rustfmt and Clippy. Validate the local configuration and inspect the current process:

```sh
cargo run --locked -- validate-config --config config/local.toml
cargo run --locked -- diagnose --config config/local.toml
cargo run --locked -- allocator-probe
```

These commands do not start a message queue. Diagnostics describe the current short-lived process, not another running node. The Rust SDK lives in `sdk/rust`; its current API covers protocol identities and resource configuration. Network producer and consumer APIs follow in T06.

For local validation:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --tests --no-deps
cargo test --workspace --locked
```

## Documentation and implementation roadmap

The [complete architecture draft in Chinese](docs/architecture.zh-CN.md) preserves all nine pages of the source PDF, including the safety invariants, failure cases, performance-reference limitations, official references, and review checklist. The source draft is dated 2026-10-19. Its original undecided-language statements are preserved; this repository selects Rust.

1. Model the protocol and deterministic failure scenarios.
2. Implement a minimal local message path.
3. Add three-node replication, producer deduplication, durable acknowledgements, and recovery.
4. Implement object packing, publication, reconciliation, and garbage collection.
5. Add Gateways and controlled scaling.
6. Benchmark and validate failures across three availability zones with matched durability and delivery semantics.

The approved baseline selects OpenRaft, s2n-quic, Protobuf, an in-repository Rust SDK, aws-sdk-s3, and RustFS for local object integration. The binary owns allocator selection; mimalloc, jemalloc, and System remain under evaluation. See the [documentation index](docs/README.zh-CN.md), [current design](docs/design/architecture.zh-CN.md), and [requirement coverage](docs/requirements-coverage.zh-CN.md). Workload values and production deployment budgets still require validation. AutoMQ figures in the historical draft are external references, not TideMQ results.

## License

TideMQ is licensed under the [MIT License](LICENSE).
