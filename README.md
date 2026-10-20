# TideMQ

English | [简体中文](README.zh-CN.md)

A cloud-native message queue built on object storage, designed for high throughput, low latency, and elastic scaling.

TideMQ targets data ingestion and stream processing. Its architecture separates message serving, replicated write-ahead logs (WALs), object storage, and control metadata so that compute and storage can scale independently.

> **Status:** The local runtime and in-repository Rust SDK support durable production, per-message acknowledgements, authenticated QUIC, separate metadata/data Raft groups, RustFS/S3 archival, cold reads, retention, and safe object reclamation. Local RF3, separate roles, process failures, automatic takeover, and membership repair pass regression tests; 17 finite RF1 workloads have been measured. Small-message consumption with durable ACKs is about 60 messages/s. High-throughput, low-latency, three-zone, and recovery-percentile goals still require optimization and validation. See the [implementation status](docs/implementation-status.zh-CN.md) and [local performance report](docs/evidence/2026-10-20-local-performance.zh-CN.md).

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

The runtime provides combined RF1 and separate Gateway, Broker, storage, and metadata roles. The local generator supports RF1 and RF3 configurations. Production deployment across three cloud availability zones needs separate infrastructure and validation. Single-node mode has no node-level high availability and is not an automatic production fallback.

Candidate recovery targets are typically within 1 second for planned migration, P99 at most 5 seconds for a Broker or Gateway failure, and P99 at most 10 seconds for a single-zone failure. These are design and benchmark targets, not an SLA. Measurements include failure detection, leader election, fencing, state recovery, routing, and SDK reconnection. Gateway failure can break existing connections; the SDK must reconnect and restore sessions.

## Getting started

The repository pins Rust 1.99.1 with rustfmt, Clippy, and matching LLVM coverage tools. Create private local certificates, credentials, and a fresh RF1 configuration, then start the service:

```sh
cargo build --locked
deploy/local/generate.sh rf1 /tmp/tidemq-demo
target/debug/tidemq validate-config --config /tmp/tidemq-demo/node-1.toml
target/debug/tidemq serve --config /tmp/tidemq-demo/node-1.toml
```

The generator requires a new directory and OpenSSL. It creates short-lived development identities; retain the private token file locally. See [running TideMQ](docs/guide/running.zh-CN.md) and the [Rust SDK](sdk/rust/README.md) for topic creation, production, consumption, durable ACKs, and RF3 deployment. Management endpoints expose the running process on loopback; the separate `diagnose` command describes its own short-lived process.

Docker deployment assets are versioned under [deploy/docker](deploy/docker/README.zh-CN.md): the [builder Dockerfile](deploy/docker/Dockerfile.builder), [runtime Dockerfile](deploy/docker/Dockerfile), complete [RF1 Compose](deploy/docker/compose.rf1.yaml) and [RF3 Compose](deploy/docker/compose.rf3.yaml) configurations, and [deployment preparation script](deploy/docker/prepare.sh). Generate a private state directory before starting Compose; see the deployment guide for build, startup, validation, and cleanup commands.

Install cargo-nextest 0.9.148, then run the same target scope as CI:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --no-deps --locked --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen -- -D warnings
cargo nextest run --workspace --all-targets --locked --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen --profile ci
cargo test --workspace --doc --locked --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen
```

See the [testing guide](docs/testing-benchmarks.zh-CN.md) for the pinned coverage tools, JSON/LCOV/HTML reports, and coverage and performance baseline gates.

The SDK uses configured timeouts by default; `RequestContext` and `CancellationToken` share cancellation and a total deadline across calls. Large uniquely owned frame payloads can use bounded vectored sending. See the [SDK guide](docs/guide/rust-sdk.zh-CN.md) and [zero-copy format evaluation](docs/research/zero-copy-formats.zh-CN.md).

The [performance workflow](.github/workflows/performance.yml) compares real RF1 and three-process RF3 runs against the previous accepted source on the same runner, including each version's SDK. Throughput and durable-ACK tail regressions fail the check. The [performance guide](docs/performance.zh-CN.md) explains baselines, noise handling, reproduction, and the current GitHub plan's required-check limitation.

## Documentation and implementation roadmap

The [complete architecture draft in Chinese](docs/architecture.zh-CN.md) preserves all nine pages of the source PDF, including the safety invariants, failure cases, performance-reference limitations, official references, and review checklist. The source draft is dated 2026-10-19. Its original undecided-language statements are preserved; this repository selects Rust.

1. Model the protocol and deterministic failure scenarios.
2. Implement a minimal local message path.
3. Add three-node replication, producer deduplication, durable acknowledgements, and recovery.
4. Implement object packing, publication, reconciliation, and garbage collection.
5. Add Gateways and controlled scaling.
6. Benchmark and validate failures across three availability zones with matched durability and delivery semantics.

The implemented baseline uses OpenRaft, s2n-quic, Protobuf, an in-repository Rust SDK, aws-sdk-s3, and RustFS for local object integration. The service binary defaults to mimalloc through `allocator-mimalloc`; System and jemalloc are mutually exclusive build alternatives selected with `--no-default-features`. The SDK leaves allocator ownership to its host. See the [allocator configuration and diagnostics](docs/operations/memory-troubleshooting.zh-CN.md), [documentation index](docs/README.zh-CN.md), [current design](docs/design/architecture.zh-CN.md), [requirement coverage](docs/requirements-coverage.zh-CN.md), and [reproducible workload tools](docs/performance.zh-CN.md). Allocator performance, workload values, and production deployment budgets still require validation. AutoMQ figures in the historical draft are external references, not TideMQ results.

## License

TideMQ is licensed under the [MIT License](LICENSE).
