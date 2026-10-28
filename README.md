# TideMQ

English | [简体中文](README.zh-CN.md)

TideMQ is a Rust message queue for durable event ingestion and background processing. Combined provides local durable execution in one process; separated roles use replicated write-ahead logs. Both support optional S3-compatible storage for historical data.

Applications use logical Topics and durable Stream or Queue subscriptions through the in-repository Rust SDK. Choose `combined` for Gateway, Broker, Metadata, Storage and native HTTPS Console in one process, with direct internal calls and no Raft or internal QUIC listener. Separated Gateway, Broker, Metadata and Storage processes retain real Raft and QUIC with RF1 or RF3; an independent Console connects through the authenticated SDK.

**Status:** TideMQ is in development (`0.1.0`). APIs, configuration and on-disk formats may change. Combined and separated RF1 have no node-level high availability; RF3 requires a majority and never falls back to RF1. Functional and fault tests do not establish a production SLA, capacity rating or throughput claim. Review the [product limits](docs/guide/overview.zh-CN.md) before deployment.

## What it provides

- Durable publishing: success means durable commitment in the owning data group, with producer identity and original-result recovery after a lost response.
- Durable subscriptions: Stream supports ordered delivery for the same key and cumulative ACK; Queue shares work across members and supports individual ACK/NACK. Delivery is at least once; application side effects need idempotency.
- Storage separation: messages and ACK progress live in data groups; optional immutable S3 objects support archival and cold reads. Object upload is outside the producer acknowledgement path.
- Bounded resource use: admission budgets, backpressure, retention and tenant/namespace quotas protect existing ACK, recovery and control work.
- Authenticated access: public QUIC with TLS and scoped credentials, loopback management endpoints, and pinned internal mTLS identities between separated roles. Combined uses authenticated calls within the process.
- Operations: structured logs, OpenMetrics, optional OTLP, a browser Console, and SDK/CLI administration for controlled membership changes and drain.

## Architecture

```text
Combined:
  SDK -> QUIC/TLS -> local Gateway / Broker / Metadata / Storage
  Browser -> native HTTPS Console -> authenticated local calls

Separated:
  SDK -> QUIC/TLS -> Gateway -> internal QUIC -> Broker -> Storage Raft
                                               |               |
                                               +-> Metadata Raft
                                                               +-> S3
  Browser -> native HTTPS Console -> authenticated QUIC -> Gateway
```

| Role | Responsibility |
| --- | --- |
| `gateway` | Public connections, authentication, routing and forwarding |
| `broker` | Serving ownership, publishing coordination and subscription delivery |
| `storage` | Messages, offsets, producer results, delivery attempts and durable ACKs; local WAL in Combined, Raft in separated roles |
| `metadata` | Identities, authorization, Topic topology, ownership, manifests and control plans |
| `combined` | Local durable execution plus built-in HTTPS Console in one process; no replicas or peer network |
| `console` | Stateless HTTPS UI and authenticated SDK access; no Raft membership or MQ data volume |

The producer receipt, durable consumer ACK, object publication and local WAL reclamation are separate completion points. A timeout does not determine whether a mutation committed. See the [architecture guide](docs/guide/architecture.zh-CN.md).

## Run locally

Prerequisites: the repository's pinned Rust **1.99.1** toolchain, Bash, OpenSSL and a supported Node.js (22.22+ on the 22 series, 24, or 26+) for Console assets. Run from the repository root with a new output directory:

```sh
cargo build --locked -p tidemq -j2
bash deploy/local/generate.sh rf1 /tmp/tidemq-demo
target/debug/tidemq validate-config --config /tmp/tidemq-demo/node-1.toml
bash deploy/local/run.sh /tmp/tidemq-demo target/debug/tidemq
```

In another terminal:

```sh
curl --fail http://127.0.0.1:8410/readyz
```

Open `https://localhost:8443` after trusting the generated development CA in your browser. `run.sh` builds missing Console assets once; an incomplete existing asset directory fails with a rebuild instruction. The same process serves QUIC, Console HTTPS and loopback management.

Startup checks the Console manifest, required assets, session keys and HTTPS server identity before opening Combined storage. `/readyz` covers the complete process; a confirmed required dependency failure stops new work and starts a bounded drain. Restore the matching assets or identity and restart after failure. See [Console readiness and shutdown](docs/guide/deployment.md#readiness-and-shutdown).

The generated Gateway is `127.0.0.1:7410`; use `ca.pem` and the private `access.token` from that directory. Generated certificates expire after seven days. RF1 has no node-level high availability. Follow the [quickstart](docs/guide/quickstart.zh-CN.md) to create a tenant, namespace, Topic and subscription, publish a message, and confirm a durable ACK.

## Deploy and integrate

| Task | Documentation |
| --- | --- |
| Understand scope and limits | [Product overview](docs/guide/overview.zh-CN.md) · [Roles and durability](docs/guide/architecture.zh-CN.md) |
| Run RF1 or RF3 | [Deployment and configuration](docs/guide/deployment.md) · [Docker](deploy/docker/README.zh-CN.md) · [Kubernetes](deploy/kubernetes/README.zh-CN.md) |
| Configure each process | [Configuration and six role examples](docs/guide/deployment.md#configuration) |
| Use the Rust SDK | [English SDK reference](sdk/rust/README.md) · [Integration guide](docs/guide/rust-sdk.zh-CN.md) · [Publishing](docs/guide/production.zh-CN.md) · [Consumption and ACK](docs/guide/consumption.zh-CN.md) |
| Use the browser Console | [Console HTTPS and sessions](docs/guide/deployment.md#console-https-and-sessions) · [HTTPS and replica deployment](deploy/console/README.zh-CN.md) |
| Secure and operate | [Security](docs/operations/security.zh-CN.md) · [Metrics](docs/operations/metrics.zh-CN.md) · [Logs](docs/operations/logging.zh-CN.md) · [Tracing](docs/operations/tracing.zh-CN.md) |
| Recover and diagnose | [Recovery](docs/operations/recovery.zh-CN.md) · [Troubleshooting](docs/operations/troubleshooting.zh-CN.md) |
| Validate a change | [Testing and evidence boundaries](docs/testing.zh-CN.md) |

The [Chinese documentation index](docs/README.zh-CN.md) is the primary user manual. Design documents and dated lab evidence remain available as supporting material. Production deployment needs real failure domains, protected credentials, durable volumes and validation against the chosen environment. The development storage format has no automatic migration from older experimental layouts; retain the complete original state and matching binary before any upgrade.

## Build the documentation

The documentation site uses VitePress's default theme and its own Node package; it is independent of `console/web`.

```sh
npm --prefix docs ci
npm --prefix docs test
npm --prefix docs run docs:build
npm --prefix docs run docs:preview
```

Use Node.js 24 or newer. The preview serves the project at `/tidemq/`. The [site maintenance guide](docs/site.zh-CN.md) explains the pinned VitePress alpha toolchain, link checks and the manually gated GitHub Pages workflow. No site publication is implied by a successful local build.

## License

[MIT](LICENSE).
