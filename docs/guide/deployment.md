# Deployment and configuration

[简体中文](running.zh-CN.md)

TideMQ offers two deployment shapes in the same binary. Choose the process role before provisioning storage and certificates.

| Deployment | Execution and durability | Browser access | Availability |
| --- | --- | --- | --- |
| `combined` | One process contains Gateway, Broker, Metadata and Storage. Internal calls and bounded queues feed durable local WAL commits; there is no Raft or internal peer listener. | Built-in native HTTPS Console, sharing the node certificate, process budget and loopback management endpoint | One host; no node-level failover |
| Separated RF1 | Independent Gateway, Broker, Metadata and Storage processes; real internal QUIC and single-voter Raft groups | Independent stateless HTTPS Console using the authenticated SDK | No node-level redundancy |
| Separated RF3 | Three Metadata voters, three Storage voters, two Brokers and two Gateways in the supplied topology | Independent stateless HTTPS Console | Each Raft group needs a majority across real failure domains |

A successful publish or durable ACK requires commitment in its owning data group. A lost response can leave an unknown result: retain the original producer or ACK identity and recover that result. Object upload, snapshot publication and physical reclamation are separate completion points. Local execution preserves authorization, tenant boundaries, generations, fencing, quotas, subscriptions and retention. Cluster membership changes and cross-node migration are unavailable in Combined.

## Run Combined

Prerequisites: Rust 1.99.1, Bash, OpenSSL, and Node.js 22.22+ on the 22 series, 24, or 26+ for building Console assets. Run from the repository root and choose a new private output directory:

```sh
cargo build --locked -p tidemq -j2
bash deploy/local/generate.sh rf1 /tmp/tidemq-demo
target/debug/tidemq validate-config --config /tmp/tidemq-demo/node-1.toml
bash deploy/local/run.sh /tmp/tidemq-demo target/debug/tidemq
```

`run.sh` builds missing Console assets into the generated `console/` directory. If that directory already exists but is incomplete, preserve or remove it and rebuild explicitly with `scripts/build-console.sh`; the script never replaces an existing release directory. Direct `tidemq serve` requires the assets to be built first.

| Endpoint | Address | Trust and credentials |
| --- | --- | --- |
| SDK QUIC | `127.0.0.1:7410` | Generated `ca.pem`, server name `node-1.local`, private `access.token` |
| Console HTTPS | `https://localhost:8443` | Trust the generated development CA in your browser; log in with your own token |
| Management HTTP | `http://127.0.0.1:8410` | Loopback only; `/healthz`, `/readyz`, `/metrics`, `/diagnostics` |

The generated node certificate covers `localhost`, `node-1.local` and `127.0.0.1`. It expires after seven days. The generator also creates a private bootstrap administrator token and an exactly 32-byte random Console session key. Never commit generated material or deploy its CA signing key to a node. Replace development identities with your organization's issued certificates for production.

Combined becomes ready only when MQ, required local storage and Console are ready together. Failure of a required listener or sustained loss of published required assets stops new admissions and drains the process under one shutdown deadline. Optional external observation failures remain partial or unknown.

Restart using the same configuration, identity, certificates and persistent directory. Do not rerun the generator, clear WAL files, or invent new operation identities to resolve an unknown result. Current local and Raft storage formats are distinct and have no automatic old-format migration or conversion.

## Configuration

The six complete templates are under [config/](https://github.com/bitcapybara/tidemq/tree/main/config): `combined.toml`, `gateway.toml`, `broker.toml`, `metadata.toml`, `storage.toml`, and `console.toml`. Comments distinguish code defaults, operational identities and disabled `#? ` examples. Relative paths resolve from the process working directory. Files are limited to 64 KiB; unknown and inactive settings are rejected.

| Role | Required role sections | Additional contract |
| --- | --- | --- |
| Combined | `[authorization]`, `[gateway]`, `[storage]`, `[console]`, `[console.session]` | Node/cluster identity, private data directory, one management listener, server certificate/key and resource limits |
| Gateway | `[authorization]`, `[gateway]` | Shared separated topology, internal TLS trust and peer pins |
| Storage | `[authorization]`, `[storage]` | Shared separated topology and private persistent volume |
| Broker / Metadata | `[authorization]` | Shared separated topology; Metadata owns its persistent control group |
| Independent Console | `[tls]`, `[cluster]`, `[session]` plus root listen/static directory/management fields | Fixed remote Gateway, MQ CA, server HTTPS identity and shared session keys; no MQ data volume |

Listener conflicts are checked within their transport protocol. QUIC uses UDP, so its port number may match an HTTPS or management TCP port. HTTPS and management TCP listeners must not overlap, including wildcard bindings.

Combined accepts `runtime.metadata_group` (default `1`) and `runtime.groups[].group_id` (default data group `2`, at most 16). IDs must be nonzero and unique across metadata and data groups. Ownership is derived from `node_id`; users never configure local voter membership. Capacity and topic-scaling policies keep their existing sections.

Combined rejects `replication_factor`, `runtime.internal_listen`, `runtime.peers`, `runtime.metadata_voters`, data-group `voters`/`standby`, `runtime.bootstrap`, and `tls.trust_roots`, even when empty or false. Its Console rejects remote MQ settings, a second management endpoint and separate TLS material. There is no extra execution-mode switch and no old schema fallback.

```toml
role = "combined"
node_id = 1
cluster_id = "00000000-0000-4000-8000-000000000001"
data_dir = "data/node-1"
management_listen = "127.0.0.1:9419"
[tls]
certificate = "certs/server.pem"
private_key = "certs/server-key.pem"
[authorization]
bootstrap_admin_token_file = "secrets/bootstrap.token"
[gateway]
listen = "127.0.0.1:7443"
[storage]
[console]
listen = "127.0.0.1:8443"
static_dir = "console/web/dist"
[console.session]
key_file = "secrets/console-session.key"
public_origin = "https://localhost:8443"
```

`validate-config` reads the input configuration and checks schema, role conditions, logical groups, topology and budgets. It does not bind sockets, open WAL, create directories or keys, read secret/certificate contents, or contact MQ/S3. Successful validation does not prove that startup materials or deployment networking work. `serve` loads and validates the actual material.

Code defaults allocate 64 MiB to application admission, including a 4 MiB control reserve. Deployment generators use 192/32 MiB. Enabling object storage requires at least those larger budgets and a process-memory high watermark at least 64 MiB above application memory. Application admission is not an RSS or container-memory limit. Topic-scaling examples require measured positive capacities before enabling them.

## Separated RF1 and RF3

```sh
bash deploy/local/generate.sh rf1-separated /tmp/tidemq-separated-rf1
bash deploy/local/run.sh /tmp/tidemq-separated-rf1 target/debug/tidemq
# Use another new directory for RF3:
bash deploy/local/generate.sh rf3 /tmp/tidemq-separated-rf3
```

RF1 creates Metadata 11, Storage 21, Broker 31 and Gateway 41. RF3 adds Metadata 12/13, Storage 22/23, Broker 32 and Gateway 42. Metadata group 1 uses Metadata voters; data group 2 uses Storage voters. QUIC Gateway addresses are `127.0.0.1:7810` and, for RF3, `127.0.0.1:7820`, with names `node-41.local` and `node-42.local`.

Separated role files must explicitly provide `replication_factor = 1` or `3` and `tls.trust_roots`; they keep real Raft even with RF1. Internal peer records cannot contain Combined nodes. RF3 voters need three distinct configured zones; local zone labels do not establish physical fault isolation. Bootstrap initializes pristine groups only. Standby replicas need real learner/joint/stable transitions; readiness alone does not authorize removing a voter or volume.

## Console HTTPS and sessions

Both Console forms terminate HTTPS directly. The server certificate SAN must cover the hostname or IP in `public_origin`, and the key must match the certificate. Failed TLS setup never falls back to HTTP. SDK trust in the MQ CA and browser trust in HTTPS certificates are separate client responsibilities.

Combined reads the node's server identity and `[console]`, and observes local MQ state directly without self HTTP or QUIC polling; an independent Console reads root `[tls]`, remote `[cluster]` and `[session]`. The independent Console never reads peer inventories or mounts MQ data. Use exactly 32 random raw bytes for the current session key and at most two distinct previous verification keys. Default session lifetime is 1800 seconds, bounded to 60–3600 seconds and by real credential expiry. Cookie, Origin and CSRF checks remain active.

Session keys must be shared across independent Console replicas. Use owner access or a read-only service group for session material. Node server private keys retain owner-only permissions. Rotate keys in stages: distribute the new key as verification material, make it current while retaining the old verification key, then remove the old key after outstanding sessions expire. Configuration and keys load at startup.

## Containers and Kubernetes

The runtime image must contain both matching binaries and the Console build at `/opt/tidemq/console`. Plain Cargo compilation does not build browser assets. See the [Docker build and validation procedure](https://github.com/bitcapybara/tidemq/blob/main/deploy/docker/README.zh-CN.md).

The isolated RF1 Compose deployment runs one Combined node with its session key and built-in HTTPS assets. RF3 runs ten separated roles. RF1 publishes only native HTTPS at host loopback port 8443 and adds an ingress bridge, allowing namespace egress; MQ and management remain on namespace loopback. RF3 keeps the private internal network and publishes no host ports. Do not add an independent Console to the Combined endpoint/port.

The Kubernetes generator creates ten separated processes, ten internal UDP Services, and an `mq` UDP Service that selects Gateway Pods only. Only the six Metadata/Storage instances receive persistent volumes. Brokers/Gateways use bounded ephemeral data directories. Each role has its own disruption budget: at least two Metadata, two Storage, one Broker and one Gateway remain available during voluntary disruption. Readiness runs against Pod loopback using exec; management is never exposed as a Service.

Use an explicit context and a new namespace/output directory. This command applies resources to the selected cluster:

```sh
bash deploy/kubernetes/prepare.sh CONTEXT tidemq-functional IMMUTABLE_IMAGE STORAGE_CLASS /absolute/new-private-directory
kubectl --context CONTEXT -n tidemq-functional wait --for=condition=Ready pods --all --timeout=180s
```

Services have stable IPs included in generated leaf certificates. Keep the CA signing key local. Secret material is copied into a bounded in-memory private volume with owner-only node key permissions. External SDK access needs a UDP-capable layer-4 endpoint and matching SANs; TCP port forwarding cannot validate QUIC. PDBs do not substitute for durable retirement evidence or protect against every forced deletion.

For the independent Console, [prepare.mjs](https://github.com/bitcapybara/tidemq/blob/main/deploy/console/prepare.mjs) generates native HTTPS deployments. Both platforms require local `certificateFile`/`privateKeyFile`; generation verifies SAN, key match and validity first. Kubernetes output contains a TLS Secret, stateless Deployment, ConfigMap and HTTPS Service, with no mandatory Ingress or TLS proxy. Docker binds HTTPS to host loopback. Outputs and settings containing secret paths must stay private.

Static generator checks, local Rust tests, native browser tests, live container deployment and real three-zone recovery are separate validation results. Keep revision, image/binary identity, logs, assertions and cleanup receipts for the specific environment. Historical test evidence does not certify the current topology.
