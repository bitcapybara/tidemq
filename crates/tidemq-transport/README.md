# tidemq-transport

Shared native QUIC sessions, TLS authentication, scoped authorization, bounded framing, connection pools, memory admission, and transport metrics for TideMQ services and the Rust SDK.

This crate uses s2n-quic and tidemq-protocol. It has no service, OpenRaft, S3, allocator, global tracing subscriber, or exporter initialization dependency. Callers own endpoint configuration, dispatch, runtime lifetime, and metrics registration. A successful send or QUIC acknowledgement never means that a message has been durably committed.

Services retain the `tidemq::transport` re-export. SDKs can depend directly on this crate and construct `budget::MemoryBudget` with `budget::MemoryLimits`.
