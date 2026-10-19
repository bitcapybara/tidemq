# tidemq-transport

Shared native QUIC sessions, TLS authentication, scoped authorization, bounded framing, connection pools, memory admission, and transport metrics for TideMQ services and the Rust SDK.

This crate uses s2n-quic and tidemq-protocol. It has no service, OpenRaft, S3, allocator, global tracing subscriber, or exporter initialization dependency. Callers own endpoint configuration, dispatch, runtime lifetime, and metrics registration. A successful send or QUIC acknowledgement never means that a message has been durably committed.

Services retain the `tidemq::transport` re-export. SDKs can depend directly on this crate and construct `budget::MemoryBudget` with `budget::MemoryLimits`.

`Client::close(deadline)` stops new connections, closes cached and direct sessions, and awaits the native endpoint. `Server::shutdown()` stops acceptance; the caller still drains accepted sessions. In pinned s2n-quic 1.91, dropping an idle acceptor does not wake native I/O. Shutdown sends one local invalid datagram to wake that endpoint; the real UDP port-release test covers this behavior. Recheck the workaround when upgrading [s2n-quic](https://github.com/aws/s2n-quic).
