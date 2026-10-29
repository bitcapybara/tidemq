# tidemq-transport

Shared native QUIC sessions, TLS authentication, scoped authorization, bounded framing, connection pools, memory admission, and transport metrics for TideMQ services and the Rust SDK.

This crate uses s2n-quic and tidemq-protocol. It has no service, OpenRaft, S3, allocator, global tracing subscriber, or exporter initialization dependency. Callers own endpoint configuration, dispatch, runtime lifetime, and metrics registration. A successful send or QUIC acknowledgement never means that a message has been durably committed.

Services retain the `tidemq::transport` re-export. SDKs can depend directly on this crate and construct `budget::MemoryBudget` with `budget::MemoryLimits`.

`TransportSnapshot.failures` retains eight classified events and the last RPC failure separately. Each event identifies the read/write/liveness gate, frame/lane/root scope, local physical connection and stream IDs, and code-defined reason labels. It excludes wire contents, tokens, and error display text. Unexpected RPC exits are recorded before resetting streams and closing response waiters; deliberate draining is excluded from RPC failure reporting. The `transport_failure` warning emits only the first four events and subsequent powers of two, at most 65 warnings per endpoint lifetime. IDs are diagnostic fields, never metric labels. Snapshots remain available without a tracing subscriber.

`malformed_total` counts failed frame reads as well as decoding failures, including IO, memory admission, and expired request budgets. It is not a count of invalid peer messages. The classified history distinguishes these gates without changing protocol validation, deadlines, late-response handling, or the unknown outcome of `rpc::Error::Closed`. History event totals include both a frame failure and its resulting RPC exit when applicable; they do not equal `malformed_total`.

`Client::close(deadline)` stops new connections, closes cached and direct sessions, and awaits the native endpoint. `Server::shutdown()` stops acceptance; the caller still drains accepted sessions. In pinned s2n-quic 1.91, dropping an idle acceptor does not wake native I/O. Shutdown sends one local invalid datagram to wake that endpoint; the real UDP port-release test covers this behavior. Recheck the workaround when upgrading [s2n-quic](https://github.com/aws/s2n-quic).
