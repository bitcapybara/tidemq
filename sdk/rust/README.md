# TideMQ Rust SDK

The SDK lives in this repository and builds independently with `cargo test -p tidemq-sdk --locked`.

Current implementation: shared identities, generated protocol messages, and client resource-budget validation. Network producer and consumer APIs are scheduled in T06 and are not available yet.

The host application owns the global allocator and tracing subscriber. This crate does not start an exporter, listener, or logging thread. Its dependency graph excludes server, consensus, WAL, and object-storage packages.
