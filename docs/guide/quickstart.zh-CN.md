# 基础快速开始

当前阶段提供协议、配置校验、有限观测和 allocator 统计探针，消息服务启动和 SDK 收发尚在后续任务中。以下命令不会创建 Topic、监听业务端口或提供 HA。

工具链固定为 Rust 1.96.0，rust-toolchain.toml 包含 fmt/clippy。首次使用由 rustup 安装；已经运行相同 rustc 版本也可能尚未安装对应命名工具链和组件。

```sh
cargo run --locked -- validate-config --config config/local.toml
cargo run --locked -- diagnose --config config/local.toml
cargo run --locked -- metrics
cargo run --locked -- allocator-probe --block-bytes 1048576 --blocks 8
```

diagnose 描述当前短生命周期进程，而非远端或其他正在运行的节点；metrics 输出基础 schema 的新进程计数。诊断不自动 purge，不复制用户正文或 WAL。allocator-probe 在自身进程最多分配 64 块/32MiB，给出分配前、持有和释放后的快照；这不是性能基准或泄漏证明。

替代分配器构建：

```sh
cargo run --locked --no-default-features --features allocator-mimalloc -- allocator-probe
cargo run --locked --no-default-features --features allocator-jemalloc -- allocator-probe
```

SDK 独立验证：

```sh
cargo test -p tidemq-sdk --locked
cargo test -p tidemq-protocol --locked
```

本地 S3 后续统一使用 RustFS；本机 macOS arm64 可使用 1.0.3 官方二进制，不必为基础阶段安装容器。证书、RustFS、完整消息链路的实际部署命令将随对应实现和验证发布。
