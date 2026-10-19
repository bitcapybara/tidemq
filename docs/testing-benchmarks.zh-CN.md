# 测试方法与现有证据

当前基础测试覆盖身份/排他边界、非法/放大 protobuf、连接类别、控制内存预留与失败归还、配置、超大日志完整丢弃、独立 sink 错误和有限指标。它们不证明实际 QUIC、持久化或 RF3 行为。

```sh
cargo fmt --all
cargo clippy --workspace --tests --no-deps
cargo test --workspace --locked
cargo test --workspace --doc --locked
cargo clippy -p tidemq --tests --no-deps --no-default-features --features allocator-mimalloc
cargo clippy -p tidemq --tests --no-deps --no-default-features --features allocator-jemalloc
```

不运行 cargo check；CI 按 Linux/macOS 和互斥 allocator 分开验证，不使用全 feature 同开。schema 生成另开 codegen 并检查已提交文件差异。

allocator-probe 是有限内存持有样本，必须从真实二进制执行；记录 backend/native 版本、编译 feature、块大小及前/中/后快照，不能用其证明碎片、泄漏或吞吐优劣。profile 和 native 工具尚待验证。

后续正确性先于正式性能：模型→真实磁盘→RustFS→RF3/QUIC/SDK→故障与弹性→三区。报告包含版本/配置/故障种子、生产与持久 ACK 口径、默认观测开销、完整延迟分布与超时/拒绝、积压斜率、CPU/RSS/cgroup/IO/网络和总成本。

当前无 TideMQ 性能或恢复 SLA。未创建云资源，也未复现 Kafka/Pulsar/AutoMQ 对标。
