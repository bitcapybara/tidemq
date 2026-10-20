# 文档导航

先读[实施状态](implementation-status.zh-CN.md)和[要求覆盖](requirements-coverage.zh-CN.md)：已运行的本地证据、候选性能目标和外部待验条件分别记录。历史[架构草案](architecture.zh-CN.md)保留原始要求，当前行为以设计、配置、测试和使用指南为准。

| 读者与任务 | 入口 |
| --- | --- |
| 首次运行 | [快速开始](guide/quickstart.zh-CN.md)、[RF1/RF3 与分角色运行](guide/running.zh-CN.md)、[配置](guide/configuration.zh-CN.md)、[本地 RustFS](operations/local-rustfs.zh-CN.md) |
| 应用接入 | [Rust SDK](guide/rust-sdk.zh-CN.md)、[生产消息](guide/production.zh-CN.md)、[消费与持久 ACK](guide/consumption.zh-CN.md) |
| 架构与协议 | [当前架构](design/architecture.zh-CN.md)、[协议](design/protocol.zh-CN.md)、[QUIC connection/stream](design/quic-transport.zh-CN.md)、[Raft 网络](design/raft-network.zh-CN.md) |
| 持久化与正确性 | [WAL 与恢复](design/storage-recovery.zh-CN.md)、[所有权](design/ownership-scaling.zh-CN.md)、[Broker 自动接管](design/broker-failover.zh-CN.md)、[生产幂等](design/producer-idempotency.zh-CN.md)、[持久订阅](design/subscriptions.zh-CN.md) |
| 冷数据与容量 | [对象格式与 S3](design/object-storage.zh-CN.md)、[生命周期](design/object-lifecycle.zh-CN.md)、[保留与配额](design/retention-quota.zh-CN.md)、[扩缩容](design/scaling.zh-CN.md) |
| 运行时与观测 | [运行时设计](design/runtime.zh-CN.md)、[观测设计](design/observability.zh-CN.md)、[内存设计](design/memory-management.zh-CN.md)、[日志](operations/logging.zh-CN.md)、[指标](operations/metrics.zh-CN.md)、[可选 OTLP](operations/tracing.zh-CN.md) |
| 当班排障 | [排障入口](operations/troubleshooting.zh-CN.md)、[运行时](operations/runtime.zh-CN.md)、[QUIC](operations/quic-troubleshooting.zh-CN.md)、[存储](operations/storage-troubleshooting.zh-CN.md)、[控制权](operations/ownership-troubleshooting.zh-CN.md) |
| 业务与对象排障 | [生产](operations/producer-troubleshooting.zh-CN.md)、[订阅](operations/subscription-troubleshooting.zh-CN.md)、[S3](operations/object-storage-troubleshooting.zh-CN.md)、[生命周期](operations/object-lifecycle.zh-CN.md)、[扩缩容](operations/scaling.zh-CN.md)、[内存](operations/memory-troubleshooting.zh-CN.md) |
| 验证与性能 | [测试方法与证据](testing-benchmarks.zh-CN.md)、[本地统一验收](evidence/2026-10-20-local-validation.zh-CN.md)、[17 项本地性能报告](evidence/2026-10-20-local-performance.zh-CN.md)、[性能工具与口径](performance.zh-CN.md)、[Broker 故障证据](evidence/2026-10-20-broker-failover.zh-CN.md)、[OTLP 故障证据](evidence/2026-10-20-otlp.zh-CN.md)、[同 allocator 发布构建栈采样证据](evidence/2026-10-19-allocator-release.zh-CN.md) |

`ssh dev` 已完成的 RF1/RF3 容器隔离、故障与原身份恢复见[Linux Docker 记录](evidence/2026-10-20-linux-docker.zh-CN.md)，复现入口见[Docker 使用文档](../deploy/docker/README.zh-CN.md)。真实三区的资源输入、费用、拓扑和故障执行顺序见[三区验证准备](guide/three-az-validation.zh-CN.md)。可选监控组件、看板与告警见[观测部署](../deploy/observability/README.zh-CN.md)。SDK 位于同仓库 `sdk/rust/`，由宿主初始化日志、指标导出和全局分配器。管理 HTTP 必须留在 loopback；访问其他节点需部署受保护代理。
