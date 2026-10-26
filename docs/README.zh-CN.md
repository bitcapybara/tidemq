# 文档导航

当前入口：[VM Kubernetes 实验室、资源清单与使用方法](../deploy/lab/README.zh-CN.md)、[部署验收记录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/deploy/lab/evidence/2026-10-25/README.zh-CN.md)、[测试方法](testing.zh-CN.md)。2026-10-26 按用户要求删除性能测试相关工具、配置、基线和专项报告，功能、覆盖率与 VM 故障 CI 继续执行。

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
| 验证与故障 | [测试方法与证据](testing.zh-CN.md)、[VM 实验室验收](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/deploy/lab/evidence/2026-10-25/README.zh-CN.md)、[本地统一验收](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-local-validation.zh-CN.md)、[Broker 故障证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-broker-failover.zh-CN.md)、[OTLP 故障证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-otlp.zh-CN.md) |

`ssh dev` 的分段 WAL、默认 mimalloc、新卷 RF1/RF3 隔离验证见[按源码标注的 Linux／Docker 记录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-wal-segments-linux.zh-CN.md)。历史 [077004e Docker 记录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-docker-077004e.zh-CN.md)和[旧版 Linux 记录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-linux-docker.zh-CN.md)独立保留；复现入口见[Docker 使用文档](../deploy/docker/README.zh-CN.md)。真实三区的资源输入、费用、拓扑和故障执行顺序见[三区验证准备](guide/three-az-validation.zh-CN.md)。可选监控组件、看板与告警见[观测部署](../deploy/observability/README.zh-CN.md)。SDK 位于同仓库 `sdk/rust/`，由宿主初始化日志、指标导出和全局分配器。管理 HTTP 必须留在 loopback；访问其他节点需部署受保护代理。
