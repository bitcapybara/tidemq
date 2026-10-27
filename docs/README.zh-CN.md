# TideMQ 使用文档

从[产品能力与限制](guide/overview.zh-CN.md)了解适用场景，再按下面的路径运行、接入和维护。指南描述当前源码中的接口；带日期的测试记录仅证明对应版本及环境。

## 开始使用

| 目标 | 文档 |
| --- | --- |
| 了解产品和职责 | [产品概览](guide/overview.zh-CN.md)、[架构与角色](guide/architecture.zh-CN.md) |
| 完成第一条消息 | [快速开始：启动、建资源、发送、消费、ACK](guide/quickstart.zh-CN.md) |
| 部署单机或集群 | [RF1 / RF3 与独立角色](guide/running.zh-CN.md)、[Docker](https://github.com/bitcapybara/tidemq/blob/main/deploy/docker/README.zh-CN.md)、[Kubernetes](https://github.com/bitcapybara/tidemq/blob/main/deploy/kubernetes/README.zh-CN.md) |
| 配置进程 | [配置来源、单位、校验和角色示例](guide/configuration.zh-CN.md) |
| 使用浏览器 | [Console](guide/console.zh-CN.md)、[HTTPS / 多副本部署](https://github.com/bitcapybara/tidemq/blob/main/deploy/console/README.zh-CN.md) |

## 应用接入

| 目标 | 文档 |
| --- | --- |
| 集成 Rust SDK | [连接、资源、预算、取消与关闭](guide/rust-sdk.zh-CN.md)、[English SDK reference](https://github.com/bitcapybara/tidemq/blob/main/sdk/rust/README.md) |
| 可靠生产 | [批量、顺序、回执和未知结果](guide/production.zh-CN.md) |
| 可靠消费 | [Stream / Queue、持久 ACK 和重投](guide/consumption.zh-CN.md) |
| 理解失败分类 | [错误诊断](operations/error-diagnostics.zh-CN.md) |

## 部署后运维

| 目标 | 文档 |
| --- | --- |
| 保护边界与凭据 | [安全与权限](operations/security.zh-CN.md) |
| 建立观测 | [指标](operations/metrics.zh-CN.md)、[日志](operations/logging.zh-CN.md)、[OTLP](operations/tracing.zh-CN.md)、[观测部署](https://github.com/bitcapybara/tidemq/blob/main/deploy/observability/README.zh-CN.md) |
| 重启、升级与恢复 | [恢复操作与数据保护](operations/recovery.zh-CN.md)、[存储排障](operations/storage-troubleshooting.zh-CN.md) |
| 定位故障 | [按现象排障](operations/troubleshooting.zh-CN.md)、[运行时](operations/runtime.zh-CN.md)、[QUIC](operations/quic-troubleshooting.zh-CN.md)、[所有权](operations/ownership-troubleshooting.zh-CN.md) |
| 定位业务或容量问题 | [生产](operations/producer-troubleshooting.zh-CN.md)、[订阅](operations/subscription-troubleshooting.zh-CN.md)、[内存](operations/memory-troubleshooting.zh-CN.md) |
| 管理冷数据与扩缩容 | [本地 RustFS](operations/local-rustfs.zh-CN.md)、[对象生命周期](operations/object-lifecycle.zh-CN.md)、[S3 排障](operations/object-storage-troubleshooting.zh-CN.md)、[扩缩容](operations/scaling.zh-CN.md) |
| 验证部署或代码 | [测试方法](testing.zh-CN.md)、[三区验证准备](guide/three-az-validation.zh-CN.md) |

## 实现资料与历史证据

[当前实现设计](design/architecture.zh-CN.md)链接协议、WAL、所有权、订阅、对象、安全回收与运行时细节；这些资料用于理解实现，不替代用户指南。[历史架构草案](architecture.zh-CN.md)保留原始需求与候选目标。

[实施状态](implementation-status.zh-CN.md)、[要求覆盖](requirements-coverage.zh-CN.md)和[证据索引](evidence/README.zh-CN.md)包含带日期的记录，不能据此判断当前部署状态。[VM 实验室](https://github.com/bitcapybara/tidemq/blob/main/deploy/lab/README.zh-CN.md)、[2026-10-25 部署验收](https://github.com/bitcapybara/tidemq/blob/main/deploy/lab/evidence/2026-10-25/README.zh-CN.md)和[runner 维护](https://github.com/bitcapybara/tidemq/blob/main/deploy/lab/runner-maintenance.zh-CN.md)面向仓库的具体实验环境。真实三区、容量、吞吐和长期稳定性需要独立验证。

文档站的本地构建、链接检查和 Pages 发布入口见[维护指南](site.zh-CN.md)。
