# TideMQ

[English](README.md) | 简体中文

基于对象存储构建的云原生消息队列，面向高吞吐、低延迟与弹性扩缩容。

TideMQ 面向数据采集与流处理，将消息服务、复制式预写日志（WAL）、对象存储和控制元数据分层，使计算与存储能够独立扩缩容。

> **项目状态：** 本地运行时与同仓库 Rust SDK 已支持持久生产、逐条 ACK、认证 QUIC、独立元数据/数据 Raft、RustFS/S3 归档、冷读、保留策略及安全对象回收。真实本地 RF3、角色分离、独立进程故障、自动接管及成员修复已通过回归；17 项有限 RF1 负载已测量。小消息消费及持久 ACK 约 60 条/秒，高吞吐、低延迟、真实三区与恢复分位目标仍待优化和验收。详见[实施状态](docs/implementation-status.zh-CN.md)和[本地性能报告](docs/evidence/2026-10-20-local-performance.zh-CN.md)。

## 设计目标

- **高吞吐、低延迟：** 采用批量追加、group commit、流水线复制与有界队列，对象存储上传不进入生产 ACK 的关键路径。
- **对象存储承载长期数据：** 将已提交消息归档为不可变对象，支持范围读取、清单管理、对象整理与安全回收。
- **计算与存储独立扩缩容：** Broker 与 WAL 存储池分离，Gateway 可以独立部署，也可以与 Broker 组合运行。
- **跨三个可用区持久化：** 同一云地域内，每个数据组设置三副本，经多数派提交后才返回生产 ACK；Raft 控制元数据与数据路径分离。
- **独立持久订阅：** 分区内稳定 offset、逐条 ACK、至少一次交付。支持并行完成或由 SDK 按 key 串行处理，外部副作用仍需业务幂等或 fencing。
- **统一客户端入口：** 固定域名与四层负载均衡后接协议 Gateway，由 SDK 完成重连、会话恢复、有界重试和背压。
- **未确认积压保护：** 同时满足最低保留时长和未确认消息保护。达到容量上限时限制或拒绝新写入，继续消费、ACK 与回收。

## 分层架构

| 组件 | 职责 |
| --- | --- |
| 客户端 SDK | 批量发送、请求身份、连接恢复、重试期限与消费并发控制 |
| Gateway | 协议连接、鉴权、路由缓存、转发与排空 |
| Broker | 写入协调、订阅分发、缓存与流控 |
| WAL 存储池 | 通过独立复制组提交消息、offset、去重结果、writer fence 与对象发布意图 |
| 元数据 Raft | Topic、分区放置、所有权 epoch、订阅配置、对象清单与生命周期 |
| 持久订阅状态 | 分片复制 ACK 进度、投递代次与恢复检查点，不进入全局元数据组 |
| 对象存储 | 不可变历史数据对象与范围读取 |

### 写入链路

1. SDK 通过统一入口发送批次，Gateway 转发至持有分区所有权的 Broker。
2. Broker 携带所有权 epoch，向分区所属数据组提交批次。
3. 数据组校验 fence、生产者身份和序号，分配 offset，并按 Raft 提交规则完成持久化复制。
4. 消费者只看见已提交消息，生产者获得已提交 offset。响应丢失时以相同批次身份重试，取回原结果。

### 后台归档

1. 从已提交消息生成不可变对象；首版仅在同一数据复制组内跨分区合装。
2. 登记并上传对象，将持久发布意图与清单的条件更新对账，再在数据组记录发布完成。
3. 对象发布、持久恢复检查点和副本追赶条件均满足后，才回收本地消息正文。删除对象还需等待引用、待发布任务与有效 pin 全部解除。

生产 ACK、持久消费 ACK、对象发布和 WAL 回收是四个不同的完成点。仅凭超时不能判断操作是否已经提交。

## 部署与恢复

运行时提供 RF1 组合角色，以及独立 Gateway、Broker、Storage 和 Metadata 角色；本地生成器可生成 RF1/RF3 配置。同地域三区部署仍需实际基础设施与独立验收。单节点不具备节点级高可用，也不是生产故障时的自动降级模式。

候选恢复目标为：计划迁移通常不超过 1 秒，单 Broker 或 Gateway 故障 P99 不超过 5 秒，单可用区故障 P99 不超过 10 秒。这些是设计与压测目标，不是 SLA。计时覆盖故障检测、选主、fence、状态恢复、路由与 SDK 重连。Gateway 故障可能中断已有连接，SDK 需要重连并恢复会话。

## 开始使用

仓库固定 Rust 1.96.0 及 rustfmt/Clippy。生成本地私有证书、令牌和全新 RF1 配置后启动服务：

```sh
cargo build --locked
deploy/local/generate.sh rf1 /tmp/tidemq-demo
target/debug/tidemq validate-config --config /tmp/tidemq-demo/node-1.toml
target/debug/tidemq serve --config /tmp/tidemq-demo/node-1.toml
```

生成器需要 OpenSSL 和尚不存在的输出目录，证书仅供短期本地开发；令牌保存在私有文件中。创建 Topic、生产、消费、持久 ACK 及 RF3 操作见[运行指南](docs/guide/running.zh-CN.md)和[Rust SDK](sdk/rust/README.md)。管理接口在 loopback 展示运行中服务；独立 `diagnose` 命令只描述它自身的短生命周期进程。

本地验证命令：

```sh
cargo fmt --all -- --check
cargo clippy --workspace --tests --no-deps
cargo test --workspace --locked
```

## 完整文档与实施路线

[完整架构草案](docs/architecture.zh-CN.md) 保留了源 PDF 的全部九页内容，包括安全约束、故障场景、性能参考的限制、官方资料与评审清单。原稿日期为 2026-10-19；其中“实现语言尚未确定”等原始表述作为历史记录保留，本仓库已选择 Rust。

1. 建立协议模型与确定性故障模拟。
2. 实现本地最小消息链路。
3. 实现三节点复制、生产去重、持久 ACK 与恢复。
4. 实现对象合装、发布、对账与垃圾回收。
5. 实现 Gateway 与受控扩缩容。
6. 在对齐持久化和交付语义的前提下，完成三可用区性能对标与故障验证。

实现基线使用 OpenRaft、s2n-quic、Protobuf、同仓库 Rust SDK、aws-sdk-s3 和本地 RustFS；分配器由二进制拥有，mimalloc/jemalloc/System 仍按性能和诊断关卡评估。参见[文档导航](docs/README.zh-CN.md)、[当前设计](docs/design/architecture.zh-CN.md)、[要求覆盖](docs/requirements-coverage.zh-CN.md)和[可复现负载工具](docs/performance.zh-CN.md)。负载数值和生产预算仍需验证；历史草案中的 AutoMQ 数据是外部参考，不是 TideMQ 实测。

## 许可证

TideMQ 使用 [MIT License](LICENSE)。
