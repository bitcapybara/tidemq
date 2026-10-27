# TideMQ

[English](README.md) | 简体中文

TideMQ 是使用 Rust 开发的持久消息队列，面向事件接入、流式消费和后台任务。在线写入由复制 WAL 提供持久性，可选的 S3 兼容对象存储承载历史数据与冷读。

应用通过同仓库 Rust SDK 使用逻辑 Topic 和持久 Stream / Queue 订阅。Gateway 提供 QUIC 入口，Broker 协调服务，Metadata 与 Storage 使用独立的 Raft 组；浏览器管理由独立无状态 Console 提供。

**当前处于 `0.1.0` 开发阶段。** API、配置和磁盘格式可能变化。RF1 没有节点级高可用；RF3 失去多数派后不会降为 RF1。功能与故障测试不构成生产 SLA、吞吐或容量承诺，部署前请阅读[产品能力与边界](docs/guide/overview.zh-CN.md)。

## 核心能力

- Producer 成功表示数据 Raft 已持久提交；回包丢失时沿原身份恢复结果。
- Stream 提供同 key 有效交付有序与累计 ACK；Queue 提供成员分摊、单条 ACK / NACK。交付至少一次，业务副作用须幂等。
- 数据组持久化消息、offset、去重结果、投递 attempt 与 ACK；对象归档在后台完成，不进入 Producer ACK 路径。
- 有界准入、背压、保留与租户/Namespace 配额；容量不足时优先保留已有 ACK、恢复和控制工作的资源。
- TLS、作用域凭据、内部固定身份 mTLS、loopback 管理 HTTP，以及日志、指标和可选 OTLP。
- 独立无状态 Console 用于日常浏览与授权范围内的操作；成员替换、扩缩容和排空使用现有 SDK / CLI。

## 本地启动

需要仓库固定的 Rust 1.99.1、Bash 和 OpenSSL。在仓库根目录使用尚不存在的输出目录：

```sh
cargo build --locked -p tidemq -j2
bash deploy/local/generate.sh rf1 /tmp/tidemq-demo
target/debug/tidemq validate-config --config /tmp/tidemq-demo/node-1.toml
bash deploy/local/run.sh /tmp/tidemq-demo target/debug/tidemq
```

另一个终端运行 `curl --fail http://127.0.0.1:8410/readyz`。SDK 使用 `127.0.0.1:7410` 和生成目录中的 `ca.pem`、私有 `access.token`；开发证书七天过期。继续按[快速开始](docs/guide/quickstart.zh-CN.md)创建资源、发送、接收并持久 ACK。

## 使用文档

| 任务 | 入口 |
| --- | --- |
| 理解产品 | [能力与限制](docs/guide/overview.zh-CN.md)、[架构与角色](docs/guide/architecture.zh-CN.md) |
| 部署与配置 | [RF1 / RF3](docs/guide/running.zh-CN.md)、[每个角色的配置](docs/guide/configuration.zh-CN.md)、[Docker](deploy/docker/README.zh-CN.md)、[Kubernetes](deploy/kubernetes/README.zh-CN.md) |
| 应用开发 | [Rust SDK](docs/guide/rust-sdk.zh-CN.md)、[生产](docs/guide/production.zh-CN.md)、[消费与 ACK](docs/guide/consumption.zh-CN.md) |
| 浏览器使用 | [Console 使用](docs/guide/console.zh-CN.md)、[Console 部署](deploy/console/README.zh-CN.md) |
| 安全与运维 | [安全](docs/operations/security.zh-CN.md)、[指标](docs/operations/metrics.zh-CN.md)、[日志](docs/operations/logging.zh-CN.md)、[追踪](docs/operations/tracing.zh-CN.md)、[恢复](docs/operations/recovery.zh-CN.md)、[排障](docs/operations/troubleshooting.zh-CN.md) |
| 开发验证 | [测试方法与证据边界](docs/testing.zh-CN.md)、[文档站维护](docs/site.zh-CN.md) |

[完整文档导航](docs/README.zh-CN.md)按用户路径组织。设计稿与带版本、日期的实验室证据作为补充资料保留。开发磁盘格式不提供旧实验格式自动迁移；升级前保留完整原目录、身份、证书与匹配二进制，禁止通过清空 WAL 解决启动失败。

许可证：[MIT](LICENSE)。
