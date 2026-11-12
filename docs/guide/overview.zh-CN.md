# 产品能力与限制

TideMQ 是 Rust 实现的持久消息队列，面向事件接入、流式消费与后台任务。你可以先把它理解成一条可靠的消息通道：应用把消息写入 Topic，消费者从 Stream 或 Queue 读取，业务处理成功后用 ACK 确认。当前仓库版本为 `0.1.0`，仍在开发阶段。

## 先记住这五个词

| 词 | 用业务语言理解 |
| --- | --- |
| Topic | 一类消息的名称，例如 `orders`；Producer 和 Consumer 通过它找到同一类数据 |
| Producer | 写入消息的应用 |
| Stream | 适合需要按 key 保持有效顺序的持久订阅，使用累计 ACK |
| Queue | 适合多个成员分摊任务的持久订阅，使用单条 ACK / NACK |
| ACK | 业务处理成功后的持久确认；收到消息不等于业务已经完成 |

第一次选择部署方式时，可以按下面判断：

| 目标 | 选择 |
| --- | --- |
| 先在一台电脑上跑通示例 | 单进程 Combined |
| 验证多进程和真实 QUIC / Raft | 分离 RF1 |
| 验证多数派和故障域恢复 | RF3；需要三个真实故障域 |

## 选择消息模型

| 需求 | 使用方式 | 业务责任 |
| --- | --- | --- |
| 持久发送事件 | Producer；成功获得 MessageId | 未知结果恢复原 PendingSend；跨进程用业务幂等键保护 |
| 按 key 顺序处理 | Stream 订阅与累计 ACK | 处理完所有已接收前缀再累计确认，避免提前确认其他任务 |
| 多成员分摊任务 | Queue 订阅、单条 ACK / NACK | 副作用幂等；失败时决定重投，不以 NACK 表示成功 |
| 多个业务独立读取 | 同一 Topic 的多个具名持久订阅 | 先建订阅再生产需要保护的数据；每个订阅各有进度 |
| 历史消息与热数据回收 | 为 Storage / Combined 配置 S3 兼容对象存储 | 维护桶、权限、容量和网络；不能绕过 manifest 直接删对象 |
| 日常浏览与管理 | Combined 内置或分离 Console，经原生 HTTPS 登录个人 token | 使用授权范围；高级集群写操作仍用 SDK / CLI |

## 当前边界

- 至少一次交付。去重与持久 ACK 不构成外部数据库、支付或第三方 API 的 exactly-once；应用需幂等或业务 fencing。
- 顺序以 API 的 key / 订阅契约为准，不承诺跨 key、跨 Producer、跨数据组的全局顺序。`send_batch` 可部分完成，不是跨组事务。
- SDK 没有持久 outbox。进程内的 PendingSend / PendingAck / PendingAdmin 应在原 Client 关闭前恢复；重启后应用须核实自己的业务结果。
- RF1 没有节点级 HA。RF3 依赖 Metadata 和相应数据组的真实多数派；失 quorum 拒绝新提交，不临时降副本数。单机的三个 zone 标签不代表三个真实故障域。
- S3 可选。未配置时只有有界热路径；容量达到上限会拒绝新写。对象上传在 Producer ACK 之后独立执行，不构成另一份同步提交承诺。
- 配置拒绝未知字段；开发格式不提供旧实验布局自动迁移或混合版本兼容承诺。升级前保留原状态与二进制，按[恢复指南](../operations/recovery.zh-CN.md)验证。
- 消息浏览按 Topic / Shard / offset 有界执行。消息 ID、key、属性筛选不提供精确发布时间搜索；需要独立内容权限。
- 功能、故障和覆盖率验证各有范围。没有已承诺的生产 SLA、三区恢复分位、吞吐或容量评级；以[测试指南](../testing.zh-CN.md)中对应版本和环境的证据为准。

## 下一步

首次运行阅读[快速开始](quickstart.zh-CN.md)；选部署角色阅读[架构](architecture.zh-CN.md)和[运行指南](running.zh-CN.md)；接入应用阅读[Rust SDK](rust-sdk.zh-CN.md)、[生产](production.zh-CN.md)与[消费](consumption.zh-CN.md)。
