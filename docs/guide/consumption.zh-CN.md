# 消费与持久 ACK

应用使用 `Client::new_stream_consumer` 或 `Client::new_queue_consumer` 的 builder，操作逻辑 Topic 与具名持久订阅。服务端和 SDK 管理物理 Shard、session、成员代次与重连；应用无需自行 Claim 或填写 epoch。完整成功示例见[快速开始](quickstart.zh-CN.md)。

## 选择 Stream 或 Queue

| 业务需求 | StreamConsumer | QueueConsumer |
| --- | --- | --- |
| 同 key 有效交付顺序 | 支持；Stream lane 与物理 Shard 独立 | 不提供 Stream 的同 key 顺序契约 |
| 多个独立业务读取同一消息 | 创建不同订阅名 | 创建不同订阅名 |
| 同一订阅多成员 | 服务端分配有界 lane | 成员分摊消息 |
| 业务确认 | `ack_cumulative(&message)` | `ack(&message)` |
| 安排重投 | 无 Queue 式 NACK 接口 | `nack(&message, delay)` |
| 交付保证 | 至少一次 | 至少一次 |

Topic 必须已存在。consumer build 创建或加入持久订阅；默认初始位置为最早仍可用的数据，可通过 builder 的 `initial_position(InitialPosition::Latest)` 选择新订阅的起点。该选项不重置已有订阅。需要保护未来消息时先建立订阅，再生产。订阅不能恢复已经逻辑过期的数据。

同一 Topic 的不同订阅具有独立进度。零成员、关闭或 Drop consumer 均不删除订阅及积压；删除由有权限的管理端显式操作，见[SDK 指南](rust-sdk.zh-CN.md)。

## 接收、处理、确认

1. `receive().await?` 等待一条消息；`try_receive()` 只检查本地队列，返回 None 不证明远端没有积压。
2. 在业务系统完成幂等处理。正文已接收、预取入队和 SDK 缓冲均不代表业务成功。
3. Queue 对原消息调用单条 ACK；Stream 在全部前置处理成功后累计 ACK。
4. 等待成功回执，才将对应范围视为持久确认。热 WAL 和 S3 冷读使用同一持久 attempt / ACK 权威。

Stream 累计 ACK 覆盖该逻辑 consumer 已交付给应用的共享前缀，不含尚未交付的预取消息。多个 clone 共享前缀：如果并发处理时较早任务尚未成功，就不能累计确认较后的消息。Queue 的乱序单条 ACK 不会跳过更早未确认的洞。

Queue NACK 成功只表示已持久安排重投，不表示业务成功。业务永久失败不能通过自动 ACK、跳过、向前移动游标或“已投递 DLQ”隐式改写为成功。

## 超时、未知结果与重投

| 状态 | 应用动作 |
| --- | --- |
| 成功 ACK 回执 | 对应确认范围已持久；可释放已处理的本地数据 |
| ACK / NACK 结果未知 | 检查 `outcome_unknown()` 和原 consumer 的 `pending_acks().await?`，对原 PendingAck 调用 `retry()` |
| 代次或 attempt 已失效 | 停止使用旧交付；不自行改 token 或代次，按结构化错误恢复成员状态 |
| 容量或在途窗口受限 | 减少新接收并发，继续完成已有业务和 ACK，检查最早未确认项 |
| 接收被取消或超时 | 不推断远端无积压；保留原逻辑 consumer，检查关闭/恢复报告 |
| 业务副作用可能已执行 | 用 MessageId 或业务幂等键核实；fencing 不能撤销已发生的外部副作用 |

一次 receive 可通过 `RequestContext::with_timeout(...)` 的 `scope` 设置总等待期限。没有显式 context 的 receive 持续等待，但其网络子请求仍有有限期限。具体网络等待实现不构成吞吐或空闲请求开销承诺；集成后的行为以相应版本测试为准。

## 共享实例、预算与退出

Consumer clone 竞争同一个接收队列、共享身份与预算，可以确认另一 clone 返回的有效消息；任一 clone 的 close 关闭整个逻辑实例。需要独立成员时再次调用 `new_*().build().await`。

`prefetch_bytes` 包含读取预留、本地队列和应用仍持有的正文。当前最小值为 262656 字节，以容纳一条最大消息和交付元数据；ACK 后仍持有正文会继续占用内存预算。不要按 Shard 数放大无界缓冲。

退出时先停止新的业务任务，处理或记录未决操作，再检查 consumer 和 Client 的 CloseReport / CloseError。close 不替业务 ACK，也不删除持久订阅；SDK 没有跨进程持久 outbox。积压、洞、成员和窗口排查见[订阅排障](../operations/subscription-troubleshooting.zh-CN.md)。
