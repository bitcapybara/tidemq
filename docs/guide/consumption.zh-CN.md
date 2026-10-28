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
| 所有权切换的临时拒绝，原结果尚未确定 | 保留错误携带的原 PendingAck；可重试 Fenced / Unavailable 不等于交付退休，按[SDK 恢复规则](rust-sdk.zh-CN.md#总期限、取消和恢复)重试原确认 |
| 代次或 attempt 已失效 | 停止使用旧交付；不自行改 token 或代次，按结构化错误恢复成员状态 |
| 容量或在途窗口受限 | 减少新接收并发，继续完成已有业务和 ACK，检查最早未确认项 |
| 接收被取消或超时 | 不推断远端无积压；保留原逻辑 consumer，检查关闭/恢复报告 |
| 业务副作用可能已执行 | 用 MessageId 或业务幂等键核实；fencing 不能撤销已发生的外部副作用 |

一次 receive 可通过 `RequestContext::with_timeout(...)` 的 `scope` 设置总等待期限。没有显式 context 的 receive 持续等待，但其网络子请求仍有有限期限。

## 空闲等待与公平读取

SDK 先用只读 `GroupWatch` 观察可读性，再用 `GroupRead` 领取消息。Watch 提示不分配消息、不签发交付 token，也不推进消费进度；收到变化提示仍须读取并取得持久结果。观察 token 失效、换主或路由变化时重新同步，不能把观察失败当作远端没有消息。

观察按最多 32 个 Shard 分页，单次服务端等待最多 1 秒；SDK 公平轮转目标，以约 30 秒、带抖动的周期安排保底读取。排队、信用不足或故障会影响实际完成时间，这个周期不构成最大交付时延保证。期限控制、取消和成员 fencing 同时作用于观察及读取。应用仍使用相同的 `receive`、ACK 和原结果恢复接口。

在 `Isolated` 模式下，最多保留 8 个普通原读取和 1 个保护原读取；短 RPC 尝试结束后，Unknown 原读取继续持有原身份和正文信用。普通请求、恢复和其他消费者不能借用本消费者的保护份额。该份额还需要 Client 和原生传输预算支持；低容量配置采用可见的 `Serial` 模式或在构建时明确拒绝，不能把串行模式视为仍有独立保护读取保证。增加 Shard 数不会增加这些额度。

对已构建的 consumer 调用 `diagnostics()?`，可以读取当前模式、普通/保护未决读取、恢复队列、活跃 RPC、观察页、过期保底及两池的可用消息槽和字节。它是 SDK 本地调度状态的即时快照；服务端的观察占用和结果看 [Watch 指标](../operations/subscription-troubleshooting.zh-CN.md#watch-与实际读取)。持续受阻也会通过 `receive()` 暴露可重试错误。宿主可周期采集这些数值为固定名称的 gauge；业务 ID 应用于受控诊断，不作为无限制指标 label。

```rust
let status = consumer.diagnostics()?;
tracing::debug!(
    mode = ?status.mode,
    ordinary_unknown = status.ordinary_unknown,
    protected_blocked = status.protected_blocked,
    observation_overdue = status.observation_overdue,
    fallback_overdue = status.fallback_overdue,
    "Consumer scheduling state"
);
```

## 共享实例、预算与退出

Consumer clone 竞争同一个接收队列、共享身份与预算，可以确认另一 clone 返回的有效消息；任一 clone 的 close 关闭整个逻辑实例。需要独立成员时再次调用 `new_*().build().await`。

`prefetch_bytes` 包含读取预留、本地队列和应用仍持有的正文。当前最小值为 262656 字节，以容纳一条最大消息和交付元数据；ACK 后仍持有正文会继续占用内存预算。不要按 Shard 数放大无界缓冲。

退出时先停止新的业务任务，处理或记录未决操作，再检查 consumer 和 Client 的 CloseReport / CloseError。close 不替业务 ACK，也不删除持久订阅；SDK 没有跨进程持久 outbox。积压、洞、成员和窗口排查见[订阅排障](../operations/subscription-troubleshooting.zh-CN.md)。
