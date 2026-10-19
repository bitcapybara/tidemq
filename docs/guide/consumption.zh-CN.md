# 消费与持久 ACK

同仓库 Rust SDK 通过实际 QUIC 调用数据组持久订阅；[SDK 指南](rust-sdk.zh-CN.md)给出 Claim、接收与 ACK 用法，[运行指南](running.zh-CN.md)给出服务入口。正文可能来自 WAL 热路径或 S3 冷路径，两者共用相同 offset、attempt 与持久 ACK 权威。

1. 创建订阅时选定仍被保留的 start offset；记录返回的不可复用 subscription UUID。
2. 以新的 session UUID 和当前 cursor/assignment 做 Claim。普通 Claim 不会抢占另一个活跃 session；显式接管必须带旧 session 并具备权限。
3. 读取当前分配的数据。每条交付包含 offset 和 token，token 已对应数据组中持久化的 attempt。SDK 重连保留 session、代次与操作身份；不能用网络重试重新生成订阅。
4. 正文进入 SDK 预留的有界队列后，SDK 才推进连续接收屏障。冷读期间，持久 ACK skip 区间也须验证同一代次；不能直接使用最大 offset。
5. 业务成功后发送原 token，等待持久 ACK 结果。ACK 入本地队列不等于服务端持久成功。丢回包时重试原 token，`already_acknowledged` 是成功结果。

默认消费允许同一 partition 中多条消息并行完成、乱序 ACK。连续前沿只在所有更早消息均已确认时前进，较晚 ACK 不会掩盖较早失败。

Read 与 SDK `fill` 的消息数是单次上限；服务可在时间预算内返回已经持久化的部分批次。持续接收使用 `Consumer::recv(deadline)`，或按实际 `fill` 数量处理队列并继续拉取，不要求一次填满配置容量。部分批次只说明当前这些 token 已持久签发，不代表业务已 ACK；继续按原 token 逐条确认。

Read 返回 Unknown 时保留同一 `Consumer`，不跳过 offset，也不把未知签发当成成功。需要重新取得分配时使用现有 `Consumer::reclaim`；新代次会使旧 token 失效，外部业务副作用仍需幂等。SDK 将 Remote Unknown 上抛给应用，不自动重试，也不向应用暴露可原样重发的 PendingRead。ACK 继续按原 token 和对应错误分类处理。

可选同 key 串行模式由 SDK 保证一个 key 同时只执行一个 handler，不同 key 可并行。前一条失败时阻塞该 key 的后续 handler；不要为追赶吞吐自动 ACK、跳过或把 DLQ 投递当成原消息成功。没有 key 的消息如何分组、handler 并发和缓冲上限应显式配置。旧 consumer 可能已在外部系统执行副作用后才被 fence，新代次阻止旧 ACK，但无法撤销这些副作用；业务应使用消息身份做幂等，或使用业务系统支持的 fencing。

| 结果 | 调用者处理 |
| --- | --- |
| 持久 `accepted` / `already_acknowledged` | 本条消息完成；可释放本地 ACK 待确认项 |
| 超时 / 连接 reset / 回包丢失 | 结果未知，以原 token、原操作身份在 deadline 内重试 |
| `StaleAttempt` | 当前 attempt 已被重投替代；不得用旧 token 改写新 attempt |
| `StaleGeneration` / `SessionMismatch` | 停止旧分配交付，恢复权威状态；不要把旧 token 改成新代次 |
| `WindowFull` / 在途数量或字节受限 | 暂停新投递，继续处理并 ACK 已有消息；检查最早未确认消息 |
| `AttemptLimit` | 消息仍未 ACK；由受权人工 replay/seek 或修复业务，不能自动忽略 |
| `OutOfRetention` | 查询当前 retained floor，显式选择可用起点；不静默跳到最新 |

向后 seek 是显式回放操作，会提升 cursor 和 assignment generation，清空旧窗口、取消旧分配，并要求重新 Claim。旧 token 不能确认新游标中的同一 offset。原业务副作用可能再次发生，应按业务幂等处理。向前 seek 同样是显式游标变更，会跳过被跨越范围，必须由有权限的操作者决定；永久失败不会自动触发它。

消费者变慢时优先检查 [订阅排障](../operations/subscription-troubleshooting.zh-CN.md) 的 frontier、holes、attempt 和 receive barrier。Topic 配额拒写时，消费与 ACK 应继续得到保留的资源准入；如果 ACK 也被数据队列阻塞，属于运行时隔离缺陷，不能靠加大无界客户端缓冲解决。
