# 订阅与 ACK 排障

## 只读证据

服务端 `SubscriptionState::diagnostics(subscription_id)` 返回一个订阅的有限状态，复杂度不随全量历史增加：代次、阶段、确认前沿、历史签发上界、当前签发/接收前缀、冷读交界、窗口/空洞/在途数量与字节、全分区汇总、一个最早 retry，以及有限枚举阻塞原因。该接口没有正文、key、token 或 signing key。通过数据组 `with_applied` 同锁读取状态与 LogId；诊断只说明该副本已应用的状态，不能独自证明当前 leader 或线性一致性。

先记录 Topic/subscription UUID、partition、owner/fence epoch、cursor/assignment、逻辑 operation 与当前 attempt，关联授权准入、Raft commit/apply 和 ACK 回包阶段。不要记录 token、静态访问凭据、消息正文或完整状态快照。常规成功投递/ACK 不逐条打 info 日志；临时调试过滤器必须有 TTL、事件/字节上限，指标不能以这些高基数字段作为 label。

| 症状 / blocker | 解释 | 处理与验证 |
| --- | --- | --- |
| `Window`，holes 高而在途低 | 最早未 ACK 消息阻止前沿，较晚 ACK 已占满有限窗口 | 定位前沿 offset 的 handler/attempt，确认当前 token；修复后 ACK 应一次释放连续空洞，不增大无界位图 |
| `InFlightMessages` / `InFlightBytes` | 当前工作超过并发或字节预算 | 降低 handler 并发/预取，排查大消息；继续 ACK，不挤占控制保留 |
| `AttemptsExhausted` | 至少一条消息达到上限，仍保护保留前沿 | 查业务失败与当前代次；采用显式受权 replay，不把写 DLQ 当 ACK |
| `AwaitReceiveBarrier` | 已签发冷前缀，但 SDK 连续入队证据未到 H | 查最早缺块、SDK 队列容量和 skip 区间验证；不能用 QUIC ACK 或业务 ACK 替代 |
| 旧代次/token 拒绝 | seek、接管、owner fence 或重投已经替代旧交付 | 获取当前分配；确认 SDK 丢弃迟到块，保留旧 operation 供结果对账，禁止篡改 token |
| ACK 本地提交快，持久响应慢 | Raft/磁盘/多数派或响应 C 流阶段阻塞 | 分别查排队、WAL sync、复制/commit/apply、控制回复；网络发送成功不能证明持久 ACK |
| ACK 随生产满额一起停顿 | 运行时 ACK 预留没有生效，或共享盘已耗尽 | 先限制新生产/回放/诊断工作，保留 ACK/控制和磁盘安全余量；恢复后用原 token 重试 |
| Read 返回少于消息上限 | 有界新投递工作片或冷页边界提前结束，已返回 token 均已持久签发 | 继续使用同一 consumer 拉取，按实际批次处理；若返回 Unknown，分别查 lease/pin 到期、WAL sync 和 quorum 阶段，不通过增大 TTL 或跳过 offset 掩盖 |
| `IdentityBudget` | 活跃身份和不可复用墓碑达到冻结容量 | 停止创建；保留现有消费/ACK。不要删除墓碑或复用 UUID；需要评审可证明的身份退休/组迁移 |

## 指标与阶段口径

纯状态机不读取时钟或初始化全局 exporter。运行时在边界计时并使用缓存的有限标签句柄：投递准入、attempt 持久完成、正文读取、发送、SDK 接收、业务等待、ACK 持久完成应分别统计。手动 ACK API 只能观测交付到 ACK 调用的等待，不能宣称它就是 handler duration。`accepted` 与 `already_acknowledged` 的持久成功计数在 Raft 返回后增加；纯 apply 次数不是成功提交数，重试可能产生多个请求但只有一个逻辑 ACK。

建议公共 registry 的有限增量如下；接入运行时前这些是指标契约，不是当前导出端点已经提供的名称：

| 指标 | 类型/单位 | 有限标签及 series |
| --- | --- | --- |
| `tidemq_subscription_delivery_attempts_total` | Counter，attempt 次数 | `kind=new,retry`，2 series |
| `tidemq_subscription_ack_results_total` | Counter，token 结果数 | `outcome=accepted,already,stale_generation,stale_attempt,invalid_token,rejected`，6 series |
| `tidemq_subscription_blocked_total` | Counter，准入拒绝次数 | `reason=window,in_flight_messages,in_flight_bytes,receive_barrier,attempt_limit`，5 series |
| 通用 `operation=ack` duration | Histogram，秒 | 复用全局有限 stage/bucket；不增加 UUID、key 或 token label |

每订阅详细值通过有权限且限频的 diagnose 按需查询；生产聚合 gauges 只使用冻结的角色/有限分组。不要每次 scrape 扫描全量持久数据。PromQL 参考 `sum(rate(tidemq_subscription_ack_results_total{outcome=~"stale_generation|stale_attempt"}[5m]))` 定位接管/重投抖动，`sum(rate(tidemq_subscription_blocked_total{reason="window"}[5m]))` 识别慢前沿；正常重投也会出现旧 token，告警应结合持续时长、业务错误比例与无前沿进展，不能见一次拒绝就报警。

看板至少显示 ACK 持久时延 P50/P99/P99.9、attempt new/retry、旧 token 拒绝率、窗口/在途汇总、冷屏障等待，以及 C 流/WAL/commit 的阶段时延。告警分三类：ACK 错误或延迟持续超 SLO、保护前沿长期不进且容量接近上限、冷热屏障持续停顿。阈值在性能基线和真实 workload 后冻结，不将占位数字当成已验证 SLO。

## 故障验证与限制

`cargo test -p tidemq --test subscription_state` 覆盖逐字节 MAC 篡改、已压缩前沿后的原 token 重试、乱序 ACK、大洞满额仍能 ACK、旧 attempt、显式 session 接管、向后 seek、owner fence、retained floor 排序、冷热前缀/skip 区间、attempt 上限和有界快照。快照 round-trip 是序列化证据；真实磁盘、quorum、掉进程、QUIC 与 SDK 联调必须另有测试结果。

本批不提供自动跳过、自动 DLQ 成功或外部副作用 exactly-once，也不通过降低 RF 继续返回持久成功。控制授权到期后停止新投递与游标变更；有限在途 ACK 仅在原 finalization、当前 fence、deadline 与数据多数派均有效时收尾。无法确认授权或持久结果时保留未知状态，由原身份重试和权威查询恢复。
