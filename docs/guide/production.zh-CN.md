# 生产消息指南

应用围绕逻辑 Topic、可选业务 key 和持久订阅使用 SDK。先按[运行指南](running.zh-CN.md)启动服务，由管理端创建租户、Namespace、凭据与 Topic，再按[SDK 指南](rust-sdk.zh-CN.md)连接。

1. 使用完整 `tenant/namespace/topic` 名称，或在 Client builder 中设置 Namespace。管理端用 `admin.create_topic(name)` 创建 Topic；Producer 不隐式创建资源。
2. 使用 `client.new_producer(topic).build().await` 创建 Producer。SDK 管理生产身份、路由和会话维护，应用不选择物理分区或填写 epoch。
3. 调用 `send(message).await`，或通过有界任务池、FuturesOrdered/FuturesUnordered 并发驱动发送。需要保持批次输入关联时使用 `send_batch`；它允许跨组部分完成，不提供事务。
4. 成功回执包含稳定 MessageId，表示数据 Raft 已持久提交。S3 归档在后台进行，不改变生产成功的完成点。
5. 超时、取消或断线后检查错误的 `outcome_unknown()` 与原 PendingSend。调用原句柄的 `retry()` 恢复，不重新 send 同一内容来替代未知操作。部分批次逐条处理原 outcomes/pending。
6. 先收束应用任务并恢复必要结果，再调用 Producer 和 Client 的 `close()`，检查 CloseReport 或 CloseError 中的未决与未领取结果。

同 key 按同一逻辑 Producer 的准入顺序发送，不同 key 可并行；回执返回顺序不建立跨 key 顺序。SDK 自动跟踪分片切分、合并和权威转移，消息出生身份在迁移后保持不变。同一 Producer 的 clone 共享身份、队列、预算和关闭状态；再次 new_producer 才创建独立实例。

| 状态 | 应用动作 |
| --- | --- |
| 成功回执 | 记录原 MessageId；不要将恢复取得的同一回执计为新业务消息 |
| InvalidArgument / PermissionDenied | 修复输入或授权，保留完整 source 链辅助诊断 |
| ResourceExhausted | 保持本地并发有界；区分明确未准入与已持有原恢复句柄的操作 |
| OutcomeUnknown | 查询或继续原 PendingSend；网络超时不证明未提交 |
| RecoveryExpired | 结果可能已持久化；通过业务幂等键或账本核实，不自动换身份重发 |
| Closed | 先检查原 close 报告；新实例无法自动恢复旧实例的本地账本 |

错误的 ErrorKind 与结果是否未知是两个维度，不能仅凭错误字符串或一次 Unavailable 决定重发。完整 source 链通过 `anyhow::Error::new(error)` 与 `"{error:#}"` 输出。

需要保护未来写入时先创建 Stream/Queue 订阅。零成员保留订阅与积压；Consumer 关闭不会删除订阅。SDK 没有持久 outbox，进程重启后需由应用明确处理业务幂等边界；不承诺跨业务外部副作用的 exactly-once。

排障见[SDK 指南](rust-sdk.zh-CN.md)和[错误诊断](../operations/error-diagnostics.zh-CN.md)。功能测试不证明吞吐、扩容线性关系或已校准容量。
