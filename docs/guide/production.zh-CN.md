# 生产消息指南

本阶段提供服务端生产核心与共享协议，完整 SDK/Gateway 网络用法由后续实现补充。不要将下面的身份流程误认为已经提供可运行的公开客户端示例。

1. 创建 Topic，保存返回的不可复用 Topic UUID。partition 只有分区内顺序，没有跨分区全局顺序。
2. 在有效控制授权下读取 `ProducerStatus` 的 `next_registration_epoch`，用 producer UUID 与该 expected epoch 显式 `OpenProducer`。并发 open 冲突可以重新读取提示；成功后固定此会话身份。
3. 从 sequence 0 开始构造有界批次，使用协议 crate 的 canonical 摘要函数。每个批次固定消息边界、内容与首序号，按消息条数推进下一序号。
4. 只有 durable produce result 才向业务报告成功。收到 timeout、连接丢失、取消或结果未知时，保留原 producer/epoch/sequence/内容，恢复路由和连接后重试同一批次。
5. 重复返回会保留原 offset 和条数。冲突、序号缺口和过期需要按下面契约处理，不能换一个随机身份立即重发。
6. 不再使用会话时显式退休。会话也会在固定期限结束，不因持续发消息而自动续期。期限届满前应停止旧会话新追加，先确定其未决结果，再由业务明确决定新会话及去重边界。

| 服务端结果 | 使用方动作 |
| --- | --- |
| 原结果 duplicate | 按原 offset 接受成功，不重复计算业务吞吐 |
| sequence gap | 对照 expected sequence 与本地未决批次账本；服务端不会缓存未来批次等待补缺口 |
| content/overlap conflict | 停止该会话，检查重试时内容或批次边界是否变化 |
| original result expired | 结果可能已经持久化；按业务幂等键或业务账本核实，不能自动重发成新消息 |
| session expired/retired | 禁止自动重建并复制旧未决消息；新的显式会话属于新的业务生产身份 |
| fenced/control authorization expired | 刷新路由和授权；原业务身份保留，不能把旧 writer epoch 固定带到新 owner |
| capacity rejected | 有限退避并保持本地队列有界；消费、ACK 与回收走各自保留资源 |
| result unknown | 重试原身份或查询权威状态；网络超时不等于没有提交 |

首版 SDK 没有持久 outbox。同一 SDK 进程重连应保留身份和未决批次；进程重启后必须由业务恢复这些信息或明确采用新的业务幂等边界。系统不提供跨业务外部副作用的 exactly-once。

排查重复、过期或吞吐停滞见[生产排障手册](../operations/producer-troubleshooting.zh-CN.md)，权威与资源边界见[生产幂等设计](../design/producer-idempotency.zh-CN.md)。
