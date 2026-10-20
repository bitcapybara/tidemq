# 持久订阅与逐消息 ACK

## 权威与完成点

每个 Topic UUID、partition 的 `SubscriptionState` 与生产状态放入同一个数据 Raft 组。Broker 只调度，不持有唯一 ACK 结果。状态机的 `apply` 返回值只是确定性业务结果；只有数据组的 Raft 提交、持久存储及应用完成后，才能向客户端返回持久 ACK 成功。不能从 QUIC ACK、发送完成、reset、请求超时或本地 ACK 入队推断业务结果。

投递依次经过授权准入、持久记录 attempt、读取已提交正文、发送、SDK 有界接收、业务处理、持久 ACK。attempt/token 在客户端可见前先提交。丢失投递回包以同一个操作身份重试，返回相同 token；丢失 ACK 回包以原 token 重试，通过恢复后的权威状态返回已确认结果。

`DataContext` 的 `committed_end`、`retained_floor` 必须来自同一次数据组应用中的生产状态，不能采用 wire 中自称的边界。所有权 fence 在同组组合入口检查；运行时还必须在提交前检查 principal 权限、控制授权有效性、总 deadline 与 quorum。创建、删除、seek、Claim 属于控制变更；新投递和 receive barrier 属于投递准入。有限 ACK 收尾使用所有权模块的有界 finalization 能力，不能因数据队列或 Topic 配额耗尽而取消保留的 ACK 准入。

## 身份、代次与接管

Topic 和 subscription 使用不可复用 UUID；partition 在 Topic 身份内稳定。订阅删除保留永久有界墓碑，不能同 ID 重建。`max_subscription_identities` 同时计算活跃项和墓碑；达到上限时拒绝新身份，不悄悄忘记旧身份以重新接纳。这是首版明确的容量边界，后续若需要持续大量创建/删除，应设计带证明的身份退休和组迁移方案。

| 事件 | cursor generation | assignment generation | 会话与数据 |
| --- | --- | --- | --- |
| 创建 | 1 | 0 | 未分配，从指定 retained offset 开始 |
| 同 session、同冷边界的 Claim 重试 | 不变 | 不变 | 返回当前分配，不抢占其他 session |
| 首次 Claim / 显式替换 session | 不变 | 加 1 | 保留 ACK 空洞、attempt；重置当前接收/签发前缀 |
| 数据所有权新 fence | 不变 | 加 1 | 清空 session，保留 ACK 与 attempt，必须重新 Claim |
| seek，包括向后 seek | 加 1 | 加 1 | 清空旧窗口与 session，之后必须重新 Claim |

新 session 的普通 Claim 仅在当前未分配时成功。接管已分配订阅需要显式提供被替换的 session，并对 cursor/assignment 做 CAS；受权 handler 决定谁可以接管。SDK 默认不得静默抢占仍活跃的 consumer。租约失效判定与强制接管权限属于运行时，纯状态机不会相信客户端的 `force=true`。

首版 `delivery_generation` 等于持久 `assignment_generation`，wire 保留显式字段并要求相等。不支持同一 assignment 内独立重启另一套 bulk 代次；重连沿用原分配和 offset，或者执行显式 Claim 接管。旧 cursor、旧 assignment、旧 delivery generation 的块、barrier、ACK 不能更新新分配。

## Token 编码、恢复和幂等

v1 token 固定 101 字节，整数均为大端：版本 1 字节、Topic UUID 16、partition 4、subscription UUID 16、cursor generation 8、assignment generation 8、offset 8、attempt 8、MAC 32。attempt 在状态中为有上限的 `u32`，wire 字段保留 8 字节并拒绝超范围值。MAC 使用带域分隔的 keyed BLAKE3，比较使用 `blake3::Hash` 的常量时间相等实现；不能改为普通 byte slice 比较。[版本化 API](https://docs.rs/blake3/1.8.7/blake3/fn.keyed_hash.html) 明确支持这种 MAC 用途。

每个订阅创建时由受信服务端生成随机 32 字节 signing key，并随创建命令、复制状态及快照持久保存。恢复必须保留原 key；丢 key 后不能另造 key 并继续宣称旧 token 可重试。首版不自动轮换；需要撤销时按受控订阅/代次操作处理，不能把轮换当成无影响的运行时配置更新。key 与 token 的 `Debug` 全部脱敏，状态 `Debug` 只显示计数与版本。token 不进入普通日志、指标 label 或 trace 属性。Raft 目录、备份和快照含 key，应采用存储层访问控制；这里没有实现静态加密或密钥服务。

ACK 首先核验当前 cursor/assignment，再验证 token MAC 和 Topic/partition/subscription 身份。未确认槽位只接受当前持久 attempt；超时后的重投必须先将 attempt 加 1 并提交，旧 attempt 不能确认新 attempt。同代次中，已经被持久 ACK 的槽位或连续前沿之前的签发 token 返回 `already_acknowledged`，无需无界保存每个成功 token。旧 attempt 在消息已经由新 attempt 确认之后，也只能得到“消息已确认”的只读结果，不能造成另一条 ACK 状态变化。

ACK 批次全部预验证后原子应用，包含坏 token 时整批拒绝；批内重复 token 分别计入 `accepted` 与 `already_acknowledged`。seek 的最后一个 operation/result 被持久保存，重试不会再提升代次；更早的 seek 遇到后来操作时得到代次不匹配，不能用旧操作回滚当前游标。

## 有界窗口、重投与保留

每个订阅维护连续确认前沿，以及前沿到 `next_delivery_offset` 的有界槽位表。未 ACK 消息与已 ACK 空洞共用槽位，ACK 原地标记后移除连续已确认前缀。慢前沿使窗口满时停止新投递；已有 ACK 不需要分配新洞，因此仍可推进并释放前沿。没有另一个无界 bitmap、成功 token 列表或 retry 队列。

每订阅与每分区同时限制窗口槽数和在途字节，每订阅另限制在途消息、attempt 次数和 ACK 批大小。字节数包括投递消息的 key/body 等业务计费字节，实际发送/解码/token 的缓冲还必须加入运行时全进程 MemoryBudget；状态中的逻辑字节计数不是 RSS。所有默认值是功能验证起点，性能阶段需要在同总预算下实测。

默认 `max_ack_batch=256` 是状态硬上限，C 类默认 frame 为 16KiB 时，101 字节 token 的实际可发送条数更少。SDK/handler 必须同时按编码后的 frame 字节预算切批，不能只按 token 数量切批。此限制不允许把普通 ACK 挤入核心控制流的保留执行额度。

retry 索引按 deadline/offset 排序，查询只取有界数量，不为每次 poll 扫描全订阅。时间来自受信 handler 写入命令，状态机不读取时钟；时钟跳变可影响重试时机，不影响 attempt/token 的安全性。达到 attempt 上限后消息仍未确认并继续保护保留前沿，从自动 retry 索引移除以免饿死其他工作。不会跳过失败消息，也不会将 DLQ 写入视为 ACK；人工 replay/seek 必须显式授权。

`protected_before()` 通过有界前沿索引直接返回所有活跃订阅的最低保护 offset。创建、删除、seek 与 floor 更新必须在同数据组排序。新建/seek 不能越过 retained floor，floor 不能越过任何活跃保护前沿。全局元数据的 desired 删除配置不等于数据组已删除，不能单凭该配置解除保护。多个分区部分完成时仍保留未完成分区的保护。

## 冷热切换

Claim 冻结冷读交界 `H=cold_before`。冷段使用 B，SDK 的连续有界接收屏障未达到 H 时禁止 D 新交付 `offset >= H`；屏障达 H 后使用 D。屏障独立于业务 ACK，慢业务不能阻止已接收数据的流公平轮换。

`next_delivery_offset` 是本 cursor 的历史签发上界；`issued_before` 是本次 assignment 已重新签发的连续前缀。接管后 `issued_before` 退到持久 ACK 前沿，不能用旧 assignment 曾经发送过的最大 offset 证明新 SDK 已经收到。SDK 必须验证完整代次 tuple，并在容量已预留、正文已经入队后推进连续接收前缀；QUIC ACK 和收到最大 offset 都不是该证据。

接管时保留的持久 ACK 空洞通过有界 `AcknowledgedRange {start,end}`（排他 end）显式输出。它们可以填充 SDK 连续接收跟踪中的已完成范围；未收到正文的非 ACK 空洞不能跨越。`ReceiveBarrier` 必须单调且不超过当前 `issued_before`。seek、Claim 或新 fence 后的迟到块不得进入新队列，迟到屏障不得改变新代次。

同代次、session 与相等接收位置的屏障可幂等重放。SDK 对该屏障的 Unknown 使用相同 operation ID、不同 wire request ID 在原 deadline 内有限重试；不能把这个规则扩大为任意控制命令的 Unknown 重试。屏障确认之前不发起后续热读取，耗尽时返回原 Unknown。

## 本批边界

DataApplication 已在真实磁盘/Raft 中组合生产、所有权与订阅状态，SDK 通过 QUIC 完成热/冷投递与持久 ACK；没有第二套 Broker ACK 权威。底层状态、实际 RustFS、分角色 Runtime 和独立 OS RF3 故障分别有[行为证据](../testing-benchmarks.zh-CN.md)。默认至少一次，同 key 串行由 SDK 调度；真实三区与外部业务副作用不在这些本地证据范围。

默认 Parallel 不承诺跨读取批次的全局 offset 交付顺序。持久签发后回包丢失时，下一批可能先交付更高 offset；原前缀仍受保留前沿保护，并在 attempt 租约到期后重投。SDK 只对当前缓冲区排序，连续 received/ACK 前沿不能因此跨洞。KeySerial 额外等待连续接收覆盖并限制 busy key；正确性测试应验证完整身份集合、正文和连续 ACK 前沿，不把 Parallel 的首次交付顺序当作缺失证据。

SDK 在 owner fence 后保留原逻辑 session，从新鲜状态恢复分配；自动恢复不抢占其他活跃 session，不修改旧 Delivery/token。KeySerial 的失败 offset 跨 assignment 保持阻塞，显式重试或 seek 才改变业务决定。H 保持冻结的 B/D 交界；实时 D 的 received/issued/持久 ACK 前沿可以超过 H，不能据此拒绝合法响应，也不能把 H 推高来掩盖错误。
