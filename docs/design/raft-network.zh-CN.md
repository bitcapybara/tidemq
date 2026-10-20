# Raft 的 QUIC 网络接入

QuicNetworkFactory 使用真实 s2n-quic、内部 mTLS 与公共 RPC 多路复用。PeerPool 由一个节点的所有组共享，按实际对端/角色/认证身份/协议/类别复用连接；配置地址与证书绑定节点表是拨号依据，不相信 BasicNode 中任意地址。每次新的逻辑 RPC 使用新 request_id，响应须匹配 request_id、operation_id 和类别；RaftResult 另须匹配内层 rpc_id。

严格关联的通用 ErrorReply 是合法拒绝，不是响应串线，不因此关闭所有组共享的 RPC。仅当完整收到 `QuotaExceeded + Rejected + retryable`，且 transport 已移除该次 pending waiter 后，才允许复用原 request_id 与 payload owner 做有限退避重试：最多 8 次尝试，等待从 2 ms 倍增至 32 ms 封顶。所有尝试使用同一硬截止，线上剩余期限递减；不增加服务端并发额度。Unknown、超时、断连、鉴权/格式错误、非 quota 或不可重试拒绝不进入此循环；关联或内层身份损坏仍关闭共享 RPC。

投票与空 AppendEntries 走 C；非空日志走 R；snapshot chunk 走按需 B。R 默认共享 4 条请求流，B 默认共享 2 条请求流；同一组稳定散列到一条 lane，其他组仍能独立前进。两者可配置 1—64 条有限流，不随分区数量增长。B 的分块复用原连接与有限流，不每 chunk 创建连接/流。非空 AppendEntries 伪装 C、相反类别错配、越权 group、错误集群/角色/发送节点一律拒绝。RPC 的 vote.leader_id 必须等于已经由 TLS pin 认证的节点。接收方只允许当前有效成员配置的 voter 发起请求；空组启动期只接受显式 bootstrap voter。Broker/Gateway 的角色不能调用 raw Raft RPC，业务命令要经过单独的语义授权入口。

数据组与 metadata 组使用同一非零 u64 group_id 体系。网络不改变 OpenRaft 的 term、prev-log、commit、vote 与 snapshot 因果；提前的空心跳不会凭空应用日志。joint membership 中的 voter 使用库的有效配置，控制能力仍沿用 ownership 模块的保守暂停策略。

opaque RPC 使用固定 postcard 格式；AppendEntries 在分配 entries 前检查上限 128，逐项验证连续索引和 term，拒绝 trailing bytes。超大多条批次使用 OpenRaft 的 PayloadTooLarge entries hint 缩小重试；单条命令本身必须在业务准入处满足 frame 限制。snapshot chunk 在反序列化分配前校验最多 256KiB，配置 OpenRaft snapshot_max_chunk_size 不得超过它，offset/done 还与外层 chunk envelope 一致。升级 RPC/disk 应用模型需要版本与兼容审查。

编码前预留保守载荷容量，最终 payload 通过 Bytes owner 持有额度；send/receive 仍继续执行公共传输的预算约束。C 编码与核心流保留专门容量。Replicate 阶段记录真实 RPC 耗时，不将一次 transport send 当作 quorum commit。网络失败、丢回包和超时不会生成假的成功响应。

## 实际验证

tests/raft_quic.rs 使用临时磁盘目录、独立 CA 与逐节点证书、随机 UDP 端口和真实 OpenRaft：RF3 自动换主、隔离旧 leader 时新写不能确认、旧副本磁盘重启追赶、丢复制回包后原业务身份重试，以及新增 learner 的 B 快照传输与成员变化。隔离通过受控 RPC handler 拒绝双向通信，未模拟操作系统丢包或真实可用区断电。

真实认证 QUIC peer 还验证合法拒绝不会关闭 sibling RPC、未知或非法错误仍严格拒绝、Quota 退避不重置硬截止。三 combined 进程的四队列生产/消费/元数据并发回归同时验证唯一交付与持久 ACK 前沿；它保留了触发共享 Core 控制额度拒绝的真实并发，不以缩小负载替代修复。

强制选举可能在旧 leader 租约有效期内被正确拒绝；故障测试启用真实自动选举并等待剩余 quorum 达成新 leader。单机测试不构成恢复 P99 或三区证据。

## 业务分派

server::run 分别保留 Core、ACK、数据执行额度，接收、业务执行和回复写入各自有界。已准入任务由统一 executor 持有，不因单个 stream reset 就宣称撤销；队列/回包字节与任务数均有限。每个 P 流的 SubmissionOrder 一直保留到业务实际入提交队列，之后释放顺序再独立等待持久回执。不能在整个 durable ACK 等待期间持有它，也不能在进入业务提交前释放。

短 ACK 流发送 FIN 后，服务端继续排空已准入回复，再结束发送方向。关闭先停新准入、有限排空，再关闭连接。分派器记录有限标签的准入、拒绝、执行失败、在途任务及日志抑制指标；同类 handler 警告最多每秒一条，失败计数不采样。所有 Collector 通过单一进程根 Registry 导出，避免重复前缀和多个 OpenMetrics EOF。tests/server_dispatch.rs 验证两个生产 handler 都等待结果时 Core 与独立 ACK 仍能前进、同 P 流准入顺序正确及 ACK 半关闭后可收到回包。

完整服务进程、SDK 真实业务联调和跨节点控制权威已有独立[运行时证据](../testing-benchmarks.zh-CN.md)；长期公平调度、真实 LB/CID、跨可用区与生产压力仍按 T06/T10/T11/T12/T13 单独验收。
