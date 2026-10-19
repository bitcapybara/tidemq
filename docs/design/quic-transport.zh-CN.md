# QUIC connection 与 stream 契约

s2n-quic 1.91.0 为唯一实现。首轮默认每实际活跃物理对端/角色/认证作用域/协议代次/流量类别一条连接，不按客户端、分区或 Raft group 建无限内部连接池。

| 类别 | 用途与 stream |
| --- | --- |
| C | 核心控制预建有限常驻 bidi 流，request_id 复用；普通 ACK 采用限额短流；先保障核心流和连接/执行/回复预算 |
| P | 生产批次与原通道回执；producer/epoch/topic/partition 顺序域绑定有界通道，流水发送，回执读取独立 |
| D | 活跃订阅/分区/代次交付，每域独立 credit，一个有效交付通道；流数超额时公平轮换 |
| B | 按需冷回放、恢复和快照，按任务分块、限速/限并发，不每 chunk 开流 |
| R | Raft 非空日志 RPC；节点对复用，控制与快照分别走 C/B，保持库的复制因果与流水线 |

纯生产 SDK 使用 C+P，纯消费 C+D，兼用 C+P+D，冷回放按需建 B。多连接可落到不同 Gateway，逐连接独立认证并绑定同一逻辑 session；连接 ID 不替代全局身份。

同流仍有字节序等待，同连接共享流控/拥塞，多连接仍共享网卡/CPU/磁盘。因此先在提交数据给 QUIC 前有界公平调度，再限制每流、每连接和进程缓冲。握手/连接/stream 创建及排空替换均有额度；MAX_STREAMS 累积额度不因本地 close 立即返还。

同一顺序域不能任意跨流散发。冷 B 切热 D 使用同 generation 的排他屏障 H，SDK 连续接收并有界入队全部 offset < H 后确认，D 才交付 offset ≥ H；它不是业务 ACK，不推进持久前沿。公平流轮换也不等待全部业务 ACK。

accept/read 不等待磁盘或 handler；发送、读取和回执各自有界。QUIC send-ready 总等待可计时，但不能未经验证拆成精确的 stream/connection credit 与拥塞耗时。关闭 0-RTT；Gateway 故障走新握手和原身份重试。

外部 TLS 验证服务端并携带静态 scoped token，内部 mTLS 还需节点/集群/组授权。管理抓取 HTTP 是独立 loopback 接口。指标/trace/文件不决定任何持久成功。

本文件是已批准的实现契约；真实网络、mTLS、CID/LB 和混合流量效果由 T01/T12/T13 分阶段验证。
