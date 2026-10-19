# QUIC 使用与排障

真实传输、服务运行入口与 SDK 恢复已接入。使用者先按[运行指南](../guide/running.zh-CN.md)启动，并查看实际节点的 `/metrics` 与 `/diagnostics`；下面的底层接入步骤面向嵌入共享传输的维护者。真实 LB/NAT/CID 和三区仍须目标环境验证。

## 接入顺序

1. 使用 Tokio 读取受保护的 PEM 文件，构造 `TlsIdentity`。服务端配置 `ServerTls` 和 `ServerAuthorization`：外部使用 `TokenGrant`，内部使用 CA roots、精确证书名称和 `NodeGrant`。私钥文件限制为服务用户可读，不提交真实 token/私钥。
2. 创建进程共享 `MemoryBudget`；所有 endpoint 使用同一预算实例，不能各自复制配置后创建独立总额度。控制预留的 1/2 为 Core、1/4 为 handshake，其余总额度为 normal；数据还经过 data 门。设置 `TransportConfig`，检查连接 envelope、应用队列、各额度与实际 RSS 上限。
3. `Server::bind` 和 `Client::bind` 在 Tokio runtime 内启动。客户端提供正确 `PeerTarget` 服务名与角色，内部还必须填精确 cluster_id/node_id；`ClientTls.expected_nodes` 的双 pin 也只能命中该目标身份。按 C/P/D/B/R 分别 `connect` 或 `pooled`。服务端同一端口可验证不同流量类别，但内部与外部认证策略应使用不同受控监听。
4. 取得 `Session` 后，唯一 dispatcher 取走并持续驱动全部 `take_initial_streams`；C 默认 2 条。独立运行 `accept`，每个新流拆分发送/接收，再安排有界业务处理与响应队列。不要创建无限 Tokio 任务。
5. 请求保留 operation_id，单次请求使用独立 request_id；Frame 带有剩余期限，跨跳扣减。生产结果在原 P lane 回包，不能依靠另一个 C 连接完成路由。
6. 通过固定前缀注册 endpoint 的 `metrics.clone()` 到管理用 `Registry::register_collector`，注册一次即可。管理抓取只导出固定快照；将同一个 endpoint 的 `RecoverySnapshot` 作为受控只读诊断，节点/连接 ID 不作为时序标签。

## 常见现象

| 现象 | 先看证据 | 排查与处理 |
| --- | --- | --- |
| 无法握手 | TLS 失败计数、native 错误链、UDP 连通 | 检查 UDP 端口/LB、防火墙、CA/证书有效期、SAN 与 PeerTarget.server_name、ALPN；不关闭验证来“修复” |
| mTLS 握手成功后 HELLO 断开 | 拒绝计数与已配置 pin/node/cluster/role/class | 核对 leaf DER pin、精确名称白名单、声明与授权；同 CA 的另一个证书不能冒充节点 |
| ACK 新流准入失败 | Core 活跃数、ACK/总流额度、STREAMS_BLOCKED | 保留并驱动全部 Core；丢流后先结束旧 handler/reader，再修复 Core，最后恢复普通 ACK；不要无限开流 |
| send 等待增长 | send_wait_seconds、PTO/丢包、RTT/cwnd、三类 BLOCKED 通知 | 区分慢接收、流/连接 credit 和拥塞的证据；没有精确时间拆分时保持“send 等待”口径 |
| 大帧卡住 | stream/window、读任务和全局额度 | 保证读取渐进释放 QUIC credit，不在读完整大帧前等待业务 ACK；frame 可以大于 window，禁止按最大帧一次性扩大所有连接窗口 |
| 吞吐正常但尾延迟升高 | C/P/D/B 混合比例、共享 CPU/磁盘、WAL 队列 | 分离 bulk，限制准入和已入 native 队列的字节；多连接仍共享机器资源，不能只增加连接数 |
| 接收/发送返回 Memory/Admission | 全局 available_bytes、活跃连接与 envelope、payload 生命周期 | 检查持有的 ReceivedFrame/Bytes clone、未消费队列和 bulk 并发；不能重新建一个 MemoryBudget 绕过限额 |
| data 满时 C 重认证或 Core 失败 | data/normal/core/handshake 各自余量、旧连接引用、握手并发 | data 满不能解释为 Core 配额耗尽；C 长期 envelope/ACK 仍需 normal，握手有独立小额度。先结束失效连接引用/handler，避免旧 envelope 一直保留；不要让普通工作改用 try_reserve_core 绕过门禁 |
| RSS 不回落但业务 Bytes 已释放 | allocator live/retained、RSS、native TLS/socket 量 | 结合内存排障手册区分真实存活、缓存/碎片与 native 缓冲；transport envelope 不是精确 RSS 计量 |
| Deadline 后下一帧报 Poisoned | 是否发生部分 prefix/body 或 send | 关闭并恢复该 lane 的全部未决请求，沿用原业务身份；不能继续猜测帧边界，超时不等于未提交 |
| 退出时还有未决操作 | 准入/在途数量、业务持久结果 | 先停止准入并有限排空，按身份查询/重试未知结果，再关闭 Session 和 listener；QUIC finish 不作提交证明 |

## 日志与指标口径

默认不记录每消息正文或每次拒绝日志。认证成功只有固定格式 debug 事件；token、私钥、HELLO payload 和用户正文不进入事件。临时开启 debug 使用基础日志管理的期限/轮转/限量能力，先观察聚合计数，不将整个 native 事件流永久提升为生产 info。

`connections_total`/`streams_total` 是累计准入量，不是当前活跃数。`frames_sent_total` 是已入队应用帧，不能作为持久成功。`bytes_sent_total` 含 4 字节前缀，接收统计不含前缀。`malformed_total` 包括 IO/读帧/解码失败；授权拒绝另进 rejected。`send_wait_seconds_total` 含 send-ready 与 enqueue 等待，不能标注为精确拥塞或 credit 时间。丢包通知包含 MTU probe；PTO 是最近一次 recovery 样本，不可跨连接相加当作业务重试次数。

## 本地回归

```sh
cargo fmt --all
cargo clippy -p tidemq --tests --no-deps -j 2
cargo test -p tidemq --test quic_transport -j 2 -- --test-threads=2
```

测试只监听 loopback 随机端口，内存生成独立证书与身份，不依赖固定证书目录、生产凭据或互联网服务。TLS、native s2n、socket 真实参与测试。当前未测真实网络丢包/带宽、Linux 生产宿主和四层 LB/CID；正式吞吐/尾延迟应遵循开发计划中的固定总资源、固定观测开销矩阵。

证书轮换采用新旧 pin 并存、切换、排空、确认、撤销顺序。升级 s2n-quic 或 native TLS 时必须重跑全部拒绝测试：DER 证据 API 是 doc-hidden，编译成功不能证明身份链路仍然安全。
