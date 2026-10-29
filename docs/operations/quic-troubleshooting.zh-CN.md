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
| 内部 Raft 返回 QuotaExceeded 或期限耗尽 | 原 RPC/frame deadline、Core 执行槽、core 内存余量、dispatch admitted/rejected 与 handler 耗时 | 仅认证 Core Raft 在原期限内等现有两个执行槽；每 stream 最多保留一个已计费待帧。其他类别及内存不足仍可立即拒绝。拒绝累计量不定位具体额度，也不能据此认定 WAL 或磁盘已满；不增加槽位、PeerPool 尝试次数或 hard TTL 掩盖问题 |
| data 满时 C 重认证或 Core 失败 | data/normal/core/handshake 各自余量、旧连接引用、握手并发 | data 满不能解释为 Core 配额耗尽；C 长期 envelope/ACK 仍需 normal，握手有独立小额度。先结束失效连接引用/handler，避免旧 envelope 一直保留；不要让普通工作改用 try_reserve_core 绕过门禁 |
| RSS 不回落但业务 Bytes 已释放 | allocator live/retained、RSS、native TLS/socket 量 | 结合内存排障手册区分真实存活、缓存/碎片与 native 缓冲；transport envelope 不是精确 RSS 计量 |
| Deadline 后下一帧报 Poisoned | 是否发生部分 prefix/body 或 send | 关闭并恢复该 lane 的全部未决请求，沿用原业务身份；不能继续猜测帧边界，超时不等于未提交 |
| RPC 返回 Closed 且 malformed 增长 | `TransportSnapshot.failures` 的 phase、scope、reason、io_kind 与连接/流标识 | 区分 IO、解码、身份校验和关闭事件；先核对同一连接的记录与原请求结果，不能仅由聚合计数认定坏帧，也不能把所有 Closed 当成可重试的业务拒绝 |
| 活跃流正常但连接在固定应用读阈值后重连 | sibling Request/Core lane 是否一直无帧、deadline/malformed 与连接累计计数 | 长寿命 reader 应使用 `receive_next`，空闲 lane 不得触发整连接关闭；`read_idle_timeout`/dispatcher `idle_timeout` 只限已开始帧组装，不通过扩大配置或关闭原生探活掩盖 |
| 退出时还有未决操作 | 准入/在途数量、业务持久结果 | 先停止准入并有限排空，按身份查询/重试未知结果，再关闭 Session 和 listener；QUIC finish 不作提交证明 |
| 退出时 native I/O 或端口释放超时 | 原关闭期限、原生任务错误链、UDP 端口持有者 | 检查业务是否先停止、handler/session 是否排空；当前实现持有并回收原生 I/O 主任务，验证 RX/TX socket 释放。不能忽略错误、等待故障连接退避或用 SO_REUSEPORT 绕过 |

## 日志与指标口径

默认不记录每消息正文或每次拒绝日志。认证成功只有固定格式 debug 事件；token、私钥、HELLO payload 和用户正文不进入事件。临时开启 debug 使用基础日志管理的期限/轮转/限量能力，先观察聚合计数，不将整个 native 事件流永久提升为生产 info。

`connections_total`/`streams_total` 是累计准入量。`active_connections` 随 ConnectionState 创建/最后引用释放计数，缓存/RPC 可暂留已原生关闭的连接；它不能单独证明网络仍存活。`frames_sent_total` 是已入队应用帧，不能作为持久成功。`bytes_sent_total` 含 4 字节前缀，接收统计不含前缀。`malformed_total` 包括 IO/读帧/解码失败；授权拒绝另进 rejected。`send_wait_seconds_total` 含 send-ready 与 enqueue 等待，不能标注为精确拥塞或 credit 时间。丢包通知包含 MTU probe；PTO 是最近一次 recovery 样本，不可跨连接相加当作业务重试次数。

`TransportSnapshot.failures` 保留累计分类次数 `total`、命中日志采样条件的次数 `logged`、最近八条记录 `recent` 和最后一条 RPC 层记录 `last_rpc_failure`。记录使用固定阶段、范围、原因和 IO 类别，可附带连接、流、流量类别及用途；不保存错误展示文本、正文或凭据。日志 `event=transport_failure` 在最初四次及后续二次幂次数记录，每个 endpoint 生命周期最多 65 条；`logged` 不代表日志已输出或持久化，实际写出仍受 subscriber、过滤和日志队列影响。最近记录和最后一次 RPC 事件用于定位失败分支，不是完整时间线，也不能单独证明因果首错；结合原请求标识、已保留日志和业务持久结果判断。

空等首字节本身不增加 `deadlines_total` 或 `malformed_total`；原生 idle 关闭带来的 IO 错误仍可计入后者。收到首字节后的慢 prefix/body 仍有有限组帧期限；超时或取消导致 poison，不执行外层 Deadline 重试来重用未知字节边界。新入口只在首字节之前允许安全取消。握手/短 ACK/单次读取仍用 `receive(absolute_deadline)`；客户端原请求取消仅移除其 waiter，不能因此关闭其他共享请求。

发送在尝试写入 native 队列前到期时，保留健康 lane，只结束该帧并计入 `deadlines_total`；RPC 请求与服务端回复 writer 都跳过该过期帧，未发送的回复不会触发 flush 成功通知。`FrameSender::is_usable()` 只说明帧边界是否仍安全，不证明业务未提交或已提交，已有 Unknown 仍须沿原身份恢复。尝试 native 写入后发生取消或失败，仍须 poison 和关闭对应 lane，不能继续拼接可能已部分写出的帧。原请求截止不重置，剩余不足一毫秒不向上延长。

Core Raft 的执行槽等待发生在完整帧接收并授权之后，沿用该 frame 的绝对 deadline。等待由既有 reader 持有 transport-charged ReceivedFrame，不增加任务、工作队列、字节或槽位额度；到期、连接关闭、服务关闭会归还待帧，已 owned handler 继续原有有限 drain。两条 lane 都等待时，同流后续 CANCEL 可能延至前帧到期后才继续处理，不承诺无队头阻塞。完整帧已接收后的等待结束，不等于部分 prefix/body 取消，也不因此 poison 健康共享流。

原生 idle 关闭空闲 Produce 连接时，其 lane 的 IO 读取错误也可能增加 malformed，而其他 Control 连接仍正常。应结合分类记录中的流量类别和连接标识判断影响范围，不能仅由聚合 malformed 推断坏帧或活跃连接误断。

## 本地回归

```sh
cargo fmt --all
cargo clippy -p tidemq --tests --no-deps -j 2
cargo test -p tidemq --test quic_transport -j 2 -- --test-threads=2
cargo nextest run -p tidemq --test server_dispatch_internal --test server_dispatch --profile ci --locked
```

后一个命令覆盖真实内部 mTLS 准入/关闭边界及 dispatcher 回归，具体断言与证明范围见 [Raft 网络接入](../design/raft-network.zh-CN.md#实际验证)。

测试只监听 loopback 随机端口，内存生成独立证书与身份，不依赖固定证书目录、生产凭据或互联网服务。TLS、native s2n、socket 真实参与测试。当前未测真实网络丢包/带宽、Linux 生产宿主和四层 LB/CID；正式吞吐/尾延迟应遵循开发计划中的固定总资源、固定观测开销矩阵。

证书轮换采用新旧 pin 并存、切换、排空、确认、撤销顺序。升级 s2n-quic 或 native TLS 时必须重跑全部拒绝测试：DER 证据 API 是 doc-hidden，编译成功不能证明身份链路仍然安全。
