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

accept 不等待磁盘或 handler，reader 不直接执行这些工作；已认证原生 Raft Core reader 可在原 frame 绝对 deadline 内等待现有执行槽，其他类别保持立即尝试准入。发送、读取和回执各自有界。QUIC send-ready 总等待可计时，但不能未经验证拆成精确的 stream/connection credit 与拥塞耗时。关闭 0-RTT；Gateway 故障走新握手和原身份重试。

外部 TLS 验证服务端并携带静态 scoped token，内部 mTLS 还需节点/集群/组授权。管理抓取 HTTP 是独立 loopback 接口。指标/trace/文件不决定任何持久成功。

## T01 实际接口与边界

传输实现位于独立 tidemq-transport crate，服务端通过 tidemq::transport 重导出。MemoryBudget/MemoryLimits 也由该 crate 提供，root 的 MemoryConfig 转换保持原调用兼容。SDK 直接使用公共传输，不依赖服务端/OpenRaft/S3，也不引入全局分配器或日志设施。

`crates/tidemq-transport` 已实现真实 s2n-quic UDP socket 与 TLS 1.3 连接。`Server::bind` 返回有界接受器，内部并行完成 HELLO，避免一个慢握手阻塞其他连接；`Client::connect` 建立独立连接，`Client::pooled` 在同一 runtime 内复用。池键包括实际拨号地址、TLS 服务名、两端角色、凭证摘要、逻辑 session 和连接类别；协议版本由固定 ALPN 绑定。不同认证作用域和 session 不共享连接，B 只有调用时才建立。一个 LB 地址背后的物理后端由该次连接固定；CID 路由与真实四层 LB 部署仍待 T10/T12 验证。

`Session::connection` 返回可克隆的开流句柄，`take_initial_streams` 将初始流移交给唯一 dispatcher。C 默认先建 2 条 Core 常驻流，其他类初始流为 Request。应用须保存并驱动全部初始流；丢弃其中一条会使本端 Core 活跃数不足，暂停新增 ACK。`Session::accept` 与读帧/处理业务独立运行，每个流由调用方安排有限 reader/handler。不要在 accept 循环等待磁盘、业务处理或持久 ACK。

初始流直接交换 HELLO/HELLO_RESULT；后续 bidi 流先交换 2 字节 prelude：发起方 `0x71,purpose`，接收方准入后回复 `0xa1,purpose`，purpose 为 Core=1、Request=2、Ack=3。它是当前 ALPN 的一部分。所有流仍使用统一 Frame；ready 只说明开流准入，不表示后续数据成功或持久化。

`Connection::open` 对 Core、普通和 ACK 使用独立额度。C 拒绝 Request，其他连接拒绝 Core/Ack；Core 不接收 ACK_BATCH/ACK_RESULT，Ack 只接收 ACK、ERROR/CANCEL。普通 ACK 在本端 Core 数量达到配置值后才准入，远端也独立检查。QUIC MAX_STREAMS 没有业务类别预留能力，因此先建控制流、限制诚实发送方的普通开流；恶意对端占满自己的 wire credit 不会被解释为全局控制保证。Core 丢失后应先清理旧 reader/handler，再修复流；若远端仍持有旧额度则排空/替换连接。

`FramedStream::split` 提供独立 `FrameSender` 和 `FrameReceiver`。`send` 在本地 send-ready 后重新缩减剩余期限并入队，保留发送流水线，不为每帧等待 native flush/QUIC ACK。`receive` 在首字节到达后开始扣减该帧预算，读 body/解码后再扣减，空闲等待不消耗下一请求的期限。调用方跨跳传递同一剩余预算和本地 deadline，不重置原操作的总期限；网络单向时间不可由本地单调时钟精确测出，端到端超时及结果恢复仍由 SDK/业务层保证。

`FrameReceiver::receive(absolute_deadline)` 保留握手、短 ACK 和单次读取的绝对收帧期限。RPC/dispatcher 的长寿命 reader 使用 `receive_next(assembly_timeout)`：可持续等待首字节，从首字节到达时才锚定有限组帧期限，prefix/body/解码时间仍扣除 peer 的剩余请求预算。兼容保留的 `RpcConfig.read_idle_timeout` 与 `DispatchConfig.idle_timeout` 字段现在限制已开始帧的组装时间，不以未使用 sibling lane 的空闲判定连接失活。真正连接空闲、失活与退出仍由原生 connection idle、认证探活和 owned shutdown 控制；没有扩大这些期限或关闭探活。空等首字节不增加 malformed/deadlines；空等期间取消不会消费字节或 poison，首字节之后取消或组帧超时仍使整 lane 失效。

单请求取消使用 CANCEL Frame 和原 request_id。取消/超时发生在部分读写时，lane 会 poison，不允许从未知字节边界继续；它只允许整 lane 的未决请求按原身份恢复。`finish` 是流结束，`reset` 是整 lane 失效，两者均不能撤销已提交操作。业务结果恢复、producer/partition 顺序调度、冷/热屏障和 OpenRaft RPC 因果由调用层实现，相关行为以 SDK、运行时和持久组合测试为准。

正常帧只入队以维持流水；可信节点的 Stop/Reconnect 控制回复可显式请求 `FrameSender::flush(deadline)`，等待原生 stream 全部在途字节得到对端认证 ACK 后，才执行 `WireService::response_sent` 关闭动作。回调保留原认证上下文、请求与响应，全部受有界回复队列及原总期限约束；普通请求默认不触发 flush。只有响应成功发送和 flush 成功才通知，不能在 handler 刚返回时关闭 endpoint，也不使用固定 sleep。该传输收讫仍不是业务持久成功，Stop 的权限、状态与提交前置由运行时另行检查。

## 编码复制与缓冲区所有权

接收路径使用带准入 owner 的 `Bytes` 解码，Prost 的 bytes 字段继续共享原缓冲区；最后一份 payload 视图释放前不能归还底层额度。`FrameSender::send_owned` 让 RPC writer 移交整个 Frame。payload 至少 16 KiB、具有唯一所有权且完整保留容量能取得额度时，编码拆成 metadata header、原 payload 和 timeout trailer，交给 s2n-quic `send_vectored`，消除第二次整帧 payload 复制。线上字节与原连续编码完全一致。

唯一 offset 切片先恢复原分配容量，按该容量及 metadata 实际容量的保守上界计费；三个片段共享一份许可，native QUIC 或任意一字节 clone 仍持有它时不能提前归还。小消息、共享/未知 owner、保留容量过大或无法取得完整额度时，使用原有有界连续编码。借用式 `send(&Frame)` 保留兼容行为，不据此承诺移交所有权。

该优化仅减少应用层的一次复制，不消除 Protobuf payload 构建、TLS 加密、QUIC 包构造或内核复制。编码探针发现 128 B 分段路径更慢，因此保留小帧路径；实际吞吐与尾延迟以 RF1/RF3 对照为准。rkyv、FlatBuffers、FlexBuffers 的适用位置、校验和格式演进限制见[格式调研](../research/zero-copy-formats.zh-CN.md)，当前不改变 wire/WAL/归档格式。

SDK 的应用操作使用 `RequestContext` 与 `CancellationToken` 管理取消，配置超时和显式绝对期限取较早者；内部每一跳仍通过有限剩余预算执行。取消已发出的调用可能得到 Unknown，不能把 token 取消视为业务回滚。SDK 与底层传输 API 的区别见[SDK 手册](../guide/rust-sdk.zh-CN.md)。

## TLS 与权限

外部监听要求 native TLS 链验证、正确服务名和 tidemq/2 ALPN；客户端使用独立 trust roots，清空系统默认根，不提供跳过证书验证选项。PEM 输入必须是有界 UTF-8 文本，以 `&str` 交给 s2n provider；该版本 `&[u8]` 重载代表 DER，会触发其不支持 DER 的路径。证书链最大深度为 8。

外部 HELLO 的 token 先检查 32—256 字节 ASCII 编码，再对 BLAKE3 Hash 做常量时间 Eq。`TokenGrant` 将 token 绑定 principal、scope 和允许 FrameKind；内部/Raft kind 不能授给外部 token。授权 scope 是已验证上下文，Gateway/Broker 还须依据权威资源映射校验 topic、订阅和管理权限，不能把 scope 字符串当作客户端自报授权。每条连接独立认证同一个 session，不将 socket/CID 当作 producer epoch 或 assignment。

内部监听必须要求客户端证书，验证配置 CA 链及精确证书名称白名单；客户端同样验证服务端 CA、服务名及预期 pin。随后从已成功握手的 TLS 会话获取 leaf DER 的 BLAKE3 pin，配置 `NodeGrant` 显式绑定 certificate_name、cluster_id、node_id、role、allowed_classes 和 allowed_groups。`ClientTls.expected_nodes` 是有限授权表，内部 `PeerTarget` 必填精确 cluster_id/node_id，逐次拨号只接受该目标身份的证书；同一个 pin 不能配置给不同节点身份。不能把已受信任的 node1 当作 node3 的 Raft 回包。PeerHello 仅能声明此授权下的身份，不能扩大它。RaftRequest、SnapshotChunk、InternalCommand 的 group_id 在交给业务前检查；opaque RPC 是否为空心跳、term/prev-log/commit 因果和响应关联由 OpenRaft 适配层校验。内部客户端的本地 role 与 cluster 也须与目标授权一致。

当前 s2n-quic-tls 0.91.0 没有填充 `take_tls_context`。唯一集中封装的证据路径是 `TlsExporterReady.session.peer_cert_chain_der`，上游将其标为 doc-hidden、用于不稳定用途。证据不存在、查询失败或 pin 不匹配一律拒绝内部连接。固定版本不是稳定性承诺，升级必须重新跑有效链、未知 CA、错误名称、无客户端证书、同 CA 未登记 pin、伪造 node/role、越权 group 的真实 socket 测试，不能只确认编译成功。

证书轮换先把新证书的名称/pin 和原节点同等范围同时加入对端授权，再部署新证书并排空旧连接，确认新连接握手/授权与业务恢复正常后撤销旧 pin。不得用 `allowed_groups` 空集合代表全部组；它代表没有组权限。令牌/证书/私钥/正文不进入日志；未通过认证的连接只递增聚合计数，避免攻击制造日志风暴。

## 有界内存与关闭

首个未分类 HELLO 独立限制为 `min(max_control_frame_bytes,16KiB)`。接收先读取 4 字节前缀、验证长度，再取得两倍 frame 长度的全局额度并渐进读取 body；Prost fields 仍由公共 codec 检查。接收 Bytes 的 owner 持有额度，所以 payload clone 也保持计费直到最后一份释放。发送先拿全局额度再编码，native QUIC 持有 Bytes 时也持有其额度，不在仅入队后提前返还。

进程共享 `MemoryBudget` 将控制预留的 1/2 专用于 Core、1/4 专用于 handshake；剩余总额度为 normal，数据还同时经过 data 门。普通 Control、ACK 和长期连接 envelope 也必须经过 normal，不能先占总额度、再让数据吃掉本应保留的 Core 余量。Core 帧收发用 `try_reserve_core`；未分类 HELLO 及其结果用 `try_reserve_handshake`，未认证连接不能消耗 Core 配额。`available_{data,normal,core,handshake}_bytes` 提供固定只读诊断。此保证是内存额度隔离，CPU/磁盘/执行队列仍须各自预留和限流。

每条连接另以 `connection_window_bytes + max_streams × send_buffer_bytes` 保守预留传输 envelope；服务端预认证 envelope 先占独立 handshake，认证后取得对应类别的 normal/data envelope，再释放握手额度；客户端已知目标类别，C 使用 normal Control。数据额度满时，新 C 仍可认证并传递 ACK，不要求关闭已有 D。handshake 配额满只拒绝/超时新握手，不侵占 Core。默认 connection/stream/send buffer 分别为 64KiB/16KiB/16KiB，最大连接 128、握手 16、流 16、Core 2、ACK 8；默认 1MiB handshake 配额会进一步限制同时进入应用 HELLO 的连接数量。它们是可测的起点，未声明为生产吞吐最优值。native TLS、UDP packet buffer、连接元数据及操作系统 socket 内存不等于该公式，必须同时看 RSS/allocator 与进程上限。

连接、握手、stream prelude、已开始帧的读取和发送均有有限数量/期限；长寿命 reader 等首字节保留在同一有限 lane 和 owned task 内，无使用的 uni stream 窗口为 0。应用接收队列满时拒绝/reset 新流，不让 dispatcher 队列无限增长。`Connection::close` 会立即关闭所有流，未决结果可能未知。通用 dispatcher 先用 `Server::stop_accepting(deadline)` 停止接受并取消握手，保留原生 I/O 排空已经接收的 Session 和 owned handler，最后 `shutdown_by(deadline)` 释放 endpoint；关闭监听错误也不跳过业务排空。全部阶段共用原来的总期限。

`Client::close(deadline)` 阻止新连接、关闭缓存和直接连接，并停止及 join 本进程持有的原生 Tokio I/O 主任务。固定版本 s2n-quic 的默认 Provider 丢弃这个 JoinHandle，连接关闭状态又可能保留三倍当前 PTO，故障后的退避不能作为五秒进程关闭的前置等待。传输层仅薄封装公开 Provider，正常业务停止后才取消该主任务；上游 RX/TX 环形队列随之关闭，退出前以有界 socket 重新绑定验证端口确实释放，超时继续返回真实错误。未决 RPC 收到 Closed/Unknown，不能当作未提交。没有延长关闭预算，也不启用 SO_REUSEPORT。这些资源关闭操作均不证明消息已持久提交。

原生 Raft 的 vote/空 Append 心跳在内部 mTLS Core 上使用独立的 2 个执行槽与队列；语义 Core 默认 8、短 ACK 默认 32、数据默认 128。语义线性化控制可能等待 Raft，因此不能占用 Raft 自己的执行槽。仅已认证并授权的 Core Raft 请求在槽满时等待原 frame 绝对 deadline；每 stream 的既有 reader 最多持有一个已由 transport 计费的 ReceivedFrame，不复制 payload、不新增任务或工作队列、不增加字节和槽位预算。其他类别仍立即尝试准入，额度不足即拒绝；PeerPool 八次尝试上限和原 hard TTL 不变。

等待到期、连接关闭或服务关闭释放尚未准入的帧与内存，已 owned handler 继续遵守原有限 drain。两条 Core lane 可能同时等待到各自原短期限，后面的 CANCEL 可在到期释放后继续处理，仍受同流顺序及自身期限约束；这不是消除全部队头阻塞。它们仍共享有限的 Core 帧内存和 CPU；执行隔离不承诺抗无限可信节点流量。真实 mTLS 测试将语义 Core 执行槽全部阻塞，验证同一复用连接上的原生 Raft 控制帧仍能返回；`fd3c01e` 新增的四项真实槽满/到期/关闭回归及性能证明范围见 [Raft 网络接入](raft-network.zh-CN.md#实际验证)。

RPC 每个物理连接拥有一项有界存活观察任务，仅在存在未决请求、短 ACK 流或原生未确认字节时探测。默认间隔和单次期限各一秒，可通过 RpcConfig/SDK ClientConfig 的 liveness_timeout 在 100ms–10s 调整。PING 发起前记录最后已发送的 OneRtt packet number，只把其后新包得到的认证 ACK 视为网络前进；迟到旧 ACK 不算。网络期限耗尽才关闭该失活物理连接并让原业务身份重握手，慢 handler 仍能收到原生 ACK 时继续等待。PING 不占业务流窗口，也不构成 WAL/quorum/消费 ACK 证明；单请求取消或期限仍不直接重置健康复用流。真正 CLI 子进程 SIGKILL/原端口重启回归保持原 Producer、PendingBatch 和 Consumer；慢语义 handler 回归验证不会被存活任务误杀。

固定 s2n-quic 1.91 的 idle acceptor Drop 和 idle Client close interest 不会主动唤醒底层 I/O。Server shutdown 在 acceptor 退出后、Client close 在首次 poll wait_idle 提交关闭兴趣后，发送一条本地无效 datagram 唤醒关闭检查，否则完全空闲时可能长期保留 UDP 端口或卡在 wait_idle。无任何连接活动与真实端口释放回归覆盖这些情形，不使用 SO_REUSEPORT 或固定等待；升级依赖时重新检查上游 endpoint/close 行为和该兼容措施。

## 诊断与已验证范围

`TransportMetrics` 提供固定、无动态标签的快照，并实现 prometheus-client `Collector`；管理入口用固定服务端/客户端前缀注册同一实例，不能每个请求新建实例。包含认证连接、拒绝、流、帧/字节、期限、读/解码失败、send 等待秒、TLS 失败、native 丢包和 DATA_BLOCKED/STREAM_DATA_BLOCKED/STREAMS_BLOCKED 通知。block frame 计数可以含重复通知，不等于阻塞持续时间。`Connection::recovery_snapshot` 只读查询最近的 min/smoothed/latest RTT、PTO count、cwnd、bytes in flight 和 congestion_limited；没有样本时为 None，不能当作零 RTT。

`tests/quic_transport.rs` 使用逐测试独立的内存证书、随机 UDP 端口和真实 TLS/socket。已覆盖大于 stream window 的帧及同流连续请求、证书/权限拒绝、双 pin 轮换与错目标节点、超大 HELLO 和非法 frame、半帧超时 poison、ACK 满时 Core 前进、慢 B 时 C 前进、并发池复用/分类隔离、空闲期限、payload clone 计费，以及 data 满且 D 保持连接时 C 重新认证、ACK 到达 dispatcher 后释放测试持有的积压额度。normal 被普通 Control 和 data 用尽时 Core 帧仍前进；此测试本身不代表业务持久 ACK，后者由真实 SDK/数据组回归交叉验证。原身份重试、Broker 接管和 RF3 独立进程多数派故障另见[测试索引](../testing.zh-CN.md)。故障网、3AZ、真实 CID/LB、按租户/分区公平调度及完整吞吐/P99/P99.9 矩阵仍待对应环境验收。

`tests/server_dispatch.rs` 以四条真实 Request lane、仅一个 affinity lane 持续工作，分别隔离客户端与服务端的 120ms 应用组帧阈值，持续业务超过阈值而保持同一物理连接，且 deadline/malformed 不增长。通用接收回归另覆盖空等取消后继续使用、无效 timeout 立即拒绝、prefix/body 各自超时与取消后 poison，以及首字节之后的延迟消耗 peer 预算；一次性 absolute API 的半帧失败与既有关闭/端口回收测试继续保留。

版本来源：[s2n-quic 1.91.0](https://docs.rs/s2n-quic/1.91.0/s2n_quic/)、[TLS Server Builder](https://docs.rs/s2n-quic-tls/0.91.0/s2n_quic_tls/server/struct.Builder.html)、[TLS session 源码](https://docs.rs/s2n-quic-tls/0.91.0/src/s2n_quic_tls/session.rs.html)、[event TLS API 源码](https://docs.rs/s2n-quic-core/0.91.0/src/s2n_quic_core/event.rs.html)、[send/flush 语义](https://docs.rs/s2n-quic/1.91.0/s2n_quic/stream/struct.SendStream.html)。首字节取消边界依据 [Tokio 1.53.3 read](https://docs.rs/tokio/1.53.3/tokio/io/trait.AsyncReadExt.html#method.read) 与 [s2n ReceiveStream 源码](https://docs.rs/s2n-quic/1.91.0/src/s2n_quic/stream/receive.rs.html)：单字节 read 在 Pending 时不消费数据，Ready 后即在同一次 poll 内启用 lane guard，再等待剩余 prefix/body。版本化 docs.rs 抓取不可用时，核对 Cargo 对应精确版本 registry 发布源码；`read_exact` 不作为空等的取消安全边界。
