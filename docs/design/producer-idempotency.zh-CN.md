# 生产身份、幂等与提交边界

本设计覆盖 T04。协议摘要位于 `tidemq-protocol::producer`，权威状态位于 `producer::ProducerState`，数据组组合入口为 `broker::DataApplication`。网络接入、Gateway 与 SDK 由后续任务接入；纯状态转换和进程内 Raft 测试不能充当 QUIC 端到端或三区性能证据。

## 身份与注册

消息身份是不可复用的 Topic UUID、partition 和消息 offset。消息 offset 在数据组提交的生产命令内按条数分配，控制记录不会消耗消息 offset；Raft log index 仅用于共识和恢复。

每个分区持久保存 `next_registration_epoch`，初值为 1。新 producer 先通过权威 `ProducerStatus` 读取提示，再以 producer UUID 和 `expected_registration_epoch` 显式注册。只有等于当前提示的代次可以创建会话，成功后以 checked 加法推进高水位。并发注册发生 CAS 冲突时，尚未建立身份的 open 操作可以重新读取提示；已经发送过消息的旧会话不能据此自动更换身份。

活跃表保存已分配代次的有限例外。相同代次和 producer UUID 重试注册返回原会话，异 UUID 冲突。任何小于高水位且不在活跃表的代次均已退休或过期，直接拒绝。未分配的未来代次返回未知会话。显式退休或到期删除活跃记录后，高水位继续拒绝旧追加，无需永久保存每个 UUID 的墓碑。

会话期限固定，不因消息、重试、重连或状态查询自动续期。到期索引按 `(expires_at_ms, producer_epoch)` 排序，推进时间时只处理已到期项。运行时将可信时间放入复制命令，状态机以 max(已持久时间, 输入时间) 推进逻辑时间；并发流水中较早捕获的时间不会使逻辑时钟回退。客户端不能提交自己的“当前时间”。持久逻辑时间防止重启后的时钟回退重新打开旧代次。时钟显著前跳可能提前终止会话，按结果未知处理，不能自动复制业务消息。

## 批次与原结果

同一生产顺序域为 producer UUID、注册代次、Topic UUID 和 partition。`first_sequence` 按消息条数推进，首个批次从 0 开始。批次原子提交，offset 与序号都使用排他上界；溢出拒绝。

共享 canonical 摘要使用固定域、Topic/partition、producer/epoch、首序号、消息数量以及每条 key/body 的长度前缀和内容。owner epoch、request/operation/trace、deadline 与网络尝试信息不参与摘要。同内容原身份经不同 Gateway、连接或 writer epoch 恢复时仍可匹配原结果。

| 输入 | 结果 |
| --- | --- |
| 首序号等于下一序号 | 验证摘要、容量与当前 writer fence 后追加 |
| 已保存的相同首序号、数量与摘要 | 返回原 `first_offset` 和 `message_count`，标记 duplicate，不再追加 |
| 相同首序号但内容或数量不同，或改变批次边界产生重叠 | 冲突，不合并、不拆分重试批次 |
| 首序号大于下一序号 | 返回 expected sequence，不建立缺口等待队列 |
| 原结果已超过条数窗口或保留期限 | 返回结果过期，不猜测是否已提交，不重新追加 |
| 会话已经退休或到期 | 返回会话过期，不自动注册 |

结果同时受每会话批次数与时间限制；保存原结果不需要保存第二份消息正文。结果丢失或过期不代表业务操作没有执行。SDK 进程内重连保留原身份和批次；首版没有持久 outbox，SDK 进程重启后的业务身份恢复由使用方负责。

## 同组权威与恢复

`DataApplication` 将 producer 状态、热消息、writer fence、订阅投递 attempt 和 ACK 权威放入同一个数据组 Application。writer 节点和 owner epoch 在每个业务 apply 时再次验证；较高 epoch 的 fence 不能覆盖已经提交的生产原结果。新 fence 会使订阅分配失效，保留 ACK 权威与未确认消息，要求新的 Claim。

恢复命令只携带分区和 writer 身份。组合状态机自行校验真实 producer 与订阅状态，以流式序列化计算摘要和双状态 revision，再产生恢复检查点。外部请求不能提供“已经恢复”的布尔值、revision 或摘要。检查点初次生成后冻结；重试返回原检查点，不因后续消息或 ACK 前移而覆盖。

生产成功由真实 Raft `client_write` 的持久 quorum 完成结果决定。正文、offset、去重结果与 fence 同组提交，只有已经 apply 的消息可读。状态读取或 QUIC 收到数据、发送完成、连接存活和 S3 上传均不是生产成功条件。S3 不在生产回执关键路径上。

## 资源与取消

生产、控制注册和热读分别有操作数及字节准入，叠加进程 `MemoryBudget`。批次限制条数、单条 key/body 合计字节和批次字节；分区及数据组限制热消息条数和逻辑字节。热读通过 checked `get` 只访问请求页，不扫描或克隆完整历史。返回页持有准入许可，实际读取工作结束并释放页面后归还；调用方取消不能提前归还 owned 工作仍持有的许可。

热页共享既有 `Bytes` 正文；准入取 `max(max_bytes, max(max_messages, 4) × size_of::<MessageData>()) + 512`，所有计算检查溢出。这个逻辑计费下限覆盖消息引用数组，也保留原字节下限；不能只按返回正文长度计费，因为少量正文仍可能对应完整预留的消息槽位。默认四个请求页保持在原 4 MiB 热读配额内，超额拒绝后释放操作数和字节许可。该口径不等于分配器、编码缓冲和进程 RSS 的精确上界；真实槽位容量与释放回归见[热读准入证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-hot-read-admission.zh-CN.md)。

有界 FIFO worker 按已准入顺序调用 `client_write_ff`，随后独立 owned waiter 持有准入许可，等待真实 Raft 回执终态；不会逐批等待 durable ACK 才提交下一批。`submit_produce` 仅等待准入与入队，调用方释放同 P 通道的顺序 guard 后再对 `ProducerSubmission::receive` 等待结果。入 Raft 前，取消与 handoff 通过单个原子状态竞争；取消胜出则任务不会提交，返回确定的期限/取消拒绝。worker 在实际 handoff 前再次检查请求期限和原控制 lease 到期时间的较早者，不能通过排队续权。已经 handoff 后，timeout、取消或丢弃 future 只结束等待，不撤销提交，也不提前释放尚由 Raft 持有的命令预算。失 quorum 后未决量达到上限即拒绝新准入；恢复或关闭 Raft 后释放。排空先关闭新准入，再等待已拥有的任务完成，受调用方总期限限制。

当前热数据未归档回收时达到上限会拒写。wire 消息在准入预算取得后会压实为拥有自身 key/body 的 Bytes，避免小切片钉住含未知字段的整帧；这一次有界复制属于生产性能成本。诊断另报消息槽位 capacity、capacity 字节及结果槽位 capacity。逻辑字节限额不等于 allocator 实际驻留或整个进程 RSS；消息结构、WAL 编码缓冲、快照、原生库及诊断余量仍由总预算和后续容量任务统筹。

容量拒绝由复制状态和固定限额确定；实际 allocator 失败不能成为某个副本独有的业务拒绝。追加在改变逻辑时间、offset 和原结果前先预留消息/结果槽位，预留失败不改变序列化权威状态。`DataApplication::try_apply` 将分配失败和内部状态不变量故障传播为 fatal 存储错误，存储层锁存失败，停止 apply、权威读和快照。重启后从持久状态恢复；已经交给 Raft 的调用仍按原身份核对结果。

## 观测

默认不记录逐消息成功日志，不记录 key、正文、内容摘要原文或令牌。业务命令、消息和完整状态使用安全 Debug，避免上游 Raft 的诊断输出泄漏内容。请求 span 仅携带有界 operation/request 身份与 attempt，重试保留原业务身份。

`ProducerMetrics` 是固定 Collector，交由管理面的单一 Registry 以 `tidemq_producer` 前缀注册。尝试按 committed、duplicate、rejected、unknown、failed 分开；新提交批次、条数和逻辑字节只在首次提交结果上计数。调用方取消可能产生 unknown，同时后台稍后观察到新提交；两者口径不同，不能相减得到跨故障的精确业务审计。进程崩溃、leader 切换和重放不会补齐指标历史，正确性使用原身份结果账本验证。

准入、总等待与 Raft 返回阶段使用本地单调时钟；Broker 的 commit duration 包含其等待的 WAL/复制/apply，不伪造精确拆分，底层 WAL/Raft 指标负责进一步定位。scrape 只读固定计数，不遍历会话或消息。

## 本地验证证据

T04 的实际检查包含格式、根包所有测试目标的零警告 Clippy、ProducerState 行为测试，以及真实文件/真实 OpenRaft 的 RF1 与 RF3 测试。RF1 组合测试经真实 MetadataAuthority 产生 grant/lease，验证 fence、双状态恢复、条件路由激活、生产与逐条 ACK，随后 snapshot/purge/reopen 保留原生产结果与 ACK 前沿。RF3 故障网络包裹真实 Raft handler，验证失多数派不成功、调用方取消后仍保留预算、恢复后原结果以及不等前批 ACK 的顺序流水。另验证小 Bytes 切片压实后释放 1 MiB 原 backing，以及可确定的容量预留失败不改变序列化权威状态。

这些证据尚不包含 QUIC/Gateway/SDK 端到端、真实三区、长期内存/吞吐尾延迟或完整对象回收，不能据此冻结生产性能或 allocator 推荐。
