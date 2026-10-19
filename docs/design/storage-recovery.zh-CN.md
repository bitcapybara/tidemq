# WAL、Raft 与持久恢复

本文记录 T02 已实现的存储契约。实现使用 OpenRaft 0.9.25 的 storage-v2，当前 RF1/RF3 证据来自真实 OpenRaft 与独立磁盘目录上的受控 RPC harness。harness 不经过 QUIC，不代表跨进程网络、三区容灾或生产性能验收。

## 状态与接入

- `DiskStore<A>::open` 返回 `DiskLog<A>` 与 `DiskStateMachine<A>`，供 `openraft::Raft::new` 使用；`Raft<A>` 是对应类型别名。
- `Application` 提供可序列化的完整状态、`Command`、`Response`、格式名称与版本。`apply` 同步且确定性，时钟、随机数、授权证明和其他外部输入必须先进入复制命令；业务拒绝通过 `Response` 返回。
- 应用自身增量保证有限状态及不变量，不能无限保存历史 command，也不能把唯一状态放到 Broker 内存。生产者原结果/退休边界、fence、订阅代次/attempt/ACK、发布授权/终止状态等均由后续领域应用纳入完整权威状态。
- `validate` 用于恢复和快照边界的完整校验，不在每个写入批次扫描保留数据。应用必须在 `apply` 内增量限制容量和拒绝非法状态变化。
- 状态和命令必须支持 postcard 的非自描述 serde 模型，禁止 `serde_json::Value`、untagged 或内部 tagging 等依赖 `deserialize_any` 的模型。变更字段/枚举时按磁盘应用版本迁移，不承诺无条件兼容。
- `with_application` 在读锁内执行有限同步查询，避免热读复制整个应用。闭包禁止 IO、长时间扫描或返回整库。权威读取先执行 Raft 读屏障；`application()` 的完整副本仅用于有界诊断和测试。
- `with_applied` 在同一读锁内提供应用投影和 last applied log id，不能分两次查询后把旧状态贴上新位置。

存储身份为 `cluster_id: Id`、`group_id: u64`、`node_id: u64` 与应用格式名称/版本。group_id 必须非零，由集群分配且不复用。元数据组和数据组使用不同 group_id 和目录。目录中的身份不匹配时拒绝打开；每个目录持有独占文件锁，避免两个 writer 同时打开。

快照只绑定 cluster/group/application 版本，允许同组不同 node 安装。它不把节点身份复制成另一副本身份。磁盘格式与公共 Protobuf wire schema 分开演进。

## Journal 格式与恢复策略

每个副本目录包含：

| 文件 | 内容 |
| --- | --- |
| `identity.bin` | 已校验的副本身份 |
| `raft.wal` | 首帧副本身份，之后顺序 vote、日志批次、committed、truncate 与 purge/checkpoint 记录 |
| `snapshot.bin` | 当前完整应用快照及 applied/membership 元数据 |
| `.writer-lock` | 操作系统独占锁句柄，不通过删除锁文件强制解锁 |
| `.incoming-*` | 接收中的有界临时快照，重新打开时清理 |
| `.replace-*`、`.journal-checkpoint` | 原子替换过程的临时文件；完成 rename 前不能作为权威数据，独占重开时清理 |

初始 frame 为 88 字节头部加有界 payload：magic/格式版本 8 字节、payload 长度 8 字节、顺序号 8 字节、独立 BLAKE3 头部校验 32 字节、BLAKE3 记录校验 32 字节。整数为小端，journal 内顺序号从零连续递增；checkpoint rewrite 重新从零开始。

先校验头部，再使用长度分配或判断尾部是否完整。独立头部校验防止长度损坏被误当作未完成的最后一帧而截断已确认记录。记录校验覆盖 magic、长度、顺序号和 payload。

恢复逐帧执行：

1. 配置和文件总大小超过上限时拒绝加载。
2. 最后不足一个完整头部，或已通过头部校验但正文尚未完成，只截断这一未完成尾部，并同步修复后的文件。
3. 完整帧的头部/正文校验错误、非法版本、序号不连续或非法长度均拒绝启动，包括最后一帧；不搜索下一段 magic 后继续服务。
4. 正文保留日志连续，禁止未来索引留洞；已被 purge 覆盖的重放前缀被忽略，不能复活旧日志。
5. 恢复快照后，仅重放已持久 committed 范围。未提交尾部可被合法新任期截断替换，不向应用公开成功结果。

journal 首帧的身份必须匹配目录 identity，不能把其他组的有效 WAL 单独复制过来使用。目录已有 durable data 而缺少 identity 时拒绝绑定新身份。

`wal::inspect` 只读校验并报告文件字节、有效字节、记录数与不完整尾部字节，不修复文件。活跃 writer 可产生暂时尾部；严肃损坏排查应先保留证据并在隔离副本上复查，不能把一次并发检查结果直接作为删除依据。

## 持久顺序与 group commit

一个副本的 vote、日志、committed、截断和 checkpoint 进入同一 writer，禁止后发 IO 越过先发 IO。应用队列同时受字节与记录数 semaphore 限制，writer 将短时间内的有限批次聚合后一次 `sync_all`。

- `append` 返回前，日志已对 reader 可见；只有整个 group 同步成功后才调用 OpenRaft `LogFlushed` 成功回调。
- `save_vote` 返回前 vote 已持久。vote 不得倒退或变得不可比较。
- `save_committed` 返回前提交边界已持久，不能倒退。状态机应用结果公开前，再保证其 commit 边界已持久；RF1 也走此规则。
- `apply` 只接收 OpenRaft 已提交日志，正文序号/offset 由应用维护。membership、fence、ACK 和控制记录同样消耗 Raft log index，因此消息 offset 不能使用 log index。
- 某 group 写入/同步失败后，writer 拒绝后续 mutation，并给所有后续未决请求失败结果。OpenRaft storage error 会停节点，不能继续返回虚假成功或自动降为 RF1。
- 调用方取消或丢失响应不表示日志未提交；仍需使用原操作身份查结果/重试。

存储硬上限是最后的 fail-closed 防线，不是正常配额策略。后续运行时必须根据剩余条数/编码字节、journal 字节、队列、快照和磁盘容量提前限制生产，预留 ACK/控制及检查点空间；不能等触碰 storage fatal 才做背压。

## 快照与共识日志压缩

快照携带格式版本、group 身份、last applied log id、last membership 和完整应用状态，正文与元数据共同校验。文件接收器在写入/seek 前检查总上限；接收采用有界 chunk，不能通过超大偏移制造稀疏文件。

1. 在一致的应用视图上生成快照。克隆和序列化的内存成本受状态/快照上限约束，序列化放入阻塞任务，不长时间阻塞 Tokio IO worker。
2. 临时文件写完并 `sync_all`，rename 到权威文件，再同步父目录，之后才报告 build/install 成功。
3. 安装时校验 checksum、格式、cluster/group/application、请求 metadata 与正文 metadata 相等，以及 membership log 不超过 last applied。拒绝回退已应用状态。
4. 真正 OpenRaft core 在快照成功后才可压缩对应日志。checkpoint rewrite 先同步完整替换文件，再 rename 和同步目录；旧文件在替换成功前保持权威。
5. 重开时，purged 范围必须被 durable snapshot 覆盖，committed 尾部必须连续且身份一致，否则拒绝恢复。

purge 会实际重写 `raft.wal`，保留 identity、vote、purged、日志尾部及 committed，随后切换 writer 文件句柄；不是在永久增长的文件中只追加删除标记。任一替换/同步/重开错误都会 poison writer，禁止继续向已被替换的旧 fd 写入。资源校验为 checkpoint 逐条 frame 和控制标记预留文件空间。16 KiB journal 的反复 snapshot/purge/继续追加及最终 SIGKILL 重开测试验证此回收路径。

底层 `RaftLogStorage::purge` 按官方合约接受 core 的删除指令，不是对外管理 API。官方 Suite 会人工构造不含快照的 purge/不一致 fixtures；真实 core 顺序另有测试覆盖。禁止在业务层直接调用低层 purge 来做对象正文回收。

共识日志压缩不等于对象发布、本地正文回收或 logical retained floor 推进。这些仍由 T07/T08/T09 的引用、终止、恢复检查点及副本条件控制。

## 初始资源预算

| 项目 | 默认值与口径 |
| --- | --- |
| 单 journal payload | 4 MiB，包含实际编码开销 |
| journal 文件 | 128 MiB，达到前应做检查点/压缩和准入控制 |
| 等待与同步中 payload | 16 MiB；64 条记录，计入活跃 group |
| group commit | 1 MiB 或 1 ms；单个较大记录仍受 4 MiB 上限 |
| retained log | 65,536 条及 64 MiB 编码字节双上限 |
| snapshot payload | 64 MiB；另加 88 字节 frame |
| reader 单次复制预算 | 1 MiB；至少允许一条有界日志，以避免大记录永久无法推进 |
| checkpoint rewrite | 每个 journal 最多一个排队/进行中的替换；另计有界 retained state 与编码缓冲 |

这些是可调的正确性基线，不能作为生产吞吐或容量承诺。应用状态本身、snapshot 视图、编码/解码、retained logs 和 writer 缓冲的总内存仍由运行时统一预算；per-record 限额不能冒充总 RSS 上限。

payload 使用 postcard 1.1.3 二进制编码，写入经过有界 `std::io::Write`，读取显式拒绝未消费尾部字节；`bytes::Bytes` 正文按字节串编码，避免 JSON 数组放大。Record 枚举顺序也是磁盘格式的一部分。若更换 codec、variant 顺序或应用格式，必须升级版本并提供明确迁移方式，不能用“serde 可反序列化”代替兼容承诺。

## Raft 与传输接缝

生产网络通过原生 `RaftNetworkFactory<TypeConfig<A>>` 接入。vote 和库真实产生的空 AppendEntries 归 C；非空 AppendEntries 归 R；快照块归 B。传输保留 term、prev_log、entries、commit 和原 RPC 因果，不自行生成心跳或改变同组流水线。

同一组的 R 被延迟、C 先到时，空心跳不能应用不存在的正文；缺失 prev_log 的请求返回 conflict，旧 term 返回更高 vote。快照块大小、并发和带宽都受限，B 的完成回复必须等完整校验和 durable install。QUIC ACK 或发送完成不等于复制持久化。

## 观测与验证

传入已有 `OperationMetrics` 后记录有限标签的 Admission、Queue、Append、Sync、Commit、Apply 直方图，不初始化全局 subscriber 或 registry。这里 Commit 表示本地 committed 标记持久耗时，不是整个 quorum RTT；生产 Replicate 计时由真实网络适配器记录。

`DiskLog::diagnostics()` 不含正文、凭证或去重明细，提供身份、vote、committed/applied/purged/snapshot、retained 与剩余预算、启动恢复和 writer 统计。查询仅反映本地状态，不证明 leader/授权/quorum。事件包括 `wal_incomplete_tail_repaired`、`wal_writer_failed`、`raft_storage_recovered`、`raft_snapshot_persisted` 和 `raft_snapshot_installed`，完整 IO 错误链保留。

有限诊断 span 包括 `raft_log_append`、`wal_group_commit`、`raft_commit_marker`、`raft_apply` 和 `raft_snapshot_build`。详细 trace 的 `wal_record_durable` 显式关联入队时 span，group commit 不丢失异步队列前的请求关联；不把正文或整个 command 的 Debug 放进 span。trace/debug 的导出与采样开销沿用运行时预算。

验证入口见 [存储排障手册](../operations/storage-troubleshooting.zh-CN.md)。已覆盖官方 storage-v2 Suite、真实文件校验/尾部/重开、SIGKILL、RF1/RF3 共识、响应丢失、选举、成员替换及分块快照。完整 QUIC RPC、慢盘/多组争用、三区网络、真实掉电和正式吞吐仍由后续集成/故障/性能关卡验证。

`sync_all`、rename 和目录同步依赖操作系统/文件系统及存储设备正确兑现持久化语义。Linux/macOS 使用实际平台 API；SIGKILL 只验证进程崩溃，不认证宿主掉电、控制器缓存或硬件故障模型。

官方依据：[OpenRaft 0.9.25](https://docs.rs/openraft/0.9.25/openraft/)、[RaftLogStorage](https://docs.rs/openraft/0.9.25/openraft/storage/trait.RaftLogStorage.html)、[RaftStateMachine](https://docs.rs/openraft/0.9.25/openraft/storage/trait.RaftStateMachine.html)、[RaftNetwork](https://docs.rs/openraft/0.9.25/openraft/network/trait.RaftNetwork.html)、[postcard to_io](https://docs.rs/postcard/1.1.3/postcard/fn.to_io.html)、[postcard take_from_bytes](https://docs.rs/postcard/1.1.3/postcard/fn.take_from_bytes.html)。

## 本地 apply 故障与确定性

Application::try_apply 默认调用确定性 apply；存在 fallible allocator 操作的应用须 override，将本地分配/内部状态损坏作为 io::Error 返回。此类错误不能转换为普通业务拒绝：不同副本的内存余量可能不同，记录不同业务响应会分叉。

storage 在 try_apply 成功后才推进 last_applied。失败会锁存 application_failed，向 OpenRaft 返回 StorageError 停止该副本；之后拒绝新 apply、权威查询、快照生成/安装。应用可能已部分变更，原诊断方法只供本地排查；MetadataAuthority、DataAuthority 和业务权威读取使用 try_with_applied/try_with_application，仍须先取得真实 Raft barrier。不能用失败状态生成恢复证据或新快照。更早已复制出的完整健康快照不包含失败状态。

已提交的日志不因本地分配失败被跳过或改写。重启从最后有效快照与已提交 tail 重新 try_apply；资源故障仍存在则启动失败。tests/application_failure.rs 注入部分状态修改后的分配错误，验证旧 applied 位置不前移、快照字节不变、所有权威读取拒绝，并在重启重放同一 committed entry 后恢复正确值。没有通过全状态 clone 在每条热写前做备份；业务应用仍应先完成纯检查和 fallible reserve，再做权威状态修改。
