# WAL、Raft 与持久恢复

本文记录分段 WAL 的本地存储契约，实现使用 OpenRaft 0.9.25 的 storage-v2。磁盘布局、容量采样和完整快照共同演进；历史单文件实现的 RF1/RF3 结果不能直接作为新布局的验收证据。QUIC、跨进程恢复、三区故障域及性能仍按相应测试记录单独核验。

## 状态与接入

- `DiskStore<A>::open` 返回 `DiskLog<A>` 与 `DiskStateMachine<A>`，供 `openraft::Raft::new` 使用；`Raft<A>` 是对应类型别名。
- `Application` 提供可序列化的完整状态、`Command`、`Response`、格式名称与版本。`apply` 同步且确定性，时钟、随机数、授权证明和其他外部输入必须先进入复制命令；业务拒绝通过 `Response` 返回。
- 应用自身增量保证有限状态及不变量，不能无限保存历史 command，也不能把唯一状态放到 Broker 内存。生产者原结果/退休边界、fence、订阅代次/attempt/ACK、发布授权/终止状态等均由后续领域应用纳入完整权威状态。
- `validate` 用于恢复和快照边界的完整校验，不在每个写入批次扫描保留数据。应用必须在 `apply` 内增量限制容量和拒绝非法状态变化。
- 状态和命令必须支持 postcard 的非自描述 serde 模型，禁止 `serde_json::Value`、untagged 或内部 tagging 等依赖 `deserialize_any` 的模型。变更字段/枚举时更新磁盘应用版本；开发阶段不维护旧格式兼容或迁移，旧版本目录明确拒绝。
- `with_application` 在读锁内执行有限同步查询，避免热读复制整个应用。闭包禁止 IO、长时间扫描或返回整库。权威读取先执行 Raft 读屏障；`application()` 的完整副本仅用于有界诊断和测试。
- `with_applied` 在同一读锁内提供应用投影和 last applied log id，不能分两次查询后把旧状态贴上新位置。

存储身份为 `cluster_id: Id`、`group_id: u64`、`node_id: u64` 与应用格式名称/版本。group_id 必须非零，由集群分配且不复用。元数据组和数据组使用不同 group_id 和目录。目录中的身份不匹配时拒绝打开；每个目录持有独占文件锁，避免两个 writer 同时打开。

快照只绑定 cluster/group/application 版本，允许同组不同 node 安装。它不把节点身份复制成另一副本身份。磁盘格式与公共 Protobuf wire schema 分开演进。

## 分段布局与恢复策略

每个副本目录包含：

| 文件 | 内容 |
| --- | --- |
| `identity.bin` | 已校验的副本身份 |
| `manifest.bin` | 唯一恢复权威；指定 active/sealed 段、控制/索引 checkpoint 和当前完整快照 |
| `segment-<uuid>.wal` | 绑定副本与物理段身份的顺序记录；active 可追加，sealed 不再修改 |
| `checkpoint-<uuid>.bin` | 有界控制状态、存活记录位置引用及精确 replay barrier；不复制所有 retained payload |
| `snapshot-<uuid>.bin` | 完整应用快照及 applied/membership 元数据；旧文件被 reader 持有时保留名称 |
| `.writer-lock` | 操作系统独占锁句柄，不通过删除锁文件强制解锁 |
| `.incoming-<uuid>` | 唯一接收中的有界临时快照 |
| `.replace-<uuid>` | 有界原子发布临时文件；没有 manifest 引用时不能作为恢复权威 |

文件名使用规范 UUID，不承接客户端路径。物理布局版本独立于应用版本和 wire schema；旧 `raft.wal` / `snapshot.bin` 布局拒绝启动。开发阶段使用全新数据目录，不提供旧格式读取、双写、迁移或自动清空。

record frame 保留 88 字节头部：magic/格式版本 8 字节、payload 长度 8 字节、顺序号 8 字节、独立 BLAKE3 头部校验 32 字节、BLAKE3 记录校验 32 字节。整数为小端。物理序号在 manifest/checkpoint 确定的恢复链内检查，和可被合法 truncate 后复用的 Raft index 分开。段头绑定副本身份和段身份；单条 record 及一个 group commit 不跨段拆分。

先校验头部，再使用长度分配或判断尾部是否完整。独立头部校验防止长度损坏被误当作未完成的最后一帧而截断已确认记录。记录校验覆盖 magic、长度、顺序号和 payload。

恢复逐帧执行：

1. 获取副本目录独占锁，校验 identity、唯一 manifest、有限文件数/字节和全部必需引用。不能按 mtime 或最大 UUID 猜测恢复链。
2. 校验 checkpoint 的控制状态和 retained 引用，再从精确 barrier 重放后续记录；被 purge 或 truncate 排除的旧 payload 不能复活。
3. 只允许修复 active 最末不足一个完整头部，或已通过头部校验但正文尚未完成的 record；截断后同步文件。sealed 长度/校验不符、缺段、错误身份、完整 frame 校验失败或非法顺序均拒绝启动，不扫描下一个 magic 跳过损坏。
4. 恢复完整快照后，只应用已持久 committed 范围；保留日志必须连续，purged 必须有快照覆盖。未提交 tail 可被合法新任期替换，不能向应用公开成功结果。
5. 只有整个恢复链及应用状态校验成功后，才清理没有引用的 engine 文件和旧快照。删除失败的文件仍占物理字节；manifest 损坏或缺失时不能先删“疑似 orphan”再尝试恢复。

段与 manifest 中的身份必须匹配目录 identity，不能把其他组的有效 WAL 单独复制过来使用。目录已有 durable data 而缺少 identity 时拒绝绑定新身份。

`DiskLog::diagnostics` 报告本地恢复与运行统计，不读取正文或建立 quorum 权威。`wal::inspect_segmented(directory, options, identity_digest)` 只读校验 manifest 指定的分段链，不修复 active 尾部；旧单文件 `Journal` / `inspect` API 已删除。活跃 writer 可产生暂时尾部；严肃损坏排查应先保留证据并在隔离副本上复查，不能把一次并发检查结果直接作为删除依据。

## 持久顺序与 group commit

一个副本的 vote、日志、committed、截断、checkpoint 和 manifest 发布进入同一 writer，禁止后发 IO 越过先发 IO。应用队列同时受字节与记录数 semaphore 限制，writer 将短时间内的有限批次聚合后一次 `sync_all`。写入前按整批实际 frame 长度判断段内余量，必要时先轮转。

- `append` 返回前，日志已对 reader 可见；只有整个 group 同步成功后才调用 OpenRaft `LogFlushed` 成功回调。
- `save_vote` 返回前 vote 已持久。vote 不得倒退或变得不可比较。
- `save_committed` 返回前提交边界已持久，不能倒退。状态机应用结果公开前，再保证其 commit 边界已持久；RF1 也走此规则。
- `apply` 只接收 OpenRaft 已提交日志，正文序号/offset 由应用维护。membership、fence、ACK 和控制记录同样消耗 Raft log index，因此消息 offset 不能使用 log index。
- 某 group 写入/同步失败后，writer 拒绝后续 mutation，并给所有后续未决请求失败结果。OpenRaft storage error 会停节点，不能继续返回虚假成功或自动降为 RF1。
- 调用方取消或丢失响应不表示日志未提交；仍需使用原操作身份查结果/重试。

存储硬上限是最后的 fail-closed 防线，不是正常配额策略。运行时已组合剩余条数/编码字节、实际 journal/快照文件、预留增长和磁盘余量，提前限制生产并保留 ACK/控制及检查点空间。容量采样缺失或陈旧会拒绝新工作；不等 storage fatal 才做背压。压力和长稳实测仍须验证采样间隔与实际余量。

## 快照与共识日志压缩

快照携带格式版本、group 身份、last applied log id、last membership 和完整应用状态，正文与元数据共同校验。文件接收器在写入/seek 前检查总上限；接收采用有界 chunk，不能通过超大偏移制造稀疏文件。

1. 在一致的应用视图上生成快照。克隆和序列化的内存成本受状态/快照上限约束，序列化放入阻塞任务，不长时间阻塞 Tokio IO worker。
2. 临时文件写完并 `sync_all`，发布到全新 UUID 文件名并同步父目录；同一 writer 将该名称纳入 manifest，持久成功后才报告 build/install 成功。不能通过文件存在或编号推断当前快照。
3. 安装时校验 checksum、格式、cluster/group/application、请求 metadata 与正文 metadata 相等，以及 membership log 不超过 last applied。拒绝回退已应用状态。
4. 真正 OpenRaft core 在快照成功后才可压缩对应日志。checkpoint 先同步有限控制状态及存活位置引用，再原子发布 manifest 并同步目录，最后删除没有引用的 sealed 段；旧文件在新恢复链持久成功前保留。
5. 重开时，purged 范围必须被 durable snapshot 覆盖，committed 尾部必须连续且身份一致，否则拒绝恢复。

purge 不再重编码所有 retained payload。检查点引用仍存活的物理 record，完整失效段只有在新 checkpoint/manifest 已持久后才删除。压力整理每次最多生成一个 `ref_only` 目标段：输入至多为一个 normal 混合源和一个已有 `ref_only` 段，存活原始 frame 加段头合计不超过 `segment_target_bytes`，默认 16 MiB。当前 active 只有在有序 checkpoint 屏障同步封存后才可作为源；不扫描或重写完整 retained tail。`ref_only` 段只供检查点定位，不把其中保留的旧物理序号当新 tail 重放。新引用/checkpoint/manifest 持久后才删除旧文件；空 active 复用，相同 checkpoint 不重复发布。

未提交 suffix 的 truncate 仍通过有序记录和精确重放边界排除；逻辑失效字节在文件真正删除前继续占容量。控制 reserve 和两个剩余段槽位用于轮转、搬迁和检查点，不能用满所有空间后再假设 GC 不需要临时空间。写入前的配置、编码或配额拒绝不等于 IO 状态不确定；实际写入、同步或发布失败会 poison writer，不绕过持久语义继续返回成功。异步文件写入先完成 `flush`，再执行 `sync_all`，避免遗漏缓冲写入的错误。

快照 reader 持有目录 owner、reader 槽位和文件引用。当前、退休及候选快照默认合计最多 4 个具名文件，reader 默认最多 8 个，另允许一个 incoming 文件；reader 还在执行 IO 时，取消等待者不会提前解除 pin。退休文件保留名称直到最后一个 reader 释放，所以物理采样不会漏算已 unlink 但仍占磁盘的快照。达到文件或 reader 上限时拒绝新增工作，不无限积累旧快照。

一次 manifest 快照发布由持有序列锁的 owned task 完成，外层取消不会停在磁盘发布与内存更新之间。入队前登记 `snapshot_publication_pending`，运行时中断时仍保护旧/current 和新/candidate 两个可能权威；后续新代拒绝，直到重开核验。发布返回错误后锁存 `snapshot_publication_uncertain`，阻止 cleanup、新代、incoming 和新 reader，不能因目录 fsync 报错就假定 rename 未发生。GC 失败保留注册 charge，计数并记录完整错误链，下次正常发布前重试；`snapshots` 同时显示文件/reader 当前量及上限。注册字节是保守 charge，真实具名文件长度另由容量采样提供。

底层 `RaftLogStorage::purge` 按官方合约接受 core 的删除指令，不是对外管理 API。官方 Suite 会人工构造不含快照的 purge/不一致 fixtures；真实 core 顺序另有测试覆盖。禁止在业务层直接调用低层 purge 来做对象正文回收。

共识日志压缩不等于对象发布、本地正文回收或 logical retained floor 推进。这些仍由 T07/T08/T09 的引用、终止、恢复检查点及副本条件控制。

## 初始资源预算

单文件成本的历史分析和分段决策来源见 [WAL 分段与容量管理评估](../research/wal-segments.zh-CN.md)。当前运行使用上述新布局；以下默认值由 `StorageOptions` / `JournalOptions` 统一校验，不是新增的角色 TOML 选项。

| 项目 | 默认值与口径 |
| --- | --- |
| 单 journal payload | 4 MiB，包含实际编码开销 |
| WAL 总文件字节 | 128 MiB；包括 active、sealed、checkpoint、manifest、WAL staging 和未清理的退休文件 |
| 目标段大小 / 段数 | 16 MiB / 64；按整批 frame 长度轮转，检查点保留所需槽位 |
| WAL 控制 reserve | 32 MiB，包含于 128 MiB 总额；覆盖一个有界段搬迁、checkpoint、manifest/段头及控制余量，不能额外用作正常新写额度 |
| 控制/索引 checkpoint | 8 MiB；旧/新 checkpoint、manifest 和段头共存也须先通过 WAL 硬限额 |
| 等待与同步中 payload | 16 MiB；64 条记录，计入活跃 group |
| group commit | 1 MiB 或 1 ms；单个较大记录仍受 4 MiB 上限 |
| retained log | 65,536 条及 64 MiB 编码字节双上限 |
| snapshot payload | 64 MiB；另加 88 字节 frame |
| snapshot 文件 / reader | 4 个具名文件、8 个 reader、另一个 incoming；被 pin 的退休文件计入具名上限 |
| 副本目录项 | 256 项；包含 identity、锁、manifest、全部段、检查点、快照及临时文件 |
| reader 单次复制预算 | 1 MiB；至少允许一条有界日志，以避免大记录永久无法推进 |
| checkpoint / manifest 发布 | 同一 writer 串行，临时文件与索引缓冲都有界；普通 append 不改写 manifest |

这些是可调的正确性基线，不能作为生产吞吐或容量承诺。应用状态本身、snapshot 视图、编码/解码、retained logs 和 writer 缓冲的总内存仍由运行时统一预算；per-record 限额不能冒充总 RSS 上限。

容量观察统计所有规范具名文件的实际长度，包括未引用段、checkpoint、staging 和被 reader 持有的旧 snapshot；不只统计 manifest 中的恢复链。unknown、畸形名称、符号链接、非普通文件、超限目录，以及缺少 identity、manifest、segment 或预期 snapshot 等必需文件种类，都会使观察不可用，不能补零继续准入。采样不解析 manifest 的每个引用，不能替代 `open` / `inspect_segmented` 对完整恢复链的校验。单副本未来文件预算为 WAL 硬限额，加 `(snapshot 文件上限 + 1) × (snapshot payload 上限 + 88)`，再加 64 KiB identity/control 余量；默认约 448.06 MiB。运行时只把该上界扣除已观察本地文件后的余量列为 `reserved_storage_bytes`，不会把现有文件再扣一次。

迁移估算另保守包含源副本的全部实际具名文件、目标完整文件预算、两份实际 framed snapshot 传输量和 64 KiB 控制余量；不沿用两倍单 WAL/快照公式。文件长度不等于已分配块或目录元数据，statvfs `available` 独立采样；部署仍须配置文件系统及并发任务余量。空间样本只决定准入，不授权删除或宣告持久成功。

payload 使用 postcard 1.1.3 二进制编码，写入经过有界 `std::io::Write`，读取显式拒绝未消费尾部字节；`bytes::Bytes` 正文按字节串编码，避免 JSON 数组放大。Record 枚举顺序也是磁盘格式的一部分。若更换 codec、variant 顺序或应用格式，必须升级版本；开发阶段使用新数据目录并明确拒绝旧版本，不自动迁移、猜测格式或清空已有数据。

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
