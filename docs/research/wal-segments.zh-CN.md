# WAL 分段决策与实现

最初评估日期：2026-10-20，分析代码基线为 `9e80cb7`。该次评估没有改动磁盘格式、删除文件或运行新性能实验。随后用户确认实施分段 WAL；当前契约见 [WAL、Raft 与持久恢复](../design/storage-recovery.zh-CN.md)，排障入口见 [存储与复制排障](../operations/storage-troubleshooting.zh-CN.md)。本文保留决策来源，避免将历史单文件证据误当成分段验收。

## 已选择的实现边界

分段是每个 Raft group 的物理存储变更，保留单 writer、group commit、完整应用快照和 quorum ACK。S3 归档仍在后台，上传或对象合并均不进入生产成功回执路径；WAL segment、消息逻辑区间和归档 object 的边界分别管理。

| 部分 | 实施约束与容量口径 |
| --- | --- |
| 恢复权威 | `manifest.bin` 唯一指定具名段、checkpoint 和当前 snapshot；文件名使用 UUID，不根据 mtime 猜链 |
| 日志回收 | checkpoint 保存有限控制状态和存活位置引用；字节压力下搬迁一个混合段，段数压力下至多将一个 normal 源和一个已有 `ref_only` 目标合并，输出不超过 16 MiB；新链持久后删除旧文件，不重编码整个 retained tail |
| 默认 WAL | 16 MiB 目标段、64 段、128 MiB 总文件字节；8 MiB checkpoint 与 32 MiB 控制 reserve 纳入总额，普通轮转保留两个段槽位 |
| 快照生命周期 | 完整 snapshot 仍为 64 MiB payload；4 个具名文件、8 个 reader、另一个 incoming；退休 reader 保留文件名和 owner lock |
| 物理采样 | 按共享规范名称校验所有实际具名文件，包含 orphan、staging 和退休快照；256 项硬界限代替旧的 16 项与 `raft.wal` 检查 |
| 未来空间 | WAL 总限额 + 5 份有 frame 的默认快照 + 64 KiB control 余量，约 448.06 MiB/副本；仅未占用部分作为目标节点增长 reserve |
| 迁移空间 | 实际源文件 + 完整目标文件预算 + 两份实际 framed snapshot + 控制余量，全部 checked arithmetic；statvfs available 独立采样 |
| 格式切换 | 新物理版本、全新数据目录；旧布局明确拒绝，无兼容读取、双写、迁移工具或自动清空 |

代码入口为 [`src/wal`](../../src/wal/mod.rs)、[`src/replication`](../../src/replication/store.rs)、[`物理采样`](../../src/runtime/physical.rs)、[`管理容量`](../../src/runtime/management.rs)和[`迁移估算`](../../src/scaling/remote/data.rs)。具名文件的长度不是文件系统已分配块上界；控制 reserve 不替代设备、文件系统和并发任务余量。

正确性测试、进程 SIGKILL 与完整 RF1/RF3 回归的实际结果由实施证据记录。设备掉电、慢盘/满盘长稳、真实三区和固定基线性能比较仍需相应环境；没有测量结果时，不宣称吞吐提升百分比或恢复时间 SLA。以下章节是 `9e80cb7` 的原始评估，描述的“当前”均指该历史基线。

## 原始结论与历史代码分析

建议为生产数据组实现分段 WAL，元数据组复用同一存储实现。当前单文件模式可以继续支持受控的小规模验证；扩充单组容量、长时间运行或增加并发组数之前，应完成分段、故障恢复和容量压测。收益主要是按文件释放已失效的前缀，避免每次 purge 重编码、复制整个保留 tail，以及减少 rewrite 与前台复制争用。

分段本身不会取消完整应用快照、扩大应用容量，或者让正文直接从磁盘借用读取。第一版应保持现有有界内存状态机、完整快照、quorum ACK 和对象生命周期协议，单独升级物理存储布局。没有测量依据时，不把收益写成吞吐提升百分比，也不同时迁移 postcard 或改为 mmap。

## 当前实现及证据位置

| 路径与关键位置 | 实际行为 |
| --- | --- |
| [`wal/journal.rs`（历史版本）](https://github.com/bitcapybara/tidemq/blob/9e80cb76790dec7a9348682bc2108fab4e44f3ce/src/wal/journal.rs)：`JournalOptions`、`Writer::append_group`、`Writer::rewrite` | 每组单 writer、单文件、有限队列、group commit；rewrite 写新文件后原子替换；当前源码已删除该文件 |
| [`wal/codec.rs`](../../src/wal/codec.rs)：`scan`、`frame`、`write_atomic` | 88 字节头、头部和正文 BLAKE3、连续物理序号、严格恢复、文件和父目录同步 |
| [`replication/store.rs`](../../src/replication/store.rs)：`DiskStore::open`、`StorageOptions`、`LogState` | `raft.wal`、`snapshot.bin`；启动保留全部原始 record 再解码；retained entries 在有界 BTreeMap 中 |
| [`replication/log.rs`](../../src/replication/log.rs)：`purge`、`checkpoint`、`truncate` | purge 克隆 log、删除前缀、逐条重编码剩余 entries，再等待整文件替换；truncate 先追加持久标记 |
| [`replication/state_machine.rs`](../../src/replication/state_machine.rs)：`recover`、`apply_committed`、`build_durable_snapshot` | 公开应用结果前持久化 committed；完整应用视图构建快照；仅恢复 committed tail |
| [`runtime/node.rs`](../../src/runtime/node.rs)：`open_replica` | OpenRaft 配置保留已入快照的 1024 条日志；未重写库的其他默认快照策略 |
| [`runtime/physical.rs`](../../src/runtime/physical.rs)、[`runtime/management.rs`](../../src/runtime/management.rs) | 固定副本目录、每目录最多 16 项、要求 `raft.wal`；文件长度及磁盘 available 分开采样 |
| [`scaling/remote/data.rs`](../../src/scaling/remote/data.rs)：`estimate` | 迁移空间估算包含实际副本文件、快照复制、未来 snapshot/rewrite/tail 增长 |
| [`lifecycle/coordinator.rs`](../../src/lifecycle/coordinator.rs)、[`lifecycle/data.rs`](../../src/lifecycle/data.rs) | 对象发布、各 voter 恢复快照、元数据检查点与本地正文 reclaim 是另一套持久协议 |

### 当前不是无限追加，也不是每次生产都做完整快照

默认 journal 文件上限 128 MiB；单 record payload 4 MiB；retained log 限制为 65,536 条及 64 MiB 编码字节；snapshot payload 上限 64 MiB。pending payload 16 MiB、64 条，group commit 目标 1 MiB 或 1 ms。所有值仍须共同满足 `StorageOptions::validate`，不是相互独立的生产容量承诺。

生产 append、vote、committed、truncate 共用有序 writer。只有 `sync_all` 成功后，才完成对应 durable 回调。完整快照由 OpenRaft 或生命周期/迁移流程触发，不是每次生产请求触发。正常 core 先完成 durable snapshot，再指示 purge；低层 storage trait 的 purge 不能作为业务删除接口。

purge 持有 log mutex，克隆有限 `LogState`，对 trial 删除前缀，再构建 identity、vote、purged、剩余 entries、committed 的 checkpoint records。`Writer::rewrite` 顺序写 `.journal-checkpoint`，`sync_all`，rename 为 `raft.wal`，同步父目录，然后重新打开 append fd。成功后才发布 trial；失败 poison writer。删除边界落在文件中间时，所有保留 tail 都要重新编码、校验和写入。

truncate 的旧正文在物理文件里仍存在，恢复靠有序 truncate 标记排除；直到后续 purge/rewrite 才回收空间。因此“逻辑 retained bytes”与“journal 文件长度”已经不同。

### 成本与磁盘余量

定义 `D` 为一次检查点周期内正常追加的 journal 字节，`L` 为 purge 后重新写出的 checkpoint 大小，`S` 为新 snapshot 文件大小。仅计算应用发给文件系统的写入量，一个周期约为 `D + L + S`；相对正常 journal 追加量约为 `1 + (L + S) / D`。这不是每消息写放大，也不包含文件系统、SSD、复制副本或 S3 的写放大。举例：`D=8 MiB`、`L=32 MiB`、`S=48 MiB` 时为 11 倍；它是算术示例，没有在本仓库测得该负载。

| 阶段 | 当前额外代价 | 分段第一版的边界 |
| --- | --- | --- |
| purge | `O(L)` 编码、frame 和文件写入；log mutex 等待整个过程，writer 后续写等待 | 旧 payload 不重写；持久化有限索引/控制检查点，再删除完整无引用段；仍有元数据和锁成本 |
| snapshot | 克隆应用视图、完整编码 payload、再复制进带头 frame、写完整快照 | 仍为 `O(S)`；`Bytes` 克隆可共享原正文，但完整序列化和 frame 仍复制字节 |
| 启动 | `scan(..., retain=true)` 持有整份原始 records，再逐条构建 retained entries；另读完整快照 | 按段/record 流式校验和重放，限制瞬时 buffer；仍要恢复有限应用与 retained entries |
| 回收粒度 | 整个 journal 原子替换 | 完整 sealed segment 删除；含活跃引用的混合段继续占空间 |
| 未提交冲突 tail | truncate 标记后等未来 rewrite | 保持标记/代次语义；物理旧段不能仅按最大 index 删除 |

临时文件存在期间，需容纳旧 WAL、新 checkpoint、旧 snapshot 与新 snapshot：保守目录可见峰值约 `W_old + L_new + S_old + S_new`，另加身份、临时接收、索引、文件系统元数据、在途增长和安全余量。默认 `reserve_growth` 以 `2 × (128 + 64) = 384 MiB` 的主文件量级估算每副本空间；它使用 snapshot payload 限额，不能视为包含 frame 头、所有临时文件和全部文件系统开销的严格物理上界。17 个本地组的对应主文件量级为 6.375 GiB。

文件长度与实际分配块也不是同一口径。旧 snapshot 被 rename 替换后，已打开的 `SnapshotFile` 可继续持有旧 inode；目录遍历不会统计该已移除名称的文件，释放 fd 前空间也未必归还。现有 statvfs 的 available 反映文件系统余量，但这不代替对退休 snapshot/segment reader 的生命周期和占用预算。分段实现必须把这些引用纳入空间估算，不能只计算 manifest 中的文件。[rename 与打开 fd 的关系](https://man7.org/linux/man-pages/man2/rename.2.html)、[unlink 的释放条件](https://man7.org/linux/man-pages/man2/unlink.2.html)。

## 必须区分的删除边界

1. **Raft log index**：vote、membership、ACK、fence、对象命令和消息命令都在同一 log。只由 core 授权 purge；本地 durable snapshot 必须覆盖删除范围，并保留需要的复制 tail。慢 follower 可以按共识协议改用 snapshot 恢复，不要求每次本地删段都新增一次全 voter ACK。
2. **消息 retained floor**：同一数据组 apply 中检查最低年龄、活跃订阅 ACK/seek 保护。它决定消息是否仍允许读取，不直接指示物理 WAL 文件删除。
3. **本地正文 local_reclaim_end**：对象实际发布、数据组授权、各所需 voter 恢复检查点及元数据登记等条件满足后，才提交 `DataCommand::Reclaim` 并移除热正文。逻辑过期后的恢复依赖还有独立 floor checkpoint；不能跳过现有协议。
4. **对象删除**：还要满足 manifest、pin、替换和终止账本条件，与本地 Raft 段文件删除分别执行。

完整 snapshot 可以继续保存未被正文 reclaim 的热数据，所以 snapshot 覆盖后的旧 Raft 日志可删除；这不表示消息数据丢失。反过来，S3 上传成功或消息 floor 已推进，也不表示可以绕过 Raft snapshot 删除 WAL。若以后让 snapshot 只引用外置正文而不携带正文，需要新增持久引用和恢复可达性证明，本次不采用这个变化。

## 建议的最小分段实现

### 物理格式和有限索引

保留单 writer 和现有 record 的语义，新增独立物理布局版本；应用格式版本不冒充物理格式版本。候选目录包含校验过的 manifest、版本化控制/索引 checkpoint、唯一编号的 WAL segments，以及按代次命名的完整 snapshot。文件名不携带用户输入，segment id 和代次不复用。

manifest 唯一指定哪些文件属于当前恢复链，包括布局版本、cluster/group/node 身份、checkpoint 代次与校验、snapshot 身份/位置/校验、sealed 列表和唯一 active 段。sealed 项记录完整长度、物理序号区间和校验；段文件不必在 seal 时改名，active/sealed 状态以 manifest 为准，避免多文件 rename 组合成为隐含提交点。

物理 record 序号与 Raft index 分离：前者在同一日志代次内跨 segment 连续，后者仍可在未提交 suffix 被 truncate 后由新 term 复用。段头绑定身份、版本、segment id、首个物理序号；record 继续独立校验头和正文，不跨 segment 拆分 record。跨段 group commit 的完成条件必须覆盖所有涉及文件；第一版可在一个有界 group 写入前轮转，使 group 留在一个段中。

沿用当前有硬上限的 retained-entry BTreeMap，第一版不增加 mmap 或另一份无限索引。控制/索引 checkpoint 保存某个 writer barrier 时刻的 vote、committed、purged 和仍存活 entry 的段号/offset/长度/LogId 引用；恢复先验证被引用 entries，再仅重放 barrier 后的物理 records。索引也有条数/字节上限，定位必须检查长度、checksum 和完整 LogId。这样 GC 不需要把 retained payload 再写一次，也不会因丢掉旧段中唯一的 vote/commit 而丢状态。

不能只在 manifest 中记“最新 vote/commit”，然后从更早的剩余段无条件重放：旧 control 标记可能使单调边界回退，truncate/replacement 也可能复活已作废 entries。checkpoint 必须绑定精确的物理 replay barrier 与该时刻的 retained-entry 引用集合。

可以从 16 MiB 目标段大小做固定 A/B 实验；它只是候选参数。初期维持现有总量级预算，分别约束 segment 总字节、文件数、索引、active 增长、reader pin、控制 reserve 和一次边界整理空间。不得把“128 MiB / 16 MiB”机械当作可用文件数。record 上限、group 大小和段内剩余空间必须联合校验；控制、ACK 和回收必须有预留，未归档正文或 reader pin 阻止 GC 时向新生产背压。

### 轮转、检查点与删除顺序

1. writer 按顺序处理完较早请求，flush/sync 旧 active；创建唯一的新段并同步段头和所在目录。现有 durable 回调不因轮转提前完成。
2. 写 manifest 临时文件并 `sync_all`，在同一文件系统 rename，再同步父目录。manifest 持久成功后才允许对新 active 的新写返回成功。之前崩溃仍以旧 manifest 为准，新文件是未引用 orphan。
3. GC 只在获得 core purge 边界且 durable snapshot 覆盖后，于同一串行顺序捕获控制/索引 checkpoint。同步 checkpoint 和目录，再发布引用它的新 manifest；此时剩余 entry 和控制状态必须能独立恢复。
4. 确认 sealed segment 不再被新 manifest、任何 retained entry、必要快照/恢复链或进行中的 reader 引用后，才从目录删除并同步目录。GC 前崩溃留下多余文件可以继续收集；manifest 不能指向已经删掉的文件。
5. 含活跃引用的混合段先保留。确有空间压力时，一次最多重写一个有界段，写新文件并更新引用 checkpoint/manifest 后才删除旧段；不要每次 purge 重写所有 segments。冲突 suffix 的垃圾仍计入物理限额，不能靠逻辑计数掩盖。

普通 append、vote 和 committed 保留现有同步语义；manifest 不需要每个 append 都重写。manifest 轮转只更新有限文件描述，控制/索引 checkpoint 可由稳定指针复用。任何 write/sync/rename/重新打开失败仍 poison writer，不自动切成无持久化模式。文件 fsync 不保证目录条目持久，目录同步必须保留；原子 rename 也不能替代掉电后的持久性。[Linux fsync 文档](https://man7.org/linux/man-pages/man2/fsync.2.html)。

### 启动与崩溃恢复

先拿目录独占锁，读取明确版本及唯一权威 manifest，校验身份、有限文件数/字节、checkpoint、snapshot 和全部必需引用。不能按 mtime 或“编号最大的文件”猜测恢复链，也不能在新格式损坏时静默回退到旧格式。

- sealed 段长度/校验不符、缺段、重复编号、顺序断裂或身份不符都 fail closed。
- 仅 active 最末 record 的不足头部，或经过头校验但正文不完整，才允许截断到最后完整 record，并同步修复；完整 checksum 错误仍拒绝启动，规则不能因位于最后一个段而放宽。
- 先恢复 checkpoint 引用的 retained entries，再按物理顺序重放 tail，校验 vote/commit/purge、truncate 及连续 Raft entries。最后只把 snapshot 后的 committed entries 应用给应用；未提交 tail 不能成为已确认结果。
- 只有确认 manifest/恢复链有效后才处理未引用文件；缺少 manifest 时不把任意段当作可丢 orphan。删除失败只留待后续重试，不把字节从物理用量提前扣除。
- 对 reader 和迁移 snapshot 设置有限持有量和期限；取消后实际 IO/reader 没有释放前仍保留空间 charge。记录退休文件占用，避免目录采样低估。

### 开发阶段的格式切换

按用户确认，项目处于开发阶段，不实现旧格式读取器、双写或离线迁移工具。分段版本使用新的明确物理格式身份和全新数据目录；现有测试与生成器一起更新。旧目录和历史报告保留用于诊断，不把旧文件自动重解释为新格式，也不自动删除已有数据。

版本不匹配、旧布局或损坏目录均清晰拒绝启动。新布局的身份初始化和 manifest 发布仍须持久化并遵守单一恢复权威；无需兼容旧版本，不等于可以省略新版本自身的崩溃一致性或误删保护。

## 实施必须覆盖的调用点

| 模块 | 同步改动 |
| --- | --- |
| `src/wal`、`src/replication` | 轮转、有限 manifest/index、严格校验、流式 replay、GC、snapshot 引用和 writer poison；保留公开 storage trait 的语义 |
| `runtime/physical.rs`、`runtime/management.rs` | 不再要求单个 `raft.wal` 或固定 16 项；按新有限布局计量，包含 staging/退休 reader 占用与真实 available；保留 unknown/陈旧样本拒绝准入 |
| `scaling/remote/data.rs` 与 placement/capacity 调用方 | 迁移估算按段、snapshot、staging、pin、增长与控制 reserve 计算，避免沿用两倍单文件假设 |
| 生命周期/retention/正文读取 | 保持既有 quorum、checkpoint、floor 与原身份恢复证明；对象 segment 不是 WAL segment，术语和诊断分开 |
| 配置、Docker 卷、使用/排障文档 | 物理版本、目录布局、总/单段/文件数/整理/迁移预算及开发期新目录要求；不暴露客户端任意路径 |
| 测试 fixtures 与性能 workload | 新布局及旧版本拒绝 fixture、损坏/轮转/崩溃 fixture、全量恢复、长稳磁盘轨迹和固定 RF1/RF3 对比 |

新增指标建议使用现有 metrics/tracing/OTel 接缝：active/sealed/retired/staging bytes、段数、GC eligible/pinned bytes、索引字节、轮转/GC/恢复时间、rewrite bytes、snapshot bytes/time、校验失败与容量拒绝。指标标签只含有限 role/stage/reason，segment id、group id、路径和操作身份放日志/有界 diagnostics；不记录正文。现有 WalAdmission/WalQueue/Append/Sync 和 replica committed/applied/purged/snapshot 诊断继续保留。

## 验收与停止条件

现有 [`wal_journal`](../../tests/wal_journal.rs)、[`replication_storage`](../../tests/replication_storage.rs)、[`replication_crash`](../../tests/replication_crash.rs) 覆盖的是单文件实现，本次阅读不等于重新运行，也不证明分段格式正确。实施时至少补齐：

1. 每个持久边界前后注入 SIGKILL：旧 active sync、新段创建/目录 sync、manifest 写/sync/rename/目录 sync、checkpoint 发布、段删除；恢复结果必须是完整旧状态或完整新状态，不能失去任何已确认数据。
2. active partial header/body 可修复；sealed partial、完整帧 checksum 错误、错误身份、缺段、重复/乱序物理序号、损坏 manifest/index 和断裂 committed tail 必须拒绝。禁止扫描 magic 跳过损坏。
3. 跨段 group commit、单 record 接近上限、vote/commit 跨段、未提交 suffix 多次 truncate/replacement、purge 落在段内部、空尾及全部日志已入快照；运行完整 OpenRaft storage-v2 Suite。
4. ENOSPC、短写、fsync/rename/delete 失败、日志/索引/段数上限、staging 空间不足、慢 reader 和多个旧 snapshot fd：不虚假 ACK，不无限分配，不误算已回收空间；新生产背压时 ACK/控制/恢复仍可推进。
5. 同身份 Produce/ACK/Read Unknown 后恢复，retention floor、本地正文 reclaim、RustFS 冷读、成员替换、QUIC 分块 snapshot 以及 RF1/RF3 全路径回归；保留订阅与对象 pin 后再证明可删除。
6. 旧版本与损坏身份明确拒绝、无自动清空或格式猜测；新目录初始化前后故障，恢复后逐一比对应用、membership、原生产结果和持久 ACK。
7. 固定 source/binary/allocator/磁盘/负载运行长稳 RF1/RF3：报告写入总量、snapshot/rewrite/GC 字节、文件数、峰值/回落磁盘用量、恢复时间，以及含 checkpoint 周期的吞吐/尾延迟。用受保护基线判定，不丢弃轮转/回收样本，也不靠放宽阈值接受回归。

以上是 `9e80cb7` 评估时列出的实施验收条件，不是已执行结果。当前开发版本切换到新分段布局；不能仅因段文件可创建、单元测试通过或短期吞吐提升就宣布生产验收完成。磁盘控制器掉电、文件系统和跨区故障模型仍需要相应环境单独验证。

OpenRaft 的日志连续性、vote/log IO 顺序与 durable callback 契约保持不变，依据 [OpenRaft 0.9.25 storage-v2 源码](https://docs.rs/openraft/0.9.25/src/openraft/storage/v2.rs.html)。现有正确性边界另见 [WAL 与恢复](../design/storage-recovery.zh-CN.md)、[保留与容量](../design/retention-quota.zh-CN.md)、[对象生命周期](../design/object-lifecycle.zh-CN.md)和[存储排障](../operations/storage-troubleshooting.zh-CN.md)。
