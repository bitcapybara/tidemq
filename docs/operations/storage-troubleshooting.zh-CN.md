# 存储与复制排障

本手册对应 `src/wal` 与 `src/replication`。真实 QUIC Raft 已接入服务运行时；管理 `/diagnostics` 返回有界的 metadata_storage 与 data_groups[].storage，含实际 WAL、commit/applied/snapshot/失败状态；`/metrics` 返回同一进程的分段统计。读取是本地诊断，不能替代 quorum 权威。以下测试使用隔离临时目录。

## 首先确定哪一步慢

用同一逻辑操作的期限、原身份、节点/组与时段关联日志及指标，不把 payload 或令牌写进支持包。

| 证据 | 含义与下一步 |
| --- | --- |
| WalAdmission 上升、pending byte 余量很低 | 等待 writer 字节/记录额度；检查写入速率、记录大小、快照/磁盘耗时；Broker Admission 另计 |
| WalQueue 上升，Sync 基本稳定 | batch/linger、writer 排队、段轮转或 checkpoint/manifest 发布；Broker Queue 与修复 RepairPace 分开 |
| Append 上升，Sync 较低 | 编码/校验或文件写入慢；CPU profile 与 IO 分开分析，不先扩大 batch |
| Sync 尾延迟上升 | 操作系统/设备同步或磁盘争用；结合磁盘队列、剩余空间和后台任务 |
| 本地落盘快，quorum 返回慢 | 看真正 RPC Replicate、peer lag、选举和网络；当前 harness 延迟不能替代网络证据 |
| Commit 上升 | Broker 等待 Raft client_write 的整体提交结果，包含复制/应用；不是 SDK 全链路 RTT |
| CommitPersist 上升 | 本地 committed 标记持久化；不能与 Broker Commit 混为一个分布 |
| Apply 上升 | 应用确定性执行慢或锁竞争；热路径不应做整库 validate、IO 或全量拷贝 |
| committed 领先 applied | 看状态机进度和 IO/storage fatal；不要直接修改 applied 标记 |
| snapshot 落后、WAL 总字节/段数接近上限 | 检查正常 core snapshot/purge、混合段引用、checkpoint 和控制 reserve；不能手工删 WAL |
| snapshot 数量接近上限，磁盘不回落 | 检查仍在传输的 reader 和退休快照 pin；取消等待不代表后台 IO 已结束，不手工 unlink |

集成的指标名称为 `tidemq_stage_duration_seconds`，标签只用已有有限 `operation`/`stage`。同一口径的 histogram buckets 可跨实例汇总；节点/组/请求身份保留在日志或诊断，不增加无限 metric labels。正式看板还需关联运行时 CPU/RSS/磁盘/QUIC 数据。

## 读取本地证据

`DiskLog::diagnostics().await` 返回：

- 副本 identity 与 vote。
- committed、applied、purged、snapshot 的 Raft log id。
- retained 条数/编码字节、剩余条数/编码字节、WAL 总文件字节和硬上限，以及 `replica_file_limit_bytes` 的完整未来文件预算。
- 本次启动的不完整尾部字节及 valid records/bytes。
- 本进程写入记录/字节、sync 次数、checkpoint rewrite 次数和 pending byte 余量。

这些都是本地状态，不能据此宣布当前 leader 或控制授权仍有效。业务排查的 offset 应从应用权威状态获取，不能把 Raft index 当 offset。

分段目录以 `manifest.bin` 为唯一恢复权威，使用 `segment-<uuid>.wal`、`checkpoint-<uuid>.bin` 和 `snapshot-<uuid>.bin`。`wal::inspect_segmented(directory, &JournalOptions, identity_digest).await` 是只读 manifest 链检查；只有正常 `open` 才可修复 active 的不完整末尾。旧单文件 `Journal` / `inspect` API 已删除。使用与运行实例一致的大小上限，超出上限不能简单调大后忽略；frame 头仍为 88 字节。

`capacity.usage.physical_storage_bytes` 合计本地具名文件的实际长度和已登记对象的保守上界；`capacity.data_filesystem.available_bytes` 独立反映配置数据目录的文件系统可用空间。`reserved_storage_bytes` 只表示完整未来文件预算尚未被本地文件占用的部分，不能再次扣除当前文件。统计包括 manifest 未引用的文件、staging 和 reader 正在持有的退休快照。未识别名称、symlink、非普通文件、目录项超限、缺少必需布局或超时使样本不可用；unknown 不能填 0 供新生产或迁移使用。

默认 WAL 总文件预算为 128 MiB，包含段、控制/索引 checkpoint、manifest 和 WAL staging；32 MiB 控制 reserve 在其中，用于有界混合段搬迁、检查点与控制余量。4 个具名 snapshot 加一个 incoming 各受 64 MiB payload + 88 字节 frame 约束，另有 64 KiB identity/control 文件余量，合计约 448.06 MiB/副本。该口径是文件长度预算，不认证文件系统元数据、块分配或设备空间；部署必须保留相应余量。

活跃写入中的最后一帧可能尚未完成。先停止该副本的新任务或复制证据到隔离目录，再判断是否持续损坏。支持包只包含诊断、构建/配置身份、有限事件和校验结论；默认不复制 journal/snapshot 正文。

## 事件与处理

| 事件/错误 | 处理 |
| --- | --- |
| `wal_incomplete_tail_repaired` | 已删除最后未完成帧；记录 discarded bytes。按原请求身份恢复未知结果，不自动新建身份重发 |
| checksum/header/sequence 错误 | 副本 fail closed。保留文件副本、构建和时间线，检查存储；从健康 quorum 按成员变更/快照恢复，禁止扫描 magic 后继续 |
| identity/group/application 不匹配 | 检查节点配置和目录映射；不要修改 identity 文件来“接管”未知数据 |
| writer lock 失败 | 检查是否已有实例使用目录；关闭实际拥有者，不能删除锁文件强行双开 |
| legacy layout / manifest 缺失或版本不符 | 当前开发版本使用全新数据目录；保留旧目录与匹配旧二进制，不修改身份、不猜测格式、不自动清空 |
| sealed 段、checkpoint 引用或 manifest 校验错误 | fail closed，保留整个恢复链证据；不能移除某一段后用剩余文件“尽量恢复” |
| snapshot reader/file、segment/index/control reserve 超限 | 停止新增对应工作，检查真实持有者与正常回收进度；不扩大无限队列或取消 pin 保护 |
| `snapshot_publication_uncertain=true` 或长期 `snapshot_publication_pending=true` | 保留旧/current 与新/candidate 文件；目录同步错误或运行时中断不能证明 rename 未发生，受控重启后按 manifest 完整核验，不能手动删除 candidate |
| `wal_writer_failed` / storage fatal | 查看完整 IO 链和容量；该 writer 后续 mutation 都失败。修复原因并走受控恢复，不仅重试接口 |
| snapshot metadata/校验/格式错误 | 保留失败证据，拒绝安装；不能用请求 metadata 覆盖文件内 metadata |
| snapshot 会回退 applied | 检查旧 leader/旧任务/迟到 snapshot；拒绝旧状态，不能降低本地 applied |
| purged 超过 durable snapshot | 不自动重建空状态；该目录无法安全恢复，按健康副本修复流程处理 |
| quorum 丢失 | 停止新的成功提交。恢复多数派/成员与网络，不能为了可用性直接降副本数 |

快照或 checkpoint 写入需要新文件的临时空间，rename 成功后仍需目录同步。旧/新文件共存必须先经过硬限额准入，不能等满盘后依赖“免费”的 GC。若满盘，优先释放可证明无引用的诊断/过期日志产物或扩容，并限制新生产；对象、快照和 WAL 删除必须遵循各自协议。

删除边界分别是 core 授权且快照覆盖的 Raft index purge、年龄和订阅 ACK 共同约束的消息 retained floor、发布和恢复检查点协议授权的本地正文 reclaim，以及对象 manifest/pin/替换账本授权的 object GC。任一个条件成立，都不能直接推导另外一种文件可删。

## 复现验证

在仓库根目录执行：

```sh
rtk cargo fmt --all
rtk cargo clippy -p tidemq --all-targets --no-deps --locked -j2 -- -D warnings
rtk cargo test -p tidemq --test wal_journal --test replication_storage --test replication_crash --test raft_cluster --locked -j2
```

- `wal_journal`：实际分段 group commit、只读 manifest 链检查、partial header/payload、完整 checksum 与长度头损坏、容量失败 poison；与应用快照和 core 恢复测试共同验证。
- `replication_storage`：官方 OpenRaft storage-v2 完整 Suite，以及 vote/commit/fence/原结果重开、未提交尾替换、跨节点快照、错组/metadata/损坏拒绝、锁/容量/查询与真实 stage 样本。
- `replication_crash`：单独 test 子进程在持久边界停住，父进程 SIGKILL 后重开，验证未执行 Drop/close 时仍恢复已持久结果。子进程只由测试持有句柄管理；具体分段边界与实际运行结果以实施证据为准。
- `raft_cluster`：真正 OpenRaft 的 RF1/RF3、同身份响应丢失重试、leader 隔离、剩余多数派选举、磁盘重开、成员变更、新副本分块 snapshot，以及控制心跳先到/缺失 prev_log/旧 term 因果。

上述底层证据与真实 QUIC 的 `raft_quic`、独立 OS RF3 的 `runtime_queue` 交叉验证，见[测试索引](../testing.zh-CN.md)。磁盘控制器掉电、跨区丢包/抖动或持续容量负载仍需独立验证。更改内核、文件系统、设备、codec、snapshot 或网络适配器后重新跑相应关卡；当前结果不是生产 SLA。

## 正常停止与恢复约束

先关闭拥有该目录的 Raft，再调用 `DiskLog::close`，最后释放 handles。`close` 会使这个 writer 的所有克隆失效，不能在正常业务仍提交时调用。SIGKILL 恢复走同一 open/replay 路径，不依赖正常退出的清理。

原有目录必须使用相同 cluster/group/node/application 版本重开；新增副本使用新目录和新 node_id，并通过共识成员协议获得正确状态。不得把旧节点目录复制后只改身份当成新副本。
