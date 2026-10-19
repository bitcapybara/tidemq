# 对象生命周期与回收

对象正文上传、清单发布、数据组完成、本地正文回收分别提交，任何一次超时均不表示操作未提交。生产成功仍只依赖数据 Raft，不等待对象服务。

## 持久状态与提交路径

`MetadataState.lifecycle()` 中的 `Catalog` 保存唯一对象身份、任务、清单代次、读取 pin、恢复依赖及永久终止记录。`DataApplication.lifecycle()` 中的 `DataState` 保存发布意图、授权、完成与不可逆 Aborted、已完成对象映射及本地回收边界。两个状态都进入原有磁盘 WAL 和完整快照，均使用 OpenRaft 的真实 quorum 提交。

1. 从数据组线性化读取已提交记录，构建同组不可变 pack；每对象至多 64 个分区，不跨组。身份和字节摘要不可更换。
2. 元数据先登记任务及预期分区清单代次，再由数据组安装授权。授权命令与 writer fence 在同组排序；旧 epoch 不能创建新的授权。
3. 完整上传并校验所有字节后，元数据记录 Uploaded。HEAD 的元数据匹配本身不能代替正文校验。
4. 元数据对全部受影响分区同时检查预期代次，原子切换清单引用。对象各自保存发布时的分区代次，不能使用日后最新代次补写旧完成记录。
5. 数据组按分区清单代次记录 Published。先提交的元数据发布尚未补完时保留本地正文，恢复者按原 object ID 对账。
6. 对当前稳定的全部 RF1 或 RF3 voters 等待真实数据状态应用，逐个构建并验证覆盖完成记录的持久快照，元数据再记录 checkpoint。之后数据组才允许推进 local reclaim 并释放热正文。

所有步骤都有总 deadline；新步骤须通过新鲜元数据和数据 Raft barrier，控制有效期从读取开始计算。持久旧证明不能在重启后续期。joint membership 或真实 voters 不匹配时暂停新能力；RF3 不转为 RF1。当前 `Coordinator` 提供持有真实本地 Raft/storage 句柄的实现，跨进程元数据访问由运行时的受认证语义适配器集成，不能通过反序列化任意数字获得权限。

## 取消与永久终止

取消先在元数据提交 CancelRequested，阻止后续 Publish；然后数据组提交 Aborted，最后元数据提交 Aborted。即使数据组从未收到授权，也创建永久 Aborted 记录，拒绝迟到的旧登记证明。若 Publish 已先提交，则完成原发布，取消不会删除已生效数据。

对象 ID 永不复用，Deleted 记录也不删除。当前每元数据组与数据组分别最多 4,096 个对象身份；达到上限停止登记，已有取消、完成、读取、删除仍可处理。此上限是明确的初版容量边界，扩展前需要保持永久退休边界的存储方案。

未知 PUT 结果保留原对象身份和任务。DELETE 后仍可能有此前未知的 S3 写入迟到，永久终止记录保证其不能重新发布；`collect` 对 Deleted 也再次执行幂等 DELETE，允许清理迟到孤儿。一次 DELETE 成功不证明所有未知服务端请求已经停止，后台必须重扫永久终止记录。

## Compaction、读取与恢复

Compaction 读取并校验原始历史，先生成完整替代对象，然后在同一次清单变更中移除全部原对象引用并增加替代引用。替代范围必须与原对象在每分区覆盖的连续范围完全一致，不按 key 丢弃消息、不部分丢弃共享对象的其他分区。

切换时旧对象自动获得持久 recovery hold。它不随节点离线或时间流逝释放。新对象数据完成且当前稳定全部 voters 的实际快照均覆盖该完成位置后，才能释放 hold。成员变更期间暂停该屏障，移除 voter 前须由弹性控制器完成安全成员变更并重新取得当前稳定视图。

读取 pin 在元数据中与清单、DELETING 串行；只有当前已发布且仍有清单引用的对象能获得新 pin。`GuardedPage` 持有 pin 与控制期限，使用记录和生成 `VerifiedColdDelivery` 前检查期限。投递证明绑定分区、offset、长度、对象和清单代次，数据状态机再检查当前对象映射；Compaction 后旧证明不能对新视图新增投递。

读取 pin 最长 10 秒，不依据节点墙钟清理。正常完成显式 release；异常遗留 pin 的 reaper 必须先做线性化读取，再在本进程 monotonic clock 等待该 pin 的完整 TTL，最后按不可变记录条件移除。重启从头等待。到期的读任务停止旧视图并重新获取。旧快照只能在重新取得有效清单 pin 后读取对象，不能使用磁盘旧映射绕过 DELETING。存储层还拒绝安装会回退已应用位置的快照。

## 边界与资源

`committed_end` 为已提交消息排他上界，`object_published_end` 为连续对象发布上界，`local_reclaim_end` 为本地已释放正文上界：

`local_reclaim_end ≤ object_published_end ≤ committed_end`。

`ProducerState.retained_floor` 保留逻辑下界含义；热正文索引单独使用 `local_reclaim_end`。本地回收不会推进订阅前沿、逻辑保留下界或生产者去重结果。逻辑保留与订阅保护由 retention 同组排序处理。

上传任务最多 1 个，后台控制任务最多 2 个，S3 总并发 N 至少为 2，正文/Range/multipart 最多占 N−1 个槽，为 HEAD/DELETE/abort 留出执行容量。完整 pack 和冷读解码仍计入共享 MemoryBudget；读取每页至多 1,024 条/1 MiB，单段范围合并，不无限预取或重排。对象存储不可用时保留正文与任务，由配额模块背压新写入。

## 观测与验证

`LifecycleMetrics::register` 注册到无前缀根 registry，全部指标名以 `tidemq_lifecycle_` 开头，使用 8 个无标签 Counter、1 个 Gauge、1 个具有固定 bucket 的 Histogram，约 20 个 series。对象、分区、操作 ID 不做标签。`Coordinator::diagnose` 通过新鲜读取返回单对象阶段、引用/pin 数、checkpoint 代次和阻塞原因，不返回消息正文、key 或凭证。

初步回归覆盖永久终止的快照 round trip、compaction 双屏障、热回收与逻辑下界分离、去重原结果、拒写回调原子性，以及正文请求挂住时 HEAD/DELETE 独立推进。实际 Raft + RustFS 的阶段重启与回收验证见对应集成测试；验证记录必须区分本机进程故障、RF3 协议与真实三可用区。
