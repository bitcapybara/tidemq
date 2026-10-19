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

### 2026-10-19 本地验证记录

- `cargo clippy -p tidemq --tests --no-deps -j2 -- -D warnings`：无告警；从固定开发树运行。
- `cargo test --workspace -j2`：145 passed、3 ignored，28 suites，23.22 秒。ignored 包括需要显式外部可执行文件的 S3/RustFS 用例。
- `TIDEMQ_TEST_RUSTFS_BIN=.../rustfs cargo test -p tidemq --test lifecycle_persistence -- --ignored`：2 passed，2.54 秒；RustFS 1.0.3 strict 模式、aws-sdk-s3 1.152.0、两个实际 RF1 Raft 组和磁盘快照/重启。
- `cargo test -p tidemq --test s3_boundaries`：7 passed，0.67 秒；包括挂住 Range 正文时 HEAD 和 DELETE 的保留并发槽验证。

以上证据是 macOS 本机进程与真实磁盘/S3 API 验证，不等同机器掉电、跨进程远程适配完成或三可用区验收。

### 保留策略与分组后台工作

`Coordinator::retire_before(partition, deadline)` 从真实数据 quorum 的同一应用读锁获得逻辑 floor、已完成发布序列和提交位置，调用者不能给任意 floor。元数据只移除整段 `end<=floor` 的引用；发布尚未在数据组补完时保守等待。每次引用变化推进清单 CAS generation，publication_generation 只随发布推进，数据完成流水不会因为保留回收而跳序。

移除引用会增加永久 floor recovery hold。只有当前全部稳定 voters 的实际快照覆盖该逻辑 floor/已完成发布位置，才能在元数据释放 hold，并在数据组记录 floor checkpoint。随后已逻辑过期的热正文可以安全释放。部分共享对象继续保留，由共享滞留上界反映其容量成本。

`Catalog::capacity_usage()` 和按数据组的 `Coordinator::capacity_usage()` 返回有限聚合：登记对象的物理字节上界、待上传上界及回收滞留上界；Unknown/上传中按完整 descriptor 计，Deleted 计零。共享对象某分区部分过期时，整个分区块字节作为保守滞留上界。此口径是登记对象视图，不能替代 S3 提供方的实际容量监控或包含未登记外来对象；迟到孤儿需按永久终止记录重删。

后台使用 `Coordinator::pending_objects(after,limit<=64,deadline)` 的按组索引分页；登记保护已建立但数据从未收到授权的任务也能被发现。`DataState::pending_objects` 提供数据侧未完成意图索引；目录与数据状态的分页都保留原对象身份，不扫描桶来重建消息清单。

构建 pack 前预留 72 MiB 原始记录/索引预算，最终 pack 仍独立持有其内存 permit；取消不能提前释放阻塞 worker 的内存所有权。临时文件和全量正文不进入日志。阶段事件默认 debug，重试失败 warn 每进程共享指标实例最多每秒一次，抑制计数通过 `tidemq_lifecycle_suppressed_logs_total` 可见。
