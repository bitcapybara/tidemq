# addf6d9 评审意见复核

2026-10-21 读取 workspace 根的 `TideMQ-review-addf6d9-2026-10-21.txt`，对照已合流产品源码 `24471ce` 复核。原评审针对旧 main `addf6d9`，不是当前分段 WAL 的动态验收。本次核对关键调用、配置与已有回归，没有执行评审建议的新故障场景，也没有为这些建议运行性能负载。

结论：需要继续改进，优先处理容量、成员追赶、路由、配置一致性和失败回滚；快照取消与 HELLO 的原路径已有修复，热页读取已部分优化。不能将所有旧意见标为仍未处理，也不能用分段 WAL 的通过覆盖未验证的问题。

## 逐项结论

| 原编号 | 当前结论 | 下一步与验证边界 |
| --- | --- | --- |
| 1 日志容量与快照 | **仍需优先处理**。运行时仍继承按 5000 条触发快照的默认策略，保留 1024 条尾日志；逻辑日志上限仍为 64 MiB。分段只改善物理布局，没有建立字节预算与快照／purge 的安全关系。 | 按保留字节提前触发共识快照／回收，尾部同时受条数与字节约束，为 ACK／控制／回收留余量。以原身份重复有效大批次，验证去重结果不变、Raft 健康且可继续写入。当前没有执行该建议回归；不能机械断言第 256 次必失败。 |
| 2 落后成员追赶 | **仍需优先处理**。`authorize` 在进入 Raft 前使用本地 `membership.voter_ids()` 过滤发送者。 | 将经 mTLS 绑定的 cluster／group／role 传输授权与 Raft 共识成员判定分开，不能允许匿名节点。补充旧 voter 离线、替换、新 voter 当选、旧 voter 恢复后继续写入的真实故障回归。 |
| 3 快照取消 | **原路径已替换并有回归**。新版本使用具名快照、manifest、reader pin；owned task 从发布到内存 meta 更新持有串行保护，调用方取消不会直接截断提交。 | 保留 `snapshot_published` 暂停后取消、恢复、重新打开及旧快照回收的实际测试；不再补一套针对旧 `snapshot.bin` 覆盖协议的实现。设备掉电仍未验证。 |
| 4 drain 后初始路由 | **仍需优先处理**。`ensure_route` 仍选择配置中首个 `serves_broker` 节点，未按新鲜持久元数据筛选 schedulable。 | 在现有权威／CAS 下选择可调度 Broker，并保证未完成创建可重放。补 drain 首个 Broker 后创建 Topic 及同操作重试回归。 |
| 5 生产协议限额 | **仍需优先处理**。公共 ProduceBatch 解码后直接构造 AppendBatch，数据组仍使用 `ProducerLimits::default()`。 | 统一公共语义限额与持久限制，提交前拒绝超限输入。用 raw QUIC 验证 limit／limit+1，拒绝时 committed_end 不变。开发阶段不需要维持旧格式兼容，但仍需明确数据状态的限制来源。 |
| 6 小帧读预算 | **仍需修正**。当前仍从 max_frame_bytes 无条件减去 8192，合法 8192 B 配置会得到零预算。 | 按可编码 envelope／洞／投递数计算预算，或在配置验证时明确拒绝不可读组合；补小帧完整 produce→claim→read→ACK 和队头大消息回归。 |
| 7 迁移 Finish 清理 | **仍有恢复闭环缺口**。Finish 仍至多等待 250 ms 并忽略错误，旧 Flight 拒绝新计划发生在 fresh_view 前，ticker 只恢复未 complete 记录。 | 用持久 cleanup-pending 重试，或读取权威证明后安全释放已完成旧 Flight；禁止无条件丢弃执行中的 repair。补丢弃首次 Finish 后自动进行第二次迁移的回归。 |
| 8 启动失败回滚 | **仍需修正**。QUIC server 已 spawn 后才绑定 management；该 await 失败会从 RunningNode 构造前返回。 | 先完成 listener bind，或用启动 guard 显式停止并等待所有已启动任务及 Raft/WAL。补端口占用失败后在同进程原配置重启、监听／锁释放回归。 |
| 9 Read 批量持久化 | **部分已优化**。有界热页复用减少重复 hot_read／读取屏障，实际回归保留；RecordDelivery 仍逐条持久提交。 | RecordDeliveryBatch 是独立协议／状态机改动，需保持每条身份、token、窗口、fence、lease、冷热 pin 和 Unknown 前缀语义。先做正确性设计，性能比较按后续手动安排。 |
| 10 冷读重复解码 | **优化建议仍适用**。相关 reader／coordinator 未在本轮改变。 | 选择带完整性校验的块索引／Range 或有界已验证索引缓存，保留共享正文的许可归属。与批量签发分开处理；本轮不以静态复杂度推导加速倍数。 |
| 11 对象账本 4096 | **已知生产化限制仍在**。Catalog 的永久对象身份是全局元数据目录上限，Deleted 仍占位；数据组 intent 有独立上限。 | 修正文档“每组”口径；后续设计安全退役／墓碑压缩或分片，不直接删墓碑腾空间。补跨组累计和迟到旧请求回归。 |
| 12 Active 前目标死亡 | **已披露限制仍在**。非 Active 阶段仍只恢复原 owner。 | 独立设计更高 epoch 的接管替换，完整保留 fence／recover／activate；不能通过放开 desired_owner 修补。 |
| 13 覆盖率波动 | **观测问题仍需跟进**。旧版同 Rust 源码的两次 LLVM 计数不同，不能归因于代码回退。新分段版精确门禁通过不解释旧差异。 | 先保留 per-file 差异并对异步分支增加确定性触发。覆盖率门禁本轮不降阈值、不删范围；暂停性能及联合晋升后仍严格比较现有基线。 |
| 14 超大 HELLO | **已修复主要断言**。当前接受 Err 或 EOF，禁止任何成功数据帧，并验证拒绝计数及零接收帧；已有原生红绿与完整回归。 | 原报告的“EOF 仅静态解释”只适用于旧报告现场，后续原生证据另行保留。不扩大到允许成功 payload。 |
| 15 性能正文与 key | **下一次性能测量前应补齐**。负载代码未改变，原建议仍适用。 | 校验完整确定性正文及 key，增加头后字节、尾字节、key、头部损坏用例。该性能工具已于 2026-10-26 按用户要求删除。 |

关键源码：[运行时配置／启动](../../src/runtime/node.rs)、[RPC 授权](../../src/replication/rpc/server.rs)、[初始路由](../../src/runtime/metadata.rs)、[生产／读取](../../src/runtime/data.rs)、[逻辑日志上限](../../src/replication/store.rs)、[追加限额检查](../../src/replication/log.rs)、[owned 快照](../../src/replication/state_machine.rs)、[取消回归](../../src/replication/cancellation_tests.rs)、[迁移 Flight](../../src/scaling/remote/data.rs)、[Finish](../../src/scaling/remote/mod.rs)、[自动迁移](../../src/scaling/remote/tick.rs)。

OpenRaft 版本从 Cargo.lock 核对为 0.9.25；本地对应依赖源的 config 默认 `since_last:5000`，运行时没有覆盖 snapshot_policy。原报告的上游链接不代替当前配置检查。外部 crates.io／docs.rs 页面本次未能由浏览工具读取，没有据此臆测新 API。

## 其余 A—J 建议

| 建议 | 处理判断 |
| --- | --- |
| A 准入精度、B SDK 复制 | 可以优化，先保留峰值余量与 backing allocation 计费；在正确性问题后按分配／准入证据选择最小改动。 |
| C HELLO 能力协商 | 应明确双方有效限额，和第 5／6 项一起设计，避免服务接受但 SDK 无法读取的组合。 |
| D 全局 mutation 锁 | 有价值，但短 CAS 与可重放远端步骤涉及一致性，不能只缩短锁作用域。 |
| E WAL segment | 当前分段 WAL 已实现；第 1 项逻辑容量策略仍需单独处理。 |
| F OTLP batch | 可改善满队列与 shutdown 效率；保持队列、内存和总期限上限。不是业务消息丢失结论。 |
| G CI 反馈时间 | 本轮按用户要求先移除自动性能、独立 allocator／heap profiling 专项 CI。是否进一步按路径拆分重型功能门禁另行评估。 |
| H action 固定 SHA | 可独立加固；本次没有依赖漏洞扫描结果，不认定存在具体漏洞。 |
| I 失败 WAL 制品 | 本地已保存的私有原始证据先保留到 worktree 外；未来手动测量需明确大小／文件白名单与 secret 排除。不将 CI artifact 仅有日志误称持久保存了 WAL。 |
| J principal／session 隔离 | 先明确同 topic 内的隔离契约，再决定是否将 principal 绑定到持久 session；不能仅凭客户端自选 HELLO session 认定实现了隔离。 |

本轮落实 CI 调整、文档准确性和 worktree 审计／清理；上述尚存业务问题未混入 WAL 合并。建议后续按 1／2、4／5／6、7／8 的边界分别设计、回归和提交，再安排批量投递及冷读优化。2026-10-26 已按用户要求删除性能测试工具、配置和基线，本次部署验收不运行性能测试。
