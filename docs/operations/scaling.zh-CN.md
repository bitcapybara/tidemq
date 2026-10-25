# 扩缩容和成员替换排障

本手册对应 `src/scaling` 的真实共识执行器、固定迁移槽和运行时 drain 接缝。不要直接修改 voter 配置、清空计划或删除仍有状态的节点目录。

## 存储节点替换

1. 保存当前 cluster、group、generation、旧 voter、目标 voter 和规范 operation ID。检查三个目标 zone、已注册 Storage 身份及目标 schedulable；向目标取得新鲜容量报告，包含复制量、已有预留和至少 1 GiB 额外余量。
2. 检查真实数据 leader 和 metadata leader，各自 quorum 健康；只变更一个 voter。运行时共享 node RepairBudget 和 PeerPool 预算，控制并发及 R/B 字节，不调整正常 C 心跳来“加快复制”。
3. 以受信 metadata 服务身份调用 `remote::Coordinator::plan` 和 `execute`，保留返回的原计划。metadata leader 自行请求源物理复制上界和目标观测，将复制字节预留持久化；正常顺序为 prepare、learner 追赶、joint、stable、实际 data leader 的线性化同锁证明、metadata complete。Group 查询或 `membership_change(group)` 可读取原计划、`copy_bytes` 和 complete 状态。域内同进程测试也可使用 `MembershipCoordinator`。
4. 任何超时/连接中断都先保留 operation ID，查询原计划、实际 membership 和目标 applied 位置。若已 stable 而元数据仍旧配置，用原计划重取真实证明补 complete；若仍 joint，用同一 target 继续，不能改成第三套成员。
5. 完成 stable 和 metadata 后，重新取得数据组 authority 并对齐新 voter/generation。仅当所有组都不再引用退出节点、没有 pending 计划、对象恢复依赖通过当前完整稳定 voter 的快照屏障、安全排空全部本地任务时，才能关闭节点。

首次迁移尚未 complete 的 Unknown，与已经 complete 后重放确认的 Unknown 必须分开。前者恢复原 pending；后者先按原 plan 对齐 canonical operation、generation、target 和持久完成记录，再取得真实稳定成员证明及再次 metadata 确认。完成记录不是跳过真实 quorum 的理由；退休记录不能复活。只读重放不应产生新的 membership log、metadata revision 或 SDK ACK 前沿变化。RPC 错误链保留固定阶段及原 typed source，例如 `verify completed stable membership`，线缆错误分类和原 deadline 保持不变。

`schedulable=false` 只停止新分配。节点身份、Storage 角色、zone 和数据目录的保留是历史证据的一部分；不通过删除身份绕过成员变更或对象恢复保护。

## 自动均衡阻塞

自动 tick 每轮只推进一个组/计划，固定 standby 必须实际托管该组并注册合法 Storage/zone。先检查 `Placement::diagnose` 的 group、operation、blocked、age_ms；再结合 Group 查询的持久 pending 和 `copy_bytes`。MissingObservation 表示缺失有效采样，不表示零负载或节点已宕机。真实 RPC 连续不可达且满足时间窗才作为网络故障修复候选。进程仍存活时，`ReplicaFailed` 只能来自被查组实际 Raft 已退出或本地 apply failed gate，必须匹配 group/node；它不使同节点其他组失效。容量缺样、语义拒绝和本地状态读取超时不构成组故障。原 pending 始终优先恢复。

Balanced 表示当前得分改善不足 200，Cooldown 表示普通均衡尚未过 60 秒。CPU、ByteRate、Connections、AckState 和 FreeCapacity 指向具体的受限资源；ACK 字节是实际保留记录的逻辑 metadata 口径，不能当作精确 heap。进程重启后冷却和不可达计数重新观测，迁移记录不会清空。

## Broker / Gateway 缩容

新 Broker 注册并启动后，Metadata 控制器也会检查已有所有权分布。两个可调度 Broker 的分片数相差超过 1 时，每次最多移交一个已有分片，成功后冷却 60 秒；不为单分片在两个节点之间反复移动。目标须响应身份/接入检查，并提供含 RPC 耗时在内不超过 5 秒的容量观测，通过 CPU、网络、连接、ACK 状态和预留容量检查；移交复用 RepairBudget、预热、停止旧 owner 准入和持久 takeover。布局或历史权威正在变更的范围暂不参加均衡。此策略按所有权数量均衡，写入热点由下面的 Topic 策略判断，60 秒不是性能校准结果。

已持久开始的节点排空优先执行，不等待普通均衡冷却。新增容量不改变既有 Producer、Consumer、消息身份或确认域；调用遇到临时路由失效时沿原操作恢复。

先持久化停止新分配，然后按有限批次预热、停止该分区的新生产/投递准入、排空有限在途、执行固定旧 epoch/route 的移交。fence/恢复/激活间中断后恢复原 grant；更高 epoch 已取代时，旧操作只允许返回 Superseded。成功路由必须精确对应新 owner 和旧代次+1。

Gateway 需通知运行时执行受控连接关闭/重连，保留在途最终确认所需的限时能力。SDK 遇断连按原业务身份对账；Unknown 不能改新 producer、sequence 或 operation 重发。检查本地连接和在途许可实际归零，再退出进程。

`DrainPlan::inspect` 仍显示 owned_partitions、storage_groups 或 pending_membership_changes 时不得退出。Metadata 角色单独阻塞，须按元数据组自己的受信成员操作完成，不与无状态入口同时删除。

## Topic 自动 Split / Merge

Metadata / Combined 节点可配置 `runtime.topic_scaling`。缺省关闭；启用时必须显式填写 `capacity` 的全部八项正值：`publish_bytes_per_second`、`publish_messages_per_second`、`delivery_bytes_per_second`、`deliveries_per_second`、`read_requests_per_second`、`ack_bytes_per_second`、`acknowledgements_per_second`、`ack_requests_per_second`。它们表示当前 RF、批量和读写组合下单个 Raft 组的容量基线，所有 Metadata 副本使用相同配置。功能测试使用人为降低的阈值，不提供生产容量数值；本阶段不进行压测或参数校准。

默认每 10 秒评估，高水位为容量的 70%，连续 60 秒；低水位为 30%，连续 10 分钟；新窗口或布局变化后至少冷却 5 分钟。Merge 窗口至少为 Split 的两倍，低水位严格低于高水位，分片数默认 1–128。配置校验拒绝零容量、无界窗口和非法数量。

Data Raft 持久记录写入、交付和 ACK 的计数，以及固定 16 桶哈希摘要，不保留 key 或正文。观测来自实际 Data Leader 的新鲜 quorum 查询；查询包含进程身份和 owner epoch，重启、owner/布局/组代次变化、计数回退或采样间隔超过三个周期均重建窗口。缺失/过期观测停止相关动作；尚未首次写入的分片，只有新鲜 quorum 明确证明提交位置和消息计数均为零时才能作为空分片使用。

Split 需要连续写入高负载和有收益的哈希切点，两侧至少各占已观测写入字节的 10%。单热点 key 或过窄热点只报告 `HotKeyOrNarrowRange`；读取/ACK 压力和归档停滞分别报告，积压本身不触发切分。目标必须具有不同的实际 Data Leader、完整 RF/故障域、各副本新鲜容量与预留余量；其他未完成布局、成员和权威转移计划占用的资源不能再次分配。无独立容量时报告 `StorageCapacityRequired` 和所需组数，不创建云资源，也不提前 Seal。

Merge 仅选择相邻 Active 区间；两源、合并后目标组及其历史回放负载必须在完整低负载窗口内满足全部八项余量，且不低于最小分片数。历史读取与 ACK 纳入目标负载，原 Stream lane、Queue 成员和消息身份继续有效。自动动作与故障修复、均衡共享 RepairBudget，执行已有持久 Prepare/Seal/Activate 恢复流程；最后一次 CAS 再次核验观测对应的布局、owner、目录和成员配置。

`/diagnostics.topic_scaling` 返回固定大小的最近决策及资源需求；`metadata.pending_layouts` 最多列出八个未完成计划。控制器保留至多 256 个当前及历史数据域，总观测预算 1 MiB；超限或预算不足报告 `ObservationCapacity` 并暂停自动动作，人工管理与持久消息不受删除或隐式回收影响。

## 指标到阻塞原因

| 现象 | 检查 | 处理 |
| --- | --- | --- |
| Queue 延迟上升，正常 C/R 健康 | 节点修复并发、有限字节队列及原 deadline | 等待原计划释放名额或减少新计划；不叠加无界重试 |
| Learner 长尾，复制数据增长慢 | 目标磁盘/网络、pace 字节率、目标实际 applied、剩余容量、观测年龄 | 修复根因并刷新容量报告；保留同一计划和 repair lease |
| joint 中断 | 实际 joint 两套 voter、多数派可达性、leader | 恢复原计划 target；两套多数派不满足时保守停止变更 |
| 实际 stable，metadata 未完成 | 原 migration slot、generation、当前真实 quorum 证明 | 同一计划重新获取三秒有效证明并补 complete |
| Authority / Placement 拒绝 | 角色/zone、实际与声明成员、新目标 schedulable、五秒容量观测、其他 pending 预留、源快照/物理文件上界 | 先恢复合法条件；CPU/流量/活动连接/ACK 状态缺样本时等待有效采样，不能填 0 或用调用者数字替代 |
| Overloaded 拒绝 | 控制/读取准入、单一源采样 worker、组内执行锁、节点 repair routes | 保持原计划并按原总 deadline 有界退避；超时 worker 仍占本地名额直到真实结束 |
| drain 无进展 | 持久分区/组/计划阻塞，本地连接和许可 | 逐项完成所有权移交/成员替换/受控重连，禁止强行清表 |
| ServiceRecovery 快但 ReplicaRepair 慢 | 分别查看两条时延，不混用口径 | 服务已恢复不表示三区完整冗余已恢复；继续有预算修复 |

`/diagnostics` 的 placement_capacity_available 表示本进程是否具备完整新鲜容量，data_groups.voters/joint_configs 来自原生实际 membership，bootstrap_voters 单列为启动配置，不能混用。传输资源 gauge 分别是 tidemq_internal_client_active_connections、tidemq_internal_server_active_connections 和 tidemq_external_server_active_connections；将同实例现有三项相加，与容量采样的连接口径一致。closed 但仍保留句柄的排空资源也继续计数。字节率看板统计应用帧字节，不包括完整 UDP/IP/TLS 开销。

看板为 `deploy/observability/dashboards/scaling.json`，告警为 `deploy/observability/alerts/scaling.yml`。Unknown 计数说明需要对账，不能据此断言数据未提交。重复 unknown 告警有持续窗口，避免每次可重试错误触发独立告警。

本地验证：`rtk cargo test -p tidemq --locked --test scaling_remote --test scaling_raft --test raft_quic --test application_failure`。`scaling_remote` 在真实原生 QUIC quorum 上验证不同 metadata/data leader 和封闭 RPC 字节往返；语义 RPC 的运行时 QUIC 装配另由 runtime 测试覆盖。测试使用隔离目录与真实 s2n-quic；`TIDEMQ_TEST_TRACE=1` 仅用于有界实验诊断，不作为生产全量日志默认值。物理三区、外部 UDP LB/CID 和正式恢复 SLO 在对应部署环境单独验收。
