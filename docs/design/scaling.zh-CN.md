# 弹性、成员替换与排空

`src/scaling` 使用真实 OpenRaft API 执行复制组成员替换；修改发现地址或节点列表不会执行共识成员变化。生产 RF3 保持三个 voter 的稳定配置，三个 voter 分别放在三个 zone；中间 joint 同时满足旧、新多数派。RF1 仅用于显式本地模式，没有从 RF3 自动降为 RF1 的入口。

## 存储成员替换

1. 从新鲜元数据读取 `DataGroup` 的 generation、旧 voter 和节点身份。`Placement` 综合字节率、CPU、连接数和 ACK 状态占用选候选；新节点必须已经注册 Storage、允许调度、满足三区放置，且有复制量之外的容量余量。观测最多 256 条，默认五秒失效；普通均衡有 60 秒冷却，故障修复可以绕过冷却。
2. `MembershipPlan` 只替换一个 voter。规范操作 ID 绑定 cluster、group、旧 generation、旧/新 voter。元数据给每个已有组保留一个当前迁移槽和 prepare/complete 两个原结果；CRUD 账本满时仍可完成已有组修复。pending 计划不能换目标；超时、丢响应和重启均恢复原计划。
3. `remote::Coordinator` 发现 metadata leader，以固定计划调用 Prepare。metadata leader 重新验证真实 quorum 与同 applied 锁中的稳定 membership、角色、zone、组配置；自行向 data leader 请求实际受限快照及配置副本目录的物理文件长度。复制预留还计入日志和快照硬上限的增长、临时替换及至少 1 GiB 余量，并将 `copy_bytes` 与原计划一起持久化。它向目标节点取得不超过五秒的本地容量观测，同时累计其他未完成计划对该节点的预留；CPU、字节率、活动连接和 ACK 状态观测缺失时拒绝分配。发起方的数字只可提高复制预留，不能降低真实源端上界或伪造目标余量。
4. 调用 `add_learner(node, BasicNode, true)` 追赶。追赶后的控制授权重新取得，避免长时间传输沿用启动时的证明。随后调用 `change_membership(target, false)`，由 OpenRaft 提交 joint 和 stable；在任一阶段结果未知时保留原计划并对账，不重新选择目标。
5. 完成 `ensure_linearizable()` 后，使用 `try_with_membership` 在同一 applied 读锁中读取真实应用身份、applied log、StoredMembership。只有单一稳定配置恰好等于 target，且提交位置覆盖 membership 日志，才生成不可反序列化、不可由外部字段构造的 `StableMembership`。证明有效期从读取发起开始，最多三秒。
6. 元数据消费该能力生成的 complete 命令，检查原计划、原 generation 和 placement，更新 voter 并将 generation 增一。重复 complete 返回原结果；退休计划不能用更高 attempt revision 复活。

metadata/data leader 不必相同；每个 `remote::Service` 仅持有本节点实际托管的 replica。`Rpc` 使用配置证书绑定的确切目标、独立 request/operation 相关性和原 deadline，调用各组当时的真实 leader。封闭 Query 仅包含 Group、View、Estimate、Capacity、Prepare、Change、ObserveStable、Complete、Finish，编码上限 16 KiB，不开放通用 `metadata::Command` 或 `DataCommand`。读写受 mTLS role/group/class 校验；metadata leader 的 Complete 自己向当前 data leader 取新鲜实际 stable 观测，经 cluster/group/plan/voter/位置核验后才构造私有证明，调用者不能提交自报证明。每节点控制、只读、源快照采样及迁移工作都有有限准入，忙时拒绝以保护原生共识控制。

已经持久 complete 的原计划重放走只读确认：metadata 的新鲜 Group 必须仍保存同一 canonical plan、原 generation+1 和 target；随后向当前 data leader 请求真实 quorum 与同 applied 锁的 ObserveStable，再次取得 metadata Group 确认原 complete 未被替代。证明仍从 data 请求开始计三秒，包含最后一次 metadata 确认时间，不刷新有效期。该路径不重新进入 Prepare、Change、Complete，不取得组的变更锁，不写 membership log 或 metadata revision；现有 repair lease 仍通过受限 Finish 对账释放。原记录退休、代次前移、目标不符、joint、实际证明无效或回复丢失均保守拒绝/Unknown。pending 继续原三阶段，不做整段自动重试。

若 leader 变化或 quorum 不可达，返回阻塞/未知并恢复同一计划；新控制器通过 Group 查询返回原 pending 计划，不能盲建替代操作。完成后 Finish 对旧、新节点进行有限清理；未清理的 lease 由同一计划重试对账释放。joint 或声明/实际成员不一致时，新生产与控制能力保持保守暂停。元数据应用快照版本为 2；旧版目录显式拒绝，不隐式把没有复制预留的旧记录当作新版。

元数据历史 fence/recovery 的 LogId 可能由已移出的 leader 产生。已提交冻结证据的快照校验核对 cluster/group、合法位置和永久注册的 Storage 身份；新证据仍须满足当前 voter 校验。已注册 Storage 身份及 zone 保留，退出节点设置 `schedulable=false`，不能删除角色或把旧身份改到另一区。这样不会因成员替换使原先成功的 fence/ACK 恢复记录变为非法。

对象恢复依赖独立于成员数量。成员替换不按离线 TTL 删除对象引用或恢复保护。T08 必须对当前稳定全部 voters 完成实际 checkpoint/snapshot 屏障，才能释放替代对象所保护的旧恢复依赖；joint 时暂停该操作。

## 修复预算和普通流量

同节点所有复制组共享 `RepairBudget`。默认同时两项修复，32 MiB/s，单次最多 1 MiB，最多 8 MiB 等候字节，注册表硬上限 32。FIFO 许可等待使用原 deadline，取消释放字节和等待名额，不能无限建立每组任务或连接。

`PeerPool::with_repair_budget` 必须接同一个预算实例。只有注册的 `(target_node, group)` learner 的 R/B 帧在交给 QUIC 之前受 pace 限制；正常 voter 的 R、全部 C 心跳/选举与普通冷读 B 不消耗该修复预算。网络等待记录到 `Stage::Queue`，并包含在 `Stage::Replicate` 中。重试不会取消已进入 Raft 的工作。

成员执行器在 Unknown 后保留对应 repair lease，继续限制后台追赶；恢复同一计划完成 stable 和元数据后才释放。运行时需在组的整个迁移生命周期保留 coordinator；不能丢弃 coordinator 后继续让该组执行无预算修复。节点退出先停止新的迁移并完成/恢复已有迁移，再关闭复制任务与预算。

## 有界自动触发

运行时的 owned ticker 调用 `remote::Service::tick(&mut Placement, fixed_standby_map, deadline)`，不创建第二套调度器。配置最多 16 个数据组，每组当前 voter 和固定 standby 的并集最多 32 个节点；每轮只评估一个组并最多推进一个计划。选择按 round-robin 排序，优先原 pending 计划、已停止调度的节点和已确认不可达的副本。pending 只恢复原 operation/复制预留，Unknown 不切换目标。

每轮最多并发四个只读容量 RPC，每次上限 500ms，仍受原总 deadline 限制。`CapacityUnavailable` 表示本地采样器无有效数据，不能当作故障；真实 RPC 不可达连续至少三轮且跨越至少三秒，才可作为网络故障修复候选。节点仍可通信时，仅该组实际 OpenRaft `running_state` 已退出或实际 applied failed gate 已关闭，才返回绑定确切 group/node 的 `ReplicaFailed`；它只影响被查组，不将同节点其他健康组判死，也不接受调用者自报健康布尔值。读取本地状态超时仍返回容量不可用。故障信号只用于候选选择，实际执行仍须当前多数派、learner 追赶和原计划证明。普通均衡要求 voter 和 standby 容量观测齐全，归一化 CPU/流量/连接/ACK 得分至少改善 200，完成后同组冷却 60 秒。故障或已停止调度节点的修复可以绕过冷却，不能绕过目标容量、三区或共识检查。源端快照估计后重新采集容量，避免耗时快照使旧观测被沿用。

`Placement::diagnose` 返回固定的 group、operation、原因枚举和单调观测年龄；原因包括缺样/过期、磁盘余量、CPU、应用字节率、活动连接、ACK 逻辑状态、拓扑、冷却、已均衡、准入和 Unknown，没有无界指标标签。冷却和不可达连续次数属于本进程采样历史，重启重新建立；持久 migration slot、复制预留和 generation 高水位继续用于恢复与防旧操作复活。

## 计算节点排空

先将节点 `schedulable=false` 持久化，阻止新的组/所有权分配；已有状态和身份保留。`DrainPlan::inspect` 根据真实元数据返回仍拥有的分区、Storage 组、未完成成员计划以及 Metadata 角色阻塞。它不以修改节点发现记录作为“已退出”。

Broker 逐分区使用 `Handoff` 固定旧 epoch、route generation 和目标 owner：`prewarm` → `drain_owner` → 规范 grant/fence/recover/activate。`DrainActions` 是运行时的语义边界。预热不迁移全部历史正文；先停止该分区新业务准入，有限在途 ACK 按现有 Finalization、fence、quorum 和原 deadline 完成。结果必须恰好是旧 epoch+1、旧 route generation+1，未知时重新查询同一 grant。

Gateway 扩容承接新连接；缩容由 `reconnect_gateway` 执行有限受控重连。QUIC 路径迁移不保留另一个进程的 TLS/连接状态；SDK 重新握手、鉴权，并按原 producer/sequence/operation 恢复未决结果。运行时只有在持久阻塞清零且本地连接/在途许可实际排完后才退出。Metadata 角色是显式阻塞，不能当作无状态 Gateway 一并关闭。

## 观测和边界

`ScalingMetrics::register` 接收服务端唯一、无前缀根 registry，不安装全局设施。只有八个 stage：Queue、Learner、Membership、Metadata、Prewarm、Drain、ServiceRecovery、ReplicaRepair，没有 Topic/组/节点/operation 标签。指标为 `tidemq_scaling_stage_duration_seconds{stage}` 和 `tidemq_scaling_unknown_outcomes_total`。前者记录本地单调时间，bucket 为 1ms、10ms、100ms、500ms、1s、5s、10s、30s、120s、600s、+Inf。进程重启会丢失未完成操作的本地计时起点，不能从 wall clock 拼接假精确时延。

`PressureSampler` 每次读取实际进程 getrusage CPU 累计量和 transport 已收发应用字节累计量，至少两次有效观测计算速率；采样间隔必须在 250ms 至五秒内，计数回退或 CPU 配额变化需要重新建立基线。Linux 以 available_parallelism 与 cgroup v2 当前及最多 32 层祖先 cpu.max 的最小值归一化，包含小数核配额；不支持或无法读取配额时容量不可用。macOS 使用宿主实际可用 CPU，source 明示口径。该字节率是应用帧收发速率，不能当作包括 IP/UDP/TLS 开销的网卡吞吐。采样只有一个 owned blocking worker，超时后继续持有名额直到真实结束；首样和缺样都不能填零。

传输层 `TransportSnapshot::active_connections` 和 Prometheus `active_connections` gauge 统计仍保有连接资源的物理连接，关闭但仍被 stream/Connection 持有的排空资源继续计入；Session/Connection 克隆不重复计数，最后一个 ConnectionState 释放时归零。容量与排空不得使用累计 connections_total 替代此值。

服务恢复与恢复完整副本分别统计。计划 Broker 移交从预热开始至新路由验证成功记 ServiceRecovery；本地执行器的存储修复从原计划开始至 stable+metadata 完成记 ReplicaRepair；远端执行器从 data Change 建立本地 repair lease 至 Finish 核实 metadata complete 记 ReplicaRepair，源快照估计和 prepare 需结合其他阶段及外部时间线查看。不同 leader 的本地执行跨度可能分别观测，不能将 histogram count 当作逻辑迁移次数。它们不能代替客户端故障发生至 SDK 第一次成功的全链路测量。日志只在逻辑完成边界输出 operation/group/generation，不在状态机重放或每个字节/重试输出日志，不含正文、token 或私钥。

本地真实 UDP/TLS 测试验证共识和调度边界；同机 RF3 不是物理三区证据。≤1 秒计划迁移、Broker/Gateway P99≤5 秒、单区 P99≤10 秒仍是候选目标，需要完整运行时和真实资源测量。

依据：[OpenRaft 0.9.25 元数据](https://crates.io/crates/openraft/0.9.25)、[版本化动态成员变更](https://docs.rs/openraft/0.9.25/openraft/docs/cluster_control/dynamic_membership/index.html)、[add_learner/change_membership API](https://docs.rs/openraft/0.9.25/openraft/struct.Raft.html)。
