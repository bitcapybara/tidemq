# 元数据、所有权与控制有效性

本批实现 T03 的确定性元数据状态、数据组 fence 状态、基于真实 OpenRaft 线性化读取的本地授权适配器，以及诊断和观测接口。代码在 `src/metadata`、`src/ownership`；生产者、持久 ACK、远端 QUIC 控制 RPC 和计划迁移仍须由相应模块联调。这里不把纯状态测试视为完整接管验收。

## 权威和身份

元数据组持久保存 Topic、分区到数据组映射、节点拓扑、订阅配置、授权 epoch 和条件路由。消息正文、producer 去重原结果、cursor/attempt/ACK、writer fence 和恢复状态属于数据组，不能用 Broker 内存、日志或 trace 代替。

`cluster_id`、Topic、订阅和操作使用不可为空的 UUID；节点和复制组使用非零 `u64`。Bootstrap 显式指定不可变的 `metadata_group_id`，数据组不能使用同一个 ID。Topic/订阅删除是保留身份的退休记录；同名重建使用新 UUID。组没有删除或 ID 回收入口。本批不实现在线分区拆分。

`LocalRf1` 只允许单 voter 数据组；`ThreeReplica` 只允许三个不同 Storage 节点，且声明的 zone 各不相同。没有自动降为 RF1 的路径。节点停止接收新分配时设置 `schedulable=false`，保留已有组和持久状态；引用中的 Storage 角色/zone、Broker 角色不能被直接移除。实际 Raft 成员变化由共识层执行，修改发现地址或拓扑记录不会调用成员变更。

zone 是受权配置中的放置声明。同机标记三个 zone 的测试不能证明物理三区容灾。

## 命令和有界状态

`metadata::Command` 携带 `operation_id` 和全局 `expected_revision`；实体更新还核对相应 generation、epoch 或 route generation。成功决策推进 revision。业务拒绝通过 `Response::Rejected` 返回，不等同于 Raft RPC 失败。

普通 CRUD 账本保存规范序列化命令的 BLAKE3 摘要和原 Response。相同操作、相同内容返回原结果；变更内容或预期 revision 后重用操作 ID 会冲突。已记录的 CRUD 拒绝也返回原拒绝，调用者修改前置条件时应生成新操作。未通过形状/长度校验的输入不进入账本。

接管有独立于 CRUD 的固定容量：每个已有分区只保存当前接管的 Grant/Fence/Recovery/Activate 四个决策槽，更高 epoch 安全替换旧槽，永久保留 epoch 高水位。接管操作 ID 规范派生自 cluster/分区/目标 epoch/owner/预期路由；阶段 ID 绑定 grant 和 stage，调用者不能把旧身份改为新 epoch 再执行。CRUD 使用 UUID v4 操作身份，内部规范派生身份使用 UUID v8，避免退休控制身份被拿去当普通 CRUD 重建。原结果窗口外，`lookup_operation` 明确返回 `UnknownOrRetired`。

接管成功后的原结果可用刷新后的尝试 revision 和更高读取屏障幂等查询。逻辑摘要绑定冻结 grant/fence/checkpoint，不包含会前移的 `CommitPosition` 或尝试 `expected_revision`；新的证据仍需合法提交位置且不能早于原屏障。未满足前置条件的接管尝试不占决策槽，允许以同一逻辑身份刷新 revision 重试。CRUD 满表不会消耗现有分区的接管容量。

响应超时或连接关闭是结果未知。对账读取也必须先获得线性化屏障；一次查询暂未发现结果不能排除原请求仍在执行。继续用同一个操作和同一份内容重试，不自动换身份。

默认上限如下，可在 Bootstrap 时降低，不能通过普通命令增加：

| 权威记录 | 默认硬上限 |
| --- | ---: |
| Topic，包含退休身份 | 1,024 |
| 分区总数 / 每 Topic 分区数 | 8,192 / 256 |
| 订阅，包含退休身份 | 4,096 |
| 节点 / 数据组 | 256 / 256 |
| CRUD 原操作决策 | 16,384 |
| 当前接管原结果 | 每个已有分区固定 4 槽 |
| 单条元数据命令编码 | 64 KiB |

名称采用 1–128 字节 ASCII 字母、数字、点、横线和下划线；zone 上限 64 字节。订阅的在途消息数、字节数和 ACK 空洞配置有独立上限，默认并行完成；`PerKey` 只是持久配置，真正的串行调度由订阅和 SDK 模块实现。

订阅记录表达期望配置。Topic/订阅退休不隐含 ACK、不推进 cursor，也不直接解除 retained-floor 保护；实际创建、删除、seek 和保护变更必须在所属数据组内排序。跨分区配置部分完成时保留保护，按配置 generation 对账。

CRUD 容量用尽返回 `Capacity`。Topic/订阅退休身份与 CRUD 原决策不会自动淘汰；扩展新增配置的生命周期容量前，需要审查可持久拒绝旧身份的退休/压缩协议。已有分区仍可接管、fence、恢复和激活，接管轮次不增加槽位数量。本批没有“清空账本继续运行”的管理命令。

## 接管流程

1. 元数据提交 `GrantOwnership`：核对当前 epoch、route generation 和目标 Broker，分配严格增加的 epoch。原路由停止作为当前路由返回；此时新拥有者还不可写。
2. 数据 leader 对受信元数据权威执行新鲜线性化读取，精确核对 cluster、Topic/分区、组、operation、epoch、owner、预期路由代次和元数据 revision。成功得到不可反序列化的 `VerifiedGrant`，消费它才能生成 fence 命令。
3. fence 在数据组中提交。相同 grant 重放返回原 fence；低 epoch 拒绝，相同 epoch 但不同内容拒绝。装入新 fence 后立即拒绝旧 owner/epoch，旧 Gateway 缓存不能绕过检查。fence 前合法提交的生产结果由数据组保留。
4. 数据应用从同一权威快照和必要日志尾部恢复 producer 与持久 ACK 状态。`FenceState::complete_recovery` 只供受信应用使用，记录绑定 fence generation、producer/ACK 状态 revision、committed end 和规范状态摘要的检查点。它不是网络 RPC，也不能用调用者的 `recovered=true` 代替。
5. `DataAuthority` 先取得真实数据组线性化屏障，再在同一 applied 读锁中读取 fence/恢复检查点与 `LogId`。元数据先记录 `FenceObserved`、再记录 `RecoveryObserved`。每份证据必须匹配当前 grant，并来自当前组 voter 的合法提交位置。
6. 使用重新验证的恢复证据提交 `ActivateRoute`。只有当前 grant、原 fence、完整恢复证据和预期 route generation 全部匹配，才将路由代次推进一次。重复激活返回原路由。

元数据中的阶段是 `Unassigned → Granted → Fenced → Recovered → Active`。更高 epoch 可以取代任何未完成接管；迟到的旧 fence/恢复/激活证据返回 `Superseded`。在 fence 与激活之间重启，可以恢复原操作继续完成，不需要删除旧证据或降低 epoch。

`check_writer` 要求当前 node/epoch 且恢复完成；实际新生产还必须持有来自 ActiveRoute 的有效 `ControlLease`，并走数据 quorum 提交。仅安装 fence 不授予新生产许可。

## 证书身份与提交授权

内部 mTLS 只证明证书绑定的 cluster/node/role，组 membership 决定允许调用哪些 RPC。普通合法内部节点不能凭 node ID 或自报更大 epoch 安装 fence。

本批采用新鲜权威读取，不增加离线签名票据体系。`VerifiedGrant`、`VerifiedFence`、`VerifiedRecovery` 的构造私有，且不实现 Deserialize/Clone/Debug；在消费为内部命令时再次检查有效期。`ControlLease` 的创建工厂也限制在 ownership 模块内。`MetadataCommand`、`FenceCommand` 的 serde 只用于 Raft 持久化和受信进程内调度，**禁止把它们直接暴露为通用 InternalCommand 网络接口**。远端适配必须验证配置允许的权威服务身份、角色和组权限，执行真正的线性化查询，不接受客户端自报的证据。

本地适配器使用 `MetadataAuthority` 和 `DataAuthority<A: OwnershipApplication>`。数据应用直接提供其真实 `FenceState`。适配器核对 cluster/group，并核对实际 Raft voter 集：元数据组还须满足模式、Metadata 角色与 zone 放置；数据组 voter 必须精确匹配受信配置。未知或 joint 配置时，本批保守拒绝生成新能力，等待 T11 的经验证成员变化路径。

OpenRaft 使用非 Byzantine 故障模型；授权 Raft voter 是可信共识成员。QUIC 组权限必须阻止无关内部节点发送共识 RPC。类型封装不替代运行时的 RPC 授权。

## 控制有效期和降级

默认有效期 3 秒，硬上限 10 秒，从线性化查询**发起时**的本地单调时钟计时；网络和排队耗时也占用此窗口。读取、applied 锁等待或响应到达已经耗尽窗口时，不能生成能力。失败刷新不能延长旧窗口，不逐消息写全局元数据 Raft。

`ControlLease` 仅从当前 ActiveRoute 的新鲜权威读取产生，不实现 serde/Clone。进程重启、leader 更换后必须重新验证，磁盘旧 grant、快照和 wall clock 不能恢复或续期本地有效性。

| 操作 | 控制窗口到期后的行为 |
| --- | --- |
| 新生产、新投递、控制变更、清单发布、GC 授权 | 拒绝 |
| 到期前已接纳的 ACK/读取收尾 | 仅限已有 `Finalization`，仍需当前 fence、权限、数据 quorum 和原 deadline |
| 新建 ACK/读取收尾许可 | 拒绝 |
| 已登记发布/恢复任务的对账 | 不因超时认定失败或删除；重新获得相应权威证据 |

收尾额外窗口最多 1 秒，同时受原请求 deadline 限制。默认最多 256 份许可，硬上限 4,096；同一 `MetadataAuthority` 的刷新共用许可池，刷新不会重置已使用名额。permit 释放归还容量。新 fence 立即使旧 grant 的收尾许可失效。许可本身不替代 ACK/读取的权限校验或数据组 quorum。

窗口依赖本地单调时钟正常推进，不依赖节点间时钟同步。系统不能瞬间发现多数派丢失。进程/主机冻结或休眠恢复后的强制重新验证由运行时负责；在这一路径完成验证前，不承诺保留休眠前的授权。时间戳不参与确定性 `apply` 或持久状态机语义。

## 快照、诊断和观测

元数据快照包含所有当前映射、阶段证据、退休身份、CRUD 原决策和每分区四个接管决策槽。数据应用快照必须包含完整 `FenceState`。解码后执行 `validate_snapshot`，检查预算、索引、角色/放置、grant、派生操作身份、阶段、route generation、提交位置和恢复摘要之间的一致性；状态与内部命令使用普通 struct、外部标记 enum 和 BTreeMap，兼容非自描述二进制 serde。

`summary` 直接读取有限计数；`diagnose_ownership(partition)` 是按键查找，输出 operation、epoch、owner、阶段、路由代次和 fence/恢复提交位置。`topics_page` 每页至多 64 条、32 KiB，游标为最后返回的 Topic ID。诊断注明 `local_applied` 和 revision，不冒充新鲜授权；查询不推进 ACK 或修改所有权。远端管理面的权限、deadline 和采集时间包装由 T10 接入。

`OwnershipMetrics::register` 不安装全局 registry，只有四个 stage（Grant/Fence/Recover/Activate）和四个 outcome（Committed/Rejected/Unknown/Failed），不使用 Topic、分区、节点、组或 operation ID 标签：

| 名称 | 类型与用途 |
| --- | --- |
| `tidemq_ownership_transitions_total{stage,outcome}` | 阶段结果计数；未知结果经原操作对账，不能直接计为失败重做 |
| `tidemq_ownership_transition_duration_seconds{stage}` | 单调阶段耗时直方图，0.001–10 秒及 +Inf |
| `tidemq_control_validity_remaining_seconds` | 运行时所监测有效授权的最小剩余秒数，未验证/过期为 0 |

指标总 series 数固定；运行时应按逻辑操作记录，传输重试另计，不在状态机重放时输出新完成事件。`emit_transition` 的结构日志字段为 event/operation_id/epoch/stage/outcome；不记录 token、证书私钥或消息正文。本批提供模块接口和有限标签测试；管理监听、统一导出及完整接管时间线由运行时联调。

## 验证与扩缩边界

纯状态测试覆盖原结果/冲突、Topic/订阅退休身份、RF3 放置拒绝隐式 RF1、配置和诊断预算、fence/恢复/激活顺序、快照中途恢复和更高 epoch 取代。真实 Raft 测试使用磁盘存储与 RF1 屏障验证授权和恢复；其检查点 fixture 只验证授权接缝，不能替代 T04/T05 的真实业务恢复。

T11 才负责预热、停止新分配、排空、受控重连、learner/joint 成员变化、限速迁移和退出。这里只提供这些流程需要的条件 grant/路由和节点调度状态。服务恢复时间与恢复三个完整副本的时间分别观测，不以本批测试宣称秒级或三区目标通过。

相关实现依据：[OpenRaft 0.9.25](https://crates.io/crates/openraft/0.9.25)、[线性化读取 API](https://docs.rs/openraft/0.9.25/openraft/struct.Raft.html#method.ensure_linearizable)。操作步骤见[所有权排障](../operations/ownership-troubleshooting.zh-CN.md)。
