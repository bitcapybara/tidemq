# TideMQ 消费者长轮询设计

状态：已按本轮 review 修订，仍待确认；不代表已经实现或通过运行验证。本轮修订覆盖 fan-out 子请求身份、Unknown 读取的信用隔离、按阻塞原因唤醒和数据组共享读取结果容量通知，并同步调整验证方案。

基线：本地 `main`，`bc1f491d6f60557f30f2b9b1981d7a2c41084634`。本文独立于现有开发计划和订阅设计；外部产品比较见[消费者消息交付模型调研](../research/consumer-delivery-models.zh-CN.md)。基线源码在本次设计时已静态核对，没有运行实现、故障或性能测试。

## 1. 结论与范围

推荐采用**两阶段长轮询**：SDK 先通过只读的 `GroupWatch` 等待一个或多个 shard 的可读条件可能发生变化，再在有消息数和字节数信用时调用现有 `GroupRead` 获取消息。`GroupWatch` 只返回变化提示和观察令牌，不分配消息、不推进订阅状态、不占用消息预取信用，也不写 Raft。真实读取、持久投递证明、业务 ACK / NACK 和 `GroupRetireRead` 保留现有语义。

本方案服务 Stream 和 Queue 两种消费者，业务仍使用现有 `receive`、`ack`、`nack`、`ack_cumulative` 和 `close`；业务不需要管理 shard、长轮询、令牌或手工发送信用。Stream 的 lane 顺序、累计 ACK 范围和旧代次隔离，以及 Queue 的逐条 ACK / NACK、成员归属和重投规则，均不能因共用读取调度而改变。

本次设计不引入持续服务端推送、消息推送回调或新的订阅类型。等待与领取拆分会在空闲变为可读时多一次请求往返，这是降低空读 Raft 写入、避免空闲等待占用消息信用的代价；是否满足延迟目标必须实测。

本方案不要求沿用旧 wire 协议或旧持久格式，也不设计新旧版本混跑和在线滚动升级。实现和发布需明确协调 Gateway、Broker、Storage 与 SDK 的版本；同版本的重连、成员变更、单节点故障和 RF3 接管仍在本设计范围。若以后需要混版本滚动升级，须单独设计协商和兼容路径，不能把“未知命令”默认为空结果。

## 2. 当前代码与问题

| 当前事实 | 影响 |
| --- | --- |
| [SDK 驱动](https://github.com/bitcapybara/tidemq/blob/main/sdk/rust/src/group_consumer/driver.rs)对每个消费者最多并发 8 个读取任务，单次最多预留 32 条；空读后计划间隔递增至 640 ms。 | 在现有 `GroupRead` 里直接长时间等待，会占住读取任务；空闲 shard 多时，其他 shard 可能无法及时读取。 |
| [消费者](https://github.com/bitcapybara/tidemq/blob/main/sdk/rust/src/group_consumer.rs)默认最多预取 256 条 / 8 MiB；[消息](https://github.com/bitcapybara/tidemq/blob/main/sdk/rust/src/message.rs)的消息槽和字节内存有不同释放时点。 | 必须分别计算读取在途、未确认消息和对象克隆占用；不能在等待阶段预留整批消息额度。 |
| [读取路径](https://github.com/bitcapybara/tidemq/blob/main/src/runtime/consumer_read.rs)、[读取状态](https://github.com/bitcapybara/tidemq/blob/main/src/subscription/group.rs)和[数据组写入](https://github.com/bitcapybara/tidemq/blob/main/src/runtime/metadata.rs)表明，正常空 `GroupRead` 记录可重放结果，随后退休也需写入。 | 仅延长空读间隔仍有周期性 Raft 写入；把空响应当成未提交会破坏原身份恢复。 |
| [dispatcher](https://github.com/bitcapybara/tidemq/blob/main/src/server/dispatch.rs)默认有 128 个数据 handler 槽；[路由](https://github.com/bitcapybara/tidemq/blob/main/src/runtime/service.rs)可经 Gateway、owner 和 leader 多跳转发。 | 在普通 handler 内等待会在多个节点长期占槽、帧内存和转发 RPC 额度。 |
| GroupRead 的空结果可能来自 lane、额度、顺序、重投时间、冷读或其他约束。 | Watch 只能报告“状态可能变化”，不能把未变化解释为全局无 backlog。 |

## 3. 必须保持的契约

1. **提示不产生投递。** Watch 成功、取消、超时、重试或丢包都不分配消息、不生成 delivery proof、不改变 GroupRead 的 sequence，也不确认消息。只有现有的持久 GroupRead 路径可以授予交付。
2. **投递仍由当前权威决定。** 真正读取时重新检查身份、租户、成员 epoch、assignment / lane、writer fence、数据组 leader、额度和冷数据证据；旧 Watch 提示不能绕过任何检查。
3. **结果不明继续恢复原操作。** 一个 GroupRead 一旦可能被提交，就必须保留原 operation ID、member epoch、sequence、消息数 / 字节数和原读取内容；回复丢失或取消不能隐式换成新 sequence。已入 SDK 队列而退休结果未知时，只恢复原退休操作，不能再次入队。
4. **等待与业务处理分开。** Watch 超时不是消息处理超时，不使在线 Queue 成员已接收但未 ACK / NACK 的消息自动重投；成员撤销和有效的 NACK 等仍按现有权威规则处理。
5. **有界资源。** 所有请求、目标、等待者、内部转发、定时器、令牌、回复和排队都计数且计字节；长轮询不借用 Raft、heartbeat、ACK 和取消所需的保留容量。任一准入失败必须有显式结果，不伪装为 `UNCHANGED`。
6. **公平性有前提。** 对仍被分配、拥有信用且服务可用的 shard，调度最终再次观察并尝试读取；普通未决读取不能用尽预先隔离的最小进展信用和请求容量。该保护自身结果 Unknown、业务持有额度、零信用、持续拒绝准入、无 quorum 或网络不可达时不承诺有限交付延迟，必须暴露退化和错误。
7. **通知可丢失，消息不能丢失。** 令牌只用于提示；权威状态始终是现有 Raft / 订阅状态。版本失效、通知丢失和计时器延迟应触发有界重查或保底读取，不能使有资格的消息永久不可见。

## 4. 总体流程

```text
                          bounded, read-only watch
SDK watch pages  --------------------------------------> Gateway
       ^                                                    |
       | changed / reset / retry / unchanged                | bounded fan-out
       |                                                    v
SDK fair ready queue <------------------------------ owner / data leader
       |                                             apply-change tokens
       | message and byte credit
       v
GroupRead -> durable delivery proofs -> SDK queue -> receive -> ACK / NACK
       |
       +--> GroupRetireRead using the original read identity
```

1. 消费者加入后，SDK 从现有 layout 和成员状态获得已分配的活跃 / 历史 shard。新的目标没有有效观察令牌，先建立 Watch 基线，并将该目标标记为需要一次真实读取；初始化不能因为“没有新事件”跳过已有 backlog。
2. 已被标记为可能可读的目标进入去重的就绪队列。SDK 以轮转方式获取有限消息信用，构造现有 GroupRead；消息和返回证明进入本地队列后，按原协议退休读取结果。
3. 空读成功且退休完成后，使用**发出该次读取之前**记录的观察令牌继续 Watch。读取期间或退休期间发生的变化不能被较新的本地状态覆盖掉；如果观察到新令牌，保留一次待处理提示。
4. Watch 在观察到变化、令牌失效、目标出现错误或等待到期时返回；SDK 根据每个目标的结果分别调度。普通等待到期不执行 GroupRead，也不执行 GroupRetireRead。
5. 若通知路径不完整，SDK 对每个仍被分配的 shard 安排低频保底 GroupRead；通过轮转和信用上限执行，不能一次性对所有 shard 突发空读。

## 5. Watch 协议

以下名称与字段是供 review 的协议草案，不是现有 API。复用现有 Frame 的 `request_id`、`operation_id` 和剩余 deadline；新增 `GroupWatch` / `GroupWatchResult` 及 Watch 专用取消。每次网络尝试使用新的 `request_id`；同一逻辑分支重试使用相同 `operation_id` 和目标摘要，拆分后的不同分支使用不同的 operation ID。

```protobuf
message GroupWatch {
  bytes topic_id = 1;
  bytes subscription_id = 2;
  bytes member_id = 3;
  uint64 member_epoch = 4;
  uint64 assignment_version = 5;
  uint64 attempt = 6;
  uint32 wait_ms = 7;
  repeated WatchTarget targets = 8;
  bytes root_operation_id = 9;
  bytes parent_operation_id = 10;
}

message WatchTarget {
  bytes shard_id = 1;
  uint32 birth_partition = 2;
  bytes observed_token = 3;
}

message GroupWatchResult {
  repeated WatchResult targets = 1;
}

enum WatchStatus {
  WATCH_STATUS_UNSPECIFIED = 0;
  WATCH_SYNC = 1;
  WATCH_CHANGED = 2;
  WATCH_UNCHANGED = 3;
  WATCH_RESET = 4;
  WATCH_RETRY = 5;
  WATCH_REJECTED = 6;
}

message WatchResult {
  bytes shard_id = 1;
  WatchStatus status = 2;
  bytes observed_token = 3;
  tidemq.common.v1.ErrorCode error_code = 4;
  bool retryable = 5;
}

message GroupWatchCancel {
  bytes root_operation_id = 1;
  bytes operation_id = 2;
  uint64 attempt = 3;
}
```

**编码和范围。** 同一请求只覆盖一个 topic、subscription、member 和 member epoch；目标必须属于该成员当前允许观察的分配。请求的 `assignment_version` 是 `ConsumerControlResult` 中的元数据 plan version；数据组内的安装版本是独立值，当前安装逻辑会基于 plan version 和 fenced 阶段计算，不能把两者原样比较或当成同一个编号。服务端依据元数据的当前成员 / plan 和该目标实际安装状态核对其对应关系；计划未完成、状态不一致或任一权威不可达时返回 `RESET` / `RETRY`，不返回 `UNCHANGED`。目标数暂定最多 32，令牌有固定长度上限，编码后还必须小于实际配置的控制帧和字段上限；不能只按目标数估算。若最小合法单目标帧也无法容纳，初始化或配置校验明确失败。`birth_partition` / `shard_id` 只是寻址输入，服务端必须核对真实 layout 和 route，不能信任客户端自称的 group、owner 或租户。[plan / 安装版本](https://github.com/bitcapybara/tidemq/blob/main/src/runtime/consumer_control.rs)、[SDK fence](https://github.com/bitcapybara/tidemq/blob/main/sdk/rust/src/group_consumer/assignment.rs)

**结果逐目标明确。** `SYNC` 表示此前没有有效令牌，返回当前令牌并要求至少一次真实读取；`CHANGED` 表示与客户端观察令牌不同，返回当前令牌并标记为可能可读；`UNCHANGED` 仅表示该目标本次成功完成观察且令牌未变，不是无消息证明。`RESET` 表示 source incarnation、路由或观察状态失效，SDK 更新路由 / 分配并重新建立基线；`RETRY` 和 `REJECTED` 分别携带可重试和终态错误。响应必须为每个输入目标给出一个结果；未访问、超时无法确认、部分转发失败或被拒绝的目标不能填 `UNCHANGED`。收到 `RETRY` 的目标保留旧令牌并重新安排观察，不因此假定产生新投递。

**等待与期限。** `wait_ms=0` 是立即返回的只读探测；非零值的候选上限为 1 秒。实际等待取该值、原 Frame 剩余 deadline 和授权有效期中允许的最短值，并为编码、逐跳回复和清理保留时间；不足以安全等待时立即返回或报期限错误。1 秒是本稿实验参数，不是已验证的产品 SLO 或必须公开的配置。

**父子请求身份。** SDK 发出的根请求满足 `root_operation_id = Frame.operation_id`、`parent_operation_id` 为空；内部转发保留根 ID，并以当前分支的 operation ID 作为下一跳的 parent ID。子分支作用域包含逻辑转发阶段、权威 data group、route epoch 及规范化目标摘要；摘要覆盖排序去重后的 shard、birth partition、原令牌和固定身份 / 等待参数，不包含每次变化的 request ID、attempt 和逐跳剩余 deadline。采用固定域和长度编码，从 `(parent_operation_id, child_scope)` 确定性派生子 operation ID，遵守现有 ID 位宽；完整摘要仍用于发现身份冲突。不要把物理 leader 地址当成 data group 身份；同一 leader 承载不同数据组，或同一父请求经不同 owner 汇合时，子 ID 仍须不同。分支目标、路由作用域或原令牌改变就创建新分支，不能复用旧 ID；不同父分支不合并为同一个下游登记。

**重试和重复登记。** 同一分支的身份摘要、目标集、原令牌和原 deadline 不可变化；同一根任务的重试递增 `attempt`，逐跳原样传递，逐跳传输重试只改变 `request_id`；attempt 不得回绕。原 deadline 指固定任务截止时间，剩余时间逐跳递减不得延长它。相同摘要及 attempt 的重复请求复用原登记或明确返回重复 / 重试结果，不追加 waiter。同一个节点按 `(principal, session, member, member_epoch, root_operation_id, operation_id)` 维护有期限登记：更高 attempt 原子替换旧等待，旧 attempt 的回复、清理和取消不能覆盖新登记；相同 ID 的不同摘要被拒绝。身份字段不授予权限，每跳仍校验认证链和目标。父节点维护本次 attempt 的有界子分支表，通过 request ID 关联确切分支和 attempt，不能仅凭根 ID 匹配。

**取消范围。** `GroupWatchCancel.operation_id` 指向被取消的分支，取消帧自身使用独立 Frame operation ID；它只取消同一认证主体下匹配根 ID、分支 ID 和 attempt 的登记，返回取消确认或明确错误。普通 Frame Cancel 若不能表达 attempt，则不能用它无条件删除 Watch 登记。取消父请求时先核对父 attempt，再逐个取消该代次登记的子分支；旧父请求清理不能扫描并取消新 attempt 的子树。节点对已取消 / 替换 attempt 保留至原 deadline 的有界代次水位并计费，迟到重复登记不能使其复活。路由变更后旧节点可能暂时保留旧登记，SDK 尽力取消，节点还必须依赖原 deadline / 最大等待时间自动释放。该去重只约束观察资源，不是持久业务去重；跨节点不能假定只有一个等待副本。

## 6. 变化令牌与唤醒

每个目标返回不透明令牌，逻辑上绑定 `(source incarnation, target identity, shard / route epoch, target readiness revision, member epoch / assignment version)`。`target identity` 包含 topic、subscription 和 shard；readiness revision 只反映该目标相关的 publish / cursor 变化或阻塞解除。**不把全租户 quota revision 或全订阅 ACK revision 直接并入每个目标令牌。** 编码可以合并这些值，但比较必须是同一授权目标上的精确相等；令牌不是授权、offset、ACK 或可交付保证，客户端构造任意令牌不能获得权限。节点重启、leader 任期变化、观察表重建 / 淘汰、快照安装或 authority 迁移必须使旧 incarnation 不相等；计数器溢出时更换 incarnation 并让相关 Watch `RESET`，不能静默回绕。

### 6.1 阻塞原因与依赖登记

Watch 注册和事件复查从已应用状态只读推导目标的可能 backlog、当前资格和阻塞原因。可复用最近一次空读的分类，但必须核对成员、读取 sequence 和状态版本，不能用过期分类证明当前无消息；不新增正文读取、冷读 pin、grant Renew 或 Raft 写入。基础监听覆盖本 shard 的 publish / topology、相关 cursor / lane 和成员身份；只有**确实受某资源阻塞且仍可能有消息**的目标才登记该资源依赖。已确认空闲的 shard 不因同租户热点的 ACK / 额度恢复而变为 dirty。

| 阻塞原因 | 依赖作用域和重新检查条件 |
| --- | --- |
| 当前没有候选消息 | 自身 shard 的 publish / 历史边界及相关 cursor 变化；不订阅其他 shard 的租户额度事件。 |
| lane / 顺序 gate、订阅投递窗口或重投时间 | 实际 cursor / lane / 共享窗口键或该 cursor 的 due 时间；仅检查可能因此前进的目标。 |
| 租户在途、速率或冷读额度不足 | 现有配额的实际资源键，包含 tenant、额度种类及其 data group / authority 等真实作用域；仅登记受该额度阻塞的目标。时间恢复的额度使用有界到期重查，不能仅等待 ACK。 |
| 数据组共享读取结果容量不足 | 以 data group 为键的 read-result capacity；它跨该数据组的订阅、birth partition 和租户共享，不能缩成单个 subscription 或 tenant。 |
| 路由、成员、授权或分类无法确认 | 返回 `RESET` / `RETRY`；不能伪称无变化，也不能为不可信目标创建永久依赖。 |

依赖表保存资源 generation / revision 供内部防竞态使用，原始资源 revision 不直接改变公开目标令牌。资源事件只访问其有界依赖集合，合并后按公平、有限批次只读复查；如果仍然无候选消息或受其他条件阻塞，更新依赖基线并继续等待，不递增 readiness revision，也不要求 SDK 发起 GroupRead。仅当目标从受阻转为可能可读，或发生本目标相关变化时推进 readiness revision；同一资格状态不因重复探测反复递增。多个阻塞条件应保存完整的有界原因集合，或每次复查重新计算，不能解除一个条件就忽略其他条件。无法完整分类时走显式退化和保底路径。

### 6.2 事件来源

| 变化来源 | 处理 |
| --- | --- |
| 新消息持久提交并应用、shard seal / 历史边界变化 | 更新相应 shard 的 publish / topology 状态，只使该 shard 的相关目标重新检查或推进 readiness revision。 |
| ACK 释放窗口、有效 NACK、重投 due 时间到达、显式重试、成员安装 / 撤销、lane / gate 改变 | 定位受影响的 cursor / lane / 共享窗口依赖，不广播给整个订阅的空闲 shard；到期由有界定时索引触发，不为每条消息无界创建任务。 |
| 租户在途额度、速率或冷读额度恢复 | 更新实际配额资源 revision，只复查登记该阻塞原因的目标；其他目标令牌不因全租户计数变化而改变。 |
| 非空读取结果退休、成员清理或其他操作实际释放共享 read-result slots | 比较应用前后数据组共享容量；容量增加时更新该资源 revision，只复查该组中受 read-result capacity 阻塞的目标，跨租户也适用。幂等退休不重复发布容量增加。 |
| authority / leader 变化、授权失效 | 使相关观察 `RESET` / `RETRY` 或拒绝，仍按实际路由和成员范围处理。 |
| 正常空 GroupRead、其结果退休、与可交付性无关的普通 heartbeat | 不因命令类型本身递增 readiness revision；若确实释放某项共享资源或回收撤销交付，只按实际变化通知相应依赖。 |

共享 read-result capacity 的依据是 [data group 预算](https://github.com/bitcapybara/tidemq/blob/main/src/broker/groups.rs)里的 `max_total_window_slots - usage.reads`，不是某个 cursor 的业务 ACK 窗口。现有 [读取退休](https://github.com/bitcapybara/tidemq/blob/main/src/subscription/group.rs)会清除保留的读取交付结果；业务 ACK 可以早于退休完成。必须覆盖“A 已 ACK、结果仍保留，B 因共享容量为零空读；仅 A 退休就使 B 可读”的路径，不能等 B 自身 publish / ACK 或 30 秒保底才发现。相关事件仍只复查该资源的阻塞者，不能将每次 RetireRead 广播给该组全部 shard。

由持久状态驱动的事件应从**已提交并应用的数据状态变化**产生，不能仅在 RPC handler 收到成功响应时产生：后者会漏掉已提交但回包丢失、内部命令、接管和恢复路径。当前 [Raft 状态机](https://github.com/bitcapybara/tidemq/blob/main/src/replication/state_machine.rs)在应用批次时持有已应用状态锁；实现时应在同一应用顺序内更新轻量 revision，再在锁外合并并发送通知。纯时间恢复和运行时资源释放由对应有界计时 / 资源管理触发，同样遵守登记、revision 和复查的同步顺序，不伪装为业务提交。应用锁内不得等待网络、分配无界队列或调用业务 handler。拒绝、幂等重放和空退休不能无条件触发自激循环。

Revision 可以是非持久的观察状态，但必须对已有目标及真实资源状态计入有界内存；在没有活跃 waiter 时仍须保存目标版本，或者淘汰目标 / 依赖分类时使之后的旧令牌明确 `RESET`。不能按任意不可信 target 建立永久条目。依赖复查队列也计数计字节，合并事件时保留“需要重查”标记；未完成复查前不得返回 `UNCHANGED`。若观察元数据无法分配或分类不确定，返回 `RESET` / `RETRY` 并依靠保底路径，不得影响已提交消息的持久结果。

### 6.3 注册竞态

目标 leader 先在有界注册表登记 waiter，再读取当前 incarnation / readiness revision 并与请求令牌比较；按相同已应用状态计算阻塞原因，登记依赖后再核对资源 generation / revision 和资格，消除“读出容量为零、容量释放、才登记依赖”的丢唤醒窗口。依赖集合变化时先登记新依赖并复查，再撤销旧依赖；和应用更新采用明确的同步顺序。公开令牌不同时立即返回 `CHANGED` / `RESET`；相同且依赖已复查才等待。应用更新先推进对应状态 / 资源 revision，再投递可合并通知；资源变化经第 6.1 节复查后才推进目标 readiness revision。返回 `UNCHANGED` 前必须再次核对 incarnation / readiness revision、相关资源依赖、资格、权限和期限；复查无法及时完成则返回 `RETRY`。

这样，变化发生在注册前时通过版本比较发现；发生在注册后时通过通知或返回前复查发现；在 GroupRead 期间发生时通过读取前令牌发现。通知队列满、唤醒丢失或调度延迟最多退化到本次 Watch 的有限等待与复查；若事件分类本身遗漏，则依赖第 8 节的保底 GroupRead。所有错误和不确定情况都不能转成 `UNCHANGED`。

对于延迟 NACK，定时索引依据已持久的 `due_ms` 计算唤醒时间；无活跃 waiter 时可移除运行时定时器，但下次注册必须重新检查是否已到期。时钟跳变、计时器重建和 leader 变更不能提前签发消息：实际 GroupRead 仍用可信服务端时间和状态机检查。不得仅因 Watch 到期而重投在线成员尚未 ACK 的消息。

### 6.4 空闲成员鉴权与数据 grant

元数据中的成员 heartbeat 和数据组中的 delivery grant 是不同期限。当前 SDK 定期续元数据成员；数据组 grant 接近过期时，现有 [GroupRead 路径](https://github.com/bitcapybara/tidemq/blob/main/src/runtime/consumer_data.rs)的 `refresh_consumer_grant` 会查询元数据并提交 `GroupCommand::Renew`。如果让每次 Watch 复用这个写路径，空闲 Watch 仍会周期性造成数据组写入；如果只凭已过期的数据 grant 拒绝 Watch，长期空闲但仍有有效 heartbeat 的消费者又可能无法观察新消息。

因此 Watch 使用**只读成员证明**：先核对数据组已安装成员的身份、active 状态、fence 和数据安装版本，再通过元数据授权查询确认相同 client / credential / member / epoch 仍有效，并确认本请求的元数据 plan version 和 shard 分配。数据 delivery grant 的到期时间可以用于判定不能立即签发，但**不单独判定只读 Watch 的成员已退出**。现有 `authorize_member` 仅核对身份，不能单凭它证明成员 active；需要补充只读验证。若 metadata plan 与已安装数据版本不能建立当前对应关系，或 metadata quorum / 授权不可用，返回 `RESET` / `RETRY`，不得利用过期缓存继续声称已成功观察。

Watch 不调用 `refresh_consumer_grant`，也不延长数据交付 grant；收到变化后真正的 GroupRead 再按现有路径刷新 grant、重验当前成员和 fence，然后才能签发。现有元数据 heartbeat、必要的线性化授权查询和真正读取时的数据 grant 续期仍可能产生网络或 Raft 工作；“空闲 Watch 不写 Raft”只指 Watch 本身不提交业务写，不表示整个集群在空闲时零 Raft 活动。元数据授权结果的有效期目前有 1 秒边界，Watch 的实际等待 / 回复不能越过该证明或成员有效期。批量目标可对相同 `(topic, subscription, member, epoch, client, credential, data assignment version)` 合并有效期内的权威查询，但每个目标仍须核对自身 route / fence / 安装状态；缓存容量、到期和元数据负载必须计量，不能延长权限有效期来降低请求数。[成员授权查询](https://github.com/bitcapybara/tidemq/blob/main/src/runtime/auth.rs)、[元数据 heartbeat](https://github.com/bitcapybara/tidemq/blob/main/src/runtime/consumer_control.rs)、[成员身份检查](https://github.com/bitcapybara/tidemq/blob/main/src/subscription/group.rs)

## 7. 服务端等待与资源边界

Gateway 完成认证、基本帧预算和成员身份检查后，将 Watch 注册到独立的有界观察管理中；请求按当前 route 分组，经现有 owner / leader 权威链到目标数据组。每一跳分别验证委托身份和目标，leader 校验当前成员及 fence。观察管理保存紧凑身份、令牌、deadline 和有限回复句柄，等待期间不占普通数据 handler 槽、消息正文预算、数据读 pin、Raft 写入或旧 writer lease。

这需要改变当前 [WireService / dispatcher](https://github.com/bitcapybara/tidemq/blob/main/src/server/dispatch.rs)一直接管 handler 直到回复的模式：Watch 完成初始准入后，将可取消的回复所有权转交给有界等待管理，释放普通 handler 及原帧的大额预算；目标变化后由等待管理提交有限回复。不能仅在 `group_data_public` 里加 `sleep`。Gateway 到 owner、owner 到 leader 的转发也必须使用**单独核算且受总预算约束**的 Watch pending 额度，不能继续各持有普通数据 handler 或无限扩大 RPC pending；回复、断线、取消和 deadline 的所有权须跨跳明确。

一个批次按路由和权威 data group 拆成内部路径，并采用第 5 节的独立子 operation ID；不能因两个分支落到同一物理 leader 就复用父 ID。任一目标变化时可以提前完成父请求；其他目标只有在权威节点已完成复查并返回结果时才能标为 `UNCHANGED`，未返回的目标标为 `RETRY` 并保留旧令牌。其余内部等待须按各自子 ID / attempt 取消，确认后释放登记；取消结果未知的本地清理记录和远端登记仍计费，远端即使错过取消也在原期限内过期。父回复发出后仍存在的下游清理由有界、可 join 的任务持有；旧 attempt 的迟到回复与取消不能操作新子树。不可达目标不阻塞可用目标，也不伪报 `UNCHANGED`。批处理只是合并外部帧：跨不同 owner / leader 仍可能产生多次内部请求，性能评估必须计入实际 fan-out。

候选资源上限用于首轮验证，不是实测容量：每个节点观察内存不超过现有 normal 内存预算的 `min(8 MiB, 1/16)`，同时上限 4096 个目标登记；每个租户最多使用节点观察预算的 1/4，每个成员每节点最多 256 个不同的活跃目标。成员额度中预留最多 32 个目标给即时探测，长等最多占用其余 224 个；旧等待重叠、新成员或其他租户占满节点时，探测仍可能被明确拒绝，不能声称无条件预留成功。父请求、子请求、同一目标在同节点的多份上下文、阻塞依赖边、资源复查队列、定时索引、回复以及哈希表 / 任务实际容量都必须计入节点总数和字节；多跳在每个节点分别计费。依赖边数还须受每目标有限原因数约束，不能另建无界反向索引。上限不得额外扩张进程原有 MemoryBudget，也不能把该应用预算当作 RSS 硬上限。节点配置不足以容纳最小观察请求时，启动校验或准入应明确失败。

观察准入在租户间采用有界轮转，租户内按成员轮转；任一队列或内存达到上限时返回带退避提示的可重试资源不足结果。等待者不得阻塞 ACK / NACK、结果退休、heartbeat、Frame Cancel、Raft 控制或真实 GroupRead。客户端断连、取消、目标变化、权限撤销、节点 drain / shutdown 和 deadline 均应终止等待并归还本跳及下游额度；下游不响应时依其独立 deadline 兜底。现有全局 256 项取消登记不能被大量 Watch 占满，Watch 登记须独立限额且保留取消可达性。

## 8. SDK 多 shard 公平调度

SDK 为每个分配目标维护有界状态：观察令牌、是否有合并的待处理提示、上次读取时的令牌、未完成的原 Read 及其信用归属、恢复退避、保底检查到期时间和所属 member epoch。相同 shard 的通知只保留一个 dirty 标记，不按事件数无界排队。失去分配时不再发新读取；已可能提交的原 Read 按现有撤销 / 退休 / fence 规则完成恢复，不能仅因丢弃 Watch 状态而遗忘。

### 8.1 等待和探测分开

- `GroupRead` 最多 8 个同时进行的 RPC 尝试；未决原 Read 记录与活跃 RPC 分开，具体隔离见第 8.3、9 节。Watch 使用独立、有界的请求额度，不消耗这 8 个读取 RPC 或消息信用。初始候选为每个消费者最多 8 个批量观察请求、每批最多 32 个目标；其中最多 7 个可长等，至少 1 个留给即时探测。实际并发还受 Client 总 pending 请求和正常内存约束，Watch 不得借用真实读取的保护容量。配置很小时，观察和探测可轮流使用同一有限额度，不能占满 ACK / 控制预留。
- Watch 页按稳定轮转顺序运行。一页完成、变化或到期后，优先安排尚未覆盖的下一页，而不是让热点页反复占据全部位置。不同消费者共享同一个 Client 时，先轮转消费者，再轮转其页，避免一个消费者独占观察 pending。
- 当目标多于同时观察能力时，使用上述保留的 **`wait_ms=0` 探测槽**，对当前未被 Watch 覆盖的页轮转检查；它只比较观察令牌，不写 Raft、不分配消息。同一目标不会同时由该成员的长等和探测重复占额。候选发起速率上限为每消费者 64 页 / 秒，并受 Client、租户与节点总速率和内存额度限制。限速是上限，不代表每个消费者都被保证得到 64 页 / 秒。
- 连续可疑变化但实际空读时，同一目标按有限退避合并 dirty 标记，避免配额不足、非本 lane 消息或共享资源变化造成空读风暴；新事件不会把 dirty 清零。信用释放时及时唤醒就绪调度。零信用时不发真实 Read，可暂停观察；恢复信用后必须重新核对令牌并按公平顺序补查。

假设未覆盖目标实际被拆成 `P` 个合法路由页，某消费者在稳定可用条件下实测完成 `R_eff > 0` 个探测页 / 秒，则一次完整轮转需要约 `P / R_eff` 秒，另加网络、排队和读取时间；实际跨节点拆页后 `P` 可能大于 `ceil(shards / 32)`。例如 10000 个目标恰好能按 32 个一页分组且稳定完成 64 页 / 秒时，单轮探测约 4.9 秒，这是理想化计算而不是服务承诺。目标越多或实际额度越低，等待时间越长；观察饱和持续超过约定容忍时间时，SDK 记录并向等待中的调用暴露可重试错误，不能静默永久饥饿。是否满足生产目标由第 12 节的测试决定。

### 8.2 真实读取公平性

读取就绪队列按 shard 轮转：每次每个 shard 最多执行一个在途 GroupRead、每轮最多一个批次；完成后仍有 backlog 的热点 shard 回到队尾，不能连续抢占所有读取槽。已到期的低频保底读取也进入同一公平队列，不绕过信用或顺序约束。若有多个消费者共享 Client，客户端共享请求 / 字节准入也按消费者轮转；现有 `try_acquire` 本身不能单独证明跨消费者公平。

默认候选保底周期为 30 秒，并加有界抖动分散同时到期。30 秒是**开始排队的目标**，不是任意 shard 数量、拥塞或故障下的最大交付时延；每个 shard 的实际保底完成时间还受就绪队列长度、读取吞吐和信用影响。正常空闲情况下 Watch 超时不触发 Raft；只有初始化、真实变化后的误报或保底读取可能产生空 GroupRead / 退休。调度器持续积压、探测被拒绝或保底期限长期逾期时必须有指标与可见错误。

### 8.3 未决读取与健康目标隔离

不能让 8 个失败 shard 各占一个常驻读取任务并锁住全部信用。候选隔离规则如下，均在发请求前准入，而不是收到 Unknown 后才调整：

1. 每消费者最多保存 8 个普通未决原 Read，另允许 1 个使用第 9 节最小保护信用的原 Read；同一 shard 仍最多一个。它们是有界状态记录，不要求各持一个永久运行任务。普通读取及原身份恢复合计最多占 7 个活跃读取 RPC，剩余 1 个只用于无未决 Read 的目标发起保护读取，不借给普通读取、恢复或 Watch。
2. 普通和保护读取进入 Unknown 时，身份、请求内容、信用池归属和必要预留不变；本次 RPC 在有限尝试期限结束后归还活跃请求 / frame 额度，保留结果恢复所需信用。迟到帧只有重新取得接收 / 解码额度并匹配原身份后才处理，不能沿用已归还的传输许可。
3. 原身份恢复独立排队、轮转和有界退避，候选最多 2 个同时恢复 RPC，并计入上述 7 个普通 RPC；它们使用原信用，不再次预留正文。一次尝试不长期占用调度槽，不能用一个不可达 shard 的持续重试饿死新读取。结果已入队后只恢复退休操作，不能再次接受正文入队。
4. 普通信用或普通未决记录满时，仍无原 Read 的就绪目标可轮转使用保护读取；回包、退休和业务释放使保护额度可复用后继续服务其他目标。保护读取也 Unknown 时保留其额度并进入恢复，不能继续借出第二份保护信用；此时明确暴露保护容量受阻，等待原结果恢复或权威 fence，不能声称能容忍任意多个未知结果。

因此，最多 8 个普通原 Read 长期 Unknown 时，只要保护读取本身能完成、业务及时释放其消息 / 克隆、所需服务和共享准入可用，第 9 个健康 shard 仍可进展。该承诺不覆盖保护读取自身也失败、应用持有保护消息或最小配置退化的情况；恢复队列、普通 / 保护占用及受阻原因都应可观测。

## 9. 信用与内存核算

Watch 只持有观察元数据和自身有限 RPC 预算；不得占用消费者消息槽、可交付正文额度或租户 in-flight 消息配额。实际 GroupRead 发送前才同时预留：消费者消息槽、消费者字节、Client 共享字节、正常请求槽及传输 / frame 预算。多项预留不能全部成功时按反向顺序释放，等待或轮转重试；不能先声明信用再无额度接收回复。

### 9.1 先划分最小进展信用

记 `M = max_message_bytes`，现有消费者允许读取的最小预取字节量为 `B_min = M + 512`，现有服务端要求 GroupRead 的 `max_bytes >= M + 256`。每条消息另计 256 字节 SDK delivery metadata。初始化时从现有消费者总信用中划分保护池 `(1 个消息槽, B_min 字节)`，普通池为其余部分；普通请求的在途预留、Unknown 及其已交付消息 / 克隆都不能借用保护池。请求变成 Unknown 后才从剩余额度中划分保护池已经太晚。

隔离还必须落实到 Client 共享字节和正常请求 / 回复传输额度。共享 Client 对采用完整隔离的消费者登记相应最小保护份额，消费者自己的保护池不能被其他消费者的普通读取借走；所有份额仍在 Client 原总预算内，保护 RPC / frame 准入也不能被普通读取、恢复或 Watch 耗尽。准入与注册表都须有界，不能为任意数量消费者凭空承诺容量。保护读取的已交付对象仍占其原池，直到实际确认 / 撤销及最后克隆释放；不能把它挪到已满普通池来伪造空闲保护额度。

完整隔离要求普通池与保护池各至少容纳一个合法最小读取，并有相应请求和传输容量。消费者或 Client 配置不足时明确进入串行退化并暴露原因：只允许一个原 Read，用原身份恢复；其 Unknown 可以阻塞后续读取，不能宣称仍有跨故障 shard 的进展保证。若连一个合法读取都无法容纳则拒绝初始化。共享保护份额准入不足也必须明确报资源不足或该退化状态，不能静默借用已有消费者的保护容量；不改变公开业务 receive / ACK 模型。

默认 256 条 / 8 MiB、`M = 256 KiB` 时，保护池为 1 条 / 262656 字节，普通池为 255 条 / 8125952 字节。8 个普通未决读取的合计预留不得超过普通池，即使全部 Unknown 也留下一个最小合法读取。按旧方式为 8 个请求各预留 32 条 / 1040384 字节会用完 256 条且只剩 65536 字节，必须在发送前被新准入规则阻止。信用隔离可能减少普通批量和峰值吞吐，须纳入性能对照。

### 9.2 批量分配与释放

对普通池本轮实际空闲字节 `B`、槽 `S` 和就绪 shard 数，按第 8.3 节可用 RPC 和普通未决记录数轮转选择目标；一个读取的 `B_i` 至少为 `B_min`，并受每帧预算约束，条数上限取：

```text
slots_i = min(32, available_slots_i, floor((B_i - M - 256) / 256))
request.max_messages = slots_i
request.max_bytes = B_i - 256 * slots_i
```

仅在 `slots_i >= 1` 且 `request.max_bytes >= M + 256` 时发送；多读取并发时 `sum(B_i) <= B`、槽位之和不超过 `S`，不能每个请求都把同一剩余额度算一遍，也不能包含保护池的空闲额。`B_i` 在候选就绪 shard 间公平分配，而不是让第一个读取拿走全部可用字节；若普通池只够一个最大消息，一次只读一个 shard，下一轮换到队尾。保护读取固定最多 1 条、预留 `B_min`，仍须通过所有共享及传输检查；小配置的串行读取使用自身实际可用预算。协议配置还需验证最大合法消息、必要元数据和 GroupRead 帧余量能同时容纳，否则应明确拒绝配置，不能形成永久无法前进的消费者。

消息数信用至少覆盖请求在途和已交付但尚未完成对应确认的记录；成功确认 / 撤销按现有规则释放消息槽。消费者和 Client 的字节额度覆盖未完成读取的预留，以及 SDK 队列、待确认记录和应用持有的消息克隆：最终克隆释放前，不能把其已持有字节重新借给其他读取。消息槽释放不等于字节释放。

网络中的编码 frame 由独立的传输预算准入，解码 / 复制时它可能与预留给 SDK 消息的字节同时存在；两份真实分配应分别计费，释放时分别归还，不能把预取额度和 frame 额度当成同一块可重复使用的内存。解码过程的临时对象、容器容量和回复排队也须有上限并纳入对应预算。分别对普通池、保护池、Client 共享份额和节点预算验证恒等式，不将应用预算误称为 RSS 硬上限。请求结果 Unknown 时继续保留原 Read 的必要消息 / 字节额度和身份，直到原结果被恢复、明确拒绝或按现有 fence / cleanup 证明安全释放；活跃 RPC 尝试结束不等于释放这些额度，不允许超时就把额度重新借出，同时又接受迟到正文。

服务端仍独立执行租户配额、成员 grant、数据组窗口和冷读 pin 上限；SDK 信用不能替代服务端校验。低信用、最大消息、消息克隆持有和慢业务导致无法继续时是正常背压，不能将缺少信用解释为 shard 不公平或触发自动重投。

## 10. 故障、取消和状态转换

| 场景 | 必须采取的动作 |
| --- | --- |
| Watch 到期或回复丢失 | 它没有业务副作用；服务端确认未变化并在期限内成功回复时才返回逐目标 `UNCHANGED`。回复丢失或传输超时是错误，按同一观察身份有界重试，不能自行合成 `UNCHANGED`。过期远端登记靠取消和原 deadline 清除，不能累积。 |
| Watch 登记 / 返回期间消息提交 | 根据读取前令牌、登记后复查和版本递增发现；漏掉即时通知时下一次 Watch / 探测或保底读取仍能发现。 |
| 分支汇合、旧 Watch attempt 的回复 / 取消迟到 | 以独立子 operation ID、父子关系和 attempt 关联；只清理匹配代次的登记与子树，不能将不同 data group 的分支互相覆盖。 |
| GroupRead 已提交、回复丢失或取消 | 保持原 operation / sequence / 请求和普通 / 保护信用归属，按有界恢复额度重试；不可为该 shard 先发送新 sequence，也不可用尽健康目标的保护份额。保护读取自身 Unknown 时明确暴露进展受阻。 |
| 读取已入队，GroupRetireRead 回复丢失 | 已接收消息不能二次入队；重试原退休身份并保持原 deadline / Unknown 语义。 |
| 成员退出、lease 失效、重分配、旧通知迟到 | 停止该成员的新 Watch / Read，校验 member epoch 和 assignment version；先等待现有权威 fence，再按现有流程清理旧消息和未决读取，旧提示不能授权投递或 ACK。 |
| Gateway / owner / leader 断线、重启、网络隔离或无 quorum | 等待取消或超时，返回 `RESET` / `RETRY`；刷新 route / leader / source incarnation；无权威时不签发消息。恢复后原成员与原未决 Read 的身份仍须按现有协议处理。 |
| shard split / merge、历史权威迁移、冷数据回收 | 旧观察身份失效，刷新活跃及历史 layout；迁移遵守原权威和 fence。冷正文只由现有读取路径取得，不能用 Watch 暗中 pin 或推进历史 ACK。 |
| 额度满、通知内存耗尽、权限撤销、节点 drain | 返回明确错误 / 退化状态，终止相关等待并归还额度；不把失败解释为没有可读消息。服务关闭须在有限 deadline 内 join / 清理等待和内部转发。 |

SDK 提供的 `close` 继续报告真实清理结果；Watch 可丢弃但它的后台任务、等待句柄和下游请求必须有明确归属。断网时远端可能仍有等待者，只有远端超时、取消确认或原 lease / 身份失效后才能认为远端登记已被清理；不能凭本地 drop 宣称全链路完成。Watch 的清理状态与尚未确定的 GroupRead / ACK 结果分别报告。

## 11. 观测、范围与预期收益

建议新增低基数指标：Watch 准入 / 拒绝、`SYNC` / `CHANGED` / `UNCHANGED` / `RESET` / `RETRY`、等待者与目标数及字节、等待时长、页探测和实际 fan-out、分支冲突 / 旧代次丢弃、通知合并 / 丢失 / 重置、按有限阻塞原因分类的依赖数 / 复查数 / 真正 dirty 数、shard 观察和保底逾期、空 GroupRead / 退休及其 Raft 写入、普通 / 保护消息与字节信用、未决 Read / 恢复尝试、保护受阻 / 串行退化、队列长度、心跳 / ACK 进度。共享结果容量记录占用与实际释放事件，区分资源通知和最终真实读取，便于发现唤醒放大。指标不得以 topic、subscription、member、shard、operation ID 或 tenant 为无限制 label；需要细粒度诊断时使用受控 trace / 日志，禁止记录 payload、凭证和 proof。

在**稳定空闲且没有保底到期**时，Watch 的正常超时预期产生零次 GroupRead / 退休 Raft 写入；在持续数据和误报情况下仍可能发生空读。该性质是待测试的设计目标，不等于已经测得 CPU、网络或 Raft 成本下降。批量 Watch 会降低外部请求数，但内部 fan-out、定时器、授权、局部版本和持续的轻量探测仍有成本。不能把没有观测到的指标、没有运行的测试或协议推导当作性能证据。

## 12. 测试和验收方案

所有测试都应保留源码提交、构建 feature、二进制 / 镜像摘要、拓扑、配置、命令、开始 / 结束时间、故障窗口、消息身份账本、日志和清理结果。以下为实现后的计划，本稿没有执行。现有测试入口和外部依赖约定见[测试说明](../testing.zh-CN.md)。

### 12.1 确定性单元和协议测试

| 目标 | 必须断言 |
| --- | --- |
| 编码、鉴权和上限 | 0 / 1 / 最大 / 超限目标、帧大小、字段数、重复目标、伪造 shard / 租户 / member、旧 epoch、权限撤销、恶意令牌均有明确结果；无正文或 proof 泄漏。 |
| 分支身份与取消 | 一个父批次经两个 owner 到同一物理 leader 的不同 data group；子 ID 和登记独立，无目标摘要冲突或相互替换。同分支重试复用子 ID，改目标 / route 产生新 ID；旧 attempt 的迟到回复、取消和父清理不能删除新登记，已取消 attempt 的迟到重复请求不能复活等待。取消未知的清理记录和代次水位计费并最终归零。 |
| 竞态和令牌 | 对注册前、注册后、版本比较后、开始等待前、等待中、返回前的变化逐一注入；丢通知、通知合并、计数器重建 / 溢出、快照安装和旧 incarnation 都不能产生错误 `UNCHANGED`。 |
| 空读与反馈 | Watch 空等和到期不调用 GroupRead / Raft；空读及 RetireRead 无可读变化时不递增可读 revision；误报、错误和饱和不能制造无限自唤醒。 |
| 按原因唤醒 | 同租户 1 个热点 shard 持续 ACK，另有 1000 个已初始化空闲 shard：排除初始化 / 保底后，空闲目标令牌、dirty、GroupRead / RetireRead 和 Raft 写入不随热点 ACK 数增长；真正受相应 quota 阻塞的目标在额度恢复后及时重查。多重阻塞未全部解除时不发真实读取。 |
| 共享结果容量 | A 的非空 Read 已入队但退休暂停，先 ACK 释放业务窗口；同 data group 中另一个订阅 / 租户 B 仍有 backlog，却因共享 read-result slots 为零空读并完成空结果退休。仅放行 A 的退休，不增加 publish / ACK，B 必须在保底触发前被依赖通知唤醒并读到消息；其他 data group 与无该阻塞的空闲目标不 dirty。幂等退休不重复增容。 |
| 信用 | 多 shard 并发、单条最大消息、槽位和字节恰好最小、临界不足、预留中途失败、Unknown、迟到回复、ACK 与最后克隆释放，逐项断言许可恒等式及归零。 |
| Unknown 隔离 | 用默认额度将 8 个普通原 Read 依次置为 Unknown，再使第 9 个健康 shard 就绪；前 8 个不恢复时，第 9 个仍能使用保护份额读取、退休、ACK / 释放克隆后再次读取。普通未决不超过 8、保护未决不超过 1、活跃读取 RPC 不超过 8、恢复 RPC 不超过 2；原身份不变，无重复入队或超额。保护读取也 Unknown、另一消费者竞争共享额度，以及不足两份最小信用的串行配置，分别断言显式受阻 / 退化且不误回收信用。 |
| 公平和上限 | shard 数为 1、8、9、256、257 和超过同时观察上限；热点 + 稀疏 shard、两个消费者 / 租户、零信用 / 恢复、队列满和有界退避下，不出现静默饥饿或超额。 |
| 时间与重投 | 延迟 NACK 到期、时钟跳变、定时器重建、有效成员长时间不 ACK；只允许按原语义重投，Watch 超时本身不重投。 |
| 长时间空闲与授权 | 空闲时间跨越数据 grant 有效期且 metadata heartbeat 持续：Watch 仍按有效成员只读观察，不额外提交数据 grant Renew；随后写入消息时由 GroupRead 正常续 grant 并投递。分别记录 metadata heartbeat / 授权查询的成本；metadata quorum 丢失或授权证明过期后不返回 `UNCHANGED`。 |

时间和竞态测试应使用可控时钟、暂停点和状态机断言，不用固定 `sleep` 充当事件发生证明。资源依赖须覆盖“观察阻塞前 / 登记前 / 登记后 / 复查前后释放容量”、多个原因切换和复查队列满；公开令牌不变只能建立在相关依赖已复查的基础上。等待队列、父子映射、依赖索引、通知表、任务数及各信用池要覆盖正常、错误、取消以及反复重试后的最终归零。

### 12.2 真实 QUIC / Raft 集成

在现有测试基础上增加 Watch 场景，而不是只以 fake transport 验收：

- 真实 SDK → Gateway → owner → data leader，分别覆盖同节点和跨节点，确认消息从写入到接收、逐条 / 累计 ACK、读取退休及 Watch 到期路径。
- 将第 12.1 节的四项回归落实到真实链路：两个 owner 汇合到同 leader 的不同数据组并乱序取消；8 个普通 Read 丢回复 / 隔离而第 9 个 shard 正常；同租户热点 ACK 与 1000 个空闲目标；同数据组跨租户 A 已 ACK、仅退休释放 B 所需结果容量。用暂停点和请求身份 / 资源账本断言，不能只看最终消息总数。
- `test-hooks` 在 Watch 注册、状态应用、通知、GroupRead 提交、响应发送、读取入队、退休提交等位置确定性暂停；分别注入取消、消息到达、回包丢失、重复与乱序。核对原身份、允许的重复、没有缺口、旧代次不得进入队列及 ACK 前沿。
- RF1 / RF3、重启和 leader / owner 转移、Gateway 断线、数据组多数派中断、成员离开 / 重加入 / lane 重分配、权限撤销、节点 drain 和 shutdown；验证有限清理、原 Consumer 恢复和旧 owner 迟到事件被隔离。无 quorum 期间不签发新消息。
- 有界观察槽、正常数据槽、控制 / ACK 路径分别满载；持续写入、消费和 ACK，验证 Watch 不能耗尽控制面；多个租户竞争和恶意大批请求不能使其他租户无限等待。
- 活跃 / 历史 shard、split / merge、跨组 authority transfer、真实 RustFS 冷读和回收；验证身份完整、历史保护、Stream 同 key 约束和 Queue 单条 ACK。外部服务、独立 OS 进程和 Kubernetes 结果分别记录，不能用内存模拟或同进程结果替代。

实现后的基础命令示例，新增用例应放入对应现有套件或另有明确入口：

```bash
cargo fmt --all
cargo clippy --workspace --tests --no-deps
cargo test -p tidemq-sdk --test quic_client
cargo test -p tidemq --test consumer_membership
cargo test -p tidemq --test server_dispatch
cargo test -p tidemq --features test-hooks --test runtime_consumer_reads
cargo test -p tidemq --test runtime_queue
```

上述命令不覆盖所有 ignored 外部依赖与部署故障。完整门禁应使用仓库现有 [integration.sh](https://github.com/bitcapybara/tidemq/blob/main/scripts/ci/integration.sh) 及具备独立 fixture 的 [lab.sh](https://github.com/bitcapybara/tidemq/blob/main/scripts/ci/lab.sh)，并显式核对其前置环境、实际运行项、终态和制品；不能把启动命令、被忽略测试或一项独立通过等同于整条流水线通过。Rust 修改后不得用 `cargo check` 代替规定的 fmt / Clippy。

### 12.3 成本和延迟对照

先固定同一机器、版本、资源、RF、payload、网络和 tenant / shard 布局；比较当前短轮询与候选实现，分别覆盖空闲、低频、突发、持续 backlog、慢消费者、冷读、稀疏热点与多租户。至少记录空 GroupRead / RetireRead 及 Raft 写入、外部 / 内部 Watch 请求、fan-out、CPU、RSS 和已计费内存、waiter / 依赖边 / 定时器数、资源复查与真实 dirty 的放大比例、普通 / 保护信用占用、消息从持久生产到业务 receive 的 p50 / p95 / p99、吞吐、shard 最长未观察 / 未服务时间、ACK / heartbeat 延迟和恢复耗时。热点 ACK + 空闲目标场景单独报告空闲侧增量；故障对照报告保护信用的正常吞吐代价及健康 shard 在其他读取 Unknown 时的交付延迟。

建议 review 时先确认以下**候选门槛**：在约定的稳定空闲场景中，空 GroupRead + 退休 Raft 写入相对基线降低至少 80%；在约定的有信用、无故障、容量充足的常规负载下，吞吐下降不超过 10%，p99 相对基线不恶化超过 20%。这些数字用于决定是否继续，不是已有结果，也不能覆盖未约定的大规模场景；若不满足，应先解释资源 / fan-out / 公平调度瓶颈，再调整设计。高 shard 场景单独报告实际 `P`、`R_eff`、轮转周期和失败 / 饱和率，不承诺固定 1 秒延迟。

仓库现有测试说明记录曾删除性能 workflow 和负载工具。本设计只要求在隔离环境执行一次可复现对照、留存证据；不恢复永久性能 workflow，不将短功能测试耗时视为性能结论。测试环境和数据须为私有 fixture，故障、未完成运行和清理失败也保留原始证据。

## 13. 实施影响与评审决策

本节描述未来可能的实现范围，本次不修改代码：

1. `crates/tidemq-protocol/proto/`、generated 协议、帧校验和 SDK 的只读请求分类：新增 Watch 请求 / 结果、父子身份、按 attempt 取消、目标上限和错误；更新协议一致性测试。只将 Watch 归类为只读，不能把有持久副作用的 GroupRead 一并改为只读。
2. `src/server/dispatch.rs`、`src/runtime/service.rs` 及路由 / 转发：有界 deferred reply、按 data group 派生子 ID、父子映射、逐跳 Watch 额度、认证、按代次取消、清理和 drain。
3. `src/replication/state_machine.rs`、`src/broker/`、`src/subscription/` 和运行时：按目标维护观察版本、有界阻塞依赖 / 复查队列、共享 read-result capacity 释放事件、时间索引、只读成员 / 数据版本验证、权威变化重置；保持持久状态机与现有交付 / ACK 契约。
4. `sdk/rust/src/group_consumer/`、Client 共享准入和消息记账：观察页 / 探测、轮转、原 Read 状态与 RPC 尝试分离、有限恢复并发、普通 / 保护信用和共享份额、串行退化、关闭和可见错误。
5. 现有协议、SDK、dispatcher、成员、真实 QUIC / Raft、冷数据和 lab 测试及低基数遥测，必须包含四项 review 回归；仅在实现后更新相应正式文档。

需要在 review 中确认的产品 / 工程取舍：

- 是否接受“变化提示后再 GroupRead”的额外往返，换取等待不占消息信用以及空闲 Watch 不写 Raft。
- 目标负载的消费者数、每消费者 shard 数、tenant 数和可接受 p99；尤其是超过同时观察额度时的探测成本与最迟发现时间。
- 第 7、8、9、12 节候选额度、1 秒等待、30 秒保底及性能门槛；这些值需要测量和最终拍板，不能当作已经验证的默认配置。
- 是否接受保护 1 条 / `M + 512` 字节及对应 Client 共享容量、普通活跃 RPC 最多 7 个、恢复最多 2 个带来的批量 / 并发代价，以及小配置串行退化和保护读取自身 Unknown 时的保证边界。
- 发布阶段是否确实可以协调同版本切换；若需要混版本滚动升级，应先补充单独的兼容设计。
