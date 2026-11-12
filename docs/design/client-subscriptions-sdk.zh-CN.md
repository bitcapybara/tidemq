# TideMQ Scalable Topics、消费与 Rust SDK 开发规格

状态：待实施的统一开发规格；2026-10-22。本文定义目标行为，不代表实现或测试结果。

## 1. 范围与不变量

- 用户使用逻辑 Topic、业务 key 和持久订阅，不管理物理分区、Raft group、epoch 或消息认领。
- 只实现 Stream/Queue，分别由 new_stream_consumer/new_queue_consumer 创建；不保留其他订阅模式、通用消费工厂或模型切换入口。
- 生产成功以数据 Raft 持久提交为准；S3 上传在后台执行。扩缩容保持部署配置要求的副本数和故障域。
- 订阅在零成员时保留进度和积压。消费为至少一次，Stream 保持同 key 的有效交付顺序；业务副作用由应用幂等处理。
- 拆分/合并只改变后续写入拓扑；历史消息身份、S3 对象及原位置不变。节点退出前完成权威状态和必要热数据移交。
- 内存、在途请求、结果记录、拓扑、迁移和后台任务均有条数/字节上限；达到边界时背压或拒绝，不丢弃持久消息。
- 不做旧 SDK、协议、磁盘格式和开发 Topic 的兼容。未知数据目录明确拒绝，不自动清空；新格式的崩溃恢复仍必须成立。

本阶段不实现租户独占资源池、精确 key 过滤、自动 DLQ、Checkpoint consumer、外部业务 exactly-once、SDK 持久 outbox、JWT/OAuth、外部客户端 mTLS 或 Rust Stream/Sink 适配器。

## 2. 资源模型与权威

资源归属：Tenant → Namespace → Topic → Subscription。
数据部署：Logical Topic → DataShard → RaftGroup → Storage Node。

| 对象 | 持久状态与职责 |
| --- | --- |
| Tenant/Namespace | 稳定 ID、租户内名称、状态、授权和总预算 |
| Logical Topic | 稳定 UUID、固定哈希契约、区间布局、前后继 DAG、资源策略 |
| DataShard | 不可复用的出生身份、key hash 半开区间、Active/Sealed 状态、原位置空间 |
| RaftGroup | 托管分片的生产去重、结果、投递、确认、保留和恢复权威 |
| Subscription | Topic 内唯一名称和 UUID、固定 Stream/Queue 模型、独立进度及历史保护 |
| OrderedLane | 订阅内的有序 hash 范围；同一时刻一个有效 owner，可覆盖相关前驱/后继分片 |
| Broker/Gateway | 服务所有权、连接、转发、credit 和公平调度，不持有唯一持久业务结果 |
| Metadata Raft | 资源、凭据/授权、配额分配、布局、成员及变更计划；不逐消息调度或提交 ACK |

多个小分片可以共享 RaftGroup；热点拆分必须获得独立的有效容量，不能只增加分片编号而仍共用过载 Leader。一个物理分片可服务多个 lane，消费并行度与物理分片数分开管理；范围过滤所需 key hash 保存在热/归档索引中。

MessageId 只表达稳定的 Topic、出生分片和原位置，是不透明 SDK 类型，不提供跨分片大小顺序。当前 authority group 单独记录，迁移不重命名消息。哈希契约由协议固定、按原始 key 字节计算，不使用客户端默认 Hasher；完整区间末端显式表示，避免半开边界溢出。None 与显式空 key 区分；无 key 消息按稳定发送身份派生路由 hash，不承诺其顺序。

| 标识/代次 | 失效范围 |
| --- | --- |
| Topic/Subscription UUID | 删除重建使用新身份，不复用旧操作和凭证 |
| layout revision | 路由快照版本，不是写入授权 |
| authority/writer fence | 当前数据权威或服务 owner；旧入口停止准入 |
| cursor generation | 游标重置后的旧交付与确认 |
| member session/epoch | 被替换或退出的成员，不能由迟到心跳复活 |
| lane assignment epoch | 相应有序范围的旧 owner |
| attempt | 单消息的一次交付；重投签发新凭证 |

成员加入不使其他成员失效，单范围移交不失效整个 Topic。分片拆分、同组 Raft 成员替换和跨组历史权威接管使用不同状态机。

## 3. Rust SDK

### 3.1 创建与基本用法

Topic 由管理端预先创建；Consumer 有权限时可创建缺失订阅。先建立订阅，再生产需要受其保护的消息。

```rust
use std::time::Duration;
use tidemq_sdk::{Client, Credentials, Message, TlsTrust};

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder(
        "mq.example.com:7443",
        TlsTrust::ca_file("certs/ca.pem"),
        Credentials::token_file("secrets/app.token"),
    )
    .namespace("acme/app")
    .connect_timeout(Duration::from_secs(5))
    .request_timeout(Duration::from_secs(30))
    .close_timeout(Duration::from_secs(5))
    .build()
    .await?;

    let stream = client
        .new_stream_consumer("orders", "order-state")
        .build()
        .await?;
    let queue = client
        .new_queue_consumer("orders", "billing-jobs")
        .prefetch_messages(256)
        .prefetch_bytes(8 * 1024 * 1024)
        .build()
        .await?;
    let producer = client.new_producer("orders").build().await?;

    let receipt = producer
        .send(Message::new("created").key("order-42"))
        .await?;
    println!("Committed message {}", receipt.message_id());

    let message = stream.receive().await?;
    process(message.payload()).await?;
    stream.ack_cumulative(&message).await?;

    let message = queue.receive().await?;
    process(message.payload()).await?;
    queue.ack(&message).await?;

    stream.close().await?;
    queue.close().await?;
    producer.close().await?;
    let report = client.close().await?;
    println!("Unresolved sends: {}", report.unresolved_sends());
    Ok(())
}

async fn process(_payload: &[u8]) -> std::io::Result<()> {
    Ok(())
}
```

必填项在创建 builder 时传入，可选项链式设置。builder/setter 无 I/O；唯一终结方法 build().await 才读取材料、申请身份、连接或加入成员，成功后返回可用实例。

send 接受 `Into<Message>`，无 key 的简单消息可直接 producer.send("hello").await；需要业务 key 或属性时使用 Message。材料读取校验大小/权限/格式，连接失败不返回半初始化实例。

配置 Namespace 后短名按 tenant/namespace/topic 解析；未配置时要求完整名称，不回退公共租户。Client 上下文不授予权限。TlsTrust 只接收服务端信任材料，Credentials 接收 token_file/token_env/token 来源；CA 私钥不属于 SDK 输入。

### 3.2 类型与完成点

| 入口/类型 | 契约 |
| --- | --- |
| Client::builder(endpoint, tls, credentials) | 返回 ClientBuilder；build 成功表示 Control 的 DNS/TLS/HELLO/认证和协议校验完成 |
| client.new_producer(topic) | 返回 ProducerBuilder；build 取得 Topic/布局及发送维护能力，不自动创建 Topic |
| client.new_stream_consumer(topic, subscription) | 返回 StreamConsumerBuilder；build 创建或加入 Stream 订阅，返回 StreamConsumer |
| client.new_queue_consumer(topic, subscription) | 返回 QueueConsumerBuilder；build 创建或加入 Queue 订阅，返回 QueueConsumer |
| producer.send(message).await | `Result<SendReceipt>`；成功为该逻辑消息持久提交 |
| producer.send_batch(messages).await | 保留每条身份和输入关联，允许跨组部分完成 |
| receive().await | 两类 Consumer 均等待并返回 `Result<ReceivedMessage>` |
| try_receive() | 只查询本地队列，返回 `Result<Option<ReceivedMessage>>`；None 不证明远端无消息 |
| StreamConsumer::ack_cumulative | 仅此类型提供，确认本成员的应用交付前缀 |
| QueueConsumer::ack/nack | 仅此类型提供，单条确认或持久安排重投 |
| pending_sends/pending_acks | 原逻辑实例的有界恢复快照，见第 8 节 |
| close().await | Client、Producer、两类 Consumer 均返回 `Result<CloseReport, CloseError>` |

业务根模块导出独立的 Message/ReceivedMessage、MessageId、SendReceipt/AckReceipt、PendingSend/PendingAck、InitialPosition、资源信息、RequestContext 和关闭结果，不要求 Bytes/Id 转换或导入 wire 类型。两类 Consumer 内部复用驱动；不增加公共 Consumer trait、通用消费类型或创建用模型枚举。服务端仍校验模型和请求类型。

### 3.3 Clone、并发与额度

Producer、StreamConsumer、QueueConsumer 均为 Clone + Send + Sync：

- clone 共享同一逻辑身份、成员/session、驱动、队列、确认序列、恢复账本、预算和关闭状态；不执行 Join、创建连接或放大额度。
- 多个 Consumer clone 竞争同一接收队列，一次出队只交给一个等待者；可确认另一 clone 返回的有效消息。ReceivedMessage clone 共享原凭证和正文计费。
- Stream 的所有 clone 共用累计前缀；某个任务不能只按自己的局部历史累计确认。同 key 业务串行及跨任务成功前缀由应用协调。
- 任一 clone 的 close 关闭整个逻辑实例；Drop 单个 clone 不影响其他句柄。最后一个应用句柄 Drop 仅尽力清理，内部任务引用不能阻止检测该条件。
- 关闭的句柄可以 clone，得到的句柄仍关闭；新建独立实例必须重新调用对应具名 builder。

Client 统一限制条数/字节、连接和任务，Consumer 窗口覆盖在途 Read 预留、SDK 队列及应用持有正文，不按分片乘倍。ACK 释放逻辑在途后，应用仍持有的正文继续计入内存预算，直到共享数据释放。控制、查询、ACK 和关闭保留独立额度。

## 4. 多租户、认证与配额

### 4.1 凭据与持久授权

Tenant、Namespace、Principal、Credential、Grant、policy revision 和租户配额由 Metadata Raft 持久提交并恢复。名称索引分别使用租户/Namespace ID；所有资源按真实包含关系授权，不能信任客户端 tenant_id、名称前缀或对象路径。

1. 首次 bootstrap 受控创建初始平台管理员，已初始化集群拒绝重复 bootstrap；平台/租户管理员只能授予自身权限范围内的能力。
2. 管理端在线创建至少 256 bit 随机性的高熵 token；随机值在 apply 外生成，Raft 只保存 token_id、principal_id、校验摘要、状态、到期及 operation_id。
3. 校验记录持久提交后才返回成功，原值只在该创建结果中交付。节点配置不保存业务 token 清单；应用通过私有文件/部署 Secret 获取自己的 token。
4. 创建回包丢失可按原操作查询 token_id，不能恢复原值；无法领取则撤销并重新创建，不因重试多生成有效凭据。
5. 支持分页列举元数据、轮换、禁用、撤销、到期。轮换为新建、应用切换、撤销旧值；不要求节点重启。日志、审计、管理列表和 Debug 不包含原值或摘要。

TLS 验证链和主机名，token 将连接映射到 tenant/principal；每个资源操作仍需授权。Gateway 转发通过受信内部连接绑定主体、凭据和规范化目标；Broker/数据组入口独立验证来源与有效决定。直连数据面、冷读、结果查询、ACK 和管理路径同样受限。

投递凭证与 access token 分开。确认域的随机签名密钥在创建入口生成、由相应数据 Raft 持久复制并随 authority transfer 导出；不依赖各节点手工配置相同业务密钥。MAC 绑定租户、Topic、Subscription、出生消息、cursor/member/lane 代次及 attempt；当前物理 authority 通过入口授权单独核对。迁移保留仍有效的确认域，旧 authority 被 fence 后不能继续受理。密钥和凭证不进入业务日志，含签名材料的 WAL/快照按私有持久状态保护。

### 4.2 缓存、撤销和过期

认证执行节点按需有界缓存，使用 Metadata 的线性一致查询、revision watch 和有限允许租约；缺口、过期或未建立可信视图时重取。租约不能从旧缓存续期，控制面不可用且租约到期后停止新准入和持续投递。

撤销回执区分持久提交与生效：提交返回 auth_revision；生效至少等待旧状态可能签发的最长租约消失，watch 加速失效。连接和长 Read 不永久继承 HELLO 的许可；凭据、主体或租户禁用均传播到已有会话。

租约以本地查询发起时刻等保守边界计时，迟到响应不能重新获得完整有效期；凭据到期取更早边界，允许的时钟误差必须有界。已准入操作可按原授权收尾，撤销不回滚已持久结果或已交付正文。过期 Credential ID 不复用，回收等待授权、管理重试及恢复依赖结束。

### 4.3 预算与满额处理

| 类别 | 约束 |
| --- | --- |
| 资源数量 | Tenant 下的 Topic/订阅/主体/凭据、连接/成员数量，创建时原子检查 |
| 吞吐 | 发布/消费速率及冷读并发，以跨节点子额度分配，突发误差有界 |
| 积压/在途 | 写入前保守预留逻辑字节、消息和在途额度；提交或幂等归还 |
| 物理容量 | 节点内存、磁盘、网络、复制/迁移临时占用；不能仅靠逻辑计费限制 |

Namespace 只能细分租户预算，所有节点子额度总和受总额限制。新增节点/分片不放大配额；转移中的旧额度在撤销或到期前不重复发放。预算降低到现有占用以下时停止新增，保留已有数据并等待释放。

租户积压按仍受未确认订阅保护的逻辑消息计一次，不按订阅数、Raft 副本或热冷副本重复计费；各订阅可独立显示积压。新订阅保护历史、回放或保留策略改变也必须核对新增资源/容量，不能绕过准入。

满额时背压或拒绝新写入，不清除未消费消息；ACK、恢复和回收仍可推进。共享资源采用租户公平调度，不承诺硬件独占。共享对象只有满足所有相关资源的保留条件后才可回收。

## 5. Producer 路由、顺序与结果

### 5.1 路由与并发

SDK 缓存带 revision 的区间布局，处理增量通知、断连、版本缺口和 StaleRoute；需要时查询快照，不逐消息访问 metadata。服务端校验 key/hash、合法区间和 writer fence。首次布局覆盖完整哈希空间，最小/最大分片与 lane 数由有界服务端策略决定。

send 是普通 async 方法，输出 `Result<SendReceipt>`。创建 Future 不准入；轮询后可能等待额度。准入后由 SDK owned driver 持有稳定发送身份、digest、原目的和必要数据，在原期限内收尾。取消等待不能使其变成另一发送。

发送身份包含逻辑 Producer 实例与单调序号，去重同时核对 canonical key/payload/properties 摘要；物理路由、epoch 和传输重试不改变逻辑身份。分片不能要求收齐全 Topic 序号：跨分片的序号间隔不属于丢消息，去重窗口按实际记录数/字节限额而非全局序号跨度计费，退休边界按实际发送域维护并允许合法乱序。序号耗尽明确拒绝而不回绕。

一个 Producer 并行服务多个 shard/owner，不把网络等待放入全 Topic 锁。同 key 按整个逻辑 Producer 的准入顺序发送，未决旧写入不能被后续发送绕过；不同 Producer 的顺序由服务端提交及前后继衔接确定。需要输入顺序时使用相应串行准入或 send_batch，结果返回顺序不构成写入顺序。

调用方可用有界 FuturesUnordered/FuturesOrdered 或任务池驱动同一个 send，复用相同发送与恢复引擎。集合的 push 不推进 Future，外部未轮询数据和未领取结果须另受应用窗口限制；SDK 预算只覆盖已准入/持有的数据。

### 5.2 未知结果与跨拓扑重试

| 原请求状态 | 处理 |
| --- | --- |
| 明确未执行 | 沿原逻辑发送身份选择当前合法目标 |
| 已持久成功 | 返回原 MessageId，不在后继重复写 |
| 可能已提交 | 向原分片当前 authority 查询/重试，保持身份与 digest |
| 源已 Seal 且证明未提交 | 持受信证明向唯一合法后继提交 |
| 仍未知/超出恢复覆盖 | PendingSend 或 OutcomeExpired，不能自动换身份重发 |

未提交证明必须覆盖原身份/digest/hash 和源 Seal 边界，并证明迟到提交已被禁止；单纯“未查到记录”不足。目标校验当前布局、证明和写权限。authority transfer 必须携带去重与结果覆盖；不能只搬正文。

send_batch 保留每条身份、同 key 输入顺序及成功/未知/拒绝/未提交结果，不提供跨组事务。子批次成功后不随整体重发。未决结果、payload 和恢复句柄均计入预算，恢复规则见第 8 节。

## 6. 订阅与消费调度

### 6.1 模型与创建

| 模型 | 分配 | 确认与失败 |
| --- | --- | --- |
| Stream | 按 key hash 的有序 lane 分摊，一个 lane 一个有效 owner；一成员可读多个分片 | 本成员累计 ACK；失败不确认，可本地重试或退出后有序重投 |
| Queue | 逐消息 attempt 分摊，同一分片可同时服务多个成员 | 单条 ACK/NACK，允许独立完成与重投，不保证同 key 顺序 |

同 Topic 可有多个 Stream 和多个 Queue 订阅；不同订阅各有进度、起点、重投和历史保护，同一订阅内成员分摊工作。订阅在零成员时继续保护积压。

创建函数确定模型；已有订阅不匹配返回 SubscriptionModeMismatch，匹配则沿原进度继续。新订阅默认 EarliestAvailable，覆盖当时仍可读的必要 sealed predecessors；Latest 捕获各有效分片截止位置，不承诺跨组全局原子 offset。创建起点不自动重置已有订阅，首版不切换原订阅模型。

两类 builder 均提供 initial_position(position)，接受 InitialPosition::EarliestAvailable 或 InitialPosition::Latest，默认前者；该选项仅在创建订阅时生效，已有订阅忽略起点并恢复原 cursor。

创建与布局/删除在 metadata 中排序，固定 UUID、布局基点和原操作，分页安装必要 cursor 与历史保护；全部准备好才成功。安装期间的保护不能因超时/控制器重启丢失；同名资源仍在创建/删除时返回其状态或原计划，不能再创建另一身份。

普通 Consumer 不指定物理范围或精确 key。Stream 自动为同 key 选择有效 owner；成员变化可改变处理进程。精确消息过滤不在本阶段实现，不能以同订阅成员收到非目标 key 后丢弃并 ACK 来模拟路由。

### 6.2 成员与范围移交

成员生命周期为 Joining → Active → Leaving → Left，Expired/Revoked 是终态；Active 成员可以暂时没有工作。Join/Leave/续租固定原操作，metadata 目标状态经相关数据组安装后才生效，恢复沿原计划继续。

SDK 使用独立心跳和有界批量持久续租，不被业务处理或空闲 receive 阻塞。成员有效期受成员租约、控制/owner 授权及访问许可共同限制；任一 QUIC lane 断开不直接创建新成员，迟到续租不能复活已失效成员。

逻辑 Consumer 与成员会话分开：SDK 在恢复期限内重连原会话；确认旧会话已失效后，先 fence/丢弃旧预取，再以固定重加入操作取得新 member epoch、重建交付序列，从持久确认边界继续。原确认结果仍沿旧操作查询，不把旧凭证套入新会话。显式 close、资源删除、权限永久失效或恢复预算耗尽时停止自动重加入并返回错误。

Stream 使用粘性分配及有界范围细分。计划移交先停旧 owner 新投递，允许有期限的确认收尾；随后持久 fence。只有相关数据组确认旧授权失效，才给新 owner 安装 grant 并重投未确认项。Drain 超时不能自动 ACK 或让双 owner 同时取得新有效投递。成员多于 lane 时允许空闲，局部移交不停止无关范围。

Stream 按同 key 的源提交顺序和前后继 DAG 交付；相关前驱业务 ACK/完成证明满足边界后才能开放后继。首版允许较粗的范围门槛，慢消息可能阻塞对应范围；SDK 收到正文或预取完成不能替代业务确认。应用并行处理同 key 不在该保证内。

### 6.3 Queue 分发与多分片读取

订阅控制面分配成员租约、有界工作集和公平目标；各数据组按有效 grant/credit 持久记录 message attempt 与唯一接收成员后返回正文。控制面不逐消息决定 owner，不把正文集中经过一个 dispatcher。

Queue 工作集不是整个分片的排他所有权。同一分片可以分摊给多个成员，一个成员也可从多个分片并行预取。新增成员取得后续分配，不抢回其他成员有效的在途项；成员失效后撤销原 attempt 再重投。

SDK 按 owner 复用连接、预留统一 credit、并行 Read 并汇入有界队列。活跃任务、工作集和轮询数有上限；空数据长轮询或退避，不退化为所有成员无协调地遍历全部分片。冷热读取公平执行；Stream 另执行其有序门槛。

Read 的结果域固定到可独立迁移的出生分片/订阅，保持原操作、请求参数和预留。回包丢失时对账并重放原投递结果，不签发另一批替代；同一结果只入队和计费一次。未知请求的 credit 只有在结果已交接、取消已确认或旧流/授权已 fence 后才释放。期限耗尽时暂停相关新取数并返回明确错误，后续 receive 可沿原读取恢复；超出结果覆盖仍无法确定时关闭/fence 当前成员，使未确认消息重新分配，不能无限持有孤立投递。

receive 在无消息、无工作或无 credit 时等待；未交给应用的取消不消耗消息，关闭/永久错误唤醒等待者。成员出队与交付账本更新使用短临界区，不包含网络等待或业务处理，也不阻塞心跳、ACK 和关闭。

## 7. 确认、NACK 与重投

### 7.1 Stream 累计范围

SDK 在消息真正返回应用时分配成员内 delivery sequence，记录原消息、凭证、attempt、确认域和 authority。序列由所有 clone 共享；成员代次改变后旧序列不能套入新会话，预取和其他成员不在前缀内。

| 出生位置 | 当前接收成员 | 成员内交付序列 |
| --- | --- | --- |
| S:1 | C1 | 1 |
| S:2 | C2 | 1 |
| S:3 | C1 | 2 |
| S:4 | C2 | 2 |

C1 累计确认 S:3 只确认自己的 S:1/S:3，不确认 C2 的 S:2。跨分片同理：前缀按 C1 的实际交付顺序展开，不按 MessageId 或物理最大 offset 推导。读取、保留和 GC 的连续前沿从真实确认状态计算。

每个订阅/出生分片的确认窗口同时计入未确认槽位和已确认空洞；慢前沿使窗口满时停止该域新投递，保留 ACK 和恢复准入。Consumer credit 不替代此状态上限，不能因正文已释放就无界积累 ACK 空洞。NACK 释放成员在途，但相应未确认项仍占窗口并保护历史。

应用必须先完成整个目标前缀；不同 key 并行完成时，后收到的消息先完成不证明较早处理成功。需要任意完成顺序和独立确认时使用 Queue。SDK 以有界交付账本固定确认集合，不逐消息复制整个 Topic 向量，也不把消费上下文写入稳定 MessageId。

### 7.2 持久提交与部分完成

1. 固定原操作、目标前缀、凭证集合和摘要，按 Subscription/出生分片确认域及帧/条数限制生成稳定子批次，再路由到当前 authority。子操作身份不依赖可变 group 地址，同一子批次不混入可独立转移的确认域。
2. 各数据组核对订阅模型、资源、cursor/member/lane、attempt 和 MAC；每个子批次预验证后原子应用，坏凭证使该子批次拒绝。
3. 同组也可能被分为多个子批次；跨子批次/跨组均允许部分成功，不提供原子回滚。只有全部原确认集合持久完成，调用才返回成功。
4. PendingAck 保存固定清单、已完成/未知/拒绝结果；重试只对账或继续原子批次，不能扩大前缀、重做整个成功集合或换新 attempt 确认。
5. 原凭证失效且尚未完成时报告具体结果；新投递须重新由业务处理后发起新确认，不能伪造旧累计成功。

首版使用合法消息凭证的有界子批次表达累计操作，服务端不信任客户端自报的最大 sequence。范围 ACK 压缩只有在证明完整覆盖和授权后才能引入。QueueConsumer 不提供累计方法，StreamConsumer 不提供单条业务 ACK/NACK；原始协议请求同样校验这些限制。

### 7.3 Queue NACK 与失败恢复

nack(message, delay) 的成功回执表示重投安排已持久记录，尚不表示再次投递或业务处理成功。delay 为有上限的 Duration，按权威状态计算重投时间。

受信 handler 将计划时戳写入命令，Raft apply 不读取系统时间。重启从快照/WAL 恢复截止索引，每次处理有界工作片，不按每轮扫描全部消息或为每条消息永久创建后台任务。

- 原 attempt 撤销，未确认消息继续保护历史；本次成员的逻辑在途可释放，正文仍按实际持有计费。
- 到期且有合法成员/credit 时签发新 attempt 和 token，可交给当前或其他成员。NACK 本身不多计一次投递。
- 同一原操作重复 NACK 返回原安排，不延后截止时间；迟到旧 ACK 不能改变新 attempt。ACK/NACK 竞争由同组原子状态决定。
- NACK 结果未知也进入 PendingAck，按原操作查询/恢复；若原安排已完成，返回原结果，不给新 attempt 再 NACK。

首版重投由持久 NACK 到期、成员退出/失效或受权人工恢复触发；不把 request_timeout 当成业务处理超时。活跃成员未确认的消息继续占用窗口；应用需要处理时限时自行协调业务取消与 NACK，无法推进时通过成员关闭或受权撤销恢复。

自动投递达到 attempt 上限后暂停并移出自动调度索引，保留消息和诊断。管理端人工恢复绑定租户/Topic/订阅/MessageId 与原操作，先对账，再撤销仍有效的旧投递并签发新 attempt；不重投已确认消息、不复用旧凭证、不重置整个订阅。

Stream 失败时不能累计越过失败前缀。可本地重试，无法继续则关闭成员，未确认消息经 fencing 后有序重投；关闭、超时和零成员均不自动确认。所有重投均保留业务幂等要求。

## 8. 期限、恢复与关闭

### 8.1 统一操作和取消

SDK/协议中的变更操作使用稳定 operation_id、资源 UUID、摘要及结果覆盖；wire request ID 只标识一次传输，不暴露为公共 cancel 参数。相同操作重试不得改变语义，摘要冲突明确拒绝。退休水位/墓碑和会话有效期共同限定可接受原操作；过期请求即使查不到明细也拒绝，不能按新操作复活资源。

公开时长使用 Duration，RequestContext/CancellationToken 约束总期限。等待额度、查询、子批次和自动重试共用剩余时间；不为子步骤刷新期限。无显式 context 的 receive 持续等待，网络子请求仍有期限。

```rust
use std::time::Duration;
use tidemq_sdk::RequestContext;

let context = RequestContext::with_timeout(Duration::from_secs(10))?;
let message = context.scope(queue.receive()).await?;
```

scope 对传入的 Result future 实施总期限/取消，并向 SDK 操作传播；独立任务显式传递 context。build/Join 被取消时沿原操作收尾，不能生成重复成员或删除已创建的持久订阅。

| 截止时状态 | 对外结果 |
| --- | --- |
| 可证明未提交 | Cancelled/DeadlineExceeded，无业务完成 |
| 可能提交或部分完成 | OutcomeUnknown/部分结果，携带原恢复句柄 |
| Join/Delete/拓扑变更未知 | 查询固定 UUID 和原计划，不按名称重新创建 |
| 已持久完成 | 原回执或本地未领取终态，不报告为明确未执行 |

服务端先查询原结果再决定是否继续操作，不能仅因当前 attempt 已变化就丢掉已完成的原结果。去重/结果覆盖至少包含允许的恢复窗口及仍有效租约/操作依赖；过期返回 OutcomeExpired，不授权新身份重试。配置降低窗口不能提前撤销已承诺的操作覆盖。

SDK 使用结构化 ErrorKind、完整 source 链和结果状态；代码、错误、日志为英文，不通过字符串匹配判断 wire 错误。

### 8.2 PendingSend/PendingAck

| 入口 | 范围与结果 |
| --- | --- |
| producer.pending_sends().await | `Result<Vec<PendingSend>>`，原逻辑 Producer 的本地有界快照 |
| consumer.pending_acks().await | `Result<Vec<PendingAck>>`，Stream 累计 ACK 或 Queue ACK/NACK 的原操作快照 |
| PendingSend::retry().await | 查询/继续原发送，成功返回原 SendReceipt |
| PendingAck::retry().await | 查询/继续原确认或 NACK 安排，成功返回标明操作类型的 AckReceipt |

快照包含仍在处理、未知/部分完成和已完成未领取结果；不包含未准入 send、未发起确认的消息或全部历史操作。查询不 drain、不重试、不确认业务。AckReceipt 必须区分持久确认与重投已安排，NACK 回执不伪装成消息已 ACK。

retry 合并仍在进行的原操作，完成则取原结果，未解决则在允许范围内继续。显式恢复可使用新调用期限，但原身份、摘要、ACK/NACK 范围和结果保留边界不变。

原 Future 被取消后，后台终态仍有界保留，直到正常调用或恢复调用将结果移交应用；Unknown/部分完成不得为腾空间静默删除。所有句柄、payload、token、未领取结果计入预算，满额阻止新准入。

恢复须在原 Client 关闭前完成网络步骤。新建实例不能自动找回旧本地账本；没有持久 outbox，不承诺进程重启后的自动恢复。

### 8.3 close

所有 close 使用独立 close deadline，返回 CloseReport，或携带报告的 CloseError：

1. 原子进入 Closing，拒绝新 build/send/投递；唤醒接收等待者。
2. 保留控制与确认额度，收尾已接受的发送和 ACK/NACK，不替业务确认未处理消息。
3. Consumer 完成持久 Leave、撤销及未确认项回收；期限内不能确认时报告错误，剩余由租约/fencing 收尾。
4. 停续租/预取，关闭 owned transport，join owned 任务；本地清理失败同样返回具体 source。
5. 并发/重复 close 合并为同一操作；等待被取消后，清理在原期限内继续，可再次取得结果。

报告分别统计本地清理、成员退出、未决发送/确认和已完成未领取结果。业务未知不等于本地泄漏；清理成功不等于所有消息已确认。实例 close 只影响其自身及 clone，Client close 关闭其全部实例。应用持有正文/恢复句柄继续按共享生命周期计费。

应用应先收束自己启动的发送/业务任务和必要恢复；close 只 join SDK owned 工作，不等待任意业务任务完成，也不代发业务确认。

## 9. 拓扑变更、历史权威与 GC

### 9.1 Split/Merge

metadata 持久记录源区间、原布局、源/目标组、资源预留、操作及步骤。分片/lane 变更、权威转移、删除和游标变更使用统一冲突检查；同一范围不能有互相覆盖的未完成计划。

| 步骤 | 必须满足 |
| --- | --- |
| Prepare | 源 Active、布局新鲜；目标组和所需 RF/故障域/容量已准备，未达到拓扑/迁移上限 |
| Seal | 源 Raft 固定写入及结果覆盖边界，禁止迟到追加，原结果仍可查 |
| Activate | 目标持久安装唯一写入范围/授权后发布新布局；布局通知本身不授予写权限 |
| Consume | 新写入进入后继；Stream 等待相应前驱业务完成门槛，Queue 不设业务顺序门槛 |
| Retire | 前驱已满足全部保留/恢复条件，或历史权威已经显式转移 |

Split 选择有负载收益的切点；Merge 只合并同 Topic/哈希契约的相邻 Active 区间，目标可承担写入、读取、ACK 和历史回放。两者保留消息出生身份、原位置及有序消费并行能力。

Prepare 前后失败沿原计划恢复；Seal 后不因超时重新开放源或创建替代计划。受影响范围允许有界停写/退避，无关范围继续。Merge 完成只表示新布局生效，不能直接删除前驱组或缩容。

lane 的细分/合并单独使用消费分配计划：停止相关旧交付、排空或撤销在途并 fence，安装完整无重叠的新范围，再从持久确认状态继续。新 lane 可从保守位置过滤读取并跳过已确认项，不要求物理分片同步 Seal/Split；key-hash 索引和范围数量均有上限。

### 9.2 跨 RaftGroup 的 AuthorityTransfer

同组副本快照/成员替换不能直接用于跨组接管。应用级状态机为：

Prepare → FenceSource → Export → ImportTarget → PublishAuthority → Checkpoint → RetireSource。

- 首版有限冻结：源停止新准入、有限收尾后固定 applied cut；导出后不能继续产生未导出的 ACK、attempt、pin 或状态变化。
- 按逻辑分片分页导出并校验摘要，不将整个源 Raft 快照安装为目标组快照。源/目标 LogId 不混用。
- 转移生产去重/退休边界与结果，订阅 cursor/ACK 空洞/确认密钥/attempt，Read 与 ACK/NACK 未决结果，对象绑定及未决归档/GC 计划、配额预留、pin、retention/recovery hold；未归档热数据按原身份复制。
- 目标通过自身 Raft 持久导入并验证完整性、授权、RF/故障域/容量后，metadata 才发布新 authority；源保留 fence，重启不恢复权限。
- 目标全部必要副本取得持久 checkpoint，相关读取/恢复依赖完成后，才释放源保护并删除源状态。源组仍托管其他分片时不能退役整个组。
- Unknown 继续原计划；不靠 TTL 清理源，不盲目回退。原发送和确认查询由当前 authority 承接，仍有效凭证与结果域一并保留。

### 9.3 不可变 S3 历史与回收

HistoricalBinding 将出生对象/range 与当前 authority、binding generation 关联；冷读核对出生身份、当前资源授权和有效 pin，不接受客户端任意提供 group。Split/Merge/Transfer 只改清单/绑定，不批量改写已归档正文；按需冷读和热尾复制仍计入实际 I/O 与修复预算。

对象回收同时满足：订阅保护、保留年龄、读取 pin、清单引用、未决归档写入、未完成导入、源/目标恢复与 checkpoint 依赖。共享对象必须满足全部相关范围/租户条件，单分片退休不能释放整个对象。删除通知或 metadata desired 状态不能替代数据组解除保护的证明。

限制 Active/Sealed 分片、DAG 深度、组数、历史绑定字节及待完成计划；长期积压保留前驱。仅在消费、读取、结果恢复和重试证明均无依赖后压缩拓扑，退休身份不复用。达到上限暂停新拓扑变更，不丢弃历史。

删除订阅默认拒绝在线成员；固定 UUID、冻结 Join、逐组解除状态/保护，全部生效才成功。部分完成继续保护剩余部分，同名重建资源不属于原操作。

Topic/租户删除默认要求先显式删除下级资源，不增加隐式级联路径。删除 Topic 先冻结新生产和布局计划，并 fence 各数据组的新准入；已准入结果及未完成范围沿原计划收束，逻辑删除完成后才成功。底层对象和组仍受上述恢复/GC 条件保护。

## 10. 部署与资源伸缩

### 10.1 部署矩阵与排空

| 方式 | 形态与要求 |
| --- | --- |
| 单节点单进程 | Combined，RF1，持久目录、bootstrap 和同格式重启恢复 |
| Docker Compose | Combined 或分角色容器，持久卷和人工注册/排空流程 |
| 多机进程/容器 | Combined 或分角色，由部署工具管理实例，TideMQ 管理状态放置 |
| 多节点 Kubernetes | Combined 或分角色，稳定身份/卷，实例变更与 Ready/DrainComplete 对接 |

各方式共用 SDK、租户授权和持久协议。Combined 退出须完成所有内置角色排空；分角色才能独立伸缩。RF1、同主机 RF3、跨节点/AZ RF3 的容错能力分别验收，不因部署工具自动提升或降低既定策略。

| 角色 | 扩容后的实际负载 | 缩容退出条件 |
| --- | --- | --- |
| Gateway | 新连接和必要的受控重连均衡 | 停新连接，收束已有连接及 owned 请求 |
| Broker | 分片服务 owner 与读写请求 | 停新分配，handoff/fence 和确认收尾 |
| Storage | 副本、Leader 和独立数据组 | 副本安全替换，权威移交/退休，checkpoint 和读取依赖完成 |
| Metadata | 控制面 Raft 副本及 Leader | 受控成员变更、Leader/计划转移；增加副本不代表增加写吞吐 |
| 消费 worker | 有效 lane 或消息分配 | 停新分配，业务收尾、Leave/fencing 与重投 |

核心提供注册、容量、Ready、Drain、DrainComplete 和 blocker。部署执行端创建/销毁进程、Pod/VM；TideMQ 负责放置和持久移交。复用 Placement、MembershipPlan、RepairBudget、Handoff、Drain、冷读/GC，先交付所有部署的人工流程，再接自动供给。

计划缩容先排空再删除；readiness、Pod 删除和副本数不证明移交完成，Metadata/Storage 不能由普通 HPA 任意减副本，卷回收不能早于安全退休。强杀进入故障恢复路径。无实例供给执行端时只报告资源需求，不伪报扩容完成。

### 10.2 Split/Merge 自动决策

按新鲜观测 → 瓶颈归因 → 收益/容量检查 → 原计划执行。目标预留同时计算节点其他组和未完成计划，故障修复、均衡与自动伸缩共享并发/带宽边界。

| 现象 | 优先动作 |
| --- | --- |
| 可分散范围持续达到数据路径高水位 | 选择有收益的切点和独立有效目标容量 |
| 多组挤在同节点 | 均衡 owner/Leader/副本，必要时增节点 |
| 业务处理或 lane/任务不足导致积压 | 调整消费者和消费分配，不直接切存储 |
| 单热点 key | 报告串行瓶颈，反复切分不能增加同 key 的有序并行度 |
| 归档落后/历史大/共享网卡或磁盘瓶颈 | 处理对应资源和保留压力，不把切分当作历史搬空 |

自动 Split 必须同时满足：完整高水位窗口；key-hash 负载摘要证明可拆；目标可承接预计写入/读取/ACK；源无冲突计划；满足 RF/故障域、最小/最大布局、拓扑及迁移预算；冷却结束。没有目标容量先申请资源，不先 Seal。

自动 Merge 要求相邻两个源及合并后目标在较长完整窗口内各相关容量维度均低于低水位，保留突发和回放余量，不低于最小分片数。保留有效 Stream lane 和 Queue 多成员分发；历史积压本身不绝对禁止 Merge，但不得忽略恢复和权威退出成本。

容量按消息大小、批量、读写/ACK 组合、RF/故障域校准。高水位可由相关维度超标触发，低水位要求全部相关维度安全；低水位严格低于高水位。样本缺失/过期、控制器重启或布局/owner 变化重建连续窗口，未知不能当零；阈值跨越和周期快照都触发重评估。

单个 Producer/Consumer 支持多分片并发；同机增加进程不增加机器总资源，Stream 受有效 lane 和单 key 串行限制，Queue 受 credit 和业务处理能力限制。扩展验收分别固定服务端资源及客户端总预算，不预设实例数与吞吐线性关系；持续吞吐还受复制、归档和存储水位影响。

## 11. 配置、诊断与实现范围

### 11.1 参数与校验

以下数值仅为功能验证初值，不能作为吞吐或接替 SLO：

| 参数 | 初值/规则 |
| --- | --- |
| connect/request/close timeout | 5s / 30s / 5s；receive 的持续等待除外 |
| Client pending requests/bytes | 256 / 16 MiB，另计 transport、解码与控制保留 |
| heartbeat/member lease | 5s / 15s；实际由服务端返回，续租不能耗尽剩余租约 |
| 成员/续租批次 | 每订阅 128、每 Client 256、每续租批次 64；帧/字节限制更小时继续拆批 |
| 自动评估 | 10s；采样带时戳、范围及覆盖窗口 |
| Split 高水位 | 校准容量 70%，连续 60s；P99 触发仍需数据路径瓶颈证据 |
| Merge 低水位 | 两源及合并后目标各维度不高于 30%，连续 10min |
| lineage 冷却 | 至少 5min，且旧计划收束、重新取得完整观测窗口 |

授权租约上限、允许恢复窗口/结果保留、凭据有效期和拓扑/内存/修复上限必须有限，在部署配置及管理创建时校验，未配置必要边界则拒绝启动/操作；不提供隐式无限值。服务端通告会话实际参数，已有租约和恢复承诺不能被配置变更提前回收。成员、授权和控制租约使用第 4/6 节的有效边界，重投延迟不复用请求 deadline。

诊断只读且限频，提供进度/窗口、成员与 grant、未知操作、计划步骤、保留/回收 blocker；不含正文、token 或签名材料。聚合指标使用有限角色/模型/结果标签，不用 Topic UUID、key、token 作无界 label。至少覆盖持久写入/确认延迟、ACK/NACK/重投、有效/空 Read、在途和未领取结果、成员/计划状态、归档/复制压力及容量。

### 11.2 模块与阶段

| 阶段 | 必需修改 | 交付门槛 |
| --- | --- | --- |
| D0 契约 | SDK 类型、wire/持久模型、身份/期限、限额配置及错误；只保留本文模型 | 接口、状态和恢复路径固定，测试数据可直接更新 |
| D1 基础与资源闭环 | Tenant/Namespace/主体/凭据/授权、分配配额、Client/Producer builder；初始区间路由；节点注册、同组成员/Leader 迁移、各部署人工排空 | 凭据动态管理和租户隔离成立；新增实例取得负载，旧节点可在同组安全替换后退出 |
| D2 消费 | 两类 Consumer/builder、成员/lane/Queue 工作集、Read/credit、累计/单条 ACK、NACK、PendingAck 和人工恢复 | 多订阅、动态增减、稀疏确认、重投及退出可恢复 |
| D3 拆分 | 手动 Split、sealed DAG、路由证明及跨拓扑消费 | 独立目标组承接负载，未知写入不重复，Stream 不越过前驱 |
| D4 合并与历史 | 手动 Merge、AuthorityTransfer、HistoricalBinding、冷读/pin/GC | 历史不批量重写，源权威和节点可安全退休 |
| D5 自动化 | 负载策略、供给执行端、长期故障/容量验证 | 冷却/上限内闭环，实际负载和数据安全有证据 |

D1 的同组成员迁移不代替 D4 跨组历史接管；D1/D2 直接采用最终区间/订阅模型，不临时恢复固定分区或静态 token 目标。

修改覆盖 SDK client/model/error/request/producer/consumer/admin，protocol proto/generated，metadata/control，subscription/broker/data/quota，lifecycle/archive/ownership/placement/scaling，以及 examples/deploy/perf、配置、文档和 CI。先用 CodeGraph 核对实现符号/调用方；同步删除旧公开 Id/epoch/Claim、partition_count 取模、通用消费工厂和排序枚举，不能只更新高级 SDK。

### 11.3 验收矩阵

| 范围 | 必须覆盖的场景 |
| --- | --- |
| SDK 创建 | builder/setter 无 I/O；build 取消/回包丢失不重复成员；模型不匹配明确失败；未配置 Namespace 的短名拒绝 |
| 类型与共享 | Producer/两类 Consumer 为 Clone + Send + Sync；错误确认方法编译不可用；clone 不 Join/倍增预算，跨 clone 确认、共享关闭和最后应用引用清理成立 |
| 认证 | bootstrap 幂等边界、token 一次交付/丢回包对账、动态增减及重启恢复；跨租户伪造、直连/转发、冷读/确认/查询越权拒绝 |
| 撤销与配额 | 断网/丢 watch/迟到响应下租约有限失效，既有长 Read 停新投递；多节点额度不复制放大，历史保护变化、迁移/降配无漏计/双归还 |
| 路由与发送 | hash 边界、None/空 key、布局缺口/乱序、源提交丢回包、Seal 未提交证明、接管后结果查询、跨组 batch 部分成功 |
| 发送 Future | 首次轮询前/准入前后取消；跨分片序号间隔不阻塞/倍增状态；FuturesOrdered/Unordered 与有界任务池复用 send，集合提前结束不把已准入消息整体重发，未领取结果有界 |
| 并发与接收 | 单句柄多组并行、短出队临界区；未知 Read 重放及 credit 不超发，晚响应/取消/成员失效竞争；空 Read、连接和任务有界 |
| 成员与顺序 | Stream 成员多于 lane、lane 细分/移交及故障 fencing；会话过期后固定重加入、不重复成员，旧预取/确认不进入新 epoch；split/merge/冷读不越过前驱，零成员积压 |
| 确认 | C1 的 S:1/S:3 累计不确认 C2 的 S:2/S:4；跨 clone/分片前缀不含预取；同组拆批/跨组部分成功、迁移中稳定子操作、凭证失效和 ACK/NACK 竞争；慢前沿下 ACK 空洞窗口有界 |
| 重投与恢复 | NACK 丢回包经 PendingAck 恢复且不延迟原安排；原结果在 attempt 改变后仍可查；上限暂停、人工撤销/恢复不丢消息或复用凭证 |
| 生命周期 | 订阅创建与布局/GC 并发保护历史；同名资源的创建/删除中状态不产生新身份；删除部分完成、重建和旧操作隔离 |
| 拓扑与 GC | 各步骤强杀/丢回复、控制/数据 Leader 切换；导出冻结/导入摘要、热尾、目标 checkpoint、源退休；共享对象、迟到 PUT/取消/压缩与 pin/recovery hold 竞争 |
| 部署与关闭 | 单进程、Compose、多机/Kubernetes 的持久卷、bootstrap、重启、人工排空；DrainComplete 前不退出；close 取消/超时/并发、未领取结果与宿主持有内存准确报告 |
| 自动策略 | 短突发、单热 key、无目标容量、观测缺失/过期/重启、高低阈值跨越、合并后负载和消费并行度、拓扑硬上限 |
| 性能 | 固定 revision/binary/config、消息/key/RF/预算，分别测单句柄、任务、进程和多机；报告吞吐、P99、错误、积压、CPU/RSS、metadata 提交、修复/冷读流量 |

RF1、同主机 RF3、真实跨节点/故障域 RF3 分别保留证据；功能测试不证明性能提升。所有网络未知/部分完成场景核对原消息身份集合、持久结果和未确认保护，不只检查 API 返回值。

Rust 实施后执行 cargo fmt --all、按修改范围运行 clippy --tests --no-deps、对应单元/集成/doc 测试，以及生成协议、默认/metrics/otel feature 和生产 panic/indexing 审计；不运行 cargo check。保留命令、配置、退出码、日志和 binary/revision 身份，故障/负载场景不能由普通回归替代。

外部事实与来源集中于[参考核对](../research/client-subscription-semantics.zh-CN.md)；该文件不定义另一套 TideMQ 实现目标。
