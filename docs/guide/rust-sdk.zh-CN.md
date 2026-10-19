# Rust SDK 接入与恢复契约

SDK 位于同仓库 `sdk/rust`，依赖 `tidemq-protocol` 和 `tidemq-transport`，不链接服务端、OpenRaft、WAL、S3 或全局分配器。运行环境为原生 QUIC/UDP；连接必须校验证书与服务名，并使用作用域静态 token。宿主负责 CA、凭证、Tokio runtime、日志、指标出口和分配器。

## 创建客户端与资源

```rust,no_run
use tidemq_sdk::{Bytes, Client, ClientConfig, Id, Producer, public};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let roots = tokio::fs::read("certs/ca.pem").await?;
let token = std::env::var("TIDEMQ_TOKEN")?;
let client = Client::connect(ClientConfig::new("localhost:7443"), roots.into(), token).await?;
let topic = client.admin().create_topic("events", 4, client.deadline()?).await?;
let topic_id = Id::from_slice(&topic.topic_id)?;
let subscription = client.admin().create_subscription(
    topic_id, "worker", 0, public::DeliveryMode::Parallel, client.deadline()?,
).await?;
# let _ = (Bytes::new(), subscription);
# Ok(()) }
```

`Client::with_target` 可显式传入地址与 TLS 服务名，适合测试和固定解析结果。`connect` 在创建时解析稳定地址；当前客户端重连沿用该解析结果，地址变更时重建 Client。它不接收内部节点表，不接受外部客户端自行指定 Broker owner epoch。发送到 Gateway 的 owner epoch 固定为 0，由服务端按新鲜路由填充。

`Admin` 提供创建、按名称查询 Topic/Subscription，查询生产会话和订阅分配，以及带期望代次和 operation ID 的分区订阅删除。创建操作的自动网络重试保持 operation ID；最终结果未知时，按唯一资源名查询并核对配置。删除某个分区不代表所有分区均已删除，积压保护的全局解除由服务端协调。

## 生产：有序发出、并行等回执

`Producer::open(client, topic, partition, producer_id, deadline)` 查询注册 epoch 提示并显式注册。多个 clone 共享一个序号域。`produce(Vec<Message>, deadline)` 在有限的顺序锁中分配序号和 enqueue，离开该锁后独立等持久回执；并发调用可以流水发送。单条消息与整个编码帧分别受协议限制，没有隐式 linger；宿主显式组成批次。

每批保存 producer ID、epoch、first sequence、内容摘要、operation ID 和消息。每次网络尝试新建 request ID，重试保留业务身份与摘要。不同内容不能复用原序号。QUIC send、transport ACK 和取消均不等于 WAL/Raft 已提交。

服务端明确返回 `Rejected + retryable` 的 `Unavailable` 或 `Fenced` 时，在原总期限和固定尝试次数内自动退避重试；控制、读与 ACK 也复用这一规则。每次只更新 request ID。生产在网络结果未知时也会在原期限与次数预算内重试原业务身份；最终仍未知则保留 PendingBatch 供显式恢复。会话过期、权限失败和永久拒绝不自动换身份。已被明确拒绝但耗尽重试预算的批次仍占原序号，不能静默跳过。

有未决 RPC 时，物理连接按 `ClientConfig::liveness_timeout` 运行有界原生 PING 探测，只有新的 1-RTT packet number 被对端确认才证明连接仍活跃。探测失败才替换该失活连接；慢业务 handler 不会单凭应用响应时间而误杀健康共享 lane。`client.transport_metrics()` 可查询探测与失败计数；该检测不把网络 ACK 当业务提交。

如果 `produce` 返回 `ProduceUnknown`，或其 future 在提交后被丢弃，原批次仍可从 `pending_batches()` 查询。按升序对最早未决批次调用 `retry`；恢复完成前拒绝新序号，不更换 producer 身份。原批次成功返回 `duplicate=true` 也是持久成功。`SequenceGap`、`ContentConflict`、权限/配额等保留类型化错误；被明确拒绝的批次仍阻止自动跨过该序号，必须按原身份处理。`SessionExpired` 或去重窗口过期不会自动重新注册。

注册回包未知返回 `RegistrationUnknown { producer_id, registration_epoch, .. }`；用 `open_at` 传回相同 ID 和 epoch，不能重新取新提示后悄悄创建另一会话。`retire` 只允许在没有未决批次时调用。SDK 没有持久 outbox：进程重启后不承诺自动重建未决消息、序号或原结果。跨进程业务幂等需要宿主保存业务身份与数据。

## 消费、逐条 ACK 与冷热切换

`Consumer::claim(client, topic, partition, subscription, config, deadline)` 先查询当前代次，再 CAS 取得分配。默认不抢占其他逻辑 session。确需接管时，先用 `Admin::subscription_status` 核对旧 session，再显式提供 `ConsumerConfig::replace_session_id`。同一 Client 的同一订阅分区只能建立一份消费状态；用 `Consumer::clone` 共享。已经创建过的 handle 被全部丢弃后，需要新 Client 和明确接管决定，避免复用丢失内存队列的旧代次。

`fill` 将消息放入有界队列；`next` 取出已排好顺序的可执行工作，`recv` 在队列为空时 fetch。`Delivery` 的 key、body 和 token 不出现在 Debug；payload/token 在最后一个 clone 释放前一直计入逻辑预算。队列满、宿主长期持有 Delivery 或未决批次时，会得到背压，不无限缓存。保留消息超过服务端 attempt 期限后，ACK 可能报告旧 token；它不会确认新的 attempt。

1. 分配冻结排他边界 H。B 只接收 H 之前的数据，D 只接收 H 及之后的数据。持续消费 D 后，持久 ACK 前沿可超过 H；H 不随 ACK 推进，后续接收屏障与 owner 恢复仍接受这种合法状态。
2. SDK 只合并实际成功入队的 offset 和服务端给出的持久 ACK 区间。缺少 offset 时不取最大值推进；最早块缺失时保留有限预取和头部容量。
3. 连续接收前沿通过 C 上的 `ReceiveBarrier` 确认；回包未知可用相同 tuple/前沿重试。服务端确认后才启用 D。
4. 接收屏障不修改持久业务 ACK。H 之前仍未 ACK 的消息可能需要 B 重投，因此启用 D 后，在冷 ACK 前沿未达到 H 时有限轮换 B/D；慢业务 ACK 不阻止 D 开始。

`ack`/`ack_many` 必须在业务成功之后调用；同时绑定 topic、partition、subscription、session、cursor/assignment/delivery generation。ACK 使用 C connection 的短 Ack streams，并按条数与真实编码帧大小切批，保留 Core 控制流。重复旧 token 不会确认新 attempt。一次 `ack_many` 的早期切片可能已提交而后续切片失败；重试原 Delivery 切片是幂等操作。

默认 `DeliveryOrdering::Parallel` 允许业务完成乱序。显式 `KeySerial` 对同 key 同时只给一个 handler，其他 key 可并行；`fail(delivery)` 保留失败决定并阻塞该 key，`retry_failed(offset)` 明确重试。发生 owner fence 后只保留有上限的失败 offset 标记，释放 SDK 队列中的旧正文/token，等待新分配重新交付，仍不会偷偷解锁失败 key。新交付的连续接收证据建立后，其他 key 可以继续；显式 retry、持久 ACK 证据或显式 seek 才解除对应失败决定。不会把失败转换成自动 ACK、跳过或 DLQ。业务副作用仍须幂等或有业务 fence，SDK 不提供外部 exactly-once。

KeySerial 按配置的最多缓冲条数，为失败 offset 元数据在 consumer/client 字节准入中额外固定预留每条 40 B；此保守逻辑额度不等于 allocator 或 RSS。收到新一代对应消息后复用已有 key 调度表，不在每次 `next` 上扫描所有失败 key。宿主持有的旧 Delivery clone 仍按原字节额度计费，SDK 无法替宿主释放它。

`seek(offset, operation_id, deadline)` 使旧 token、排队数据、失败决定与接收屏障失效，并重新 Claim。seek 结果未知或 future 取消后，本地停止派发；以相同 operation ID 重试 seek。`fill`/`recv` 收到 owner 的 `Fenced`/`StaleGeneration` 后，在传入的原总 deadline 和固定次数内查询最新 status 并 CAS 重新 Claim，Consumer clone 共享恢复后的状态。仅原逻辑 session 或未分配状态允许自动恢复；其他 session 已占用时返回 AssignmentBusy，自动路径不会复用配置中既有的 takeover consent 去抢占它。确需接管仍由业务明确调用 `reclaim`，并匹配 `replace_session_id`。

新代次从持久 ACK frontier 重新建立连续 enqueue/冷热屏障，旧 Delivery 的代次和 token 保持原样；旧 ACK 不会被改写到新分配或自动成功。`ack`/`ack_many` 本身返回原操作错误，让下一次 `fill`/`recv` 恢复，业务须按幂等契约处理重交付。`next` 只访问本地队列，没有网络 deadline，不主动恢复。普通 QUIC 断连自动重握手并保留原逻辑 session；无法确定的 seek 不会被 owner 自动恢复分支擅自重解释。

## connection、stream 与公平准入

一个 Client 的 C/P/D/B 共享逻辑 session，每类只使用一个到稳定入口的 connection；每条连接仍独立完成 TLS/应用认证。C 在 connect 时建立，P/D 按实际操作创建，B 在冷读或冷重投时创建。

默认每个数据类别复用 4 个 Request lanes，C 持续驱动全部预建 Core lanes。生产顺序域固定绑定 lane，affinity 不含序号或网络 attempt；不同域共享有限 lane 集合。lane 计数不会随分区、订阅或 handle 数增长。物理连接更换时，旧未决批次先恢复，不能带着新序号跨 lane 发出。不同域哈希碰撞仍会共享一个 stream，是有限复用的性能边界；默认 lane 数须结合基准调优。

全客户端生产在途/未决对象默认最多 256，每个 producer 最多 32；生产准入用 Tokio FIFO semaphore，在调用总期限内排队，顺序锁不跨等待回执。SDK 的 payload/元数据预算、每个 RPC 池的 pending 字节、原生传输 envelope 和控制保留分别受限，不能把其中一个数直接当作进程 RSS 上限。宿主输入 `Bytes` 的底层大块共享分配也不能仅凭切片长度还原；RSS/分配器开销由宿主观测。

`Client::cancel(request_id, deadline)` 发送跨流取消，保留其他复用请求；它只说明取消已进入有限发送队列，不证明服务端业务已经撤销。`Client::close(deadline)` 停止新准入并有限排空；超时未决结果按原身份查询/重试，不把 close 当事务回滚。

## 宿主观测与验证

SDK 从不安装全局 subscriber/exporter/HTTP 服务或 global allocator。`tracing` 使用有期限的请求 span，包含固定操作类别及 operation ID，跳过 token、正文、TLS key 和 Delivery token。生产端到端计时包含本地排队到结果；手动消费只能观察交付/ACK，真实 handler 时间由宿主记录。

宿主需要跨进程追踪时，用 `scope_traceparent(w3c_context, sdk_operation_future).await` 为单次有限操作传入上下文。它复用协议的 W3C 校验，拒绝非法/超长值且不执行该操作，任务之间不互相污染；后续未作用域的调用不继承旧 context。宿主新 spawn 的任务需自己设置作用域。上下文只进请求帧，不影响业务摘要、幂等或授权，也不安装 OpenTelemetry 后端。

启用 `metrics` feature 后，调用 `client.metrics().register(registry.sub_registry_with_prefix("my_client"))` 复用宿主 registry。指标只有固定系列：请求/重试/未知结果、交付与持久 ACK 计数、操作时延直方图、准入等待累计时间，以及保留字节、批次、Delivery 的逻辑 gauge。completed 后仍被宿主持有的 `PendingBatch`/`Delivery` 也计费，不能把 gauge 直接解释为未提交消息数。

```bash
cargo test -p tidemq-sdk --all-features
cargo clippy -p tidemq-sdk --all-features --all-targets --no-deps
cargo run -p tidemq-sdk --example host_observability --features metrics
```

`produce` 示例需要 `TIDEMQ_ENDPOINT`、`TIDEMQ_CA_PEM`、`TIDEMQ_TOKEN`、`TIDEMQ_TOPIC_ID`；`consume` 再需要 `TIDEMQ_SUBSCRIPTION_ID`，设置 `TIDEMQ_ORDERING=key` 演示同 key 串行，否则并行。消息必须来自真实服务端，示例不内嵌伪造 broker。

`sdk/rust/tests/quic_client.rs` 使用真实 UDP/TLS 的协议测试 peer，覆盖流水、回包丢失/取消/物理重连、固定 lanes、屏障/ACK 洞、同 key 失败、seek 隔离、ACK 切批和宿主 registry。它不证明 RF1/RF3 持久性、真实 Gateway 接管、Linux/负载均衡/三区性能；这些由集成运行时和后续端到端验收给出。

## 小内存预算与句柄释放

`max_pending_bytes` 是 SDK 逻辑在途数据和交付元数据的共享上限，传输端另有显式 `transport_memory_bytes` 预算。生产者最多占用共享上限扣除消费保留部分：保留至少四分之一，并且足以容纳一条最大消息及交付元数据。未决生产批次占满生产额度时，消费仍有准入空间；ACK 和控制继续使用独立核心传输额度。宿主持有的交付或 PendingBatch clone 会继续持有相应额度。

消费 Read 在发出前预扣消息体、令牌及交付元数据的全部 credit，按返回交付拆分许可，未用部分归还；服务端不能用逻辑载荷字节越过调用者队列预算。小队列会降低请求条数，以容纳一条实际可装入的消息。SDK 为需跨未知结果保留的生产内容创建有界副本，避免一个很小的 Bytes 切片长期保留调用方的大块底层分配；这会增加一次复制，性能报告需包含该成本。

最后一个 Consumer clone 释放后，本地 claim 额度回收；这不修改服务端持久 assignment，也不会替业务 ACK。连接关闭会终止并等待 SDK/RPC 所有的接收、发送和 stream acceptance 任务，最多等待传入 deadline。上游 QUIC endpoint 的异步释放可能仍需短暂调度与协议关闭过程；重启验证检测实际 UDP 端口释放，不通过复用端口绕过。
