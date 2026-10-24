# Rust SDK 接入与恢复

SDK 位于 sdk/rust，只依赖共享协议和传输库。应用使用逻辑 Topic、可选业务 key 和持久订阅；服务端与 SDK 处理分片、路由和成员代次。SDK 不安装全局分配器、日志订阅器或遥测出口。

## 创建与使用

Topic 由管理端预先创建。订阅只保护其起始位置以来仍可用的消息；需要保护未来生产的消息时，先创建订阅。

```rust,no_run
use std::time::Duration;
use tidemq_sdk::{Client, Credentials, Message, TlsTrust};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
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

let queue = client.new_queue_consumer("orders", "jobs").build().await?;
let producer = client.new_producer("orders").build().await?;
let receipt = producer.send(Message::new("created").key("order-42")).await?;
println!("Committed message {}", receipt.message_id());

let message = queue.receive().await?;
process(message.payload()).await?;
queue.ack(&message).await?;

queue.close().await?;
producer.close().await?;
let report = client.close().await?;
println!("Unresolved sends: {}", report.unresolved_sends());
# Ok(()) }
# async fn process(_: &[u8]) -> std::io::Result<()> { Ok(()) }
```

管理端使用 `admin.create_topic(name)` 创建逻辑 Topic，不选择固定分区数。`Producer`、`StreamConsumer` 和 `QueueConsumer` 是三个独立的业务类型。

必填项在 builder 构造时传入，选填项链式设置。构造器和 setter 不做 I/O；build().await 成功才返回可用实例。未配置 Namespace 时使用完整 tenant/namespace/topic 名称。Client 上的 Namespace 只解析名称，不授予权限。

TlsTrust 接受服务端 CA 文件或 PEM，不接收 CA 私钥。Credentials 支持 token_file、token_env 和 token。集群在线签发凭据，Metadata Raft 持久保存校验记录；应用通过私有文件或部署 Secret 取得 token。撤销和过期影响已有连接的新准入。Debug、日志和管理元数据不得输出凭据原值或签名材料。

## 租户限额

管理员使用 `ResourceLimits::builder()` 配合 `set_tenant_limits(tenant, limits)` 或 `set_namespace_limits(namespace, limits)` 设置资源数量、积压、在途、发布/消费速率和冷读并发。builder 仅修改显式设置的字段，Namespace 未设置的字段初始继承当前租户限制；`ResourceLimits::default()` 显式恢复标准限额。Namespace 共享租户总预算，增加节点或分片不会增加租户额度。

`PolicyUpdate::Applied` 表示策略及所需的数据准入屏障已经持久完成；`Superseded` 表示完成前被后续策略取代。降配到现有占用以下时暂停新增，保留数据，并允许 ACK 和恢复继续释放占用。超时或取消沿原 PendingAdmin 恢复，不能仅凭管理请求已经提交就宣告数据侧完成。

连接限额按各 Gateway、各流量类别的物理连接合计；Client clone 共享连接池，独立 Client 分别占用容量。连接和冷读批次采用 Metadata Raft 中的有限租约，失联或 Leader 变更不会复制额度。管理员可通过 `set_principal_enabled(principal, false)` 禁用主体，新准入同时受到凭据及主体有效性的约束。

`admin.tenants()`、`namespaces(tenant)`、`principals(tenant)`、`platform_principals()` 和 `credentials(principal)` 返回强类型分页对象。使用 `page_size(1..=32)` 与 `next_page().await?` 逐页读取，None 表示本次枚举结束。页面按真实父资源和主体作用域过滤，返回身份及有效限额，不含 token 原值或校验摘要。身份目录在翻页中发生变化时返回 `CatalogChanged`，应重新创建分页对象；常规消息、成员和额度操作不使身份分页失效。

`delete_namespace(id)` 要求先显式删除 Topic、禁用该 Namespace 下的主体，并撤销仍有效的凭据；`delete_tenant(id)` 还要求先删除其 Namespace。删除保留旧身份和原结果，同名重建分配新身份。Tenant、Namespace 的分页结果通过 `IdentityStatus::{Active, Disabled, Deleted}` 区分状态。临时停用使用 `set_tenant_enabled` 或 `set_namespace_enabled`，已删除身份不能重新启用。

## 生产与并发

send 接受 Into<Message>，简单消息可写 producer.send("hello").await。Message 用于设置 key 和属性。成功回执表示数据 Raft 已持久提交；QUIC packet ACK、发送入队和后台 S3 上传均不替代该完成点。

一个 Producer 可并发发送到多个分片。应用使用有界 FuturesOrdered/FuturesUnordered 或任务池驱动多个 send；集合 push 本身不推进 Future。逐条 await 会按应用写法串行等待。同 key 的发送按同一逻辑 Producer 的准入顺序推进，返回回执的顺序不建立跨 key 顺序。

send_batch 保留各条输入关联，允许跨组部分完成，不提供跨组事务。未知结果沿原发送身份和摘要恢复；不要重新 send 一遍来处理未知结果。

## 两类持久订阅

| 入口 | 语义 |
| --- | --- |
| new_stream_consumer(topic, subscription) | 同 key 有效交付有序，提供 ack_cumulative |
| new_queue_consumer(topic, subscription) | 多成员共同分摊消息，提供单条 ack 和 nack |
| receive().await | 等待并返回一条消息 |
| try_receive() | 只检查本地队列，返回 Option；None 不证明远端没有积压 |

同一 Topic 可有多个独立订阅。零成员保留订阅、确认进度和积压。Consumer 关闭或 Drop 不删除订阅。交付至少一次，业务副作用需要幂等。

Stream 的累计确认覆盖应用实际接收的共享前缀，不包含预取消息或别的成员的消息。多个 clone 共用该前缀；并行业务处理应先协调全部前置处理结果，才能累计确认较后的消息。Queue 不提供累计确认；nack(message, delay) 持久安排重投，成功回执表示重投已安排，不表示业务成功。在线成员的慢处理不会仅因缓存授权租约过期而被当成处理超时。

## Clone、预算与关闭

Client、Producer、StreamConsumer 和 QueueConsumer 的 clone 共享同一逻辑实例、身份、预算、驱动和关闭状态。Consumer clone 竞争同一队列，可确认另一 clone 返回的有效消息。新建独立实例应再次调用具名 new_* builder。任一 clone 的 close 关闭整个逻辑实例。

Client 和各逻辑实例分别限制消息数、字节、请求、结果、连接与任务。Consumer 的 prefetch_bytes 至少容纳一条最大消息和 512 字节交付元数据；当前 SDK 最低为 262656 字节，小于该值会在 build 时明确拒绝。Read 预留保持一条最大消息的余量，应用持有正文导致剩余额度不足时暂停预取，释放正文后继续。Consumer prefetch 覆盖 Read 预留、本地队列及应用持有的正文，不能按分片放大额度。ACK 释放逻辑交付额度后，应用仍持有的正文继续计费。传输、控制和恢复额度单独受限；逻辑计费不能当作进程 RSS 上限。

close 使用独立 close_timeout，返回 CloseReport，失败返回携带报告的 CloseError。报告区分本地清理、成员退出、未决 Read/发送/ACK/订阅删除/管理操作和未领取结果。Consumer 关闭在成员 fencing 完成后退休已接受或结果未知的 Read，未完成的退休通过 `unresolved_reads()` 报告。close 不替应用 ACK，不删除持久订阅；调用前应收束应用自身的发送和业务任务。

## 总期限、取消和恢复

默认网络操作使用 request_timeout。每次新建物理连接重新解析 endpoint；DNS、候选地址尝试、认证与流初始化共用 connect_timeout，同时受当前请求剩余期限限制。健康连接继续复用，重连保持原逻辑会话和共享预算。排队、连接、子请求、重试和退避共用总期限。无显式 context 的 receive 持续等待，其网络子请求仍有有限期限。

```rust,no_run
use std::time::Duration;
use tidemq_sdk::{QueueConsumer, RequestContext};

# async fn example(queue: &QueueConsumer) -> tidemq_sdk::Result<()> {
let context = RequestContext::with_timeout(Duration::from_secs(10))?;
let message = context.scope(queue.receive()).await?;
# let _ = message;
# Ok(()) }
```

context.cancellation_token() 可接入宿主取消。嵌套调用使用更早期限；通过 tokio::spawn 创建的新任务要显式传入 context。取消等待不撤销已提交操作，已准入发送和确认由 SDK owned driver 在原期限内收尾。

| 快照 | 恢复 |
| --- | --- |
| producer.pending_sends().await | PendingSend::retry().await 查询或继续原发送 |
| consumer.pending_acks().await | PendingAck::retry().await 查询或继续原 ACK/NACK |
| admin.pending_subscription_deletions() | PendingSubscriptionDeletion::retry().await 继续固定 UUID 的删除 |
| admin.pending_operations() | PendingAdmin::retry().await 查询或继续原资源创建、凭据变更、Topic 拆分/合并/删除 |

管理操作成功仍返回对应的强类型结果；未知结果返回携带原 PendingAdmin 的 Error::Administration。retry 返回 AdminReceipt，保持原操作、资源身份和布局条件。管理恢复共享 Client 的资源预留，最多保留 16 项，每项预留 128 KiB；其他逻辑实例占用预留时可能更早拒绝新操作。凭据原值只交付一次，回包丢失后只返回已提交的 Credential ID；收到 CredentialSecretUnavailable 后先撤销该凭据，再签发新值。

快照包含仍在处理、未知或部分完成，以及完成但尚未领取的结果。查询不 drain、不自动重试、不替业务确认。未知结果保留原身份和范围，不能为了腾空间静默丢弃。恢复句柄与正文均计入预算；没有 SDK 持久 outbox，跨进程重启不会自动找回本地账本。网络恢复须在原 Client 关闭前完成。

管理端使用 stream_subscription/queue_subscription 查询，使用 delete_stream_subscription/delete_queue_subscription 删除。删除默认拒绝在线成员，固定 UUID 并冻结新加入，逐数据域解除状态和保护，全部完成才成功。中断后继续 pending 句柄，不按名称发起替代删除；同名重建订阅使用新 UUID。

管理端使用 `delete_topic(name)` 删除逻辑 Topic。必须先显式删除其全部订阅；操作固定原 Topic UUID，阻止新创建和布局变更，逐数据域持久封写后才成功。缺失数据组时保留进度并通过 `pending_operations()` 继续，不能以超时视为完成。同名重建使用新 UUID；原删除重试不作用于新资源。旧身份保留结果查询和清理路由；已提交但丢回包的发送仍返回原 MessageId。旧 Producer 不会刷新为同名新 Topic。逻辑删除不会绕过历史对象的保留、读取和恢复依赖。

## 观测与验证

平台管理员通过 `admin.register_node(NodeRegistration { ... })` 在线登记节点 ID、角色、故障域、内部地址、证书名与精确证书 pin；`NodeRegistration::certificate_pin(der)` 从叶证书 DER 计算 pin。登记信息由 Metadata Raft 持久保存，已有进程自动更新内部信任和目标目录。节点 ID 和证书绑定不可复用；新增进程仍需提供自己的 TLS 材料及初始 Metadata 地址。

节点登记不等于数据组已就绪。部署端先启动新组的实际副本，再调用 `admin.create_data_group(group_id, voters)`；服务端核对各副本已初始化且具有一致的成员集合，持久登记后等待副本识别目录，再返回成功。新 Topic 和 Split 从持久登记的可分配组选择目标。`cluster_nodes()`、`data_groups()` 返回有界、一致的目录快照；`set_node_schedulable(id, false)` 只停止新放置，节点退出仍须完成排空。节点/组变更与其他管理操作共用 `pending_operations()` 恢复账本，普通 Producer/Consumer 无需管理这些物理对象。

错误保留结构化状态与完整 source 链。outcome_unknown() 表示业务结果仍需恢复，不能仅凭外层 Display 或错误字符串决定重试。宿主需要完整链时，可用 anyhow::Error::new(error)，按 "{error:#}" 输出；SDK 不安装日志设施或增加 anyhow 运行时依赖。

宿主负责 Tokio、subscriber、provider、exporter 和 allocator。metrics feature 通过 client.metrics().register(...) 注册固定系列；opentelemetry feature 复用宿主的 MeterProvider 和 tracing bridge。scope_traceparent 为有限操作传递经过校验的 W3C 上下文；宿主创建的新任务自行设置作用域。不要重复累计 Prometheus 与 OpenTelemetry 的同义指标。

更多内容见[错误诊断](../operations/error-diagnostics.zh-CN.md)和[链路追踪](../operations/tracing.zh-CN.md)。生产示例使用 TIDEMQ_TOPIC 的完整名称；消费示例再提供 TIDEMQ_SUBSCRIPTION，TIDEMQ_MESSAGE_COUNT 默认十条。示例连接真实服务，不内嵌伪造 Broker。

```bash
cargo clippy -p tidemq-sdk --all-features --all-targets --no-deps --locked -- -D warnings
cargo test -p tidemq-sdk --all-features --doc --locked
cargo run -p tidemq-sdk --example host_observability --features metrics
```

服务功能和故障集成测试位于仓库 tests 目录。功能测试不证明吞吐提升、线性扩展或已校准的容量。
