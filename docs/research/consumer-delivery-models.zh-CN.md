# 消费者消息交付模型调研

调研日期：2026-10-27。外部资料使用官方文档、协议和项目一手源码，版本分别注明；云服务和部分项目文档会持续更新。TideMQ 代码依据本地 `main` 的 `bc1f491d6f60557f30f2b9b1981d7a2c41084634` 静态核验。本文没有运行功能、故障或性能测试。

TideMQ 的 Stream 和 Queue 消费者共用 SDK 后台读取驱动：它按分配域发起有数量和字节上限的 `GroupRead`，将响应写入本地队列；空读后逐步退避。正常完成的一轮空读仍经过读取结果和结果退休两个数据组写入。具体代码与限制见下文。

## 判断

建议 TideMQ **保留 SDK 后台、有界批量的拉取和现有 `receive` / `ack` 使用方式，优先验证带事件唤醒和有限等待的 long poll**。保留消息数与字节数两种信用，Stream 和 Queue 使用各自的确认语义。此方向与 Kafka 传统消费者、RocketMQ 的后台拉取封装、JetStream pull 等公开机制有可比之处，但不意味着 TideMQ 已具备它们的协议、保证或性能。

这是一项**待实验验证的设计判断**：有可靠唤醒时，long poll 有望减少空读请求和 Raft 写入，并缩短现有退避造成的等待；它也会引入服务端等待者、唤醒、取消、跨节点转发和资源上限等成本。不能简单地在当前 handler 内休眠。Pulsar 式显式 credit 服务端推送或 Pub/Sub 式双向流可作为后续备选；现有资料不足以证明更换交付协议对 TideMQ 必要。

## 比较口径

- **应用 API**：业务代码看到的 `poll`、`receive`、迭代器或回调。回调只说明 SDK 如何交给业务，不能据此判断网络方向。
- **网络交付**：短轮询是客户端请求后立即或尽快返回；long poll 由客户端发起请求、服务端可在限定时间内等待再响应；服务端推送是客户端建立订阅或流后，由服务端在许可和协议约束内主动发送。一次由客户端发起、服务端流式返回多条响应的 RPC，仍应说明请求是谁发起，不能只凭 `stream` 一词归类。
- **缓冲 / 信用 / 流控**：分别说明限制的是一次请求、客户端缓冲、服务端许可、未确认消息，还是速率。它们不必然是同一个窗口，也不必然提供进程内存的硬上限。
- **确认**：offset 提交、逐条 ack、接收句柄删除、ack deadline 和租约不是同一种语义；重试和业务副作用是否重复，要按各自契约分析。

| 产品和范围 | 应用 API | 主要网络交付方向 | 核心限制或确认方式 |
| --- | --- | --- | --- |
| Kafka 4.2 传统消费者 | `poll` | 客户端发起 Fetch，可等待 | 请求字节数与 poll 返回条数是不同限制；按分区提交 offset。[配置](https://kafka.apache.org/42/configuration/consumer-configs/)、[消费者 API](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) |
| Kafka 4.2 Share Groups | `poll` | 客户端获取带限时锁的记录 | 可逐条确认，多消费者可共享一个分区；批量模式可能超过 poll 条数设置。[Share API](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaShareConsumer.html)、[升级说明](https://kafka.apache.org/42/getting-started/upgrade/) |
| Pulsar 4.x 常规消费者 | `receive`、异步接收或 listener | 客户端发 `Flow` 许可，broker 推送 | 客户端队列和消息数 permit；按订阅类型逐条或累计 ack。[协议](https://pulsar.apache.org/docs/4.0.x/developing-binary-protocol/)、[确认和订阅](https://pulsar.apache.org/docs/4.0.x/concepts-messaging/) |
| RabbitMQ AMQP 0-9-1 队列 | 回调或单条 `basic.get` | `basic.consume` 推送；`basic.get` 短轮询 | 手动确认窗口由 `basic.qos` prefetch 控制；`basic.get` 不受该窗口控制。[消费者](https://www.rabbitmq.com/docs/consumers)、[确认](https://www.rabbitmq.com/docs/confirms) |
| RocketMQ 5.x | `PushConsumer` listener 或 `SimpleConsumer.receive` | SDK 发起长轮询；5.x gRPC 接收 RPC 使用服务端流式响应 | Push 本地缓存；Simple 显式逐条 ack、消息不可见期。[消费者类型](https://rocketmq.apache.org/docs/featureBehavior/06consumertype/)、[v2.0.4 协议](https://github.com/apache/rocketmq-apis/blob/v2.0.4/apache/rocketmq/v2/service.proto) |
| NATS JetStream | `fetch`，或库在后台拉取后回调；也支持 push consumer | pull 请求可等待；push 向投递 subject 发送 | pull 次数 / 字节数 / 等待时间，consumer 级 `MaxAckPending`；可逐条或累计 ack。[pull 协议](https://docs.nats.io/reference/jetstream/api/consumer/get-next)、[consumer 配置](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/consumers.md) |
| AWS SQS API | `ReceiveMessage` | 客户端短轮询或 long poll | 每次最多请求 10 条；可见性超时，使用当前 receipt handle 删除。[ReceiveMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html)、[可见性](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html) |
| Google Cloud Pub/Sub | 高级库异步回调；也可显式 Pull；另有 HTTP push | 客户端发起 unary Pull 或双向 StreamingPull；push subscription 由服务端向 HTTP endpoint 发请求 | StreamingPull 可限制未确认条数和字节数；按 ack ID 确认。[Pull](https://docs.cloud.google.com/pubsub/docs/pull)、[RPC](https://docs.cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1)、[Push](https://docs.cloud.google.com/pubsub/docs/push) |

## 逐项核验

### 1. Apache Kafka

Kafka 4.4.0 已于 2026-10-22 发布，其协议仍定义带等待和多分区的 Fetch 请求。下文以有明确 API 文档的 4.2 客户端行为为例；不将具体配置和默认值未经核验地推广到所有版本。[4.4 发布说明](https://kafka.apache.org/blog/2026/10/22/apache-kafka-4.4.0-release-announcement/)、[4.4 协议](https://kafka.apache.org/44/design/protocol/)

**传统 `KafkaConsumer`，以 4.2 Java 客户端为准。** 应用调用 `poll(Duration)`；客户端向 broker 发 Fetch 请求，不是 broker 无请求地向应用推送。`fetch.min.bytes` 决定请求期望的最少数据，数据不足时可等待，`fetch.max.wait.ms` 给出等待上限；本地日志和远端存储的等待配置应分别看版本文档。[消费者配置](https://kafka.apache.org/42/configuration/consumer-configs/)

**预取和流控。** 客户端按批获取并缓存记录，`max.poll.records` 仅限制一次 `poll` 返回多少条，不控制底层 Fetch。`fetch.max.bytes` 和 `max.partition.fetch.bytes` 是获取数据的配置，但为了保证前进，首个非空分区的首个记录批次即使超限也可能返回；客户端还可能并行执行多个 Fetch。因此这些参数不是进程总内存的严格硬上限。[消费者配置](https://kafka.apache.org/42/configuration/consumer-configs/)

**确认和局限。** 传统 consumer group 以分区 offset 记录进度，不是对每条消息独立 ack。自动提交与手动提交的时点决定故障时可能重复处理或跳过未完成工作；应用处理过慢还需要考虑 `max.poll.interval.ms` 和再均衡。[KafkaConsumer API](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)、[消费者配置](https://kafka.apache.org/42/configuration/consumer-configs/)

**版本例外：Share Groups。** Kafka 4.1 将其作为预览，4.2 宣布生产可用；一个分区可由多个消费者协作，获取的记录有时限锁，并支持逐条 accept / release / reject，4.2 还支持 renew。它适合逐条处理的队列式场景，不能把传统分区 offset 模型当作 Kafka 唯一模型。4.2 ShareConsumer 默认 `batch_optimized` 可预取且返回数可能超过 `max.poll.records`；`record_limit` 提供不同的限制行为。该分支不等同于传统消费者的有序流语义。[4.2 升级说明](https://kafka.apache.org/42/getting-started/upgrade/)、[4.2 ShareConsumer API](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaShareConsumer.html)、[4.2 发布说明](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/)

### 2. Apache Pulsar

**4.x 应用和网络方向。** `receive()` 从本地接收队列取消息，也有异步接收和 listener；二进制协议中消费者向 broker 发送 `Flow`，其中 `messagePermits` 增加允许投递的消息数，broker 以 `Message` 命令在许可内推送。协议文档给出的典型客户端在应用取走队列中一半消息后补充许可；具体触发策略不能泛化到所有客户端版本。[4.0 客户端](https://pulsar.apache.org/docs/4.0.x/concepts-clients/)、[4.0 二进制协议](https://pulsar.apache.org/docs/4.0.x/developing-binary-protocol/)

**流控与确认。** `receiverQueueSize` 控制本地接收队列的消息数；协议 permit 是消息数，不等同于字节数硬上限或未确认消息数。4.x 可逐条或累计 ack，但 Shared 和 Key_Shared 不支持累计 ack。默认 ack timeout 关闭；启用后由客户端跟踪并请求重投。负确认可能影响有序订阅的原始交付顺序。[客户端](https://pulsar.apache.org/docs/4.0.x/concepts-clients/)、[消息与确认](https://pulsar.apache.org/docs/4.0.x/concepts-messaging/)

**版本边界。** Pulsar 5 的 Java v5 API 将消费者模型区分为 Stream、Queue 和 Checkpoint：Stream 提供有序处理及累计确认，Queue 提供逐条确认等队列语义。旧 v4 API 在常规 topic 上仍受支持；v5 API 有 Pulsar 5.x broker 与 scalable-topic 协议等前提，迁移时不能假定旧订阅直接与新消费者混用。上段 `Flow` 机制的引用限定于 4.x 二进制协议，不把它未经核验地推广为 v5 scalable-topic 协议的全部细节。[5.0 发布说明](https://pulsar.apache.org/blog/2026/10/05/announcing-apache-pulsar-5-0/)、[v5 迁移指南](https://pulsar.apache.org/docs/client-libraries/java-migrate-to-v5/)

### 3. RabbitMQ

**范围限定为 AMQP 0-9-1 队列。** `basic.consume` 建立长生命周期消费者，broker 异步投递到客户端回调；`basic.get` 每次主动获取一条，队列空时返回，官方不建议以频繁 `basic.get` 作为常规消费方式。这与长轮询不同。[消费者文档](https://www.rabbitmq.com/docs/consumers)

**prefetch 与 ack。** 手动确认时，`basic.qos` 的 `prefetch_count` 限制已投递但尚未确认的窗口；确认后可继续投递，`0` 表示不限制。RabbitMQ 将 `global=false` 应用于 channel 上新建的**每个消费者**，而非按标准原意共享整个 channel；global QoS 已弃用。`basic.get` 不受 QoS prefetch 约束，自动确认也不提供同样的未确认窗口。手动 ack 可逐条或使用 `multiple` 批量确认，nack / reject 可控制重入队；连接或 channel 关闭时未确认的投递可重入队。[prefetch 3.13 文档](https://www.rabbitmq.com/docs/3.13/consumer-prefetch)、[确认与重投](https://www.rabbitmq.com/docs/confirms)、[弃用列表](https://www.rabbitmq.com/release-information/deprecated-features-list)

**范围和版本局限。** 这里的窗口主要以未确认的消息数计，不是字节级内存上限。RabbitMQ 的 Streams 还支持独立的 Stream 协议和不同信用机制；不能把本段 AMQP 队列结论推广到所有 RabbitMQ 交付方式。不同队列类型与版本对 global QoS 的支持也需分别核验。[Streams 文档](https://www.rabbitmq.com/docs/streams)、[弃用列表](https://www.rabbitmq.com/release-information/deprecated-features-list)

### 4. Apache RocketMQ

**5.x `PushConsumer`。** 应用注册 listener 并返回消费结果；官方描述 SDK 后台通过长轮询取消息、放入本地队列，再分发给消费线程。名称中的 Push 指向业务的交付形态，不能由此推断 broker 不经请求主动发送。listener 提前返回成功后异步处理，可能让 SDK 无法感知后续失败；官方要求在 listener 调用范围内同步返回实际消费结果。[消费者类型](https://rocketmq.apache.org/docs/featureBehavior/06consumertype/)

**5.x `SimpleConsumer`。** 应用显式 `receive`、处理、`ack`，可控制接收并发和不可见期，并可修改不可见期；未及时确认可触发重试。Java 示例的 `awaitDuration` 用于 long poll。RocketMQ API v2.0.4 的 `ReceiveMessage` 由客户端请求，返回 `stream ReceiveMessageResponse`，请求包含批量大小、不可见期和 `long_polling_timeout`；单个服务端流式响应不等于无请求的推送订阅。[消费者类型](https://rocketmq.apache.org/docs/featureBehavior/06consumertype/)、[Java 示例](https://github.com/apache/rocketmq-clients/blob/master/java/client/src/main/java/org/apache/rocketmq/client/java/example/SimpleConsumerExample.java)、[v2.0.4 IDL](https://github.com/apache/rocketmq-apis/blob/v2.0.4/apache/rocketmq/v2/service.proto)

**缓冲和版本。** Java gRPC SDK `java-5.2.2` 的 PushConsumer builder 提供本地缓存消息数和字节数配置，这说明其有本地缓存阈值，不能由此推断服务端按同样信用逐条投递。官方列出旧 Remoting SDK 支持 4.x / 5.x 服务端，而新 gRPC SDK 对应 5.x；5.0 PushConsumer 与旧版的负载均衡粒度也不同。SimpleConsumer 的并发和处理负载主要由应用决定。[Java 5.2.2 builder](https://github.com/apache/rocketmq-clients/blob/java-5.2.2/java/client-apis/src/main/java/org/apache/rocketmq/client/apis/consumer/PushConsumerBuilder.java)、[SDK 对比](https://rocketmq.apache.org/docs/sdk/01overview/)、[消费者类型](https://rocketmq.apache.org/docs/featureBehavior/06consumertype/)

### 5. NATS JetStream

**pull 与 push 都存在。** pull consumer 向 `$JS.API.CONSUMER.MSG.NEXT...` 发请求，可给出 `batch`、`max_bytes`、`expires` 和 `no_wait`；服务端按请求投递。高级客户端也可用持续消费 / 回调形式，在后台继续发 pull，因此回调不等于网络 push。push consumer 则向配置的 `DeliverSubject` 主动发送；项目文档建议新项目在需要伸缩、细粒度流控或错误处理时优先考虑 pull。[pull 请求](https://docs.nats.io/reference/jetstream/api/consumer/get-next)、[pull 消费](https://docs.nats.io/learn/jetstream/pull-consumers)、[consumer 配置](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/consumers.md)

**不同窗口。** `MaxAckPending` 是同一 JetStream consumer 所有绑定订阅共享的未确认消息数上限，pull / push 都适用；pull 自身还受请求数量、字节数及服务端等待请求上限约束。push 的可选 `FlowControl` 是按订阅交换控制消息的滑动窗口，与 consumer 级 `MaxAckPending` 不同。`expires=0` 可表示请求不失效，原始请求在安静流上可能长期等待；客户端默认值依语言和版本而异。[consumer 配置](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/consumers.md)、[pull 消费](https://docs.nats.io/learn/jetstream/pull-consumers)、[pull 请求](https://docs.nats.io/reference/jetstream/api/consumer/get-next)

**确认和版本。** `AckExplicit` 逐条确认；`AckAll` 会累计确认，针对共享 pull consumer 使用时需特别核对其对其他订阅者 pending 的影响；`AckNone` 不等待确认。`AckWait` 超时可重投，配置 `BackOff` 时按其规则处理。文档标明 `MaxAckPending`、push `FlowControl` 等选项自 2.2.0 起提供、`MaxRequestExpires` 自 2.7.0 起提供；当前文档另列 2.14 的 `AckFlowControl`，不要套用到旧版本。[consumer 配置及版本表](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/consumers.md)

### 6. AWS SQS

**请求模式。** `ReceiveMessage` 一次请求最多 10 条。`WaitTimeSeconds=0` 为短轮询，服务端抽样部分服务器，可能出现假空响应；大于 0 启用 long poll，最长 20 秒。响应可能少于请求数，HTTP 读超时需要与等待时间协调。[ReceiveMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html)、[短轮询与长轮询](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html)、[long poll 实践](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/best-practices-setting-up-long-polling.html)

**流控和确认。** 请求条数及调用并发控制客户端获取速率；SQS 的 in-flight 配额是队列侧限制，不是每消费者的消息 / 字节 prefetch 信用。收到消息后进入 visibility timeout，处理完成通过 `DeleteMessage` 和该次接收的 `ReceiptHandle` 删除；处理较久可以延长可见性。旧 handle 的删除请求可能成功返回但消息未被删除，因此应使用最近一次 handle。[in-flight 排查](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/troubleshooting-messages-not-returned-ReceiveMessage.html)、[可见性](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)、[DeleteMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html)

**局限。** 达到 in-flight 限制时，long poll 可能不返回新消息而不是给出配额错误；空响应不等于队列中没有消息。标准队列的至少一次交付意味着即使在可见性期间或删除后也可能再次收到消息；业务需要考虑幂等，不能仅靠 timeout 推断恰好处理一次。[in-flight 排查](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/troubleshooting-messages-not-returned-ReceiveMessage.html)、[可见性](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)、[DeleteMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html)

### 7. Google Cloud Pub/Sub

**Pull、StreamingPull 与独立的 HTTP Push。** Pull subscription 由客户端发起 unary Pull 或双向 gRPC StreamingPull；高级客户端通常在后台使用 StreamingPull，并异步调用业务处理函数。StreamingPull 的已建立连接可收到多次服务端响应，应用回调本身不能说明是 HTTP push。unary Pull 的弃用字段 `return_immediately=false` 允许服务端有界等待，但不保证等待或一定返回消息；空响应不证明无可用消息。Push subscription 则是服务端向业务 HTTP endpoint 发送 POST。[Pull](https://docs.cloud.google.com/pubsub/docs/pull)、[RPC](https://docs.cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1)、[Push](https://docs.cloud.google.com/pubsub/docs/push)

**流控和确认。** StreamingPull 初始请求可设置 `max_outstanding_messages` 与 `max_outstanding_bytes`；达到相应未 ack / nack 阈值后服务端停止继续发送，后续消息获得确认或否定确认后恢复，初始请求后的更改会使流报错。高级客户端还提供自己的 flow control 和 lease 管理；例如 Go v2 文档说明批量接收时字节阈值可能暂时超出，不能据此承诺严格的内存上限。客户端以 ack ID 确认，deadline 到期可重投；push 通过指定 HTTP 状态码确认，服务端根据成功与失败调整推送窗口和退避。[RPC](https://docs.cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1)、[Go v2 客户端](https://docs.cloud.google.com/go/docs/reference/cloud.google.com/go/pubsub/v2/latest)、[Push](https://docs.cloud.google.com/pubsub/docs/push)

**交付保证的边界。** 默认是至少一次；可选 exactly-once delivery 仅适用于 pull / StreamingPull，保证限定在单个云区域和 Pub/Sub 定义的消息 ID 范围。过期后重投的有效 ack ID、ack 成功与业务副作用之间仍需应用正确处理，不能泛化为跨系统业务恰好一次。[Exactly-once 文档](https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery)、[订阅概览](https://docs.cloud.google.com/pubsub/docs/subscription-overview)

## TideMQ 当前代码的证据

1. **读取与调度。** 构造消费者并加入后，SDK 启动独立的 heartbeat 和读取任务；读取任务按分配的 shard 轮转，最多同时运行 8 个读取任务。一次新读取最多预留 32 条消息及受帧大小、消费者和客户端共同内存约束的字节额度。非空读取完成后，下一次读取设置至少 10 ms 的间隔；连续空读的计划间隔从 20 ms 递增至 640 ms，驱动还受 25 ms 定时器、任务完成事件和请求耗时影响。这里的数值是代码参数，不是端到端延迟保证。[SDK 驱动](https://github.com/bitcapybara/tidemq/blob/main/sdk/rust/src/group_consumer/driver.rs)
2. **应用接收与信用。** 默认每个消费者的预取上限为 256 条和 8 MiB，并可通过 builder 调小；`receive()` 从本地队列取消息，空时等待通知。读取前 SDK 预留消息槽和字节额度，已收取数据的消息槽在相应确认结果成功处理、撤销等路径释放；字节内存由消息对象及其克隆的生命周期继续持有，直到最后引用释放。不能把出队、消息槽释放、内存释放和业务确认视为同一个事件。[消费者](https://github.com/bitcapybara/tidemq/blob/main/sdk/rust/src/group_consumer.rs)、[消息额度](https://github.com/bitcapybara/tidemq/blob/main/sdk/rust/src/message.rs)、[确认结果处理](https://github.com/bitcapybara/tidemq/blob/main/sdk/rust/src/group_consumer/ack.rs)
3. **空读也有持久状态。** 当前协议没有等待参数。`GroupRead` 经 `read_consumer_page` / `group_commit` 调用数据组的 `raft.client_write`；状态机记录读取序号、摘要及结果，即使结果为空。SDK 接受响应后还发 `GroupRetireRead`，退休读取结果也是一个数据组写入。因此，一轮**正常完成**的空读通常包含两次数据组 Raft 写入；失败、重试或路由变更时实际行为不同。[协议](https://github.com/bitcapybara/tidemq/blob/main/crates/tidemq-protocol/proto/public.proto)、[读取](https://github.com/bitcapybara/tidemq/blob/main/src/runtime/consumer_read.rs)、[提交和退休](https://github.com/bitcapybara/tidemq/blob/main/src/runtime/consumer_data.rs)、[Raft 写入](https://github.com/bitcapybara/tidemq/blob/main/src/runtime/metadata.rs)、[读取幂等状态](https://github.com/bitcapybara/tidemq/blob/main/src/subscription/group.rs)
4. **等待会占用现有额度。** 服务端数据 handler 默认 128 个槽，持有到 handler 和响应排队结束；外部请求的取消登记表当前最多 256 项。读取可能先后经 Gateway、owner 和数据组 leader 转发，现有转发路径逐跳等待响应。若只是将等待加进 handler，这些有限资源可能在多跳被占住；取消请求也需要能被准入。SDK 端的等待还会占据读取任务以及已预留的消息 / 字节额度。[dispatcher](https://github.com/bitcapybara/tidemq/blob/main/src/server/dispatch.rs)、[路由和取消](https://github.com/bitcapybara/tidemq/blob/main/src/runtime/service.rs)、[客户端预留](https://github.com/bitcapybara/tidemq/blob/main/sdk/rust/src/group_consumer/driver.rs)
5. **空结果不等于全局无消息。** 消费者的 lane、当前成员、在途窗口、租户额度、延迟重投和消息大小等条件都会影响当前可交付结果。代码已有带租户额度竞争的空读用例；因此不能只监听新消息写入，也不能依据一次空读断定订阅没有 backlog。[读取与投递](https://github.com/bitcapybara/tidemq/blob/main/src/subscription/group.rs)、[额度用例](https://github.com/bitcapybara/tidemq/blob/main/tests/scalable_support/data_quota_raft.rs)

例如，在单个分配域持续空闲、没有资源竞争且忽略请求和调度耗时的理想化情况下，640 ms 退避对应每秒约 1.56 轮空读，正常每轮还涉及读取和退休两个 Raft 写入。这只是根据代码计算的上限量级，不是当前部署的实测 QPS；实际频率会受并发上限、网络、Raft、空读原因及调度影响。

## 候选方案与选择

| 方案 | 可能收益 | TideMQ 需要承担的成本 | 判断 |
| --- | --- | --- | --- |
| 保持当前短轮询 | 实现和恢复路径已存在，资源持有时间通常较短 | 空闲时反复请求并产生读取 / 退休写入；退避会影响消息到达后的发现时间 | 作为实验基线；不能假定当前一定是瓶颈 |
| 有界 long poll + SDK 预取 | 可用时及时唤醒，空闲时减少请求；保留主动请求和批量信用 | 需要有界等待、跨跳资源管理、可靠唤醒、取消和多 shard 公平性 | **优先验证**，不是简单延长客户端 sleep |
| 信用驱动的持续流式投递 | 一次建流后可连续交付，可能进一步减少请求往返 | 需要重新定义流信用、接收确认与持久结果恢复，处理重连、旧代次和流重建 | 当 long poll 的往返或等待者成本被实测证明不能满足目标时再评估 |

这三种方案的延迟、吞吐和资源差异依赖负载与实现，不能从协议名称推出性能排名。无论选哪种，都应保留有限的消息数和字节数准入，分别维护 Stream 的顺序 / 累计确认约束和 Queue 的逐条确认 / 重投约束；不要因共用 driver 就合并其业务语义。

另一个可先测量的独立问题是：当前**非空**读取完成后也设置了至少 10 ms 的下一次读取间隔。可以在不改变业务 API 的前提下评估有 backlog 且信用可用时更快调度，但必须同时维持 shard 和租户公平，不能未经实验就认定这会改善整体吞吐。

## 有界 long poll 的设计边界

以下是候选约束，不表示当前代码已经实现。

1. **等待与实际读取分离。** 先按现有身份、路由和当前状态判断是否可能交付；无工作时登记一个有限等待，唤醒后重新进入受控的读取路径。等待阶段不能无限持有当前数据 handler、整帧预算、冷数据 pin、Raft 操作或旧 writer lease；应为等待者单独计数，按全局、租户和连接 / 成员限制，满额时明确拒绝或回退。还要覆盖 Gateway 到 owner、leader 的转发链，避免仅释放最终节点的槽。如何在现有 dispatcher 中安全交还并重新准入仍需设计。
2. **避免漏唤醒和只监听新消息。** 登记后重新核对条件 / 版本，避免消息恰好在检查与注册之间到达；通知只作为提示，唤醒后重新检查。除新写入，还需考虑 ack / nack、延迟重投到期、额度释放、分配变化、授权或 lease 到期、owner / leader 变更及关闭。必要的有限超时重查是兜底，但不能把超时当作确认已无数据。
3. **保留结果身份与 Unknown 语义。** 当前读取结果以成员 epoch、sequence 和请求摘要绑定，并在退休前可重放。若沿用现有协议，正常等待到期返回空结果时仍须完成读取提交，由 SDK 收到响应后再退休该结果，并为 Raft / 回复 / 退休预留原操作期限内的时间。等待发生在提交前不能证明已经提交；可能提交后发生取消、断线或超时则继续按原身份恢复，不能当成未执行而换新操作。任何取消轮转等待者的方案也要处理这个边界。
4. **防止多 shard 饥饿。** 现有最多 8 个并发读取任务和有限预取额度意味着：如果几个空闲 shard 长期占住读取任务 / 信用，其他活跃 shard 可能迟迟得不到读取机会。需要设计并验证轮转、有界等待、按目标共享的等待提示或其他公平机制；多 shard 聚合跨数据组时还须单独处理身份、权限和路由。不能仅增加等待时间而不解决调度。
5. **保证资源释放和控制面进展。** 等待结束、取消、断线、客户端 close、服务关闭、租户撤权、成员撤销、超时和故障切换均应释放等待登记及额度。心跳、ACK / NACK、结果退休、取消和 Raft 控制工作必须在等待者满载时仍能获得准入；重新交付前检查当前授权、成员 epoch、分配、额度和数据组 fence。不能把客户端取消视作撤销已提交操作。
6. **避免新增未经证明的配置。** 等待上限应小于原总 deadline 并考虑鉴权有效期、处理与回复预算；候选等待时长、调度与额度上限先用实验确定，再决定是否需要公开参数。与其默认某个 Kafka 或 SQS 数值，不如根据 TideMQ 的多 shard 和租户工作负载选择。

## 验证与决策

先固定待测提交、配置和环境，分别记录当前短轮询与候选 long poll 的结果；若要比较持续流式方案，单独保留同负载的原型结果，不用不同机器或不同容错等级的结果直接排名。

**正确性 / 故障矩阵：** Stream 与 Queue；无消息、突发、持续 backlog、冷热历史读取；超过 8 个 shard 且只有部分 shard 活跃；慢处理、未确认、持有消息克隆、大消息和预取耗尽；多个消费者与租户额度竞争；消息写入与等待注册竞态、重投时间到期、ACK / NACK 释放额度；注册后取消、到期与唤醒竞态、提交后回包丢失、连接中断、成员退出 / 重分配、权限变更、owner / leader 切换、进程重启和关闭排空。核对消息身份集合、允许的重复、缺口、顺序 / 累计确认、旧代次隔离、未决结果恢复，以及槽位 / 内存最终释放。

**性能 / 容量矩阵：** 按空闲消费者与 shard 数、请求速率、payload 大小、热点 / 稀疏分布、预取额度和网络延迟分层。至少记录消息从生产提交到业务 `receive` 的 p50 / p95 / p99、吞吐、空读率、`GroupRead` / `GroupRetireRead` 次数及数据组 Raft 写入、等待者数量与存活时间、handler / 取消表占用、客户端及服务端内存、CPU、网络、跨 shard 公平性、错误 / Unknown / 重试、心跳和 ACK 延迟。缺少的细分指标需要在实验中补充或通过已有遥测与追踪验证，不能把建议的指标当作当前已存在。

决策时先确认没有丢消息、越权、过期交付和资源泄漏，再用约定的延迟、吞吐、容量与成本目标比较；如果收益不足或等待开销不可接受，就保留当前方案并记录适用范围。当前没有运行上述测试，也没有测量结果。
