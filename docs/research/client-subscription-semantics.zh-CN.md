# Scalable Topics 与客户端语义：一手来源核对

核对日期：2026-10-22。TideMQ 的唯一开发目标见[开发规格](../design/client-subscriptions-sdk.zh-CN.md)；本文件保留支撑该设计的外部事实，不定义候选接口或第二套实现方案。

## 1. Pulsar v5 消费与动态拓扑

| 外部事实 | 一手来源 |
| --- | --- |
| key 仍是可选消息属性；Scalable Topics 按 key hash 区间路由到 segment，布局由服务端维护和推送 | [v5 生产示例](https://pulsar.apache.org/docs/client-libraries/java-v5/#produce-messages)、[区间拓扑](https://pulsar.apache.org/docs/5.0.x/concepts-scalable-topics/#how-it-works) |
| segment 内 entry bucket 是消费分配单位，不等于物理分片；多个 Stream 成员可分摊 bucket，移交需要排空 | [消费并行度](https://pulsar.apache.org/docs/5.0.x/concepts-scalable-topics/#entry-buckets-and-consumer-parallelism) |
| Stream 有序并累计确认，Queue 无序并逐消息确认/NACK；Pulsar 还提供由应用管理位置的 Checkpoint | [消费模型](https://pulsar.apache.org/docs/5.0.x/concepts-scalable-topics/#consumer-model) |
| Stream 累计范围覆盖此前交给当前 consumer 的多个 segment/bucket；不能据此确认其他成员，应用先完成原前缀 | [Stream 确认范围](https://pulsar.apache.org/docs/client-libraries/java-v5/#stream-consumer) |
| Stream/Queue 依附具名订阅；创建起点用于首次创建，已有订阅恢复持久进度 | [v5 消费与起点](https://pulsar.apache.org/docs/client-libraries/java-v5/#consume-messages) |
| sealed predecessor 需要满足保留时间及已知订阅排空条件后才能删除；Checkpoint 不持有 durable backlog | [布局变化与保留](https://pulsar.apache.org/docs/5.0.x/concepts-scalable-topics/#retention-across-layout-changes) |

TideMQ 采用独立 Raft 数据权威、不可变 S3 历史绑定和自身资源生命周期协议。上述资料不证明 TideMQ 已支持动态拓扑，也不提供跨组权威转移或业务副作用 exactly-once 的现成保证。

## 2. 累计 ACK 的实现依据

固定源码/规范修订：3ad7e6e91b6add1b7a7518737a5142120cdafcd6。

- [客户端规范第 6 节](https://github.com/apache/pulsar/blob/3ad7e6e91b6add1b7a7518737a5142120cdafcd6/spec/scalable-topics/client-behavior.md#6-stream-cumulative-ack-position-vector)
- [ScalableStreamConsumer](https://github.com/apache/pulsar/blob/3ad7e6e91b6add1b7a7518737a5142120cdafcd6/pulsar-client-v5/src/main/java/org/apache/pulsar/client/impl/v5/ScalableStreamConsumer.java#L323)

Pulsar 为当前 consumer 的消息保存各 segment 投递位置的 position vector。acknowledgeCumulative 按向量展开，不只确认目标消息所在 segment。

| segment 分配方式 | 底层确认 |
| --- | --- |
| consumer 独占整个 segment | 可直接累计确认该 segment |
| 多个 consumer 按 bucket 分摊 | 底层复用 Key_Shared；ackSegmentUpTo 从 sharedSegmentUnacked 取当前 consumer 的已投递 ID，转换为逐消息 ACK |

因此 C1 收到 S:1/S:3、C2 收到 S:2/S:4 时，C1 累计 S:3 不确认 C2 的 S:2。业务接口的累计语义不要求使用物理位置前缀指令。

源码在接收队列入队时捕获向量。TideMQ 只借鉴“累计范围展开为合法子确认”的思路：在实际交付应用时分配成员 sequence，保存有界账本，固定原前缀并按 authority/帧预算拆批。消费上下文不进入稳定 MessageId，不复制其底层 v4 兼容实现；跨组部分成功按 TideMQ 原操作恢复。

## 3. Consumer key 选择的边界

标准 [StreamConsumerBuilder](https://pulsar.apache.org/api/client-v5/5.0.x/org/apache/pulsar/client/api/v5/StreamConsumerBuilder.html) 没有精确消息 key 订阅 setter。property/properties 设置 Consumer 元数据，namespace 的 propertyFilters 选择 Topic 属性，都不是消息 key 过滤。

按 key hash 进行自动分配与“只接收某个业务 key”是不同能力。TideMQ 首版将前者用于 Stream，不提供后者；如果后续实现过滤，需要独立定义订阅级规则、未匹配消息的保护/跳过、过滤变更、回放及精确索引，不能以成员本地丢弃代替路由。

## 4. Rust 异步接口依据

- [Future 的运行方式](https://doc.rust-lang.org/std/future/trait.Future.html#runtime-characteristics)：创建 Future 不等于已执行；TideMQ 的准入、持久成功和取消恢复须自行定义。
- [FuturesUnordered](https://docs.rs/futures-util/0.3.34/futures_util/stream/struct.FuturesUnordered.html)：push 后由后续轮询驱动，不替应用提供消息条数/字节上限。
- [FuturesOrdered](https://docs.rs/futures-util/0.3.34/futures_util/stream/struct.FuturesOrdered.html)：并发推进但按插入顺序输出，不证明消息写入顺序。

这些组合器可直接驱动普通 send Future，不要求 SDK 增加第二阶段回执、逐消息 task 或 Rust Stream/Sink 适配器。客户端内存、原身份恢复、业务 ACK 和持久提交边界均由 TideMQ 开发规格定义。
