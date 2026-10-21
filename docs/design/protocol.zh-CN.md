# 协议与有界解码

协议版本为 1，ALPN 为 tidemq/1。Protobuf 原文件在 crates/tidemq-protocol/proto，公共、内部和 common 分开；生成 Rust 文件随包提供，SDK 构建不需要 protoc。维护者运行：

```sh
cargo build -p tidemq-protocol --features codegen --locked
cargo fmt --all
git diff -- crates/tidemq-protocol/src/generated
```

生成器固定 Prost 0.14.4 和 protoc-bin-vendored 3.3.0。codegen 是显式维护 feature，默认构建只编译已提交的 Rust。

## Frame

每帧为 4 字节大端 u32 长度和对应 Protobuf Frame，包含版本、非零 16 字节 request_id/operation_id、连接流量类别、kind、traceparent、payload 和剩余期限毫秒。剩余期限为 1—300000ms；中间节点递减预算，不能在每跳重置总 deadline。

接收先按已绑定 connection 类别校验长度，再有限读取/释放传输 credit，不能先按不可信长度分配。Frame 顶层最多 32 个字段；payload 顶层默认最多 1100 字段，在 Prost 解码前检查，防止大量零长度 repeated message 放大分配。拒绝溢出/截断 varint、越界 length 和未使用的 group wire type。具体业务仍需检查批次条数、消息/解压大小及身份/代次，raw frame 限额不能代替解码预算。

同一连接类别不可更改，kind 也须匹配类别。内部 opaque command/Raft payload 由服务端按其业务操作再次校验；例如非空 AppendEntries 不能标成控制心跳。传输接收、QUIC ACK、finish/reset 不等于业务提交或撤销。

## 身份与错误

普通业务身份使用 `Id::new_v7() -> Result<Id>`：先通过 `getrandom::fill` 获取 10 字节随机输入，再交给 `uuid::Builder::from_unix_timestamp_millis` 编码，不自行实现 UUID 格式，不引入全局计数器、锁或时钟回拨等待。熵源失败、系统时间早于 Unix epoch、毫秒值超过 48 位均返回错误；不调用默认 RNG 失败时会 panic 的 `Uuid::new_v7` / `Uuid::now_v7`。`Id::random()` 保留为不携带时间的 UUID v4，供确需随机输入的用途使用。两种生成方式都保持 16 字节表示；解析仍拒绝 nil 与错误长度，Topic/producer/订阅等角色身份还需在对应权威状态中验证。

v7 只提供毫秒级时间排序：同一毫秒的随机尾部、跨节点时钟偏差与时钟回拨都不构成全局单调顺序。消息顺序和 fencing 仍由 offset、sequence、epoch 与 Raft 提交位置决定；业务重试复用原 operation ID，每次网络尝试才创建新的 request ID。重试抖动从随机尾部取值，不能使用相近时间创建的 ID 共有的时间前缀。这里选择无额外协调状态的实现以保持简单；未据此宣称生成吞吐或消息吞吐改善。

cluster_id、topic/producer/subscription/operation 身份使用 Id；node_id 与 group_id 使用非零 u64。group_id 在集群内分配且不可复用，metadata_group_id 单独保留，数据组不能占用它；磁盘与内部网络使用同一组身份，不另建 UUID 到 wire ID 的隐式映射。

错误使用固定 ErrorCode、可读 message、retryable 和 Outcome；明确 rejected、committed、unknown，SDK 不匹配错误文本决定重试。expected_sequence、expected_registration_epoch 和 retry_after_ms 是可选结构化提示，不能据此把未知结果改为拒绝。敏感 token/body 不进入日志或 instrument 参数。取消按 request_id 处理，不能为取消一个请求 RESET 整条复用流。

traceparent 只接受有界 W3C version 00；无效上下文在帧总额内丢弃，不改变业务身份。它不参与去重摘要、授权或持久状态机，不绑定共享 connection，不默认传任意 baggage。

生产会话通过 OpenProducer 显式注册，用单分区注册 epoch 高水位 CAS；过期后的旧 ProduceBatch 不创建新身份。共享 producer::canonical_produce_digest 以固定 domain、身份、epoch、起始序列、条数及逐条 key/body 长度和内容计算 BLAKE3；owner epoch、请求期限与 trace 不参与摘要。borrowed parts 接口避免服务端复制消息后再哈希。

共享 `producer::partition_for_key` 使用 BLAKE3(`tidemq/key-routing/v1\0` + 原 key 字节)，取摘要前 8 字节的大端 u64 对非零 Topic 分区数取模。SDK 直接导出该函数；Topic 分区数在首版固定，副本/owner 迁移不改变映射。空 key 也有确定映射；无 key 业务若要分散负载，应显式选择分区。不同语言必须复用此版本算法；97 分区下空 key、UTF-8 `customer-42`、二进制 `00ff` 分别为 10、65、59。不存在跨分区排序或自动拆分现有 Topic。

消费 ClaimAssignment 显式分配会话，替换已有会话需要 replace_session_id 与代次 CAS。assignment_generation 同时充当 delivery_generation，wire 两字段必须一致；seek、owner fence 和新分配使旧投递失效。DeliveryBatch 携带完整 topic/partition/subscription/session/cursor/assignment/delivery 绑定，issued_before 限制本代次连续签发范围，acknowledged_ranges 只表示持久 ACK 洞。SDK 必须按成功入队的消息和已验证洞区间连续推进 received_before；最大收到 offset、QUIC ACK 或业务 ACK 都不能代替冷热接收屏障。

schema 包含注册/状态/退役、分配/状态/seek/删除和按名称查询，均走 Control 类别；生产走 Produce，冷热消息分别走 Delivery/Bulk。运行时已经接入 TLS/mTLS、有界业务分派及真实 SDK。网络收包、收到正文、Producer 持久成功、业务持久 ACK 和对象发布分别保留其完成口径。

`message` 保留具体校验约束和类型化业务拒绝原因，并经内部节点转发到 SDK；SDK 的 `Error::Remote` 保留该文本和结构化重试提示。诊断文本不参与重试判断。内部 IO/存储故障对外提供失败阶段，完整错误链留给关联 request_id / operation_id 的日志。错误枚举的 `display` 只描述本层上下文，不拼接 `source`；需要完整链时用 `anyhow::Error::new(err)` 包装，再用 `"{err:#}"` 输出。详见[错误诊断约定](../operations/error-diagnostics.zh-CN.md)。
