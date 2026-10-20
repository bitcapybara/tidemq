# rkyv、FlatBuffers、FlexBuffers 的适用边界

调研日期：2026-10-20。用户提到的 `rykv` 按 `rkyv` 理解。本次仅调研格式和实现边界，没有替换生产序列化格式，也没有新增生产依赖。

## 结论

三者都值得了解，但不适合把 TideMQ 现有的 Protobuf、WAL、归档格式统一替换掉。优先消除现有共享缓冲区路径中的重复复制；如果仍有明确的解码瓶颈，再围绕具体数据结构验证候选格式。

- `rkyv` 更适合 Rust 内部可重建的只读索引或缓存；采用前必须设计归档视图、缓冲区所有权、对齐和版本边界。
- FlatBuffers 更适合需要多语言访问、固定 schema、按字段读取的大块不可变消息或索引；它的 schema 演进能力比直接归档 Rust 类型更贴近长期协议。
- FlexBuffers 更适合动态用户数据或扩展属性；其动态类型、转换默认值和 Rust 实现的校验边界，不适合成为控制面、Raft 或 WAL 的默认表示。

上述为结合库特性作出的工程判断，不是三种格式在 TideMQ 上的性能实测结论。

## 核实的版本与来源

版本和发布时间来自 crates.io API 的 `max_stable_version`、对应 `versions[].created_at`；没有用版本名称或 docs.rs 构建时间推断发布时间。

| 库 | 核实版本 | 发布时间 UTC | 上游 |
| --- | --- | --- | --- |
| rkyv | 0.8.18 | 2026-08-05 12:01:41 | [rkyv/rkyv](https://github.com/rkyv/rkyv) |
| flatbuffers | 25.12.19 | 2025-12-19 23:16:16 | [google/flatbuffers](https://github.com/google/flatbuffers) |
| flexbuffers | 25.12.19 | 2025-12-19 23:19:30 | [google/flatbuffers](https://github.com/google/flatbuffers) |

元数据：[rkyv](https://crates.io/api/v1/crates/rkyv)、[flatbuffers](https://crates.io/api/v1/crates/flatbuffers)、[flexbuffers](https://crates.io/api/v1/crates/flexbuffers)。API 查询需要正常的 User-Agent；某些浏览器入口返回不可访问，不代表包不存在。

## rkyv：直接读取归档类型

`rkyv` 把 Rust 类型转换为独立的 `Archived<T>` 表示，可以直接访问归档中的字符串、容器和相对指针，不必先重建整棵原生对象树。`access::<T, E>(&[u8]) -> Result<&T, E>` 是带 `bytecheck` 验证的安全入口；`from_bytes` 则会反序列化回普通类型，两种用法的分配成本不同。不能用 unchecked API 读取未经验证的网络或磁盘数据。[0.8.18 文档](https://docs.rs/rkyv/0.8.18/rkyv/)、[安全 access 签名](https://docs.rs/rkyv/0.8.18/rkyv/fn.access.html)

默认格式采用 little-endian、对齐的基本类型、32 位相对指针。格式控制 features 会改变数据表示；更改这些 features 可能让历史数据不可读。对齐模式下，普通 `Bytes` 的任意切片不能自动视为满足归档类型的地址对齐；必须保留正确的存储所有权并验证对齐，或显式选择并冻结 unaligned 格式。读取归档的生命周期不得超过承载整个归档的缓冲区。[格式控制与兼容性](https://docs.rs/rkyv/0.8.18/rkyv/#format-control)、[归档布局](https://rkyv.org/format.html)

官方兼容性条件要求 schema 未改变、格式控制配置未改变，并且库版本 semver 兼容。这不是 Protobuf/FlatBuffers 式的任意增量字段演进约定。若用于持久存储，仍需自己的格式版本、类型身份、升级读取器和迁移策略。[兼容性约定](https://docs.rs/rkyv/0.8.18/rkyv/#compatibility)

它主要面向 Rust 类型系统与派生归档类型；本次没有找到可直接替代 Protobuf/FlatBuffers 多语言 schema/codegen 流程的官方约定。因此跨语言 SDK 不应把 Rust 归档内存布局作为默认外部协议。官方的性能介绍说明设计目标，不能替代 TideMQ 的真实负载测试。[类型系统与 API](https://docs.rs/rkyv/0.8.18/rkyv/)

## FlatBuffers：schema 驱动的借用视图

FlatBuffers 的 Rust runtime 配合 `flatc --rust` 生成的代码工作。读取端通过偏移访问原始缓冲区，适合只读取部分字段；不需要为了访问一个字段先生成完整原生对象。构建端仍需构建 FlatBuffer、写入字符串和向量；不能据此宣称从应用数据到网络或磁盘全程无复制。应冻结 runtime、生成器和 schema 的组合，并以生成代码和兼容性测试验证。[Rust 官方指南](https://flatbuffers.dev/languages/rust/)、[25.12.19 runtime](https://docs.rs/flatbuffers/25.12.19/flatbuffers/)

`root` 和 `root_with_opts` 先验证，再返回借用视图。`VerifierOptions` 提供深度、table 数量和表观大小等边界；结构校验之外，应用仍要验证消息数、字段长度、身份和业务不变量。禁止为性能直接对未知输入调用 `root_unchecked`。[root](https://docs.rs/flatbuffers/25.12.19/flatbuffers/fn.root.html)、[VerifierOptions](https://docs.rs/flatbuffers/25.12.19/flatbuffers/struct.VerifierOptions.html)

table 支持受规则约束的前后兼容：新增字段通常放在末尾，或为全部字段指定稳定 ID；已有字段应 deprecated 而非删除；随意改变字段类型或默认值会破坏语义。struct 是固定布局，不提供同等的字段扩展能力。多语言能力属于 FlatBuffers 的主要适用条件，具体语言功能仍需按官方支持情况和实际 SDK 验证。[schema 演进](https://flatbuffers.dev/evolution/)、[struct/table 布局](https://flatbuffers.dev/internals/)、[官方语言入口](https://flatbuffers.dev/)

格式使用 little-endian 标量并定义内部对齐和偏移；这不等于可以把任意字节地址直接强转成任意 Rust 类型。借用视图、校验结果和缓冲区的不可变性必须一起保留。[格式布局](https://flatbuffers.dev/internals/)

## FlexBuffers：动态数据的延迟访问

FlexBuffers 是独立的无 schema 格式，可以单独使用或嵌入 FlatBuffer。它保存动态类型、键和位宽信息；官方说明其访问可以避免完整解析、复制和对象分配，同时明确提醒它通常比 schema 化的 FlatBuffers 更慢，应在确实需要动态结构时使用。[官方 FlexBuffers 说明](https://flatbuffers.dev/flexbuffers/)

Rust 的 `Reader<B>` 按需访问数据；默认 `B = &[u8]` 时，字符串和 blob 可借用原缓冲区。`Buffer` trait 允许自定义引用计数缓冲区，但需要自行满足切片、字符串有效性和所有权约定。通过 Serde 反序列化成带 `String`、`Vec` 的普通结构仍可能分配，不能把 `from_slice` 一概视作零分配读取。[Reader](https://docs.rs/flexbuffers/25.12.19/flexbuffers/struct.Reader.html)、[Buffer](https://docs.rs/flexbuffers/25.12.19/flexbuffers/trait.Buffer.html)

必须区分严格读取与宽松转换：`get_*` 返回 `Result`，`as_*` 会尝试类型转换，失败时可能返回默认值。控制协议不能把缺失、损坏、类型错误默认为合法的 `0`、空字符串或空集合。`get_slice<T>` 已被标注为因安全问题而弃用；不能用它承诺任意数值向量都能安全零复制映射。[Reader 方法与弃用说明](https://docs.rs/flexbuffers/25.12.19/flexbuffers/struct.Reader.html)

`Reader::get_root` 的实现读取根部尾缀和偏移，没有递归验证整棵树；部分读取函数仍包含边界敏感的直接索引。因此仅成功创建 root 或看到 `Result` 签名，不足以证明对恶意输入不会 panic。采用前需要针对实际读取路径做边界审计、畸形输入测试、资源限制和 fuzz；本次没有完成这些验证，也没有将它引入生产。[25.12.19 Reader 源码](https://docs.rs/flexbuffers/25.12.19/src/flexbuffers/reader/mod.rs.html#225-237)

无 schema 可以让动态键随数据变化，但不会自动提供应用级版本兼容、必填字段和类型约束。其官方说明提供 C++、Java 用例，Rust 有独立实现；不能由 FlatBuffers 支持某语言推断同语言的 FlexBuffers 功能完全一致。[官方用例](https://flatbuffers.dev/flexbuffers/)、[Rust 实现](https://docs.rs/flexbuffers/25.12.19/flexbuffers/)

## 稳定格式不等于规范化字节

FlatBuffers 官方明确允许相同输入由不同实现编码为不同的合法二进制，包括不同字段顺序和对象布局。rkyv 的确定性对象遍历布局也不等于所有容器和版本都有通用 canonical encoding；FlexBuffers 的动态位宽和共享策略同样不能当作规范化哈希契约。[FlatBuffers 非唯一编码](https://flatbuffers.dev/internals/)、[rkyv 布局](https://rkyv.org/format.html)、[FlexBuffers 编码选择](https://flatbuffers.dev/flexbuffers/)

因此，TideMQ 的内容摘要和恢复摘要需要独立且有版本的逻辑字段编码：定义字段顺序、整数和长度表示、集合顺序、可选值、浮点规则与域分隔。不能只因为更换了存储 codec 就直接更换摘要输入，也不能认为“可跨版本读取”意味着“字节哈希必然相同”。这是根据上述格式约定作出的设计要求。

## 与当前 TideMQ 的关系

以下为主 agent 对当前仓库的代码核实结果与采用建议：

1. Protobuf `Frame` 和嵌套 payload 已通过 `Bytes` 解码共享数据；重复复制主要发生在已编码 payload 再写入完整 Frame 的发送环节。本轮先做所有权受控的分段发送，保持既有线上字节格式。
2. 归档 `TIDEPK01` 已有固定二进制布局、`Arc<RecordBuffer>` 与经过检查的 key/body 偏移视图。替换成 FlatBuffers 不会自动消除原本就不存在的完整对象重建成本；只有 schema 演进、跨语言读取或真实热点收益成立时才值得设计新版本。
3. WAL 使用有界 Postcard 编码并通过 `DeserializeOwned` 恢复模型。rkyv 不是 Serde codec 的直接替代：需要独立的 Archived 读取路径、缓冲区生命周期、对齐、格式身份和升级边界。
4. 首个可考虑的 rkyv 原型应是可重建的本地只读索引/缓存；FlatBuffers 原型应围绕多语言、大块只读数据；FlexBuffers 保留给确需动态结构的用户 payload。原型结果应同时报告校验成本、分配字节、延迟、保留内存和兼容性，而非只报无校验的访问速度。

FlatBuffers 也可以先作为用户消息 body 的格式验证：应用从 `Delivery::body()` 取得字节切片，调用带验证的 root 入口取得借用视图，TideMQ 继续按 opaque bytes 传输与计费。这条路径不需要修改 MQ 协议或持久格式；应用仍负责 schema 身份、版本、验证边界和借用生命周期。它是采用建议，本轮没有增加这种应用层 codec 或完成端到端对照。

本轮未进行三种新格式的 TideMQ 基准对比、兼容迁移或第三方实现的完整安全审计；没有据此宣布替换 Postcard、Protobuf 或现有归档格式。
