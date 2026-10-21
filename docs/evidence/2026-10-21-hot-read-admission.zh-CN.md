# HotRead 元数据内存准入修复证据

本轮修复真实的页面元数据少计费问题。基点为 `84ce35f6625a88fb21d0180c675039f54984b69c`，其中 Broker 产品代码与 `80c7659` 相同；修复范围只有 `Broker::hot_read` 的准入计算及 Broker 测试，没有改变预算、配置、正文存储或读取结果语义。

## 真实红绿验证

使用现有单节点 Raft / DiskStore Broker fixture 写入 1,024 条单字节消息，通过真实 `Broker::hot_read(max_messages=1024, max_bytes=1)` 读取，再同时观察返回页 `messages.capacity()` 与共享 `MemoryBudget::available_bytes()` 的变化。

| 阶段 | 返回条数 | Vec 容量 | 元素大小 | Vec 元数据占用 | 实际内存计费 | 终态 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 修复前 | 1 | 1,024 | 64 B | 65,536 B | 513 B | 预期断言失败，exit 101 |
| 最小修复后，同一用例 | 1 | 1,024 | 64 B | 65,536 B | 66,048 B | 1/1 PASS，exit 0 |

这是实际 Vec 容量与实际预算扣减的比较，不是对计费公式的模拟。`ProducerState::archive_read` 在按字节边界停止前已经预留 Vec 容量，因此结果仅有一条消息并不意味着只分配一个元素。

准入现在使用 checked arithmetic 计算 `max(max_bytes, max(max_messages, 4) * size_of::<MessageData>()) + 512`。页面中的 `Bytes` 仍共享已有热数据；保留原字节预算下限，并覆盖新增 Vec 元数据，没有将共享正文重复相加。准入仍由同一个 `HotRead` 持有的 `Admission` 管理，释放句柄后恢复额度。

## 边界与回归

- `max_messages=1/2/3/4` 的真实 Vec 容量均为 4，占用 256 B；每页计费 768 B。`max_messages=1024` 占用 65,536 B，计费 66,048 B。每次 drop 后 total/data/normal 三项额度均恢复。
- 默认四个不同分区各持有一页，参数为 `256 messages / 1,040,384 bytes`，实际合计计费 4,163,584 B，满足原 4 MiB read lane。第五页返回 `Capacity` 且不泄漏；释放一页后再次读取成功，最终额度全部恢复。
- 元数据乘法溢出、转换到 `u32` 溢出、附加固定开销溢出均返回 `InvalidRequest`，不扣减额度。read lane 容量拒绝及真实 MemoryBudget 拒绝均不泄漏操作、lane bytes 或内存许可，恢复条件后可再次读取。
- 新增 4 个边界测试全部通过；全部 9 个 Broker 回归测试通过（1.12 s）。`cargo fmt --all`、`cargo clippy -p tidemq --tests --no-deps --locked -- -D warnings`、`--all-targets` 严格 Clippy，以及 lib/bins 的 panic / unwrap / expect / indexing 等生产审计均为 exit 0。

运行环境为 macOS `aarch64-apple-darwin`、Rust 1.99.1 / LLVM 23.1.1，默认 features，`CARGO_BUILD_JOBS=2`，使用本 worktree 独立 target。具体命令、文件身份、原始日志 SHA-256 及计数见[小型原始摘要](hot-read-admission/20261021-native.json)。原始日志保留在被 Git 忽略的 `target/hot-read-admission-20261021/`，没有提交编译产物。

本证据仅覆盖这次修复的 native 准入行为和 Broker 回归。此前 `80c7659` 的 Linux / Docker / 覆盖率记录仍指向其原版本，原 RF3 性能 READ deadline 失败也完整保留；这些记录不替代本修复后的完整冻结版本验收，性能基线未晋升。
