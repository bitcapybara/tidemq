# 请求取消、共享缓冲区与性能门禁验证

日期：2026-10-20。以下记录区分已完成的检查、失败实验和后续验收，不将运行中的命令计为通过。

## 实现范围

- Rust SDK 默认从 `ClientConfig.request_timeout_ms` 为一次逻辑调用建立总期限；移除业务调用末尾的 `client.deadline()`。需要跨调用的总预算或取消时，使用 `RequestContext::scope` 与 `tokio_util::sync::CancellationToken`，嵌套调用保留最早期限和父级取消。
- 取消发生在可能提交之后时，生产保留原 `PendingBatch` 并返回 Unknown；取消不证明服务端未执行。主动关闭独立清理 owned I/O。
- 大块、唯一所有的编码 payload 可以通过共享 `Bytes` 分段发送；保留容量计费与最后引用的许可生命周期。小消息、共享/静态存储或无法计费的所有权继续使用有界连续编码。线上和磁盘格式未变。
- [rkyv、FlatBuffers、FlexBuffers 评估](../research/zero-copy-formats.zh-CN.md)依据 crates.io、版本文档和官方格式说明完成；没有新增生产格式或依赖。

## 已完成的本地检查

冻结集成源码 `5393238479ccf6d878e726556c9d6b35e9340971`，macOS arm64、Rust 1.96.0：

| 检查 | 终态结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| workspace tests/examples Clippy，`--no-deps --locked -- -D warnings` | 通过 |
| production lib/bins 的 panic、unwrap、expect、未实现及索引 Clippy 审计 | 通过 |
| `cargo test --workspace --locked -j 2 -- --test-threads=1` | 268 passed，12 ignored，0 failed；含 34 个普通/文档 suite |
| `cargo test -p tidemq-sdk --all-features --locked -j 2 -- --test-threads=1` | 7 个 unit、29 个真实 UDP/TLS integration、2 个文档测试通过 |
| Node 性能工具测试 | 8 项通过，包括已知回退、噪声、缺失报告、持久前沿、合并直方图、HTTP 总期限与过期晋升拒绝 |

SDK 和 runtime 的专项检查与 workspace 有重叠，不能相加为不同测试总数。ignored 中有需要 RustFS/Collector 的既有测试与有限计时探针；本轮没有重新运行全部外部服务测试。真实 RF3 进程运行于同一宿主，不代表三个可用区。

有限 release codec 探针每项 4096 次，连续编码与分段编码耗时分别为：128 B：0.365/0.653 ms；16 KiB：12.803/0.648 ms；256 KiB：19.308/0.625 ms。该探针解释小消息保留原路径的选择；256 KiB 项减少约 1 GiB 逻辑 payload 复制。它不是服务端到端吞吐或 TLS、内核全程零复制证据。

## Linux Docker 首轮失败实验

在用户指定的 `ssh dev` 上，独立目录 `/root/tidemq-performance/20261020-0257`，以现有 Rust 1.96 builder 镜像启动无宿主端口容器，UID 10001，CPU 上限 4，内存上限 8 GiB，PID 上限 2048。只操作本轮拥有的容器、目录与进程，没有停止既有业务容器。基线服务源码为 `9fe9c952276d2a2b7b27a41ca1021926aaa3c283`，候选源码为 `5393238479ccf6d878e726556c9d6b35e9340971`，服务与各自 SDK 都使用实际 release 构建和默认 mimalloc。

`comparison-1/summary.json` 的终态为 failed，没有更新数值基线。

- RF1 五对交错试次都通过完整交付与持久 ACK 校验，吞吐约 57–60 条/秒。吞吐 log ratio 的 95% CI 半宽约 0.03039，生产 p99 约 0.40694，端到端 p99 约 0.12069，超过原 0.03 精度要求，结论为 inconclusive。没有把不能判别的结果计为性能通过。
- 随后的首个 RF3 基线试次在预热阶段失败：四个独立队列各提交 64 条，持久 ACK 分别为 8、8、0、0，返回 `Unavailable / Rejected`。节点日志出现领导权确认失败和 `Raft response does not match its request`。
- 对候选源码另行运行 `rf3-candidate-smoke-1` 同样失败：预热四个队列均提交 64 条，持久 ACK 为 0、48、0、0。该问题同时影响历史基线和候选，不能以替换基线跳过正确性要求。

所有失败试次的原始报告、服务日志、metrics/diagnostics 与 WAL 留在独立输出目录。失败记录包含运行凭据和数据路径，不能直接提交完整目录或公开分享。后续实验必须使用新目录，不覆盖这轮记录。

## 固定速率探针：尚未形成有效校准

首次固定速率场景仍为四个分区，每分区 64 条预热、256 条测量、160 ms 间隔，四分区错开，合计 25 条/秒。服务二进制与 SDK 源码保持 `5393238`；新负载来自 `50c87cf`，release 构建 43.48 s，负载 SHA-256 为 `d0b14d4b0b1bb9ce7f8ac9b8af34706b71f0211a5dba85bbbedc2df080f2fde7`。它是场景探针，不是新旧源码性能比较。

| 输出目录 | 持久消息 | 生产 p99 上界 | 调度延误 p99 上界 | offered→持久 ACK p99 上界 |
| --- | ---: | ---: | ---: | ---: |
| `rf1-paced-smoke-1` | 1024 | 71.303167 ms | 1.605631 ms | 146.800639 ms |
| `rf1-paced-smoke-2` | 1024 | 29.360127 ms | 1.835007 ms | 134.217727 ms |

第三次 `rf1-paced-smoke-3` 失败，后续重复试次停止：预热四分区均持久 ACK 64 条；测量四分区均提交 256 条，持久 ACK 为 256、62、256、256，一个分区报 `Unavailable / Unknown`。没有丢弃失败样本后计算通过的 A/A 区间。已保留的服务指标显示 Control/Unknown 增加一次，Read/Unknown、Ack/Unknown 和 transport deadline 均未增加；具体原因需独立定位。

代码检查另外发现，四组随机 topic/producer/subscription 身份经 affinity 哈希后，每次可能形成不同的 request-lane 碰撞。它是待控制变量，不是已证明的尾延迟根因。最终延迟场景改为一个分区、256 条预热、1024 条测量、40 ms 计划间隔，同样 25 条/秒；吞吐场景仍为四分区。上述两个成功探针和第三次失败都属于旧四分区场景，不能作为新场景或正确性修复的通过证据。

## Raft 控制准入错误修复

使用相同的真实三 combined 进程、四 topic 各 64 条并发生产/消费与元数据线性读取，原实现 4.73 s 失败；仅解析合法 ErrorReply 后 8.50 s 仍因 quota 拒绝失败。定向探针确认，错误回复的类别、request_id、operation_id 全部正确，只有 kind 是合法 `Error`，实际错误为 `QuotaExceeded / Rejected / retryable`。原代码把该回复当作关联损坏，关闭所有组共享的 Core RPC，造成级联。

`639be66` 修复错误分类并加入严格限定的退避，`384ccef` 只调整英文错误名。没有提高服务器控制并发额度。真实四队列回归转绿（14.43 s），原分角色 RF3 用例也通过（3.37 s）；各 64 条唯一交付和持久 ACK 前沿全部验证。

四个真实认证 QUIC peer 边界测试覆盖：合法拒绝保留共享连接；只有完整关联、明确未执行的可重试 quota 拒绝才以原身份、原 payload owner 和原硬截止重试；剩余期限递减；Unknown/非 quota/不可重试只发送一次；非法外层/内层身份或非法错误枚举继续拒绝。另有三个既有 Raft QUIC 认证、隔离和恢复测试、格式、Clippy 与生产 panic/索引审计通过。

## 接收屏障 Unknown 的恢复

原 RF1 报告无法唯一证明失败发生于哪个 Control 命令。代码路径显示，每次接收位置前进后，SDK 在后续读取之前发送 `ReceiveBarrier`；其 Unknown 会直接传播为该次读取失败。原形态的两次独立进程复跑通过，说明它是间歇性症状；不能把未复现宣称修复。

`6272842` 使用真实 UDP/TLS 协议 peer 加入确定性故障：屏障在提交前或提交后返回 `Unavailable / Unknown`，另验证持续 Unknown 耗尽重试。原代码测试失败，修复后两个测试通过（0.39 s）。SDK 仅在同一代次、session、接收位置和 operation ID 下有限重试该幂等屏障，各次 wire attempt 使用新 request_id；确认之前不发起热读取，原期限或重试预算耗尽仍保留原 Unknown。

该修改的 SDK 普通测试 37 项及两项文档测试、Clippy 和生产 panic/索引审计通过。它证明屏障丢响应路径已补齐，不单独证明旧 RF1 制品的唯一根因。负载另为分配、receive、buffered dequeue、持久 ACK 和最终前沿查询增加固定英文阶段上下文，保留完整错误链；没有改变负载或 ACK/Unknown 语义。

合流后的 root package 普通测试 86 项、真实 Raft QUIC 3 项、runtime queue 11 项通过；5 项 ignored 未运行。runtime queue 包括新的三 combined 四队列和原有分角色、强杀与恢复，不把其单机进程总用时当作三区恢复分位数。

## CI 验证边界

[Rust CI 39362732099](https://github.com/bitcapybara/tidemq/actions/runs/39362732099) 首次 Ubuntu validate 因 crates.io `socket2` 索引超时失败；allocator-system、allocator-mimalloc、allocator-jemalloc 的 Linux 检查与完整 root package 测试，以及 Linux/macOS heap-profile job 均通过。

仅重跑失败 job 的 attempt 2 中，macOS 的真实分角色 RF3 runtime case 在 `recv` 返回 `Unavailable / Rejected`，9 项 runtime 通过、1 项失败；Ubuntu validate 被矩阵 fail-fast 取消。没有把这些 job 计为通过。后续修复需重新取得终态 CI 证据。

GitHub 当前私有仓库的分支保护 API 返回 403，要求升级套餐或公开仓库；自动性能检查可以失败，但当前不能通过平台设置强制禁止合并。没有变更套餐、仓库可见性或现有权限。

## 修复合流后的终态本地验证

生产源码冻结于 `3b75d9c301229d8c40652e0ac51957a7b5e3fa54`。随后 `b2dde4b` 只固定校准源码和修改 JavaScript 校准工具，服务、SDK、crates、Cargo.toml 和 Cargo.lock 没有变化。

- workspace 普通及文档测试共 276 passed、12 ignored，34 个 suite，0 failed，终态日志为 `/tmp/tidemq-final-workspace-tests-20261020.log`。
- SDK all-features 为 7 个 unit、31 个真实 UDP/TLS integration、2 个文档测试，0 failed，终态日志为 `/tmp/tidemq-final-sdk-all-features-20261020.log`。这与 workspace 有重叠，不相加。
- workspace tests/examples Clippy `-D warnings`、production lib/bins 的 panic/unwrap/expect/未实现/直接索引审计、格式检查均通过。
- `f56b48f` 与 `1d50fed` 只修正扩容测试的磁盘准入和元数据 quorum 就绪前提。合流后 root crate Clippy 通过，runtime queue 11 passed/3 ignored、scaling Raft 3 passed、scaling remote 2 passed，0 failed。日志为 `/tmp/tidemq-ci-fixtures-clippy-20261020.log` 与 `/tmp/tidemq-ci-fixtures-tests-20261020.log`。磁盘证据和回放见[专项记录](2026-10-20-linux-scaling-capacity.zh-CN.md)。
- 八对比较工具 `1d3bfa2` 的 16 项 Node 测试通过；包括原始逐消息耗时均值、保留尾样本、旧报告拒绝晋升以及每项指标稳定退化 1% 的拒绝。这不是实际性能通过证据。

## 修复后的完整 A/A：语义通过，统计未通过

`corrected-aa-1` 使用同一源码 `3b75d9c301229d8c40652e0ac51957a7b5e3fa54`，工具源码为 `b2dde4bf1351b049320a7da52580d791e0e4a5ca`，保留原五对策略。`same_source_reused_builds=true`：两侧实际复用同一个已校验的服务二进制和负载二进制，不是分别构建后假定相同。

| 身份 | SHA-256 |
| --- | --- |
| 服务二进制 | `beab7144cb51a40f0d804381f6ef9b8f94fcb54b271597c702fff497771ccf04` |
| 负载二进制 | `341c6dfdfdf070d6c81daf26c3242304708ce931bd4d6f5dcbaecf1b4cc02719` |
| Cargo.lock | `4ac8dfd7ef50eec37d7be7e1703bbc55b093352d96ff140193831cb235eeb175` |

RF1 和三 combined 节点各完成十个真实试次。每个试次包含四分区吞吐场景和单分区固定速率场景，各 1024 条测量消息，全部通过唯一交付、持久 ACK、前沿和正常退出校验；总计 40,960 条测量消息，另有独立预热。容器继续保持 4 CPU、8 GiB 和 PID 2048 上限，没有停止既有业务容器。

| 拓扑 | 指标 | log CI 半宽 | slowdown ratio 的 95% CI |
| --- | --- | ---: | --- |
| RF1 | 完整持久吞吐 | 0.04304 | [0.94076, 1.02534] |
| RF1 | 生产 p99 | 0.22880 | [0.84541, 1.33598] |
| RF1 | offered→持久 ACK p99 | 0.11958 | [0.88318, 1.12181] |
| RF3 | 完整持久吞吐 | 0.07463 | [0.95002, 1.10296] |
| RF3 | 生产 p99 | 0.68271 | [0.55394, 2.17000] |
| RF3 | offered→持久 ACK p99 | 0.38708 | [0.73204, 1.58763] |

这里的 slowdown ratio 对吞吐使用倒数，使大于 1 始终表示更慢。六项区间都包含 1，但半宽均超过原 0.03 上限，因此最终进程返回 1，状态为 `inconclusive`。没有晋升 `baseline-report.json`，也没有删除低尾或高尾试次后重算通过结果。原始目录为 `/root/tidemq-performance/20261020-0257/corrected-aa-1`，运行日志为相邻 `corrected-aa-1.log`。

RF3 的 `4-baseline` 测量阶段有 7200 个 Produce/Sync 样本，没有超过 10 ms 的样本；`4-candidate` 有 6976 个样本，其中 25 个超过 10 ms、14 个超过 50 ms。WalQueue 在这些轮次没有超过 10 ms 的样本。`Sync` 计时覆盖 `sync_all().await`，包含 blocking pool 调度、内核同步和任务唤醒，不能仅凭该指标确定物理磁盘为唯一原因。十个 RF3 试次的数据组均由 node 1、term 1 领导；当前没有依据归因于不同 leader。

后续策略必须基于完整校准和实际判别能力；不能把此轮五对失败报告改写成八对通过，也不能将同机校准称为三可用区证据。最终数值基线与新 CI 终态将在实际完成后记录。
