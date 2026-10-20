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

修复测试前提并释放临时 runner 的未使用 Android SDK 后，[Rust CI 39371810742](https://github.com/bitcapybara/tidemq/actions/runs/39371810742) 在 `aaaf23497d2c32905e5084b6be1fe7b24a8e1b8b` 终态 success：Linux/macOS validate、System/mimalloc/jemalloc 三个 allocator、Linux/macOS heap-profile 共七个 job 全部通过。Linux workspace 日志确认 34 个 suite、276 passed、12 ignored、0 failed；释放空间后 `df` 显示 25 GiB 可用。没有降低生产磁盘安全阈值、关闭扩容测试或重试整个执行操作。该工作流不包含尚未接受的性能基线，不能由 Rust CI 通过推导性能门禁通过。

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

## 单次 fsync 诊断：不用作基线

`rf3-fsync-diagnostic-1` 保持上述 S 服务和 B 工具、相同二进制摘要及 4 CPU/8 GiB 限额。两个场景合计 2048 条测量消息通过完整持久 ACK，Unknown 为零，三个服务和 tracer 均正常退出。宿主已有 `strace 6.8`，只附加本轮三个服务 PID `633829/633849/633879`，对应容器 PID `57/70/83`；负载继续核验真实服务 PID `57` 和可执行文件，没有把 tracer PID 当作服务身份。跟踪只包含 `fsync/fdatasync`，单文件 `-f -tt -T`，31 MiB 文件硬限制、270 s 超时；未修改或跟踪业务容器。

吞吐阶段的跟踪从中途开始，不用于完整分布对照。完整延迟测量窗口为 2026-10-20 05:05:06.813—05:05:47.853 UTC，node 1 数据 WAL 的观测如下。

| 指标 | fsync 调用区间 | 异步 Sync 墙钟 |
| --- | ---: | ---: |
| 次数 | 5706 | 5707 |
| 平均 | 1.780 ms | 2.298 ms |
| p99 | 2.913 ms | ≤5 ms 桶上界 |
| >10 ms | 38 | 40 |
| >50 ms | 26 | 28 |
| >100 ms | 1 | 1 |

快照边界相差一个 Sync，没有逐请求配对，均值差约 0.52 ms 只能作近似观察。三个节点在 05:05:45.940 UTC 同时出现约 115 ms 的 fsync。大部分长尾已经出现在被跟踪的系统调用区间内；调用区间仍包含调度与 ptrace 扰动，不能据此认定块设备是唯一原因，也不能把全部均值差归为 blocking pool 排队。

证据位于同一远端根目录的 `rf3-fsync-diagnostic-1-trace/`：`sync.strace` 为 3,618,772 B，`analyze.mjs` 为 7,219 B，`summary.json` 为 6,274 B，整个 trace 目录为 4,059,309 B。业务报告、正常退出记录位于 `rf3-fsync-diagnostic-1/`。这些数值只用于定位同步阶段，不计入 A/A 或回归比较。

## 下一轮固定策略

`f86bae0` 将 policy/summary 升级到 schema 4，固定八对、十项比较的 Bonferroni 区间、三个主指标 0.15 log 半宽预算；p99 继续参与显著回退否决，但不作窄精度放行证明。十九项 Node 测试通过，包括同二进制显著改善的时序偏差拒绝、宽 p99 的明确诊断状态和主指标噪声拒绝。详细检测边界见[性能手册](../performance.zh-CN.md)。这项工具验证没有把旧报告晋升为新基线，新八对 cohort 必须完整重新执行。

`5a7db0f35632457dd0d62f0efbec039b3f3d9ba4` 已推送 main，启动 [Performance 39373827288](https://github.com/bitcapybara/tidemq/actions/runs/39373827288) 及新 [Rust 39373827309](https://github.com/bitcapybara/tidemq/actions/runs/39373827309)。虽然该提交与此前七项 CI 全绿的 `aaaf234` 之间没有 Rust 源码变化，新一轮 mimalloc job 仍失败：combined 用例的 `recv` 返回 Deadline，分角色 RF3 用例首先收到 offset 5 而断言期待 0。两项均需独立定位，不能归为之前的磁盘容量问题，也不能把先前通过写成当前全部 CI 通过。

当前性能和开发服务器的 `corrected-aa-2` 八对 cohort 保持原输入完整执行，不挑选试次。此文档提交会使 main 前进；已有晋升步骤的 exact-HEAD 检查因此跳过 `5a7db0f`，避免独立 Rust 工作流失败时自动晋升。该跳过不代表数值基线已经更新。后续晋升需要同一提交的 main Rust 工作流成功，并保留失败 runtime 进程日志制品。

## 八对同二进制 A/A 终态

`corrected-aa-2` 在固定 schema 4、原 0.15 主指标预算下完整执行；RF1/RF3 各十六个试次、合计 65,536 条测量消息全部完成持久语义校验并正常退出，预热另外计数。真实服务仍为源码 `3b75d9c`、SHA-256 `beab7144cb51a40f0d804381f6ef9b8f94fcb54b271597c702fff497771ccf04`；工具为 `5a7db0f`，负载 SHA-256 为 `955e64f72b916ccc753bebbd5cb7cf3e1cf3e4b3b350ad1b97109856c1f9af5e`。两侧实际复用同一服务及负载构建。

完整数值和全部八对样本保存在[校准报告](../../performance/calibrations/2026-10-20-dev-aa-schema4.json)，文件原样取自远端 summary；主 agent 核对了 schema、相同构建、源码、policy 摘要，并使用当前未修改策略重新计算两拓扑及各指标状态。该文件不是 `baseline-report.json`，也不是晋升记录。

| 拓扑 | 持久吞吐 H | 生产原始均值 H | offered→持久 ACK 均值 H | 拓扑终态 |
| --- | ---: | ---: | ---: | --- |
| RF1 | 0.016868 | 0.154613 | 0.085362 | inconclusive |
| RF3 | 0.111292 | 0.105219 | 0.074054 | passed |

H 为十项比较 Bonferroni 校正区间的 log 半宽。RF1 的生产均值超过预设 0.15，不能四舍五入放行；其区间为 [0.835296, 1.137982]，没有显著退化证据，但精度不合格。四个 p99 全为 `imprecise`，H 为 0.936850、0.768403、0.773199、0.511939，不能宣称小幅尾延迟退化已被排除。总终态为 `inconclusive`、进程退出 1，未接受或晋升数值基线。没有删除试次、追加样本或提高阈值让本轮通过。成功语义试次的数据已按脚本清理，原始报告、指标与日志保留；WAL 保留规则针对业务校验失败的试次。

## 首轮 GitHub 八对比较终态

[Performance 39373827288](https://github.com/bitcapybara/tidemq/actions/runs/39373827288) 完成全部 32 个试次的持久语义校验，统计终态为 `inconclusive`，工作流失败，晋升步骤跳过。基线源码为 `3b75d9c`，候选源码为 `5a7db0f`；两侧实际服务 SHA-256 均为 `7e3e6d47ec192289a83fe0253712aaa0ab3872eebb7f933dc0993ff0e78788ea`，负载均为 `a67dc8895bcd79f15d2618974e797e6bbb3eccd13d306e92345f2fe9b1d82f7d`。源码身份不同，报告正确保留 `same_source_reused_builds=false`，不能改写为同源码复用构建。

[原始 summary](../../performance/calibrations/2026-10-20-github-aa-schema4.json) 原样取自 artifact `12244321353`；下载 ZIP 的 SHA-256 为 `c90735b22690cce4f4291bc66e9f2b16007143050534e52d026e6461e5ba2f6f`。主 agent 按当前未修改的策略重算两拓扑及所有指标状态，结果与报告一致。

| 拓扑 | 持久吞吐 H | 生产原始均值 H | offered→持久 ACK 均值 H | 拓扑终态 |
| --- | ---: | ---: | ---: | --- |
| RF1 | 0.014842 | 1.216972 | 0.755020 | inconclusive |
| RF3 | 0.023801 | 0.060118 | 0.015120 | passed |

RF1 首个 baseline 延迟试次的生产均值为 67.23 ms、计划发送迟到均值为 59.64 ms，生产 p99 约 1.61 s；其余十五个 RF1 试次的生产均值约为 6.00—7.47 ms。该试次 Sync 有七个超过 50 ms、六个超过 100 ms、一个超过 500 ms，WalQueue 没有超过 50 ms 的样本。观测支持一次长停顿引发计划发送积压，尚不能唯一归因于构建、宿主调度或磁盘。没有删除首个试次、扣除调度迟到或修改门槛。

`b298ccc` 将固定构建与测量拆为不同 GitHub VM：测量前核对外部 manifest 摘要、干净源码、策略、SDK、lockfile、harness 和四个实际二进制，再恢复固定文件的执行权限。测量不运行 Cargo，首个试次前固定执行一次有界 sync；两个版本仍在同一测量 VM 交错运行。三十项 Node 测试及 YAML/shell 语法检查通过，包括损坏、缺失、symlink 和同源码伪复用拒绝。该环境隔离是下一轮控制条件，不能当作已证明旧长尾根因或新性能通过。

## CI 正确性失败与定向修复

`1ebc66d` 修正 Parallel 测试的全局首次交付顺序假设。真实运行时在第二条 RecordDelivery 持久提交后丢弃 Read 回复，旧断言稳定收到 offset 2 而期待 0；正确契约为 offset 2 可以先交付，ACK 前沿仍为 0、保留保护仍覆盖 0，未确认的 0/1 在默认租约到期后待重投。测试改为验证身份全集、正文和连续 ACK 前沿。该定向证据不唯一证明旧 CI 的具体丢包时机。

`74a25d0` 在映射 wire 错误前增加静态 runtime Unknown 原因日志，与 dispatch 共用每秒限频；字节级测试确认错误回复未改变。`549095b` 为 combined 测试按默认 30 s 投递租约加一次 Read/ACK 的原有预算安排恢复窗口，没有修改生产超时或租约。合流 `edffba4` 的本地 root lib 为 90 passed/2 ignored，runtime 为 11 passed/3 ignored；fmt、测试 Clippy 与生产 panic/索引审计通过。对应日志为 `/tmp/tidemq-final-unknown-runtime-tests-20261020.log`、`/tmp/tidemq-final-unknown-clippy-20261020.log` 和 `/tmp/tidemq-final-unknown-panic-audit-20261020.log`。

[Rust 39377051991](https://github.com/bitcapybara/tidemq/actions/runs/39377051991) 在更早的 `fb12dc1` 终态失败：System 的两个创建 fixture 返回 `ContentConflict`，已完成副本替换的重放返回 Unknown；Linux validate 的 combined 读取返回 `Unavailable / Rejected`。mimalloc、macOS validate 和两个 heap-profile job 通过，jemalloc 取消，不能称为全绿。失败子进程的 stdout/stderr 日志制品已成功保留；嵌入式 `RunningNode` 的副本迁移测试没有独立 process log，不能从其他 fixture 日志推定其内部相位。

`7f80775` 定向复现并修复元数据创建重放的 CAS 问题：首次成功后，服务器为同一 operation 重新取当前 revision，导致完整 Command 摘要变化。现在仅对已 Applied 的原结果以受检查的 `revision - 1` 还原原 CAS，再走既有 digest 与 CAS 校验。两项真实创建重放先红为 `ContentConflict`，修复后三项运行时回归通过；五项 metadata state 测试通过，改变内容仍拒绝，已缓存拒绝不转为成功。原 System combined 和 broker failover 分别通过 13.63 s 与 19.59 s 的专项检查；旧 CI 首次回复究竟丢失还是提交后建路由失败，仍没有唯一现场证明。

旧 docs-only main 的 [Rust 39375744150](https://github.com/bitcapybara/tidemq/actions/runs/39375744150) 也失败：System 的创建返回相同 `ContentConflict`；macOS 的稳定 UDP 测试在首次读取时依次出现已发送 RPC Deadline、空批次及总 Deadline，20 s 后终止。现场没有保留嵌入节点日志，不能唯一确认该次持久签发状态；不过测试的 20 s 总期限确实短于默认 30 s 投递租约，与上述丢 Read 回复后的正确契约冲突。

共享 `durable_consume` 测试 helper 现在使用默认投递租约加原 20 s 恢复预算，保留每次读取的原 2 s 范围；默认 Parallel 验证期望 offset 全集和逐条正文，每次持久 ACK 都核对当前连续前沿。RF3、稳定 UDP、Broker 故障与冷读调用方一并更新。生产代码、SDK 超时和投递租约未改。合流元数据修复后的 runtime queue 为 11 passed/3 ignored，0 failed，116.27 s；测试 Clippy 与 diff 检查通过，日志为 `/tmp/tidemq-recovery-fixture-tests-20261020.log`、`/tmp/tidemq-recovery-fixture-clippy-20261020.log`。这不代表其他尚在定位的 CI 拒绝已经解决。

## 后续结果边界和完成重放修复

`56e8743` 的已完成迁移重放改为双重线性 metadata 确认与真实 data 稳定 quorum 证明，保持原三秒有效期；pending 仍走原变更阶段。定向并发变更锁竞争先红后绿，原计划、generation、target、退休、证明丢失和证明后代次变化均有回归，详见[专项证据](2026-10-20-completed-membership-replay.zh-CN.md)。root 合流后的 lib 94 passed/2 ignored、metadata state 5 passed、scaling remote 2 passed，tests/examples Clippy 与严格生产审计通过；这些检查尚未包含下一段新增 Read 修复。

`8e868d8` 修复整个 Read 的后续错误分类。真实运行时先持久签发 offset 0，再让 offset 1 的只读 hot read 单次返回 Draining，旧实现误报 Rejected，SDK 合法自动重试后返回 `Ok(2)`，隐藏 offset 0；红例 1.58 s。修复后的相同测试 1.51 s 返回整批 Unknown，SDK 缓冲为空；原 ACK、cold/issued 与保留前沿均不跨过 0，高 offset 可依 Parallel 规则交付，原 0 在默认租约后待重投。相关模块 6 passed/2 RustFS ignored，8.39 s，测试 Clippy 与严格生产审计通过。后续所有 `?`、session 和最终回复都通过同一结果边界保护，已有 Unknown 和已验证部分成功不变。旧 CI 缺少具体 Read 相位，仍不能宣称其每次故障都由该漏洞引起。

combined 端到端 fixture 在原 50 s 总窗口内显式恢复 Read Unknown 和可重试 Unavailable/Fenced Rejected，记录次数；不可重试、权限/协议错误、生产与 ACK 失败仍失败。四个队列各 64 条的 key、正文与 offset 身份、每批连续 ACK 前沿和最终 64 前沿全部核验。没有修改 SDK 重试、生产准入、Raft 退避次数或租约，也没有将失败性能试次改写为成功。

`30bf7e2` 纠正 A/A 身份判断：测量和晋升都依据两侧实际服务及负载 SHA-256，而不是源码提交是否相等或是否复用构建。不同源码若产生相同二进制，也必须否决统计显著的双向时序偏差；仅一种二进制相同仍按 A/B 处理。31 项 Node 测试通过，旧两份原始 summary 在此实际身份规则下重新计算，所有指标和拓扑状态仍与原报告相同，保持 inconclusive。

## 最新修复源码的本地终态

生产修复与消费 fixture 冻结为 `56a83498ae4ad31ff122504b2f865a7caf7c1126`。最终 workspace 普通及文档测试为 34 个 suite、284 passed、12 ignored、0 failed，其中 runtime queue 11 passed/3 ignored、82.22 s。workspace tests/examples Clippy、最后变动 runtime fixture 的 Clippy、严格 production lib/bins panic/索引审计和 fmt 均通过；31 项 Node 测试通过。日志分别为 `/tmp/tidemq-final-read-contract-workspace-tests-20261020.log`、`/tmp/tidemq-final-read-contract-clippy-20261020.log`、`/tmp/tidemq-final-combined-contract-clippy-20261020.log`、`/tmp/tidemq-final-read-contract-panic-audit-20261020.log` 与 `/tmp/tidemq-performance-identity-tests-20261020.log`。

由于尚未接受任何数值基线，新一轮初始源码引用更新为上述包含全部正确性修复的冻结提交；原 `3b75d9c` 不再作为未修复漏洞的起点。该更新没有修改统计策略或重标旧报告。下一轮在构建和测量分离的 VM 上完整执行固定 32 个试次，对应 main Rust CI 必须成功；运行开始前仍没有 `baseline-report.json`，本地正确性通过不等于性能通过。
