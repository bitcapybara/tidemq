# TideMQ 性能门禁、负载与实测证据

本工具使用仓库中的 Rust SDK，经真实 UDP/TLS/QUIC 连接独立 `tidemq serve` 操作系统进程。生产成功以服务端持久提交回执为准；消费完成以逐消息 token 的持久 ACK 为准。运行工具不代替 RF3、故障恢复和对象生命周期正确性测试。

已完成的本地执行见 [2026-10-20 实测报告](evidence/2026-10-20-local-performance.zh-CN.md)。该轮 17 项均通过有限负载校验；结果尚未证明高吞吐、低延迟目标达成，也不用于选择生产 allocator。

## CI 单节点与三节点回归门禁

[Performance 工作流](../.github/workflows/performance.yml) 在隔离的 Ubuntu 24.04 runner 上运行真实 QUIC/TLS、Raft 与同步持久化，不关闭生产或逐消息 ACK 的持久性。`rf1` 是一个完整角色进程；`rf3-combined` 是三个完整角色进程，元数据和数据 voter 均为 `[1,2,3]`。原有角色分离的 `rf3` 十进程部署保持原样，不能把它称作三进程基准。同机 loopback 与共享磁盘测试用于代码回归，不代表三可用区性能或故障隔离。

仓库 [baseline.json](../performance/baseline.json) 固定上次接受的服务源码、负载和统计策略，实际数值、构建身份与原始试次由 `baseline-report.json` 保存。PR 使用 base commit 的工具和策略，不接受候选提交降低门槛或替换基线。基线与候选在同一测量 job、同一机器上各测八次，顺序交替为 A/B、B/A，各占四对；不拿不同 GitHub runner 的绝对吞吐直接比较。每次从新目录启动并预热，服务使用 mimalloc 与四个 Tokio worker。两个拓扑共 32 次完整 trial。

CI 将构建和测量拆到独立的 Ubuntu 24.04 runner：构建 job 最多 45 分钟，生成两个服务二进制、各自链接 SDK 的两个负载二进制、原始 identity 与构建日志；测量 job 最多 55 分钟，下载固定产物并核验后运行。每次构建前执行 `cargo clean --workspace --release --locked`，只清理 workspace 包，保留外部依赖缓存，避免不同 checkout 的旧文件时间戳让 Cargo 复用另一版本的可执行文件。真实双 workspace 回归交替构建四次，同时验证输出身份正确及外部依赖未重编译。manifest 通过 job output 提供外部 SHA-256，并绑定受保护策略、工具提交、两版源码、Cargo.lock、SDK、负载文件与实际二进制摘要。测量阶段重新核对干净 checkout，拒绝缺失、损坏或符号链接产物，全部校验通过后仅对四个固定二进制恢复执行权限。构建与测量的实际主机、内核、runner image 和 job 分别记录，构建来源不会重标成测量环境。

测量 job 不编译、不恢复 Rust 构建缓存，在首个 trial 前执行一次最多 60 秒的固定 `sync`，之后保留原有预热和全部样本。这是预先声明的隔离措施；旧 cohort 的持久化路径长停顿尚不能归因为编译残留 I/O，也不会因此删除旧首轮或重标旧报告。构建缓存继续在失败时保存，成功构建也可独立于测量结果保存缓存。固定二进制传递 artifact 保留 1 天。稳定名称的汇总检查只有在构建和测量均成功时通过；构建/测量上界为 45/55 分钟，汇总 1 分钟，晋升最多 45 分钟包含等待同源码 Rust 工作流结束，串行执行时间上界合计 146 分钟，不包含 runner 排队。正常情况下 Rust 与性能并行完成，等待不会再触发编译或补跑性能样本。构建上界用于真正重编译两版 workspace，不改变负载、测量上界或统计验收门槛。

`077004ee6884de7304a658e9b551aad2c381242f` 的[首次拆分工作流运行](https://github.com/bitcapybara/tidemq/actions/runs/39383759275)构建成功，但测量命令的选项名校验不接受 `--manifest-sha256` 中的数字，在参数解析阶段终止。完成 trial 数为零，没有性能 `summary.json`；汇总检查失败、晋升跳过。[构建 artifact](https://github.com/bitcapybara/tidemq/actions/runs/39383759275/artifacts/12246803251)可用于核对固定产物身份，不能作为已测量或通过的性能证据。修复工具后须另行开始完整固定 cohort，原失败运行保留，负载与统计门槛不因这次工具错误改变。

main 只有 Markdown 或 LICENSE 变动时跳过重复性能测量，PR 检查仍运行。首次接受真实数值报告之前，清单中的源码仅是经过正确性验证的比较起点，不能因文件存在就称已有数值基线。

当前 CI 使用 Rust 1.99.1，并通过 `RUSTUP_TOOLCHAIN` 显式让基线与候选都使用同一编译器，不由旧 checkout 的 toolchain 文件暗中选择旧版本；服务身份中的 rustc 信息从实际构建目录读取。基线使用其冻结源码自带的配置生成器，候选使用受保护 tools 的生成器，分别记录源码和 SHA-256，不修改历史源码，也不为生产代码保留旧配置兼容。每次在测量外对实际二进制执行有界 `diagnose --config`，只在测试工具中归一化 role 布局及随机 UUID/目录，严格比较全部其余有效配置，并硬校验全角色节点数、quorum、端口、内存和协议预算；原始配置和 allocator snapshot 不保存，报告保留非秘密预算、配置摘要及生成器身份。性能负载、请求预算、八对顺序和统计门槛保持原样。

本轮 `401c7c9` 的 [39402803303](https://github.com/bitcapybara/tidemq/actions/runs/39402803303) 构建结束，但候选配置校验拒绝新 `authorization` 段，未产生候选测量数据；[原始失败汇总](../performance/calibrations/20261020-401c7c9-build-cache-failed.json) 保留。取回准确产物后确认两版服务、两版负载工具分别哈希相同，候选构建仅耗时 0.26／0.19 秒；真实双目录最小回归复现 Cargo 以旧源码时间戳复用另一 checkout 的输出。这个失败不支持代码性能回退结论，也不晋升覆盖率或性能基线。修复后每个构建都清理 workspace 包、保留外部依赖缓存；失败摘要同时保留有界错误原因和配置校验日志，下一轮必须从头执行完整固定 cohort。源码 HEAD 与二进制摘要一致的记录不能单独证明 Cargo 实际编译了该源码，当前清理与真实 Cargo 回归承担这一保证。

修复后的 `693f9bd` 已完成 [39411371718](https://github.com/bitcapybara/tidemq/actions/runs/39411371718) 全部 32 个固定试次，build／measure 两个真实 job 成功，三个主指标的原统计门禁通过；RF3 两项 p99 为 `imprecise`，不能宣称尾延迟无退化。随后汇总 job 因 GitHub 付款／spending limit 问题未获得 runner，整体 workflow 为 failure，自动晋升跳过。独立核对 64 份 workload、32 份 execution、四个实际二进制及同源码 Rust 8／8 成功结果后，本地执行未经修改的联合脚本，以 `d38fb80` 一次记录两套数值基线与逐字节原性能报告。此处明确为本地审计晋升，不冒充 hosted 汇总成功；当前性能基线源码为 `693f9bd`，详细计数、区间、摘要与计费注释见[验收证据](evidence/2026-10-20-ci-refactor.zh-CN.md)。

2026-10-20 的初始比较源码冻结为 `56a83498ae4ad31ff122504b2f865a7caf7c1126`，包含元数据创建、已完成迁移重放与持久 Read 前缀的正确性修复。旧 `3b75d9c` 的完整失败/不确定报告继续保留，schema、八对试次、负载、比较族、指标与精度预算不变。随后 [Performance 39386162718](https://github.com/bitcapybara/tidemq/actions/runs/39386162718) 完整完成 32 个试次，测量、汇总与晋升均成功；对应 `d7c913e` 的七项 Rust CI job 全部成功。`e16c79d` 已将真实 [baseline-report.json](https://github.com/bitcapybara/tidemq/blob/e16c79d87fb0633c33a0c9b0a465d200dc4c1e6d/performance/baseline-report.json) 入库，并将接受源码推进到 `d7c913ebd21a5f1f07840304e9b8f8d9d8a3bfae`。

这次两侧实际服务和负载二进制各自完全相同，属于 A/A 校准；六项主指标通过固定 0.15 log 半宽预算，四项 p99 仍显示 `imprecise`。它建立有限回归负载的首份数值基线，不是代码提速证明，也不证明已排除小幅尾延迟退化或已达到生产性能目标。完整身份、原始报告和区间见[本轮记录](evidence/2026-10-20-cancellation-zero-copy-performance.zh-CN.md)。

两个场景都使用 128 B 正文、单条生产批次、有界持久 ACK 批次、四条 request lanes。吞吐场景有四个独立分区队列，各有独立 Producer/Consumer，每分区 32 条预热、256 条无节奏限制的测量。延迟场景使用一个分区、一对 Producer/Consumer，256 条预热、1024 条测量，每 40 ms 计划一条，固定 25 条/秒。单分区避免每轮随机身份映射到 request lanes 时出现不同的碰撞组合；四分区压力仍由吞吐场景验证。生产和端到端延迟从计划发送时刻计时，调度延误与 SDK 背压不会从样本中消失；另报调度延误分布。延迟场景的固定速率不能当作服务吞吐能力。

每个场景均先验证所有预期身份、唯一交付、offset、生产回执、消费持久 ACK 与最终持久前沿，再取无节奏限制场景的完整阶段吞吐，以及固定速率场景的生产持久回执、offered-to-durable-ACK 均值和 p99。均值按每个分区原始累计耗时导出的 `mean_ns × samples` 加权汇总，不使用直方图桶估算，也不使用固定速率下近乎恒定的阶段时长/消息数；迟发和少量长耗时始终计入均值。p99 合并所有分区直方图，不平均各分区的 p99。任意失败、超时、遗漏、强制退出或不完整报告均失败，不能少做工作后声称提速。原始报告分别保存为 `report-throughput.json` 和 `report-latency.json`。

两个负载二进制分别链接基线和候选的 SDK/protocol/transport；构建时只在一次性源码归档中覆盖受保护的四个负载文件，不修改真实 checkout。报告记录服务/SDK 源提交、锁文件、负载文件与实际二进制摘要。两者必须编译同一份受保护负载，不保留旧 SDK 的替换例外。

首次针对 `9fe9c95` 的旧场景比较失败：突发负载 p99 精度不足，三节点基线与候选都出现控制 RPC 错误。原始证据保留，不能把它晋升为已通过的历史基线。新场景先做相同二进制 A/A 校准，再以修复正确性问题的源码建立初始数值基线；详细状态见[本轮记录](evidence/2026-10-20-cancellation-zero-copy-performance.zh-CN.md)。

统计比较使用八组成对 log ratio 的 Student t 模型，预先固定五项指标 × 两个拓扑共十个比较。采用 [Bonferroni 校正](https://itl.nist.gov/div898/handbook/prc/section4/prc473.htm)：单项双侧区间的名义置信度为 99.5%，整个比较族的同时覆盖名义值至少为 95%，df=7，临界值为 4.029337177642459。校正不要求各指标独立，但 Student t 推断仍依赖各对样本足够独立及配对 log ratio 的模型假设；有限重尾样本不构成覆盖率保证。报告使用 `slowdown_ci_adjusted` 和 `inference` 记录校正区间与比较族，不把单项区间标成 95%。

五项指标中任何一项的校正区间完全落在退化方向时均失败，即使检测到的退化只有 1%；不存在默认放行 5%/10% 的回退额度。精度验收只适用于吞吐、生产原始均值和 offered-to-durable-ACK 原始均值，这三项 log ratio 区间半宽均不得超过预先固定的 0.15，超过则为 `inconclusive` 并失败。0.15 是统计不确定性预算，不是允许降低 15% 性能。主指标精度达标、区间仍包含零，只表示未检测到回退，不证明差异为零。

p99 继续记录全部观测并参与显著回退否决，但不要求其区间达到主指标的精度。其 `precision_required=false`；区间半宽超过 0.15 的诊断参考尺度时显示 `imprecise`，否则显示 `diagnostic`，这两个状态均不表示尾延迟精度通过。一个成功报告可以同时有很宽的 p99 区间，因此不能据此排除小幅、稀有尾延迟退化，也不能声称具备 3% p99 精度或 5% 尾退化检出保证。直方图桶上界宽度最高约 3.125%，不等于 p99 的统计精度。

旧五对同二进制 pilot 的 RF3 配对方差，投影到八对并应用上述校正后，三个主指标预计最大 log 半宽约 0.115，均值退化的统计检出尺度约为 15% 量级；这是基于 pilot 方差的规划估计，不是每个提交的检出保证。0.15 预算在下一轮完整 cohort 开始前固定，不能把旧五对结果重标为新规则已通过。开发服务器的噪声不能代表 GitHub runner；每次 CI 仍检查本次实际区间。按 pilot 用时，32 次 trial 的测量预算约 40–45 分钟，90 分钟总预算还要覆盖构建和制品上传，实际执行必须核验。

固定 cohort 开始后，不丢首轮、不替换慢样本、不追加试次直到通过。失败或 `inconclusive` 的完整证据保留；排查并记录环境或代码变化后才能声明新一轮完整比较，不能从反复重跑中挑选通过结果。A/A 校准还检查五项指标是否出现任一方向的显著偏差，不能将相同二进制的时序偏差晋升为基线。判断以两侧实际服务与负载二进制 SHA-256 同时相等为准，与源码提交是否相同、是否复用构建无关；晋升重算使用同一规则，缺少有效二进制摘要时失败。

复现需要 Node.js 20+、Rust 1.99.1、C/C++、CMake、pkg-config、Git、tar 和 OpenSSL。对比旧源码时设置 `RUSTUP_TOOLCHAIN=1.99.1`，使两侧使用同一编译器。三个路径必须指向干净的提交，baseline 的 HEAD 必须匹配受保护清单；输出目录必须不存在。

```bash
node --test scripts/performance/*.test.mjs
node scripts/performance-gate.mjs run \
  --tools /absolute/path/to/trusted \
  --baseline /absolute/path/to/previous-baseline \
  --candidate /absolute/path/to/candidate \
  --output /absolute/path/to/new-results \
  --target /absolute/path/to/cargo-cache
```

本地 `run` 仍依次准备并测量同一目录的固定产物。分阶段复现可先执行 `prepare`，保存其标准输出的 `manifest_sha256`，再将产物目录传给 `measure`；两个阶段使用相同的干净源码和受保护工具。更换主机时复制整个产物目录，执行权限会在完整校验后恢复。

```bash
node scripts/performance-gate.mjs prepare \
  --tools /absolute/path/to/trusted \
  --baseline /absolute/path/to/previous-baseline \
  --candidate /absolute/path/to/candidate \
  --output /absolute/path/to/new-builds
# Set PREPARED_MANIFEST_SHA256 to the 64-character value after manifest_sha256=.
node scripts/performance-gate.mjs measure \
  --tools /absolute/path/to/trusted \
  --baseline /absolute/path/to/previous-baseline \
  --candidate /absolute/path/to/candidate \
  --prepared /absolute/path/to/new-builds \
  --manifest-sha256 "$PREPARED_MANIFEST_SHA256" \
  --output /absolute/path/to/new-measurements
```

自动晋升要求完整成功的 main 运行，同时提交真实报告；晋升前重新比较全部五项指标，校验受保护策略摘要、报告 schema、基线身份和当前干净 HEAD，并要求同一 main 提交最新 push Rust 工作流已完成且成功。晋升 job 下载该 Rust run 的覆盖率原始 JSON 与摘要，重算 lines/functions/regions 的精确分数，校验策略、旧基线、原始制品与 run ID；只有两套门禁都通过才一次性提交性能报告和两套基线，避免独立机器人提交令另一个 cohort 的 main 身份失效。正确性/覆盖率失败、仍在运行或不存在时均不晋升；main 已前进时旧运行直接跳过，跳过不代表基线更新。首次覆盖率初始化与 PR 的受保护范围见[测试手册](testing-benchmarks.zh-CN.md#ci-门禁与覆盖率基线)。policy/summary 使用 schema 4，原始 Rust 报告仍为 schema 3；旧五对或旧统计 schema 的报告不能复用。失败保留 `summary.json`、原始报告、每节点指标/诊断、有限进程日志与失败 WAL；成功时在检查所有节点并正常排空后清理试次数据目录。CI artifact 保留 30 天，仓库基线报告长期跟随 Git 历史。Rust 工作流另在失败时上传临时目录中的 `process-*.log`，保留 7 天，不上传配置、token 和证书文件。

GitHub 的 `Performance gate (RF1 and RF3)` 检查失败会阻止该检查通过。当前私有仓库套餐的分支保护 API 返回 403，无法设为平台强制的 required status check；具有合并权限的人仍可能绕过失败检查。升级套餐或调整仓库可见性后，应将该名称设为 main 的必需检查并保留原有规则。

这组有限回归负载不覆盖冷回放、大消息饱和吞吐、跨可用区网络、所有 allocator 或长期资源漂移；下面的手动矩阵保留这些独立测量入口。新格式评估见 [rkyv / FlatBuffers / FlexBuffers](research/zero-copy-formats.zh-CN.md)，格式迁移前必须实测校验、编码和保留内存成本。

## 固定构建

从已提交、锁定依赖的源码构建，运行期间不要修改源码。需要 Rust 1.99.1、C/C++ 工具链、CMake、Bash、OpenSSL、curl，以及已有 allocator 构建依赖。对象回放还需要已核验的 RustFS 1.0.3。构建可能较慢；测量前停止其他编译、测试和负载。不同目录的已有服务不会被脚本停止。

```bash
bash scripts/perf-build.sh target/perf-candidates all
```

也可把最后一个参数改为 `system`、`mimalloc` 或 `jemalloc`。脚本对相同优化、锁文件和源码执行互斥 feature 构建，每种 allocator 保存独立服务二进制、负载工具和 `identity.json`。服务使用选定的 global allocator；负载工具明确使用 `std::alloc::System`、四个 Tokio worker，不继承 `src/main.rs` 的声明。

身份记录包含源提交、Cargo.lock SHA-256、服务二进制 SHA-256、rustc、主机和 release profile。运行报告另外计算实际服务/负载二进制及实际配置的 BLAKE3，并核对 PID 对应的可执行文件、服务就绪状态和运行中 allocator backend。源码中新增 `heap-profiling` 会改变构建身份，不能与普通 jemalloc 结果混写。

## 有限本地试跑

每次使用全新的输出目录。脚本创建私有证书、限定作用域的开发 token 和 RF1 数据目录，启动独立服务，运行预热与测量，然后等待排空退出。服务和负载进程清空继承环境，仅保留固定 PATH，服务显式设置 `TOKIO_WORKER_THREADS=4`，负载程序自身也固定四个 worker，避免继承宿主 malloc 调试选项干扰对照。`execution.json` 记录该策略；这不代表整个服务只有四个 OS 线程，阻塞采样和原生库还可能使用其他线程。固定占用 `127.0.0.1:7410/7411/8410`；回放额外占用 `127.0.0.1:19000`。端口冲突会失败，不会连接已有进程后假定其归本次试跑所有。

```bash
bash scripts/perf-run.sh target/perf-candidates/system target/perf-results/system-small small recommended
bash scripts/perf-run.sh target/perf-candidates/system target/perf-results/system-large large recommended
bash scripts/perf-run.sh target/perf-candidates/system target/perf-results/system-slow slow recommended
TIDEMQ_PERF_RUSTFS_BIN=/absolute/path/to/verified/rustfs \
  bash scripts/perf-run.sh target/perf-candidates/system target/perf-results/system-replay replay recommended
```

| 场景 | 测量消息 | 正文/批量 | 生产 worker | 消费与控制 |
| --- | ---: | --- | ---: | --- |
| small | 4096 | 128 B / 32 条 | 4 | 有界批量 ACK；每 50 ms 一个真实 topic 查询 |
| large | 64 | 262136 B 正文 + 8 B key = 256 KiB / 1 条 | 4 | 同样的 ACK/控制路径；不关闭持久化 |
| slow | 2048 | 128 B / 32 条 | 4 | 每 ACK 批次人为等待 20 ms，观察业务积压和尾延迟 |
| replay | 256 | 64 KiB / 4 条 | 4 | 生产后等待 5 s，再创建消费分配；要求真实冷读计数增加 |

四个 Producer worker 并行生产，一个 Consumer 顺序执行 fill、处理和持久 ACK。

预热使用独立 topic，样本不会混入测量。消费慢场景中的 20 ms 等待计入 offered-to-durable-ACK，ACK RPC 本身单独计时。回放中的“生产后再分配”只确定冻结的 B 类边界，不自动证明物理对象读取；`--require-cold` 要求同一独立服务的 `tidemq_s3_cache_misses_total` 增加。没有此证据会输出失败报告，不把热数据重读称为冷读。

这里的冷读指经过真实对象存储读取路径，至少发生一次冷读缓存 miss；不声称 RustFS、操作系统页缓存或物理磁盘缓存已经被驱逐，也不由一次 miss 推导全部消息都来自对象存储。完整冷热占比应结合最终源分类指标、归档/本地回收状态与 S3 字节证据核对。

完整的短跑矩阵共 17 次：原 16 次包含三种 allocator 各运行四个推荐观测场景、同一 System 二进制的必要观测与详细诊断对照，以及 System 在 small/replay 上的 2/4 Request lane 对照；第 17 次在相同 System-small 推荐日志配置上启用可选 OTLP。默认和基线均为 4 lanes：

```bash
TIDEMQ_PERF_RUSTFS_BIN=/absolute/path/to/verified/rustfs \
TIDEMQ_PERF_OTELCOL_BIN=/absolute/path/to/verified/otelcol \
  bash scripts/perf-matrix.sh target/perf-candidates target/perf-results/matrix
```

这是有限的闭环负载。并行数不足或服务变慢时，产生的 offered load 也会下降；报告不声称校正 coordinated omission，也不把这组短跑作为饱和吞吐上界。固定运行顺序可能受温度、后台活动和页缓存影响；正式比较需要重复、多次交错顺序和受控硬件。

## 同预算的 Request lane 对照

`perf-run.sh` 第五个参数与 `perf_load --request-lanes` 只接受 `2` 或 `4`。对照保持同一 System 服务/负载二进制、场景、四个生产 worker、ACK 和 Core 保留。只改变每个活跃 P/D/B connection 复用的 Request stream 数，C 仍持续驱动两个 Core streams，ACK 仍使用独立短流且最多八个。类别按实际操作建立 connection；不强行创建未使用类别，也不改变内部 C/R/B 布局。

```bash
bash scripts/perf-run.sh target/perf-candidates/system target/perf-results/lanes2-small small recommended 2
bash scripts/perf-run.sh target/perf-candidates/system target/perf-results/lanes4-small small recommended 4
```

`sdk_configuration` 记录实际传给 SDK 的参数。默认共享逻辑在途上限 16 MiB，原生 transport envelope 为 128 MiB、控制保留 16 MiB；每 connection 的最大 streams 仍为 16，connection window 64 KiB、stream window/send buffer 各 16 KiB。原生每 connection 预留公式为 `send_buffer × max_streams + connection_window`，两种 lane 数均为 320 KiB，不因少开 Request lane 就扩大窗口或挪用 Core 保留。这些额度是各自的逻辑准入范围，不能相加为 RSS。

应连同 `produce_batch`、`ack_batch`、`control_query` 的样本数/分位、SDK 准入等待与保留字节、QUIC flow-blocked/send-wait 及服务 RSS/allocator/容量观察解释结果。不同新建 topic/producer 的 affinity 哈希及短跑样本量会影响 stream 使用；单组对照不证明 2 或 4 是全局最佳值。共享 connection、每请求短流及跨真实故障域的布局仍需独立实验。

## 观测成本比较

| 模式 | 实际开关 | 保留能力 |
| --- | --- | --- |
| minimal | 配置 `logging.filter = "warn"` | 所有必要指标、管理采样、警告与错误；不关闭共识、计数或容量检查 |
| recommended | 服务默认 `warn,tidemq=info` | 默认有界 JSON 日志、指标和每秒资源采样；性能基线的参考模式 |
| detailed | 同一二进制 `serve --trace-seconds 300` | 限时 runtime 请求 span/完成记录，自动恢复默认过滤；不启用逐包日志或 heap dump |

以上三档均关闭可选 OTLP，只比较日志、默认指标与限时日志诊断的开销；不能用它们声称已测得导出成本。第 17 次 `recommended-otlp` 单独测服务端可选导出，保持 small 的 4096 条测量、256 条预热、128 B 正文、32 条 batch、4 个 worker 和 4 lanes。SDK 仍不安装宿主 subscriber 或 exporter。

```bash
TIDEMQ_PERF_OTELCOL_BIN=/absolute/path/to/verified/otelcol \
  bash scripts/perf-run.sh target/perf-candidates/system target/perf-results/system-small-otlp small recommended-otlp 4
```

该用例要求绝对路径的官方 core Collector 0.162.0，复用仓库 [collector.yaml](../deploy/observability/collector.yaml)，监听 `127.0.0.1:4318/18888`。Collector 清空继承环境，仅设置 PATH、`GOMAXPROCS=2`、`GOMEMLIMIT=96MiB`；Go 软限制与 Compose 示例一致，native 运行不具备容器的 CPU/内存硬限制。memory limiter 128 MiB、spike 32 MiB，使用 `nop` 接收后丢弃，不连接持久追踪后端。TideMQ 保持推荐日志，显式开启 `[tracing]`：1% head sampling、每秒最多 128 span、queue 256 条、batch 32 条、导出间隔与总 timeout 均为 500 ms。每 span 4096 B 的队列逻辑许可约 1 MiB，不是 RSS；队列、导出失败与退出的口径见[追踪手册](operations/tracing.zh-CN.md)。

`collector/identity.txt` 与 `version.txt` 记录实际二进制/配置 SHA-256、版本和环境；PID 单独保存在 `process.pid`。Collector 在负载前、负载与冷却后、TideMQ 排空退出后各抓一次自身指标，每次 HTTP 最多 2 s、正文最多 256 KiB。`resources-*.txt` 保留实际暴露的进程自监控指标，包括 `otelcol_process_cpu_seconds_total` 与 `otelcol_process_memory_rss_bytes`，缺失时明确不可用；短跑前后 RSS 不是全程峰值。Collector 的 CPU/RSS 与 TideMQ 的独立口径分别报告，不相加成服务 RSS。

`collector/evidence.json` 保存 `otelcol_receiver_accepted_spans_total`、`otelcol_receiver_refused_spans_total`、`otelcol_receiver_failed_spans_total` 的真实接收/拒绝/失败计数与采集失败数；不存在的系列为 null。计数涵盖该 owned Collector 的启动、预热、测量、冷却和服务关闭，不冒充仅测量阶段的样本数。TideMQ 每阶段的原始指标另含 `tidemq_runtime_trace_*` 准入、采样/容量丢弃、pending、尝试/字节、失败/超时；排队导出的完成时间可能落入后续区间，应结合最终接收计数解释。服务先退出，Collector 再以 SIGINT 排空；两者实际 exit status 和强制停止标志分别写入 `service-exit.json` 与 `collector-exit.json`，沿用有界清理期限。

1% 有限短跑可能没有任何 span 被接收。此时用例保留业务报告、标为缺少导出成本证据并返回失败；接收计数为零或 null 时不得声称测得导出成本。Collector 日志同样限制为 32 MiB。即使接收成功，`nop`、本地明文 HTTP 与单次短跑也不证明后端持久化、远程 HTTPS、Collector 故障成本、SDK 宿主 exporter 或生产 SLO。

`perf_load --observability` 只是记录实际模式的名字，不会修改服务。启动脚本负责配置真实开关。详细档 TTL 必须覆盖预热和测量；两次阶段上限各为 120 s，启动、注册与清理另占时间，应按报告的阶段起始 Unix 时间和服务日志核验完整覆盖。更长手动负载需要分段，不能把 TTL 到期后的区间继续标为详细档。原始配置摘要和日志应与报告一起核对。

脚本对服务和 RustFS stdout 各保留最多 32 MiB。达到上限后管道关闭，服务的日志 sink 错误计数可见；服务日志达到上限会令试跑失败。不要在长期稳态中复用这个短跑日志接收方式；部署时按运维日志文档配置外部轮转与保留。

## 报告口径与边界

`report.json` 使用新建、0600 文件，最多 8 MiB；不会覆盖已有报告。报告保留如下独立口径：

| 字段 | 口径 |
| --- | --- |
| `produce_batch` | 每次原逻辑批次从 SDK `produce` 调用到持久回执的延迟；原身份 Unknown 恢复时间计入同一个样本 |
| `receive_call` | 每次 `recv` 调用耗时，包括空结果；不是逐消息端到端时延 |
| `ack_batch` | 同一个有界 ACK 批次至持久结果的耗时，包括允许的 Unknown 重试 |
| `offered_to_durable_ack` | 负载进程单调时钟，从该批消息构建前到对应业务 ACK 完成；包括积压、读和人为消费等待 |
| `control_query` | 混合负载下真实 `GET_TOPIC` 往返，不与 ACK 的时延合并 |
| 吞吐 | 实际成功消息数 / 对应生产或消费阶段的单调时间；回放等待不算消费吞吐时间 |
| `resources` | 独立服务 PID 累计 CPU 时间、OS RSS 与服务实际诊断中的进程/cgroup/allocator/容量；不存在的值是 null |
| `client_metrics_before/after` | SDK 自身的请求、重试、Unknown 与逻辑准入快照；包含 SDK 内部已透明恢复、未暴露给负载器的事件 |
| `client_transport_before/after` | 真实 SDK QUIC 连接/stream、应用帧字节、原生流控阻塞、丢包与 PING 探测；应用帧字节不冒充线速 UDP 字节 |
| `sdk_configuration` | 实际 Request lanes、SDK pending 上限、原生 transport/Core 保留、stream 上限和流控窗口；用于核对同预算对照 |
| `server_metrics_before/after` | 复用每阶段原有两次冷读证据抓取，保留完整有界 OpenMetrics 文本和唯一 EOF；可计算该阶段真实 WAL/Read/ACK/排队/源分类增量，避免把预热混入测量 |

`logical_bytes` 是计划正文与 key 总量；另存实际提交/ACK 的逻辑字节和字节吞吐，不把失败阶段的计划量当作成功量。`produce_unknown` 与 `produce_recovered_original_identity` 仅统计暴露给负载器的 Unknown/显式恢复，SDK 内部重试应结合其前后快照判断。`artifact-sizes.txt` 记录报告、最终 metrics/diagnostics、服务/RustFS 日志的实际输出字节及本地数据目录的文件系统占用；后者不是累计写入 I/O，也不能与对象字节或 RSS 相加。固定 WAL quorum/fsync 语义，对照中不关闭持久化。

直方图始终记录全部成功样本，固定 2048 个计数槽，不随消息数增长。分位是 log2 每级 32 子槽的上界，最大相对槽宽 3.125%，并非精确排序值。P99 至少需要 1000 个样本，P99.9 至少需要 10000 个样本；不足时为 null。小消息短跑只有 128 个生产批次，不能据此宣称 P99/P99.9。需要更大的有限样本时直接运行 `perf_load --help` 配置消息数和批量，仍受总字节、并发和阶段期限限制。

OS `ps` 的累计 CPU 分辨率依平台，不能从短至几毫秒的差值推导稳定 CPU 利用率。RSS 每秒采样；阶段超过 240 s 时适当放宽间隔，每阶段最多 242 个点，只能称为采样最大值。allocator 单点统计超过 8192 B 时，周期样本明确省略详细统计，初始完整诊断仍保留。RSS、allocator resident 和 cgroup usage 不相加；cgroup 可能包含文件缓存或其他进程。

工具用固定大小位图校验每个消息 identity 和实际 offset 恰好出现一次，并核对最终持久 ACK frontier 等于计划消息数。生产 Unknown 只重试当前 worker 保留的原 batch；注册 Unknown 使用原 producer ID/registration epoch，不创建替代身份。重试次数和总阶段期限有限。终止错误、未恢复 Unknown、重复/缺失消息、控制失败和观测失败使报告失败；不丢弃失败样本后只报成功吞吐。`status = passed` 仅表示该有限负载的语义与观测校验通过，不代表达到生产性能 SLO。

服务日志、证书、token、S3 凭据和诊断产物放在 0700 目录中，不能提交到仓库或公开分享。报告包含运行身份和配置路径，但不复制 token、正文、私钥、WAL 或对象内容。脚本只停止自己启动的 PID；排空超时会记录失败并停止该私有试跑进程，原始数据保留供排查。

## 仍需独立放行的实验

本工具固定当前正式 C/P/D/B 与内部 C/R/B 类别隔离，仅对已有的 2/4 Request lanes 做同预算对照。混合连接/短流后端、网络丢包与限速、RF3 三区 quorum、跨 Gateway 故障、长时间突发/排空/冷却、Linux cgroup/OOM、同 allocator 的符号化存活栈和竞品同语义对照仍须分别取得证据。实际内存诊断能力见 [allocator 发布构建证据](evidence/2026-10-19-allocator-release.zh-CN.md)，该 jemalloc profiling 证据不能冒充 mimalloc 或 System 的现场采样。

仓库当前默认 allocator 为用户指定的 mimalloc，System/jemalloc 通过互斥 features 选择。这个配置决定不表示它已通过生产性能验收；短跑吞吐、一次 RSS 差值或 malloc 微基准不足以证明它是全局最佳选择，仍需目标平台、长期内存与同 allocator 排查能力的独立证据。
