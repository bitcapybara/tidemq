# TideMQ 本地负载与性能证据

本工具使用仓库中的 Rust SDK，经真实 UDP/TLS/QUIC 连接独立 `tidemq serve` 操作系统进程。生产成功以服务端持久提交回执为准；消费完成以逐消息 token 的持久 ACK 为准。运行工具不代替 RF3、故障恢复和对象生命周期正确性测试。

## 固定构建

从已提交、锁定依赖的源码构建，运行期间不要修改源码。需要 Rust 1.96、C/C++ 工具链、CMake、Bash、OpenSSL、curl，以及已有 allocator 构建依赖。对象回放还需要已核验的 RustFS 1.0.3。构建可能较慢；测量前停止其他编译、测试和负载。不同目录的已有服务不会被脚本停止。

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

| 场景 | 测量消息 | 正文/批量 | 并行 worker | 消费与控制 |
| --- | ---: | --- | ---: | --- |
| small | 4096 | 128 B / 32 条 | 4 | 有界批量 ACK；每 50 ms 一个真实 topic 查询 |
| large | 64 | 262136 B 正文 + 8 B key = 256 KiB / 1 条 | 4 | 同样的 ACK/控制路径；不关闭持久化 |
| slow | 2048 | 128 B / 32 条 | 4 | 每 ACK 批次人为等待 20 ms，观察业务积压和尾延迟 |
| replay | 256 | 64 KiB / 4 条 | 4 | 生产后等待 5 s，再创建消费分配；要求真实冷读计数增加 |

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

该用例要求绝对路径的官方 core Collector 0.162.0，复用仓库 [collector.yaml](../deploy/observability/collector.yaml)，监听 `127.0.0.1:4318/18888`。Collector 清空继承环境，仅设置 PATH、`GOMAXPROCS=2`、`GOMEMLIMIT=192MiB`；memory limiter 128 MiB、spike 32 MiB，使用 `nop` 接收后丢弃，不连接持久追踪后端。TideMQ 保持推荐日志，显式开启 `[tracing]`：1% head sampling、每秒最多 128 span、queue 256 条、batch 32 条、导出间隔与总 timeout 均为 500 ms。每 span 4096 B 的队列逻辑许可约 1 MiB，不是 RSS；队列、导出失败与退出的口径见[追踪手册](operations/tracing.zh-CN.md)。

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

allocator 生产默认保持未定案。短跑吞吐、一次 RSS 差值或 malloc 微基准均不足以冻结它；还必须满足目标平台、长期内存与同 allocator 排查能力的计划关卡。
