# 测试方法与现有证据

验收先检查持久正确性，再做性能。每类测试说明实际边界；参考模型、真实 QUIC/磁盘、RustFS 和独立 CLI 进程分别报告。本地通过项和外部待验条件见[实施状态](implementation-status.zh-CN.md)，不能由测试文件存在推断已经通过。

## CI 门禁与覆盖率基线

[Rust 工作流](../.github/workflows/rust.yml) 固定 Rust 1.99.1，仅 main push、PR 和手动 dispatch 触发，避免同时推送 feature/main 时重复构建。Linux/macOS 均先执行 fmt，再对 workspace 的 `--all-targets` 执行严格 Clippy，覆盖 lib、bin、单元测试、集成测试、example、benchmark 和构建脚本；同时启用 SDK 的 metrics、OpenTelemetry 与协议 codegen。Linux 另验 SDK 未启用可选 instrumentation 的代码路径。生产 lib/bin 另保留 panic、unwrap、expect、todo、unimplemented、unreachable 和越界索引审计。默认验证/覆盖率使用 mimalloc，另有 System/jemalloc 矩阵，heap-profiling 在 Linux/macOS 另验，不对这些互斥 feature 使用 `--all-features`。

普通测试使用固定 cargo-nextest 0.9.148，配置见 [nextest.toml](../.config/nextest.toml)。`ci` profile 不重试失败项，逐测试进程执行，保留 JUnit；测试并发为 1，避免同时启动多个 Raft fixture 争用宿主资源，测试内部原有业务并发不变。稳定 Rust 的 doctest 另外用 `cargo test --doc` 运行；nextest 不运行 doctest，稳定 LLVM 覆盖率也不把这些 doctest 纳入分母。依赖 RustFS/官方 Collector 的 ignored 场景仍需按下文显式运行，不能由默认 CI 通过推断其结果。

Linux 覆盖率 job 使用固定 cargo-llvm-cov 0.9.1 与匹配工具链的 LLVM tools，调用受保护的 [coverage-run.sh](../scripts/coverage-run.sh)，对相同 workspace 全部目标和 SDK 可选 feature 运行 instrumented nextest，生成 LLVM JSON、LCOV 和 HTML，连同原始 JUnit 保留 30 天。[policy.json](../coverage/policy.json) 固定范围和工具版本；仅保留 cargo-llvm-cov 默认源码排除，没有为提高数字额外排除测试、example 或生成代码。该指标表示 LLVM 能报告的 workspace 源码覆盖情况，不宣称覆盖未构建的互斥 allocator、ignored 外部环境或稳定 doctest。

[coverage/baseline.json](../coverage/baseline.json) 维护上一份接受报告的 lines、functions、regions 原始 `covered/count`。比较使用 BigInt 交叉相乘，`next.covered × old.count >= old.covered × next.count` 三项都成立才通过；没有小数四舍五入、容差或“低于几个百分点才失败”。[边界测试](../scripts/coverage/gate.test.mjs) 验证微小下降、大整数、缺失/伪造数据、来源/策略/制品摘要变更及双门禁晋升前的原子检查。

PR 从 base commit 读取比较器、测试范围、nextest 配置与既有基线，候选不能替换自己的覆盖率基线。首次引入时，基线明确标记 `uninitialized`，没有伪造零值：PR 和普通 feature 不能据此通过；只有 main 显式 `--allow-initialize` 的真实非零测量可以等待首次晋升。已有数值基线时，该参数不能跳过回退比较。

性能与覆盖率统一在 [Performance 的晋升 job](../.github/workflows/performance.yml) 中更新，避免一个工作流提前推进 main 令另一个固定 cohort 失去源码身份。晋升要求当前 main 未前进、同提交最新 push Rust 工作流成功，下载该 run 的覆盖率制品，再独立重算 LLVM 原始计数、策略/旧基线/制品摘要、Rust run ID 以及 RF1/RF3 全部成对性能结果。任一不通过，两套基线都不写入；通过后只产生一次包含两套基线的提交。首次部署这一流水线时必须先完成这次 main cohort，不能将 `uninitialized` 当作已建立数值基线。

当前数值基线对应源码 `693f9bd`：行 27907／35176、函数 2576／3637、region 38365／49848。Rust run `39411371654` 的 8 个 job 成功，Performance run `39411371718` 的构建、32 个测量试次及统计比较成功；其后汇总 job 因 GitHub 计费限制没有启动。对全部原始制品做独立交叉审计后，本地运行同一联合脚本，以 `d38fb80` 一次记录覆盖率和性能基线，未修改样本或阈值，也未把整个 Performance workflow 标为成功。后续 PR 使用这个已接受数值基线严格比较，恢复 Actions 额度后的远端验收仍须实际运行；详见[本轮证据](evidence/2026-10-20-ci-refactor.zh-CN.md)。

构建缓存使用 Swatinem/rust-cache：相同 OS/工具链的普通 debug job 共享 key 前缀，再按默认、System/jemalloc 与 heap-profiling 分开 variant key，避免 GitHub 不可变缓存只保留最早写入的后端子集。LLVM instrumentation 使用独立 key 和 target，release 性能构建另用独立 key。保留依赖指纹和锁文件校验，不把覆盖率对象链接进普通二进制，也不把过期 workspace 二进制当作命中。所有编译缓存允许失败时保存，nextest/llvm-cov 下载固定预构建版本；性能测量使用全新 runner，既不编译，也不恢复构建缓存。

当前 GitHub 私有仓库套餐不能设置 required checks；工作流会失败，但有合并权限的人仍可绕过。具备平台条件后，应将稳定名称 `Rust gate` 和 `Performance gate (RF1 and RF3)` 都设为必需检查。

2026-10-20 合流后的实际命令、246 项普通测试、九项真实后端 ignored、默认 credit 下 512 条 Read 与空闲 sibling lane 的红绿回归、SDK/allocator feature 和原生观测结果见[本地统一验收记录](evidence/2026-10-20-local-validation.zh-CN.md)。冻结 `1398353` 的 17 项短 RF1 负载、全部持久 ACK、三分配器/日志档位/2 与 4 lane/OTLP 对照见[本地性能报告](evidence/2026-10-20-local-performance.zh-CN.md)，小消息消费约 60 条/秒，不能作为生产性能目标已达成的结论。

同日 `ssh dev` 的最新 [Docker 故障矩阵](evidence/2026-10-20-docker-077004e.zh-CN.md)冻结 `077004e`：RF1 三项、RF3 七项全部通过，十个独立 case 合计 736 条完整唯一交付和持久 ACK，六项故障均核对原 Pending 与最终连续前沿。新项目容器和网络已移除，原有 29 个容器与旧镜像身份前后一致。[旧版 Linux Docker 实测](evidence/2026-10-20-linux-docker.zh-CN.md)单独保留，不与新版计数相加。只读源码、RustFS fixture 的可写 `target` 和 Rustdoc 可执行临时目录要求见 [Docker 使用文档](../deploy/docker/README.zh-CN.md)。

历史 `a3f8f4e` Linux 普通测试 246 通过、11 ignored；十个真实 RustFS ignored 显式通过，余下官方 Collector ignored 未在 Linux 执行。workspace 文档三项、验证 driver 六项，以及 SDK metrics＋OpenTelemetry 的 25 项普通和一项文档测试通过，计数有重叠。最新冻结 `56a8349` 的本地 workspace 与 main `d7c913e` 的 Linux CI 均为 34 个 suite、284 passed/12 ignored/0 failed，当前七项 Rust CI job 全部通过，见[本轮记录](evidence/2026-10-20-cancellation-zero-copy-performance.zh-CN.md)。格式、Clippy 与生产 panic/索引审计均通过。完整回归使用 `--test-threads=1` 隔离 fixture，测试内部真实 Raft/QUIC/SDK 并发保持原样；历史失败保留原日志，不由后续通过反推唯一现场根因。

新增性能门禁覆盖真实单节点与三个完整角色节点，对照两版 SDK/服务端的同机八组成对样本，A/B、B/A 各四对。构建与测量使用独立 runner，通过外部 manifest 摘要、干净源码和四个二进制身份校验传递固定产物；测量 job 不编译，产物缺失、损坏或源身份不符均失败。五项指标 × 两拓扑统一使用 95% 比较族的 Bonferroni 校正，任何显著退化均失败；吞吐和两项原始消息延迟均值还必须满足 0.15 log 半宽的不确定性预算。p99 保留显著回退否决和完整诊断区间，宽区间显示 `imprecise`，其宽度本身不阻塞主指标验收，不能称作已排除小幅尾回退。每次测量必须通过全部持久正确性校验，固定 cohort 不得筛样本或重跑挑选结果。策略、检测能力、A/A 校准、初始基线、晋升和 GitHub 分支保护限制见[性能手册](performance.zh-CN.md)。大帧发送的字节等价、原分配容量计费和最后 clone 释放测试见 `crates/tidemq-protocol/src/framing.rs` 与 `crates/tidemq-transport/src/frame/tests.rs`；SDK 默认预算、嵌套取消域与发送后 Unknown 由真实网络测试覆盖。[本轮验证记录](evidence/2026-10-20-cancellation-zero-copy-performance.zh-CN.md)区分已完成的检查与失败实验，codec 探针不能替代端到端性能测量。

## 可复现验证

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --no-deps --locked -j2 --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen -- -D warnings
CARGO_BUILD_JOBS=2 cargo nextest run --workspace --all-targets --locked --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen --profile ci
cargo test --workspace --doc --locked --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen
# Linux only, from committed clean sources; the first main CI cohort initializes the baseline.
bash scripts/coverage-run.sh . /absolute/path/to/new/coverage-result
```

默认 mimalloc 调整后的 249 项串行测试、显式 System/jemalloc 构建、原始并行失败与 mimalloc-pprof 候选验证见[本次验证记录](evidence/2026-10-20-mimalloc-default.zh-CN.md)。

运行时集成测试共用进程级 tracing subscriber，并启动多个受限 Raft 子进程，历史 cargo test 使用 `--test-threads=1`；当前 nextest `ci` profile 保留逐项隔离。`CARGO_BUILD_JOBS=2` 限制构建并行度；nextest 的 `-j` 是测试线程数，不能混为构建限制。跨进程 exporter 可乱序到达，链路测试等待完整 SDK → Gateway → Broker 父子关系后再断言。测试隔离不构成业务并发吞吐或长稳证明。

不运行 cargo check。生产代码的额外 panic 审计与允许 unwrap 的测试分开：

```sh
cargo clippy --workspace --lib --bins --no-deps --locked -j2 -- -D warnings -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::unimplemented -D clippy::todo -D clippy::indexing_slicing
```

默认测试不依赖用户既有服务。真实 S3 用新建隔离 RustFS：先运行 `scripts/rustfs-precheck.sh` 校验并缓存固定官方发行包，再把绝对路径传给测试。测试只管理自己创建的进程、桶和目录。

```sh
scripts/rustfs-precheck.sh
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs cargo test -p tidemq --test s3_rustfs -- --ignored --nocapture
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs cargo test -p tidemq --test lifecycle_compaction -- --ignored --nocapture
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs cargo test -p tidemq --test lifecycle_persistence -- --ignored --nocapture
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs cargo test -p tidemq --test runtime_queue actual_runtime_sdk -- --ignored --nocapture
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs cargo test -p tidemq --lib runtime::data::reclaim_tests -- --include-ignored --test-threads=1
TIDEMQ_OTELCOL_BIN=/absolute/path/to/verified/otelcol cargo test -p tidemq --test tracing_export official_collector_accepts_real_otlp_and_releases_ports -- --exact --ignored
```

## 证据索引

| 代号与入口 | 实际验证内容 | 范围限制 |
| --- | --- | --- |
| S01 [真实 QUIC](../tests/quic_transport.rs)、[分派](../tests/server_dispatch.rs)、SDK 的网络测试 | TLS/mTLS 与伪造身份拒绝、有限分类连接/流、取消隔离、control/ACK 推进、RPC 结果匹配；四 lane 单 affinity 持续请求不因 sibling 空闲断连，半帧仍 poison；原期限内关闭 owned native I/O 并重绑端口；新 1-RTT 包 ACK 探测能淘汰失联旧连接，慢业务有网络进展时不误杀 | 本机 UDP；真实 LB/NAT/CID 和同预算性能对照另验 |
| S02 [WAL](../tests/wal_journal.rs)、[存储](../tests/replication_storage.rs)、[Raft QUIC](../tests/raft_quic.rs)、[崩溃](../tests/replication_crash.rs)、[应用失败](../tests/application_failure.rs) | fsync/vote/log/commit/snapshot、官方 storage-v2、RF1/RF3 多数派、换主、B 快照与 R 日志、原结果、SIGKILL、日志 rewrite、状态损坏/应用失败关闭 | 一部分底层测试使用受控内存网络；raft_quic 才是原生网络，不混称三区 |
| S03 [元数据](../tests/metadata_state.rs)、[权威](../tests/ownership_authority.rs) | CRUD 与永久身份、grant/fence/recover/activate、重试/CAS/旧 epoch、TTL 从请求开始、控制失 quorum、有限安全收尾 | 纯状态证据与 S08 的实际角色接管分别记录，前者不能代表入口故障 |
| S04 [Broker 测试](../src/broker/tests/mod.rs)、SDK 网络测试 | producer 注册/固定过期/全局高水位、canonical digest、sequence/冲突/原结果、owned 取消后提交、快照恢复、原 PendingBatch 重试 | 有限原结果窗口；SDK 进程重启没有持久 outbox |
| S05 [订阅状态](../tests/subscription_state.rs)、[持久组合](../tests/lifecycle_persistence.rs)、SDK 网络测试 | 逐条 attempt/token/ACK、洞/连续前沿、旧代次拒绝、seek、KeySerial、冷 H/接收屏障和 B 重试 | 外部副作用需要业务幂等；自动接管仍以运行时回归为准 |
| S06 [格式](../tests/archive_format.rs)、[S3 边界](../tests/s3_boundaries.rs)、[RustFS](../tests/s3_rustfs.rs) | 不可变 pack、合装、checksum/索引/Range、坏响应、挂起 Range 不占全部控制槽、multipart cancel/abort/reconcile、实际 aws-sdk-s3 与 RustFS | 受控 HTTP 边界测试和真实 RustFS 分开；不代表公网 S3 中断或掉电 |
| S07 [生命周期持久组合](../tests/lifecycle_persistence.rs)、[实现测试](../src/lifecycle/mod.rs) | 原对象授权/上传/发布/完成对账、不可逆取消、防迟到复活、替换验证/原子换引用、pin/恢复保护、全 voter 快照证明、本地回收/floor/DELETING | RustFS 场景需显式运行 ignored；新 voter 与运行时自动任务组合另验 |
| S08 [运行时](../tests/runtime_queue.rs)、[Broker 故障](../tests/runtime_support/broker_failover.rs) | 实际 SDK 的生产/接收/持久 ACK；独立 RF3 CLI 强杀 Storage/失多数派恢复；九进程 Broker 自动接管及旧 mTLS 身份被 Fenced；稳定 UDP 入口 Gateway 强杀/丢包/重排/MTU 黑洞；原 Client/Producer/Consumer 恢复；RF1/RF3 全部热正文回收后 33 条冷读 | 同进程 Runtime 与独立 OS 子进程分别识别；真实 RustFS 需显式 ignored。单机不是三区，恢复单例不是 P99 |
| S09 [成员迁移](../tests/scaling_raft.rs)、[远程闭合迁移](../tests/scaling_remote.rs) | learner/joint/stable、隔离后保留原计划、joint 中换主、完成证明私有、容量新鲜度与预算、真实 native RF3 自动替换、缺样不创建计划 | 部分远程 fixture 为语义 codec loopback；S08 另验证实际 CLI/mTLS 修复，跨区与高压力组合仍待验 |
| S10 [观测边界](../tests/observability_boundaries.rs)、[分配器](../tests/allocator_diagnostics.rs)、[堆采样](../tests/heap_profile.rs)、quota/retention 单元测试 | 字节许可、控制保留、有限指标、日志丢弃/失败/脱敏、临时过滤、真实 RSS、cgroup 解析、迟滞、同 allocator profile；原生 Prometheus/Grafana 实际 RF1 查询、29 面板和匿名只读权限，见[部署证据](../deploy/observability/README.zh-CN.md)；Linux 容器限额与 RF1 受控真 OOM 见 S12 | Linux 同 allocator 分配栈、长期趋势与浏览器画面待验；Collector 与正式开销单独记录 |
| S11 [追踪导出](../tests/tracing_export.rs)、[四角色链路](../tests/runtime_support/tracing.rs) | 官方 OTLP HTTP/protobuf、TLS 拒绝、宿主设施共存、外部 sampled 仍受本地预算限制、慢/503/超大响应/关闭与恢复；真实 SDK→Gateway→Broker 父关系、owned 提交及持久 ACK、原期限清理；Linux 普通回归覆盖内置采集器边界；官方 native Collector 在 macOS 实际 accepted 与退出端口释放 | native Collector 用例需显式 ignored，本轮 Linux 未执行；默认 nop 不持久化 trace。域名/私有 CA/mTLS 由本地 Collector 代理；公网与正式 Linux Collector 部署待验，Docker 集群默认未开 OTLP |
| S12 [Docker 脚本](../scripts/docker-validate.sh)、[真实 SDK driver](../examples/deploy_validation.rs)、[读取回收回归](../src/runtime/data_reclaim_tests.rs)、[最新 Docker 记录](evidence/2026-10-20-docker-077004e.zh-CN.md) | 冻结同版 RF1 三项、RF3 七项；真 OOM、实际 Storage leader/Broker owner 强杀、两类多数派中断及 Gateway 冷重启；原 Client/Producer/Consumer/Pending、64/80 持久前沿；历史真实 S3 回收竞态的首条/前缀、逻辑过期及 Unknown 四边界另保留原记录 | 单机共享项目内 netns，无宿主端口；最新 736 条为正确性短例，非性能或恢复分位数。Gateway 只冷重启，真实 LB/三区/长稳待验；故障中不可达采样及历史失败保留原记录 |
| M01 [有限参考模型](../tests/safety_model.rs) | 发布 18 状态/540 转换，订阅 411 状态/972 次旧 token 尝试；枚举 Unknown、迟到授权、取消、pin、丢回包 | 有限状态图，不模拟任意并发/时钟/磁盘，也不代替 S03/S05/S07 |
| P01 [性能工具与报告口径](performance.zh-CN.md)、[17 项报告](evidence/2026-10-20-local-performance.zh-CN.md) | 外部 SDK、独立 RF1 服务进程、warmup/measure/cooldown、原 PendingBatch、unique ID/offset 与最终持久 ACK；36032 条测量/2264 条预热；三 allocator、日志三档、同预算 2/4 lanes 及官方 Collector 53 span | 单消费者的有限闭环短跑；高吞吐低延迟、生产 P99.9、长稳及公平竞品未验。原 16 项缺独立子进程 exit-status 制品；样本不足高分位为 null |
| P02 [CI 性能门禁](../.github/workflows/performance.yml)、[统计校验](../scripts/performance/compare.test.mjs)、[产物校验](../scripts/performance/prepared.test.mjs)、[本轮记录](evidence/2026-10-20-ci-refactor.zh-CN.md) | 真 RF1/三进程 RF3、两版服务/SDK、独立构建 runner 与测量 runner、四个固定二进制校验、同机八对平衡交错、独立吞吐/计划发送延迟；十项比较族校正，五项显著回退否决、三项主指标精度验收；`693f9bd` 完整 32 试次及八项 Rust CI 成功，`d38fb80` 本地执行原联合脚本晋升数值基线 | RF3 两项 p99 仍 imprecise，无法排除尾退化；旧失败／不确定报告保留；远端汇总受 GitHub 计费阻塞，套餐另限制 required status |

## 构建、生成与 allocator

```sh
cargo build -p tidemq-protocol --features codegen --locked
cargo fmt --all
git diff --exit-code -- crates/tidemq-protocol/src/generated
cargo clippy -p tidemq --tests --no-deps --locked --no-default-features --features allocator-mimalloc -- -D warnings
cargo test -p tidemq --locked --no-default-features --features allocator-mimalloc --test allocator_diagnostics
cargo clippy -p tidemq --tests --no-deps --locked --no-default-features --features allocator-system -- -D warnings
cargo test -p tidemq --locked --no-default-features --features allocator-system --test allocator_diagnostics
cargo clippy -p tidemq --tests --no-deps --locked --no-default-features --features allocator-jemalloc -- -D warnings
cargo test -p tidemq --locked --no-default-features --features allocator-jemalloc --test allocator_diagnostics
```

CI 配置 Linux/macOS 和互斥 allocator，不能使用互斥 feature 的 all-features。CI 文件存在不表示远端已运行。同 allocator 的 release 栈、持有/释放、跨线程释放和符号化已在本机验证，见[原始采样记录](evidence/2026-10-19-allocator-release.zh-CN.md)；mimalloc 汇总统计不提供同等分配栈证据。

## 性能与外部前置

当前未创建云资源。真实三可用区要提供节点/区/磁盘/网络/稳定 UDP 入口与预算，再执行阶段 F；本机 zone 名称不制造故障域。报告须包含总 deadline、超时/拒绝/Unknown、生产持久 ACK 与消费持久 ACK 两套分布、积压斜率、CPU/RSS/cgroup/磁盘/网络、故障至 SDK 持久成功的时间线及全部计算/WAL/S3 成本。

Kafka/Pulsar/AutoMQ 只有在 RF、持久 ACK、订阅、批量/压缩、分区与顺序一致时才可对照。历史官方参考不算 TideMQ 实测；测试总耗时不当作恢复延迟，均值不替代 P99，阶段 P99 也不能相加。
