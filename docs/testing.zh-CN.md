# 测试方法与现有证据

当前校验包含纯单元测试、功能集成、真实外部依赖、覆盖率，以及 Kubernetes / VM 故障恢复。本次按用户要求删除性能 workflow、负载工具、比较器、性能基线、配置和专项报告；不执行性能测试，也不据功能测试耗时推断吞吐或延迟。

## CI 与覆盖率

[Rust 工作流](../.github/workflows/rust.yml) 固定 Rust 1.99.1，main push、PR 和手动 dispatch 触发。Linux 使用 ssh dev 的独立 runner VM，macOS 使用 GitHub-hosted runner。workflow 负责调度与制品，执行步骤位于 [scripts/ci](../scripts/ci/)。详细环境、入口与验收见[VM 实验室](../deploy/lab/README.zh-CN.md)。

先运行 fmt、workspace 全目标 Clippy、生产 panic / 索引审计、脚本自检、协议生成检查，再运行单元与集成。单元阶段只运行 lib / bin / example，移除外部服务环境变量；需要 RustFS、Collector 或网络服务的场景位于 tests/，Linux 显式运行真实依赖测试，缺少固定工具则失败。SDK 默认与 instrumentation 两条路径均覆盖。

固定 nextest 0.9.148，ci profile 逐测试隔离、并发为 1、不重试失败项，保留 JUnit；业务测试内部并发不变。doctest 单独由 cargo test 执行。Linux 的固定 RustFS / Collector 版本与哈希由 inventory 和制品记录，不使用用户既有数据。

Linux 单元和集成阶段直接使用 cargo-llvm-cov 0.9.1 的 `show-env --sh` 插桩构建；开始时只清理一次历史 profile，单元、SDK 默认 feature、原生集成、故障钩子及真实外部服务测试共用同一构建目录并累计 profile。测试结束后 `scripts/coverage-run.sh` 只读取已有二进制和 profile，生成 LLVM JSON、LCOV、HTML，不重新编译或执行测试。后续 coverage job 只下载报告并比较；measurement 绑定原始 LLVM 哈希、源码提交、工具版本、CI run、profile 数量和五阶段 JUnit 哈希，拒绝跨提交或跨运行混用。macOS 独立执行普通兼容测试；稳定版 doctest 不作为覆盖率证明。

[policy.json](../coverage/policy.json) 固定源码范围；[baseline.json](../coverage/baseline.json) 保留已有覆盖率基线。比较器用 BigInt 对 lines / functions / regions 原始整数比例逐项比较，任何下降失败，不设浮点容差，不额外排除源码来抬高数字。PR 使用 base 的可信工具和策略，不能替换自己的基线。当前 CI 只比较覆盖率，不自动写入或推送基线。

VM 阶段新建每次 CI 独立的 namespace、证书、RustFS a/b、数据目录和角色实例，通过 loadgen 的真实 SDK 验证持久消息、原 Pending / Consumer 恢复及三类观测入库。完成或失败均恢复故障并清理该次资源。Linux 插桩校验、集成、报告导出及 VM 故障与手动运维共享宿主 flock；只读的覆盖率报告比较不占用故障租约。VM 内的发布镜像以普通 release 构建，VM 故障场景另留独立验收结果，不混入 LLVM 覆盖率。

## 可复现验证

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --no-deps --locked -j2 --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen -- -D warnings
CARGO_BUILD_JOBS=2 cargo nextest run --workspace --all-targets --locked --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen --profile ci
cargo test --workspace --doc --locked --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen
# Linux only, from committed clean sources.
TIDEMQ_REQUIRE_EXTERNAL=1 TIDEMQ_COVERAGE=1 bash scripts/ci/validate-and-integrate.sh
node scripts/coverage-gate.mjs compare --report target/ci/coverage/llvm.json --measurement target/ci/coverage/measurement.json --output target/ci/coverage/summary.json
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
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs cargo test -p tidemq --features test-hooks --test runtime_reclamation -- --include-ignored --test-threads=1
TIDEMQ_OTELCOL_BIN=/absolute/path/to/verified/otelcol cargo test -p tidemq --test tracing_export official_collector_accepts_real_otlp_and_releases_ports -- --exact --ignored
```

`test-hooks` 仅用于确定性集成故障注入，默认服务构建不包含这些暂停点。单元测试不依赖 RustFS，也没有 `#[ignore]`；WAL 故障测试的子进程入口由父测试提供私有环境，普通运行直接返回。

冷读缓存同时受单实例字节/条目上限和节点共享持有额度约束，后者为数据准入预算的 1/8，并计入 S3 缓冲持有的完整额度。满额时返回已验证的读取结果并跳过缓存。运行时复用维护任务回收闲置 60 秒的缓存；活动读取视图继续持有自己的内存许可，不随缓存淘汰提前释放。该默认比例与时长尚未做性能校准。

## 证据索引

| 代号与入口 | 实际验证内容 | 范围限制 |
| --- | --- | --- |
| S01 [真实 QUIC](../tests/quic_transport.rs)、[分派](../tests/server_dispatch.rs)、SDK 的网络测试 | TLS/mTLS 与伪造身份拒绝、有限分类连接/流、取消隔离、control/ACK 推进、RPC 结果匹配；四 lane 单 affinity 持续请求不因 sibling 空闲断连，半帧仍 poison；原期限内关闭 owned native I/O 并重绑端口；新 1-RTT 包 ACK 探测能淘汰失联旧连接，慢业务有网络进展时不误杀 | 本机 UDP；真实 LB/NAT/CID 和同预算性能对照另验 |
| S02 [WAL](../tests/wal_journal.rs)、[分段边界](../src/wal/segmented/tests.rs)、[段发布故障](../src/wal/segmented/fault_tests.rs)、[存储](../tests/replication_storage.rs)、[Raft QUIC](../tests/raft_quic.rs)、[崩溃](../tests/replication_crash.rs)、[应用失败](../tests/application_failure.rs) | 分段 fsync/vote/log/commit、唯一 manifest、refs-only 检查点、原 index 重用／truncate、受限段搬迁、快照 reader pin 与取消、实际文件大小限制写失败、SIGKILL 发布屏障、损坏拒绝；官方 storage-v2、RF1/RF3 多数派、换主与原结果 | 完整应用快照仍非增量；新格式用新目录，拒绝旧布局。一部分测试使用受控内存网络；真实 QUIC、Linux、性能按冻结版本分别验收，进程崩溃不代表宿主掉电 |
| S03 [元数据](../tests/metadata_state.rs)、[权威](../tests/ownership_authority.rs) | CRUD 与永久身份、grant/fence/recover/activate、重试/CAS/旧 epoch、TTL 从请求开始、控制失 quorum、有限安全收尾 | 纯状态证据与 S08 的实际角色接管分别记录，前者不能代表入口故障 |
| S04 [Broker 测试](../src/broker/tests/mod.rs)、SDK 网络测试 | producer 注册/固定过期/全局高水位、canonical digest、sequence/冲突/原结果、owned 取消后提交、快照恢复、原 PendingBatch 重试 | 有限原结果窗口；SDK 进程重启没有持久 outbox |
| S05 [订阅状态](../tests/subscription_state.rs)、[持久组合](../tests/lifecycle_persistence.rs)、SDK 网络测试 | 逐条 attempt/token/ACK、洞/连续前沿、旧代次拒绝、seek、KeySerial、冷 H/接收屏障和 B 重试 | 外部副作用需要业务幂等；自动接管仍以运行时回归为准 |
| S06 [格式](../tests/archive_format.rs)、[S3 边界](../tests/s3_boundaries.rs)、[RustFS](../tests/s3_rustfs.rs) | 不可变 pack、合装、checksum/索引/Range、坏响应、挂起 Range 不占全部控制槽、multipart cancel/abort/reconcile、实际 aws-sdk-s3 与 RustFS | 受控 HTTP 边界测试和真实 RustFS 分开；不代表公网 S3 中断或掉电 |
| S07 [生命周期持久组合](../tests/lifecycle_persistence.rs)、[实现测试](../src/lifecycle/mod.rs) | 原对象授权/上传/发布/完成对账、不可逆取消、防迟到复活、替换验证/原子换引用、pin/恢复保护、全 voter 快照证明、本地回收/floor/DELETING | RustFS 场景需显式运行 ignored；新 voter 与运行时自动任务组合另验 |
| S08 [运行时](../tests/runtime_queue.rs)、[Broker 故障](../tests/runtime_support/broker_failover.rs) | 实际 SDK 的生产/接收/持久 ACK；独立 RF3 CLI 强杀 Storage/失多数派恢复；九进程 Broker 自动接管及旧 mTLS 身份被 Fenced；稳定 UDP 入口 Gateway 强杀/丢包/重排/MTU 黑洞；原 Client/Producer/Consumer 恢复；RF1/RF3 全部热正文回收后 33 条冷读 | 同进程 Runtime 与独立 OS 子进程分别识别；真实 RustFS 需显式 ignored。单机不是三区，恢复单例不是 P99 |
| S09 [成员迁移](../tests/scaling_raft.rs)、[远程闭合迁移](../tests/scaling_remote.rs) | learner/joint/stable、隔离后保留原计划、joint 中换主、完成证明私有、容量新鲜度与预算、真实 native RF3 自动替换、缺样不创建计划 | 部分远程 fixture 为语义 codec loopback；S08 另验证实际 CLI/mTLS 修复，跨区与高压力组合仍待验 |
| S10 [观测边界](../tests/observability_boundaries.rs)、[分配器](../tests/allocator_diagnostics.rs)、[堆采样](../tests/heap_profile.rs)、quota/retention 单元测试 | 字节许可、控制保留、有限指标、日志丢弃/失败/脱敏、临时过滤、真实 RSS、cgroup 解析、迟滞、同 allocator profile；原生 Prometheus/Grafana 实际 RF1 查询、29 面板和匿名只读权限，见[部署证据](../deploy/observability/README.zh-CN.md)；Linux 容器限额与 RF1 受控真 OOM 见 S12 | Linux 同 allocator 分配栈、长期趋势与浏览器画面待验；Collector 与正式开销单独记录 |
| S11 [追踪导出](../tests/tracing_export.rs)、[四角色链路](../tests/runtime_support/tracing.rs) | 官方 OTLP HTTP/protobuf、TLS 拒绝、宿主设施共存、外部 sampled 仍受本地预算限制、慢/503/超大响应/关闭与恢复；真实 SDK→Gateway→Broker 父关系、owned 提交及持久 ACK、原期限清理；Linux 普通回归覆盖内置采集器边界；官方 native Collector 在 macOS 实际 accepted 与退出端口释放 | native Collector 用例需显式 ignored，当前 Linux CI 已执行；默认 nop 不持久化 trace。域名/私有 CA/mTLS 由本地 Collector 代理；公网与正式 Linux Collector 部署待验，Docker 集群默认未开 OTLP |
| S12 [Docker 脚本](../scripts/docker-validate.sh)、[真实 SDK driver](../examples/deploy_validation.rs)、[读取回收集成测试](../tests/runtime_reclamation.rs)、[最新 Docker 记录](evidence/2026-10-20-docker-077004e.zh-CN.md) | 冻结同版 RF1 三项、RF3 七项；真 OOM、实际 Storage leader/Broker owner 强杀、两类多数派中断及 Gateway 冷重启；原 Client/Producer/Consumer/Pending、64/80 持久前沿；历史真实 S3 回收竞态的首条/前缀、逻辑过期及 Unknown 四边界另保留原记录 | 单机共享项目内 netns，无宿主端口；最新 736 条为正确性短例，非性能或恢复分位数。Gateway 只冷重启，真实 LB/三区/长稳待验；故障中不可达采样及历史失败保留原记录 |
| M01 [有限参考模型](../tests/safety_model.rs) | 发布 18 状态/540 转换，订阅 411 状态/972 次旧 token 尝试；枚举 Unknown、迟到授权、取消、pin、丢回包 | 有限状态图，不模拟任意并发/时钟/磁盘，也不代替 S03/S05/S07 |

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

CI 覆盖 Linux/macOS；allocator 专项按需手动执行，不能使用互斥 feature 的 all-features。CI 文件存在不表示远端已运行。同 allocator 的 release 栈、持有/释放、跨线程释放和符号化已在本机验证，见[原始采样记录](evidence/2026-10-19-allocator-release.zh-CN.md)；mimalloc 汇总统计不提供同等分配栈证据。


## 验收边界

当前单宿主 VM 环境不代表物理三区或宿主掉电，有限功能流量不构成容量、吞吐、尾延迟或长期稳定性结论。当前结果按源码、镜像、工具版本及真实 CI run 分开记录，失败保留原始状态。
