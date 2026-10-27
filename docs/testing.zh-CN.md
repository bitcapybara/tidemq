# 测试方法与现有证据

仓库校验分为单元、功能集成、真实外部依赖、覆盖率、前端交互，以及 Kubernetes / VM 故障恢复。测试配置、某个 job 通过、完整 pipeline 通过和实际部署验收分别记录；下文带日期的历史成功记录不代表最新提交已通过。不据功能测试耗时推断吞吐或延迟。

## 按改动选择入口

| 改动 | 先执行 | 不能替代的验证 |
| --- | --- | --- |
| 产品文档 / 站点 | `npm --prefix docs ci`、`npm --prefix docs test`、`npm --prefix docs run docs:build` | 浏览器预览、远端 Pages 实际发布、示例的真实集群行为 |
| Console 前端 | `console/web/package.json` 中的类型、lint、测试和构建脚本，以及 Rust workflow 的对应 job | 浏览器交互、真实用户凭据/权限、上游写入结果恢复和平台部署 |
| 一个 Rust crate | `cargo fmt --all`，再对修改的 package 执行 `cargo clippy -p PACKAGE --tests --no-deps --locked`，并运行相关测试 | 跨角色、外部依赖、持久故障和完整 CI |
| 跨 crate / 协议 / 恢复 | 下文 workspace 检查、对应集成与故障回归 | 实际平台 quorum、旧状态、外部存储和故障域验证 |

不使用 `cargo check` 作为 Rust 校验入口；Clippy 有发现时修复并重复到通过。文档站的构建会检查 Markdown 死链及生成 HTML 的本地路径、资源与锚点；它不会查询远程链接或验证服务行为。站点命令见[文档站维护](site.zh-CN.md)。

## Console 前端质量门禁

前端使用 Vitest/V8、React Testing Library、user-event、jsdom 和 MSW。单元测试覆盖协议解析与业务逻辑；DOM 集成测试通过实际组件、React Query 和网络 mock 检查交互、权限失效、表单及原写入结果恢复。覆盖率包含全部应用 `src/**/*.{ts,tsx}`，只排除测试和类型声明：行、语句、函数至少 80%，分支至少 70%；认证、会话、权限、写入恢复和表单模块逐文件要求行至少 90%、分支至少 80%。HTML、LCOV 和 JSON 报告保存在 `console/web/coverage/unit-integration/`。降低阈值或排除未测页面不能代替补测试。

Playwright 的 `mock-network` 套件覆盖确定性的页面操作和故障展示，不计作真实后端验收。`live` 套件通过独立 combined、Console 和 HTTPS 代理进程运行实际登录、导航、刷新、退出与凭据写入结果恢复，使用本次生成的私有身份和数据。真实凭据用例禁用 trace、视频和截图；mock 套件保留失败制品。浏览器结果与 Vitest 源码覆盖率分别统计。

```sh
npm --prefix console/web ci
npm --prefix console/web run typecheck
npm --prefix console/web run lint
npm --prefix console/web run test:coverage
npm --prefix console/web run build
npm --prefix console/web exec -- playwright install chromium firefox webkit
npm --prefix console/web run test:e2e
cargo build --locked -p tidemq -j2
npm --prefix console/web run test:e2e:live
```

真实用例启动器默认使用仓库 `target/debug/tidemq`；自定义 Cargo target 时通过 `TIDEMQ_BINARY` 指定本次构建的绝对路径。启动器完成就绪检查、有限进程退出、私有文件清理并保存清理收据。PR 运行 Chromium，main 运行 Chromium/Firefox/WebKit；Rust 最终 gate 同时要求前端覆盖率、mock 浏览器和真实后端浏览器 job 成功。真实后端 job 校验同一次 Linux 构建制品的源码提交与二进制 SHA256，fork PR 不访问自托管服务。未实际运行的检查不计作该版本验收结果。

## CI 与覆盖率

[Rust 工作流](https://github.com/bitcapybara/tidemq/blob/main/.github/workflows/rust.yml) 固定 Rust 1.99.1，main push、PR 和手动 dispatch 触发。Linux 使用 ssh dev 的 runner VM 及其中新增的一个容器 runner，macOS 使用 GitHub-hosted runner。workflow 负责调度与制品，执行步骤位于 [scripts/ci](https://github.com/bitcapybara/tidemq/tree/main/scripts/ci)。详细环境、入口与验收见[VM 实验室](https://github.com/bitcapybara/tidemq/blob/main/deploy/lab/README.zh-CN.md)。

先运行 fmt、workspace 全目标 Clippy、生产 panic / 索引审计、脚本自检、协议生成检查，再运行单元与集成。单元阶段只运行 lib / bin / example，移除外部服务环境变量；需要 RustFS、Collector 或网络服务的场景位于 tests/，Linux 显式运行真实依赖测试，缺少固定工具则失败。SDK 默认与 instrumentation 两条路径均覆盖。

固定 nextest 0.9.148，ci profile 逐测试隔离、并发为 1、不重试失败项，保留 JUnit；业务测试内部并发不变。doctest 单独由 cargo test 执行。Linux 的固定 RustFS / Collector 版本与哈希由 inventory 和制品记录，不使用用户既有数据。

Linux 分为两条并行流程：容器执行静态检查、单元、默认 SDK 和普通集成；原生 runner 执行故障钩子、真实外部服务和 K8s 黑盒/VM 故障。两边均使用 cargo-llvm-cov 0.9.1 的 `show-env --sh`，各自开始时清理一次历史 profile，使用独立 target 和带流程前缀的 profile 文件名。Cargo registry/git 下载缓存共用，任务持有共享缓存锁，清理必须取得独占锁；编译和覆盖率目录不共用。K8s 阶段把插桩生成的 `tidemq` debug 二进制部署为 Metadata / Storage / Broker / Gateway 共 10 个独立实例，在 loadgen VM 通过真实 QUIC SDK 和 CLI 调用服务。客户端自身的覆盖率不能代替服务端覆盖率。

两条流程分别上传已有 ELF 二进制、profile、JUnit 和清单，短期制品为 `coverage-inputs-native` / `coverage-inputs-lab`。[bundle.mjs](https://github.com/bitcapybara/tidemq/blob/main/scripts/coverage/bundle.mjs) 校验相同源码提交、CI run、工作区绝对路径、工具版本及每个文件的大小/SHA256；拒绝路径穿越、符号链接、缺失流程和篡改输入。相同文件去重，不同 feature 的二进制均保留。coverage job 下载两份输入并调用 [coverage-run.sh](https://github.com/bitcapybara/tidemq/blob/main/scripts/coverage-run.sh)，统一导出 LLVM JSON、LCOV、HTML、原始 `profiles.tar.gz` 和 `inputs.json`，随后比较基线，全程不重编译或重跑 Rust 测试。measurement 绑定原始 LLVM 哈希、源码提交、工具版本、CI run、profile 数量、五阶段 JUnit 哈希，以及 [API 采集清单](https://github.com/bitcapybara/tidemq/blob/main/scripts/coverage/api.mjs)。清单记录两种部署二进制的 SHA256、镜像、10 个 Pod 的 UID/角色/worker/imageID、各个 profile 的大小/哈希、9 个远程用例的报告哈希及行为断言。缺少任一角色、客户端、行为结果或跨提交/跨运行混用均失败。macOS 独立执行普通兼容测试；稳定版 doctest 不作为覆盖率证明。

[policy.json](https://github.com/bitcapybara/tidemq/blob/main/coverage/policy.json) 固定源码范围；[baseline.json](https://github.com/bitcapybara/tidemq/blob/main/coverage/baseline.json) 保留已有覆盖率基线。比较器用 BigInt 对 lines / functions / regions 原始整数比例逐项比较，任何下降失败，不设浮点容差，不额外排除源码来抬高数字。PR 使用 base 的可信工具和策略，不能替换自己的基线。当前 CI 只比较覆盖率，不自动写入或推送基线。

VM 阶段新建每次 CI 独立的 namespace、证书、RustFS a/b、数据目录和角色实例，通过 loadgen 的真实 SDK 验证持久消息、原 Pending / Consumer 恢复及三类观测入库。只有故障/部署流程与手动运维共享宿主 flock；容器普通测试无需该租约。两边的 profile 和二进制上传后才执行完成清理；coverage job 从制品重建报告目录，不占用故障租约。常驻公共 workload 和手动普通验收仍使用 release 构建。

服务端 `LLVM_PROFILE_FILE=/coverage/node-ID-%h-%p-%m.profraw` 写入该 run 独享的 worker 目录；loadgen 的 CLI/SDK 使用独享 `profiles/client-%h-%p-%m.profraw`。收集业务结果和日志后，将该 CI namespace 的 10 个 StatefulSet 受控缩容为 0，等待进程正常处理 SIGTERM 并退出，然后逐实例通过 SSH 回收非空 profile，校验并复制到原生覆盖率构建目录。最后才删除 namespace、PV、worker profile、RustFS fixture 和私有构建；取消留下的资源由 runner 维护机制重试。最终覆盖率导出及制品上传完成后，才运行任务完成清理钩子。

LLVM 正常退出写入机制及文件名模板见 [Rust 官方文档](https://doc.rust-lang.org/rustc/instrument-coverage.html)，外部程序与已有测试合并的方式见 [cargo-llvm-cov 官方说明](https://github.com/taiki-e/cargo-llvm-cov#usage)。VM 断电或 SIGKILL 前来不及写出的计数可能丢失，不能把最终报告当作每次崩溃前执行路径的完整记录；恢复后的全部 10 个实例必须正常退出并产生有效 profile。故障用例是否满足行为契约仍独立断言，不能用覆盖率数字代替。

## 远程 API 黑盒断言

入口为 [tests/deployment/driver.rs](https://github.com/bitcapybara/tidemq/blob/main/tests/deployment/driver.rs)，保留 `cargo build --example deploy_validation` 构建接口。纯解析/账本单元测试只测试本地逻辑，外部服务行为由 [lab.sh](https://github.com/bitcapybara/tidemq/blob/main/scripts/ci/lab.sh) 在部署后显式运行，不混入 lib/bin 单元测试。

| 场景 | 预期行为 |
|---|---|
| `api-contract` 管理与授权 | 创建/分页列举 Tenant、Namespace、Principal、Credential；Topic 身份稳定；Namespace Topic 限额拒绝超额；只读凭据拒绝管理；撤销 token 后拒绝访问；查询集群/数据组；删除订阅、Topic 和父资源 |
| `api-contract` 消息语义 | Producer 提交；Queue 收到相同 ID/正文/key，NACK 后保持 ID 重投并持久 ACK；Stream 独立收到相同消息，累计 ACK 后重连不会重放已确认消息 |
| `smoke` / `replay` | 完整账本无丢失、正文错误和未确认消息；延迟加入持久订阅后能读取归档消息 |
| worker 掉电 / 数据网隔离 / 多数派丢失恢复 | 故障窗口、原 Pending/Consumer 身份、恢复后持久前沿及有限关闭满足原断言 |
| 控制面故障 / RustFS a 隔离 / `recovered` | 备用控制面可用、独立 RustFS b 存活、恢复后的完整生产/消费/ACK |

独立 watchdog 另验证 VM 暂停后自动恢复；Loki/Tempo/Prometheus 另验证同一 run 的日志、trace、metrics 可查询。以上是有限正确性矩阵，未声称穷尽所有管理 API、部署形态或故障组合。本轮选用现有多角色 K8s 环境，未另加单机插桩部署。

`tests/` 目录包含不同层次的集成测试，不能把目录位置或总用例数直接当成完整端到端覆盖。状态机/受控网络测试检查局部契约；`RunningNode` fixture 在同一测试进程启动真实角色与 QUIC；CLI fixture 再检查独立 OS 进程的启动、崩溃与恢复；上述 K8s driver 才从外部客户端访问已部署的服务二进制。各层互补，K8s 用例没有自动继承原生集成测试的全部覆盖。

| 端到端范围 | 当前自动校验与缺口 |
|---|---|
| 消息、身份与权限 | K8s 已有表中的主链路；尚未对每个 SDK/管理 API、错误分类及边界参数建立远程用例 |
| 扩缩容、成员替换、Shard 迁移、节点排空 | 原生集成测试有功能和故障回归；本轮 K8s 黑盒矩阵尚未包含这些管理操作 |
| 对象存储中断 | K8s 检查 fixture a 故障窗口的业务和独立 b 的可用性；尚未显式强制冷读命中故障 S3，再断言恢复后归档积压清空及数据闭合 |
| 观测与告警 | K8s 检查本次 namespace 的 10 个指标目标以及可查询日志、trace；不等于逐条校验三类信号关联，也未覆盖真实告警触发、通知送达和恢复闭环 |
| 部署形态 | 本轮为 Linux RF3 多角色 K8s；单机插桩部署、真实跨可用区和宿主/整盘故障不在该矩阵内 |

完整 [CI 40322221174](https://github.com/bitcapybara/tidemq/actions/runs/40322221174) 已通过，测试源码为 `87b3442ce1c26a3c646284bcff96c5e511aded57`。两个 Linux job 在 `2026-10-26T12:23:59Z` 同时启动；容器校验脚本用时 19 分 40 秒，原生故障/部署脚本用时 34 分 8 秒，包含各自编译和前置检查，制品上传另计。分阶段 JUnit 记录如下；这些测试执行时间不能作为吞吐或延迟结论。

| 平台 / 阶段 | 通过数量 | JUnit 秒数 |
|---|---:|---:|
| Linux 单元 | 284 | 46.522 |
| Linux 默认 SDK 单元 | 29 | 0.599 |
| Linux 普通集成 | 267 | 693.922 |
| Linux 故障钩子集成 | 75 | 647.158 |
| Linux 真实外部依赖 | 16 | 109.238 |
| macOS 单元 / 默认 SDK / 普通集成 / 故障钩子 | 655 | 1001.870 |

9 个 K8s/API 场景、独立 watchdog、日志/trace/metrics 查询和退出清理均通过。最终报告合并 922 份 profile，其中 10 份服务端、11 份远程客户端；已逐个核对归档内容的大小和 SHA256，并核对全部 profile 出现在实际 LLVM 合并输入中。覆盖率 lines 89.36%、functions 80.98%、regions 86.15%，原基线和策略未修改，门禁通过。可长期定位的环境、哈希、失败尝试及验证记录见 [runner 验收](https://github.com/bitcapybara/tidemq/blob/main/deploy/lab/evidence/runner-maintenance.json)。

第一次 coverage job 因 GitHub 制品下载停滞超时；随后只重跑报告 job，复用第一次尝试的两份输入和五份 JUnit，Rust 测试没有重跑。成功尝试的下载耗时 10 分 15 秒，恢复/导出脚本耗时 80 秒。当前输入未压缩总量约 3.99 GiB；后续配置将输入压缩等级设为 6、报告 job 总期限设为 45 分钟，并将恢复字节上限设为 8 GiB。该配置调整经过 Node 自检和 YAML 解析，尚未另跑整条 CI；上述完整运行验证的是 `87b3442`。

其中 62 个故障注入用例与普通集成测试同名；成员迁移和 Shard 转移等同名用例具有条件编译的额外故障断言，不能直接按重名删除。目前 `ci` profile 的 `test-threads = 1` 保持不变；拆分的是两条独立 job，同时保留原断言、五份 JUnit、插桩二进制和 profile。VM/网络故障仍持有实验室独占租约。测试层次的缺口不因增加 runner 自动消失。

功能测试不要求固定 CPU 吞吐。自动 split/merge 的消息 drain 使用 20 秒无进展期限和 2 分钟总上限，继续逐条验证完整消息集合、正文、持久 ACK 和最终 merge；整批运行时间不再共用一个 20 秒期限。曾有失败在整批约 27 秒时发生，但距离上次 ACK 不足 1 毫秒，诊断表明是整批预算耗尽。Combined RF3 的只读 `topic_info` 查询只对明确的 `QuotaExceeded + Rejected + retryable` 在原 40 秒 phase 内遵守背压；其他错误直接失败，四个 Topic 各 64 次成功查询、发送和 ACK 的断言不变。生产代码、默认 admission 容量和 nextest 的零重试设置均未因此修改。

## 可复现验证

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --no-deps --locked -j2 --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen -- -D warnings
CARGO_BUILD_JOBS=2 cargo nextest run --workspace --all-targets --locked --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen --profile ci
cargo test --workspace --doc --locked --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen
# Linux only, from committed clean sources.
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs \
TIDEMQ_OTELCOL_BIN=/absolute/path/to/verified/otelcol \
TIDEMQ_REQUIRE_EXTERNAL=1 TIDEMQ_COVERAGE=1 \
bash scripts/ci/with-cargo-cache.sh bash scripts/lab/with-lease.sh bash scripts/ci/validate-and-integrate.sh
node scripts/coverage-gate.mjs compare --report target/ci/coverage/llvm.json --measurement target/ci/coverage/measurement.json --output target/ci/coverage/summary.json
```

默认 mimalloc 调整后的 249 项串行测试、显式 System/jemalloc 构建、原始并行失败与 mimalloc-pprof 候选验证见[本次验证记录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-mimalloc-default.zh-CN.md)。

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
| S01 [真实 QUIC](https://github.com/bitcapybara/tidemq/blob/main/tests/quic_transport.rs)、[分派](https://github.com/bitcapybara/tidemq/blob/main/tests/server_dispatch.rs)、SDK 的网络测试 | TLS/mTLS 与伪造身份拒绝、有限分类连接/流、取消隔离、control/ACK 推进、RPC 结果匹配；四 lane 单 affinity 持续请求不因 sibling 空闲断连，半帧仍 poison；原期限内关闭 owned native I/O 并重绑端口；新 1-RTT 包 ACK 探测能淘汰失联旧连接，慢业务有网络进展时不误杀 | 本机 UDP；真实 LB/NAT/CID 和同预算性能对照另验 |
| S02 [WAL](https://github.com/bitcapybara/tidemq/blob/main/tests/wal_journal.rs)、[分段边界](https://github.com/bitcapybara/tidemq/blob/main/src/wal/segmented/tests.rs)、[段发布故障](https://github.com/bitcapybara/tidemq/blob/main/src/wal/segmented/fault_tests.rs)、[存储](https://github.com/bitcapybara/tidemq/blob/main/tests/replication_storage.rs)、[Raft QUIC](https://github.com/bitcapybara/tidemq/blob/main/tests/raft_quic.rs)、[崩溃](https://github.com/bitcapybara/tidemq/blob/main/tests/replication_crash.rs)、[应用失败](https://github.com/bitcapybara/tidemq/blob/main/tests/application_failure.rs) | 分段 fsync/vote/log/commit、唯一 manifest、refs-only 检查点、原 index 重用／truncate、受限段搬迁、快照 reader pin 与取消、实际文件大小限制写失败、SIGKILL 发布屏障、损坏拒绝；官方 storage-v2、RF1/RF3 多数派、换主与原结果 | 完整应用快照仍非增量；新格式用新目录，拒绝旧布局。一部分测试使用受控内存网络；真实 QUIC、Linux、性能按冻结版本分别验收，进程崩溃不代表宿主掉电 |
| S03 [元数据](https://github.com/bitcapybara/tidemq/blob/main/tests/metadata_state.rs)、[权威](https://github.com/bitcapybara/tidemq/blob/main/tests/ownership_authority.rs) | CRUD 与永久身份、grant/fence/recover/activate、重试/CAS/旧 epoch、TTL 从请求开始、控制失 quorum、有限安全收尾 | 纯状态证据与 S08 的实际角色接管分别记录，前者不能代表入口故障 |
| S04 [Broker 测试](https://github.com/bitcapybara/tidemq/blob/main/src/broker/tests/mod.rs)、SDK 网络测试 | producer 注册/固定过期/全局高水位、canonical digest、sequence/冲突/原结果、owned 取消后提交、快照恢复、原 PendingBatch 重试 | 有限原结果窗口；SDK 进程重启没有持久 outbox |
| S05 [订阅状态](https://github.com/bitcapybara/tidemq/blob/main/tests/subscription_state.rs)、[持久组合](https://github.com/bitcapybara/tidemq/blob/main/tests/lifecycle_persistence.rs)、SDK 网络测试 | 逐条 attempt/token/ACK、洞/连续前沿、旧代次拒绝、seek、KeySerial、冷 H/接收屏障和 B 重试 | 外部副作用需要业务幂等；自动接管仍以运行时回归为准 |
| S06 [格式](https://github.com/bitcapybara/tidemq/blob/main/tests/archive_format.rs)、[S3 边界](https://github.com/bitcapybara/tidemq/blob/main/tests/s3_boundaries.rs)、[RustFS](https://github.com/bitcapybara/tidemq/blob/main/tests/s3_rustfs.rs) | 不可变 pack、合装、checksum/索引/Range、坏响应、挂起 Range 不占全部控制槽、multipart cancel/abort/reconcile、实际 aws-sdk-s3 与 RustFS | 受控 HTTP 边界测试和真实 RustFS 分开；不代表公网 S3 中断或掉电 |
| S07 [生命周期持久组合](https://github.com/bitcapybara/tidemq/blob/main/tests/lifecycle_persistence.rs)、[实现测试](https://github.com/bitcapybara/tidemq/blob/main/src/lifecycle/mod.rs) | 原对象授权/上传/发布/完成对账、不可逆取消、防迟到复活、替换验证/原子换引用、pin/恢复保护、全 voter 快照证明、本地回收/floor/DELETING | RustFS 场景需显式运行 ignored；新 voter 与运行时自动任务组合另验 |
| S08 [运行时](https://github.com/bitcapybara/tidemq/blob/main/tests/runtime_queue.rs)、[Broker 故障](https://github.com/bitcapybara/tidemq/blob/main/tests/runtime_support/broker_failover.rs) | 实际 SDK 的生产/接收/持久 ACK；独立 RF3 CLI 强杀 Storage/失多数派恢复；九进程 Broker 自动接管及旧 mTLS 身份被 Fenced；稳定 UDP 入口 Gateway 强杀/丢包/重排/MTU 黑洞；原 Client/Producer/Consumer 恢复；RF1/RF3 全部热正文回收后 33 条冷读 | 同进程 Runtime 与独立 OS 子进程分别识别；真实 RustFS 需显式 ignored。单机不是三区，恢复单例不是 P99 |
| S09 [成员迁移](https://github.com/bitcapybara/tidemq/blob/main/tests/scaling_raft.rs)、[远程闭合迁移](https://github.com/bitcapybara/tidemq/blob/main/tests/scaling_remote.rs) | learner/joint/stable、隔离后保留原计划、joint 中换主、完成证明私有、容量新鲜度与预算、真实 native RF3 自动替换、缺样不创建计划 | 部分远程 fixture 为语义 codec loopback；S08 另验证实际 CLI/mTLS 修复，跨区与高压力组合仍待验 |
| S10 [观测边界](https://github.com/bitcapybara/tidemq/blob/main/tests/observability_boundaries.rs)、[分配器](https://github.com/bitcapybara/tidemq/blob/main/tests/allocator_diagnostics.rs)、[堆采样](https://github.com/bitcapybara/tidemq/blob/main/tests/heap_profile.rs)、quota/retention 单元测试 | 字节许可、控制保留、有限指标、日志丢弃/失败/脱敏、临时过滤、真实 RSS、cgroup 解析、迟滞、同 allocator profile；原生 Prometheus/Grafana 实际 RF1 查询、29 面板和匿名只读权限，见[部署证据](https://github.com/bitcapybara/tidemq/blob/main/deploy/observability/README.zh-CN.md)；Linux 容器限额与 RF1 受控真 OOM 见 S12 | Linux 同 allocator 分配栈、长期趋势与浏览器画面待验；Collector 与正式开销单独记录 |
| S11 [追踪导出](https://github.com/bitcapybara/tidemq/blob/main/tests/tracing_export.rs)、[四角色链路](https://github.com/bitcapybara/tidemq/blob/main/tests/runtime_support/tracing.rs) | 官方 OTLP HTTP/protobuf、TLS 拒绝、宿主设施共存、外部 sampled 仍受本地预算限制、慢/503/超大响应/关闭与恢复；真实 SDK→Gateway→Broker 父关系、owned 提交及持久 ACK、原期限清理；Linux 普通回归覆盖内置采集器边界；官方 native Collector 在 macOS 实际 accepted 与退出端口释放 | native Collector 用例需显式 ignored，当前 Linux CI 已执行；默认 nop 不持久化 trace。域名/私有 CA/mTLS 由本地 Collector 代理；公网与正式 Linux Collector 部署待验，Docker 集群默认未开 OTLP |
| S12 [Docker 脚本](https://github.com/bitcapybara/tidemq/blob/main/scripts/docker-validate.sh)、[真实 SDK driver](https://github.com/bitcapybara/tidemq/blob/main/tests/deployment/driver.rs)、[读取回收集成测试](https://github.com/bitcapybara/tidemq/blob/main/tests/runtime_reclamation.rs)、[最新 Docker 记录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-docker-077004e.zh-CN.md) | 冻结同版 RF1 三项、RF3 七项；真 OOM、实际 Storage leader/Broker owner 强杀、两类多数派中断及 Gateway 冷重启；原 Client/Producer/Consumer/Pending、64/80 持久前沿；历史真实 S3 回收竞态的首条/前缀、逻辑过期及 Unknown 四边界另保留原记录 | 单机共享项目内 netns，无宿主端口；最新 736 条为正确性短例，非性能或恢复分位数。Gateway 只冷重启，真实 LB/三区/长稳待验；故障中不可达采样及历史失败保留原记录 |
| M01 [有限参考模型](https://github.com/bitcapybara/tidemq/blob/main/tests/safety_model.rs) | 发布 18 状态/540 转换，订阅 411 状态/972 次旧 token 尝试；枚举 Unknown、迟到授权、取消、pin、丢回包 | 有限状态图，不模拟任意并发/时钟/磁盘，也不代替 S03/S05/S07 |

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

CI 覆盖 Linux/macOS；allocator 专项按需手动执行，不能使用互斥 feature 的 all-features。CI 文件存在不表示远端已运行。同 allocator 的 release 栈、持有/释放、跨线程释放和符号化已在本机验证，见[原始采样记录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-19-allocator-release.zh-CN.md)；mimalloc 汇总统计不提供同等分配栈证据。


## 验收边界

当前单宿主 VM 环境不代表物理三区或宿主掉电，有限功能流量不构成容量、吞吐、尾延迟或长期稳定性结论。当前结果按源码、镜像、工具版本及真实 CI run 分开记录，失败保留原始状态。
