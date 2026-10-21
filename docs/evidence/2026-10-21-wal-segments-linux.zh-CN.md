# 分段 WAL 的 Linux 与 Docker 验证证据

本文保留 `4f79a50`、`426ff65` 的历史运行及当时的诊断状态。后续 `80c7659` 使用独立源码、镜像和新卷重新验证，见[本版 Linux / Docker 证据](2026-10-21-wal-segments-linux-80c7659.zh-CN.md)；结果按提交分别判断。

## 验收范围与版本

本文按源码提交区分结果。`4f79a5012bbc5e4edd18663e84db1764bac99b15` 已完成下述 Linux 检查、真实 RustFS 测试以及新建 RF1/RF3 Docker 十项场景。各场景的 committed、received、acknowledged 和 frontier 均达到期望值，十项合计 736 条。该计数来自不同场景，不能解释为同一条全局消息流。

后续产品提交 `426ff6598c7b6a4337cc07ca79ac70d51d12b413` 包含 Core admission 等待调整。普通 Linux 完整 nextest 已通过 351/351，其他 release 前检查也已完成；该提交的受保护覆盖率终态为 350 PASS / 1 FAIL / 13 skipped，explicit replacement 失败正在另行诊断。该提交没有新 release 镜像或 Docker 运行。本文不把前一提交的 Docker 结果用于验收后一提交，不宣称覆盖率、性能基线或最终 CI 已通过。

本文是验证记录，不改变恢复协议、CI 策略、超时、容量阈值或性能基线。覆盖率的原始计数及门禁结果由[覆盖率证据](2026-10-21-wal-segments-coverage.zh-CN.md)单独记录。

## 源码、工具和镜像身份

所有源码由对应提交的 `git archive` 和独立 Git bundle 导入新的私有目录，恢复 Git HEAD/index 后验证 tracked clean，再以只读方式挂载。生成代码使用单独的可写目录，正常检查后与冻结源逐字节比较。依赖 target 与覆盖率 target 分离；复用依赖产物前执行 `cargo clean --workspace --locked`，避免不同源码时间戳导致旧 workspace 产物复用。

| 身份 | `4f79a50` | `426ff65` |
| --- | --- | --- |
| 完整提交 | `4f79a5012bbc5e4edd18663e84db1764bac99b15` | `426ff6598c7b6a4337cc07ca79ac70d51d12b413` |
| Git tree | `bdf169b11d95da4fafc40e94c40a2f5290b3861f` | `773e9d7f261087fb9d1585cb8bc069e3445ba68c` |
| 源码 archive SHA-256 | `690844529bf2da6d10807b7344a7195c81bd780181c5971517a3710b55fc1151` | `deea37a033976a6f8f2f39d8d5da3533d52f048785e72f588a719da3935208bb` |
| Git bundle SHA-256 | `e79153e1e485ddd9311c4e0cb759438ea1fbdece0a2808dc91018d670c37420c` | `9aa4e0e1ab86d3443dd722daa865eac4848bf1fe033e0ac86ec31483551fce59` |

两版 `Cargo.lock` 的 SHA-256 均为 `d4c609c32697a3d04f3b9b235bb746943804a7fac9fc63c51da4db6f2e90d63e`。新建专用 Cargo registry 后，按 lock checksum 核验 419 个 crate archive 及其 sparse index 版本/checksum；正式构建在 `--network none`、offline 配置下执行，没有复制凭据或修改旧全局 Cargo 配置。

运行环境为 `ssh dev` 的 Ubuntu 24.04/x86_64、内核 `7.0.0-28-generic`、Docker 29.9.1、Compose 5.6.0。使用已有、经核验的 Rust builder 镜像 `sha256:86fd5dd1673b5f458ceeec96cd228e2580402e92edae806605fc13d7864ee291`，其中 rustc/cargo 为 1.99.1、LLVM 为 23.1.1；设置 `RUSTUP_TOOLCHAIN=1.99.1`，没有使用宿主 Rust 1.96。普通门禁容器为 4 CPU、8 GiB RAM、RAM+swap 总额 8 GiB、512 PID、UID/GID 10001、只读根目录、无网络、drop ALL capabilities 和 no-new-privileges。Cargo 编译 jobs=2；覆盖率使用不同 target/CargoHome 和原有 2 CPU 限额。

其他实际工具：

| 工具 | 版本 / SHA-256 |
| --- | --- |
| cargo-nextest | 0.9.148；二进制 `53470cfc9b5cbcd96780db2d348700d76af962467503791f1f8a8824d8967e5c` |
| Node.js | v22.23.3；二进制 `fde6a4bf8d0562f7751d1a2d6cb9b417c4cfe107bbcb0aa3e9a24e125e348f48` |
| RustFS | 1.0.3；官方二进制 `c14fd1216f99efba9cac4bb469a5fa31ea9e461b82b79a28b69280e057613c93` |
| cache hint helper Python | 3.12.3；相邻 `.venv`，仅标准库 |

`4f79a50` 的 release 运行镜像为 `tidemq-wal-segments:4f79a50-xnfbxhby`，ID `sha256:730a56090d4f12ac668c659ffc666127a239f521fe3e3e9b4617752750ef9842`，amd64、用户 `10001:10001`。固定基础镜像为 `debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587`。镜像内重新计算的二进制 SHA-256 与 release 产物一致：

| 二进制 | SHA-256 |
| --- | --- |
| `tidemq` | `318b249944aa9e9db1bc1b1911e534bdb356cf03c2177258ce87918543dccda2` |
| `deploy_validation` | `c371e2c1fe459d307d69843b5f34ac3a9310cc114fe8eaf5f9f079c7a0472fb2` |

## 普通 Linux 检查

主检查使用 feature 集 `tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen`，命令在冻结源码目录执行：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --no-deps --locked \
  --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen \
  -- -D warnings
cargo clippy --workspace --lib --bins --no-deps --locked \
  --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen \
  -- -D warnings -D clippy::unwrap_used -D clippy::expect_used \
  -D clippy::panic -D clippy::unimplemented -D clippy::todo \
  -D clippy::unreachable -D clippy::indexing_slicing
cargo nextest run --workspace --all-targets --locked \
  --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen \
  --profile ci
cargo test --workspace --doc --locked \
  --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen
cargo clippy -p tidemq-sdk --all-targets --no-deps --locked -- -D warnings
cargo nextest run -p tidemq-sdk --all-targets --locked --profile ci
cargo run --locked --bin tidemq -- validate-config --config config/local.toml
cargo run --locked --bin tidemq -- allocator-probe --blocks 2
node --test scripts/coverage/*.test.mjs scripts/performance/*.test.mjs
```

`4f79a50` 的结果如下。每一步均有原始日志、退出码和起止时间；没有运行 `cargo check`。

| 检查 | 真实终态 |
| --- | --- |
| fmt、all-targets strict Clippy | exit 0 |
| 生产 lib/bins panic/indexing 审计 | exit 0 |
| 生成代码与冻结源码比较 | 无差异，exit 0 |
| workspace nextest | 347/347 PASS，13 skipped，165.912 s，exit 0 |
| doctests | 4 PASS，exit 0 |
| SDK 不开可选 feature 的 strict Clippy | exit 0 |
| SDK 不开可选 feature 的 nextest | 37/37 PASS，10.269 s，exit 0 |
| 配置检查 | exit 0 |
| allocator-probe | exit 0；实际 backend=`mimalloc`，native_version=`30302`，作用范围为 service binary |
| Node 信任边界测试 | 44/44 PASS，exit 0 |
| 下述精确选择的真实 RustFS 测试 | 10/10 PASS，141 skipped，38.271 s，exit 0 |
| release：service binary 与部署验证 example | exit 0 |

真实 RustFS 测试显式排除了必须由父测试传入环境的内部 crash child；父测试仍在普通 workspace cohort 中运行。最终选择命令为：

```bash
cargo nextest run -p tidemq --locked \
  --test s3_rustfs --test lifecycle_compaction --test lifecycle_persistence \
  --test runtime_queue --lib --run-ignored only --profile ci \
  -E 'not test(wal::segmented::fault_tests::crash_boundary_child)'
cargo build -p tidemq --release --locked --bin tidemq --example deploy_validation
```

`4f79a50` 的结果来自保留原文的 normal-v3 和修正选择器后的 normal-v4 两个运行；不能表述为一个从头到尾未修改的私有 harness 全绿。normal-v3 已完成前述主检查，但错误地把内部 child 也作为独立 ignored 测试选择，最终 exit 100；normal-v4 的十项真实 RustFS 和 release 均 exit 0。失败原文未覆盖。

## `426ff65` 的普通检查与真实停止范围

独立只读源码和新日志目录 `normal-426ff65` 使用同样的命令、限额和透明 build/cache 边界，未重放或筛选失败测试。结果为：

| 检查 | 真实终态 |
| --- | --- |
| fmt、all-targets strict Clippy、生产 panic/indexing 审计 | exit 0 |
| 生成代码比较 | 3 个文件逐字节相同，exit 0；终态再次核验冻结源 tracked clean |
| workspace nextest | 351/351 PASS，13 skipped，167.902 s，exit 0 |
| doctests | 4 PASS，exit 0 |
| SDK 无可选 feature 的 Clippy / nextest | exit 0；37/37 PASS，10.443 s |
| 配置、mimalloc allocator-probe | exit 0 |
| Node 信任边界测试 | 44/44 PASS，exit 0 |
| 真实 RustFS 十项 | 10/10 PASS，141 skipped，36.877 s，exit 0 |
| release / 新镜像 / 新 RF1/RF3 Docker | 未执行 |

原完整普通 nextest 中，`native_runtime_scaling_explicit_replacement_preserves_original_sdk_traffic_and_ack` 为 PASS，3.922 s。它只是与同提交覆盖率失败的对照；覆盖率原终态 350/1/13 仍然失败，不能由普通测试通过改记为成功。覆盖率失败记录的 phase 是 `complete metadata`，返回 Rejected（code 6、outcome 3、retryable=true）；截至本记录，尚未完成因果定位。

这里存在一次必须披露的 harness 操作：协调方要求暂停新的 release/Docker 后，在 **2026-10-20T18:15:32.357414Z**、main nextest 仍在运行时，修改了私有 `run-gates.sh` 尾部的 release 行。采用等字节长度写入，所有 release 前命令保持原文；但这仍是**运行中修改私有 harness**，不能声称原 harness 未修改整轮通过。Bash 可能预读文件，不能仅凭修改动作认定停止成功。

实际终态日志在 18:22:18Z 完成 RustFS 后打印 `All pre-release gates completed; release paused by coordinator` 并 exit 0；没有 `release.log`、新安装二进制或 `426ff65` 镜像。故本记录仅确认上述 release 前检查。原始 harness SHA-256 为 `1ef93dbab4b5aa8776544ddb89779e8bb788969e2d70afccdd796592d2dc2711`，修改后为 `cdf89c4d2bd7d54c10e9bf5c0cd8e2ce6a3f1a65b0fb45c8abc9a6ff7499b9d1`。保留了[原文](wal-segments-linux/426ff65-run-gates-original.sh.txt)、[改后原文](wal-segments-linux/426ff65-run-gates-edited.sh.txt)、[精确 diff](wal-segments-linux/426ff65-harness-change.diff.txt)与[编辑时点](wal-segments-linux/20261021-426ff65-harness-change.json)。仓库产品源码、仓库 gate 脚本和覆盖率脚本没有改变。

[普通门禁终态摘要](wal-segments-linux/20261021-426ff65-normal.json)包含 container ID、原限额和每一步 exit。复核原有 68 个容器仍无变化；两个已准备的 `426ff65` 私有项目容器数均为 0，见[隔离比较](wal-segments-linux/20261021-426ff65-resource-isolation.json)。未来新冻结提交需重新执行启动后不再修改的完整 harness、release 和新卷 Docker，不能沿用本次停止范围作为替代。

## RF1 / RF3 新卷 Docker 十项场景

仅使用 `4f79a50` 的新镜像、新项目、新卷。RF1 项目 `tidemq-validation-rf1-ae539f145ddf9ff5`，RF3 项目 `tidemq-validation-rf3-ef48e540e2e42a4d`；全部通过仓库现有 `scripts/docker-validate.sh STATE CASE` 执行，没有重写故障行为、放宽阈值或自动重跑失败场景。

| 模式 | 场景 | committed / received / durable ACK / frontier | harness exit |
| --- | --- | --- | --- |
| RF1 | smoke | 64 / 64 / 64 / 64 | 0 |
| RF1 | replay | 64 / 64 / 64 / 64 | 0 |
| RF1 | forced OOM | 80 / 80 / 80 / 80 | 0 |
| RF3 | smoke | 64 / 64 / 64 / 64 | 0 |
| RF3 | replay | 64 / 64 / 64 / 64 | 0 |
| RF3 | storage leader kill | 80 / 80 / 80 / 80 | 0 |
| RF3 | broker owner kill | 80 / 80 / 80 / 80 | 0 |
| RF3 | metadata quorum loss / restore | 80 / 80 / 80 / 80 | 0 |
| RF3 | storage quorum loss / restore | 80 / 80 / 80 / 80 | 0 |
| RF3 | gateway kill | 80 / 80 / 80 / 80 | 0 |

十项汇总为 passed，driver exit 0；[原始矩阵摘要](wal-segments-linux/20261021-4f79a50-docker.json)保留每项起止时间、验证容器 ID、ledger 和证据目录。quorum 故障依据现有脚本检查多数派丢失时不产生成功 receipt，并保留原 pending batch 恢复后完成；不是仅检查进程重启成功。

OOM 场景通过现有 harness 将 RF1 node1 的内存上限从 384 MiB 临时降至 6 MiB，观察到 `OOMKilled=true` 和 exit 137，再恢复原限额并验证 80 条消息。它证明受控 OOM 恢复路径，不证明默认限额下正常负载会发生 OOM。

运行使用真实 QUIC/TLS 与 RustFS S3。角色容器均为 UID/GID 10001、只读根目录、drop ALL capabilities、no-new-privileges、PID 128、0.5 CPU、384 MiB，RAM+swap 总限额等于 RAM；RustFS 的 RF1/RF3 限额分别为 512 MiB / 1 GiB。私有 bridge network 为 internal，未发布宿主端口。卷内验证 owner、0700 权限及写入能力，bucket 初始化和各角色 ready 检查均有终态证据。

测试结束后只停止本轮两个项目，保留卷和容器用于调查；RF1 保留 7 个、RF3 保留 20 个容器，运行数均为 0。原有 68 个容器的 ID、名称、镜像、Compose 标签、状态/健康、限制和挂载前后比较无变化，见[隔离比较](wal-segments-linux/20261021-4f79a50-resource-isolation.json)。业务资源的完整身份投影保存在私有证据中，没有提交到公开仓库。

## 必须保留的失败与环境修正

1. `7b84c6d` 首次离线预检缺少锁定的 `aws-sdk-s3=1.152.0`，在构建产品前失败。随后只在新的私有 CargoHome 从公开 crates.io 准备并核验依赖，没有反复使用不完整旧索引。该失败不算产品测试。
2. `4f79a50` 原 normal-v2 的完整 nextest 为 333 PASS / 14 FAIL / 13 skipped；原覆盖率也存在相应失败。失败包含 typed `QuotaExceeded` 与 deadline，不能删除或改记成功。构建容器的 raw `memory.current` 接近 8 GiB，其中 file cache 约 7.75 GiB；Docker 工作集显示约 3 GiB，不能据此判断进程还有足够容量。最初 normal-v2 容器退出后 cgroup 已消失，未保存到该轮完整 raw peak 的缺口如实保留。
3. 在同一个受限 cgroup、相同源码和原 CLI 条件下，只对本轮 private target 文件施加 `POSIX_FADV_DONTNEED`：回收前 CLI 0.40 s 返回 typed quota、exit 101；回收后 `memory.current` 从约 7.86 GiB 降至约 14.45 MiB，同一 CLI 4.52 s PASS。没有提高内存/CPU、放宽 deadline、修改容量策略，或使用全局 drop_caches。
4. 后续通过私有透明 nextest wrapper 先以原参数加 `--no-run` 完成编译，在测试计时开始前停到 build boundary；宿主只 hint 白名单 private target 的 clean file cache，再以原始参数执行真正测试。普通门禁主轮记录 `memory.current` 从 8,177,860,608 降到 651,505,664 bytes，helper errors=0。`fadvise` 的 advised 数量不是实际回收字节数；内存变化依据 cgroup 前后样本。wrapper 不修改仓库 CI/coverage 脚本，也不删除 LLVM profiles。
5. normal-v3 的 RustFS 选择器额外选中了需要父进程环境的内部 crash child。原日志和 exit 100 保留；修正私有选择器、保留十项真实 RustFS 后，normal-v4 10/10 PASS。没有把这次 harness 错误解释为生产代码回归，也没有把原失败掩盖为同一轮成功。

cache helper 仅访问明确白名单的本轮私有 target，校验 live container ID、name、image、限额、host PID、cgroup，使用 no-follow 打开目录与文件并检查 regular file；跳过 symlink、非普通文件及所有 `nlink != 1` 的文件，防止触及外部 hardlink。它不读写文件内容、不删除文件、不改变业务资源。实际 [helper 原文](wal-segments-linux/cache-hint-helper.py.txt) SHA-256 为 `cfd83450414560684eae93301b2ef7e906600f845f46426afa7db651f9829e32`，[透明 wrapper 原文](wal-segments-linux/nextest-wrapper.sh.txt)为 `be07929b5dfb485ac0317b06266790ed7eb8b3ccd0cd403d2acb05881a5ff131`；这些 `.txt` 文件只保留证据，不是通用操作工具。[安全检查](wal-segments-linux/20261021-cache-helper-safety.json)及各阶段允许清单保留于归档；实际脚本相邻 `.venv` 已运行语法、help、observe-only 和安全测试。

## 证据归档与未验证边界

远端本轮私有根目录为 `/root/tidemq-validation/wal-segments-20261020.xnfbxhby`，本地保留于 `target/wal-linux-20261020`，均未作为大文件提交。终态小型元数据及由退出文件派生的摘要放在 [wal-segments-linux](wal-segments-linux/20261021-4f79a50-normal.json)。

| 归档 | SHA-256 |
| --- | --- |
| `linux-docker-4f79a50-evidence.tar.gz`，1449 项原始文件 | `1061ef905d2be1d725e1e3d792a2237b1c3a72caaf87c1cf9dfc400568b08540` |
| `normal-4f79a50-failed-evidence.tar.gz` | `6b7c4cea8535ada704072bb71050a0bb1351557257613a33ca9e41757f695824` |
| `normal-426ff65-evidence.tar.gz`，71 项原始文件 | `9851bbaf4839b710b0d21bdb0b1540e71058fe4a3a9d451f903266cf4efb778c` |

归档包含对应运行实际产生的命令、退出码、时间、日志、JUnit、cgroup raw 采样等；`4f79a50` 归档另外包含角色诊断/指标、镜像与二进制身份、持久消息 ledger。`4f79a50` 归档按本轮实际生成的 token、S3 key、delivery key 及 PEM private-key 标记检查无匹配。`426ff65` 只归档明确选择的 71 项日志/元数据/harness；未加入 fixture、配置、home、Cargo registry 或 target，PEM private-key 标记检查无匹配，该轮结束后已无残留 TOML secret 值可供比对。init 密钥和配置凭据不包含在归档，也不提交到公开仓库。

本轮存储改动主链使用默认 mimalloc。尚未重跑 System/jemalloc 的完整互斥构建矩阵，也未重新做 heap stack profile；native allocator-probe 是聚合统计，不是堆栈采样或经过测量的 Linux RSS。没有新增云资源、调整付款或使用宿主全局 cache 清理。

这些证据限于单台 Linux 宿主的隔离容器、短时故障与重放。真实三可用区、跨主机网络故障、负载均衡切换、长时间 soak、性能门禁及基线联合晋升仍需对应环境与最终报告，不能由本记录推断。
