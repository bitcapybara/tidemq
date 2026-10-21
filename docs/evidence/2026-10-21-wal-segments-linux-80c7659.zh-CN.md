# `80c7659` 的 Linux 与 Docker 验证

冻结源码 `80c7659c39b3429b185c4e6bfc3436c8e892917e` 已完成普通 Linux 门禁、release 构建、新镜像验证及全新 RF1/RF3 Docker 十项场景。十项共 736 条消息，各项 committed、received、durable ACK 与 frontier 均达到该项期望值，没有重跑实际 case。

**该版本尚未通过独立性能验收。** 同源 Mac 性能 cohort 在 RF3 latency 场景实际发生 READ deadline，记录 commit=1024、ACK=1000，整轮失败；归因仍待进一步证据。本篇功能与隔离结果不替代性能失败，没有修改策略或晋升性能基线。

此前 `4f79a50`、`426ff65` 的通过或失败均保留在[历史 Linux 证据](2026-10-21-wal-segments-linux.zh-CN.md)，没有用于代替本版结果。本文后续的文档提交也不作为验收源码 SHA。

## 来源与构建身份

| 项目 | 实际身份 |
| --- | --- |
| Git commit | `80c7659c39b3429b185c4e6bfc3436c8e892917e` |
| Git tree | `0c8073e9372202e5206d0b74f1340c965219e655` |
| 源码 archive SHA-256 | `fd298a2bd674bc183a6fd4bb4ade0d70b2d5c6931aef43896666874dc28512a5` |
| Git bundle SHA-256 | `d72bb0af64f94d362f164c17c728c69df1ae3670d03cb6e3a2a5ec8ec29e9aa7` |
| Cargo.lock SHA-256 | `d4c609c32697a3d04f3b9b235bb746943804a7fac9fc63c51da4db6f2e90d63e` |
| Rust builder image ID | `sha256:86fd5dd1673b5f458ceeec96cd228e2580402e92edae806605fc13d7864ee291` |
| 新运行镜像 tag | `tidemq-wal-segments:80c7659-xnfbxhby` |
| 新运行镜像 ID | `sha256:3a804d8432b4ceb1b80e260edba7437bfdd225250f016f8b4d106e34afc78139` |
| 固定运行基础镜像 | `debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587` |
| service binary SHA-256 | `562df4eedce7ab1be6f025d9537636556c37f4bf5c78c6913e48e6ded6a788f8` |
| deploy_validation SHA-256 | `c371e2c1fe459d307d69843b5f34ac3a9310cc114fe8eaf5f9f079c7a0472fb2` |

源码 archive 与 bundle 导入 `/root/tidemq-validation/wal-segments-20261020.xnfbxhby/source-80c7659` 后，核验 HEAD/tree/lock、tracked clean，再只读挂载。普通检查和覆盖率共享该冻结源，使用各自 target、CargoHome、生成代码 overlay 和日志目录。复用外部依赖缓存前执行 `cargo clean --workspace --locked`，重新编译 workspace；没有复用旧镜像、旧数据卷或旧 case 结果。[源码元数据](wal-segments-linux/20261021-80c7659-source.json)及[镜像元数据](wal-segments-linux/20261021-80c7659-image.json)保留完整值。

宿主为 `ssh dev` Ubuntu 24.04/x86_64、内核 `7.0.0-28-generic`、Docker 29.9.1、Compose 5.6.0。builder 实测 Rust/cargo 1.99.1、LLVM 23.1.1；nextest 0.9.148，Node v22.23.3。Cargo 使用锁定依赖的专有 offline registry，在无网络容器内运行，未复制凭据。工具与依赖原始核验记录一并保留于归档。

release 构建与镜像内重新计算的两个二进制 SHA 完全一致。`deploy_validation` 重新构建后的字节与早期客户端相同，不代表沿用旧镜像或旧验收结果。镜像内实际执行 release `tidemq allocator-probe --blocks 2`，exit 0，backend=`mimalloc`、scope=`service_binary`、native version=`30302`。它是 native 聚合统计，不是堆栈 profile 或经过测量的 Linux RSS。独立 SDK 验证客户端报告的 allocator=`system`，与服务二进制的全局分配器作用域分开记录。

## 完整普通 Linux 门禁

完整[固定 harness 原文](wal-segments-linux/80c7659-normal-harness.sh.txt)在启动前写完，SHA-256 为 `34504fa3fe8b6a31778447da828e6b404cd24fbae88be58a982683c0c956c787`，root-owned `0444` 并单独只读 bind；启动后未编辑，终态哈希相同。主检查 feature 集为 `tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen`，各步骤命令、原始日志、exit 和时间均在归档。

| 检查 | 实际终态 |
| --- | --- |
| `cargo fmt --all -- --check` | exit 0 |
| workspace `clippy --all-targets --no-deps --locked -- -D warnings` | exit 0，37 s，包含 test/example/benchmark 等 target |
| 生产 lib/bins panic、unwrap/expect、indexing 等 deny 审计 | exit 0，26 s |
| 生成代码与冻结源比较 | 3 个文件相同，exit 0；终态再次确认 tracked clean |
| workspace nextest，原 `ci` profile | **356/356 PASS，13 skipped，172.141 s，exit 0** |
| doctests | 4 PASS，exit 0 |
| SDK 不开可选 feature 的 strict Clippy | exit 0 |
| SDK 不开可选 feature 的 nextest | 37/37 PASS，10.564 s，exit 0 |
| 配置验证、allocator-probe | exit 0 |
| Node coverage/performance 脚本测试 | 44/44 PASS，exit 0 |
| 真实 RustFS 测试 | **10/10 PASS，145 skipped，47.970 s，exit 0** |
| release service + deploy_validation | exit 0，3 min 49 s |
| 整个固定普通 harness | **exit 0**，末尾正常完成安装和源码检查 |

all-targets Clippy 与生产 deny 审计的精确 feature/flags 见原文；未运行 `cargo check`。ignored RustFS 的选择器显式排除了必须由父测试启动的内部 crash child，父测试仍由普通主轮执行。没有筛选或重跑实际失败测试。

[启动预检](wal-segments-linux/20261021-80c7659-launch.json)逐项核对完整 source label、owner、image、原限额、只读源码 HEAD/tree/lock/clean 和只读 harness。普通容器为 4 CPU、8 GiB RAM、RAM+swap 总额 8 GiB、512 PID、UID/GID 10001、只读根目录、drop ALL capabilities、no-new-privileges、无网络；Cargo jobs=2。

按此前已有因果证据，透明 nextest wrapper 先以原参数加 `--no-run` 完成编译，在原测试计时前仅 hint 本轮 private target 的 clean file cache，再执行原始参数。主轮 `memory.current` 从 7,926,886,400 降为 118,337,536 bytes，helper errors=0；SDK 与 RustFS 各有独立 build boundary。609 个每 2 秒 raw 资源样本中的 `oom_kill` 最大值为 0。没有删除文件/profiles、清理全局 cache、提高预算或放宽 deadline；advised 文件数不作为实际回收字节数。[普通终态摘要](wal-segments-linux/20261021-80c7659-normal.json)含每一步 exit、边界前后值及容器真实终态。

同版受保护覆盖率在独立容器完成 **356/356 PASS、13 skipped、177.315 s**，原比较门禁 exit 0；精确分子/分母、源/生成代码 guard 及历史失败由[覆盖率证据](2026-10-21-wal-segments-coverage-80c7659.zh-CN.md)独立保存。本篇没有用普通测试或数值比较替代完整覆盖率 gate。

## 全新 Docker 十项场景

使用本版新镜像，并在启动前确认两个新项目的容器、named volume 和 network 数均为 0：

- RF1：`tidemq-validation-rf1-be8202d8d74cd0ba`，RustFS 512 MiB。
- RF3：`tidemq-validation-rf3-57e8e7e74a3f8c78`，RustFS 1 GiB。

调用冻结源中原有 `scripts/docker-validate.sh STATE CASE`，没有修改部署脚本、原 workload、故障步骤、等待预算或成功标准。[固定编排原文](wal-segments-linux/80c7659-matrix-driver.py.txt) SHA-256 为 `2e1924b92dab1a79997202579ad07e7510d2007b5ca8a4eb47f2194c6fe3672b`，执行前后相同；[实际执行终态](wal-segments-linux/20261021-80c7659-matrix-execution.json)为 exit 0。

| 模式 / case | expected | committed / received / durable ACK / frontier | exit |
| --- | --- | --- | --- |
| RF1 smoke | 64 | 64 / 64 / 64 / 64 | 0 |
| RF1 replay | 64 | 64 / 64 / 64 / 64 | 0 |
| RF1 forced OOM | 80 | 80 / 80 / 80 / 80 | 0 |
| RF3 smoke | 64 | 64 / 64 / 64 / 64 | 0 |
| RF3 replay | 64 | 64 / 64 / 64 / 64 | 0 |
| RF3 storage-kill | 80 | 80 / 80 / 80 / 80 | 0 |
| RF3 broker-kill | 80 | 80 / 80 / 80 / 80 | 0 |
| RF3 metadata-quorum | 80 | 80 / 80 / 80 / 80 | 0 |
| RF3 storage-quorum | 80 | 80 / 80 / 80 / 80 | 0 |
| RF3 gateway-kill | 80 | 80 / 80 / 80 / 80 | 0 |

RF1 合计 208、RF3 合计 528，十项合计 736；这是跨场景求和，不是单个全局 session。完整[矩阵摘要](wal-segments-linux/20261021-80c7659-docker.json)记录每项时间、验证容器、独立 evidence 目录和 ledger。执行窗口为 02:11:40Z 至 02:24:01Z。quorum 场景检查多数派丢失时不返回成功 receipt，并保留原 pending batch 恢复后继续完成，未仅以进程重新 ready 判定成功。

受控 OOM 按原 harness 将同一个 RF1 node1 从 384 MiB 临时降至 6 MiB，实际观察 `OOMKilled=true`、exit 137，再恢复 384 MiB 并完成 80 条 ACK；前后容器 ID 相同，见[OOM 投影](wal-segments-linux/20261021-80c7659-oom.json)。它验证恢复路径，不代表正常默认负载下发生 OOM。

运行时实际核对 UID/GID 10001、只读根目录、drop ALL capabilities、no-new-privileges、PID 128、无宿主端口；TideMQ 角色为 0.5 CPU / 384 MiB，RAM+swap 总限额等于 RAM。实际私有 bridge 为 internal；角色共享本项目 network namespace，但保留各自 PID namespace/cgroup/数据卷。数据卷的 owner、0700 和写入能力经过容器内检查，bucket 初始化和各角色 ready 检查完成。原始[RF1](wal-segments-linux/20261021-80c7659-rf1-isolation.json)与[RF3](wal-segments-linux/20261021-80c7659-rf3-isolation.json)身份/限制投影保留实际字段。

结束后仅停止本轮精确项目并保留现场：RF1 7 容器、3 named volumes；RF3 20 容器、12 named volumes；全部 running=0，所有被挂载的 named volumes 均实际存在。原有 68 个业务容器的 ID/name/image/tag/labels/state/health/limits/mounts，相对本轮 before 和最初 inventory 均无变化，见[隔离复核](wal-segments-linux/20261021-80c7659-resource-isolation.json)。完整业务身份投影保存在私有目录，未提交到公开仓库。

## 保留的前置错误与换版

`799d57a` 普通轮因协调方新增 test-only 恢复契约收紧而显式 operator stop，退出 137、`OOMKilled=false`，没有开始测试/release/Docker；标记为 `operator-superseded`，不是产品失败或 full gate pass。私有 launcher 当时的 source label 存在短前缀替换错误，而真实只读 source/HEAD/tree 正确；首次 stop 因标签不符拒绝，额外核验精确自有 CID/name/image/owner/mount/HEAD 后才停止。实际错误标签与哈希未改写，见[停止记录](wal-segments-linux/20261021-799d57a-operator-stop.json)和[封存摘要](wal-segments-linux/20261021-799d57a-superseded.json)。本版从显式参数直接构造完整 source label，并在 workload 启动前严格等值预检，不继承该部分结果。

本版 Docker 编排初次在 Python 预解析阶段发生 `else80` SyntaxError（line 101），任何语句和 case 均未执行；两个新项目的容器/卷/网络计数均为 0。只增加一处空格 `else80` → `else 80`，没有改变十个 case、720 秒 outer timeout 或 120 秒 startup budget。[精确 diff](wal-segments-linux/80c7659-matrix-syntax-fix.diff.txt)及[零资源/语法预检](wal-segments-linux/20261021-80c7659-matrix-preflight.json)保留了原错误和修正前后哈希。随后在实际脚本相邻 `.venv` 的 Python 3.12.3 中 `py_compile` exit 0，核对全部 case/参数，固定脚本为 `0444` 后才启动，运行中未修改。仅使用标准库，没有第三方依赖；仓库里的 `.py.txt`/`.sh.txt` 是证据原文，不是新增的部署工具。

同源覆盖率 cohort5 缺少私有 control 目录，在产品测试前退出；修正 preflight 后，cohort6 才完成上述完整 gate，二者没有合并记成一次无修改通过，详见覆盖率证据。

## 原始制品与边界

1508 项原始制品封存为 `linux-docker-80c7659-evidence.tar.gz`，2,884,338 bytes，SHA-256 **`f20e49ef06fdf0cf8dd1e9d16d064e88728306bb985fd283f2434960ad35b7b8`**。包含完整 normal 命令/exit/time/JUnit/raw cgroup、镜像构建与核验、固定编排及其前置错误、十项故障日志/诊断/指标/ledger、实际资源投影。对本轮实际生成的 24 种凭据表示及三类 private-key 标记检查均无匹配；init、配置凭据、Cargo registry、编译 target 和 venv 未加入归档。[归档元数据](wal-segments-linux/20261021-80c7659-archive.json)保留精确值。

远端制品根为 `/root/tidemq-validation/wal-segments-20261020.xnfbxhby`，本地大制品留在忽略目录 `target/wal-linux-20261020`。旧红轮、operator stop 与新轮采用独立目录/归档，没有覆盖或删除。

本轮未另跑 System/jemalloc 的完整互斥构建矩阵或 heap stack profile，也没有测试三区、跨主机网络故障、负载均衡切换、宿主掉电或长时间 soak。Docker 结果限于单台 Linux 宿主的真实 QUIC/TLS/S3、短时进程/多数派故障和重放；分段回收与 reader pin 的底层边界仍以同源测试及恢复设计为准。独立性能 READ deadline 失败尚未解决，不能从本篇推出生产 SLA、吞吐/延迟达标或基线晋升。
