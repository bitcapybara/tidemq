# 分段 WAL 的 Linux 覆盖率门禁证据

## 结果与源码范围

冻结源码 `4f79a5012bbc5e4edd18663e84db1764bac99b15` 的正式第二轮 Linux x64 覆盖率流程退出 0：347 项测试通过、13 项跳过，测试阶段 202.604 秒；三项受保护的精确覆盖率比较均通过。这个结论只对应该源码，后续产品代码变化需要重新执行完整流程。原有基线没有晋升或改写。

| 指标 | 已覆盖／总数 | 展示比例 | 原基线已覆盖／总数 | 原基线展示比例 |
| --- | --- | --- | --- | --- |
| 行 | 30248／37780 | 80.063526% | 27907／35176 | 79.335342% |
| 函数 | 2804／3938 | 71.203657% | 2576／3637 | 70.827605% |
| region | 41836／53836 | 77.710082% | 38365／49848 | 76.963970% |

门禁逐项比较原始整数交叉乘积；三项差值分别为 `9677188`、`53860`、`20022788`，均非负。展示比例不参与判定。原始 LLVM summary、完整 LLVM JSON 和 compare summary 的计数已经独立核对一致，共包含 134 个 workspace 源码文件及 `crates/tidemq-protocol/build.rs`。

仓库保留未经重写的[原始比较摘要](wal-segments-coverage/20261021-4f79a50-summary.json)、[原始 LLVM summary](wal-segments-coverage/20261021-4f79a50-llvm-summary.json)、[独立核对结果](wal-segments-coverage/20261021-4f79a50-audit.json)和[首次完整失败的比较摘要](wal-segments-coverage/20261021-4f79a50-original-failed-summary.json)。

## 固定身份与执行范围

| 项目 | 实际身份 |
| --- | --- |
| Git tree | `bdf169b11d95da4fafc40e94c40a2f5290b3861f` |
| 源码 archive SHA-256 | `690844529bf2da6d10807b7344a7195c81bd780181c5971517a3710b55fc1151` |
| Git bundle SHA-256 | `e79153e1e485ddd9311c4e0cb759438ea1fbdece0a2808dc91018d670c37420c` |
| Cargo.lock SHA-256 | `d4c609c32697a3d04f3b9b235bb746943804a7fac9fc63c51da4db6f2e90d63e` |
| builder 镜像 ID | `sha256:86fd5dd1673b5f458ceeec96cd228e2580402e92edae806605fc13d7864ee291` |
| Rust／Cargo | 1.99.1，Linux x86_64，LLVM 23.1.1 |
| nextest／cargo-llvm-cov | 0.9.148／0.9.1；LLVM JSON 3.1.0 |
| 分配器／构建并发 | 默认 mimalloc；Cargo jobs 2 |
| 容器限制 | 2 CPU、8 GiB RAM、额外 8 GiB swap、512 PID；network none、UID 10001 |
| policy SHA-256 | `ff5e41e78593ce972649cee96b768db3132bb8a157d08bcded4f48329b0bed57` |
| 未变更的 baseline SHA-256 | `638b8e963abe9e8936c7df0f2571093c0244b83946a4a173d5df7c7bf15fb2ee` |

源码来自真实 Git bundle，HEAD 与 tree 均核对通过。源码只读挂载，target、Cargo 缓存和 protocol generated overlay 各自独立；代码生成后及流程退出时 generated overlay 与冻结源码逐字节相同，Git tracked diff 为空。419 个锁定 crate archive 的 SHA 和 sparse index 校验均通过，产品编译在隔离容器中离线执行。

正式命令为未经修改的仓库脚本：

```sh
bash /work/scripts/coverage-run.sh /work /report --require-baseline-unchanged
```

features、workspace all-targets、build-script 范围均由原 `coverage/policy.json` 决定。CI profile 仍为串行、retries 0，测试自身 deadline 未调整。13 项跳过没有算作通过；需要外部 RustFS、Collector 和 release 探针的独立验证不由这里的覆盖率结果替代。

## 准备失败与首次完整失败

两次容器准备失败均在产品编译／测试前终止，原始日志保留：第一次旧离线 nora 缓存缺少锁定的 `aws-sdk-s3 1.152.0`；第二次 nextest 固定写入 `/work/target/nextest/ci`，只读源码没有该目录的独立可写挂载，且空 target 缺少 Cargo 要求的 `CACHEDIR.TAG`。修正仅涉及私有公共 crates.io 缓存、标准 marker 和自有 target 挂载，随后对 JUnit、RustFS fixture 目录及临时可执行路径做了真实预检。

首次完整插桩流程退出 100：347 项执行中 333 通过、14 失败，13 项跳过，测试阶段 228.837 秒。失败为 `runtime_queue` 的 11 项和 `runtime::data::reclaim_tests` 的 3 项，包含真实 typed `QuotaExceeded` 及后续 deadline；普通 Linux 流程同版复现了相同 14 项失败。

原始失败 cohort 的三项计数为行 `28867/37780`、函数 `2695/3938`、region `39995/53836`，均低于基线。该次脚本停在 nextest 失败处；随后在运行任何反馈测试前，单独导出其 LLVM／LCOV／HTML 和原比较摘要，封存 466 个 raw profile 与 merged profdata。此失败没有重标为成功，也没有与后续反馈 profile 混计。

## 私有构建缓存的因果对照与阶段隔离

首次完整流程的 cgroup 原始记录显示 `memory.current=8015450112`、`file=7753605120`、`inactive_file=7353192448`，`memory.peak` 达到 8 GiB；OOM／OOM-kill 为 0。Docker stats 的工作集不包含同样的文件缓存量，不能替代这些 raw 字段。

在独占、同一 8 GiB／2 CPU cgroup 内完成了有界对照：

1. 只对本轮 private target 的普通、单硬链接文件执行 `POSIX_FADV_DONTNEED`，不跟随 symlink，不删除或改写文件。
2. 使用固定 1 MiB buffer 顺序读取 18 个 private build 文件，每 32 MiB 采样一次，实际读取 7,828,685,152 B，在 7.36 秒内达到约 91.5% 的原内存上限。单次总读量上限为 16 GiB／180 秒，没有分配匿名内存制造压力。
3. 原 CLI 恢复测试使用相同 binary、原配置与 deadline，直接 `--exact --nocapture` 执行，在 0.40 秒返回明确的 `QuotaExceeded`。
4. 唯一干预为再次定向释放相同 private build 文件的 clean cache，raw current 从 `7860871168` 降到 `14446592`；原测试随后 1／1 通过，耗时 4.52 秒。

上述诊断 profile 只写入独立 causal 目录。test binary SHA 为 `e26cca4336d8db4c8a992d46678da70a6123baf81f53415f103406947cf4cec5`，服务 binary SHA 为 `801aff43d09856e3262ba539e21feb9cce1a6f0e3e34a248dcbd48afadefbff8`。没有采到首次失败时完整的 `quota.container` 响应，因此不把缺失的内部诊断字段写成直接观测。

第二轮正式 cohort 保留原脚本及其真实 workspace clean；cargo-llvm-cov 0.9.1 的该命令会清 workspace 编译产物及 profile，保留外部依赖缓存。私有透明 nextest wrapper 先在原插桩环境用相同参数附加 `--no-run` 完成构建，然后在确定性握手边界暂停。宿主只对精确登记的自有容器／private target 执行同一缓存 hint，完成后按完全原参数运行 nextest。

正式 build-only 耗时 7 分 15 秒，实际测试调用 fresh 0.69 秒。边界 raw current 从 `8042401792` 降至 `100417536`；操作前后 73 个已有正式 profraw 的 SHA 完全一致，build-script 覆盖数据保留。源码、原 90／80 水位、容器限额、测试范围、deadline 和重试策略均未改变，也没有清宿主全局 cache。整轮另保留 236 次两秒间隔 raw cgroup 采样；最大采样 current 为 `8589791232` B，该值包含构建阶段。

## 原始制品与边界

完整通过归档为 `coverage-4f79-cohort2-accepted.tgz`，SHA-256：

```text
69e4de1747dfd3f5789205fec5cd929237e76c49d4a8b6e5d9ba42f6ebb406d7
```

它保留完整 LLVM JSON／summary、LCOV、HTML、JUnit、原始比较、日志、正式 raw／merged profiles、源码／工具／容器身份、cgroup 连续采样、cache-phase 握手、因果对照和实际 harness。首次失败及 fresh 反馈的独立归档 `coverage-4f79-failed-and-feedback.tgz` 的 SHA-256 为 `bcbf57bce0ca62fafb8887b84835c6e36146659c168f4afca43ebc91af37be7e`；fixture 凭证、私钥和 WAL 没有复制进这些交付归档。

远端私有制品根目录为 `/root/tidemq-validation/wal-coverage-20261020.fwMOj3b7`。归档已取回本轮 test worktree 的 ignored `target/wal-coverage-20261020/` 并复核 SHA。原始 LLVM summary 的 SHA 为 `214c0f14a491f457451def0a66b7222ca0a823b39da8fb2fd63d55329c70d93a`，完整 LLVM JSON 为 `848a3af424e96f7fb8c47d767ec0aa69dce1728ce44098ad0d4431cd283b35e3`，原始比较摘要为 `5600d53b95250e9eed9edc0e0982696f7d5079291c54f5ba65815a51ca2154af`。

本轮没有运行 GitHub Actions、修改付款设置或晋升基线。它覆盖该固定源码的 Linux 插桩回归与覆盖率，真实三区、宿主掉电、长期稳定性和最终性能 cohort 仍分别验收；后续产品源码也不能引用本报告代替重新测量。
