# 426ff65 分段 WAL 覆盖率与替换副本失败证据

## 原始正式结果

冻结源码 `426ff6598c7b6a4337cc07ca79ac70d51d12b413` 的首个完整 Linux x64 插桩 cohort（本次验证流程的 cohort3）**退出 100，未通过完整门禁**：351 项执行中 350 通过、1 失败，另有 13 项跳过，测试阶段 226.280 秒。唯一失败为 `native_runtime_scaling_explicit_replacement_preserves_original_sdk_traffic_and_ack`：首次替换调用在 `complete metadata` 阶段收到 `Unavailable`／`Unknown`／`retryable=true`，原测试直接 `unwrap`，3.501 秒后失败。

原脚本因 nextest 失败而终止。随后在任何反馈测试之前，单独使用原 report 命令导出 LLVM summary、完整 JSON、LCOV、HTML 和原受保护 compare summary。三项数值比较通过，但**数值比较不能替代失败的测试门禁**；基线未改写或晋升。

| 指标 | 已覆盖／总数 | 展示比例 | 原基线已覆盖／总数 | 原基线展示比例 |
| --- | --- | --- | --- | --- |
| 行 | 30273／37791 | 80.106375% | 27907／35176 | 79.335342% |
| 函数 | 2809／3939 | 71.312516% | 2576／3637 | 70.827605% |
| region | 41878／53865 | 77.746217% | 38365／49848 | 76.963970% |

原始整数交叉乘积差分别为 `10249611`、`69469`、`21003819`。LLVM summary、完整 LLVM JSON 与 compare 的计数一致，包含 134 个 workspace 源码文件以及 `crates/tidemq-protocol/build.rs`。仓库保留[整轮结果](wal-segments-coverage/20261021-426ff65-cohort.json)、[原始 compare](wal-segments-coverage/20261021-426ff65-summary.json)、[原始 LLVM summary](wal-segments-coverage/20261021-426ff65-llvm-summary.json)和[独立计数核验](wal-segments-coverage/20261021-426ff65-audit.json)。

## 固定身份与隔离范围

| 项目 | 实际身份 |
| --- | --- |
| Git tree | `773e9d7f261087fb9d1585cb8bc069e3445ba68c` |
| 源码 archive SHA-256 | `deea37a033976a6f8f2f39d8d5da3533d52f048785e72f588a719da3935208bb` |
| Git bundle SHA-256 | `9aa4e0e1ab86d3443dd722daa865eac4848bf1fe033e0ac86ec31483551fce59` |
| Cargo.lock SHA-256 | `d4c609c32697a3d04f3b9b235bb746943804a7fac9fc63c51da4db6f2e90d63e` |
| builder image | `sha256:86fd5dd1673b5f458ceeec96cd228e2580402e92edae806605fc13d7864ee291` |
| 工具 | Rust／Cargo 1.99.1；LLVM 23.1.1；nextest 0.9.148；cargo-llvm-cov 0.9.1 |
| 资源 | 默认 mimalloc；2 CPU；8 GiB RAM；额外 8 GiB swap；512 PID；Cargo jobs 2 |
| 执行隔离 | network none；UID 10001；源码只读；target、缓存、generated overlay 独立 |
| policy SHA-256 | `ff5e41e78593ce972649cee96b768db3132bb8a157d08bcded4f48329b0bed57` |
| 未变更 baseline SHA-256 | `638b8e963abe9e8936c7df0f2571093c0244b83946a4a173d5df7c7bf15fb2ee` |

完整命令仍为未经修改的 `bash /work/scripts/coverage-run.sh /work /report --require-baseline-unchanged`。原 policy 的 features、workspace all-targets、build-script 范围均保留；nextest 串行、retries 0，测试 deadline 未调整。真实 Git HEAD、tree、原始 archive／bundle 一致；退出时 tracked diff 与 generated diff 均为空。

沿用[4f79 已验证的私有构建缓存阶段隔离](2026-10-21-wal-segments-coverage.zh-CN.md)：透明 nextest wrapper 先以原参数加 `--no-run` 完成构建，然后只定向 hint 本轮 private target 的 clean cache，最后运行完全原参数。build-only 7 分 04 秒；hint 错误 0，raw `memory.current` 从 `8146771968` 降到 `671100928` B，原正式 profiles 的 SHA 保持一致；实际测试调用 fresh 0.45 秒。没有改产品 90／80 水位、容器限额或宿主全局 cache。

整轮保留 288 次两秒 raw cgroup 采样，最大采样 current 为 8 GiB，包含构建阶段。失败用例在 UTC `2026-10-20T18:10:34.316` 开始；其相邻采样 current 约 1.136–1.153 GB，OOM／OOM-kill 为 0，`memory.events.max=5476` 在该窗口没有增长。CPU throttling 有增长；这些粗粒度观测不能唯一解释该次语义失败，也没有该窗口容器内存高水位的直接证据。

## 原现场的持久状态核验

原失败 fixture 被保留并私有复制。诊断读取器仅打开新的 store 工作副本，使用冻结 426 的已编译 library 和 `StorageOptions::default()` 恢复；原 fixture 只读挂载，前后所有文件的 SHA 一致。输出只含有界 identity、诊断、计划、成员与日志摘要，不含凭据、私钥或消息正文。原始投影保留在[postmortem JSON](wal-segments-coverage/20261021-426ff65-postmortem.json)。

| 副本 | 事后持久状态 |
| --- | --- |
| metadata 11、12、13 | vote term2／leader11；committed／applied index19；group2 generation1，voters 21、22、23；同一 pending plan，completed 为 null |
| data 21、22、24 | vote term2／leader21；committed／applied 与稳定 membership index18；单配置 voters 21、22、24 |
| removed data 23 | 保留 joint membership index17 |

原计划 operation 为 `3fc40343-398c-8457-8b58-5151d5270845`，prepared revision18，copy reserve 为 `469901775` B。三个 metadata 副本全部 20 条 retained log 均已检查，尾项为 index19 的 `PrepareMembership`；没有 snapshot／purge，也没有留下已提交或未提交的 `CompleteMembership`。

这些是**事后恢复投影，不是失败瞬间的 live quorum 证明**。它们说明数据成员变化已经持久，而 metadata 仍保存原 pending 计划；不能将原失败解释成“Complete 已提交，只丢失成功回包”，也不能据此声称某条命令从未短暂写入后又被丢弃。

原 fixture 的相关节点在同一测试进程内，没有单独节点日志。`runtime::Error::failure` 会将通用 `Operation` 映射为 `Unavailable`／`Unknown`／retryable，现有 WARN 记录不包含该分支完整 nested source；因此原来的 code6／outcome3 无法唯一分辨控制名额冲突、quorum、authority 等源错误。此限制没有被猜测补全。

## 独立反馈与制品

封存原 cohort 后，仅运行一次原 SHA test binary 的同名 exact 用例，独立 profiles：2.32 秒返回同类 Unknown，但 phase 为 `prepare membership`。它与原 `complete metadata` 失败分开记录。随后唯一 WARN 探针只在该测试 setup 增加五行现有订阅器，冻结产品 library 仍只读、依赖按原 Cargo fingerprint 精确选取，测试输出与 profiles 独立；1／1 通过，3.73 秒。该探针仍出现后台 scaling Unknown，但没有原失败的完整 typed source，不能作为完整覆盖率通过或唯一因果结论。

原始 test binary SHA-256 为 `2ff22f41fa1d11a9eaeec2f9bba2229916eaf39d4686935fcd2d1daa570be9c8`，服务 binary 为 `1ab5d71ccc1343dbae8e31715676b77418601c3b198761375d867025293cce19`。旧版本 [4f79 的完整 green](2026-10-21-wal-segments-coverage.zh-CN.md) 和普通 Linux 426 的同名用例通过均不能覆盖本轮失败。

完整原始归档 `coverage-426-cohort3-original-failed.tgz` 的 SHA-256 为：

```text
835f64d6af2393e2924b40c14bc2e337f7701be675df6b97d8f2a4133c35e6a0
```

归档保留整轮 JUnit、日志、LLVM full／summary、LCOV、HTML、原 compare、512 个正式 raw profiles 与 merged profdata、源码／容器身份、阶段握手和连续资源采样。原 fixture 的凭据、私钥与 WAL 不在交付归档中。远端根为 `/root/tidemq-validation/wal-coverage-20261020.fwMOj3b7`；归档已下载至 test worktree 的 ignored `target/wal-coverage-20261020/` 并复核 SHA。

## 调用者恢复契约的定向回归

测试提交 `b8ece31` 只调整 fixture 与回归：先冻结该全新 fixture 的 generation1 canonical plan，所有恢复调用均使用原同一 30 秒绝对 deadline，并检查返回计划的完整相等性。只有来源链中明确的 retryable Unknown 或 typed scaling Unknown 才可恢复；永久拒绝、不可重试 Unknown、结果明确被拒绝以及未分类错误均直接返回。外层 timeout 仍使用原 deadline，没有新增产品重试策略、预算或配置。

先保留“只调用一次”的 test-helper 对照实现，回归在 nested `Operation → Remote → Rejected Unknown` 首次结果处失败；此人为对照用于验证 helper，不能代替前述真实 426 失败。随后同 deadline 的恢复逻辑通过回归，另验证五类永久／未分类错误只能调用一次，以及已到期 deadline 不开始新调用。

确定性 seam 复用 production `Coordinator`、`Service` 与封闭 Query／Reply codec，以及现有 proof-drop fixture。在保留实际 data leader 的新场景中，真实 Raft 已返回稳定目标配置的 proof；只丢弃一次该 proof 回复，使首次 execute 返回合法 Unknown。此时检查 metadata 仍保存原 pending plan、data 的实际目标成员已稳定且 applied 覆盖 membership；随后发现并恢复完整相同计划，继续使用同一个原 12 秒 `WAIT` deadline。原数据、成员、SDK 与 ACK 断言均保留。

该 seam 的 **Raft 复制使用真实 QUIC，scaling 语义 RPC 使用测试拥有的 codec loopback**，不是 scaling 端到端 mTLS／QUIC 故障注入，也没有新生产测试开关。它验证失败／恢复契约，不定位原现场唯一源错误。

Rust 1.99.1／macOS ARM64 下，fmt 与严格 package tests Clippy 均退出 0；nextest 0.9.148 的原 ci profile、retries 0 定向执行 7／7 通过，10.508 秒，其中原 runtime explicit 场景 3.499 秒、新 proof-loss 场景 1.395 秒。保留[工具、source tree、red／green 与日志 SHA](wal-segments-coverage/20261021-b8ece31-fixture-contract.json)。这是定向契约证据；后续聚合源码／测试仍需要独立冻结与完整 Linux cohort，不能将本轮数值比较重新标成全门禁 green。
