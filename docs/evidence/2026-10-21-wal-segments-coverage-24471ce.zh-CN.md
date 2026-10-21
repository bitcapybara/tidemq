# 24471ce 完整 Linux 覆盖率证据

冻结源码 `24471ce18f26739eefc47af63fdd0c183e9a787c` 的完整 Linux x64 coverage cohort7 **退出 0，原受保护门禁通过**：364／364 项通过、13 项按原策略跳过，测试阶段 224.382 秒。原始 nextest run 为 `886d31ba-2e4b-4ad6-8d15-9273083929e2`，原始命令为：

```bash
bash /work/scripts/coverage-run.sh /work /report --require-baseline-unchanged
```

沿用原 workspace all-targets、build-script 范围、默认 mimalloc 和三个 features：`tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen`。nextest 为原 `ci` profile，串行、retries 0、fail-fast=false，原 deadline 和测试断言未修改。JUnit 的 364 项执行项 failures/errors 均为 0；另有 nextest 报告的 13 项跳过，不计入 JUnit 根节点的 skipped=0。

| 指标 | 已覆盖／总数 | 展示比例 | 原基线已覆盖／总数 | 原基线展示比例 |
| --- | --- | --- | --- | --- |
| 行 | 30412／37896 | 80.251214% | 27907／35176 | 79.335342% |
| 函数 | 2824／3947 | 71.548011% | 2576／3637 | 70.827605% |
| region | 42135／54060 | 77.941176% | 38365／49848 | 76.963970% |

精确整数交叉乘积差分别为 `12208840`、`103416`、`26333580`，三项均通过。独立核验确认 LLVM summary、完整 LLVM JSON 与原 compare 计数一致，共 134 个 workspace 源文件，包含 `crates/tidemq-protocol/build.rs`。policy、分母规则和 baseline 都未修改，baseline 未晋升。

仓库保留[完整执行摘要](wal-segments-coverage/20261021-24471ce-cohort.json)、[原 compare](wal-segments-coverage/20261021-24471ce-summary.json)、[原 LLVM summary](wal-segments-coverage/20261021-24471ce-llvm-summary.json)、[精确计数核验](wal-segments-coverage/20261021-24471ce-audit.json)及[制品索引](wal-segments-coverage/20261021-24471ce-artifacts.json)。这些小型 JSON 已在本地复核 SHA、原 baseline/policy 和精确计数。本轮不复用旧 `80c7659` 的通过结果或 profiles；性能联合验收独立进行。

## 源码、工具与资源

| 项目 | 实际身份 |
| --- | --- |
| Git tree | `062cacc15ac04a59bc5c38002fce2a17f8f9e2c1` |
| 源码 archive SHA-256 | `e3a9ded3546ed99680fbce69a402a1614ace40c9619f34686cbfd96eef319e2f` |
| Git bundle SHA-256 | `062a5df79cf813169090b2244d892260e7273593839f067db07a47f2d0510ee5` |
| Cargo.lock SHA-256 | `d4c609c32697a3d04f3b9b235bb746943804a7fac9fc63c51da4db6f2e90d63e` |
| builder image | `sha256:86fd5dd1673b5f458ceeec96cd228e2580402e92edae806605fc13d7864ee291` |
| 工具 | Rust/Cargo 1.99.1；LLVM 23.1.1；nextest 0.9.148；cargo-llvm-cov 0.9.1；Node 22.23.3 |
| 资源 | 2 CPU；8 GiB RAM；额外 8 GiB swap；512 PID；Cargo jobs 2 |
| policy SHA-256 | `ff5e41e78593ce972649cee96b768db3132bb8a157d08bcded4f48329b0bed57` |
| 原 baseline SHA-256 | `638b8e963abe9e8936c7df0f2571093c0244b83946a4a173d5df7c7bf15fb2ee` |

真实 Git HEAD/tree 与[源码 metadata](wal-segments-coverage/20261021-24471ce-source.json)一致。产品源码只读，UID 10001、network none；target、Cargo cache、generated overlay、临时目录、profiles 和报告均私有隔离。启动前验证精确工具 PATH、wrapper/helper SHA、所有握手文件父目录的实际读写、LLVM profile/临时可执行文件输出及 target 双挂同 dev/inode。运行前后 tracked diff 与 generated diff 均为空，启动后没有编辑 harness。

首次环境 preflight 在 Docker 创建容器时退出 125：新的只读 source archive 缺少 ignored `target` 挂载点，因此尚未启动任何构建或测试。保留原日志后，只补充 ignored `target/CACHEDIR.TAG`，真实 Git HEAD/tree 和 tracked 内容不变；原 preflight 随后退出 0，才启动本次完整 cohort7。没有修改产品源码、原脚本、wrapper、策略或资源。

## 构建、测试与保留边界

原脚本从 UTC `2026-10-21T04:02:31Z` 运行至 `04:14:27Z`。透明 wrapper 先用原参数加 `--no-run` 构建，build-only 为 7 分 33 秒；原 private-target helper 随后只对本轮白名单 target 执行 `POSIX_FADV_DONTNEED`，最后用完全原参数执行 nextest，fresh 调用为 0.71 秒。

helper 错误 0，raw `memory.current` 从 `8186028032` 降至 `26722304` B，实际开测前为 `26624000` B。没有修改产品 90/80 水位、原限额或宿主全局 cache；hint 字节数不当作实际回收量。正式 profiles 在 cache 操作前后、报告导出前后和归档前后 SHA 均一致。

容器注册后的两秒 raw cgroup 采样共 288 份，时段为 UTC `04:04:41.280340` 至 `04:14:24.568920`，容器于 `04:14:27.833540` 消失后记录终态。采样包含 memory.current/stat/events、CPU 和 IO；最大采样 current 与累计 peak 均为 `8589934592` B，OOM kill 为 0。该 peak 包含构建阶段，不能当作测试阶段 RSS。

真实提交后 Claim 租约过期的错误链回归通过（3.765 秒），另两类 fence 错误的原 wire 比较通过（0.011 秒）；原 runtime explicit replacement 通过（3.862 秒），默认测试栈上的完整 OpenRaft storage-v2 Suite 通过（10.199 秒）。本轮没有新增诊断负载或挑选成功测试。原 [37/38 Claim 失败](2026-10-21-claim-postcommit-unknown.zh-CN.md)的具体原因仍未知，不因本次通过而改写；`80c` 及更早失败/停止的 cohort 也继续独立保留。

## 完整制品

远端私有根为 `/root/tidemq-validation/wal-coverage-20261020.fwMOj3b7`，本轮报告位于 `report-cohort7-24471ce`。`coverage-24471ce-cohort7-accepted.tgz` 已在远端完整封存并核验 tar 成员，其 SHA-256 为 `b95000e90669aff74b725cc483633a2e2d0558e46da7881a1f28092fdbb857cc`。

归档包含 525 个正式 raw profiles、merged profdata、JUnit、LLVM full/summary、LCOV、HTML、原 compare、完整日志、原始 cgroup/阶段采样、身份、preflight 和固定 harness，不包含原 fixture 数据、凭据、私钥或 WAL。

| 原报告 | SHA-256 |
| --- | --- |
| LLVM summary | `3024dcc8083c98de1f8897c0242e534d15633d617abf0a6233fba6619ae0274c` |
| 完整 LLVM JSON | `3ea483f6a53b839c01f8607c6cba90e96fe26e0121a0983a07fd54e38fafa618` |
| 原 compare | `2b96743605bfa2f91671639f3289c0e906dc25325cdedebe29a9f24813b6576f` |

Mac 正在进行独立的 32 次性能测量，本轮仅取回小型 JSON/文本；大归档仍在远端，本地完整归档核验待该测量终态后进行。当前没有把远端归档冒称为已在本地完整核验。
