# 80c7659 分段 WAL 完整 Linux 覆盖率证据

## 完整结果

冻结源码 `80c7659c39b3429b185c4e6bfc3436c8e892917e` 的完整 Linux x64 覆盖率 cohort6 **退出 0，受保护门禁通过**：356／356 项通过、13 项跳过，测试阶段 177.315 秒。原始命令为：

```bash
bash /work/scripts/coverage-run.sh /work /report --require-baseline-unchanged
```

沿用原 policy 的 workspace all-targets、build-script、默认 mimalloc 与三个 features；nextest 使用原 `ci` profile、串行、retries 0，原 deadline 未调整。JUnit 中 356 项执行项的 failures／errors 均为 0；nextest 另报 13 项跳过，未计入 JUnit 根节点的 skipped 值。没有筛选成功样本或复用旧 cohort profiles。

| 指标 | 已覆盖／总数 | 展示比例 | 原基线已覆盖／总数 | 原基线展示比例 |
| --- | --- | --- | --- | --- |
| 行 | 30346／37856 | 80.161665% | 27907／35176 | 79.335342% |
| 函数 | 2812／3942 | 71.334348% | 2576／3637 | 70.827605% |
| region | 42027／53995 | 77.834985% | 38365／49848 | 76.963970% |

原始整数交叉乘积差为 `11003504`、`72652`、`23443721`，三项均通过。独立核验确认 LLVM summary、完整 LLVM JSON 和原 compare 的计数一致，包含 134 个 workspace 源码文件及 `crates/tidemq-protocol/build.rs`。**policy、分母规则、baseline 均未修改，baseline 未晋升。**

仓库保留[完整执行摘要](wal-segments-coverage/20261021-80c7659-cohort.json)、[原 compare](wal-segments-coverage/20261021-80c7659-summary.json)、[原 LLVM summary](wal-segments-coverage/20261021-80c7659-llvm-summary.json)及[精确计数核验](wal-segments-coverage/20261021-80c7659-audit.json)。这是该冻结版本的 Linux 覆盖率证据；性能联合验收独立进行。

## 源码、工具与资源身份

| 项目 | 实际身份 |
| --- | --- |
| Git tree | `0c8073e9372202e5206d0b74f1340c965219e655` |
| 源码 archive SHA-256 | `fd298a2bd674bc183a6fd4bb4ade0d70b2d5c6931aef43896666874dc28512a5` |
| Git bundle SHA-256 | `d72bb0af64f94d362f164c17c728c69df1ae3670d03cb6e3a2a5ec8ec29e9aa7` |
| Cargo.lock SHA-256 | `d4c609c32697a3d04f3b9b235bb746943804a7fac9fc63c51da4db6f2e90d63e` |
| builder image | `sha256:86fd5dd1673b5f458ceeec96cd228e2580402e92edae806605fc13d7864ee291` |
| 工具 | Rust／Cargo 1.99.1；LLVM 23.1.1；nextest 0.9.148；cargo-llvm-cov 0.9.1；Node 22.23.3 |
| 资源 | 2 CPU；8 GiB RAM；额外 8 GiB swap；512 PID；Cargo jobs 2 |
| 原 coverage-run SHA-256 | `1ae21b03ca9e5d83564483cfb6de60ce40a6183c0c7357744d0997ab39c03a89` |
| policy SHA-256 | `ff5e41e78593ce972649cee96b768db3132bb8a157d08bcded4f48329b0bed57` |
| 未变更 baseline SHA-256 | `638b8e963abe9e8936c7df0f2571093c0244b83946a4a173d5df7c7bf15fb2ee` |

真实 Git HEAD、tree、archive／bundle 与[源码 metadata](wal-segments-coverage/20261021-80c7659-source.json)一致。产品源码只读挂载；UID 10001、network none；target、Cargo cache、generated overlay、临时目录、profiles 与报告均私有隔离。容器采用原 owner／task 标签，源码身份由实际只读 mount 与 Git 核验提供，没有声称具备 source 标签。运行前后 tracked diff 与 generated diff 均为空，启动后 harness 未编辑。

## 构建与测试阶段隔离

继续使用已验证的透明 nextest wrapper：先以原参数加 `--no-run` 构建，再由原 private-target helper 执行 `POSIX_FADV_DONTNEED`，最后执行完全原参数。没有修改 wrapper、产品 90／80 水位、原限额或宿主全局 cache。

本轮 build-only 为 7 分 16 秒。helper 错误 0，raw `memory.current` 从 `7853785088` 降到 `56954880` B，ACK 前实际采样为 `56639488` B；正式测试调用 fresh 0.42 秒。正式 profiles 在 cache 操作前后、报告导出前后的 SHA 均一致。完整原脚本从 UTC `2026-10-21T01:45:19Z` 运行到 `01:56:03Z`。

275 次两秒 raw cgroup 采样与阶段原始资源文件均已封存；最大采样 current 为 `8589737984` B，包含构建阶段，不能当作测试阶段内存用量。

完整执行中，原 runtime explicit replacement 场景通过（3.967 秒），stable proof 回复丢失后恢复同一计划的 seam 通过（2.179 秒），收紧后的 remote 场景通过（4.148 秒），OpenRaft storage-v2 Suite 使用默认测试栈通过（9.724 秒）。新契约保留原计划身份与绝对 deadline，永久拒绝仍立即失败；seam 的 scaling RPC 为 codec loopback，Raft 复制使用真实 QUIC，详见[原失败与契约修正证据](2026-10-21-wal-segments-coverage-426ff65.zh-CN.md)。本轮通过没有反推原 426 失败的唯一源错误。

## 先前尝试分别保留

| cohort | 原始终态 | 证据口径 |
| --- | --- | --- |
| 426 cohort3 | 350 通过／1 失败，退出 100 | 原 `complete metadata` Unknown 失败保持原样，见前述独立证据 |
| 799 cohort4 | operator stop；run 143／phase monitor 1 | root 要求在测试收紧后重冻；停止时仍在编译，没有执行测试或 compare |
| 80c cohort5 | 退出 1 | workspace 编译完成后，原 wrapper 因缺 `/report/control` 目录退出；没有执行测试或 compare |
| 80c cohort6 | 356／356 通过，原 compare 通过，退出 0 | 本文完整验收结果 |

799 的 [operator-stop 原始 JSON](wal-segments-coverage/20261021-799d57a-operator-stop.json)保留调用、时段、身份和 1 个不完整 run 的 raw profile。cohort5 的[原始环境失败 JSON](wal-segments-coverage/20261021-80c7659-cohort5-environment-failure.json)保留具体缺失路径、guard、原退出码及 75 个 raw profiles。两轮都不作为产品失败、覆盖率数值通过或完整验收结果。

cohort6 仅修复私有环境准备：启动前创建并实际验证 control 及所有握手文件父目录，检查路径 absolute／非 symlink、UID 10001 实际读写、source 只读、target 双挂同 inode、LLVM profile／JUnit 输出、精确工具 PATH 与 SHA。相关[preflight 原始差异](wal-segments-coverage/20261021-80c7659-preflight-change.diff.txt)和完整前后 harness 均保留。原受保护脚本、wrapper、产品、策略、资源与 baseline 没有改动。

## 完整制品

远端私有根为 `/root/tidemq-validation/wal-coverage-20261020.fwMOj3b7`。各轮报告、源码、generated 与临时现场分别保留，不合并 profiles。文件及 SHA-256 记录在[制品索引](wal-segments-coverage/20261021-80c7659-artifacts.json)：

| 归档 | SHA-256 |
| --- | --- |
| `coverage-799-cohort4-operator-superseded.tgz` | `cca3c6cc971b10a71aada051376b11e0f3f784cc658b0b88797b7082f7dfc892` |
| `coverage-80c-cohort5-missing-control-directory.tgz` | `bd7602de661b28e3288c41a78005e0f2ba9fc1f5c6c99a1ceb037c39e43ec3fa` |
| `coverage-80c-cohort6-accepted.tgz` | `bac9c5cf595d3af9f56cf3b09ac4c25b847df1776345d5055dd13354ef567b79` |

cohort6 归档包含 517 个正式 raw profiles、merged profdata、JUnit、LLVM full／summary、LCOV、HTML、原 compare、日志、资源原始采样、环境／源码／容器身份以及阶段握手和 harness。profile 导出、独立核验均已退出 0，没有追加诊断负载或测试。归档不含 fixture 凭据、私钥或 WAL。

Mac 性能测量期间仅取回小型 JSON 和文本。性能测量终态后，两个 80c 完整归档均已下载到本 test worktree 的 ignored `target/wal-coverage-20261020/`，SHA 与远端索引完全一致，tar 成员可读；accepted 归档中的原 cohort、compare、LLVM summary、audit SHA 也与仓库小型证据一致。799 归档已在测量开始前下载并核验。本次仅补齐制品保留状态，没有改写原测试结果、覆盖率计数或 baseline。
