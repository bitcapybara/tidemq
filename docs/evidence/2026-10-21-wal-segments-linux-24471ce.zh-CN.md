# 分段 WAL 最终冻结版本 Linux / Docker 验证

`24471ce18f26739eefc47af63fdd0c183e9a787c` 在 `ssh dev` 完成了新的普通 Linux 全部门禁、release 构建、新镜像内核验，以及全新卷 RF1/RF3 十项验证。十项共计 736 条持久 ACK，各项 `expected / committed / received / acknowledged / frontier` 均一致。本轮没有重试失败的产品用例，也没有修改源码、预算、deadline、门槛或性能/覆盖率基线。

## 精确身份

| 项目 | 值 |
| --- | --- |
| 产品源码 SHA | `24471ce18f26739eefc47af63fdd0c183e9a787c` |
| Git tree | `062cacc15ac04a59bc5c38002fce2a17f8f9e2c1` |
| Cargo.lock SHA-256 | `d4c609c32697a3d04f3b9b235bb746943804a7fac9fc63c51da4db6f2e90d63e` |
| 源码 archive SHA-256 | `e3a9ded3546ed99680fbce69a402a1614ace40c9619f34686cbfd96eef319e2f` |
| Git bundle SHA-256 | `062a5df79cf813169090b2244d892260e7273593839f067db07a47f2d0510ee5` |
| builder image | `sha256:86fd5dd1673b5f458ceeec96cd228e2580402e92edae806605fc13d7864ee291` |
| runtime tag | `tidemq-wal-segments:24471ce-xnfbxhby` |
| runtime image | `sha256:b6d450aba5f13096a429d20a2a7f25a6870902ede04dce39bf2ba3cb9aabbc8c` |
| Linux 服务二进制 SHA-256 | `4ecd9ddaa6d1f833dcb9744a2e0750d9a1d838d12a703b713aa92ece30c9bac8` |
| 验证客户端 SHA-256 | `c371e2c1fe459d307d69843b5f34ac3a9310cc114fe8eaf5f9f079c7a0472fb2` |

完整[源码身份](wal-segments-linux/20261021-24471ce-source.json)、[镜像身份](wal-segments-linux/20261021-24471ce-image.json)与实际命令均保留。源码从本轮 archive + bundle 独立导入，Git HEAD/tree、锁文件、tracked clean 与生成文件一致性在容器内实际核验。验收使用只读的 `source-24471ce`，之后的文档提交不改变本次产品身份。

客户端由本轮 `cargo clean --workspace --locked` 后的 release 流程重新构建；其字节恰与旧版本相同，不是复用了旧镜像、数据卷或用例结果。镜像中两个实际二进制的 SHA 均与本轮 release 文件相同。服务默认 allocator probe 为 `mimalloc / service_binary / 30302`，镜像构建、二进制核验和 probe 分别 exit 0。

## 普通 Linux 完整门禁

环境为 Linux/x86_64、Rust 1.99.1、LLVM 23.1.1、nextest 0.9.148、Node 22.23.3。builder 为 UID/GID `10001:10001`、4 CPU、8 GiB RAM / 8 GiB RAM+swap 总额、512 PID、network none、只读根文件系统、cap-drop ALL；Cargo jobs 为 2，target 与覆盖率流程相互独立，公开依赖缓存离线使用。

| 检查 | 真实终态 |
| --- | --- |
| fmt、workspace/all-targets 严格 Clippy | exit 0 |
| codegen 格式化及与冻结生成文件逐字节比较 | exit 0 |
| 生产 lib/bins 的 panic、unwrap、expect、indexing 等审计 | exit 0 |
| workspace/all-targets nextest | **364/364 PASS，13 skipped，213.679 s** |
| doc tests | **4 PASS** |
| SDK 默认配置严格 Clippy / nextest | exit 0；**37/37 PASS，10.973 s** |
| validate-config / allocator-probe | exit 0 |
| Node trust-boundary tests | **44/44 PASS** |
| 真实 RustFS 原十项 | **10/10 PASS，153 skipped，38.400 s** |
| release 服务及验证客户端 | exit 0，3 min 49 s |

真实 RustFS 测试仍使用原选择器，包含 `actual_runtime_sdk_automatically_compacts_partial_cold_objects_and_reclaims_sources`（13.017 s）、本地回收后 S3 重放（3.054 s）及 RF3 独立 leader 归档重放（7.341 s）。没有筛除 Claim 场景。完整不可变流程从 `04:01:50Z` 到 `04:22:44Z`，整体 exit 0；[普通门禁摘要](wal-segments-linux/20261021-24471ce-normal.json)记录逐步退出码、时间和日志 SHA。

实际[启动 argv](wal-segments-linux/20261021-24471ce-command.json)与[容器预检](wal-segments-linux/20261021-24471ce-launch-preflight.json)验证了完整 source label 等值、owner/image、只读源码及独立脚本挂载、UID、limits、control/log/tmp 可写、两个 target 挂载同 inode。执行中的[门禁脚本原文](wal-segments-linux/24471ce-run-gates.sh.txt)从启动后未再编辑，前后 SHA 均为 `6fb225bf7aad8a4669e6b234e343f2833768335df082a53dc96e4334468bad61`。

保留原 `nextest --no-run → private-target-only POSIX_FADV_DONTNEED → 原 argv 测试` 阶段隔离。三次 raw `memory.current` 为 `7,860,387,840 → 299,204,608`、`2,213,097,472 → 576,270,336`、`7,756,722,176 → 823,664,640` B，helper 错误均为 0。没有删除 profile、修改全局缓存或提高资源限额。619 份 2 秒 raw 采样的 peak 为 8 GiB，`oom_kill=0`；该峰值包含编译期间的文件缓存，不是单独的服务 RSS。

## 全新 RF1 / RF3 十项验证

本轮项目为 RF1 `tidemq-validation-rf1-917044c655fb5531` 与 RF3 `tidemq-validation-rf3-c24f19b9f1d664f7`。每个项目启动前的容器、网络、数据卷均为零，使用新凭据和新身份。十项运行窗口为 `04:25:23.779Z–04:37:45.192Z`，固定 driver 整体 exit 0。

| 拓扑 | 原用例 | 每项完整持久 ACK |
| --- | --- | ---: |
| RF1 | smoke、replay | 各 64 |
| RF1 | controlled OOM | 80 |
| RF3 | smoke、replay | 各 64 |
| RF3 | storage-kill、broker-kill、metadata-quorum、storage-quorum、gateway-kill | 各 80 |

[十项原始终态及 ledger](wal-segments-linux/20261021-24471ce-docker.json)记录每项 validator、evidence 路径和实际时间；合计 RF1 208 + RF3 528 = 736，是十个独立用例的总数。原 case timeout 720 s、startup timeout 120 s 均保留。[driver 预检](wal-segments-linux/20261021-24471ce-matrix-preflight.json)用相邻 stdlib venv 的 Python 执行 `py_compile` 并核对全部参数；[与旧 driver 的差异](wal-segments-linux/24471ce-matrix-driver-vs-80c.diff)仅为五处源码身份/日志常量，运行后未编辑。

实际容器检查确认 UID/GID 10001、只读根、cap-drop ALL、no-new-privileges、128 PID、无 host ports；各节点原限额 0.5 CPU / 384 MiB，RustFS RF1 为 512 MiB、RF3 为 1 GiB。每个项目使用自己的 internal bridge 和 network namespace，数据目录均实际验证 10001:10001、0700、可写。

受控 OOM 的同一容器 `a75306f7c945bb9d9c2cc259c41887afd6a458384dc306bf306edcfac7c5587a` 从 384 MiB 调至 6 MiB，实际出现 `OOMKilled=true / exit 137`，再恢复 384 MiB并重启，最终 ledger 80 全部一致。这验证调低限额后的恢复，不代表默认限额下自然负载 OOM。

结束后仅停止本轮项目：RF1 7 个容器、RF3 20 个容器，均 0 running。实际 `docker volume inspect` 确认 RF1 3 个、RF3 12 个 named volumes 仍存在且 project owner 正确。原 68 个业务容器的完整身份、state/health、limits、mounts 前后逐项相同，也与旧 80c 验证前记录一致；完整业务投影仅保留在私有目录。[制品摘要](wal-segments-linux/20261021-24471ce-artifacts.json)含对照摘要、真实 OOM 与数据卷检查。

## 原始环境失败与证据保存

两次私有准备错误均保留，没有把它们记作产品用例失败或隐去：

1. host driver 准备时 SSH 单引号导致原文本匹配校验失败，发生在写入新 driver 及启动任何 case 前。[准备错误摘要](wal-segments-linux/20261021-24471ce-preparation-error.json)保留原因；随后固定新 driver 并完成语法/参数预检。
2. 镜像创建与镜像内验证已经成功，但 image-driver 尾部首次 Compose config 缺少 `RUSTFS_BINARY`，driver 整体 exit 1，原脚本和错误日志保留。独立、固定的[continuation](wal-segments-linux/20261021-24471ce-compose-continuation.json)补上原官方 RustFS 路径及 SHA，两个完整 config render 均 exit 0。它于 `04:25:23.376Z` 完成，早于 matrix 启动和第一 case；没有重建已验镜像、修改运行中的脚本或重跑产品 case。

相邻 venv Python 为 3.12.3，以上私有 Python 工具仅依赖 stdlib，实际执行前均 `py_compile` 通过。旧 4f79/426/799/80c 的失败和结果仍分别保留，均不冒认本轮源码验收。

远端私有根为 `/root/tidemq-validation/wal-segments-20261020.xnfbxhby/`。`linux-docker-24471ce-evidence.tar.gz` 已封存，SHA-256 为 `2009438f18d2f735d7cd010189602be051f63033db7cfc96351844a66dc537f2`，3,016,943 B / 1,494 members；远端重新打开并逐一核验了 1,493 份文件 manifest 条目的大小与 SHA。24 种真实 secret 表示及 3 种私钥标记扫描均零匹配；凭据目录、registry、编译 target、二进制及 venv 未进入归档。Mac 独占性能测量期间未下载大归档，[远端回读核验](wal-segments-linux/20261021-24471ce-archive-verification.json)明确区分此状态。

同版[完整受保护覆盖率](2026-10-21-wal-segments-coverage-24471ce.zh-CN.md)由独立流程记录。本证据不替代独立 RF1/RF3 配对性能、最终 main 对应 SHA 的远端 CI，亦不代表三可用区、LB、真实掉电或长期压力验证。System/jemalloc/heap-profiling 新版本完整矩阵本轮未追加；本提交仅更新证据，没有晋升基线。
