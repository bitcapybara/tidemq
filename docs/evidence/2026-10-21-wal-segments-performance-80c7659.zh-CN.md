# 分段 WAL 80c7659 性能失败记录

冻结 `80c7659` 的 macOS 全量测试、严格 Clippy、生产 panic／索引审计与配置／mimalloc 探针通过；Linux [完整覆盖率](2026-10-21-wal-segments-coverage-80c7659.zh-CN.md)通过。**完整性能 cohort 退出 1，未通过联合验收，基线未晋升。** RF1 八对通过统计门禁；RF3 完成第一对后，第二对候选的 latency 子场景失败。成功试次不复用到下一次验收。

## 固定身份与原始条件

| 项目 | 身份 |
| --- | --- |
| 候选源码 | `80c7659c39b3429b185c4e6bfc3436c8e892917e` |
| 候选 tree | `0c8073e9372202e5206d0b74f1340c965219e655` |
| 基线源码 | `693f9bd91a570758f7891f15edb2e95fdcf47864` |
| prepared manifest SHA-256 | `57a5b858f98923a3f39a1f5d77613cc467bd6819c2c256aab0918daa3d2ea0cb` |
| 基线／候选 service SHA-256 | `9806c39098c2eaa637b3f923899fc4213dc7f4745537de894ae34f7e43b263a6` / `a58395c75e25a230ed39827202c8ef8b47c64e6e4ac79a455a4db42cec739cfa` |
| 基线／候选 SDK load SHA-256 | `ddd654f687e00fcfc0459277c1f85aed70109a1f2467bf91fe78523eaa3789b9` / `7eecb0b5a9c0a0868668d071994fd32483ccca6be45c362cd9dc05ab54bceb73` |
| 环境 | macOS ARM64、Apple M2 Max、12 logical CPU、64 GiB、Rust 1.99.1、Node 26.12.0 |

四个二进制隔离重建，service 使用默认 mimalloc，load 使用 System。两版 SDK 分别来自对应源码，采用同一受保护 workload 文件。原两拓扑各八对、平衡交错顺序、十项 Bonferroni 比较、精度门槛、40 ms 计划发送间隔、60 秒 phase 及所有请求预算保持不变。测量期间未编译、运行其他测试或下载大制品；root 的 HEAD 与 tracked 文件保持冻结。

macOS nextest 为 356／356、13 skipped、37 binaries、175.349 秒，另有文档测试 4 项、Node 门禁测试 44 项；[命令日志 SHA 和摘要](wal-segments-performance/20261021-80c7659-macos.json)独立保留。这些通过不能替代性能通过。

## RF3 原始失败

失败目录为 ignored `target/wal-segments-performance-result-80c7659/rf3-combined/1-candidate/`。同一个 trial 的 throughput 子场景四个分区均为 256 committed／256 acknowledged，warmup 各 32／32；随后 latency warmup 为 256／256。latency measured 的原结果为：

| 项目 | 结果 |
| --- | --- |
| planned／committed／durable acknowledged | 1024／1024／1000 |
| produce Unknown／ACK Unknown／control errors | 0／0／0 |
| Produce 完成耗时 | 51.197 秒 |
| Consume／ACK 截止耗时 | 60.003 秒 |
| terminal error | `RPC deadline exceeded; may have been sent: true` |
| 三个 service 退出 | 全部 code 0，无 forced／output error |

原 report SHA-256 为 `cdb84ab5ead87c7e4ec97f2d1196c032a5287c0de6cc3127fa1942e17e2c5420`。原始状态和计数见[有界审计 JSON](wal-segments-performance/20261021-80c7659-failed-audit.json)。整体 summary 是 `failed`，其中 RF3 的原序列化拓扑状态仍为 `running`、一对完成；保留该原值，不将它解释为进程仍在运行或整轮通过。

## 速度与超时的证据边界

| RF3 latency measured | Produce 秒 | Consume／ACK 秒 | 计划发送迟到均值秒 | 完成 ACK |
| --- | --- | --- | --- | --- |
| pair0 baseline | 48.630 | 57.820 | 3.748 | 1024 |
| pair0 candidate | 46.967 | 55.820 | 2.917 | 1024 |
| pair1 candidate | 51.197 | 60.003，失败 | 5.976 | 1000 |

三个样本的成功 receive 调用均值约为 324–326 ms。对应 Produce／Commit 服务阶段均值约为 45–49 ms，ACK 服务阶段均值约为 45–48 ms；Sync 约为 7.3–7.7 ms。候选各只有一次约 64–67 ms 的 WAL checkpoint。[阶段直方图原始计数差分](wal-segments-performance/20261021-80c7659-stage-delta.json)按各自 measured 前后 Prometheus 文本计算；新旧 WalQueue 的记录时点需核对，不能仅用其数值差判断排队回退。

这些观测支持“处理速度和排队累积接近固定 phase 期限”的待验证方向，没有证明快照死锁。停止前管理诊断采样中的 group2 committed／applied 均为 index5931，snapshot4999，purged3975；快照 publication pending／uncertain 均为 false，reader 为 0。该采样早于 owned 任务排空，不能当作关闭后最终持久投影。配额四项均未阻塞，容器限额在 macOS 为 Unsupported、未阻塞，RSS 约 85 MB。一次快照事件与超时相邻，不足以建立因果；事后持久状态也不能证明失败瞬间的线性化点或成功回包丢失。

后续只通过保留原条件的独立反馈和具体路径的正确性回归验证优化；不延长期限、降低计划发送速率、增加重试预算，或为有副作用的 Read 加通用重试。

## 历史 cohort 分别保留

`4f79a50` 的[原失败摘要](wal-segments-performance/20261021-4f79a50-failed-audit.json)属于 throughput：四个分区全部 committed256，ACK 分别为 7／256／6／6，三个分区收到 Unavailable／Unknown。它与本次 latency 的 1024／1000 超时分别记录。`426ff65` 只完成 prepared 构建、零测量试次；`799d57a` 在测试期限收紧前被替代，没有进入性能测量。没有把任一旧版的通过部分拼成新版本的完整 32 试次。

公开仓库后旧 main 的 hosted 性能门禁通过、Rust 失败和联合 promotion 拒绝见[独立 CI 证据](2026-10-21-hosted-ci-addf6d9.zh-CN.md)，其来源是 `addf6d9`，不用于本版 WAL 的验收。
