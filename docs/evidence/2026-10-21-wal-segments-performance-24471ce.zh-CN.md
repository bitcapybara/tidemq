# 24471ce 完整 macOS 性能 cohort：精度不足，未放行

本轮原性能命令完成了全部 32 个试次，终态为 `inconclusive`、exit 1。RF1 通过；RF3 的生产平均延迟精度不足，不能用吞吐、ACK 或功能测试的通过替代。性能和覆盖率基线均未晋升，也没有为本轮修改负载、期限、重试、样本数或统计策略。

## 固定身份与完整性

| 项目 | 实际身份 |
| --- | --- |
| 候选源码 / tree | `24471ce18f26739eefc47af63fdd0c183e9a787c` / `062cacc15ac04a59bc5c38002fce2a17f8f9e2c1` |
| 基线源码 | `693f9bd91a570758f7891f15edb2e95fdcf47864` |
| 固定构建 manifest SHA-256 | `025f2b3adb57bf99496af58f37a332b32f8a811fca7a792997fec94bfbf2d928` |
| 基线服务 binary SHA-256 | `9806c39098c2eaa637b3f923899fc4213dc7f4745537de894ae34f7e43b263a6` |
| 候选服务 binary SHA-256 | `05d451fbaa68fcfe73437aab1659a84e531e71d99458d2c1ccfd8aa99395ce76` |
| 基线 / 候选 SDK 负载 binary SHA-256 | `cd6c5c9687bce1f8c431e60a23f7554b004f47a4937802e667b6325d7951107d` / `fe5b4c032ca8b98a0b4b9c9b6b57a4874a0b491a6d141063a1f61699044a8839` |
| 编译器 / allocator | Rust 1.99.1，服务显式 mimalloc，SDK generator 为 System |
| 测量宿主 | Apple M2 Max，12 logical CPU、64 GiB，macOS arm64 / Darwin 25.6.0，Node 26.12.0 |

四份二进制由原 `prepare` 命令在干净源码上实际构建。每次清理 workspace release 产物并复用第三方依赖，SDK 负载使用同一受保护 harness；测量前核验外部 manifest digest。测量全程保持候选和 tools 源码不变，本机没有其他 agent 构建或大制品回传。

全部 32 试次、64 份 workload report、65,536 条测量消息、12,288 条 warmup 消息及 64 次节点正常退出完整。所有报告经原 `summarizeReport` 独立复算，消息数、原始统计、八对交替顺序、每个节点 `code=0 / forced=false / output_error=null` 均与最终汇总一致。独立审计在写出这些核验结果后，最终 `status == passed` 断言仍为 exit 1，正确保留原 `inconclusive`。

## 原统计门禁

比较器保持八对配对、Bonferroni 十项比较、整体 95% 置信度，每项 99.5%、自由度 7。主指标的最大 log CI 半宽仍为 `0.15`。

| 拓扑 | 主指标 | 几何减速比 | log CI 半宽 | 原判定 |
| --- | --- | ---: | ---: | --- |
| RF1 | durable messages/s | 0.965575 | 0.077664 | passed |
| RF1 | produce mean | 0.951026 | 0.130521 | passed |
| RF1 | offered-to-durable-ACK mean | 0.961613 | 0.130943 | passed |
| RF3 | durable messages/s | 0.949441 | 0.019990 | passed |
| RF3 | produce mean | 0.673249 | **0.379174** | **inconclusive** |
| RF3 | offered-to-durable-ACK mean | 0.844862 | 0.080080 | passed |

RF3 的八个 produce mean 候选/基线比分别为 `0.658111、0.670896、0.818777、0.936722、0.576715、0.812226、0.676477、0.393354`，各点估计均改善，但效果的离散程度仍超过原精度约束。调整后区间为 `[0.460790, 0.983668]`；不能因为点估计改善或区间上界低于 1 就忽略独立的精度要求。

RF1 两项 p99 和 RF3 produce p99 为 `imprecise`；RF3 ACK p99 为 `diagnostic`。这次测量不证明所有尾延迟均无退化。完整数值及逐对比值见[独立统计审计](wal-segments-performance/20261021-24471ce-audit.json)。

## 其他门禁与后续边界

本源码 macOS 完整 nextest 为 364/364 通过、13 skipped、184.413 秒，run ID 为 `1da85db9-4614-4a69-b793-0e9f142139df`；文档 4 项、Node 44 项、全 target 严格 Clippy、生产 panic/索引审计、配置和 mimalloc 探针均 exit 0。探针的原生版本标识为 `30302`。原始日志身份见[本机摘要](wal-segments-performance/20261021-24471ce-macos.json)。

新 Linux 完整测试、覆盖率及 Docker 十项故障验证分别保留自己的源码和制品身份；这些通过不替代本次性能结论。原 `80c7659` 的 RF3 latency deadline 失败、`4f79a5` 的 throughput 失败，以及原 37/38 Claim 原因未知记录继续保留。本轮全部候选消息均完成持久 ACK，不把它改写为历史失败原因已被唯一确认。

已对固定 40 ms offered schedule 附近的迟到、调度后到完成时间及 WAL 阶段指标完成[有界分解](2026-10-21-performance-24471ce-queue-amplification.zh-CN.md)：约 96%–99% 的原 produce 均值来自计划发送迟到，支持串行排队放大机制，但不足以唯一归因于 fsync 或宿主调度噪声。已授权的 `ssh dev` Docker 环境用于独立声明的一组 Linux 测量；构建与测量使用不同容器、同一份冻结源码和原策略。Mac 非通过数据完整保留，新环境结论仍须等待真实完整 cohort，不能预报通过或反复运行至成功。

完整原始数据保留于被 Git 忽略的 `target/wal-segments-performance-{build,result}-24471ce/`，汇总、命令日志及审计脚本的 SHA-256 见[制品摘要](wal-segments-performance/20261021-24471ce-artifacts.json)。本文及小型摘要不含集群配置、凭据、TLS 私钥或 WAL 正文。
