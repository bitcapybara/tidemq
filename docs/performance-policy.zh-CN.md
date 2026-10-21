# 性能测试与基线暂缓策略

2026-10-21 按用户最新要求：暂停 CI 自动性能测量和提交前性能测量，后续按需、不定期手动安排。此决定取代此前每个 PR／main push 必须完成 RF1/RF3 性能比较才能提交的流程要求。

## 当前执行范围

- [Performance 工作流](../.github/workflows/performance.yml) 仅保留 `workflow_dispatch`，PR、push 和定时任务均不自动触发。
- 本地开发、提交、合并与 push 不以性能测试为前置条件；本轮不再补跑或重启任何性能 cohort。
- [Rust 工作流](../.github/workflows/rust.yml) 的 fmt、全 target 严格 Clippy、生产 panic／索引审计、nextest、文档测试和 LLVM 覆盖率门禁继续执行。按用户后续要求移除独立 allocator 矩阵、heap profiling job 及 allocator probe 步骤；后端 features、实现和本地诊断入口保留。比较器、清单和信任边界的单元测试继续保留，它们不运行 RF1/RF3 性能负载。
- 现有负载代码、构建／测量／比较／晋升脚本、统计策略、历史报告及两套已接受基线全部保留。没有降低阈值、替换失败样本或改写历史结论。
- 暂停自动基线晋升。原晋升 job 仍要求 `push main`，仅手动 dispatch 的工作流不会满足此条件，因此手动测量也不会自动改写基线。覆盖率继续严格比较既有基线并生成 JSON／LCOV／HTML；原联合晋升流程恢复前，两套基线均保持当前值。

## 手动入口

在另行决定需要测量时，可从 GitHub Actions 手动选择 Performance，或运行：

```sh
gh workflow run performance.yml --ref main
```

该命令本轮没有执行。手动运行仍使用原隔离构建、固定二进制 manifest、RF1/RF3 八对交错、持久 ACK 校验和统计策略。原本地命令、环境要求和矩阵保留在[性能手册](performance.zh-CN.md)。后续如需调整负载、安排频率、接受新基线或恢复 PR 性能门禁，先明确对应策略再执行；本轮不新增定时自动化或自动接受规则。

## 本轮已有结果

冻结产品源码为 `24471ce18f26739eefc47af63fdd0c183e9a787c`。功能、覆盖率和 Docker 故障验证见[实施状态](implementation-status.zh-CN.md)。

- [macOS 完整 cohort](evidence/2026-10-21-wal-segments-performance-24471ce.zh-CN.md)：32 个试次完整结束，原结果仍为 `inconclusive`，RF3 produce mean 精度不足；未晋升。
- Linux 独立 cohort 在收到本次要求后停止。已有第一对 RF1 两侧完整结束，下一试次只有 throughput 完成、latency 被取消；共 3 份 execution、5 份 workload report，没有 RF3 结果。原驱动的 `failed / SIGTERM` 日志保留，外层原因明确为 `user_cancelled / incomplete / not_evaluated`，既不算产品性能失败，也不算通过。
- 所有原始数据和身份分别封存；不将局部结果拼接为完整 cohort，不因暂停而替换旧基线。
