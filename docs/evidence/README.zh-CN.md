# 历史验证归档

历史一次性命令、临时探针、重复 LLVM/运行快照和报告已移出当前工作目录，完整过程仍由 Git 跟踪。归档基准为整理前已推送的提交 `e0be78cfc00c81f7d36caaf3f7835b9c45e963b6`；不重写历史、不重复携带旧构建和缓存。

当前可重复执行的验证入口为[测试指南](../testing.zh-CN.md)和 [VM 实验室](../../deploy/lab/README.zh-CN.md)，最近部署验收见[合并结果](../../deploy/lab/evidence/2026-10-25/README.zh-CN.md)。覆盖率基线和所有可复用功能/故障回归测试仍保留在当前项目。

完整[历史目录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/)包含原始 JSON、日志摘要、临时脚本/补丁和失败尝试；下表按原报告保留索引：

| 历史报告 | 原路径 |
|---|---|
| [macOS release 分配栈探针](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-19-allocator-release.zh-CN.md) | `docs/evidence/2026-10-19-allocator-release.zh-CN.md` |
| [Broker 真实进程故障自动接管验证](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-broker-failover.zh-CN.md) | `docs/evidence/2026-10-20-broker-failover.zh-CN.md` |
| [CI、代码质量与角色配置重构证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-ci-refactor.zh-CN.md) | `docs/evidence/2026-10-20-ci-refactor.zh-CN.md` |
| [2026-10-20 参数清理、错误观察与字符串共享](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-code-quality.zh-CN.md) | `docs/evidence/2026-10-20-code-quality.zh-CN.md` |
| [已完成成员计划重放的并发确认](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-completed-membership-replay.zh-CN.md) | `docs/evidence/2026-10-20-completed-membership-replay.zh-CN.md` |
| [077004e Linux Docker 隔离故障矩阵（2026-10-20）](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-docker-077004e.zh-CN.md) | `docs/evidence/2026-10-20-docker-077004e.zh-CN.md` |
| [2026-10-20 过期回复导致共享 Control stream 关闭](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-expired-control-replies.zh-CN.md) | `docs/evidence/2026-10-20-expired-control-replies.zh-CN.md` |
| [Linux Docker 隔离部署实测记录（2026-10-20）](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-linux-docker.zh-CN.md) | `docs/evidence/2026-10-20-linux-docker.zh-CN.md` |
| [Linux CI 扩容测试的磁盘余量诊断](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-linux-scaling-capacity.zh-CN.md) | `docs/evidence/2026-10-20-linux-scaling-capacity.zh-CN.md` |
| [本地统一验收记录（2026-10-20）](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-local-validation.zh-CN.md) | `docs/evidence/2026-10-20-local-validation.zh-CN.md` |
| [默认 mimalloc 与诊断候选验证](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-mimalloc-default.zh-CN.md) | `docs/evidence/2026-10-20-mimalloc-default.zh-CN.md` |
| [自动对象压缩的本地证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-object-compaction.zh-CN.md) | `docs/evidence/2026-10-20-object-compaction.zh-CN.md` |
| [可选 OTLP 导出验证记录（2026-10-20）](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-otlp.zh-CN.md) | `docs/evidence/2026-10-20-otlp.zh-CN.md` |
| [扩缩容测试的 Metadata 换主回归](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-scaling-metadata-authority.zh-CN.md) | `docs/evidence/2026-10-20-scaling-metadata-authority.zh-CN.md` |
| [Claim 已提交后 Unknown 的有界调查与错误链修复](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-claim-postcommit-unknown.zh-CN.md) | `docs/evidence/2026-10-21-claim-postcommit-unknown.zh-CN.md` |
| [公开仓库后旧 main 的 hosted CI 第二次运行](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-hosted-ci-addf6d9.zh-CN.md) | `docs/evidence/2026-10-21-hosted-ci-addf6d9.zh-CN.md` |
| [HotRead 元数据内存准入修复证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-hot-read-admission.zh-CN.md) | `docs/evidence/2026-10-21-hot-read-admission.zh-CN.md` |
| [Live Read 页复用与一次原负载反馈](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-live-read-hot-pages.zh-CN.md) | `docs/evidence/2026-10-21-live-read-hot-pages.zh-CN.md` |
| [24471ce 完整 Linux 覆盖率证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-wal-segments-coverage-24471ce.zh-CN.md) | `docs/evidence/2026-10-21-wal-segments-coverage-24471ce.zh-CN.md` |
| [426ff65 分段 WAL 覆盖率与替换副本失败证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-wal-segments-coverage-426ff65.zh-CN.md) | `docs/evidence/2026-10-21-wal-segments-coverage-426ff65.zh-CN.md` |
| [80c7659 分段 WAL 完整 Linux 覆盖率证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-wal-segments-coverage-80c7659.zh-CN.md) | `docs/evidence/2026-10-21-wal-segments-coverage-80c7659.zh-CN.md` |
| [分段 WAL 的 Linux 覆盖率门禁证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-wal-segments-coverage.zh-CN.md) | `docs/evidence/2026-10-21-wal-segments-coverage.zh-CN.md` |
| [分段 WAL 最终冻结版本 Linux / Docker 验证](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-wal-segments-linux-24471ce.zh-CN.md) | `docs/evidence/2026-10-21-wal-segments-linux-24471ce.zh-CN.md` |
| [`80c7659` 的 Linux 与 Docker 验证](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-wal-segments-linux-80c7659.zh-CN.md) | `docs/evidence/2026-10-21-wal-segments-linux-80c7659.zh-CN.md` |
| [分段 WAL 的 Linux 与 Docker 验证证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-wal-segments-linux.zh-CN.md) | `docs/evidence/2026-10-21-wal-segments-linux.zh-CN.md` |

无需恢复到工作目录即可读取；需要复现旧版时先检出对应源码提交，不在当前环境执行旧的一次性删除脚本：

```bash
git show e0be78cfc00c81f7d36caaf3f7835b9c45e963b6:docs/evidence/2026-10-21-wal-segments-linux-24471ce.zh-CN.md
git ls-tree -r --name-only e0be78cfc00c81f7d36caaf3f7835b9c45e963b6 -- docs/evidence deploy/lab/evidence
```

旧环境迁移脚本 `cleanup-legacy-host.sh` / `cleanup-legacy-units.sh` 也已从当前目录删除，其授权仅覆盖当时那批旧环境，完整实现及执行结果仍在上述历史提交中。

针对旧提交的一次性[评审核对表](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/reviews/2026-10-21-addf6d9-assessment.zh-CN.md)也归入历史；它不再作为当前版本的问题清单或验收结论。
