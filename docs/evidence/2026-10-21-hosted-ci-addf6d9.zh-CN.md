# 公开仓库后旧 main 的 hosted CI 第二次运行

本记录只对应 `addf6d9ef48e5c04eb3a6223b348958a0a6e88d4` 的既有 push workflow 第 2 次运行。GitHub API 已返回仓库 `private=false`、`visibility=public`，runner 实际执行了测试和性能测量；本次失败已有具体测试与覆盖率证据，不能继续记为 runner 因额度问题未启动。[仓库](https://github.com/bitcapybara/tidemq)、[Rust](https://github.com/bitcapybara/tidemq/actions/runs/39419125570)、[Performance](https://github.com/bitcapybara/tidemq/actions/runs/39419125608)

**Rust workflow 失败；性能 build、measure、gate 成功；联合基线 promotion 因 Rust 失败而拒绝。** 没有在本次采集中重跑、修改分支保护、放宽门禁或更新基线。结构化身份、原始计数、job/step、性能区间及摘要 SHA 见[小型 JSON](hosted-ci/20261021-addf6d9-attempt2.json)。

## Rust：测试失败与覆盖率失败分别保留

run `39419125570`、attempt `2`、source `addf6d9` 的 8 个 job 均已终态：

| Job | 结果 | 具体范围 |
| --- | --- | --- |
| Validate (macos-latest) | success | 该 job 通过 |
| Validate (ubuntu-24.04) | failure | nextest 309/310 通过、12 skipped；HELLO 拒绝断言失败 |
| Allocator (allocator-system) | success | 该 allocator job 通过 |
| Allocator (allocator-jemalloc) | failure | nextest 249/250 通过、11 skipped；同一 HELLO 拒绝断言失败 |
| Heap profile (ubuntu-24.04) | success | 该 job 通过 |
| Heap profile (macos-latest) | success | 该 job 通过 |
| Coverage baseline (Linux) | failure | 插桩测试 310/310 通过、12 skipped；精确覆盖计数下降 |
| Rust gate | failure | 汇总门禁正确拒绝上述失败 |

Linux 与 jemalloc 的失败测试均为 `oversized_unclassified_hello_is_rejected_without_body_allocation`，断言是两秒内 `stream.receive()` 的结果必须 `is_err()`。原 hosted 日志没有输出返回值，不能由该日志单独判断是正常 EOF 还是成功数据。后续 `80afe17` 的独立本地回归确实捕获过 `Ok(None)`、rejected=1、frames_received=0 并修正测试，但没有把这个旧 hosted attempt 改判为成功。[Linux job](https://github.com/bitcapybara/tidemq/actions/runs/39419125570/job/118791851560)、[jemalloc job](https://github.com/bitcapybara/tidemq/actions/runs/39419125570/job/118791851591)

覆盖率 artifact `12286229209` 的原 summary 为 `regressed`。与仓库原基线相比，三个分母完全相同，covered 计数分别下降：

| 指标 | 原基线 covered / count | 本次 covered / count | covered 变化 |
| --- | --- | --- | --- |
| lines | 27,907 / 35,176 | 27,895 / 35,176 | -12 |
| functions | 2,576 / 3,637 | 2,572 / 3,637 | -4 |
| regions | 38,365 / 49,848 | 38,350 / 49,848 | -15 |

原 policy SHA 为 `ff5e41e78593ce972649cee96b768db3132bb8a157d08bcded4f48329b0bed57`，未修改。已核对本地保存的原 `llvm.json` SHA 为 `d3f8db2ccaf207ba9831f4d1e3310d0b62b75b546df08ac0388fbd95d40693f4`，与 summary 完全一致；“测试全通过”和“覆盖率门禁通过”是不同结论。[Coverage job](https://github.com/bitcapybara/tidemq/actions/runs/39419125570/job/118791851493)、[原 artifact](https://github.com/bitcapybara/tidemq/actions/runs/39419125570/artifacts/12286229209)

## Performance：原性能门禁通过，联合提升未执行

run `39419125608`、attempt `2` 的实际 job 状态为：

| Job | 结果 |
| --- | --- |
| Build fixed performance binaries | success |
| Measure RF1 and RF3 on a fresh runner | success |
| Performance gate (RF1 and RF3) | success |
| Record successful main performance and coverage baselines | failure，第 4 步 |

artifact `12288874860` 的 `summary.json` 实际为 `passed`，基线服务来源 `693f9bd91a570758f7891f15edb2e95fdcf47864`，候选服务来源 `addf6d9ef48e5c04eb3a6223b348958a0a6e88d4`。RF1 与 RF3 combined 各完成 8 对、合计 32 个侧试次，每侧包含 throughput 与 latency。它们使用该运行固定的 service/SDK 二进制与原统计政策；精确二进制 SHA、manifest SHA 和十项比较区间均保留在 JSON 中。[测量 job](https://github.com/bitcapybara/tidemq/actions/runs/39419125608/job/118799300582)、[性能门禁](https://github.com/bitcapybara/tidemq/actions/runs/39419125608/job/118817292967)、[性能 artifact](https://github.com/bitcapybara/tidemq/actions/runs/39419125608/artifacts/12288874860)

该 summary 中 throughput 和两个原始时延均值指标为 `passed`，P99 指标保留原 `diagnostic` 口径；不能把 `passed` 解释为尾延迟完全不变。环境是 Linux x64、同机配对、测量阶段不编译，`physical_availability_zone_validation=false`，不是三区验收。

promotion job `118817351138` 第 4 步执行 `gh run watch "$rust_run_id" --exit-status --interval 30`，实际输出为：

```text
Run Rust (39419125570) has already completed with 'failure'
Process completed with exit code 1.
```

该步骤使用 `bash -e`。上面是实际执行输出；日志前面展示的完整脚本文本不代表后面的命令已执行。失败发生在下载 coverage、调用 `promote-baselines.mjs`、创建或推送基线提交之前，所以性能测量成功没有绕过联合质量门禁。[promotion job 第 4 步](https://github.com/bitcapybara/tidemq/actions/runs/39419125608/job/118817351138)、[该提交的 workflow](https://github.com/bitcapybara/tidemq/blob/addf6d9ef48e5c04eb3a6223b348958a0a6e88d4/.github/workflows/performance.yml)

## 原始证据与验证边界

原始 API、jobs、Rust 日志、测量/promotion 日志、coverage/performance summary 与当时的 policy/workflow 快照保存在 core worktree ignored 目录 `target/evidence/hosted-addf6d9-attempt2/`，其 `SHA256SUMS` 的 SHA 为 `973c802228cf87a185ec79f4dcf525d0a14cd3b3505a7e94dbd3c7ac100be882`。完整 LLVM 报告复用 root 已下载的 coverage artifact，只核验摘要和 SHA。

性能原 ZIP 为 7,963,855 字节。本次通过 HTTP Range 只读取 ZIP 目录和根 `summary.json`，共传输 112,115 字节，没有下载逐消息报告。摘要为 168,134 字节，ZIP entry CRC32 已核对，SHA 为 `2af6d730c7db16e75f9189602436dff2f2bda0f366b93250a90b6f6e04f37bda`。整包 digest 只记录 GitHub 提供的值，不声称已在本地完整重算；临时下载地址与凭据没有写入证据。

这份记录不能用作后续 segmented WAL、`799d57a` 或更新冻结版本的通过证据。那些版本的完整测试、精确覆盖率、Docker/Linux 与性能结果继续按各自源码身份分别验收。
