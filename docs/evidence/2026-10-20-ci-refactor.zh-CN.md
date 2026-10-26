# CI、代码质量与角色配置重构证据

## 版本与范围

本轮按用户授权在独立 `wt` worktree 并行执行 CI、代码质量、角色配置和 WAL 分析，然后在 `feat/yyx/integration-main` 正常合流并快进 main。首个推送源码为 `401c7c959c0fe5f44fea0d4039a1238e2747920c`。

- CI：`c7b3dd2`。
- 代码质量与 Rust 兼容修正：`7d877e1`、`d3f210c`。
- 角色配置：`00c54f2`、`ede3b0c`；最终只接受当前格式。
- SDK OpenTelemetry 指标：`fdd3d74`。
- WAL 分析：`b926f7c`，开发阶段不做旧格式迁移的说明另以 `fe630cd` 更新。

Rust 固定 1.99.1，覆盖率与普通测试使用 cargo-nextest 0.9.148、cargo-llvm-cov 0.9.1、配套 llvm-tools-preview。工具版本和 Linux x64 基线范围写在 `coverage/policy.json`；LLVM JSON 导出版本为 3.1.0。

## 本地实际检查

以下为 macOS arm64 的实际终态；最终 Rust 源码对应 `1102c1b`，`693f9bd` 仅补充证据和本轮 cohort 声明。早期检查单独标注，不与最终检查混为同一次运行。

| 检查 | 结果与范围 |
| --- | --- |
| workspace fmt | 通过 |
| workspace 全 target Clippy | `-D warnings` 通过；包含 SDK metrics／opentelemetry 与 protocol codegen |
| 生产 panic／索引 Clippy | 通过；lib／bin 单独启用 unwrap、expect、panic、todo、unimplemented、unreachable、indexing_slicing 禁止项 |
| workspace nextest | 首轮普通／CI 串行各 309 passed／12 skipped；三项修复汇合后源码 `1102c1b` 的 CI 串行 profile 为 310 passed／12 skipped，测试阶段 155.320 秒；这些是重叠回归，不相加 |
| workspace 文档测试 | 4 passed |
| SDK metrics／opentelemetry nextest | 39 passed；与 workspace 重叠，不相加 |
| SDK 全 feature、无可选观测 feature 的 Clippy | 均通过 |
| CI 边界测试 | 初次 Node 42 passed；修复后 44 passed，新增真实 Cargo 交替构建与有界错误原因回归；精确覆盖率比例、伪造报告、基线替换、联合晋升失败、性能统计和配置预算边界继续通过 |
| CI YAML／shell | 两份 workflow 解析、32 个 run shell 片段语法通过 |
| 当前工具真实 CLI smoke | 临时三 crate workspace 的 4 个 nextest 测试通过，实际生成并读取 JSON／LCOV／HTML；此结果不是 TideMQ 覆盖率基线 |
| 实际配置预检 | 当前 debug 二进制的 RF1 和 RF3 三节点 diagnose JSON 均通过严格归一化核对 |
| Collector | 官方 core 0.162.0 对新增 metrics pipeline 的配置校验通过；未在本轮新增远端 OTLP 导出实测 |

12 个跳过项分别为 10 个需要受验证 RustFS 的测试、1 个需要官方 Collector 的测试和 1 个需要 release 模式单独执行的 codec 时间探针；没有计为通过。先前版本的 Docker／RustFS 证据保留各自源码身份，不用于证明本轮版本已经重跑。

## 行为与取舍

multipart 的 Admission 由 owned worker 持有，worker 和 abort 完成后才释放；阻塞 abort 的回归测试验证该边界。真正没有用途的参数已删除，具有 RAII 生命周期用途的 guard 保留。错误日志从 map_err 移至 inspect_err，保留原错误与完整错误链。

Gateway 的授权身份与取消请求索引复用 `Arc<str>`，仅在网络编码边界生成需要的字符串，未新增 FastStr 依赖。SDK 可选 OpenTelemetry feature 使用宿主全局 MeterProvider，注册十个固定无标签 instrument，不安装 provider／reader／exporter／后台线程。宿主负责导出资源预算与关闭；原 Prometheus 注册接口仍独立可用。

配置分为公共段、authorization、runtime 和适用的 gateway／storage 段。旧字段、混合布局、不适用角色段、未知字段及生成器第三参数均拒绝。当前服务没有旧格式兼容分支；历史性能基线源码只在测试工具中使用其自身冻结生成器，完整有效配置严格核对后才启动测量。

WAL 分析建议生产版本采用 segment，避免 purge 时 O(保留尾部) 重写；应用完整快照仍有独立的 O(状态) 复制／编码成本。分段实现必须同步容量采样、迁移预留、manifest、读者 pin 与恢复／删除屏障。本轮没有实现新存储格式或为其新增故障测试，详情见 `docs/research/wal-segments.zh-CN.md`。

## 第一轮 Linux CI 与基线

Rust：[39402803302](https://github.com/bitcapybara/tidemq/actions/runs/39402803302)。

Performance：[39402803303](https://github.com/bitcapybara/tidemq/actions/runs/39402803303)。

源码 `401c7c9` 的 Coverage job 已成功：插桩 nextest 309 passed／12 skipped，测试阶段 124.969 秒。原始 LLVM 报告共 127 个源码文件，覆盖 root、protocol、transport、SDK 以及 workspace build.rs。主流程独立调用受保护的 `validatePromotion` 再次核验原始计数、源码、策略、旧基线摘要、工具与 Rust run 身份，验证通过但未写入仓库基线。

| 覆盖率 | 已覆盖／总数 | 展示比例 |
| --- | --- | --- |
| 行 | 27892／35173 | 79.2995% |
| 函数 | 2574／3637 | 70.7726% |
| region | 38340／49845 | 76.9184% |

门禁比较原始整数交叉乘积，不使用展示比例。第一轮 Rust 全流程失败，以上覆盖率没有晋升为已接受基线。

## 第一轮失败与具体修复

Rust 流程 8 个 job 中 6 个成功；System allocator 为 247 passed／2 failed／11 skipped，因此 Rust gate 失败。Linux 与 macOS 默认 workspace 均 309 passed／12 skipped，文档各 4 passed；Linux SDK 默认 feature 37 passed，jemalloc 249 passed／11 skipped，两平台 heap 各 1 passed。这些检查有重叠，不能相加。

System 两个失败分别为 combined RF3 阶段到期与换主 helper 到期。已经分别锁定并修复两个实际缺陷，但原始 Linux 失败没有足够阶段信息，不能声称排除了所有触发原因：

- [过期 Control 回复](2026-10-20-expired-control-replies.zh-CN.md)：真实 TLS／QUIC 中，已过期结果会让共享回复 writer 退出，使无关 RPC 返回 Closed。修改前 4／4 red，修改后 30／30 green；生产改动仅为发送前跳过过期回复，未改 deadline、lease、重试或准入配额。
- [Metadata 权威 helper](2026-10-20-scaling-metadata-authority.zh-CN.md)：真实 Metadata leader 已变更时，旧 helper 仍固定查询 node 1。权威探针确认旧节点返回 ForwardToLeader，而新 Metadata 与 Data leader 都能完成线性读；强化测试修改前 13.324 秒 red，修正 helper 后在原 12 秒期限内 green，合流后 System suite 3／3、强化回归 20／20。原计划、失 quorum、退役与持久完成断言保留。

Performance 构建完成后，候选 `validate-config` 拒绝新 `authorization` 段，尚未产生候选性能数据。取回准确的四个产物，服务两侧哈希均为 `d5ac0fd46f875fc667b0286792b876d3e5800b2f74c425e7bd2924e6974559a8`，负载两侧哈希均为 `9de1047974984679aa20bcc7feac87713aad7917320f1b7059021248786a86d1`；候选构建仅为 0.26／0.19 秒。原始失败汇总逐字节保存在 calibrations（相关性能资料已移除）。

主流程用两个相同 package 名称、不同内容和旧文件时间戳的真实 Cargo workspace，复现第二次构建被判 fresh 却执行第一个版本的错误。修复后每次构建先 `cargo clean --workspace --release --locked`，四次交替构建均执行正确版本，外部依赖 rlib 的 mtime 不变；该 red／green 直接调用正式构建函数，约 1.4／3.3 秒。构建 job 允许真正重建四个 workspace 产物，上界从 30 改为 45 分钟；测量 55 分钟上界、负载、全部试次及统计门槛不变。失败摘要另保留有界错误原因，artifact 收录配置校验日志。

这些修复完成后声明新一轮完整 cohort：候选为合入 `6f8ca0d`、`95194d6`、`f8eb251` 的下一次干净 main push，由工作流记录完整 Git SHA；基线仍为 `d7c913e`，使用同一 Rust 1.99.1。两拓扑各八个交替 A/B、B/A pair，不能复用首轮样本、删除慢试次或追加直到通过。只有新一轮全部 Rust／覆盖率和完整性能比较成功才联合晋升；现有基线与历史失败报告不重标。

## 第二轮实际结果与基线

候选完整源码为 `693f9bd91a570758f7891f15edb2e95fdcf47864`，与基线 `d7c913e` 均使用 Rust 1.99.1。测量结束前 main 保持在该候选，未复用首轮样本或修改验收策略。

[Rust run 39411371654](https://github.com/bitcapybara/tidemq/actions/runs/39411371654) 的 8／8 job 全部成功。Linux／macOS workspace 各 310 passed／12 skipped，文档各 4 passed；Linux SDK 默认 feature 为 37 passed；System／jemalloc 各 250 passed／11 skipped，heap 两平台各 1 passed，Node 边界测试 44 passed。这些检查存在重叠，不相加。

System 中首轮两个失败场景本轮分别在 7.926 秒和 3.816 秒通过，jemalloc 分别为 6.239 秒和 3.602 秒；新增过期回复回归也通过。Linux 默认下同三场景分别为 5.970、2.896 和 0.600 秒。未放宽原期限或添加重试。

Coverage 原始 LLVM artifact 为 `12259986265`，共 127 个源码文件，范围与首轮相同，包含 workspace build.rs。独立审计重算原始计数并通过受保护的 `validatePromotion`：

| 覆盖率 | 已覆盖／总数 | 展示比例 |
| --- | --- | --- |
| 行 | 27907／35176 | 79.3353% |
| 函数 | 2576／3637 | 70.8276% |
| region | 38365／49848 | 76.9640% |

原始 LLVM JSON SHA-256 为 `f62befee53ba66d1870dfa643acd61f5859c1a1191a2989e35cd3880b9eb23bf`，策略 SHA-256 为 `ff5e41e78593ce972649cee96b768db3132bb8a157d08bcded4f48329b0bed57`。门禁仍比较三项原始整数分数，展示比例不参与判断。

[Performance run 39411371718](https://github.com/bitcapybara/tidemq/actions/runs/39411371718) 的 build job `118589678584` 与 measure job `118593430705` 均成功。构建耗时 9 分 41 秒，两侧服务分别实际编译 2 分 34 秒，负载工具分别编译 1 分 26 秒和 1 分 24 秒。取回四个实际产物后重新计算 SHA-256，均与固定 manifest 一致，两侧服务与负载哈希分别不同：

| 产物 | 基线 SHA-256 前缀 | 候选 SHA-256 前缀 |
| --- | --- | --- |
| 服务 | `5760bb1fdfb2` | `a3d5788789cd` |
| SDK 负载 | `9de104797498` | `829f133cd4b4` |

完整 manifest SHA-256 为 `b318e4e1b0c1c3a3005ac54c23c5b2796dd41c276ce2030c6aaf8f82798e8205`，性能汇总 SHA-256 为 `c16e933c8d0074b04acc8fbc1923b658855c5e01d0486bf3a60249d5529ec058`。另一个 agent 独立重算全部 64 份原始 workload 报告与 32 份 execution：65,536 条测量消息、12,288 条预热消息均有持久 ACK，64 个节点退出均正常；全部配置预算、生成器 Git 原始摘要、四个真实二进制和冻结 Cargo.lock 身份相符。

两拓扑各八对固定样本、五项指标严格重算均无 resolved regression，三个主指标的 log CI 半宽均不超过原有 0.15 门槛。下表慢化比率统一为大于 1 表示变慢，吞吐取倒数；区间为原有十项比较族的单项 99.5% 校正区间。

| 拓扑 | 指标 | 慢化比率 | 校正 CI | log 半宽 | 结论 |
| --- | --- | --- | --- | --- | --- |
| RF1 | 吞吐 | 1.0015 | 0.9934—1.0096 | 0.0081 | passed |
| RF1 | produce 均值 | 0.9927 | 0.8882—1.1094 | 0.1112 | passed |
| RF1 | produce p99 上界 | 1.0267 | 0.9130—1.1545 | 0.1173 | diagnostic |
| RF1 | offered→持久 ACK 均值 | 0.9957 | 0.9633—1.0292 | 0.0331 | passed |
| RF1 | offered→持久 ACK p99 上界 | 0.9978 | 0.9705—1.0258 | 0.0277 | diagnostic |
| RF3 | 吞吐 | 0.9952 | 0.9570—1.0349 | 0.0391 | passed |
| RF3 | produce 均值 | 1.0215 | 0.9654—1.0808 | 0.0564 | passed |
| RF3 | produce p99 上界 | 1.1828 | 0.7636—1.8322 | 0.4376 | imprecise |
| RF3 | offered→持久 ACK 均值 | 1.0117 | 0.9932—1.0304 | 0.0184 | passed |
| RF3 | offered→持久 ACK p99 上界 | 1.1672 | 0.7677—1.7747 | 0.4190 | imprecise |

RF3 两项 p99 精度不足，点估计也较慢，不能宣称尾延迟无退化；按原有策略，p99 保留显著回退否决，宽度本身不阻塞主指标验收。全部慢试次保留，没有删样本、补试次、放宽门槛或重标旧报告。跨平台复算两处派生浮点值相差 1 ULP，严格比较的结论相同，原 Linux 报告逐字节保留。

## 计费阻塞与本地审计晋升

Performance workflow 整体状态仍为 failure：汇总 job `118607602402` 的 `runner_id=0`、`steps=[]`，GitHub 拒绝启动 runner，注释原文为：

> The job was not started because recent account payments have failed or your spending limit needs to be increased. Please check the 'Billing & plans' section in your settings

因此自动晋升 job 被跳过，不能把该 workflow 写成成功。付款或额度设置没有更改，额度处理已交用户决定。

在 main 仍为 `693f9bd`、Rust 全部成功、performance build／measure 成功及原始数据交叉审计通过后，本地执行了与工作流相同、未经修改的 `scripts/promote-baselines.mjs`，同时传入 Rust run `39411371654` 和 Performance run `39411371718`。原脚本重新核验同一干净 HEAD、生成器与配置、全部统计结论、覆盖率原始计数、策略／旧基线／制品摘要和 run 身份后，才写入两套基线。

提交 `d38fb80` 一次记录 `coverage/baseline.json`、`performance/baseline.json` 和 `performance/baseline-report.json`；两套基线均指向真实测量源码 `693f9bd`，性能报告与原 artifact 逐字节相同。当前覆盖率已是数值基线，后续 PR 三项任意精确比例下降都会失败。此次为本地审计晋升，未伪造 hosted 汇总结果；恢复 Actions 额度后的远端验收仍需实际运行。

## 验收边界

同机 RF3 不等于三个可用区；本轮没有生产 LB／跨区／掉电／长稳验收。当前 GitHub 私有仓库套餐对 main protection API 返回 403，检查可以失败但无法设置 required status；未更改套餐或仓库公开性。
