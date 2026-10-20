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

以下为 macOS arm64 的实际终态，Rust 源码相同；CI 合流只增加脚本、配置和文档。

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

## Linux CI 与基线

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

Performance 构建完成后，候选 `validate-config` 拒绝新 `authorization` 段，尚未产生候选性能数据。取回准确的四个产物，服务两侧哈希均为 `d5ac0fd46f875fc667b0286792b876d3e5800b2f74c425e7bd2924e6974559a8`，负载两侧哈希均为 `9de1047974984679aa20bcc7feac87713aad7917320f1b7059021248786a86d1`；候选构建仅为 0.26／0.19 秒。原始失败汇总逐字节保存在 [calibrations](../../performance/calibrations/20261020-401c7c9-build-cache-failed.json)。

主流程用两个相同 package 名称、不同内容和旧文件时间戳的真实 Cargo workspace，复现第二次构建被判 fresh 却执行第一个版本的错误。修复后每次构建先 `cargo clean --workspace --release --locked`，四次交替构建均执行正确版本，外部依赖 rlib 的 mtime 不变；该 red／green 直接调用正式构建函数，约 1.4／3.3 秒。构建 job 允许真正重建四个 workspace 产物，上界从 30 改为 45 分钟；测量 55 分钟上界、负载、全部试次及统计门槛不变。失败摘要另保留有界错误原因，artifact 收录配置校验日志。

这些修复完成后声明新一轮完整 cohort：候选为合入 `6f8ca0d`、`95194d6`、`f8eb251` 的下一次干净 main push，由工作流记录完整 Git SHA；基线仍为 `d7c913e`，使用同一 Rust 1.99.1。两拓扑各八个交替 A/B、B/A pair，不能复用首轮样本、删除慢试次或追加直到通过。只有新一轮全部 Rust／覆盖率和完整性能比较成功才联合晋升；现有基线与历史失败报告不重标。

新 Linux 终态、覆盖率计数、性能结论与晋升提交在实际完成后补充；进行中的流程不计成功。

## 验收边界

同机 RF3 不等于三个可用区；本轮没有生产 LB／跨区／掉电／长稳验收。当前 GitHub 私有仓库套餐对 main protection API 返回 403，检查可以失败但无法设置 required status；未更改套餐或仓库公开性。
