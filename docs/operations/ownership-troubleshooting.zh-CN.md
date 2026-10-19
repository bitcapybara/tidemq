# 所有权与授权排障

本页对应 `metadata::MetadataState`、`ownership::MetadataAuthority`、`DataAuthority`、`ControlLease`。当前是可供运行时接入的库 API；还没有独立的远端 `ownership diagnose` CLI，不能把基础 CLI 的本进程配置输出当作集群状态。

## 先定位一笔操作

记录受影响 Topic ID/分区、operation ID 和 SDK 首次失败时间。用只读 `diagnose_ownership(partition)` 查看当前阶段、epoch、owner、route generation 和 fence/recovery commit index；同时记录该输出的 revision、采集时间及来源。`local_applied` 可能陈旧，不能单独用来恢复写入。运行时必须先取得线性化屏障，才能把当前 grant、原操作结果或数据恢复状态作为授权证据。

状态查询、日志和 trace 都不产生新的提交事实；没有日志不表示未提交。RPC 超时后查询原 `operation_result`，并以原操作与原内容重试。一次尚未查到结果，也不能排除原请求随后提交。

## 按阶段处理

| 现象 / 阶段 | 核对证据 | 允许的恢复操作 |
| --- | --- | --- |
| `Granted` 停滞 | 目标 Broker、组 voter/Metadata 拓扑、授权读取错误、epoch/operation 是否仍当前 | 对当前 grant 重新取得 `VerifiedGrant` 并提交 fence；不手填更大 epoch |
| 数据 fence 已提交，元数据仍 `Granted` | 数据组线性化查询的 fence、同锁 applied LogId | 重新生成 `VerifiedFence` 并记录观察；复用原操作重试 |
| `Fenced` 停滞 | producer 和持久 ACK 检查点是否已从权威状态重建；是否缺日志/快照/对象依赖 | 完成真实恢复；保留必要数据和恢复引用，不用布尔开关强行就绪 |
| `Recovered` 停滞 | 当前 grant、预期 route generation、最新恢复证据 | 重新验证后条件 `ActivateRoute` |
| `Superseded` | 当前诊断中的更高 epoch 和新 operation | 让当前操作继续完成；旧操作不得覆盖新路由 |
| `GenerationConflict` / `RevisionConflict` | 原决策与最新权威 generation/revision | CRUD 用新操作提出新变更；接管原逻辑身份可刷新尝试 revision，但不得改变冻结的 grant/fence/checkpoint |
| `OperationConflict` | 是否改内容或预期 revision 后重用 ID | 保留原请求用于对账；明确的新变更使用新操作 ID |
| 老 Gateway 路由导致写入拒绝 | 数据当前 fence、请求 owner/epoch | 刷新路由并保留原 producer/sequence 身份重试，不能绕过 fence |

fence/恢复/激活都缺证据时保持停止服务。它们之间的重启可以从原操作继续，也可以由更高 epoch 安全取代；不要降低 epoch、清空 fence 或删除去重/ACK 检查点。

## 失去控制 quorum 或有效期

默认从线性化查询开始 3 秒到期，最大配置 10 秒。检查 `LeaseDiagnostic` 的 read age、remaining millis、epoch、route generation 和 JSON 中的 `fresh_linearizable_read`/`expired` 原因。读取失败、超时、成员不匹配或当前 grant 已变化时，刷新不能延长旧 lease。

`MembershipMismatch` 要核对实际 Raft voter 集、metadata 节点角色和 zone，以及数据组受信配置。joint 配置期间本批不生成新能力；不要临时改成 RF1 规避拒绝。元数据组和数据组的 group ID 不得重用。

到期后停止新生产、投递、控制变更、发布和 GC 授权。只有到期前已经取得的 ACK/读取收尾许可可以继续，最多额外 1 秒、默认 256 份，并仍受权限、数据 quorum、原 deadline 和当前 fence 限制。`FinalizationCapacity` 表示额度耗尽，不要通过重建 Authority 对象、并发刷新或增加重试绕开限制。

进程重启、leader 更换或休眠恢复后重新验证；不能从磁盘、wall clock 或日志推算剩余授权。恢复对象存储可用性也不等于恢复元数据授权。

## 指标和日志

先看 `tidemq_ownership_transition_duration_seconds` 的 stage 分布，定位 grant/fence/recover/activate 中哪段变慢，再用 `ownership_transition` 事件的 operation ID 和 epoch 查对应诊断。`Unknown` 表示响应/提交结果仍需对账，不能直接当作安全失败。

`tidemq_control_validity_remaining_seconds` 长期接近 0，应先检查 metadata quorum、网络、授权读取耗时和运行时调度，再检查刷新任务。这个 gauge 是所监测授权的最小剩余值，不代表每个组的状态；按组详情使用有界诊断，不增加无限基数指标标签。

运行时接入告警时：阶段耗时达到有效期预算、无当前路由且阶段持续停滞、授权剩余为 0 且已有业务拒绝，才形成需要处置的事件；恢复条件为新鲜授权、完整恢复证据和 ActiveRoute 同时成立。不要只因抓取失败就宣布 fence 失效，也不要只因进程存活就宣告可写。

日志只能包含有限阶段/结果枚举和必要 ID，不采集 token、证书私钥或消息正文。运行时输出指标/日志时不得在 snapshot 重放中重复产生新逻辑完成计数。

## 容量与支持包

`summary().remaining_operations` 接近 0 时提前停止非必要 CRUD 并安排容量/退休协议评审。默认 16,384 个 CRUD 原决策、1,024 个 Topic 和 4,096 个订阅都包含历史退休记录；达到上限会安全拒绝。现有分区接管使用独立四槽，不受 CRUD 满表影响，连续接管也不增加槽数。旧接管原结果不再保留时返回 `UnknownOrRetired`，旧 epoch 不能重新执行。没有清空墓碑或账本的操作步骤。

支持包只收集当前阶段诊断、原操作的有限响应、拓扑/有效配置摘要、版本、指标和相关事件时间线。Topic 分页每页最多 64 条、32 KiB；不要导出完整 MetadataState 快照作为通用诊断。管理权限、采集超时和结果字节上限由运行时保护。

## 可复现实验

在仓库目录运行：

```bash
rtk cargo fmt --all
rtk cargo clippy -p tidemq --tests --no-deps -j 2
rtk cargo test -p tidemq --test metadata_state
rtk cargo test -p tidemq --lib metadata::tests
rtk cargo test -p tidemq --lib ownership::tests
rtk cargo test -p tidemq --lib ownership::lease::tests
```

这些命令验证确定性模型、退休身份、容量、快照和本地单调期限。真实 RF1 授权测试另见 `tests/ownership_authority.rs`；其中数据状态是授权接缝 fixture，不能证明生产者和订阅业务端到端恢复、真实 QUIC、RF3 故障或三区故障通过。

本批已在 macOS arm64、Rust 1.96.0 运行 `cargo test -p tidemq --locked -j 2`：64 项通过，包含真实 RF1 的授权/fence/恢复/激活、快照持久化后 purge/reopen、实际 RF1 拒绝冒充 RF3、过期能力不能消费，以及 CRUD 满表后连续 20 轮分区接管的回归。完整基线结果由集成分支继续验证。
