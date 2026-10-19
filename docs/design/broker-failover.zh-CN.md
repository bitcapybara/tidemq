# Broker 自动故障接管

Metadata 当前 Leader 在进程拥有的 250 ms reconcile 循环内探测配置中的 Broker。探测通过已有 mTLS 内部 `NODE_CONTROL` 的只读 `Probe` 请求，检查回复的 cluster、node、确切角色与停止准入状态；不信任 SDK 报告，也不把缺少容量样本当成节点死亡。外部静态令牌不能发送该内部请求。

每轮最多四个并发探测，共用不超过 250 ms 的期限。失败表只包含配置内有限 Broker 身份，至多 32 项。建立连接阶段只有明确的传输关闭或连接超时计为失联；内存、配置、授权和其他错误不计死亡。已建立连接上的应用请求超时，若原生连接仍有进展，也不计失联。任何实际返回的业务错误清除连续失败证据。只有连续至少三次失联，且从首次到末次跨越至少一秒，才进入 suspected；健康回复、Metadata quorum 读失败或 Leader term 改变清除证据。候选健康证据与失联证据均有三秒新鲜度上限。

每轮读取至多 16 个已配置分区，最多执行一次移交。先取得 Metadata quorum 线性化读，再选择近期通过 Probe、在 Metadata 中仍可调度且具有 Broker 角色的其他节点。已有 `execute_takeover` 在原 route epoch/generation 上执行 CAS，沿用 grant → data quorum fence → recover → activate。锁只在该原有变更边界内取得；后台任务不持 `metadata_mutation` 再调用重入方法。

如果原 grant 已持久化而后续阶段结果未知，下一轮继续原 grant、operation ID 与目标，只补齐未完成步骤。不会每个 tick 创建新 epoch。新目标也必须仍健康；目标在 grant 后死亡时保持阻塞，等待该目标恢复或明确的受信操作处理，不跳过未完成的 fence。Metadata 无多数派无法创建或激活新路由，Data 无多数派无法生成 fence/recovery 证据；RF 始终保持配置值。

退出 watch 的取消覆盖整个 reconcile cycle；所有探测和提交共用该轮原两秒期限。旧 Broker 重启后仍只能按当前 route 转发，Storage 重新读取路由并核对发送者 node 和已持久化 epoch，因此旧证书身份不恢复旧写权限。计划排空仍先持久化 unschedule，再走原来的受信移交流程。

## 指标与排查

下列 Counter 均在同一个进程 Registry 注册，不使用 node、topic、partition、request 或 operation 标签：

| 指标 | 含义 |
| --- | --- |
| `tidemq_failover_probes_total` | 已完成的受信只读探测次数 |
| `tidemq_failover_unreachable_total` | 未取得原生传输进展的探测次数 |
| `tidemq_failover_suspected_total` | 同时达到次数和观察跨度阈值的状态转换次数 |
| `tidemq_failover_takeover_attempts_total` | 对 Active 路由开始 CAS 接管的尝试次数 |
| `tidemq_failover_resume_attempts_total` | 对已持久化 grant 的续作尝试次数 |
| `tidemq_failover_activated_total` | 完成新鲜 quorum 证明与路由激活的次数 |

尝试次数不是新 epoch 数。已有 `tidemq_ownership_transitions_total` 和 ownership 阶段时延区分 Grant、Fence、Recover、Activate 的结果。`broker_owner_unreachable` 只在进入 suspected 时记录一次；健康回复后再次失联才重新记录。`broker_owner_activated` 记录完成的目标 node 与 epoch，重试等待只记录低成本 debug 事件。

先检查 Meta 是否有多数派、数据组是否有多数派，再比较 suspected、takeover、resume、activated 与 ownership 阶段计数。只有 suspected 增长时检查 TLS 身份、UDP 路径和目标进程；takeover 增长但没有激活时检查 Data fence 与 recovery。不要通过降低 RF、跳过 epoch 或重建 Producer/Consumer 规避问题。

## 本地验证边界

`tests/runtime_queue.rs` 装配 `runtime_support/broker_failover.rs`，复用已有证书、CLI 进程与 SDK fixture。测试要求三 Meta、三 Storage、两 Broker 和一个 Gateway 均为独立真实进程；先持久 ACK，再 SIGKILL 当前 Broker。原 Producer 的 Pending 身份和原 Consumer 必须在元数据自动接管后继续成功，旧 Broker 使用原目录重启后其真实旧 mTLS 身份必须被 Storage 判 Fenced。附加分支失去 Metadata 多数派时要求无新成功、无新鲜 route，恢复多数派后继续原 Pending。

故障到原 SDK 持久 Producer 回执及 Consumer ACK 的单次计时仅是本地故障证据，不能当作 P99 或生产恢复 SLO。16 分区分页、32 节点探测轮转、宿主机调度和 quorum 选举均会影响恢复时间；三区、真实负载和 Linux 环境仍需独立验证。
