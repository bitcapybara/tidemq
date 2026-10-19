# 生产路径排障

先根据客户端保存的 Topic/partition、producer/epoch/首序号及 operation/request 身份区分业务操作与网络尝试。不要把连接 ID 当成业务身份，不输出消息正文、key、摘要原文或访问令牌。

## 指标与口径

将 `ProducerMetrics` 克隆一次注册为管理面单一 Registry 的固定 Collector，前缀为 `tidemq_producer`。没有动态 Topic、producer、request、partition 标签；共 9 条 counter 与 2 条 gauge。计数单位分别为请求、批次、消息和逻辑字节，时间使用现有 `tidemq_stage_duration_seconds` 的 Produce/Admission、Commit、Total。

| 指标 | 用途 |
| --- | --- |
| `tidemq_producer_attempts_total` | 生产协议请求次数，重试会增加 |
| `tidemq_producer_committed_attempts_total` | 调用方看到的持久结果（生产批次不含 duplicate；注册/退休控制请求也计入） |
| `tidemq_producer_duplicate_attempts_total` | 调用方看到的原结果命中 |
| `tidemq_producer_rejected_attempts_total` | 准入、身份、容量或确定性业务拒绝 |
| `tidemq_producer_unknown_attempts_total` | 调用方未能看到确定结果，包括交给 Raft 后的调用方取消 |
| `tidemq_producer_failed_attempts_total` | 本地运行时故障 |
| `tidemq_producer_new_batches_total`、`new_messages_total`、`new_bytes_total` | 当前 runtime 观察到的首次提交；重复、follower apply 和重放不增加 |
| `tidemq_producer_pending_operations`、`pending_bytes` | 已进入 FIFO 或等待 Raft 终态、仍由 owned submission 持有的工作量和准入字节 |

Counter 随进程重启归零，Broker 崩溃后尚未观察到的已提交操作可能不计入。unknown 与后台新提交允许同时出现，两个维度不可合并为审计账本。Commit histogram 是 Broker 等待 `client_write` 返回的总阶段，再结合 WAL sync、复制和 apply 证据细分，不直接把阶段重叠值相加。

## 现象与只读验证

| 现象 | 先看证据 | 只读核查与处理 |
| --- | --- | --- |
| duplicate 比例上升 | 尝试率、unknown、网络断开/期限 | 对照原批次身份与 offset；检查 Gateway/QUIC 回包阶段，不能把 duplicate 当数据重复追加 |
| sequence gap | expected sequence、SDK 未决批次 | 核对同顺序域是否跨通道乱序或改变批次；保持原身份补充结果恢复，禁止建无界缺口队列 |
| 内容冲突 | producer/epoch/sequence、消息数量 | 在业务侧比较原始账本；不要将正文写入诊断日志，也不要自动换身份 |
| session/result expired | 注册到期、结果窗口下界、逻辑时间 | 确认是否超出固定 TTL/批次数窗口或发生时钟前跳；旧结果不再能确定时交业务账本处理 |
| pending 长期贴上限且新提交停滞 | 数据组 quorum、Raft commit、WAL sync、队列字节 | 查看对应组多数派和磁盘；停止无效重试风暴。超时仍持有许可是防止 Raft 未决命令无限堆积的设计 |
| 生产拒写但消费/ACK 可推进 | 分区/数据组 hot byte/count、归档状态 | 通过有界诊断查看容量；没有完成安全归档/回收前不手工删除 WAL 或消息 |
| fenced/control 到期 | 当前 owner epoch、fence generation、lease age | 依所有权 runbook 刷新权威路由；不手工修改 fence 或把过期缓存当授权 |

诊断投影为 `ProducerState::diagnostic()` 和有界 session/status 查询。它们不推进 ACK、不改变所有权、不扫描正文；直接读取本地快照只是本地视图，权威状态必须经 Raft read barrier 和有效授权。

建议告警使用持续窗口及最小样本：unknown 请求比例持续升高、pending 长期接近操作数/字节上限且新提交无进展、拒绝率伴随容量水位上升。duplicate 仅作为诊断信号，不能单独触发数据丢失告警。恢复条件分别为 unknown 比例下降、pending 排空、新提交恢复及容量降至低水位；具体阈值由真实负载基线冻结。

## 故障验证边界

单元测试验证原结果、冲突、缺口、过期、退休高水位、容量和快照不变量。真实文件及 OpenRaft 测试验证持久提交与重启/快照；进程内故障网络不等于 QUIC、LB/NAT、三区或生产吞吐验证。运行测试前遵循仓库规定，先 `cargo fmt --all`，再 `cargo clippy -p tidemq --tests --no-deps`，最后按测试过滤器执行。

## 看板与示例告警

随模块提供 `deploy/observability/dashboards/producer.json`（Grafana Classic JSON）和 `deploy/observability/alerts/producer.yml`。数据源为调用方选择的 Prometheus，所有查询采用固定指标名。例子对未知结果比例要求至少 100 次尝试且持续 5 分钟，对拒绝比例要求至少 100 次尝试且持续 10 分钟；无提交进展要求存在 pending、最近 5 分钟至少 10 次请求且持续 5 分钟。阈值需在真实负载后冻结，不能将低样本分位数当成 SLA。Collector 由 T10 管理面注册后才会产生数据。

已校验 JSON/YAML 可解析及 Collector 指标输出；尚未在 Grafana 中导入或用 promtool 检验 PromQL/告警时间线。格式依据 [Grafana JSON 模型文档](https://grafana.com/docs/grafana/latest/visualizations/dashboards/build-dashboards/view-dashboard-json-model/)和 [Prometheus 告警规则文档](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/)。

额度用尽是可以确定拒绝的业务结果；实际 allocator 分配失败发生在已提交 apply 时属于副本故障。数据 Application 的 `try_apply` 将其传播为 fatal 存储错误，不得在不同副本返回不同的已提交业务拒绝。失败前不改变权威追加状态，失败副本停止权威读和快照，恢复时重新从持久快照/已提交日志重放。原生 OOM 仍可能 abort 或被 OS/cgroup kill，不能承诺都有最后日志；调用方按原生产身份恢复未知结果。
