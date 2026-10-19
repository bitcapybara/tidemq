# 指标与采集

运行中节点通过 loopback 管理端口的 `/metrics` 返回统一 OpenMetrics text，一次编码只有一个 EOF。命令 `tidemq metrics` 创建新的短生命周期进程，不能用它查询已经运行的服务。SDK 的 collector 由宿主显式注册；库不自行监听端口或安装全局观测设施。

| 指标族 | 口径与用途 |
| --- | --- |
| `tidemq_operations_total{operation,outcome}` | 固定 Produce/Read/Ack/Control/Archive/Recover 与 Committed/Rejected/Unknown/Failed。按记录节点和权威阶段理解；不能把跨角色转发计数相加当作唯一业务消息数 |
| `tidemq_stage_duration_seconds{operation,stage}` | 本进程单调分段直方图；Admission/Queue/Decode/Append/Sync/Replicate/Commit/Apply/Response/Total。未接入或无样本的阶段不能解释为零耗时 |
| `tidemq_producer_*` | attempts、committed/duplicate/rejected/unknown/failed、new_messages/new_bytes、owned pending。新持久消息与重复原结果分别统计；业务吞吐采用 new_messages |
| `tidemq_dispatch_*` | 有限 handler 准入/拒绝/错误/活跃与限速日志抑制，独立于日志是否成功写出 |
| `tidemq_internal_client_*` / `internal_server_*` / `external_server_*` | 连接、active_connections、流、帧/字节、deadline、TLS、丢包和连接/流 credit 阻塞；内部客户端还含 liveness probe/failure。连接总计与当前连接不是同一个值 |
| `tidemq_capacity_used_bytes{kind}` / `blocked{kind}` | LogicalBacklog、PhysicalStorage、SharedReclaimLag、ProcessMemory，四种独立口径，不相加。低水位迟滞与样本年龄共同决定新工作准入 |
| `tidemq_capacity_container_blocked` / `container_observation_available` | 独立 cgroup headroom 与字段可用性；字段可用不表示样本永远新鲜。实际年龄、明确无限/不支持/读取失败见诊断 |
| `tidemq_runtime_ready` / `process_rss_bytes` / `cgroup_current_bytes` / `cgroup_limit_bytes` | 角色就绪与真实 OS/容器用量；不可用字段不导出为 0。RSS 包含原生页与 allocator 驻留，不再重复相加 |
| `tidemq_runtime_log_dropped_events_total` / `log_sink_errors_total` / `log_oversized_events_total` | 队列、writer 与事件大小造成的证据损失；与业务成功独立 |
| `tidemq_s3_*` | 实际 AWS SDK attempt/retry/HTTP error、准入、headers/body/operation、decode queue/decode、cache hit/miss；不以 HTTP 成功代替 manifest 发布 |
| `tidemq_lifecycle_*` | register/upload/publish/cancel/delete、reclaimed bytes、retry/failure、活跃工作；Uploaded、Published 与 Reclaimed 分别观测 |
| `tidemq_ownership_*` / `tidemq_control_validity_remaining_seconds` | Grant/Fence/Recover/Activate 阶段结果与有效期；诊断仍需核对真实数据组 fence 和当前元数据路由 |
| `tidemq_scaling_*` | 原计划 learner/catch-up/joint/stable 等阶段时长与 Unknown；实际新鲜容量、成员和拒绝原因见有限诊断 |

公共阶段 bucket 为 50μs、100μs、500μs、1ms、5ms、10ms、50ms、100ms、500ms、1s、5s、30s 和 +Inf。先合并相同口径的 bucket，再算分位；不要平均 P99、相加阶段 P99，或把服务端批次耗时当作 SDK 单消息持久时延。计数器重启会归零，不作为精确审计。

## 本地 Prometheus 与 Grafana

[采集配置](../../deploy/observability/prometheus.yml)默认抓取 RF1 的 `127.0.0.1:8410`，15 秒一次、3 秒期限、4 MB 响应与 20000 样本硬上限。Prometheus 须与管理端口同机，或使用已有受保护代理。RF3 按[运行指南](../guide/running.zh-CN.md)把 targets 改为十个实际管理地址；目标标签来自固定部署清单，不使用 topic/partition/group/request/object/用户名称生成 series。

```sh
promtool check config deploy/observability/prometheus.yml
promtool test rules deploy/observability/tests/runtime.yml
prometheus --config.file=deploy/observability/prometheus.yml --web.listen-address=127.0.0.1:9090 --storage.tsdb.path=/private/path/prometheus --storage.tsdb.retention.time=24h --storage.tsdb.retention.size=1GB
```

导入 `deploy/observability/dashboards/runtime.json`、producer/object-lifecycle/scaling 看板并选择 Prometheus 数据源。告警模板共有四个文件；阈值是可调示例，不是已承诺 SLA。先核对抓取成功、固定样本上限、权威口径和基线，再接入部署方现有通知渠道。服务恢复后观察对应计数停止增长或水位回落，不能仅手动静音。

2026-10-19 本机使用官方 Prometheus 3.15.0 darwin-arm64 的 promtool：采集配置、四个规则文件共 12 条规则及 readiness/capacity 的触发与恢复测试均通过。发行包 SHA256 为 `920df4d17e78b3b0175af144eb318b0c74d1cf7b1d1251b326966f0e81977260`，与[官方发布页](https://prometheus.io/download/)一致。

随后真实 debug RF1 服务与独立 Prometheus 进程完成 OpenMetrics 1.0 抓取：`up=1`、target lastError 为空，实际 1186 个样本、单一 EOF，查询 producer counter 成功。启动持久化产生 Control/Produce 的 WAL Sync 样本分别为 12/8；这是初始化事件，不能当作消息提交或消费 ACK 延迟。关闭 Prometheus 后服务仍 ready，正常关闭服务后四个私有 TCP/UDP 端口均可重新绑定。该实验未验证负载中的 Collector 中断、Grafana 渲染或三档观测开销。

`promtool check metrics` 的默认 Prometheus 文本 lint 会把 OpenMetrics 的 counter family HELP 与 `_total` 样本名误作缺少 HELP；本次采用实际 HTTP Content-Type 协商与 Prometheus 抓取验证，没有修改合法 OpenMetrics 输出去迎合该文本 lint。

采集上限和 scrape 失败语义参照 [Prometheus 配置](https://prometheus.io/docs/prometheus/3.15/configuration/configuration/)与[作业指标](https://prometheus.io/docs/concepts/jobs_instances/)。Collector 不可达不会改变 quorum、ACK 或 GC 的权威状态；`up=0` 表示抓取失败，需要与服务真实请求结果对照。
