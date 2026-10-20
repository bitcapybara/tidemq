# 观测与诊断契约

服务入口拥有全局 tracing subscriber、有限日志队列和统一 prometheus-client Registry。可选 OTLP 默认关闭，由入口拥有官方 OpenTelemetry SDK/exporter 和单个有界 Tokio worker。SDK 的可选桥接读取宿主 trace 上下文，并通过宿主 MeterProvider 记录固定十个无动态标签的指标；不安装自己的全局设施、exporter 或 reader。SDK 同义的 Prometheus/OTel 观测不得重复求和。基础标签仅使用有限 operation/stage/outcome，request/tenant/topic/partition/object ID 留在受控事件或诊断。白名单、采样、字段/字节/退出预算和 Collector 故障行为见[追踪手册](../operations/tracing.zh-CN.md)。

逻辑 operation_id 与每次 request_id/attempt 分开，跨 Gateway、异步任务和重试传递有界请求级 traceparent。trace 不参与去重、fence 或持久状态，也不绑定整个长连接；无效或未采集上下文不改变业务事实。临时详细过滤最多 300 秒后恢复；未采集历史不能事后补齐。错误计数和限速摘要独立于日志输出。

## 时延口径

| 采样 | 实际边界 |
| --- | --- |
| SDK offered-to-durable-ACK | 一个负载进程单调时钟，构建消息前至业务逐消息持久 ACK；包含排队、重试、读与业务等待 |
| SDK produce/ACK batch | 原逻辑批次至持久回执；Unknown 的原身份恢复仍计入同一样本，不是单消息延迟 |
| Runtime Read/Ack/Control Total | 本地 handler 进入到生成结果；取消为 Unknown，不包含后续发送队列与对端业务处理 |
| Broker Admission / Queue | 前台业务许可与 owned submission 队列，各自有限 |
| Broker Commit / Total | client_write 等待复制/提交/应用结果，以及本地整个生产 attempt；不是纯磁盘或纯网络 |
| WAL WalAdmission / WalQueue | journal 字节/记录许可与 writer 排队；和 Broker 的准入/队列分开 |
| WAL Append / Sync | 本地有限批次编码/文件写入与 sync_all；group commit 的一次样本不能当作每消息一次 fsync |
| CommitPersist | 本地持久 committed 标记；和 Broker 等待结果的 Commit 分开，禁止混样本算分位 |
| Replicate / RepairPace | 原生 Raft RPC 含连接、修复 pacing 和对端响应的总等待；RepairPace 是其中受修复预算影响的部分，不与总时长相加 |
| Apply | 本地已提交日志批次的状态机应用，包含实际应用锁/执行；不是客户端全链路 |
| Ownership / Scaling / S3 | 对应模块的实际权威转换、learner/joint/stable 阶段与 SDK headers/body/decode；独立直方图和口径 |

磁盘采样以数据组和元数据组绑定到 Produce/Control；同数据组的 ACK、fence、生命周期记录也会落该 WAL。不要将底层 Produce 标签理解为每条记录都是正文追加。预注册 Decode/Response 暂无完整分段样本，count=0 表示无观测，不表示免费。并行/嵌套阶段不能相加，各节点 P99 不能平均。

日志默认 JSON、有独立事件条数/字节上限和 sink 错误计数，超大事件整体丢弃。请求 span 使用 skip_all，只记录有界身份和固定分类；token、正文/key、私钥和 S3 凭据不进入支持包。轮转、保留和持久日志存储由部署日志接收端负责，详见[日志运维](../operations/logging.zh-CN.md)。

## 运行与限制

管理 HTTP 仅 loopback，四项并发连接和有限请求/响应/期限；抓取读取已注册计数/快照，不扫描正文。诊断包括本地 applied/quorum/成员、WAL、容量样本年龄、真实 RSS/cgroup、allocator、transport 与 placement。指标只描述观察，不授权 ACK、写入、发布或 GC。profile 独占 native worker，并有期限/体积/权限约束。

实际采集配置、看板、告警及 Prometheus 抓取证据见[指标手册](../operations/metrics.zh-CN.md)。日志/Collector 故障不能改变持久事实；队列丢失、sink 错误与采集失败单独可见。本地 OTLP 接收器覆盖断开、慢响应、503、超大响应和恢复，并核对 trace parent 与真实生产/持久 ACK；官方 native Collector 证据与适用范围记录在追踪手册。Linux OOM/长稳、真实 3AZ 与三档正式成本仍需对应实验。[性能报告](../performance.zh-CN.md)必须保留真实 allocator/构建、开关与完整失败样本，不以占位 histogram 或一次 CPU/RSS 差值证明定位精度和开销。
