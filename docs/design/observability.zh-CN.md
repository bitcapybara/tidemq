# 观测与诊断契约

日志使用 tracing/subscriber/appender，指标使用 prometheus-client，OTLP 为可选导出。最终应用入口拥有全局 subscriber，SDK 仅发事件并接入宿主设施。基础指标标签使用固定 operation/stage/outcome 枚举，无 request/tenant/topic/partition/object ID。

逻辑 operation_id 与每次 request_id/attempt 分开；跨 Gateway、异步任务和重试传播有界请求级上下文。合批用有限 links/摘要，不为整个长连接建立无限 trace。head sampling 未采集的历史不能事后补齐；错误摘要与计数独立保留且限流。

阶段用本进程单调时钟计时，SDK 端到端包含排队/linger/退避/重试。WAL append、同步、复制、commit、apply 与回包分开；消费 handler 与持久 ACK 分开。并行阶段不能简单相加，各节点 P99 不能平均。

日志有独立有界事件队列、单事件上限和 sink 错误计数。超大事件整体丢弃，避免截断后形成非法 JSON；丢弃、采集失败与恢复均须可诊断。临时过滤需有 Tokio runtime、有效期和代次，到期恢复，旧计时器不得覆盖新设置。

每模块交付事件/error/outcome、指标边界/bucket/标签预算、诊断查询和关联 runbook。抓取仅读有限快照，不扫描全量持久数据。日志/Collector 失败不得进入 quorum/ACK/GC 的成功判断或反压业务；诊断和 native 操作开销另行测试。

当前基础层提供日志管理、有限指标 schema 和统计快照。服务端管理监听、业务埋点、看板/告警及 Collector 部署仍随 T01—T14 交付，不能将 fresh-process metrics 命令视为运行中集群监控。
