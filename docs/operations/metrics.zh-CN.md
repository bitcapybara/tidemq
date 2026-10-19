# 基础指标字典

当前 metrics 命令导出新进程的 OpenMetrics text，不是运行中集群 scrape。管理监听与业务埋点随模块接入。所有标签是固定枚举，不接受用户字符串，句柄预注册后以 Arc 取得，不为每次请求克隆标签表。

| 名称 | 类型/单位 | 边界 |
| --- | --- | --- |
| tidemq_operations_total | counter，次 | operation=Produce/Read/Ack/Control/Archive/Recover；outcome=Committed/Rejected/Unknown/Failed；记录逻辑操作结果，传输重试另计 |
| tidemq_stage_duration_seconds | classic histogram，秒 | operation、stage=Admission/Queue/Decode/Append/Sync/Replicate/Commit/Apply/Response/Total；本进程单调计时，只接受非负有限数 |
| tidemq_admitted_bytes | gauge，字节 | 接入 admission permit 的业务缓冲，尚需运行入口更新；不是 allocator/RSS 统计 |

bucket 为 50μs、100μs、500μs、1ms、5ms、10ms、50ms、100ms、500ms、1s、5s、30s 和 +Inf。按完整分布聚合后计算分位；不能平均 P99，不把批次时延冒充单消息时延，也不把各阶段 P99 相加。

业务占用、allocator、RSS 和 cgroup 指标有不同范围，不合计；缺失为 unavailable。请求/trace/topic/group/partition/object 地址或名称放有界日志/诊断，不注册无限 series。重启/leader 变化的计数重置不能作为精确审计。

日志队列丢弃、oversized_events 与 sink_errors 的读取接口已提供；后台采集、指标映射、告警和外部采集示例随运行入口接入，不宣称现有三项已覆盖全部观测要求。
