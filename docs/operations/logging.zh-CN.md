# 日志管理

服务端入口拥有 subscriber/worker 生命周期，默认结构化 JSON stdout。基础事件 service_status 包含 version、allocator 和 messaging_runtime_available；业务事件随各模块登记，不逐消息成功写 INFO，不记录 body、key、令牌或敏感连接串。

有界输出使用 lossy non-blocking 队列。事件 writer 限制整个事件字节，超大事件整体丢弃而非截断 JSON；字段仍应在构造边界限制，writer 不能消除上游格式化/错误构造开销。队列满计数与 oversized_events、底层 sink_errors 分开；tracing-appender 自带丢弃计数不含文件写错误。

业务 handler 正常返回 ErrorFrame、但持久结果为 Unknown 时，记录 `event_code=wire_request_unknown`、`stage=handler_reply`、静态 `command_kind`、request/operation ID、错误码与 outcome。它复用 handler 失败日志的每秒一次限流；被抑制的日志仍计入既有抑制计数，逻辑 Unknown 指标不采样。成功、明确 Rejected 与无法解码的 reply 不冒充此事件；日志不带正文、key、token 或错误消息中的潜在凭据。可通过 command kind 区分接收屏障、Read 和 ACK，不仅凭聚合 Control/Unknown 推断命令。

默认日志规模按事件数×单事件上限估算队列，再计复制/活跃 formatter/native 余量。容器采集器负责大小轮转、保留和总磁盘预算；当前实现没有内建文件 sink。不得让日志盘占用 WAL/控制预留，也不默认输出双份完整日志。

模块过滤禁用 regex。临时过滤限制 1024 字节与 1—300s，要求活动 Tokio runtime，代次隔离旧恢复计时器；无效表达式不覆盖原过滤。当前 API 在服务端库，管理入口/操作审计由 T10 接入。SDK 不安装该设施。

可选 OTLP 与日志使用同一 subscriber 的独立 per-layer filter。专用 `tidemq_trace` span 不交给 stdout formatter，日志诊断过滤不放大 OTLP target/字段白名单。OTLP 有自己的有限队列、字节/速率/超时和丢弃指标，关闭时不创建导出任务；见[追踪手册](tracing.zh-CN.md)。

ERROR 不保证永不丢。worker 退出 flush 期限受上游实现限制，flush 不是 fsync，强杀可能丢尾日志。观测失败不能阻塞提交/ACK，关键业务状态必须能通过持久权威和独立计数诊断。
