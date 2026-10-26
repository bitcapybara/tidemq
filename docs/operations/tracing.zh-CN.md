# 可选 OTLP 链路追踪

TideMQ 默认关闭 OTLP。基础指标与日志不依赖 Collector；开启追踪也不改变 WAL quorum、生产回执、持久 ACK、fence 或对象回收条件。Collector 故障丢失的是观测数据，业务不等待导出。

## 启用与默认预算

将以下配置加入服务配置文件，再正常启动 `tidemq serve --config <path>`。入口创建 subscriber、官方 OpenTelemetry SDK/exporter 和一个 Tokio owned 导出任务；SDK 库不创建这些全局设施。

```toml
[tracing]
enabled = true
endpoint = "http://127.0.0.1:4318/v1/traces"
sample_per_mille = 10
spans_per_second = 128
queue_spans = 256
batch_spans = 32
export_interval_ms = 500
export_timeout_ms = 500
```

| 约束 | 实际含义 |
| --- | --- |
| 默认采样 10/1000 | 按 trace ID 选择；每个进程仍执行自己的每秒 span 上限，外部 `sampled` 不授予例外 |
| `spans_per_second` 1—1024 | 固定一秒窗口，竞争本地采样锁也会丢弃；跨节点受本地预算影响，可能出现不完整 trace |
| `queue_spans` 1—1024 | 已结束、待导出和正在导出的 span 共用同一容量；满时 `try_send`/许可失败立即丢弃 |
| 每 span 4096 字节许可 | 独立观测预算：默认 1 MiB，最大 4 MiB；是逻辑 admission charge，不是 allocator/RSS 精确上界 |
| `batch_spans` 1—32，且不超过 queue | 单次官方 OTLP protobuf body 最大 64 KiB，单个 HTTP 响应最多保留 4 KiB；无 Content-Length 也累计检查 |
| `export_interval_ms` 100—5000 | 每次尝试完成后至少等该间隔，再发下一批；单出口、并发 1，恢复不会抢跑排空 |
| `export_timeout_ms` 50—1000 | 覆盖连接、HTTP 收发和整体 export；官方 exporter 与 HTTP client 都关闭重试，失败批次不重试、不落盘 |
| span 内容 | 只接受固定 `tidemq_trace` target、四个 span 名与字段白名单；最多 8 属性，字符串最多 64 字节，events/links 为 0 |
| 退出 | 先关闭业务任务/存储，再在入口同一个原始 5 秒总期限内排空 exporter；到期 abort + join 并返回明确错误 |

该预算与应用消息许可分开，必须计入进程部署余量。最大队列逻辑许可 4 MiB 之外，还需给 SDK/span 元数据、编码时的有限复制、HTTP/TLS/native 和日志缓冲留出空间；实际水位仍以 RSS/cgroup 与容量准入监控为准。不要把 `pending_bytes` 当作进程总内存。开启前按正式性能流程比较 CPU/RSS、导出字节和业务时延。

只接受路径为 `/v1/traces`、主机为字面 IP 的绝对地址：明文 HTTP 必须是 loopback；远端必须 HTTPS，并通过严格的 IP SAN 与编译内置 Mozilla 根证书链离线校验。导出任务不做 DNS 解析或平台证书联网获取，避免这些阻塞工作逃逸原退出期限。禁止 URL 用户名/密码、query、fragment、重定向、系统代理和任意 headers。需要域名、私有 CA、mTLS 或认证 header 时，由受保护的本机 Collector 代理；配置中不得放凭据。

开启时拒绝 `OTEL_EXPORTER_OTLP_*` 及对应 `TRACES_*` 的 endpoint/headers/protocol/compression/timeout 环境覆盖，错误不会打印其值。Resource 从空资源构造，只含固定 service name/version 和每次初始化生成的启动实例 ID，不采集 `OTEL_RESOURCE_ATTRIBUTES`。服务启动日志的 `trace_instance_id` 可对应该 Resource，不能把多个进程的合法独立导出误判成单进程超速。即使上游配置漂移，受限 HTTP 适配器也只发送给已验证地址并重建固定 Content-Type。

## 上下文与观测范围

Rust SDK 开启 `opentelemetry` feature 后，从宿主当前 tracing span 获取 OpenTelemetry 0.33 的上下文。宿主自行安装与 tracing-opentelemetry 0.34 兼容的 subscriber/layer；SDK 不覆盖已有 subscriber、provider、过滤器或 exporter。不开启 feature 时，仍可使用已有 `scope_traceparent` 显式传入经过校验的 W3C 父上下文。显式 scope 优先，不能作为授权或去重身份。

每个请求携带 55 字节 W3C traceparent；不携带 baggage/tracestate。Gateway/Broker/Storage/Metadata 的服务 span 接受远端父关系，逐跳请求使用当前 span 的子关系。生产提交队列只保留有界父上下文，独立等待任务重新建立提交 span；观测 span 到原请求期限结束，owned 业务 future 继续等待权威结果，观测到期不会取消提交。

当前固定 span 名为 `tidemq.sdk.request`、`tidemq.sdk.produce`、`tidemq.server.request` 和 `producer_submission`。服务 span 字段为 node/role/frame kind/traffic class；SDK/提交 span 使用有限分类、epoch/attempt 与有界 operation/request ID。日志格式化层不接收专用 `tidemq_trace` span，关闭 OTLP 时不会为它分配 formatter 状态；临时日志级别不绕过导出白名单。

正文、业务 key、投递 token、凭据、原始错误链、任意 dependency DEBUG、event/link 和 status 描述都不进入内置 OTLP。普通日志和业务指标保留各自口径。ACK/读请求可由宿主当前操作关联；不持久化生产 trace 历史，不自动把较晚的消费 trace 伪装成原生产 span 的子节点。Raft 后台合批/心跳也不能伪造唯一业务父调用。`producer_submission` 覆盖该 owned 提交等待至原期限的观测，超过期限后的结果仍通过业务权威与指标判定。

## 故障定位

服务管理端 `/metrics` 在启用后增加以下无动态 label 的有限指标，HTTP `/diagnostics` 的 runtime sample 带对应 trace snapshot；CLI 对应命令为 `diagnose`。计数随进程重启归零，采样不是审计。

| `tidemq_runtime_` 后缀 | 类型与用途 |
| --- | --- |
| `trace_admitted_total` | 完成 span 入队数 |
| `trace_sampled_out_total` | 本地比例、速率或锁预算丢弃 |
| `trace_budget_dropped_total` | 结束 span 因有限队列/字节许可不足而丢弃 |
| `trace_invalid_dropped_total` | 超限属性或不允许的值被最终校验丢弃 |
| `trace_discarded_spans_total` | 已完成采样 span 在校验、容量、导出失败或退出时被本地丢弃；不计 head sampling，也不能推断后端持久接收 |
| `trace_pending_spans` / `trace_pending_bytes` | 排队和当前导出共同持有的条数/逻辑许可字节 |
| `trace_exports_total` / `trace_export_bytes_total` | 实际单次导出尝试及编码 body 字节；不等于后端持久接收 |
| `trace_export_failures_total` | 失败尝试，包含超时；没有每失败重试循环 |
| `trace_export_timeouts_total` | 整体或 HTTP 超时，是失败的子集 |
| `trace_response_rejected_total` | 已声明或实际读取响应超过 4 KiB |
| `trace_shutdown_timeouts_total` | 原总退出期限耗尽，worker 已取消并 join |

1. 业务指标异常时，先区分准入、WAL/commit、quorum、ACK 或 S3 阶段；trace 不改变权威判断。
2. 缺少 trace 时，先看采样丢弃和队列许可，再看 export failure/timeout/response rejection。失败只更新这些独立计数，不递归打印 exporter 错误，也不无限落盘。
3. Collector 恢复后观察每次尝试后的固定间隔；计数恢复不代表历史缺失 span 已补齐。队列满时历史记录已经丢失。
4. 退出报 trace deadline 错误时，业务清理已经先执行；不要据此重发新业务身份。按 SDK 原 operation/batch 身份查询或重试权威结果。

## 本地 Collector

仓库 [collector.yaml](../../deploy/observability/collector.yaml) 针对官方 core **0.162.0**，HTTP 监听 `127.0.0.1:4318`，自身 Prometheus 指标监听 `127.0.0.1:18888`。HTTP 请求体 64 KiB，read/header/write/idle 各 1 秒；memory limiter 128 MiB、spike 32 MiB，单 batch 无 metadata 分片、32 条触发/128 条上限/500 ms。

默认 `nop` exporter 接收后丢弃：出口队列 0、网络出口 0、重试 0，既不记录 span 内容，也不提供 trace 搜索/持久存储。用 receiver accepted/refused span 指标判断接收情况。接真实后端时显式替换成受信 HTTPS OTLP exporter，同时设置有限发送队列、并发、单请求 timeout、整体重试期限和磁盘预算；不能直接把默认黑洞接收等同生产 trace 存储。

此版本实际暴露 `otelcol_receiver_accepted_spans_total`、`otelcol_receiver_refused_spans_total`、`otelcol_receiver_failed_spans_total`，在首个真实请求前可能没有对应系列。进程 CPU/RSS 使用 `otelcol_process_cpu_seconds_total` 与 `otelcol_process_memory_rss_bytes`；Go heap/分配观察 `otelcol_process_runtime_heap_alloc_bytes`、`otelcol_process_runtime_total_alloc_bytes_total` 与 `otelcol_process_runtime_total_sys_memory_bytes`。不能把尚未出现的系列直接解释为一次失败观测中的零。

```sh
otelcol validate --config deploy/observability/collector.yaml
otelcol --config deploy/observability/collector.yaml
```

容器 profile 的主机网络、内存/CPU、日志轮转与停止期限由同目录 Compose 配置负责。macOS Docker Desktop 需要支持并启用 host networking；配置验证、native Collector 运行与容器运行是不同证据。

## 可复现验证与版本来源

```sh
cargo test -p tidemq --test tracing_export
cargo test -p tidemq --test runtime_queue tracing_tests::sdk_gateway_broker_trace_and_durable_ack_survive_collector_faults -- --exact
TIDEMQ_OTELCOL_BIN=/absolute/path/to/otelcol cargo test -p tidemq --test tracing_export official_collector_accepts_real_otlp_and_releases_ports -- --exact --ignored
```

版本固定为 [opentelemetry 0.33.1](https://crates.io/crates/opentelemetry/0.33.1)、[SDK 0.33.1](https://crates.io/crates/opentelemetry_sdk/0.33.1)、[OTLP 0.33.1](https://crates.io/crates/opentelemetry-otlp/0.33.1)、[tracing bridge 0.34.0](https://crates.io/crates/tracing-opentelemetry/0.34.0)、[opentelemetry-http 0.33.1](https://crates.io/crates/opentelemetry-http/0.33.1)、[reqwest 0.13.5](https://crates.io/crates/reqwest/0.13.5)、[webpki-root-certs 1.0.9](https://crates.io/crates/webpki-root-certs/1.0.9) 和 [async-trait 0.1.89](https://crates.io/crates/async-trait/0.1.89)。先核对 crates.io metadata/upstream，再查 versioned docs.rs；部分 docs.rs 抓取不可用，签名/实现补查对应版本 registry 发布源码。OTLP/SDK/http 的 `internal-logs` 与 bridge metrics/tracing-log 默认 feature 均关闭；`http-proto` 上游附带编译 metrics 类型，但没有初始化 OTLP metrics exporter。HTTP/3 feature 未启用，Quinn 不在实际构建依赖树。测试专用 tokio-rustls 0.26.6 验证自签 IP 证书拒绝与握手停滞期限。

实现依据：[OTLP HTTP API](https://docs.rs/opentelemetry-otlp/0.33.1/opentelemetry_otlp/trait.WithHttpConfig.html)、[SpanProcessor](https://docs.rs/opentelemetry_sdk/0.33.1/opentelemetry_sdk/trace/trait.SpanProcessor.html)、[bridge layer](https://docs.rs/tracing-opentelemetry/0.34.0/tracing_opentelemetry/struct.OpenTelemetryLayer.html)、[Span context bridge](https://docs.rs/tracing-opentelemetry/0.34.0/tracing_opentelemetry/trait.OpenTelemetrySpanExt.html)。薄 processor 仅补充 Tokio owned queue、统一字节许可、单出口速率、自监控和可取消退出；protobuf 编码与协议重试分类仍使用官方 exporter。

Collector 使用[官方 0.162.0 资产](https://github.com/open-telemetry/opentelemetry-collector-releases/releases/tag/v0.162.0)。Darwin arm64 tar SHA256 为 `53de34c02913f54aa240861e0febf67a6839bc8ecf1bfab83a8ae7d355d30674`，与官方 GitHub asset digest 核对；[nop 实现](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.162.0/exporter/nopexporter/nop_exporter.go)、[batch 上限](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.162.0/processor/batchprocessor/config.go) 和 [memory limiter](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.162.0/internal/memorylimiter/config.go) 对应此版本。实际测试证据见[OTLP 验证记录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-otlp.zh-CN.md)。
