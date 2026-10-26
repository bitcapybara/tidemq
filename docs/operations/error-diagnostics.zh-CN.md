# 对外错误与日志诊断

对外错误应说明失败对象、违反的约束或失败阶段。配置项互不相关时分别校验，避免把 endpoint、deadline、队列、重试等错误合并成一个提示。字段之间的约束应同时给出字段名，例如 `request_lanes must not exceed transport.max_streams`。拓扑错误还应指出相关 node ID 或 group ID。

## 校验入口

| 入口 | 应保留的诊断 |
| --- | --- |
| SDK ClientBuilder、ProducerBuilder、具名 ConsumerBuilder、Admin | 配置项、资源名、准入资源与响应约束 |
| ProtocolLimits | frame、message、batch、payload field 与 traceparent 各自的限制 |
| QUIC / RPC / MemoryBudget | connection、stream、窗口、时限、内存保留与具体设置阶段 |
| TOML 与运行时拓扑 | 角色对应的节、授权文件路径、监听地址、node/group/voter/zone 约束 |
| S3、容量与 tracing 配置 | endpoint、bucket、prefix、region、对象预算、水位与导出预算 |
| 管理 HTTP | 具体请求解析失败原因，返回 JSON 转义后的 400 响应 |

身份生成错误区分熵源、早于 Unix epoch 的时钟和超过 UUID v7 48 位范围的时间。普通身份使用 `Id::new_v7()`；随机测试凭据和端口随机种子继续使用 v4，接管阶段的规范派生 UUID v8 保持确定性。

## 错误链与输出

Snafu 错误通过 `source` 保存底层错误，`display` 只描述本层上下文。不要在 `display` 中写 `...: {source}`，也不要用 Debug 输出替代可读的完整链。调用方需要完整链时，使用 `anyhow::Error::new(err)` 包装后输出 `"{err:#}"`。这样每层原因只出现一次，保留类型化 source，不依赖错误文本匹配。

SDK 保留服务端 `ErrorReply.message` 与结构化 code、outcome、retryable、expected_sequence、expected_registration_epoch、retry_after_ms。服务端类型化业务拒绝的具体原因经过内部转发保留；本地 IO 等内部故障只向客户端提供失败阶段及原操作身份的恢复提示，详细 source 留在服务端关联日志中。

`Rejected` 表示本次业务被明确拒绝；`Unknown` 表示结果需要使用原身份核对。错误文本不能把 Unknown 改成 Rejected，也不能作为自动换身份、跳序号或确认消息的依据。

## 排查步骤

1. 配置错误先查看字段和约束；有嵌套 source 时按完整错误链定位协议、传输或内存配置。
2. 服务端拒绝先查看 message，再读取结构化重试提示；需要恢复时保持原业务身份。
3. 对于未知结果，用 request_id、operation_id、command_kind 和 stage 关联运行时与 wire handler 日志。handler 失败日志输出完整错误链，未知回复日志保留服务端原因和 retryable。
4. 对象生命周期协调器保留独立失败计数，由运行时维护任务持有错误并输出 object_id 和完整 `anyhow` 错误链；日志继续复用生命周期指标中的限速，不因错误数量增加而无限输出。详细依赖仍以权威诊断接口为准。

日志不记录消息 body、access token、私钥或 delivery token。诊断文本与 ID 不作为动态指标标签，避免高基数。

## 验证与性能约定

回归测试覆盖配置字段、嵌套 source、服务端 message 的 SDK 传递、内部错误转发及敏感内部原因的对外隔离。UUID v7 测试覆盖版本、字节表示、毫秒排序、相同毫秒的随机输入、时钟边界及普通/派生操作身份隔离。

本次不运行性能测试，相关负载工具、配置、基线和专项报告已按用户要求删除。时间排序与索引局部性不等于已测得的消息吞吐或延迟改善。
