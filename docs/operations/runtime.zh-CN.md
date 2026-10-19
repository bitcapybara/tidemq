# 运行时排障与验证

先检查管理面的 `/healthz` 与 `/readyz`，再对照 `/diagnostics` 的控制、数据 quorum、quota sample age 与实际进程内存。健康表示管理线程可响应；就绪不代表每个主题都有剩余配额，也不替代一次具体操作的 lease/fence 检查。诊断数据注明本进程、本地已应用范围，不将 follower 状态当成线性读。

## 有界观测

默认 JSON 日志经过有限队列和单事件字节上限，记录稳定 event code。采集 `tidemq_runtime_log_dropped_events_total`、sink errors 与 oversized events；日志丢失时指标仍能表明诊断证据不完整。不得在日志中记录令牌、私钥、S3 凭据、签名根、消息 key/body 或投递 token。

一次短诊断可在启动时添加 `--trace-seconds 60`，上限 300 秒。运行时生成带 node/role/cluster、request/operation、class、traceparent 的 span 和完成耗时事件，期限后恢复原日志 filter。trace 不生成消息正文的转储，不把 request 或 topic ID 变成指标标签。详细日志会影响负载，应在相同工作负载下分别比较关闭、基础日志和短时 trace。

`/diagnostics` 的 `allocator.backend` 说明服务二进制选择的分配器，`process.process_memory` 为 Mach 或 Linux /proc 取得的 RSS；两者口径不同。mimalloc 默认 native aggregate stats 不包含分配调用栈。需要调用栈时使用 `allocator-jemalloc,heap-profiling` 构建并从进程启动时启用 profiling。`serve --profile-output <0700目录>` 在真实服务内采集五秒、最多 8 MiB 的样本；同时只能有一个 native sampling 操作。样本包含地址等敏感诊断信息，限时保存。具体符号化与分配器构建参数见分配器操作文档。

## 常见阻塞

| 现象 | 应检查的证据 | 行动 |
| --- | --- | --- |
| 新写 Unavailable，Read/ACK 仍可完成 | 元数据 quorum、lease 时效、joint membership、quota 样本年龄 | 恢复控制或采样链路，按原身份重试 |
| QuotaExceeded | backlog、physical、shared reclaim lag、RSS/cgroup 独立水位、文件系统可用量 | 处理消费滞后或恢复归档/GC，等待低水位 |
| 对象长期不能删除 | Catalog blocker、manifest refs、pin、checkpoint、floor | 按原对象 ID 对账，不手动删桶或 WAL |
| 首次启动没有 Leader | peer 地址、CA/DER pin、role/group、初始 voter 与 zone | 修复明确配置；不要临时降 RF 或关闭 mTLS |
| 重启 AddressInUse | 旧进程、五秒关闭错误、三秒绑定期限 | 等待明确退出并查 native drain，禁止 SO_REUSEPORT 绕过 |
| Producer 结果 Unknown | 原 pending batch 与 producer epoch/sequence/digest | 保留批次，查询或 retry；不要产生新身份 |
| 消费者 D 暂停或 B 重投 | H、received prefix、ACK frontier、delivery attempt | 区分收到与业务确认，先处理缺口 |

客户端透明恢复包括重新连接、路由更新和会话恢复，允许有界暂停。服务恢复时间与复制修复时间是两个指标；本机测试不证明真实三故障域的吞吐、延迟或第二级恢复承诺。事故分析需保留 workload、版本、拓扑、分配器、负载与开关配置。

## 验证命令

```sh
cargo fmt --all
cargo clippy --workspace --tests --no-deps -j2 -- -D warnings
cargo test -p tidemq --test runtime_queue
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs cargo test -p tidemq --test runtime_queue actual_runtime_sdk -- --ignored --nocapture
```

运行时回归使用真实 UDP/TLS、真实 Raft 和磁盘 WAL；对象回放测试使用真实 RustFS 和 AWS SDK。`cli_crash_restart_preserves_original_sdk_producer_batch_and_consumer` 用独立 CLI 子进程崩溃和原 Producer/Consumer 对象验证恢复。需从测试实际通过结果判断当前版本是否满足恢复要求，不以测试文件存在作为通过证据。
