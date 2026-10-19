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


## 阶段定位与自动放置

`tidemq_operations_total` 与 `tidemq_stage_duration_seconds` 按固定 Read、Ack、Control 操作和结果记录本进程请求完成，Total 使用单调时钟，取消时保留 Unknown。Produce 继续使用 Broker 的提交指标。WAL 的真实 WalAdmission、WalQueue、Append、Sync、CommitPersist 和 Apply 阶段接入同一注册表；其中 CommitPersist 表示提交标记持久化，Broker 的 Admission、Queue 和 Commit 保持各自的请求语义，避免混合分布；`/diagnostics` 的 `metadata_storage` 与各 `data_groups.storage` 提供 committed/applied、apply failure、journal runtime 与恢复状态，帮助区分传输等待、共识等待和磁盘同步。

ownership 的 Grant、Fence、Recover、Activate 在实际受信提交边界记录。`tidemq_control_validity_remaining_seconds` 是有限本地已验证 lease 集合的最小剩余时间；无证据、过期或暂时无法读取时为零。`tidemq_admitted_bytes` 是共用 MemoryBudget 当前已许可占用，不能替代 RSS 或分配器统计。

每个 Metadata 进程有一个 owned 的一秒放置循环，只有当前 leader 执行。它从固定 standby 候选和新鲜真实容量样本生成至多一个迁移计划，恢复未完成计划，限制并行探测、复制预算及再均衡冷却时间。缺少容量样本不会被当作节点死亡；真实不可达必须连续多次并持续达到阈值才触发替换。诊断 `placement` 展示固定阻塞原因、计划和年龄，`placement_capacity_available` 表示当前本地样本是否可用于放置。自动成员修复依赖既有可达多数派，无法绕过 quorum。


## 有限 UDP 故障验证

`stable_udp_endpoint_recovers_original_sdk_after_gateway_crash_with_network_faults` 使用一个稳定 UDP 监听和两个明确后端，原 SDK 不更换地址或业务身份。它对真实报文按固定频率丢包、保留至多两个报文制造短时乱序，并在 Gateway CLI 子进程被 SIGKILL 后制造短期 1199 字节 MTU 黑洞；黑洞期间不能获得成功写回执。恢复到 1200 字节后，原 PendingBatch 按原 epoch/sequence/digest 重试，原 Consumer 继续提交 ACK，且 SDK 实际建立新 QUIC 连接到存活 Gateway。测试只在丢包、乱序和 MTU 丢弃计数均非零时通过。

这是单机有限代理的真实传输回归，不代表生产负载均衡器的 CID 路由、NAT 行为或真实三区网络已经验证。生产多 Gateway 的外部证书须覆盖相同服务 DNS SAN，SDK 使用该服务名；内部 mTLS 仍按独立 node 身份和精确证书 pin 验证。故障恢复期间允许有界暂停，旧 QUIC 连接不承诺保持不变。


后台压缩与新归档共用每数据组的单 owned 维护循环：先对账原对象 ID，再推进 retention、floor 快照和本地 reclaim，通过 BackgroundSnapshot 配额后每轮至多检查八个对象并替换一个候选。完全没有新写时仍会检查部分失效对象。压缩从带短期 pin 的 S3 正文复制所有存活区间，持久化替换任务，经当前全 voter 检查点后，下一轮才可删除无引用旧对象；Pinned 和恢复依赖作为保留条件。`tidemq_lifecycle_compacted_total` 与 `tidemq_lifecycle_deleted_total` 可分别验证替换和真正删除的推进。

`actual_runtime_sdk_automatically_compacts_partial_cold_objects_and_reclaims_sources` 使用真实 RustFS、Raft、SDK 和默认 192/32 MiB 应用/控制预算。它将测试保留时间设为零，验证原正文已本地回收、部分 ACK 推进 floor、自动压缩及旧 S3 对象删除后，同一个 Consumer 仍能读取存活后缀并提交 ACK。生产保留时间仍使用显式配置；测试不会修改默认策略。

后台归档每个数据组每轮至多生成一个 pack，按实际 key 与正文总字节选择连续已提交范围，最多 8 MiB、8192 条消息。选择过程复用 `archive_read` 的有限页（最多 1024 条、1 MiB，并进一步收敛到该 Producer 的配置限制），页面引用先占用共享 MemoryBudget，退出即释放，再由原有 pack 构建许可接管。它不会复制或遍历完整积压，不提高默认 192/32 MiB 预算；单条消息必须满足已有消息及页上限。原 5 秒截止时间和定时周期保持有效，达到条数、字节或时间边界后交给下一轮；分区游标从本轮实际归档位置继续，避免一个分区长期占用全部机会。

冷读在一个受保护页面内批量获取最多 256 条、1 MiB 数据，并为每条已持久化投递绑定原对象的私有证明。开始批量 Read 前会按需刷新临近到期的控制证明，仍遵守原控制 TTL 和请求总截止时间；页面一直保留到帧编码完成，随后再次校验并释放 pin。Read 中途失败或回复丢失时，已持久化投递不代表业务 ACK：后缀完成只会留下 ACK 孔，丢失前缀继续受保护，并在 ACK 超时（默认 30 秒）后重投。客户端必须补齐该前缀后才能推进连续业务 ACK 前沿，旧 attempt token 仍被拒绝。`lost_partial_cold_reply_preserves_the_prefix_until_visibility_retry_and_business_ack` 对 25 条丢失响应、8 条后缀 ACK、持久快照恢复和最终前沿 33 做了回归。

后台维护仍由配置周期触发。如果本轮新增归档遇到过期的容量样本，会在原 5 秒截止时间内等待管理采样器发布下一份真实观测，再重新检查配额，避免两个 1 Hz 定时器长期相撞。通知本身不提供准入证据，也不额外触发一轮维护；样本年龄上限仍为 1000 ms，采样失败不会刷新年龄，压力或未测量等其他拒绝保持原样，停机可立即取消等待。
