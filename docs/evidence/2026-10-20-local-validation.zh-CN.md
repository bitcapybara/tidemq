# 本地统一验收记录（2026-10-20）

环境为 macOS arm64、Rust/Cargo 1.96.0，集成分支为 `feat/yyx/integration-main`。SDK 位于同一仓库；业务网络使用实际 s2n-quic 1.91.0，测试使用真实磁盘和 loopback UDP。独立 CLI 测试的 zone 只是本机拓扑标签，不代表真实故障域。

## 统一检查

命令均在集成 worktree 通过 RTK 执行。下表省略 RTK 前缀；测试运行时间不包含编译，独立运行的测试有重叠，不相加作为不同用例总数。

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all` | 通过 |
| `cargo clippy --workspace --tests --examples --no-deps --locked -j2 -- -D warnings` | 通过，零警告 |
| workspace lib/bins 的 unwrap、expect、panic、todo、unimplemented、indexing/slicing 严格 clippy | 通过，命令见[测试方法](../testing.zh-CN.md) |
| `cargo test --workspace --locked -j2 --no-fail-fast -- --test-threads=2` | **246 passed，9 ignored，34 suites，76.85 s** |
| 指定真实 RustFS 与官方 Collector，`cargo test --workspace --locked -j2 -- --ignored --test-threads=1` | **9 passed，246 filtered out，34 suites，36.70 s** |
| `cargo test --workspace --doc --locked -j2` | **3 passed，4 suites，0.28 s** |
| `cargo test -p tidemq-sdk --locked --features 'metrics opentelemetry' -j2` | **26 passed，3 suites，0.61 s** |
| protocol `codegen` 构建、fmt、generated diff | 通过，生成文件无差异 |
| mimalloc 的 tests clippy 与 `allocator_diagnostics` | 零警告；**2 passed，2.04 s** |
| jemalloc 的 tests clippy 与 `allocator_diagnostics` | 零警告；**2 passed，2.06 s** |
| `heap-profiling` 的 tests clippy，以及 `heap_profile`/`allocator_diagnostics` | 零警告；**3 passed，2 suites，3.19 s** |
| `cargo test -p tidemq --example perf_load --locked -j2` | **10 passed，0.01 s**；含真实 TCP 管理采样边界 |

九个 ignored 分别为八个真实 RustFS 用例和一个官方 native Collector 用例。RustFS 固定 1.0.3；隔离实例覆盖 S3、发布/取消/检查点、本地回收、对象整理和 RF1/RF3 冷读。Collector 固定 core 0.162.0；验证实际 HTTP/protobuf 接收、accepted 计数、正常退出和端口释放。

普通回归覆盖真实独立进程的 Storage 强杀/多数派丢失与恢复、Broker 自动接管及旧身份 Fenced、计划移交/自动修复，以及稳定 UDP 地址上的 Gateway 强杀、丢包、重排和 MTU 黑洞。原 SDK Producer、PendingBatch、Consumer 的身份、offset 和持久 ACK 继续核对。分别参见 [Broker 故障证据](2026-10-20-broker-failover.zh-CN.md)和 [OTLP 故障证据](2026-10-20-otlp.zh-CN.md)。

## 红绿证据与边界修复

统一回归首轮在 Storage 多数派恢复后遇到 `Unavailable/Unknown`，原 fixture 对一次 `Producer::retry` 直接断言成功。服务日志确认已提交更高 term 和预期复制重连，没有 corruption、apply/storage failure 或内容冲突。最终 fixture 在原十秒总期限内对同一 Pending 做有限重试；确定错误或 `retryable=false` 立即失败，每次 Unknown 后仍核对 operation、epoch、sequence 和 digest，最终严格要求 offset 5 和持久 ACK frontier 6。没有扩大 SDK 配置、换身份或延长总期限。

原失败日志保留在集成 worktree 的私有 `target/unified-before-producer-helper-fix.log`，原 fixture 数据目录也保留；定向两个独立 RF3 场景和最终完整普通回归均已通过。heap profiling 专门检查另修正了退出条件的 clippy 嵌套写法，原期限和行为保持一致，并单独运行该 feature 的实际测试。

正式负载另发现三处问题，失败批次均未当作性能成功数据。第一处是负载工具仍按旧 132096 B 总响应上限读取指标，而最终服务指标正文已达 138163 B、服务正文上限为 262144 B。采样工具对齐服务端已有上限，保留独立 1024 B 头部上限、两秒采样期限和有限总读取，并校验唯一 Content-Length、正文长度及单个 OpenMetrics EOF。真实 TCP 测试验证 138163 B 与 262144 B 成功、262145 B 拒绝；解析测试另覆盖超限头部、截断、重复长度和 chunked 拒绝。

第二处是 Read 最多逐条持久签发 256 条投递，却整批共用起始短期 lease；整批尚未完成就到期时，已签发的前缀也没有返回。独立真实 RF1 回归在旧 `10eea25` 上先取得 512 条持久生产回执，首次 `fill` 于约 2.561 s 返回 `Unavailable/Unknown`，交付与 ACK 均为零。修复保留原 TTL、总请求期限、消息 credit 和每条持久签发，按有限新工作片及 writer、冷页 pin、全部控制 lease 的最早期限返回明确成功的部分前缀；Unknown 和最终代次、冷页校验仍返回错误。具体契约见[运行时设计](../design/runtime.zh-CN.md)。

合流后同一 512×1KiB 默认 credit 回归通过：32 次 Read 完整交付，最终持久 ACK 前沿为 512，其中 16 次在仍有完整 credit 时返回非空部分批次。单次 Read 最长约 296 ms，说明 250 ms 是新签发的准入窗口，不是整个 RPC 的硬时延保证。这些耗时仅是功能回归证据，不作为正式性能或高分位结论。旧失败日志保留在性能 worktree 的私有 `target/runtime-read-batches-10eea25-red.log`，绿色日志保留在集成 worktree 的私有 `target/read-batches-sliced-green-20261020.log`；原现场均未提交。新增三个预算测试与一个冷页最早期限测试覆盖短 TTL、首条冷准备、原 deadline、只收紧不续期及 pin/全部 lease 最早边界。

第三处是长寿命 RPC 的每条 sibling lane 独立按 30 秒空等收帧，闲置 lane 超时会关闭仍有活跃业务的整个连接。冻结 `f4d400a` 的首项负载中，4096 条生产全部持久成功，消费 ACK 仅 2474 条，最后成功回执的连续前沿为 1468，随后因未收齐预期消息失败；该现场与剩余未运行的 16 项均未算作成功。只读检查原 WAL 的完整文件、帧长度、连续序号和已提交命令发现：1468—1485 的 18 条投递已持久签发但没有 ACK，后续 1006 条 ACK 洞占满 1024 条订阅窗口，负载退出早于首批可见性重试期限约 12.3 秒。该辅助检查没有验证每帧 BLAKE3、token MAC 或完整 Rust 状态机回放，也不能判断客户端或服务端哪一端先关闭；正式负载的退出规则和可见性期限均保持原值。

在生产修复前新增两项真实 QUIC 回归，分别隔离客户端、服务端的 120 ms 读阈值：四条 Request lane 中仅一个 affinity 持续工作，两次执行均为 0 passed / 2 failed（约 0.44 / 0.40 秒），错误为活跃 RPC 被关闭。修后同命令 2 passed（0.89 秒），两种方向均持续业务超过阈值、始终一条物理连接，deadline/malformed 均为零。长寿命 reader 现在安全等待首字节，收到首字节后仍有限组帧并扣减 peer 剩余预算；一次性绝对收帧 API、native idle、探活和原关闭期限保留，具体见 [QUIC 设计](../design/quic-transport.zh-CN.md)。

修复的定向门禁另通过 transport 单元 5 项、QUIC 23 项、dispatch 3 项、SDK 含文档 26 项，以及真实 Gateway 强杀、丢包/重排、1199 B MTU 黑洞后恢复到 1200 B 的原身份恢复单例。后者测试共 11.26 秒，恢复段约 7.42 秒且含四秒注入黑洞，不代表恢复 SLO。负向回归覆盖首字节前取消后复用、无效组帧期限、prefix/body 超时与取消后 poison，以及首字节后的原预算扣减；失败原文、WAL 和脱敏前诊断只保存在各 worktree 私有 `target`，不提交业务正文或身份秘密。

正式性能测量后另补纯测试提交 `b55c4ab`，生产代码、依赖和性能参数没有变化。`native_idle_produce_connection_records_lane_read_errors_while_control_progresses` 使用原生 idle 一秒及组帧阈值 120 ms：P 四 lane 在 250 ms 时仍开，约 1.22 秒后按原生 idle 关闭；C 持续完成 52 次 Diagnose，两端 malformed 各增加四，deadline/探活失败/拒绝为零且连接累计仍各为二。全 dispatch target 最终四项通过（1.76 秒），再次合流后普通 246 项和真实后端九项均通过。

该独立实验支持“静止 P 原生关闭也可产生宽口径读错误”这一解释；性能原现场没有逐帧原因记录，不能当作全部错误已直接归因。`active_connections` 随连接状态最后引用释放而下降，缓存/RPC 可保留已原生关闭的状态，因此不能单看该值判断网络存活。排障方法见 [QUIC 手册](../operations/quic-troubleshooting.zh-CN.md)。

## 原生观测部署

使用官方 Collector core 0.162.0、Prometheus 3.15.0、Grafana OSS 13.2.4 与真实 RF1 服务进程：

- 四个看板共 29 面板实际 provisioning 加载，均不可编辑；全部 50 条 PromQL 通过官方 promtool 解析。
- Grafana 数据源实际查询到 `up{job="tidemq"}=1` 和 `up{job="tidemq-collector"}=1`；匿名 Viewer 访问管理员接口返回 403。
- 四个规则文件共 14 条规则配置校验通过；readiness/capacity、trace loss/Collector downtime 两组触发与恢复测试通过。
- 四个自有进程正常退出；五个 TCP 和两个 UDP 监听端口均可重新绑定。
- 官方 Compose 5.6.0 执行配置校验通过；当前主机没有 Docker，未声称容器、cgroup 硬限制或浏览器画面已经验证。

部署参数和资源边界见[观测部署](../../deploy/observability/README.zh-CN.md)。默认 Collector `nop` 仅接收后丢弃；真实 span 接收不等同于后端持久存储。

## 剩余外部验收

真实三区、生产负载均衡/NAT/CID、Linux/cgroup OOM、掉电、长稳、生产规模和竞品公平对标仍未执行。未 Active 的原 grant 目标同时死亡时保守等待目标恢复或受信处理；对象永久账本等固定容量上限见[实施状态](../implementation-status.zh-CN.md)。这些边界不能由本地正确性通过推导为已验收。

性能使用另行冻结的 `1398353` 源码、锁文件、三种分配器 release 二进制和配置身份，17 项有限 RF1 负载的测量 36032 条、预热 2264 条均完整提交并持久 ACK；官方 Collector 实际接收 53 个 span。原始表、资源与分位口径见本地性能报告（相关性能资料已移除）。小消息消费及持久 ACK 约 60 条/秒，不能作为高吞吐、低延迟或生产 allocator 已达标的结论。本记录的测试耗时、恢复单例和成功数量也不替代正式性能分布。
