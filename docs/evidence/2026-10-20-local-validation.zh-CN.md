# 本地统一验收记录（2026-10-20）

环境为 macOS arm64、Rust/Cargo 1.96.0，集成分支为 `feat/yyx/integration-main`。SDK 位于同一仓库；业务网络使用实际 s2n-quic 1.91.0，测试使用真实磁盘和 loopback UDP。独立 CLI 测试的 zone 只是本机拓扑标签，不代表真实故障域。

## 统一检查

命令均在集成 worktree 通过 RTK 执行。下表省略 RTK 前缀；测试运行时间不包含编译，独立运行的测试有重叠，不相加作为不同用例总数。

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all` | 通过 |
| `cargo clippy --workspace --tests --examples --no-deps --locked -j2 -- -D warnings` | 通过，零警告 |
| workspace lib/bins 的 unwrap、expect、panic、todo、unimplemented、indexing/slicing 严格 clippy | 通过，命令见[测试方法](../testing-benchmarks.zh-CN.md) |
| `cargo test --workspace --locked -j2 --no-fail-fast -- --test-threads=2` | **241 passed，9 ignored，34 suites，76.25 s** |
| 指定真实 RustFS 与官方 Collector，`cargo test --workspace --locked -j2 -- --ignored --test-threads=1` | **9 passed，241 filtered out，34 suites，36.55 s** |
| `cargo test --workspace --doc --locked -j2` | **3 passed，4 suites，0.24 s** |
| `cargo test -p tidemq-sdk --locked --features 'metrics opentelemetry' -j2` | **26 passed，3 suites，0.60 s** |
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

正式负载另发现两处问题，失败批次均未当作性能成功数据。第一处是负载工具仍按旧 132096 B 总响应上限读取指标，而最终服务指标正文已达 138163 B、服务正文上限为 262144 B。采样工具对齐服务端已有上限，保留独立 1024 B 头部上限、两秒采样期限和有限总读取，并校验唯一 Content-Length、正文长度及单个 OpenMetrics EOF。真实 TCP 测试验证 138163 B 与 262144 B 成功、262145 B 拒绝；解析测试另覆盖超限头部、截断、重复长度和 chunked 拒绝。

第二处是 Read 最多逐条持久签发 256 条投递，却整批共用起始短期 lease；整批尚未完成就到期时，已签发的前缀也没有返回。独立真实 RF1 回归在旧 `10eea25` 上先取得 512 条持久生产回执，首次 `fill` 于约 2.561 s 返回 `Unavailable/Unknown`，交付与 ACK 均为零。修复保留原 TTL、总请求期限、消息 credit 和每条持久签发，按有限新工作片及 writer、冷页 pin、全部控制 lease 的最早期限返回明确成功的部分前缀；Unknown 和最终代次、冷页校验仍返回错误。具体契约见[运行时设计](../design/runtime.zh-CN.md)。

合流后同一 512×1KiB 默认 credit 回归通过：32 次 Read 完整交付，最终持久 ACK 前沿为 512，其中 16 次在仍有完整 credit 时返回非空部分批次。单次 Read 最长约 296 ms，说明 250 ms 是新签发的准入窗口，不是整个 RPC 的硬时延保证。这些耗时仅是功能回归证据，不作为正式性能或高分位结论。旧失败日志保留在性能 worktree 的私有 `target/runtime-read-batches-10eea25-red.log`，绿色日志保留在集成 worktree 的私有 `target/read-batches-sliced-green-20261020.log`；原现场均未提交。新增三个预算测试与一个冷页最早期限测试覆盖短 TTL、首条冷准备、原 deadline、只收紧不续期及 pin/全部 lease 最早边界，最终 241 项普通与九项真实后端已在合流后重新执行通过。

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

性能使用另行冻结的源码、锁文件、二进制和配置身份，运行独立矩阵；本记录的测试耗时、恢复单例和成功数量均不作为吞吐、P99 或生产 allocator 结论。
