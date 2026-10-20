# 默认 mimalloc 与诊断候选验证

日期：2026-10-20；主机：macOS arm64，Rust 1.96.0。基线为合流提交 `bf706583e51f`，随后修改 Cargo 默认 feature、CI、文档与 tracing 测试等待条件；最终版本以包含本文的提交为准。本轮不修改生产共识、WAL 或消息处理算法。

## 实际变更

- `allocator-mimalloc` 默认开启；`allocator-system`、`allocator-jemalloc` 仍为互斥的显式选项。替代构建必须关闭默认 feature。
- SDK 不安装全局 allocator，仍由宿主拥有。
- CI 增加 System 显式构建，并通过默认命令运行 allocator 探针；默认测试和 allocator 矩阵使用串行测试线程。
- tracing 回归在原八秒等待预算内等到完整 SDK → Gateway → Broker 父子链，保留原断言及 Collector 故障验证。

## 已完成的命令

| 命令或检查 | 终态 |
| --- | --- |
| `cargo fmt --all` | 0 |
| `cargo clippy --workspace --tests --examples --no-deps --locked -j2 -- -D warnings` | 0，修改测试后重新通过 |
| workspace lib/bin 的严格 panic、unwrap、expect、indexing Clippy | 0 |
| `cargo test --workspace --locked -j2 -- --test-threads=1` | 0，249 passed、11 ignored，34 suites、122.78 s |
| System 显式 feature 的 tests Clippy 与 allocator 二进制回归 | 0，2 passed |
| jemalloc 显式 feature 的 tests Clippy 与 allocator 二进制回归 | 0，2 passed |
| 默认 feature 的 allocator 二进制回归 | 0，2 passed |
| 默认真实二进制 8 MiB allocator-probe | 0，before/held/after_free 均报告 mimalloc、native 30302、有效统计对象 |
| 保留默认 mimalloc 并额外启用 allocator-system 的 build | 101，按预期拒绝互斥 feature |

默认探针的 native 30302 对应当前 3.3.2。快照统计不提供调用栈，也不将 reserved/committed 重命名为进程 RSS。

## 原始失败与边界

首次普通并行 workspace 测试在 tracing 回归发现 Gateway span 的 SDK 父 span 尚不可见，旧等待条件仅检查角色与 submission。单项原测试随后通过，表明需要处理独立 exporter 的到达顺序。

修正等待条件后的并行 runtime_queue 复跑仍出现三项失败：RF3 创建阶段 ContentConflict、tracing 等待期限、Broker 故障后的 Consumer 恢复期限。该测试进程安装全局 tracing subscriber，其他并行测试也会向同一有限队列产生 SDK span，且多套有限期限 Raft 子进程竞争本机资源。串行全量随后通过；这不构成上述全部并行失败的因果定位，也不证明生产并发故障恢复、吞吐或尾延迟已经通过。CI 与操作文档采用本次实际通过的串行隔离方式，保留并行压力与生产验收为后续工作。

本轮没有重跑真实 RustFS ignored、重新部署 Linux mimalloc 镜像、三可用区或长期性能测试。此前 Linux Docker 证据仍只属于 System 构建。

## mimalloc-pprof 候选

独立临时项目实际验证 1.1.0，内置 native 3.5.0；发行 SHA 为 `80a513dfa6f84a16a07b8612c4041acb7cee4091`。本机 release 的 8 MiB 持有、跨线程 realloc 至 10 MiB、释放后存活量归零、停止后 arena 归零及已知函数符号化通过；64 KiB 采样预算不足时业务分配成功、采样丢弃可见。无 pprof 编译 feature 时不能启动采样。

它与现有官方依赖均声明同一个 native links 名称，不能靠 optional feature 并存；采用时需统一替换底层 allocator。采样持久预算不包含 dump 的 native 临时内存。macOS 手工校正映射后可符号化，Linux、pprof CLI 直接解析、并发启停及 TideMQ 负载开销尚未验证，因此本轮没有替换为该 fork。API、来源和操作限制见[内存排障](../operations/memory-troubleshooting.zh-CN.md)。
