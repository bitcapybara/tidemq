# 单节点压测和 CPU 火焰图

本工具通过真实 Rust SDK 访问独立 TideMQ 进程。适用于 RF1 combined 的可复现基线、长轮询前后对比和 CPU 热点诊断；不修改运行时或 CI。不要使用 `cargo test` 耗时代表吞吐。

## 构建并保留二进制

Linux 需要项目指定 Rust、C/C++ 构建工具、OpenSSL、Git、jq、curl、util-linux、Linux perf 和 Perl。先在独立的源码目录准备基线版本，不能在构建或测量过程中修改源码。使用同一组编译选项构建待比较版本，分开放置二进制，保留基线。

```sh
env CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=/private/path/target-baseline-raw bash scripts/perf/build.sh /private/path/baseline-bin
```

源码由 `git archive` 复制、没有 `.git` 时，必须通过 `TIDEMQ_PERF_REVISION` 提供完整源版本。`build.sh` 保留 release 默认 mimalloc、行号信息，并对 Rust/C 开启 frame pointer；构建日志、编译器、Cargo 版本、参数、二进制 SHA256 和工具源码 SHA256 都保存在输出目录。

每个完整源码版本及原始/观测副本分别使用独立 `CARGO_TARGET_DIR`，并记录目标目录与源码身份。不要在 A/B 或原始/插桩副本之间直接复用同一个编译目标目录：归档保留的文件时间可能早于缓存产物，导致旧工作区 crate 被判为可复用。registry 和 Git 下载缓存可以共享。若复制已有编译缓存，只能在本轮专用副本内先执行 `cargo clean --workspace --locked --offline`，确认工作区 crate 产物已清除后再构建，并保留清理日志；不要清理公共 CI 缓存。示例中的目标目录也应为每个版本和副本换成新的唯一目录。

## 无采样基线

以下目录必须不存在，父目录必须已存在。脚本先以非阻塞方式获取 `/var/lock/tidemq-lab.lock`，锁忙时返回 75，且不启动服务。租约一直持有到服务、采样器和私有数据清理完成。脚本不操作公共服务、runner 或现有 fixture。

```sh
bash scripts/perf/run.sh /private/path/baseline-bin/tidemq /private/path/baseline-bin/single_node_bench /private/path/run-traffic traffic
bash scripts/perf/run.sh /private/path/baseline-bin/tidemq /private/path/baseline-bin/single_node_bench /private/path/run-idle idle
```

每次新建一套 `deploy/local/generate.sh rf1` 私有 TLS/令牌和数据目录，独立服务监听默认 17410/17411/18410，先做 32 条消息预热。负载程序准备好 topic、原始 producer 和 Queue consumer 后，与脚本通过文件握手；服务 CPU 和指标快照排除初始化、预热和消费者关闭操作。

默认服务绑核 2–5，负载程序绑核 6–7。必须结合 `cpu-topology.txt` 确认两组没有共享物理核，并确认没有其他作业争用同一物理宿主机。租约避免合作的测试相互干扰，不能排除公共服务、虚拟机或外部进程的干扰。保留主机负载快照，不把单次短运行当成容量上限。

可通过下列环境变量固定工作负载；有效范围由脚本和 CLI 双重检查。

| 变量 | 默认值 | 限制 |
| --- | --- | --- |
| `TIDEMQ_PERF_MESSAGES` | 2048 | 1–200000 |
| `TIDEMQ_PERF_MESSAGE_BYTES` | 1024 | 32–65536，总 payload 不超过 512 MiB |
| `TIDEMQ_PERF_CONCURRENCY` | 4 | 1–32，不超过消息数 |
| `TIDEMQ_PERF_IDLE_SECONDS` | 10 | 1–60 |
| `TIDEMQ_PERF_REQUEST_TIMEOUT_SECONDS` | 10 | 1–120，必须小于总 deadline |
| `TIDEMQ_PERF_TIMEOUT_SECONDS` | 120 | 15–600，必须大于请求预算和 idle 时长 |
| `TIDEMQ_PERF_SERVICE_CPUS` | 2-5 | taskset CPU 列表 |
| `TIDEMQ_PERF_CLIENT_CPUS` | 6-7 | taskset CPU 列表 |
| `TIDEMQ_PERF_PORT` | 17410 | 1024–55500；内部端口 +1，管理端口 +1000 |

每个发送/消费 worker 共享同一原始 SDK handle。payload 包含本轮 ID、业务序号和校验内容；ledger 保存每条消息的 SDK receipt ID、delivery ID、提交/接收/持久 ACK 单调时间。允许接收先于发送 receipt 返回，最终要求同一身份逐条一致、无重复、无遗漏。收齐后额外观察 500 ms 捕获已排队的额外消息；不证明窗口之后永不出现重复。

任何 SDK 错误或 Unknown 都使测量失败，不重新提交新身份，也不执行应用层重试。失败报告、部分 ledger 和原始 Pending 的诊断状态被保留。总 deadline 默认 120 秒，SDK 请求预算默认 10 秒，SDK 关闭另有 5 秒。调大 SDK 请求预算不会延长服务端控制 lease，Unknown 仍使测量失败。输出目录已存在时拒绝复用。

`measurement/report.json` 中吞吐为发送开始到最后一条持久 ACK 的消息数/秒；提交延迟为每次 `send` 调用前到 receipt 返回，端到端延迟为同一次发送开始到接收。p50/p95/p99 使用最近秩，不插值。统计包括 SDK 排队和 RPC，排除本地 payload 构造。idle 模式不计算吞吐和延迟。

`metrics.before.prom` / `metrics.after.prom` 保留全部服务指标，`diagnostics.before.json` / `diagnostics.after.json` 保留 Raft group、last_applied、存储诊断和采样时间。`metrics.delta.tsv` 是数值差（包括 gauge 的变化，不都代表累计量）。诊断由服务周期采样，分析 Raft index 差时要记录时间滞后；基线没有细分暴露的 empty-read/retirement 次数不能由全局 Raft 写入量冒充。

`service-cpu.json` 来自服务 PID 的 `/proc` CPU ticks 差；RSS、线程数和累计 CPU 采样见 `service-samples.txt`，服务生命周期峰值 RSS 见 `service-status.after.txt` 的 `VmHWM`。PID 是服务本身，不是整个进程树或负载程序。CPU 秒与墙钟秒分别报告，多核进程 CPU 秒可以大于墙钟秒。

## 独立采样运行

```sh
env TIDEMQ_PERF_MESSAGES=512 bash scripts/perf/run.sh /private/path/baseline-bin/tidemq /private/path/baseline-bin/single_node_bench /private/path/run-profile profile
```

仅对本次服务 PID 使用 `perf record -e cpu-clock -F 499 --call-graph fp`，最多 15 秒。构建已强制启用 frame pointer。保留 `perf.data`、`perf script`、`perf report` 和折叠栈，使用 [官方 FlameGraph](https://github.com/brendangregg/FlameGraph) 的 Perl 脚本生成 SVG，固定提交 `41fee1f99f9276008b7cd112fca19dc3ea84ac32`。方法依据 [CPU Flame Graphs](https://www.brendangregg.com/FlameGraphs/cpuflamegraphs.html)。

profile 报告保留完整性检查，明确标记采样，并省略吞吐/延迟摘要；不要把采样时间与无采样基线对比。`cpu-clock` 表示 CPU 上的采样分布，不包含阻塞时间。负载提前结束时发送 SIGINT 停止采样。perf 不可用或采不到栈时失败，不生成假火焰图。分析 OpenRaft、WAL、QUIC、序列化的 CPU 栈时使用含该调用链的样本比例；嵌套栈可能重叠，不作为互斥分摊。fsync、队列和提交的等待应另看服务 stage_duration 墙钟直方图；QUIC 共享栈还包含 SDK 外部连接，不能全部算作组件内部通信。

还应核对未展开出函数帧的样本占比：丢样为零不代表调用栈完整。`stackcollapse-perf.pl` 可能按 CPU-clock 的 period 纳秒加权，折叠栈尾部数值不一定是样本个数；样本个数应以 perf record/report 或原始事件数为准。样本少、无函数帧比例高或 SDK 出现 Unknown 时，只保留诊断结论，不作为吞吐门禁或移除 Raft 的定量收益依据。

成功与失败均保留证据。私钥、令牌、服务数据和原始日志使用输出目录之外的私有临时目录；全部日志脱敏成功后才复制到证据目录。脱敏失败仍继续删除私有目录，并使原本成功的运行失败；`cleanup.json` 记录脱敏和实际删除结果。不要把整个远端目录递归复制到公共制品：只取文本/JSON/指标/火焰图，原始 perf 数据仅用于私有诊断。

## 校验

```sh
cargo fmt --all
cargo clippy -p tidemq --tests --example single_node_bench --no-deps
cargo test -p tidemq --example single_node_bench
bash -n scripts/perf/build.sh scripts/perf/run.sh
```

CLI 单元测试覆盖边界、最近秩计算、业务身份/消息身份校验、乱序 receipt、重复和缺失消息。交付前还需用独立服务实跑小 traffic、idle、profile，并验证失败退出、锁忙退出和清理回执。
