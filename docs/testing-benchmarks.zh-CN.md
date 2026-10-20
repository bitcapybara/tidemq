# 测试方法与现有证据

验收先检查持久正确性，再做性能。每类测试说明实际边界；参考模型、真实 QUIC/磁盘、RustFS 和独立 CLI 进程分别报告。本地通过项和外部待验条件见[实施状态](implementation-status.zh-CN.md)，不能由测试文件存在推断已经通过。

2026-10-20 合流后的实际命令、246 项普通测试、九项真实后端 ignored、默认 credit 下 512 条 Read 与空闲 sibling lane 的红绿回归、SDK/allocator feature 和原生观测结果见[本地统一验收记录](evidence/2026-10-20-local-validation.zh-CN.md)。冻结 `1398353` 的 17 项短 RF1 负载、全部持久 ACK、三分配器/日志档位/2 与 4 lane/OTLP 对照见[本地性能报告](evidence/2026-10-20-local-performance.zh-CN.md)，小消息消费约 60 条/秒，不能作为生产性能目标已达成的结论。

同日新增 `ssh dev` 的 [Linux Docker 实测](evidence/2026-10-20-linux-docker.zh-CN.md)：同版 `a3f8f4e` 的 RF1 三项、RF3 七项全部通过，十个独立 case 合计 736 条完整唯一交付和持久 ACK。Linux 源码、镜像与门禁日志独立记录；不把旧 macOS、旧 Linux 和新版重跑计数相加。只读源码、RustFS fixture 的可写 `target` 和 Rustdoc 可执行临时目录要求见 [Docker 使用文档](../deploy/docker/README.zh-CN.md)。

新版 Linux 普通测试 246 通过、11 ignored；十个真实 RustFS ignored 显式通过，余下官方 Collector ignored 未在 Linux 执行。workspace 文档三项、验证 driver 六项，以及 SDK metrics＋OpenTelemetry 的 25 项普通和一项文档测试通过，计数有重叠。格式、Clippy 与生产 panic/索引审计均通过。完整回归使用 `--test-threads=1` 隔离 fixture，测试内部真实 Raft/QUIC/SDK 并发保持原样；macOS 原 drain 瞬时拒绝仍未定位。

## 可复现验证

```sh
cargo fmt --all
cargo clippy --workspace --tests --examples --no-deps --locked -j2 -- -D warnings
cargo test --workspace --locked -j2
cargo test --workspace --doc --locked
cargo test -p tidemq-sdk --locked --features metrics
cargo test -p tidemq-sdk --locked --features 'metrics opentelemetry'
```

不运行 cargo check。生产代码的额外 panic 审计与允许 unwrap 的测试分开：

```sh
cargo clippy --workspace --lib --bins --no-deps --locked -j2 -- -D warnings -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::unimplemented -D clippy::todo -D clippy::indexing_slicing
```

默认测试不依赖用户既有服务。真实 S3 用新建隔离 RustFS：先运行 `scripts/rustfs-precheck.sh` 校验并缓存固定官方发行包，再把绝对路径传给测试。测试只管理自己创建的进程、桶和目录。

```sh
scripts/rustfs-precheck.sh
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs cargo test -p tidemq --test s3_rustfs -- --ignored --nocapture
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs cargo test -p tidemq --test lifecycle_compaction -- --ignored --nocapture
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs cargo test -p tidemq --test lifecycle_persistence -- --ignored --nocapture
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs cargo test -p tidemq --test runtime_queue actual_runtime_sdk -- --ignored --nocapture
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs cargo test -p tidemq --lib runtime::data::reclaim_tests -- --include-ignored --test-threads=1
TIDEMQ_OTELCOL_BIN=/absolute/path/to/verified/otelcol cargo test -p tidemq --test tracing_export official_collector_accepts_real_otlp_and_releases_ports -- --exact --ignored
```

## 证据索引

| 代号与入口 | 实际验证内容 | 范围限制 |
| --- | --- | --- |
| S01 [真实 QUIC](../tests/quic_transport.rs)、[分派](../tests/server_dispatch.rs)、SDK 的网络测试 | TLS/mTLS 与伪造身份拒绝、有限分类连接/流、取消隔离、control/ACK 推进、RPC 结果匹配；四 lane 单 affinity 持续请求不因 sibling 空闲断连，半帧仍 poison；原期限内关闭 owned native I/O 并重绑端口；新 1-RTT 包 ACK 探测能淘汰失联旧连接，慢业务有网络进展时不误杀 | 本机 UDP；真实 LB/NAT/CID 和同预算性能对照另验 |
| S02 [WAL](../tests/wal_journal.rs)、[存储](../tests/replication_storage.rs)、[Raft QUIC](../tests/raft_quic.rs)、[崩溃](../tests/replication_crash.rs)、[应用失败](../tests/application_failure.rs) | fsync/vote/log/commit/snapshot、官方 storage-v2、RF1/RF3 多数派、换主、B 快照与 R 日志、原结果、SIGKILL、日志 rewrite、状态损坏/应用失败关闭 | 一部分底层测试使用受控内存网络；raft_quic 才是原生网络，不混称三区 |
| S03 [元数据](../tests/metadata_state.rs)、[权威](../tests/ownership_authority.rs) | CRUD 与永久身份、grant/fence/recover/activate、重试/CAS/旧 epoch、TTL 从请求开始、控制失 quorum、有限安全收尾 | 纯状态证据与 S08 的实际角色接管分别记录，前者不能代表入口故障 |
| S04 [Broker 测试](../src/broker/tests/mod.rs)、SDK 网络测试 | producer 注册/固定过期/全局高水位、canonical digest、sequence/冲突/原结果、owned 取消后提交、快照恢复、原 PendingBatch 重试 | 有限原结果窗口；SDK 进程重启没有持久 outbox |
| S05 [订阅状态](../tests/subscription_state.rs)、[持久组合](../tests/lifecycle_persistence.rs)、SDK 网络测试 | 逐条 attempt/token/ACK、洞/连续前沿、旧代次拒绝、seek、KeySerial、冷 H/接收屏障和 B 重试 | 外部副作用需要业务幂等；自动接管仍以运行时回归为准 |
| S06 [格式](../tests/archive_format.rs)、[S3 边界](../tests/s3_boundaries.rs)、[RustFS](../tests/s3_rustfs.rs) | 不可变 pack、合装、checksum/索引/Range、坏响应、挂起 Range 不占全部控制槽、multipart cancel/abort/reconcile、实际 aws-sdk-s3 与 RustFS | 受控 HTTP 边界测试和真实 RustFS 分开；不代表公网 S3 中断或掉电 |
| S07 [生命周期持久组合](../tests/lifecycle_persistence.rs)、[实现测试](../src/lifecycle/mod.rs) | 原对象授权/上传/发布/完成对账、不可逆取消、防迟到复活、替换验证/原子换引用、pin/恢复保护、全 voter 快照证明、本地回收/floor/DELETING | RustFS 场景需显式运行 ignored；新 voter 与运行时自动任务组合另验 |
| S08 [运行时](../tests/runtime_queue.rs)、[Broker 故障](../tests/runtime_support/broker_failover.rs) | 实际 SDK 的生产/接收/持久 ACK；独立 RF3 CLI 强杀 Storage/失多数派恢复；九进程 Broker 自动接管及旧 mTLS 身份被 Fenced；稳定 UDP 入口 Gateway 强杀/丢包/重排/MTU 黑洞；原 Client/Producer/Consumer 恢复；RF1/RF3 全部热正文回收后 33 条冷读 | 同进程 Runtime 与独立 OS 子进程分别识别；真实 RustFS 需显式 ignored。单机不是三区，恢复单例不是 P99 |
| S09 [成员迁移](../tests/scaling_raft.rs)、[远程闭合迁移](../tests/scaling_remote.rs) | learner/joint/stable、隔离后保留原计划、joint 中换主、完成证明私有、容量新鲜度与预算、真实 native RF3 自动替换、缺样不创建计划 | 部分远程 fixture 为语义 codec loopback；S08 另验证实际 CLI/mTLS 修复，跨区与高压力组合仍待验 |
| S10 [观测边界](../tests/observability_boundaries.rs)、[分配器](../tests/allocator_diagnostics.rs)、[堆采样](../tests/heap_profile.rs)、quota/retention 单元测试 | 字节许可、控制保留、有限指标、日志丢弃/失败/脱敏、临时过滤、真实 RSS、cgroup 解析、迟滞、同 allocator profile；原生 Prometheus/Grafana 实际 RF1 查询、29 面板和匿名只读权限，见[部署证据](../deploy/observability/README.zh-CN.md)；Linux 容器限额与 RF1 受控真 OOM 见 S12 | Linux 同 allocator 分配栈、长期趋势与浏览器画面待验；Collector 与正式开销单独记录 |
| S11 [追踪导出](../tests/tracing_export.rs)、[四角色链路](../tests/runtime_support/tracing.rs) | 官方 OTLP HTTP/protobuf、TLS 拒绝、宿主设施共存、外部 sampled 仍受本地预算限制、慢/503/超大响应/关闭与恢复；真实 SDK→Gateway→Broker 父关系、owned 提交及持久 ACK、原期限清理；Linux 普通回归覆盖内置采集器边界；官方 native Collector 在 macOS 实际 accepted 与退出端口释放 | native Collector 用例需显式 ignored，本轮 Linux 未执行；默认 nop 不持久化 trace。域名/私有 CA/mTLS 由本地 Collector 代理；公网与正式 Linux Collector 部署待验，Docker 集群默认未开 OTLP |
| S12 [Docker 脚本](../scripts/docker-validate.sh)、[真实 SDK driver](../examples/deploy_validation.rs)、[读取回收回归](../src/runtime/data_reclaim_tests.rs)、[Linux 记录](evidence/2026-10-20-linux-docker.zh-CN.md) | 冻结同版 RF1 三项、RF3 七项；真 OOM、实际 Storage leader/Broker owner 强杀、两类多数派中断及 Gateway 冷重启；原 Client/Producer/Consumer/Pending、64/80 持久前沿；真实 S3 回收竞态的首条/前缀、逻辑过期及 Unknown 四边界 | 单机共享项目内 netns，无宿主端口；736 条为正确性短例，非性能或恢复分位数。Gateway 只冷重启，真实 LB/三区/长稳待验；macOS drain 瞬时拒绝另保留未定位记录 |
| M01 [有限参考模型](../tests/safety_model.rs) | 发布 18 状态/540 转换，订阅 411 状态/972 次旧 token 尝试；枚举 Unknown、迟到授权、取消、pin、丢回包 | 有限状态图，不模拟任意并发/时钟/磁盘，也不代替 S03/S05/S07 |
| P01 [性能工具与报告口径](performance.zh-CN.md)、[17 项报告](evidence/2026-10-20-local-performance.zh-CN.md) | 外部 SDK、独立 RF1 服务进程、warmup/measure/cooldown、原 PendingBatch、unique ID/offset 与最终持久 ACK；36032 条测量/2264 条预热；三 allocator、日志三档、同预算 2/4 lanes 及官方 Collector 53 span | 单消费者的有限闭环短跑；高吞吐低延迟、生产 P99.9、长稳及公平竞品未验。原 16 项缺独立子进程 exit-status 制品；样本不足高分位为 null |

## 构建、生成与 allocator

```sh
cargo build -p tidemq-protocol --features codegen --locked
cargo fmt --all
git diff --exit-code -- crates/tidemq-protocol/src/generated
cargo clippy -p tidemq --tests --no-deps --locked --no-default-features --features allocator-mimalloc -- -D warnings
cargo test -p tidemq --locked --no-default-features --features allocator-mimalloc --test allocator_diagnostics
cargo clippy -p tidemq --tests --no-deps --locked --no-default-features --features allocator-jemalloc -- -D warnings
cargo test -p tidemq --locked --no-default-features --features allocator-jemalloc --test allocator_diagnostics
```

CI 配置 Linux/macOS 和互斥 allocator，不能使用互斥 feature 的 all-features。CI 文件存在不表示远端已运行。同 allocator 的 release 栈、持有/释放、跨线程释放和符号化已在本机验证，见[原始采样记录](evidence/2026-10-19-allocator-release.zh-CN.md)；mimalloc 汇总统计不提供同等分配栈证据。

## 性能与外部前置

当前未创建云资源。真实三可用区要提供节点/区/磁盘/网络/稳定 UDP 入口与预算，再执行阶段 F；本机 zone 名称不制造故障域。报告须包含总 deadline、超时/拒绝/Unknown、生产持久 ACK 与消费持久 ACK 两套分布、积压斜率、CPU/RSS/cgroup/磁盘/网络、故障至 SDK 持久成功的时间线及全部计算/WAL/S3 成本。

Kafka/Pulsar/AutoMQ 只有在 RF、持久 ACK、订阅、批量/压缩、分区与顺序一致时才可对照。历史官方参考不算 TideMQ 实测；测试总耗时不当作恢复延迟，均值不替代 P99，阶段 P99 也不能相加。
