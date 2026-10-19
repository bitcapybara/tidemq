# 测试方法与现有证据

当前基础测试覆盖身份/排他边界、非法/放大 protobuf、连接类别、控制内存预留与失败归还、配置、超大日志完整丢弃、独立 sink 错误和有限指标。它们不证明实际 QUIC、持久化或 RF3 行为。

## 第一批故障验证

2026-10-19，T12 第一批新增以下可复现实验。抽象模型、真实基础接口和 RustFS 进程证据分别记录；只有后续真实模块联调通过，才可将相应功能记为已实现。

| 证据 | 当前验证 | 不能据此宣称 |
| --- | --- | --- |
| `tests/safety_model.rs`、`tests/models` | 穷举有限状态图：登记、迟到授权、条件发布、完成、取消请求、不可逆终止；有/无相关权威 quorum、提交后丢回包；模型快照序列化/恢复与历史 pin 保护 | 已实现元数据/对象生命周期；真实磁盘或 Raft 快照可恢复 |
| 同一参考模型的订阅部分 | generation/assignment/attempt 均取 0—2；保存旧 token，穷举重投、分配变化、seek、ACK 和丢响应；旧 token 不改变新投递，失 quorum 不出现持久成功 | 已实现订阅调度、token 安全认证或真实持久 ACK |
| `DataBoundaries` 真实 API | 0—4 的四个边界共 625 组输入；排他回收上界 ≤ 发布上界 ≤ 提交上界，逻辑 retained floor 独立；消费 ACK 不隐式推进发布或回收 | 单次 API 校验等于后台回收安全，或 ACK 本身属于这些正文边界 |
| `tests/observability_boundaries.rs` | 跨任务持有/取消许可 32 轮后总容量恢复且数据不能消耗控制预留；有效请求中的非法 trace context 不改变身份/正文；固定种子 `0x5eed`、长度 0—256 的非法帧安全失败 | 进程 RSS 有硬上限、QUIC 真实窗口/队列已经联调，或这是完整协议 fuzz |
| 同一观测测试的真实日志/指标 API | 在隔离测试进程验证临时过滤过期、旧计时器不能覆盖新过滤、SDK 当前配置 API 不接管宿主日志；超大事件整体丢弃、完整 JSON 恢复、有界队列饱和丢弃、关闭 sink 的独立错误计数；非法时延不入样本、重复观测不增加 series | 文件轮转/满盘、collector、远程管理授权、业务阶段埋点或默认观测开销已验证 |
| `tests/allocator_diagnostics.rs` | 从实际服务端 binary 运行有限探针，核对所选 backend、统计可用性和参数预算；测试宿主独立声明 System allocator 并嵌入 SDK 当前配置 API，不与服务 binary 冲突 | Cargo harness 自动使用服务端 allocator；统计探针就是分配栈采样、RSS 是泄漏量，或已有生产 allocator 性能结论 |
| `scripts/rustfs-precheck.sh` | 原生 RustFS 1.0.3 的严格持久设置、桶覆盖、S3 基础操作和进程强杀重启，详见[本地 RustFS](operations/local-rustfs.zh-CN.md) | aws-sdk-s3、checksum trailer、multipart、宿主掉电、RF3 或三区验证已完成 |

模型的 `Unknown` 表示调用者没有可用的持久成功证据，丢响应可以发生在状态已经提交之后。`CancelRequested` 只能禁止后续条件发布；取得数据侧不可逆 `Aborted` 证明后才允许结束取消。旧登记证明在终止前可以迟到，在终止后不能复活授权。模型保存的快照 pin 独立于当前任务的终止状态，必须显式释放后才可 GC。

模型只包含单对象操作和单 offset 投递；它不模拟任意并发度、网络时钟、成员变更或持久 IO。穷举使用去重状态集合，不依赖随机调度和墙钟；每个终止状态均参与后续迟到命令尝试。日志过滤的真实计时实验则使用 Tokio 定时器，与模型证据分开。

```sh
cargo test -p tidemq --locked --test safety_model -- --nocapture
cargo test -p tidemq --locked --test observability_boundaries
cargo test -p tidemq --locked --test allocator_diagnostics
scripts/rustfs-precheck.sh
```

`--nocapture` 输出模型状态/转换数；当前发布模型 18 状态/540 转换，订阅模型 411 状态/972 次旧 token 尝试。这些数量不是生产规模上限。首次运行 RustFS 脚本下载固定发行包，下载缓存和临时数据均在当前 checkout 的 `target` 下；脚本只控制自己启动的 PID。测试命令不连接用户已有对象服务。

## Rust 与分配器构建验证

```sh
cargo fmt --all
cargo clippy --workspace --tests --no-deps
cargo test --workspace --locked
cargo test --workspace --doc --locked
cargo clippy -p tidemq --tests --no-deps --no-default-features --features allocator-mimalloc
cargo clippy -p tidemq --tests --no-deps --no-default-features --features allocator-jemalloc
```

不运行 cargo check；CI 按 Linux/macOS 和互斥 allocator 分开验证，不使用全 feature 同开。schema 生成另开 codegen 并检查已提交文件差异。

allocator-probe 是有限内存持有样本，必须从真实二进制执行；记录 backend/native 版本、编译 feature、块大小及前/中/后快照，不能用其证明碎片、泄漏或吞吐优劣。profile 和 native 工具尚待验证。

对每个替代 allocator，先运行对应互斥 feature 的 clippy，再对同 feature 执行 binary 测试；测试程序的 System allocator 仅用于宿主所有权验证。三个构建分别记录结果，不使用 `--all-features`。

```sh
cargo test -p tidemq --locked --no-default-features --features allocator-mimalloc --test allocator_diagnostics
cargo test -p tidemq --locked --no-default-features --features allocator-jemalloc --test allocator_diagnostics
```

当前尚无 collector/堆 profile 接口，不能通过编译 profiling feature 或成功读取汇总统计宣布采样可用。后续需验证同生产 allocator 的已知分配栈、持续持有/释放、跨线程释放、符号、诊断预算及 OOM 子进程后的真实持久恢复。

## 后续放行条件

后续正确性先于正式性能：模型→真实磁盘→RustFS→RF3/QUIC/SDK→故障与弹性→三区。报告包含版本/配置/故障种子、生产与持久 ACK 口径、默认观测开销、完整延迟分布与超时/拒绝、积压斜率、CPU/RSS/cgroup/IO/网络和总成本。

当前无 TideMQ 性能或恢复 SLA。未创建云资源，也未复现 Kafka/Pulsar/AutoMQ 对标。
