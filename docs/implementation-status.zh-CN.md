# 实施状态

2026-10-19：用户已批准完整计划。协调集成分支为 feat/yyx/integration-main，开发由 wt 创建固定目录分支，公共接口与 Cargo/lockfile 由协调者维护。不推送或合并远端 main。

| 任务 | 当前状态 | 验证范围与未完成边界 |
| --- | --- | --- |
| T00 | 核心实现已验证，分配器诊断可行性持续验证 | 三 package、schema/codec/ID/边界、配置/控制预算、日志/指标和 allocator 快照；不是完整运行时或生产选型 |
| T01 | 独立 worktree 实现中 | 实际 s2n-quic、TLS/mTLS、连接池/流与有界 IO、观测 |
| T02 | 独立 worktree 实现中 | 持久 WAL/vote/log/snapshot 与 OpenRaft；真实 RF3 和网络仍需后续验收 |
| T03—T11 | 等待前置接口 | 不宣布生产/消费、S3、回收或弹性可用 |
| T12 | 第一批模型/RustFS 预检已合入 | 参考模型与真实基础边界测试通过；后续增量覆盖实际服务故障 |
| T13 | 未开始 | 默认分配器/窗口/并发、性能及三区目标均未实测 |
| T14 | 文档随任务更新 | 最终导航/示例/设计/告警与运行验证尚未完成 |

## 已有本地证据

- macOS arm64，Rust/Cargo 1.96.0：集成分支全 workspace Clippy 无告警，默认构建 31 项测试通过（含 SDK rustdoc 与基础边界/二进制测试）。
- 真实 binary 读取 config/local.toml 的 validate-config 返回 valid=true。
- allocator-mimalloc 构建 Clippy 无告警；真实 binary 的 2MiB 探针返回 backend=mimalloc、native_version=30302 和可读统计。jemalloc 统计与 profiling 构建 Clippy 通过；真实 binary 返回原生 5.3.1 统计、生成持有/释放 profile，atos 已解析已知分配函数。探针不证明生产收益、Linux 能力或 mimalloc 栈捕获。
- 发布/取消/快照保护参考模型枚举 18 状态、540 转换；订阅模型枚举 411 状态、972 次旧 token 尝试。这些是有限参考模型，尚不代表业务实现已满足。
- CodeGraph 已初始化。本机无容器运行时，T12 已实际运行官方 RustFS 1.0.3 macOS arm64 隔离实例，验证 strict/新桶继承/已有覆盖、建桶、PUT/GET/HEAD、Range 206/416、删除以及 SIGKILL 后重启读取；aws-sdk-s3/multipart 与掉电持久性尚未验证。

阶段 A 尚未整体放行。网络、持久 quorum、完整故障模型和同生产 allocator 栈采集仍需对应证据；本地多进程不替代三可用区测试。
