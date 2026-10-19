# 实施状态

2026-10-19：用户已批准完整计划。协调集成分支为 feat/yyx/integration-main，开发由 wt 创建固定目录分支，公共接口与 Cargo/lockfile 由协调者维护。不推送或合并远端 main。

| 任务 | 当前状态 | 验证范围与未完成边界 |
| --- | --- | --- |
| T00 | 核心实现已验证，分配器诊断可行性持续验证 | 四 package（service/protocol/transport/SDK）、schema/codec/ID/边界、配置/控制预算、日志/指标和 allocator 快照；不是完整运行时或生产选型 |
| T01 | 核心传输已合入并验证 | 实际 s2n-quic、TLS/mTLS、连接池/流、有界 IO、有限观测；业务分派、调度、Core 自动修复及真实 LB 仍待 T06/T10/T12 |
| T02 | 持久化与真实 OpenRaft 已合入并验证 | WAL/vote/log/commit/snapshot、官方 storage-v2 Suite、RF1/RF3 quorum/换主/成员变化、SIGKILL恢复和物理日志压缩；RF3 当前用受控内存网络，真实 QUIC RPC 与三区待验 |
| T03—T05 | 独立 worktree 并行开发 | 控制权威/owner fencing、生产幂等、持久订阅；共享注册/分配/seek/屏障协议和内容摘要已合入 |
| T06—T11 | 等待当前业务接口 | 公共 transport 已独立以供 SDK 复用；不宣布真实业务链路、S3、回收或弹性可用 |
| T12 | 第一批模型/RustFS 预检已合入 | 参考模型与真实基础边界测试通过；后续增量覆盖实际服务故障 |
| T13 | 未开始 | 默认分配器/窗口/并发、性能及三区目标均未实测 |
| T14 | 文档随任务更新 | 最终导航/示例/设计/告警与运行验证尚未完成 |

## 已有本地证据

- macOS arm64，Rust/Cargo 1.96.0：集成分支全 workspace Clippy 无告警，共享传输提取前默认构建 74 项测试通过（含 SDK rustdoc 与基础边界/二进制测试）。
- 真实 binary 读取 config/local.toml 的 validate-config 返回 valid=true。
- allocator-mimalloc 构建 Clippy 无告警；真实 binary 的 2MiB 探针返回 backend=mimalloc、native_version=30302 和可读统计。jemalloc 统计与 profiling 构建 Clippy 通过，release 构建同 allocator 的统计和可符号化栈[实测记录](evidence/2026-10-19-allocator-release.zh-CN.md)已保存；真实 binary 返回原生 5.3.1 统计、生成持有/释放 profile，atos 已解析已知分配函数。探针不证明生产收益、Linux 能力或 mimalloc 栈捕获。
- 发布/取消/快照保护参考模型枚举 18 状态、540 转换；订阅模型枚举 411 状态、972 次旧 token 尝试。这些是有限参考模型，尚不代表业务实现已满足。
- CodeGraph 已初始化。本机无容器运行时，T12 已实际运行官方 RustFS 1.0.3 macOS arm64 隔离实例，验证 strict/新桶继承/已有覆盖、建桶、PUT/GET/HEAD、Range 206/416、删除以及 SIGKILL 后重启读取；aws-sdk-s3/multipart 与掉电持久性尚未验证。

阶段 A 尚未整体放行。QUIC 与持久 quorum 已各自验证，组合运行时、完整业务故障模型、同生产 allocator 选型及生产负载栈采集仍需对应证据；本地多进程不替代三可用区测试。
