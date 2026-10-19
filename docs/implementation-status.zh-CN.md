# 实施状态

2026-10-19：完整计划已获用户批准，当前在固定集成分支 `feat/yyx/integration-main` 汇合多个 `wt` worktree。Rust SDK 位于本仓库。所有本地提交均保留在开发分支，尚未推送或合并远端 main。

| 任务 | 已实现与已有行为证据 | 剩余验收边界 |
| --- | --- | --- |
| T00 基础与内存 | 四个 package、schema/codec/ID/边界、Snafu 错误、有限日志/指标、配置、应用许可与控制预留；System/mimalloc/jemalloc 实际二进制可识别；同 jemalloc 发布构建可符号化采样 | 生产 allocator 需匹配环境负载选择；Linux/OOM 与长稳待验 |
| T01 QUIC | 实际 s2n-quic 1.91、TLS/mTLS、有限分类连接池/流/队列、独立读写、单请求取消、原生新包 ACK 活性探测、端点关闭释放 | 真实 LB/NAT/CID、同预算 connection/stream 性能矩阵待验 |
| T02 持久化 | fsync WAL/vote/log/commit/snapshot、官方 OpenRaft storage-v2 Suite、RF1/RF3 quorum、真实 QUIC 换主/快照/丢回包、SIGKILL 恢复、物理日志压缩与失败关闭 | 真实掉电、跨区故障和长稳待验；同机不代表独立故障域 |
| T03 控制与所有权 | 独立元数据组、CRUD 容量与永久身份、grant/fence/recover/activate、有限 TTL、陈旧路由拒绝、可恢复计划操作 | 分角色运行时接管回归与最终故障组合持续验证 |
| T04 生产 | 同组持久 producer/epoch/sequence/摘要/原结果、显式注册、固定过期、owned 提交；稳定版本 key 路由 helper | 无持久 outbox；有限原结果窗口届满不能自动以新身份重发 |
| T05 订阅 | 同组持久 attempt/token/ACK、连续前沿/ACK 洞、seek/代次、冷读 H/接收屏障与 B 重试、窗口硬上限 | 外部业务副作用仍需幂等；KeySerial 与接管组合回归持续验证 |
| T06 Rust SDK | 原生 QUIC Admin/Producer/Consumer、总 deadline/有限重试、保留 PendingBatch、独立宿主观测、同进程原对象跨 CLI 强杀重启恢复 | 自动 Consumer 所有权恢复与真实多 Gateway 入口故障继续验收；SDK 重启无自动 outbox |
| T07 S3 | 固定 aws-sdk-s3、不可变 pack/校验/索引/Range、合装、multipart 取消对账、有界缓存；真实 RustFS 基础及 AWS SDK 测试 | 生产 S3/网络故障、持续中断容量与掉电待验 |
| T08 生命周期 | 两权威注册/授权/发布/完成/取消、原对象对账、pin/恢复保护、替换验证/换引用、全 voter 持久检查点、floor/本地回收、DELETING 重试 | 新 voter 加入与冷回收的运行时组合、对象永久账本规模待验；账本硬上限 4096 |
| T09 保留与配额 | 同 apply 年龄/ACK/CAS floor、有界时间索引、独立逻辑/物理/共享滞后/RSS 水位、真实磁盘/RSS 采样、cgroup v2 可见祖先 headroom、已有 ACK/control/GC 保留准入 | Linux 实机/OOM、持续归档中断和高压力下隔离待验 |
| T10 运行时 | serve、组合/独立角色、实际内外 QUIC、管理健康/就绪/指标/诊断、TLS/静态授权、正常关闭、配置生成器；真实 RF1、分角色 RF3 SDK 热路径和 RustFS 冷路径 | 分角色 OS 故障、Broker 接管、自动伸缩组合回归持续验证 |
| T11 弹性 | 私有稳定成员证明、learner/joint/stable、真实 Raft 迁移、独立修复预算、真实容量/CPU/网络/连接/ACK 评分、保留原计划的自动 tick | 运行时 owned ticker 与实际多角色 explicit/automatic repair 联调、冷却/长期均衡待验 |
| T12 故障 | 有限参考模型、实际磁盘/QUIC/RF3/SDK/RustFS 回归、真实 CLI SIGKILL 与原 SDK 恢复 | 全量最终回归、RF3 独立 OS 进程故障、LB/三区、掉电待验 |
| T13 性能 | 实际外部 SDK 负载、总量与持久 ACK 正确性校验、有限延迟直方图、三 allocator 与三档观测脚本、PID 资源样本 | 冻结版本的正式本地报告、长稳/生产规模/竞品公平对标与三区资源预算待验 |
| T14 文档 | 各任务使用/设计/排障文档、有限指标、模块看板告警；统一导航与逐条覆盖更新中 | 文档命令/链接/监控配置和最终实现一致性核验 |

## 证据口径

测试与复现入口见[测试方法](testing-benchmarks.zh-CN.md)，逐条对应见[要求覆盖](requirements-coverage.zh-CN.md)。本机为 macOS arm64，Rust/Cargo 1.96.0；真实 QUIC 测试使用本机 UDP/TLS 和真实磁盘。分角色 RF3 的早期回归在一个测试进程启动多个独立 Runtime；CLI RF1 强杀测试另用独立 OS 进程。两者不能混称为 RF3 独立进程故障。

最近一次整体回归暴露了 Broker 接管后旧路由缓存重试导致 ProducerBlocked 的问题；已定位并在集成修复，最终全量结果以复测记录为准。不会将局部通过或编译成功写成阶段整体放行。

RustFS 1.0.3 原生隔离实例已覆盖 PUT/GET/HEAD、206/416、multipart、AWS SDK、原对象恢复、发布/检查点/本地回收和冷读持久 ACK；仅启动脚本基础预检不包含这些后续证据。同机 SIGKILL 不证明宿主掉电安全。分配器实际发布构建证据见[采样记录](evidence/2026-10-19-allocator-release.zh-CN.md)。

阶段 A—D 的本地核心行为已有证据；阶段 E 的运行时自动接管/伸缩与最终组合仍在验收。阶段 F 尚未创建云资源，没有三可用区 SLA、生产 allocator 结论或 Kafka/Pulsar/AutoMQ 性能结论。候选秒级恢复目标需要从故障发生至原 SDK 再次持久成功单独计时，测试总用时不能当作恢复延迟。
