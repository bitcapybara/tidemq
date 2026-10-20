# 实施状态

2026-10-20：完整计划已获用户批准，当前在固定集成分支 `feat/yyx/integration-main` 汇合多个 `wt` worktree。Rust SDK 位于本仓库。所有本地提交均保留在开发分支，尚未推送或合并远端 main。

| 任务 | 已实现与已有行为证据 | 剩余验收边界 |
| --- | --- | --- |
| T00 基础与内存 | 四个 package、schema/codec/ID/边界、Snafu 错误、有限日志/指标、配置、应用许可与控制预留；System/mimalloc/jemalloc 实际二进制可识别；同 jemalloc 发布构建可符号化采样；Linux Docker 的 System/cgroup 与 RF1 受控 OOM 恢复已验 | 生产 allocator、Linux 分配栈与长稳待验 |
| T01 QUIC | 实际 s2n-quic 1.91、TLS/mTLS、有限分类连接池/流/队列、独立读写、单请求取消、原生新包 ACK 活性探测；空闲 sibling 不关闭活跃连接，半帧仍有限组装；原期限内关闭 owned I/O 并释放端口；有限 2/4 lane 同预算对照 | 真实 LB/NAT/CID、混合连接/短流/公平性与完整规模矩阵待验 |
| T02 持久化 | fsync WAL/vote/log/commit/snapshot、官方 OpenRaft storage-v2 Suite、RF1/RF3 quorum、真实 QUIC 换主/快照/丢回包、SIGKILL 恢复、物理日志压缩与失败关闭 | 真实掉电、跨区故障和长稳待验；同机不代表独立故障域 |
| T03 控制与所有权 | 独立元数据组、CRUD 容量与永久身份、grant/fence/recover/activate、有限 TTL；真实 Broker 强杀后自动接管、旧身份拒绝及元数据少数派停授权 | 未 Active 的原 grant 目标同时死亡时保守阻塞；三区与长期故障组合待验 |
| T04 生产 | 同组持久 producer/epoch/sequence/摘要/原结果、显式注册、固定过期、owned 提交；稳定版本 key 路由 helper | 无持久 outbox；有限原结果窗口届满不能自动以新身份重发 |
| T05 订阅 | 同组持久 attempt/token/ACK、连续前沿/ACK 洞、seek/代次、冷读 H/接收屏障与 B 重试、窗口硬上限；部分冷页提交后丢响应、尾部先 ACK 和重启补投 | 外部业务副作用仍需幂等；高压力和长稳待验 |
| T06 Rust SDK | 原生 QUIC Admin/Producer/Consumer、总 deadline/有限重试、保留 PendingBatch、独立宿主观测；原对象跨 Broker/Storage/Gateway 强杀恢复及稳定 UDP 地址上的丢包/重排/MTU 故障 | SDK 重启无自动 outbox；真实生产 LB/NAT/CID 和跨区恢复待验 |
| T07 S3 | 固定 aws-sdk-s3、不可变 pack/校验/索引/Range、合装、multipart 取消对账、有界缓存；真实 RustFS 基础及 AWS SDK 测试 | 生产 S3/网络故障、持续中断容量与掉电待验 |
| T08 生命周期 | 两权威注册/授权/发布/完成/取消、原对象对账、pin/恢复保护、自动整理替换验证/换引用、全 voter 持久检查点、floor/本地回收、DELETING 重试；RF1/RF3 真实 RustFS 多消息回收后冷读 | 新 voter 加入与冷回收的压力组合、对象永久账本规模待验；账本硬上限 4096 |
| T09 保留与配额 | 同 apply 年龄/ACK/CAS floor、有界时间索引、独立逻辑/物理/共享滞后/RSS 水位、真实磁盘/RSS 采样、cgroup v2 可见祖先 headroom、已有 ACK/control/GC 保留准入；Linux 384 MiB→6 MiB 真实 OOM、恢复原限额/原 Pending 与持久 ACK 已验 | 默认限额自然压力、持续归档中断和高压力下隔离待验 |
| T10 运行时 | serve、组合/独立角色、实际内外 QUIC、管理健康/就绪/指标/诊断、TLS/静态授权、正常关闭；兼容 LibreSSL 的配置生成器；真实 RF1/RF3 热冷路径、独立 OS 进程故障与原 SDK 恢复；Linux 独立 Docker 项目、UID/卷/cgroup/无宿主端口实际核验 | 真实生产部署、跨区及长期压力待验 |
| T11 弹性 | 私有稳定成员证明、learner/joint/stable、真实 Raft 迁移、独立修复预算、真实容量/CPU/网络/连接/ACK 评分；owned ticker 驱动显式/自动 Storage 修复及 Broker 移交 | 冷却/长期均衡、高压力及跨区容量待验 |
| T12 故障 | 有限参考模型、实际磁盘/QUIC/RF3/SDK/RustFS；独立 RF3 CLI 与 Linux Docker 的 Storage/Broker 强杀、Metadata/Storage 多数派中断、Gateway 冷重启、RF1 真 OOM；新版十个 Docker case 合计 736 条完整唯一交付和持久 ACK；新版 Linux 普通 246、十个真实 RustFS ignored 及三个 docs 通过 | macOS drain 瞬时拒绝未确证；Linux 官方 Collector ignored 未执行；生产 LB/三区、掉电与长稳待验 |
| T13 性能 | 冻结 `1398353` 的 17 项有限 RF1 报告；三 allocator、日志三档、2/4 lane 与 1% 服务 OTLP；测量 36032 条、预热 2264 条完整持久 ACK；PID 和 Collector 独立资源 | 小消息消费约 60 条/秒，高吞吐低延迟目标未放行；完整布局/长稳/诊断成本/生产规模/竞品/三区及默认 allocator 待验 |
| T14 文档 | 各任务使用/设计/排障文档、有限指标、模块看板告警；可选 OTLP 有界导出、实际四角色关联、Collector 多故障隔离；统一导航、76 条覆盖、Linux Docker 构建/隔离/故障脚本与真实命令证据 | Linux 分配栈、生产命令与浏览器画面仍按对应环境验收 |

## 证据口径

测试与复现入口见[测试方法](testing-benchmarks.zh-CN.md)，逐条对应见[要求覆盖](requirements-coverage.zh-CN.md)。先前本地验收为 macOS arm64，Rust/Cargo 1.96.0；真实 QUIC 测试使用本机 UDP/TLS 和真实磁盘。分角色 RF3 的早期回归在一个测试进程启动多个 Runtime；后续 RF1/RF3 强杀与自动接管另以独立 CLI 子进程验证。新增 Linux Docker 证据另行冻结源码、镜像和二进制身份；各类证据分别标注，单机 zone 不代表独立故障域。

回归中发现并修复了旧路由缓存重试、冷页逐条取 pin 耗尽控制期限、整批逐条签发耗尽起始 lease、归档固定条数限制、采样相位导致配额陈旧、空闲 sibling 误关闭活跃 RPC 及失联连接清理耗尽关闭期限。Read 在最早权限期限内分片返回，每条仍须持久签发，Unknown 与最终安全校验不变。Broker 自动接管的真实进程证据见[故障记录](evidence/2026-10-20-broker-failover.zh-CN.md)。合流后普通测试 246 通过、9 ignored；使用真实 RustFS/官方 Collector 单独执行九个 ignored，全部通过。SDK metrics＋OpenTelemetry 组合 26 项及 workspace 三个文档测试也通过；这些是独立且有重叠的检查，不相加作为不同测试总数。

RustFS 1.0.3 原生隔离实例已覆盖 PUT/GET/HEAD、206/416、multipart、AWS SDK、原对象恢复、发布/检查点/本地回收和冷读持久 ACK；仅启动脚本基础预检不包含这些后续证据。同机 SIGKILL 不证明宿主掉电安全。分配器实际发布构建证据见[采样记录](evidence/2026-10-19-allocator-release.zh-CN.md)。

阶段 A—E 的本地正确性门禁通过，17 项有限 RF1 [性能报告](evidence/2026-10-20-local-performance.zh-CN.md)已交付；整体计划尚未完成生产验收。该短跑只有四个生产 worker 和一个顺序消费/ACK 的 Consumer；System 小消息生产约 1829 条/秒、消费约 60.30 条/秒，offered→持久 ACK P99 上界约 65.50 秒，包含突发后的排队积压。它不支持高吞吐或低延迟目标已达成，也不能冻结生产分配器。

原生 Collector/Prometheus/Grafana 已验证实际 RF1 与 Collector 抓取、29 个看板面板、50 条 PromQL 和只读权限；该观测部署验证的四个自有进程正常退出，五个 TCP 和两个 UDP 端口释放。

按用户指定，阶段 F 先在 `ssh dev` 完成 [Linux Docker 隔离验证](evidence/2026-10-20-linux-docker.zh-CN.md)。新版冻结 `a3f8f4e` 的 RF1 三项和 RF3 七项全部通过；同时修复热正文取得与持久签发之间的本地回收竞态，真正逻辑过期仍终态拒绝，Unknown 仍使整批报错。RF1 已停，RF3 十角色、RustFS 和 namespace holder 保留运行；没有宿主端口，原身份、卷与受限证据保留。部署前后既有 17 个业务容器的 ID、运行状态、启动时间和重启次数完全一致。

新版 Linux 的普通测试 246 通过、11 ignored，另显式执行十个真实 RustFS ignored 全部通过；验证 driver 六项和 workspace 文档三项通过。SDK metrics＋OpenTelemetry 组合另有 25 项普通及一项文档测试通过。格式、Clippy 和生产 panic/索引审计通过。新四项读取边界、feature 与文档检查有重叠，不能相加；Linux 官方 Collector ignored 尚未运行。旧 macOS 多 fixture 回归中的 drain 瞬时拒绝仍保留原日志，本轮 Linux 相同串行条件下该用例通过，不代表 macOS 问题已定位。

真实三区阶段尚未创建云资源，没有三可用区 SLA、生产 allocator 结论或 Kafka/Pulsar/AutoMQ 性能结论。候选秒级恢复目标需要从故障发生至原 SDK 再次持久成功单独计时，单例和测试总用时不能当作恢复分位数。
