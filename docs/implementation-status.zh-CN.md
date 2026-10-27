# 实施状态

2026-10-26 当前安排：已完成方案 A 的 11 台 KVM VM、6 节点 kubeadm、独立公共 RustFS / 观测 / runner / fixtures / loadgen 部署。修复版本 `14372cd` 的完整 Rust CI（Linux/macOS、真实依赖、覆盖率、VM 故障）通过，常驻 10 个角色及 loadgen 已升级到同版。资源、入口、使用方法和逐次证据见[实验室清单](https://github.com/bitcapybara/tidemq/blob/main/deploy/lab/README.zh-CN.md)。按用户要求删除性能 workflow、负载工具、比较器、配置、基线和专项报告；本次不做性能测试。以下带日期的内容为历史实施范围，旧宿主服务、源码、数据和配置已清理，不能作为当前运行清单。

2026-10-21：分段 WAL 已实现，包含唯一 manifest、有序 writer、引用检查点、有界段搬迁、快照 reader pin、失败关闭、物理容量采样与迁移预留；详见[当前存储设计](design/storage-recovery.zh-CN.md)。磁盘布局版本为 2，开发阶段使用全新数据目录，明确拒绝旧格式。冻结 `24471ce` 的 macOS／Linux 普通及 Linux 插桩 nextest 均为 **364 passed／13 skipped**；[精确覆盖率](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-wal-segments-coverage-24471ce.zh-CN.md)和[新卷 Docker 十项／736 条持久 ACK](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-wal-segments-linux-24471ce.zh-CN.md)通过，完整原始归档已回传并核验。

`24471ce` 包含[有界热页复用](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-live-read-hot-pages.zh-CN.md)、[热页元数据准入](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-hot-read-admission.zh-CN.md)和[提交后错误源保留](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-claim-postcommit-unknown.zh-CN.md)，原期限、配额、逐条持久签发及 wire Unknown 字段保持一致。真实租约到期红绿回归保留原 Unknown 并恢复完整错误链。原 37／38 集成运行中的 Claim 首错原因仍未知；历史失败不能由新版本通过反推唯一原因。

2026-10-21：按用户要求，仓库已设为 Public，并重跑远端 main `addf6d9` 的 [Rust](https://github.com/bitcapybara/tidemq/actions/runs/39419125570) 与 [Performance](https://github.com/bitcapybara/tidemq/actions/runs/39419125608)。runner 已实际执行；Rust 因超大 HELLO 拒绝断言和精确覆盖率下降失败，性能 build／measure／gate 通过，联合 promotion 因 Rust 失败而正确停止，见[完整 hosted 证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-hosted-ci-addf6d9.zh-CN.md)。HELLO 已用真实 EOF 红绿回归修正测试，仍禁止任何成功帧。没有修改付款、负载、期限、比较策略或基线。

2026-10-20 历史重构：Rust 已固定为 1.99.1，配置仅接受当前按角色拆分的格式。源码 `693f9bd` 的 Rust CI 8／8 job 成功，Linux／macOS workspace 各 310 passed／12 skipped，文档各 4 passed；System／jemalloc 各 250 passed／11 skipped，全 target 严格 Clippy 与生产 panic／索引审计通过。无效参数、错误日志位置、共享身份字符串及 multipart 许可生命周期已整理；SDK 可选 OpenTelemetry 指标复用宿主 provider。该轮仅完成 WAL [分析](research/wal-segments.zh-CN.md)，后续实现以上述新布局为准。

首轮 main `401c7c9` 的两平台默认测试与 Linux 覆盖率通过，但 System 两例超时，性能候选在配置校验退出；覆盖率与性能基线均未晋升。已修复可复现的过期 Control 回复关闭共享 stream、换主测试固定旧 Metadata 权威，以及共享 Cargo target 错用另一源码产物三个缺陷；均有实际 red／green 回归，未改 lease、deadline、重试、负载或统计门槛。原始失败记录及新完整 cohort 声明见[本轮证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-ci-refactor.zh-CN.md)。

2026-10-20：完整计划已获用户批准，本轮在固定集成分支 `feat/yyx/integration-main` 汇合多个 `wt` worktree。Rust SDK 位于本仓库，默认 mimalloc。取消上下文、共享缓冲区、元数据创建/迁移重放及持久 Read 前缀结果边界已修复，冻结 `56a8349` 的 workspace 为 284 passed/12 ignored、34 个 suite、0 failed，Clippy/严格审计通过。main `077004e` 与 `d7c913e` 的七项 Rust CI job 均全部通过；`077004e` 的 Linux Docker 十项矩阵及 736 条完整持久 ACK 通过，精确版本与隔离审计见[最新记录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-docker-077004e.zh-CN.md)。

| 任务 | 已实现与已有行为证据 | 剩余验收边界 |
| --- | --- | --- |
| T00 基础与内存 | 四个 package、schema/codec/ID/边界、Snafu 错误、有限日志/指标、配置、应用许可与控制预留；默认 mimalloc，System/jemalloc 为互斥选项，实际二进制可识别；同 jemalloc 发布构建可符号化采样；Linux Docker 的 System/cgroup 与 RF1 受控 OOM 恢复已验 | 默认 mimalloc 的生产性能、Linux 分配栈与长稳待验 |
| T01 QUIC | 实际 s2n-quic 1.91、TLS/mTLS、有限分类连接池/流/队列、独立读写、单请求取消、原生新包 ACK 活性探测；空闲 sibling 不关闭活跃连接，半帧仍有限组装；原期限内关闭 owned I/O 并释放端口；大块唯一所有 payload 分段发送、容量计费与末引用许可；有限 2/4 lane 同预算对照 | 真实 LB/NAT/CID、混合连接/短流/公平性与完整规模矩阵待验；codec 探针不能代替服务性能结论 |
| T02 持久化 | 分段 fsync WAL/vote/log/commit、唯一 manifest、refs-only 检查点、有界搬迁、具名快照与 reader pin；官方 OpenRaft storage-v2 Suite、RF1/RF3 quorum、真实 QUIC 换主/快照/丢回包、SIGKILL 发布屏障、实际文件写失败与失败关闭；24471ce 完整覆盖率及 Docker 矩阵通过 | 评审指出的逻辑日志字节与快照策略仍需改进；完整应用快照仍有 O(状态) 成本，真实掉电、跨区和长稳待验 |
| T03 控制与所有权 | 独立元数据组、CRUD 容量与永久身份、grant/fence/recover/activate、有限 TTL；真实 Broker 强杀后自动接管、旧身份拒绝及元数据少数派停授权 | 未 Active 的原 grant 目标同时死亡时保守阻塞；三区与长期故障组合待验 |
| T04 生产 | 同组持久 producer/epoch/sequence/摘要/原结果、显式注册、固定过期、owned 提交；稳定版本 key 路由 helper | 无持久 outbox；有限原结果窗口届满不能自动以新身份重发 |
| T05 订阅 | 同组持久 attempt/token/ACK、连续前沿/ACK 洞、seek/代次、冷读 H/接收屏障与 B 重试、窗口硬上限；部分冷页提交后丢响应、尾部先 ACK 和重启补投 | 外部业务副作用仍需幂等；高压力和长稳待验 |
| T06 Rust SDK | 原生 QUIC Admin/Producer/Consumer、默认逻辑调用总预算、`RequestContext` / `CancellationToken` 取消域、有限重试、保留 PendingBatch、独立宿主观测；嵌套期限、关闭与发送后 Unknown；原对象跨 Broker/Storage/Gateway 强杀恢复及稳定 UDP 地址上的丢包/重排/MTU 故障 | SDK 重启无自动 outbox；新 Tokio task 需显式传入取消域；真实生产 LB/NAT/CID 和跨区恢复待验 |
| T07 S3 | 固定 aws-sdk-s3、不可变 pack/校验/索引/Range、合装、multipart 取消对账、有界缓存；真实 RustFS 基础及 AWS SDK 测试 | 生产 S3/网络故障、持续中断容量与掉电待验 |
| T08 生命周期 | 两权威注册/授权/发布/完成/取消、原对象对账、pin/恢复保护、自动整理替换验证/换引用、全 voter 持久检查点、floor/本地回收、DELETING 重试；RF1/RF3 真实 RustFS 多消息回收后冷读 | 新 voter 加入与冷回收的压力组合、对象永久账本规模待验；账本硬上限 4096 |
| T09 保留与配额 | 同 apply 年龄/ACK/CAS floor、有界时间索引、独立逻辑/物理/共享滞后/RSS 水位、真实磁盘/RSS 采样、cgroup v2 可见祖先 headroom、已有 ACK/control/GC 保留准入；Linux 384 MiB→6 MiB 真实 OOM、恢复原限额/原 Pending 与持久 ACK 已验 | 默认限额自然压力、持续归档中断和高压力下隔离待验 |
| T10 运行时 | serve、组合/独立角色、实际内外 QUIC、管理健康/就绪/指标/诊断、TLS/静态授权、正常关闭；兼容 LibreSSL 的配置生成器；真实 RF1/RF3 热冷路径、独立 OS 进程故障与原 SDK 恢复；Linux 独立 Docker 项目、UID/卷/cgroup/无宿主端口实际核验 | 真实生产部署、跨区及长期压力待验 |
| T11 弹性 | 私有稳定成员证明、learner/joint/stable、真实 Raft 迁移、独立修复预算、真实容量/CPU/网络/连接/ACK 评分；owned ticker 驱动显式/自动 Storage 修复及 Broker 移交 | 冷却/长期均衡、高压力及跨区容量待验 |
| T12 故障 | 有限参考模型、实际磁盘/QUIC/RF3/SDK/RustFS；`24471ce` Linux Docker 十项／736 条持久 ACK，覆盖 Storage/Broker 强杀、Metadata/Storage 多数派中断、Gateway 冷重启与 RF1 受控 OOM；普通及插桩 workspace 各 364 passed／13 skipped，真实 RustFS 另行十项通过 | RustFS 与普通门禁分开计数；受控 OOM 为原容器 384 MiB→6 MiB 后恢复原限额；Linux 官方 Collector ignored、生产 LB/三区、掉电与长稳待验 |
| T13 性能 | 性能测试相关代码、脚本、配置、基线和专项报告已删除 | 本次不执行性能验收，容量、吞吐、尾延迟及生产 SLO 均未放行 |
| T14 文档 | 各任务使用/设计/排障文档、有限指标、模块看板告警；可选 OTLP 有界导出、实际四角色关联、Collector 多故障隔离；统一导航、84 条覆盖、Linux Docker 构建/隔离/故障脚本与真实命令证据 | Linux 分配栈、生产命令与浏览器画面仍按对应环境验收 |

## 证据口径

测试与复现入口见[测试方法](testing.zh-CN.md)，逐条对应见[要求覆盖](requirements-coverage.zh-CN.md)。先前本地验收为 macOS arm64，Rust/Cargo 1.96.0；真实 QUIC 测试使用本机 UDP/TLS 和真实磁盘。分角色 RF3 的早期回归在一个测试进程启动多个 Runtime；后续 RF1/RF3 强杀与自动接管另以独立 CLI 子进程验证。新增 Linux Docker 证据另行冻结源码、镜像和二进制身份；各类证据分别标注，单机 zone 不代表独立故障域。

回归中发现并修复了旧路由缓存重试、冷页逐条取 pin 耗尽控制期限、整批逐条签发耗尽起始 lease、归档固定条数限制、采样相位导致配额陈旧、空闲 sibling 误关闭活跃 RPC 及失联连接清理耗尽关闭期限。Read 在最早权限期限内分片返回，每条仍须持久签发，Unknown 与最终安全校验不变。Broker 自动接管的真实进程证据见[故障记录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-broker-failover.zh-CN.md)。合流后普通测试 246 通过、9 ignored；使用真实 RustFS/官方 Collector 单独执行九个 ignored，全部通过。SDK metrics＋OpenTelemetry 组合 26 项及 workspace 三个文档测试也通过；这些是独立且有重叠的检查，不相加作为不同测试总数。

RustFS 1.0.3 原生隔离实例已覆盖 PUT/GET/HEAD、206/416、multipart、AWS SDK、原对象恢复、发布/检查点/本地回收和冷读持久 ACK；仅启动脚本基础预检不包含这些后续证据。同机 SIGKILL 不证明宿主掉电安全。分配器实际发布构建证据见[采样记录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-19-allocator-release.zh-CN.md)。

阶段 A—E 的有限本地正确性门禁不代表完整生产验收；当前部署和 CI 结果以实验室清单及其 revision 对应的证据为准。

原生 Collector/Prometheus/Grafana 已验证实际 RF1 与 Collector 抓取、29 个看板面板、50 条 PromQL 和只读权限；该观测部署验证的四个自有进程正常退出，五个 TCP 和两个 UDP 端口释放。

按用户指定，阶段 F 先在 `ssh dev` 完成 [Linux Docker 隔离验证](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-linux-docker.zh-CN.md)。旧版冻结 `a3f8f4e` 的 RF1 三项和 RF3 七项全部通过；同时修复热正文取得与持久签发之间的本地回收竞态，真正逻辑过期仍终态拒绝，Unknown 仍使整批报错。该历史 RF1 已停，RF3 十角色、RustFS 和 namespace holder 保留运行；没有宿主端口，原身份、卷与受限证据保留。

最新另建的 [077004e Docker 矩阵](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-20-docker-077004e.zh-CN.md)同样十项通过、736 条完整持久 ACK；原 Pending、逻辑 SDK session、关闭及最终配额归零全部核验。该版服务实际为 mimalloc，driver 自身为 System；不混用身份。新项目所有容器和网络已移除，保留 15 个自有卷和私有证据；原 17 个业务容器及旧 RF3 的 12 个容器，精确身份、运行状态、启动时间和重启次数前后一致，旧镜像 tag 身份也一致。故障中允许不可达的采集错误及单主机网络边界在记录中明确保留，不作为独立三区或恢复分位数证据。

历史 `a3f8f4e` Linux 的普通测试 246 通过、11 ignored，另显式执行十个真实 RustFS ignored 全部通过；验证 driver 六项和 workspace 文档三项通过。SDK metrics＋OpenTelemetry 组合另有 25 项普通及一项文档测试通过。格式、Clippy 和生产 panic/索引审计通过。新四项读取边界、feature 与文档检查有重叠，不能相加；Linux 官方 Collector ignored 尚未运行。旧 macOS 多 fixture 回归中的 drain 瞬时拒绝仍保留原日志，该版 Linux 相同串行条件下用例通过，不代表旧 macOS 问题的现场根因已唯一确认。

真实三区阶段尚未创建云资源，没有三可用区 SLA、生产 allocator 结论或 Kafka/Pulsar/AutoMQ 性能结论。候选秒级恢复目标需要从故障发生至原 SDK 再次持久成功单独计时，单例和测试总用时不能当作恢复分位数。
