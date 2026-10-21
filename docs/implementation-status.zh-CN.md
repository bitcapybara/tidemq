# 实施状态

2026-10-21：分段 WAL 已实现，包含唯一 manifest、有序 writer、引用检查点、有界段搬迁、快照 reader pin、失败关闭、物理容量采样与迁移预留；详见[当前存储设计](design/storage-recovery.zh-CN.md)。磁盘布局版本为 2，开发阶段使用全新数据目录，明确拒绝旧格式。源码 `426ff65` 的 macOS／Linux 普通 nextest 均为 351 passed／13 skipped；该版 Linux 插桩回归另有一项迁移 Unknown 失败，仍在定位，不由普通通过替代。`4f79a5` 的[完整 Linux 覆盖率](evidence/2026-10-21-wal-segments-coverage.zh-CN.md)及独立 Docker 十项矩阵通过，但其完整性能 cohort 在 RF3 持久 ACK 校验失败；新版本完整性能与覆盖率仍待验收，旧结果不冒认后续源码。

2026-10-21：按用户要求，仓库已设为 Public，并重跑远端 main `addf6d9` 的 [Rust](https://github.com/bitcapybara/tidemq/actions/runs/39419125570) 与 [Performance](https://github.com/bitcapybara/tidemq/actions/runs/39419125608)。runner 已实际执行；Rust 重跑因超大 HELLO 拒绝断言和精确覆盖率下降失败，不能继续记作计费未启动。HELLO 已用真实 EOF 红绿回归修正测试，仍禁止任何成功帧；性能重跑的最终结果另行记录。没有修改付款、负载、期限、比较策略或基线。

2026-10-20 历史重构：Rust 已固定为 1.99.1，配置仅接受当前按角色拆分的格式。源码 `693f9bd` 的 Rust CI 8／8 job 成功，Linux／macOS workspace 各 310 passed／12 skipped，文档各 4 passed；System／jemalloc 各 250 passed／11 skipped，全 target 严格 Clippy 与生产 panic／索引审计通过。无效参数、错误日志位置、共享身份字符串及 multipart 许可生命周期已整理；SDK 可选 OpenTelemetry 指标复用宿主 provider。该轮仅完成 WAL [分析](research/wal-segments.zh-CN.md)，后续实现以上述新布局为准。

首轮 main `401c7c9` 的两平台默认测试与 Linux 覆盖率通过，但 System 两例超时，性能候选在配置校验退出；覆盖率与性能基线均未晋升。已修复可复现的过期 Control 回复关闭共享 stream、换主测试固定旧 Metadata 权威，以及共享 Cargo target 错用另一源码产物三个缺陷；均有实际 red／green 回归，未改 lease、deadline、重试、负载或统计门槛。原始失败记录及新完整 cohort 声明见[本轮证据](evidence/2026-10-20-ci-refactor.zh-CN.md)。

第二轮完整 32 个性能试次和原有统计门禁通过；覆盖率原始计数为行 27907／35176、函数 2576／3637、region 38365／49848。Performance 汇总 job 未获得 runner，GitHub 注释明确付款／spending limit 问题，整体 workflow 仍为 failure，自动晋升被跳过。独立重算全部原始报告、核对源码及实际成功 job 后，本地运行原联合脚本，以 `d38fb80` 一次记录指向 `693f9bd` 的两套数值基线和逐字节原报告。RF3 两项 p99 仍 imprecise，不能宣称尾延迟无退化；付款设置未改，恢复额度后的远端验收仍待实际运行，详见[本轮证据](evidence/2026-10-20-ci-refactor.zh-CN.md)。

2026-10-20：完整计划已获用户批准，本轮在固定集成分支 `feat/yyx/integration-main` 汇合多个 `wt` worktree。Rust SDK 位于本仓库，默认 mimalloc。取消上下文、共享缓冲区、元数据创建/迁移重放及持久 Read 前缀结果边界已修复，冻结 `56a8349` 的 workspace 为 284 passed/12 ignored、34 个 suite、0 failed，Clippy/严格审计通过。main `077004e` 与 `d7c913e` 的七项 Rust CI job 均全部通过；`077004e` 的 Linux Docker 十项矩阵及 736 条完整持久 ACK 通过，精确版本与隔离审计见[最新记录](evidence/2026-10-20-docker-077004e.zh-CN.md)。隔离构建/测量的 `d7c913e` 完整 32 个试次、汇总与晋升均成功，`e16c79d` 已提交真实数值基线。四项 p99 仍为 imprecise，不证明小幅尾回退已排除或生产性能已达标；历史失败和不确定报告按[本轮记录](evidence/2026-10-20-cancellation-zero-copy-performance.zh-CN.md)分别保留。

| 任务 | 已实现与已有行为证据 | 剩余验收边界 |
| --- | --- | --- |
| T00 基础与内存 | 四个 package、schema/codec/ID/边界、Snafu 错误、有限日志/指标、配置、应用许可与控制预留；默认 mimalloc，System/jemalloc 为互斥选项，实际二进制可识别；同 jemalloc 发布构建可符号化采样；Linux Docker 的 System/cgroup 与 RF1 受控 OOM 恢复已验 | 默认 mimalloc 的生产性能、Linux 分配栈与长稳待验 |
| T01 QUIC | 实际 s2n-quic 1.91、TLS/mTLS、有限分类连接池/流/队列、独立读写、单请求取消、原生新包 ACK 活性探测；空闲 sibling 不关闭活跃连接，半帧仍有限组装；原期限内关闭 owned I/O 并释放端口；大块唯一所有 payload 分段发送、容量计费与末引用许可；有限 2/4 lane 同预算对照 | 真实 LB/NAT/CID、混合连接/短流/公平性与完整规模矩阵待验；codec 探针不能代替服务性能结论 |
| T02 持久化 | 分段 fsync WAL/vote/log/commit、唯一 manifest、refs-only 检查点、有界搬迁、具名快照与 reader pin；官方 OpenRaft storage-v2 Suite、RF1/RF3 quorum、真实 QUIC 换主/快照/丢回包、SIGKILL 发布屏障、实际文件写失败与失败关闭 | 新冻结版本的完整覆盖率／性能仍待验；完整应用快照仍有 O(状态) 成本，真实掉电、跨区和长稳待验 |
| T03 控制与所有权 | 独立元数据组、CRUD 容量与永久身份、grant/fence/recover/activate、有限 TTL；真实 Broker 强杀后自动接管、旧身份拒绝及元数据少数派停授权 | 未 Active 的原 grant 目标同时死亡时保守阻塞；三区与长期故障组合待验 |
| T04 生产 | 同组持久 producer/epoch/sequence/摘要/原结果、显式注册、固定过期、owned 提交；稳定版本 key 路由 helper | 无持久 outbox；有限原结果窗口届满不能自动以新身份重发 |
| T05 订阅 | 同组持久 attempt/token/ACK、连续前沿/ACK 洞、seek/代次、冷读 H/接收屏障与 B 重试、窗口硬上限；部分冷页提交后丢响应、尾部先 ACK 和重启补投 | 外部业务副作用仍需幂等；高压力和长稳待验 |
| T06 Rust SDK | 原生 QUIC Admin/Producer/Consumer、默认逻辑调用总预算、`RequestContext` / `CancellationToken` 取消域、有限重试、保留 PendingBatch、独立宿主观测；嵌套期限、关闭与发送后 Unknown；原对象跨 Broker/Storage/Gateway 强杀恢复及稳定 UDP 地址上的丢包/重排/MTU 故障 | SDK 重启无自动 outbox；新 Tokio task 需显式传入取消域；真实生产 LB/NAT/CID 和跨区恢复待验 |
| T07 S3 | 固定 aws-sdk-s3、不可变 pack/校验/索引/Range、合装、multipart 取消对账、有界缓存；真实 RustFS 基础及 AWS SDK 测试 | 生产 S3/网络故障、持续中断容量与掉电待验 |
| T08 生命周期 | 两权威注册/授权/发布/完成/取消、原对象对账、pin/恢复保护、自动整理替换验证/换引用、全 voter 持久检查点、floor/本地回收、DELETING 重试；RF1/RF3 真实 RustFS 多消息回收后冷读 | 新 voter 加入与冷回收的压力组合、对象永久账本规模待验；账本硬上限 4096 |
| T09 保留与配额 | 同 apply 年龄/ACK/CAS floor、有界时间索引、独立逻辑/物理/共享滞后/RSS 水位、真实磁盘/RSS 采样、cgroup v2 可见祖先 headroom、已有 ACK/control/GC 保留准入；Linux 384 MiB→6 MiB 真实 OOM、恢复原限额/原 Pending 与持久 ACK 已验 | 默认限额自然压力、持续归档中断和高压力下隔离待验 |
| T10 运行时 | serve、组合/独立角色、实际内外 QUIC、管理健康/就绪/指标/诊断、TLS/静态授权、正常关闭；兼容 LibreSSL 的配置生成器；真实 RF1/RF3 热冷路径、独立 OS 进程故障与原 SDK 恢复；Linux 独立 Docker 项目、UID/卷/cgroup/无宿主端口实际核验 | 真实生产部署、跨区及长期压力待验 |
| T11 弹性 | 私有稳定成员证明、learner/joint/stable、真实 Raft 迁移、独立修复预算、真实容量/CPU/网络/连接/ACK 评分；owned ticker 驱动显式/自动 Storage 修复及 Broker 移交 | 冷却/长期均衡、高压力及跨区容量待验 |
| T12 故障 | 有限参考模型、实际磁盘/QUIC/RF3/SDK/RustFS；独立 RF3 CLI 与 Linux Docker 的 Storage/Broker 强杀、Metadata/Storage 多数派中断、Gateway 冷重启、RF1 真 OOM；`077004e` 十个 Docker case 合计 736 条完整唯一交付和持久 ACK；`d7c913e` Linux workspace 284 passed/12 ignored、34 个 suite | 历史 RustFS ignored 与当前普通门禁分开计数；Linux 官方 Collector ignored 未执行；生产 LB/三区、掉电与长稳待验 |
| T13 性能 | 冻结 `1398353` 的 17 项有限 RF1 报告；三 allocator、日志三档、2/4 lane 与 1% 服务 OTLP；真实 RF1/三进程 RF3、受保护策略和原始逐消息延迟；`693f9bd` 完整 32 试次通过，`d38fb80` 本地审计记录性能／覆盖率数值基线 | 原 RF3 两项 p99 仍 imprecise；历史计费失败与公开后的真实重跑分别保留。新分段 WAL 版的完整性能仍待验；高吞吐低延迟、长稳和三区目标未放行 |
| T14 文档 | 各任务使用/设计/排障文档、有限指标、模块看板告警；可选 OTLP 有界导出、实际四角色关联、Collector 多故障隔离；统一导航、84 条覆盖、Linux Docker 构建/隔离/故障脚本与真实命令证据 | Linux 分配栈、生产命令与浏览器画面仍按对应环境验收 |

## 证据口径

测试与复现入口见[测试方法](testing-benchmarks.zh-CN.md)，逐条对应见[要求覆盖](requirements-coverage.zh-CN.md)。先前本地验收为 macOS arm64，Rust/Cargo 1.96.0；真实 QUIC 测试使用本机 UDP/TLS 和真实磁盘。分角色 RF3 的早期回归在一个测试进程启动多个 Runtime；后续 RF1/RF3 强杀与自动接管另以独立 CLI 子进程验证。新增 Linux Docker 证据另行冻结源码、镜像和二进制身份；各类证据分别标注，单机 zone 不代表独立故障域。

回归中发现并修复了旧路由缓存重试、冷页逐条取 pin 耗尽控制期限、整批逐条签发耗尽起始 lease、归档固定条数限制、采样相位导致配额陈旧、空闲 sibling 误关闭活跃 RPC 及失联连接清理耗尽关闭期限。Read 在最早权限期限内分片返回，每条仍须持久签发，Unknown 与最终安全校验不变。Broker 自动接管的真实进程证据见[故障记录](evidence/2026-10-20-broker-failover.zh-CN.md)。合流后普通测试 246 通过、9 ignored；使用真实 RustFS/官方 Collector 单独执行九个 ignored，全部通过。SDK metrics＋OpenTelemetry 组合 26 项及 workspace 三个文档测试也通过；这些是独立且有重叠的检查，不相加作为不同测试总数。

RustFS 1.0.3 原生隔离实例已覆盖 PUT/GET/HEAD、206/416、multipart、AWS SDK、原对象恢复、发布/检查点/本地回收和冷读持久 ACK；仅启动脚本基础预检不包含这些后续证据。同机 SIGKILL 不证明宿主掉电安全。分配器实际发布构建证据见[采样记录](evidence/2026-10-19-allocator-release.zh-CN.md)。

阶段 A—E 的本地正确性门禁通过，17 项有限 RF1 [性能报告](evidence/2026-10-20-local-performance.zh-CN.md)已交付；整体计划尚未完成生产验收。该短跑只有四个生产 worker 和一个顺序消费/ACK 的 Consumer；System 小消息生产约 1829 条/秒、消费约 60.30 条/秒，offered→持久 ACK P99 上界约 65.50 秒，包含突发后的排队积压。它不支持高吞吐或低延迟目标已达成，也不是此次将 mimalloc 设为默认的性能证明。默认值调整后 249 项串行测试通过，普通并行 runtime_queue 回归仍有不稳定项，详见[后续验证记录](evidence/2026-10-20-mimalloc-default.zh-CN.md)；串行通过不替代并发压力和生产恢复验收。

原生 Collector/Prometheus/Grafana 已验证实际 RF1 与 Collector 抓取、29 个看板面板、50 条 PromQL 和只读权限；该观测部署验证的四个自有进程正常退出，五个 TCP 和两个 UDP 端口释放。

按用户指定，阶段 F 先在 `ssh dev` 完成 [Linux Docker 隔离验证](evidence/2026-10-20-linux-docker.zh-CN.md)。旧版冻结 `a3f8f4e` 的 RF1 三项和 RF3 七项全部通过；同时修复热正文取得与持久签发之间的本地回收竞态，真正逻辑过期仍终态拒绝，Unknown 仍使整批报错。该历史 RF1 已停，RF3 十角色、RustFS 和 namespace holder 保留运行；没有宿主端口，原身份、卷与受限证据保留。

最新另建的 [077004e Docker 矩阵](evidence/2026-10-20-docker-077004e.zh-CN.md)同样十项通过、736 条完整持久 ACK；原 Pending、逻辑 SDK session、关闭及最终配额归零全部核验。该版服务实际为 mimalloc，driver 自身为 System；不混用身份。新项目所有容器和网络已移除，保留 15 个自有卷和私有证据；原 17 个业务容器及旧 RF3 的 12 个容器，精确身份、运行状态、启动时间和重启次数前后一致，旧镜像 tag 身份也一致。故障中允许不可达的采集错误及单主机网络边界在记录中明确保留，不作为独立三区或恢复分位数证据。

历史 `a3f8f4e` Linux 的普通测试 246 通过、11 ignored，另显式执行十个真实 RustFS ignored 全部通过；验证 driver 六项和 workspace 文档三项通过。SDK metrics＋OpenTelemetry 组合另有 25 项普通及一项文档测试通过。格式、Clippy 和生产 panic/索引审计通过。新四项读取边界、feature 与文档检查有重叠，不能相加；Linux 官方 Collector ignored 尚未运行。旧 macOS 多 fixture 回归中的 drain 瞬时拒绝仍保留原日志，该版 Linux 相同串行条件下用例通过，不代表旧 macOS 问题的现场根因已唯一确认。

真实三区阶段尚未创建云资源，没有三可用区 SLA、生产 allocator 结论或 Kafka/Pulsar/AutoMQ 性能结论。候选秒级恢复目标需要从故障发生至原 SDK 再次持久成功单独计时，单例和测试总用时不能当作恢复分位数。
