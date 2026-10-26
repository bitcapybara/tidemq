# VM 实验室实施与验收记录

实施日期为 2026-10-25 UTC，部分工作对应北京时间 2026-10-26。目录名表示本次实施批次，JSON 中的 `capturedAt` 才是具体采集时间。运维入口、资源分配和使用方法见 [实验室清单](../../README.zh-CN.md)。本记录区分手动故障验证、干净提交的常驻部署和 GitHub workflow 结果。2026-10-26 已把常驻 10 个角色和 loadgen 更新到 SDK 修复版本，并重新验证外部访问与观测；性能测试相关工具、配置、基线和专项报告已按用户要求删除。

## 资源与外部入口

| 证据 | 内容 |
|---|---|
| [final-cleanup.json](final-cleanup.json) | 20 个旧环境目录均不存在、宿主 SSH/libvirt/agent/入口服务健康、无宿主 Docker 应用或历史 core dump |
| [final-state.json](final-state.json) | 升级后 11 台 VM running、6 节点 Ready、10 角色的实际镜像、身份保留及旧单元清理状态 |
| [public-upgrade.json](public-upgrade.json) | 固定修复镜像与二进制 SHA、10 个 Pod 逐个替换前后身份，以及未变更的 Secret/Service/PVC 元数据 |
| [final-loadgen-report.json](final-loadgen-report.json)、[final-external-sdk-report.json](final-external-sdk-report.json) | 修复版本在 loadgen 与 Mac LAN/VPN 入口分别完成 64 条持久消息及客户端关闭 |
| [final-client-identity.json](final-client-identity.json) | loadgen 与 Mac 验证二进制的源码、镜像和 SHA-256 |
| [final-external-acceptance.json](final-external-acceptance.json) | 升级后外部 S3 PUT/GET/DELETE、跨 bucket 拒绝、TLS/mTLS、页面、35 个健康实例、应用与设备三类观测、无活动告警 |
| [resource-snapshot.json](resource-snapshot.json) | 宿主资源、11 台 VM 的 UUID、实际磁盘、地址和保留项 |
| [public-cluster-resources.json](public-cluster-resources.json) | 6 个 Kubernetes 节点、常驻 10 个角色 Pod、实际 Service/NodePort 与镜像 digest |
| [listeners/](listeners/) | 宿主及全部 VM 的实际 TCP/UDP 监听；仅运维清单列出的端口映射到 LAN |
| [cold-toolchain-validation.txt](cold-toolchain-validation.txt) | 从固定构建 bundle 解压后验证 Rust、Clippy、LLVM 和组件，构建身份不依赖热缓存 |
| [external-tool-identity.json](external-tool-identity.json) | GitHub Linux job 实际执行的 RustFS / Collector 版本、二进制 SHA 与来源 archive 锁 |
| [public-service-acceptance.json](public-service-acceptance.json) | 公共 RustFS 两个 bucket 账户的自身访问 200、跨 bucket 访问 403 |
| [public-external-sdk-report.json](public-external-sdk-report.json) | Mac 经 LAN/VPN、TLS 校验及真实 QUIC 入口完成 64 条消息发送/接收/ACK，并关闭原客户端 |
| [external-telemetry-acceptance.json](external-telemetry-acceptance.json) | 外部 mTLS Collector 上报的 metric、log 与指定 trace 实际入库查询结果 |
| [external-pages-acceptance.json](external-pages-acceptance.json) | RustFS、Alertmanager 页面/前端资源/API 及未认证入口的 HTTP 状态 |
| [public-observability-acceptance.json](public-observability-acceptance.json) | 35 个采集实例、常驻 10 个应用指标实例，以及应用日志和 traces |
| [public-trace-batch-update.json](public-trace-batch-update.json) | 常驻 10 个角色的 trace 批次更新与实际配置 SHA-256 |
| [public-trace-budget-acceptance.json](public-trace-budget-acceptance.json) | 连续 194 秒、6,480 次导出，10 个实例的 discarded/export failure 计数均不增长 |
| [public-alerts-after-trace-fix.json](public-alerts-after-trace-fix.json) | 修复后查询无 pending/firing 告警 |
| [rustfs-observability-evidence.json](rustfs-observability-evidence.json) | 公共及 a/b fixture 的历史 readiness、fixture namespace/实例标签与公共 RustFS traces |
| [cri-rotation-replay.json](cri-rotation-replay.json) | 停止节点 agent、写入并轮转 CRI 文件、重启 agent 后，两条未压缩日志均可查询 |
| [alert-routing-acceptance.json](alert-routing-acceptance.json) | Alertmanager 至本机审计 webhook 的 firing/resolved 事件；未向人员发送通知 |
| [coverage-summary.json](coverage-summary.json) | 修复提交 `14372cd` 的覆盖率门槛结果、564 项插桩测试环境和报告 SHA；行覆盖率 83.64% |
| [github-rust-run.json](github-rust-run.json)、[github-vm-validation-passed.json](github-vm-validation-passed.json) | 修复提交完整 GitHub Rust CI 全绿及 8 个 VM 业务场景、watchdog、三类入库信号 |

首次常驻部署来自 [public-source-commit.txt](public-source-commit.txt) 中的干净提交，源码补丁 SHA-256 为 [public-source-patch-sha256.txt](public-source-patch-sha256.txt) 中的空补丁哈希，镜像 ID 在 [public-image-id.txt](public-image-id.txt)。[public-initial-report.json](public-initial-report.json) 是独立 loadgen VM 的首次 64 条验收。上述 initial / public-source 文件保留初次部署身份；当前运行版本以 public-upgrade.json、final-state.json 及 final 两份 SDK 报告为准。客户端 CA、token、服务密钥、含 Secret 的 Kubernetes 资源和磁盘镜像均未纳入本目录。

RustFS 1.0.3 的嵌入式管理页面位于 `/rustfs/console/index.html`，LAN 入口的 `/` 已配置跳转；已验证页面及前端脚本返回 200。该路径见 [RustFS 状态检查文档](https://docs.rustfs.com/en/operations/status-check)，锁定版本见 [1.0.3 release](https://github.com/rustfs/rustfs/releases/tag/1.0.3)。无客户端证书的 OTLP 请求被拒绝（HTTP 400）；metrics 入口未经认证返回 401。

## VM 故障验证

接受的手动运行是 `ci-1792941791-1`，聚合记录为 [vm-validation-accepted.json](vm-validation-accepted.json)，业务退出码和清理退出码均为 0。它的源码身份为 `297a72ffe59cee1126585eac47e3be1c581e7ac3` 加记录中的 Rust 补丁 SHA，不等同于常驻实例的干净提交。记录包含应用/验证器/镜像及关键脚本 SHA-256。

| 场景 | 验收 |
|---|---|
| 基础 SDK 与延后读取 | 各 64 条，发送、接收、ACK 数量一致 |
| 单 worker 断电 | 故障前后共 80 条，原客户端和 Consumer 完成恢复 |
| 单 worker 数据网络隔离 | 80 条，管理网络独立可达，恢复后原客户端继续 |
| 双 worker 丢失仲裁 | 必须观察 unavailable；恢复同一个 Pending 和 Consumer，80 条唯一消息及 ACK 完整 |
| control-plane 故障 | 64 条，业务继续通过其余控制节点运行 |
| RustFS a 故障 | a 独立恢复、b 保持健康；64 条完成，公共 RustFS 不参与故障 |
| 恢复后检查 | 64 条，角色恢复 Ready |
| 独立恢复 watchdog | 暂停 loadgen 后不再发送恢复指令，宿主计时器自行恢复 VM |
| 观测完整性 | CI namespace 的 metrics、logs、traces 均实际入库后才接受结果 |

基础设施恢复与 SDK 恢复分别计时。实际 worker 及应用恢复约 80–90 秒，在 110 秒基础设施限额内；此后才通知验证器重试原 Pending。SDK 的单次请求 10 秒和原操作恢复窗口 60 秒保持原值，报告保存各阶段时间，不能把主机 Ready 当作应用可用。

单 worker 断电及数据网隔离场景的 `fault_consumption` / `fault_observed` 阶段均在 `host_recovered` 之前完成，验证了剩余副本在故障期间仍可完成发送、接收与 ACK。80–90 秒记录的是全部 VM/角色重新就绪的时间。双 worker 丢失仲裁则要求先观察不可用，再恢复原 Pending；这些有限案例不产生生产恢复 P99。

早期失败均未作为通过：最初修复了 Docker 镜像导入、namespace 初始化、UDP `externalTrafficPolicy` 和 SDK 临时拒绝丢失原请求问题；后续两次发现应用未恢复就开始 SDK 重试及 Consumer 暂态重加入错误；第 8 次业务与 watchdog 通过但 CRI 日志未入库，因此整体仍失败，修复非 root agent 的最小 ACL 和轮转采集后第 9 次才接受。完整本机日志保存在宿主 `/var/lib/tidemq-lab/logs`，手动运行报告保存在 runner `/srv/tidemq/work/manual/target/ci/lab-report`。

## GitHub 与原生测试

已将 Linux 校验、集成测试、覆盖率和 VM 故障任务接入 `tidemq-dev-u126-8` runner，macOS 保留 GitHub-hosted。Linux 作业与手动任务都需取得宿主独占租约。故障场景使用独立 namespace、RustFS 实例及凭据，workflow 在失败时保存有界证据并执行清理。

| GitHub 运行 | 结果与后续 |
|---|---|
| [40190778196](https://github.com/bitcapybara/tidemq/actions/runs/40190778196) | 未通过；修复 JUnit 工作区路径及原发送恢复 |
| [40202579925](https://github.com/bitcapybara/tidemq/actions/runs/40202579925) | 未通过；修复 Raft 测试检查旧 leader 和 tracing 测试抢先检查计数的竞态 |
| [40204878896](https://github.com/bitcapybara/tidemq/actions/runs/40204878896) | macOS 完整通过；Linux 265/267，通过失败诊断定位原 ACK 恢复和关闭超时 |
| [40210315888](https://github.com/bitcapybara/tidemq/actions/runs/40210315888) | 提交 `1ab8c171`；macOS 完整通过，Linux 单元 297/297、SDK default 29/29、原生集成 266/267；自动分片接收超时仍在定位，因此 lab/coverage 未执行 |
| [40215415839](https://github.com/bitcapybara/tidemq/actions/runs/40215415839) | 提交 `8a1bb94`；Linux/macOS 校验通过，Linux 原生 267/267、故障钩子 75/75、真实依赖 16/16；覆盖率 564/564、行覆盖率 83.62%；VM 仲裁恢复失败，清理成功，整条 pipeline 未通过 |
| [40221753920](https://github.com/bitcapybara/tidemq/actions/runs/40221753920) | 修复提交 `14372cd`，完整通过：Linux/macOS 单元 297/297、SDK default 29/29、原生集成 267/267、故障钩子 75/75；Linux 真实依赖 16/16、覆盖率 564/564；VM 8 个业务场景、独立恢复 watchdog 和三类信号通过，业务及清理退出码均为 0 |
| [40254563550](https://github.com/bitcapybara/tidemq/actions/runs/40254563550) | 删除性能工具后的提交 `3e52ed7` 完整复验，等待最终结果；不以先前修复版通过代替本次结果 |

专项复现另有：Linux scaling membership 用例修复后连续 20 次通过；同一个 PendingAck 恢复用例连续 5 次通过；原 5 秒 close 时限的显式扩缩容用例在正确 TMPDIR 和独占资源下连续 20 次通过。后者未修改生产关闭语义，不能据此宣称根因已修复。macOS tracing 拒绝计数用例连续 10 次通过。Rust 修改执行 fmt 与 Clippy；脚本执行 Node 测试、Linux Bash 语法检查及 actionlint。

自动分片用例在独占 runner 上第 3 次复现失败，[scaling-delivery-failure-summary.json](scaling-delivery-failure-summary.json) 保存原始证据 SHA 和节点状态：134 条已发布、85 条已接收，节点均 Ready，无 quota 阻塞或 QUIC liveness failure。原测试将发布、分片与消费共用 20 秒期限，且将切换时原 Consumer 的后台读取结果未知当作终止。调整为各阶段分别 20 秒、仅在结果未知时继续同一 Consumer，完整消息/ACK/唯一性断言不变；之后 Linux 连续 20 次、macOS 连续 3 次通过。失败诊断仍保留，未增加 CI 自动重试。

另发现常驻 trace 每批 1 条、每 100 ms 导出的容量低于 16 spans/s 采纳预算，持续触发真实的丢弃告警。已将批次设为 16，并增加“批次容量覆盖导出时间预算”的配置校验；不修改丢弃告警规则。已逐个重建 OnDelete StatefulSet 的 Pod，等待 Ready 后才进行下一个，保留持久卷与身份，并同步私有配置备份。修复后 194 秒内增加 6,480 次导出，丢弃/导出失败计数无增长；启动时 sidecar 尚未监听导致的少量历史失败仍如实保留在计数中。

GitHub VM 仲裁恢复发现了 SDK 进一步缺陷：[失败报告](ci-40215415839-failure/quorum-loss-report.json)中，故障前 64 条已完成，故障期间的 8 条仍等待原回执；重试遇到一个错误标记为完成的本地 Deadline。临时准入拒绝没有记录原操作的最终结果，但 SDK 在本地截止时释放了该 Pending 的 payload。[回归记录](ci-40215415839-failure/regression.json)保存修复前 `complete=true` 的失败、修复提交及 Linux/macOS 各连续 10 次通过结果；[清理结果](ci-40215415839-failure/result.json)为 `cleanupExitCode=0`。

修复提交 `14372cd5a053bedf536fd3e91bf569d4bb17339c` 保留原操作和 payload，直到取得原回执或已记录的最终拒绝；请求与恢复时限均未修改。配额集成测试现在要求同一 Pending 恢复原消息，删除了发送替代消息的兜底。完整复验 [Rust 40221753920](https://github.com/bitcapybara/tidemq/actions/runs/40221753920) 全部通过：双 worker 恢复后 80 个唯一消息与 80 次持久 ACK 完整，故障中的 8 个原 Pending 各重试一次，原 Consumer 与客户端关闭检查均通过。

按用户最新要求，不兼容旧 SDK，不继续性能测量。性能 workflow、perf_load 及其模块、构建/执行/比较脚本、性能基线/校准配置、联合晋升脚本和专项报告共 39 个文件已删除；覆盖率自身的比较器、基线与边界测试保留。测试指南更名为 docs/testing.zh-CN.md，并清理当前入口中的旧操作说明。

## 清理与适用范围

已复查 11 个遗留应用 service/target 均为 not-found，3 条 YuXiangagent root cron 保留；升级快照时宿主根文件系统使用 55%，可用约 203 GiB（与初始 97% 对应）。已删除旧 Docker 应用、约 205 个容器和约 150 个卷、旧测试源码/配置/报告、指定遗留 systemd service/target/enablement 链接、stressctl/kache 和 TDengine 本机残留，以及 4 个约 3.9 GiB 的历史 core dump。旧清理授权仅适用于该批遗留资源。用户明确要求保留 YuXiangagent 的 3 条 root 定时任务及其文件；OS、SSH 和新实验室服务不属于旧应用。

升级启动时，应用与 sidecar 并发启动可产生少量 trace export failure，计数保留且会触发原有短时告警；不会通过清零计数或关闭告警隐藏。稳定后外部复查无活动告警，历史批次容量修复另有 194 秒的零增长证据。

本次验收未覆盖性能测试、24 小时以上 soak、饱和容量、完整磁盘耗尽、物理宿主断电、NVMe 硬件损坏、真实跨可用区及分布式 RustFS 多主机容错。三个 worker 被公共 MQ 与 CI 共享，CI 节点故障会影响公共 MQ；公共 RustFS/观测后端处于独立 VM。单宿主结果不能作为生产高可用容量承诺。
