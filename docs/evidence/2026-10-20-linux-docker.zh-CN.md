# Linux Docker 隔离部署实测记录（2026-10-20）

本轮在获授权的 `ssh dev` Linux 主机上执行真实 Docker 部署，使用同仓库 Rust SDK、独立 TideMQ 服务容器和 RustFS。修复后的同版源码 `a3f8f4e` 通过 RF1 三项、RF3 七项，共完整提交、唯一接收并持久 ACK 736 条消息（RF1 208，RF3 528）；Linux release 的九项门禁也全部退出 0。旧版 `184d56f` 的结果、正常读写失败及工具准备失败均在后文独立保留。

这是单主机上的进程、cgroup 和数据卷隔离验证。节点共享各自 Compose 项目内的 loopback 网络命名空间；没有发布宿主端口，不代表真实三区、独立网络故障域、生产负载均衡或生产 SLO 已验收。

## 同版 Docker 十项结果

下表均来自源码 `a3f8f4e9c3b6dc7a67d1611ed64452e3cdf44d44` 和同一 runtime 镜像 `sha256:e4ec405544effd8fefe8893baeb07c37bcca42b7e7a2c2aabbf674250ab5f10c`；十项的 driver 与 harness 均为 `passed`。耗时为 driver 连接、生产、消费、host 标记等待和 SDK 关闭的整个流程时间，**不能作为故障恢复 SLO**。

| 拓扑 / 场景 | 实际 case | 最终提交 / 唯一接收 / 持久 ACK / 前沿 | H | SDK retries / unknown | 流程耗时 |
| --- | --- | --- | --- | --- | --- |
| RF1 smoke | `smoke.mja3Imm6` | 64 / 64 / 64 / 64 | 0 | 0 / 0 | 1345 ms |
| RF1 replay | `replay.3C7uO6lM` | 64 / 64 / 64 / 64 | 64 | 0 / 0 | 6650 ms |
| RF1 OOM | `oom.kqCfldvS` | 80 / 80 / 80 / 80 | 0 | 4 / 3 | 21372 ms |
| RF3 smoke | `smoke.455v9B5n` | 64 / 64 / 64 / 64 | 0 | 0 / 0 | 2206 ms |
| RF3 replay | `replay.BygnsVRN` | 64 / 64 / 64 / 64 | 64 | 0 / 0 | 7915 ms |
| RF3 Storage leader kill | `storage-kill.ujOosCDk` | 80 / 80 / 80 / 80 | 0 | 0 / 1 | 52079 ms |
| RF3 active Broker kill | `broker-kill.AFbJZt5R` | 80 / 80 / 80 / 80 | 0 | 0 / 2 | 45744 ms |
| RF3 Metadata 多数派 pause | `metadata-quorum.p3WDJC39` | 80 / 80 / 80 / 80 | 0 | 2 / 0 | 44790 ms |
| RF3 Storage 多数派 pause | `storage-quorum.weYKI7tR` | 80 / 80 / 80 / 80 | 0 | 2 / 0 | 44319 ms |
| RF3 Gateway kill / restart | `gateway-kill.MpyWcNYJ` | 80 / 80 / 80 / 80 | 0 | 2 / 3 | 42557 ms |

smoke 和故障用例首轮均为 64 × 1024 B；replay 为 64 × 65536 B，生产后 settle 5000 ms 再 claim。故障用例另有原批次八条及恢复后八条。十项均核对原 logical consumer session，ACK Unknown 外层重试为零，最终 SDK pending batches、active deliveries、admitted bytes 为零，`client_close_completed=true`。SDK retries / unknown 是请求级计数器快照，不代表不同业务批次数；原 Pending 的外层重试次数在下文单列。

## 环境与制品身份

| 项目 | 实际值 |
| --- | --- |
| 主机 | Linux `7.0.0-28-generic`，x86_64，44 CPU、64 GiB 内存 |
| 容器工具 | Docker `29.9.1`，Compose `5.6.0`，cgroup v2 |
| 编译工具链 | Rust `1.96.0`，release，锁定 Cargo.lock；专用构建容器限 4 CPU、8 GiB、512 PID，Cargo `-j2` |
| 同版服务及 SDK driver 源码 | `a3f8f4e9c3b6dc7a67d1611ed64452e3cdf44d44` |
| 核心回收竞态修复提交 | `a1f2a65a6883c0e2f4629c514d1545a8ae3f579f` |
| 同版源码 archive SHA256 | `3c9db5d8fa5797baa6632e4a18b5baef51901cdd559855c0581dcad657af5300` |
| Cargo.lock SHA256 | `569937f47b344b9b500ea6f8724dc8b0afff90d7c089872dda50fb6e6b313b87` |
| builder 镜像身份 | `sha256:2e2609d9458c18b6a1d784133d2bbc36e0888235cae1f3150d3c804ae68f0557` |
| runtime 镜像名称 | `tidemq-validation:a3f8f4e-c6c399` |
| runtime 镜像 ID | `sha256:e4ec405544effd8fefe8893baeb07c37bcca42b7e7a2c2aabbf674250ab5f10c` |
| Linux `tidemq` SHA256 | `969c10150a71202b93cbabcbd0c421a35a457619f89e22d33d36ac7e7aaa6cf7` |
| Linux `deploy_validation` SHA256 | `f00119764c78e7ee78e1efdd3d64fa3771230b79c8159835ee8e4e24662bab70` |
| 分配器 | System；本轮没有比较其他 allocator，也没有变更生产选择 |
| 对象存储 | 官方 RustFS `1.0.3`，Linux musl 发行包 |
| RustFS archive SHA256 / 大小 | `29a083f156fe1a246f0336afe16ff77fffe587628e97339448d8f4fba38dba54` / 101926479 B |
| RustFS binary SHA256 | `c14fd1216f99efba9cac4bb469a5fa31ea9e461b82b79a28b69280e057613c93` |

新服务 release 构建及镜像构建均有退出码 0，构建容器 `OOMKilled=false`；driver 二进制 SHA 与旧版相同。以上服务与 driver 的源码冻结身份独立于部署脚本后续修复；证据目录名称中的较早短 SHA 不作为镜像内源码身份。

RF1 项目为 `tidemq-validation-rf1-1b323e02e76af5fc`。RF3 项目为 `tidemq-validation-rf3-58e613156cb9453b`，十个 TideMQ 角色容器的 readiness、独立数据卷、UID `10001` 与目录 `0700`、每节点 0.5 CPU / 384 MiB 限制已经实际检查。RustFS 限 1 CPU / 1 GiB；RF3 连同辅助工具的配置内存预算合计 5104 MiB。这些是容器配置上限，不是实际 RSS、容量上界或可用区隔离证明。

## 方法与验收条件

业务入口为真实 `deploy_validation` 进程，所有业务调用经 TLS QUIC 到 TideMQ。每个用例只创建一个 Client、Producer 和 Consumer，生命周期内保持这些对象及其逻辑身份。单 Producer 按最多八条组成一个批次，单 Consumer 顺序接收、核对内容并逐条持久 ACK；默认 credit 为 256 条 / 4 MiB。消息 key、正文内的运行身份、序号、长度及内容均核对，offset 必须与该单 Producer 的原始序号一致，重复或缺失消息直接失败。

普通用例总期限为 180 秒，故障用例为 360 秒，均包含 SDK 关闭与报告预算；单次请求最多十秒。每个 host 标记等待最多 120 秒。确定错误直接失败，ACK Unknown 仅对原 Delivery/token 有限重试。生产 Unknown 或明确允许重试的拒绝，仅重试 SDK 保留的原 PendingBatch，并核对 operation、producer epoch、first sequence 及 canonical digest。若错误发生在 SDK 建立 Pending 之前而拿不到唯一原批次，driver 会失败，不会重新生产来补造身份。

故障按以下顺序协同，避免用固定 sleep 猜测注入时机：

1. 首轮 64 条全部取得持久生产回执、唯一交付和逐条 ACK，权威前沿为 64，driver 发布 `waiting-fault.json`。
2. host 核实自有项目、service、镜像与故障目标后实施注入，再由 UID `10001` 创建 `fault-injected`。driver 对同一 Producer 发出下一批八条。
3. 要求不可用的用例，必须先看到 typed 可恢复失败 / Unknown，原 Pending 已保留且尚无成功回执，才发布 `fault-observed.json`；host 恢复资源并创建 `recovered` 后，driver 只恢复原批次。允许单副本故障后继续服务的用例，则必须在 `recovered` 前完成该批生产与持久 ACK，才发布观察标记。
4. 最后仍使用原 Producer / Consumer 再发送并 ACK 八条，要求 committed、unique received、acknowledged 与连续前沿全部为 80，并再次查询权威订阅状态及原 logical consumer session。

输出目录必须全新，由 driver 创建为 `0700`；JSON 有 64 KiB 上限、权限 `0600`，完整写入并同步后原子发布。公共结果文档不复制 token、证书私钥、消息正文、Delivery 签名或 WAL。

## 同版故障与对象路径证据

### 故障仍在时的成功与原 Pending 恢复

| 同版用例 | 实际目标与状态 | `fault-observed` 处回执 / 持久前沿 | 原 Pending 外层重试 | 最终权威前沿 |
| --- | --- | --- | --- | --- |
| RF1 OOM | node-1，memory/swap 384 MiB → 6 MiB，`OOMKilled=true` / 137 | 无成功回执，前沿 64 | 恢复 384 MiB 并重启后，原 batch 一次重试成功 | 64 → 72 → 80 |
| RF3 Storage kill | 实际数据 leader node-21，137，`OOMKilled=false` | 目标仍离线时已有成功回执、持久前沿 72 | 原 batch 一次重试成功 | 64 → 72 → 80 |
| RF3 Broker kill | 实际 active owner node-31，137，`OOMKilled=false` | 目标仍离线时已有成功回执、持久前沿 72 | 原 batch 两次重试成功 | 64 → 72 → 80 |
| RF3 Metadata 多数派 pause | node-11 / node-12 均 `Paused=true` | 无成功回执，前沿 64，`Unavailable / Rejected / retryable` | unpause 后原 batch 一次重试成功 | 64 → 72 → 80 |
| RF3 Storage 多数派 pause | node-21 / node-22 均 `Paused=true` | 无成功回执，前沿 64，`Unavailable / Rejected / retryable` | unpause 后原 batch 一次重试成功 | 64 → 72 → 80 |
| RF3 Gateway kill | node-41，137，`OOMKilled=false` | 无成功回执，前沿 64，transport `ProduceUnknown` | 同端点 Gateway 重启后原 batch 一次重试成功 | 64 → 72 → 80 |

六项首次故障批次都是 epoch 1 / first sequence 64 / 八条消息，`fault-observed` 与最终报告中的原 operation、epoch、sequence、canonical digest 保持一致。OOM 与 Gateway 首次得到 transport 来源的 `ProduceUnknown`；两项多数派 pause 是明确允许重试的 `Unavailable / Rejected`，并非 Unknown。四项的观察标记均明确 `receipt_seen=false`、前沿 64，不能把“不曾得到成功回执”混写为所有 Unknown 都已证明未提交。Storage 单节点及 Broker 两项首次得到 remote `Unavailable / Unknown`，在恢复原目标之前已经核对八条完整内容并逐条持久 ACK，观察标记为 `receipt_seen=true`、前沿 72。整个进程没有替换 Producer、Consumer 或用新生产补造原批次。

Storage 的注入前 diagnostics 显示 group 2 的 leader=21 / term=1；后续 node-22、node-23 都显示 leader=22 / term=2。Broker 的三个 Metadata 节点注入前本地 applied 视图均为 active owner 31 / epoch 1 / route generation 1，注入后均为 active owner 32 / epoch 2 / route generation 2。这确认实际命中了 leader / active owner，而不是仅杀到闲置角色；owner HTTP 视图的 `source=local_applied`，不能把单次该 GET 自身称为线性一致读或独立的 quorum 持久证明。

Gateway 用例强杀后重启的是原 node-41，driver 仍连接 `127.0.0.1:7810` / `node-41.local`。它验证固定地址冷重启后的原 SDK 逻辑身份恢复，不是自动切到 node-42，也没有经过负载均衡器；旧 QUIC 连接或 stream 不会原样跨进程存续。

before / after 指标、diagnostics 和容器状态均严格校验通过。during 是有限预算内的故障现场采集，不能写成全量成功：两项多数派 pause 分别保留六个、五个非空采样 error 文件，包含两秒 HTTP 超时及采集预算不足；Gateway 用例有两个管理端口拒连 error 文件。这些缺口与业务 ACK 账本分别记录，没有隐藏、补零或放宽原预算。

### 同版归档回放的阶段指标

每个 replay 的正文加八字节 key 共 4194816 B。两项均 H=64、全 64 条内容与持久 ACK 正确；阶段前后原值为：

| 拓扑 / Storage | published | reclaimed bytes | S3 cache misses |
| --- | --- | --- | --- |
| RF1 node-1 | 1 → 3（+2） | 66048 → 4260864（+4194816 B） | 1 → 3（+2） |
| RF3 node-21 | 1 → 2（+1） | 66048 → 4260864（+4194816 B） | 1 → 2（+1） |
| RF3 node-22 | 0 → 0 | 0 → 0 | 0 → 0 |
| RF3 node-23 | 0 → 0 | 0 → 0 | 0 → 0 |

对应指标为 `tidemq_lifecycle_published_total`、`tidemq_lifecycle_reclaimed_bytes_total`、`tidemq_s3_cache_misses_total`。它们共同支持实际对象发布、本地字节回收及对象冷范围缓存 miss 后的完整读取；不证明宿主或 RustFS 的 OS page cache 已冷，也不构成物理冷盘吞吐测试。

新增 `tidemq_runtime_read_hot_reclaim_conflicts_total` 在前七项的 86 份 before / after 进程指标快照中均为零；后三项另有实际非零观测：Metadata 多数派用例的 node-22 为 0 → 1，Gateway 用例的 node-23 为 0 → 1，且两项完整业务账本通过。node-22 后续快照延续为 1 → 1，不能把重复采样的值相加当作新的冲突次数。进程重启还会重置计数，不从阶段快照推导所有中间时刻的准确次数；精确竞态边界另由下文确定性 S3 / reclaim 红绿测试验证。

### 同版资源与退出状态

实际 RF3 配置记录确认十个 TideMQ 节点均运行新 ImageID，每节点 memory / memory-swap 均为 402653184 B，NanoCpus 为 500000000，十个宿主 PID 不同，所有节点复用同一个项目内 holder 的 netns，没有宿主 PortBindings。RF1 完成三项后已 stop：node-1 与 RustFS 退出 0，network holder 退出 137 / `OOMKilled=false`，三者均 `Running=false` / PID=0；单独保留 helper 的退出状态，不将它当作 OOM 或全部容器退出 0。

本轮最终审计时 RF3 按安排继续运行：十个 TideMQ 角色、RustFS 和 holder 共十二个容器 `Running=true`，其中十一个配置了健康探针且均 healthy，holder 无健康探针，没有 unhealthy。节点的实际 User 为 `10001:10001`，rootfs 只读，CapDrop=ALL，`no-new-privileges=true`，日志驱动为 `local`、`max-size=10m`、`max-file=3`。数据卷与 keys 留在远端，没有复制到公共文档。该终态不能写成所有测试服务都已停止。

既有十七个业务容器的部署前 / 最终快照按 Id、Name、Running、StartedAt、RestartCount 完整 `cmp` 一致，审计摘要与原值均保留；这里只陈述这些观测字段前后一致，不扩大为其他业务数据或宿主状态的全面证明。

## 同版 Linux release 门禁

源码 `a3f8f4e` 的 Linux release 门禁已有以下终态。普通测试和额外执行的 ignored / example / feature 结果分别列出，避免把过滤重跑或边界用例重复相加：

| 检查 | 同版 Linux 结果 |
| --- | --- |
| fmt、workspace clippy、严格生产审计 | 均退出 0 |
| workspace 普通 release tests | 首个 `Doc-tests` 前 30 个终态 suites：246 passed / 0 failed / 11 ignored；workspace 命令退出 0 |
| workspace 文档测试 | 3 passed：根 crate 两个 compile-fail，SDK 一个可编译示例 |
| `deploy_validation` example | 6 passed |
| S3 / lifecycle / compaction | 三个 suite 分别 1 / 3 / 1 passed，共五项真实 RustFS |
| runtime-S3 | 3 passed，21.05 s |
| reclaim 边界 | 4 passed，5.98 s；其中两项普通边界已计入 246，另两项是真实 RustFS ignored |
| SDK `metrics opentelemetry` feature 门禁 | 普通测试 25 passed（lib 1 + QUIC network 24），另 SDK 文档 1 passed；命令退出 0 |

显式执行的真实 RustFS ignored 合计十项（5 + 3 + 2）全部通过；普通回归中的另一个官方 Collector ignored 未在本轮 Linux 执行，不能写成十一项 ignored 全部执行通过。workspace 此次 Linux 串行运行中的 RF3 drain 与 tracing 用例均通过；这不抹去后文独立保留的 macOS 历史红结果。下文旧版 244 / 9 与八项 RustFS 成功仍归于原 `184d56f` 身份。

SDK feature 命令的 25 项是普通测试数，不能写成 26 项普通测试；另外的 SDK 文档一项与 workspace 文档三项中的 SDK 示例重叠，不相加为新的独立覆盖。`fmt / clippy / strict / workspace / driver / s3 / runtime-s3 / reclaim / sdk-observability` 九个 `.exit` 均为 0，门禁 builder 的最终状态为 exited / exit 0 / `OOMKilled=false` / PID=0，专用 builder 运行清单为空。所有门禁完成后，既有十七个业务容器的指定快照字段再次与 predeploy 完整 `cmp` 一致。

## 历史：旧版 RF1 结果

以下记录属于原始源码 `184d56fa9b749f684e55675ae090a60f45a1013f`，镜像 tag `tidemq-validation:184d56f-c6c399` / ID `sha256:85491156fe1cc0288ff341eedaa44a03159fdb9e885081871631c36a062e5fa9`。旧 Linux 服务 SHA256 为 `c21afbb51d4da0fbd4adff8d08e38b2ecd844fc2c0456a2c97d8a97e7f339343`，driver SHA 与上方相同；旧目录、镜像和失败证据均保留，没有用新结果覆盖。

三个用例的 `harness-report.json`、driver `report.json` 均为 `passed`，validator 正常退出。各表中的耗时是 driver 从连接至关闭的整个流程时间，包含 settle 或 host 故障协调等待，不是吞吐或故障恢复 SLO。

| 用例 | 首轮负载 | 最终提交 / 唯一接收 / 持久 ACK / 前沿 | 初始 H | driver 总耗时 | SDK retries / unknown | 结果 |
| --- | --- | --- | --- | --- | --- | --- |
| smoke | 64 × 1024 B | 64 / 64 / 64 / 64 | 0 | 1280 ms | 0 / 0 | 通过 |
| replay | 64 × 65536 B，生产后 settle 5000 ms 再 claim | 64 / 64 / 64 / 64 | 64 | 6581 ms | 0 / 0 | 通过 |
| OOM | 64 × 1024 B；故障批八条、恢复后再八条 | 80 / 80 / 80 / 80 | 0 | 21373 ms | 4 / 3 | 通过 |

三项 `same_consumer_session_verified` 和 `client_close_completed` 均为 true，ACK Unknown 的外层重试次数均为零；最终 SDK `pending_batches`、`active_deliveries`、`admitted_bytes` 均为零。SDK retries / unknown 是其请求计数器快照，不代表不同业务批次数量。

### 归档、回收与回放

replay 首轮 64 条的正文为 4 MiB，每条另含八字节 key；实际正文加 key 共 4194816 B。生产后等待五秒再 claim，初始 H 为 64，最终完整交付和持久 ACK 均为 64。同一节点的阶段前后指标为：

| 实际指标 | before | after | 增量 |
| --- | --- | --- | --- |
| `tidemq_lifecycle_published_total` | 1 | 2 | 1 |
| `tidemq_lifecycle_reclaimed_bytes_total` | 66048 | 4260864 | 4194816 B |
| `tidemq_s3_cache_misses_total` | 1 | 2 | 1 |

这些观测共同支持本轮发生对象发布、本地消息字节回收及对象冷范围缓存 miss 后的完整读取。它们不证明宿主磁盘或 RustFS 的 OS page cache 已冷，也不构成对象存储断电持久性、规模化吞吐或竞品比较。

### 真实 OOM 与原身份恢复

故障目标为 RF1 的自有 `node-1` 容器。host 将 memory / memory-swap 从 `402653184 / 402653184` B（384 MiB）降到 `6291456 / 6291456` B（6 MiB）。Docker 保存状态为 `OOMKilled=true`、`Running=false`、`ExitCode=137`。这与单独发送 SIGKILL 的退出码观测不同，本轮另有实际内存上限变更及 OOM 标志。

driver 在前沿 64 处对下一批八条首次得到 `ProduceUnknown`，source 为 transport；`fault-observed.json` 明确记录 `receipt_seen=false`，保留原 epoch 1 / first sequence 64、原 operation 与匹配的 canonical digest。host 恢复 384 MiB 限额并重启该容器后，driver 对同一个 PendingBatch 外层重试一次取得成功；随后用原 Consumer 完成该八条，再生产并 ACK 最后八条。权威前沿依次为 64、72、80，最终完整账本为 80。

恢复后的 `memory.max` 为 `402653184` B，容器重新运行。采样中的重启后 `memory.events` 为新生命周期计数，不能拿它的零值否定此前 OOM；本轮 OOM 判断依据为降额记录与保存的 Docker OOMKilled / 137 状态。服务在用例后由 host 停止，数据卷与私有证据保留。

## 历史：旧版 RF3 与 Linux release 回归

以下为原始源码 `184d56f`、相同初始 runtime 镜像的实际结果：

| RF3 尝试 | 实际终态 | 提交 / 接收 / ACK / 前沿 | driver 耗时 |
| --- | --- | --- | --- |
| `smoke.aygAsaqq` | 通过，64 × 1024 B，H=0 | 64 / 64 / 64 / 64 | 2247 ms |
| `replay.rDrhJtPa` | 前置采集失败，validator `not_started` | 无业务结果 | 未启动 |
| `replay.nCD7BNyM` | 通过，64 × 65536 B，settle 5000 ms，H=64 | 64 / 64 / 64 / 64 | 8044 ms |
| `storage-kill.qD4mNELn` | 首轮正常消费失败，未注入故障 | 64 / 22 / 22 / 22 | 1160 ms |

两个成功用例的 SDK retries / unknown 与 ACK Unknown 外层重试均为零，最终 pending、active deliveries 和 admitted bytes 均为零，SDK close 完成。首次 replay 尝试在原 30 秒前置采集预算结束时缺少最后一个 `node-42` diagnostics 而失败，driver 尚未启动，所有节点仍 healthy。该失败完整保留；它是工具前置采集失败，没有业务消息或 S3 读写结果，不能记作业务失败或通过。

harness 在保持原预算和完整采集验收的前提下，缓存已经验证的 holder container ID，减少每次 HTTP 采样重复执行 Compose ps 的开销；随后 `replay.nCD7BNyM` 成功。该用例三个 Storage 节点的 before / after 阶段指标如下：

| 节点 | published | reclaimed bytes | S3 cache misses |
| --- | --- | --- | --- |
| node-21 | 1 → 3（+2） | 66048 → 3736512（+3670464 B） | 1 → 3（+2） |
| node-22 | 0 → 0 | 0 → 0 | 0 → 0 |
| node-23 | 0 → 0 | 0 → 0 | 0 → 0 |

node-21 的对象发布、本地回收和缓存 miss 增量支持本轮实际经过对象读取路径。回收增量没有覆盖本轮全部 4194816 B 正文加 key，不能声称所有消息都从已完全冷却的磁盘读取；H=64、完整交付 64 与对象 miss 是各自不同的观测。

RF3 首次 Storage kill 尝试在故障注入前的首轮读写已失败：64 条生产全部提交，持久 ACK 为 22 条，随后返回确定错误 `OffsetExpired / Rejected`，`retry_allowed=false`，SDK retries / unknown 均为零，driver 未吞掉或重试该确定错误。此时尚未执行 Storage 故障注入，所有节点仍健康。因此这是正常业务路径的失败证据，不能当作故障恢复成功，也不能归因于尚未发生的强杀。

问题定位到 hot clone 与 RecordDelivery 之间的本地回收竞态。真实 S3 / reclaim seam 的确定性测试先复现首条 offset 0 及已有成功前缀后的 offset 1 两种失败，均保留 local floor 8 / logical floor 0，证明本地热数据消失不能直接等同于消息已经越过逻辑保留前沿。修复后的这两项真实 S3 测试和两项普通边界测试已通过；新镜像已通过上方十项容器验收，同版 Linux workspace 与真实 RustFS 结果见上方独立表格。

### 回收竞态修复与定向边界

修复将 hot 签发时的本地回收冲突表示为内部 typed `NoEffect`，并再次核对逻辑 floor。若已经形成成功前缀，只返回确实持久签发的部分；首条尚无前缀时，最多重取一次，保留原 operation、attempt、deadline、工作 slice 与 S3 proof。Unknown 仍使整批返回错误，不能用成功前缀吞掉不确定结果。新增固定、无 label 的计数器为 `tidemq_runtime_read_hot_reclaim_conflicts_total`，用于区分该竞态与真实逻辑过期。

修复后的 Mac 定向验证中，新增四项边界全部通过；真实新增 S3 两项加既有 runtime-S3 三项共五项通过。这些是修复边界的定向证据，独立于上方原始 `184d56f` 的 Linux 测试计数。Mac 完整 runtime 的那次记录为十项中九项通过，多阶段 drain 返回 `Unavailable`，具体失败 stage 仍未定位，不能写成该次完整套件已通过。

临时诊断的三个源码文件改动已经撤回，核心修复分支回到干净 `a1f2a65`；私有诊断 patch、summary 和日志保留在本轮制品的 `regression-macos/`。Linux 随后在冻结 `a3f8f4e` 上以串行配置完成完整 workspace，RF3 drain 与 tracing 均通过；两种平台的事实分别保留，不用 Linux 成功替代或杜撰 macOS 红结果的根因。

Linux 源码 `184d56f` 的已核对回归终态如下：

| 检查 | 当前已确认状态 |
| --- | --- |
| workspace 普通 clippy 与严格审计 | 已终态通过 |
| workspace 普通 release Rust tests | 首个 `Doc-tests` 之前 30 个终态 suites：244 passed / 0 failed / 9 ignored |
| `deploy_validation` example 的纯边界测试 | 另行执行 6 passed，退出 0 |
| workspace 文档测试 | 最初退出 101；仅调整临时目录后，三个文档测试全部通过 |
| 八个真实 RustFS ignored 测试 | 修正可写 target 挂载后全部通过，四个命令及容器均退出 0，容器 OOMKilled=false |

244 / 9 是对原始 `linux-gates.log` 首次文档测试之前的分段统计；该原命令后来因文档测试环境问题整体退出 101，不能将该整体退出码写为 0。文档测试和 example 的另外执行结果独立列出；此前 macOS 的 246 项属于不同平台/执行记录，不与这里相加。普通回归中的九个 ignored 也不直接算作已执行；本节另行确认的真实对象存储用例为八项：

| 实际 RustFS target | passed | 终态测试耗时 |
| --- | --- | --- |
| S3 | 1 | 2.56 s |
| compaction | 1 | 1.48 s |
| lifecycle | 3 | 3.34 s |
| runtime-S3 | 3 | 20.12 s |

文档测试的原始失败来自 Docker `/tmp` 为 noexec：同一可执行文件放在 `/tmp` 时退出 126，放在 `/build` 时能运行。仅设置 `TMPDIR=/build/doctest-tmp` 后三个文档测试通过，未修改业务源码、测试契约或期限；原失败与修正后的成功日志都保留。

RustFS 测试 fixture 将临时文件写入 `current_dir/target`，初始容器的源码挂载为 `/work:ro`，因而在准备阶段遇到 EROFS。修正方式是在继续只读挂载源码的同时，把同一个自有构建目录分别 bind 到 `/build` 和 `/work/target`，并设置 `TMPDIR=/build/doctest-tmp`；没有产品源码改动。原 EROFS 记录属于测试容器准备失败，修正后的八项产品测试另有真实运行及成功终态，失败日志没有被覆盖。

## 实测发现与部署脚本修复

- Compose 的 container network 模式与 `extra_hosts` 不兼容，部署脚本在 `1bece06` 移除该组合；这是同项目共享 loopback netns 的适配，不是跨节点网络验证。
- 实际 Bookworm curl 的 SigV4 请求未自动带上 `x-amz-content-sha256`，创建 RustFS bucket 返回 HTTP 400。`afe4291` 显式传入空正文 SHA256 后，在同一 RustFS 实例成功创建 bucket。该修复针对部署初始化请求，未替换 SDK 的 `aws-sdk-s3` 业务路径。
- harness 固定 validator name 与 case labels，并在动作前校验 exact Compose project、service、image，限制操作范围为本轮自有容器。Linux 上的 GNU timeout 进程组自检已经通过；业务 SDK 的原期限、消息身份及持久 ACK 契约未放宽。

## 证据与边界

远端私有根目录为 `/root/tidemq-validation/tidemq-yyx-20261020-4354e95-c6c399`。RF1 原始制品已下载至集成 worktree 的私有 `target/tidemq-yyx-20261020-4354e95-c6c399/rf1/evidence/`，分别保留 `smoke.H1PFlUAZ`、`replay.Zc3zirOt`、`oom.Xb4nq2iI`。其中包含 driver/harness 报告、阶段指标与 diagnostics、容器状态、OOM 限额记录和日志；本提交仅包含脱敏中文结果，不提交这些原始制品。

初始 Linux / RF3 结果归档 `linux-rf3-pre-fix-results.tgz` 的实际 SHA256 为 `be725320220b84f9436e8c5fed15358e620125a3fc7cde4ac43fbe27fdf4a160`，已在本地复核。解压后 `rf3/evidence/` 保留上述成功与失败尝试，`evidence/` 保留 gates / docs / noexec probe / 容器配置与内部网络检查，`build/linux-followup/` 和 `build/linux-rustfs-writable/` 分别保留原准备失败及修正后八项成功日志与退出码。

修复后首七项归档 `fixed-first-seven-results.tgz` 的实际 SHA256 为 `3b54e017decc927c7920728131c25ef6f4c825345a7b1e08ce382ecd16b0089f`，同样已本地复核。它保留新 case 的 report / markers / 前后指标、Storage leader 与 Broker owner 视图、故障目标退出状态，新构建的 source / binary / image 身份，以及 RF1 stop 与 RF3 实际容器配置。

完整同版 Docker 十项归档 `fixed-docker-matrix-results.tgz` 的实际 SHA256 为 `980264603b555c43f426110a428fbbc0ddbe2760643f0070c24db84ab07e85a3`，已本地复核。它另包含后三项 markers / 原 Pending / 最终账本、during 采样错误、最终 RF3 容器及内部网络配置、RF1 stop 状态、十七个既有业务容器的前后快照与审计摘要。RF3 继续运行，不能从该归档标题推导为全部服务已清理。

最终 Linux 门禁归档 `fixed-linux-gates-results.tgz` 的实际 SHA256 为 `59f2e185b26eb5823075170d2b7985a5b686841011961c89553c0b491884889b`，已本地复核。`build/linux-a3f8f4e/` 保留完整九项日志和退出码，`evidence/fixed-linux-gates.*` 保留总命令与 builder 终态，另有专用 builder 运行清单为空及门禁后十七个业务容器再次前后相同的记录。所有测试数量均按这些终态日志分段复核，不按中间进度或含 compile-fail 预期编译错误的单行日志判断成功与失败。

本轮是有限消息数、单 Producer 与单 Consumer、System allocator 的部署和故障正确性验证。没有重复统计、并发饱和实验或延迟直方图，不能用这些秒级流程时间推断 P99 / P99.9、高吞吐低延迟目标或生产 allocator 选择。此前独立冻结的 17 项 macOS RF1 基线见[本地性能报告](2026-10-20-local-performance.zh-CN.md)，其源码、主机和二进制身份与本轮不同，不合并数值排名。

真实三区、独立网络分区与交换机故障、生产 LB/NAT/CID、宿主掉电、长期稳定性、容量极限及竞争产品公平对照仍不在本轮已验收范围。
