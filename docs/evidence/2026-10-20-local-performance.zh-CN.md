# 2026-10-20 本地 RF1 性能基线（17 项）

本轮完成同一冻结源码的 17 项有限负载，测量阶段共 36,032 条消息持久提交并逐消息 ACK，另有独立预热 2,264 条，合计 38,296 条；每个 topic 的持久 ACK 前沿均等于对应消息数。17 份 `report.status` 均为 `passed`，17 份编排 `execution.exit_status` 均为 0。

这组结果只建立本地 RF1 正确性与性能基线，高吞吐、低延迟目标尚未达成。首项 System-small 的生产吞吐为 **1828.89 条/秒**，单消费者的消费加持久 ACK 吞吐仅 **60.30 条/秒**；offered-to-durable-ACK 的 P50 上界为 **34.360 秒**、P99 上界为 **65.498 秒**。生产回执速度、单次 RPC 延迟与业务完成延迟应分别评估。

## 环境与不可变构建身份

- 测量源码：`1398353c5bd473bbaaf9c2f89d34d30c80fae0c2`，构建和整个测量期间 tracked source 均干净；本报告在全部测量结束后编写。
- Cargo.lock SHA-256：`569937f47b344b9b500ea6f8724dc8b0afff90d7c089872dda50fb6e6b313b87`。
- 主机：Apple M2 Max，12 个物理/逻辑核心，64 GiB 内存；macOS 26.6.2（25G83），Darwin arm64。
- 工具链：`rustc 1.96.0 (ac68faa20 2026-05-25)`，release（optimized + debuginfo），`--locked --no-default-features`，分别选择唯一 allocator feature，构建 `-j2`。
- 服务为独立 `tidemq serve` 操作系统进程；SDK 和负载工具来自当前仓库。负载工具固定使用 System allocator，服务才使用候选 global allocator。

实际执行 `scripts/perf-build.sh target/perf-candidates-20261020-1398353 all`。共享 `target/perf-build` 只复用依赖编译缓存，三种服务与负载工具均按新源码实际重建、复制并重新计算哈希；旧冻结候选没有冒充本轮制品。

| feature / 构建耗时 | 服务 SHA-256 | 负载工具 SHA-256 |
| --- | --- | --- |
| allocator-system / 1m38s | `5409dc7ca4428da12aaa45d4a13968819250f43a6c20913b2d987d0ee2dbd22b` | `e65098ea7c2cc58956034a1495b06cb39e61e69bb5f37b7f7fc253dc8112174b` |
| allocator-mimalloc / 1m32s | `267b67f69f797aeb369ac6026037f8e1dcb384424800606da89ad7276cb54fb0` | `7574f26887f2c1012f8326dbdfba77fe401303a8c49ebe4cfaebe4692ccd9041` |
| allocator-jemalloc / 1m37s | `6ff9b52508eb4ab454484de15119deab04ebe7598a65ada6f8d3e8bddbf45720` | `9150d2712c9c267930d403652cbc04aeaa4ef421a1589ce1aad249eb70765c98` |

所有初始诊断、周期诊断与最终诊断中的 allocator backend 均与 feature 相符。System 的原生版本及详细分配器统计为不可用/null；mimalloc 原生探针返回 `30302`；jemalloc 返回 `5.3.1-0-g81034ce1f1373e37dc865038e1bc8eeecf559ce8`。这些是实际探针值，不由 crate 版本推断。

## 执行、负载与测量口径

全部构建退出后，全局进程检查未发现 Cargo/rustc、TideMQ、负载工具、RustFS 或 Collector，固定测试端口空闲；随后实际静置 30 秒，静置不计入样本。其他任务在整个矩阵期间没有编译、测试或启动服务。本轮使用同一桌面主机，未控制所有宿主后台活动、温度、电源状态或 CPU 绑定。

执行层先运行 System-small，核对成功后串行运行剩余 16 项，任一 case 失败立即停止。这两份私有 `run-first.sh`/`run-remaining.sh` 调用的场景、参数和顺序与 `perf-matrix.sh` 的 17 项一致；它们采用 fail-fast 调度，不能称为逐字执行原会继续收集失败的矩阵脚本。跟踪的构建、负载、门禁脚本未作本轮临时修改。

| 场景 | 测量 / 预热消息数 | 正文 + key | 生产 batch | 额外条件 |
| --- | ---: | --- | ---: | --- |
| small | 4096 / 256 | 128 B + 8 B | 32 | live 路径 |
| large | 64 / 8 | 262136 B + 8 B | 1 | 每条逻辑消息恰为 256 KiB |
| slow | 2048 / 128 | 128 B + 8 B | 32 | 每个 ACK 批次前等待 20 ms |
| replay | 256 / 16 | 65536 B + 8 B | 4 | 生产后等待 5 s，再 claim；必须实际增加 S3 cache miss |

每个 case 使用独立服务、磁盘目录和新 topic/producer/consumer 身份；预热与测量使用不同 topic。四个 worker **仅用于生产**，消费只有一个 Consumer，顺序 fill、处理和批量 ACK，不能解释为四个并发消费者。默认 consumer credit 为 256 条/4 MiB，ACK 批量上限 32。控制路径每约 50 ms 发起真实 GET_TOPIC。

服务与负载工具各固定四个 Tokio worker，清空继承环境并仅保留显式 PATH 和服务线程设置。服务应用准入预算为 192 MiB、Core 保留 32 MiB。每阶段原总期限为 120 s、请求期限为 10 s、冷却 3 s；原 100 次生产结束后空读门禁和 30 s 可见性期限保持不变。持久 WAL/quorum 回执、逐消息 ACK、原身份恢复契约均保留。

这是有限闭环负载：服务变慢会降低 offered load；单消费者串行操作进一步限制消费压力，没有 coordinated omission 校正、饱和点搜索、长稳态或排队上界证明。测量阶段起点范围为北京时间 2026-10-20 03:12:31.516—03:25:21.856，按上表顺序执行。每次新建 topic/producer 的哈希 affinity 不同，顺序、页缓存和 fsync 噪声会影响对照。

## 吞吐、时间与服务资源

以下均为 measured，预热不混入。生产吞吐使用实际持久提交数/生产阶段时间；消费吞吐使用实际 ACK 数/消费加 ACK 时间，replay 的 5 s settle 不计入消费时间。MiB/s 为逻辑正文加 key 字节率。CPU 是目标服务 PID 的成功 OS 样本首尾累计 CPU 差；RSS 是 1 Hz 采样最大值。CPU/RSS 采样区间覆盖测量过程，replay 还覆盖生产及 settle，不能将 CPU 差除以下表消费时间后当稳定利用率。

| # | 分配器 / 场景 / 观测 / lanes | 提交=ACK=前沿 | 生产 msg/s（MiB/s） | 消费+ACK msg/s（MiB/s） | 消费+ACK s | CPU差 s | 采样RSS最大 MiB / 点数 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | system / small / recommended / 4 | 4096 | 1828.89（0.237） | 60.30（0.008） | 67.926 | 6.930 | 55.88 / 69 |
| 2 | system / large / recommended / 4 | 64 | 53.87（13.468） | 22.48（5.620） | 2.847 | 0.720 | 67.84 / 4 |
| 3 | system / slow / recommended / 4 | 2048 | 1887.01（0.245） | 55.53（0.007） | 36.880 | 3.540 | 41.39 / 38 |
| 4 | system / replay / recommended / 4 | 256 | 242.49（15.158） | 46.07（2.880） | 5.557 | 1.760 | 140.67 / 13 |
| 5 | mimalloc / small / recommended / 4 | 4096 | 1888.73（0.245） | 60.27（0.008） | 67.960 | 7.080 | 60.14 / 69 |
| 6 | mimalloc / large / recommended / 4 | 64 | 56.07（14.017） | 23.26（5.816） | 2.751 | 0.630 | 81.30 / 4 |
| 7 | mimalloc / slow / recommended / 4 | 2048 | 1896.08（0.246） | 55.97（0.007） | 36.594 | 3.560 | 46.22 / 38 |
| 8 | mimalloc / replay / recommended / 4 | 256 | 205.75（12.861） | 45.86（2.866） | 5.583 | 1.650 | 166.25 / 13 |
| 9 | jemalloc / small / recommended / 4 | 4096 | 1823.71（0.237） | 59.75（0.008） | 68.556 | 7.040 | 61.08 / 70 |
| 10 | jemalloc / large / recommended / 4 | 64 | 52.30（13.076） | 23.83（5.957） | 2.686 | 0.570 | 80.72 / 4 |
| 11 | jemalloc / slow / recommended / 4 | 2048 | 1896.04（0.246） | 55.38（0.007） | 36.983 | 3.630 | 46.78 / 38 |
| 12 | jemalloc / replay / recommended / 4 | 256 | 218.28（13.644） | 46.35（2.897） | 5.524 | 1.600 | 119.92 / 13 |
| 13 | system / small / minimal / 4 | 4096 | 1843.84（0.239） | 60.60（0.008） | 67.588 | 6.920 | 55.70 / 69 |
| 14 | system / small / detailed / 4 | 4096 | 1860.76（0.241） | 58.84（0.008） | 69.608 | 8.550 | 56.20 / 71 |
| 15 | system / small / recommended / 2 | 4096 | 1773.34（0.230） | 60.32（0.008） | 67.910 | 7.120 | 55.94 / 69 |
| 16 | system / replay / recommended / 2 | 256 | 234.02（14.628） | 41.75（2.610） | 6.131 | 1.640 | 141.22 / 14 |
| 17 | system / small / recommended-otlp / 4 | 4096 | 1827.47（0.237） | 60.66（0.008） | 67.521 | 7.390 | 57.39 / 69 |

各 case 的最终 frontier 与上表 N 一致；预热总计 2,264 条亦全部提交、ACK 并达到对应 frontier。所有预热/测量的 terminal_error、控制错误和资源采样错误均为零/null；测量阶段 SDK retries、SDK Unknown、transport deadline、liveness failure、packet-loss 通知与拒绝增量均为零。语义校验没有丢弃失败消息后仅统计成功吞吐。本轮没有触发生产或 ACK Unknown 的原身份恢复，不能由零 Unknown 推导故障恢复预算已达成；该能力需结合独立故障回归评估。

## 延迟、实际样本数与分位限制

每格为 **P50 / P99 [实际样本数 N]**，单位 ms。直方图保留全部成功样本，固定 2048 槽，采用 log2 每级 32 子槽的上界，最大相对槽宽 3.125%；这些数不是精确排序分位。P99 要求至少 1000 个样本，P99.9 要求至少 10000 个样本。本轮最大 N=4096，**所有 P99.9 均为 null**，所有未达门槛的 P99 也均为 null，不以 0 替代。

| # | Produce batch | Receive call | ACK batch | Control query | Offered→durable ACK |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 71.303 / null [128] | 276.824 / null [233] | 15.466 / null [232] | 0.852 / 1.638 [1359] | 34359.738 / 65498.251 [4096] |
| 2 | 73.400 / null [64] | 75.497 / null [24] | 16.253 / null [22] | 0.352 / null [57] | 1409.286 / null [64] |
| 3 | 71.303 / null [64] | 276.824 / null [118] | 16.253 / null [117] | 0.819 / null [738] | 19327.353 / 35849.557 [2048] |
| 4 | 66.060 / null [64] | 276.824 / null [21] | 14.156 / null [21] | 0.868 / null [233] | 8858.370 / null [256] |
| 5 | 69.206 / null [128] | 276.824 / null [233] | 15.991 / null [232] | 1.180 / 3.015 [1360] | 34359.738 / 65498.251 [4096] |
| 6 | 69.206 / null [64] | 73.400 / null [24] | 15.204 / null [22] | 0.295 / null [56] | 1342.177 / null [64] |
| 7 | 71.303 / null [64] | 276.824 / null [117] | 16.515 / null [116] | 0.819 / null [732] | 18790.482 / 35433.480 [2048] |
| 8 | 75.497 / null [64] | 276.824 / null [21] | 14.942 / null [21] | 0.918 / null [237] | 8858.370 / null [256] |
| 9 | 71.303 / null [128] | 276.824 / null [235] | 15.991 / null [234] | 0.885 / 3.211 [1372] | 34359.738 / 66337.518 [4096] |
| 10 | 73.400 / null [64] | 66.060 / null [24] | 14.156 / null [22] | 0.242 / null [54] | 1308.623 / null [64] |
| 11 | 69.206 / null [64] | 276.824 / null [118] | 16.515 / null [117] | 0.868 / null [740] | 18790.482 / 35958.647 [2048] |
| 12 | 71.303 / null [64] | 276.824 / null [21] | 14.156 / null [21] | 0.606 / null [235] | 8858.370 / null [256] |
| 13 | 71.303 / null [128] | 276.824 / null [233] | 15.466 / null [232] | 0.868 / 2.884 [1352] | 34359.738 / 65435.515 [4096] |
| 14 | 71.303 / null [128] | 276.824 / null [238] | 15.991 / null [237] | 0.934 / 1.835 [1393] | 35433.480 / 67477.478 [4096] |
| 15 | 73.400 / null [128] | 276.824 / null [233] | 15.729 / null [232] | 0.950 / 2.884 [1359] | 34359.738 / 65498.251 [4096] |
| 16 | 66.060 / null [64] | 293.601 / null [21] | 14.418 / null [21] | 0.623 / null [245] | 9395.241 / null [256] |
| 17 | 71.303 / null [128] | 276.824 / null [232] | 15.466 / null [231] | 0.868 / 1.737 [1350] | 34359.738 / 65348.525 [4096] |

`Receive call` 是每次 recv 调用耗时（含空结果），后续 next 从队列取消息不逐条追加该样本；它不是逐消息网络时延。ACK batch 为同一有界 token 批次至持久结果。Offered→durable ACK 按每条消息计数，含积压、读取和 slow 人为处理等待；replay 还会体现生产、settle 与消费之前的等待。控制查询与 ACK 独立计时。

#1 的 GET_TOPIC P50/P99 为 0.852/1.638 ms，ACK batch P50 为 15.466 ms，但业务 ACK 完成的 P50 达到 34.360 s。不能用低控制延迟或较快生产回执遮盖消费端积压。

## 分配器、观测与 Request lane 对照

下表为上表原值的单次算术相对差，公式为 `(对照/参考−1)×100%`。它不是显著性检验、稳定开销估计或因果排序。CPU 差取不同长度采样区间，短场景尤其易受分辨率与边界影响。

| 对照# / 参考# | 生产吞吐差 | 消费+ACK吞吐差 | 采样RSS差 | 采样CPU差 |
| --- | ---: | ---: | ---: | ---: |
| 5 / 1 | +3.3% | -0.1% | +7.6% | +2.2% |
| 6 / 2 | +4.1% | +3.5% | +19.8% | -12.5% |
| 7 / 3 | +0.5% | +0.8% | +11.7% | +0.6% |
| 8 / 4 | -15.2% | -0.5% | +18.2% | -6.3% |
| 9 / 1 | -0.3% | -0.9% | +9.3% | +1.6% |
| 10 / 2 | -2.9% | +6.0% | +19.0% | -20.8% |
| 11 / 3 | +0.5% | -0.3% | +13.0% | +2.5% |
| 12 / 4 | -10.0% | +0.6% | -14.8% | -9.1% |
| 13 / 1 | +0.8% | +0.5% | -0.3% | -0.1% |
| 14 / 1 | +1.7% | -2.4% | +0.6% | +23.4% |
| 15 / 1 | -3.0% | +0.0% | +0.1% | +2.7% |
| 16 / 4 | -3.5% | -9.4% | +0.4% | -6.8% |
| 17 / 1 | -0.1% | +0.6% | +2.7% | +6.6% |

三种 allocator 的 small 消费加 ACK 约为 60.30/60.27/59.75 条/秒。本轮不据此选择生产 allocator；固定顺序、新身份哈希、磁盘与 OS 缓存、短时 RSS 及缺少重复/交错运行均不足以支持排名。详细日志档的采样 CPU 比参考高 23.4%、消费吞吐低 2.4%，OTLP 档分别为 +6.6%/+0.6%；这两组只描述本轮原值，不能直接称为普遍导出或日志开销。

`minimal` 保留必要指标与警告/错误，filter=warn；`recommended` 使用默认有界 JSON 日志与指标；`detailed` 使用原 `--trace-seconds 300` 限时请求日志。详细档的预热、测量和冷却均在 300 s 内完成。前三档都关闭 OTLP，第17项单独开启，不能由前三档推断导出成本。

2/4 Request lanes 仅改变 P/D/B 活跃 connection 上既有 Request stream 数。C 的两条 Core streams、独立 ACK 流上限 8、每 connection stream 上限 16、connection window 64 KiB、stream window/send buffer 各 16 KiB不变；每 connection 逻辑 envelope 仍为 `16 KiB×16+64 KiB=320 KiB`。SDK pending 16 MiB、native transport 128 MiB/Core 16 MiB相同，报告已逐项比对除 lane 外的 SDK 配置。它保留类别隔离，尚不覆盖共享连接/每请求短流或不同 connection/window 布局的完整对照。

### 排队、流控与控制公平性

以下是同一 measured 阶段 SDK 和原生 QUIC 的累计增量。SDK准入包括其有界状态/额度等待；发送等待不是服务执行时延。blocked 是原生帧通知次数，不是被阻塞的消息数；应用帧字节不等于 UDP 线速字节。

| # | SDK准入累计 ms | QUIC发送等待累计 ms | DATA / STREAM_DATA / STREAMS blocked | frame-read/decoding失败增量 | 应用发送 / 接收 MiB |
| --- | ---: | ---: | --- | ---: | ---: |
| 1 | 0.414 | 27.740 | 0 / 0 / 0 | 4 | 1.190 / 1.200 |
| 2 | 0.044 | 174.649 | 0 / 1299 / 0 | 0 | 16.028 / 16.023 |
| 3 | 0.215 | 15.259 | 0 / 0 / 0 | 4 | 0.602 / 0.607 |
| 4 | 0.054 | 248.759 | 0 / 1528 / 0 | 0 | 16.068 / 16.066 |
| 5 | 0.387 | 37.799 | 0 / 0 / 0 | 4 | 1.191 / 1.201 |
| 6 | 0.038 | 186.250 | 0 / 1353 / 0 | 0 | 16.028 / 16.023 |
| 7 | 0.298 | 15.857 | 0 / 0 / 0 | 4 | 0.601 / 0.606 |
| 8 | 0.050 | 178.024 | 0 / 1327 / 0 | 0 | 16.068 / 16.067 |
| 9 | 0.391 | 31.667 | 0 / 0 / 0 | 4 | 1.192 / 1.202 |
| 10 | 0.052 | 188.280 | 0 / 1233 / 0 | 0 | 16.028 / 16.023 |
| 11 | 0.225 | 15.979 | 0 / 0 / 0 | 4 | 0.602 / 0.608 |
| 12 | 0.057 | 175.743 | 0 / 1336 / 0 | 0 | 16.068 / 16.066 |
| 13 | 0.397 | 34.062 | 0 / 0 / 0 | 4 | 1.190 / 1.200 |
| 14 | 0.612 | 31.377 | 0 / 0 / 0 | 4 | 1.195 / 1.206 |
| 15 | 0.598 | 33.226 | 0 / 0 / 0 | 2 | 1.190 / 1.200 |
| 16 | 0.051 | 185.409 | 0 / 1352 / 0 | 0 | 16.069 / 16.067 |
| 17 | 0.421 | 29.032 | 0 / 0 / 0 | 4 | 1.189 / 1.199 |

所有场景 GET_TOPIC 错误为 0，ACK 与控制分开记录。large 与 replay 中可见实际 STREAM_DATA_BLOCKED，同时控制查询和持久 ACK 仍完成；本轮没有由此证明所有负载下公平性或队头阻塞问题已消除。

`malformed_total` 的定义是宽口径 frame 读取/解码失败，包含 read_first 错误，不能直接当坏协议包计数。长 small/slow 的增量为 4，2-lane small 为 2，短场景为 0；本轮没有对应业务重试、Unknown、deadline 或探活失败。代码路径与计数形态支持“生产结束后 P connection 达到原生 30 s idle，而 C/D 继续”这一解释，但本轮原快照没有逐帧关闭原因，不能确认每一条的归因。`active_connections` 统计仍被持有的连接状态/资源，缓存引用可保留 native 已关闭的状态，不能凭它不变断言原生连接未关闭。

独立纯测试提交 `b55c4ab` 的 [native_idle_produce_connection_records_lane_read_errors_while_control_progresses](../../tests/server_dispatch.rs) 将 native idle 缩为 1 s：P4 成功生产一条后静止，C 在相同 endpoint 持续 52 次 Diagnose；250 ms 时 P 仍开且 malformed=0，约 1.222 s 时 P/native+RPC 已关闭，client/server malformed 各增加 4，deadline/liveness failure/rejected 均为 0，同 target 的 4 项测试共 1.76 s 通过。这支持上述解释，未改本轮1398353生产源码或重新解释为原17项逐帧定位。相关 transport 语义见 [QUIC 设计](../design/quic-transport.zh-CN.md)。

## 原始阶段指标与 I/O 口径

每阶段保留前后完整 OpenMetrics，全部快照均为唯一且结尾的 EOF。以下是 #1 measured 的真实差值，来自原始 stage sum/count。

| operation / stage | 次数 | 累计 s | 均值 ms |
| --- | ---: | ---: | ---: |
| Produce / Admission | 128 | 0.005352 | 0.041815 |
| Produce / Queue | 128 | 0.002108 | 0.016471 |
| Produce / Commit | 128 | 8.659831 | 67.654932 |
| Produce / WalAdmission | 9376 | 0.009564 | 0.001020 |
| Produce / WalQueue | 9376 | 22.784521 | 2.430090 |
| Produce / Append | 9376 | 0.172600 | 0.018409 |
| Produce / Sync | 9376 | 43.275756 | 4.615588 |
| Produce / CommitPersist | 4688 | 33.152933 | 7.071871 |
| Produce / Apply | 4688 | 0.076534 | 0.016326 |
| Read / Total | 233 | 59.896984 | 257.068603 |
| Ack / Total | 232 | 3.657898 | 15.766802 |
| Control / Total | 1592 | 3.963886 | 2.489878 |

`operation=Produce` 的 WAL/Apply/Sync/CommitPersist 系列是同 DataGroup 的标签，也覆盖 RecordDelivery、ACK、ReceivedBarrier 等命令；不能把 4688 次 Apply 或 9376 次 Sync 当新生产 batch。独立 producer_new_batches/new_messages/new_bytes 增量为 **128 / 4096 / 557056**，与 SDK 生产账本相符。

同一 #1 measured 中 Produce 的 Decode、Response、Replicate 的 count/sum 前后均为零，当前入口未提供这些阶段的有效延迟样本；不能将其解释为零成本，也不能由 RF1 推断跨节点复制延迟。完整阶段装配与对应负载仍需补验。

Read Total 累计约 59.897 s，DataGroup 的 Sync 累计约 43.276 s、WalQueue 约 22.785 s；这些为累计、可嵌套或并发的阶段时间，不能相加当消费 wall time。本轮显示有必要进一步验证投递簿记持久化、WAL 排队和 group commit 的批量化改进，优化仍须保留相同 quorum、token、可见性与 ACK 语义；当前没有证明单一因果瓶颈。

下表为最终诊断中的 WAL 运行累计计数与制品实际字节，**含初始化、预热、测量和冷却**。written_bytes 是应用编码/写入计数，sync 为 journal 计数，可能包含检查点重写；它们不是块设备累计 I/O 或物理写放大测量。data 占用取 `du -sk cluster/data-1`，是当前文件系统分配量，既不等于累计写字节，也未合并 RustFS 数据目录。

| # | Data WAL bytes / records / sync | Metadata WAL bytes / records / sync | 本地data占用 KiB | report / metrics / diagnostics / service-log B | RustFS日志 B |
| --- | ---: | ---: | ---: | --- | ---: |
| 1 | 2519600 / 10024 / 10025 | 4720 / 36 / 36 | 916 | 751836 / 138633 / 5411 / 4180 | — |
| 2 | 18945927 / 448 / 448 | 4717 / 36 / 36 | 19480 | 631586 / 138425 / 5311 / 3885 | — |
| 3 | 1264101 / 5048 / 5048 | 4720 / 36 / 36 | 2068 | 694298 / 138524 / 5313 / 3885 | — |
| 4 | 18003670 / 872 / 872 | 21410 / 166 / 166 | 18608 | 684436 / 145353 / 5369 / 5065 | 0 |
| 5 | 2519601 / 10024 / 10025 | 4720 / 36 / 36 | 912 | 778776 / 138623 / 17418 / 4180 | — |
| 6 | 18945927 / 448 / 448 | 4717 / 36 / 36 | 19480 | 660883 / 138407 / 17295 / 3885 | — |
| 7 | 1263578 / 5044 / 5044 | 4720 / 36 / 36 | 2068 | 722413 / 138537 / 17304 / 3885 | — |
| 8 | 18003670 / 872 / 872 | 21900 / 170 / 170 | 18608 | 713315 / 145361 / 17383 / 5065 | 552 |
| 9 | 2520647 / 10032 / 10033 | 4720 / 36 / 36 | 916 | 770793 / 138603 / 5558 / 4180 | — |
| 10 | 18945927 / 448 / 448 | 4717 / 36 / 36 | 19480 | 633840 / 138400 / 5459 / 3885 | — |
| 11 | 1264101 / 5048 / 5048 | 4720 / 36 / 36 | 2068 | 704234 / 138519 / 5460 / 3885 | — |
| 12 | 18003670 / 872 / 872 | 21410 / 166 / 166 | 18608 | 689825 / 145384 / 5519 / 5065 | 552 |
| 13 | 2519601 / 10024 / 10025 | 4720 / 36 / 36 | 912 | 751896 / 138637 / 5412 / 0 | — |
| 14 | 2522216 / 10044 / 10045 | 4720 / 36 / 36 | 916 | 755318 / 138638 / 5411 / 904860 | — |
| 15 | 2519600 / 10024 / 10025 | 4720 / 36 / 36 | 912 | 751869 / 138623 / 5411 / 4180 | — |
| 16 | 18003670 / 872 / 872 | 21900 / 170 / 170 | 18608 | 686191 / 145374 / 5372 / 5065 | 0 |
| 17 | 2519078 / 10020 / 10021 | 4720 / 36 / 36 | 912 | 796435 / 141107 / 5654 / 4208 | — |

详细日志档的 service.log 为 904,860 B，minimal 为 0 B；其余实际输出如表，均低于原 32 MiB 日志上限。全部最终诊断 log_dropped_events、log_oversized_events、log_sink_errors 为 0。报告均小于原 8 MiB 上限；指标/诊断 HTTP reader 保留独立 262144 B body 与 1024 B header 上限和原 2 s 期限。

CPU/RSS 仅对应 TideMQ 目标 PID，replay 的 RustFS 进程没有连续资源采样。macOS 的 cgroup current/limit 字节为 null、limit=Unsupported，不写成 0。应用预算、SDK预算、allocator resident、RSS 和 cgroup 口径不能相加。allocator 只有实际快照，本轮未开启 heap-profiling 或执行符号化堆采样；另有 [jemalloc 发布构建证据](2026-10-19-allocator-release.zh-CN.md)，不能冒充本轮 System/mimalloc 的现场 profile。

## 冷读与对象存储

使用已核验的 RustFS 1.0.3 本地独立进程，保持 aws-sdk-s3 的真实对象路径。四个 replay（#4/#8/#12/#16）测量 cache miss 均从 1 增至 4（+3）、hit +18，physical_cold_io_observed=true，最终 ACK 前沿均为 256；各阶段 S3 SDK attempts +15，SDK retries、HTTP error 和 admission rejected 增量均为 0。

这些是经过真实对象存储、至少出现 miss 的冷热证据，没有清空 RustFS、OS 页缓存或物理磁盘缓存，也不由三次 miss 声称 256 条都来自物理冷磁盘。应用解码缓存和已驻留对象会形成热命中；本轮未执行独立物理冷缓存矩阵或完整冷热源占比/设备 I/O 对照。

## 第17项：实际 OTLP 接收与独立 Collector 资源

官方 core Collector 0.162.0，二进制 SHA-256 `72ea1f0bca7ed32039381b95c8a093dfd2247d630ea145d79bc63dbee499173a`；复制的仓库 Collector 配置 SHA-256 `6ad3b5925a9afdc6a530c2bba827f50ad5fe101236916499ab7c32736a414fd2`。只接收本地 HTTP/protobuf 后进入 nop，不持久化，SDK 不安装宿主 subscriber/exporter。

Collector 显式 GOMAXPROCS=2、GOMEMLIMIT=96MiB，后者是 Go 软限制，native 运行没有容器 CPU/RSS 硬限制；memory limiter 128 MiB、spike 32 MiB。服务配置为 1% head sampling、128 span/s、queue 256、batch 32、间隔/总 timeout 各500 ms。每 span 4096 B 队列额度是逻辑 charge，不是 RSS。

| 服务追踪指标 | measured 前 | measured 后 | 最终诊断 |
| --- | ---: | ---: | ---: |
| admitted | 4 | 53 | 53 |
| sampled_out | 232 | 3784 | 3834 |
| exports | 3 | 47 | 47 |
| export_bytes | 1092 | 14855 | 14855 |
| budget/invalid/discarded/failures/timeouts/response_rejected/shutdown_timeouts | 全0 | 全0 | 全0 |
| pending_spans / pending_bytes | 0 / 0 | 0 / 0 | 0 / 0 |

Collector before 的 accepted/refused/failed 系列均为 null（尚未出现）；after 与 final 实际为 accepted=53、refused=0、failed=0，capture_errors=0，status=received。这是 owned Collector 从启动、预热、测量、冷却到服务关闭的接收总量，不能冒充只在 measured 内收到53条。服务 measured 准入增量为49；异步导出可跨区间完成。

| Collector 自监控 | 负载前 | 负载+冷却后 | 服务退出后 |
| --- | ---: | ---: | ---: |
| 累计 CPU s | 0.036611 | 0.132021 | 0.133017 |
| RSS MiB | 69.22 | 71.31 | 71.48 |
| runtime heap alloc B | 9845968 | 10852928 | 10852928 |
| uptime s | 2.401607 | 77.891152 | 78.034837 |

三次快照的 before→final CPU 差约0.096406 s，最后RSS为71.48MiB；它们不构成全程峰值，不能与TideMQ measured CPU/RSS直接相加。第17项具备非零真实导出接收证据，但1%采样、本地明文HTTP、nop和一次短跑不代表远程HTTPS、持久追踪后端、Collector故障成本或SDK exporter成本。完整口径见[追踪运维](../operations/tracing.zh-CN.md)。

## 保留的失败轮次与修复区别

所有旧失败目录均保留私有原物，未覆盖、删除或当作成功样本并入本轮。以下失败是实际执行结果，不能从最终17个 passed 中省略。

1. **b177dba：6次 setup失败、1次中止。** System四场景及mimalloc small/large在HTTP观测读取阶段失败，mimalloc slow随后在诊断时中止；未形成可用预热/测量性能证据。真实metrics body为138163 B、1743行、单EOF，生产服务允许262144 B，但旧bench reader总上限132096 B。`7822080`仅修bench reader独立body/header边界、Content-Length一致性及有限读取，真实TCP 10项通过；未改服务或业务期限。
2. **10eea25：首System-small的两个phase均Read Unknown。** warmup提交256、measured提交4096，均0交付/ACK；分别在约2.913/2.904 s消费段收到Unavailable/Unknown。produce样本8/128，receive仅各1次成功空返回，ACK样本0。随后单Producer、512×1KiB、默认credit的真实RF1回归在首次fill约2.561352 s复现相同错误。`bd28`将新签发准入收紧为250 ms slice，同时保留原总deadline与所有lease最早界限；合流后同回归32页完整ACK512，16次仍有完整credit时返回非空partial，最长约295844µs。250ms不是整个RPC的硬时延上限，快平台允许合法满256条；回归不以人为sleep或改credit造partial。
3. **f4d400a：warmup通过，measured缺投递。** 4096条全部持久生产，2474条ACK，最后成功ACK前沿1468；43.024528s消费段以`Production finished without all expected unique deliveries`失败。失败后代码未查询最终SubscriptionStatus，所以1468不是终态assignment快照。ReadCommitted增量242、recv成功样本241、ACK141；SDK retries+1、Unknown+0，剩余16项立即未运行。只读辅助检查发现1468—1485的18条已签发未ACK，1486—2491有1006条ACK洞，合计占满1024窗口；首批visibility约18:39:17.533Z，而工具约18:39:05.258Z退出，早约12.274s。原100空读门禁、30s可见性和120s总阶段期限没有被放宽。

f4d原WAL为1,844,997 B，整文件SHA-256 `78bb41ee305282a2a7abb63f9c477bdac6a7f534315efa8e01cbe15b7b2bb856`；辅助检查连续消费6542帧、commit/applied=3269，但只解析格式、边界、序号和命令，**没有重验每帧BLAKE3、token MAC或完整Rust状态机回放**，也不能判断哪端先关闭。另行隔离客户端/服务端的真实QUIC回归在修前连续两轮均0 passed/2 failed（约0.44/0.40s），修后2 passed（约0.89s），确认两端都存在闲置sibling lane误关活跃共享连接的问题。`3a86cd1`修复在本轮冻结1398353内；一次性组帧、native idle、探活和原关闭期限保持原值。

上述红绿回归属于功能与诊断证据，不作为正式吞吐或高分位样本；详情见[本地验收证据](2026-10-20-local-validation.zh-CN.md)及[真实批量读取回归](../../tests/runtime_support/read_batches.rs)。

## 制品、退出与仍未放行的范围

当前仓库的私有根目录为 `target/perf-candidates-20261020-1398353` 和 `target/perf-results-20261020-1398353`。前者保留三套服务/负载工具、identity、toolchain、实际SHA账本、私有编排命令和完整matrix console；后者逐case保留report、每阶段原始指标、最终diagnostics、日志及磁盘目录。旧 `b177dba`、`10eea25`、`f4d400a` 目录和失败回归现场均保留。报告/console/WAL核验为0600，试跑根目录0700；证书、token、S3凭据、业务正文、WAL和原始诊断不提交。

17项编排全部exit0，控制台没有forced-termination信息；全部结束后ps无本轮服务/负载/RustFS/Collector，7410/7411 UDP、8410/19000/4318/18888 TCP均已释放。第17项独立记录服务TERM退出0、Collector INT退出0，双方forced=false。**原16项没有独立保存service/RustFS的wait退出码**，不能把编排exit0扩大为每个子进程精确exit0；可见服务日志均正常排空关闭，minimal的warn过滤使其日志为空。该审计边界保留在此，不追补不存在的退出码。

下列是逐case实际服务配置BLAKE3；SDK公共预算及模式/lanes见前文。完整配置和PID/executable核对只保留于私有制品，不公开身份秘密。

| # | 实际服务配置 BLAKE3 |
| --- | --- |
| 1 | `521019ce2a96b8b7b2f9c75d04ac6e28d1ff91089924b51da90d2296567d1e7f` |
| 2 | `b30dbbebe2818dbb8f4c39b6b6f88e435ce12a5c1d8ffaf81be5b31c1aab74e4` |
| 3 | `d6e3525815c0dd9d7ebd6b2530d47f9ccdd7e2fa96e3783cf1d46be5a430aad8` |
| 4 | `f1c76250b7ecc392204b04283538438ce7dc5abe5c04f1e246460382fd76b1e9` |
| 5 | `d5f671866e6ded7f7fd6048884c14ca1d8725c394358b648e4e81dc55ba1095d` |
| 6 | `9bea47b0bbf7532b67e04720ba6a491a6b870fdb724724adc54fcd1d5457f7ea` |
| 7 | `1bceb7dc757998b26183bec462d8ccb5bd2545e79f34132c13736bd1564b3723` |
| 8 | `906225682ad94dd923ea4ddb628ff7ad67874fc01114fe2f3efcd361844fbc93` |
| 9 | `4e12a7814a9371f987082ed191964f04996d8d4081ee183634b2e50f4f60fe3e` |
| 10 | `1ade516b3567da80330fd13bb55422eba2e39c5398885b6f38209d2e97e38695` |
| 11 | `b50faedf8dc879f64db6746237703384fdb1bbb4f9ca4e949705e0bd2b60124f` |
| 12 | `0873300edd4735a40239115b078f8dd2c29883efbf53fe999df3d9a6902b2081` |
| 13 | `5d3070ede915a3f4cf86fc826760c190a944fb9775fb3c3b4310b1954cb15c86` |
| 14 | `8b0a72f5168b77ad51f4ecf980763e40f1915165bdb1e4eedc1fc924da292a0d` |
| 15 | `7a6fa40b15e82462ecf19c9d9e17ee73fea4d69baaa912069cc3fdbf2f6d2677` |
| 16 | `0fca25fbfd795a146ac5d5d3717a63d68693bb3988509a190331ea8e5ef1495f` |
| 17 | `a9c9498c06d377ee1d4fdd0ab9f22ea099a1a54ece9c775804444c9c3c4497f0` |

本轮没有选择生产allocator，也没有放行RF3/三区、跨故障域QUIC连接布局、网络丢包/限速下性能、Linux cgroup/OOM、长期碎片与泄漏、同allocator符号化现场、容量上界或竞品同语义SLO。已取得的是有限RF1闭环、真实SDK/独立服务、持久ACK及可选本地导出的可审计基线；后续性能优化与生产放行仍需独立证据。复现方法与字段定义见[性能使用手册](../performance.zh-CN.md)。
