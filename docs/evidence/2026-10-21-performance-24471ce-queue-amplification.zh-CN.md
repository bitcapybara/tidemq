# 24471ce 性能队列放大诊断与独立 Linux 验证

## Mac 原始结论

原 Mac 完整 32 trial / 64 report 结论保持 **inconclusive，退出 1**。RF1 通过；RF3 的吞吐量和 offered-to-durable-ACK 均值通过，但 `produce_mean_ns` 的 log CI 半宽为 `0.3791735514574764`，超过原策略 `0.15`。八对点估计均改善不构成放行理由。原始构建 manifest SHA256 为 `025f2b3adb57bf99496af58f37a332b32f8a811fca7a792997fec94bfbf2d928`。

本页仅分析该 cohort 的八对 RF3 latency 报告，不删除任何样本、不替换指标、不追加 Mac 试次。机器可读结果与 32 个输入文件的校验值见 [分解结果](wal-segments-performance/20261021-24471ce-queue-amplification.json)。

## 可证伪方向与测量定义

先列四个方向，再从原报告提取字段：计划发送迟到累积、fsync/commit 条件波动、Read/ACK 竞争变化、host 调度或资源差异。原比较函数对既有八对数据再次计算，得到相同的 `inconclusive` 和退出 1；该纯分析反馈没有重新运行负载。

固定 latency 场景为一个 partition、一个 producer、1,024 条测量消息，绝对计划发送间隔 40ms。`examples/perf/workload.rs` 的 `offer_time` 保持最初绝对计划；下一次发送须等待上一次 produce 返回。每条 `produce_batch` 记录从计划发送到完成，`produce_scheduling_lateness` 记录从计划发送到构造消息之前。两者有相同的 1,024 个观测，因此可准确相减其均值：

```text
P_i = completion_i - planned_offer_i
L_i = before_message_construction_i - planned_offer_i
mean(P_i) - mean(L_i) = mean(completion_i - before_message_construction_i)
```

相减后仍包括消息构造、SDK 调用及可能的恢复工作，不把它标为纯 RPC 网络时延，也不将它改作验收指标。

|配对|原 produce 均值，baseline / candidate，秒|调度后到完成均值，baseline / candidate，毫秒|原均值比|均值差中来自迟到的比例|
|---|---|---|---|---|
|1|2.747 / 1.808|45.194 / 43.704|0.658|99.84%|
|2|3.747 / 2.514|46.999 / 44.565|0.671|99.80%|
|3|3.496 / 2.862|46.266 / 45.447|0.819|99.87%|
|4|3.118 / 2.921|46.277 / 45.671|0.937|99.69%|
|5|3.640 / 2.099|46.783 / 43.961|0.577|99.82%|
|6|2.537 / 2.061|44.926 / 43.640|0.812|99.73%|
|7|1.754 / 1.187|43.418 / 42.182|0.676|99.78%|
|8|2.547 / 1.002|45.089 / 41.983|0.393|99.80%|

16 个 trial 中，原 produce 均值的 95.81%–98.75% 是计划发送迟到；八对均值差的 99.69%–99.87% 也来自这一部分。调度后到完成约 41.983–46.999ms，接近但均高于原 40ms 计划间隔；少量单次耗时差异会在有限串行发送中累积成秒级迟到。该代数分解支持队列放大机制，不能独立证明它来自操作系统调度噪声。

## 阶段指标及因果边界

按 node 1 的 measured-phase `server_metrics_after - server_metrics_before` 取 Prometheus histogram 差分，Produce Sync 均值为 6.351–7.565ms，p99 上界均为 50ms；CommitPersist 均值约 8.514–10.120ms。candidate 每 trial 仅一次 WAL checkpoint，32.056–69.509ms。部分 candidate 的 Sync 更慢，但 produce 均值仍改善，因此证据不足以把总体波动单独归因于 fsync。这里是记录到的阶段观测，不能代替物理 I/O 因果探针；也未证明新旧 WalQueue 埋点完全等价，不以该列数值差直接判定排队退化。

八对 candidate 的成功 receive 调用均减少，baseline 为 148–155 次，candidate 为 140–148 次；Control Replicate 观测数从约 17.6k–18.0k 降为约 15.3k–15.9k，与页复用减少 quorum 工作的方向一致。它是产品工作量变化的证据，不是 host 噪声的证明。

全部 16 个 trial 的 SDK transport lost/deadline/rejected/liveness 增量及 produce/ACK Unknown、control errors 均为 0。已有 leader 进程资源采样 RSS 最大值约 84–86MB，平均约 0.10–0.15 个 CPU 核，无本进程 CPU 饱和证据。原报告没有 host 瞬时 I/O、调度或 follower 同相位资源采样，不能排除其他进程或 follower 的瞬时影响，也不能把描述性相关系数作为因果。

## 独立 Linux cohort

声明仅执行一次新的 Linux 完整原策略 cohort，候选与可信工具固定 `24471ce18f26739eefc47af63fdd0c183e9a787c`，基线固定 `693f9bd91a570758f7891f15edb2e95fdcf47864`。它提供另一套隔离测量条件，不能覆盖或重新标记 Mac 的非通过结论。禁止修改策略、业务限额、消息数、8 pairs、4 worker threads、超时、重试或 40ms 计划。

构建与测量各使用独立容器，均为 4 CPU、8GiB RAM、总 RAM+swap 8GiB、512 PID、UID/GID 10001、network none、无发布端口，capabilities 全移除。构建使用私有离线依赖及全新 release target；原 prepare 程序顺序构建四个二进制，每次先 clean workspace。构建容器退出后，测量容器仅获得只读源码/Node 和四个二进制、identity、build log、manifest 的私有副本及新 results，**不挂载 target 或 Cargo registry/cache**。

预检实际 host 为 44 logical CPU、load average 约 2.44/2.98/3.05、可用内存 45.23GB、可用磁盘 81.17GB，既有 TideMQ 验证容器为 0。原业务容器不操作。容器预检实测 Rust/Cargo 1.99.1、Node 22.23.3、`memory.max=8589934592`、`cpu.max=400000 100000`、`pids.max=512`。私有源和依赖准备后，磁盘可用约 78.8GB。

当前 Linux 构建与测量结果待终态；本页不声明放行或基线晋升。完整脚本、分解数据、输入校验和与运行记录保留于 core worktree 的 ignored `target/evidence/performance-24471ce-diagnosis/`；远端私有目录为 `/root/tidemq-validation/performance-24471ce-20261021.XemuKYWG`。测量只监测本次容器的 cgroup，不操作 host cache 或其他容器。
