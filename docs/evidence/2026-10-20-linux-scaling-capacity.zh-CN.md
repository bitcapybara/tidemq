# Linux CI 扩容测试的磁盘余量诊断

本记录基于 `b2dde4bf1351b049320a7da52580d791e0e4a5ca` 的本机回放，以及 CI run `39368917241` 的 jemalloc job `118452077844`。没有修改生产配额、复制预留、共识校验或重试策略。

## 原始观测与边界

自动修复测试在等待 `[21,22,24]` 稳定成员时超时；显式替换返回 `Unavailable / Unknown / retryable`。失败摘要来自 Storage 21，其文件系统可用量为 `1,352,884,224` 字节，本地增长预留为 `402,647,713` 字节。扣除预留后只剩 `950,236,511` 字节，低于额外安全余量 `1,073,741,824` 字节，尚未计入真实复制量。

目标 Storage 24 和 21 使用同一 runner 文件系统、同样的一组日志和快照上限。每组本地增长预留为 `2 * (128 MiB WAL + 64 MiB snapshot)`，再扣除已有本地文件。目标也缺少安全余量是由该配置和同文件系统观测推得；原日志没有直接记录目标 24 的容量。

`remote::Service::prepare` 获取真实源端复制上界和目标观测后，计算 `copy_bytes + pending_reservations + 1 GiB`。`Placement::capacity_block` 先检查 `free_bytes - reserved_bytes`，不足时返回 `FreeCapacity`，尚未持久 prepare 计划或改变 native membership。自动 tick 在初选阶段即使暂按复制量零计算，也仍需额外 1 GiB。运行时将 Operation 错误保守编码为 `Unavailable / Unknown`，因此显式调用的线缆错误本身不能区分磁盘不足与其他失败。

Storage 21 的 `placement.status=starting` 不是 metadata leader 的调度诊断；不能据此断言 ticker 没有启动。传输期限和存活性计数是并存现象，不能替代容量准入证据。

## 反馈循环

- 原始两项 `native_runtime_scaling_` 测试在本机空间充足时通过：2 tests，10.83 秒。
- 只将既有 `scaling_remote` 目标容量源改为上述 CI 的可用和预留字节，真实 RF3 quorum、源端 Estimate、metadata Prepare 和原执行路径在 2.15 秒内复现失败，精确错误为 `Capacity { reason: FreeCapacity }`。
- 保留回归：显式 Prepare 和自动 tick 都必须拒绝不足空间，不能创建持久迁移槽；显式路径还检查实际成员仍为 `[1,2,3]`。恢复空间后继续同一 canonical plan，完成真实成员替换。
- 改进后的 runtime 测试从目标 24 的真实容量样本先检查必要的 1 GiB 余量。失败直接报告可用、预留字节；超时只收集当前节点、三个 Metadata 和目标的有限摘要，不再输出整份 diagnostics。每次 HTTP 读取和响应大小都有上限，原操作等待仍为 30 秒。

## 本机验证与后续

`cargo fmt --all`、包级 tests Clippy 和 workspace 的严格生产 Clippy 均通过。`--lib --test scaling_remote` 为 89 passed、2 ignored，5.23 秒；两个真实 runtime scaling 场景为 2 passed，11.81 秒。

这些结果验证容量门和恢复语义，没有证明清理后的 Linux allocator CI 已通过。CI 临时 runner 应提供足够磁盘空间后重跑对应作业；不能通过降低生产安全余量、跳过成员证明或盲目增加重试绕过该环境条件。本轮没有在 Linux 性能试跑主机上编译或运行负载。
