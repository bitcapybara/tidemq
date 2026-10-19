# 分配器与内存管理

mimalloc 为首选候选，System 与 TiKV jemalloc 为对照。当前基础构建显式选择 System；这不是生产选型结果。只有服务端二进制声明 global_allocator，SDK/协议 crate 没有 allocator 依赖或全局声明。lib/test harness 的 feature 不证明其已使用服务端入口的分配器，统计探针应运行真实 binary。

| 构建 feature | 实际依赖与当前能力 |
| --- | --- |
| allocator-system | 显式 std::alloc::System；细粒度统计为 unavailable，不能填零冒充 |
| allocator-mimalloc | mimalloc 0.1.52 → sys 0.1.49 → 原生 3.3.2；extended 汇总 JSON，不是分配栈；release 详细计数仍受 MI_STAT 影响 |
| allocator-jemalloc | tikv-jemallocator/ctl 0.7.0，stats feature；epoch 刷新后读 allocated/active/resident/retained，非原子过程快照 |
| heap-profiling | jemalloc profiling + profiling_libunwind 的构建能力；运行期配置/启停/dump 仍需独立验证，不等于已提供在线采样命令 |

feature 互斥；替代构建使用 --no-default-features，运行中不切换。默认不覆盖 libc malloc；Rust、FFI、直接 mmap、线程栈和 cgroup 文件缓存分别核对。部分 s2n TLS Rust 回调跟随 Rust allocator，底层 crypto 不据此视为全部覆盖。

MemoryBudget 用总/数据两层许可保留控制容量，失败路径和取消通过 RAII 归还。当前参数限制被显式接入的应用缓冲，不是进程 RSS 硬限制；日志、指标、native allocator 保留及系统内存还需在 T09 全进程容量模型统一计入。

业务逻辑字节与 Vec/String capacity、共享 Bytes/Arc 引用及实际堆占用不同。allocator resident 不是进程 RSS，retained 通常是虚拟地址保留。不能将各口径相加，或将差值/高 RSS 直接称为泄漏。

生产默认定案须有同 allocator 的有效统计和可符号化调用栈/存活增长诊断。静态 mimalloc 不保证普通 heaptrack/LD_PRELOAD 能拦截；其汇总统计不能代替 profiler。诊断构建变更版本/feature/unwinder/布局必须标注，不能据其推断原生产碎片或时延。

OOM 可能 abort 或被 cgroup kill；不承诺完整尾日志。先背压/限制 bulk、缓存和预取，保留 ACK/控制余量，进程故障后仍按持久 quorum/去重/ACK 规则恢复。scrape/只读 diagnose 不强制 collect/purge。
