# 内存排障流程与工具覆盖

先运行真实服务二进制的 allocator-probe，确认 backend/native_version 与 stats 可读，再分析业务负载。默认 System 的详细 stats 为 null，不能解释为零分配。

1. 对齐队列/缓存/未决任务的条数、字节、capacity 和共享引用：小切片或 Arc 仍可能持有大块，取消不等于任务资源已释放。
2. 对比分配率与存活占用：频繁分配但存活平稳优先找临时对象/复制；存活持续增长再找持有者、无界状态或泄漏。
3. 比较 allocator 与 OS：活跃下降但保留/RSS 不降，继续检查页利用、线程缓存和保留策略；allocator 平稳而 RSS/cgroup 增长则看 native、mmap、栈、文件缓存或同 cgroup 其他进程。
4. 使用相同 allocator/优化/配置和匹配符号，比较基线、稳态、突发、排空及冷却快照。不要只用一次 RSS 差值下结论。

## 当前能力与限制

mimalloc 扩展 JSON 为聚合统计。MI_STAT 与 release/debug 编译有关，运行时 MIMALLOC_SHOW_STATS 不会补开编译掉的计数；当前未提供分配调用栈 API。jemalloc stats 使用 epoch 刷新，allocated、active、resident 和 retained 是不同口径，字段读取也不是原子停机快照。

heap-profiling 仅准备 jemalloc 构建选项，profile 命令和安全 wrapper 尚未交付。jemalloc 还需启动 prof、运行期 active、采样率和有效配置回读，不能事后补齐未采样的历史。profiling_libunwind 需要验证目标平台依赖与并发回溯，不直接把默认 unwinder 当作生产安全路径。

静态 mimalloc 调用 mi_*，普通 malloc interposition/LD_PRELOAD/heaptrack 不保证覆盖。Valgrind 需要正确 annotations 和工具环境；诊断构建必须标注与发布版的差别。Linux heaptrack 采集能力不能推定 macOS 也可采集。当前尚未验证生产同 allocator 的 heap stack 捕获率，因此默认选型未定案。

Linux 的 RSS/cgroup 从 OS 采集，不能将 mimalloc commit 估算重命名为 RSS。macOS 原生采集路径另行验证；系统分析器、权限和符号不假定已安装。OOM 测试只在隔离子进程，不能保证进程终止前成功写出日志或 profile。
