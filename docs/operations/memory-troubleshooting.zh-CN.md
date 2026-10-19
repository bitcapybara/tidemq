# 内存排障流程与工具覆盖

先运行真实服务二进制的 allocator-probe，确认 backend/native_version 与 stats 可读，再分析业务负载。默认 System 的详细 stats 为 null，不能解释为零分配。

1. 对齐队列/缓存/未决任务的条数、字节、capacity 和共享引用：小切片或 Arc 仍可能持有大块，取消不等于任务资源已释放。
2. 对比分配率与存活占用：频繁分配但存活平稳优先找临时对象/复制；存活持续增长再找持有者、无界状态或泄漏。
3. 比较 allocator 与 OS：活跃下降但保留/RSS 不降，继续检查页利用、线程缓存和保留策略；allocator 平稳而 RSS/cgroup 增长则看 native、mmap、栈、文件缓存或同 cgroup 其他进程。
4. 使用相同 allocator/优化/配置和匹配符号，比较基线、稳态、突发、排空及冷却快照。不要只用一次 RSS 差值下结论。

## 当前能力与限制

mimalloc 扩展 JSON 为聚合统计。MI_STAT 与 release/debug 编译有关，运行时 MIMALLOC_SHOW_STATS 不会补开编译掉的计数；当前未提供分配调用栈 API。jemalloc stats 使用 epoch 刷新，allocated、active、resident 和 retained 是不同口径，字段读取也不是原子停机快照。

heap-profiling 提供原生采样 wrapper 与 allocator-profile-probe 命令。先构建，再仅对实际服务二进制设置启动变量，避免把该变量传给使用不同 jemalloc 配置的 Cargo/rustc。当前构建的原生符号前缀为 _rjem_，实际生效入口是 _RJEM_MALLOC_CONF；修改链接方式后必须重新回读验证。

```sh
rtk cargo build --locked --no-default-features --features heap-profiling
install -d -m 700 target/diagnostics
env _RJEM_MALLOC_CONF=prof:true,prof_active:false,lg_prof_sample:17,prof_accum:false,prof_final:false,lg_prof_interval:-1,prof_gdump:false target/debug/tidemq allocator-profile-probe --output "$PWD/target/diagnostics"
```

探针按已知调用栈在阻塞任务分配 8MiB，生成持有和跨线程释放后的两份 heap_v2 文件。采样率是示例值；未被采样的历史不能事后还原。文件包含地址、调用栈和映射，不包含消息正文；仍按敏感诊断产物保管。目录要求真实私有目录、模式 0700，产物 0600，默认最多 8 个；清理选定的旧产物后再采集，不自动删除其他文件。

macOS 使用匹配的二进制、MAPPED_LIBRARIES 中该二进制的加载基址和 @ 行内地址运行 `atos -o <matching-binary> -l <load-base> <address>`，已验证解析到 known_live_profile_allocation。保存匹配二进制及调试符号，再切换 Cargo feature，避免重建覆盖分析对象。Linux 需要 libunwind 开发包，并用匹配版本的 jeprof/符号工具验证；本机证据不替代 Linux 验证。

同一进程的采样和 dump 独占执行，限制采样区间、输出和保留数量。超时不表示同步 native dump 已停止，后续请求会被拒绝直到完成；不会叠加 native 操作。磁盘不足、目录不私有、输出超限或启动 prof 未开启均报错，不能在运行期补开未编译/未启动启用的能力。profile 构建的实际业务停顿和长期成本尚需性能阶段验证。

静态 mimalloc 调用 mi_*，普通 malloc interposition/LD_PRELOAD/heaptrack 不保证覆盖。Valgrind 需要正确 annotations 和工具环境；诊断构建必须标注与发布版的差别。Linux heaptrack 采集能力不能推定 macOS 也可采集。当前尚未验证生产同 allocator 的 heap stack 捕获率，因此默认选型未定案。

Linux 的 RSS/cgroup 从 OS 采集，不能将 mimalloc commit 估算重命名为 RSS。macOS 原生采集路径另行验证；系统分析器、权限和符号不假定已安装。OOM 测试只在隔离子进程，不能保证进程终止前成功写出日志或 profile。
