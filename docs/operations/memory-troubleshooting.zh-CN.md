# 内存排障流程与工具覆盖

服务端默认启用 mimalloc。先运行真实服务二进制的 `allocator-probe`，确认 `backend`、`native_version` 与 `stats`，再分析业务负载。默认值是构建选择，不是性能排名；历史 System 验证记录仍保持其原始口径。

## 构建与切换

| 分配器 | 构建命令 |
| --- | --- |
| mimalloc，默认 | `cargo build --release --locked` |
| mimalloc，显式选择 | `cargo build --release --locked --no-default-features --features allocator-mimalloc` |
| System | `cargo build --release --locked --no-default-features --features allocator-system` |
| jemalloc | `cargo build --release --locked --no-default-features --features allocator-jemalloc` |
| jemalloc 堆采样 | `cargo build --release --locked --no-default-features --features heap-profiling` |

三种 allocator feature 互斥，不能使用 `--all-features`；`heap-profiling` 会启用 jemalloc，也必须先关闭默认 feature。切换后重建、重启，不支持运行中切换。SDK 不声明全局分配器，由宿主决定。System 的详细统计当前为 null，不能解释为零分配。

## mimalloc 诊断

| 手段 | 用途与边界 |
| --- | --- |
| 原生汇总统计 | 已接入 `allocator-probe` 与服务 `/diagnostics` 的 allocator 快照。比较预热、稳态、突发、排空、冷却的同口径变化；JSON 不包含分配调用栈，详细计数受原生 `MI_STAT` 编译设置限制 |
| OS 与应用指标 | 将真实 RSS、`/proc/<pid>/smaps_rollup`、cgroup v2 的 `memory.current`/`memory.stat` 与应用许可、队列、缓存、共享引用分别比较；allocator committed/reserved 不是 RSS，也不覆盖全部 FFI、mmap、线程栈和文件缓存 |
| mimalloc debug/secure 构建 | 在隔离环境辅助检测部分堆损坏、无效释放、重复释放或越界。`debug` 和 `secure` 是 Rust 绑定已有的依赖 feature；它们会改变原生检查、布局及开销，不作为原发布版的性能或碎片证据 |
| ASan / Valgrind | 上游提供对应原生 tracking 构建方式，用于内存访问错误和泄漏诊断。当前 Cargo 绑定没有直接暴露这两种 tracking feature；需要匹配的原生构建、工具及应用插桩，普通 mimalloc 构建不能据此宣称已覆盖 |
| heaptrack 集成 | 静态调用 `mi_*` 不保证被普通 malloc 拦截。需要验证所用版本的实际钩子覆盖，或接入 heaptrack 的自定义分配器事件 API；当前项目未接入该 API，也未验收捕获率 |

默认构建可直接运行有界探针：

```sh
rtk cargo build --release --locked -j2
rtk proxy env MIMALLOC_SHOW_STATS=1 target/release/tidemq allocator-probe --block-bytes 1048576 --blocks 8
```

`MIMALLOC_SHOW_STATS=1` 请求原生统计输出；`MIMALLOC_VERBOSE=1` 可辅助核对启动选项，输出到 stderr。这些环境变量不会补开编译时被禁用的详细计数，也不会产生分配栈。正常服务优先用已有的有界 JSON 快照；不要持续开启详细原生日志替代监控。

需要额外堆检查时使用独立构建目录，保留原二进制和符号：

```sh
rtk proxy env CARGO_TARGET_DIR=target/mimalloc-debug cargo build --release --locked --features mimalloc/debug -j2
rtk proxy env MIMALLOC_SHOW_ERRORS=1 MIMALLOC_SHOW_STATS=1 target/mimalloc-debug/release/tidemq allocator-probe --block-bytes 1048576 --blocks 8
```

`--features mimalloc/secure` 可用于另一份独立诊断构建，不能把安全检查当作完整内存错误检测。上游依据：[Rust 绑定的版本化 feature](https://docs.rs/crate/mimalloc/0.1.52/source/Cargo.toml.orig)、[统计说明](https://microsoft.github.io/mimalloc/using.html)、[ASan 与 Valgrind](https://microsoft.github.io/mimalloc/tools.html)、[heaptrack 自定义分配器事件 API](https://github.com/KDE/heaptrack/blob/master/src/track/heaptrack_api.h)。工具原理存在不等于当前目标平台已验证。

## mimalloc-pprof 候选评估

`mimalloc-pprof` 能直接在其 mimalloc 实现内采样存活分配，并导出 pprof 兼容堆文件，值得作为调用栈诊断候选。本轮核实的发行版本为 **1.1.0**，发行包对应 [80a513d 的源码](https://github.com/zackees/mimalloc-pprof/tree/80a513dfa6f84a16a07b8612c4041acb7cee4091/rust/mimalloc-pprof)，内置原生版本为 **3.5.0**；当前默认依赖仍是官方 Rust 绑定 `mimalloc 0.1.52` / native **3.3.2**。

它是包含采样及其他修改的 allocator fork，不是可附加到当前 allocator 的观察器。两者的 native crate 都声明 `links = "mimalloc"`；即使配置为互斥的 optional feature，Cargo 解析也会拒绝同时出现。接入时必须统一替换底层实现，不能直接新增第四个可并存的 feature。

1.1.0 的 `pprof` 编译 feature 默认关闭，开启后仍需运行时启动采样。同一个生产发行构建若要运行时诊断，应预先编入采样能力、默认停止采样；关闭后不承诺零额外开销，须在真实工作负载下比较。

本机 macOS arm64、Rust 1.96 的独立 release 探针已验证：未启动时没有采样 arena；已知函数持有 8 MiB 的采样存活量为 8 MiB，跨线程 realloc 到 10 MiB 后为 10 MiB，跨线程释放后存活量及栈记录归零，停止后采样 arena 归零。此有限探针最大采样 arena 为 64 KiB、未发生丢样，三份 heap_v2 文件均约 4.1 KiB，目录 0700、文件 0600。匹配二进制用 `atos` 可解析到已知分配函数；原生 macOS module map 的地址还需结合 Mach-O `__TEXT` vmaddr 校正，尚未证明 pprof CLI 可直接完成符号化。

预算仍需单独设计：`ProfConfig.max_profiler_bytes` 只限制持久采样 arena，不包含 snapshot、dump、proto 的临时内存；`dump_to_vec` 还会申请 Rust 缓冲。writer 回调前原生代码已经生成完整缓冲，因此回调限制落盘字节不能限制此前的内存峰值。64 KiB 持久预算压力探针中，8 MiB 业务分配成功，采样丢弃 2914 条且 arena 保持 64 KiB；这只证明本次预算降级，不证明生产 OOM 安全性。必须先 dump 再 stop，stop 会清除样本；采样窗口前的分配也不能事后补采。release 的详细 requested 计数也不能将未维护的零解释为没有分配。

Linux 原生栈捕获使用 frame-pointer walk，接入时须让 Rust 与 C 调用链保留 frame pointers，并验证实际捕获率；当前线程原生栈不等于 Tokio 任务的完整异步因果链。不默认开启 `full`/`owner-gate` 等会进一步改变分配快路径的组合。

该探针不替代 TideMQ 集成、Linux、长稳、CPU/尾延迟或 dump 峰值验收。本轮先保留当前默认实现；候选若后续接入，还须验证重分配失败、采样预算耗尽、已知调用栈覆盖及退出/超时路径，并保留匹配的二进制、编译 feature 与符号身份。

## 排查顺序

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

静态 mimalloc 的当前 Rust 分配入口为 `mi_malloc_aligned`、`mi_zalloc_aligned`、`mi_realloc_aligned` 和 `mi_free`。只观察普通 `malloc` 或未对齐的 `mi_malloc` 不足以证明覆盖全部 Rust 分配；捕获必须验证已知调用栈、持续持有、重分配和跨线程释放。诊断构建必须标注与发布版的差别；Linux 工具的能力不能推定 macOS 也可采集。当前默认已设为 mimalloc，同 allocator 分配栈的目标平台验收仍未完成。

Linux 的 RSS/cgroup 从 OS 采集，不能将 mimalloc commit 估算重命名为 RSS。macOS 原生采集路径另行验证；系统分析器、权限和符号不假定已安装。OOM 测试只在隔离子进程，不能保证进程终止前成功写出日志或 profile。
