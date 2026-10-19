# macOS release 分配栈探针

此记录验证同一 release jemalloc profiling 构建的统计与可符号化存活分配栈；尚未比较生产 allocator 的性能。

| 项目 | 实测值 |
| --- | --- |
| 源码提交 | f89930519ed31499a37d22b08cbfad713f256a17 |
| Cargo.lock SHA256 | a94527a85de48f81ed63b323d47bd9888857e3db435e8c4a56c9b1953eb20e6c |
| 主机 | macOS 26.6.2 / 25G83，Apple M2 Max，12 核，64 GiB RAM |
| Rust | 1.96.0 / ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96，LLVM 22.1.2 |
| Target | aarch64-apple-darwin |
| Feature | --no-default-features --features heap-profiling |
| 二进制 SHA256 | bd1e9f757565da740c64488d3382c4a2f55a609a055f9d9cac04978ceffdf2d6 |
| 原生 allocator | jemalloc 5.3.1-0-g81034ce1f1373e37dc865038e1bc8eeecf559ce8 |

```sh
cargo build --release --locked --no-default-features --features heap-profiling -j 2
env _RJEM_MALLOC_CONF=prof:true,prof_active:false,lg_prof_sample:17,prof_accum:false,prof_final:false,lg_prof_interval:-1,prof_gdump:false \
  target/release/tidemq allocator-profile-probe --output "$PWD/target/diagnostics-release"
target/release/tidemq allocator-probe --blocks 2
```

原生环境变量仅设置给被测程序，不设置给 Cargo 或 rustc。release 配置保留 line-tables-only，原生栈由 profiling_libunwind 提供。探针函数一次持有 8 MiB，通过另一个线程释放，再次采集。

实测持有 profile 为 3704 字节，包含 8,388,608 字节存活记录；释放 profile 为 3486 字节，对应记录消失。匹配二进制及 profile 中的映射基址经 atos 解析为：

```text
tidemq::allocator::profile::known_live_profile_allocation::h6495e74d8d490856 (in tidemq) + 64
```

另一次 2 MiB 统计探针的 allocated 为 1,537,544 → 3,674,680 → 1,577,528 字节；释放后 resident 仍为 10,600,448 字节，不能据此判断泄漏。统计与栈属于这个启用 profiling 的实际二进制，不代表普通 jemalloc 构建已有历史分配栈。

本机保留副本为 target/evidence-release/tidemq；profile 位于 target/diagnostics-release 下的两个私有 heap-* 目录。持有/释放 profile SHA256 分别为 6687e50bf82bb2cc99bd8436f234e9457aa10f646713c6e9f5a7b6aaba259a2e、8ba7ef3e7543f5a423fb046b45904c078575df9cc23370c620c227b054e53690。二进制与原始堆产物未提交，避免提交机器地址或体积较大的生成物。后续集成分支变更需要重新构建才代表新版本。

尚未验证 Linux、实际服务压力下采集停顿、长时间存活增长、内存限制/OOM，也未验证 mimalloc 的可符号化栈路径。生产默认分配器仍未定案。
