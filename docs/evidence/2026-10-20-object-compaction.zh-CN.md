# 自动对象压缩的本地证据

此记录覆盖自动候选选择、保留前缀裁剪、共享对象存活范围和原任务恢复。执行环境为 macOS 本机，RustFS 1.0.3 strict 模式、aws-sdk-s3 1.152.0、真实 RF1 元数据/数据 Raft 组、磁盘 WAL 与快照；RF1 测试网络没有远端 voter。

新用例在同一数据组创建两个分区，产生共享 pack，发布并完成真实持久检查点后回收全部热正文。S3Store 和 Coordinator 共享同一个 192 MiB application / 32 MiB Core MemoryBudget。用例验证：

- 部分保留水位推进后，`compact_next` 自动发现源对象，只裁剪过期前缀，并保留另一分区全部存活记录。
- 旧对象的读取 pin 继续阻止 DELETE；新替换通过检查点后仍需显式释放 pin，之后实际 S3 DELETE 才成功。
- 在 replacement 注册但尚未上传时重启两个 Raft 组，持久 source hold 仍指向原 replacement ID；再次自动扫描不创建新任务，原 ID 从 S3 冷源重建、发布并通过检查点。
- 某分区整体过期并完成 floor checkpoint 后，下一替换可以省去该分区，其他分区冷读的 offset、key、body 完整一致。
- 注册任务后保留水位再次推进、全部源引用消失时，源 recovery hold 仍阻止删除；原任务必须完成不可逆取消才释放保护。
- 逻辑 floor 之前的冷读被拒绝，不通过压缩恢复已过期消息。

执行命令：

```bash
rtk cargo fmt --all
rtk cargo clippy -p tidemq --tests --no-deps -j2 -- -D warnings
rtk cargo clippy -p tidemq --lib --no-deps -j2 -- -D warnings -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::unimplemented -D clippy::todo -D clippy::indexing_slicing
rtk proxy env TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs rtk cargo test -p tidemq --lib --test lifecycle_compaction --test lifecycle_persistence -j2 -- --include-ignored
rtk git diff --check
```

结果：fmt 成功；两组 clippy 均无告警；测试 70 passed，3 suites，8.52 秒；diff 无空白错误。其中新自动共享压缩 1 例、原生命周期持久化 3 例，其余 66 例为库测试。8.52 秒是整组测试耗时，不是吞吐、恢复延迟或 P99 指标。

这是 Coordinator 与持久生命周期的真实 S3 行为验证；运行时 ticker 的装配及 SDK 端到端压缩回放由运行时集成验证单独记录。不包含最大 64 MiB 对象压力、独立机器掉电、真实三可用区、云 S3 中断或生产吞吐验收。
