# 2026-10-20 参数清理、错误观察与字符串共享

本次改动基于 `9e80cb7`，范围包括 SDK Producer、Gateway 授权、Runtime 调度和 S3 multipart。

验证前正常合并工具链提交 `a53b9ee`，使用 Rust `1.99.1`；未切换工作分支或复刻其他 worktree 的实现。

## 不使用参数与资源持有

- 删除 `Producer::finish` 未读取的 deadline 参数，原有 RPC deadline 和取消语义继续由提交请求及其拥有的响应 future 执行。
- 删除已经完成统一授权之后，`data_public` 到 `subscription_public` 间未使用的 `Access` 参数。调用方仍先执行 principal/scope、topic/partition、owner epoch 和角色检查。
- 删除 `multipart_worker` 的 `_admission: Admission` 参数；拥有的 `tokio::spawn` future 保持 Admission，在 worker 完成（包括 abort 清理）之后显式释放。调用方取消或丢弃 Upload 不会提前返还容量。
- 新增本地 HTTP fixture：丢弃 Upload 后阻塞实际发出的 S3 abort 请求，检查内存仍被计费；放行 abort 后检查预算恢复。此测试不依赖外部 RustFS。
- `HotRead::_admission`、内存 permit、信号量 permit、对象计时器、WAL 文件锁、取消注册等通过 Drop 产生必要效果。它们不是未使用资源，继续保留。
- `WireService` 等 trait 实现中不需要的 deadline 或采样参数属于统一接口，不能作为独立参数删除。

## 错误观察

S3 multipart 创建失败时，`map_err` 只转换错误；`inspect_err` 负责原有 warning，继续以 `anyhow::Error::new` 包装结构化错误并用 `{error:#}` 输出完整链，然后保留对外的 Unknown 结果。

对 workspace 的 217 个 Rust 文件、960 处 `map_err` 调用执行括号匹配扫描，并检查匹配到的日志宏；修改后未发现日志宏位于 `map_err` 内。

## 字符串复制

Gateway 旧实现每次委托授权会为组合 BTreeMap 查询构造两个临时 String，再为返回的 Access 复制两次。外部请求注册取消回调时，还会再次复制 principal/scope 到取消键及清理句柄。

授权表改为 principal → scope 的 BTreeMap，接受 `&str` 借用查询，返回共享的 `Arc<str>`；取消表和清理句柄只复制引用计数。需要发送内部 protobuf 委托请求时，才生成 wire 层要求的 String。授权表的 principal/scope/operation 限制、相同授权冲突检查及取消隔离保持原样。

无需引入 FastStr；已有标准库共享字符串即可消除这里重复分配的字符串缓冲区。指针同一性测试验证重复授权和 Access 克隆确实共享存储；权限测试验证不同 principal、scope、operation 和连接授权不会串用。

## 验证

新工具链将 `Atomic::fetch_update` 标为过时，七处同签名调用改为 `try_update`，原有 ordering、饱和计算和返回值处理保持不变。OpenRaft trait 强制使用的 `RPCError` 返回类型只在生产 append helper 上加有解释的函数级 lint 豁免；两处测试路由 helper 改为返回实际产生的较小 `Unreachable`，由 trait 调用边界的 `?` 完成转换，删除无用的错误类型泛型。

- `cargo +1.99.1 fmt --all`：通过。
- `cargo +1.99.1 clippy --workspace --all-targets --no-deps --locked -- -D warnings`：通过，涵盖 library、binary、test 和 example 等全部已声明 target。
- `cargo +1.99.1 clippy -p tidemq-sdk --all-targets --all-features --no-deps --locked -- -D warnings`：通过，覆盖 SDK 可选 metrics 和 OpenTelemetry 代码。
- `cargo +1.99.1 nextest run -p tidemq -p tidemq-sdk --features tidemq-sdk/metrics,tidemq-sdk/opentelemetry --locked --test-threads 1 -E 'package(tidemq-sdk) | test(gateway::tests::) | binary(s3_boundaries) | binary(runtime_queue) | binary(raft_cluster)'`：63 通过、210 未选择，98.633 秒。包括授权/存储共享 2、Raft 集成 5、Runtime RF1/RF3/重启/故障恢复 11、S3 边界 7、SDK 单元 7 和实际 QUIC 集成 31。
- 新增 multipart 所有权测试后，再次执行 fmt 和全 target 严格 Clippy，通过；`cargo +1.99.1 nextest run -p tidemq --test s3_boundaries --locked --test-threads 1`：8 通过、0 跳过，2.533 秒。此处包含先前 7 个 S3 测试，不累计为 71 个独立用例。

本地 Nextest 版本为 `0.9.85`。没有执行需要单独提供原生 RustFS `1.0.3` 二进制的 ignored 测试，也没有将这些结果作为新覆盖率或性能基线；集成后的完整 CI 由统一流水线验证。

此改动没有声称端到端吞吐或延迟收益；既有性能流水线继续使用完整工作负载进行比较。
