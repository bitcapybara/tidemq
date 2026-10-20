# 请求取消、共享缓冲区与性能门禁验证

日期：2026-10-20。以下记录区分已完成的检查、失败实验和后续验收，不将运行中的命令计为通过。

## 实现范围

- Rust SDK 默认从 `ClientConfig.request_timeout_ms` 为一次逻辑调用建立总期限；移除业务调用末尾的 `client.deadline()`。需要跨调用的总预算或取消时，使用 `RequestContext::scope` 与 `tokio_util::sync::CancellationToken`，嵌套调用保留最早期限和父级取消。
- 取消发生在可能提交之后时，生产保留原 `PendingBatch` 并返回 Unknown；取消不证明服务端未执行。主动关闭独立清理 owned I/O。
- 大块、唯一所有的编码 payload 可以通过共享 `Bytes` 分段发送；保留容量计费与最后引用的许可生命周期。小消息、共享/静态存储或无法计费的所有权继续使用有界连续编码。线上和磁盘格式未变。
- [rkyv、FlatBuffers、FlexBuffers 评估](../research/zero-copy-formats.zh-CN.md)依据 crates.io、版本文档和官方格式说明完成；没有新增生产格式或依赖。

## 已完成的本地检查

冻结集成源码 `5393238479ccf6d878e726556c9d6b35e9340971`，macOS arm64、Rust 1.96.0：

| 检查 | 终态结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| workspace tests/examples Clippy，`--no-deps --locked -- -D warnings` | 通过 |
| production lib/bins 的 panic、unwrap、expect、未实现及索引 Clippy 审计 | 通过 |
| `cargo test --workspace --locked -j 2 -- --test-threads=1` | 268 passed，12 ignored，0 failed；含 34 个普通/文档 suite |
| `cargo test -p tidemq-sdk --all-features --locked -j 2 -- --test-threads=1` | 7 个 unit、29 个真实 UDP/TLS integration、2 个文档测试通过 |
| Node 性能工具测试 | 8 项通过，包括已知回退、噪声、缺失报告、持久前沿、合并直方图、HTTP 总期限与过期晋升拒绝 |

SDK 和 runtime 的专项检查与 workspace 有重叠，不能相加为不同测试总数。ignored 中有需要 RustFS/Collector 的既有测试与有限计时探针；本轮没有重新运行全部外部服务测试。真实 RF3 进程运行于同一宿主，不代表三个可用区。

有限 release codec 探针每项 4096 次，连续编码与分段编码耗时分别为：128 B：0.365/0.653 ms；16 KiB：12.803/0.648 ms；256 KiB：19.308/0.625 ms。该探针解释小消息保留原路径的选择；256 KiB 项减少约 1 GiB 逻辑 payload 复制。它不是服务端到端吞吐或 TLS、内核全程零复制证据。

## Linux Docker 首轮失败实验

在用户指定的 `ssh dev` 上，独立目录 `/root/tidemq-performance/20261020-0257`，以现有 Rust 1.96 builder 镜像启动无宿主端口容器，UID 10001，CPU 上限 4，内存上限 8 GiB，PID 上限 2048。只操作本轮拥有的容器、目录与进程，没有停止既有业务容器。基线服务源码为 `9fe9c952276d2a2b7b27a41ca1021926aaa3c283`，候选源码为 `5393238479ccf6d878e726556c9d6b35e9340971`，服务与各自 SDK 都使用实际 release 构建和默认 mimalloc。

`comparison-1/summary.json` 的终态为 failed，没有更新数值基线。

- RF1 五对交错试次都通过完整交付与持久 ACK 校验，吞吐约 57–60 条/秒。吞吐 log ratio 的 95% CI 半宽约 0.03039，生产 p99 约 0.40694，端到端 p99 约 0.12069，超过原 0.03 精度要求，结论为 inconclusive。没有把不能判别的结果计为性能通过。
- 随后的首个 RF3 基线试次在预热阶段失败：四个独立队列各提交 64 条，持久 ACK 分别为 8、8、0、0，返回 `Unavailable / Rejected`。节点日志出现领导权确认失败和 `Raft response does not match its request`。
- 对候选源码另行运行 `rf3-candidate-smoke-1` 同样失败：预热四个队列均提交 64 条，持久 ACK 为 0、48、0、0。该问题同时影响历史基线和候选，不能以替换基线跳过正确性要求。

所有失败试次的原始报告、服务日志、metrics/diagnostics 与 WAL 留在独立输出目录。失败记录包含运行凭据和数据路径，不能直接提交完整目录或公开分享。后续实验必须使用新目录，不覆盖这轮记录。

## CI 验证边界

[Rust CI 39362732099](https://github.com/bitcapybara/tidemq/actions/runs/39362732099) 首次 Ubuntu validate 因 crates.io `socket2` 索引超时失败；allocator-system、allocator-mimalloc、allocator-jemalloc 的 Linux 检查与完整 root package 测试，以及 Linux/macOS heap-profile job 均通过。

仅重跑失败 job 的 attempt 2 中，macOS 的真实分角色 RF3 runtime case 在 `recv` 返回 `Unavailable / Rejected`，9 项 runtime 通过、1 项失败；Ubuntu validate 被矩阵 fail-fast 取消。没有把这些 job 计为通过。后续修复需重新取得终态 CI 证据。

GitHub 当前私有仓库的分支保护 API 返回 403，要求升级套餐或公开仓库；自动性能检查可以失败，但当前不能通过平台设置强制禁止合并。没有变更套餐、仓库可见性或现有权限。

## 尚待本轮后续记录

三节点错误修复、分离延迟与吞吐负载的 A/A 校准、最终数值基线和修复后的 CI 终态将在实际完成后追加。当前记录不表示性能门禁已经可放行，也不表示生产高吞吐、低延迟或三区目标已经达成。
