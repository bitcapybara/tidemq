# 2026-10-20 过期回复导致共享 Control stream 关闭

## 原始证据与范围

Rust CI run `39402803302`、System allocator job `118561398038` 在源码 `401c7c9` 的 combined RF3 用例失败。四个 topic 均生产 64 条、完成 64 次 metadata 查询；两个 topic 约 40.3 秒完成所有 durable ACK，另两个在原有 50 秒阶段 deadline 到达时只有 63/61 个不同消息、连续 ACK frontier 为 21/22。

保留的 node 1 日志包含 115 次 leadership read 超时、99 次 RPC 错误及多次 retryable QUOTA heartbeat 拒绝；另外两节点仅有启动和投票记录。`read_rejected_after_delivery` 分别出现在阶段开始约 7.6 秒和 39.7 秒。晚发生的 Unknown Read 仍可能留下完整默认 visibility lease；这些日志不足以唯一归因原始 50 秒失败。

Mac 上使用 Rust `1.99.1`、Nextest `0.9.148`、System allocator 和原始用例执行 10 次独立重复，全部通过。原始 CI 用例尚未在本地复现，因此没有增加其 deadline、重试 CI、删减断言或降低负载。

## 缩小后的真实 QUIC 回路

`tests/server_dispatch.rs::expired_handler_reply_keeps_other_control_requests_live` 使用实际 TLS/QUIC，两个请求共享 Control stream：

1. 第一个服务端处理器被 gate 阻塞，服务端 100 ms deadline 先到期，调用方 500 ms deadline 随后到期。
2. 第二个仍在有效 deadline 内的处理器保持阻塞，确保存在不能被关闭的活跃 waiter。
3. 放行第一个过期处理器，然后发送不阻塞的 Control probe，并最终验证第二个 waiter 仍能收到匹配回复。

修改前，该回路稳定返回 `Closed`：过期结果进入回复队列，writer 调用带过期 deadline 的 `sender.send` 后退出，连带关闭同 stream 的其他 RPC。生产文件 diff 为空时，细化后的回路连续 4 次 red；每次约 0.62 秒。

该回路验证共享响应 writer 的实际故障路径；不把它当作原始 CI 所有超时的唯一原因。

## 修复

共享服务端 writer 在发送之前丢弃已经到达 deadline 的回复。回复的内存/队列 permit 正常释放；不会为过期回复调用 `response_sent` 或触发依赖 flush 的通知。仍在有效 deadline 内的回复继续发送，实际网络写入失败保留原有终止语义。

客户端 writer 已有等价的发送前过期检查。此次生产修改为 3 行检查加 1 行原因注释，没有增加重试、修改协议/lease/deadline 或扩大 admission 配额。

## 验证

- `cargo +1.99.1 fmt --all`：通过。
- System allocator、SDK metrics/OpenTelemetry、protocol codegen 的 workspace 全 target 严格 Clippy：通过。
- System allocator `server_dispatch`：5/5 通过，2.953 秒，包括原有 idle lane、Produce 顺序和 Control 进展验证。
- 新回归用例 Nextest `--stress-count 30`：30/30 通过，18.386 秒。
- 原始 combined System 用例修复后再次执行：1/1 通过，13.977 秒；与修复前本地结果均为 green，不能据此声称消除了 Linux 原始失败。

原始 CI 进程日志与上述 red/green 命令的本地输出保留于本次会话的 `/tmp/tidemq-*20261020*.log`。未将其作为性能或覆盖率基线，也未把 Mac 重复通过解释为 Linux CI 已通过。
