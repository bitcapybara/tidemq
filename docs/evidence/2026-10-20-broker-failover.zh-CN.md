# Broker 真实进程故障自动接管验证

日期：2026-10-20。环境为 `aarch64-apple-darwin`、Rust 1.96.0；native QUIC 使用锁定的 s2n-quic 1.91.0。九个服务进程均运行真实 `tidemq serve`，每进程四个 Tokio worker，使用独立目录、监听端口和测试 CA 签发的 mTLS 身份。Zone 字符串仅用于本地拓扑，不代表真实可用区。

## 完整用例

`independent_rf3_broker_crash_automatically_fences_and_resumes_original_sdk` 的拓扑为 Metadata 11/12/13、Storage 21/22/23、Broker 31/32、Gateway 41。两组 Raft 都保持 RF3，测试不调用人工 takeover，不重建 Producer/Consumer，也不扩大 SDK 重试配置。

1. 原 Producer 写入 offset 0，原 Consumer 收到并将业务 ACK 持久化；经真实 mTLS quorum 查询确认初始 owner 是 31。
2. SIGKILL 31，立即对原 Producer 提交一个短期限批次，保留其 Pending。元数据 Leader 后台自行探测失联并切换到 32。重试断言 operation、producer epoch、sequence 和 digest 保持原值；原 Consumer 继续收到 offset 1 并持久 ACK。
3. 本次完整成功运行中，SIGKILL 到原 SDK Producer 回执和 Consumer 持久 ACK 共 **4.089240375 秒**。随后新鲜 quorum route 的 epoch 和 generation 都严格只增加一。
4. 使用原配置、证书和目录重启 31。测试以 31 的真实 mTLS 身份发送旧 epoch 的 hop 2 OpenProducer 请求，Storage 明确返回 `Fenced`；原 Producer/Consumer 仍经当前 owner 完成 offset 2。
5. SIGKILL Metadata 12/13 和当前 owner 32。新批次不成功；跨越原 `ControlPolicy` 三秒 TTL 后，再次重试原 Pending 仍不成功，Metadata 少数派不能返回新鲜 quorum route。
6. 恢复原 Metadata 12/13，自动 owner 变为存活的 31。原 Pending 获得 offset 3，原 Consumer 持久 ACK frontier 为 4；route epoch/generation 再次严格只增加一。

单次故障时间不能作为 P99、P99.9 或生产 SLO。测试只涉及四条小消息，不是吞吐压测，也不证明跨区网络、负载均衡器或故障组合的生产性能。

## 检查结果

```sh
cargo fmt --all
cargo clippy -p tidemq --tests --no-deps -j2 -- -D warnings
cargo clippy -p tidemq --lib --bins --no-deps -j2 -- \
  -D warnings -D clippy::unwrap_used -D clippy::expect_used \
  -D clippy::panic -D clippy::unimplemented -D clippy::todo \
  -D clippy::indexing_slicing
cargo test -p tidemq --lib --test runtime_queue -j2 -- --nocapture --test-threads=1
```

- fmt、tests clippy、生产 lib/bins 的 panic/indexing 检查全部通过。
- lib：68 passed，0 failed，2.59 秒。包括观察次数与最短时间跨度共同判定、响应恢复清零、新鲜度到期、固定四节点轮转和丢失权威重置证据。
- runtime_queue：8 passed，0 failed，3 ignored，57.35 秒。通过的既有回归还包括 RF1 真实 CLI 强杀恢复、RF3 Storage 强杀与少数派拒绝、自动/显式 Storage 成员替换、计划 Broker 移交，以及稳定 UDP 地址上的 Gateway 强杀和真实数据报故障。
- 三个 ignored 为真实 RustFS 运行时用例，本轮不将它们算入已运行测试；合流后由完整对象存储验证执行。

原始最终日志保留在本 worktree 的 `target/broker-failover-runtime-final.log`。扩大回归的首轮失败原始输出另存 `target/broker-failover-runtime-before-helper-fix.log`：公共 helper 错把 reclaim 期间 `Consumer::next` 的 `StaleDelivery` 直接 unwrap，另一个恢复点错把 readyz 当成随后每次 quorum 读的保证。最终 helper 使用 SDK 正式 `recv` API，并在原期限内等待新鲜只读 status；offset、Pending 身份、持久 ACK、epoch/generation 和旧身份拒绝断言保持严格。

## 尚未覆盖

真实三区、Linux、长期负载、内存压力下的故障恢复和生产恢复分位数仍未执行。已持久化但未 Active 的 grant 继续原 operation/目标；若该目标同时死亡，保守地等待目标恢复或受信显式处理，不自动创建新的终止或改授协议。该边界和排查指标见 [Broker 自动故障接管设计](../design/broker-failover.zh-CN.md)。
