# 快速开始

从仓库根目录构建服务，生成一套新的本地身份与配置，再启动 RF1：

```sh
cargo build -p tidemq --locked -j2
bash deploy/local/generate.sh rf1 /tmp/tidemq-demo
target/debug/tidemq validate-config --config /tmp/tidemq-demo/node-1.toml
bash deploy/local/run.sh /tmp/tidemq-demo target/debug/tidemq
```

工具链固定为 Rust 1.96.0，生成器需要 Bash 和 OpenSSL。脚本显式使用 P-256 named curve 和 SHA-256，兼容 macOS 系统 LibreSSL 与 OpenSSL；不要删除这些参数而依赖工具的默认编码或签名摘要。输出目录必须不存在；生成的开发证书有效七天。RF1 是单节点持久队列，不提供高可用。完整配置、RF3 分角色运行、后台归档和正常关闭见[运行指南](running.zh-CN.md)。

在另一个终端检查实际服务：

```sh
curl --fail http://127.0.0.1:8410/healthz
curl --fail http://127.0.0.1:8410/readyz
curl --fail http://127.0.0.1:8410/diagnostics
curl --fail http://127.0.0.1:8410/metrics
```

`/healthz` 表示进程能响应，`/readyz` 还检查权威与新鲜容量；启动初期可能尚未 ready。SDK 连接 `127.0.0.1:7410`，TLS 服务名 `node-1.local`，信任生成目录的 `ca.pem`，访问令牌读取 `access.token`。不要输出或提交令牌和私钥。

同仓库的 [Rust SDK 指南](rust-sdk.zh-CN.md)提供 `Client → Admin → Producer → Consumer` 的实际网络用法。先创建 Topic 和订阅，再提交消息；Producer 成功表示 WAL quorum 持久提交，业务处理完成后 `ack()` 成功才表示持久 ACK。冷读的接收屏障不代替业务 ACK。未知生产结果保留 `PendingBatch` 原身份重试，详情见[生产指南](production.zh-CN.md)。

不配置对象存储时仍可运行热数据路径；达到有界热数据或物理容量上限会拒绝新写。需要冷数据和本地正文回收时，按[本地 RustFS](../operations/local-rustfs.zh-CN.md)创建隔离后端，再添加运行指南中的 S3 配置。

诊断当前短生命周期命令进程与运行中服务是两种入口：

```sh
target/debug/tidemq diagnose --config /tmp/tidemq-demo/node-1.toml
target/debug/tidemq metrics
target/debug/tidemq allocator-probe --block-bytes 1048576 --blocks 8
cargo test -p tidemq-sdk --locked
```

上述 `diagnose`、`metrics` 不查询已经运行的节点；线上状态使用管理 HTTP。`allocator-probe` 是最多 64 块、32 MiB 的自身进程探针，不证明吞吐、碎片或泄漏。候选 allocator 的构建、诊断与限制见[内存排障](../operations/memory-troubleshooting.zh-CN.md)。
