# Rust SDK 接入边界

包位于 sdk/rust，名称 tidemq-sdk。当前可用 Id、生成的公共 wire 消息、ClientConfig 与预算校验；Client/Producer/Consumer 网络 API 尚未交付。SDK 使用者不需要 protoc、服务端、OpenRaft 或 S3 依赖。

```rust
use tidemq_sdk::ClientConfig;

fn validate_client() -> Result<(), tidemq_sdk::Error> {
    let config = ClientConfig::new("localhost:7443");
    config.validate()
}
```

宿主选择 global allocator、tracing subscriber、meter/exporter 和采集策略。SDK 不安装全局设施、不启端口，不要求其他用户改变分配器。后续 API 继续遵守总 deadline、有界队列/字节、原身份重试和持久 ACK 完成点。

首版无持久 outbox；进程重启后的未决请求不能承诺自动恢复。按 key 处理失败默认阻塞该 key，默认并行允许乱序完成；业务副作用仍需自身幂等。
