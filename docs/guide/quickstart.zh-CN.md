# 快速开始

本页在全新的本地 RF1 上完成启动、创建资源、发送、接收和持久 ACK。需要仓库固定的 Rust 1.99.1、Bash 与 OpenSSL；RF1 没有节点级高可用。

## 1. 启动单节点

从仓库根目录执行，输出目录必须尚不存在：

```sh
cargo build --locked -p tidemq -j2
bash deploy/local/generate.sh rf1 /tmp/tidemq-demo
target/debug/tidemq validate-config --config /tmp/tidemq-demo/node-1.toml
bash deploy/local/run.sh /tmp/tidemq-demo target/debug/tidemq
```

运行脚本保留在前台，各节点输出写入生成目录的 `node-ID.log`。Ctrl-C 会向本次启动的进程发送 SIGTERM 并等待退出。重新启动使用同一配置和数据目录，不再运行生成器。

生成器创建私有开发 CA、节点证书和 bootstrap 管理员 token；证书有效七天。不要输出或提交 `access.token`、`bootstrap.token`、CA/节点私钥。它们只适用于隔离开发环境，生产身份管理见[安全指南](../operations/security.zh-CN.md)。

## 2. 确认服务就绪

另开终端：

```sh
curl --fail http://127.0.0.1:8410/healthz
curl --fail http://127.0.0.1:8410/readyz
curl --fail http://127.0.0.1:8410/diagnostics
```

`healthz` 表示本进程能响应；`readyz` 还检查控制可用性、数据初始化和新鲜容量，启动初期可能暂未 ready。失败时先看本次目录的节点日志，不删除 WAL。

## 3. 创建一个使用 SDK 的应用

保留服务终端，在另一个终端从仓库根目录创建独立应用：

```sh
TIDEMQ_SOURCE=$(pwd)
cargo new --edition 2024 /tmp/tidemq-client-demo
cargo add --manifest-path /tmp/tidemq-client-demo/Cargo.toml tidemq-sdk --path "$TIDEMQ_SOURCE/sdk/rust"
cargo add --manifest-path /tmp/tidemq-client-demo/Cargo.toml tokio --features macros,rt-multi-thread
```

把下列完整程序保存为 `/tmp/tidemq-client-demo/src/main.rs`。在这套全新集群中运行一次；再次创建同名资源应按业务查询已有资源，不能在未知结果后直接重建。

```rust
use std::time::Duration;
use tidemq_sdk::{Client, Credentials, Message, RequestContext, TlsTrust};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder(
        "127.0.0.1:7410",
        TlsTrust::ca_file("/tmp/tidemq-demo/ca.pem"),
        Credentials::token_file("/tmp/tidemq-demo/access.token"),
    )
    .server_name("node-1.local")
    .build()
    .await?;
    let admin = client.admin();
    let tenant = admin.create_tenant("demo").await?;
    admin.create_namespace(tenant, "app").await?;
    let topic = "demo/app/events";
    admin.create_topic(topic).await?;
    let queue = client.new_queue_consumer(topic, "jobs").build().await?;
    let producer = client.new_producer(topic).build().await?;
    let receipt = producer.send(Message::new("hello").key("order-42")).await?;
    println!("Committed message {}", receipt.message_id());

    let context = RequestContext::with_timeout(Duration::from_secs(10))?;
    let message = context.scope(queue.receive()).await?;
    println!("Received message {}, bytes={}", message.message_id(), message.payload().len());
    queue.ack(&message).await?;
    println!("Durable acknowledgement completed");

    queue.close().await?;
    producer.close().await?;
    let report = client.close().await?;
    println!("Unresolved sends: {}", report.unresolved_sends());
    Ok(())
}
```

```sh
cargo run --manifest-path /tmp/tidemq-client-demo/Cargo.toml
```

先创建订阅再发送需要保护的消息。成功时可对照发送与接收的 MessageId，并看到持久 ACK 完成。这里用打印代表最小业务处理；实际业务先成功执行幂等副作用，再 ACK。示例使用 bootstrap 管理员便于建资源，应用上线须改用最小权限的独立凭据。

此示例展示成功路径；超时、取消或断线不证明操作未提交。生产使用原 `PendingSend`，确认使用原 `PendingAck`，管理创建使用原 `PendingAdmin` 恢复；不要直接重新运行建资源或发送代码。详见[生产](production.zh-CN.md)、[消费](consumption.zh-CN.md)和[SDK 恢复](rust-sdk.zh-CN.md)。

## 4. 继续使用

不配置对象存储也能运行有界热路径，达到容量上限会拒绝新写。需要归档、冷读和本地正文回收时，阅读[本地 RustFS](../operations/local-rustfs.zh-CN.md)。RF3、Docker、Kubernetes 及正常恢复见[运行指南](running.zh-CN.md)。

`tidemq diagnose` 和 `tidemq metrics` 描述它们自身的新进程；运行中服务使用上述管理 HTTP。观察指标、日志和故障入口分别见[指标](../operations/metrics.zh-CN.md)、[日志](../operations/logging.zh-CN.md)与[排障](../operations/troubleshooting.zh-CN.md)。
