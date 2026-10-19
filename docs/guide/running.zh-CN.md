# 运行 TideMQ

可执行程序的运行入口是 `tidemq serve --config <配置文件>`。Gateway、Broker、Storage、Metadata 和 Combined 使用同一个程序，通过 `role` 选择职责。所有队列与内部节点通信使用 s2n-quic；AWS SDK 到 S3 的 HTTP 请求属于对象存储接口。

## 本地 RF1

在仓库根目录执行：

```sh
cargo build -p tidemq -j2
bash deploy/local/generate.sh rf1 /tmp/tidemq-rf1
target/debug/tidemq validate-config --config /tmp/tidemq-rf1/node-1.toml
bash deploy/local/run.sh /tmp/tidemq-rf1 target/debug/tidemq
```

生成器要求 Bash 和 OpenSSL，只向新的目录写入文件，不覆盖已有身份。它生成七天有效的开发 CA、带客户端和服务端用途的节点证书、DER 证书指纹来源、64 字符访问令牌以及共享投递签名根。目录与密钥只允许当前用户访问。SDK 信任 `ca.pem`，访问令牌从 `access.token` 读取，服务名使用 `node-1.local`，地址为 `127.0.0.1:7410`。访问令牌、私钥和签名根应留在生成目录，不提交到仓库。

管理入口为 `http://127.0.0.1:8410`。`/healthz` 表示进程仍可响应；`/readyz` 还检查元数据控制可用性、本地数据初始化与新鲜容量采样，并在本地数据 Leader 上检查真实 quorum。`/metrics` 返回统一 OpenMetrics 文本；`/diagnostics` 返回有界的本进程诊断。管理监听必须是 loopback，只提供只读接口。`validate-config` 验证拓扑和容量限制，不打开监听或数据目录；证书、私密文件权限和 S3 连接在运行时检查。

Metadata/Combined 的 `/ownership/<topic UUID>/<partition u32>` 按键返回一个分区的 owner、phase、epoch 和 route generation，沿用两秒请求期限及并发、字节限额，不扫描全部分区。`partition` 使用现有的 `"<UUID>/<partition>"` 字符串格式。`source=local_applied` 表示该副本的已应用视图，不能当作线性一致读或写入授权证明；非 Metadata 节点或不存在的分区返回 404，非法键返回 400。本地读取不可用时返回 503，若已耗尽总期限则可能直接断开。故障验证先比对多数 Metadata 的 `active` owner/epoch/route generation，并结合实际生产、持久 ACK 确认结果。

`config/local.toml` 是路径示例，需要自行提供 `certs/` 与 `secrets/`。生成器输出可以直接启动。静态令牌文件中每条授权指定 `principal`、`scope` 与操作白名单；`scope='topic:events'` 只授权该主题，`scope='*'` 用于本地管理员。Gateway 和 Storage 都重新检查业务授权。

## 本地 RF3 与分角色

```sh
bash deploy/local/generate.sh rf3 /tmp/tidemq-rf3
bash deploy/local/run.sh /tmp/tidemq-rf3 target/debug/tidemq
```

该配置启动三个 Metadata、三个 Storage、两个 Broker 和两个 Gateway 独立进程。Metadata 组为 1，数据组为 2。外部 Gateway 地址分别为 `127.0.0.1:7810`、`127.0.0.1:7820`；对应服务名为 `node-41.local`、`node-42.local`。其余节点只开放内部 QUIC 和本地管理入口。各节点端口遵循外部 `7400 + node_id * 10`、内部再加 1、管理再加 1000。

本机的三个 `zone` 仅用于验证拓扑约束；生产环境必须放置在三个真实独立故障域。生产稳定域名由 UDP/QUIC 的四层负载均衡指向 Gateway。SDK 会重新连接和恢复业务状态，已有连接可能中断；应用必须保留未知结果的原批次身份。

`bootstrap=true` 只在指定最小 voter 的全新、未写过 vote/commit/snapshot/WAL 的目录上初始化。重启从原目录恢复。不要为了修复未知结果删除 WAL、改变 cluster/node/group 身份，或在现有集群旁建立第二套初始 voter。数据组可显式配置 `standby=[...]`，为后续 learner 准备空副本；standby 不计入 voter、不独立初始化，也不获得新写权限。

## 接入 RustFS 或 S3

先创建桶，并提供服务端可访问的 S3 endpoint。生成器和本地示例默认分配 192 MiB 应用预算、32 MiB 控制保留以及至少 64 MiB 额外 RSS 水位空间；开启对象存储时验证这些下限，避免临时打包占用和最终对象缓冲永远无法同时准入。修改预算时也必须相应调整 RSS 水位。开发环境可运行经验证的 RustFS 1.0.3；生产 endpoint 使用 HTTPS。为 Storage/Combined 配置添加：

```toml
[runtime.objects]
endpoint = 'http://127.0.0.1:9000'
region = 'us-east-1'
bucket = 'tidemq'
prefix = 'local'
credentials_file = '/private/path/s3.toml'
allow_http_loopback = true
```

凭据文件权限为 `0600`，内容为 `access_key='...'` 和 `secret_key='...'`。运行时使用显式凭据，不隐式读取机器上的默认云凭据链。HTTP 仅允许明确开启的 loopback 开发 endpoint。对象存储配置缺省时只运行热数据路径，不进行对象归档或本地正文回收，达到热存储上限会明确拒绝新写。

配置 S3 后，当前数据 Leader 有界扫描分区并自动执行注册、授权、上传、发布、全 voter 检查点与本地回收。未知结果按原对象 ID 对账，重启从持久目录恢复任务；不会通过生成新对象绕开未知状态。对象上传不在 Producer ACK 的关键路径。冷读在读取期间持有带 TTL 的 manifest pin，并以私有字节验证能力提交投递记录。

## SDK 与关闭

Rust SDK 位于同仓库 `sdk/rust`。初始化时配置固定 Gateway endpoint、CA、访问令牌及预算；生产者按主题/分区/生产者/epoch 维护顺序。客户端收到 Producer ACK 后表示复制 WAL 已 quorum 持久提交。消费者 `ack()` 成功表示游标的持久 ACK，和 SDK 接收到消息、receive barrier 是不同状态。

超时或断线可能留下未知结果。使用 `Producer::pending_batches()` 与 `retry()` 保留原 operation、producer ID、epoch、sequence 和 digest；不以新批次重建相同业务消息。消费者保留原 assignment，并在 fencing 后重新获取状态。SDK 显式拒绝和未知结果的处理见 `sdk/rust/README.md`。

向进程发送 SIGTERM 或 Ctrl-C 会停止新准入并关闭 owned worker、Raft/WAL 与原生 QUIC 端点。关闭使用同一个五秒总期限，失败会返回错误。下次启动对真实 `AddressInUse` 在共用三秒绑定期限内做短退避重试；不设置 `SO_REUSEPORT`，不掩盖重复进程。
