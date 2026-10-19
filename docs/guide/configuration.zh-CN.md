# 配置来源、单位与校验

基础配置为显式 --config 路径，格式 TOML，最多 64KiB，拒绝未知字段。缺省值见 config/local.toml；相对数据/证书路径相对启动工作目录。validate-config 校验结构与预算，不验证尚未启动的服务是否可达，也不检查证书内容。

| 字段 | 作用/单位与限制 |
| --- | --- |
| node_id / cluster_id | 非零节点 ID、非 nil UUID；默认 cluster_id 仅用于本地示例，实际集群必须统一且不可混用 |
| role / replication_factor | combined/gateway/broker/storage/metadata；RF 只能显式为 1 或 3；配置值不代表已实现该部署模式 |
| listen / management_listen | 业务 QUIC 与独立管理地址，不能相同；当前管理地址必须 loopback，远程需受保护代理 |
| memory.total_bytes / control_reserved_bytes | 被许可缓冲的总/控制预留字节；数据不能占用控制预留，不是进程 RSS 上限 |
| max_connections / max_streams_per_connection / max_pending_requests | 分别为 1—4096、2—4096、1—65536；实际接入点随模块实现 |
| protocol | raw frame 最大 16MiB、控制帧不超过数据帧；消息必须能放入帧，批次与解码字段数协调限制 |
| logging | 模块过滤、JSON、队列事件数与单事件最大字节；默认 1024×16KiB，需另计活跃 formatter、复制、队列和 writer 开销 |
| tls | 证书、私钥和信任根路径；不把私钥或访问令牌写入配置/日志，认证 secret 由后续运行入口显式引用环境或秘密文件 |

构建时选择 allocator，不提供运行中切换。替代 allocator 必须 --no-default-features。编译统计/profile 与 native 启动设置不同；后续实现必须回读生效状态，不能仅检查环境变量是否存在。

目前低频统计命令和无监听配置校验可用。TTL/lease、S3、节点/组放置、认证与完整容量模型尚未在运行时实现，不添加看似可配置但没有行为的占位字段。
