# 配置来源、单位与校验

服务通过显式 `--config` 读取 TOML，最多 64 KiB，拒绝未知字段。相对数据、证书和秘密文件路径相对启动工作目录。`config/local.toml` 展示字段；[本地生成器](running.zh-CN.md)生成可直接启动的完整文件。`validate-config` 校验结构、角色拓扑和预算；运行时才读取证书/秘密内容、打开 WAL 并连接对端和 S3。

| 字段 | 作用与边界 |
| --- | --- |
| `node_id` / `cluster_id` / `data_dir` | 非零节点 ID、非 nil UUID、独立持久目录。恢复时保留三者与原有节点证书身份 |
| `role` / `replication_factor` | `combined/gateway/broker/storage/metadata`；RF 显式为 1 或 3，失去多数派不会降为 RF1 |
| `gateway.listen` / `runtime.internal_listen` / `management_listen` | 外部业务 QUIC、内部 mTLS QUIC、管理 HTTP。只有 Gateway/Combined 配置并占用外部监听；管理必须 loopback，实际启用的地址不能冲突；远程管理由受保护代理提供 |
| `memory.total_bytes` / `control_reserved_bytes` | 应用许可预算及其中的控制预留，单位字节；数据不能消耗控制预留。这不是进程 RSS 上限 |
| `memory.max_connections` / `max_streams_per_connection` / `max_pending_requests` | 有限连接、流、未决请求；范围分别为 1—4096、2—4096、1—65536，实际 transport 还按类别限制池和队列 |
| `protocol` | 帧、消息、批次条数和解码字段上限联合校验。默认消息 256 KiB 包含 key 与 body，默认帧 1 MiB，消息必须能放进帧 |
| `logging` | 模块过滤、JSON、有限事件队列与单事件字节。默认 1024×16 KiB；formatter、writer 和活跃事件另占空间，输出交由部署日志管理轮转保留 |
| `tracing` | 默认关闭。启用后使用官方 OTLP HTTP exporter，默认 1% 采样、128 spans/s、队列 256×4096 字节逻辑许可、batch 32、间隔/期限各 500 ms；完整边界见[链路追踪](../operations/tracing.zh-CN.md)。endpoint 只接受字面 IP，HTTP 限 loopback，HTTPS 要求严格 IP SAN 与内置根离线校验；拒绝任意 OTLP 环境覆盖 |
| `tls` | 证书、私钥和 CA 路径；内部节点还需固定 DER 证书绑定 cluster/node/role/group，不信任自行声明的身份 |
| `runtime.metadata_group` / `metadata_voters` / `groups` / `peers` | 元数据独立组、数据组和有限节点目录；RF3 voter 须具备不同 zone，元数据不承载正文和逐条 ACK |
| `runtime.groups[].standby` | 有限空副本候选；不属于 voter，不自行初始化，待安全 learner/joint/stable 迁移 |
| `runtime.bootstrap` | 仅在原始身份的全空持久目录初始化；发现 vote/log/commit/snapshot 时走恢复，不能靠删除目录消除未知结果 |
| `authorization.access_file` / `delivery_key_file` | 所有角色共用的静态授权和投递签名根路径；内部 delegated 请求也检查授权，Metadata 还派生投递密钥。秘密文件须为当前用户私有，令牌不直接写进配置和日志。授权指定 principal、topic scope、操作白名单 |
| `storage.objects` | 仅 Storage/Combined 配置；显式 endpoint/region/bucket/prefix/credentials_file，不使用隐式机器默认凭据链。仅显式开启时允许 loopback HTTP；公网使用 HTTPS |
| `runtime.capacity` / `storage.retention` | 各进程共同使用容量水位；数据角色配置最短保留与有界时间索引。新鲜采样缺失或超限拒绝新写，已有 ACK/read/control/GC 保留各自准入 |
| `storage.archive_interval_ms` | 仅 Storage/Combined 的后台归档/回收工作间隔；对象上传不进入 Producer ACK 关键路径，未知任务按原对象身份对账 |

## 按角色组织 TOML

`role` 显式选择一个运行模式；`combined` 同时启用四个角色。公共身份、内存、协议、TLS、日志和 tracing 在顶层公共表中只配置一次；`[runtime]` 只放各角色都需要的可信拓扑、内部监听、首次初始化和容量限制。`[authorization]` 不属于 Gateway 私有配置，不能从其他角色的文件中删掉。

| 运行模式 | 必需角色表 | 共享表 |
| --- | --- | --- |
| `gateway` | `[gateway]`，其中 `listen` 必须显式填写 | `[authorization]`、`[runtime]` 和公共设置 |
| `storage` | `[storage]`，可用默认归档间隔和 retention；对象存储按需添加 `[storage.objects]` | 同上 |
| `broker` / `metadata` | 当前没有私有调参项，不添加空表 | 同上 |
| `combined` | `[gateway]` 和 `[storage]` | 同上 |

生成器分别输出每个进程的 `node-ID.toml`。例如 RF3 的 `node-41.toml` 只有 Gateway 表，`node-21.toml` 只有 Storage 表，`node-31.toml` 和 `node-11.toml` 只有共享设置；无需将整套角色的配置复制进每个进程。`config/local.toml` 展示 Combined 配置。配置中误加未启用角色的表、未知字段、缺少必需角色表或授权路径时立即失败，不静默忽略。

当前开发阶段只接受这一套按角色组织的配置。旧版顶层 `listen`、`[runtime]` 中的授权/对象/retention/归档字段，以及新旧位置混合都明确拒绝，不提供自动兼容或回退分支。已有开发环境需要按当前示例更新配置，显式填写 `role`、共享授权路径和适用的角色表；保留原 cluster/node/group 身份、证书和数据目录，不自动删除已有数据，也不为了重建配置清空 WAL 或快照。

生成器的完整接口为 `bash deploy/local/generate.sh MODE NEW_OUTPUT`，`MODE` 为 `rf1`、`rf3` 或 `rf3-combined`。它始终生成当前角色格式，拒绝多余参数。生成目录必须尚不存在，只为新的开发拓扑创建身份；已有持久目录的配置调整应保留其原始身份与密钥。

本地生成配置明确给出 192 MiB 应用预算和 32 MiB 控制预留。开启 S3 时要求至少这些预算，并要求 RSS 高水位至少高于应用预算 64 MiB，保证打包临时缓冲、最终对象和控制工作能同时准入。修改应用预算必须同步检查 RSS、水位与容器限制；原生 QUIC、线程栈、诊断、追踪和 allocator 保留不包含在应用许可里。Linux cgroup v2 另按可见祖先的实际压力做独立门控，无法读取时不能填零。

构建时只能选择一个全局 allocator；默认开启 `allocator-mimalloc`。System 使用 `--no-default-features --features allocator-system`，jemalloc 使用 `--no-default-features --features allocator-jemalloc`；jemalloc 堆采样使用 `--no-default-features --features heap-profiling`。不支持运行中切换，也不能使用 `--all-features`。SDK 不安装全局 allocator、subscriber 或 exporter。统计、采样 feature、native 启动设置和运行中实际生效状态分别核对，详见[内存管理](../design/memory-management.zh-CN.md)与[内存排障](../operations/memory-troubleshooting.zh-CN.md)。

当前数据组与元数据状态格式版本为 2。发现不支持的版本应失败关闭；没有旧实验格式的自动迁移工具。升级前保留一致的 WAL/快照和二进制，不手改序列化内容或复制运行中目录。
