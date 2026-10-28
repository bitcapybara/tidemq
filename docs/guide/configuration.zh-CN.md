# 配置来源、单位与校验

服务通过显式 `--config` 读取 TOML，最多 64 KiB，拒绝未知字段。相对数据、证书和秘密文件路径相对启动工作目录。[Combined 示例](https://github.com/bitcapybara/tidemq/blob/main/config/combined.toml)展示 MQ 字段；[本地生成器](running.zh-CN.md)生成可直接启动的完整文件。`validate-config` 校验结构、角色拓扑和预算；运行时才读取证书/秘密内容、打开 WAL 并连接对端和 S3。

`role = "console"` 使用独立的 HTTPS / tls / cluster / session 配置，见本页 Console 表；不要把 MQ 的 `runtime`、`gateway`、`storage` 或 `authorization` 表复制给 Console。

## 每个角色的完整样例

| 角色 | 配置文件 | 样例拓扑与用途 |
| --- | --- | --- |
| Combined | [config/combined.toml](https://github.com/bitcapybara/tidemq/blob/main/config/combined.toml) | 节点 1 的单进程本地执行，包含四个 MQ 职责与 HTTPS Console |
| Gateway | [config/gateway.toml](https://github.com/bitcapybara/tidemq/blob/main/config/gateway.toml) | 分离 RF1 的节点 41，提供应用 QUIC 入口 |
| Broker | [config/broker.toml](https://github.com/bitcapybara/tidemq/blob/main/config/broker.toml) | 分离 RF1 的节点 31，协调服务所有权与投递 |
| Storage | [config/storage.toml](https://github.com/bitcapybara/tidemq/blob/main/config/storage.toml) | 分离 RF1 的节点 21，持久数据组副本 |
| Metadata | [config/metadata.toml](https://github.com/bitcapybara/tidemq/blob/main/config/metadata.toml) | 分离 RF1 的节点 11，持久控制组副本 |
| Console | [config/console.toml](https://github.com/bitcapybara/tidemq/blob/main/config/console.toml) | 独立无状态 HTTPS，连接已部署的 Gateway |

四份分离角色文件共同使用 peers 11/21/31/41、Metadata voters [11]、data voters [21]，不能把它们与 Combined 节点 1 的拓扑混用。它们是待供应证书、私钥、token 和目录的完整字段样例；直接运行仍须先替换部署身份与路径。首次运行优先使用[生成器](running.zh-CN.md)，分离 RF1 使用 `rf1-separated`，分离 RF3 使用 `rf3`，并部署在真实独立故障域。

字段值区分代码默认、部署身份和可选示例。MQ 样例采用代码默认的 **64 MiB 应用预算 / 4 MiB 控制预留**；生成器为了归档验证采用 **192 / 32 MiB**，两者不是同一个默认值。`#? ` 开头的行是禁用的可选块，须完整启用并供应必要参数；对象存储先提高预算，自动 Topic 分片的八项零容量须改成当前 RF 下实测的正数。分离角色中的 `bootstrap = true` 是新集群的运维选择，代码默认是 false；Combined 不接受该字段。

## MQ 字段参考

| 字段 | 作用与边界 |
| --- | --- |
| `node_id` / `cluster_id` / `data_dir` | 非零节点 ID、非 nil UUID、独立持久目录。恢复时保留三者与原有节点证书身份 |
| `role` / `replication_factor` | MQ 角色为 `combined/gateway/broker/storage/metadata`；角色直接决定执行方式；Combined 不接受 replication_factor，分离角色必须显式配置 RF 为1或3，并提供 tls.trust_roots，失去多数派不降级；无额外 mode 开关 |
| `gateway.listen` / `runtime.internal_listen` / `management_listen` | 外部业务 QUIC、内部 mTLS QUIC、管理 HTTP。只有 Gateway/Combined 配置并占用外部监听，Combined 不接受或启动内部监听；管理必须 loopback，同一协议的监听地址不得冲突（包括通配绑定）；QUIC的UDP端口号可与HTTPS/管理TCP相同，两个TCP监听不得重叠；远程管理由受保护代理提供 |
| `memory.total_bytes` / `control_reserved_bytes` | 应用许可预算及其中的控制预留，单位字节；数据不能消耗控制预留。这不是进程 RSS 上限 |
| `memory.max_connections` / `max_streams_per_connection` / `max_pending_requests` | 有限连接、流、未决请求；范围分别为 1—4096、2—4096、1—65536，实际 transport 还按类别限制池和队列 |
| `protocol` | 帧、消息、批次条数和解码字段上限联合校验。默认消息 256 KiB 包含 key 与 body，默认帧 1 MiB，消息必须能放进帧 |
| `logging` | 模块过滤、JSON、有限事件队列与单事件字节。默认 1024×16 KiB；formatter、writer 和活跃事件另占空间，输出交由部署日志管理轮转保留 |
| `tracing` | 默认关闭。启用后使用官方 OTLP HTTP exporter，默认 1% 采样、128 spans/s、队列 256×4096 字节逻辑许可、batch 32、间隔/期限各 500 ms；完整边界见[链路追踪](../operations/tracing.zh-CN.md)。endpoint 只接受字面 IP，HTTP 限 loopback，HTTPS 要求严格 IP SAN 与内置根离线校验；拒绝任意 OTLP 环境覆盖 |
| `tls` | Combined 只接受服务端 certificate/private_key，复用于 QUIC 与 HTTPS；启动检查叶证书当前有效、允许服务端认证、SAN 覆盖公开 Origin 且私钥匹配。分离 MQ 还使用 trust_roots 与 peer DER pin，不信任自行声明的身份 |
| `runtime.metadata_group` / `metadata_voters` / `groups` / `peers` | Combined 只接受 metadata_group（默认1）和 groups[].group_id（默认数据组2，最多16个），ID非零且不重复，owner由node_id派生；分离角色才接受 voters/peers，RF3 voter 须在三个不同 zone |
| `runtime.groups[].standby` | 仅分离角色接受。有限空副本候选；不属于 voter，不自行初始化，待安全 learner/joint/stable 迁移 |
| `runtime.bootstrap` | 仅分离角色接受；在原始身份的全空持久目录初始化；发现 vote/log/commit/snapshot 时走恢复，不能靠删除目录消除未知结果 |
| `authorization.bootstrap_admin_token_file` | 仅首次初始化的 Metadata/Combined 节点读取私有管理员 token 文件，将校验摘要持久提交到 Metadata Group；集群初始化后不再读取。其他角色无需业务 token 清单。凭据、授权、到期和撤销在线管理；确认域密钥由数据组持久保存，无共享密钥文件 |
| `storage.objects` | 仅 Storage/Combined 配置；显式 endpoint/region/bucket/prefix/credentials_file，不使用隐式机器默认凭据链。仅显式开启时允许 loopback HTTP；公网使用 HTTPS |
| `runtime.capacity` / `storage.retention` | 各进程共同使用容量水位；数据角色配置最短保留与有界时间索引。新鲜采样缺失或超限拒绝新写，已有 ACK/read/control/GC 保留各自准入 |
| `storage.archive_interval_ms` | 仅 Storage/Combined 的后台归档/回收工作间隔；对象上传不进入 Producer ACK 关键路径，未知任务按原对象身份对账 |

## 按角色组织 TOML

`role` 显式选择一个运行模式；`combined` 同时启用四个 MQ 职责和内置 HTTPS Console。公共身份、内存、协议、TLS、日志和 tracing 在顶层公共表中只配置一次；Combined 的 `[runtime]` 只放逻辑组 ID、容量和业务弹性策略；可信 peer/voter 拓扑、内部监听和 Raft bootstrap 仅属于分离角色。`[authorization]` 不属于 Gateway 私有配置，不能从其他角色的文件中删掉。

| 运行模式 | 必需角色表 | 共享表 |
| --- | --- | --- |
| `gateway` | `[gateway]`，其中 `listen` 必须显式填写 | `[authorization]`、`[runtime]` 和公共设置 |
| `storage` | `[storage]`，可用默认归档间隔和 retention；对象存储按需添加 `[storage.objects]` | 同上 |
| `broker` / `metadata` | 当前没有私有调参项，不添加空表 | 同上 |
| `combined` | `[gateway]`、`[storage]`、`[console]` 与 `[console.session]` | `[authorization]`、逻辑组/容量和公共设置 |

生成器分别输出每个进程的 `node-ID.toml`。例如 RF3 的 `node-41.toml` 只有 Gateway 表，`node-21.toml` 只有 Storage 表，`node-31.toml` 和 `node-11.toml` 只有共享设置；无需将整套角色的配置复制进每个进程。`config/combined.toml` 展示 Combined 配置。配置中误加未启用角色的表、未知字段、缺少必需角色表或公共 `[authorization]` 表时立即失败，不静默忽略。

当前开发阶段只接受这一套按角色组织的配置。旧版顶层 `listen`、`[runtime]` 中的授权/对象/retention/归档字段，以及新旧位置混合都明确拒绝，不提供自动兼容或回退分支。已有开发环境需要按当前示例更新配置，显式填写 `role`、公共 `[authorization]` 和适用的角色表；保留原 cluster/node/group 身份、证书和数据目录，不自动删除已有数据，也不为了重建配置清空 WAL 或快照。

生成器的完整接口为 `bash deploy/local/generate.sh MODE NEW_OUTPUT`，`MODE` 为 `rf1`（Combined）、`rf1-separated` 或 `rf3`（分离角色）。它始终生成当前角色格式，拒绝多余参数。生成目录必须尚不存在，只为新的开发拓扑创建身份；已有持久目录的配置调整应保留其原始身份与密钥。

生成器明确给出 192 MiB 应用预算和 32 MiB 控制预留；每角色样例采用 64 / 4 MiB 代码默认。开启 S3 时至少提升至 192 / 32 MiB，并要求 RSS 高水位至少高于应用预算 64 MiB，保证打包临时缓冲、最终对象和控制工作能同时准入。修改应用预算必须同步检查 RSS、水位与容器限制；原生 QUIC、线程栈、诊断、追踪和 allocator 保留不包含在应用许可里。Linux cgroup v2 另按可见祖先的实际压力做独立门控，无法读取时不能填零。

构建时只能选择一个全局 allocator；默认开启 `allocator-mimalloc`。System 使用 `--no-default-features --features allocator-system`，jemalloc 使用 `--no-default-features --features allocator-jemalloc`；jemalloc 堆采样使用 `--no-default-features --features heap-profiling`。不支持运行中切换，也不能使用 `--all-features`。SDK 不安装全局 allocator、subscriber 或 exporter。统计、采样 feature、native 启动设置和运行中实际生效状态分别核对，详见[内存管理](../design/memory-management.zh-CN.md)与[内存排障](../operations/memory-troubleshooting.zh-CN.md)。

Combined 本地格式与分离 Raft 格式分别校验身份和提交位置，不能交换数据目录。发现不支持的版本应失败关闭；没有旧实验格式的自动迁移工具。升级前保留一致的 WAL/快照和二进制，不手改序列化内容或复制运行中目录。

## Console 配置参考

完整示例为 [config/console.toml](https://github.com/bitcapybara/tidemq/blob/main/config/console.toml)，部署与 Secret 分发见[Console 部署](https://github.com/bitcapybara/tidemq/blob/main/deploy/console/README.zh-CN.md)。Console 不配置 node/cluster 身份、RF、数据目录或 Raft peer。

| 字段 | 含义与边界 |
| --- | --- |
| `role` / `listen` / `management_listen` | `console`、原生 HTTPS 地址、不同的 loopback 管理地址；无需额外 TLS 代理 |
| `tls.certificate` / `tls.private_key` | 服务端 PEM 身份；叶证书当前有效、允许服务端认证、SAN 覆盖 session.public_origin 且私钥匹配；浏览器信任与 SDK 的 MQ CA 信任分别配置 |
| `static_dir` | 与二进制来自同一源码版本的已构建目录，包含 index.html、.vite/manifest.json 与实际必需 JS/CSS；启动捕获内容指纹，运行中检测故障后有界复核 |
| `cluster.address` / `server_name` / `trust_roots` | 固定 Gateway 地址、证书匹配的 DNS 服务名、CA 文件；不接受用户提供任意集群 URL |
| `session.key_file` / `previous_key_files` | 恰好 32 字节的随机当前密钥及最多两个旧验证密钥，所有副本保持一致；启动加载 |
| `session.public_origin` / `max_age_seconds` | 精确 HTTPS Origin；默认最长 1800 秒，同时受真实凭据到期约束 |
| `observability.components` | 最多 32 个固定 ID / 名称 / 健康入口及可选 HTTPS 详情链接；不携带个人 token |
| `observability.alerts_url` / `alerts_detail_url` | 固定 Prometheus `/api/v1/alerts` 和可选 HTTPS 详情；缺失或失败保持未知 |
| `logging` / `tracing` | 沿用有界日志和可选 OTLP；不设置任意转发目标、header 或查询语言 |

Combined 中把下表字段放在 `[console]` 下：`listen`、`static_dir`、`session.*`、`observability.*`；不接受 role、management_listen、tls、cluster 或远程 MQ 配置。会话默认最长1800秒、范围60—3600秒，最多两个旧验证密钥。

Combined 明确拒绝 replication_factor、内部监听、peers、metadata_voters、groups[].voters/standby、bootstrap 与 tls.trust_roots；空数组、空字符串和 false 也不视为“未配置”。不能复用过去的多节点 Combined 配置；没有旧配置或数据格式自动转换。

Combined 在创建数据目录和打开 WAL 前只读校验 Console 资源、会话密钥和 HTTPS 身份；不会替部署生成缺失的 key。部署时须先构建资源并供应真实证书，浏览器另行信任签发 CA。资源损坏后的就绪与重启规则见[Console 故障处理](console.zh-CN.md#就绪和故障)。

`validate-config` 只读输入文件并校验结构、角色条件、Group ID 与预算；不绑定端口、不打开 WAL、不创建目录/会话 key，不读取或生成证书、token、key 内容，不连接 MQ/S3。文件存在性、权限、TLS握手和静态资源完整性在 serve 启动检查。六种角色使用同一命令：

```sh
target/debug/tidemq validate-config --config config/combined.toml
target/debug/tidemq validate-config --config config/console.toml
```
