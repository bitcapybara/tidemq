# Console 使用

Combined 内置 Console，与 MQ 在同一进程直接通过 HTTPS 提供页面和同源 API；按登录用户真实权限调用本地业务路径。分离部署使用同一二进制的独立 `role = "console"`，通过 SDK / QUIC 访问固定 Gateway。独立 Console 无状态、不加入 Raft、不挂载 MQ 数据目录；两种 Console 均不持有 Docker / Kubernetes 写权限。

## 部署与登录

Combined 使用节点证书与私钥，以及 `[console]` 中的监听、资源目录、会话 key_file 与 HTTPS public_origin；不用另起 Console、代理或管理监听。分离 Console 另配置顶层 `[tls]` 的服务端证书/私钥和 `[cluster]` 的固定 Gateway、服务名与信任根。两者均需要同版本静态资源、恰好 32 字节随机会话密钥，证书 SAN 必须覆盖公开 Origin。样例配置为[console.toml](https://github.com/bitcapybara/tidemq/blob/main/config/console.toml)，完整打包、Compose、Kubernetes、Secret 权限和密钥轮换步骤见[Console 部署](https://github.com/bitcapybara/tidemq/blob/main/deploy/console/README.zh-CN.md)。

使用自己的 token 登录。会话保存于带 Secure / HttpOnly 的加密 Cookie，不写入浏览器 localStorage；默认最长 30 分钟，还受凭据真实有效期限制。写入须同时满足精确 HTTPS Origin、CSRF 和核心授权，反向代理需要保留 Origin。退出清除本浏览器会话，撤销 token 仍需在凭据管理中执行。

## 日常路径

| 目标 | 操作与结果边界 |
| --- | --- |
| 查看资源与状态 | 从概览、Topic、消费者、身份权限和集群视图进入；仅显示当前凭据允许的数据 |
| 管理业务资源 | 在权限范围内创建/删除 Topic，管理用户或应用身份、凭据与授权，手动发送消息 |
| 核实消费进度 | 区分持久订阅、成员租约、积压、连续 ACK 前沿与观察时间；进度不是业务副作用审计 |
| 查看生产状态 | 在 Topic 相关视图核实 producer 登记和租约；不是全局逐请求实时追踪 |
| 浏览消息 | 选择 Topic、Shard、offset 范围和扫描预算，再按消息 ID、key 或属性筛选 |
| 查看组件与告警 | 核实数据来源、采样时间和 partial/unknown；需要时跳转既有观测系统 |

消息正文需要独立内容检查权限，Produce、Consume 和普通 Inspect 不自动授予正文访问。浏览不加入消费组、不推进投递、不 ACK；范围外、冷数据、预算耗尽或局部失败应按页面结果理解，不能把部分扫描当作完整搜索。当前不提供可靠的精确发布时间筛选。

扩缩容、排空、成员替换、调度和权威迁移使用现有 SDK / CLI，见[扩缩容运维](../operations/scaling.zh-CN.md)。Console 配置不创建平台资源。

## 写入未知与多副本

写入超时、断线或页面重载后查询原操作结果，不用新操作 ID 自动重发。已接受操作与恢复记录由 TideMQ 核心保存；成功恢复不代表又执行一次写入。凭据明文只展示一次，丢失后不能恢复，应按返回的凭据身份撤销并重新签发。

所有 Console 副本共享当前会话密钥及兼容的验证密钥，连接同一固定集群；无持久卷或粘性路由要求。更换集群、公开域名或密钥前核对[部署轮换流程](https://github.com/bitcapybara/tidemq/blob/main/deploy/console/README.zh-CN.md)。

## 就绪和故障

Combined 的 ready 由 MQ、存储与内置 Console 共同决定，必需资源持续失效会使整体停止新准入并收尾。独立 Console 自身 ready 表示本地配置、TLS、密钥和静态资源可用；上游集群不可达时，本地页面仍可用，业务请求会明确失败。节点已登记不等于健康；缺少新鲜 readiness、leader quorum 证明或观测源时保持未知。告警源失败不能表示“零告警”。

先检查 HTTPS / Origin、会话有效期、个人 token 和作用域，分离Console再检查到Gateway的DNS/UDP/证书；Combined检查本地组与进程状态，两者分别检查已配置的外部观测入口。组件健康探针不携带个人凭据，独立容器中的 loopback 不指向其他主机。详见[安全](../operations/security.zh-CN.md)与[排障](../operations/troubleshooting.zh-CN.md)。
